"""Nexa Chat Altare agent: outbound encrypted WebSocket to Render gateway.

Runs existing Nexa Chat app in-process, preserving .env, SQLite, media & SMTP.
Do not host public Nexa Chat without HTTPS and an authenticated gateway.
"""
from __future__ import annotations
import asyncio
import base64
import json
import os
from pathlib import Path
from urllib.parse import urlencode

import httpx
import uvicorn
import websockets

ROOT=Path(__file__).resolve().parent
BRIDGE_FILE=ROOT/'data'/'nexa_bridge.env'
if BRIDGE_FILE.is_file():
    for line in BRIDGE_FILE.read_text(encoding='utf-8').splitlines():
        line=line.strip()
        if not line or line.startswith('#') or '=' not in line: continue
        key,value=line.split('=',1)
        if key in ('NEXA_GATEWAY_WSS','NEXA_BRIDGE_SECRET','NEXA_PUBLIC_ORIGIN','NEXA_BRIDGE_PORT'):
            os.environ.setdefault(key,value.strip())

GATEWAY=os.environ.get('NEXA_GATEWAY_WSS','').strip().rstrip('/')
SECRET=os.environ.get('NEXA_BRIDGE_SECRET','').strip()
PUBLIC=os.environ.get('NEXA_PUBLIC_ORIGIN','https://nexachat.devs.li').rstrip('/')
PORT=int(os.getenv('NEXA_BRIDGE_PORT') or os.getenv('SERVER_PORT') or os.getenv('PORT') or '25436')
if not GATEWAY.startswith('wss://') or not GATEWAY.endswith('/__nexa_bridge'):
    raise RuntimeError('NEXA_GATEWAY_WSS must be wss://<your-service>.onrender.com/__nexa_bridge')
if len(SECRET)<32:
    raise RuntimeError('NEXA_BRIDGE_SECRET must contain at least 32 characters')
if not PUBLIC.startswith('https://'):
    raise RuntimeError('NEXA_PUBLIC_ORIGIN must be https://...')
if not (1<=PORT<=65535):
    raise RuntimeError('Invalid port')

# These must be set before app import. A trusted local proxy supplies the original HTTPS scheme.
os.environ['PUBLIC_ORIGIN']=PUBLIC
os.environ['HTTPS_ONLY']='1'

CHUNK=48*1024
MAX_INPUT=16*1024*1024
MAX_OUTPUT=24*1024*1024
SKIP_HEADERS={'connection','keep-alive','proxy-authenticate','proxy-authorization',
              'te','trailer','transfer-encoding','upgrade','content-length','host',
              'x-forwarded-host','x-forwarded-for','x-forwarded-proto','x-real-ip'}

class Agent:
    def __init__(self,bridge):
        self.bridge=bridge
        self.send_lock=asyncio.Lock()
        self.requests={}
        self.streams={}
        self.jobs=set()
        self.closing=False

    async def send(self,data):
        async with self.send_lock:
            await self.bridge.send(json.dumps(data,separators=(',',':'),ensure_ascii=False))

    async def send_bytes(self,typ,ident,payload):
        for start in range(0,len(payload),CHUNK):
            await self.send({'type':typ,'id':ident,
                             'data':base64.b64encode(payload[start:start+CHUNK]).decode('ascii')})

    def spawn(self,coro):
        task=asyncio.create_task(coro)
        self.jobs.add(task)
        task.add_done_callback(lambda t:self.jobs.discard(t))

    async def do_http(self,ident,req):
        try:
            headers={k:v for k,v in req['headers'] if k.lower() not in SKIP_HEADERS}
            host=PUBLIC.removeprefix('https://')
            headers.update({'host':host,'x-forwarded-proto':'https',
                            'x-forwarded-host':host,'x-real-ip':req.get('client_ip','')})
            path=req['path']
            if not path.startswith('/') or path.startswith('//'):
                raise ValueError('Invalid path')
            query=req.get('query','')
            url=f'http://127.0.0.1:{PORT}{path}'
            if query: url+='?'+query
            async with httpx.AsyncClient(timeout=90,follow_redirects=False,trust_env=False) as client:
                result=await client.request(req['method'],url,headers=headers,content=bytes(req['body']))
            body=result.content
            if len(body)>MAX_OUTPUT: raise ValueError('Response larger than 24 MiB')
            out_headers=[[k,v] for k,v in result.headers.multi_items() if k.lower() not in SKIP_HEADERS]
            await self.send({'type':'http_res','id':ident,'status':result.status_code,'headers':out_headers})
            await self.send_bytes('http_res_body',ident,body)
            await self.send({'type':'http_res_end','id':ident})
        except Exception as exc:
            try:
                msg=json.dumps({'detail':'Altare request failed: '+type(exc).__name__}).encode()
                await self.send({'type':'http_res','id':ident,'status':502,
                                 'headers':[['content-type','application/json']]})
                await self.send_bytes('http_res_body',ident,msg)
                await self.send({'type':'http_res_end','id':ident})
            except Exception: pass

    async def open_ws(self,ident,msg):
        try:
            headers=dict(msg.get('headers',[]))
            cookie=headers.get('cookie','')
            origin=headers.get('origin',PUBLIC)
            if origin!=PUBLIC: raise ValueError('WebSocket origin mismatch')
            extra={'X-Forwarded-Proto':'https',
                   'X-Forwarded-Host':PUBLIC.removeprefix('https://')}
            if cookie: extra['Cookie']=cookie
            url=f'ws://127.0.0.1:{PORT}/ws'
            if msg.get('query'): url+='?'+msg['query']
            remote=await websockets.connect(url,origin=origin,additional_headers=extra,
                                             open_timeout=10,ping_interval=25,ping_timeout=25,
                                             max_size=MAX_INPUT,proxy=None)
            self.streams[ident]=remote
            await self.send({'type':'ws_ready','id':ident})
            self.spawn(self.read_ws(ident,remote))
        except Exception:
            await self.send({'type':'ws_failed','id':ident})

    async def read_ws(self,ident,remote):
        try:
            async for data in remote:
                if isinstance(data,str):
                    await self.send({'type':'ws_data','id':ident,'kind':'text','text':data})
                else:
                    await self.send_bytes('ws_bytes_up',ident,data)
        except Exception: pass
        finally:
            self.streams.pop(ident,None)
            try: await self.send({'type':'ws_end','id':ident})
            except Exception: pass

    async def run(self):
        async for raw in self.bridge:
            if len(raw)>200_000: continue
            try: m=json.loads(raw)
            except Exception: continue
            typ=m.get('type'); ident=m.get('id')
            if typ=='ping':
                await self.send({'type':'pong'})
            elif typ=='http_req':
                if not isinstance(ident,str) or len(self.requests)>30: continue
                self.requests[ident]={**m,'body':bytearray()}
            elif typ=='http_body':
                r=self.requests.get(ident)
                if r is not None:
                    chunk=base64.b64decode(m.get('data',''),validate=True)
                    if len(r['body'])+len(chunk)>MAX_INPUT: self.requests.pop(ident,None)
                    else: r['body'].extend(chunk)
            elif typ=='http_end':
                r=self.requests.pop(ident,None)
                if r is not None: self.spawn(self.do_http(ident,r))
            elif typ=='ws_open':
                self.spawn(self.open_ws(ident,m))
            elif typ=='ws_bytes':
                remote=self.streams.get(ident)
                if remote is not None:
                    try: await remote.send(base64.b64decode(m.get('data',''),validate=True))
                    except Exception: pass
            elif typ=='ws_data':
                remote=self.streams.get(ident)
                if remote is not None and m.get('kind')=='text':
                    try: await remote.send(m.get('text',''))
                    except Exception: pass
            elif typ=='ws_end':
                remote=self.streams.pop(ident,None)
                if remote is not None:
                    try: await remote.close()
                    except Exception: pass

    async def close(self):
        for ws in list(self.streams.values()):
            try: await ws.close()
            except Exception: pass
        for job in list(self.jobs): job.cancel()
        if self.jobs: await asyncio.gather(*self.jobs,return_exceptions=True)

async def connect_loop():
    delay=2
    while True:
        try:
            async with websockets.connect(GATEWAY,
                additional_headers={'Authorization':'Bearer '+SECRET},
                max_size=200_000,open_timeout=70,
                ping_interval=25,ping_timeout=30,
                proxy=None) as bridge:
                print('[Nexa Bridge] Secure gateway connected. URL: '+PUBLIC,flush=True)
                delay=2
                agent=Agent(bridge)
                try:
                    async def heartbeat():
                        while True:
                            await asyncio.sleep(90)
                            await agent.send({'type':'pong'})
                    job=asyncio.create_task(heartbeat())
                    try: await agent.run()
                    finally: job.cancel()
                finally:
                    await agent.close()
        except Exception as exc:
            print('[Nexa Bridge] Reconnecting ('+type(exc).__name__+') in '+str(delay)+'s',flush=True)
            await asyncio.sleep(delay)
            delay=min(delay*2,60)

async def main():
    server=uvicorn.Server(uvicorn.Config('app:app',host='0.0.0.0',port=PORT,
                                        proxy_headers=True,forwarded_allow_ips='127.0.0.1',
                                        workers=1,log_level='info'))
    gateway=asyncio.create_task(connect_loop())
    try: await server.serve()
    finally:
        gateway.cancel()
        await asyncio.gather(gateway,return_exceptions=True)

if __name__=='__main__':
    print('[Nexa Bridge] Starting Nexa Chat and encrypted Render gateway...',flush=True)
    asyncio.run(main())

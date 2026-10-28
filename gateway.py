"""Nexa Chat Render gateway: HTTPS edge -> authenticated WSS -> Altare agent.

No passwords, user database or message history are stored on Render.
For testing/hobby use. Free Render is not appropriate for production payments.
"""
import asyncio
import base64
import hmac
import json
import os
import uuid
from dataclasses import dataclass, field

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response, PlainTextResponse

TOKEN = os.environ.get('NEXA_BRIDGE_SECRET', '')
PUBLIC_HOST = os.environ.get('NEXA_PUBLIC_HOST', '').lower().strip()
if len(TOKEN) < 32:
    raise RuntimeError('NEXA_BRIDGE_SECRET must contain at least 32 characters')

CHUNK = 48 * 1024
MAX_BODY = 16 * 1024 * 1024
MAX_RESPONSE = 24 * 1024 * 1024
HOP_HEADERS = {'host','connection','transfer-encoding','te','trailer','keep-alive',
               'proxy-authenticate','proxy-authorization','upgrade','content-length',
               'x-forwarded-for','x-forwarded-host','x-forwarded-proto','x-real-ip'}

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

@dataclass
class HttpState:
    future: asyncio.Future
    status: int = 502
    headers: list = field(default_factory=list)
    body: bytearray = field(default_factory=bytearray)

@dataclass
class BrowserState:
    socket: WebSocket
    opened: asyncio.Future
    outgoing: asyncio.Queue = field(default_factory=asyncio.Queue)

class LinkState:
    def __init__(self):
        self.agent = None
        self.agent_lock = asyncio.Lock()
        self.http = {}
        self.browsers = {}
        self.generation = 0

    async def send(self, payload, expected=None):
        async with self.agent_lock:
            ws = self.agent
            if not ws or (expected is not None and ws is not expected):
                raise ConnectionError('Altare agent offline')
            await ws.send_text(json.dumps(payload, separators=(',', ':'), ensure_ascii=False))

state = LinkState()

async def send_blob(typ, ident, blob, expected=None):
    for start in range(0, len(blob), CHUNK):
        await state.send({'type':typ, 'id':ident,
                          'data':base64.b64encode(blob[start:start+CHUNK]).decode('ascii')}, expected)

@app.get('/__nexa_health')
async def health():
    return {'status':'ok', 'altare_connected':state.agent is not None,
            'service':'Nexa Chat Gateway'}

@app.websocket('/__nexa_bridge')
async def bridge(ws: WebSocket):
    auth=ws.headers.get('authorization','')
    if not hmac.compare_digest(auth, 'Bearer '+TOKEN):
        await ws.close(code=1008)
        return
    await ws.accept()
    prior=state.agent
    state.agent=ws
    state.generation+=1
    generation=state.generation
    if prior:
        try: await prior.close(code=1012)
        except Exception: pass
    print('[Gateway] Altare agent connected',flush=True)
    try:
        while True:
            data=await ws.receive_text()
            if len(data) > 200_000:
                raise ValueError('Frame too large')
            msg=json.loads(data)
            typ=msg.get('type'); ident=msg.get('id')
            h=state.http.get(ident)
            b=state.browsers.get(ident)
            if typ=='pong':
                continue
            if h is not None:
                if typ=='http_res':
                    h.status=int(msg.get('status',502))
                    h.headers=msg.get('headers',[])
                elif typ=='http_res_body':
                    chunk=base64.b64decode(msg.get('data',''),validate=True)
                    if len(h.body)+len(chunk)>MAX_RESPONSE:
                        if not h.future.done(): h.future.set_exception(ValueError('Response exceeds gateway limit'))
                    else: h.body.extend(chunk)
                elif typ=='http_res_end' and not h.future.done():
                    h.future.set_result(h)
            elif b is not None:
                if typ=='ws_ready':
                    if not b.opened.done(): b.opened.set_result(True)
                elif typ=='ws_failed':
                    if not b.opened.done(): b.opened.set_result(False)
                    await b.outgoing.put(('close',None))
                elif typ=='ws_bytes_up':
                    await b.outgoing.put(('bytes',base64.b64decode(msg.get('data',''),validate=True)))
                elif typ=='ws_data':
                    kind=msg.get('kind')
                    if kind=='text': await b.outgoing.put(('text',msg.get('text','')))
                    elif kind=='bytes': await b.outgoing.put(('bytes',base64.b64decode(msg.get('data',''),validate=True)))
                elif typ=='ws_end':
                    await b.outgoing.put(('close',None))
    except (WebSocketDisconnect,RuntimeError,ValueError,json.JSONDecodeError) as exc:
        print('[Gateway] Agent disconnected',type(exc).__name__,flush=True)
    finally:
        if state.agent is ws and state.generation==generation:
            state.agent=None
            for h in list(state.http.values()):
                if not h.future.done(): h.future.set_exception(ConnectionError('Altare disconnected'))
            for b in list(state.browsers.values()):
                if not b.opened.done(): b.opened.set_result(False)
                try: b.outgoing.put_nowait(('close',None))
                except Exception: pass


def check_host(host):
    host=host.split(':',1)[0].lower()
    return not PUBLIC_HOST or host==PUBLIC_HOST

@app.websocket('/ws')
async def user_ws(ws:WebSocket):
    if not check_host(ws.headers.get('host','')) or state.agent is None:
        await ws.close(code=1008)
        return
    ident=uuid.uuid4().hex
    fut=asyncio.get_running_loop().create_future()
    b=BrowserState(ws,fut)
    state.browsers[ident]=b
    agent=state.agent
    client_headers=[]
    for key,value in ws.headers.items():
        if key.lower() in ('cookie','origin','sec-websocket-protocol'):
            client_headers.append([key,value])
    try:
        await state.send({'type':'ws_open','id':ident,'path':'/ws',
                          'query':ws.url.query,'headers':client_headers},agent)
        if not await asyncio.wait_for(fut,timeout=15):
            await ws.close(code=1008)
            return
        await ws.accept()
        async def from_browser():
            while True:
                packet=await ws.receive()
                if packet['type']=='websocket.disconnect': break
                if packet.get('text') is not None:
                    await state.send({'type':'ws_data','id':ident,'kind':'text',
                                      'text':packet['text']},agent)
                if packet.get('bytes') is not None:
                    await send_blob('ws_bytes',ident,packet['bytes'],agent)
        async def to_browser():
            while True:
                kind,payload=await b.outgoing.get()
                if kind=='close': break
                if kind=='text': await ws.send_text(payload)
                elif kind=='bytes': await ws.send_bytes(payload)
        tasks=[asyncio.create_task(from_browser()),asyncio.create_task(to_browser())]
        done,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
        for task in pending: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    except (WebSocketDisconnect,ConnectionError,RuntimeError,asyncio.TimeoutError):
        pass
    finally:
        state.browsers.pop(ident,None)
        try: await state.send({'type':'ws_end','id':ident},agent)
        except Exception: pass
        try: await ws.close()
        except Exception: pass

@app.api_route('/{path:path}',methods=['GET','POST','PUT','PATCH','DELETE','OPTIONS','HEAD'])
async def http_proxy(req:Request,path:str):
    if not check_host(req.headers.get('host','')):
        return PlainTextResponse('Unknown hostname',status_code=421)
    agent=state.agent
    if agent is None:
        return JSONResponse({'detail':'Nexa Chat is reconnecting to Altare. Retry shortly.'},status_code=503,
                            headers={'Retry-After':'10','Cache-Control':'no-store'})
    body=await req.body()
    if len(body)>MAX_BODY:
        return JSONResponse({'detail':'Upload too large'},status_code=413)
    ident=uuid.uuid4().hex
    future=asyncio.get_running_loop().create_future()
    h=HttpState(future)
    state.http[ident]=h
    headers=[[k,v] for k,v in req.headers.items() if k.lower() not in HOP_HEADERS]
    client_ip=(req.headers.get('x-forwarded-for','') or (req.client.host if req.client else '')).split(',')[0].strip()
    try:
        await state.send({'type':'http_req','id':ident,'method':req.method,
                          'path':'/'+path,'query':req.url.query,'headers':headers,
                          'host':PUBLIC_HOST or req.headers.get('host',''),
                          'client_ip':client_ip,'length':len(body)},agent)
        await send_blob('http_body',ident,body,agent)
        await state.send({'type':'http_end','id':ident},agent)
        reply=await asyncio.wait_for(future,timeout=100)
        result=Response(content=bytes(reply.body),status_code=reply.status)
        for pair in reply.headers:
            if isinstance(pair,list) and len(pair)==2 and pair[0].lower() not in HOP_HEADERS:
                result.headers.append(pair[0],pair[1])
        result.headers['Cache-Control']=result.headers.get('Cache-Control','no-store')
        return result
    except (ConnectionError,asyncio.TimeoutError,ValueError) as exc:
        return JSONResponse({'detail':'Gateway unavailable: '+type(exc).__name__},status_code=502,
                            headers={'Cache-Control':'no-store'})
    finally:
        state.http.pop(ident,None)

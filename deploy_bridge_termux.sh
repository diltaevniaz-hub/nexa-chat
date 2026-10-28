#!/data/data/com.termux/files/usr/bin/bash
# Configure Nexa Chat secure Render gateway from an Android device through SFTP.
set -euo pipefail
BASE="$(cd "$(dirname "$0")" && pwd)"
HOST="de-fra01.altare.global"
USER="tenant_4eb46767.a1572947"
PORT=2022
for cmd in sftp openssl mktemp; do
    command -v "$cmd" >/dev/null || { echo "Missing $cmd. Install: pkg install openssh openssl"; exit 1; }
done
printf '\nNexa Chat bridge: Render ⇄ Altare (secure WSS)\n'
read -r -p 'Render URL (e.g. https://nexa-chat-gateway.onrender.com): ' RENDER
RENDER="${RENDER%/}"
case "$RENDER" in https://*.onrender.com) ;; *) echo 'Expected https://<service>.onrender.com'; exit 1 ;; esac
GATEWAY="wss://${RENDER#https://}/__nexa_bridge"
CONF="$HOME/.config/nexachat/nexa_bridge.env"
mkdir -p "${CONF%/*}"
chmod 700 "${CONF%/*}"
if [[ -f "$CONF" ]]; then
  SECRET="$(sed -n 's/^NEXA_BRIDGE_SECRET=//p' "$CONF" | head -1)"
else
  SECRET="$(openssl rand -hex 32)"
fi
[[ ${#SECRET} -ge 32 ]] || { echo 'Bad secret'; exit 1; }
printf 'NEXA_GATEWAY_WSS=%s\nNEXA_BRIDGE_SECRET=%s\nNEXA_PUBLIC_ORIGIN=https://nexachat.devs.li\n' "$GATEWAY" "$SECRET" > "$CONF"
chmod 600 "$CONF"
cat <<EOF

IMPORTANT: Set these environment variables at Render -> Environment:
NEXA_PUBLIC_HOST = nexachat.devs.li
NEXA_BRIDGE_SECRET = $SECRET

The secret is printed ONLY in your Termux terminal. Do not share it.
EOF
read -r -p 'Once saved in Render, type YES to upload to Altare: ' OK
[[ "$OK" == YES ]] || { echo 'Config saved locally; no upload'; exit 0; }
TMP="$(mktemp -d "$HOME/nexa-bridge.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
chmod 700 "$TMP"
# Pull existing requirements, preserving all original dependencies and versions.
printf 'get "requirements.txt" "%s/requirements.txt"\nbye\n' "$TMP" \
 | sftp -P "$PORT" -o BatchMode=no "$USER@$HOST" || { echo 'Cannot read Altare requirements.txt'; exit 1; }
[[ -f "$TMP/requirements.txt" ]] || { echo 'Original requirements file missing'; exit 1; }
cp "$TMP/requirements.txt" "$TMP/requirements.new"
if ! grep -Eq '^httpx([<>=!~]|$)' "$TMP/requirements.new"; then printf '\nhttpx>=0.27,<1\n' >> "$TMP/requirements.new"; fi
if ! grep -Eq '^websockets([<>=!~]|$)' "$TMP/requirements.new"; then printf 'websockets>=15,<16\n' >> "$TMP/requirements.new"; fi
# Backup existing requirements in Termux as well.
cp "$TMP/requirements.txt" "$HOME/nexachat-old-requirements.$(date +%Y%m%d-%H%M%S).txt"
printf 'put "%s/nexa_bridge.py" "nexa_bridge.py"\nput "%s/requirements.new" "requirements.txt"\n-mkdir data\nput "%s" "data/nexa_bridge.env"\nchmod 600 data/nexa_bridge.env\nls -l nexa_bridge.py\nbye\n' \
    "$BASE" "$TMP" "$CONF" \
  | sftp -P "$PORT" -o BatchMode=no "$USER@$HOST"
echo 'Uploaded bridge. Altare Startup: PY_FILE=nexa_bridge.py, REQUIREMENTS_FILE=requirements.txt'
echo 'IMPORTANT: Stop Nexa Chat on Altare first; then set Startup and start it.'
echo 'Do not delete app.py, .env, data/chat.sqlite3, nexa_https.py or other Nexa files.'

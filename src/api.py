"""Zoom → RegisterABot adapter (multi-tenant) — the Zoom twin of the Slack adapter.

Receives Zoom Team Chat messages (and files) over Zoom's outbound event WebSocket and forwards
them to a bot over the RegisterABot relay as a *service client*.

Two tokens, do not confuse them:
  - This adapter's OWN service key (ZOOM_REGISTERABOT_TOKEN) — its identity as a SERVICE on the
    relay, used to send service→bot. Lives in THIS adapter's Infisical.
  - The bot's key lives in the PROFILE (its registerabot binding) — used by the instar
    registerabot connector for the bot to connect as a BOT. None of this adapter's business.

WHICH bot we route to comes from the profile: on launch the gatekeeper calls
POST /slugs/{bot_slug}/connect (multi-tenant interface contract). There is no BOT_SLUG in config.

Zoom's chatbot API (imchat:bot) sends CARDS only — it cannot push binary — so outbound
files/audio/video are delivered as the relay's short-lived hosted links.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import structlog
import websockets

structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"),
                                structlog.dev.ConsoleRenderer()])
log = structlog.get_logger()

# --- creds / config (self-fetched from Infisical by entrypoint.sh) ------------
ZOOM_CLIENT_ID = os.environ.get("ZOOM_CLIENT_ID", "")
ZOOM_CLIENT_SECRET = os.environ.get("ZOOM_CLIENT_SECRET", "")
ZOOM_BOT_JID = os.environ.get("ZOOM_BOT_JID", "")
# Accept either name — Infisical may hold it as ZOOM_SUBSCRIPTION_ID (Zoom's own label) or the
# WS-prefixed name. Required for Zoom's event WebSocket; without it the WS endpoint 404s.
ZOOM_WS_SUBSCRIPTION_ID = (os.environ.get("ZOOM_WS_SUBSCRIPTION_ID")
                           or os.environ.get("ZOOM_SUBSCRIPTION_ID", ""))
ZOOM_VERIFICATION_TOKEN = os.environ.get("ZOOM_VERIFICATION_TOKEN", "")
ZOOM_WS_URL = os.environ.get("ZOOM_WS_URL", "wss://ws.zoom.us/ws")

RELAY_URL = os.environ.get("REGISTERABOT_RELAY_URL", "").rstrip("/")   # wss://relay… (shared)
# This adapter's OWN relay identity (a SERVICE). Its own slug + own key — NOT the profile's.
SERVICE_SLUG = (os.environ.get("ZOOM_REGISTERABOT_SERVICE_SLUG")
                or os.environ.get("REGISTERABOT_SERVICE_SLUG", "zoom-adapter"))
SERVICE_KEY = os.environ.get("ZOOM_REGISTERABOT_TOKEN", "")            # our service key
CONTROL_PORT = int(os.environ.get("CONTROL_PORT", "8092"))
SESSION_TTL = int(os.environ.get("SESSION_TTL", "600"))

TOKEN_URL = "https://zoom.us/oauth/token"
CHATBOT_API = "https://api.zoom.us/v2/im/chat/messages"

# --- relay + routing state ----------------------------------------------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
_active_bot: str | None = None            # which bot we route TO — comes from the profile
# session_id -> {"to_jid", "account_id", "ts"}
_sessions: dict[str, dict] = {}

# --- Zoom S2S token cache -----------------------------------------------------
_zoom_token: str | None = None
_zoom_token_exp: float = 0.0


async def _get_zoom_token() -> str | None:
    """Server-to-Server client-credentials token, cached with a 60s expiry buffer."""
    global _zoom_token, _zoom_token_exp
    if _zoom_token and time.time() < _zoom_token_exp - 60:
        return _zoom_token
    basic = base64.b64encode(f"{ZOOM_CLIENT_ID}:{ZOOM_CLIENT_SECRET}".encode()).decode()
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(f"{TOKEN_URL}?grant_type=client_credentials",
                             headers={"Authorization": f"Basic {basic}"}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            _zoom_token = data.get("access_token")
            _zoom_token_exp = time.time() + int(data.get("expires_in", 3600))
            return _zoom_token
        log.warning("zoom_token_failed", status=r.status_code, body=r.text[:200])
    except Exception as e:
        log.warning("zoom_token_error", error=str(e))
    return None


def _prune_sessions():
    cutoff = time.time() - SESSION_TTL
    for sid in [s for s, v in _sessions.items() if v.get("ts", 0) < cutoff]:
        _sessions.pop(sid, None)


# --- relay client -------------------------------------------------------------
async def _relay_client():
    """Service-client WS to the relay for whichever bot the profile connected us to. We auth
    with OUR OWN service slug + key; the bot slug is the profile's. Reconnects on bot change."""
    global _relay_ws
    while True:
        bot = _active_bot
        if not bot:
            await asyncio.sleep(1)
            continue
        url = f"{RELAY_URL}/ws/service/{SERVICE_SLUG}?key={SERVICE_KEY}&bot={bot}"
        try:
            async with websockets.connect(url, max_size=None) as ws:
                _relay_ws = ws
                log.info("relay_connected", service=SERVICE_SLUG, bot=bot)
                async for raw in ws:
                    if _active_bot != bot:
                        break
                    try:
                        env = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if env.get("session_id"):
                        try:
                            await _dispatch_frame(env)
                        except Exception as e:
                            log.warning("dispatch_error", error=str(e))
        except Exception as e:
            log.warning("relay_disconnected", error=str(e))
        finally:
            _relay_ws = None
        await asyncio.sleep(2)


async def _send_to_bot(sid: str, text: str, user_id: str, attachments: list):
    """Send one chat_request over the relay to the currently-connected bot."""
    if _relay_ws is None or not _active_bot:
        return
    envelope = {
        "v": 1, "type": "chat_request", "session_id": sid,
        "timestamp": int(time.time() * 1000),
        "from": {"kind": "service", "slug": SERVICE_SLUG, "name": "Zoom"},
        "to": {"kind": "bot", "slug": _active_bot},
        "encrypted": False,
        # Attachments go ON THE MESSAGE — the bot-side registerabot connector reads
        # m.get('attachments') per message and forwards them to /process (on_file barrier).
        "payload": json.dumps({
            "messages": [{"role": "user", "content": text, "attachments": attachments}],
            "user_context": {"user_id": user_id},
        }),
    }
    try:
        await _relay_ws.send(json.dumps(envelope))
    except Exception as e:
        log.warning("send_to_bot_failed", error=str(e))


async def _dispatch_frame(env: dict):
    """A bot→service frame arrived on the relay. Route it to the mapped Zoom conversation."""
    dest = _sessions.get(env.get("session_id"))
    if not dest:
        return
    to_jid, account_id = dest["to_jid"], dest["account_id"]
    ftype = env.get("type")
    try:
        payload = json.loads(env.get("payload") or "{}")
    except Exception:
        payload = {}

    if ftype == "final_response":
        reply = payload.get("reply") or ""
        if reply:
            await _send_zoom_reply(to_jid, account_id, reply)
        items = [a for a in (payload.get("attachments") or []) if a.get("type") != "audio"]
        if payload.get("audio"):
            items.append({**payload["audio"], "name": "voice.mp3"})
        refs = [_as_ref(i, i.get("name", "file")) for i in items]
        hosted = [r for r in refs if r.get("url")]
        no_host = len(refs) - len(hosted)
        if hosted or no_host:
            await _deliver_media(to_jid, account_id, hosted, no_host)
    elif ftype == "video":
        r = _as_ref(payload, "avatar.mp4")
        if r.get("url"):
            await _deliver_media(to_jid, account_id, [r], 0)
        else:
            await _deliver_media(to_jid, account_id, [], 1)
    # 'audio' (duplicate of embedded) and 'idle' frames are intentionally ignored.


def _as_ref(item: dict, default_name: str) -> dict:
    """Normalize a media item to {name, url, expires_at}. The relay hands adapters a hosted
    `url` (bytes stripped, TTL ~10 min); Zoom's chatbot can't push binary, so we forward it."""
    return {"name": item.get("name", default_name), "url": item.get("url"),
            "expires_at": item.get("expires_at")}


async def _deliver_media(to_jid: str, account_id: str, refs: list, undeliverable: int = 0):
    """Post the bot's outbound media into Zoom as short-lived links (Zoom auto-links URLs)."""
    lines = []
    for r in refs:
        note = ""
        exp = r.get("expires_at")
        if exp:
            secs = int(exp - time.time())
            if secs > 0:
                note = f" (link expires in ~{max(1, secs // 60)} min)"
        lines.append(f"📎 {r['name']}: {r['url']}{note}")
    if undeliverable:
        lines.append(f"⚠️ {undeliverable} attachment(s) couldn't be hosted for a link "
                     f"(relay file store unavailable).")
    if lines:
        log.info("zoom_media_delivered", links=len(refs), undeliverable=undeliverable)
        await _send_zoom_reply(to_jid, account_id, "\n".join(lines))


# --- Zoom file fetch (best-effort inbound — validate against a real payload) ---
async def _fetch_zoom_files(obj: dict) -> list:
    """Download files dropped in a Zoom Team Chat message → attachment dicts. Zoom's inbound
    file schema was never in the legacy connector, so this handles the documented
    `files:[{download_url}]` shape and loudly logs a `file_ids`-only payload."""
    files = obj.get("files") or obj.get("file") or []
    if isinstance(files, dict):
        files = [files]
    if not files and obj.get("file_ids"):
        log.warning("zoom_file_ids_unhandled", file_ids=obj.get("file_ids"),
                    hint="inbound file arrived as file_ids only — needs Zoom file-download API wiring")
        return []
    token = await _get_zoom_token()
    out = []
    for f in files:
        url = f.get("download_url") or f.get("url")
        if not url or not token:
            continue
        try:
            async with httpx.AsyncClient() as c:
                r = await c.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
            if r.status_code == 200:
                out.append({
                    "name": f.get("name") or f.get("file_name", "file"),
                    "mime": f.get("file_type") or f.get("mime", ""),
                    "encoding": "base64",
                    "data": base64.b64encode(r.content).decode(),
                })
                log.info("zoom_file_fetched", name=f.get("name") or f.get("file_name"), bytes=len(r.content))
        except Exception as e:
            log.warning("zoom_file_fetch_failed", error=str(e))
    return out


# --- Zoom reply ----------------------------------------------------------------
async def _send_zoom_reply(to_jid: str, account_id: str, text: str):
    """Post a message back into the Zoom conversation via the chatbot API."""
    token = await _get_zoom_token()
    if not token:
        log.warning("zoom_reply_no_token")
        return
    payload = {
        "robot_jid": ZOOM_BOT_JID,
        "to_jid": to_jid,
        "account_id": account_id,
        "content": {"body": [{"type": "message", "text": text}]},
    }
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(CHATBOT_API,
                             headers={"Authorization": f"Bearer {token}",
                                      "Content-Type": "application/json"},
                             json=payload, timeout=15)
        if r.status_code != 200:
            log.warning("zoom_reply_failed", status=r.status_code, body=r.text[:200])
    except Exception as e:
        log.warning("zoom_reply_error", error=str(e))


# --- Zoom event WebSocket ------------------------------------------------------
async def _handle_event(raw: str, ws):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return
    event = data.get("event")
    payload = data.get("payload", {}) or {}

    if event == "endpoint.url_validation":
        plain = payload.get("plainToken", "")
        if plain and ZOOM_VERIFICATION_TOKEN:
            enc = hmac.new(ZOOM_VERIFICATION_TOKEN.encode(), plain.encode(),
                           hashlib.sha256).hexdigest()
            await ws.send(json.dumps({"module": "endpoint.url_validation",
                                      "payload": {"plainToken": plain, "encryptedToken": enc}}))
        return

    if event != "chat_message.sent":
        return

    obj = payload.get("object", {}) or {}
    sender = obj.get("sender", "")
    if sender == ZOOM_BOT_JID:
        return
    text = (obj.get("message") or "").strip()
    to_jid = obj.get("to_jid") or obj.get("channel_id") or ""
    account_id = payload.get("account_id") or obj.get("account_id") or data.get("account_id") or ""
    user_id = obj.get("sender") or "zoom"

    attachments = await _fetch_zoom_files(obj)
    if not text and not attachments:
        return

    if _relay_ws is None or not _active_bot:
        await _send_zoom_reply(to_jid, account_id, "No bot is connected to this Zoom app yet.")
        return

    sid = str(uuid.uuid4())
    _prune_sessions()
    _sessions[sid] = {"to_jid": to_jid, "account_id": account_id, "ts": time.time()}
    await _send_to_bot(sid, text or "(file attached)", user_id, attachments)


async def _zoom_client():
    """Maintain Zoom's outbound event WebSocket (reconnecting), with a 30s heartbeat."""
    while True:
        token = await _get_zoom_token()
        if not token:
            log.warning("zoom_no_token_retry")
            await asyncio.sleep(10)
            continue
        url = f"{ZOOM_WS_URL}?access_token={token}"
        if ZOOM_WS_SUBSCRIPTION_ID:
            url = f"{ZOOM_WS_URL}?subscriptionId={ZOOM_WS_SUBSCRIPTION_ID}&access_token={token}"
        try:
            async with websockets.connect(url, max_size=None) as ws:
                log.info("zoom_ws_connected")
                hb = asyncio.create_task(_heartbeat(ws))
                try:
                    async for raw in ws:
                        await _handle_event(raw, ws)
                finally:
                    hb.cancel()
        except Exception as e:
            log.warning("zoom_ws_disconnected", error=str(e))
        await asyncio.sleep(5)


async def _heartbeat(ws):
    try:
        while True:
            await asyncio.sleep(30)
            await ws.send(json.dumps({"module": "heartbeat"}))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.warning("zoom_heartbeat_error", error=str(e))


# --- multi-tenant control plane (instar connects/disconnects us per profile) --
def _set_active_bot(bot: str | None):
    """Point (or unpoint) the adapter at a bot slug (from the profile). Only force a reconnect
    when the bot actually CHANGES — the gatekeeper keepalive re-calls connect every cycle with
    the same bot, and churning the socket each time would drop it for ~2s."""
    global _active_bot
    if bot == _active_bot:
        return  # no change — leave the live socket alone
    _active_bot = bot
    if _relay_loop and _relay_ws is not None:
        asyncio.run_coroutine_threadsafe(_relay_ws.close(), _relay_loop)


class _Control(BaseHTTPRequestHandler):
    def _reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/health", "/status"):
            return self._reply(200, {"status": "ok", "service": SERVICE_SLUG,
                                     "active_bot": _active_bot})
        self._reply(404, {"error": "not found"})

    def do_POST(self):
        # The profile hands us ONLY its bot slug (in the path); its own token is the bot's, not
        # ours, so we ignore any body.
        parts = [p for p in self.path.split("/") if p]  # ['slugs', '{bot}', 'connect']
        if len(parts) == 3 and parts[0] == "slugs":
            bot, action = parts[1], parts[2]
            try:
                n = int(self.headers.get("Content-Length") or 0)
                if n:
                    self.rfile.read(n)
            except Exception:
                pass
            if action == "connect":
                _set_active_bot(bot)
                log.info("profile_connected", bot=bot, service=SERVICE_SLUG)
                return self._reply(200, {"status": "connected", "bot": bot, "service": SERVICE_SLUG})
            if action == "disconnect":
                if _active_bot == bot:
                    _set_active_bot(None)
                log.info("profile_disconnected", bot=bot)
                return self._reply(200, {"status": "disconnected", "bot": bot})
        self._reply(404, {"error": "not found"})

    def log_message(self, *a):
        pass


def _start_control_server():
    HTTPServer(("0.0.0.0", CONTROL_PORT), _Control).serve_forever()


async def main():
    global _relay_loop
    missing = [k for k, v in {
        "ZOOM_CLIENT_ID": ZOOM_CLIENT_ID, "ZOOM_CLIENT_SECRET": ZOOM_CLIENT_SECRET,
        "ZOOM_BOT_JID": ZOOM_BOT_JID, "REGISTERABOT_RELAY_URL": RELAY_URL,
        "ZOOM_REGISTERABOT_TOKEN": SERVICE_KEY,
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing required env: {missing}")

    _relay_loop = asyncio.get_running_loop()
    threading.Thread(target=_start_control_server, daemon=True).start()
    log.info("zoom_registerabot_adapter_starting", service=SERVICE_SLUG, control_port=CONTROL_PORT)
    await asyncio.gather(_relay_client(), _zoom_client())


if __name__ == "__main__":
    asyncio.run(main())

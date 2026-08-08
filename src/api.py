"""Zoom → RegisterABot adapter (multi-tenant).

Zoom Team Chat *chatbots* deliver user messages to a **Bot Endpoint webhook** (Zoom POSTs to a
public URL) — NOT over a WebSocket, unlike Slack's Socket Mode. So the inbound leg is a webhook
(exposed via a Cloudflare Tunnel → this adapter's :WEBHOOK_PORT), and registerabot is the leg
AFTER that, toward the bot. Flow:

    Zoom  ──POST /webhook (via tunnel)──►  this adapter  ──relay chat_request──►  bot
    bot   ──final_response (relay)──►  this adapter  ──Zoom chatbot API──►  Zoom reply

Identity split (same as the Slack adapter):
  - This adapter's OWN service key (ZOOM_REGISTERABOT_TOKEN) — its identity as a SERVICE on the
    relay (service→bot). NOT the bot's key.
  - WHICH bot we route to comes from the PROFILE via /slugs/{bot}/connect (multi-tenant contract).

Creds (from Infisical): Zoom S2S OAuth (ZOOM_CLIENT_ID/SECRET) for reply tokens, ZOOM_BOT_JID,
ZOOM_VERIFICATION_TOKEN (webhook url_validation), + the relay identity.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import structlog

structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"),
                                structlog.dev.ConsoleRenderer()])
log = structlog.get_logger()

# --- creds / config (self-fetched from Infisical by entrypoint.sh) ------------
ZOOM_CLIENT_ID = os.environ.get("ZOOM_CLIENT_ID", "")
ZOOM_CLIENT_SECRET = os.environ.get("ZOOM_CLIENT_SECRET", "")
ZOOM_BOT_JID = os.environ.get("ZOOM_BOT_JID", "")
ZOOM_VERIFICATION_TOKEN = os.environ.get("ZOOM_VERIFICATION_TOKEN", "")

RELAY_URL = os.environ.get("REGISTERABOT_RELAY_URL", "").rstrip("/")   # wss://relay… (shared)
# This adapter's OWN relay identity (a SERVICE). Its own slug + own key — NOT the profile's.
SERVICE_SLUG = (os.environ.get("ZOOM_REGISTERABOT_SERVICE_SLUG")
                or os.environ.get("REGISTERABOT_SERVICE_SLUG", "zoom-adapter"))
SERVICE_KEY = os.environ.get("ZOOM_REGISTERABOT_TOKEN", "")            # our service key
CONTROL_PORT = int(os.environ.get("CONTROL_PORT", "8092"))    # instar multi-tenant connect/disconnect (internal)
WEBHOOK_PORT = int(os.environ.get("WEBHOOK_PORT", "8087"))    # Zoom Bot Endpoint (tunnel → here)
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
    import websockets
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
    """Normalize a media item to {name, url, expires_at} — the relay's hosted ref."""
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
        # Zoom's chatbot API returns 201 Created on success (not 200).
        if r.status_code not in (200, 201):
            log.warning("zoom_reply_failed", status=r.status_code, body=r.text[:200])
        else:
            log.info("zoom_reply_sent", status=r.status_code)
    except Exception as e:
        log.warning("zoom_reply_error", error=str(e))


# --- inbound: Zoom Bot Endpoint webhook (tunnel → here) -----------------------
def _handle_bot_notification(payload: dict):
    """A user messaged the chatbot. Route it to the connected bot via the relay."""
    text = (payload.get("cmd") or payload.get("message") or "").strip()
    to_jid = payload.get("toJid") or payload.get("to_jid") or ""
    account_id = payload.get("accountId") or payload.get("account_id") or ""
    user_id = payload.get("userJid") or payload.get("userId") or "zoom"
    if not text:
        return
    if _relay_loop is None or not _active_bot or _relay_ws is None:
        if _relay_loop:
            asyncio.run_coroutine_threadsafe(
                _send_zoom_reply(to_jid, account_id, "No bot is connected to this Zoom app yet."),
                _relay_loop)
        return
    # STABLE key, not a per-message uuid. session_id becomes the gatekeeper's conversation_id,
    # so a fresh uuid each turn threw away context and burned a bot session per message.
    # One rolling conversation per Zoom peer (the user↔bot chat).
    sid = f"zoom-{account_id}-{to_jid or user_id}"
    _prune_sessions()
    _sessions[sid] = {"to_jid": to_jid, "account_id": account_id, "ts": time.time()}
    log.info("zoom_message", user=payload.get("userName"), bot=_active_bot)
    asyncio.run_coroutine_threadsafe(_send_to_bot(sid, text, user_id, []), _relay_loop)


class _Webhook(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/health", "/status"):
            return self._json(200, {"status": "ok", "service": SERVICE_SLUG,
                                    "active_bot": _active_bot})
        self._json(404, {"error": "not found"})

    def do_POST(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        except Exception:
            body = {}
        event = body.get("event")
        payload = body.get("payload", {}) or {}

        # Zoom endpoint validation challenge.
        if event == "endpoint.url_validation":
            plain = payload.get("plainToken", "")
            enc = hmac.new(ZOOM_VERIFICATION_TOKEN.encode(), plain.encode(),
                           hashlib.sha256).hexdigest() if ZOOM_VERIFICATION_TOKEN else ""
            return self._json(200, {"plainToken": plain, "encryptedToken": enc})

        # A user messaged the chatbot.
        if event == "bot_notification":
            try:
                _handle_bot_notification(payload)
            except Exception as e:
                log.warning("bot_notification_error", error=str(e))
            return self._json(200, {"status": "ok"})

        # Other events (chat_message.sent, etc.) — acknowledge, nothing to do.
        return self._json(200, {"status": "ignored"})

    def log_message(self, *a):
        pass


def _start_webhook_server():
    HTTPServer(("0.0.0.0", WEBHOOK_PORT), _Webhook).serve_forever()


# --- multi-tenant control plane (instar connects/disconnects us per profile) --
def _set_active_bot(bot: str | None):
    """Point (or unpoint) the adapter at a bot slug (from the profile). Only force a reconnect
    when the bot actually CHANGES — the gatekeeper keepalive re-calls connect every cycle with
    the same bot, and churning the socket each time would drop it for ~2s."""
    global _active_bot
    if bot == _active_bot:
        return
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
    threading.Thread(target=_start_webhook_server, daemon=True).start()
    log.info("zoom_registerabot_adapter_starting", service=SERVICE_SLUG,
             control_port=CONTROL_PORT, webhook_port=WEBHOOK_PORT)
    await _relay_client()


if __name__ == "__main__":
    asyncio.run(main())

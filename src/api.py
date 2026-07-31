"""Zoom → RegisterABot adapter.

A THIN bridge, not a full connector — the Zoom twin of instar-interface-slack-registerabot.
It receives Zoom Team Chat messages (and files) over Zoom's outbound WebSocket and forwards
them to the bot over the RegisterABot relay as a *service client*, using this adapter's OWN
relay identity (ZOOM_REGISTERABOT_TOKEN). It never talks to the instar gatekeeper/bot
directly; the relay routes to the bot, and the bot-side registerabot connector extracts the
attachments and runs the on_file barrier.

Two credential sets (both injected from Infisical by the stack):
  - Zoom: ZOOM_CLIENT_ID + ZOOM_CLIENT_SECRET (Server-to-Server OAuth) — to open the event
    WebSocket, send chat replies, and download dropped files (Zoom files sit behind an
    authenticated download URL; only the Zoom token can fetch them). Plus ZOOM_BOT_JID and
    ZOOM_WS_SUBSCRIPTION_ID for the chatbot + WS wiring.
  - Relay: ZOOM_REGISTERABOT_TOKEN — this adapter's identity ON the relay.

Protocol (registerabot SDK): connect wss://relay/ws/service/{serviceSlug}?key=…&bot=…,
send {type:"chat_request", session_id, from:{kind:service}, to:{kind:bot}, payload:
JSON({messages:[{role,content,attachments}], user_context})}, receive {type:"final_response",
session_id, payload:JSON({reply})}. Correlate request↔reply by session_id.

Zoom's inbound *file* event schema was never implemented in the legacy connector, so
_fetch_zoom_files is best-effort (documented) and must be validated against a real payload.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import os
import time
import uuid

import httpx
import structlog
import websockets

structlog.configure(processors=[structlog.processors.TimeStamper(fmt="iso"),
                                structlog.dev.ConsoleRenderer()])
log = structlog.get_logger()

# --- creds / config (injected from Infisical by the stack) --------------------
ZOOM_CLIENT_ID = os.environ.get("ZOOM_CLIENT_ID", "")
ZOOM_CLIENT_SECRET = os.environ.get("ZOOM_CLIENT_SECRET", "")
ZOOM_BOT_JID = os.environ.get("ZOOM_BOT_JID", "")
ZOOM_WS_SUBSCRIPTION_ID = os.environ.get("ZOOM_WS_SUBSCRIPTION_ID", "")
ZOOM_VERIFICATION_TOKEN = os.environ.get("ZOOM_VERIFICATION_TOKEN", "")
ZOOM_WS_URL = os.environ.get("ZOOM_WS_URL", "wss://ws.zoom.us/ws")

RELAY_URL = os.environ.get("REGISTERABOT_RELAY_URL", "").rstrip("/")   # wss://relay…
SERVICE_SLUG = os.environ.get("REGISTERABOT_SERVICE_SLUG", "zoom-adapter")
SERVICE_KEY = os.environ.get("ZOOM_REGISTERABOT_TOKEN", "")            # relay identity
BOT_SLUG = os.environ.get("REGISTERABOT_BOT_SLUG", "")
REPLY_TIMEOUT = int(os.environ.get("REPLY_TIMEOUT", "300"))

TOKEN_URL = "https://zoom.us/oauth/token"
CHATBOT_API = "https://api.zoom.us/v2/im/chat/messages"

# --- relay websocket state (one persistent service connection) ----------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
_pending: dict[str, asyncio.Future] = {}   # session_id -> future awaiting final_response

# --- Zoom S2S token cache -----------------------------------------------------
_zoom_token: str | None = None
_zoom_token_exp: float = 0.0


async def _get_zoom_token() -> str | None:
    """Server-to-Server client-credentials token, cached with a 60s expiry buffer.
    Used for the event WS, sending replies, and downloading files."""
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


# --- relay client (identical shape to the Slack adapter) ----------------------
async def _relay_client():
    """Maintain one persistent service-client WS to the relay; resolve replies by session_id."""
    global _relay_ws
    url = f"{RELAY_URL}/ws/service/{SERVICE_SLUG}?key={SERVICE_KEY}&bot={BOT_SLUG}"
    while True:
        try:
            async with websockets.connect(url, max_size=None) as ws:
                _relay_ws = ws
                log.info("relay_connected", service=SERVICE_SLUG, bot=BOT_SLUG)
                async for raw in ws:
                    try:
                        env = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if env.get("type") == "final_response":
                        sid = env.get("session_id")
                        fut = _pending.pop(sid, None)
                        if fut and not fut.done():
                            try:
                                fut.set_result(json.loads(env.get("payload") or "{}"))
                            except Exception:
                                fut.set_result({})
        except Exception as e:
            log.warning("relay_disconnected", error=str(e))
        finally:
            _relay_ws = None
        await asyncio.sleep(5)  # reconnect


async def _send_to_bot(text: str, user_id: str, attachments: list) -> dict | None:
    """Send one chat_request over the relay and await the bot's final_response."""
    if _relay_ws is None:
        return None
    sid = str(uuid.uuid4())
    envelope = {
        "v": 1, "type": "chat_request", "session_id": sid,
        "timestamp": int(time.time() * 1000),
        "from": {"kind": "service", "slug": SERVICE_SLUG, "name": "Zoom"},
        "to": {"kind": "bot", "slug": BOT_SLUG},
        "encrypted": False,
        # Attachments go ON THE MESSAGE — the bot-side registerabot connector reads
        # m.get('attachments') per message and forwards them to /process, which feeds
        # the on_file barrier. (Top-level payload attachments would be missed.)
        "payload": json.dumps({
            "messages": [{"role": "user", "content": text, "attachments": attachments}],
            "user_context": {"user_id": user_id},
        }),
    }
    fut = _relay_loop.create_future()
    _pending[sid] = fut
    try:
        await _relay_ws.send(json.dumps(envelope))
        return await asyncio.wait_for(fut, timeout=REPLY_TIMEOUT)
    except Exception as e:
        _pending.pop(sid, None)
        log.warning("send_to_bot_failed", error=str(e))
        return None


# --- Zoom file fetch (best-effort — validate against a real payload) ----------
async def _fetch_zoom_files(obj: dict) -> list:
    """Download files dropped in a Zoom Team Chat message → attachment dicts
    {name, mime, encoding:base64, data}.

    NOTE: the legacy Zoom connector never handled inbound files, so Zoom's exact
    inbound file schema is UNCONFIRMED. This handles the documented shape where the
    message object carries `files: [{name/file_name, download_url, ...}]` (same shape
    as recording files, which download via the Zoom bearer token). If a deployment
    surfaces `file_ids` only (no download_url), wire the Zoom file-download API here
    after inspecting one real payload — logged loudly so it isn't silently dropped."""
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
    """Post the bot's reply back into the Zoom conversation via the chatbot API."""
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
        # Zoom challenge — echo the hashed plainToken to prove ownership.
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
    if sender == ZOOM_BOT_JID:   # ignore our own echoes
        return
    text = (obj.get("message") or "").strip()
    to_jid = obj.get("to_jid") or obj.get("channel_id") or ""
    account_id = payload.get("account_id") or obj.get("account_id") or data.get("account_id") or ""
    user_id = obj.get("sender") or "zoom"

    attachments = await _fetch_zoom_files(obj)
    if not text and not attachments:
        return

    result = await _send_to_bot(text or "(file attached)", user_id, attachments)
    reply = (result or {}).get("reply") if result else None
    await _send_zoom_reply(to_jid, account_id,
                           reply or "I couldn't reach the bot right now — try again in a moment.")


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
        await asyncio.sleep(5)  # reconnect


async def _heartbeat(ws):
    try:
        while True:
            await asyncio.sleep(30)
            await ws.send(json.dumps({"module": "heartbeat"}))
    except asyncio.CancelledError:
        pass
    except Exception as e:
        log.warning("zoom_heartbeat_error", error=str(e))


async def main():
    global _relay_loop
    missing = [k for k, v in {
        "ZOOM_CLIENT_ID": ZOOM_CLIENT_ID, "ZOOM_CLIENT_SECRET": ZOOM_CLIENT_SECRET,
        "ZOOM_BOT_JID": ZOOM_BOT_JID, "REGISTERABOT_RELAY_URL": RELAY_URL,
        "ZOOM_REGISTERABOT_TOKEN": SERVICE_KEY, "REGISTERABOT_BOT_SLUG": BOT_SLUG,
    }.items() if not v]
    if missing:
        raise SystemExit(f"Missing required env: {missing}")

    _relay_loop = asyncio.get_running_loop()
    log.info("zoom_registerabot_adapter_starting", service=SERVICE_SLUG, bot=BOT_SLUG)
    await asyncio.gather(_relay_client(), _zoom_client())


if __name__ == "__main__":
    asyncio.run(main())

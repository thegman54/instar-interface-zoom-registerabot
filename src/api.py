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

RELAY_URL = os.environ.get("REGISTERABOT_RELAY_URL", "").rstrip("/")   # wss://relay… (shared)
# Service + bot slugs differ per adapter, so they're ZOOM_-prefixed (like the token) to avoid
# colliding with the Slack adapter's values in a shared Infisical project. Generic fallbacks kept.
SERVICE_SLUG = (os.environ.get("ZOOM_REGISTERABOT_SERVICE_SLUG")
                or os.environ.get("REGISTERABOT_SERVICE_SLUG", "zoom-adapter"))  # who we speak AS
SERVICE_KEY = os.environ.get("ZOOM_REGISTERABOT_TOKEN", "")            # that service's key
BOT_SLUG = (os.environ.get("ZOOM_REGISTERABOT_BOT_SLUG")
            or os.environ.get("REGISTERABOT_BOT_SLUG", ""))            # which bot we route TO
REPLY_TIMEOUT = int(os.environ.get("REPLY_TIMEOUT", "300"))

TOKEN_URL = "https://zoom.us/oauth/token"
CHATBOT_API = "https://api.zoom.us/v2/im/chat/messages"

# --- relay websocket state (one persistent service connection) ----------------
_relay_ws = None
_relay_loop: asyncio.AbstractEventLoop | None = None
# session_id -> {"to_jid", "account_id", "ts"} — where to post this turn's frames.
# The relay routes EVERY bot→service frame (final_response, video, …) by session_id, so we
# keep the reply destination around long enough to catch trailing frames.
_sessions: dict[str, dict] = {}
SESSION_TTL = int(os.environ.get("SESSION_TTL", "600"))

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
                    if env.get("session_id"):
                        try:
                            await _dispatch_frame(env)
                        except Exception as e:
                            log.warning("dispatch_error", error=str(e))
        except Exception as e:
            log.warning("relay_disconnected", error=str(e))
        finally:
            _relay_ws = None
        await asyncio.sleep(5)  # reconnect


def _prune_sessions():
    cutoff = time.time() - SESSION_TTL
    for sid in [s for s, v in _sessions.items() if v.get("ts", 0) < cutoff]:
        _sessions.pop(sid, None)


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
        # Outbound files + embedded voice audio → hosted links (the relay stored them).
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
    `url` (bytes stripped, TTL ~10 min); Zoom's chatbot can't push binary, so we forward the
    link. If there's no url (relay didn't store it — e.g. KV unconfigured or oversized), the
    item is undeliverable to Zoom and we surface that instead of pretending."""
    return {"name": item.get("name", default_name), "url": item.get("url"),
            "expires_at": item.get("expires_at")}


async def _deliver_media(to_jid: str, account_id: str, refs: list, undeliverable: int = 0):
    """Post the bot's outbound media into Zoom as short-lived links.

    Zoom's chatbot API (imchat:bot) sends message CARDS only — it cannot push binary files —
    so audio/video/files are delivered as the relay's hosted links (see docs/INTERFACE_BUS.md
    §4.1). Zoom auto-links URLs in message text, so a plain line per file is enough."""
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


async def _send_to_bot(sid: str, text: str, user_id: str, attachments: list):
    """Send one chat_request over the relay. Replies arrive asynchronously as frames."""
    if _relay_ws is None:
        return
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
    try:
        await _relay_ws.send(json.dumps(envelope))
    except Exception as e:
        log.warning("send_to_bot_failed", error=str(e))


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

    if _relay_ws is None:
        await _send_zoom_reply(to_jid, account_id,
                               "I couldn't reach the bot right now — try again in a moment.")
        return

    # Register where this turn's frames should land, then fire the request (non-blocking).
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

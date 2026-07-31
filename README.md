# instar-interface-zoom-registerabot

Thin **Zoom → RegisterABot** adapter — the Zoom twin of
[`instar-interface-slack-registerabot`](../instar-interface-slack-registerabot). Part of the
one-bus/thin-adapter architecture (`project-instar/docs/INTERFACE_BUS.md`): collapse the
per-interface full connectors into thin adapters that pipe onto the single RegisterABot
websocket bus.

## What it does

```
Zoom Team Chat ──(this adapter)──►  RegisterABot RELAY  ──►  bot-side registerabot connector
   msg + file          │  own relay identity                       │  → gatekeeper /process
                       │  (ZOOM_REGISTERABOT_TOKEN)                 ▼  → on_file barrier
                       └── downloads Zoom files with the Zoom token   the bot + importer
```

- Opens Zoom's **outbound** event WebSocket (S2S OAuth client-credentials token; heartbeat +
  reconnect; answers `endpoint.url_validation`). No inbound ports.
- On `chat_message.sent`: downloads any dropped files with the Zoom token, then sends one
  `chat_request` envelope over the relay — **attachments ride on the message**
  (`messages[].attachments`), which is where the bot-side connector reads them.
- Awaits the `final_response` (correlated by `session_id`) and posts the reply back into the
  Zoom conversation via the chatbot API.
- **Never** contacts the instar gatekeeper/bot directly — the relay routes to the bot. The
  bot side is written once and shared by every adapter.

## Credentials (from Infisical)

`ZOOM_CLIENT_ID`, `ZOOM_CLIENT_SECRET`, `ZOOM_BOT_JID`, `REGISTERABOT_RELAY_URL`,
`ZOOM_REGISTERABOT_TOKEN`, `REGISTERABOT_BOT_SLUG` are required; `ZOOM_WS_SUBSCRIPTION_ID`,
`ZOOM_VERIFICATION_TOKEN`, `REGISTERABOT_SERVICE_SLUG` are optional. See `manifest.yaml`.

To run it you also need, on the relay side: a RegisterABot **service** (slug + API key =
`ZOOM_REGISTERABOT_TOKEN`) that is **authorized for the target bot**.

## Known gap

Zoom's **inbound file** event schema was never implemented in the legacy connector, so
`_fetch_zoom_files` is **best-effort** — it handles the documented `files: [{download_url}]`
shape and loudly logs (rather than silently drops) a `file_ids`-only payload. Validate it
against one real Zoom file drop and wire the file-download API if needed.

# llmgw build plan G: the WebSocket data plane

Written 2026-09-18 against PLAN-2.md "Phase G" (PLAN-2.md:426-468), the four
voice sweeps (`capabilities/voice-*.md`, 16 Sep), the cross-provider summary
(`capabilities/voice.md`), the Phase D/E/F code as shipped, and the consumer's
own connect code (LiveKit plugins pinned at 1.6.3 by
`/Users/sanjay/Layrs/voice-agent/requirements.txt:8-12`). Phase G is the only
route by which the production voice path fronts the gateway
(`capabilities/voice.md:125-135`): production TTS is Inworld over
`wss://api.inworld.ai/tts/v1/voice:streamBidirectional` and production STT is
Inworld over `wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional`,
both through the LiveKit plugin; fallbacks are AssemblyAI universal-streaming
and OpenAI Realtime transcription.

Verification legend used throughout: **[live]** verified on the wire on 16
Sep; **[source]** read from the plugin or provider example source; **[doc]**
provider documentation only; **[captures-ws]** unverified today, to be
settled by the probe run being captured into `capabilities/captures-ws.md`
(the fixture source for every fake in this plan).

Every code claim cites `file:line` in `/Users/sanjay/PREP/Evo/llmgw/src/llmgw`
unless another root is given.

---

## G0 results (2026-09-18): what the live captures changed in this plan

`capabilities/captures-ws.md` (21 sockets: Inworld TTS and STT, OpenAI
Realtime transcription and full) is now the fixture source and overrides
any **[captures-ws]**, **[source]** or **[doc]** claim below where they
disagree. The deltas that change a design decision:

1. **Inworld checks the credential lazily, on the first client message.**
   Every upgrade returns 101 with identical headers whatever the credential;
   a socket that sends nothing receives nothing (>20 s) whatever the
   credential. A bad key answers the first message ~320 ms later with a
   top-level `error` code 7 and a server CLOSE 1000 in the same millisecond;
   a missing credential is code 16. So "101 then silence" is a healthy idle
   socket, not a failure, and the connect budget for Inworld ends at the
   reply to the first relayed client frame (`contextCreated` or `error`),
   exactly as 3.1 says; `silent-after-101` is NOT a bad-key mode and the fake
   must model `error-7-then-close-1000-on-first-message` instead.
2. **Only `Authorization: Basic <key>` works on Inworld**; `?key=` is treated
   as no credential (code 16). The tenant-side Basic scheme in 5 stands.
3. **Inworld TTS usage is exact on the wire**: `audioChunk.usage.
   processedCharactersCount` on the FIRST chunk of every flush (0 after), with
   `modelId`. Sum per flush; do not take the last. 7.2's "characters estimated
   from `send_text`" becomes the fallback only when no chunk carried usage.
4. **Inworld STT usage is exact on the wire too**: one `result.usage
   {transcribedAudioMs, modelId}` after `closeStream` (1500 ms for 1.63 s of
   audio; silence is not counted, so bytes cannot derive it). 0.1's
   correction about the plugin's client-side `RECOGNITION_USAGE` still holds
   for the plugin; the gateway takes the server frame, and a session that
   ends without `closeStream` has no meter (estimated, `cost_notes` says so).
   `transcribeConfig` is not acknowledged: the first server frame is
   `speechStarted`, only once audio flows.
5. **Inworld never closes a healthy socket and never pings** (75 s idle, no
   frame, context still usable). Its only server-initiated close is 1000
   right after a fatal `error`. Liveness on Inworld is client-driven; the
   `idle` budget is the gateway's alone.
6. **A top-level Inworld `error` is fatal only when followed by CLOSE**: a
   malformed frame gets code 3 (`INVALID_ARGUMENT`) and the socket survives;
   `{"foo":"bar"}` gets no reply. In-context faults arrive as
   `result.status.code != 0` with the `contextId` (5 unknown context, 8 sixth
   context, 3 over 2,000 chars per `send_text`, after which no
   `flushCompleted` follows). Per-`send_text` limit 2,000 chars; a 1 MiB
   text frame is accepted at the transport.
7. **OpenAI checks auth and model AFTER the 101** (error + CLOSE 3000 for
   auth, 4000 for shape, ~0 ms later), so the pre-101 HTTP 401 path in 7.1
   does not exist for Realtime; accept-after-upstream-ready still holds
   (`session.created` or the error+close arrives before we accept the
   client). `OpenAI-Beta: realtime=v1` is fatal (4000
   `beta_api_shape_disabled`) on both `?model=` and `?intent=transcription`:
   strip it. Legacy `transcription_session.update` / `input_audio_format`
   are rejected; the GA shape is `session.update{session:{type:
   "transcription", audio:{input:{format:{type:"audio/pcm", rate:24000},
   transcription:{model}, turn_detection}}}}`, rate >= 24000; a rejected
   `session.update` is discarded whole.
8. **OpenAI pings every ~20.3 s** (4-byte payload) and nothing else while
   idle; `websockets` answers pings automatically on the upstream side and
   uvicorn on the client side, so the relay forwards none. `error` events are
   non-fatal (four in a row, next `response.create` completed);
   `rate_limits.updated` arrives ~43 ms AFTER `response.done`, so
   `response.done` is a per-response terminal but not the last frame of the
   cycle. Transcription sessions carry usage on
   `conversation.item.input_audio_transcription.completed.usage`
   (`total_tokens`, `input_tokens`, `input_token_details{text_tokens,
   audio_tokens}`, `output_tokens`) and send no `rate_limits.updated`. The
   exact `response.done.response.usage` object is in the captures (cached
   tokens are a subset of input, with `cached_tokens_details`).
9. **Inworld LINEAR16 chunks**: the first and the tiny final chunk of each
   flush begin with a 44-byte RIFF/WAVE header. The relay passes bytes
   through untouched; this is a consumer fact (the plugin strips it), noted so
   nobody "fixes" it in the gateway.
10. **Sizes and latencies**: largest server frames 25,012 B (Inworld) and
    1,254 B (OpenAI); connect to 101 0.46 to 1.4 s (Inworld) and 0.84 to
    2.1 s (OpenAI) from Chennai; Inworld first audio 370 to 455 ms after
    `send_text`; OpenAI first text delta 476 ms after `response.create`.
    The `tts_session`/`realtime_session` profile numbers in 4.1 must sit
    above these with the same margin the HTTP profiles use.

Sweep rows contradicted by the captures (fix in `capabilities/voice-*.md`
during G1): voice-inworld.md "bad key = 101 then silence" and "no credential =
error as first frame" (both artefacts of whether a message was sent);
"`RECOGNITION_USAGE` every 5 s" (one `result.usage` after `closeStream`);
voice-openai.md "no `rate_limits.updated` in a one-response session"
(arrives 43 ms after `response.done`); "WS auth failure is HTTP 401" (it is
101 then error + CLOSE 3000).

---

## 0. What this plan reuses, and the one sentence that governs it

PLAN-2.md:426-431: "nothing in the HTTP pump applies, while deadlines,
admission, breakers, credential scoping, capture and drain do." That is the
design rule. The socket plane is a second data path that plugs into the same
process-wide objects `Gateway` already owns (`server/app.py:1197-1356`) and
writes the same records. It invents a relay, a session object, a close-code
taxonomy and per-product frame classifiers, and nothing else.

### 0.1 The consumer, exactly

`/Users/sanjay/Layrs/backend/tcg` contains no Inworld, AssemblyAI, LiveKit or
`wss://` code (grep hits are SQL migrations and a course JSON); it is the
TypeScript curriculum generator. The voice consumer is
`/Users/sanjay/Layrs/voice-agent` and the plugin sources it pins.

| Plugin | Connect code | URL built | Auth on the upgrade | First frames | Keepalive / close | Override hook |
|---|---|---|---|---|---|---|
| `livekit-plugins-inworld` TTS 1.6.3 (`tts.py`) | `_InworldConnection.connect` tts.py:253-276 | `urljoin(self._ws_url, "/tts/v1/voice:streamBidirectional")` tts.py:259 | headers `Authorization: <"Basic "+key>` (tts.py:263-267; the string is built by the `TTS` ctor from `INWORLD_API_KEY`, tts.py:913), `X-User-Agent`, `X-Request-Id` | `{"create":{voiceId, modelId, audioConfig{audioEncoding, sampleRateHertz, bitrate, speakingRate}, temperature, bufferCharThreshold, maxBufferDelayMs, timestampTransportStrategy, language?, timestampType?, applyTextNormalization?, deliveryMode?, autoMode:true}, "contextId"}` tts.py:390-417; then `send_text`, `flush_context`, `close_context` tts.py:419-432 | no app ping; `receive(timeout=60)` loop tts.py:445; connection-level `error` logged and ignored tts.py:462-470; `result.status.code != 0` fails that context tts.py:478-496; stale CLOSING contexts swept at 120 s tts.py:590-601; connection error fails all contexts and the pool opens replacements tts.py:603-616 | `TTS(ws_url=...)` tts.py:843, 889-890; `max_connections=20` pool x `MAX_CONTEXTS=5` tts.py:174, 643 |
| `livekit-plugins-inworld` STT 1.6.3 (`stt.py`) | `_connect_ws` stt.py:270-285 | `base_url.replace("https://","wss://").rstrip("/") + "/stt/v1/transcribe:streamBidirectional"` stt.py:271-272, 52 | `Authorization: Basic <key>` stt.py:122, 277 | `{"transcribeConfig": {...}}` immediately after connect stt.py:283; then `{"audioChunk":{"content":<b64 pcm>}}` per frame stt.py:355, `{"endTurn":{}}` on flush stt.py:346, `{"closeStream":{}}` at end stt.py:364 | none; `_run` reconnects on `_reconnect_event` and closes the socket in `finally` stt.py:287-336; reads `result.speechStarted`, `result.transcription{transcript,isFinal,voiceProfile}` stt.py:406-465 | `STT(base_url=...)` stt.py:87 (prefix preserved) |
| `livekit-plugins-openai` STT (uv cache 1.3.12; 1.6.3 to confirm) | `_connect_ws` stt.py:369-393 | `f"{base_url.rstrip('/')}/realtime?intent=transcription"`, `http`->`ws` stt.py:379-388 | `Authorization: Bearer <key>`, `User-Agent` stt.py:382-385 | `session.update {session:{type:"transcription", audio:{input:{format, transcription, turn_detection, noise_reduction?}}}}` stt.py:369-377, 392; then `input_audio_buffer.append` stt.py:502 | `ws.receive()` loop stt.py:517; `ws.close()` stt.py:395-396 | `STT(base_url=...)` stt.py:84, 136 |
| `livekit-plugins-openai` Realtime | `_create_ws_conn` realtime_model.py:786-818 | `process_base_url`: `http`->`ws`, `/realtime` appended when path is `""`, `/v1` or `/openai`, `model=` added to the query realtime_model.py:615-647 | `Authorization: Bearer <key>`, `User-Agent: LiveKit Agents` realtime_model.py:787-795 | `session.update` first realtime_model.py:673, 1005-1011 | send task closes with `ws_conn.close()` realtime_model.py:845-846; `response.done` and `error` handled realtime_model.py:937-939 | `RealtimeModel(base_url=...)` realtime_model.py:185, 322-345 |
| `livekit-plugins-assemblyai` (GitHub main = 1.8.2; the 1.6.3 sdist must be diffed before G4) | `_connect_ws` stt.py:~880-905 | `f"{self._base_url}/v3/ws?{urlencode(config)}"` with `sample_rate`, `encoding`, `speech_model`, EoT knobs stt.py:~838-897 | `Authorization: <raw key>`, `Content-Type`, `User-Agent: AssemblyAI/1.0 (integration=Livekit)` stt.py:~887-891 | binary `send_bytes(frame)` stt.py:~706; `{"type":"ForceEndpoint"}` stt.py:~670; `{"type":"Terminate"}` on close stt.py:~544, 711 | `wait_for(ws.receive(), timeout=5)` loop, warning every 15 s of silence stt.py:~722-735; handles `Begin`, `Termination{audio_duration_seconds, session_duration_seconds}`, `Turn` stt.py:~913-952 | `STT(base_url="wss://streaming.assemblyai.com")` stt.py:~175-184 |

Layrs constructs all of these WITHOUT a base URL today
(`harness/livekit_setup.py:134-140` Inworld TTS, `:168-181` AssemblyAI,
`:205-210` Inworld STT; `framework/skeleton/session.py:92-93, 116-122, 136`),
and pins `inworld-tts-1.5-mini` / `inworld/inworld-stt-1`
(`harness/config.py:84, 90`). Two Layrs-side facts shape this plan:

1. **The tenant token arrives as `Authorization: Basic <token>`** from both
   Inworld plugins, as a raw `Authorization: <token>` from AssemblyAI's, and
   as `Bearer` only from OpenAI's. `bearer_token` (`server/app.py:411-428`)
   returns None for any scheme but Bearer, so the socket routes need a
   per-route scheme set (section 3.4).
2. **The Inworld TTS plugin drops any path prefix** (`urljoin` with an
   absolute path, tts.py:259), so `/workloads/{w}/...` twins cannot be
   reached from it; the STT plugin keeps the prefix (stt.py:272). The TTS
   target must therefore be resolvable from the socket's own first frame
   (`create.modelId`) and the tenant's policy, not from the URL.

### 0.2 Provider facts this plan depends on, by verification status

| Fact | Status | Where |
|---|---|---|
| Inworld WS upgrade returns 101 with no credential; auth error arrives in-band as the first text frame; with a BAD key the socket is silent >15 s | [live] | voice-inworld.md:33, 69, 236-237 |
| Inworld TTS WS frame vocabulary (`create`/`send_text`/`flush_context`/`close_context`; `result.contextCreated`/`audioChunk{audioContent,timestampInfo}`/`flushCompleted`/`contextClosed`/`status{code,message}`; top-level `error{code,message}`) | [source] | voice-inworld.md:243; plugin tts.py:390-432, 459-582 |
| Inworld TTS <=5 contexts per socket, no server ping, 60 s plugin receive timeout | [source] | voice-inworld.md:52, 68-69; tts.py:174, 445 |
| Inworld STT frame vocabulary (`transcribeConfig`, `audioChunk`, `endTurn`, `closeStream`; `result.speechStarted`, `result.transcription{isFinal}`, `result.status`) | [source] | voice-inworld.md:244; stt.py:283, 346-364, 406-465 |
| Inworld STT usage: **the sweep's `RECOGNITION_USAGE` every 5 s (voice-inworld.md:55, 127; voice.md:117) is the plugin's OWN client-side event** computed from audio it pushed (`_audio_duration_collector`, stt.py:351, 368-376), not a wire frame | [source], corrects the sweep | stt.py:368-376 |
| Inworld TTS WS success path, `contextCreated` timing, `audioChunk` sizes, usage on WS frames, server close codes, idle timeout, Basic vs Bearer on the upgrade, upstream 101 headers | [captures-ws] | voice-inworld.md:41 "still unverified live" |
| Inworld error object: gRPC-status JSON, `details[].reconnectType`, codes 16/7/5/3 | [live] | voice-inworld.md:110-114 |
| OpenAI Realtime: `wss://api.openai.com/v1/realtime?model=`, bearer only, `permessage-deflate` negotiated, `session.created` is the first frame at ~1.7 s, client close -> 1000 | [live] | voice-openai.md:55, 327-348 |
| OpenAI `OpenAI-Beta: realtime=v1` -> in-band `error beta_api_shape_disabled` then server close 4000 with the error's `type.code` as reason | [live] | voice-openai.md:92, 350-355 |
| OpenAI `error` events are non-fatal unless followed by a close; `response.done.usage` shape with audio/cached splits; 60 min session cap; browser subprotocols `realtime`, `openai-insecure-api-key.*` | [live]/[doc] | voice-openai.md:103, 113, 105, 93 |
| OpenAI transcription session (`intent=transcription`) event shapes, usage location, close code at 60 min | [captures-ws] | voice-openai.md:59, 129 |
| AssemblyAI `wss://streaming.assemblyai.com/v3/ws`, raw `Authorization`, binary audio in, `Begin/Turn/Heartbeat/Termination/Error` out, `Terminate` in; close codes 1008 (auth AND too many sessions), 1011, 3005-3009, 410; billing = session-open wall time; `Termination.session_duration_seconds` | [doc] | voice-assemblyai.md:11, 25-30, 40, 59-61, 70 |
| ElevenLabs `stream-input` / multi-context / Agents app-level `ping`->`pong`; `xi-api-key`; no key provisioned | [doc] | voice-elevenlabs.md:16-17, 32-35, 54-57 |

**What `capabilities/captures-ws.md` must deliver** (the fake fixtures and the
unit-test vectors are built from it; G1 cannot be accepted without items 1-6):

1. Inworld TTS: a full success transcript with relative timestamps
   (`create` -> `contextCreated` -> `send_text` x N -> `audioChunk` x M ->
   `flush_context` -> `flushCompleted` -> `close_context` -> `contextClosed`),
   the largest `audioChunk` text frame in bytes for LINEAR16 24 kHz and for
   MP3, and whether any frame carries a usage object.
2. Inworld TTS with a bad key AFTER `create`: error frame (shape, code) or
   silence; with `Bearer` instead of `Basic` on the upgrade: identical or not.
3. Inworld upstream 101 response headers (is `x-inworld-request-id` there?).
4. Inworld: server behaviour when the client sends `close_context` on an
   unknown id (`status.code 5`?), when the client closes without
   `close_context`, and how long a socket with zero contexts stays open
   (idle timeout, close code).
5. Inworld STT: whether `transcribeConfig` is acknowledged; bad-key
   behaviour; one `speechStarted`/`transcription` sequence with timings;
   whether ANY server frame carries audio duration or usage; what follows
   `closeStream` (server close? code?).
6. Inworld STT and TTS: whether the server ever pings, and the max inbound
   text frame the server accepts (send a 1 MiB `audioChunk`).
7. OpenAI Realtime transcription: first frame with `intent=transcription`;
   the `conversation.item.input_audio_transcription.*` sequence; where
   usage for `gpt-live-transcribe` appears, if anywhere; a non-fatal `error`
   (e.g. `input_audio_buffer.commit` on an empty buffer).
8. OpenAI Realtime full: `rate_limits.updated` shape; close code when the
   session limit is hit (if provokable cheaply, skip otherwise).
9. AssemblyAI and ElevenLabs: nothing (no keys); shapes come from the docs
   and are contract-only in this plan.

---

## 1. Dependency and server support

### 1.1 Decision

Add **`websockets>=15,<18`** to `pyproject.toml` `dependencies` (comment in the
style of the uvicorn pin), `make lock` to update `uv.lock`, and use it on
BOTH sides:

* server: `uvicorn.Config(..., ws="websockets-sansio")` in
  `server/lifecycle.py:194-209`, plus `ws_max_size=config.max_frame_bytes`,
  `ws_per_message_deflate=False`, and the defaults `ws_ping_interval=20`,
  `ws_ping_timeout=20` (`uvicorn/config.py:202-206`);
* upstream: `websockets.asyncio.client.connect` (the asyncio API introduced
  in 13.0; 15.0.1 and 17.1 are already in `~/.cache/uv`, 17.1 requires
  Python >=3.11, matching `requires-python`).

Version floor 15 rather than 13: 13/14 still route some client paths through
the deprecated `legacy` package and 15 is the first line where the asyncio
client's `process_exception`/`additional_headers`/`subprotocols` surface is
stable enough to pin tests to. Ceiling 18: a major bump is a deliberate act,
as with uvicorn (`pyproject.toml:17-26`).

### 1.2 Alternatives rejected

| Option | Why not |
|---|---|
| `uvicorn[standard]` | Pulls `httptools`, `uvloop`, `watchfiles`, `python-dotenv`, `pyyaml` (uvicorn METADATA lines 31-36). `uvloop` changes the loop `lifecycle.serve()` installs its signal handlers on (`lifecycle.py:273-287`) and `httptools` replaces the h11 protocol the drain tests were run against. Nothing in the extra is needed except `websockets`. |
| `ws="websockets"` (default impl) | `uvicorn/protocols/websockets/websockets_impl.py:12,18` imports `websockets.legacy.*`, deprecated since 14 and slated for removal; the sansio impl uses `websockets.server.ServerProtocol` (`websockets_sansio_impl.py:21`) and is what `auto.py` already selects when `websockets` is importable. Pinning it explicitly removes the auto-selection from the deploy arithmetic. |
| `wsproto` (`ws="wsproto"`) | Would need a second client library upstream (`httpx-ws`), i.e. two WebSocket stacks with two sets of close-code semantics and two ping implementations. |
| `httpx-ws` upstream | Built on `wsproto`; no server side; adds a dependency on httpx internals that `upstream.py:754-860` does not need for sockets (there is no connection pool to share: one socket per session). |
| `aiohttp` (what the plugins use) | A third HTTP stack in the image; its client semantics are the plugins' problem, not the gateway's. |

### 1.3 What does not change

* `_DrainingServer.capture_signals` (`lifecycle.py:92-106`), the loop-level
  signal handlers, `UVICORN_SHUTDOWN_TIMEOUT_S=3.0` (`lifecycle.py:89`) and
  the post-serve settle (`lifecycle.py:296-308`) apply to socket tasks
  exactly as to request tasks: uvicorn's h11 protocol upgrades a connection
  to the ws protocol class per connection, so `bind_sockets` (`lifecycle.py:150-174`)
  is untouched. Contract test `test_ws_lifecycle.py::dualstack_upgrade` proves it.
* uvicorn's shutdown CANCELS the socket handler task after the 3 s bound; the
  relay must absorb that cancel the way `PassthroughEndpoint.__call__` does
  (`server/app.py:2317-2378`): `uncancel()`, `gw.shutdown_cuts.note(...)`,
  NO per-socket log line (finding 41, `docs/15-findings-log.md:688-724`).
* Dockerfile: no change beyond the lock (`uv sync --frozen`,
  `Dockerfile:27-34`); `websockets` ships a C speedup wheel for
  `cp311-manylinux`, no compiler needed.
* `fly.toml`: `[http_service.concurrency] type="connections"` (fly.toml:135-138)
  already counts sockets; the process-side `LLMGW_MAX_STREAMS` also counts
  them (section 5). `kill_timeout=140s` stays; sessions longer than the grace
  are handled by the drain hook, not by a longer grace (section 4.3).
  Recommend the plugins use `layrs-llmgw.internal` (direct, fly.toml:7-9) so
  the Fly proxy's idle handling is not in the path; if `.flycast` is used,
  the 20 s uvicorn pings keep the proxy's idle timer reset (risk R5).

---

## 2. Module layout: `src/llmgw/ws/`

```
src/llmgw/ws/
  __init__.py       WS_REGISTRY: tuple[WsSurface, ...]; names into metrics.SURFACES
  client.py         connect_upstream(target, path, query, *, subprotocols, extra_headers,
                    budgets, deadline) -> UpstreamSocket; build_headers reuse; scheme swap
  relay.py          Relay: two pumps (client->upstream, upstream->client), each with a
                    _ByteBuffer, byte-rate bound, progress/liveness marks, commitment flag
  session.py        Session: tenant, target, exchange, permits/tickets, clocks, usage,
                    drain() hook, terminal record; registers with Gateway.ws_sessions
  errors.py         CloseCode enum (4900-4907), close_code_class(), classify_close(),
                    classify_frame() -> the third classification input
  routes.py         WsEndpoint (ASGI class endpoint per WsSurface) + build_ws_routes(gateway)
  surfaces/
    base.py         WsSurface protocol: name, routes, product, auth_schemes, accept_policy,
                    upstream_path(route), resolve_model(scope, first_frame), rewrite_first_frame,
                    classify_client(frame), classify_upstream(frame), usage_from_frame,
                    terminate_frame(), is_termination(frame), unit_of_commitment
    inworld_tts.py  routes ("/tts/v1/voice:streamBidirectional",)
    inworld_stt.py  routes ("/stt/v1/transcribe:streamBidirectional",)
    openai_realtime.py  routes ("/v1/realtime",) transcription and full, by ?intent=
    assemblyai_streaming.py routes ("/assemblyai/v3/ws",)
    elevenlabs_tts.py DEFERRED: file present, `enabled=False`, not registered (no key)
```

`_ByteBuffer` moves from `pump.py:638-725` to `llmgw/bytebuf.py` and
`pump.py` imports it (pure move; `tests/unit/test_pump.py` unchanged).

### 2.1 Reused unchanged

| Object | Where | Used for |
|---|---|---|
| `Gateway.draining`, `note_draining_denied`, `over_capacity`, `note_overloaded_denied`, `stream_entered`, `stream_exited`, `inflight`, `shutdown_cuts` | app.py:1315-1356, 1563-1651, 1143-1195 | the same entry/finally pair around every socket, so `begin_drain` waits on sockets and `max_streams` counts them |
| `Gateway.resolve_tenant` (+ new scheme set), `realtime_pin`, `tenant_limits_for` | app.py:1469-1517 | tenant, Realtime pin |
| `AdmissionController.admit` (Permit as async CM), `reserve_session`, `live_sessions` | admission.py:341-460 | per-tenant rate + concurrency; `max_sessions` for relayed sessions |
| `ProviderKeyLimiter.acquire` | admission.py:517 | per-credential socket cap (`ProviderConn.max_concurrency`, catalog.py:122) |
| `_GateWatch`, `_WatchedBreaker`, `credential_health_key` | app.py:808-860; executor.py:426 | target + credential breakers, tickets recorded with the close/frame disposition |
| `build_headers` | upstream.py:381-434 | auth headers; the ws client drops `content-type`/`accept` from its output |
| `join_url` | upstream.py:301 | path join incl. `path_prefix`; scheme swapped `https->wss`, `http->ws` |
| `Exchange`, `gw_headers`, `observe_upstream`, `parse_upstream_telemetry` | app.py:1710-1797, 603-645 | `X-Gw-*` on the 101, upstream request id |
| `send_json_error`, `send_error` | app.py:2832-2937 | pre-101 refusals, sent through Starlette `send_denial_response` |
| `Deadline`, `Budgets`, `StallClock`, `Clock.timeout` | clocks.py:240-440 | every wait |
| `errors.*` classes, `decide`, `from_http_status` (for the upgrade response status) | errors.py:123-860, 1024 | taxonomy, commitment gate |
| `PolicySnapshot.plan_for(kind=)` | policy.py:510 | target resolution with aliases (A1) |
| `Capture.offer`, `CaptureRecord` | capture.py:68-160, 297 | terminal records |
| `Collectors` | metrics.py | `stream_open/close`, `request`, `committed`, new ws families |
| `accounting._cost_usd` (via new `account_usage`) | accounting.py:395-515 | seconds/characters/token pricing |
| `apply_api_model(body, api_model, key=)` | upstream.py:480 | rewriting `modelId`/`model` inside the first frame |
| `ShutdownCuts` | app.py:1143 | the one summary line at exit |

### 2.2 Small extensions (each a separate commit inside G1 unless noted)

| Extension | File | Detail |
|---|---|---|
| `Budgets.session_total: float | None = None`, `Budgets.idle: float | None = None` | clocks.py:241-296 | session-scale clocks beside per-unit budgets; `None` = unbounded/disabled |
| Profiles `tts_session`, `stt_session`, `realtime_session` | config/workloads.example.toml (after :115); `policy.py:677-690` needs no change | values in section 4.1 |
| `largest_total()` ignores `session_total` | policy.py:567-578 | the deploy inequality is about per-unit totals; sessions are drained by the hook |
| `ServerConfig.ws_drain_wait_s: float = 20.0`; `check_drain_arithmetic` adds `ws_drain_wait_s <= drain_grace_seconds` and logs the largest `session_total` above the grace at INFO | config.py:637-690, 821-852 | DEPLOY.md "arithmetic" gains a second line |
| `AuthScheme` gains `"basic"`; `build_headers` emits `Authorization: Basic <key>`; inworld row switches to `basic` | catalog.py:64, 397-408; upstream.py:417-427 | Basic and Bearer are identical on Inworld HTTP [live] voice-inworld.md:23, so the D surfaces do not change behaviour; the socket path sends exactly what the plugin sends |
| Tenant credential parsing: `credential_token(scope, *, schemes: frozenset[str], subprotocols: bool)` beside `bearer_token`; `resolve_tenant(scope, schemes=...)` | app.py:411-428, 1469 | HTTP routes keep `{"bearer"}`; ws surfaces declare `auth_schemes` |
| `metrics.SURFACES` + `inworld_tts_ws`, `inworld_stt_ws`, `openai_realtime`, `assemblyai_streaming`, `elevenlabs_tts_ws` | metrics.py:56-75 | closed set stays closed; `series_estimate` re-run against `CARDINALITY_BUDGET` (metrics.py:482) |
| New metric families (section 7.3) | metrics.py:253+ | |
| `CaptureRecord.session_id: str | None = None`; `kind` values `"session"`, `"response"` | capture.py:68-160 | links Realtime per-response records to their session |
| `accounting.account_usage(usage, *, target, catalog, outcome, committed, attempts, workload_id, policy_id, code) -> AccountingRecord` factored out of `_account` | accounting.py:281-340 | sessions have no `ExecutionResult`; `_account` becomes a thin adapter |
| Catalog row `inworld.stt-1` (`api_model="inworld/inworld-stt-1"`, `unit="seconds"`, `per_minute=0.0025` = $0.15/h on-demand, `priced_at="2026-09-16"`, `default_profile="stt_session"`); aliases: `inworld.tts-2-flash` gains `"inworld-tts-1.5-mini"` only if Layrs has not moved off the deprecated id by G1 exit (PLAN-2.md:482-486 says the Layrs fix rides along; the alias is the fallback, flagged in `cost_notes`) | catalog.py:790-940 | voice-inworld.md:127, 166 |
| `AdmissionController.enter_session(tenant) -> Permit` counting against `max_sessions` alongside the TTL reservations | admission.py:406-450 | a relayed session is a live session (C19 wording extended in C23) |
| `Gateway.ws_sessions: set[Session]` and `begin_drain` calling `session.drain()` on each before waiting on `_idle` | app.py:1653-1702 | the only edit to `begin_drain`: one loop before the existing `async with self.clock.timeout(grace_s)` |

---

## 3. Per-product frame semantics

Common vocabulary for the tables: **PROGRESS** resets the direction's progress
clock and, the first time, sets `first_event_at`; **LIVENESS** resets only
the liveness mark (C7); **META** is relayed and ignored by the clocks;
**ERROR** goes through `ws/errors.classify_frame`; **TERMINAL** ends the
unit of commitment (context/response/session); **COMMIT** is the frame whose
relay to the client sets the commitment flag for that unit.

### 3.1 Inworld TTS (`inworld_tts_ws`, the production path)

| Aspect | Value | Status |
|---|---|---|
| Client route | `GET /tts/v1/voice:streamBidirectional` (Upgrade). No `/workloads/{w}` use from the plugin (0.1) but the twin is still registered for other callers | — |
| Upstream URL | `wss://api.inworld.ai/tts/v1/voice:streamBidirectional` via `join_url(provider.base_url, path)` with scheme swap | [source] tts.py:57-58, 259 |
| Upstream auth | `Authorization: Basic <INWORLD_API_KEY>` (`auth_scheme="basic"`); `X-Request-Id` forwarded (allowlist config.py:104); `X-User-Agent` not forwarded (gateway sends its own) | [live] Basic on HTTP; Basic on WS [captures-ws #2] |
| Tenant auth | `Authorization: Basic <tenant token>` or `Bearer <tenant token>` | plugin tts.py:263 |
| Subprotocol | none | [source] |
| Accept policy | **accept-then-relay**: the client 101 is sent after tenant/admission/cap checks and the upstream 101 (connect budget, TCP+TLS+upgrade); there is no unsolicited upstream frame to wait for (voice-inworld.md:236: bad key = silence) | [live] |
| Handshake budget | `connect` (TCP+TLS+101) then `headers` (`clocks.py:256`, 10 s default) from the FIRST relayed `create` to `contextCreated` or an `error`/`status` frame. Silence past `headers` after a `create` is `HeadersTimeout` (NEUTRAL, try_next, errors.py:335-360), because with a bad key Inworld is silent and only a `create` distinguishes "authenticated and idle" from "unauthenticated" | [live] + [captures-ws #1,#2] |
| Model resolution | `create.modelId` -> `plan_for(default workload, model=<id>, kind="openai")` via aliases; `modelId` rewritten to `api_model` with `apply_api_model(key="modelId")`; the rewrite is per context (each `create` may name a model; v1 requires every `create` on one socket to resolve to the same target, else `status`-free close 4907 with reason `llmgw:invalid_request`) | A1 semantics |
| Client->upstream frames | `create` = config (replayable pre-commit); `send_text` = CONTENT-IN (progress for the inbound direction; characters counted); `flush_context`, `close_context` = META | [source] tts.py:390-432 |
| Upstream->client frames | `result.contextCreated` = META (ends the handshake budget); `result.audioChunk` = PROGRESS + COMMIT for its `contextId`; `result.flushCompleted` = META; `result.contextClosed` = TERMINAL for that context; `result.status.code != 0` = ERROR (code 5 "Context not found" is benign META per the plugin, voice-inworld.md:118); top-level `error{code,message,details}` = ERROR (connection-level) | [source] |
| Liveness | none from the provider; the relay's own WS pings (uvicorn 20 s client-side, `websockets` `ping_interval=20` upstream) are transport liveness and do not touch the progress clock | [source] voice-inworld.md:34 |
| Terminate on drain | per open context: nothing synthesised to the client (C2); the drain waits for contexts to reach zero, then closes upstream 1000 and the client 4900 (section 4.3) | — |
| Termination to wait for | `contextClosed` for each context after `close_context` | [source] |
| Multiplexing | one client socket = one upstream socket, contexts relayed 1:1 by `contextId`. Not one-upstream-fan-out: the plugin already pools 20 sockets x 5 contexts (tts.py:643, 174), the provider's concurrency unit is contexts (voice-inworld.md:112), and a shared upstream socket would make one tenant's socket failure cut another tenant's contexts. The gateway enforces `<=5` open contexts per socket (6th `create` -> close 4907 reason `llmgw:invalid_request`) so the provider never sees the violation | [source] |
| Max message | inbound text frame bound = `max_frame_bytes` (1 MiB); an `audioChunk` is one second of audio at most (64 KB LINEAR16 on the HTTP path, voice-inworld.md:51) [captures-ws #1 for the WS figure] | |
| Usage | characters = sum of `len(send_text.text)` per context, `estimated`; exact only if a usage object appears on WS frames [captures-ws #1] | |
| Unit of commitment | context | PLAN-2.md:449 |

### 3.2 Inworld STT (`inworld_stt_ws`)

| Aspect | Value | Status |
|---|---|---|
| Client route | `GET /stt/v1/transcribe:streamBidirectional`; `/workloads/{w}` twin reachable (prefix preserved, stt.py:272) | |
| Upstream URL / auth | `wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional`, Basic | [source] stt.py:52, 271-277 |
| Tenant auth | Basic or Bearer | stt.py:122 |
| Accept policy | accept-then-relay (no unsolicited first frame: with no credential the error comes at once, voice-inworld.md:237; with a bad key silence is presumed [captures-ws #5]) | [live]/[captures-ws] |
| Handshake budget | `connect`, then `headers` from the relayed `transcribeConfig` to: an ack if one exists [captures-ws #5], else the first server frame or the first `audioChunk` forwarded (whichever first). Error frame inside the window -> classify, pre-commit, fallback across Inworld rows only | |
| Model resolution | `transcribeConfig.modelId` -> alias -> `inworld.stt-1`; rewritten to `api_model` | |
| Client->upstream | `transcribeConfig` = config (replayable); `audioChunk` = PROGRESS-IN (and COMMIT for the session on the first forwarded chunk: from here no fallback); `endTurn`, `closeStream` = META | [source] stt.py:283-364 |
| Upstream->client | `result.speechStarted` = LIVENESS; `result.transcription{isFinal:false}` = PROGRESS; `{isFinal:true}` = PROGRESS; `result.status.code!=0` = ERROR; `error{}` = ERROR | [source] stt.py:406-465 |
| Progress rule | the session is healthy while EITHER direction progresses: a silent learner (no audio) is a client-side idle, not a provider stall; audio flowing in with no transcription for `progress` is a provider stall (PLAN-2.md:445-448) | |
| Terminate on drain | forward `{"closeStream":{}}` upstream; close client 4900 at once (the plugin reconnects, stt.py:287-336) | |
| Termination to wait for | whatever follows `closeStream` [captures-ws #5]; else upstream close within `ws_drain_wait_s` | |
| Max message | client `audioChunk` frames are tiny (one `rtc.AudioFrame`, tens of ms); bound 1 MiB | |
| Usage | seconds = decoded bytes of forwarded `audioChunk.content` / (`sampleRateHertz` x `numberOfChannels` x 2) from `transcribeConfig` (LINEAR16 only; other encodings -> gateway clock while audio was flowing), `estimated`, `cost_notes=["seconds derived from relayed audio bytes"]`; exact if a provider usage frame exists [captures-ws #5] | corrects voice-inworld.md:55 |
| Unit of commitment | session | |

### 3.3 OpenAI Realtime (`openai_realtime`): transcription first, then full

| Aspect | Value | Status |
|---|---|---|
| Client route | `GET /v1/realtime?model=<catalog id or alias>[&intent=transcription]`; `/workloads/{w}/v1/realtime` twin | plugins: stt.py:386, realtime_model.py:615-647 |
| Upstream URL | `wss://api.openai.com/v1/realtime?model=<api_model>[&intent=...]`; query forwarded minus credential-looking keys (the D2 rule, app.py:2425 `_upstream_path`); `model` rewritten | [live] voice-openai.md:55 |
| Upstream auth/headers | `Authorization: Bearer`; `OpenAI-Safety-Identifier: <tenant>` (realtime_control.py `SAFETY_IDENTIFIER_HEADER`); `OpenAI-Beta` NEVER forwarded (not in the allowlist and explicitly refused if present: 400 pre-101 `invalid_request` "OpenAI-Beta is not supported on the GA Realtime API", so a stale client learns it from the gateway rather than from a close 4000) | [live] voice-openai.md:92, 350-355 |
| Tenant auth | `Bearer <tenant token>`; browsers: subprotocol `openai-insecure-api-key.<tenant token>` (stripped before upstream), `realtime` echoed as the accepted subprotocol and offered upstream | [doc] voice-openai.md:93 |
| Accept policy | **accept-after-upstream-ready**: upstream connect + `session.created` (first frame, unsolicited, ~1.7 s [live] voice-openai.md:332) BEFORE the client 101; `first_event` budget covers it. Any failure here is HTTP on the upgrade and eligible for fallback to another OpenAI-dialect Realtime row (e.g. a second credential). The `session.created` frame is relayed to the client immediately after 101 | [live] |
| Model / pin | `?model=` (or tenant pin `[tenants.<id>.realtime].model`, which wins, app.py:2253-2265, C19); the client's `session.update` is rewritten with the pinned keys (`PINNABLE_SESSION_KEYS`, realtime_control.py) and `pinned_keys` recorded; `X-Gw-Body-Modified: 1` on the 101 when any rewrite happened | C19 |
| Client->upstream | `session.update` = config (replayable pre-commit); `input_audio_buffer.append` = PROGRESS-IN; `input_audio_buffer.commit/clear`, `conversation.item.*`, `response.create/cancel`, `output_audio_buffer.clear` = META | [doc] voice-openai.md:71 |
| Upstream->client | `session.created/updated` = META; `response.created` = opens a response unit; `response.output_audio.delta`, `response.output_text.delta`, `response.function_call_arguments.delta`, `conversation.item.input_audio_transcription.delta` = PROGRESS (+COMMIT for the open unit); `response.done` = TERMINAL for that unit + usage; `conversation.item.input_audio_transcription.completed` = PROGRESS (transcription sessions have no `response.*`; the unit is one completed item); `rate_limits.updated` = META (+ per-credential gauges, A6 analogue); `input_audio_buffer.speech_started/stopped` = LIVENESS; `error` = ERROR **non-fatal** (relayed, counted, session continues) unless a server close follows within 1 s, in which case the close is the verdict (voice-openai.md:103) | [live]/[doc] |
| Liveness | none app-level; transport pings | |
| Terminate on drain | no provider terminate message: close client 4900, close upstream 1000 after an in-flight `response.done` or `ws_drain_wait_s`, whichever first | |
| Max message | 1 MiB; audio deltas are small base64 chunks | |
| Usage | per `response.done.response.usage`: `input_tokens`, `output_tokens`, `input_token_details.{text,audio,image,cached}_tokens`, `cached_tokens_details.{text,audio}`, `output_token_details.{text,audio}` -> `Usage` kinds (B3, surfaces/base.py Usage fields, catalog rows `openai.gpt-realtime-mini` catalog.py:770-790), exact; transcription sessions: seconds derived from forwarded audio bytes at the declared `audio.input.format.rate` (24 kHz PCM default), `estimated`, unless [captures-ws #7] finds a usage event | [live] voice-openai.md:342-346 |
| Unit of commitment | response (full) / completed transcription item (transcription) | PLAN-2.md:449 |
| Session cap | 60 min (voice-openai.md:105) -> `session_total=3600` in `realtime_session` | [doc] |

### 3.4 AssemblyAI universal-streaming (`assemblyai_streaming`; contract-only, no key)

| Aspect | Value | Status |
|---|---|---|
| Client route | `GET /assemblyai/v3/ws?<query>`; the plugin's `base_url` becomes `ws://layrs-llmgw.internal:8080/assemblyai` | plugin stt.py:~175, 897 |
| Upstream | `wss://streaming.assemblyai.com/v3/ws?<query>` on provider row `assemblyai-streaming` (catalog.py:439-448, `auth_scheme="raw"`, `forbidden_means="rate_limit"`); `speech_model` rewritten to `api_model`; `token` refused (credential-looking key) | [doc] voice-assemblyai.md:11, 49 |
| Tenant auth | raw `Authorization: <tenant token>` or Bearer | plugin stt.py:~887 |
| Accept policy | accept-after-upstream-ready: `Begin{id, expires_at, configuration}` is the first server frame; `first_event` budget; `Begin.configuration.model` checked against the target's `api_model` (unknown params are silently ignored by the provider, voice-assemblyai.md:27) -> mismatch is `ModelNotFound`-shaped pre-101 | [doc] |
| Client->upstream | binary frames = PROGRESS-IN (+ session COMMIT on the first forwarded frame); JSON `UpdateConfiguration`, `ForceEndpoint`, `KeepAlive` = META; `Terminate` = client-initiated end (relay, then wait for `Termination`) | [doc] voice-assemblyai.md:25-26 |
| Upstream->client | `Begin` = META (ends handshake); `SpeechStarted` = LIVENESS; `Turn` = PROGRESS; `Heartbeat{realtime_factor, total_audio_received_ms}` = LIVENESS (+ blame input: `realtime_factor < 1` with inbound audio stalled = client fault); `SpeakerRevision`, `LLMGatewayResponse` = META; `Termination{audio_duration_seconds, session_duration_seconds}` = TERMINAL + usage; `Error{error_code, error}` = ERROR, remembered for the close that follows | [doc] voice-assemblyai.md:27, 40, 60 |
| Terminate on drain | send `{"type":"Terminate"}` upstream, wait <= `ws_drain_wait_s` for `Termination` (the billing number), close client 4900 at once | PLAN-2.md:442-444 |
| Session cap | 3 h (3008) or the token's cap -> `session_total=10800`; `inactivity_timeout` 5-3600 s is the provider's, the gateway's `idle` is 60 s of neither audio nor `Turn` | [doc] voice-assemblyai.md:29 |
| Usage | `seconds = Termination.session_duration_seconds` exact (billing is session-open time, voice-assemblyai.md:70); gateway clock `estimated` if the socket dies first | [doc] |
| Unit of commitment | session | |

### 3.5 ElevenLabs (`elevenlabs_tts_ws`) — deferred

File present with the frame table from voice-elevenlabs.md:32-35
(`stream-input` `{"text":" "}` init, `audio` = PROGRESS, `isFinal` = TERMINAL,
`inactivity_timeout` 20 s default; multi-context `initialiseContext`/
`closeContext`; Agents `ping{event_id}` -> `pong` answered BY THE GATEWAY within
1 s if the client has not answered in 500 ms, so a slow client never trips the
provider's deadline), `auth_scheme="header"` (`xi-api-key`, catalog.py:409-424).
Not registered, no fake, no tests beyond a unit test that the classifier
table is total. Enabled when a key exists; multi-context follows the Inworld
rule (one client socket = one upstream socket).

---

## 4. Clocks and drain

### 4.1 Budget profiles (policy `[profiles.<name>.budgets]`, B6 mechanism, policy.py:677-690)

| Profile | connect | headers (handshake) | first_event | progress | client_stall | idle | session_total | Notes |
|---|---|---|---|---|---|---|---|---|
| `tts_session` (Inworld TTS WS) | 5 | 10 (`create` -> `contextCreated`) | 2 per context (`send_text`/`flush` -> first `audioChunk`; TTS-2 P90 100 ms, voice-inworld.md:88) | 5 per context | 10 | 600 (zero contexts; the plugin's own idle sweep is the pool's `idle_connection_timeout`, tts.py:661, 898) | 3600 | a socket lives across many utterances |
| `stt_session` (Inworld STT, AssemblyAI) | 5 | 10 (`transcribeConfig`/`Begin`) | 10 (`Begin` or first frame) | 60 (no `Turn` while audio flows in) | 10 | 60 (no audio in, no `Turn` out; the AssemblyAI plugin warns at 15 s of silence, the Inworld plugin times out at 60 s) | 10800 | inbound audio is progress |
| `realtime_session` (OpenAI) | 5 | 10 | 10 (`session.created`) | 15 per response | 10 | 300 | 3600 | 60 min provider cap |

`client_stall` on a socket means: the client->relay buffer for the
upstream->client direction has been full (256 KiB) for that long. Close
client 4903, upstream 1000. It is the S5 backpressure rule (FAILURE-MODES row 4)
with a close code instead of `ClientTooSlow` on an HTTP body.

### 4.2 Byte-rate bounds

Per socket, per direction, token-bucket over 1 s windows, configurable per
surface in `ServerConfig.surface_limits` (config.py:97-127, `SurfaceLimits`
gains `max_in_bps` / `max_out_bps`, `None` = inherit):

| Surface | client->upstream | upstream->client | Basis |
|---|---|---|---|
| `inworld_stt_ws`, `assemblyai_streaming` | 64 KB/s (16 kHz PCM16 mono is 32 KB/s, x1.33 base64 = 43 KB/s; 64 leaves headroom for 24 kHz) | 16 KB/s (JSON transcripts) | voice-inworld.md:70 |
| `inworld_tts_ws` | 16 KB/s (text) | 128 KB/s (64 KB/s per second of 24 kHz LINEAR16 on the wire; a burst of two contexts) | voice-inworld.md:51, 65 |
| `openai_realtime` | 64 KB/s | 128 KB/s | 24 kHz PCM both ways |

Exceeding the inbound bound: the relay stops reading the client (TCP
backpressure) rather than dropping; exceeding it for `client_stall` seconds is
close 4903. The provider's own pacing rule (AssemblyAI 3007) is thereby never
triggered by the gateway.

### 4.3 Drain

`Gateway.begin_drain` (app.py:1653-1702) keeps its three steps; step 1 gains
one loop:

```
self.draining = True                       # unchanged: /healthz 503, new upgrades 503
for s in list(self.ws_sessions): s.drain() # NEW: schedule per-session drain, no await
async with self.clock.timeout(grace_s): await self._idle.wait()   # unchanged
```

`Session.drain()` per product:

| Product | Client side | Upstream side | Bounded by |
|---|---|---|---|
| Inworld TTS | wait until open contexts reach zero (each utterance is <=2 min of audio, well inside the 130 s grace), then close **4900** `llmgw:draining`; if contexts are still open at `grace - ws_drain_wait_s`, close 4900 anyway (the plugin fails those contexts and re-synthesises on a fresh socket, tts.py:603-616) | close 1000 after the client close | grace |
| Inworld STT | close **4900** immediately (the plugin's `_run` loop reconnects, stt.py:287-336; DNS `layrs-llmgw.internal` spreads reconnects across machines) | send `closeStream`, wait for the provider's termination [captures-ws #5] or close, <= `ws_drain_wait_s` | 20 s |
| AssemblyAI | close 4900 immediately | send `Terminate`, wait for `Termination` (exact seconds) <= `ws_drain_wait_s` | 20 s |
| OpenAI Realtime | close 4900 immediately | close 1000 after an in-flight `response.done` or `ws_drain_wait_s` | 20 s |

Why the client is cut first for STT/Realtime: waiting the whole grace with
the client attached buys nothing (the session would still be cut at the end)
and costs 130 s of session billing plus a reconnect storm at the worst
moment (grace expiry, when uvicorn's 3 s bound is about to cancel). Cutting
at drain start gives the plugin 130 s to be steady on another machine.

Arithmetic (DEPLOY.md:143-165 gains the second line):

```
LLMGW_BUDGET_TOTAL (120) <= LLMGW_DRAIN_GRACE (130) < grace + 3 < kill_timeout (140)
LLMGW_WS_DRAIN_WAIT (20) <= LLMGW_DRAIN_GRACE (130);  session_total may exceed the grace
```

`check_drain_arithmetic` (config.py:821-852) enforces the second inequality and
`largest_total()` (policy.py:567) excludes `session_total`, so a 3 h
`stt_session` profile no longer refuses startup.

### 4.4 `X-Gw-*` on the 101

Sent through `WebSocket.accept(subprotocol=..., headers=...)`
(starlette/websockets.py:100-110), built by `Exchange.gw_headers()` (app.py:1785-1797):
`X-Gw-Policy`, `X-Gw-Model` (catalog id), `X-Gw-Workload`, `X-Gw-Served-By`,
`X-Gw-Attempts`, `X-Gw-Tenant`, `X-Gw-Breaker`, `X-Gw-Upstream-Request-Id`
(from the upstream 101 response headers, `connect(...).response.headers`;
Inworld's `x-inworld-request-id` [captures-ws #3]), `X-Gw-Body-Modified: 1`
when a first-frame rewrite happened, and new `X-Gw-Session-Id` (the capture
record's `request_id`). Under accept-then-relay (Inworld) `X-Gw-Served-By`
names the target the socket was OPENED TOWARD; the capture record is the
final word (C23 states this; it differs from app.py:2293-2301's "answered"
semantics only for Inworld).

---

## 5. Admission, caps, isolation

Order on the upgrade, identical to `PassthroughEndpoint.__call__`
(app.py:2141-2216) and refusing at the same statuses, all pre-101 through
`send_denial_response`:

1. `stream_entered()` + `collectors.stream_open(surface=)`; `finally` pairs
   them (app.py:2141-2143, 2411-2419). Sockets therefore count toward
   `LLMGW_MAX_STREAMS` (`over_capacity`, app.py:1628-1637) and toward
   `llmgw_streams_open`. `fly.toml:72` note gains "sockets count".
2. `gw.draining` -> 503 `draining`.
3. tenant via `credential_token` with the surface's scheme set -> 401.
4. `over_capacity()` -> 503 `overloaded`, `Retry-After: 1`.
5. workload (`X-Gw-Workload` or path prefix) -> 400.
6. `admission.admit(tenant)` -> 429 (rate or concurrency; C6 denial is free),
   held for the socket's lifetime; `admission.enter_session(tenant)` ->
   429 with `Retry-After` = earliest expiry when `max_sessions` is reached
   (config/tenants.example.toml:92; C19 mechanism).
7. per-credential `ProviderKeyLimiter.acquire(key, cap)` -> 429/`try_next`
   (`ProviderKeyExhausted`, errors.py:258); breakers via `_GateWatch`.
8. upstream connect (section 3 accept policy) -> 502/503/504 by taxonomy,
   `send_error` with the scrub rule (`scrub_error_bodies="all"` for Inworld,
   catalog.py:406).

Isolation properties carried over: one tenant's socket storm is refused at
step 6 before any memory is allocated; a slow client is bounded by its own
256 KiB buffer per direction and closed 4903; a mass disconnect runs each
session's `finally` (permit release, ticket release, record) with NO
per-socket log line, and the chaos tier proves the fd/task/permit counts
return to baseline (finding 41 shape; `tests/chaos/test_invariants.py`
pattern). Per-socket memory target: <=600 KiB worst case (two full buffers
plus framing), measured in S10.

---

## 6. Commitment and fallback

* Commitment is per unit (3.x tables): context (Inworld TTS), response /
  completed transcription item (OpenAI), session (Inworld STT, AssemblyAI).
  The flag is set BEFORE the await that writes the committing frame to the
  client, the `Pump._committed` rule (pump.py:18, 585).
* **Pre-commit fallback**: allowed for handshake failures (connect, upgrade
  status, first-frame auth error, `HeadersTimeout`), walking the plan like
  the executor does, with the same tickets/permits/dispositions. What may be
  replayed to the next target is the **config prefix** only: the first
  config frame (`create`, `transcribeConfig`, `session.update`), capped at
  64 KiB, buffered by the session until the unit commits. Content frames
  (audio in either direction, `send_text`) are never buffered for replay:
  a failure after the first content frame was forwarded upstream, even if
  nothing came back yet, is a session failure (close 4904/4907), by policy
  (PLAN-2.md:449-452), because replaying audio to a second provider bills the
  tenant twice for one utterance and can produce two transcripts. The
  plugins already reconnect on their side.
* **Post-commit**: `decide(err, committed=True)` (errors.py:837-849): never
  retry, never try next; outcome INTERRUPTED; usage so far recorded
  `estimated` (C3).
* Cross-product fallback (Inworld STT -> AssemblyAI) is out of scope: the
  wire dialects differ and the client speaks one of them. Fallback stays
  within a dialect (`plan_for(kind=...)`, the C4 rule in policy.py).

---

## 7. Errors, accounting, metrics

### 7.1 Close codes and in-band frames as the third classification input

`ws/errors.py`:

```
class CloseCode(IntEnum):
    DRAINING = 4900; SESSION_TOTAL = 4901; PROVIDER_STALL = 4902; CLIENT_STALL = 4903
    UPSTREAM_GONE = 4904; FRAME_TOO_LARGE = 4905; IDLE = 4906; UPSTREAM_HANDSHAKE = 4907
```

Range 4900-4999 is clear of every provider code observed or documented
(1000/1001/1006/1008/1011, AssemblyAI 3005-3009 and 410, OpenAI 4000,
ElevenLabs 4300). Reason strings are `llmgw:<code>` with `<code>` from
`errors.ERROR_CODES` (errors.py:1165), so a client can switch on either. C25.

Upstream close -> taxonomy (`classify_close(code, reason, last_error_frame, product)`):

| Upstream close | Preceding frame | Class | Blame | Health | retry_same / try_next (pre-commit) |
|---|---|---|---|---|---|
| 1000 after our terminate / client's `Terminate`/`closeStream` | any | normal end, outcome COMPLETED | — | NEUTRAL | — |
| 1000 unsolicited mid-session | none | `IncompleteStream` (errors.py:442) | PROVIDER | FAILURE | no / yes |
| 1001 | — | `UpstreamDisconnected` (going away = provider deploy) | PROVIDER | NEUTRAL | yes / yes |
| 1006 (abnormal, no close frame) | — | `UpstreamDisconnected` | PROVIDER | FAILURE | yes / yes |
| 1008 AssemblyAI | `Error` mentions "Too many concurrent sessions" / 3009 | `RateLimited` | PROVIDER | NEUTRAL | backoff / yes |
| 1008 AssemblyAI | `Error` mentions auth / no `Error` frame | `AuthenticationFailed` (credential scope) | POLICY | FAILURE(cred) | no / yes |
| 1008 other providers | — | `InvalidRequest` | CLIENT | NEUTRAL | no / no |
| 1009 (too big) | — | `FrameTooLarge`-shaped, gateway blame (our bound failed) | GATEWAY | NEUTRAL | no / no |
| 1011, 3005 | — | `UpstreamServerError` | PROVIDER | FAILURE | yes / yes |
| 3006 AssemblyAI | `Error` "inactivity" | idle end, COMPLETED with note | CLIENT | NEUTRAL | — |
| 3006/3007 AssemblyAI (invalid message / pacing) | — | `InvalidRequest` | CLIENT | NEUTRAL | no / no |
| 3008 AssemblyAI (max session) | — | `TotalDeadlineExceeded`-shaped, expected | — | NEUTRAL | — |
| 410 AssemblyAI | — | `PolicyError` (config drift) | POLICY | NEUTRAL | no / no |
| 4000 OpenAI | `error` frame | class from the frame's `error.code`/`type` via the existing body reader (`_error_hints`, errors.py:918) | per frame | per frame | no / yes |
| 4300 ElevenLabs Agents queue timeout | — | `UpstreamOverloaded` | PROVIDER | NEUTRAL | yes / yes |

In-band frames -> taxonomy (`classify_frame(product, frame)`):

| Product | Frame | Class | Fatal? |
|---|---|---|---|
| Inworld | `error.code 16` (UNAUTHENTICATED), `7` (PERMISSION_DENIED) | `AuthenticationFailed`, credential scope; body scrubbed (Inworld reflects the key prefix, voice-inworld.md:24) | yes |
| Inworld | `error.code 3` (invalid argument), `5` on `create` (unknown voice/model) | `InvalidRequest` / `ModelNotFound` (D4 rule) | that context only; connection-level `error` closes 4907 |
| Inworld | `result.status.code 5 "Context not found"` | benign META (plugin: tts.py:478-496 treats as context failure, the sweep marks benign; relay it, count it, do not end the socket) | no |
| Inworld | `error.details[].reconnectType == "NO_RETRY"` | forces `retry_same=False` regardless of class (provider-supplied hint, voice-inworld.md:110) | — |
| OpenAI | `error` with `code` in {`invalid_api_key`, `insufficient_quota`, billing codes from A2} | `AuthenticationFailed` / `InsufficientCredits` | wait 1 s for the close; the close is the verdict |
| OpenAI | `error` otherwise (e.g. empty buffer commit) | counted `llmgw_ws_inband_errors_total`, relayed, session continues | no |
| OpenAI | `response.done.response.status == "failed"/"incomplete"` | per-response outcome INTERRUPTED/COMPLETED with `stop_reason` from `status_details` (A3 vocabulary) | no |
| AssemblyAI | `Error{error_code, error}` | remembered; classified with the close that follows (close reason is truncated to 123 bytes, voice-assemblyai.md:60) | with the close |

Client-facing report:

* **Pre-101**: HTTP status + the existing JSON error body (`send_error`,
  C4/C11 unchanged; `scrub_all` for Inworld). The plugins surface it as
  `APIStatusError` (stt.py:322-326), a path they already handle.
* **Post-101**: a close frame with the code above and reason `llmgw:<code>`,
  never a synthesised provider frame, never a trailing JSON error (C2, C20
  extended by C25). Provider closes are forwarded with their own code and
  reason, byte for byte.

### 7.2 Accounting and capture

* One `CaptureRecord` per session at close, `kind="session"`: `request_id`
  (= `X-Gw-Session-Id`), tenant, workload, provider, model, outcome
  (COMPLETED on a clean end, INTERRUPTED after commit, FAILED before, CANCELED
  on client close before commit / shutdown cut), `committed`, `attempts`,
  `duration_s` (session), `first_event_latency` (handshake to first
  PROGRESS), `units.seconds` / `units.characters`, `tokens` (summed over
  responses), `cost_usd`, `basis`, `error_code`, `upstream_request_id`,
  `cost_notes` (`"seconds derived from relayed audio bytes"`, `"contexts=N"`).
* OpenAI Realtime full sessions additionally write one record per
  `response.done`, `kind="response"`, `session_id` = the session's id,
  tokens exact from `usage`, so per-turn cost is queryable; the session
  record's tokens are the sum. Transcription sessions: session record only.
* `basis`: exact when the unit's meter came from the provider
  (`Termination.session_duration_seconds`, `response.done.usage`, any Inworld
  usage frame); estimated otherwise (gateway clock, audio bytes,
  `send_text` characters). C21 extended by C27.
* Pricing through `accounting.account_usage` -> `_cost_usd`
  (accounting.py:395-515): `seconds/60 x per_minute`, `characters x
  input_per_m`, token kinds for Realtime including `audio_input`,
  `audio_output`, `cached_audio_input` (metrics.py:97-112).
* Metrics: `llmgw_requests_total{surface, outcome, code}` once per session
  (the exactly-once metric, app.py `_record`), `llmgw_committed_total`,
  `llmgw_cost_usd_total`, `llmgw_units_total{kind}`, token counters, TTFE
  histogram with the handshake-to-first-progress number.

### 7.3 New metric families (closed label sets)

| Family | Labels | Values |
|---|---|---|
| `llmgw_ws_sessions_open` (gauge) | `surface` | the 5 ws names |
| `llmgw_ws_bytes_total` (counter) | `surface`, `direction` | `direction` in (`client_in`, `client_out`) — bytes relayed, after framing |
| `llmgw_ws_close_total` (counter) | `surface`, `side`, `code_class` | `side` in (`client`, `upstream`); `code_class` in (`normal_1000`, `going_away_1001`, `abnormal_1006`, `policy_1008`, `internal_1011`, `provider_3xxx`, `provider_4xxx`, `gateway_49xx`, `other`) |
| `llmgw_ws_inband_errors_total` (counter) | `surface`, `fatal` | `fatal` in (`true`, `false`) |
| `llmgw_ws_session_seconds` (histogram) | `surface` | buckets 1,10,60,300,900,1800,3600,7200,10800 |

Series added at the design point (metrics.py:487): 5 + 10 + 90 + 10 + 5x11 = ~170, far inside the 60k budget.

---

## 8. Fakes, tests, load, live

### 8.1 `fakes/ws.py`

Mounted on the existing fake app (`fakes/upstream.py:1322 build_app`) as
`WebSocketRoute`s so the contract tier's `serve_in_thread` fixture
(`tests/contract/conftest.py:73-82`) serves them on the same port; the mode
comes from `X-Fake-Mode` on the upgrade (the plugins send no such header, the
tests and the bench do; the gateway forwards `x-fake-*` in the bench
config, `bench/_gwproc.py:60-71`). Routes: `/tts/v1/voice:streamBidirectional`,
`/stt/v1/transcribe:streamBidirectional`, `/v1/realtime`, `/v3/ws`.
Shapes come from `capabilities/captures-ws.md` (Inworld, OpenAI) and the
docs (AssemblyAI). Counters extend `Stats` (fakes/upstream.py:290) with
`ws_open`, `ws_closed_by_client`, `terminates_received`, `bytes_in/out`.

| Mode | Behaviour |
|---|---|
| `ok` | full success transcript per product; audio at the real cadence (`X-Fake-Interval`), sizes per `X-Fake-Bytes` (default small; `48044` for the measured Inworld line) |
| `auth-fail-in-band` | 101 then the Inworld code-16 `error` frame (voice-inworld.md:237) / OpenAI `error` + close 4000 / AssemblyAI `Error` + close 1008 |
| `silent-after-101` | 101 and nothing, ever (Inworld bad key) |
| `close-1008-with-error-frame` / `close-1008-without` | AssemblyAI ambiguity both ways |
| `queued-before-begin` | AssemblyAI `Begin` after N s (first_event budget) |
| `stall-mid-session` | stops emitting after M frames while reading audio (provider stall vs silent client, decided by `X-Fake-Stall-Side`) |
| `die-mid-session` | TCP reset mid-frame (1006) |
| `slow-consumer` | the fake reads the client's audio at `X-Fake-Read-Bps` (backpressure toward the gateway's inbound buffer) |
| `usage-in-termination` | AssemblyAI `Termination{session_duration_seconds}` on `Terminate`; Inworld `contextClosed` on `close_context` |
| `context-multiplex` | Inworld: accepts up to 5 contexts, errors `status.code 8` on the 6th (the gateway must refuse first) |
| `nonfatal-error` | OpenAI: an `error` event followed by normal `response.*` |
| `terminate-then-hang` | never answers `Terminate`/`closeStream` (drain wait bound) |

### 8.2 Tests

* **Unit** (`tests/unit/test_ws_*.py`): frame classifiers per product
  (table-driven from captures), `classify_close` table, `CloseCode`
  vocabulary vs `ERROR_CODES`, `credential_token` scheme set and subprotocol
  stripping, first-frame model rewrite + alias, config-prefix replay cap,
  byte-rate bucket, `Budgets.session_total`/`idle` validation,
  `check_drain_arithmetic` second inequality, `largest_total` exclusion,
  `account_usage` equivalence with `_account` on existing fixtures,
  metrics closed sets, `series_estimate`.
* **Contract** (`tests/contract/test_ws_*.py`, real sockets via
  `websockets.asyncio.client` against the fake and the gateway from
  `build_app`): `X-Gw-*` on the 101; pre-101 401/429/503/502 as HTTP with
  the existing body; handshake failure -> fallback to the second row with the
  config frame replayed and the fake counters proving the first row never
  saw content; post-commit failure -> no second upstream opened, close 4904
  with reason; Inworld bad-key silence -> `HeadersTimeout` after `headers`
  budget; OpenAI non-fatal `error` does not end the session; AssemblyAI 1008
  both ways; byte bounds (slow client -> 4903 within `client_stall`;
  inbound over-rate -> read pause, never a 3007 at the fake); tenant caps
  (`max_concurrency`, `max_sessions` counting a relayed session, C6 free
  denial); `max_streams` counts sockets (extends `test_overload.py`);
  session capture record fields and basis per product; drain: forwards
  `Terminate`/`closeStream`, waits for termination, client sees 4900 within
  `ws_drain_wait_s + 1`, `begin_drain` report `cut=0` (extends
  `test_lifecycle.py:234-620`); shutdown cut of a hung session is counted in
  `ShutdownCuts` and produces zero tracebacks (extends
  `test_shutdown_cuts_are_one_summary_line_and_no_tracebacks`); dual-stack
  upgrade through `bind_sockets`.
* **Chaos** (`tests/chaos/test_ws_invariants.py`): random mode per session,
  random client disconnects, 500 sessions, stderr on a PIPE that is never
  read (finding 41): invariants are `llmgw_ws_sessions_open` == 0,
  `permits_in_use` == 0, breaker tickets settled, task count baseline,
  process still answers `/healthz`, upstream fake `ws_open` == closed.

### 8.3 Load scenarios S9-S12 (`bench/scenarios`, `bench/load.py` gains a `ws` worker kind)

| Scenario | Shape | Measures | Pass |
|---|---|---|---|
| S9 "64 KB/s each direction" | 200 STT-shaped sessions at 43 KB/s in (b64 PCM) + 200 TTS-shaped at 64 KB/s out, 5 min, Arm D (direct to fake) vs Arm G | added frame latency p50/p99 per direction, CPU per session, RSS, bytes relayed == bytes sent (both arms) | p99 added latency <= 10 ms at 1 process; zero byte loss; CPU per session recorded for the `max_streams` re-derivation |
| S10 "thousands of idle sockets" | 2,000 Realtime-shaped sessions, `session.created` then silence, pings only, 10 min | RSS per socket, fds, task count, ping CPU | <= 100 KiB marginal RSS per idle socket; fds = 2 per session + baseline; no idle close before `idle` budget |
| S11 "mass disconnect" | 1,000 STT sessions streaming; all clients close within 1 s while the fake keeps sending; gateway stderr on a PIPE nobody reads | time to `sessions_open` == 0, upstream closes at the fake, stderr bytes, process liveness | all upstream sockets closed <= 5 s; stderr < 4 KiB total (no per-socket line); `/healthz` 200 throughout; no fd/task drift after 30 s |
| S12 "deploy under open sessions" | 500 STT + 100 TTS sessions; SIGTERM at 60 s (S8 driver, `bench/load.py:1626`) | client close codes and timing, `Terminate` count at the fake, capture records, exit time | 100% of clients see 4900 (STT) within `ws_drain_wait_s + 1`; TTS clients see 4900 after their contexts close; fake `terminates_received` == STT sessions; every session has a record with `seconds` and `basis`; process exits before grace + 3 s; `cut == 0` |

S2/S3/S5/S8 re-run after G1 because `pump.py` changed (the `_ByteBuffer`
move; PLAN-2.md:477-479) and after G5 because `begin_drain` changed.

### 8.4 Live smoke (`live/smoke_ws.py`, same style as `live/smoke_voice.py:295-330`)

With the keys that exist (Inworld, OpenAI): Inworld TTS one context, 19
characters LINEAR16, asserts `audioChunk` bytes through the gateway ==
direct, `X-Gw-*` on the 101, session record `characters=19`; Inworld TTS
two contexts interleaved on one socket; Inworld STT with a 3 s 16 kHz tone
WAV (`live/smoke_voice.py:274-293`), asserts a `transcription` frame and
`seconds` ~3 `estimated`; OpenAI Realtime text-only `response.create` ->
`response.done` (the 16 Sep probe shape, voice-openai.md:331-346), asserts
per-response record tokens exact; OpenAI transcription session with the
tone; a drain while an STT session is open (SIGTERM to the smoke's own
gateway process) asserting 4900 and the record. Spend: cents. AssemblyAI and
ElevenLabs: contract-only until keys exist; the smoke prints `ABSENT (cases
skip)` as today (`live/smoke_voice.py:314-316`).

---

## 9. Build order

### G0. Live probes (running now)

Deliverable: `capabilities/captures-ws.md` with items 1-9 of section 0.2.
Acceptance before G1 is accepted: items 1-6 present with byte sizes and
timings; every `[captures-ws]` row in this plan resolved to `[live]` or
marked "not observed" with the probe that tried.

### G1. Transport core + Inworld TTS bidirectional (8-10 days)

Deliverables:
* `websockets` dependency, lock, `lifecycle.py` `ws=` and `ws_*` settings.
* `llmgw/bytebuf.py` (move), `ws/client.py`, `ws/relay.py`, `ws/session.py`,
  `ws/errors.py`, `ws/routes.py`, `ws/surfaces/base.py`,
  `ws/surfaces/inworld_tts.py`; registration in `build_app`
  (app.py:3217-3300 loop gains `for s in WS_REGISTRY: WebSocketRoute(...)`
  and the `/workloads/{w}` twin).
* Extensions: `Budgets.session_total/idle`, `tts_session` profile,
  `largest_total` exclusion, `ws_drain_wait_s` + arithmetic, `AuthScheme
  "basic"`, `credential_token`, `SURFACES` widening, new metric families,
  `CaptureRecord.session_id`, `account_usage`, `enter_session`,
  `Gateway.ws_sessions` + `begin_drain` loop, `SurfaceLimits` byte rates.
* `fakes/ws.py` Inworld TTS modes; unit + contract + chaos tests of 8.2 for
  this product; `live/smoke_ws.py` Inworld TTS cases.
* Docs: `docs/19-websocket-plane.md`, CONTRACTS C23-C27, FAILURE-MODES rows
  32-35 (slow socket client; mass disconnect; session outlives deploy;
  1008 ambiguity), DEPLOY.md arithmetic line, fly.toml comment.

What G1 must contain for Layrs to point the LiveKit Inworld TTS plugin at
the gateway: the route `/tts/v1/voice:streamBidirectional` accepting
`Authorization: Basic <tenant token>`; the `create.modelId` rewrite with an
alias for whatever id Layrs sends (`inworld-tts-1.5-mini` today,
`harness/config.py:84`); <=5 contexts relayed 1:1 with per-context
commitment; `contextClosed`/`status` semantics that match tts.py:478-582 so
the plugin's state machine is undisturbed; the drain hook; the session
record with characters; `X-Gw-*` on the 101; sockets counted under
`LLMGW_MAX_STREAMS`; and, Layrs-side, `inworld.TTS(ws_url=os.environ.get("INWORLD_WS_URL", DEFAULT))`
in `harness/livekit_setup.py:134-140` and `framework/skeleton/session.py:92`
with `INWORLD_API_KEY` set to the `layrs` tenant token on the machines that
route through the gateway (the plugin sends it as `Basic`).

Contracts added: **C23** (a socket is a stream: admission, caps,
`max_sessions`, pre-101 refusals are HTTP with the existing body, `X-Gw-*`
on the 101 with Served-By meaning "opened toward" under accept-then-relay);
**C24** (commitment per context/response/session; pre-commit fallback
replays only the config prefix; content is never replayed); **C25**
(post-101 failure is a close code in 4900-4999 with reason `llmgw:<code>`;
provider closes pass through; no synthesised frame); **C26** (drain forwards
the provider's terminate and waits a bounded `ws_drain_wait_s`; clients see
4900; `session_total` may exceed the grace); **C27** (session seconds and
characters are exact only from a provider meter, otherwise estimated with
the derivation named in `cost_notes`).

Verifiable live: everything above against Inworld with the portal key.
Acceptance the coordinator checks: all tiers green (unit, contract, chaos);
`make lint`; `series_estimate` under budget; S2/S3/S5/S8 re-run within noise
of 18 Sep; `live/smoke_ws.py --only inworld-tts` passes with byte-identical
audio and a `characters` record; `test_lifecycle` extended tests pass; a
local end-to-end with the real plugin (`livekit-plugins-inworld==1.6.3`,
`ws_url` pointed at a local gateway with `INWORLD_API_KEY=<tenant token>`)
synthesises one utterance — this last check is the G1 exit gate, run by
hand and pasted into `VERIFICATION-G1.md`.

### G2. Inworld STT (3-4 days)

`ws/surfaces/inworld_stt.py`, catalog row `inworld.stt-1`, `stt_session`
profile, inbound-audio-is-progress rule, seconds-from-bytes meter, fake
modes, tests, smoke case, plugin end-to-end (`inworld.STT(base_url=...)`).
Contract added: **C28** (for STT the progress clock counts inbound audio; a
silent client is idle, not a provider stall; blame on a stall follows the
direction that stopped). Verifiable live. Acceptance: as G1 plus the
`/workloads/{w}` twin exercised by the STT plugin's prefix-preserving URL.

### G3. OpenAI Realtime (5-6 days): transcription session first, then full

`ws/surfaces/openai_realtime.py`, `realtime_session` profile,
accept-after-upstream-ready, subprotocol handling, `OpenAI-Beta` refusal,
`session.update` pin rewrite, non-fatal `error`, per-response records,
`rate_limits.updated` gauges. Contract added: **C29** (an in-band `error`
never ends a Realtime session; only a close does; the tenant pin applies to
relayed sessions as to mints). Verifiable live with the OpenAI key (text-only
full session, tone transcription session). Acceptance: smoke passes;
`test_mint.py`'s pin fixtures reused against the relayed `session.update`.

### G4. AssemblyAI streaming (3 days, contract-only)

`ws/surfaces/assemblyai_streaming.py`, `Begin`-gated accept, `Terminate` ->
`Termination` drain, 1008 disambiguation, binary inbound frames, exact
seconds from `Termination`. Before coding: download the
`livekit-plugins-assemblyai==1.6.3` sdist and diff its `_connect_ws` against
the main-branch code quoted in 0.1. Contract added: **C30** (AssemblyAI's
1008 is classified by the preceding `Error` frame; absent one it is an auth
failure scoped to the credential). Acceptance: contract suite green; a
`live/smoke_ws.py` case exists and skips without a key.

### G5. Load family, drain verification, deploy (5-7 days)

S9-S12 implemented and run (Arm D vs G, 1 and 4 gateway processes), the
`LLMGW_MAX_STREAMS` re-derivation with sockets in the mix on the Fly VM
size (DEPLOY.md:165-184 procedure), S8 both arms re-run, `fly.toml`
comments updated, deploy of `layrs-llmgw`, then the first Layrs TTS session
through `layrs-llmgw.internal` with the plugin's `ws_url` swapped, compared
against a direct session on TTFB/`characters_count`/`audio_duration`
(`harness/dsa/metrics_capture.py:59-124`). Acceptance: all four scenarios
PASS by 8.3; `bench/results/load-S9..S12-*.md` written by the harness; the
paired Layrs measurement shows <= 10 ms added TTFB p50; `VERIFICATION-G.md`.

Total: 24-30 days, inside PLAN-2's 4-6 weeks (PLAN-2.md:48).

---

## 10. Contracts to add (numbering continues at C23)

C23 A WebSocket session is a stream. C24 Commitment on a socket is per
context, response or session, and content is never replayed. C25 A failure
after the 101 is a close code, never a frame. C26 A drain forwards the
provider's own terminate, waits a bounded time, and closes clients with
4900. C27 Session units are exact only from a provider meter. C28 For STT,
inbound audio is progress. C29 A Realtime `error` event does not end the
session. C30 AssemblyAI's 1008 is read with its `Error` frame. Full text in
section 9 per phase; each names its enforcing tests.

---

## 11. Risks and open questions

| # | Risk / question | Settled by |
|---|---|---|
| R1 | Inworld's WS upgrade accepts `Authorization: Basic` (the plugin proves it) but does it accept `Bearer`? The gateway sends Basic regardless (`auth_scheme="basic"`), so this only matters for the HTTP surfaces' scheme switch | captures-ws #2 |
| R2 | Inworld TTS bad key after `create`: error frame or silence? Decides whether `HeadersTimeout` is the only signal | captures-ws #2 |
| R3 | Inworld `audioChunk` maximum frame size vs the 1 MiB bound and `ws_max_size` | captures-ws #1 |
| R4 | Inworld STT has no server usage frame (plugin computes it locally): seconds are `estimated` from bytes. If a frame exists the meter becomes exact | captures-ws #5 |
| R5 | Fly proxy WebSocket idle timeout on `.flycast`; whether `fly proxy`/flycast passes upgrades at all | G5: one session through `.flycast` idle for 10 min with 20 s pings; recommendation stays `.internal` |
| R6 | uvicorn 0.52 sansio + websockets 17 compatibility (the sansio impl is younger than the legacy one) | G1 contract tests on the locked versions; `test_lifecycle` extension; a pin to 15.x if 17 misbehaves |
| R7 | `bind_sockets` dual-stack + WebSocket upgrade | G1 contract test over `::` |
| R8 | permessage-deflate CPU on shared-cpu-1x vs ~25% bandwidth saving on base64 audio | S9 with deflate on/off; default off |
| R9 | The LiveKit Inworld TTS plugin cannot carry a path prefix or query, so per-workload routing for TTS is by tenant default + `create.modelId` only | accepted; documented in C23; a `[tenants.<id>].default_workload` is the follow-up if two TTS workloads per tenant are ever needed |
| R10 | Layrs still sends `inworld-tts-1.5-mini` (deprecated, voice-inworld.md:166); alias vs Layrs-side change | Layrs fix rides along (PLAN-2.md:482-486); alias with `cost_notes` as the fallback |
| R11 | Reconnect storm at drain: 500 plugins reconnecting within 1 s to the surviving machine; admission rate buckets may 429 them | S12 measures; tenant `burst` sized for it in `config/tenants.toml` |
| R12 | OpenAI transcription sessions carry no `response.done`; billing basis for `gpt-live-transcribe` | captures-ws #7; estimated from audio bytes otherwise |
| R13 | `websocket.http.response` denial extension: present in the sansio impl (uvicorn `websockets_sansio_impl.py:265,476`); if a future uvicorn drops it, pre-101 refusals degrade to close 1008 before accept (Starlette default) | unit test asserts the extension is in `scope["extensions"]` at startup |
| R14 | A client that never sends a `create` on an Inworld TTS socket holds an upstream socket idle | `idle` budget 600 s -> close 4906; the plugin's pool does the same at its own idle timeout |
| R15 | `max_sessions` now counts relayed sessions AND minted TTLs; a tenant using both may need a higher number | `config/tenants.example.toml:92` comment; `/workloads/{w}/probe` reports `live_sessions` |

---

### Critical files for implementation

- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/server/app.py (Gateway lifecycle hooks 1563-1702, Exchange 1710-1797, refusal order 2141-2216, cancel handling 2317-2378, route table 3217-3300)
- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/server/lifecycle.py (uvicorn config 194-209; ws= and ws_* settings; drain sequence)
- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/clocks.py (Budgets 240-296: session_total, idle; Deadline/StallClock reuse)
- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/server/config.py (check_drain_arithmetic 821-852, SurfaceLimits 97-127, TenantTable 198-380)
- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/pump.py (the _ByteBuffer 638-725 to move; clock/commitment vocabulary to mirror)
- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/upstream.py (build_headers 381-434, join_url 301; basic scheme)
- /Users/sanjay/PREP/Evo/llmgw/src/llmgw/catalog.py (AuthScheme 64, inworld row 397-408, voice rows 790-940; new inworld.stt-1)
- /Users/sanjay/PREP/Evo/llmgw/fakes/upstream.py and fakes/voice.py (fake style and mode selection to extend with fakes/ws.py)
- /private/tmp/claude-501/-Users-sanjay-PREP-Evo/40856d22-376b-4cd5-8fdf-c8174c004740/scratchpad/inworld-src/livekit_plugins_inworld-1.6.3/livekit/plugins/inworld/{tts.py,stt.py} (the consumer's exact wire behaviour)

# Voice sweep: OpenAI audio and Realtime vs llmgw (2026-09-16)

Docs read today at `developers.openai.com` (API reference now lives under
`/api/reference/resources/...`; the `/api/docs/guides/realtime-websocket` and
`realtime-sip` pages returned 404 to the fetcher, so those two rows cite search
snippets and are marked `[snippet]`). Repo: `/Users/sanjay/PREP/Evo/llmgw` @
`bc1d065`. Chat-completions audio is covered only for units; the chat path
itself is in `capabilities/openai.md`.

Doc refs: SPEECH https://developers.openai.com/api/reference/resources/audio/subresources/speech/methods/create ·
TRANS https://developers.openai.com/api/reference/resources/audio/subresources/transcriptions/methods/create ·
TRANSL https://developers.openai.com/api/reference/resources/audio/subresources/translations/methods/create ·
STT https://developers.openai.com/api/docs/guides/speech-to-text · TTS https://developers.openai.com/api/docs/guides/text-to-speech ·
RT https://developers.openai.com/api/docs/guides/realtime · RTC https://developers.openai.com/api/docs/guides/realtime-conversations ·
RTT https://developers.openai.com/api/docs/guides/realtime-transcription · VAD https://developers.openai.com/api/docs/guides/realtime-vad ·
WEBRTC https://developers.openai.com/api/docs/guides/voice-webrtc · WS https://developers.openai.com/api/docs/guides/realtime-websocket `[snippet]` ·
SIP https://developers.openai.com/api/docs/guides/realtime-sip `[snippet]` · XLATE https://developers.openai.com/api/docs/guides/realtime-translation ·
SEC https://developers.openai.com/api/reference/resources/realtime/subresources/client_secrets/methods/create ·
SEV https://developers.openai.com/api/docs/api-reference/realtime-server-events · CEV https://developers.openai.com/api/docs/api-reference/realtime-client-events ·
PRICE https://developers.openai.com/api/docs/pricing · M-RT https://developers.openai.com/api/docs/models/gpt-realtime ·
M-RTM https://developers.openai.com/api/docs/models/gpt-realtime-mini · M-TTS https://developers.openai.com/api/docs/models/gpt-4o-mini-tts ·
M-4OT https://developers.openai.com/api/docs/models/gpt-4o-transcribe · M-T https://developers.openai.com/api/docs/models/gpt-transcribe ·
M-LT https://developers.openai.com/api/docs/models/gpt-live-transcribe · RL https://developers.openai.com/api/docs/guides/rate-limits

llmgw facts every row leans on: routes are `/v1/chat/completions` and
`/anthropic/v1/messages` only, `/v1/responses` is 501 (`server/app.py:237-286`);
the request body is read up to `max_request_bytes` = 4 MiB (`app.py:1722`) and
`parse_request` calls `parse_json_object(body)` (`surfaces/openai.py:21`), so a
multipart body is an `InvalidRequest` before routing; `build_headers` forces
`content-type: application/json` and `accept: text/event-stream | application/json`
(`upstream.py:308-310`); the streaming pump feeds every byte to an `SSEParser`
(`pump.py:198,366`), and a non-SSE streamed body ends as `incomplete_stream`
with $0 accounted (the gzip note, `upstream.py:606-612`); accounting has four
token kinds, `input/output/cache_read/cache_write` (`accounting.py:97`), priced
by `ModelSpec.input_per_m/output_per_m/...` (`accounting.py:302-311`); the ASGI
app has no websocket route and `uv.lock` carries neither `websockets` nor
`wsproto` (only `h2`), so uvicorn would refuse a WS upgrade today.

Fit legend: **Proxyable today** (works or nearly, with the listed breakage) ·
**Needs surface** (HTTP in/out but a new `Surface` + header/body rules) ·
**Needs new transport** (WebSocket/WebRTC/SIP) · **Needs new accounting**
(non-token or new token kinds) · **Not a gateway concern**.

## 1. Products and endpoints

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Text to speech `POST /v1/audio/speech` | SPEECH, TTS | JSON in; **binary chunked audio out** (`audio/mpeg` etc.) or SSE when `stream_format: "sse"` | Needs surface. Binary mode: streamed body has no SSE frames → `incomplete_stream`, $0; buffered mode: binary body stored as if JSON | `pump.py:366`, `upstream.py:606-612`, `app.py:1934` | `content-type` IS forwarded (`app.py:310`), so the bytes would reach the client; the gateway would just misreport it. SSE mode is the proxyable shape |
| Speech to text `POST /v1/audio/transcriptions` | TRANS, STT | **multipart in** (file ≤ 25 MB); JSON out, or SSE `transcript.text.delta/done` when `stream: true` | Needs surface. Multipart → `parse_request` raises; 4 MiB cap vs 25 MB; upstream `content-type` forced to JSON | `openai.py:21`, `app.py:1722`, `upstream.py:308` | SSE-out half would parse today; the request half is the work |
| Translation `POST /v1/audio/translations` | TRANSL, STT | multipart in; JSON out; `whisper-1` only; no streaming | Needs surface (same multipart issue) | same | English-only output; low priority |
| Realtime session, WebSocket `wss://api.openai.com/v1/realtime?model=...` | WS `[snippet]`, RT | bidirectional JSON text frames; audio as base64 in events | **Needs new transport** | no `websocket` scope handling in `app.py`; no `websockets`/`wsproto` in `uv.lock` | Data plane; the natural thing for a server-side voice agent to proxy |
| Realtime session, WebRTC `POST /v1/realtime/calls` (SDP offer, `application/sdp` or multipart with session config) + data channel `oai-events` | WEBRTC | SDP over HTTP, then media + events peer-to-peer with OpenAI | **Not proxyable for media**; the SDP exchange is HTTP and could pass | — | Browser talks to OpenAI directly; a gateway only sees the setup |
| Realtime over SIP: project SIP URI, `realtime.call.incoming` webhook, `/v1/realtime/calls/{call_id}/{accept,reject,refer,hangup}` | SIP `[snippet]` | SIP media to OpenAI; control via REST + webhook; backend attaches over WS with `call_id` | Control plane: **proxyable REST** (JSON, small); media not a gateway concern | — | Telephony |
| Ephemeral client secrets `POST /v1/realtime/client_secrets` | SEC | JSON; `expires_after.seconds` 10–7200 (default 600); `session` config pinned server-side; returns `ek_...` | **Proxyable today as a buffered JSON call once a route exists — and arguably the one Realtime piece the gateway SHOULD own** | route table `app.py:237` | Per-tenant minting = admission + session config pinning + cost attribution before any audio flows. See "What llmgw would need" |
| Realtime transcription sessions (`session.type: "transcription"`, `gpt-live-transcribe`, `gpt-transcribe`; `v1/realtime/transcription_sessions` per model pages) | RTT, M-LT, M-T | WebSocket | Needs new transport | — | This is what LiveKit-style STT plugins use for streaming STT |
| Realtime translation `/v1/realtime/translations`, `gpt-realtime-translate` | XLATE | WebSocket / WebRTC; events `session.output_audio.delta`, `session.output_transcript.delta`, `session.input_transcript.delta`, `session.closed` | Needs new transport | — | New product; no pricing on the page |
| Chat completions audio (`modalities: ["text","audio"]`, `input_audio`, `gpt-audio*`) | PRICE, capabilities/openai.md §2 | JSON/SSE on the existing chat route | Passthrough today; **needs new accounting** (audio tokens at $32/$64 per 1M vs text) | `openai.py:196-204` reads only prompt/completion/cached | Covered in the chat sweep; units in §6 |
| Voice Agents SDK (Agents SDK realtime transport) | RT | Wraps WebRTC/WebSocket above | Same as the transports it wraps | — | Layrs uses LiveKit, not this |

## 2. Streaming shape

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS binary stream (default / `stream_format: "audio"`) | SPEECH, TTS | chunked transfer of encoded audio; `wav`/`pcm` recommended for lowest latency | Pump treats bytes as SSE → no events → `incomplete_stream`; progress clock never advances on audio bytes | `pump.py:366`, `sse.py` | Needs an opaque-bytes pump mode: progress = any byte, terminal = EOF, native ending = close |
| TTS SSE stream (`stream_format: "sse"`, gpt-4o-mini-tts only) | SPEECH | `data:` JSON events `speech.audio.delta` (base64 `audio`), `speech.audio.done` (`usage`) | Parses today; needs an `audio_speech` Surface: delta=CONTENT, done=TERMINAL, usage `input_tokens/output_tokens`, native ending = close without `done` | `surfaces/base.py:182-200` | Base64 audio frames vs `max_frame_bytes` 1 MiB: fine for typical chunking, verify |
| STT file stream (`stream: true`; `gpt-transcribe`, `gpt-4o-transcribe`, `-mini`, `-diarize`; not `whisper-1`) | TRANS, STT | SSE `transcript.text.delta` (`delta`, optional `logprobs`), `transcript.text.done` (`text`, `usage`) | Response half parses today with a new Surface; request half blocked by multipart + 4 MiB | `openai.py:21`, `app.py:1722` | One-way: whole file up, text down |
| Realtime WS event catalog (client: `session.update`, `input_audio_buffer.append/commit/clear`, `conversation.item.*`, `response.create/cancel`, `output_audio_buffer.clear`; server: `session.created/updated`, `input_audio_buffer.speech_started/stopped/timeout_triggered/dtmf_event_received`, `response.created/done`, `response.output_audio.delta`, `response.content_part.*`, `response.function_calls_arguments.delta`, `conversation.item.input_audio_transcription.delta/completed/failed/segment`, `mcp_list_tools.*`, `rate_limits.updated`, `error`) | CEV, SEV | bidirectional JSON; session ≤ 60 min (RTC); 32k context / 4,096 max output (M-RT) | Needs new transport; the event names map cleanly onto EventKind (audio delta = CONTENT, `response.done` = per-response TERMINAL + usage, `error` = failure, `rate_limits.updated` = META) | `surfaces/base.py:81` | "Commitment" becomes per-response, not per-connection; a session outlives every existing clock default |
| Interruptions: `speech_started` cancels, `conversation.item.truncate` trims unplayed audio | RTC | client event | Transport-level; nothing to classify | — | A proxy must forward client events unmodified and in order |
| Turn detection: `server_vad` (`threshold`, `prefix_padding_ms`, `silence_duration_ms`, `create_response`, `interrupt_response`), `semantic_vad` (`eagerness: low|medium|high|auto`), `null` for manual `commit` | VAD | session config | Passthrough content of `session.update` | — | Could be pinned per tenant via client-secret session config |
| WebRTC media | WEBRTC | RTP between browser and OpenAI | Not proxyable | — | — |
| Keepalives / close | WS `[snippet]` | JSON frames; `session.closed` on translation sessions | Needs transport; liveness = any frame, progress = audio/text deltas | `clocks.py` heartbeat vs progress split transfers unchanged | — |

## 3. Latency knobs and metrics

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS: `wav`/`pcm` fastest; `tts-1` lower latency than `gpt-4o-mini-tts`; `speed` 0.25–4.0 | TTS, SPEECH | request params | Passthrough once a surface exists | — | TTFB on a binary stream = first audio byte; the gateway's `time_to_first_event` would need the opaque pump to emit it |
| STT: `gpt-transcribe` recommended; `gpt-live-transcribe` "tunable latency" via `delay: minimal|low|medium|high|xhigh`, `keywords`, `languages[]`, `prompt` | STT, RTT | session/request params | Passthrough | — | `delay` is the one knob that trades accuracy for latency |
| Realtime: `gpt-realtime-mini` vs `gpt-realtime`; VAD `silence_duration_ms` / `eagerness` govern end-of-turn latency | VAD, M-RTM | session config | Passthrough | — | End-of-turn latency dominates perceived latency, not model TTFT |
| What OpenAI exposes: `rate_limits.updated` (per session), `openai-processing-ms` on HTTP audio endpoints | SEV, capabilities/openai.md §4 | event / header | Header is dropped today (`app.py:310`); event would be META | — | Per-response latency must be measured by the proxy itself (`response.created` → first `output_audio.delta`) |
| Rate limits: RPM/TPM per tier; "audio minutes per minute for some streaming audio models" | RL, M-* | tier tables | Not consumed | — | gpt-realtime T1 200 RPM / 40k TPM; gpt-4o-mini-tts T1 500 / 50k; gpt-4o-transcribe T1 500 / 10k; gpt-live-transcribe T1 500 / 60k |

## 4. Auth and headers

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Standard bearer on all HTTP audio endpoints | SPEECH, TRANS | header | Supported by `build_headers` | `upstream.py:315` | — |
| Realtime WS server-side: `Authorization: Bearer <key>`, `OpenAI-Safety-Identifier: <hashed user>`; **no `OpenAI-Beta: realtime=v1` on GA** | WS `[snippet]`, RT | upgrade headers | Needs transport; `OpenAI-Safety-Identifier` is the natural place for the gateway's tenant id | `config.py:104` forward list has no such header | Same idea as `safety_identifier` on chat (chat sweep) |
| Realtime WS browser auth via subprotocols (ephemeral key, organization, project) | WS `[snippet]` | `Sec-WebSocket-Protocol` | Needs transport; a proxy would terminate the client's subprotocol auth and inject its own bearer | — | — |
| Ephemeral keys `ek_...`: minted server-side with a standard key, TTL 10–7200 s, one secret → many sessions, session config pinned | SEC | JSON | Proxyable as a buffered route; **should be gateway-owned** | — | Pinned config = the gateway decides model, voice, tools, VAD, `max_output_tokens` per tenant, and the client cannot widen it |
| WebRTC: `Authorization: Bearer ek_...`, `Content-Type: application/sdp` on `/v1/realtime/calls` | WEBRTC | HTTP | SDP body is not JSON → `parse_request` would reject; skip | `openai.py:21` | Browser should call OpenAI directly with the gateway-minted `ek_` |
| SIP calls: standard key on `/v1/realtime/calls/{id}/*` | SIP `[snippet]` | JSON | Proxyable buffered routes | — | Webhook receiver is not a proxy concern |

## 5. Errors, rate limits, concurrency

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| HTTP audio endpoints: same error body and status taxonomy as chat (400/401/429/5xx) | capabilities/openai.md §5 | HTTP | Classification transfers unchanged; the 429-billing-code gap from the chat sweep applies here too | `errors.py:890-915` | — |
| Realtime `error` event: `type`, `code`, `message`, `param`, `event_id` (echoes the client event that caused it) | SEV, RTC | in-band JSON | Needs transport; maps to `InStreamError`-shaped handling but is **non-fatal** (session continues); today's in-stream error ends the stream | `openai.py:224-243` | Must not be treated as commitment failure |
| `rate_limits.updated`: `rate_limits[]` of `{name: requests|tokens, limit, remaining, reset_seconds}` | SEV | in-band | META today; is the Realtime equivalent of `x-ratelimit-*` (which the gateway drops) | — | Per-credential gauges would come for free here |
| Session limits: 60 min max; 32k context / 4,096 output per response (`gpt-realtime`, `-mini`) | RTC, M-RT, M-RTM | — | `budgets.total` 120 s default would kill a session at 2 min; per-response clocks fit, per-session ones do not | `config.py` budgets | A voice surface needs `session_total` separate from `response_total` |
| Concurrency: no documented concurrent-session cap on the fetched pages; RPM/TPM tiers only; "audio minutes per minute" limits exist for some streaming audio models | RL, M-* | tier | Provider-key limiter counts connections — a long WS is one connection for an hour, so the cap semantics actually fit better than for chat | `admission.py`, FAILURE-MODES row 7 | — |
| TTS/STT retry safety | SPEECH, TRANS | — | Pre-commit retry rules transfer: TTS SSE commits on first `speech.audio.delta`; binary TTS commits on first byte; STT commits on first `transcript.text.delta` | CONTRACTS C1/C2 | Multipart bodies must be buffered to retry — 25 MB per attempt in RAM |

## 6. Usage and billing units

| capability | OpenAI (doc) | unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Realtime `response.done.usage`: `input_tokens`, `output_tokens`, `total_tokens`, `input_token_details{audio_tokens,text_tokens,cached_audio_tokens,cached_text_tokens}`, `output_token_details` | SEV | tokens, per response | **Needs new accounting**: audio vs text at different rates (`gpt-realtime`: text $4/$0.40/$16, audio $32/$0.40/$64, image $5/$0.50 per 1M; `-mini`: text $0.60/$0.06/$2.40, audio per PRICE) | `accounting.py:97` four kinds; `catalog.py:110-121` single rate set | Needs `audio_input`, `audio_output`, `cached_audio` kinds or a per-modality rate table on `ModelSpec` |
| Chat-completions audio: `gpt-audio` / `gpt-audio-1.5` audio $32 in / $64 out; `gpt-audio-mini` $10 / $20 per 1M audio tokens | PRICE | tokens | Same new kinds; the fields already arrive in `prompt_tokens_details.audio_tokens` / `completion_tokens_details.audio_tokens` and are unread | `openai.py:196-204` | Today audio tokens are priced at the text rate → under-billed ~50x on `gpt-audio` |
| TTS `gpt-4o-mini-tts`: $0.60 per 1M text input tokens + $12 per 1M audio output tokens; input ≤ 2,000 tokens / 4,096 chars; usage in `speech.audio.done` (SSE only) | M-TTS, SPEECH | tokens (SSE) / **nothing** (binary mode) | SSE mode fits the existing four kinds exactly (`input_per_m=0.60, output_per_m=12`); binary mode has no usage object → estimate from characters or `content-length` | `accounting.py:302-311` | `tts-1` $15 / `tts-1-hd` $30 per 1M characters (PRICE): a characters unit the catalog lacks |
| STT tokens: `gpt-4o-transcribe` $2.50 per 1M audio in / $10 per 1M text out (`-mini` $1.25 / $5); usage `{type: "tokens", input_tokens, output_tokens, input_token_details}` | M-4OT, TRANS | tokens | Fits existing kinds with an audio-input rate | — | — |
| STT duration: `whisper-1` $0.006/min, `gpt-transcribe` $0.0045/min, `gpt-4o-transcribe` "$0.006/min" estimate, `gpt-live-transcribe` $0.017/min; usage `{type: "duration", seconds}` | PRICE, M-T, M-LT, TRANS | **seconds** | **Needs new accounting** (a `seconds` unit and `per_minute` rate) | `ModelSpec` has no time unit | Layrs' own `pricing.py` already prices STT per second and TTS per minute (see §7) |
| Cached audio tokens $0.40 per 1M on `gpt-realtime` | M-RT | tokens | Needs the `cached_audio` split | — | `prompt_caching` listed as a supported feature of gpt-realtime |
| Realtime translation / SIP pricing | XLATE, SIP | not on fetched pages | Unknown | — | — |

## 7. Layrs usage today

Read-only from `/Users/sanjay/Layrs/voice-agent` (no key values touched).

| item | where | what |
|---|---|---|
| Production STT/TTS | `harness/config.py:83-91`, `requirements.txt:5-12` | **Inworld** for both (`inworld-tts-1.5-mini` voice `Aarav`; `inworld/inworld-stt-1` realtime). `livekit-plugins-inworld==1.6.3` pinned |
| OpenAI TTS fallback | `harness/livekit_setup.py:104-110`, `config.py:86-87` | `openai.TTS(model="gpt-4o-mini-tts", voice="cedar", instructions=_TTS_INSTRUCTIONS)` via `livekit-plugins-openai==1.6.3` → `POST /v1/audio/speech` (plugin transport not verified locally: no venv on this machine; LiveKit's plugin streams the binary response, i.e. the non-SSE shape) |
| OpenAI STT fallback | `framework/skeleton/session.py:135-136` (skeleton), harness falls back to **AssemblyAI** not OpenAI (`livekit_setup.py:227-233`) | skeleton uses `openai.STT(model="gpt-4o-transcribe")`; LiveKit's OpenAI STT plugin uses the **Realtime transcription WebSocket** for streaming (verify against the pinned plugin; if so, a gateway would need the WS transport to carry it) |
| LLM in voice sessions | `livekit_setup.py:34-78`, `evals/llm_factory.py:223,258` | `openai.LLM(...)` / `openai.LLM.with_deepseek(...)` / `anthropic.LLM` → chat completions; these can point at llmgw today via `base_url` (the chat sweep's `model`-echo gap applies) |
| Pricing table | `harness/evals/pricing.py:134-153` | `openai.gpt-4o-mini-tts` at `$0.015 / min audio (legacy) (2026-06-03)`; STT per second (Inworld $0.10/h, AssemblyAI $0.15/h) — Layrs already bills voice in **minutes/seconds**, the unit llmgw lacks |
| Turn detection | `livekit_setup.py:205-210`, `:296` | Silero VAD + Inworld end-of-turn thresholds; OpenAI VAD unused |
| Realtime API | grep | not used anywhere |

## What llmgw has and lacks for this provider

Transfers unchanged: provider credential injection and header allowlists;
per-tenant admission and the provider-key connection cap (a WS session is one
connection, which is the right unit); breaker per provider+model+credential
(open on failed session/handshake, `error` events NEUTRAL); the
liveness-vs-progress clock split (`ping`/keepalive vs audio/text delta); the
commitment rule for the HTTP shapes (first `speech.audio.delta`, first
`transcript.text.delta`, first audio byte); the SSE parser for TTS-SSE and
STT-stream responses; byte-bounded pumps; the catalog/price-freshness
discipline; capture and metrics contracts.

Lacks, in build order:

1. **Buffered JSON routes for `POST /v1/realtime/client_secrets` and
   `/v1/realtime/calls/{id}/{accept,reject,refer,hangup}`** — smallest change,
   biggest leverage. Minting per tenant lets the gateway pin `session` (model,
   voice, tools, VAD, `max_output_tokens`, `OpenAI-Safety-Identifier` = tenant),
   apply admission and record who opened a session, while the browser then
   talks WebRTC to OpenAI directly (media never transits the gateway). Needs a
   `realtime_control` Surface whose `parse_request` reads `session.model`, a
   catalog row per realtime model, and a capture record for the mint.
2. **Multipart request handling**: `read_request_body` already buffers; add a
   per-surface body cap (25 MB for `/v1/audio/*`, 32 MB for Anthropic per the
   chat sweep), skip `parse_json_object` for multipart, forward the client's
   `content-type` boundary upstream instead of forcing JSON
   (`upstream.py:308`), read `model`/`stream` from form fields.
3. **`audio_transcription` Surface**: SSE events `transcript.text.delta/done`,
   usage of either type; buffered JSON otherwise. Plus `audio_speech` Surface
   for `stream_format: "sse"`.
4. **Opaque-bytes pump mode** for binary TTS (and any non-SSE stream):
   progress on every chunk, TERMINAL on EOF, native ending = close, TTFB =
   first byte; no usage → estimate from input characters × catalog rate.
5. **Accounting units**: `audio_input`, `audio_output`, `cached_audio` token
   kinds with per-kind rates on `ModelSpec`; a `seconds` unit with
   `per_minute` for duration-billed STT; a `characters` unit for `tts-1*`.
   Wire `prompt_tokens_details.audio_tokens` /
   `completion_tokens_details.audio_tokens` on the chat surface at the same
   time (today priced as text).
6. **WebSocket transport** (last, largest): `websockets` or `wsproto` in the
   lock; an ASGI `websocket` scope handler that authenticates the tenant,
   opens an upstream WS with the provider bearer, relays frames in order both
   ways, classifies server events (deltas = progress, `response.done` = usage,
   `error` = non-fatal, `rate_limits.updated` = gauges), and drains by
   forwarding a close instead of cutting. Clocks split into per-session total
   (≤ 60 min) and per-response first-event/progress. Same transport then
   carries Realtime transcription and translation sessions.

## Gaps ranked (impact on the Layrs voice agent)

1. **Nothing voice can go through the gateway today** — every audio endpoint
   is a 404 (`/v1/audio/*`) or unroutable (WS). Layrs' OpenAI TTS/STT
   fallbacks bypass llmgw entirely, so fallback traffic is unmetered and uses
   the raw key.
2. **Ephemeral-key minting is unowned.** If Layrs ever moves to browser
   WebRTC, the server that mints `ek_` keys IS the policy point; llmgw has
   every primitive for it (tenant, admission, catalog, capture) and no route.
3. **Audio tokens on the chat route are billed as text.** `gpt-audio` audio is
   $32/$64 per 1M vs the text rate; the detail fields arrive and are ignored.
   Only matters once a chat-audio caller exists, but it is silent under-billing.
4. **No duration or character units.** All of OpenAI's transcription and
   `tts-1*` pricing is per minute / per character; Layrs' own `pricing.py`
   already bills voice that way. The catalog cannot express Layrs' current
   voice costs at all.
5. **Binary TTS is the shape the LiveKit plugin uses, and it is the one that
   would silently fail** (`incomplete_stream`, $0) if someone pointed the
   plugin's `base_url` at the gateway. A clear 501 for unknown content types
   would be better than today's behaviour until the opaque pump exists.
6. **Session-scale clocks.** A Realtime session is an hour; `budgets.total`
   defaults to 120 s and the drain grace to 130 s. Voice needs a session total
   and a per-response budget, and a drain that forwards a close rather than
   cutting a live call.
7. **`OpenAI-Safety-Identifier`** is the per-user attribution header on both
   WS and WebRTC; the gateway has a tenant id and no place to put it (same gap
   as `safety_identifier` on chat).
8. **The chat sweep's 429-billing and `model`-echo gaps apply verbatim** to
   any voice-agent LLM traffic routed through the gateway; the LiveKit
   `openai.LLM` path re-sends the model it received.

## Doc deltas (Layrs and repo vs docs today)

- `harness/evals/pricing.py:134,143` prices `gpt-4o-mini-tts` at $0.015/min
  "(legacy)"; the model page bills $0.60 per 1M text input tokens + $12 per 1M
  audio output tokens (M-TTS, no per-minute figure); `tts-1`/`tts-1-hd` remain
  per-character ($15/$30 per 1M) (PRICE).
- `gpt-4o-transcribe` / `gpt-4o-mini-transcribe` are now listed as **legacy**;
  `gpt-transcribe` ($0.0045/min) is the recommended file model and
  `gpt-live-transcribe` ($0.017/min) the realtime one (STT, M-T, M-LT). The
  skeleton's `openai.STT(model="gpt-4o-transcribe")` should move.
- `whisper-1` is the only translation model and the only one with
  `timestamp_granularities[]`; it does not support `stream: true` (STT).
- Realtime GA: remove `OpenAI-Beta: realtime=v1` (RT migration note); guide
  examples use `gpt-realtime-2.1`, the model page's default snapshot is
  `gpt-realtime-2025-08-28`, and the client-secrets reference lists
  `gpt-realtime-2` and `gpt-4o-realtime-preview` — three ids for one family;
  pin by snapshot in any catalog row. Sessions max 60 min; 32k context /
  4,096 output per response (RTC, M-RT).
- New `OpenAI-Safety-Identifier` header on Realtime connections (WS, WEBRTC).
- New products with no pricing on their pages: Realtime translation
  (`gpt-realtime-translate`, `/v1/realtime/translations`) and GPT-Live
  ("listen while speaking") (XLATE, audio overview).
- `gpt-4o-mini-tts` default snapshot is `gpt-4o-mini-tts-2025-12-15`; voices
  now include `marin` and `cedar` (Layrs already uses `cedar`) (M-TTS, TTS).
- Rate-limit docs mention "audio minutes per minute" as a limit unit for some
  streaming audio models; no tier numbers are published for it (RL).
- Repo comment `upstream.py:606-612` describes exactly the failure a binary
  audio response would produce today (streamed: `incomplete_stream`, $0;
  buffered: success with a binary body); it was written about gzip and is
  equally true of `audio/*` bodies.

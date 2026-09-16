# Voice sweep: OpenAI audio and Realtime vs llmgw (2026-09-16)

Docs read today at `developers.openai.com` (API reference now lives under
`/api/reference/resources/...`). The `/api/docs/guides/realtime-websocket` and
`realtime-sip` URLs are 404 (and `platform.openai.com` 301s to them); the live
pages are `voice-websockets?api=realtime` and `voice-sip?api=realtime`, read on
the second pass. Rows marked `[live]` were confirmed against the API on 16 Sep
with minimal probes (about $0.005 total; scripts in the session scratchpad,
observed shapes in the appendix). Repo: `/Users/sanjay/PREP/Evo/llmgw` @
`27275a8`. Chat-completions audio is covered only for units; the chat path
itself is in `capabilities/openai.md`.

Doc refs: SPEECH https://developers.openai.com/api/reference/resources/audio/subresources/speech/methods/create ·
TRANS https://developers.openai.com/api/reference/resources/audio/subresources/transcriptions/methods/create ·
TRANSL https://developers.openai.com/api/reference/resources/audio/subresources/translations/methods/create ·
STT https://developers.openai.com/api/docs/guides/speech-to-text · TTS https://developers.openai.com/api/docs/guides/text-to-speech ·
RT https://developers.openai.com/api/docs/guides/realtime · RTC https://developers.openai.com/api/docs/guides/realtime-conversations ·
RTT https://developers.openai.com/api/docs/guides/realtime-transcription · VAD https://developers.openai.com/api/docs/guides/realtime-vad ·
WEBRTC https://developers.openai.com/api/docs/guides/voice-webrtc · WS https://developers.openai.com/api/docs/guides/voice-websockets?api=realtime ·
SIP https://developers.openai.com/api/docs/guides/voice-sip?api=realtime · XLATE https://developers.openai.com/api/docs/guides/realtime-translation ·
SEC https://developers.openai.com/api/reference/resources/realtime/subresources/client_secrets/methods/create ·
CALLS https://developers.openai.com/api/reference/resources/realtime/subresources/calls/methods/{create,accept,refer,hangup} ·
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
| Text to speech `POST /v1/audio/speech` `[live]` | SPEECH, TTS | JSON in; **binary chunked audio out** (`audio/pcm`, `audio/mpeg`; HTTP/2 chunks ≤ 8,192 B) or SSE when `stream_format: "sse"` (`text/event-stream`, CRLF terminators, 92 frames / max 2,600 B for 26 chars) | Needs surface. Binary mode: no SSE frames → `incomplete_stream`, $0, and **no usage anywhere** (headers included); SSE mode: `sse.py` parses it (CRLF handled), but `OpenAIChatSurface.classify` returns HEARTBEAT for every `speech.audio.delta` and META for `speech.audio.done`, so no progress, no commitment as content, usage never applied | `pump.py:366`, `upstream.py:606-612`, `app.py:1934`; probe 16 Sep | `content-type` IS forwarded (`app.py:310`), so the bytes would reach the client; the gateway would just misreport it. Response headers carry `x-request-id`, `openai-processing-ms` (365–793 ms observed), `x-ratelimit-*-tokens` |
| Speech to text `POST /v1/audio/transcriptions` `[live]` | TRANS, STT | **multipart in** (file ≤ 25 MB); JSON out (`text`, `languages[]`, `usage`), or SSE `transcript.text.delta/done` + `data: [DONE]` when `stream: true` (`gpt-transcribe` confirmed; CRLF terminators) | Needs surface. Multipart → `parse_request` raises; 4 MiB cap vs 25 MB; upstream `content-type` forced to JSON. SSE half parses in `sse.py`; chat surface classifies deltas HEARTBEAT, `done` META (usage dropped) | `openai.py:21`, `app.py:1722`, `upstream.py:308`; probe 16 Sep | `usage` on `gpt-transcribe` and `whisper-1` is `{type: "duration", seconds}` and **rounds up to whole seconds** (2.44 s of audio → 3); no `x-ratelimit-*-tokens` on this endpoint, only `-requests` |
| Translation `POST /v1/audio/translations` | TRANSL, STT | multipart in; JSON out; `whisper-1` only; no streaming | Needs surface (same multipart issue) | same | English-only output; low priority |
| Realtime session, WebSocket `wss://api.openai.com/v1/realtime?model=...` `[live]` | WS, RT | bidirectional JSON text frames; audio as base64 in events; `permessage-deflate` negotiated | **Needs new transport** | no `websocket` scope handling in `app.py`; no `websockets`/`wsproto` in `uv.lock` | Data plane. Observed: handshake 1.66 s from `sin`, `session.created` as the first frame, text-only `response.create` → `response.done` in about 700 ms. A separate Live API exists at `wss://api.openai.com/v1/live/sessions` (`session.start` / `session.started`, no `response.create`) |
| Realtime session, WebRTC `POST /v1/realtime/calls` (SDP offer, `application/sdp` or multipart with session config) + data channel `oai-events` | WEBRTC | SDP over HTTP, then media + events peer-to-peer with OpenAI | **Not proxyable for media**; the SDP exchange is HTTP and could pass | — | Browser talks to OpenAI directly; a gateway only sees the setup |
| Realtime over SIP: `sip:$PROJECT_ID@sip.api.openai.com;transport=tls` (or `sip-eu.`), `realtime.call.incoming` webhook (`call_id`, `sip_headers[]`), `POST /v1/realtime/calls/{call_id}/accept` (session config body), `/reject` (`{status_code: 486}` optional), `/refer` (`{target_uri}`), `/hangup` (empty); backend attaches at `wss://api.openai.com/v1/realtime?call_id=...` | SIP, CALLS | SIP media to OpenAI; control via REST + webhook | Control plane: **proxyable REST** (JSON, small); media not a gateway concern | — | Telephony; all four control calls take the standard bearer |
| Ephemeral client secrets `POST /v1/realtime/client_secrets` `[live]` | SEC | JSON; `expires_after.{anchor, seconds}` 10–7200 (default 600); `session` (`type: realtime | transcription`) pinned server-side; returns `{value: "ek_…" (35 chars), expires_at, session}` with the full effective session echoed (defaults observed: `server_vad` threshold 0.5 / 300 ms / 200 ms, `audio/pcm` 24 kHz in and out, `max_output_tokens: "inf"`, OpenAI's default `instructions` text, `tool_choice: auto`) | **Proxyable today as a buffered JSON call once a route exists — and arguably the one Realtime piece the gateway SHOULD own** | route table `app.py:237`; probe 16 Sep | Per-tenant minting = admission + session config pinning + cost attribution before any audio flows. The echoed session shows exactly what the client cannot widen. See "What llmgw would need" |
| Realtime transcription sessions (`session.type: "transcription"`, `gpt-live-transcribe`, `gpt-transcribe`; `v1/realtime/transcription_sessions` per model pages) | RTT, M-LT, M-T | WebSocket | Needs new transport | — | This is what LiveKit-style STT plugins use for streaming STT |
| Realtime translation `/v1/realtime/translations`, `gpt-realtime-translate` | XLATE | WebSocket / WebRTC; events `session.output_audio.delta`, `session.output_transcript.delta`, `session.input_transcript.delta`, `session.closed` | Needs new transport | — | New product; no pricing on the page |
| Chat completions audio (`modalities: ["text","audio"]`, `input_audio`, `gpt-audio*`) | PRICE, capabilities/openai.md §2 | JSON/SSE on the existing chat route | Passthrough today; **needs new accounting** (audio tokens at $32/$64 per 1M vs text) | `openai.py:196-204` reads only prompt/completion/cached | Covered in the chat sweep; units in §6 |
| Voice Agents SDK (Agents SDK realtime transport) | RT | Wraps WebRTC/WebSocket above | Same as the transports it wraps | — | Layrs uses LiveKit, not this |

## 2. Streaming shape

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS binary stream (default / `stream_format: "audio"`) | SPEECH, TTS | chunked transfer of encoded audio; `wav`/`pcm` recommended for lowest latency | Pump treats bytes as SSE → no events → `incomplete_stream`; progress clock never advances on audio bytes | `pump.py:366`, `sse.py` | Needs an opaque-bytes pump mode: progress = any byte, terminal = EOF, native ending = close |
| TTS SSE stream (`stream_format: "sse"`, not on `tts-1*`) `[live]` | SPEECH | `data:` JSON events, **CRLF-terminated** (`\r\n\r\n`; chat uses `\n\n`): `speech.audio.delta {type, audio}` × 90, `speech.audio.done {type, usage: {input_tokens, output_tokens, total_tokens}}`, then `data: [DONE]` | `sse.py` parses it (CRLF is handled, verified by feeding the captured bytes in 7-byte chunks: 92 events). Needs an `audio_speech` Surface: delta=CONTENT, done=usage, `[DONE]`=TERMINAL, native ending = close without `[DONE]`. Through the chat surface today: deltas HEARTBEAT, done META, `[DONE]` TERMINAL | `surfaces/base.py:182-200`; probe 16 Sep | Max frame 2,600 B for a 26-char input (about 1.95 KB of base64 per delta); the 1 MiB bound is not a concern. Usage has no `type` field; 6 in / 72 out for 26 chars, so about 2.8 audio tokens per character |
| STT file stream (`stream: true`; `gpt-transcribe` confirmed; per docs also `gpt-4o-transcribe`, `-mini`, `-diarize`; not `whisper-1`) `[live]` | TRANS, STT | SSE, CRLF-terminated: `transcript.text.delta {type, delta[, logprobs[]]}` per token, `transcript.text.done {type, text, usage: {type: "duration", seconds}, languages: [{code}][, logprobs[]]}`, then `data: [DONE]` | Response half parses today (8 events, max 135 B; 592 B with `include[]=logprobs`); needs an `audio_transcription` Surface; request half blocked by multipart + 4 MiB | `openai.py:21`, `app.py:1722`; probe 16 Sep | One-way: whole file up, text down. Whole stream took 877 ms for a 2.4 s clip, first frame at 835 ms |
| Realtime WS event catalog (client: `session.update`, `input_audio_buffer.append/commit/clear`, `conversation.item.*`, `response.create/cancel`, `output_audio_buffer.clear`; server: `session.created/updated`, `input_audio_buffer.speech_started/stopped/timeout_triggered/dtmf_event_received`, `response.created/done`, `response.output_audio.delta`, `response.content_part.*`, `response.function_calls_arguments.delta`, `conversation.item.input_audio_transcription.delta/completed/failed/segment`, `mcp_list_tools.*`, `rate_limits.updated`, `error`) | CEV, SEV | bidirectional JSON; session ≤ 60 min (RTC); 32k context / 4,096 max output (M-RT) | Needs new transport; the event names map cleanly onto EventKind (audio delta = CONTENT, `response.done` = per-response TERMINAL + usage, `error` = failure, `rate_limits.updated` = META) | `surfaces/base.py:81` | "Commitment" becomes per-response, not per-connection; a session outlives every existing clock default |
| Interruptions: `speech_started` cancels, `conversation.item.truncate` trims unplayed audio | RTC | client event | Transport-level; nothing to classify | — | A proxy must forward client events unmodified and in order |
| Turn detection: `server_vad` (`threshold`, `prefix_padding_ms`, `silence_duration_ms`, `create_response`, `interrupt_response`), `semantic_vad` (`eagerness: low|medium|high|auto`), `null` for manual `commit` | VAD | session config | Passthrough content of `session.update` | — | Could be pinned per tenant via client-secret session config |
| WebRTC media | WEBRTC | RTP between browser and OpenAI | Not proxyable | — | — |
| Keepalives / close | WS | No ping/keepalive documented; graceful close is client `session.close` → server `session.closed` (Live and translation sessions); observed close code 1000 on a plain client close, 4000 with reason `invalid_request_error.beta_api_shape_disabled` when the server rejects the session | Needs transport; liveness = any frame, progress = audio/text deltas | `clocks.py` heartbeat vs progress split transfers unchanged | — |

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
| Realtime WS server-side: `Authorization: Bearer <key>`, optional `OpenAI-Safety-Identifier`; **`OpenAI-Beta: realtime=v1` is now rejected**, not ignored `[live]` | WS, RT | upgrade headers | Needs transport; `OpenAI-Safety-Identifier` is the natural place for the gateway's tenant id | `config.py:104` forward list has no such header | Sending the beta header still completes the upgrade, then the first frame is `error {code: "beta_api_shape_disabled", message: "The Realtime Beta API is no longer supported. Please use /v1/realtime for the GA API."}` and the socket closes 4000. A relay must strip it |
| Realtime WS browser auth via subprotocols: `realtime`, `openai-insecure-api-key.<ephemeral key>`, optional `openai-organization.<org>`, `openai-project.<project>` | WS | `Sec-WebSocket-Protocol` | Needs transport; a proxy would terminate the client's subprotocol auth and inject its own bearer | — | — |
| Ephemeral keys `ek_...`: minted server-side with a standard key, TTL 10–7200 s, one secret → many sessions, session config pinned | SEC | JSON | Proxyable as a buffered route; **should be gateway-owned** | — | Pinned config = the gateway decides model, voice, tools, VAD, `max_output_tokens` per tenant, and the client cannot widen it |
| WebRTC: `Authorization: Bearer ek_...`, `Content-Type: application/sdp` on `/v1/realtime/calls` | WEBRTC | HTTP | SDP body is not JSON → `parse_request` would reject; skip | `openai.py:21` | Browser should call OpenAI directly with the gateway-minted `ek_` |
| SIP calls: standard key on `/v1/realtime/calls/{id}/{accept,reject,refer,hangup}` | SIP, CALLS | JSON | Proxyable buffered routes | — | Webhook receiver is not a proxy concern. WebRTC `POST /v1/realtime/calls` is multipart (`sdp` as `application/sdp` + `session` as JSON), not JSON |

## 5. Errors, rate limits, concurrency

| capability | OpenAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| HTTP audio endpoints: same error body and status taxonomy as chat `[live]` | capabilities/openai.md §5 | HTTP | Classification transfers unchanged; the 429-billing-code gap from the chat sweep applies here too | `errors.py:890-915`; probe 16 Sep | Observed: unknown `model` on transcriptions is **404** `model_not_found` (not the chat-style 400), so `errors.py`'s 404 rule fits; a non-audio upload is 400 `{type: invalid_request_error, code: unsupported_value, param: "file"}`; a bad key is 401 with **`content-type: text/plain`** carrying a JSON body whose message echoes the key as `sk-proj-***************1234` (last four characters in clear) plus `x-openai-authorization-error` headers, so the C11 scrub matters here too |
| Realtime `error` event: `type`, `code`, `message`, `param`, `event_id` (echoes the client event that caused it) `[live]` | SEV, RTC; probe 16 Sep | in-band JSON `{type: "error", event_id, error: {type, code, message, param, event_id}}` | Needs transport; maps to `InStreamError`-shaped handling but is usually **non-fatal** (session continues); today's in-stream error ends the stream | `openai.py:224-243` | Must not be treated as commitment failure by default; the one fatal case observed (`beta_api_shape_disabled`) is followed by a server close with code 4000 and the error's `type.code` as the close reason, so "fatal" is signalled by the close, not the event |
| `rate_limits.updated`: `rate_limits[]` of `{name: requests|tokens, limit, remaining, reset_seconds}` | SEV | in-band | META today; is the Realtime equivalent of `x-ratelimit-*` (which the gateway drops) | — | Per-credential gauges would come for free here |
| Session limits: 60 min max; 32k context / 4,096 output per response (`gpt-realtime`, `-mini`) | RTC, M-RT, M-RTM | — | `budgets.total` 120 s default would kill a session at 2 min; per-response clocks fit, per-session ones do not | `config.py` budgets | A voice surface needs `session_total` separate from `response_total` |
| Concurrency: no documented concurrent-session cap on the fetched pages; RPM/TPM tiers only; "audio minutes per minute" limits exist for some streaming audio models | RL, M-* | tier | Provider-key limiter counts connections — a long WS is one connection for an hour, so the cap semantics actually fit better than for chat | `admission.py`, FAILURE-MODES row 7 | — |
| TTS/STT retry safety | SPEECH, TRANS | — | Pre-commit retry rules transfer: TTS SSE commits on first `speech.audio.delta`; binary TTS commits on first byte; STT commits on first `transcript.text.delta` | CONTRACTS C1/C2 | Multipart bodies must be buffered to retry — 25 MB per attempt in RAM |

## 6. Usage and billing units

| capability | OpenAI (doc) | unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Realtime `response.done.usage` `[live]`: `input_tokens`, `output_tokens`, `total_tokens`, `input_token_details {text_tokens, audio_tokens, image_tokens, cached_tokens, cached_tokens_details {text_tokens, audio_tokens, image_tokens}}`, `output_token_details {text_tokens, audio_tokens}` | SEV; probe 16 Sep | tokens, per response | **Needs new accounting**: audio vs text at different rates (`gpt-realtime`: text $4/$0.40/$16, audio $32/$0.40/$64, image $5/$0.50 per 1M; `-mini`: text $0.60/$0.06/$2.40, audio per PRICE) | `accounting.py:97` four kinds; `catalog.py:110-121` single rate set | Needs `audio_input`, `audio_output`, `cached_audio` kinds or a per-modality rate table on `ModelSpec`. The observed shape nests cached tokens by modality, so cache reads also need the split |
| Chat-completions audio: `gpt-audio` / `gpt-audio-1.5` audio $32 in / $64 out; `gpt-audio-mini` $10 / $20 per 1M audio tokens | PRICE | tokens | Same new kinds; the fields already arrive in `prompt_tokens_details.audio_tokens` / `completion_tokens_details.audio_tokens` and are unread | `openai.py:196-204` | Today audio tokens are priced at the text rate → under-billed ~50x on `gpt-audio` |
| TTS `gpt-4o-mini-tts`: $0.60 per 1M text input tokens + $12 per 1M audio output tokens; input ≤ 2,000 tokens / 4,096 chars; usage in `speech.audio.done` (SSE only) `[live]` | M-TTS, SPEECH; probe 16 Sep | tokens (SSE) / **nothing** (binary mode, verified: no usage in body or headers) | SSE mode fits the existing four kinds exactly (`input_per_m=0.60, output_per_m=12`); binary mode has no usage object → estimate from characters (observed ratio about 2.8 audio tokens per input character) or from bytes (pcm: 24 kHz × 16-bit = 48,000 B/s) | `accounting.py:302-311` | `tts-1` $15 / `tts-1-hd` $30 per 1M characters (PRICE): a characters unit the catalog lacks. Observed 26 chars → 6 in / 72 out ≈ $0.00087 |
| STT tokens: `gpt-4o-transcribe` $2.50 per 1M audio in / $10 per 1M text out (`-mini` $1.25 / $5); usage `{type: "tokens", input_tokens, output_tokens, input_token_details}` | M-4OT, TRANS | tokens | Fits existing kinds with an audio-input rate | — | Not probed (legacy models) |
| STT duration: `whisper-1` $0.006/min, `gpt-transcribe` $0.0045/min, `gpt-4o-transcribe` "$0.006/min" estimate, `gpt-live-transcribe` $0.017/min; usage `{type: "duration", seconds}` `[live]` on `gpt-transcribe` (streamed and not) and `whisper-1` | PRICE, M-T, M-LT, TRANS; probe 16 Sep | **seconds, rounded up** (`verbose_json` reported `duration: 2.44`, `usage.seconds: 3`) | **Needs new accounting** (a `seconds` unit and `per_minute` rate) | `ModelSpec` has no time unit | Layrs' own `pricing.py` already prices STT per second and TTS per minute (see §7); the provider bills whole seconds |
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
- The transcriptions reference says streaming responses are SSE with `data:`
  prefixes and shows `gpt-4o-mini-transcribe`; live, `gpt-transcribe` streams
  the same shape and both audio endpoints terminate frames with CRLF, which
  the chat endpoint does not. `sse.py` handles both (its own docstring, lines
  39-45, anticipated it).
- The speech reference does not state the default `stream_format`; live, a
  request without it returns binary audio, so `audio` is the default.

## Observed wire shapes (16 Sep 2026, live)

Probes from this laptop (region MAA per `cf-ray`) with `httpx` over HTTP/2 and
`websockets` 17.1; about $0.005 in total. Header values below are redacted
where they identify the account; the key never appeared in any response
except the 401 message's last four characters.

**TTS SSE** (`POST /v1/audio/speech`, `gpt-4o-mini-tts`, 26 chars, voice
`cedar`, `stream_format: "sse"`): 200, `content-type: text/event-stream;
charset=utf-8`, `x-request-id: req_…`, `openai-processing-ms: 524`,
`openai-version: 2020-10-01`, `openai-organization`, `openai-project`,
`x-ratelimit-limit-requests: 30000`, `x-ratelimit-limit-tokens: 150000000`,
`x-ratelimit-remaining-tokens`, `x-ratelimit-reset-tokens: 0s`. Headers at
1,139 ms, body complete at 1,858 ms, 63,498 bytes in 23 HTTP/2 chunks
(≤ 8,179 B). 92 frames, CRLF-terminated, max 2,600 B:

```
data: {"type":"speech.audio.delta","audio":"//PExABc…"}\r\n\r\n      × 90
data: {"type":"speech.audio.done","usage":{"input_tokens":6,"output_tokens":72,"total_tokens":78}}\r\n\r\n
data: [DONE]\r\n\r\n
```

`llmgw.sse.SSEParser` fed the bytes in 7-byte pieces yields 92 events;
`OpenAIChatSurface.classify` labels them HEARTBEAT × 90, META × 1, TERMINAL × 1.

**TTS binary** (same request without `stream_format`): `response_format:
"pcm"` → `content-type: audio/pcm`, 127,200 bytes (2.65 s at 24 kHz s16le) in
18 chunks ≤ 8,192 B, first byte at 964 ms, done at 1,738 ms;
`response_format: "mp3"` → `audio/mpeg`, 39,168 bytes in 17 chunks ≤ 5,760 B,
first byte at 1,075 ms. No `usage` in either body; no usage-bearing header
(only the `x-ratelimit-*` set). `openai-processing-ms` 365 / 793.

**STT stream** (`POST /v1/audio/transcriptions`, multipart, the 39 KB mp3
above, `model=gpt-transcribe`, `stream=true`): 200, `text/event-stream`,
`openai-processing-ms: 341`, `x-ratelimit-limit-requests: 30000` and no
token rate-limit headers. 505 bytes, first frame at 835 ms, done at 877 ms:

```
data: {"type":"transcript.text.delta","delta":"The"}\r\n\r\n                       × 6
data: {"type":"transcript.text.done","text":"The quick brown fox jumps.","usage":{"type":"duration","seconds":3},"languages":[{"code":"en"}]}\r\n\r\n
data: [DONE]\r\n\r\n
```

With `include[]=logprobs` every delta carries `logprobs: [{token, logprob,
bytes[]}]` and the stream grows to 1,470 bytes (max frame 592 B).

**STT non-stream** (`gpt-transcribe`, `response_format=json`):
`{"text": …, "languages": [{"code": "en"}], "usage": {"type": "duration",
"seconds": 3}}`, gzip-encoded, `openai-processing-ms: 288`. `whisper-1` with
`verbose_json`: keys `task, language, duration, text, segments, usage`,
`duration: 2.440000057220459`, `usage: {"type": "duration", "seconds": 3}`,
`x-ratelimit-limit-requests: 10000`.

**STT errors**: unknown model → 404 `{"error": {"message": "The model
`no-such-model` does not exist or you do not have access to it.", "type":
"invalid_request_error", "param": null, "code": "model_not_found"}}`; a text
file as `file` → 400 `{"error": {"message": "Unsupported file format txt",
"type": "invalid_request_error", "param": "file", "code":
"unsupported_value"}}`.

**Client secret** (`POST /v1/realtime/client_secrets`, `expires_after:
{anchor: created_at, seconds: 60}`, `session: {type: realtime, model:
gpt-realtime-mini, audio.output.voice: cedar}`): 200, `x-request-id` in UUID
form (not `req_…`), `openai-processing-ms: 702`. Body: `value` (35 chars,
`ek_` prefix), `expires_at` (epoch seconds), `session` = the full effective
session: `id: sess_…`, `object: realtime.session`, `model`,
`output_modalities: ["audio"]`, the default `instructions` paragraph, `tools:
[]`, `tool_choice: auto`, `max_output_tokens: "inf"`, `truncation: auto`,
`tracing: null`, `prompt: null`, `expires_at: 0`, `audio.input.format:
{type: audio/pcm, rate: 24000}`, `audio.input.turn_detection: {type:
server_vad, threshold: 0.5, prefix_padding_ms: 300, silence_duration_ms: 200,
idle_timeout_ms: null, create_response: true, interrupt_response: true}`,
`audio.output: {format: {audio/pcm, 24000}, voice: cedar, speed: 1.0}`,
`include: null`.

**Realtime WebSocket** (`wss://api.openai.com/v1/realtime?model=gpt-realtime-mini`,
bearer only): upgrade in 1,660 ms with `sec-websocket-extensions:
permessage-deflate`; frames in order with times from connect:

```
1669 ms  session.created          {event_id, session, type}; session.type=realtime, output_modalities=[audio], same defaults as the client-secret echo
1927 ms  session.updated          after session.update {type: realtime, output_modalities: [text]}
2195 ms  response.created
2316 ms  response.output_item.added, conversation.item.added, response.content_part.added
2318 ms  response.output_text.delta {content_index, delta, event_id, item_id, obfuscation, output_index, response_id, type}
2365 ms  response.output_text.done, response.content_part.done
2367 ms  conversation.item.done, response.output_item.done
2370 ms  response.done            response.status=completed; usage below
```

`response.done.response.usage`: `{"total_tokens": 11, "input_tokens": 8,
"output_tokens": 3, "input_token_details": {"text_tokens": 8, "audio_tokens":
0, "image_tokens": 0, "cached_tokens": 0, "cached_tokens_details":
{"text_tokens": 0, "audio_tokens": 0, "image_tokens": 0}},
"output_token_details": {"text_tokens": 3, "audio_tokens": 0}}`. No
`rate_limits.updated` arrived in this one-response session. Client close →
close code 1000.

Same connection with `OpenAI-Beta: realtime=v1` added: upgrade succeeds, first
frame is `{"type": "error", "event_id": …, "error": {"type":
"invalid_request_error", "code": "beta_api_shape_disabled", "message": "The
Realtime Beta API is no longer supported. Please use /v1/realtime for the GA
API.", "param": null, "event_id": null}}`, then server close 4000 with reason
`invalid_request_error.beta_api_shape_disabled`.

**Bad key** (`POST /v1/audio/speech` with a fake `sk-proj-…1234`): 401,
`content-type: text/plain` (body is JSON nonetheless), `x-request-id`,
`x-openai-authorization-error`, `x-openai-ide-error-code`, `x-error-json`;
body `{"error": {"message": "Incorrect API key provided:
sk-proj-***************1234. You can find your API key at …", "type":
"invalid_request_error", "code": "invalid_api_key", "param": null},
"status": 401}`.

**Chat SSE control** (same account, `gpt-4o-mini`, `max_tokens: 3`): frames
terminated `\n\n`, each chunk carrying `"obfuscation": ""`; six frames, max
343 B. The CRLF terminator is specific to the audio endpoints.

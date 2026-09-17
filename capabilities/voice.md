# Voice providers vs llmgw: AssemblyAI, ElevenLabs, Inworld, OpenAI audio

Swept 2026-09-16 against current documentation and the Layrs voice agent
(`/Users/sanjay/Layrs/voice-agent`, read-only). One detailed file per
provider, each row with a doc URL and `file:line` evidence:

- [voice-assemblyai.md](voice-assemblyai.md)
- [voice-elevenlabs.md](voice-elevenlabs.md)
- [voice-inworld.md](voice-inworld.md)
- [voice-openai.md](voice-openai.md)

Fit legend. **Proxyable today**: HTTP JSON or SSE that a new surface could
carry with small changes. **Framing**: HTTP but not SSE (NDJSON or raw bytes);
needs a second framer in the pump, not a new transport. **Transport**:
WebSocket, WebRTC or SIP; not possible in the current HTTP-only ASGI app.
**Accounting**: the wire works or nearly works, but the unit is characters,
seconds or audio tokens, which the catalog and cost model cannot express.
**Control**: not a data-plane concern.

## Status after PLAN-2 Phase D (18 Sep 2026)

The HTTP half of the table below has moved. Rows marked **Framing** or
**Accounting** on 16 Sep are **Supported** as of Phase D: `audio_speech`
(OpenAI binary TTS), `audio_transcription` (OpenAI STT and translation,
multipart, SSE or JSON), `inworld_tts` (sync and `:stream` NDJSON),
`elevenlabs_tts` (`{voice_id}` and `/stream`, query forwarded, raw auth
header) and `assemblyai_sync` (raw-key auth, raw PCM in) are registered
surfaces with `jsonl` and `raw` framers, `characters` and `seconds` units,
per-provider 403 meaning, and the `detail.*` and gRPC-status body readers.
See `docs/18-voice-surfaces.md`. Still not supported: everything marked
**Transport** (the WebSocket data plane), OpenAI TTS SSE mode and ElevenLabs
`with-timestamps` as registered routes (per-request framing selection), and
ElevenLabs / AssemblyAI live verification (no keys provisioned).

## The verdict

**Nothing the Layrs voice agent runs in production can pass through llmgw
today, and the reason is transport, not design.** Production TTS and STT are
Inworld over bidirectional WebSockets via the LiveKit plugin; the fallbacks
(OpenAI TTS, AssemblyAI STT) are a chunked binary HTTP stream and a WebSocket
respectively. The gateway has no WebSocket route, no WebSocket upstream
client (httpx only, no `websockets`/`wsproto` in the lock), and a
one-directional pump that feeds every byte to an SSE parser. Point any of
these providers at the gateway and the result is not a clean refusal: a
binary or NDJSON stream is copied to the client for a few seconds while no
event ever fires, then cut by the first-event clock or the 1 MiB frame bound
and recorded as a provider stall with $0 accounted.

What does transfer, unchanged and conceptually intact: the absolute deadline
and the four budgets, commitment before the first client write, byte-bounded
backpressure, the per-credential concurrency cap (which is exactly how
ElevenLabs and Inworld meter concurrency), credential-scoped breakers,
per-tenant admission, the liveness-versus-progress clock split (`ping`,
`Heartbeat`, keep-alive comments are liveness; `Turn`, `audioChunk`,
`speech.audio.delta` are progress), the status-code taxonomy, header
allowlists, the credential scrub, capture and drain. The event vocabularies of
all four providers map onto the existing `EventKind` classifier without
strain. The design holds; the code needs a second framer, a third credential
style, non-token units, and eventually a second data plane.

## Cross-provider matrix

### Products and transports

| Product | AssemblyAI | ElevenLabs | Inworld | OpenAI | llmgw fit |
|---|---|---|---|---|---|
| Streaming STT | WebSocket, binary in / JSON out | WebSocket (Scribe v2 Realtime), base64 in / JSON out | WebSocket `:streamBidirectional`, base64 in / JSON out | WebSocket (Realtime transcription session) | Transport |
| Batch / sync STT | HTTP raw PCM in, JSON out (Sync, ≤120 s, ≤40 MB); async REST + poll/webhook | multipart in (or `source_url` JSON), JSON out | HTTP JSON, audio inline (≤16 MB) | multipart in, JSON or SSE out (≤25 MB) | **Supported** (Phase D) for AssemblyAI sync (`assemblyai_sync`, raw PCM) and OpenAI (`audio_transcription`, multipart); ElevenLabs and Inworld batch STT not built |
| TTS streaming over HTTP | – | chunked binary (`/stream`); NDJSON (`/stream/with-timestamps`) | NDJSON (`:stream`) | chunked binary, or SSE with `stream_format: "sse"` | **Supported** (Phase D): `elevenlabs_tts`, `inworld_tts`, `audio_speech` binary; OpenAI SSE mode and `with-timestamps` have surfaces but are not yet registered routes |
| TTS streaming over WebSocket | – | `stream-input`, multi-context | `:streamBidirectional`, ≤5 contexts per socket | – | Transport |
| TTS buffered | – | HTTP JSON in, whole audio out | HTTP JSON in, JSON with base64 out | HTTP JSON in, whole audio out | **Supported** (Phase D): Inworld sync is the `inworld_tts` surface as a one-frame stream; OpenAI buffered is the binary `audio_speech`; ElevenLabs non-stream is the same `elevenlabs_tts` route |
| Speech-to-speech / realtime agent | Voice Agent API (product) | Agents Platform (WS/WebRTC) | Realtime API (OpenAI-protocol WS/WebRTC) | Realtime API (WS/WebRTC/SIP) | Transport; arguably Control (competing runtimes) |
| Session token minting for browsers | `GET /v3/token` (one-time, ≤600 s, session cap) | single-use tokens, signed URLs | one-time bearer tokens | `POST /v1/realtime/client_secrets` (session config pinned) | Proxyable today as a buffered JSON route, and the one voice piece a gateway should own |
| LLM gateway of their own | LLM Gateway (OpenAI-compatible) | – | Router at `api.inworld.ai/v1` (OpenAI/Anthropic-compatible, fallback array) | – | Proxyable today as a `ProviderConn(kind="openai")` row; double-gatewaying, one layer must own retries |

### Wire framing the pump would meet

| Shape | Who | What the current pump does | Needed |
|---|---|---|---|
| SSE `data:` JSON, CRLF-terminated | OpenAI TTS (`sse`), OpenAI STT `stream: true` | Parses (verified live 16 Sep: CRLF frames, max frame 2.6 KB against the 1 MiB bound), but `OpenAIChatSurface.classify` labels `speech.audio.delta` and `transcript.text.delta` HEARTBEAT and the `done` events META: no progress, no commitment, usage never applied | `audio_speech`, `audio_transcription` surfaces; the parser is not the problem |
| NDJSON, one object per line, no blank lines, ends on close | Inworld `:stream`, ElevenLabs `with-timestamps` | Every line appended to one SSE frame as an unknown field; no event fires; `FirstEventTimeout` at 20 s; `FrameTooLarge` at 1 MiB cumulative (about 16 s of 24 kHz PCM) | `JSONLFramer` beside `SSEParser`: one event per line, per-line bound, no terminator; surface classifies audio lines as CONTENT, timestamp-only lines as META |
| Raw bytes (mp3, pcm), no framing, ends on close | ElevenLabs `/stream`, OpenAI TTS default | Same failure as above; the repo's gzip note (`upstream.py:606-612`) describes it exactly | Raw framing: first byte = first event, every chunk = progress, EOF = terminal, native ending = close |
| Bidirectional JSON or binary frames | all four providers' realtime products | Not reachable | WebSocket data plane |

### Auth

| Provider | Scheme | llmgw today |
|---|---|---|
| AssemblyAI | `Authorization: <raw key>` (no `Bearer`); temporary tokens as a query param | Not injectable: `build_headers` knows `Bearer` and `x-api-key`; `authorization` is in `NEVER_FORWARDED`, so this is code not config |
| ElevenLabs | `xi-api-key` | Not injectable; two lines once a `ProviderKind` exists |
| Inworld | `Authorization: Basic <portal key>` in the docs, but `Bearer <key>` behaves identically (verified live 16 Sep, unauthenticated probes); `x-api-key` ignored. Bad key is 403 code 7, missing key 401 code 16; the 403 body echoes the key's first four characters masked | **Injectable today** with the existing bearer path; the scrub must still cover the 403 body (finding 30 shape, prefix not suffix) |
| OpenAI audio | `Bearer`; `OpenAI-Safety-Identifier` for per-user attribution; ephemeral `ek_` keys for browsers | Bearer works; no place to inject the tenant as the safety identifier |

A `ProviderConn.auth_scheme` (`bearer | x-api-key | raw`) covers all four;
Inworld and OpenAI already fit the bearer path.

### Errors and limits that collide with current rules

| Collision | Where | Consequence today |
|---|---|---|
| **403 means rate-limited** (AssemblyAI REST, 20k requests per 5 min) or **plan / voice / model denied** (ElevenLabs, its most common policy failure) | `errors.py` maps 403 with 401 to `AuthenticationFailed` | A polling burst or a per-voice mistake opens the credential breaker for every tenant |
| WebSocket close 1008 is both bad auth and too many sessions (AssemblyAI) | no WS layer | Would need `Error`-frame sniffing, like the 400/429 body rules |
| Concurrency is a rate of opens per minute that auto-scales (AssemblyAI), a per-plan count charged only while generating (ElevenLabs), or unpublished per plan (Inworld) | `ProviderKeyLimiter` is a connection count | Right primitive for ElevenLabs and Inworld once the plan number is known; wrong shape for AssemblyAI |
| No `Retry-After` anywhere on any of the four | `retry.py` floor logic | Unused; jittered backoff applies |
| Queueing instead of rejecting (ElevenLabs adds ~50 ms; AssemblyAI queues pre-recorded jobs FIFO) | first-event budget 20 s | Fine for TTS; a proxied poll loop would burn the total budget |
| Error bodies are `detail.*` (ElevenLabs), gRPC-status (Inworld), `Error` frames (AssemblyAI) | body sniffs read `error.type` / `message` | Status-only classification; a third and fourth body reader needed |

### Billing units

| Provider | Unit | Per-call meter | llmgw today |
|---|---|---|---|
| AssemblyAI streaming | session-open wall time per second ($0.15/h Universal-Streaming, $0.45/h Universal-3.5 Pro) | only on `Termination.session_duration_seconds`, dashboard after close | Token-only catalog and accounting; nothing can be recorded |
| AssemblyAI pre-recorded, sync | audio seconds ($0.15 to $0.45/h plus additive add-ons) | `audio_duration` in the response | Same |
| ElevenLabs TTS | characters ($0.05 per 1k Flash, $0.10 per 1k v3) | `character-cost` response header, arrives before the body | Header stripped by the response allowlist |
| ElevenLabs STT | audio hours ($0.22 batch, $0.39 realtime) | derivable from timestamps or audio sent | Same |
| Inworld TTS | characters ($15 to $25 per 1M on-demand, tier-dependent) | `result.usage.processedCharactersCount` on every stream line: the full count on the first line, `0` after (verified live 16 Sep), so billing is exact even on a cut stream; empty text is a 200 with `usage: null` | Would be dropped |
| Inworld STT | audio hours ($0.15/h on-demand, $0.10/h paid tiers) | `RECOGNITION_USAGE` every 5 s over the socket | Transport |
| OpenAI Realtime, chat audio | tokens, but audio at 8 to 50x the text rate ($32 in / $64 out per 1M on `gpt-realtime` and `gpt-audio`) | `usage.input_token_details.audio_tokens` etc. | Fields arrive on the chat route today and are priced as text: silent under-billing |
| OpenAI TTS | `gpt-4o-mini-tts` tokens ($0.60 text in, $12 audio out per 1M) in SSE mode; nothing in binary mode; `tts-1*` per character | `speech.audio.done.usage` (SSE only) | SSE mode fits the four kinds exactly; binary mode has no meter |
| OpenAI transcription | per minute ($0.0045 to $0.017) with `usage: {type: "duration", seconds}`, or audio tokens on `gpt-4o-transcribe` | response field | No time unit |

Layrs' own `harness/evals/pricing.py` already bills voice per second and per
minute, so the unit the gateway lacks is the unit the consumer already uses.

## What Layrs runs, and what that means

| Role | Provider and transport | Through llmgw? |
|---|---|---|
| Production TTS | Inworld `inworld-tts-1.5-mini`, voice Aarav, LiveKit plugin over WebSocket | No (transport) |
| Production STT | Inworld `inworld/inworld-stt-1`, LiveKit plugin over WebSocket | No (transport) |
| TTS fallback | OpenAI `gpt-4o-mini-tts`, voice cedar, binary stream | No (`/v1/audio/*` is 404; would fail as `incomplete_stream` if routed) |
| STT fallback | AssemblyAI `universal-streaming-english` (harness) over WebSocket; skeleton uses OpenAI `gpt-4o-transcribe` over the Realtime transcription socket | No (transport) |
| Simulated learner voice | Inworld sync REST `POST /tts/v1/voice`, MP3 24 kHz | Proxyable once an Inworld surface and Basic auth exist |
| LLM inside voice sessions | `openai.LLM`, `anthropic.LLM`, DeepSeek via chat completions | **Yes, today**, by `base_url`; the chat sweep's `model`-echo and 429-billing gaps apply |
| ElevenLabs | not used; env placeholders only | – |

Layrs-side findings that do not wait for the gateway:

- `harness/config.py:84` runs `inworld-tts-1.5-mini`, which Inworld lists as
  deprecated (TTS-1 and 1-max were discontinued 15 Jun 2026); the current line
  is `inworld-tts-2` and `inworld-tts-2-flash`, and `pricing.py` prices TTS per
  audio minute, a unit Inworld does not bill in.
- `framework/skeleton/session.py:116` constructs `assemblyai.STT` without
  `model=`, so plugin 1.6.2+ silently runs `universal-3-5-pro` at $0.45/h while
  the cost label says $0.15/h. `harness/livekit_setup.py` pins the model; the
  skeleton did not follow.
- The skeleton's `openai.STT(model="gpt-4o-transcribe")` names a model OpenAI
  now marks legacy (`gpt-transcribe` for files, `gpt-live-transcribe` for
  realtime). `pricing.py` still carries the $0.015/min "legacy" TTS figure.
- `livekit_setup.py:189` docstring names a `groq/whisper-large-v3` STT default
  the Inworld docs no longer list.
- `voice-agent/.env.example` has two names for one ElevenLabs secret.

## What llmgw would need, in build order

1. **A second and third framer** (`framing.py`: SSE, JSONL, raw), chosen by
   the surface; the pump reads events from the framer. This alone unlocks
   Inworld HTTP TTS, ElevenLabs HTTP TTS, OpenAI binary TTS, music and SFX.
   Small: one file and a branch in the pump, no new invariants.
2. **`ProviderConn.auth_scheme`** (`bearer | x-api-key | raw | basic`) and a
   wider scrub for reversible credentials. Small.
3. **Units other than tokens**: `ModelSpec.unit` and per-unit rates
   (`characters`, `seconds`, audio token kinds `audio_input`, `audio_output`,
   `cached_audio`); a matching bucket in accounting and the metrics closed set;
   read `character-cost`, `processedCharactersCount`, `usage.seconds`, and the
   audio token details already arriving on the chat route. Medium; the cost
   dot product generalises.
4. **Per-surface body caps and multipart forwarding**: 25 MB (OpenAI audio),
   32 MB (Anthropic), 40 MB (AssemblyAI sync), 48 MiB (DeepSeek vision);
   forward the client's `content-type` boundary instead of forcing JSON.
   Medium; also closes the vision and PDF gaps from the chat sweep.
5. **Voice surfaces** on the HTTP paths: `audio_speech`,
   `audio_transcription`, `inworld_tts`, `elevenlabs_tts`, `assemblyai_sync`,
   with path templating and query passthrough (`{voice_id}`, `output_format`)
   and a `tts` budget profile (first event about 2 s, progress about 5 s).
   Medium each.
6. **Token minting routes** (`/v1/realtime/client_secrets`, AssemblyAI
   `/v3/token`, ElevenLabs single-use tokens): buffered JSON, per-tenant
   admission, session config pinned by the gateway, session duration capped at
   the drain grace, `OpenAI-Safety-Identifier` = tenant. Small, and the highest
   leverage of anything here if browsers ever talk to a provider directly.
7. **Error rules**: provider-scoped 403 handling, `detail.*` and gRPC-status
   body readers, WebSocket close-code and `Error`-frame mapping when the
   transport exists. Small each.
8. **A WebSocket data plane**: ASGI websocket route, upstream WS client,
   two-way relay with per-direction byte bounds, context multiplexing
   (Inworld, ElevenLabs), app-level ping/pong relay (ElevenLabs Agents),
   in-band non-fatal `error` events (OpenAI Realtime), commitment per
   response or per context rather than per connection, session totals in
   hours alongside per-response budgets, and a drain that forwards a close and
   waits for the provider's termination instead of cutting. This is a second
   program, not a phase; it is the only route by which the production voice
   path ever fronts the gateway.

## Gaps ranked across the four providers

1. **No WebSocket transport.** Every product Layrs runs in production or as a
   live fallback is a WebSocket. Until this exists the voice path bypasses the
   gateway entirely, along with its breakers, budgets and cost records.
2. **Non-SSE streams are cut, not proxied.** NDJSON and raw audio over HTTP
   produce a few seconds of audio to the client, then a provider-blamed cut
   and $0 accounted. A clear 501 for unknown content types would be better than
   today's behaviour until the framers exist.
3. **Characters, seconds and audio tokens are not a currency the gateway
   has.** No voice cost record can be right; the per-call meters exist on the
   wire for every provider and are all dropped today.
4. **Two credential styles cannot be injected** (AssemblyAI's raw key,
   ElevenLabs' `xi-api-key`). Two lines each once `auth_scheme` exists;
   Inworld turned out to accept `Bearer`.
5. **403 is not a credential failure on two of these providers.** Rate limit
   (AssemblyAI) and plan/voice/model denial (ElevenLabs) would open the
   credential breaker for every tenant. On Inworld a 403 is a bad key, so the
   rule must be provider-scoped rather than changed globally.
6. **Session length breaks the deploy arithmetic.** AssemblyAI sessions run to
   3 hours, OpenAI Realtime to 60 minutes, against a 120 s total budget and a
   130 s drain grace. Token minting with a capped session duration is the
   lever; voice needs session totals separate from response budgets.
7. **Progress semantics invert for STT.** A silent user is not a stalled
   provider; the progress clock must count inbound audio, with
   `realtime_factor` (AssemblyAI) or audio sent deciding blame.
8. **Token minting is unowned.** Ephemeral keys and session tokens are the one
   place a gateway can apply admission and pin configuration before any audio
   flows, and the gateway has every primitive for it and no route.
9. **Layrs' own voice config has drifted** (deprecated Inworld model,
   AssemblyAI default model, legacy OpenAI STT model, per-minute prices for
   per-character products). Independent of the gateway.
10. **Provider-side LLM routers overlap the gateway** (AssemblyAI LLM Gateway,
    Inworld Router). If either is ever used behind llmgw, exactly one layer
    must own retries (`X-Gw-No-Retry`).

## Verification boundary

Inworld's `docs.inworld.ai/api-reference/*` requires a login; its wire shapes
come from the downloaded `livekit-plugins-inworld==1.6.3` source, Inworld's
`inworld-api-examples` repository and Pipecat, cross-checked where two agree,
plus unauthenticated live probes on 16 Sep that pinned the error bodies
(gRPC-status JSON), the 401/403 split, the auth scheme, credential
reflection, `x-inworld-request-id`, the WebSocket "101 then in-band error"
behaviour and the Router's endpoints, then authenticated probes (about $0.06)
that confirmed the `:stream` framing (`application/json`, chunked, one JSON
object per newline, no terminator, close-ended; a steady-state LINEAR16 line
is exactly one second of audio, 64 KB on the wire; `SSEParser` produced zero
events and tripped `FrameTooLarge` at line 19 of 123), `usage` placement, the
44-byte RIFF header on the first chunk, six error shapes with no credential
reflection, and that Inworld sends no rate-limit headers at all. Still
unverified for Inworld: numeric rate and concurrency limits, `Retry-After`,
5xx bodies, the WebSocket success path. AssemblyAI's Sync API
reference page was unreachable; its request shape comes from the quickstart.
OpenAI's audio and Realtime rows were verified live on 16 Sep (about $0.005):
TTS SSE and binary, STT streaming and `whisper-1`, a client-secret mint, a
text-only Realtime WebSocket session and a bad-key 401; the two guides that
404'd were found under `voice-websockets` and `voice-sip`.

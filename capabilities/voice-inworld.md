# Voice sweep: Inworld AI vs llmgw (2026-09-16)

Repo: `/Users/sanjay/PREP/Evo/llmgw` @ `bc1d065`. Docs read today. Status key for the "llmgw fit" column: **Proxyable today** (works through the current HTTP path as-is), **Needs new transport** (WebSocket, or a non-SSE framing mode), **Needs new accounting** (works or nearly works on the wire but the catalog/cost/usage model cannot represent it), **Not a gateway concern**.

## What could and could not be verified

- `docs.inworld.ai/api-reference/*` pages **302 to a login** (`platform.inworld.ai/docs-auth/login-redirect`), so the formal request/response schemas were not readable. Wire shapes below come from Inworld's public tutorials, the guide pages that are public (`/tts/tts`, `/tts/tts-models`, `/stt/overview`, `/realtime/overview`, `/router/introduction`, `/portal/authentication`, `/docs/resources/rate-limits`), the LiveKit plugin source (`livekit-plugins-inworld`, the one Layrs pins at 1.6.3), and Pipecat's Inworld service source. Where two of those agree the row is marked verified; where only one says it, the note says so.
- **Not verified:** exact error-body JSON for HTTP errors (guide says "standard gRPC status format": `code`, `message`, `details`; not seen in a real body), numeric rate limits per tier (docs defer to the pricing page, which lists none), Retry-After on 429 (not documented), request-id response header (none documented; clients send `X-Request-Id`), WebSocket keepalive protocol (plugin uses a 60 s receive timeout and no ping), one-time-token minting endpoint path, Router base URL beyond a marketing page (`https://api.inworld.ai/v1`; the docs migration page host `dev.docs.inworld.ai` does not resolve).
- No provider call was made.

Doc URLs used: DOCS https://docs.inworld.ai · TTS https://docs.inworld.ai/tts/tts · MODELS https://docs.inworld.ai/tts/tts-models · STT https://docs.inworld.ai/stt/overview · RT https://docs.inworld.ai/realtime/overview · ROUTER https://docs.inworld.ai/router/introduction · AUTH https://docs.inworld.ai/portal/authentication · RL https://docs.inworld.ai/docs/resources/rate-limits · PRICE https://inworld.ai/pricing · QS https://inworld.ai/resources/tts-api-quickstart · JS https://inworld.ai/resources/javascript-tts-api-tutorial · LK-TTS livekit-plugins-inworld `tts.py` · LK-STT `stt.py` · PC pipecat `services/inworld/tts.py` · LKDOC https://docs.livekit.io/agents/models/tts/inworld/ · EX https://github.com/inworld-ai/inworld-api-examples · LEAK https://github.com/openclaw/openclaw/issues/146804.

## 1. Products and endpoints

| capability | Inworld (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS sync `POST https://api.inworld.ai/tts/v1/voice` | QS, JS | REST JSON in, JSON out (`audioContent` base64, `usage`) | **Proxyable today** as a buffered route, but **Needs new accounting** | `server/app.py:237` `ROUTE_TO_UPSTREAM_PATH` has only two chat routes; buffered path exists (`app.py` non-stream, `config.py:401` 8 MiB response cap) | Route table addition + a surface. Response is one JSON object; 2,000-char input → at most a few hundred KB of base64 audio, inside the 8 MiB cap |
| TTS stream `POST /tts/v1/voice:stream` | JS, LK-TTS, PC | **NDJSON**: one JSON object per line, `{"result":{"audioContent":"<b64>","timestampInfo":{...}},"usage":{...}}`; ends on connection close, no terminator | **Needs new transport (framing)** | `pump.py:198` constructs `SSEParser` unconditionally; `sse.py:330-357` appends every line to `_raw` and treats `{"result"…` as an unknown field, never dispatching | See "What the SSE parser does with NDJSON" below: bytes are copied to the client, but the progress clock never ticks and the frame bound trips |
| TTS WebSocket `wss://api.inworld.ai/tts/v1/voice:streamBidirectional` | LK-TTS, PC (docs page gated) | WS JSON: client `create` / `send_text` / `flush_context` / `close_context` with `contextId`; server `contextCreated` / `audioChunk` / `flushCompleted` / `contextClosed` / `error`; ≤5 contexts per socket | **Needs new transport (WebSocket)** | no `websocket` anywhere in `src/llmgw` (grep); Starlette app is HTTP routes only (`app.py:2488-2505`) | This is what the LiveKit plugin Layrs runs uses in production |
| Voices: list `GET /tts/v1/voices`, clone `POST /voices/v1/voices:clone`, design, publish, update, delete | QS, LK-TTS | REST JSON | **Not a gateway concern** (control plane) | — | Clone bodies carry base64 samples; if ever proxied the 4 MiB request cap (`config.py:395`) bites |
| STT sync `POST /stt/v1/transcribe` | STT | REST JSON; audio inline; ~16 MB max | **Proxyable today** as buffered (with cap raised) + **Needs new accounting** | `config.py:395` 4 MiB < 16 MB | Layrs does not use sync STT |
| STT streaming `wss://api.inworld.ai/stt/v1/transcribe:streamBidirectional` | STT, LK-STT | WS JSON: first message `transcribeConfig`, then `{"audioChunk":{"content":"<b64 pcm>"}}`, `endTurn`, `closeStream`; server events START/INTERIM/FINAL/END_OF_SPEECH, `RECOGNITION_USAGE` every 5 s | **Needs new transport (WebSocket)** | as above | Production STT path for Layrs |
| Realtime API (speech-to-speech) | RT | WebSocket following OpenAI Realtime protocol "with extensions", plus WebRTC; composes STT+LLM+TTS; Router-aware | **Needs new transport**; arguably **Not a gateway concern** | — | Session tokens; per-session concurrency; the LLM inside it is Inworld's Router, not yours |
| LLM Router `https://api.inworld.ai/v1` (OpenAI-compatible chat completions; Anthropic-compatible too per third-party profile) | ROUTER, marketing pages | REST/SSE, `model` + `models[]` fallback array | **Proxyable today** as one more `ProviderConn(kind="openai")` | `catalog.py:60` `ProviderKind = openai|anthropic`; `upstream.py:316` bearer | Direct overlap with llmgw (fallback, cost routing, A/B). If used, exactly one layer must own retries — PLAN §3 contract 5, `X-Gw-No-Retry` |
| One-time tokens for clients (`Authorization: Bearer <token>`, minutes TTL, single use) | AUTH | REST mint endpoint (path gated) | **Not a gateway concern** today | — | Relevant if browsers ever talk to Inworld directly through the gateway |

## 2. Streaming shape

| capability | Inworld (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| HTTP stream framing | JS, LK-TTS, PC | NDJSON, `\n`-terminated JSON objects, no blank lines, no `data:` prefix, no terminal marker; stream ends at TCP close | **Needs new transport (framing)** | `sse.py:224-357` | A JSONL framing mode is the smallest change; see below |
| Audio payload | JS, LK-TTS | `result.audioContent` base64 of the chosen encoding; PCM/LINEAR16 chunks may start with a 44-byte RIFF header (Pipecat strips it) | passthrough bytes | `pump.py` copies chunks verbatim | Base64 inflates 1.33×; PCM 24 kHz 16-bit ≈ 64 KB/s on the wire |
| Timestamps | LK-TTS, PC, EX | `timestampInfo.wordAlignment{words[], wordStartTimeSeconds[], wordEndTimeSeconds[]}` / `characterAlignment`; phonemes and visemes on TTS-2; `timestampType: WORD|CHARACTER`, `timestampTransportStrategy: SYNC|ASYNC` | passthrough | — | With `ASYNC`, timestamps arrive in separate lines after audio — a "content vs meta" distinction the surface would classify |
| Errors mid-stream | LK-TTS | a line with top-level `error: {code, message}` (HTTP) or `result.status.code != 0` (WS) | needs surface `error_from_event` | `surfaces/base.py:199` | Same contract shape as `error_from_event` today |
| WebSocket direction | LK-TTS | bidirectional, multiplexed by `contextId`; buffering knobs `autoMode`, `bufferCharThreshold` 120, `maxBufferDelayMs` 3000 | **Needs new transport** | — | Multiplexing several contexts on one socket is why the plugin pools ≤20 sockets × 5 contexts |
| Session lifecycle / keepalive | LK-TTS | no ping/pong documented; plugin uses 60 s receive timeout, 300 s idle close, `Context not found` (code 5) treated as benign | — | `clocks.py` liveness/progress split would map: `audioChunk` = progress, anything else = liveness | |
| STT audio direction | LK-STT | client → server base64 PCM16 16 kHz mono in JSON; server → client JSON events | **Needs new transport** | — | Upload-heavy; the gateway would be relaying ~32 KB/s per session upstream |
| Limits | JS, STT | TTS 2,000 characters per request; STT sync ~16 MB; concurrency per plan | — | — | Long text must be chunked client-side ("long-text chunking" example in EX) |

### What the SSE parser does with NDJSON (precisely)

`pump.py:198` builds `SSEParser(max_frame_bytes=1 MiB, emit_comments=True)` and feeds every chunk (`pump.py:366`). For a line `{"result":{"audioContent":"…"}}\n`:

1. `sse.py:330` appends the raw line to `_raw` **before** parsing the field name.
2. `line.partition(b":")` yields field name `{"result"` → the `else` branch at `sse.py:352` returns `None`: unknown field, `_have_fields` stays False, nothing dispatched.
3. No blank line ever arrives, so `_dispatch()` never runs and `_raw` grows by every audio line. `_check_bound(len(self._raw))` (`sse.py:334`) raises `FrameTooLarge` once the cumulative stream passes 1 MiB — roughly 16 s of 24 kHz PCM or ~90 s of 64 kbps MP3 — poisoning the parser and ending the stream.
4. Before that, no `EventKind.CONTENT` is ever produced, so `Pump._started` never sets and the first-event clock fires (`Budgets.first_event` 20 s, `clocks.py:251`) and, had it not, the progress clock would (15 s, `clocks.py:267`). The copied bytes DO reach the client meanwhile (parse-then-enqueue in `pump.py:345-366`), so a caller would see a few seconds of audio and then a cut.

A **JSONL framing mode** would need: (a) a `Framer` protocol with two implementations, SSE and newline-JSON, chosen by the surface (or by upstream `content-type`: Inworld returns `application/json`-ish for the stream; verify); (b) the JSONL framer emits one event per line with `data=<line>` and the same `max_frame_bytes` bound per line (a line is a frame; audio lines are ~4–64 KB, fine); (c) the pump's "parse first, enqueue second" and commitment semantics unchanged; (d) the surface classifies `result.audioContent` present → CONTENT, `timestampInfo`-only → META, `error` → failure, and `native_ending` = close without more lines (there is no terminator to withhold); (e) heartbeat: none documented on the HTTP stream, so liveness = progress here.

## 3. Latency knobs and metrics

| capability | Inworld (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Model tier | MODELS | `inworld-tts-2` (100 ms P90 TTFB server-side), `inworld-tts-2-flash` (20 ms) | catalog row | `catalog.py` `ModelSpec` has no tier concept beyond the row | Fine as two rows |
| `deliveryMode` STABLE / BALANCED / CREATIVE (TTS-2) | LK-TTS, EX | request field | passthrough | — | |
| `temperature` (TTS-1.5 only, 0–2), `speakingRate`, `applyTextNormalization` ON/OFF, `language` BCP-47 | LK-TTS, EX | request fields | passthrough | — | Layrs pins `text_normalization="ON"` (`livekit_setup.py`) |
| Encoding / sample rate | JS, LK-TTS | MP3, LINEAR16/PCM, WAV, OGG_OPUS, MULAW, ALAW, FLAC; 8–48 kHz; `bitrate` | passthrough | — | Encoding changes bytes/s → affects the frame bound and the pump buffer sizing (`buffer_bytes` 256 KiB) |
| WS buffering `bufferCharThreshold` / `maxBufferDelayMs` / `autoMode` | LK-TTS | WS create fields | n/a until WS | — | |
| Provider TTFB exposure | MODELS | server-side P90 figures only; no per-response timing header documented | gateway measures its own TTFE | `metrics.py` `llmgw_time_to_first_event_seconds` | The gateway's first CONTENT (first audio line) is the right TTFB proxy |
| Client-side metrics Layrs reads today | `harness/dsa/metrics_capture.py:59-124` | LiveKit `TTSMetrics.ttfb`, `characters_count`, `audio_duration` per segment | — | — | Same three numbers the gateway would need per request: TTFB, characters, audio seconds (the last is derivable from bytes ÷ (rate × width) for PCM, not for MP3) |

## 4. Auth and headers

| capability | Inworld (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Server credential | AUTH, QS | `Authorization: Basic <api-key>` — the portal key is used verbatim after `Basic ` (already base64; LKDOC says "requires Base64-encoded key") | **Needs a third injection style** | `upstream.py:309-316` knows `x-api-key` (anthropic) and `Bearer` (openai) only; `NEVER_FORWARDED` strips client `authorization` (`config.py:113`) | `ProviderConn` needs an `auth_scheme` (or `kind="inworld"`); the credential-scoped breaker (`errors.py` AuthenticationFailed) transfers unchanged |
| Client one-time tokens | AUTH | `Authorization: Bearer <token>`, single use, minutes | Not a gateway concern | — | |
| Request tracing | LK-TTS, PC | clients send `X-Request-Id` (uuid) and `X-User-Agent`; no response request-id documented | `x-request-id` already in the forward allowlist | `config.py:104` | |
| Workspace / region | AUTH | not documented on the public page | — | — | |
| Credential reflection in errors | LEAK | proxies in front of Inworld have reflected the `Authorization: Basic …` header into error bodies that then reached logs | the C11 scrub covers only `AuthenticationFailed` bodies | `app.py` `send_error` (commit d90f5ed) | For a Basic credential (reversible base64) the scrub should cover **every** non-2xx body from this provider, not only 401/403 |

## 5. Errors, rate limits, concurrency

| capability | Inworld (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Error body | STT ("standard gRPC status format"), LK-TTS | HTTP: JSON with `code` (gRPC int), `message`, `details`; NDJSON line: `{"error":{"message","code"}}`; WS: `result.status.code` | classification by status works; body sniffs (`_looks_like_unknown_model`, "too long") are LLM-shaped | `errors.py:843-910` | gRPC codes (3 INVALID_ARGUMENT, 5 NOT_FOUND, 7 PERMISSION_DENIED, 8 RESOURCE_EXHAUSTED, 16 UNAUTHENTICATED) would give a cleaner second axis than body text |
| 429 | RL | "Your request is not processed — you need to wait and retry"; exponential backoff + jitter recommended; **no Retry-After documented** | `RateLimited`, NEUTRAL, jittered backoff | `errors.py:890`, `retry.py:145-202` | Retry-After floor simply unused |
| Concurrency limits | RL, PRICE | per plan: TTS concurrent generations and WS connections; STT streaming concurrency; Realtime sessions; Router RPS — **numbers not published on either page** | `ProviderConn.max_concurrency` (connection count) | `catalog.py:68-80`, FAILURE-MODES row 7 | Set from the plan's number once known; for WS the unit is *contexts*, not connections |
| 401/403 | LEAK (observed shape), AUTH | 401 for bad key | `AuthenticationFailed`, credential-scoped breaker, body scrubbed | `errors.py:892` | Transfers unchanged |
| 400 text validation | `tts_mixin.py:24` (Layrs) | "Text cannot be empty" for whitespace/punctuation-only text | `InvalidRequest` (client blame) | `errors.py:898` | Layrs already filters this before TTS |
| 5xx / overloaded | not documented | — | `UpstreamServerError` / `UpstreamOverloaded` (503) | `errors.py:911-915` | |
| Mid-stream WS error | LK-TTS | `result.status.code != 0`; code 5 "Context not found" benign | needs WS transport | — | |
| Deadlines | — | TTS-2 TTFB 100 ms; a 2,000-char utterance streams for ~2 min of audio at most | `Budgets`: connect 2 s, first_event 20 s, progress 15 s, total 120 s | `clocks.py:241-273` | Defaults are LLM-shaped but not wrong for TTS; a TTS workload would want first_event ≈ 2 s, progress ≈ 5 s |

## 6. Usage and billing units

| capability | Inworld (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| TTS unit | PRICE | **characters**: TTS-2 $25/1M on-demand → $12.50 Growth → "as low as $5" Enterprise; TTS-2 Flash $15 → $7 → sub-$5; free tier ≈70 min | **Needs new accounting** | `catalog.py:110-121` `ModelSpec` prices per 1M *tokens* (`input_per_m`, `output_per_m`, cache fields); `accounting.py:97` `TOKEN_KINDS = input/output/cache_read/cache_write`; `metrics.py` token counters | A `characters` kind (or a generic `unit` with a per-1M rate) is the minimal change; the dot product in `accounting.py:292-311` generalises |
| Per-call usage report | JS, apis.io schema (via search), LK-TTS | `usage: {processedCharactersCount: int, modelId: str}` on the sync response and on NDJSON lines | surface `apply_usage` | `surfaces/base.py:198` | Exact, not estimated, when present — same two-flag exactness as finding 26 |
| STT unit | PRICE | **per hour of audio**: $0.15/hr on-demand, $0.10/hr Creator+ ; streaming reports `RECOGNITION_USAGE` cumulative duration every 5 s | **Needs new accounting** + WS | — | Layrs `pricing.py:145` already prices STT per audio second |
| Realtime | PRICE | not itemised on the page | — | — | |
| LLM Router | PRICE | "at cost" (provider list price), 220+ models | token accounting transfers if the Router returns standard `usage` | `surfaces/openai.py:167-222` | Router's own pricing would need catalog rows per routed model |
| Tier discounts | PRICE | price depends on the account's plan and monthly spend | `ModelSpec` has one rate | — | Same limitation DeepSeek's peak/off-peak exposed |
| Layrs' own price table | `voice-agent/harness/evals/pricing.py:139-148` | `inworld.inworld-tts-1.5-mini` $0.005/min, `-1.5-max` $0.010/min (2026-06-19), STT $0.10/hr | — | — | Priced per *audio minute*, a unit the provider does not bill in; and both TTS rows name deprecated models |

## 7. Layrs usage today

| item | value | evidence |
|---|---|---|
| Production TTS | LiveKit plugin `livekit-plugins-inworld==1.6.3`, `inworld.TTS(model="inworld-tts-1.5-mini", voice="Aarav", text_normalization="ON")` | `harness/config.py:83-85`, `harness/livekit_setup.py:184-216`, `requirements.txt:8-12` |
| Production STT | `inworld.STT(model="inworld/inworld-stt-1", end_of_turn_confidence_threshold=0.7, min_end_of_turn_silence_when_confident=160)` | `harness/config.py:88-91`, `tests/test_inworld_smoke.py` |
| Transport actually used | plugin → `wss://api.inworld.ai/tts/v1/voice:streamBidirectional` (pooled, ≤20 sockets × 5 contexts) and `wss://…/stt/v1/transcribe:streamBidirectional`; **not** the HTTP stream | LK-TTS, LK-STT |
| Fallbacks | silent fallback to OpenAI TTS (`gpt-4o-mini-tts`, voice `cedar`) and AssemblyAI STT (`universal-streaming-english`) when plugin/key missing | `livekit_setup.py`, `test_tts_factory.py` |
| One-shot REST use | simulated-learner voice in `testing/user-loop/src/tts.ts`: `POST https://api.inworld.ai/tts/v1/voice`, `Authorization: Basic ${key}`, `{voiceId, modelId, audioConfig:{audioEncoding:"MP3", sampleRateHertz:24000}}`, reads `audioContent` | `tts.ts:9-44` |
| Metrics captured | per TTS segment: `characters_count`, `audio_duration`, `ttfb`; per STT: `audio_duration`; priced via `MEDIA_PRICING` | `metrics_capture.py:59-124`, `pricing.py` |
| Credential handling | `INWORLD_API_KEY` env; the same value was one of the five keys found in `src/llmgw/.env` on 9 Sep (finding 29) | `live/RESULTS.md §0` |
| Text hygiene | sentence-boundary buffering and empty/punctuation-only filtering before TTS because Inworld 400s on it | `harness/speech/tts_mixin.py:24-60` |

## What llmgw has and lacks for this provider

**Transfers unchanged** (the primitives are protocol-agnostic): the absolute `Deadline` and the four budgets (`clocks.py`); the byte-bounded pump and commitment flag (`pump.py`), once the framer is pluggable; per-tenant admission (`admission.py`); the credential-scoped breaker and the `NEUTRAL/FAILURE` health axis (`errors.py`, `breaker.py`); provider-key concurrency caps; the header allowlists; the 401 scrub; capture and the metrics contract minus token labels; the policy/workload model (a `tts` workload with its own budgets is exactly what `PolicySnapshot` was built for).

**Lacks, in order of size:**

1. **A second framer.** The HTTP stream is NDJSON; today it is mis-parsed into a single ever-growing SSE frame (details in §2). ~1 file (`framing.py` with `SSEFramer` + `JSONLFramer`), a `Surface.framer` hook, and the pump reading events from the framer instead of `SSEParser` directly.
2. **An `inworld` surface** (`surfaces/inworld_tts.py`): `parse_request` (`modelId`, no `stream` flag — streaming is the *path* `:stream`, so the route decides), `classify` (`result.audioContent` → CONTENT; timestamp-only → META; `error` → failure), `apply_usage` from `usage.processedCharactersCount`, `native_ending` = close. Plus route entries `/tts/v1/voice` (buffered) and `/tts/v1/voice:stream`. The model rewrite (`X-Gw-Body-Modified`) would rewrite `modelId`, not `model`.
3. **A third credential style.** `ProviderConn.auth_scheme: bearer | x-api-key | basic` (or `kind="inworld"`); `build_headers` (`upstream.py:283-316`) branches on it. Scrub every non-2xx body from a Basic-auth provider, not only 401/403 (LEAK).
4. **Character (and second) accounting.** `ModelSpec` gains a unit (`per_m_characters` / `per_hour_audio`) and `accounting` a `characters` kind; `metrics.TOKEN_KINDS` closed set gains it too. The two-flag exactness carries over: `processedCharactersCount` present → exact.
5. **WebSocket transport** for what Layrs actually runs (TTS bidirectional, STT streaming, Realtime). This is a different program: an ASGI websocket route, a per-connection upstream socket, context multiplexing, and a commitment definition per *context* rather than per request. None of the HTTP pump applies; deadlines and admission do. Not a P-phase, a new plan.
6. **Body cap per surface:** STT sync 16 MB and cloning samples exceed the 4 MiB request cap; TTS requests are tiny.
7. **A `tts` workload budget profile:** first_event ~2 s (TTS-2's P90 is 100 ms), progress ~5 s, total ~150 s for a 2,000-character utterance.

## Gaps ranked (impact on the Layrs voice agent)

1. **Nothing Layrs runs in production can go through llmgw today** — both TTS and STT are WebSocket (`:streamBidirectional`) via the LiveKit plugin; the gateway has no WebSocket transport. Everything below is about the HTTP paths, which Layrs uses only for the simulated learner.
2. **The HTTP TTS stream would be cut, not proxied**: NDJSON is mis-framed, no CONTENT event ever fires, `FirstEventTimeout` at 20 s and `FrameTooLarge` at 1 MiB of cumulative audio. A caller would hear a few seconds and then a truncated stream reported as a provider stall.
3. **No character-based cost**: `llmgw_cost_usd_total` cannot represent $/1M characters; Inworld's own `usage.processedCharactersCount` is the exact number and would be dropped.
4. **Basic auth is not an injection style** the upstream layer knows; and a reversible credential means the error-body scrub must widen to all non-2xx from this provider.
5. **Layrs' models are deprecated**: `inworld-tts-1.5-mini` (production) and `-1.5-max` (priced) are listed as deprecated on MODELS; `inworld-tts-1`/`-1-max` were discontinued 15 Jun 2026 with auto-routing. Current ids are `inworld-tts-2` / `inworld-tts-2-flash`, which also change the price basis (Layrs' $0.005/min row vs $15–25 per 1M characters).
6. **Rate/concurrency limits are plan-dependent and unpublished**; the gateway's `max_concurrency` for an Inworld row would be a guess until the account's plan limits are read from the portal.
7. **Deadline defaults are LLM-shaped**: a TTS-2 call that has not produced audio in 2 s is already wrong, but the default first-event budget waits 20 s before falling back.
8. **The Router overlaps the gateway**: `https://api.inworld.ai/v1` is itself an OpenAI/Anthropic-compatible router with fallback and A/B. If Layrs ever routes LLM traffic through it, the two layers must not both retry (`X-Gw-No-Retry: 1` contract), and its "at cost" billing would need per-model catalog rows.

## Doc deltas (Layrs code vs Inworld docs today)

- `harness/config.py:84` `tts_inworld_model = "inworld-tts-1.5-mini"` and `pricing.py:141-142` — **deprecated model ids** per MODELS; still accepted (EX README lists 1.5 models as supported; MODELS says deprecated, no sunset date given) but the current line is TTS-2 / TTS-2 Flash.
- `pricing.py:132` "TTS-1.5-mini ≈ $0.005/min ($5/1M chars)" — PRICE today lists TTS-2 Flash from $15/1M characters on-demand ($7 Growth) and TTS-2 $25 ($12.50); no 1.5 pricing is shown. The per-minute conversion is Layrs' own; Inworld bills characters.
- `pricing.py:133,145` STT $0.10/hr — PRICE says $0.15/hr on-demand, $0.10/hr from the Creator tier; correct only if the account is on a paid tier.
- `livekit_setup.py:189` docstring says the STT default is `groq/whisper-large-v3`; config uses `inworld/inworld-stt-1`, and STT docs list only `inworld/inworld-stt-1` (the Groq/Whisper route is not documented today).
- `testing/user-loop/src/tts.ts` uses the sync REST path with MP3 24 kHz — matches the public quickstart.
- LKDOC's plugin default model is `inworld-tts-1.5-max` (also deprecated); Layrs overrides it, good.
- Docs host note: `docs.inworld.ai/api-reference/*` requires login; public schemas live in the tutorials and third-party SDK sources.

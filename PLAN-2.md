# llmgw build plan 2: text-to-text completeness and voice

Written 2026-09-16 from the capability sweeps ([CAPABILITIES.md](CAPABILITIES.md),
[capabilities/voice.md](capabilities/voice.md)) and the live runs of 15 and
16 Sep. [PLAN.md](PLAN.md) built the gateway (P0 to P8); this file is the route
from a working chat proxy to a gateway that carries everything the Layrs
stack sends to a model provider, text and voice. Every phase ends green,
ships behind configuration, and is verified against real providers before
the next starts. Nothing here changes CONTRACTS C1 to C11; several phases add
contracts, numbered C12 onward.

---

## 0. The ordering, stated once

Three facts from the sweeps decide the order.

1. **Two correctness bugs are live now.** The response `model` cannot be sent
   back, and out-of-money 429s are retried. Every caller hits them in week
   one. They go first, along with the catalog corrections, because they change
   what the deployed gateway does to real traffic today.
2. **Text and voice share the same missing primitives.** Non-token units,
   per-surface body caps, multipart forwarding, a second framer, a third
   credential style, per-target request defaults. Building them once, for
   both, is cheaper than building them twice and they unblock the chat gaps
   (audio tokens billed as text, vision and PDF over 4 MiB) at the same time.
3. **Voice in production is WebSocket.** No amount of HTTP work fronts the
   Layrs voice agent. The HTTP voice surfaces are worth building because they
   carry the simulated learner, batch narration and the OpenAI fallbacks, and
   because they force the primitives above to exist; but the WebSocket data
   plane is a separate program with its own invariants and gets its own
   plan, started only after everything HTTP is in.

So: **A** correctness, **B** shared primitives, **C** text surfaces, **D** voice
HTTP surfaces, **E** token minting, **F** Responses API, **G** the WebSocket
plane (planned here, built under its own document). A and B are sequential;
C, D and E can run in parallel once B lands; F and G are independent of each
other and of D/E.

| Phase | Name | Unblocks | Size |
|---|---|---|---|
| A | Week-one correctness | every chat caller; voice-session LLMs pointed at the gateway | 2 to 3 days |
| B | Shared primitives | everything below; chat audio tokens, vision, PDF | 1 to 2 weeks |
| C | Text surfaces | agent frameworks that call `/v1/models`, embeddings, token counting; DeepSeek as a candidate on the Anthropic surface | 3 to 4 days |
| D | Voice HTTP surfaces | simulated learner, batch narration, OpenAI TTS fallback | 1 to 2 weeks |
| E | Token minting | browser callers on any provider without exposing a key | 3 to 4 days |
| F | Responses API | Responses-SDK callers, hosted tools, reasoning items | 1 to 2 weeks |
| G | WebSocket data plane | the production voice path | separate plan; 4 to 6 weeks |

Sizes assume one engineer plus agents in the pattern used for P0/P1: parallel
agents on disjoint files, verification agent afterwards, live smoke before
merge.

---

## Phase A: week-one correctness

Goal: the deployed gateway stops doing four wrong things to real traffic and
its catalog stops lying. Every item is one file plus a test; the phase is
deliberately boring.

### A1. Model aliases and response model

- `policy.plan_for` accepts, as aliases for a catalog id, the target's
  `api_model`, any snapshot id the provider has returned for it (a new
  `ModelSpec.aliases: tuple[str, ...]`), and the catalog id itself. Resolution
  order is exact catalog id, then alias table, then error. Ambiguity (one
  wire id under two catalog ids on different providers) is a `PolicyError` at
  load, not a runtime guess.
- Buffered responses: rewrite the response `model` back to the catalog id
  when `X-Gw-Body-Modified` is set. Streamed responses are not rewritten
  (byte-for-byte passthrough holds); instead the alias table makes the echoed
  wire id acceptable on the next turn. `X-Gw-Model: <catalog id>` on every
  response so a client can learn the canonical name without parsing.
- Tests: unit for alias resolution and the ambiguity error; contract test
  that a two-turn loop echoing the provider's `model` succeeds through the
  gateway against the fake, and live smoke with `gpt-4o-mini` echoing
  `gpt-4o-mini-2024-07-18`.

### A2. Billing states that arrive as 429

- `errors.classify` gains a body-code rule at the 429 site: OpenAI
  `insufficient_quota`, `credit_balance_exhausted`,
  `organization_spend_limit_exceeded`, `project_spend_limit_exceeded`,
  `organization_usage_limit_exceeded`; Anthropic
  `error.details.error_code == "enforced_spend_limit_reached"`; Anthropic 400
  "reached your specified API usage limits". All map to `InsufficientCredits`
  (no retry-same, try-next, NEUTRAL health, POLICY blame).
- Rule table lives next to the existing 400/402/404 body rules, one entry per
  provider dialect, with the real bodies from `live/RESULTS.md` and the
  sweeps as fixtures in `tests/unit/test_real_error_bodies.py`.
- Contract: a fake mode `429-billing` per dialect; assert no second attempt on
  the same target, fallback to the incumbent, `blame=POLICY` in the record.

### A3. Stop and finish reasons

- Surfaces read `finish_reason` (OpenAI dialect) and `message_delta.delta.stop_reason`
  (Anthropic) into `Usage.stop_reason`, closed set: `stop`, `length`,
  `tool_calls`, `content_filter`, `refusal`, `pause_turn`,
  `context_window_exceeded`, `provider_shed` (DeepSeek
  `insufficient_system_resource`, `aborted`), `unknown`.
- `llmgw_requests_total` does not gain a label (cardinality); a new counter
  `llmgw_stop_reason_total{surface, stop_reason}` and a capture field do.
  `provider_shed` also counts as `Health.FAILURE` for the breaker, because it
  is a provider refusing work inside a 200.
- Tests: unit per surface for every value; contract against fakes emitting
  `length` and `insufficient_system_resource`.

### A4. Queued provider is not a dead provider

- Pump records whether any liveness signal (SSE comment, empty-choices
  frame, `ping`) arrived while waiting for the first event. `FirstEventTimeout`
  raised after liveness was observed carries `Health.NEUTRAL` and a new
  `queued=True` attribute; `llmgw_queued_at_provider_total{provider, model}`
  counts it. Fallback behaviour is unchanged (try next).
- Contract: fake mode `queue-then-serve` (comments for N seconds, then a
  normal stream) with a short first-event budget; assert NEUTRAL, the counter,
  and that five in a row do not open the breaker.

### A5. Catalog corrections and the reconciler in CI

- DeepSeek: prices from the 10 Sep list (Flash 0.30/0.006/1.20, Pro
  1.32/0.044/3.96 per 1M, `priced_at 2026-09-16`, marked peak-rate upper
  bound), `can_reason=True` on Flash, `reasoning` default recorded; OpenRouter
  rows' `api_model` re-verified by `make probe`.
- Anthropic: Sonnet 4.6 `context_window=1_000_000`; Haiku 4.5
  `can_reason=True`; `cache_write_per_m` on both (1.25x); `claude-sonnet-5`
  added at $2/$10 as the default Anthropic route; a note that Haiku 4.5's
  retirement floor is 2026-10-15.
- OpenAI: `gpt-4o-mini` marked verified 2026-09-16; one current reasoning
  model added with a dated price; `ModelSpec.reasoning` literal widened to the
  union of provider vocabularies.
- `make probe` runs in CI (a GitHub Actions job on `backend/llmgw/**`, the
  repo's first workflow) and fails on any `api_model` that a provider's model
  list does not contain; price drift stays a manual step with a dated field
  until a provider exposes prices.

### A6. Small classification and header rules

- 403 is provider-scoped: OpenAI region block and Anthropic permission error
  stay `AuthenticationFailed` (credential outcome is right); a provider row may
  declare `forbidden_means="rate_limit" | "policy"` for AssemblyAI and
  ElevenLabs when those rows exist (B2).
- Anthropic 413 maps to a client-facing `RequestTooLarge`-shaped error (no
  retry, no breaker); 409 to `InvalidRequest`.
- `stream_options.include_usage` injected on the OpenAI dialect when the
  client asked to stream and did not set `stream_options` (finding 27); the
  body is already rewritten for `model`, so this adds no new rewrite path.
  `X-Gw-Body-Modified` already covers it.
- Upstream `x-request-id` / `request-id` / `x-inworld-request-id` captured
  into the capture record and returned as `X-Gw-Upstream-Request-Id`;
  `openai-processing-ms` and `x-envoy-upstream-service-time` captured as
  `upstream_processing_ms`. Neither is a credential.
- Rate-limit headers (`x-ratelimit-remaining-*`, `anthropic-ratelimit-*`)
  parsed into per-credential gauges `llmgw_provider_ratelimit_remaining{credential, kind}`
  and `_reset_seconds`; never forwarded to clients.

### A exit

All tiers green; live smoke extended with the two-turn echo case and a
billing-429 fixture; a deploy of `layrs-llmgw`; and the Layrs voice-session
LLM clients (`openai.LLM`, `anthropic.LLM`) can point at
`http://layrs-llmgw.internal:8080` with the `layrs` tenant token. That is the
first real consumer, and it does not wait for anything below.

---

## Phase B: shared primitives

Goal: the six things both text and voice need, each landing with a text-side
consumer so it is exercised before any voice code exists.

### B1. Framing (`framing.py`)

- `Framer` protocol: `feed(bytes) -> Iterable[Event]`, `flush()`, with a
  per-frame byte bound. Three implementations: `SSEFramer` (wraps the
  existing parser, CRLF and LF), `JSONLFramer` (one event per newline-
  terminated JSON object, blank lines skipped, no terminator, per-line bound),
  `RawFramer` (every chunk is one CONTENT event, EOF is TERMINAL, no
  terminator).
- `Surface.framing: Literal["sse", "jsonl", "raw"]`; the pump asks the surface
  for its framer instead of constructing `SSEParser`. Commitment, byte bounds,
  progress and native-ending semantics are unchanged; for `raw`, first byte is
  first event and native ending is close.
- Measured sizing: Inworld LINEAR16 lines are 64 KB on the wire (one second of
  audio), MP3 lines under 16 KB, OpenAI audio SSE frames under 3 KB, so the
  1 MiB per-frame bound stands; the buffered-response cap becomes per surface
  (B4).
- Unknown upstream `content-type` on the streaming path is a 502
  `unsupported_upstream_framing` with the content type in the message, instead
  of today's silent `incomplete_stream` at $0 (the gzip-shaped failure).
- Text consumer: none needed; the SSE framer is a refactor with the existing
  808-plus tests behind it. Contract test: the byte-split invariance suite runs
  against all three framers.

### B2. Credential styles (`ProviderConn.auth_scheme`)

- `auth_scheme: Literal["bearer", "x-api-key", "raw", "header"]` with
  `auth_header: str | None`: bearer (OpenAI, DeepSeek, Inworld, OpenRouter),
  `x-api-key` plus `anthropic-version` (Anthropic), raw key in `Authorization`
  (AssemblyAI), named header (ElevenLabs `xi-api-key`). `build_headers`
  branches on it; `NEVER_FORWARDED` is untouched.
- Scrub widening: providers whose keys are reversible or echoed
  (`scrub_error_bodies="auth" | "all"`) get every non-2xx body replaced with
  the gateway's own error, not only 401/403. Inworld and the OpenAI audio
  endpoints echo key fragments; both set `"all"`.
- Text consumer: no behaviour change for existing rows; unit tests per scheme.

### B3. Units and accounting

- `ModelSpec` gains `unit: Literal["tokens", "characters", "seconds"]` with
  the existing per-million-token fields meaning per-million-units, plus
  `audio_input_per_m`, `audio_output_per_m`, `cached_audio_input_per_m`,
  `cache_write_1h_per_m`, and a `tool_rates: Mapping[str, float]` for
  per-call server tools (web search at $10 per 1k).
- `Usage` gains `characters`, `seconds`, `audio_input_tokens`,
  `audio_output_tokens`, `cached_audio_input_tokens`, `cache_write_1h_tokens`,
  `reasoning_tokens`, `server_tool_calls: Mapping[str, int]`, with the
  two-flag exactness per kind. `accounting.cost` is the same dot product over
  more kinds. `metrics.TOKEN_KINDS` closed set grows by the same names; a new
  `llmgw_units_total{kind}` carries non-token units.
- Surfaces populate what they can today: OpenAI `prompt_tokens_details.
  audio_tokens`, `cache_write_tokens`, `completion_tokens_details.
  reasoning_tokens`, `audio_tokens`; Anthropic `cache_creation.ephemeral_1h_
  input_tokens`, `output_tokens_details.thinking_tokens`, `server_tool_use`,
  `usage.iterations[]`; DeepSeek `reasoning_tokens`.
- Text consumer: chat audio (`gpt-audio`) stops being billed at the text
  rate; Anthropic cache writes bill at 1.25x/2x. Unit tests with the real usage
  objects captured in the sweeps as fixtures.

### B4. Body caps and multipart

- `max_request_bytes` and `max_response_bytes` become per surface with a
  global default; `ServerConfig` gains `surface_limits: Mapping[str, Limits]`.
  Defaults: chat 4 MiB, Anthropic messages 32 MiB, OpenAI audio 25 MiB,
  AssemblyAI sync 40 MiB, DeepSeek vision 48 MiB, TTS buffered response 16
  MiB. The byte-bounded body read is unchanged.
- Multipart: a surface may declare `body="json" | "multipart" | "raw"`. For
  `multipart` the gateway forwards the client's `content-type` (boundary
  included) and the raw body, and reads `model` and `stream` from the form
  fields by a bounded streaming parse of the first fields only (no full
  decode). For `raw`, `model` comes from the query string or `X-Gw-Model`.
  `upstream.build_headers` stops forcing `application/json` for these.
- Retry rule: a multipart or raw body is buffered up to the cap so a
  pre-commit retry can re-send it; the cap is the memory bound per request.
- Text consumer: vision base64 and PDFs above 4 MiB on the chat and messages
  surfaces. Contract tests with 20 MiB bodies against the fakes.

### B5. Per-target request defaults

- `ModelSpec.request_defaults: Mapping[str, Any]` applied at the same rewrite
  point as `model`, only for keys the client did not send, only on JSON
  bodies. First uses: DeepSeek `thinking: {type: "disabled"}` for the cheap
  candidate role (or `reasoning_effort` per workload), Anthropic
  `output_config.effort` per workload, `safety_identifier` set to the tenant
  id on OpenAI.
- `X-Gw-Body-Modified: 1` already signals a rewrite; the capture record lists
  which keys were defaulted.
- Policy may override defaults per workload (`[workloads.x.request_defaults]`);
  the merge is client, then workload, then model, and the snapshot is
  immutable as before.

### B6. Budget profiles

- Named budget profiles in the policy file (`[profiles.tts]` first_event 2 s,
  progress 5 s, total 150 s; `[profiles.long_context]` total 600 s with the
  drain-grace check applied per profile) and `Surface.default_profile`. The
  drain arithmetic (`total <= grace`) is validated against the largest total in
  use, not only the global default.

### B exit

All tiers green; the byte-split suite passes on all three framers; the live
smoke adds a chat-audio usage assertion and a 10 MiB vision call; S2 and S8
re-run because the pump changed (the numbers must match 15 Sep within
noise). Deploy.

---

## Phase C: text surfaces

- **C1. `/v1/models`**: served from the catalog as an OpenAI-shaped list of
  catalog ids and their aliases, filtered by what the tenant's policy can
  route to; no upstream call. Anthropic `GET /anthropic/v1/models` likewise.
- **C2. `/anthropic/v1/messages/count_tokens`**: buffered JSON passthrough,
  no accounting, its own small body cap, counts under `llmgw_requests_total`
  with a `count_tokens` surface label.
- **C3. `/v1/embeddings`**: buffered JSON surface, token accounting from
  `usage.prompt_tokens`, `unit="tokens"`; `text-embedding-3-small` row.
- **C4. DeepSeek on the Anthropic surface and `/beta`**: two provider rows
  (`deepseek-anthropic`, `deepseek-beta`) and a `ProviderConn.path_prefix` so
  `join_url` emits `/beta/v1/chat/completions`; the cross-dialect rule in
  `policy.py` is unchanged, the new row simply makes DeepSeek a same-dialect
  candidate for Anthropic workloads. Verified live before merge (the
  `/beta/v1` join is an open question in the sweep).
- **C5. Anthropic surface live coverage**: `live/smoke.py` gains tool round
  trip, vision (base64 and url), PDF, structured outputs and adaptive thinking
  on Sonnet 4.6; parallel tool calls on OpenAI; a DeepSeek non-streaming
  keep-alive fake mode.

Contracts added: **C12** `/v1/models` never calls upstream and lists only
routable ids. Size: three to four days; every item independent.

---

## Phase D: voice HTTP surfaces

Goal: every voice product that is plain HTTP passes through with correct
framing, accounting and errors, verified against the fakes and live.

### D1. Fake voice upstreams

`fakes/upstream.py` gains an `audio` port with modes derived from the
measured shapes: `openai-tts-sse`, `openai-tts-raw` (chunked `audio/pcm`,
8 KB chunks, no usage), `openai-stt-sse` (CRLF frames, `usage.type=duration`),
`inworld-ndjson` (64 KB lines, RIFF on the first, `result.usage` on line one,
`usage: null` for empty text), `elevenlabs-raw` (`character-cost` header before
the body), `assemblyai-sync` (raw PCM in, JSON out with `audio_duration_ms`),
plus per-mode error variants (`inworld-400-code3`, `elevenlabs-403-voice`,
`assemblyai-403-ratelimit`). Every contract shape from P2 to P6 (commit,
stall, cancel, drain, backpressure) runs against these modes.

### D2. Surfaces

| Surface | Route | Framing | Body | Usage | Unit |
|---|---|---|---|---|---|
| `audio_speech` | `POST /v1/audio/speech` | `sse` when `stream_format=sse`, else `raw` | json | `speech.audio.done.usage`; raw mode estimates from input characters, `cost_basis=estimated` | tokens |
| `audio_transcription` | `POST /v1/audio/transcriptions`, `/translations` | `sse` when `stream=true`, else buffered | multipart | `usage.type` tokens or duration (seconds, rounded up as the provider does) | tokens or seconds |
| `inworld_tts` | `POST /inworld/tts/v1/voice`, `:stream` | `jsonl` / buffered | json | `result.usage.processedCharactersCount` from the first line; empty text is a client-fault outcome with zero cost | characters |
| `elevenlabs_tts` | `POST /elevenlabs/v1/text-to-speech/{voice_id}[/stream]` with query passthrough | `raw` / buffered; `with-timestamps` as `jsonl` | json | `character-cost` response header | characters |
| `assemblyai_sync` | `POST /assemblyai/transcribe` | buffered | raw | `audio_duration_ms` | seconds |

Path templating: routes may carry `{voice_id}`-style params validated by the
surface; the query string is forwarded verbatim except for credential-looking
keys (`token`, `key`, `api_key`), which are refused.

Each surface implements the same protocol as today: `parse_request` (model,
stream, size estimate), `classify`, `apply_usage`, `native_ending`,
`error_from_event`, plus `framing`, `body`, `default_profile`.

### D3. Catalog rows and provider rows

`openai.gpt-4o-mini-tts` (tokens, audio out rate), `openai.gpt-transcribe`
(seconds), `openai.whisper-1` (seconds), `inworld.tts-2`, `inworld.tts-2-flash`
(characters; plan-dependent rate recorded as on-demand, `priced_at`),
`elevenlabs.flash-v2-5`, `elevenlabs.v3-conversational` (characters, per-plan
concurrency), `assemblyai.sync` (seconds). Provider rows for Inworld (bearer),
ElevenLabs (`xi-api-key`, India residency base), AssemblyAI (raw key,
`forbidden_means="rate_limit"`).

### D4. Errors

Body readers for ElevenLabs `detail.*` and Inworld gRPC-status JSON;
provider-scoped 403; Inworld unknown model 400 code 3 and unknown voice 404
code 5 map to `ModelNotFound`; ElevenLabs `voice_access_denied` and
`model_access_denied` map to a policy-scoped class that never touches the
credential breaker.

### D5. Verification

Contract suite against D1 for every surface; `live/smoke.py` gains one call
per surface with the real providers (Inworld and OpenAI keys exist; ElevenLabs
and AssemblyAI when keys exist), asserting framing, usage exactness and cost
within 1% of the provider's meter; a paired latency run per surface in
`bench/live_fly.py` (TTFB direct vs gateway, added bytes per second of audio);
S3 and S5 re-run with the raw framer because backpressure on 64 KB/s streams
is the new worst case.

Contracts added: **C13** a voice stream cut after commitment ends by close
with no fabricated frame; **C14** character and second usage are exact when
the provider reported them and `estimated` otherwise. Size: one to two weeks.

### D exit

The Layrs simulated learner and the batch article narration can use
`/inworld/tts/v1/voice` through the gateway; the OpenAI TTS fallback plugin,
if pointed at the gateway, gets a proper stream instead of a cut. Production
Inworld WebSocket traffic still bypasses the gateway, by design until G.

---

## Phase E: token minting

Goal: the gateway is the place a browser or device gets a short-lived
provider credential, and therefore the place admission and configuration
apply before any media flows.

- `POST /v1/realtime/client_secrets` (OpenAI): buffered JSON; the gateway
  merges the tenant's pinned `session` (model, voice, tools, turn detection,
  `max_output_tokens`, `OpenAI-Safety-Identifier` = tenant) over the client's
  request, caps `expires_after.seconds`, records a capture entry
  (`kind=mint`), and returns the provider's `ek_` unchanged.
- `GET /assemblyai/v3/token`: same shape; `max_session_duration_seconds` is
  capped at the drain grace of the deployment that will carry the session
  (today that is not this gateway; the cap protects the provider bill).
- ElevenLabs single-use tokens and Inworld `token:generate` (HMAC with a
  key-secret pair, so a second credential type `hmac_pair`) when those
  providers are in use.
- Admission: mints count against the tenant's rate and a new
  `max_sessions` limit; the capture record ties a session to a tenant, which
  is the only cost attribution possible for media that never transits the
  gateway.

Contracts added: **C15** a mint never returns a long-lived credential and
never widens the pinned session config. Size: three to four days.

---

## Phase F: Responses API

`POST /v1/responses` as a first-class surface: semantic SSE events
(`response.*`), `response.failed` and `response.incomplete` as the native
ending forwarded when upstream sent them (the P1 note already says so),
`previous_response_id` and `conversation` as passthrough with the caveat that
state lives at the provider, hosted tools and reasoning items as passthrough
with `usage.output_tokens_details.reasoning_tokens` accounted (B3), and
`background: true` refused with a clear error until polling routes exist.
Model aliasing (A1) matters most here because the Responses SDK echoes ids.
DeepSeek's stateless Responses endpoint rides the same surface. Size: one to
two weeks; independent of D and E.

---

## Phase G: the WebSocket data plane (scope only; its own plan)

This is the only route by which the production voice path ever fronts the
gateway, and it is a second program: nothing in the HTTP pump applies, while
deadlines, admission, breakers, credential scoping, capture and drain do.
What its plan has to decide, with the sweep facts that constrain each:

- **Transport.** An ASGI `websocket` route per provider product, an upstream
  client (`websockets` or `httpx-ws`, added to the lock), frame relay in both
  directions with per-direction byte bounds. Inworld and ElevenLabs multiplex
  contexts on one socket, so one client connection may map to one upstream
  context, not one upstream socket. Inworld returns 101 before authenticating
  and rejects in-band, so the connect budget ends at the first frame, not the
  handshake.
- **Clocks.** Session total in hours (AssemblyAI 3 h, Realtime 60 min) beside
  per-response first-event and progress budgets; the drain arithmetic must
  hold per session profile, and the drain forwards a close (or AssemblyAI
  `Terminate`, Inworld `closeStream`) and waits for the provider's termination
  message instead of cutting.
- **Progress and blame.** For STT, inbound audio is progress and a silent user
  is not a stalled provider; AssemblyAI's `realtime_factor` and audio-sent
  counters decide blame. For TTS, `audioChunk`/`audio` frames are progress,
  `Heartbeat`/`ping` liveness, everything else META.
- **Commitment.** Per response (OpenAI Realtime), per context (Inworld,
  ElevenLabs) or per session (AssemblyAI). No post-commit fallback, as today,
  but the reason differs: replaying buffered audio to a second provider is
  possible and is refused by policy, not by impossibility.
- **Errors.** WebSocket close codes and in-band error frames as a third
  classification input beside status and body; OpenAI Realtime `error` events
  are non-fatal and must not end the session; AssemblyAI 1008 needs the
  `Error` frame to disambiguate; app-level ping/pong (ElevenLabs Agents) must
  be answered within the deadline.
- **Accounting.** Session seconds from the gateway's own clock as the
  estimated basis, exact when the provider's termination message carries the
  number; per-response token usage on Realtime; characters sent for TTS.
- **Load.** A new scenario family (S9 to S12): streams of 64 KB/s in each
  direction, thousands of idle sockets, mass disconnect, deploy under open
  sessions. The 10 Sep single-process wedge (mass disconnect plus a log
  burst) is exactly the shape this plane will produce; finding 41's fix must
  be verified here before anything else.

Start G only after D and E are live, because they build the fakes, the
units, the credential styles and the budget profiles G depends on.

---

## Cross-cutting, from day one

- **CI**: lint, unit, contract on every PR; `make probe` nightly; chaos on
  merge; a deploy job gated on all of them. This is the repo's first
  workflow and belongs in Phase A.
- **Every phase re-runs the scenarios its change touches**: pump changes
  (B1, D) re-run S2, S3, S5, S8; admission changes (E) re-run S6; anything
  touching the drain re-runs S8 in both arms.
- **Live smoke grows monotonically**; a phase is not done until its calls
  are in `live/smoke.py` and have run against the real provider.
- **Layrs-side fixes ride along** but are not gateway work: the deprecated
  Inworld TTS model and per-minute price rows, the AssemblyAI default model
  in the skeleton, the legacy OpenAI STT model, the duplicate ElevenLabs env
  name. They should land before Layrs points anything at the gateway so the
  first measurements compare like with like.
- **Deployment**: the pending dual-stack redeploy (`de35c06`) precedes Phase
  A's exit; each phase deploys once its live smoke passes; `LLMGW_MAX_STREAMS`
  is re-derived on the Fly VM size before the first Layrs consumer.

## Cut order if time runs short

G is already deferred. Then F (Responses) before E (minting) before C4
(DeepSeek extra rows) before D's ElevenLabs and AssemblyAI surfaces (no Layrs
consumer today) before D's Inworld and OpenAI surfaces (real consumers).
Never cut A or B3: A is what callers see in week one, B3 is what the bills say.

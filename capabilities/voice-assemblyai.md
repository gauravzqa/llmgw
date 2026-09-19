# Voice sweep: AssemblyAI vs llmgw (2026-09-16)

Docs read today (assemblyai.com/docs): `api-reference/streaming-api/streaming-api` (STR), `speech-to-text/universal-streaming` (US), `streaming/common-session-errors-and-closures` (ERR), `faq/how-does-automatically-scaling-concurrency-for-streaming-stt-work` (CONC), `getting-started/usage-limits` (LIM), `streaming/authenticate-with-a-temporary-token` (TOK), `streaming/endpoints-and-data-zones` (ZONES), `api-reference/transcripts/submit` (TX), `webhooks` (WH), `sync-stt` + `sync-stt/endpoints-and-data-zones` (SYNC), `llm-gateway/overview` (LLMGW), `streaming/guides/apply-llm-gateway-to-streaming` (LLMGW-STR), `/pricing` (PRICE). Not fetchable: `api-reference/sync-api/transcribe-live` (linked, 404 at the guessed paths), `api-reference/transcripts/sync`.

Repo: `/Users/sanjay/PREP/Evo/llmgw` @ `bc1d065`. Fit legend: **Proxyable today** (HTTP JSON/SSE; a new buffered or streaming surface with small changes), **Needs new transport** (WebSocket / bidirectional / long-lived binary — not possible in the current ASGI app), **Needs new accounting** (seconds, not tokens), **Not a gateway concern**.

## 1. Products and endpoints

| capability | AssemblyAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Streaming STT `wss://streaming.assemblyai.com/v3/ws` (+ `.us.` / `.eu.`) | STR, ZONES | WebSocket, binary audio in / JSON events out; data plane | **Needs new transport** | `server/app.py:238-239,286` only HTTP routes; no websocket route anywhere; `upstream.py:12,565-572` is `httpx.AsyncClient` (no WS client) | The product the Layrs voice agent actually uses |
| Temporary token `GET https://streaming.assemblyai.com/v3/token?expires_in_seconds=1..600&max_session_duration_seconds=60..10800` | TOK | REST JSON; control plane per session | **Proxyable today** (buffered) | a buffered surface like the non-stream chat path; response is `{"token"}` | One-time-use token; the natural place for a gateway to mint per-tenant tokens with a capped `max_session_duration_seconds` (= the drain grace) |
| Sync STT `POST https://sync.assemblyai.com/v1/transcribe` (`/transcribe` is an alias; + `.us.` / `.eu.`) | SYNC | HTTP, **multipart/form-data with an `audio` part** (raw PCM is allowed only as that part, with an optional `config` part), **`X-AAI-Model` mandatory**, ≤120 s audio, ≤40 MB, one JSON back, ~450 ms measured for 1.8 s of audio | **Built and working** (`assemblyai_sync`, 19 Sep 2026) | request body cap 4 MiB (`config.py:395`) vs 40 MB; body is binary not JSON (`app.py` reads `model` from a JSON body via `surfaces/base.py:295 require_model`) | Best fit for a gateway: request/response, deadline semantics unchanged, `first_event == total` |
| Pre-recorded `POST /v2/transcript`, `GET /v2/transcript/{id}`, `POST /v2/upload` (`api.assemblyai.com`, `api.eu.assemblyai.com`) | TX | REST JSON + binary upload; async job, poll or webhook; control-plane-ish | **Proxyable today** for submit/poll (buffered JSON); upload blocked by the 4 MiB cap | `config.py:395` | Polling loops through a gateway waste admission slots; webhook completion is the sane path |
| Webhooks (`webhook_url`, `webhook_auth_header_name/value`) | WH | outbound POST from AssemblyAI `{transcript_id,status}`, 10 s window, 10 retries, 4xx = permanent | **Not a gateway concern** (inbound receiver) | — | Fixed source IPs 44.238.19.20 (US) / 54.220.25.36 (EU) |
| LLM Gateway `POST https://llm-gateway.assemblyai.com/v1/chat/completions` (+ `.eu.`) | LLMGW | OpenAI-compatible chat; SSE streaming (OpenAI models); tools, structured outputs, prompt caching; 25+ models | **Proxyable today** as a `ProviderConn(kind="openai")` row | `catalog.py` provider table; `surfaces/openai.py` | A competitor gateway; proxying it would be double-gatewaying. Relevant only as a fallback provider row |
| LLM Gateway inside the streaming socket (`llm_gateway` query param → `LLMGatewayResponse` per turn) | LLMGW-STR, STR | JSON over the same WebSocket | **Needs new transport** | — | Usage per turn is `input_tokens/output_tokens/total_tokens` on the message; not reachable without WS |
| Audio intelligence add-ons (PII redaction, diarization, entity, topics, moderation) | TX, PRICE | flags on the transcript / stream request; per-hour add-on prices | rides whichever transport carries the call | — | Additive per-hour pricing the catalog cannot express |
| Voice Agent API ($4.50/hr all-inclusive STT+LLM+TTS) | PRICE | orchestration product | **Not a gateway concern** | — | Competes with the Layrs LiveKit stack, not a layer under it |

## 2. Streaming shape

| capability | AssemblyAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Client → server: binary frames of 50–1000 ms audio, real-time pace or slower; `pcm_s16le` default, `pcm_mulaw`, `opus`, `ogg_opus`, `aac`; `sample_rate` 8000–96000 | STR | binary WS | Needs new transport | `pump.py` is one-directional upstream→client (`pump.py:18,219 committed` is about the first byte *to the client*) | 3007 close on out-of-range chunk or over-real-time pacing |
| Client → server JSON: `UpdateConfiguration` (mid-session: prompt, keyterms, silences, mode, VAD, language_codes, heartbeat), `ForceEndpoint`, `KeepAlive`, `Terminate` | STR | text WS | Needs new transport | — | A gateway could inject/override these (e.g. force `Terminate` on drain) |
| Server → client: `Begin{id, expires_at, configuration}`, `SpeechStarted`, `Turn{turn_order, transcript, utterance, end_of_turn, turn_is_formatted, end_of_turn_confidence, words[]…}`, `SpeakerRevision`, `Heartbeat` (every 5 s if `session_heartbeat`), `LLMGatewayResponse`, `Termination{audio_duration_seconds, session_duration_seconds}`, `Error{error_code, error}` | STR, ERR | text WS | Needs new transport; the *event taxonomy* maps cleanly onto llmgw's classifier (`Begin`=headers-equivalent, `Turn`=CONTENT, `Heartbeat`=HEARTBEAT, `Termination`=TERMINAL, `Error`=in-stream error) | `surfaces/anthropic.py:118-140` event-name classification; `sse.py:59-73` comment→liveness rule | `Begin.configuration.model` must be checked against the requested `speech_model`: unknown params are silently ignored (STR) |
| Interim vs final | STR | `include_partial_turns` (default true), `continuous_partials` (~3 s cadence, Pro), `end_of_turn: true` marks final; `word_is_final` per word | — | — | Ordering by `turn_order`; partial `Turn`s are the "progress" signal |
| Session end | STR, ERR | client `Terminate` → `Termination`; server 3008 at 3 h max (or token's `max_session_duration_seconds`); `inactivity_timeout` 5–3600 s reset by `KeepAlive`/audio | — | — | Billing runs the whole open time (PRICE): an idle open socket costs money — Layrs already works around this (`harness/runner.py:978-984`) |
| Bidirectional | yes | — | Needs new transport | — | "Commitment" has no single definition: the client is committed the moment audio is sent; the upstream is committed on `Begin`. Fallback mid-session means re-sending buffered audio to a second provider, which llmgw's C2 explicitly forbids for LLM streams |

## 3. Latency knobs and metrics

| capability | AssemblyAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Models: `universal-3-5-pro` (default), `universal-streaming-english`, `universal-streaming-multilingual`; Slam-1 deprecated | STR, PRICE | query param | catalog row per model would be needed | `catalog.py:102-112` ModelSpec has token prices only | Layrs pins `universal-streaming-english` deliberately (`harness/livekit_setup.py:167-181`) |
| `mode` `min_latency | balanced | max_accuracy` (Pro) | STR | query | — | — | Trades EoT latency vs accuracy |
| End-of-turn: `end_of_turn_confidence_threshold` (0.4 default, Universal-Streaming only), `min_turn_silence` 50–10000 ms, `max_turn_silence` 1536 ms Pro / 1280 ms others, `vad_threshold`, `interruption_delay` 0–1000 ms (Pro), `ForceEndpoint` | STR | query / UpdateConfiguration | — | — | Layrs: threshold 0.7, min 160 ms, max 2400 ms (`framework/skeleton/session.py:116-121`) |
| Context: `prompt` ≤1750 chars, `keyterms_prompt` ≤100, `agent_context` (TTS reply), `previous_context_n_turns` 0–100 (Pro) | STR | query / UpdateConfiguration | — | — | `agent_context` is a per-turn write from the agent — inherently bidirectional |
| `Heartbeat.realtime_factor`, `total_audio_received_ms`, `total_duration_ms`; `Termination.audio_duration_seconds` vs `session_duration_seconds` | STR | server JSON | maps to llmgw's liveness/progress split; the two durations are the billing vs work split | `clocks.py:241` Budgets (connect/first_event/progress/client_stall) | `realtime_factor < 1` = client under-delivering = client fault, the same blame problem llmgw solved for LLM streams (findings 1–3) |
| First partial ≈ `interruption_delay` + 256 ms (Pro); provider markets ~300 ms EoT | STR, US | — | — | — | No per-request latency header |
| Sync STT: `request_time_ms`, `audio_duration_ms`, `session_id` in the response | SYNC | JSON | Proxyable; a `processing-ms`-style field llmgw could capture | — | ~134 ms p50 claimed for ≤2 min clips |

## 4. Auth and headers

| capability | AssemblyAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| `Authorization: <key>` (raw key, **no `Bearer`**) on REST and WS | STR, TX, LLMGW | header | `upstream.py:315-316 build_headers` emits `Bearer` for `kind="openai"`; a new `ProviderKind` or an `auth_scheme` field is needed | `catalog.py` ProviderConn has `kind` + `extra_headers` only | `extra_headers={"authorization": key}` is blocked by design (`NEVER_FORWARDED`, `config.py:109-117`), so this is a code change not config |
| `token=` query param (temporary, REUSABLE — not one-time, probe A5f; ≤600 s to first use, session cap ≤3 h, capped by `max_session_duration_seconds`) | TOK | WS query | Proxyable today (mint via buffered surface) | — | Credential leaves the gateway only as a short-lived token: the right shape for browser callers |
| `AssemblyAI-Version` header (optional pin; `Begin.configuration.api_version` echoes, e.g. `2025-05-12`) | STR | header | forward-list entry | `config.py:104` forward allowlist | Analogue of `anthropic-version` |
| Regions: global edge, `.us.`, `.eu.` for streaming, sync and REST; LLM Gateway EU lacks OpenAI models | ZONES, SYNC, LLMGW | base URL | one `ProviderConn` per region | — | Data-residency routing is a policy-file concern |
| Account-level limits shared across all keys of an account | LIM | — | maps to llmgw's credential-scoped breaker/limiter | `breaker.py` credential scope (FAILURE-MODES row 8) | Per-project isolation does not exist provider-side |

## 5. Errors, rate limits, concurrency

| capability | AssemblyAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Streaming: new sessions/minute, not concurrent sessions: free 5/min, paid 100+/min, auto-scales +10 % per minute at ≥70 % utilisation, no ceiling; scales back <50 % | CONC | WS admission | llmgw's `ProviderKeyLimiter` is a *concurrency* cap (FAILURE-MODES row 7); AssemblyAI's limit is a *rate* of opens | `admission.py` token bucket is per tenant, not per credential | A gateway would need a per-credential opens-per-minute bucket that tracks the auto-scaling limit |
| Over the opens limit: close **1008** "Unauthorized connection: Too many concurrent sessions" (also **3009** in ERR) | CONC, ERR | WS close | classify as RateLimited (NEUTRAL, retry with backoff); no `Retry-After` anywhere | `errors.py:890` RateLimited | Same code 1008 is used for bad/missing auth → a body-sniff on the `Error` frame is required, like the 400/429 rules in `errors.py` |
| `Error` text frame `{type:"Error", error_code, error}` precedes close; close reason is truncated to 123 bytes | ERR | WS | the analogue of in-stream `error` events (`surfaces/anthropic.py:236-260`) | — | Read the frame, not the close reason |
| Close codes: 1008 auth/account/limit; 1011 internal; 3005 unknown server error (retry); 3006 invalid message / inactivity; 3007 chunk duration or pacing violation; 3008 max session (3 h / token cap); 3009 too many sessions; 410 deprecated endpoint | ERR | WS | mapping: 1011/3005 → UpstreamServerError (FAILURE health, retry); 3006/3007 → client fault (NEUTRAL); 3008 → deadline-shaped, expected; 1008/3009 → RateLimited or AuthenticationFailed by message; 410 → PolicyError (config drift) | `errors.py` taxonomy | 3006 doubles as "inactivity timeout" — again message-sniff |
| REST: 400/401/429/500/503 on transcripts; HTTP rate limit 20,000 requests / 5 min → **403**; parallel-job limit (free 5, paid 200+) → jobs are **queued FIFO**, not rejected; balance < 0 → limit reduced to 1 | TX, LIM | HTTP | 403-as-rate-limit collides with `errors.py:892` (403 → AuthenticationFailed, credential breaker) | `errors.py:890-896` | Exactly the misclassification the OpenAI sweep flagged for 403, with worse consequences here: a burst of polling opens the credential breaker |
| Pre-recorded queueing under the job cap | LIM | — | invisible to a caller except as long poll times; llmgw's `total` budget would fire on a proxied poll loop | `config.py:358-361` budgets | Webhook path avoids it |
| Sync STT errors | SYNC | HTTP | not documented in the fetched pages (413 for >40 MB / >120 s presumed) | — | Unknown; test: 121 s clip |

## 6. Usage and billing units

| capability | AssemblyAI (doc) | protocol/unit | llmgw fit | evidence | note |
|---|---|---|---|---|---|
| Streaming: billed on **session-open wall time**, per second, no minimums: Universal-3.5 Pro Realtime $0.45/h, Universal-Streaming English/Multilingual $0.15/h | PRICE, STR | seconds of connection | **Needs new accounting**: `TOKEN_KINDS = (input, output, cache_read, cache_write)` and `ModelSpec.*_per_m` are token-only | `accounting.py:97,292-293`; `catalog.py:110-112` | Usage per session is on `Termination.session_duration_seconds`; if the socket dies without it, the gateway must bill from its own clock (an `estimated` basis, like a cut LLM stream) |
| Pre-recorded: per hour of **audio**, per second: Universal-3.5 Pro $0.21/h, Universal-2 $0.15/h; add-ons stack (+$0.02 diarization … +$0.15 medical/topics/moderation) | PRICE | audio seconds | Needs new accounting (`audio_per_hour` + additive add-on rates) | — | `audio_duration` is on the transcript JSON |
| Sync STT: $0.45/h of audio | SYNC | audio seconds | Needs new accounting; the unit is in the response (`audio_duration_ms`) | — | Same rate as Pro Realtime |
| LLM Gateway: per-1M-token prices (e.g. Haiku 4.5 $1/$5, GPT-5 Nano $0.05/$0.40, Gemini 3.5 Flash $1.50/$9) | PRICE | tokens | fits `ModelSpec` unchanged | `catalog.py:110-112` | Prices differ from going direct (Haiku 4.5 matches Anthropic list; others are AssemblyAI's own) |
| Usage visibility: streaming usage populates on the dashboard only after the session closes; no per-request usage header | LIM, STR | — | the gateway's own clock is the only real-time source | — | Argues for gateway-side session accounting |
| Free credit $50; EU = US pricing | PRICE | — | — | — | — |

## 7. Layrs usage today

| capability | where | what it depends on | note |
|---|---|---|---|
| AssemblyAI is a **fallback STT**, not production: production is Inworld STT+TTS since 2026-06-22; AssemblyAI is chosen only when `stt_provider="assemblyai"` or Inworld construction fails | `harness/evals/pricing.py:128-131`; `harness/livekit_setup.py:184-234`; `framework/skeleton/session.py:128-135` | `ASSEMBLYAI_API_KEY` env; `livekit-plugins-assemblyai==1.6.3` (`voice-agent/requirements.txt:9`) | The upgrade notes flag that a silent Inworld failure degrades to AssemblyAI without a hard fail (`docs/livekit-1.6.3-upgrade-findings.md:846-847`) |
| Construction: `assemblyai.STT(model="universal-streaming-english", end_of_turn_confidence_threshold=0.7, min_turn_silence=160, max_turn_silence=2400)`; the skeleton path omits `model=` and inherits the plugin default | `harness/livekit_setup.py:167-181`; `framework/skeleton/session.py:111-124` | Streaming WS v3, EoT tunables (§3) | Plugin 1.6.2+ default is `universal-3-5-pro` ($0.45/h) — the skeleton path pays 3× the labelled rate |
| Billing model understood: WS-open time, idle included; `SttIdleManager` terminates the socket in "away" windows and tallies `stt_billed_seconds` | `harness/runner.py:121-133, 978-984, 1071-1073` | `Termination`/socket lifetime | This is exactly the accounting a gateway would have to own if it sat in the path |
| Cost table: `assemblyai.universal-streaming` at $0.15/h, dated 2026-06-03, labelled legacy | `harness/evals/pricing.py:135-147` | PRICE | Still correct for the pinned model; no row for `universal-3-5-pro` |
| Turn detection lives in LiveKit (`turn_handling`, multilingual model, preemptive generation off), not in AssemblyAI's EoT alone | `harness/livekit_setup.py:237-251` | — | A gateway in the STT path must not add jitter to `Turn` delivery; EoT timing is the product |
| No use of pre-recorded, sync, temporary tokens, webhooks or LLM Gateway | grep | — | The Sync API would fit the DSA/eval offline paths better than a WS if those ever need STT |

## What llmgw has and lacks for this provider

Transfers unchanged, conceptually: the error taxonomy's two-axis retry/health split (a 3007 pacing violation is the client's fault and must not open the breaker, the same rule as findings 1–3); credential-scoped breakers (AssemblyAI limits are per account); the heartbeat-vs-progress clock distinction (`Heartbeat`/`SpeechStarted` are liveness, `Turn` is progress); admission per tenant; bounded capture; drain semantics (a `Terminate` sent to every open socket on SIGTERM, with the grace sized to the session cap).

Needs redefinition for a bidirectional STT socket: **first event** = `Begin` (not the first transcript; silence is legal); **progress** = any `Turn` *or* client audio still flowing (a silent user is not a stalled provider — `Heartbeat.realtime_factor` is the discriminator); **commitment** = `Begin` received *and* audio forwarded; no post-commit fallback is possible without replaying buffered audio, so the C2 rule holds but for a different reason; **total** = the session cap, hours not seconds (`LLMGW_BUDGET_TOTAL` defaults to 120 s and the drain grace must exceed it — a 3 h session breaks the deploy arithmetic outright).

Cannot be done in the current app at all: anything over `wss://` (Starlette supports WebSocket routes, but nothing in `pump.py`, `upstream.py` (httpx has no WS client), `sse.py` or the surfaces is bidirectional or binary-aware).

## What llmgw would need

1. A WebSocket surface: ASGI websocket route, an upstream WS client (`websockets` or `httpx-ws`), a two-way pump with per-direction byte bounds, close-code passthrough, and `Error`-frame classification. New module family, not an extension of `pump.py`.
2. A binary buffered surface for Sync STT: raw-body passthrough with `content-type` preserved, model from the query string or a gateway header rather than a JSON body, per-surface `max_request_bytes` (40 MB here).
3. `ProviderConn.auth_scheme` (`bearer | raw | x-api-key`) — AssemblyAI wants the bare key in `Authorization`.
4. Accounting by time: `ModelSpec.audio_per_hour`, `session_per_hour`, additive add-on rates; a `seconds` usage kind alongside the four token kinds; `cost_basis=estimated` when `Termination` never arrives and the gateway bills from its own clock.
5. Per-credential opens-per-minute limiter tracking AssemblyAI's auto-scaling rule, distinct from the concurrency cap.
6. Error rules: WS close codes and the `Error` frame body; REST 403-as-rate-limit for `api.assemblyai.com` (provider-specific override of the 403 → auth rule).
7. Budgets in hours for STT workloads and a drain that sends `Terminate` and waits for `Termination`, with `LLMGW_DRAIN_GRACE` reconciled against the session cap (or tokens minted with `max_session_duration_seconds` ≤ grace).
8. A token-minting surface (`GET /v3/token`) so browsers never see the credential and every session carries a gateway-chosen duration cap.
9. Catalog rows for `universal-3-5-pro`, `universal-streaming-english`, `universal-streaming-multilingual`, `universal-2`, sync; regional provider rows.

## Gaps ranked (impact on the Layrs voice agent)

1. **No WebSocket transport.** The only AssemblyAI product Layrs uses cannot pass through llmgw at all; the LiveKit plugin talks to `streaming.assemblyai.com` directly with the raw key.
2. **Billing is connection-time, not tokens or audio.** Even with a transport, `llmgw_cost_usd_total` would read zero for STT; the idle-socket cost that `SttIdleManager` exists to control would be invisible.
3. **Session length vs deploy arithmetic.** A 3 h STT session against a 130 s drain grace means every deploy cuts every call; the token endpoint's `max_session_duration_seconds` is the only lever that reconciles them.
4. **403 means "rate limited" on AssemblyAI REST** and "bad credential" in `errors.py:892`: a polling burst would open the credential breaker for every tenant.
5. **1008 is overloaded** (auth failure *and* too-many-sessions); without `Error`-frame sniffing a capacity blip looks like a revoked key.
6. **Model default drift in the skeleton path**: `framework/skeleton/session.py:116` omits `model=`, so plugin 1.6.2+ silently runs `universal-3-5-pro` at $0.45/h while cost labels say `universal-streaming` at $0.15/h. (Layrs-side, not gateway-side; `harness/livekit_setup.py` already pins it.)
7. **Progress semantics inverted**: a silent learner would trip a 15 s progress budget; the gateway must treat client-side audio flow and `Heartbeat` as liveness and only `Turn` as progress, with `realtime_factor` deciding blame.
8. **Sync STT is the one AssemblyAI product that fits the gateway well** (HTTP request/response, sub-second, priced per audio second) and it is blocked only by the 4 MiB body cap, the JSON-body model lookup and the auth scheme. If any Layrs path ever needs non-live STT, this is the entry point.

## Doc deltas

- `harness/evals/pricing.py:135` AssemblyAI $0.15/h dated 2026-06-03: still the list price for `universal-streaming-english`/`-multilingual` (PRICE); no row exists for `universal-3-5-pro` ($0.45/h), which the skeleton path can select by default.
- `framework/skeleton/session.py:116-121` constructs `assemblyai.STT` without `model=`; the livekit 1.6.3 findings (`docs/livekit-1.6.3-upgrade-findings.md:471-540`) call this out and `harness/livekit_setup.py:174` fixed it; the skeleton did not follow.
- `min_end_of_turn_silence_when_confident` is a deprecated alias of `min_turn_silence` (STR lists only `min_turn_silence`); the harness already renamed, the upgrade notes say the repo "emits this warning today" — verify no remaining callers (`harness/dsa/runner.py` ~L260 per the upgrade plan).
- `end_of_turn_confidence_threshold` applies to Universal-Streaming models only (STR); if the model is ever switched to `universal-3-5-pro`, the Layrs EoT tuning (0.7 / 160 / 2400) partly stops applying and `mode` + `interruption_delay` become the knobs.
- No AssemblyAI references in the repo point at deprecated endpoints (v2 realtime is gone; the plugin uses v3).


---

## Corrections applied 19 Sep 2026

Everything in this file above was a documentation sweep with no key. A live
key exists now, and `capabilities/captures-sarvam-assemblyai.md` §1 is the
authority where the two disagree. The claims disproved, in the order they
appear:

1. **§1 "raw PCM body".** A raw body is a 415. The body is
   `multipart/form-data` with a part named `audio`; raw PCM may be that
   part's payload, with an optional `config` part
   `{"sample_rate":16000,"channels":1}`.
2. **§1 path.** `/v1/transcribe`, not `/transcribe` — though the short form
   is a live alias and both answer 200.
3. **§1 said nothing about `X-AAI-Model`,** which is MANDATORY and is read by
   the AWS load balancer: without it, or with a model this host does not
   serve (`universal-2` is one), the reply is `404 Not found`, `text/plain`,
   `server: awselb/2.0`, and no application code runs.
4. **The 403-means-rate-limit rule** on the sync host describes a response it
   does not send. A bad key there is a **404** with
   `application/problem+json` and `detail: "Invalid API key"`. The
   `assemblyai-sync` row now says `forbidden_means="auth"` and `errors.py`
   reads that body. On `api.assemblyai.com` a bad key is a plain 401; the
   403 rate limit remains documented-only and is marked unverified in the
   catalog.
5. **§4 "one-time" token.** Not one-time: the same token opened a second
   session cleanly (probe A5f).
6. **§5 "Sync STT errors … not documented".** All five are captured and are
   RFC 7807 (`{status,title,detail}` under `application/problem+json`); they
   are fixtures in `tests/unit/test_real_error_bodies.py`.
7. **Billing.** Sync reports `audio_duration_ms` in EXACT milliseconds (1840
   for a 1.84 s file); the async product reports `audio_duration` in whole
   seconds rounded up. Two products, two rounding rules, one price table.
8. **No TTS.** AssemblyAI has no text-to-speech product; `/v2/tts` and
   `/v1/speech` both 404.
9. **§6 pricing** ($0.45/h sync) is still correct at 2026-09-18.

# Capability sweep: OpenAI API vs llmgw (2026-09-16)

Repo: `/Users/sanjay/PREP/Evo/llmgw` @ `p0-drain-shed-cap`. Docs read today; `platform.openai.com/docs/*` now 301s to `developers.openai.com/api/docs/*` (the api-reference pages on the old host return 403). Status legend: **Supported** = gateway parses/accounts/classifies/routes it; **Passthrough** = forwarded byte-for-byte and works, gateway blind to it; **Unsupported** = no route / stripped / rejected; **Unknown** = needs a test.

Ground truth for "works": `bench/results/live_smoke.md` (11 Sep: tools, tool round-trip, vision data-URL, json_object, cancel, bad key, ghost model) and `bench/results/live-fly-20260916.md` (16 Sep: 122/122 OK incl. vision by URL, tool_call, json_mode, multi-turn, `stream_options.include_usage`).

## 1. Endpoints

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| `POST /v1/chat/completions` | developers.openai.com/api/docs/api-reference/chat/create | **Supported** | `server/app.py:238` `ROUTE_TO_UPSTREAM_PATH`; `surfaces/openai.py:46` | Streaming + buffered |
| `POST /v1/responses` | …/api-reference/responses/create | **Supported** (streaming + buffered; PLAN-2 Phase F, 18 Sep 2026) | `surfaces/responses.py:232` `OpenAIResponsesSurface` (`routes` :238, `upstream_path` :239); `app.py:305` `UNIMPLEMENTED_ROUTES` now empty; live smoke `responses stream` / `responses buffered` PASS 18 Sep | `background: true` → **400 at the gateway**, no upstream call (`responses.py:272-278`, CONTRACTS.md C22); buffered `model` is rewritten to the catalog id (A1, `x-gw-body-modified: 1`); wire shapes in `capabilities/captures-responses.md` |
| `GET /v1/models` | …/api-reference/introduction (nav) | **Unsupported** (404) | no route in `app.py:238-296`; `/workloads/<w>/probe` is the gateway's own catalog view | SDKs call this rarely; some agent frameworks call it at boot |
| `/v1/embeddings` | api-reference nav | **Unsupported** (404) | `live_smoke.md:19` | Non-streaming, cheap to add as a buffered surface |
| `POST /v1/images/generations` | …/api-reference/images/create | **Supported** (buffered + SSE, 20 Sep 2026) | `surfaces/images.py` `ImagesGenerationsSurface`; catalog `openai.gpt-image-1` / `openai.gpt-image-1-mini`; live `live/smoke_images.py` 5/5 PASS 20 Sep | Billed in TOKENS and always `exact` -- the endpoint reports `usage {input_tokens, output_tokens, *_details}` on both forms. **`dall-e-3` and `dall-e-2` are RETIRED** (400 `The model 'dall-e-3' does not exist.`, absent from the pricing page), so no image model is priced per image and the catalog needs no `images` billing unit. Stream is `image_generation.partial_image` x N then `image_generation.completed` -- **no `data: [DONE]`**, so the completed event is the terminal marker. Partials are billed (+~100 output tokens each). Per-surface caps: 1 MiB request, 32 MiB response, 8 MiB frame (one partial frame measured 1.70 MB, over the 1 MiB global) |
| `POST /v1/images/edits` | …/api-reference/images/createEdit | **Unsupported** (404) | no route; `surfaces/images.py` module docstring | Deliberate, not an oversight: it bills input images at their own rate ($10/1M on `gpt-image-1` against $5/1M for text) as `input_tokens_details.image_tokens`, and neither `ModelSpec` nor `metrics.TOKEN_KINDS` has a rate or a label for that kind. Mounting it without them would bill every uploaded image at half price silently. On a *generation* that field is always 0, so the gap is unreachable from the route that IS built |
| `POST /v1/images/variations` | …/api-reference/images/createVariation | **Unsupported** (404) | no route | `dall-e-2`-only per the API reference, and `dall-e-2` is retired -- there is nothing left to route it to |
| `/v1/audio/*` (speech, transcriptions, translations) | api-reference nav | **Unsupported** | no route | multipart uploads; different body shape |
| `/v1/files`, `/v1/uploads`, `/v1/batch`, `/v1/vector_stores`, `/v1/fine_tuning`, `/v1/moderations`, `/v1/conversations`, `/v1/containers`, `/v1/evals` | api-reference nav | **Unsupported** | no route | Control-plane-ish; not a streaming-gateway concern except Batch cost accounting |
| Realtime (WebRTC/WebSocket/SIP) | …/api-reference/realtime | **Unsupported** | HTTP-only ASGI app; no websocket route | Out of scope by architecture |
| Webhooks (inbound events) | api-reference nav | **Unsupported** | — | Would need a receiver; not a proxy concern |
| `/workloads/{w}/v1/chat/completions` (gateway-native workload routing) | n/a | **Supported** | `app.py:252` | Gateway addition, not OpenAI |

## 2. Chat-path request features

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| `stream: true` | …/guides/streaming-responses | **Supported** | `surfaces/base.py:304` `read_stream`; pump path | Strict bool; `"stream":"yes"` is `InvalidRequest` |
| `stream_options.include_usage` | …/api-reference/chat/create | **Supported** | `openai.py:71-79` (`include_usage` fact), `openai.py:167-222` `apply_usage` | Without it `Usage.exact` stays False → `cost_basis=estimated` (documented) |
| `stream_options.include_obfuscation` | chat/create | **Passthrough** | frames carry an extra `obfuscation` field; `classify` ignores unknown keys `openai.py:91-118` | Harmless; adds bytes to `max_frame_bytes` budget |
| `tools` / `tool_choice` (none/auto/required/specific/`allowed_tools`) | …/guides/function-calling | **Passthrough** (works) | `_PROGRESS_KEYS` includes `tool_calls` `openai.py:43`; `live_smoke.md:13-15`; live-fly `tool_call_1/2` | Gateway does not count tool-call output separately; `finish_reason=tool_calls` not surfaced in accounting outcome |
| `parallel_tool_calls` (multi-index deltas) | function-calling | **Unknown** | `live_smoke.md:91` "not covered: parallel tool calls (more than one index)" | Passthrough should work; test: force 2 calls, assert both indexes reach client and progress clock holds |
| `strict: true` on functions | function-calling | **Passthrough** | body untouched except `model` (`app.py:1729`) | — |
| Custom tools (freeform, Lark/regex grammars) | function-calling | **Passthrough** | — | Responses-first feature; on chat only if provider accepts |
| `functions` / `function_call` (deprecated) | chat/create | **Passthrough** | `_PROGRESS_KEYS` includes `function_call` `openai.py:43` | Deprecated upstream, still classified as progress |
| `response_format: json_object` | …/guides/structured-outputs | **Passthrough** (works) | `live_smoke.md:20`; live-fly `json_mode` | Docs: json_object now "deprecated in favor of Structured Outputs" |
| `response_format: json_schema` (+`strict`) | structured-outputs | **Passthrough** | body untouched | Not live-tested; `refusal` deltas count as progress (`openai.py:43`), `text_delta` returns None for them (`openai.py:143`) |
| Vision `image_url` (https URL) | …/guides/images-vision | **Passthrough** (works) | live-fly `vision` 4/4 OK, `detail: low` | 2,847 prompt tok/image billed; cached tokens handled |
| Vision `image_url` (base64 data URL) | images-vision | **Passthrough** (works) | `live_smoke.md:18` | **4 MiB request cap** `config.py:395` `max_request_bytes` vs OpenAI's 512 MB/request, 1,500 images → large base64 payloads get 413 |
| Vision `detail: original` / `file_id` image inputs | images-vision | **Passthrough** | — | `file_id` requires Files API, which the gateway does not proxy → client must upload direct |
| Audio in (`input_audio`) / audio out (`modalities`, `audio`) | chat/create | **Passthrough, accounting blind** | `apply_usage` reads only `prompt_tokens`/`completion_tokens`/`cached_tokens` `openai.py:196-204`; `catalog.ModelSpec` has no audio rate `catalog.py:110-121` | Audio tokens priced differently → **mis-costed** if used |
| File / PDF input (`type: "file"`) | chat/create lists it; images-vision guide silent | **Passthrough** | — | Same 4 MiB cap applies to `file_data` |
| `reasoning_effort` (none/minimal/low/medium/high/xhigh/max) | …/guides/reasoning | **Passthrough** | `catalog.ModelSpec.reasoning: Literal["off","low","high","xhigh"]` `catalog.py:120` | Catalog vocabulary lacks `minimal`/`medium`/`max`; gpt-6-astra 400s on `none` — passes through as `InvalidRequest` |
| Reasoning tokens in usage (`completion_tokens_details.reasoning_tokens`) | reasoning | **Unsupported (blind)** | not read in `apply_usage`; `live_smoke.md:69-77` shows the consequence (deepseek-flash spent whole budget on reasoning) | Cost is still right (billed as output, included in `completion_tokens`) but no metric/record splits it |
| Reasoning summaries / `encrypted_content` | reasoning | **Passthrough** (Responses) | `responses.py:89-99` `CONTENT_TYPES`: `response.reasoning_summary_text.delta` and `response.reasoning_text.delta` reset the progress clock; `reasoning` output items with `encrypted_content` are META and forwarded byte-for-byte; `usage.output_tokens_details.reasoning_tokens` recorded (`responses.py:427-439`) | `openai.gpt-5-nano` row shipped (`catalog.py:516`, priced 2026-09-18); live `responses reasoning (gpt-5-nano)` PASS with `reasoning_tokens=0` at effort low (probe 4: 0 is a legitimate value) |
| `max_completion_tokens` vs `max_tokens` | chat/create | **Supported** | `openai.py:74-77` prefers `max_completion_tokens` | Used only for deadline sizing; both forwarded verbatim |
| `verbosity` | chat/create | **Passthrough** | — | — |
| `n > 1` | chat/create | **Passthrough, partial** | `text_delta` returns first choice only `openai.py:146-150`; `classify` any-choice `openai.py:114` | Transcript capture is choice 0 only; billing correct (usage is aggregate) |
| `logprobs` / `top_logprobs` | chat/create | **Passthrough** | — | Larger frames; within 1 MiB frame cap |
| `seed`, `stop`, `temperature`, `top_p`, penalties, `logit_bias` | chat/create | **Passthrough** | — | `stop` unsupported on o3/o4-mini → provider 400 → `InvalidRequest` (`errors.py:898-910`) |
| `metadata`, `store` | chat/create | **Passthrough** | — | `store=true` stores under the *provider* project; the gateway rewrites `model` before storing (see §"rewrite" below) |
| `user` (deprecated) / `safety_identifier` | chat/create | **Passthrough** | — | Gateway does not inject its tenant id as `safety_identifier`; could (per-tenant abuse attribution at OpenAI) |
| `prompt_cache_key`, `prompt_cache_options{mode,ttl}`, `prompt_cache_retention` | …/guides/prompt-caching | **Passthrough** | — | Gateway does not set a per-tenant `prompt_cache_key`; with one shared key all tenants share cache routing (fine) |
| Cached-token accounting (`prompt_tokens_details.cached_tokens`) | prompt-caching | **Supported** | `openai.py:196-212` subtracts cached from prompt (disjoint buckets) | Correct per docs ("cached tokens are a subset of total input tokens") |
| Cache **write** tokens (`prompt_tokens_details.cache_write_tokens`, billed 1.25×) | prompt-caching; chat/create usage | **Unsupported (blind)** | `openai.py:186-189` "chat API's caching … reports no creation count" — **no longer true** | Under-bills explicit-cache writes on GPT-5.6+ |
| `service_tier` (auto/default/flex/scale/priority/fast) | chat/create | **Passthrough** | — | flex = slower first token; default `first_event` budget is 20 s (probe output) → flex may trip `FirstEventTimeout`; needs per-workload budget |
| `prediction` (predicted outputs) | chat/create | **Passthrough** | — | `accepted/rejected_prediction_tokens` not split; cost still inside `completion_tokens` |
| `web_search_options` / built-in hosted tools | chat/create; responses | **Passthrough** (chat) / **Supported, counted** (Responses tools) | `responses.py:112-126` `SERVER_TOOL_KEYS`; stream: `response.<tool>_call.completed` events → `Usage.server_tool_calls` (`responses.py:146-153`, `:370-372`); buffered: `output[]` items (`:397-404`); `response.tool_usage.web_search.num_requests` is authoritative when present (`:155-165`, `:405-417`) | Hosted-tool calls now land on the record as `server_tool_calls` and on `llmgw_server_tool_calls_total{web_search_requests}` (live `responses web-search` PASS, metric +1, `num_requests=1`). Priced only where `ModelSpec.tool_rates` has a rate — the OpenAI rows have none yet, so OpenAI web search is counted but still not costed |
| `moderation` param / `moderation` response field | chat/create | **Passthrough** | — | New; unknown keys ignored |
| `background: true`, `previous_response_id`, `conversation` | responses/create | `background: true` **Unsupported** (400 at the gateway, no upstream call); `previous_response_id`, `conversation` **Passthrough** | `responses.py:272-278` refuses background in `parse_request`; `:128` `STATE_KEYS`, `:288` `needs_state`; `check_target` `:291-313` refuses state only for a `stateless_responses` provider (DeepSeek), OpenAI holds it | CONTRACTS.md C22. Background needs `GET /v1/responses/{id}` polling routes the gateway does not have. Live 18 Sep: two-turn `previous_response_id` with the SNAPSHOT id `gpt-4o-mini-2024-07-18` in turn B PASS (alias table, A1); `responses background refused` PASS (400, `x-gw-served-by: -`, `x-gw-attempts: 0`) |
| Fine-tuned model ids (`ft:gpt-4o-mini:…`) | models | **Unsupported** | client must send a catalog id (`policy.plan_for` → `policy_error: unknown model`, seen live 16 Sep); no catalog entry can be added without a price row | Add as `ModelSpec` with `api_model="ft:…"` |
| Model rewrite (`X-Gw-Body-Modified: 1`) | n/a | **Supported** | `app.py:1729`, `upstream.py` | Rewrites `model` to `api_model`; response `model` comes back as provider's snapshot id (`gpt-4o-mini-2024-07-18`) so clients see the wire name, not the catalog id — SDKs that echo `model` into the next turn will send the wire id and get `policy_error`. Real interop hazard for multi-turn agent loops. |

## 3. Streaming protocol

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| SSE `data:` frames, `text/event-stream` | streaming-responses | **Supported** | `sse.py` incremental parser; `content-type` in `FORWARDED_RESPONSE_HEADERS` `app.py:310` | Byte-split invariant tested |
| `data: [DONE]` terminal | streaming-responses (Chat) | **Supported** | `openai.py:99` `is_done_marker` → TERMINAL; native ending = no `[DONE]` on post-commit failure `openai.py:245-262` | C2 contract |
| Empty-`choices` keepalive / final usage chunk | chat/create | **Supported** | `openai.py:110-116` HEARTBEAT vs META | — |
| Comment keepalives (`: …`) | (OpenRouter practice) | **Supported** | `openai.py:93-97` | Resets liveness only |
| `chat.completion.chunk` delta keys (`content`, `tool_calls`, `refusal`, `reasoning_content`) | chat/create | **Supported** (progress classification) | `openai.py:43` | `reasoning_content` is DeepSeek's key; OpenAI chat does not stream reasoning text |
| Mid-stream `{"error":…}` inside 200 body | streaming-responses (`error` event) | **Supported** | `openai.py:224-243` `error_from_event` → `UpstreamOverloaded`/`InStreamError` | Post-commit: stream closes without `[DONE]` |
| Responses semantic events (`response.*`, `response.failed`, `response.incomplete`) | responses/create | **Supported** | `responses.py:317-357` `classify` by `type`: `response.completed`/`response.incomplete` TERMINAL (`:82`), `error`/`response.failed` ERROR (`:85`), deltas CONTENT (`:89`), lifecycle META; `error_from_event` `:467-496`; `native_ending` `:498-521` returns nothing — `response.failed` is forwarded when upstream sent it and never synthesised (C2 third row, C22); stop reason from `status` + `incomplete_details.reason` (`:202-230`) | No `data: [DONE]` on this dialect; usage rides the terminal frame, so no `include_usage` injection (`:247`). Live 18 Sep: `responses incomplete` PASS (terminal `response.incomplete`, `reason=max_output_tokens`, stop `length`, no `response.completed`) |
| Frame size | n/a | **Supported** (bounded) | `config.py:390` `max_frame_bytes` 1 MiB | A single logprobs-heavy or base64-audio chunk >1 MiB → `FrameTooLarge` |
| `content-encoding` | n/a | **Supported** | `upstream.py:596-624` `accept-encoding: identity` | Finding #24 fixed |
| HTTP/2 to provider | n/a | **Supported** | `upstream.py:546-590`; live-fly logs `HTTP/2 200 OK` | — |

## 4. Response & usage shape

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| `usage.prompt_tokens`/`completion_tokens` | chat/create | **Supported** | `openai.py:196-215` | `total_tokens` ignored (derived) |
| `prompt_tokens_details.cached_tokens` | chat/create | **Supported** | `openai.py:200-212` | — |
| `prompt_tokens_details.cache_write_tokens`, `audio_tokens`, `image_tokens`, `text_tokens` | chat/create | **Unsupported (blind)** | not read | image_tokens would let the gateway price vision explicitly |
| `completion_tokens_details.reasoning_tokens`, `audio_tokens`, `text_tokens`, `accepted/rejected_prediction_tokens` | chat/create | **Unsupported (blind)** | not read | — |
| `finish_reason` (stop/length/tool_calls/content_filter/function_call) | chat/create | **Passthrough** | not recorded in accounting outcome (outcome = completed/interrupted/canceled) | `length` vs `stop` is useful for the "reasoning ate the budget" case (`live_smoke.md:69`) |
| `refusal` (structured outputs) | structured-outputs | **Passthrough** | progress key `openai.py:43` | Not surfaced as an outcome |
| `system_fingerprint`, `service_tier`, `model` snapshot id | chat/create | **Passthrough** | — | `model` echo hazard: see §2 last row |
| `x-request-id` (response) | api-reference/introduction | **Unsupported** (dropped) | `FORWARDED_RESPONSE_HEADERS` = content-type, cache-control, x-accel-buffering only `app.py:310-312` | Provider request id never reaches client or `X-Gw-*`; should at least land in the capture record for support tickets |
| `openai-processing-ms`, `openai-version`, `openai-organization` | introduction | **Unsupported** (dropped) | same allowlist | `openai-processing-ms` vs gateway TTFE = provider-vs-gateway split for free; capture-worthy |
| `x-ratelimit-*` (9 headers incl. project-tokens) | …/guides/rate-limits | **Unsupported** (dropped, unread) | allowlist docstring `app.py:320-324` justifies not *forwarding*; nothing *reads* them either | Gateway has no TPM awareness of the shared key → provider-key cap is a connection count (FAILURE-MODES row 7 residual) |

## 5. Errors & rate limits

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| 400 `invalid_request_error` (+`param`) | …/guides/error-codes | **Supported** | `errors.py:898-910`; `param` deliberately excluded from matching `errors.py:823-834` (finding #7) | Passed through to client |
| 400 unknown model (`model_not_found` in body) | error-codes | **Supported** | `errors.py:843-858` `_looks_like_unknown_model` | Finding #6 fixed |
| 400 context length | error-codes | **Supported** | `errors.py:906` substring rule | Weak signal, narrow blast radius (documented) |
| 400 `unsupported_parameter` (e.g. `temperature` on reasoning models, `reasoning_effort:none` on gpt-6-astra) | reasoning | **Supported** as `InvalidRequest` | `errors.py:909` default | try_next=True → falls back to incumbent, which may accept it: correct |
| 401 | error-codes | **Supported** | `errors.py:892` `AuthenticationFailed`, credential-scoped breaker; body **scrubbed** (C11, commit d90f5ed) | — |
| 403 "unsupported country/region" | error-codes | **Misclassified** | `errors.py:892` lumps 403 with 401 → `AuthenticationFailed` → credential breaker | A region block is not a bad key; blame/health same outcome (stop using key) so low impact |
| 404 | error-codes | **Supported** | `errors.py:896` `ModelNotFound` | — |
| 429 `rate_limit_exceeded` / `slow_down` + `Retry-After` | rate-limits; error-codes | **Supported** | `errors.py:890` `RateLimited`; `retry.py:145-202` Retry-After as floor; `NEUTRAL` health | Docs: treat Retry-After as a minimum + jitter — matches |
| 429 **billing** codes: `credit_balance_exhausted`, `organization_spend_limit_exceeded`, `project_spend_limit_exceeded`, `organization_usage_limit_exceeded`, `insufficient_quota` | error-codes ("don't retry — update credits first") | **Misclassified** | `errors.py:890` classifies every 429 as `RateLimited` (retryable, NEUTRAL); only HTTP **402** maps to `InsufficientCredits` `errors.py:894` | OpenAI signals out-of-money as **429**, not 402 → the gateway retries an unpaid invoice and never blames POLICY. Same bug class as finding #8, different provider. |
| 503 `server_is_overloaded` | rate-limits; error-codes | **Supported** | `errors.py:775` `_OVERLOAD_STATUSES {503,529}` → `UpstreamOverloaded` | — |
| 500 `server_error` | error-codes | **Supported** | `errors.py:915` `UpstreamServerError` | — |
| 408 | — | **Supported** | `errors.py:913` `ConnectTimeout` | — |
| Error body passthrough (non-auth) | n/a | **Supported** | C4; `live_smoke.md:23` | — |

## 6. Auth & headers

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| `Authorization: Bearer` (provider) | introduction | **Supported** | `upstream.py:316` `build_headers` | Client's `authorization` is the *tenant* token, never forwarded `config.py:111` |
| `OpenAI-Organization` / `OpenAI-Project` | introduction | **Unsupported** (client-supplied dropped; operator can set) | not in `DEFAULT_FORWARD_REQUEST_HEADERS` `config.py:104`; `ProviderConn.extra_headers` can pin them `upstream.py:133,320` | Correct: the gateway owns the credential's org/project |
| `OpenAI-Beta` | (referenced; not on intro page) | **Supported** (forwarded) | `config.py:104` `openai-beta` | — |
| `X-Client-Request-Id` (≤512 ASCII) | introduction | **Unsupported** (dropped) | not in allowlist; `x-request-id` *is* forwarded (`config.py:104`) | New header name; add to default forward list |
| `traceparent`/`tracestate` | n/a | **Supported** | `config.py:104` | — |
| Idempotency-Key | not documented for chat | n/a | — | — |
| BYOK / per-tenant provider key | n/a | **Unsupported** | credential is per `ProviderConn`, not per tenant (FAILURE-MODES row 8 describes the *breaker* scoping, not BYOK) | Out of scope per PLAN |

## 7. Limits & catalog

| capability | OpenAI (doc URL) | llmgw status | evidence | note |
|---|---|---|---|---|
| `gpt-4o-mini`: 128k ctx / 16,384 out / $0.15 / $0.075 cached / $0.60 | …/models/gpt-4o-mini | **Supported, matches** | `catalog.py` `openai.gpt-4o-mini` (commit da45bb3) | Snapshot `gpt-4o-mini-2024-07-18`; no deprecation notice today |
| Current flagship/reasoning families (`gpt-6-astra`, `gpt-5.6-terra`, `gpt-5.6-luna`, `o3`) | reasoning; chat/create examples | **Unsupported** (not in catalog) | only `openai.gpt-4o-mini` shipped | One row each with a dated price; `can_reason=True`; reasoning-by-default semantics (see live_smoke finding 1) |
| Prompt-cache thresholds (1,024 visible tokens; 128-multiple rounding on older models) | prompt-caching | n/a (provider behaviour) | — | Explains `cached_tokens` = 19,712 of 22,776 on the vision run |
| Cache write pricing (1.25×) | prompt-caching | **Unsupported** | `ModelSpec.cache_write_per_m` exists `catalog.py:116` but OpenAI usage never populates `cache_write_tokens` in `apply_usage` | Wire `prompt_tokens_details.cache_write_tokens` → `usage.cache_write_tokens` |
| Request payload: 512 MB / 1,500 images | images-vision | **Unsupported** by design | `max_request_bytes` 4 MiB `config.py:395` | Raise per workload or document the cap; a 413 is returned before upstream (C-safe) |
| Response size cap | n/a | **Supported** (bounded) | `config.py:401` 8 MiB buffered | Non-streaming 16k-token output ≈ 70 KB — fine |
| Rate-limit tiers (usage tiers 1-5; ramp ≤50%/15 min above 1M TPM) | rate-limits | n/a | — | Gateway's per-tenant limits are rps/concurrency, not TPM |

## Gaps ranked (impact on a Layrs/tcg-style caller: agents with tools, streaming, vision, JSON)

1. **`model` echo hazard** — the response body's `model` is the provider's snapshot id (`gpt-4o-mini-2024-07-18`); any SDK/agent loop that echoes it into the next request gets `400 policy_error: unknown model`. Fix: accept `api_model`/snapshot ids as aliases in `policy.plan_for`, or rewrite `model` back in the response for the buffered path (streaming rewrite is off the table by C-passthrough). Evidence: live 16 Sep 400 on `gpt-4o-mini`; `app.py:1729`.
2. **OpenAI's out-of-money is a 429, classified as retryable rate limit** — `credit_balance_exhausted`, `*_spend_limit_exceeded`, `insufficient_quota` → `RateLimited` (retry_same, NEUTRAL) instead of `InsufficientCredits` (try_next, POLICY blame). Body-code rule needed at `errors.py:890`, mirror of finding #8.
3. **`/v1/responses` — closed 18 Sep 2026 (PLAN-2 Phase F).** `surfaces/responses.py` serves it streamed and buffered with hosted tools counted, `previous_response_id`/`conversation` passed through to OpenAI, reasoning items passed through and `reasoning_tokens` recorded (CONTRACTS.md C22). What remains: `background: true` is a **400 at the gateway** until `GET /v1/responses/{id}` polling routes exist, and OpenAI hosted-tool per-call pricing is counted but not costed until the OpenAI rows carry `tool_rates`.
4. **Provider request id and processing time are dropped** — `x-request-id` / `openai-processing-ms` reach neither the client nor the capture record; support escalations to OpenAI need the former. Add to capture + an `X-Gw-Upstream-Request-Id` header.
5. **Usage detail fields ignored** — `reasoning_tokens`, `cache_write_tokens` (1.25× billing), `audio_tokens`, `image_tokens`, prediction tokens. Cost is right for text+cache-read only; wrong for explicit-cache writes and audio; and no metric can show "budget eaten by reasoning".
6. **Reasoning vocabulary drift** — `ModelSpec.reasoning` allows `off/low/high/xhigh`; API accepts `none/minimal/low/medium/high/xhigh/max` and 400s on `none` for gpt-6-astra. Catalog cannot describe current models; only `gpt-4o-mini` is shipped.
7. **4 MiB request cap vs multi-image / file inputs** (OpenAI allows 512 MB). Base64 vision beyond ~3 MB → 413 before upstream. Make it per-workload (`vision` workload higher) rather than global.
8. **No TPM awareness** — `x-ratelimit-remaining-tokens`/`reset-tokens` are dropped unread; the provider-key limiter is a connection count. Under real multi-tenant load the shared key hits 429 with no early signal (FAILURE-MODES row 7 residual).
9. **`parallel_tool_calls` untested** through the gateway (multi-index deltas). Likely fine; a contract test against a fake emitting two indexes settles it.
10. **`service_tier: flex` vs the 20 s `first_event` budget** — flex deliberately trades latency; a workload using it needs its own budgets or every call trips `FirstEventTimeout` and falls back (spending the incumbent's money).

Also: `/v1/models` 404 and `/v1/embeddings` 404 are cheap buffered surfaces; `X-Client-Request-Id` should join the default forward list; `safety_identifier` could be injected per tenant.

## Doc deltas (docs today vs repo comments/catalog)

- **Docs host moved**: `platform.openai.com/docs/*` → `developers.openai.com/api/docs/*` (301; old api-reference URLs 403). `live/probe.py:85` still probes `api.openai.com` (API host unchanged — fine), but any doc links in `docs/`/`SOURCEBOOK.md` are stale.
- **`openai.py:186-189`** says the chat API "reports no creation count" for caching — the current usage object has `prompt_tokens_details.cache_write_tokens` and `prompt_cache_options{mode: explicit, ttl}` exists. Comment and accounting are out of date.
- **`user` is deprecated** in favour of `safety_identifier` + `prompt_cache_key`; `max_tokens` is deprecated (repo already prefers `max_completion_tokens`, `openai.py:74`); `prompt_cache_retention` deprecated for `prompt_cache_options.ttl`; `json_object` deprecated in favour of `json_schema`.
- **`reasoning_effort` values** now `none/minimal/low/medium/high/xhigh/max`; `catalog.py:120` Literal is `off/low/high/xhigh`.
- **Billing errors are 429 codes** (`credit_balance_exhausted`, spend/usage-limit codes), not 402 — `errors.py:894`'s 402 rule was written from OpenRouter's behaviour (finding #8) and does not cover OpenAI.
- **503 `server_is_overloaded`** confirmed as the overload signal (repo's `_OVERLOAD_STATUSES {503, 529}` covers it; 529 is Anthropic's).
- **New request/response headers**: `X-Client-Request-Id` (request), `openai-organization` (response), and three `x-ratelimit-*-project-tokens` headers — none referenced in the repo.
- **`gpt-4o-mini` prices/limits match** the catalog row added 16 Sep ($0.15/$0.075/$0.60, 128k/16,384); the `priced_at="2026-09-09"` note "not externally verified" can now cite the model page as verified 2026-09-16. No deprecation.
- Current default models in docs are `gpt-6-astra` / `gpt-5.6-*`; the catalog has none of them. `openai.gpt-5-nano` shipped 18 Sep 2026 (`catalog.py:516`, $0.05 / $0.005 / $0.40 per 1M read from developers.openai.com/api/docs/pricing that day) and `live/smoke.py` no longer carries its own copy.
- Structured-outputs docs list the `refusal` field and `incomplete_details.reason` (Responses). The Responses surface records both as stop reasons (`responses.py:202-230`: `incomplete_details.reason` → `length` / `content_filter`, a `refusal` content part → `refusal`); the chat surface still treats `refusal` as progress only.

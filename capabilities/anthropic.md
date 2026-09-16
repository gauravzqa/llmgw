# Anthropic capability sweep vs llmgw (2026-09-16)

Docs read today at `platform.claude.com` (docs.anthropic.com 301s there). Repo: `/Users/sanjay/PREP/Evo/llmgw` @ `p0-drain-shed-cap`. Status key: **Supported** = gateway parses/accounts/classifies/routes it; **Passthrough** = forwarded byte-for-byte and works, gateway blind to it; **Unsupported** = no route / stripped / rejected / parser would break; **Unknown** = untested, with the test that settles it.

Doc URLs (abbreviated in tables as `[ref]`):
- OV https://platform.claude.com/docs/en/api/overview · MSG https://platform.claude.com/docs/en/api/messages · STR https://platform.claude.com/docs/en/build-with-claude/streaming · ERR https://platform.claude.com/docs/en/api/errors · RL https://platform.claude.com/docs/en/api/rate-limits · VER https://platform.claude.com/docs/en/api/versioning · BETA https://platform.claude.com/docs/en/api/beta-headers · CACHE https://platform.claude.com/docs/en/build-with-claude/prompt-caching · THINK https://platform.claude.com/docs/en/build-with-claude/thinking · XT https://platform.claude.com/docs/en/build-with-claude/extended-thinking · EFF https://platform.claude.com/docs/en/build-with-claude/effort · TOOLS https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview · TREF https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-reference · MCP https://platform.claude.com/docs/en/agents-and-tools/mcp-connector · SO https://platform.claude.com/docs/en/build-with-claude/structured-outputs · VIS https://platform.claude.com/docs/en/build-with-claude/vision · PDF https://platform.claude.com/docs/en/build-with-claude/pdf-support · CIT https://platform.claude.com/docs/en/build-with-claude/citations · CTX https://platform.claude.com/docs/en/build-with-claude/context-windows · CMP https://platform.claude.com/docs/en/build-with-claude/compaction · FILES https://platform.claude.com/docs/en/build-with-claude/files · BATCH https://platform.claude.com/docs/en/build-with-claude/batch-processing · STOP https://platform.claude.com/docs/en/build-with-claude/handling-stop-reasons · MODELS https://platform.claude.com/docs/en/models/overview · PRICE https://platform.claude.com/docs/en/about-claude/pricing · S46 https://platform.claude.com/docs/en/models/sonnet-4-6/overview · H45 https://platform.claude.com/docs/en/models/haiku-4-5/overview · DEP https://platform.claude.com/docs/en/about-claude/model-deprecations. Not fetchable: `/docs/en/build-with-claude/service-tiers` (404); `service_tier` taken from MSG.

Repo facts every row leans on: the only Anthropic route is `POST /anthropic/v1/messages → /v1/messages` (`server/app.py:239`); bodies are forwarded verbatim and only `model`, `stream`, `max_tokens` are read (`surfaces/anthropic.py:59-93`); request headers forwarded upstream are only `anthropic-beta, openai-beta, x-request-id, traceparent, tracestate` (`server/config.py:104`); response headers forwarded to the client are only `content-type, cache-control, x-accel-buffering` (`server/app.py:310`); `x-api-key` + `anthropic-version: 2023-06-01` are injected (`upstream.py:100,313-314`); `accept-encoding: identity` upstream (`upstream.py:624`).

## 1. Endpoints

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| `POST /v1/messages` | OV, MSG | **Supported** | `app.py:239`; smoke `d.thinking.anthropic` PASS (`bench/results/live_smoke.md:16`) | Client path is `/anthropic/v1/messages`; `model` must be the catalog id (`anthropic.haiku-4-5`), rewritten to the wire id |
| `POST /v1/messages/count_tokens` | OV (32 MB limit), count_tokens ref | **Unsupported** | no route; `app.py:238-239,286` list only chat, messages, `/v1/responses` 501 | Free endpoint callers use for pre-flight budgeting; 404 through the gateway |
| `POST /v1/messages/batches` (+ list/retrieve/results/cancel/delete) | OV, BATCH (50% off, 256 MB) | **Unsupported** | no route | Different rate-limit pool and billing; out of scope for a streaming proxy but tcg's article bake is batch-shaped |
| `GET /v1/models`, `GET /v1/models/{id}` | OV, MODELS ("returns `max_input_tokens`, `max_tokens`, `capabilities`") | **Unsupported** | no route; `live/probe.py:85` calls it directly for reconciliation | The catalog's ceilings could be refreshed from this (finding 35/39) |
| Files API `POST/GET/DELETE /v1/files`, `/content` | FILES (now GA, no beta header; 500 MB) | **Unsupported** | no route; `max_request_bytes` 4 MiB (`config.py:395`) | `file_id` references inside a Messages body are Passthrough (see §2) |
| Skills, Agents/Sessions/Environments (beta), Admin/usage/cost, Rate Limits API | OV, BETA | **Unsupported** | no route | Not a data-plane concern |

## 2. Chat-path request features (Messages API)

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| `model` (required) | MSG | **Supported** | `surfaces/base.py:295 require_model`; rewrite to `api_model` (finding 21) | Unknown catalog id → 400 `policy_error` before any upstream call |
| `max_tokens` (required) | MSG | **Passthrough** (read, not validated) | `anthropic.py:83-86` | Gateway does not enforce presence or the model's `max_output`; provider does |
| `messages`, `system` (string or blocks) | MSG | **Passthrough** | body verbatim | — |
| `metadata.user_id` | MSG | **Passthrough** | — | Not used for tenant attribution; gateway tenant comes from the bearer token |
| `stop_sequences` | MSG | **Passthrough** | — | — |
| `temperature` / `top_p` / `top_k` | MSG (deprecated; 400 on 4.7+ when non-default, DEP) | **Passthrough** | — | A 400 for these is classified `InvalidRequest` (client blame) — correct |
| `stream` | MSG | **Supported** | `base.py:304 read_stream` | Selects the pump path |
| `tools` (custom, `input_schema`, `strict`, `cache_control`, `defer_loading`, `allowed_callers`, `input_examples`, `eager_input_streaming`) | TOOLS, TREF | **Passthrough** | body verbatim; `input_json_delta` counted as progress (`anthropic.py:141-150`) | Anthropic `tool_use` round trip never live-tested (`live_smoke.md:91` lists it under "not exercised") — **Unknown** end-to-end |
| `tool_choice` (`auto`/`any`/`tool`/`none`, `disable_parallel_tool_use`) | TOOLS; `any`/`tool` 400 on Fable 5.1 (ERR) | **Passthrough** | — | — |
| Server tools (`web_search_20260318`, `web_fetch_*`, `code_execution_*`, `tool_search_*`, `advisor_20260301`) | TREF | **Passthrough** | — | `server_tool_use` usage (web_search_requests etc.) is NOT accounted: `accounting.py` reads only the four token kinds; **$10/1k searches invisible to cost** |
| Anthropic-schema client tools (`bash_20250124`, `text_editor_20250728`, `memory_20250818`, `computer_toolset_20260801`, `browser_toolset_20260801`) | TREF | **Passthrough** | — | Screenshots in `tool_result` are image tokens — accounted only via `input_tokens` |
| MCP connector (`mcp_servers` + `mcp_toolset`, beta `mcp-client-2025-11-20`) | MCP | **Passthrough** | `anthropic-beta` forwarded (`config.py:105`) | `mcp_tool_use`/`mcp_tool_result` blocks are META to the classifier; `authorization_token` in the body transits the gateway and the capture path if capture ever stores bodies (today it does not) |
| `thinking: {type: "enabled", budget_tokens}` (Haiku 4.5 only mode; deprecated on 4.6) | XT, H45 | **Passthrough** | smoke `d.thinking.anthropic` PASS: `thinking_delta` frames intact (`live_smoke.md:16,40`) | `thinking_delta` counts as CONTENT progress (`anthropic.py:126`), so a long think does not trip the progress clock — correct |
| `thinking: {type: "adaptive"}`, `display: summarized/omitted/updates` | THINK | **Passthrough** | — | Sonnet 4.6+ default; `redacted_thinking` blocks and `signature_delta` are CONTENT deltas → progress, not text (`anthropic.py:159`) |
| `output_config.effort` (`low…max`) | EFF (Sonnet 4.6 yes; Haiku 4.5 no) | **Passthrough** | — | Catalog has no effort/adaptive flag; `ModelSpec.reasoning` (`catalog.py:120`) is unused for Anthropic |
| `output_config.format` (json_schema) / legacy `output_format`; strict tools | SO (GA, no beta) | **Passthrough** | — | Equivalent of OpenAI `response_format`; smoke tested only on OpenAI (`g.json_mode`) |
| `cache_control` (top-level automatic, or per-block; `ttl: "1h"`) | CACHE (≤4 breakpoints, no beta) | **Passthrough** | design note `anthropic.py:65-68` explicitly refuses to re-serialise so `cache_control` survives | Cache accounting: see §4 |
| Image `source.type` = `base64` / `url` / `file` | VIS | **Passthrough** | vision smoke used OpenAI only (`e.vision`) | 4 MiB `max_request_bytes` vs provider's 32 MB and 10 MB/image → **large base64 images get a gateway 413 first** |
| Document `source.type` = `base64` / `url` / `file` / `text` / `content`; `citations.enabled` | PDF, CIT | **Passthrough** | — | Same 4 MiB ceiling; `citations_delta` is a `content_block_delta` → CONTENT, fine |
| `service_tier` (`auto` / `standard_only`), `inference_geo` (1.1× on `us`) | MSG, PRICE | **Passthrough** | — | Cost model ignores the 1.1× multiplier and priority tier |
| `container` (code-exec container reuse), `container_upload` blocks | MSG, FILES | **Passthrough** | — | — |
| `context_management.edits` (compaction `compact_20260112`, beta `compact-2026-01-12`; tool-result / thinking clearing) | CMP, CTX | **Passthrough** | `anthropic-beta` forwarded | `usage.iterations[]` (compaction billing) NOT summed by accounting → under-bills compacted turns |
| Task budgets, mid-conversation `output_config` (beta), `thinking.block_binding` | EFF, ERR | **Passthrough** | — | All body-level; nothing to do |
| Request size 32 MB | OV | **Unsupported above 4 MiB** | `config.py:395` (`LLMGW_MAX_REQUEST_BYTES`) | Gateway 413 is `RequestTooLarge` (`errors.py:410`), not the provider's `request_too_large` body |

## 3. Streaming protocol

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| Named events `message_start → content_block_start → content_block_delta* → content_block_stop → message_delta → message_stop` | STR §Event types | **Supported** | `anthropic.py:47,118-140`; smoke saw all seven names intact (`live_smoke.md:40`) | `event:` name is authoritative, payload `type` is fallback |
| `ping` events | STR §Ping | **Supported** | `anthropic.py:128` → HEARTBEAT; resets liveness clock only (CONTRACTS C7, `sse.py:67-71`); live run confirmed real pings (`docs/11 §6`) | — |
| `error` event inside a 200 body (`overloaded_error` etc.) | STR §Error events, ERR | **Supported** | `anthropic.py:236-260` → `UpstreamOverloaded` / `InStreamError`; pump captures it (`pump.py:426`) | Detected as failure, not passed as content. Post-commit → native ending without `message_stop` (C2, `anthropic.py:262-274`); pre-commit `try_next=True` (`errors.py:608`). Never synthesises an `error` event |
| Unknown future event types | STR §Other events, VER | **Supported** | `anthropic.py:136-140` → META | Forwarded, never counted as progress |
| `text_delta` | STR | **Supported** | `anthropic.py:141-162` (transcript + progress) | — |
| `input_json_delta` (tool args; fine-grained via `eager_input_streaming`) | STR, TREF | **Supported** (progress) / **Passthrough** (content) | `anthropic.py:143-146` | Not parsed into tool calls; that is the caller's job |
| `thinking_delta`, `signature_delta`, `redacted_thinking` | STR §Thinking delta, THINK | **Supported** (progress) | same | Empty `thinking_delta` under `display: omitted` still a CONTENT frame → progress clock ticks on zero text; acceptable |
| `citations_delta`, `compaction_delta` | CIT, CMP | **Supported** (progress) | any `content_block_delta` is CONTENT | — |
| Usage split: input/cache on `message_start`, output on `message_delta` | STR, MSG | **Supported** | `anthropic.py:163-234`; `input_exact`/`output_exact` two flags (finding 26) | `message_delta` is META so it never resets the progress clock (`anthropic.py:97-104`) |
| `stop_reason` in `message_delta` | STOP | **Passthrough** | no reference to `stop_reason` anywhere in `src/` (grep) | Gateway cannot distinguish `max_tokens`/`refusal`/`model_context_window_exceeded`/`pause_turn` from `end_turn`; all are `outcome=completed` |
| `output_tokens_details.thinking_tokens` on final `message_delta` | XT | **Unsupported** (dropped) | `apply_usage` reads four fields only | Billed inside `output_tokens` anyway, so cost is right; the breakdown is lost |
| gzip on SSE | docs/11 §1 (measured) | **Supported** | `upstream.py:596-624` `accept-encoding: identity` | Live-verified |
| 10-minute non-streaming guidance / TCP keepalive | ERR §Long requests | **Supported** by budgets | total 120 s default (`config.py`), `LLMGW_BUDGET_TOTAL` | A 128k-token non-streaming call would hit the gateway's total first — by design |

## 4. Response & usage shape

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| `usage.input_tokens` (after last breakpoint), `cache_read_input_tokens`, `cache_creation_input_tokens` — disjoint | CACHE ("cache reads and writes are disjoint"), RL | **Supported** | `anthropic.py:205-222`; `Usage` convention `base.py:126-130`; finding 25 | Matches provider semantics exactly; `total_input_tokens` property gives the inclusive view |
| `cache_creation.ephemeral_5m_input_tokens` / `ephemeral_1h_input_tokens` | CACHE | **Unsupported** (collapsed) | only the sum is read | Cost uses one `cache_write_per_m`; 1h writes are 2× not 1.25×, and **`cache_write_per_m` is unset for both Anthropic entries → falls back to `input_per_m` (1.0×)** (`catalog.py:238-260`, `accounting.py:304-305`) → under-bills every cache write by 20–50% |
| `usage.server_tool_use` (`web_search_requests`, `code_execution_requests`, `web_fetch_requests`) | PRICE §tool pricing | **Unsupported** (dropped) | not read | Web search $10/1k is a real cost line the gateway cannot see |
| `usage.iterations[]` (compaction) | CMP | **Unsupported** (dropped) | not read | Top-level counts exclude compaction iterations per the doc |
| `usage.service_tier` (`standard`/`priority`/`batch`) | MSG | **Unsupported** (dropped) | — | — |
| `stop_reason` values `end_turn, max_tokens, stop_sequence, tool_use, pause_turn, refusal, model_context_window_exceeded, compaction`; `stop_details` on refusal | STOP, CMP | **Passthrough** | see §3 | No `refusal` metric; no truncation metric |
| `request-id` response header | ERR, OV | **Unsupported** (stripped) | allowlist `app.py:310` | Callers cannot quote Anthropic's request id to support; `X-Gw-*` headers exist but no upstream id. `x-request-id` is forwarded *upstream* (`config.py:105`) so the caller's own id does reach Anthropic |
| `anthropic-organization-id`, `anthropic-workspace-id` | OV | **Unsupported** (stripped) | same | Fine — describe the gateway's credential, not the client's |
| Non-streaming JSON response | MSG | **Supported** | smoke non-stream paths; buffered mode in `app.py` | Usage parsed from the JSON body |
| `content` block types in responses (`text`, `tool_use`, `thinking`, `redacted_thinking`, `server_tool_use`, `web_search_tool_result`, `mcp_tool_use/result`, `compaction`, `container`) | MSG, TREF, CMP | **Passthrough** | — | — |

## 5. Errors & rate limits

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| 400 `invalid_request_error` (incl. self-set spend limit, prefill-not-supported, thinking-mode mismatches, `block_binding`) | ERR | **Supported** | `errors.py:898-910` → `InvalidRequest`/`ContextLengthExceeded`/`ModelNotFound` by body sniff | `"prompt is too long"` matches the `"too long"` sniff → `ContextLengthExceeded` ✓; spend-limit 400 ("You have reached your specified API usage limits") is classified as the CLIENT's fault — arguably a `PolicyError`/billing state |
| 401 `authentication_error`, 403 `permission_error` | ERR | **Supported** | `errors.py:892` → `AuthenticationFailed`, scoped to credential; body scrubbed to the client (C11, finding 30 fixed) | — |
| 402 `billing_error` | ERR | **Supported** | `errors.py:894` → `InsufficientCredits` (NEUTRAL, try_next) | — |
| 404 `not_found_error` | ERR | **Supported** | `errors.py:896` → `ModelNotFound` | Anthropic is the one provider where this rule is exact |
| 409 `conflict_error` | ERR | **Unknown → misclassified** | falls through to `UpstreamServerError("unexpected upstream status 409")` (`errors.py:918`), `retry_same=True` | Would be retried and counted as provider FAILURE; low frequency on Messages |
| 413 `request_too_large` (32 MB) | ERR | **Misclassified** | same fallthrough → `UpstreamServerError`, retried, breaker-visible | Rare because the gateway's own 4 MiB 413 fires first, but a raised `LLMGW_MAX_REQUEST_BYTES` exposes it |
| 429 `rate_limit_error` + `retry-after` | ERR, RL | **Supported** | `errors.py:890` → `RateLimited` (NEUTRAL, `retry-after` as floor `retry.py:186`; parsed `errors.py:779`) | Spend-cap 429 (`details.error_code = enforced_spend_limit_reached`, **no** `retry-after`) is indistinguishable from a transient 429 → retried/fallen back forever with backoff; not read |
| 500 `api_error` | ERR | **Supported** | `errors.py:915` → `UpstreamServerError` | — |
| 504 `timeout_error` | ERR | **Supported** (as 5xx) | same | — |
| 529 `overloaded_error` | ERR | **Supported** | `_OVERLOAD_STATUSES = {503, 529}` (`errors.py:775,911`) → `UpstreamOverloaded` | Also matched by `etype` containing "overloaded" |
| Mid-stream error after 200 | ERR, STR | **Supported** | §3 | — |
| `anthropic-ratelimit-{requests,tokens,input-tokens,output-tokens}-{limit,remaining,reset}`, `anthropic-priority-*`, `anthropic-fast-*` | RL §Response headers | **Unsupported** (dropped) | not in response allowlist (`app.py:310-324`, deliberately); not read by admission/breaker (grep `ratelimit` → only the allowlist comment) | Neither forwarded nor consumed. A provider-side TPM budget is invisible until the 429 arrives; `ProviderKeyLimiter` is a connection cap, not tokens (FAILURE-MODES row 7) |
| Token-bucket semantics, per-model buckets, cache-aware ITPM (cache reads don't count) | RL | n/a | tenant buckets are gateway-side (`admission.py`) | Gateway's own limiter is requests + concurrency, not tokens (ex06 TPM limiter never landed) |
| Acceleration-limit 429s on traffic ramps | ERR | **Supported** (as 429) | — | Gateway itself has no ramp control |
| Error body shape `{type:"error", error:{type,message}, request_id}` | ERR | **Supported** | `_looks_like_unknown_model` etc. read `error.type`/`message` | `request_id` in the body IS forwarded on non-auth errors (passthrough body) — the one place callers can find it |

## 6. Auth & headers

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| `x-api-key` | OV ("legacy fallback, still supported") | **Supported** | `upstream.py:313` injected; client `x-api-key` never forwarded (`config.py:116`) | — |
| `Authorization: Bearer <token>` (now the primary auth; WIF short-lived tokens) | OV | **Unsupported** for upstream | `NEVER_FORWARDED` contains `authorization`; injection uses `x-api-key` only | Fine today; blocks Workload Identity Federation / OAuth tokens later |
| `anthropic-version: 2023-06-01` (current and only non-deprecated) | VER | **Supported** | `upstream.py:100` | Value is current |
| `anthropic-beta` (client-supplied, comma-joined) | BETA | **Supported** (forwarded) | `config.py:104-105` | Invalid beta → provider 400 → `InvalidRequest` ✓. Beta names currently relevant to Layrs: `mcp-client-2025-11-20`, `compact-2026-01-12`, `context-management-2025-06-27`, `interleaved-thinking-2025-05-14` (no-op on Haiku 4.5), `output-300k-2026-03-24` (batch only), `mid-conversation-output-config-2026-07-01`, `thinking-binding-controls-2026-08-01`, `model-context-window-exceeded-2025-08-26`, `advisor-tool-2026-03-01`, `computer-use-2025-11-24`. `files-api-2025-04-14` and `mcp-client-2025-04-04` deprecated |
| `anthropic-workspace-id` (request header; required for multi-workspace keys) | OV | **Unsupported** | not in forward allowlist; no per-target extra header | Add via `ProviderConn.extra_headers` if a workspace-scoped key is ever used |
| `anthropic-user-profile-id` (beta `user-profiles`) | count_tokens ref | **Unsupported** | not forwarded | — |
| `anthropic-organization-id` / `anthropic-workspace-id` response echo | OV | stripped | §4 | — |
| `x-request-id`, `traceparent`, `tracestate` forwarded upstream | — | **Supported** | `config.py:105` | Good for correlation with Anthropic support |
| Operator override of `anthropic-version` via `extra_headers` | — | **Supported** | `upstream.py:318-320` merge order | — |

## 7. Limits & catalog

| capability | Anthropic [ref] | llmgw status | evidence | note |
|---|---|---|---|---|
| `claude-haiku-4-5-20251001`: 200K ctx, 64K out, $1/$5, cache write $1.25 (5m) / $2 (1h), read $0.10; extended thinking only; no effort; retirement ≥ 2026-10-15 | H45, PRICE, DEP | **Supported** (matches) | `catalog.py:238-248`: 200_000 / 64_000 / 1.00 / 0.10 / 5.00, `priced_at 2026-05-27`, `can_reason=False` | `cache_write_per_m` unset (see §4); `can_reason=False` is wrong — Haiku 4.5 supports `thinking.type=enabled` and the smoke proved it |
| `claude-sonnet-4-6`: **1M ctx**, 128K out, $3/$15, cache write $3.75/$6, read $0.30; adaptive thinking (extended deprecated); effort supported; legacy status; retirement ≥ 2027-02-17 | S46, PRICE, DEP | **Partly stale** | `catalog.py:249-260`: `context_window=200_000` (doc: 1M, no beta, standard pricing), 128_000 ✓, prices ✓, `priced_at 2026-08-03` | Context ceiling 5× too low (finding 35 shape); `cache_write_per_m` unset |
| Newer models absent: `claude-sonnet-5` ($2/$10, 1M/128K), `claude-opus-5` ($5/$25), `claude-fable-5-1` ($10/$50, cache read 0.025×) | MODELS, PRICE | **Unsupported** (not in catalog) | — | Sonnet 5 is cheaper than Sonnet 4.6 on every axis; the catalog's "current Sonnet" is the legacy one |
| Cache-read multiplier 0.1× (0.025× on Fable 5.1) | PRICE | **Supported** for listed models | `cached_input_per_m` explicit | — |
| Long-context pricing | PRICE | n/a | — | No premium any more on 4.6+; nothing to model |
| `inference_geo: "us"` 1.1× | PRICE | **Unsupported** | cost formula has no multiplier | Only matters if a caller sets it |
| Batch 50% discount | PRICE | n/a | no batch route | — |
| Min cacheable prompt: 4,096 tokens on Haiku 4.5 / 1,024 on Sonnet 4.6 | CACHE | **Passthrough** | — | Sub-minimum prompts silently uncached (no error) → callers see `cache_creation_input_tokens: 0`; the gateway's cache metrics will look like a broken integration for short Haiku prompts |
| Images: 100/request (200K models) or 600; 10 MB each; 28×28 visual tokens; high-res tier on 4.7+ | VIS | **Passthrough** / **4 MiB body cap** | — | — |
| PDFs: 100 pages (200K ctx) / 600; 32 MB request | PDF | same | — | — |
| Model retirements affecting the catalog | DEP | — | — | Haiku 4.5 retirement floor is 2026-10-15 (one month out); nothing deprecated yet |
| Tool-use system-prompt overhead (496–589 tokens on Haiku 4.5 / Sonnet 4.6) | TOOLS §Pricing | **Supported** (implicitly) | billed inside provider `input_tokens` | Finding 37's "chat template is billable" applies |

## Gaps ranked (impact on a Layrs/tcg-style caller: agents with tools, streaming, caching, vision/PDF, thinking)

1. **Cache-write cost is wrong for every Anthropic cache-bearing request.** `cache_write_per_m` is `None` on both entries, so writes are priced at 1.0× base instead of 1.25× (5m) / 2.0× (1h); the 5m/1h split in `usage.cache_creation` is discarded. For a tcg system prompt that is written once per 5 min and read many times this is a few percent; for 1h-TTL agent prompts it is a 50% under-bill on the write line. Fix: set `cache_write_per_m` (5m rate) on both, read `cache_creation.ephemeral_1h_input_tokens` and price it at 2×.
2. **`stop_reason` is invisible.** `max_tokens`, `refusal` (with `stop_details`), `model_context_window_exceeded`, `pause_turn` all land as `outcome=completed`. An agent loop truncated at `max_tokens` on every turn shows up as 100% success. Fix: read `message_delta.delta.stop_reason` in `apply_usage`/pump, add a closed-set `stop_reason` label on `llmgw_requests_total` (or a separate counter) and a capture field.
3. **Sonnet 4.6 `context_window` is 200K in the catalog; the model has 1M with no beta header.** Any capability filter or pre-flight that trusts the catalog will refuse legitimate long-context requests (finding 35 again). Also `can_reason=False` on Haiku 4.5 contradicts the smoke test. Fix: 1_000_000; `can_reason=True`; consider `GET /v1/models` in `make probe` to reconcile ceilings automatically.
4. **4 MiB `max_request_bytes` vs Anthropic's 32 MB.** Vision/PDF callers sending base64 (10 MB/image, 100-page PDFs) get the gateway's 413 before the provider's. Fix: raise the default to 32 MiB for the Anthropic surface or make it per-surface; keep the byte-bounded body read.
5. **Spend-cap 429 is treated as transient.** `error.details.error_code == "enforced_spend_limit_reached"` with no `retry-after` means "no access until the 1st"; today it is `RateLimited` (NEUTRAL, retry_same) → retries and fallbacks burn budget until deadline. Fix: sniff that code → `InsufficientCredits`-like class (no retry_same, try_next, POLICY blame).
6. **Server-tool and compaction usage are not accounted.** `server_tool_use.web_search_requests` ($10/1k) and `usage.iterations[]` (compaction re-sampling) are dropped, so `llmgw_cost_usd_total` under-reports agentic turns that use web search or compaction. Fix: read both, add `web_search` as a priced kind in the catalog/accounting.
7. **`anthropic-ratelimit-*` headers are neither surfaced nor consumed.** The gateway learns about provider token budgets only from 429s. Even without a TPM limiter, exporting `remaining`/`reset` as gauges per credential would give an early-warning signal; forwarding them to clients is correctly avoided.
8. **409 and 413 from the provider fall into `UpstreamServerError`** (`retry_same=True`, provider FAILURE → breaker). 413 is masked today by gap 4; 409 is rare on Messages. Fix: explicit rules — 413 → `RequestTooLarge`-shaped client error, 409 → `InvalidRequest` (no retry, no breaker).
9. **`request-id` is stripped from responses** on success and auth-scrubbed errors. Callers debugging with Anthropic support have nothing to quote. Fix: allowlist `request-id` (rename to `x-gw-upstream-request-id` if you want it namespaced) — it is not credential material.
10. **Anthropic tool-use round trip, vision, PDF and structured outputs have never been exercised on the Anthropic surface** (`live_smoke.md:91`). They are byte-passthrough so they *should* work, but `content_block_start` for `tool_use` + `input_json_delta` + `tool_result` replay is exactly the shape that found the OpenAI bugs. Test that settles it: extend `bench/live_smoke.py` with `b.tools.anthropic`, `c.tool_roundtrip.anthropic`, `e.vision.anthropic` (base64 + url), `f.pdf.anthropic`, `g.json.anthropic` (`output_config.format`), and `d.thinking.adaptive` on Sonnet 4.6.

Also worth knowing, lower impact: no `count_tokens`/`models`/`files`/`batches` routes (callers must go direct, which splits credentials); `Authorization: Bearer` upstream is not supported (blocks WIF tokens); `anthropic-workspace-id` cannot be set per target without `extra_headers`; `output_tokens_details.thinking_tokens` is dropped (cost still right).

## Doc deltas (current docs vs repo comments/catalog)

| Repo says | Docs say (URL) | Action |
|---|---|---|
| `catalog.py:256` Sonnet 4.6 `context_window=200_000` | 1M tokens, default, no beta, standard pricing (S46, CTX) | Set 1_000_000 |
| `catalog.py:247` Haiku 4.5 `can_reason=False` (default) | Supports extended thinking (`thinking.type: enabled`) (H45, XT); smoke proved it | Set `can_reason=True` |
| Both Anthropic entries `cache_write_per_m=None` → priced at `input_per_m` | 5m write 1.25×, 1h write 2.0× (PRICE, CACHE): Haiku $1.25/$2, Sonnet $3.75/$6 | Add `cache_write_per_m` (5m) and a 1h rate/field |
| `ANTHROPIC_VERSION = "2023-06-01"` (`upstream.py:100`) | Still the only current version (VER) | None |
| `anthropic.py:186-189` "Anthropic input excludes both cache fields" | Confirmed: `input_tokens` = tokens after the last breakpoint; disjoint (CACHE, RL) | None |
| `errors.py:486` "503, or Anthropic's 529" | 529 `overloaded_error` confirmed; 503 not an Anthropic code but harmless (ERR) | None |
| `errors.py:898-903` "Anthropic returns 404 for an unknown model" | Confirmed `not_found_error` 404 (ERR) | None |
| `anthropic.py:262-274` no synthesised `error` event; close before `message_stop` | Docs show `event: error` mid-stream is provider-originated; versioning says clients must tolerate unknown events (STR, VER) | None |
| `live/smoke.py` and `bench/live_fly.py` use `thinking: {type: enabled, budget_tokens}` on Haiku 4.5 | Correct for Haiku 4.5; would 400 on 4.7+ and is deprecated on Sonnet 4.6 (XT) | Use `adaptive` + `output_config.effort` for Sonnet 4.6 tests |
| `docs/15` finding 35 "catalog output ceilings too low" | Haiku 4.5 64K ✓, Sonnet 4.6 128K ✓ (H45, S46); the stale number is now the *context* window | Reword |
| `.env.example` / README examples name `claude-haiku-4-5-20251001` | Still the pinned id; alias `claude-haiku-4-5` also valid; retirement floor 2026-10-15 (H45, DEP) | Plan the successor entry (Sonnet 5 or a Haiku 5) before mid-October |
| Catalog has no `claude-sonnet-5` / `claude-opus-5` | Current lineup; Sonnet 5 is $2/$10 vs Sonnet 4.6 $3/$15 (MODELS, PRICE) | Add entries; make Sonnet 5 the default Anthropic route |
| `temperature`/`top_p`/`top_k` treated as ordinary params in examples | Deprecated; 400 on 4.7+ when non-default (DEP) | Note for future model entries; `InvalidRequest` classification already correct |
| `FAILURE-MODES` row 7 "cap is a connection count, not a token count" | Provider limits are RPM + ITPM + OTPM per model class with cache-aware ITPM (RL) | Residual stands; headers now exist to read it |

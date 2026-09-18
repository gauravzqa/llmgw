# Capability sweep: DeepSeek API vs llmgw (2026-09-16)

Docs read today: `https://api-docs.deepseek.com/` (home), `/quick_start/pricing`, `/quick_start/rate_limit`, `/quick_start/error_codes`, `/quick_start/token_usage`, `/quick_start/parameter_settings`, `/api/create-chat-completion`, `/api/list-models`, `/api/get-user-balance`, `/guides/thinking_mode`, `/guides/tool_calls`, `/guides/json_mode`, `/guides/chat_prefix_completion`, `/guides/fim_completion`, `/guides/multi_round_chat`, `/guides/kv_cache`, `/guides/vision`, `/guides/files_api`, `/guides/responses_api`, `/guides/anthropic_api`, `/news/news260910`. Old URLs `/guides/reasoning_model` and `/guides/function_calling` now redirect to the quick start (content moved to `thinking_mode` / `tool_calls`).

Status legend: **Supported** = native handling · **Passthrough** = forwarded byte-for-byte, works, gateway blind · **Unsupported** = missing/rejected/broken · **Unknown** = untested, with the test that settles it.

Repo facts that drive most rows: DeepSeek is `ProviderConn(kind="openai", base_url="https://api.deepseek.com")` (`catalog.py:174-180`) and rides the OpenAI surface; the request body is never re-serialised, only `model` is rewritten (`surfaces/openai.py:59-86`, `live_smoke.md` "everything is byte-faithful passthrough"). Routes: `/v1/chat/completions`, `/anthropic/v1/messages`, and since PLAN-2 Phase F (18 Sep 2026) `/v1/responses` (`surfaces/responses.py`; `server/app.py:305` `UNIMPLEMENTED_ROUTES` is empty).

## Model line-up as of today (matters for every row below)

- Live ids: **`deepseek-flash`** (= V4.1-Flash, released 2026-09-10) and **`deepseek-v4-pro`**. `deepseek-v4-flash` and `deepseek-v4-flash-vision-exp` are retired aliases routed to V4.1-Flash. `deepseek-chat` / `deepseek-reasoner` no longer appear anywhere in the docs.
- News 2026-09-10 says `deepseek-v4-pro` requests route to V4.1-Flash **at Flash rates from 2026-09-14**; the home page says "we have decided to continue providing API services for DeepSeek V4 Pro after September 14, 2026, billing unchanged"; the pricing page still lists both at different prices. The docs contradict each other — see Doc deltas.
- Thinking is **on by default, effort `high`, on all models** (`/guides/thinking_mode`). This is the single most consequential fact for a cheap-candidate plan (see Gaps 1–2).

## 1. Endpoints

| capability | DeepSeek (doc) | llmgw status | evidence | note |
|---|---|---|---|---|
| `POST /chat/completions` (OpenAI dialect), base `https://api.deepseek.com` or `/v1` alias | home, `/api/create-chat-completion` | **Supported** | `catalog.py:174-180`; `upstream.py:261-279` (`join_url` → `https://api.deepseek.com/v1/chat/completions`, accepted alias); smoke `a.stream/a.nonstream` PASS (`live_smoke.md:11-12`) | |
| Anthropic-compatible `POST /anthropic/v1/messages`, `x-api-key`, thinking mapped, images, tools | `/guides/anthropic_api` | **Unsupported (config-only gap)** | no `ProviderConn(kind="anthropic", base_url="https://api.deepseek.com/anthropic")` in `catalog.py`; header injection for `kind="anthropic"` sends exactly what DeepSeek wants (`upstream.py:312-314`) | Adding the provider entry would let `/anthropic/v1/messages` callers fall to DeepSeek without a dialect change (policy rejects cross-dialect plans, `policy.py:227,246`). Untested. |
| Responses API `POST /responses` (stateless, no `previous_response_id`, `output_config.effort`) | `/guides/responses_api` | **Supported** (stateless; PLAN-2 Phase F, 18 Sep 2026) | Same `surfaces/responses.py` surface on the same `kind="openai"` provider; `catalog.py:351` `stateless_responses=True`; `responses.py:291-313` `check_target` **refuses `previous_response_id` / `conversation` with 400 and no upstream call** because DeepSeek does not reject them — it answers 200 with `previous_response_id: null` and silently drops the history (probe 12b, `captures-responses.md`). Nothing is stripped from the body (CONTRACTS.md C22). Upstream path stays `/v1/responses`: probes 10a/10b found `/v1/responses` and `/responses` identical | `reasoning.effort` accepted; `output_config.effort` silently ignored upstream (probe 11c); reasoning streams in clear as `response.reasoning_text.delta` (progress, `responses.py:94`); a small `max_output_tokens` is spent on reasoning first, so `incomplete` with only a `reasoning` item is routine. Live 18 Sep: `responses deepseek buffered` / `previous_response_id refused (C22)` / `deepseek stream` all PASS |
| `/beta` base URL (prefix completion, strict tools) | `/guides/chat_prefix_completion`, `/guides/tool_calls` | **Supported** (provider row; not yet in the live smoke) | `catalog.py:380-389` `deepseek-beta` (`path_prefix="/beta"`, `upstream.py:304, 922`). The open question is settled: the 17 Sep 2026 probe (`captures-responses.md` 10c/10d) got **200 from both** `POST /beta/chat/completions` and `POST /beta/v1/chat/completions`, so DeepSeek accepts the `/v1` inside `/beta` and no path-join special case is needed beyond the prefix | Strict tools and prefix completion remain unexercised end to end. |
| FIM `POST /beta/completions` (prompt/suffix, max 4K) | `/guides/fim_completion` | **Unsupported** | no `/v1/completions` surface (`app.py:238`) | |
| Files API `POST/GET/DELETE /files` | `/guides/files_api` | **Unsupported** | no route | Callers can upload direct and pass `file_id` in a chat message — that part is Passthrough. |
| `GET /models` | `/api/list-models` | **Unsupported as a route; used by the catalog probe** | `live/probe.py` reconciles against it (`RESULTS.md:53`, 3 ids then; 2 now) | No `/v1/models` route for clients. |
| `GET /user/balance` (`is_available`, `balance_infos[]`) | `/api/get-user-balance` | **Unsupported** | — | Could pre-empt 402s: a `credential_ok` check next to `credential_present` in `/probe`. |

## 2. Chat-path request features

| capability | DeepSeek (doc) | llmgw status | evidence | note |
|---|---|---|---|---|
| `thinking: {type: enabled|disabled}`; `reasoning_effort: none|low|high|max`; **default enabled/high on every model** | `/guides/thinking_mode`, `/api/create-chat-completion` | **Passthrough**, catalog wrong | smoke `d.thinking.deepseek` PASS (`live_smoke.md:17`); `catalog.py:290-298` has `can_reason` unset (False) for `deepseek.deepseek-v4-flash` and no `reasoning=` default; `ModelSpec.reasoning` vocabulary is `off/low/high/xhigh` (`catalog.py:120`) — DeepSeek's is `none/low/high/max` | Gateway never injects `thinking` or `reasoning_effort`; a caller that omits them pays for high-effort reasoning on the "cheap" model. |
| `reasoning_content` in responses/deltas | `/guides/thinking_mode` | **Passthrough**, progress-aware | `surfaces/openai.py:43` `_PROGRESS_KEYS` includes `reasoning_content` → a reasoning-only stretch resets the progress clock (`pump.py:415-416`) | Correct: long thinking does not trip `StallTimeout`. |
| `reasoning_content` in **input** messages: ignored without tools; **required (400 otherwise) for all prior turns when `tools` is present** | `/guides/thinking_mode` | **Passthrough** (client's job) | body never rewritten | Gateway cannot help; the OpenAI SDK's message objects drop it unless the client copies it. This is the #1 multi-turn-tool failure a Layrs caller will hit. |
| `max_tokens`: default 8K (non-thinking) / 64K (thinking) / 128K (max); ceiling 393,216 | `/api/create-chat-completion` | **Passthrough**; catalog ceiling correct | `catalog.py:269,297 max_output=393_216`; `surfaces/openai.py:85 read_max_tokens` reads it only for estimation | Small `max_tokens` + default thinking = empty `content`, `finish_reason=length` (`live_smoke.md:69`). |
| `tools` / `tool_choice` (`none|auto|required|named`); tools in thinking mode; streaming tool deltas | `/guides/tool_calls`, `/api/create-chat-completion` | **Passthrough** | smoke `b.tools.deepseek` PASS, `finish_reason=tool_calls` (`live_smoke.md:14`); `_PROGRESS_KEYS` includes `tool_calls` | Doc note: `tool_choice` "not supported in thinking mode" per the API reference table. |
| `strict: true` tools (beta base URL, `additionalProperties:false`, all props required) | `/guides/tool_calls` | **Unsupported** | needs `/beta` (row above) | |
| Parallel tool calls | not documented today | **Unknown** | — | Test: prompt needing two calls, check `tool_calls[]` length through gateway vs direct. |
| `response_format: json_object` (prompt must contain "json"; may return empty content) | `/guides/json_mode` | **Passthrough** | smoke `c.json` on other providers; body untouched | No `json_schema`/structured outputs on DeepSeek. |
| Chat prefix completion (`prefix: true` on last assistant msg, beta) | `/guides/chat_prefix_completion` | **Unsupported** | `/beta` gap | |
| `logprobs` / `top_logprobs` (0–20) | `/api/create-chat-completion` | **Passthrough** | — | Gateway ignores `logprobs` in deltas (not a progress key) — harmless. |
| `stop` (≤16 sequences) | same | **Passthrough** | — | |
| `n` | not in the parameter table | **Unknown / likely unsupported upstream** | `classify` handles multi-choice arrays (`openai.py:123`) | |
| `seed` | not in the parameter table | **Unknown** | — | Test: send `seed`, see whether DeepSeek 422s or ignores. |
| `frequency_penalty` / `presence_penalty` | **deprecated, no effect** | **Passthrough** | — | Silent no-op upstream; no gateway concern. |
| `temperature` (0–2, default 1; **no effect in thinking mode**), `top_p` (floored to 0.95 in thinking) | `/api/create-chat-completion`, `/quick_start/parameter_settings` | **Passthrough** | — | A `temperature=0` "deterministic" workload on the default-thinking model is not deterministic. |
| `stream_options.include_usage` | same | **Passthrough**, not injected | `openai.py:77-78` only *reads* the flag (finding 27) | Without it a streamed DeepSeek call bills by estimate. Live bench sent it explicitly. |
| Vision: `image_url` (URL/base64), `detail: low|high|original|auto`, ≤600 images, 32 MiB each, **user messages only**, `deepseek-flash` only; `file` blocks via Files API | `/guides/vision` | **Passthrough** (chat); 48 MiB provider body cap vs gateway `max_request_bytes` 4 MiB default | `config.py:317` `max_request_bytes = 4 MiB` | A base64 image over ~3 MB is a 413 at the gateway before DeepSeek ever sees it — by design, but undocumented as a vision limit. |
| Embeddings | none on DeepSeek | n/a | — | |
| System messages | supported | **Passthrough** | — | |
| `user_id` (per-user isolation / concurrency, `[a-zA-Z0-9-_]{≤512}`) | `/api/create-chat-completion`, `/quick_start/rate_limit` | **Passthrough** | — | Could be *set by the gateway* to the tenant id for DeepSeek-side isolation of Layrs tenants; today the client would have to send it. |
| Multi-round: stateless, resend history | `/guides/multi_round_chat` | **Passthrough** | live bench `multi_turn` 12/12 OK (gpt-4o-mini; not run on DeepSeek) | |

## 3. Streaming protocol

| capability | DeepSeek (doc) | llmgw status | evidence | note |
|---|---|---|---|---|
| SSE `data:` JSON chunks, terminated by `data: [DONE]` | `/api/create-chat-completion` | **Supported** | `surfaces/openai.py:94,246-253` terminal marker + native ending; smoke 152 events + `[DONE]` | |
| `reasoning_content` deltas before `content` | `/guides/thinking_mode` | **Supported** (progress) | `openai.py:43` | |
| `usage` only in the final chunk (`null` before), needs `include_usage` | `/api/create-chat-completion` | **Supported** when present | `openai.py:120-124` META, `apply_usage` | |
| **`: keep-alive` SSE comment lines while queued (no hard limit → up to 10 min before inference)** | `/quick_start/rate_limit` | **Supported as liveness, not progress** — and therefore **times out at `first_event`** | `sse.py:59-73,323-329` comments surfaced; `pump.py:427` `mark_liveness()`; first-event budget 20 s (`config.py:359`) | Correct per C7. Consequence: a queued DeepSeek is a `FirstEventTimeout` at 20 s → `try_next` to the incumbent. Fine for a candidate plan; wrong for a DeepSeek-only workload unless `first_event` is raised. No metric distinguishes "queued at provider" from "provider dead". |
| **Non-streaming: "continuously return empty lines" before the JSON body** | `/quick_start/rate_limit` | **Unknown** | non-stream path not read for this shape; fakes have no "blank lines then JSON" mode | Test: fake mode emitting `\n` every 2 s for 30 s then JSON; check (a) client receives valid JSON prefixed by newlines byte-for-byte, (b) which clock fires, (c) usage parse still works. |
| `finish_reason` values `stop|length|content_filter|tool_calls|insufficient_system_resource|aborted` | `/api/create-chat-completion` | **Passthrough**, not read | no `finish_reason` handling anywhere in `src/` (grep) | `insufficient_system_resource` is a provider-side shed *inside a 200*: today it is `outcome=completed`, health neutral, no breaker signal, no fallback (post-commit anyway). Worth a counter. |
| Gzip on SSE | not documented; DeepSeek gzips JSON (finding 24) | **Supported** | `upstream.py` sends `accept-encoding: identity` (finding 24 fix) | |
| Cancellation propagates upstream | — | **Supported** | smoke `h.cancel` PASS (`live_smoke.md:21`) | |

## 4. Response & usage shape

| capability | DeepSeek (doc) | llmgw status | evidence | note |
|---|---|---|---|---|
| `usage.prompt_tokens` = hit + miss; `prompt_tokens_details.{cached_tokens, prompt_cache_hit_tokens, prompt_cache_miss_tokens}` | `/api/create-chat-completion` | **Supported** via `cached_tokens` | `openai.py:200-208` subtracts `prompt_tokens_details.cached_tokens` → disjoint buckets | Top-level legacy `prompt_cache_hit_tokens` not read; fine because the OpenAI-style mirror exists. |
| `completion_tokens_details.reasoning_tokens` | same | **Not read** | `openai.py:209-211` takes `completion_tokens` only; `accounting.py:97 TOKEN_KINDS` has no reasoning bucket (finding 27) | Cost is still right (DeepSeek bills reasoning at the output rate) but visibility is zero: a caller cannot see that 151/159 output tokens were thinking. |
| Cache-hit priced separately (peak: $0.006 flash / $0.044 pro per 1M) | `/quick_start/pricing` | **Supported** mechanism, **wrong numbers** | `accounting.py:302-311` dot product with `cached_input_per_m`; `catalog.py:266,294` values stale | See §7. |
| **Peak / off-peak pricing (off-peak = 50%; peak = 01:00–04:00 & 06:00–10:00 UTC, Mon–Fri)** | `/quick_start/pricing` | **Unsupported** | `ModelSpec` has one rate set, no time dimension (`catalog.py:110-121`) | Every DeepSeek cost record is wrong by 2× half the week, whichever rate the catalog holds. |
| Response `model` string | echoes the requested id | **Passthrough** | — | Gateway rewrites request `model` to the wire id (finding 21); response is not rewritten back, so a client that sent `deepseek.deepseek-v4-flash` sees `deepseek-flash`. |
| `system_fingerprint`, `id` | present | **Passthrough** | — | No request-id response header documented by DeepSeek; gateway's own `X-Gw-*` only. |
| Chat-template overhead billed (90 prompt tokens for a 7-word prompt) | not documented (`/quick_start/token_usage` silent) | **Recorded exactly, not modelled** | finding 37 | |

## 5. Errors & rate limits

| capability | DeepSeek (doc) | llmgw status | evidence | note |
|---|---|---|---|---|
| 400 invalid body (`error.type=invalid_request_error`, `param`, `code`) — includes unknown model | `/quick_start/error_codes`; real body `RESULTS.md:437-440` | **Supported** | `errors.py:898-910` status first, body refines; `_looks_like_unknown_model` → `ModelNotFound`; smoke `i.ghost_model` PASS | |
| 401 wrong key (body echoes `****xxxx`) | error codes; `live_smoke.md:81` | **Supported, scrubbed** | `AuthenticationFailed` scoped to credential; body replaced with `upstream_auth` (commit d90f5ed) | |
| 402 out of balance | error codes | **Supported** | `errors.py:515-540 InsufficientCredits`: no retry-same, `try_next`, health NEUTRAL, blame POLICY | |
| 422 invalid parameters | error codes | **Supported** | `errors.py:898` treats 400 and 422 alike → `InvalidRequest` | |
| 429 (per-account concurrency: 2,500 flash / 500 pro) | `/quick_start/rate_limit` | **Supported** | `RateLimited`, NEUTRAL health, `Retry-After` floor (`errors.py:497-512,778`) | DeepSeek does not document a `Retry-After` header; retry uses jitter. Gateway's own `max_concurrency=32` (`catalog.py:179`) is far below the provider's 2,500 — a self-imposed cap, fine. |
| 500 server error | error codes | **Supported** | `UpstreamServerError` `retry_same=True, try_next=True` (`errors.py:475-480`) | |
| 503 overloaded | error codes | **Supported** | `_OVERLOAD_STATUSES={503,529}` → `UpstreamOverloaded`, retry_same True (`errors.py:485-494,775`) | |
| **Queue-instead-of-429 behaviour, 10-minute server cutoff** | `/quick_start/rate_limit` | **Handled by clocks** (see §3) | `first_event=20 s`, `total=120 s` defaults | Not an error the gateway can classify; it shows up as `FirstEventTimeout` (provider-blamed → breaker FAILURE). A provider that is merely queueing will open its breaker after 5 such events in 30 s. Consider NEUTRAL health when keep-alive comments were seen during the wait. |
| In-body error inside a 200 | proxies do this | **Supported** | `openai.py:225-244 error_from_event` | |
| Error body shape is OpenAI-style | observed | **Supported** | `_error_hints` | |

## 6. Auth & headers

| capability | DeepSeek (doc) | llmgw status | evidence | note |
|---|---|---|---|---|
| `Authorization: Bearer <key>` | home | **Supported** | `upstream.py:315-316` | |
| Base URL `https://api.deepseek.com` (and `/v1`, `/beta`, `/anthropic`) | home, guides | `/`, `/beta`, `/anthropic` **Supported** (three provider rows) | `catalog.py:342` `deepseek`, `:370` `deepseek-anthropic`, `:380` `deepseek-beta` (`path_prefix="/beta"`); both `/beta/…` URL forms return 200 (17 Sep probe) | `/beta` and `/anthropic` rows are not in the live smoke yet. |
| No version header for the OpenAI dialect; `anthropic-version` on the Anthropic one | `/guides/anthropic_api` | n/a / would be injected | `upstream.py:312-314` | |
| `anthropic-beta: files-api-2025-04-14` for Files on the Anthropic path | `/guides/files_api` | **Passthrough** if a DeepSeek-anthropic provider existed | `config.py:104-106` forwards `anthropic-beta`, `openai-beta` | |
| Client-supplied `Authorization` never forwarded (BYOK not a thing) | — | **Supported** | `config.py:109-117 NEVER_FORWARDED` | |

## 7. Limits & catalog

Doc values today (`/quick_start/pricing`, per 1M tokens, **peak**; off-peak is half):

| model | context | max out | input cache-hit | input cache-miss | output | vision | concurrency |
|---|---|---|---|---|---|---|---|
| `deepseek-flash` | 1M | 384K (393,216) | $0.006 | $0.30 | $1.20 | yes | 2,500 |
| `deepseek-v4-pro` | 1M | 384K | $0.044 | $1.32 | $3.96 | no | 500 |

| catalog row (`catalog.py`) | field | repo | doc (peak) | doc (off-peak) | verdict |
|---|---|---|---|---|---|
| `deepseek.deepseek-v4-flash` → wire `deepseek-flash` (:290-298) | input_per_m | 0.14 | **0.30** | 0.15 | wrong (marked UNVERIFIED in-file) |
| | cached_input_per_m | 0.0028 | **0.006** | 0.003 | wrong |
| | output_per_m | 0.28 | **1.20** | 0.60 | **wrong 4.3×** |
| | context / max_output | 1,048,576 / 393,216 | 1M / 393,216 | | ok |
| | can_reason | False (unset) | thinking on by default | | **wrong** |
| | priced_at | 2026-06-22 | prices changed 2026-09-10 | | stale |
| `deepseek.deepseek-v4-pro` (:262-272) | input_per_m | 0.435 | **1.32** | 0.66 | **wrong 3×** |
| | cached_input_per_m | 0.003625 | **0.044** | 0.022 | **wrong 12×** |
| | output_per_m | 0.87 | **3.96** | 1.98 | **wrong 4.6×** |
| | can_reason | True | | | ok |
| | priced_at | 2026-06-22 | | | stale; and the model may be routed to Flash since 09-14 (see Doc deltas) |
| `openrouter.deepseek-v4-*` (:315-336) | api_model | `deepseek/deepseek-v4-flash`, `deepseek/deepseek-v4-pro` | DeepSeek retired `deepseek-v4-flash` | | OpenRouter's own ids; verify with `make probe`, out of scope here |

Other limits: DeepSeek request body 48 MiB vs gateway `max_request_bytes` 4 MiB (`config.py:317`); FIM 4K max; images ≤1,024 tokens each; `stop` ≤16; `top_logprobs` ≤20.

## Gaps ranked (impact on a Layrs caller using DeepSeek as the cheap candidate)

1. **Thinking is on by default at `high` and the catalog says `can_reason=False` for the Flash row.** Every candidate call pays for reasoning tokens at the output rate and a modest `max_tokens` yields empty content with `finish_reason=length` — the candidate "succeeds" (200, committed) with no answer, so the incumbent never runs. Fix: catalog `can_reason=True, reasoning="high"` on both rows, and a per-target request default (`thinking: {type: disabled}` / `reasoning_effort`) the policy can set — the gateway has no request-default mechanism at all today.
2. **DeepSeek prices are wrong 3–12× and have no peak/off-peak dimension.** Cost records for the whole DeepSeek candidate arm are fiction; the routing argument "DeepSeek is the cheap candidate" cannot be defended from `llmgw_cost_usd_total`. Fix: new numbers + `priced_at=2026-09-16`, and either a time-of-day rate schedule on `ModelSpec` or a documented "peak rate, upper bound" convention.
3. **Multi-turn with tools requires `reasoning_content` to be echoed back or DeepSeek returns 400.** Standard OpenAI SDK message handling drops it. The gateway passes the 400 through as `InvalidRequest` (client blame, no fallback) — correct classification, but the caller sees a working provider "randomly" reject turn 2. Documentation + a contract test against the fake.
4. **Queued requests (`: keep-alive` for up to 10 min) become `FirstEventTimeout` at 20 s, blamed on the provider, feeding the breaker.** A busy-but-healthy DeepSeek opens its own circuit after five queued requests. Fix: health NEUTRAL when liveness comments were observed during the wait, and a `queued_at_provider` counter.
5. **Non-streaming keep-alive ("continuously return empty lines") is untested.** Unknown whether the buffered path and usage parse survive a body that starts with newlines. One fake mode settles it.
6. **`insufficient_system_resource` / `aborted` finish reasons are invisible.** A provider-side shed inside a 200 counts as `completed`. Add a `finish_reason` label to `llmgw_committed_total` or a dedicated counter; it is the only signal that DeepSeek is degrading under load.
7. **No `stream_options.include_usage` injection** → streamed DeepSeek billing is an estimate unless every caller remembers the flag (finding 27; still open).
8. **No Anthropic-compatible DeepSeek provider row** → `/anthropic/v1/messages` workloads cannot use DeepSeek as a candidate at all (cross-dialect plans are rejected). Config-only fix, untested.
9. **`/beta` features — provider row shipped (`deepseek-beta`, `catalog.py:380`, `path_prefix="/beta"`); 17 Sep probe: `/beta/chat/completions` and `/beta/v1/chat/completions` both 200.** Strict tools and prefix completion are reachable but untested live; FIM still needs a `/v1/completions` surface.
10. **`reasoning_tokens` are not separated in usage/metrics**, so nobody can see that most DeepSeek "output" is thinking, which is what makes gap 1 hard to notice from dashboards.

Lower: `user_id` could carry the gateway tenant id for provider-side isolation; `/user/balance` could back a `credential_ok` probe; response `model` is not rewritten back to the catalog id; `ModelSpec.reasoning` vocabulary (`xhigh`) ≠ DeepSeek's (`max`).

## Doc deltas (repo vs docs today)

- **Model ids:** docs serve `deepseek-flash` + `deepseek-v4-pro`; `deepseek-v4-flash`/`-vision-exp` retired (temporary redirect). The catalog already uses `deepseek-flash` as the wire id (reconciled 2026-09-10) but keeps the catalog id `deepseek.deepseek-v4-flash` — fine, but the OpenRouter rows still name `deepseek/deepseek-v4-flash`.
- **V4 Pro contradiction in DeepSeek's own docs:** news 2026-09-10: "from September 14, all `deepseek-v4-pro` requests will route to V4.1-Flash at V4.1-Flash rates"; home page: "continue providing API services for DeepSeek V4 Pro after September 14, 2026, billing unchanged"; pricing page still lists Pro at 4.4× Flash. Until settled, `deepseek.deepseek-v4-pro` may be billed at either rate and served by either model. `make probe` + a priced call are the test.
- **Prices:** all six DeepSeek numbers in `catalog.py:262-298` differ from the pricing page (table in §7); `priced_at="2026-06-22"` predates the 2026-09-10 repricing.
- **Peak/off-peak windows** (01:00–04:00, 06:00–10:00 UTC Mon–Fri = peak; else 50% off) exist nowhere in the repo.
- **Thinking default:** docs say enabled/`high` on all models; `catalog.py` marks Flash non-reasoning and sets no `reasoning` default on either row.
- **`reasoning_effort`** values `none|low|high|max` vs `ModelSpec.reasoning` `off|low|high|xhigh`.
- **Concurrency:** provider allows 2,500/500 per account; catalog `max_concurrency=32` (deliberate, but the comment does not say so).
- **Request body cap:** provider 48 MiB vs gateway 4 MiB default — undocumented interaction for vision/base64.
- **`frequency_penalty`/`presence_penalty`** are deprecated no-ops; nothing in the repo claims otherwise.
- **Base URLs:** `/anthropic` and `/beta` exist upstream and now have provider rows (`deepseek-anthropic`, `deepseek-beta`); `/v1/responses` and `/responses` are the same endpoint (17 Sep probe), so the Responses surface keeps `/v1/responses` upstream for every provider.
- **No `Retry-After`** documented on 429; repo's floor logic is simply unused for DeepSeek.
- **Old doc URLs** referenced in this brief (`/guides/reasoning_model`, `/guides/function_calling`) now redirect to the quick start; use `/guides/thinking_mode` and `/guides/tool_calls`.

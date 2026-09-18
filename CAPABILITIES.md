# Capability matrix: OpenAI, Anthropic, DeepSeek vs llmgw

Swept 2026-09-16 against the providers' current documentation (OpenAI docs now
live at `developers.openai.com`, Anthropic at `platform.claude.com`, DeepSeek at
`api-docs.deepseek.com`) and this repository at commit `de35c06`. One detailed
file per provider, with a doc URL and `file:line` evidence on every row:

- [capabilities/openai.md](capabilities/openai.md)
- [capabilities/anthropic.md](capabilities/anthropic.md)
- [capabilities/deepseek.md](capabilities/deepseek.md)
- [capabilities/voice.md](capabilities/voice.md): AssemblyAI, ElevenLabs,
  Inworld and OpenAI audio/Realtime, with the Layrs voice agent as the
  reference caller (summary in the "Voice providers" section below)

Legend. **S** supported: the gateway parses, accounts, classifies or routes it.
**P** passthrough: forwarded byte-for-byte and it works, but the gateway is
blind to it (not counted, not classified, not in the catalog). **U**
unsupported: no route, header stripped, parser would break, or rejected by
design. **?** unknown: untested; the detailed file names the test that settles
it. **–** not applicable to that provider.

## The one-paragraph verdict

The chat path is in good shape on all three providers: streaming, tools and
tool round trips, JSON mode, vision by URL and base64, cancellation, gzip,
heartbeats, in-stream errors, and the 400/401/402/404/429/5xx/529 taxonomy
are handled natively and were exercised against real providers (11 Sep smoke,
16 Sep live bench: 122/122 OK). Because the body is never re-serialised,
almost every request feature the providers added since the code was written
works today as passthrough. The gaps are of three kinds: **things the gateway
cannot see** (stop reasons, reasoning and cache-write tokens, server-tool
usage, upstream request ids, rate-limit headers), **a catalog that has drifted
from the price lists and model line-ups** (DeepSeek prices off 3 to 12x,
Sonnet 4.6 context 5x too low, current models absent, one alias retired), and
**surfaces that do not exist** (files, batches; `/v1/models`, `count_tokens`
and embeddings shipped in Phase C on 16 Sep 2026 and `/v1/responses` in
Phase F on 18 Sep 2026, see below). Two of the gaps are correctness
bugs a caller will hit in the first week: a response `model` that cannot be
sent back, and out-of-money 429s that are retried.

## 1. Endpoints

| Capability | OpenAI | Anthropic | DeepSeek | Note |
|---|---|---|---|---|
| Chat / Messages (streaming and buffered) | S | S | S | `/v1/chat/completions`, `/anthropic/v1/messages`; `model` must be the catalog id |
| Responses API | S | – | S (stateless) | `surfaces/responses.py` (Phase F, 18 Sep 2026); `background: true` → 400 at the gateway, no upstream call; DeepSeek + `previous_response_id`/`conversation` → 400 because DeepSeek silently drops them (CONTRACTS.md C22) |
| Models list | U (404) | U | U | Some agent frameworks call it at boot; `/probe` is the gateway's own view |
| Token counting | – | U | – | Free pre-flight endpoint callers use for budgeting |
| Embeddings | U | – | – | Cheap buffered surface if wanted |
| Images generation, audio, files, batches, fine-tuning, vector stores, admin | U | U | U | Not a streaming-gateway concern except batch cost |
| Realtime (WebSocket) | U | – | – | Out of scope by architecture |
| Provider-specific alternate bases | – | – | U | DeepSeek `/beta` (strict tools, prefix completion, FIM) and `/anthropic` need provider rows; `join_url` may not tolerate `/beta/v1` |
| Workload-scoped routes `/workloads/{w}/...` | S | S | S | Gateway addition |

## 2. Chat-path request features

| Capability | OpenAI | Anthropic | DeepSeek | Note |
|---|---|---|---|---|
| Streaming | S | S | S | |
| Usage in stream | S (reads `include_usage`) | S (`message_start` + `message_delta`) | S when caller sets `include_usage` | Gateway does not inject `stream_options.include_usage`; without it OpenAI-dialect streams bill by estimate (finding 27, open) |
| Tools, tool_choice, strict | P (works) | P | P (`strict` needs `/beta`: U) | Tool round trip live-tested on OpenAI and DeepSeek only; Anthropic round trip untested (?) |
| Parallel tool calls | ? | P | ? | Contract test against a two-index fake settles it |
| Structured outputs / JSON | P (works) | P | P (`json_object` only) | OpenAI deprecates `json_object` for `json_schema` |
| Vision by URL / base64 / file id | P (works) / P / P (needs Files API) | P / P / P | P / P / P | Gateway body cap 32 MiB per surface since Phase B (was 4 MiB); providers allow 512 MB / 32 MB / 48 MiB |
| PDF / documents | P | P | – | Same body cap |
| Audio in/out | P, accounting blind | – | – | Audio tokens priced differently; mis-costed if used |
| Reasoning: request params | P | P (`enabled`, `adaptive`, `effort`) | P | Catalog `reasoning` vocabulary is `off/low/high/xhigh`; APIs use `none/minimal/low/medium/high/xhigh/max` (OpenAI) and `none/low/high/max` (DeepSeek) |
| Reasoning: default behaviour | provider default | adaptive by default on 4.6+ | **on by default at `high` on every model** | No per-target request defaults exist, so the "cheap candidate" pays for reasoning and can return empty content at small `max_tokens` |
| Reasoning content in stream | – (not on chat) | S as progress (`thinking_delta`) | S as progress (`reasoning_content`) | Long thinking does not trip the progress clock: correct |
| Reasoning content on input turns | – | P | P, **required when tools present or provider 400s** | The standard OpenAI SDK drops it; first multi-turn-tool failure a caller hits |
| Prompt caching: request side | P (`prompt_cache_key`, options) | P (`cache_control`, 1h TTL) | automatic | Explicitly preserved by not re-serialising |
| Prompt caching: read accounting | S | S (disjoint convention, finding 25) | S | |
| Prompt caching: write accounting | U (`cache_write_tokens` unread) | U (`cache_write_per_m` unset on both entries; 5m/1h split discarded) | – | Anthropic writes priced at 1.0x instead of 1.25x / 2.0x |
| `max_tokens` handling | S (`max_completion_tokens` preferred) | P (read, not validated) | P | Used for deadline sizing only |
| `n > 1` | P, transcript is choice 0 | – | ? | Billing correct (usage is aggregate) |
| `service_tier`, `inference_geo`, priority | P | P | – | Flex tier vs the 20 s first-event budget; 1.1x geo multiplier not costed |
| Server / hosted tools (web search, code exec, MCP connector) | P on chat, S on Responses (counted) | P | – | Responses: `*_call.completed` events / `output[]` items → `server_tool_calls`, with `response.tool_usage.web_search.num_requests` authoritative; on `llmgw_server_tool_calls_total`. Costed only where `ModelSpec.tool_rates` has a rate (Anthropic rows do, OpenAI rows do not yet) |
| Prediction, verbosity, moderation, metadata, store, logprobs, seed, stop, sampling params | P | P | P | `store` stores under the provider project with the rewritten model |
| Fine-tuned model ids | U | – | – | Need a catalog row with a price |
| Body model rewrite | S | S | S | `X-Gw-Body-Modified: 1`; response `model` is the provider's wire id, see gap 1 |
| Request body cap | 32 MiB | 32 MiB | 32 MiB | `LLMGW_MAX_REQUEST_BYTES` global default, per-surface override `LLMGW_MAX_REQUEST_BYTES__<SURFACE>` (Phase B) |

## 3. Streaming protocol

| Capability | OpenAI | Anthropic | DeepSeek | Note |
|---|---|---|---|---|
| SSE framing, byte-split invariance | S | S | S | |
| Terminal marker and native ending on post-commit failure | S (`[DONE]`) | S (no `message_stop`) | S (`[DONE]`) | CONTRACTS C2 |
| Heartbeats | S (empty-choices, comments) | S (`ping`) | S (`: keep-alive` comments) | Liveness only, never progress (C7) |
| Queued-at-provider behaviour | – | – | **`FirstEventTimeout` at 20 s** | DeepSeek queues up to 10 min while sending comments; the timeout is provider-blamed and feeds the breaker |
| Non-streaming keep-alive (blank lines before JSON) | – | – | ? | Fake mode settles it |
| In-stream error inside a 200 | S | S (`overloaded_error` etc.) | S | Failure, not content |
| Unknown future event types | S (ignored) | S (META) | S | |
| Responses semantic events | S | – | S | `response.completed`/`response.incomplete` terminal; `response.failed`/`error` forwarded byte-for-byte and classified, never synthesised (C2, C22); no `[DONE]` |
| Frame size bound | S (1 MiB) | S | S | Logprobs-heavy or base64-audio frames could exceed it |
| Compression | S (`identity` upstream) | S | S | Finding 24 |
| HTTP/2 to provider | S | S | S | |

## 4. Response and usage shape

| Capability | OpenAI | Anthropic | DeepSeek | Note |
|---|---|---|---|---|
| Base token counts | S | S | S | |
| Cached input tokens | S | S | S | |
| Cache write tokens | U | U (collapsed, unpriced) | – | |
| Reasoning tokens | U | U (`thinking_tokens`) | U | Cost still right (billed as output); visibility zero |
| Audio / image / prediction token details | U | – | – | |
| Server-tool usage, compaction iterations | – | U | – | Web search at $10 per 1k requests invisible |
| Finish / stop reason | P (not recorded) | P (not recorded) | P (not recorded) | `length`, `refusal`, `max_tokens`, `pause_turn`, `model_context_window_exceeded`, `insufficient_system_resource` all count as `completed` |
| Response `model` field | P (snapshot id) | P (wire id) | P (wire id) | Cannot be sent back as-is: gap 1 |
| Upstream request id header | U (dropped) | U (dropped) | – | Nothing to quote to provider support |
| Processing-time header | U (dropped) | – | – | Free provider-vs-gateway split |
| Rate-limit headers | U (dropped, unread) | U (dropped, unread) | – (none documented) | Correct not to forward; nothing consumes them either |

## 5. Errors and rate limits

| Capability | OpenAI | Anthropic | DeepSeek | Note |
|---|---|---|---|---|
| 400 invalid request, unknown model, context length | S | S | S | Body sniff; `param` deliberately excluded (finding 7) |
| 401 / 403 | S, scrubbed | S, scrubbed | S, scrubbed | 403 is provider-scoped since Phase A (`forbidden_means`: auth, rate_limit, policy); the default `auth` is right for these three providers |
| 402 | – | S | S | `InsufficientCredits`: no retry-same, try-next, POLICY blame |
| **Out-of-money as 429** | **U: `credit_balance_exhausted`, spend and usage limit codes retried as `RateLimited`** | **U: `enforced_spend_limit_reached` (no `retry-after`) retried** | – | Same bug class as finding 8 on two more providers |
| 404 | S | S | S | |
| 409, 413 from provider | – | U (fall to `UpstreamServerError`, retried, breaker-visible) | – | 413 masked today by the gateway's own cap |
| 429 rate limit with `Retry-After` floor | S | S | S (no `Retry-After` documented) | |
| 500 / 504 | S | S | S | |
| 503 / 529 overloaded | S | S | S | |
| Mid-stream error after 200 | S | S | S | |
| Provider-side shed inside a 200 | – | – | U (`finish_reason=insufficient_system_resource`) | Only signal that DeepSeek is degrading; invisible |

## 6. Auth and headers

| Capability | OpenAI | Anthropic | DeepSeek | Note |
|---|---|---|---|---|
| Provider credential injection | S (bearer) | S (`x-api-key`, `anthropic-version: 2023-06-01`) | S (bearer) | Client `authorization` is the tenant token, never forwarded |
| Bearer / OAuth tokens upstream | – | U | – | Blocks Workload Identity Federation later |
| Beta headers from client | S (`openai-beta`) | S (`anthropic-beta`) | S | |
| Org / project / workspace headers | U (operator can pin via `extra_headers`) | U (`anthropic-workspace-id`) | – | Correct default: the gateway owns the credential |
| Client request id forwarded upstream | S (`x-request-id`) | S | S | OpenAI's new `X-Client-Request-Id` not in the list |
| Trace context | S | S | S | |
| BYOK per tenant | U | U | U | Out of scope per PLAN |

## 7. Limits and catalog

| Item | OpenAI | Anthropic | DeepSeek |
|---|---|---|---|
| Rows shipped | `gpt-4o-mini` only; matches docs (verified 2026-09-16) | `haiku-4-5`, `sonnet-4-6` | `deepseek-v4-flash` (wire `deepseek-flash`), `deepseek-v4-pro`, two OpenRouter rows |
| Prices | correct | correct base rates; `cache_write_per_m` unset | **wrong 3 to 12x** (repriced 2026-09-10); peak/off-peak schedule inexpressible |
| Context / max output | correct | Sonnet 4.6 context **200K in catalog, 1M in docs**; outputs correct | correct |
| `can_reason` | – | Haiku 4.5 **False, should be True** | Flash **False, should be True** (thinking default on) |
| Missing current models | `gpt-6-astra`, `gpt-5.6-*`, `o3` | `claude-sonnet-5` ($2/$10, cheaper than 4.6 on every axis), `opus-5`, `fable-5-1` | – |
| Retirements | none | Haiku 4.5 retirement floor **2026-10-15** | `deepseek-v4-flash` alias retired; V4 Pro routing contradictory in DeepSeek's own docs |
| Provider request body | 512 MB | 32 MB | 48 MiB |

## Gaps ranked across providers

Ranked by impact on the first real caller (a tcg-style agent: streaming, tools,
multi-turn, vision, JSON, DeepSeek as the cheap candidate).

1. **The response `model` cannot be sent back.** The gateway rewrites the
   request model to the wire id and does not translate the response, so an SDK
   loop that echoes `model` into the next turn gets `400 policy_error: unknown
   model` (seen live 16 Sep). Fix: accept wire ids and snapshot ids as aliases
   in `policy.plan_for`, plus rewrite `model` back on the buffered path. Small.
2. **Out-of-money is retried on OpenAI and Anthropic.** OpenAI signals it as
   429 with codes (`credit_balance_exhausted`, spend and usage limits,
   `insufficient_quota`); Anthropic as 429 `enforced_spend_limit_reached`
   without `retry-after`. Both classify as `RateLimited` (retry-same, NEUTRAL)
   instead of `InsufficientCredits` (try-next, POLICY blame). Body-code sniff at
   the 429 rule in `errors.py`, mirror of finding 8. Small.
3. **Stop and finish reasons are invisible.** Truncation (`length`,
   `max_tokens`), `refusal`, `pause_turn`, `model_context_window_exceeded` and
   DeepSeek's `insufficient_system_resource` all record as `completed`. An agent
   truncated on every turn is 100% success on the dashboard. Fix: read the
   field in `apply_usage`, closed-set label on a counter, capture field. Small.
4. **Catalog drift.** DeepSeek prices 3 to 12x off and stale since the 10 Sep
   repricing; Sonnet 4.6 context 200K vs 1M; `can_reason` wrong on Haiku 4.5
   and DeepSeek Flash; Sonnet 5 absent although cheaper than the shipped Sonnet;
   no current OpenAI reasoning model; Haiku 4.5 retires in a month. Catalog
   edits plus `make probe` in CI. Small, but touches billing.
5. **DeepSeek thinks by default and the gateway cannot say otherwise.** No
   per-target request defaults exist, so a candidate plan pays reasoning rates
   and small `max_tokens` yields empty content that still counts as a committed
   success, so the incumbent never runs. Fix: `ModelSpec` request defaults
   (`thinking`, `reasoning_effort`) applied only when the client omitted the
   key. Medium; interacts with byte-for-byte passthrough (buffered rewrite is
   fine; the request body is already rewritten for `model`).
6. **Cache-write and detail tokens are not accounted.** Anthropic cache writes
   priced at 1.0x (should be 1.25x / 2.0x by TTL); OpenAI `cache_write_tokens`
   unread; reasoning, audio, image, prediction, server-tool and compaction
   usage dropped. Cost is exact only for text plus cache reads. Medium.
7. **A queueing provider looks like a dead one.** DeepSeek holds requests up to
   10 min with keep-alive comments; the gateway's 20 s first-event timeout is
   provider-blamed and five of them open the breaker. Fix: NEUTRAL health when
   liveness was observed during the wait, and a `queued_at_provider` counter.
   Small.
8. **Body cap is global.** (Closed in Phase B: 32 MiB default, per-surface overrides.) Was 4 MiB against 32 MB (Anthropic), 48 MiB (DeepSeek),
   512 MB (OpenAI) means base64 vision and PDFs get the gateway's 413 first.
   Make it per surface or per workload. Small.
9. **Nothing from the provider's response headers survives.** Upstream request
   id and processing time reach neither the client nor the capture record;
   rate-limit headers are neither surfaced nor consumed, so a shared key's
   token budget is invisible until the 429. Add the request id to capture and an
   `X-Gw-Upstream-Request-Id` header; export `remaining` and `reset` as gauges
   per credential. Small to medium.
10. **Surfaces that did not exist — closed.** `/v1/models`, `count_tokens` and
    `/v1/embeddings` shipped in Phase C (16 Sep 2026); `/v1/responses` shipped
    in Phase F (18 Sep 2026: `surfaces/responses.py`, CONTRACTS.md C22, wire
    captures in `capabilities/captures-responses.md`, live smoke `responses *`
    cases). Still missing: files, batches, and the Responses polling routes
    (`GET /v1/responses/{id}`), which is why `background: true` is a 400.
11. **Untested shapes.** Anthropic tool round trip, vision, PDF and structured
    outputs on the Anthropic surface; parallel tool calls on any provider;
    DeepSeek non-streaming keep-alive; `/beta/v1` path joining. Each has a named
    test in the detailed files; five smoke cases and two fake modes cover them.
12. **Minor misclassifications.** 403 region block as bad key; Anthropic 409 and
    413 as retryable server errors; reasoning-effort vocabulary drift in
    `ModelSpec`; `stream_options.include_usage` not injected (finding 27).

## Voice providers: AssemblyAI, ElevenLabs, Inworld, OpenAI audio

Full matrix in [capabilities/voice.md](capabilities/voice.md). The short
version: **nothing the Layrs voice agent runs in production can pass through
llmgw today, and the reason is transport, not design.** Production TTS and
STT are Inworld over bidirectional WebSockets; the fallbacks are OpenAI's
binary TTS stream and AssemblyAI's WebSocket STT. The gateway has no
WebSocket route or upstream client and a pump that feeds every byte to an SSE
parser, so a binary or NDJSON stream is copied to the client for a few
seconds, then cut by the first-event clock or the 1 MiB frame bound and
recorded as a provider stall with $0 accounted.

| Need | Covers | Size |
|---|---|---|
| Second and third framer (JSONL, raw bytes) beside the SSE parser | Inworld and ElevenLabs HTTP TTS, OpenAI binary TTS | small |
| `ProviderConn.auth_scheme` (raw key, `xi-api-key`, Basic) and a wider scrub for reversible credentials | all three non-OpenAI providers | small |
| Units other than tokens (characters, seconds, audio token kinds) | every voice product; also the audio tokens already arriving unpriced on the chat route | medium |
| Per-surface body caps and multipart forwarding | OpenAI and ElevenLabs STT, AssemblyAI sync; also chat vision and PDF | medium |
| Voice HTTP surfaces with path and query templating, a `tts` budget profile | all HTTP voice paths | medium each |
| Token minting routes with pinned session config and capped duration | OpenAI `client_secrets`, AssemblyAI `/v3/token`, ElevenLabs tokens | small, highest leverage |
| A WebSocket data plane | the production voice path | a second program, not a phase |

What transfers unchanged: deadlines, commitment, byte-bounded backpressure,
per-credential concurrency (which is how ElevenLabs and Inworld meter it),
credential-scoped breakers, admission, the liveness-versus-progress split,
the status taxonomy, capture and drain. Two collisions to fix regardless: 403
is a rate limit on AssemblyAI and a plan/voice denial on ElevenLabs, and both
would open the credential breaker; and voice sessions run to hours against a
120 s total budget and 130 s drain grace. Layrs-side drift found on the way:
a deprecated Inworld TTS model in production, an AssemblyAI default model
running at 3x the labelled rate, a legacy OpenAI STT model, and per-minute
prices for per-character products.

## Suggested order

- **First pass, all small, all correctness:** gaps 1, 2, 3, 7, and the catalog
  corrections in 4 (prices, contexts, `can_reason`, `cache_write_per_m`,
  Sonnet 5, a current OpenAI reasoning model). Each is a one-file change with
  an obvious unit test; together they change what a caller sees in week one.
- **Second pass, accounting and defaults:** gaps 5, 6, 8, 9, plus
  `include_usage` injection and the 409/413/403 rules.
- **Third pass, surfaces (done 16-18 Sep 2026):** `/v1/models`, `count_tokens`,
  `/v1/embeddings` as buffered passthroughs; `/v1/responses` as its own
  surface; DeepSeek `/anthropic` and `/beta` provider rows. Still open:
  peak/off-peak pricing on `ModelSpec`.
- **Tests to settle the unknowns** can go in at any point and should go first
  if an Anthropic tool-using caller is imminent.

## Doc deltas worth fixing in the repository

- `surfaces/openai.py:186-189` claims the chat API reports no cache creation
  count; it now reports `prompt_tokens_details.cache_write_tokens`.
- `catalog.py`: Sonnet 4.6 `context_window` 200_000 should be 1_000_000; Haiku
  4.5 and DeepSeek Flash `can_reason` should be True; all six DeepSeek prices
  and `priced_at`; both Anthropic `cache_write_per_m`; OpenRouter rows still
  name `deepseek/deepseek-v4-flash`.
- `errors.py:894` maps only HTTP 402 to `InsufficientCredits`; OpenAI and
  Anthropic use 429 codes for the same state.
- `ModelSpec.reasoning` literal does not cover `none`, `minimal`, `medium`,
  `max`.
- OpenAI deprecations: `user` (use `safety_identifier`), `max_tokens`,
  `prompt_cache_retention`, `json_object`. New headers: `X-Client-Request-Id`,
  `openai-organization`, `x-ratelimit-*-project-tokens`.
- `live/smoke.py` and `bench/live_fly.py` use `thinking: enabled` with
  `budget_tokens`; correct for Haiku 4.5, deprecated on Sonnet 4.6 and a 400 on
  4.7 and later. Use `adaptive` plus `output_config.effort` there.
- Old doc URLs in SOURCEBOOK and the teaching notes point at
  `platform.openai.com/docs` and `docs.anthropic.com`; both hosts redirect.
- Finding 35 ("output ceilings too low") is resolved for outputs; the stale
  number is now Sonnet 4.6's context window.

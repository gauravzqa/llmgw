# Failure modes register

Every way this gateway can fail, what it takes down with it, how you find out,
what is built to contain it, and what is left over. Grown one module at a time;
a row is only marked *built* when a named test enforces it.

Read the **Residual** column carefully. It is the honest half — the part a
design review is actually for — and a row with an empty residual column is
usually a row nobody thought hard about.

| # | Failure | Blast radius | Detection | Mitigation | Residual risk | State |
|---|---|---|---|---|---|---|
| 1 | Provider returns 5xx | One (provider, model) | `attempts_total{result="failed"}` | Pre-commit fallback to incumbent; breaker; retry budget | Post-commitment failures are unrecoverable **by design** — C1 forbids the splice | built |
| 2 | Provider stalls mid-stream | One request, one connection | `progress` clock; `requests_total{outcome="interrupted"}` | Progress clock ends it in ~300 ms; native ending per C2 | Cannot distinguish a slow model from a dead one; a genuinely slow reasoning model can be cut | built |
| 3 | Provider heartbeats but never produces | One request, up to the total deadline | `progress` vs `liveness` divergence | Heartbeats reset liveness only (C7) | If a provider's heartbeat comes from the same component as its tokens, we cut a healthy stream | built |
| 4 | Slow client | Memory first, then everything | `pump_buffered_bytes`, `client_stall_seconds` | Byte-bounded pump; `client_stall` budget ends the request | A slow client still holds a tenant permit for its whole duration | built |
| 5 | Client disconnect not detected | Leaked upstream spend and connections | `permits_in_use` not returning to zero; `tasks` gauge drift | Cancellation propagated within one event interval; chaos-tier invariants | ASGI disconnect detection is server-dependent; uvicorn only surfaces it between reads | built |
| 6 | One hot tenant | Every other tenant | `admission_denied_total{reason}`; per-tenant p99 | Per-tenant token bucket + concurrency permit, at ingress; a concurrency denial costs no rate credit (C6) | **Process-local.** N instances give a tenant N× its cap; shared state not built. Isolation is a claim about *admission*, not about provider health — a shared `(provider,model)` breaker still refuses every tenant, correctly | **built** |
| 7 | Shared provider key exhausted | Every tenant on that key | `permits_in_use{scope="provider_key"}` | Per-credential concurrency cap (`ProviderKeyLimiter`), separate from tenant caps, keyed on the credential so `openrouter`+`openrouter-toolsafe` share it; `try_next` to a different key | Cap is a connection count, not a token count; a token limit would need a per-call estimate | **built** |
| 8 | One tenant's BYOK key revoked | Would be: every tenant on that provider | `breaker_transitions_total` | Auth failures scope to the **credential alone** (`("cred", id)`, no provider entry), so a revoked key opens exactly one circuit — and two entries sharing a key share it, tripping once not twice | — | **built** |
| 9 | Capture sink stalls or fills the disk | Would be: every request | `capture_queue_bytes`, `capture_dropped_total` | Bounded-by-**bytes** queue, single drain worker, hot path never awaits the sink (`offer()` is sync and non-raising) | Dropped records are **lost, not deferred**. Diagnostic loss during exactly the incident you want to investigate | **built** |
| 10 | Config reload mid-request | Split-brain: routed by v1, billed by v2 | `policy_id` + `catalog_id` on every capture record | Immutable `PolicySnapshot` pinned per request; **two** ids, because routing lives in the policy file and prices live in the catalog and they version independently | Age is deliberately unbounded but now *observable* via `age(clock)`. A 40-minute stream can still bill on 40-minute-old prices — the right ceiling is a deployment property this module cannot know | **built** |
| 11 | Deploy during open streams | Every in-flight stream | `draining` gauge; client error count during deploy | SIGTERM → readiness 503 → finish open streams → exit. `ServerConfig.validated()` refuses `budgets.total > drain_grace_seconds` (defaults 120 s / 130 s; `LLMGW_DRAIN_ALLOW_SHORT=1` downgrades it to a startup warning). uvicorn's own post-drain wait is bounded to 3 s, and nothing on the shutdown path logs per stream: cuts are counted (`ShutdownCuts`) and reported as one WARNING after the server stops, so shutdown stderr is bounded by the catalog, not by open streams (the S8-B pipe hang) | The third number is outside the process: the orchestrator's kill timeout must exceed the grace, and nothing here can check it. A workload whose policy-file `total` exceeds the grace is still cut. The 600 s / 25 s pair shipped for six phases with every test green | **built** |
| 12 | Retry amplification | The provider, then everyone | `attempts_total / requests_total` | One absolute `Deadline`; retry budget; `X-Gw-No-Retry` (C5) | Only holds if every layer agrees who retries. We can offer the header; we cannot make callers use it | **partly built** |
| 13 | Breaker flapping | Availability oscillates | `breaker_transitions_total` rate | Per-`(provider,model)` breaker: sliding window, one half-open probe, epoch-fenced stale results, `NEUTRAL` for 429/cancel/breaker-open | **Counts, does not rate**: 5 failures/30s trips a 10k-rps target at 0.05% errors. Thresholds are guesses until S7 | **built** |
| 14 | Price table wrong (not merely stale) | Every cost report and routing decision | `live/probe.py` reconciles against the providers' own endpoints, free | Mandatory `priced_at`; `stale_prices()` in CI | **The date column is not a proof.** On 2026-09-10 six rates were wrong by 1.7x-20x while every one passed the 120-day freshness check — fresh and wrong simultaneously. A shorter threshold only re-asserts a wrong number more often; only a source fixes it. `probe` is not yet wired into CI | **partly built** |
| 15 | Metric label explosion | The metrics backend, then the gateway's memory | `total_series()` projection test | Closed label vocabularies; unbounded fields go to capture; histogram label limit | "Active targets" is a runtime property — a routing change can move you past the design point without a code change | **built** |
| 16 | Catalog misconfiguration | One workload, discovered on the hot path | Startup validation | `Catalog._validate()` raises at construction | — | **built** |
| 17 | Huge single event (8 MiB `data:` line) | Memory, and every co-resident stream | `FrameTooLarge` counter | Byte-bounded parser frame limit, checked against the frame under construction (not the buffer — see below) | **The advertised bound is optimistic by 2x**: `SSEEvent` holds the payload in both `raw` and `data`, so a 1 MiB bound is ~2 MiB resident at dispatch. Also, `feed()` appends a whole chunk before any check, so the real defence against a hostile *call* is httpx's read size, not this bound | **built** |
| 18 | fd exhaustion | The whole process | `upstream_connections` gauge vs `ulimit -n` | Per-provider pooling; per-credential concurrency caps | macOS default `ulimit -n` is 256; the cap has to be set from the limit, and nothing currently reads it | partly built |
| 19 | Permit/ticket leak on an error path | Capacity ratchets to zero over hours | `permits_in_use` / breaker `probes_in_flight` drift | Permit + two breaker tickets, each released on all four exits; multi-tenant chaos asserts all return to zero after every iteration; a breaker without a limiter is refused at construction (so a full pool cannot masquerade as a provider fault and open a circuit) | Invisible without the drift gauges; still process-local | **built** |
| 21 | Provider gzips the response body | Every request to that provider | None — the buffered case returns HTTP 200 with a correct `content-length` and a gzip body, and **nothing raises anywhere** | `accept-encoding: identity` on every upstream client | Any future client constructed without that header reintroduces it silently. Found only against real providers: 745 local tests passed over it because the fakes do not compress | **built** |
| 22 | Process saturated: degrades before it sheds | Every stream on that process, then every client waiting on one | `llmgw_admission_denied_total{reason="overloaded"}` rising means the cap is working; TTFE p50 climbing with that counter flat means it is set too high. `/probe` shows `inflight` against `max_streams` | Per-process cap `max_streams` (`LLMGW_MAX_STREAMS`, default 150: the largest cap at which S2 shed before it degraded on the campaign laptop; 300 still pinned CPU), checked at ingress after the tenant and before admission: 503 `overloaded`, `Retry-After: 1`, no bucket credit, no body read, no upstream call. Pair with the edge's connection limit so the balancer stops routing before the process refuses | **A stream count standing in for an event rate.** 150 slow streams are idle and 150 fast-model streams are 6,000 events/s; the honest signal is event-loop lag and it is not measured. Per-process: N replicas shed independently and a fleet-wide overload is N local decisions. On 2026-09-10 (S2) four processes went from 29 ms to 2.6 s p50 TTFE with zero refusals; on 2026-09-15 cap 150 held admitted TTFE at the direct arm's 31 ms with zero 504s while cap 300 still saturated (`load-S2-cap150-gw4-run.md`, `load-S2-cap300-gw4-run.md`). Both numbers are one laptop's; the default is a number to re-derive per deployment, not a fact, and S4 re-run with cap 150 on 2026-09-15: the cap breaks first, 150/process admitted, 96.7% shed as 503, admitted streams within 2 ms of the direct arm at p99, 57 MB / 18% CPU per process ([load-S4-cap150-gw4-run.md](bench/results/load-S4-cap150-gw4-run.md)) | **built** |
| 23 | Production gateway on the zero-config tenant path | Every caller shares one bucket; a rotated or leaked token means nothing because none is checked | Startup WARNING; `/probe` `tenant_mode: anonymous` (nobody reads either during an incident) | `LLMGW_REQUIRE_TENANTS=1` refuses to start without a tenants file or with one whose only tenant is `anonymous` (C10); tokens resolve from `token_env` at startup so the file is committable and a missing secret fails the deploy, not the tenant | Opt-in: a deployment that forgets the flag is back on the loud-but-permitted path. Enforced per process, so it cannot notice a fleet where one replica was started differently | **built** |
| 24 | Provider echoes credential material in an error body | Under a shared key, whoever sent the request learns part of the key | None until the live run read the body (findings #30) | `AuthenticationFailed` (401/403) keeps the upstream status and replaces the body with the gateway's own `upstream_auth` error (C11); no upstream response header reaches the client from the error path in the first place | Scoped to 401/403 by class. A provider that quotes a key in some OTHER status's body (a 400 on a malformed key, say) is still forwarded byte for byte under C4; the classifier, not a body scan, decides. Under BYOK the scrub also hides the tenant's own key from the tenant, which is the safe direction but a worse message | **built** |
| 25 | Out of money arrives as a 429 | Every request on that credential is retried with backoff against a provider that cannot succeed; the bill is blamed on nobody | `llmgw_requests_total{code="insufficient_credits"}` rising with `blame=policy` in capture | Body-code rule at the 429 site: OpenAI `insufficient_quota` and the credit/spend/usage-limit codes, Anthropic `details.error_code=enforced_spend_limit_reached`, and Anthropic's 400 usage-limit prose classify as `InsufficientCredits` (no retry-same, try-next, NEUTRAL, POLICY) (C13) | The codes are doc-transcribed, not live-captured; a provider that renames one is back to `RateLimited` until the fixture is refreshed. Three providers covered; OpenRouter's 402 was the only live observation | **built** |
| 26 | A provider queues instead of refusing | A busy-but-healthy provider (DeepSeek holds requests up to 10 min sending keep-alive comments) trips the first-event clock five times and opens its own circuit; every tenant loses it | `llmgw_queued_at_provider_total` rising while `llmgw_breaker_state` stays closed | `FirstEventTimeout(queued=True)` when liveness was seen before the clock fired: NEUTRAL health, still `try_next`, counted (C13). Fallback behaviour unchanged, so the client's budget is respected | The pump decides "liveness was seen"; a provider that queues silently (no comments) is indistinguishable from a dead one and is still counted as FAILURE, correctly. **The forwarded keep-alives COMMIT the response** (a comment line is a body byte, C1), so today a queued wait that outlives the budget is truncated post-commitment rather than falling back: the executor's commitment hold would have to be extended past heartbeat-only chunks for the incumbent to get the request (executor-owned, not built). Per process; no cross-replica view of a queueing provider | **built** |
| 27 | Upstream body is not the framing the surface expects (NDJSON or raw audio under an SSE surface, or SSE under the wrong content type) | That request: a few seconds of misframed bytes reach the client, then a cut recorded as a provider stall with $0 billed; the target takes a FAILURE it did not earn | `llmgw_requests_total{code="unsupported_upstream_framing"}`; before the fix it hid under `first_event_timeout` / `frame_too_large` with `cost_usd=0` | Surfaces declare `framing` (sse, jsonl, raw) and the pump uses the matching `Framer`; an SSE surface refuses a non-`text/event-stream` body as a 502 before the status is committed, so it falls back (C14) | Only the SSE case is checkable from a content type; a `jsonl` or `raw` surface trusts its declaration. The executor must run the check before `commitment.open`; the pump's own check is a defensive twin. `content-type` absent is let through | **built** |
| 27 | A kind of usage the catalog cannot price | Every request that carries it is under-billed with a confident `exact` basis: audio tokens at the text rate (8-50x under on `gpt-audio`), 1-hour cache writes at 1.25x instead of 2x, web-search calls at $0, TTS characters and STT seconds not at all | `cost_notes` non-empty on capture records; `llmgw_units_total` / `llmgw_server_tool_calls_total` moving while cost does not | `ModelSpec.unit`, per-kind rates, `tool_rates`; accounting prices every kind at its own rate and every fallback at a real rate with a note (C15) | The rates are copied from price pages, not probed (only OpenRouter publishes prices); a row that never gets its audio rate stays on the noted fallback forever, correct-looking on every dashboard except the one that reads `cost_notes` | **built** |
| 28 | The headers wait shares the connect budget | A healthy provider that takes longer than a TCP handshake to send its status line (OpenAI on a long prompt: headers arrive with the first token) is a 504 `HeadersTimeout`; seen once in 122 live calls on 17 Sep with the 2 s default | `llmgw_requests_total{code="headers_timeout"}` on a provider whose error rate is otherwise zero | `Budgets.headers` (default 10 s, `LLMGW_BUDGET_HEADERS`) separate from `connect` (TCP+TLS, 2 s); finding 47 | `HeadersTimeout` is `retry_same=False` by design (the request was accepted), so a single-target route still 504s when the budget is genuinely too short; profiles let a workload size it (`tts` uses 2 s) | **built** |
| 20 | **The gateway itself is down** | Everything | External health check | **Independent bypass**: the library facade can build a direct provider client from the same catalog entry with no gateway machinery | The bypass facade is not built. Fleet-level redundancy, a load balancer and multiple zones, is a deployment concern outside this repo; the load campaign runs four worker processes behind one port each | not built |
| 21 | Multipart or raw bodies buffered per request | Memory per in-flight upload | RSS vs streams_open; 413 counts | Bodies buffered only up to the surface cap (`limits_for`), which is the stated per-request bound; retries resend from the buffer, never re-read the client | The bound is the cap, not the body: 100 concurrent 25 MiB uploads are 2.5 GB. Size `LLMGW_MAX_REQUEST_BYTES__<surface>` with `max_streams` in mind | **built** |
| 22 | Request defaults drift from what the client thinks it sent | Billing and behaviour differ per target silently | `X-Gw-Body-Modified`, capture `defaulted_keys` | Defaults fill only absent keys, never overwrite, never touch non-JSON; the capture record names each key | A client that compares its request to the provider's echo sees the gateway's keys; the header is the notice | **built** |

## The bypass (row 20)

Worth stating separately because it is the answer to "what if your gateway is
the point of failure", and it costs almost nothing to build.

`Gateway` and a direct client both resolve through the same `Catalog`. If
`llmgw` is unavailable, a caller constructs the direct client from the same
`Target` and loses breakers, budgets, and accounting — but keeps serving.
Degraded and alive beats consistent and down.

It is only real if it is exercised, so the bypass is to be tested by killing the gateway
mid-scenario rather than by asserting the code path exists.

## Two limits of the test rig itself

A register of failure modes is worth little if the tests that prove the
mitigations cannot actually produce the failure. Two cases where they cannot:

**The connect clock is not testable through the fake upstreams.** By the time
an ASGI handler runs, uvicorn has completed the TCP handshake and parsed the
request, so `stall-before-headers` stalls a *time-to-first-byte* clock, not a
*connect* clock. Those classify differently and on purpose: a connect-phase
breach means nothing was accepted upstream (`ConnectTimeout.retry_same=True`,
the safest retry in the system), while a first-byte breach means the model may
already be generating (`FirstEventTimeout.retry_same=False`, because a retry
pays twice). Testing the connect budget needs a listener that binds and never
accepts — a raw `asyncio.start_server`, not a fifteenth fake mode. Until then
the connect clock is asserted only in the unit tier on a `ManualClock`.

**`die-mid-stream` sends a FIN, not an RST.** The generator raises, uvicorn
sees the response already started and closes the transport, so h11 never emits
the chunked terminator and the client gets `RemoteProtocolError: peer closed
connection without sending complete message body`. That is the right *class*
of failure, but a provider whose process is killed can produce `ECONNRESET` on
a different code path with a different exception type. ASGI exposes no way to
reset a socket.

**Update after a later verification pass.** The RST gap turned out to be a
null result worth keeping: a raw socket with `SO_LINGER 0` was used to send a
genuine TCP reset, and httpx surfaces it as the same `RemoteProtocolError` a
FIN produces. So `die-mid-stream` was never weaker than the real thing on this
path — but that is now measured rather than assumed, which is the difference
between a limitation and a guess.

The connect-clock gap stands. An attempt to exhaust the accept backlog failed
because macOS accepts past `listen(0)`, so there is still no way to make a
connection hang at the connect phase through this rig.

Stated here rather than in a comment nobody reads, because "we tested it" and
"we tested the thing that actually happens in production" are different
claims.

## What this register deliberately does not contain

Multi-region failover, zone loss, shared cross-instance limiter state, KMS and
key rotation, PII redaction in captures, and semantic response caching. Not
because they do not matter — because they are not built, and a register that
lists mitigations that do not exist is worse than no register.

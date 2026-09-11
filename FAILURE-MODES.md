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
| 11 | Deploy during open streams | Every in-flight stream | `draining` gauge; client error count during deploy | SIGTERM → readiness 503 → finish open streams → exit | Streams longer than the grace period are still cut. A 5-minute stream and a 30-second grace do not reconcile | built |
| 12 | Retry amplification | The provider, then everyone | `attempts_total / requests_total` | One absolute `Deadline`; retry budget; `X-Gw-No-Retry` (C5) | Only holds if every layer agrees who retries. We can offer the header; we cannot make callers use it | **partly built** |
| 13 | Breaker flapping | Availability oscillates | `breaker_transitions_total` rate | Per-`(provider,model)` breaker: sliding window, one half-open probe, epoch-fenced stale results, `NEUTRAL` for 429/cancel/breaker-open | **Counts, does not rate**: 5 failures/30s trips a 10k-rps target at 0.05% errors. Thresholds are guesses until S7 | **built** |
| 14 | Price table wrong (not merely stale) | Every cost report and routing decision | `live/probe.py` reconciles against the providers' own endpoints, free | Mandatory `priced_at`; `stale_prices()` in CI | **The date column is not a proof.** On 2026-09-10 six rates were wrong by 1.7x-20x while every one passed the 120-day freshness check — fresh and wrong simultaneously. A shorter threshold only re-asserts a wrong number more often; only a source fixes it. `probe` is not yet wired into CI | **partly built** |
| 15 | Metric label explosion | The metrics backend, then the gateway's memory | `total_series()` projection test | Closed label vocabularies; unbounded fields go to capture; histogram label limit | "Active targets" is a runtime property — a routing change can move you past the design point without a code change | **built** |
| 16 | Catalog misconfiguration | One workload, discovered on the hot path | Startup validation | `Catalog._validate()` raises at construction | — | **built** |
| 17 | Huge single event (8 MiB `data:` line) | Memory, and every co-resident stream | `FrameTooLarge` counter | Byte-bounded parser frame limit, checked against the frame under construction (not the buffer — see below) | **The advertised bound is optimistic by 2x**: `SSEEvent` holds the payload in both `raw` and `data`, so a 1 MiB bound is ~2 MiB resident at dispatch. Also, `feed()` appends a whole chunk before any check, so the real defence against a hostile *call* is httpx's read size, not this bound | **built** |
| 18 | fd exhaustion | The whole process | `upstream_connections` gauge vs `ulimit -n` | Per-provider pooling; per-credential concurrency caps | macOS default `ulimit -n` is 256; the cap has to be set from the limit, and nothing currently reads it | partly built |
| 19 | Permit/ticket leak on an error path | Capacity ratchets to zero over hours | `permits_in_use` / breaker `probes_in_flight` drift | Permit + two breaker tickets, each released on all four exits; multi-tenant chaos asserts all return to zero after every iteration; a breaker without a limiter is refused at construction (so a full pool cannot masquerade as a provider fault and open a circuit) | Invisible without the drift gauges; still process-local | **built** |
| 21 | Provider gzips the response body | Every request to that provider | None — the buffered case returns HTTP 200 with a correct `content-length` and a gzip body, and **nothing raises anywhere** | `accept-encoding: identity` on every upstream client | Any future client constructed without that header reintroduces it silently. Found only against real providers: 745 local tests passed over it because the fakes do not compress | **built** |
| 20 | **The gateway itself is down** | Everything | External health check | **Independent bypass**: the library facade can build a direct provider client from the same catalog entry with no gateway machinery | The bypass facade is not built. Fleet-level redundancy, a load balancer and multiple zones, is a deployment concern outside this repo; the load campaign runs four worker processes behind one port each | not built |

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

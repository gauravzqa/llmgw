# llmgw

A streaming LLM gateway in Python: an asyncio core library plus an OpenAI- and
Anthropic-compatible proxy. Built so that every reliability claim is a test you
can run and every performance claim is a number that was measured.

It exists to answer one question with sockets instead of slides: what does a
gateway have to do so that it is never the point of failure between a client
and a model provider. Deadlines that never reset across attempts, retries only
before the first byte reaches the client, circuit breakers scoped to
provider, model and credential, bounded backpressure, per-tenant admission,
a drain that finishes in-flight streams on SIGTERM, and a usage record for
every terminal outcome.

See [CONTRACTS.md](CONTRACTS.md) for the promises,
[FAILURE-MODES.md](FAILURE-MODES.md) for what breaks without each piece,
[CAPABILITIES.md](CAPABILITIES.md) for what the three providers offer and
which of it the gateway handles, passes through, or lacks (swept 16 Sep 2026),
[PLAN-2.md](PLAN-2.md) for the build order that closes those gaps, text and voice.

Author: Gaurav Pal.

## What is built

- Error taxonomy, clocks and deadlines, the catalog, the metric contract
- Hostile fake upstreams (15 modes), the SSE parser, surface adapters
- Upstream client, bounded pump, byte-for-byte passthrough, the server
- Policy snapshots, retry budget, the attempt loop, pre-commit fallback
- Circuit breaker, admission control, provider-key caps, tenant isolation
- Usage accounting, cost, bounded capture, `/metrics`
- Drain on SIGTERM, `/healthz`, `/workloads/<w>/probe`, cancellation
- Load harness, scenarios S1 to S8, measured results

Verified against real providers: streaming chat, streaming tool calls with a
second-turn round trip, reasoning passthrough, vision, JSON mode, mid-stream
cancellation, and error classification, on Anthropic, OpenAI and DeepSeek.
See [bench/results/live_smoke.md](bench/results/live_smoke.md).

## Run it

```bash
make venv      # uv venv + editable install
make test      # tier 1: 1013 tests, no sockets, no sleeps.  ~1.9s
make contract  # tier 2: 165 tests, real sockets + real uvicorn. ~57s
make chaos     # tier 3:  22 tests, randomized faults + invariants. ~60s
make live      # tier 4:  10 tests, REAL providers, real money (~$0.0002)
make trace     # walk one real stream through every layer
make run       # the gateway, against the local fakes
make serve     # the gateway, against real providers (needs $LLMGW_ENV_FILE)
make scale     # tier 5: S1-S8 load scenarios, four workers each. ~1 hour
make lint
make lock      # re-resolve uv.lock after editing pyproject.toml
make image     # build the container image locally (needs Docker)
make deploy    # fly deploy with the git SHA baked in; first-time setup in DEPLOY.md
```

The venv, the image and the contract tests all install from `uv.lock`, so
they see one dependency set. That is deliberate: the drain leans on uvicorn
0.52 internals, and an image that resolved a different uvicorn would break
the drain only in production. Deployment is Fly.io, private network only;
[DEPLOY.md](DEPLOY.md) has the three commands and the arithmetic behind
`kill_timeout`.

One scenario at a time, with the knobs the campaign used:

```bash
.venv/bin/python -m bench.load --scenario S3 --gw-workers 4 --fake-workers 4 --repeats 1
.venv/bin/python -m bench.monitor --gateway-port auto --out bench/results/monitor.jsonl   # 1 Hz sampler, separate terminal
```

Reports land in `bench/results/load-<S>-gw<N>-run.md`. The monitor writes a
JSONL trace next to them; traces are ignored by git because they run to 40 MB.

## Results

Night of 10 Sep 2026: four gateway processes, four fake-provider processes,
eight load generators, one 16-core laptop with a load average of 3 to 5 from
other work, so tails are provisional and medians and slopes are the numbers
to trust. Every test runs two arms, direct to the fake and through the
gateway, and the gateway's cost is the difference.

Six of eight scenarios passed that night. Unsaturated, the gateway adds
about one millisecond at p50 and 0.4 ms at p99 with zero errors in 144,470
requests (S1). An open stream costs about 60 KiB, two sockets and five
asyncio tasks (S3). Memory stays flat under slow clients with zero bytes
buffered (S5). A tenant flooding at five times its cap is rate-limited while
its neighbour's p99 moves 1.4 ms (S6). The breaker opens on a killed provider
and closes when it returns (S7).

Two failed, both at saturation, and a third had passed for the wrong
reason. Four processes offered about 100,000 streamed events a second pinned
at 100 percent CPU, latency went from 29 ms to 2.6 s, and only then did they
shed with 504s (S2). The same wall arrived at about 4,800 open streams per
process; the laptop itself failed the direct arm at 20,000 (S4). And the
drain (S8) reported zero cuts while all four workers were still alive a
minute after SIGTERM: uvicorn's own shutdown wait was unbounded, so the
process outlived the drain that had given up on it, and every cut stream
logged a full traceback into a pipe nobody was reading, which is enough on
its own to hang a worker in its logger. Findings 40 and 41 in
[docs/15-findings-log.md](docs/15-findings-log.md).

Re-run on 15 Sep 2026 with the fixes, same laptop, load average 2.7 to 7.5
from other work. The drain now ends the process: with a grace longer than
the streams, all four workers exited at 100 s, the instant the last 100 s
stream finished, with zero in-flight cuts (S8, arm A); with a 30 s grace
against the same streams they exited at 33.4 s, grace plus uvicorn's 3 s
bound, and the 684 cuts are reported as cuts rather than hidden (arm B).
The process now sheds before it degrades: with the per-process cap at 150
streams, S2 returned 27,707 immediate 503s, zero 504s, CPU peaked at 68
percent, and the streams it did admit were indistinguishable from the
direct arm at first-event p90 30.9 ms against 31.6 ms. At a cap of 300 the
cap held but the CPU did not, so 150 is the default and the number to
re-derive per machine. S4 re-run with the cap answers its own question
plainly: what breaks first at 10,000 offered streams is the cap, on purpose,
and nothing else. Each process admitted exactly 150 streams and shed the
rest with 34,907 immediate 503s and zero 504s; the admitted streams matched
the direct arm to within 2 ms at p99, and a process peaked at 57 MB and 18
percent CPU instead of 396 MB and 100 percent. What that run cannot show is
the gateway between 600 and 20,000 open streams, because the cap never lets
it get there; the 10 Sep baseline remains the record of that. The
single-process wedge from the first night is not reproduced; finding 41 is
the leading hypothesis.

| Report | What it holds |
|---|---|
| [load-S1-gw4-run.md](bench/results/load-S1-gw4-run.md) | 400 rps short calls: the per-request overhead floor (16 Sep, after Phase A: +0.37 ms p50) |
| [load-S1-gw4-run-20260910-baseline.md](bench/results/load-S1-gw4-run-20260910-baseline.md) | the same on 10 Sep (+1.04 ms p50) |
| [load-S2-cap150-gw4-run.md](bench/results/load-S2-cap150-gw4-run.md) | 2,500 typical streams with the cap at 150: shed before degrade (15 Sep) |
| [load-S2-cap300-gw4-run.md](bench/results/load-S2-cap300-gw4-run.md) | the same at 300: the cap holds, the CPU does not (15 Sep) |
| [load-S2-gw4-run-20260910-baseline.md](bench/results/load-S2-gw4-run-20260910-baseline.md) | 2,500 typical streams, no cap: the event-throughput ceiling (10 Sep) |
| [load-S3-gw4-run.md](bench/results/load-S3-gw4-run.md) | 1,000 slow streams: memory, sockets and tasks per stream |
| [load-S4-cap150-gw4-run.md](bench/results/load-S4-cap150-gw4-run.md) | push to 10,000 streams with the cap: the cap breaks first, admitted streams unaffected (15 Sep) |
| [load-S4-gw4-run-20260910-baseline.md](bench/results/load-S4-gw4-run-20260910-baseline.md) | push to 10,000 streams: what breaks first (10 Sep, before the cap) |
| [load-S5-gw4-run.md](bench/results/load-S5-gw4-run.md) | slow clients: backpressure |
| [load-S6-gw4-run.md](bench/results/load-S6-gw4-run.md) | one hot tenant: isolation |
| [load-S7-gw4-run.md](bench/results/load-S7-gw4-run.md) | provider killed mid-stream: breaker open and close |
| [load-S8A-gw4-run.md](bench/results/load-S8A-gw4-run.md) | deploy under load, grace longer than the streams: zero cuts, exit at last stream end (16 Sep, after Phase A) |
| [load-S8A-gw4-run-20260915.md](bench/results/load-S8A-gw4-run-20260915.md) | the same on 15 Sep |
| [load-S8B-gw4-run.md](bench/results/load-S8B-gw4-run.md) | deploy under load, grace shorter than the streams: exit at grace + 3 s, cuts counted (15 Sep) |
| [load-S8-gw4-run-20260910-baseline.md](bench/results/load-S8-gw4-run-20260910-baseline.md) | the 10 Sep drain that never exited |
| [preliminary-S1S2S3.md](bench/results/preliminary-S1S2S3.md) | the single-process night, with the wedge |
| [overhead.md](bench/results/overhead.md) | added latency at concurrency 1 against an in-process fake |
| [live_overhead.md](bench/results/live_overhead.md) | 35 paired calls against a real provider |

## The four foundations

Four files, and the order they were written in is the argument.

**`errors.py`: 27 error classes, no dependencies.** Every reliability
decision downstream is a switch statement over these, so the taxonomy comes
before the mechanism. Retryability is two booleans, not one: a 400 may fall
back to another target but may never be re-sent to the same one. Health is a
third axis: client disconnects and the breaker's own rejections are
`NEUTRAL`, or the breaker learns from events that say nothing about the
provider. Authentication failures scope to the *credential*, not the target,
so one tenant's expired BYOK key cannot open a breaker for everyone else.

**`clocks.py`: one absolute deadline, four budgets inside it.**
`Deadline.slice(budget)` returns `min(remaining, budget)`, which is the whole
"total never resets across attempts" guarantee expressed as arithmetic rather
than as a rule someone has to remember. Heartbeats reset a *liveness* clock
and never a *progress* clock.

**`catalog.py`: one table for provider, price, and capability.** Replaces the
one table instead of the usual three that drift. Every price carries the
date it was verified, and a test fails when the table goes stale, because a
price you cannot date is a price you cannot defend in a billing dispute.

**`metrics.py`: the metric contract, declared before it is implemented.**
Metric names are an interface that dashboards and alerts bind to. Labels are a
closed set: `tenant_id` and `request_id` are absent by rule and belong in
capture records, because a counter labelled by tenant is how you take out your
own metrics backend.

The commitment invariant, *once a byte reaches the client, no further target
may be opened*, is written in exactly one function, `errors.decide()`, and
enforced against all 27 classes by a parameterized test. An invariant written
in two places is one that will eventually disagree with itself.

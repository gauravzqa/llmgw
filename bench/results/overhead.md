# Gateway-overhead benchmark: llmgw (Arm G − Arm D)

> **PROVISIONAL.** The machine was under non-trivial load during this run (see load samples below). A latency benchmark on a loaded machine OVER-reports overhead. These numbers must be re-run on a quiet machine before they are quoted as the defensible figure.

## The question
How much latency does the gateway add per call, versus calling the provider directly? The delta **G − D** is *gateway machinery + one extra localhost TCP hop* — which is exactly what a real deployment pays (a request crosses a socket to reach the gateway and another to reach the provider). That is the honest overhead; the hop is NOT subtracted away to manufacture a smaller 'pure machinery' figure.

Arm D is the control. Its own absolute numbers are reported next to G; if Arm D's p99 were not comfortably below Arm G's, the client would be the bottleneck and the delta suspect. See the client-saturation check per workload.

## Configuration

- Python: 3.11.15
- Machine: arm64, 16 logical cores
- Fake mode: `ok` (deterministic), surface openai, model `fake.echo` → provider `fake-openai`
- Client: httpx.AsyncClient(http2=True); negotiated protocol observed = HTTP/1.1 (h2c is not used over plaintext localhost, so httpx runs HTTP/1.1 — the same as the contract tier)
- Gateway served with lifespan ON (pool/collectors/capture started); zero-config single-target routing to `fake.echo`
- Primary concurrency: 1 (pure per-request floor). Concurrency 4 & 16 points included and labelled as queueing.
- Run at: 2026-09-10 05:55:25Z

### Machine load (honesty requirement 1)

- Before: load1=4.56 load5=5.07 load15=5.32
- After:  load1=5.10 load5=5.15 load15=5.34
- Competing processes seen (pytest/uvicorn/chaos): 1
    - `17596   0.0 .venv/bin/python .venv/bin/uvicorn llmgw.server.app:app --p`

## Workload: non-streaming (1 event, S1-style) — concurrency 1

iterations=2000 (timed), warmup=200 discarded, events=1, response bytes: D=458 G=1229

Verification: Arm G X-Gw-Attempts=`1`, X-Gw-Served-By=`fake-openai/fake.echo`; Arm D carries X-Gw-*: False; fake request counter advanced by 4000 (expected ≥ 4000).

| arm | metric | n | mean | p50 | p90 | p99 | p99.9 | max |
|-----|--------|---|------|-----|-----|-----|-------|-----|
| D (direct) | first-event | 2000 | 0.798 | 0.779 | 0.858 | 1.036 | 1.864 | 2.721 |
| D (direct) | total | 2000 | 0.798 | 0.779 | 0.858 | 1.036 | 1.864 | 2.721 |
| G (gateway) | first-event | 2000 | 1.796 | 1.759 | 1.937 | 2.332 | 6.444 | 6.927 |
| G (gateway) | total | 2000 | 1.796 | 1.759 | 1.937 | 2.332 | 6.444 | 6.927 |

**Added latency (paired delta G − D), all values in ms:**

| metric | p50 D | p50 G | **Δ p50 (G−D)** | p99 D | p99 G | **Δ p99 (G−D)** |
|--------|-------|-------|-----------------|-------|-------|-----------------|
| first-event | 0.779 | 1.759 | **0.979** | 1.036 | 2.332 | **1.296** |
| total | 0.779 | 1.759 | **0.979** | 1.036 | 2.332 | **1.296** |

Per-iteration paired delta (G_i − D_i, same payload, interleaved):

| metric | median | p99 | mean |
|--------|--------|-----|------|
| first-event | 0.968 | 1.449 | 0.998 |
| total | 0.968 | 1.449 | 0.998 |

Client-saturation check: Arm D total p99 (1.036 ms) is below Arm G's (2.332 ms) — the client is not the binding constraint. OK.

## Workload: streaming (small stream) — concurrency 1

iterations=2000 (timed), warmup=200 discarded, events=5, response bytes: D=1229 G=1229

Verification: Arm G X-Gw-Attempts=`1`, X-Gw-Served-By=`fake-openai/fake.echo`; Arm D carries X-Gw-*: False; fake request counter advanced by 4000 (expected ≥ 4000).

| arm | metric | n | mean | p50 | p90 | p99 | p99.9 | max |
|-----|--------|---|------|-----|-----|-----|-------|-----|
| D (direct) | first-event | 2000 | 0.633 | 0.624 | 0.686 | 0.871 | 1.440 | 4.950 |
| D (direct) | total | 2000 | 0.894 | 0.876 | 0.986 | 1.232 | 2.115 | 5.899 |
| G (gateway) | first-event | 2000 | 1.654 | 1.636 | 1.812 | 2.155 | 5.086 | 6.150 |
| G (gateway) | total | 2000 | 2.386 | 2.348 | 2.567 | 3.171 | 6.868 | 8.715 |

**Added latency (paired delta G − D), all values in ms:**

| metric | p50 D | p50 G | **Δ p50 (G−D)** | p99 D | p99 G | **Δ p99 (G−D)** |
|--------|-------|-------|-----------------|-------|-------|-----------------|
| first-event | 0.624 | 1.636 | **1.012** | 0.871 | 2.155 | **1.285** |
| total | 0.876 | 2.348 | **1.473** | 1.232 | 3.171 | **1.939** |

Per-iteration paired delta (G_i − D_i, same payload, interleaved):

| metric | median | p99 | mean |
|--------|--------|-----|------|
| first-event | 1.013 | 1.411 | 1.021 |
| total | 1.467 | 2.146 | 1.492 |

Client-saturation check: Arm D total p99 (1.232 ms) is below Arm G's (3.171 ms) — the client is not the binding constraint. OK.

## Workload: streaming (small stream) — concurrency 4  *(includes queueing — NOT pure per-request overhead)*

iterations=1000 (timed), warmup=100 discarded, events=5, response bytes: D=0 G=0

| arm | metric | n | mean | p50 | p90 | p99 | p99.9 | max |
|-----|--------|---|------|-----|-----|-----|-------|-----|
| D (direct) | first-event | 1000 | 2.657 | 2.465 | 3.633 | 4.304 | 5.499 | 5.988 |
| D (direct) | total | 1000 | 3.327 | 3.203 | 4.295 | 5.138 | 6.479 | 6.532 |
| G (gateway) | first-event | 1000 | 5.917 | 5.796 | 7.204 | 8.679 | 13.777 | 13.992 |
| G (gateway) | total | 1000 | 8.892 | 8.822 | 10.157 | 12.063 | 15.575 | 17.869 |

**Added latency (paired delta G − D), all values in ms:**

| metric | p50 D | p50 G | **Δ p50 (G−D)** | p99 D | p99 G | **Δ p99 (G−D)** |
|--------|-------|-------|-----------------|-------|-------|-----------------|
| first-event | 2.465 | 5.796 | **3.331** | 4.304 | 8.679 | **4.375** |
| total | 3.203 | 8.822 | **5.619** | 5.138 | 12.063 | **6.924** |

Client-saturation check: Arm D total p99 (5.138 ms) is below Arm G's (12.063 ms) — the client is not the binding constraint. OK.

## Workload: streaming (small stream) — concurrency 16  *(includes queueing — NOT pure per-request overhead)*

iterations=1000 (timed), warmup=100 discarded, events=5, response bytes: D=0 G=0

| arm | metric | n | mean | p50 | p90 | p99 | p99.9 | max |
|-----|--------|---|------|-----|-----|-----|-------|-----|
| D (direct) | first-event | 1000 | 24.232 | 20.717 | 42.846 | 84.895 | 93.610 | 95.954 |
| D (direct) | total | 1000 | 25.638 | 22.103 | 44.184 | 86.490 | 94.880 | 97.478 |
| G (gateway) | first-event | 1000 | 53.879 | 44.322 | 97.229 | 174.999 | 226.176 | 244.808 |
| G (gateway) | total | 1000 | 56.436 | 47.035 | 100.215 | 176.782 | 229.202 | 250.139 |

**Added latency (paired delta G − D), all values in ms:**

| metric | p50 D | p50 G | **Δ p50 (G−D)** | p99 D | p99 G | **Δ p99 (G−D)** |
|--------|-------|-------|-----------------|-------|-------|-----------------|
| first-event | 20.717 | 44.322 | **23.605** | 84.895 | 174.999 | **90.104** |
| total | 22.103 | 47.035 | **24.932** | 86.490 | 176.782 | **90.292** |

Client-saturation check: Arm D total p99 (86.490 ms) is below Arm G's (176.782 ms) — the client is not the binding constraint. OK.

## Does the 1 to 4 ms p50 prediction hold?

Headline added **first-event** latency at concurrency 1 (streaming) = **1.012 ms p50** (G 1.636 − D 0.624), **1.285 ms p99**. Added **total** latency = **1.473 ms p50** (G 2.348 − D 0.876). The first-event p50 delta lands **at the low edge of** the predicted 1 to 4 ms band — and since these numbers are PROVISIONAL on a loaded machine (which OVER-reports), the quiet-machine figure is likely at or just under 1 ms, i.e. the bottom of the band rather than the middle.

In CPython sub-millisecond is not expected, and a ~1 ms per-call floor IS the answer to 'why not write this in Go': the delta is the interpreter walking the async pump, the SSE parse/re-emit, the admission/breaker/accounting machinery, and one extra localhost TCP hop — none of which a compiled runtime pays in the same amount. The number is small, defensible, and explained, which is the point.

## How to re-run

```
.venv/bin/python -m bench.overhead
```

Source: `bench/overhead.py`. Re-run on a QUIET machine (load < ~1.0, no pytest/chaos tier running) for the defensible figure.

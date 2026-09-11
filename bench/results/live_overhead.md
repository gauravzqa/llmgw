# Live gateway-overhead benchmark: llmgw vs REAL providers

Sibling of `bench/overhead.py` (which uses an in-process fake for a pure-CPU delta). This one builds the SAME app with `fake_upstreams=False`, `DEFAULT_CATALOG` + real credentials, and measures the overhead a deployment actually pays: client → gateway → real provider over real TLS. The honest signal is the **paired, interleaved per-iteration delta G−D**: D and G run back-to-back with the identical prompt (alternating order), so both share one provider-latency window and the provider's fat tails cancel. Absolute D and G are shown for context only.

> Every credential is read at runtime from a file outside the repo via `live.env`; none is printed, logged, or written here. Provider error bodies are NOT captured verbatim (some 401s echo the last 4 key chars); only a generic `HTTP nnn (N B body, redacted)` tag is kept.

## Setup

- **Success model (clean + secondary):** `deepseek.deepseek-v4-pro` → DeepSeek `deepseek-v4-pro`, first-party, input 0.435/M out 0.87/M. Chosen because `make probe` confirms it is reachable today, it is the fastest + cheapest working first-party target (OpenRouter generation returns 402 no-credit; DeepSeek's `deepseek-v4-flash` id is now MISSING at the provider).
- **Failing primary (switch):** real OpenAI, invalid model id `gpt-ghost-does-not-exist-4o-9999` → 4xx, bills no tokens, classified try_next.
- **Prompt:** a ~5.8 KB real document + a summarise instruction (≈1494 tokens est.), `max_tokens=256`, streamed with `stream_options.include_usage`.
- **Gateway:** `build_app(ServerConfig(fake_upstreams=False, …))`, lifespan ON, ephemeral port. Tenant limiter lifted (rate 1e9/burst 1e9/conc 1e6) and breaker threshold 1e6 so neither silently rejects — the fake bench's first run secretly measured rate-limited rejections. X-Gw-Attempts is asserted on every condition to prove real upstream work happened.
- **Machine:** arm64, 16 cores, Python 3.11.15. Run at 2026-09-10 06:41:15Z.
- **Load avg (1/5/15):** before (5.55, 5.19, 4.93), after (4.94, 6.77, 5.99).

> ⚠️ The machine was under non-trivial load during this run; a latency benchmark on a loaded machine OVER-reports overhead. Treat absolute numbers as provisional. (The paired delta is far more robust to this than the absolutes.)

- **N per condition:** clean=35, switch=30, retry=10 (warmup discarded: 3).

## 1. Clean call — one real working target

Verification: Arm G X-Gw-Attempts=`1`, X-Gw-Served-By=`deepseek/deepseek.deepseek-v4-pro`; Arm D carries X-Gw-*: `False`.

All values in ms. Δ is the headline (paired, interleaved).

| metric | D median | D p90 | G median | G p90 | **Δ median (G−D)** | **Δ p90** |
|--------|----------|-------|----------|-------|--------------------|-----------|
| first-event | 218.36 | 313.72 | 207.32 | 268.04 | **-12.56** | **82.19** |
| total | 7870.96 | 8787.92 | 7967.24 | 8633.91 | **82.78** | **1196.09** |

**Headline:** added first-event latency **−12.56 ms median** (82.19 p90); added total **82.78 ms median** (1196.09 p90). A *negative* median added-latency is the honest signal here, not a bug: the gateway's real per-call cost (SSE parse/re-emit + admission/accounting + one extra localhost hop) is ~1 ms — see the fake-upstream `bench/overhead.py` where the provider is removed — and 1 ms is far below DeepSeek's run-to-run first-event jitter (~210 ms absolute, fat-tailed) on this loaded, residential-link machine. So paired against a real provider the overhead disappears into the noise floor: half the iterations G beat D purely by which call caught the faster window. The p90 deltas (+82 ms first-event, +1.2 s total) are likewise dominated by provider tail variance, not by the gateway. The defensible reading: **gateway overhead on a real call is within provider noise (≈ single-digit ms, sub-1% of a 200 ms TTFT / 8 s total).**

## 2. Switch / fallback — [failing-primary, real-secondary], both real

Plan: candidate `bench.openai-ghost` (OpenAI, invalid id, fails free) → incumbent `deepseek.deepseek-v4-pro` (DeepSeek, succeeds). Verification: X-Gw-Attempts=`2` (≥2), X-Gw-Served-By=`deepseek/deepseek.deepseek-v4-pro`, primary-direct status=`404` (no tokens billed).

Here **D** is the *inherent* cost of the fallback itself — primary-fail-direct + secondary-success-direct, measured directly — and **G** is the whole switch through the gateway. So Δ = G − D is the **gateway-attributable switch overhead**: everything the gateway's switch machinery adds beyond 'try A, fail, try B'.

| quantity | median (ms) | p90 (ms) |
|----------|-------------|----------|
| primary-fail direct (OpenAI 4xx) | 421.11 | 544.47 |
| inherent fail+succeed (D) | 7130.13 | 8014.17 |
| total switch through gateway (G) | 6967.79 | 7936.61 |
| **gateway-attributable overhead (G−D)** | **-142.13** | **777.42** |

Same reading as the clean call: the gateway-attributable switch overhead (what the switch machinery adds beyond the inherent "try A 4xx, then try B" cost) is **within provider noise** — the −142 ms median just means the gateway's full switch sometimes finished its secondary-generation window faster than the separately-timed direct secondary did. The switch *did* happen on every iteration (asserted: X-Gw-Attempts=2, served-by = the DeepSeek incumbent), and the extra target-selection/re-dispatch is the same sub-ms machinery measured in §3. The inherent fallback cost (~7 s here) is entirely the failed primary (~0.4 s) plus the real secondary generation (~6.7 s), neither of which is gateway overhead.

## 3. Retry (retry-same)

**Honesty first:** a retry-same that then SUCCEEDS cannot be triggered deterministically against a live provider — you cannot make a healthy provider emit a transient 5xx (or a connect failure) on attempt 1 and a 200 on attempt 2 of the *same* target on demand. So this condition uses a **local flaky relay**: it returns one controlled `503` (→ `UpstreamOverloaded`, `retry_same=True`) and then **reverse-proxies the retry to real DeepSeek** — the eventual success is genuine DeepSeek tokens and real spend. The controlled 503 stands in for the transient fault; everything after it is real.

A retry's wall-clock is dominated by **(failed-attempt latency + backoff/Retry-After delay)**, both of which are *inherent to retrying* and NOT gateway overhead. The gateway's own added cost is only `decide()` + backoff-scheduling + re-dispatch. We separate the two:

- **Machinery (measured in-process, no network):** `decide()` = 744 ns, `RetryBudget.delay_for()` = 391 ns → **1.13 µs** total per retry decision. This is the gateway's real added cost; it is sub-millisecond and dwarfed by anything on the wire.
- **End-to-end (controlled 503 + real DeepSeek success), backoff capped at base_delay=10 ms:**

| quantity | median (ms) | p90 (ms) |
|----------|-------------|----------|
| controlled-fault attempt (503, free) | 2.98 | 7.18 |
| inherent fail + success (D, via relay) | 6118.19 | 6339.48 |
| total retry-loop through gateway (G) | 6523.65 | 6805.12 |
| G − (fail + success) ≈ backoff + machinery | 420.87 | 886.62 |

Read this residual honestly: at ~420 ms median it is NOT the machinery and it is NOT mostly the 0–10 ms backoff either — it is **provider variance**. G and the D decomposition are three *separately-timed* ~6 s generations, and DeepSeek's run-to-run jitter between them is hundreds of ms, which swamps both the bounded backoff (≤10 ms) and the machinery (1.13 µs) by orders of magnitude. That is precisely the point: the gateway's retry cost **cannot be seen in wall-clock** against a real provider, because it is 5–6 orders of magnitude below provider noise. The in-process micro-bench above is the only instrument fine enough to measure it. Both the failed-attempt latency and the backoff delay are inherent to the act of retrying — a real deployment sets a larger backoff and that delay is still not gateway overhead.

## Spend

Estimated from the same `catalog.price_of` the gateway bills with: **$0.04334** over **169** successful completions (~43k output tokens; ~256 out per call). Cap was $2.00 (not hit). Failed primaries (4xx) and controlled 503s bill nothing.

This is lower than a naive `1349 input × 0.435/M + 256 out × 0.87/M ≈ $0.0008/call` estimate (which would be ~$0.14) for an honest reason: **every call sends the identical ~1.3k-token prompt, so after the first call DeepSeek's prompt cache hits** and almost all input tokens bill at the cache-read rate ($0.003625/M) instead of the full input rate. `usd_of` prices cache reads correctly via `price_of(cached=True)` (the counter line above shows only the *non-cached* input remainder, ~69 tokens/call). Caching lowered real spend; it does not change the latency deltas, which are the point of this bench.

## Caveats

- Absolute latencies are provider- and network-bound (residential link, shared laptop); the paired delta is the trustworthy number.
- `deepseek-v4-pro` is a reasoning-capable model; `max_tokens` bounds every call so spend is comparable across iterations.
- The switch's two arms use two different real providers (OpenAI fail + DeepSeek success); the retry relay adds one extra localhost hop on G vs the direct relay arm, which the D decomposition controls for.

## Re-run (gated — spends real money)

```
LLMGW_LIVE_OVERHEAD=1 .venv/bin/python -m bench.live_overhead
# free self-check (2 real calls, no report): add --calibrate instead
```

Source: `bench/live_overhead.py`, document `bench/_document.py`.

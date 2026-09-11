# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [6.1083984375, 6.4765625, 6.2275390625], "ts": "2026-09-10 16:09:36Z"}

config: rate=400.0 rps, workers=8, warm=60.0s measure=300.0s repeats=3

## Median run (2 of 3, by Arm G p99 TTFE)

### Arm D -- rate 400 rps, 8 workers, wall 360.2s
started=144471 ok=144471 error=0 bytes=66167718 late=4654 peak_inflight(sum over workers)=38

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    1.78 |    2.50 |    6.79 |   14.69 |    1.95 |
| total          |  120530 |    1.78 |    2.50 |    6.79 |   14.69 |    1.95 |

status: {'200': 144471}

### Arm G -- rate 400 rps, 8 workers, wall 360.2s
started=144471 ok=144471 error=0 bytes=66167718 late=3842 peak_inflight(sum over workers)=42

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    2.86 |    4.86 |    9.56 |   22.71 |    3.33 |
| total          |  120530 |    2.86 |    4.86 |    9.56 |   22.71 |    3.33 |

status: {'200': 144471}

gateway samples: {"n_samples": 336, "peak_streams_open": 6.0, "peak_tasks": 21.0, "baseline_tasks": 4.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 373.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 48656.0, "min_rss_kib": 48656.0, "peak_cpu_pct": 63.1, "peak_fd_inbound": 9, "peak_fd_upstream": 9, "marginal_rss_kib_per_stream": 0.0}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    1.78 |    2.86 |   +1.08 |
| p90 |    2.50 |    4.86 |   +2.36 |
| p99 |    6.79 |    9.56 |   +2.77 |
| p99.9 |   14.69 |   22.71 |   +8.02 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=144471 G=144471 requests
- CALIBRATION: Arm D p99 TTFE=6.79 ms < Arm G p99 TTFE=9.56 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 4654/144471 (3.2%) arrivals fired LATE (max 11 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 3842/144471 (2.7%) arrivals fired LATE (max 5 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

run-to-run spread (Arm G p99 TTFE ms): min=8.71 max=10.39 median=9.56

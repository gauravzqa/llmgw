# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.2451171875, 5.73193359375, 5.69384765625], "ts": "2026-09-10 09:50:36Z"}

config: rate=40.0 rps, workers=2, warm=1.0s measure=4.0s repeats=1

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 40 rps, 2 workers, wall 5.2s
started=198 ok=198 error=0 bytes=90684 late=4 peak_inflight(sum over workers)=6

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     159 |    5.47 |    8.62 |   15.32 |   17.46 |    5.71 |
| total          |     159 |    5.47 |    8.62 |   15.32 |   17.46 |    5.71 |

status: {'200': 198}

### Arm G -- rate 40 rps, 2 workers, wall 5.2s
started=198 ok=198 error=0 bytes=90684 late=4 peak_inflight(sum over workers)=6

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     159 |    5.43 |   11.81 |   17.38 |   19.59 |    6.53 |
| total          |     159 |    5.43 |   11.81 |   17.38 |   19.59 |    6.53 |

status: {'200': 198}

gateway samples: {"n_samples": 5, "peak_streams_open": 1.0, "peak_tasks": 5.0, "baseline_tasks": 4.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 46912.0, "min_rss_kib": 38448.0, "peak_cpu_pct": 16.9, "peak_fd_inbound": 1, "peak_fd_upstream": 3, "marginal_rss_kib_per_stream": -16.0}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    5.47 |    5.43 |   -0.04 |
| p90 |    8.62 |   11.81 |   +3.19 |
| p99 |   15.32 |   17.38 |   +2.06 |
| p99.9 |   17.46 |   19.59 |   +2.13 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=198 G=198 requests
- CALIBRATION: Arm D p99 TTFE=15.32 ms < Arm G p99 TTFE=17.38 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 4/198 (2.0%) arrivals fired LATE (max 2 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 4/198 (2.0%) arrivals fired LATE (max 1 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

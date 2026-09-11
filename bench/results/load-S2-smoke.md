# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.18505859375, 5.35693359375, 5.5546875], "ts": "2026-09-10 09:51:24Z"}

config: rate=20.0 rps, workers=2, warm=1.0s measure=4.0s repeats=1

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 20 rps, 2 workers, wall 6.1s
started=99 ok=99 error=0 bytes=789030 late=0 peak_inflight(sum over workers)=34

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |      82 |   26.75 |   28.13 |   31.20 |   31.58 |   27.36 |
| total          |      82 | 1059.25 | 1109.17 | 1120.73 | 1121.89 | 1029.25 |
| inter-event    |    3362 |   26.37 |   27.81 |   28.15 |   28.18 |   24.43 |

status: {'200': 99}

### Arm G -- rate 20 rps, 2 workers, wall 6.1s
started=99 ok=99 error=0 bytes=789030 late=1 peak_inflight(sum over workers)=34

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |      82 |   29.81 |   31.41 |   36.22 |   39.44 |   29.67 |
| total          |      82 | 1059.25 | 1109.17 | 1120.73 | 1121.89 | 1028.03 |
| inter-event    |    3362 |   26.21 |   27.79 |   28.16 |   34.13 |   24.32 |

status: {'200': 99}

gateway samples: {"n_samples": 6, "peak_streams_open": 32.0, "peak_tasks": 162.0, "baseline_tasks": 54.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 49104.0, "min_rss_kib": 38624.0, "peak_cpu_pct": 20.7, "peak_fd_inbound": 34, "peak_fd_upstream": 33, "marginal_rss_kib_per_stream": 8.96898079763663}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   26.75 |   29.81 |   +3.06 |
| p90 |   28.13 |   31.41 |   +3.28 |
| p99 |   31.20 |   36.22 |   +5.03 |
| p99.9 |   31.58 |   39.44 |   +7.86 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=99 G=99 requests
- CALIBRATION: Arm D p99 TTFE=31.20 ms < Arm G p99 TTFE=36.22 ms -> CALIBRATED (client faster than gateway path, number is honest)
- no generator-saturation flags (schedule kept, errors bounded)

# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [3.9404296875, 4.5517578125, 4.86865234375], "ts": "2026-09-10 17:28:23Z"}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=3

## Median run (2 of 3, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 425.9s
started=36107 ok=5792 error=30299 bytes=1117079872 late=718 peak_inflight(sum over workers)=15548

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   13203 | 1263.76 | 1796.91 | 2186.00 | 2562.95 | 1221.52 |
| total          |       0 |    n/a |    n/a |    n/a |    n/a |    n/a |
| inter-event    | 9274738 |  282.35 |  834.69 |  890.35 |  989.01 |  402.86 |

status: {'200': 19034}

### Arm G -- rate 100 rps, 8 workers, wall 367.9s
started=36107 ok=2829 error=33278 bytes=546440268 late=384 peak_inflight(sum over workers)=5467

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    9207 | 18445.11 | 36089.54 | 58081.14 | 62575.38 | 21664.37 |
| total          |    7986 | 19287.55 | 57296.53 | 84936.48 | 88697.10 | 27560.35 |
| inter-event    | 2574517 |    0.00 |    0.00 |    0.54 | 9639.31 |   33.05 |

status: {'200': 4051, '504': 10543}

gateway samples: {"n_samples": 83, "peak_streams_open": 2106.0, "peak_tasks": 8126.0, "baseline_tasks": 8126.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 92445.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 739328.0, "min_rss_kib": 584560.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 3385, "peak_fd_upstream": 2836, "marginal_rss_kib_per_stream": 33.71797214835084}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 | 1263.76 | 18445.11 | +17181.35 |
| p90 | 1796.91 | 36089.54 | +34292.62 |
| p99 | 2186.00 | 58081.14 | +55895.15 |
| p99.9 | 2562.95 | 62575.38 | +60012.43 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=19034 G=4764 requests
- CALIBRATION: Arm D p99 TTFE=2186.00 ms < Arm G p99 TTFE=58081.14 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 30299 errors (84.0%): {'connect': 17073, 'ReadError': 13234}
- Arm G: 33278 errors (92.2%): {'connect': 18964, 'timeout': 3770, 'protocol': 1}

run-to-run spread (Arm G p99 TTFE ms): min=55920.75 max=58081.14 median=57000.94

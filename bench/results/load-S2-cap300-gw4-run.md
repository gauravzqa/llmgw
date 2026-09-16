# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [6.12158203125, 5.978515625, 5.86376953125], "ts": "2026-09-15 17:41:06Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": 300, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 386.2s
started=36107 ok=36107 error=0 bytes=6963812662 late=225 peak_inflight(sum over workers)=2924

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |   29.15 |   32.53 |   50.16 |   73.37 |   29.80 |
| total          |   30276 | 26607.25 | 27861.21 | 28151.40 | 28180.58 | 25592.36 |
| inter-event    | 30306276 |   26.20 |   27.88 |   38.81 |   49.62 |   25.54 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 386.3s
started=36107 ok=15993 error=20114 bytes=3086865083 late=148 peak_inflight(sum over workers)=1446

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |  167.88 | 1776.63 | 3041.86 | 3214.83 |  457.80 |
| total          |   30276 | 2514.81 | 27454.58 | 28120.95 | 29451.41 | 11673.06 |
| inter-event    | 13143130 |   31.67 |   42.25 |   99.07 |  181.14 |   25.64 |

status: {'200': 15993, '503': 16804, '504': 3217, '502': 93}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 299, "workers": 4, "peak_streams_open": 1200.0, "peak_tasks": 6024.0, "baseline_tasks": 26.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 5401.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 280880.0, "min_rss_kib": 155168.0, "peak_cpu_pct": 400.0, "peak_fd_inbound": 1217, "peak_fd_upstream": 1203, "peak_rss_kib_max_worker": 70480.0, "peak_cpu_pct_max_worker": 100.0, "peak_fd_inbound_max_worker": 308, "peak_fd_upstream_max_worker": 302, "requests_total_delta": 19287.0, "per_worker": {"40146": {"port": 46535, "peak_rss_kib": 70480.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 306, "peak_fd_upstream": 300, "peak_streams_open": 300.0, "requests_total_delta": 4977.0}, "40147": {"port": 46536, "peak_rss_kib": 70400.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 305, "peak_fd_upstream": 302, "peak_streams_open": 300.0, "requests_total_delta": 4833.0}, "40148": {"port": 46537, "peak_rss_kib": 69952.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 308, "peak_fd_upstream": 301, "peak_streams_open": 300.0, "requests_total_delta": 4843.0}, "40149": {"port": 46538, "peak_rss_kib": 70048.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 305, "peak_fd_upstream": 300, "peak_streams_open": 300.0, "requests_total_delta": 4634.0}}, "marginal_rss_kib_per_stream": 20.96730062772264}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   29.15 |  167.88 | +138.73 |
| p90 |   32.53 | 1776.63 | +1744.10 |
| p99 |   50.16 | 3041.86 | +2991.69 |
| p99.9 |   73.37 | 3214.83 | +3141.46 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=16075 requests
- CALIBRATION: Arm D p99 TTFE=50.16 ms < Arm G p99 TTFE=3041.86 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm G: 20114 errors (55.7%): {}

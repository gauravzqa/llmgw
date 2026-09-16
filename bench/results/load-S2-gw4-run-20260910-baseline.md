# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [3.60107421875, 4.1884765625, 3.939453125], "ts": "2026-09-10 20:02:57Z", "gw_workers": 4, "fake_workers": 4}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 386.0s
started=36107 ok=36107 error=0 bytes=6963812662 late=156 peak_inflight(sum over workers)=2890

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |   28.81 |   31.18 |   35.18 |   60.74 |   28.76 |
| total          |   30276 | 26607.25 | 27861.21 | 28151.40 | 28180.58 | 25277.68 |
| inter-event    | 30306276 |   26.02 |   27.75 |   28.15 |   38.56 |   25.22 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 388.0s
started=36107 ok=17933 error=18174 bytes=3460084750 late=161 peak_inflight(sum over workers)=3190

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 | 2595.31 | 9249.13 | 11155.91 | 15774.95 | 3743.68 |
| total          |   30276 | 10713.31 | 32584.26 | 35191.13 | 35463.03 | 17654.80 |
| inter-event    | 14526512 |    0.00 |   41.88 |  533.33 | 1668.02 |   26.25 |

status: {'200': 17933, '504': 17934, '502': 240}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 191, "workers": 4, "peak_streams_open": 2623.0, "peak_tasks": 12122.0, "baseline_tasks": 241.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 39609.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 554640.0, "min_rss_kib": 155536.0, "peak_cpu_pct": 399.5, "peak_fd_inbound": 2868, "peak_fd_upstream": 2505, "peak_rss_kib_max_worker": 142032.0, "peak_cpu_pct_max_worker": 100.0, "peak_fd_inbound_max_worker": 743, "peak_fd_upstream_max_worker": 633, "requests_total_delta": 35880.0, "per_worker": {"40645": {"port": 54876, "peak_rss_kib": 142032.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 743, "peak_fd_upstream": 633, "peak_streams_open": 662.0, "requests_total_delta": 9019.0}, "40646": {"port": 54877, "peak_rss_kib": 137792.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 713, "peak_fd_upstream": 631, "peak_streams_open": 674.0, "requests_total_delta": 8984.0}, "40647": {"port": 54878, "peak_rss_kib": 138400.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 735, "peak_fd_upstream": 612, "peak_streams_open": 665.0, "requests_total_delta": 8957.0}, "40648": {"port": 54879, "peak_rss_kib": 136416.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 741, "peak_fd_upstream": 629, "peak_streams_open": 669.0, "requests_total_delta": 8920.0}}, "marginal_rss_kib_per_stream": 7.246548534571735}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   28.81 | 2595.31 | +2566.49 |
| p90 |   31.18 | 9249.13 | +9217.95 |
| p99 |   35.18 | 11155.91 | +11120.73 |
| p99.9 |   60.74 | 15774.95 | +15714.21 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=18822 requests
- CALIBRATION: Arm D p99 TTFE=35.18 ms < Arm G p99 TTFE=11155.91 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm G: 18174 errors (50.3%): {}

# Scenario S5: Slow clients
_Backpressure: is gateway memory bounded when clients read slower than the upstream writes._

produces: peak pump_buffered_bytes (must stay under the pump ceiling); RSS bounded; no unbounded growth with 500 slow readers.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.79736328125, 6.61962890625, 6.720703125], "ts": "2026-09-17 22:30:35Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=25.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 25 rps, 8 workers, wall 397.9s
started=8821 ok=8821 error=0 bytes=681916226 late=8 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   53.14 |   55.69 |   60.09 |   73.27 |   52.14 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.79 |
| inter-event    | 2930508 |   94.39 |   98.90 |   99.95 |  121.09 |   93.99 |

status: {'200': 8821}

### Arm G -- rate 25 rps, 8 workers, wall 398.0s
started=8821 ok=8821 error=0 bytes=681916226 late=5 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   53.65 |   58.98 |   70.25 |   83.93 |   54.99 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.86 |
| inter-event    | 2930508 |   94.38 |   98.92 |   99.96 |  120.70 |   93.98 |

status: {'200': 8821}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 358, "workers": 4, "peak_streams_open": 545.0, "peak_tasks": 2739.0, "baseline_tasks": 16.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 645.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 232800.0, "min_rss_kib": 94640.0, "peak_cpu_pct": 132.3, "peak_fd_inbound": 670, "peak_fd_upstream": 555, "peak_rss_kib_max_worker": 58304.0, "peak_cpu_pct_max_worker": 33.7, "peak_fd_inbound_max_worker": 170, "peak_fd_upstream_max_worker": 139, "requests_total_delta": 8807.0, "per_worker": {"23122": {"port": 62514, "peak_rss_kib": 58304.0, "peak_cpu_pct": 33.2, "peak_fd_inbound": 169, "peak_fd_upstream": 139, "peak_streams_open": 136.0, "requests_total_delta": 2201.0}, "23123": {"port": 62515, "peak_rss_kib": 58208.0, "peak_cpu_pct": 33.7, "peak_fd_inbound": 170, "peak_fd_upstream": 139, "peak_streams_open": 137.0, "requests_total_delta": 2202.0}, "23124": {"port": 62516, "peak_rss_kib": 58032.0, "peak_cpu_pct": 33.3, "peak_fd_inbound": 168, "peak_fd_upstream": 139, "peak_streams_open": 138.0, "requests_total_delta": 2202.0}, "23125": {"port": 62517, "peak_rss_kib": 58256.0, "peak_cpu_pct": 32.5, "peak_fd_inbound": 168, "peak_fd_upstream": 138, "peak_streams_open": 137.0, "requests_total_delta": 2202.0}}, "marginal_rss_kib_per_stream": 28.821878925548365}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   53.14 |   53.65 |   +0.51 |
| p90 |   55.69 |   58.98 |   +3.29 |
| p99 |   60.09 |   70.25 |  +10.16 |
| p99.9 |   73.27 |   83.93 |  +10.65 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=8821 G=8821 requests
- CALIBRATION: Arm D p99 TTFE=60.09 ms < Arm G p99 TTFE=70.25 ms -> CALIBRATED (client faster than gateway path, number is honest)
- no generator-saturation flags (schedule kept, errors bounded)

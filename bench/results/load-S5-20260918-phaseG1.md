# Scenario S5: Slow clients
_Backpressure: is gateway memory bounded when clients read slower than the upstream writes._

produces: peak pump_buffered_bytes (must stay under the pump ceiling); RSS bounded; no unbounded growth with 500 slow readers.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [17.47314453125, 14.58447265625, 11.205078125], "ts": "2026-09-18 12:07:16Z", "gw_workers": 2, "fake_workers": 2, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=25.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=2 fake_workers=2

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 25 rps, 8 workers, wall 397.9s
started=8821 ok=8821 error=0 bytes=681916226 late=4 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   53.12 |   55.66 |   57.07 |   68.29 |   51.98 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.82 |
| inter-event    | 2930508 |   94.39 |   98.89 |   99.94 |  120.83 |   93.99 |

status: {'200': 8821}

### Arm G -- rate 25 rps, 8 workers, wall 397.9s
started=8821 ok=8821 error=0 bytes=681916226 late=9 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   57.46 |   69.07 |   88.58 |  116.63 |   59.60 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.83 |
| inter-event    | 2930508 |   94.38 |   98.93 |   99.98 |  121.57 |   93.97 |

status: {'200': 8821}

gateway samples (2 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 358, "workers": 2, "peak_streams_open": 548.0, "peak_tasks": 2742.0, "baseline_tasks": 8.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 664.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 132528.0, "min_rss_kib": 50816.0, "peak_cpu_pct": 129.7, "peak_fd_inbound": 667, "peak_fd_upstream": 550, "peak_rss_kib_max_worker": 66320.0, "peak_cpu_pct_max_worker": 70.2, "peak_fd_inbound_max_worker": 334, "peak_fd_upstream_max_worker": 275, "requests_total_delta": 8808.0, "per_worker": {"46547": {"port": 32771, "peak_rss_kib": 66320.0, "peak_cpu_pct": 70.2, "peak_fd_inbound": 334, "peak_fd_upstream": 275, "peak_streams_open": 275.0, "requests_total_delta": 4404.0}, "46549": {"port": 32772, "peak_rss_kib": 66208.0, "peak_cpu_pct": 67.0, "peak_fd_inbound": 334, "peak_fd_upstream": 275, "peak_streams_open": 273.0, "requests_total_delta": 4404.0}}, "marginal_rss_kib_per_stream": 17.916357255274797}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   53.12 |   57.46 |   +4.34 |
| p90 |   55.66 |   69.07 |  +13.42 |
| p99 |   57.07 |   88.58 |  +31.52 |
| p99.9 |   68.29 |  116.63 |  +48.34 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=8821 G=8821 requests
- CALIBRATION: Arm D p99 TTFE=57.07 ms < Arm G p99 TTFE=88.58 ms -> CALIBRATED (client faster than gateway path, number is honest)
- no generator-saturation flags (schedule kept, errors bounded)

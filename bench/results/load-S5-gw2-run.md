# Scenario S5: Slow clients
_Backpressure: is gateway memory bounded when clients read slower than the upstream writes._

produces: peak pump_buffered_bytes (must stay under the pump ceiling); RSS bounded; no unbounded growth with 500 slow readers.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [6.6416015625, 5.42626953125, 5.45947265625], "ts": "2026-09-17 20:01:47Z", "gw_workers": 2, "fake_workers": 2, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=25.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=2 fake_workers=2

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 25 rps, 8 workers, wall 397.8s
started=8821 ok=8821 error=0 bytes=681916226 late=8 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   53.12 |   55.65 |   56.87 |   73.59 |   52.46 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.88 |
| inter-event    | 2930508 |   94.39 |   98.88 |   99.92 |  121.24 |   93.99 |

status: {'200': 8821}

### Arm G -- rate 25 rps, 8 workers, wall 397.9s
started=8821 ok=8821 error=0 bytes=681916226 late=9 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   55.84 |   66.42 |   78.58 |   96.52 |   58.02 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.75 |
| inter-event    | 2930508 |   94.39 |   98.88 |   99.92 |  120.33 |   93.97 |

status: {'200': 8821}

gateway samples (2 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 359, "workers": 2, "peak_streams_open": 548.0, "peak_tasks": 2748.0, "baseline_tasks": 8.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 1256.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 133744.0, "min_rss_kib": 77936.0, "peak_cpu_pct": 111.19999999999999, "peak_fd_inbound": 672, "peak_fd_upstream": 552, "peak_rss_kib_max_worker": 66928.0, "peak_cpu_pct_max_worker": 56.5, "peak_fd_inbound_max_worker": 338, "peak_fd_upstream_max_worker": 276, "requests_total_delta": 8811.0, "per_worker": {"89322": {"port": 57718, "peak_rss_kib": 66928.0, "peak_cpu_pct": 56.5, "peak_fd_inbound": 338, "peak_fd_upstream": 276, "peak_streams_open": 275.0, "requests_total_delta": 4405.0}, "89323": {"port": 57719, "peak_rss_kib": 66816.0, "peak_cpu_pct": 55.6, "peak_fd_inbound": 336, "peak_fd_upstream": 276, "peak_streams_open": 274.0, "requests_total_delta": 4406.0}}, "marginal_rss_kib_per_stream": 15.25316115575453}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   53.12 |   55.84 |   +2.72 |
| p90 |   55.65 |   66.42 |  +10.77 |
| p99 |   56.87 |   78.58 |  +21.72 |
| p99.9 |   73.59 |   96.52 |  +22.93 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=8821 G=8821 requests
- CALIBRATION: Arm D p99 TTFE=56.87 ms < Arm G p99 TTFE=78.58 ms -> CALIBRATED (client faster than gateway path, number is honest)
- no generator-saturation flags (schedule kept, errors bounded)

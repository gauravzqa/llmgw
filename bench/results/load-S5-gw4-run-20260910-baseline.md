# Scenario S5: Slow clients
_Backpressure: is gateway memory bounded when clients read slower than the upstream writes._

produces: peak pump_buffered_bytes (must stay under the pump ceiling); RSS bounded; no unbounded growth with 500 slow readers.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.52783203125, 6.91015625, 6.115234375], "ts": "2026-09-10 20:48:57Z", "gw_workers": 4, "fake_workers": 4}

config: rate=25.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 25 rps, 8 workers, wall 397.9s
started=8821 ok=8821 error=0 bytes=681916226 late=4 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   53.10 |   55.60 |   56.18 |   62.87 |   51.72 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.56 |
| inter-event    | 2930508 |   94.39 |   98.88 |   99.91 |  120.21 |   93.99 |

status: {'200': 8821}

### Arm G -- rate 25 rps, 8 workers, wall 397.8s
started=8821 ok=8821 error=0 bytes=681916226 late=2 peak_inflight(sum over workers)=1127

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    7308 |   53.42 |   56.22 |   62.57 |   73.58 |   54.32 |
| total          |    7308 | 37583.74 | 39355.01 | 39764.91 | 39806.13 | 37747.62 |
| inter-event    | 2930508 |   94.39 |   98.88 |   99.91 |  120.22 |   93.98 |

status: {'200': 8821}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 348, "workers": 4, "peak_streams_open": 547.0, "peak_tasks": 2747.0, "baseline_tasks": 16.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 384.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 236944.0, "min_rss_kib": 155664.0, "peak_cpu_pct": 123.9, "peak_fd_inbound": 668, "peak_fd_upstream": 554, "peak_rss_kib_max_worker": 59376.0, "peak_cpu_pct_max_worker": 34.4, "peak_fd_inbound_max_worker": 169, "peak_fd_upstream_max_worker": 139, "requests_total_delta": 8787.0, "per_worker": {"56336": {"port": 26349, "peak_rss_kib": 59328.0, "peak_cpu_pct": 32.7, "peak_fd_inbound": 169, "peak_fd_upstream": 138, "peak_streams_open": 137.0, "requests_total_delta": 2197.0}, "56337": {"port": 26350, "peak_rss_kib": 59376.0, "peak_cpu_pct": 34.4, "peak_fd_inbound": 169, "peak_fd_upstream": 139, "peak_streams_open": 138.0, "requests_total_delta": 2198.0}, "56338": {"port": 26351, "peak_rss_kib": 59056.0, "peak_cpu_pct": 31.8, "peak_fd_inbound": 167, "peak_fd_upstream": 139, "peak_streams_open": 139.0, "requests_total_delta": 2197.0}, "56339": {"port": 26352, "peak_rss_kib": 59184.0, "peak_cpu_pct": 32.1, "peak_fd_inbound": 167, "peak_fd_upstream": 138, "peak_streams_open": 137.0, "requests_total_delta": 2195.0}}, "marginal_rss_kib_per_stream": 15.937381608452197}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   53.10 |   53.42 |   +0.33 |
| p90 |   55.60 |   56.22 |   +0.62 |
| p99 |   56.18 |   62.57 |   +6.38 |
| p99.9 |   62.87 |   73.58 |  +10.71 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=8821 G=8821 requests
- CALIBRATION: Arm D p99 TTFE=56.18 ms < Arm G p99 TTFE=62.57 ms -> CALIBRATED (client faster than gateway path, number is honest)
- no generator-saturation flags (schedule kept, errors bounded)

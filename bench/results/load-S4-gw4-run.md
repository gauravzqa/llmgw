# Scenario S4: 10k open streams
_WHAT BREAKS FIRST at 10,000 open streams._

produces: the first limit hit (predict ulimit -n, then per-stream asyncio task/buffer overhead); RSS at 10k; marginal bytes/stream (slope).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.041015625, 3.99462890625, 5.02978515625], "ts": "2026-09-10 20:34:45Z", "gw_workers": 4, "fake_workers": 4}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 425.9s
started=36107 ok=22466 error=13625 bytes=871366276 late=382 peak_inflight(sum over workers)=20595

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 | 1059.56 | 1109.75 | 1121.37 | 1241.95 | 1008.56 |
| total          |   16635 | 211348.90 | 221309.47 | 223614.52 | 223846.34 | 200431.39 |
| inter-event    | 5137854 | 1049.91 | 1107.55 | 1120.95 | 1223.60 |  995.38 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 425.5s
started=36107 ok=19000 error=17091 bytes=738147212 late=450 peak_inflight(sum over workers)=21281

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30265 | 15854.79 | 31331.33 | 42460.87 | 48328.86 | 14570.34 |
| total          |   28730 | 32150.22 | 219270.01 | 237824.28 | 249819.09 | 110871.68 |
| inter-event    | 2875568 |  698.94 | 3610.91 | 5045.68 | 6081.50 | 1003.49 |

status: {'200': 20542, '504': 15554}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 190, "workers": 4, "peak_streams_open": 19295.0, "peak_tasks": 95663.0, "baseline_tasks": 7139.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 13050.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 1560624.0, "min_rss_kib": 155200.0, "peak_cpu_pct": 396.6, "peak_fd_inbound": 20875, "peak_fd_upstream": 19418, "peak_rss_kib_max_worker": 396464.0, "peak_cpu_pct_max_worker": 100.0, "peak_fd_inbound_max_worker": 5250, "peak_fd_upstream_max_worker": 4870, "requests_total_delta": 34674.0, "per_worker": {"51802": {"port": 24421, "peak_rss_kib": 390000.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 5201, "peak_fd_upstream": 4842, "peak_streams_open": 4865.0, "requests_total_delta": 8603.0}, "51803": {"port": 24422, "peak_rss_kib": 396464.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 5250, "peak_fd_upstream": 4864, "peak_streams_open": 4828.0, "requests_total_delta": 8751.0}, "51804": {"port": 24423, "peak_rss_kib": 388112.0, "peak_cpu_pct": 99.5, "peak_fd_inbound": 5215, "peak_fd_upstream": 4870, "peak_streams_open": 4835.0, "requests_total_delta": 8509.0}, "51805": {"port": 24424, "peak_rss_kib": 386048.0, "peak_cpu_pct": 99.5, "peak_fd_inbound": 5209, "peak_fd_upstream": 4842, "peak_streams_open": 4821.0, "requests_total_delta": 8772.0}}, "marginal_rss_kib_per_stream": 13.558391958364068}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 | 1059.56 | 15854.79 | +14795.23 |
| p90 | 1109.75 | 31331.33 | +30221.58 |
| p99 | 1121.37 | 42460.87 | +41339.49 |
| p99.9 | 1241.95 | 48328.86 | +47086.91 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=20773 requests
- CALIBRATION: Arm D p99 TTFE=1121.37 ms < Arm G p99 TTFE=42460.87 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 13625 errors (37.8%): {'ReadError': 13633}
- Arm G: 17091 errors (47.4%): {'timeout': 11, 'protocol': 14, 'ReadError': 1520}

# Scenario S4: 10k open streams
_WHAT BREAKS FIRST at 10,000 open streams._

produces: the first limit hit (predict ulimit -n, then per-stream asyncio task/buffer overhead); RSS at 10k; marginal bytes/stream (slope).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.1572265625, 3.501953125, 3.8671875], "ts": "2026-09-15 19:29:26Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": 150, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 426.0s
started=36107 ok=22420 error=13671 bytes=869582120 late=459 peak_inflight(sum over workers)=20599

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 | 1059.58 | 1109.79 | 1121.41 | 1242.87 | 1009.39 |
| total          |   16589 | 211348.90 | 221309.47 | 223614.52 | 223846.34 | 200471.08 |
| inter-event    | 5137331 | 1047.43 | 1107.14 | 1121.04 | 1224.79 |  995.83 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 407.0s
started=36107 ok=1200 error=34907 bytes=50906575 late=479 peak_inflight(sum over workers)=674

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30278 |    2.64 |    6.33 | 1058.69 | 1115.52 |   23.19 |
| total          |   30278 |    3.10 |    7.24 | 211236.19 | 222575.23 | 3973.13 |
| inter-event    |  120600 | 1038.78 | 1104.85 | 1120.29 | 1121.85 |  991.51 |

status: {'200': 1200, '503': 34907}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 363, "workers": 4, "peak_streams_open": 600.0, "peak_tasks": 3017.0, "baseline_tasks": 201.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 227280.0, "min_rss_kib": 93344.0, "peak_cpu_pct": 69.7, "peak_fd_inbound": 606, "peak_fd_upstream": 600, "peak_rss_kib_max_worker": 57024.0, "peak_cpu_pct_max_worker": 18.1, "peak_fd_inbound_max_worker": 152, "peak_fd_upstream_max_worker": 150, "requests_total_delta": 1137.0, "per_worker": {"88788": {"port": 21151, "peak_rss_kib": 56960.0, "peak_cpu_pct": 15.8, "peak_fd_inbound": 152, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 285.0}, "88789": {"port": 21152, "peak_rss_kib": 56576.0, "peak_cpu_pct": 18.0, "peak_fd_inbound": 152, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 285.0}, "88790": {"port": 21153, "peak_rss_kib": 56720.0, "peak_cpu_pct": 18.1, "peak_fd_inbound": 152, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 284.0}, "88791": {"port": 21154, "peak_rss_kib": 57024.0, "peak_cpu_pct": 17.8, "peak_fd_inbound": 152, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 283.0}}, "marginal_rss_kib_per_stream": 33.49190975848964}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 | 1059.58 |    2.64 | -1056.94 |
| p90 | 1109.79 |    6.33 | -1103.46 |
| p99 | 1121.41 | 1058.69 |  -62.72 |
| p99.9 | 1242.87 | 1115.52 | -127.35 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=1200 requests
- CALIBRATION: Arm D p99 TTFE=1121.41 ms >= Arm G p99 TTFE=1058.69 ms -> INVALID (client is the bottleneck, not the gateway)
## Saturation / instrument-is-a-SUT flags
- Arm D: 13671 errors (37.9%): {'ReadError': 13679}
- Arm G: 34907 errors (96.7%): {}

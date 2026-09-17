# Scenario S3: 1k open streams
_Memory and fd at 1,000 open slow-drip streams._

produces: RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.6865234375, 5.52880859375, 5.705078125], "ts": "2026-09-17 22:08:18Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=10.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3197 error=320 bytes=123998842 late=4 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.88 |  555.90 |  561.69 |  562.28 |  506.19 |
| total          |    2591 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100409.24 |
| inter-event    |  576030 |  490.81 |  546.02 |  560.69 |  562.18 |  497.50 |

status: {'200': 3533}

### Arm G -- rate 10 rps, 8 workers, wall 426.0s
started=3533 ok=3194 error=323 bytes=123882733 late=4 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.87 |  555.94 |  561.74 |  562.32 |  508.96 |
| total          |    2591 | 105918.30 | 110916.00 | 112072.59 | 112188.91 | 100217.01 |
| inter-event    |  575426 |  490.73 |  545.99 |  560.70 |  562.19 |  497.13 |

admitted only (status 200; refused requests excluded):

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2924 |  530.91 |  555.94 |  561.74 |  562.32 |  509.47 |
| total          |    2588 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100333.16 |
| inter-event    |  575426 |  490.73 |  545.99 |  560.70 |  562.19 |  497.13 |

refused before a byte: 3 (0.1% of 3517 answered): {'502': 3}

status: {'200': 3530, '502': 3}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 378, "workers": 4, "peak_streams_open": 1045.0, "peak_tasks": 5227.0, "baseline_tasks": 1706.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 261568.0, "min_rss_kib": 158768.0, "peak_cpu_pct": 71.7, "peak_fd_inbound": 1046, "peak_fd_upstream": 1049, "peak_rss_kib_max_worker": 65520.0, "peak_cpu_pct_max_worker": 20.7, "peak_fd_inbound_max_worker": 263, "peak_fd_upstream_max_worker": 263, "requests_total_delta": 3181.0, "per_worker": {"18445": {"port": 53713, "peak_rss_kib": 65328.0, "peak_cpu_pct": 20.7, "peak_fd_inbound": 262, "peak_fd_upstream": 261, "peak_streams_open": 261.0, "requests_total_delta": 794.0}, "18446": {"port": 53714, "peak_rss_kib": 65392.0, "peak_cpu_pct": 19.8, "peak_fd_inbound": 261, "peak_fd_upstream": 262, "peak_streams_open": 260.0, "requests_total_delta": 795.0}, "18447": {"port": 53715, "peak_rss_kib": 65328.0, "peak_cpu_pct": 19.8, "peak_fd_inbound": 263, "peak_fd_upstream": 263, "peak_streams_open": 262.0, "requests_total_delta": 795.0}, "18448": {"port": 53716, "peak_rss_kib": 65520.0, "peak_cpu_pct": 18.2, "peak_fd_inbound": 263, "peak_fd_upstream": 263, "peak_streams_open": 262.0, "requests_total_delta": 797.0}}, "marginal_rss_kib_per_stream": 49.4689187743393}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.88 |  530.87 |   -0.01 |
| p90 |  555.90 |  555.94 |   +0.03 |
| p99 |  561.69 |  561.74 |   +0.04 |
| p99.9 |  562.28 |  562.32 |   +0.04 |

added first-event latency (gateway path), matched-quantile, admitted only:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.88 |  530.91 |   +0.02 |
| p90 |  555.90 |  555.94 |   +0.04 |
| p99 |  561.69 |  561.74 |   +0.04 |
| p99.9 |  562.28 |  562.32 |   +0.04 |
shed (refused before a byte): Arm D 0.0%, Arm G 0.1%

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=3533 G=3530 requests
- CALIBRATION: Arm D p99 TTFE=561.69 ms < Arm G p99 TTFE=561.74 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 320 errors (9.1%): {'ReadError': 328}
- Arm G: 323 errors (9.2%): {'ReadError': 328}

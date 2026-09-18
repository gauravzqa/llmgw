# Scenario S3: 1k open streams
_Memory and fd at 1,000 open slow-drip streams._

produces: RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.65869140625, 5.32421875, 5.486328125], "ts": "2026-09-18 00:01:50Z", "gw_workers": 2, "fake_workers": 2, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=10.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=2 fake_workers=2

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3195 error=322 bytes=123921270 late=7 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.89 |  555.92 |  561.72 |  562.30 |  506.92 |
| total          |    2589 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100466.41 |
| inter-event    |  575920 |  508.03 |  551.05 |  561.22 |  562.25 |  497.83 |

status: {'200': 3533}

### Arm G -- rate 10 rps, 8 workers, wall 426.0s
started=3533 ok=3196 error=321 bytes=123960191 late=5 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.99 |  556.12 |  561.94 |  611.92 |  509.68 |
| total          |    2591 | 105923.02 | 110916.99 | 112072.69 | 112188.92 | 100301.13 |
| inter-event    |  575817 |  510.69 |  551.67 |  561.34 |  562.31 |  497.15 |

admitted only (status 200; refused requests excluded):

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2926 |  531.00 |  556.12 |  561.94 |  611.93 |  509.85 |
| total          |    2590 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100339.85 |
| inter-event    |  575817 |  510.69 |  551.67 |  561.34 |  562.31 |  497.15 |

refused before a byte: 1 (0.0% of 3517 answered): {'502': 1}

status: {'200': 3532, '502': 1}

gateway samples (2 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 380, "workers": 2, "peak_streams_open": 1047.0, "peak_tasks": 5225.0, "baseline_tasks": 641.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 4532.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 163104.0, "min_rss_kib": 79536.0, "peak_cpu_pct": 64.3, "peak_fd_inbound": 1048, "peak_fd_upstream": 1047, "peak_rss_kib_max_worker": 81584.0, "peak_cpu_pct_max_worker": 38.4, "peak_fd_inbound_max_worker": 525, "peak_fd_upstream_max_worker": 524, "requests_total_delta": 3391.0, "per_worker": {"41072": {"port": 40474, "peak_rss_kib": 81584.0, "peak_cpu_pct": 38.2, "peak_fd_inbound": 525, "peak_fd_upstream": 524, "peak_streams_open": 524.0, "requests_total_delta": 1692.0}, "41073": {"port": 40475, "peak_rss_kib": 81520.0, "peak_cpu_pct": 38.4, "peak_fd_inbound": 523, "peak_fd_upstream": 523, "peak_streams_open": 523.0, "requests_total_delta": 1699.0}}, "marginal_rss_kib_per_stream": 56.751405968429204}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.89 |  530.99 |   +0.09 |
| p90 |  555.92 |  556.12 |   +0.20 |
| p99 |  561.72 |  561.94 |   +0.22 |
| p99.9 |  562.30 |  611.92 |  +49.62 |

added first-event latency (gateway path), matched-quantile, admitted only:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.89 |  531.00 |   +0.10 |
| p90 |  555.92 |  556.12 |   +0.20 |
| p99 |  561.72 |  561.94 |   +0.22 |
| p99.9 |  562.30 |  611.93 |  +49.63 |
shed (refused before a byte): Arm D 0.0%, Arm G 0.0%

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=3533 G=3532 requests
- CALIBRATION: Arm D p99 TTFE=561.72 ms < Arm G p99 TTFE=561.94 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 322 errors (9.2%): {'ReadError': 330}
- Arm G: 321 errors (9.1%): {'ReadError': 328}

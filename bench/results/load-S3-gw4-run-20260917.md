# Scenario S3: 1k open streams
_Memory and fd at 1,000 open slow-drip streams._

produces: RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.984375, 7.49072265625, 8.14599609375], "ts": "2026-09-17 19:15:33Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=10.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3197 error=320 bytes=123998842 late=9 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.88 |  555.90 |  561.69 |  562.28 |  506.87 |
| total          |    2591 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100499.81 |
| inter-event    |  576008 |  508.82 |  551.20 |  561.22 |  562.23 |  497.92 |

status: {'200': 3533}

### Arm G -- rate 10 rps, 8 workers, wall 426.0s
started=3533 ok=3195 error=322 bytes=123921270 late=7 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.89 |  555.92 |  561.72 |  562.30 |  508.80 |
| total          |    2589 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100390.93 |
| inter-event    |  575868 |  508.76 |  551.25 |  561.29 |  562.30 |  497.49 |

status: {'200': 3533}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 378, "workers": 4, "peak_streams_open": 1041.0, "peak_tasks": 5219.0, "baseline_tasks": 1690.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 1262.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 262016.0, "min_rss_kib": 156160.0, "peak_cpu_pct": 61.6, "peak_fd_inbound": 1045, "peak_fd_upstream": 1050, "peak_rss_kib_max_worker": 65648.0, "peak_cpu_pct_max_worker": 21.9, "peak_fd_inbound_max_worker": 263, "peak_fd_upstream_max_worker": 263, "requests_total_delta": 3184.0, "per_worker": {"81985": {"port": 33904, "peak_rss_kib": 65392.0, "peak_cpu_pct": 21.5, "peak_fd_inbound": 262, "peak_fd_upstream": 262, "peak_streams_open": 261.0, "requests_total_delta": 794.0}, "81986": {"port": 33905, "peak_rss_kib": 65648.0, "peak_cpu_pct": 16.5, "peak_fd_inbound": 262, "peak_fd_upstream": 262, "peak_streams_open": 261.0, "requests_total_delta": 795.0}, "81987": {"port": 33906, "peak_rss_kib": 65456.0, "peak_cpu_pct": 15.5, "peak_fd_inbound": 263, "peak_fd_upstream": 263, "peak_streams_open": 262.0, "requests_total_delta": 796.0}, "81988": {"port": 33907, "peak_rss_kib": 65536.0, "peak_cpu_pct": 21.9, "peak_fd_inbound": 262, "peak_fd_upstream": 263, "peak_streams_open": 262.0, "requests_total_delta": 799.0}}, "marginal_rss_kib_per_stream": 59.68718414384347}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.88 |  530.89 |   +0.01 |
| p90 |  555.90 |  555.92 |   +0.02 |
| p99 |  561.69 |  561.72 |   +0.02 |
| p99.9 |  562.28 |  562.30 |   +0.02 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=3533 G=3533 requests
- CALIBRATION: Arm D p99 TTFE=561.69 ms < Arm G p99 TTFE=561.72 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 320 errors (9.1%): {'ReadError': 328}
- Arm G: 322 errors (9.2%): {'ReadError': 330}

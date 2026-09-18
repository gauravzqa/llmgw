# Scenario S3: 1k open streams
_Memory and fd at 1,000 open slow-drip streams._

produces: RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [6.4619140625, 5.64013671875, 5.359375], "ts": "2026-09-18 11:40:06Z", "gw_workers": 2, "fake_workers": 2, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=10.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=2 fake_workers=2

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3195 error=322 bytes=123921270 late=7 peak_inflight(sum over workers)=1130

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.92 |  555.96 |  561.76 |  567.09 |  510.53 |
| total          |    2589 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100874.45 |
| inter-event    |  575830 |  508.77 |  551.28 |  561.33 |  562.34 |  499.74 |

status: {'200': 3533}

### Arm G -- rate 10 rps, 8 workers, wall 426.0s
started=3533 ok=3196 error=321 bytes=123960056 late=9 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  531.00 |  556.12 |  561.94 |  611.92 |  512.29 |
| total          |    2590 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100462.68 |
| inter-event    |  575956 |  507.50 |  550.98 |  561.27 |  562.31 |  497.74 |

status: {'200': 3533}

gateway samples (2 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 378, "workers": 2, "peak_streams_open": 1042.0, "peak_tasks": 5216.0, "baseline_tasks": 1708.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 662.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 166448.0, "min_rss_kib": 49136.0, "peak_cpu_pct": 60.4, "peak_fd_inbound": 1044, "peak_fd_upstream": 1049, "peak_rss_kib_max_worker": 83344.0, "peak_cpu_pct_max_worker": 39.9, "peak_fd_inbound_max_worker": 523, "peak_fd_upstream_max_worker": 525, "requests_total_delta": 3191.0, "per_worker": {"37820": {"port": 23307, "peak_rss_kib": 83104.0, "peak_cpu_pct": 33.0, "peak_fd_inbound": 523, "peak_fd_upstream": 525, "peak_streams_open": 522.0, "requests_total_delta": 1595.0}, "37821": {"port": 23308, "peak_rss_kib": 83344.0, "peak_cpu_pct": 39.9, "peak_fd_inbound": 523, "peak_fd_upstream": 524, "peak_streams_open": 522.0, "requests_total_delta": 1589.0}}, "marginal_rss_kib_per_stream": 59.45952338075064}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.92 |  531.00 |   +0.08 |
| p90 |  555.96 |  556.12 |   +0.16 |
| p99 |  561.76 |  561.94 |   +0.18 |
| p99.9 |  567.09 |  611.92 |  +44.83 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=3533 G=3533 requests
- CALIBRATION: Arm D p99 TTFE=561.76 ms < Arm G p99 TTFE=561.94 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 322 errors (9.2%): {'ReadError': 330}
- Arm G: 321 errors (9.1%): {'ReadError': 329}

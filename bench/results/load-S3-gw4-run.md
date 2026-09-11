# Scenario S3: 1k open streams
_Memory and fd at 1,000 open slow-drip streams._

produces: RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [3.3154296875, 3.37451171875, 3.39111328125], "ts": "2026-09-10 19:48:45Z", "gw_workers": 4, "fake_workers": 4}

config: rate=10.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3198 error=319 bytes=124037628 late=2 peak_inflight(sum over workers)=1128

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.88 |  555.90 |  561.69 |  562.28 |  504.23 |
| total          |    2592 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100189.07 |
| inter-event    |  576110 |  484.53 |  540.88 |  560.16 |  562.12 |  496.44 |

status: {'200': 3533}

### Arm G -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3198 error=319 bytes=124037628 late=1 peak_inflight(sum over workers)=1128

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.88 |  555.90 |  561.69 |  562.28 |  506.98 |
| total          |    2592 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100185.26 |
| inter-event    |  576130 |  485.28 |  541.77 |  560.25 |  562.13 |  496.38 |

status: {'200': 3533}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 371, "workers": 4, "peak_streams_open": 1044.0, "peak_tasks": 5218.0, "baseline_tasks": 1491.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 261824.0, "min_rss_kib": 155520.0, "peak_cpu_pct": 66.30000000000001, "peak_fd_inbound": 1046, "peak_fd_upstream": 1048, "peak_rss_kib_max_worker": 65568.0, "peak_cpu_pct_max_worker": 20.1, "peak_fd_inbound_max_worker": 262, "peak_fd_upstream_max_worker": 263, "requests_total_delta": 3224.0, "per_worker": {"35975": {"port": 59320, "peak_rss_kib": 65392.0, "peak_cpu_pct": 20.1, "peak_fd_inbound": 261, "peak_fd_upstream": 261, "peak_streams_open": 261.0, "requests_total_delta": 804.0}, "35976": {"port": 59321, "peak_rss_kib": 65568.0, "peak_cpu_pct": 17.7, "peak_fd_inbound": 262, "peak_fd_upstream": 262, "peak_streams_open": 262.0, "requests_total_delta": 807.0}, "35977": {"port": 59322, "peak_rss_kib": 65376.0, "peak_cpu_pct": 19.2, "peak_fd_inbound": 262, "peak_fd_upstream": 262, "peak_streams_open": 262.0, "requests_total_delta": 806.0}, "35978": {"port": 59323, "peak_rss_kib": 65488.0, "peak_cpu_pct": 16.2, "peak_fd_inbound": 262, "peak_fd_upstream": 263, "peak_streams_open": 261.0, "requests_total_delta": 807.0}}, "marginal_rss_kib_per_stream": 59.3748746464344}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.88 |  530.88 |   +0.00 |
| p90 |  555.90 |  555.90 |   +0.00 |
| p99 |  561.69 |  561.69 |   +0.00 |
| p99.9 |  562.28 |  562.28 |   +0.00 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=3533 G=3533 requests
- CALIBRATION: Arm D p99 TTFE=561.69 ms >= Arm G p99 TTFE=561.69 ms -> INVALID (client is the bottleneck, not the gateway)
## Saturation / instrument-is-a-SUT flags
- Arm D: 319 errors (9.1%): {'ReadError': 327}
- Arm G: 319 errors (9.1%): {'ReadError': 327}

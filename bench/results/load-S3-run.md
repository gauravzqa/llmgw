# Scenario S3: 1k open streams
_Memory and fd at 1,000 open slow-drip streams._

produces: RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.8017578125, 5.02880859375, 5.08251953125], "ts": "2026-09-10 16:45:38Z"}

config: rate=10.0 rps, workers=8, warm=60.0s measure=300.0s repeats=3

## Median run (3 of 3, by Arm G p99 TTFE)

### Arm D -- rate 10 rps, 8 workers, wall 426.0s
started=3533 ok=3198 error=319 bytes=124037628 late=4 peak_inflight(sum over workers)=1129

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  530.88 |  555.90 |  561.69 |  562.28 |  506.16 |
| total          |    2592 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100227.36 |
| inter-event    |  576073 |  485.30 |  541.79 |  560.25 |  562.13 |  496.63 |

status: {'200': 3533}

### Arm G -- rate 10 rps, 8 workers, wall 425.9s
started=3533 ok=3171 error=346 bytes=122993447 late=4 peak_inflight(sum over workers)=1116

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    2927 |  531.28 |  557.11 |  604.60 |  684.49 |  510.65 |
| total          |    2590 | 105865.96 | 110905.04 | 112071.49 | 112188.80 | 99383.62 |
| inter-event    |  570954 |  509.04 |  551.86 |  561.98 |  622.00 |  497.16 |

status: {'200': 3508, '429': 23, '502': 2}

gateway samples: {"n_samples": 388, "peak_streams_open": 1024.0, "peak_tasks": 5118.0, "baseline_tasks": 1694.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 1156.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 116816.0, "min_rss_kib": 116736.0, "peak_cpu_pct": 64.0, "peak_fd_inbound": 1025, "peak_fd_upstream": 1024, "marginal_rss_kib_per_stream": 0.04201820230877578}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  530.88 |  531.28 |   +0.40 |
| p90 |  555.90 |  557.11 |   +1.20 |
| p99 |  561.69 |  604.60 |  +42.91 |
| p99.9 |  562.28 |  684.49 | +122.21 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=3533 G=3508 requests
- CALIBRATION: Arm D p99 TTFE=561.69 ms < Arm G p99 TTFE=604.60 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 319 errors (9.1%): {'ReadError': 327}
- Arm G: 346 errors (9.8%): {'ReadError': 329}

run-to-run spread (Arm G p99 TTFE ms): min=592.97 max=612.34 median=604.60

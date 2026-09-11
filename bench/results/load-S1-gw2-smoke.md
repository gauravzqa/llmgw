# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.625, 4.548828125, 4.5], "ts": "2026-09-10 19:12:13Z", "gw_workers": 2, "fake_workers": 2}

config: rate=40.0 rps, workers=2, warm=1.0s measure=4.0s repeats=1 gw_workers=2 fake_workers=2

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 40 rps, 2 workers, wall 5.2s
started=198 ok=198 error=0 bytes=90684 late=4 peak_inflight(sum over workers)=4

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     159 |    1.83 |    2.38 |    4.68 |   13.87 |    2.00 |
| total          |     159 |    1.83 |    2.38 |    4.68 |   13.87 |    2.00 |

status: {'200': 198}

### Arm G -- rate 40 rps, 2 workers, wall 5.2s
started=198 ok=198 error=0 bytes=90684 late=3 peak_inflight(sum over workers)=4

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     159 |    3.30 |    3.92 |    6.61 |   11.02 |    3.38 |
| total          |     159 |    3.30 |    3.92 |    6.61 |   11.02 |    3.38 |

status: {'200': 198}

gateway samples (2 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 5, "workers": 2, "peak_streams_open": 0.0, "peak_tasks": 8.0, "baseline_tasks": 8.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 94464.0, "min_rss_kib": 77248.0, "peak_cpu_pct": 9.9, "peak_fd_inbound": 2, "peak_fd_upstream": 4, "peak_rss_kib_max_worker": 47280.0, "peak_cpu_pct_max_worker": 5.0, "peak_fd_inbound_max_worker": 1, "peak_fd_upstream_max_worker": 2, "requests_total_delta": 141.0, "per_worker": {"24371": {"port": 55337, "peak_rss_kib": 47184.0, "peak_cpu_pct": 4.9, "peak_fd_inbound": 1, "peak_fd_upstream": 2, "peak_streams_open": 0.0, "requests_total_delta": 71.0}, "24372": {"port": 55338, "peak_rss_kib": 47280.0, "peak_cpu_pct": 5.0, "peak_fd_inbound": 1, "peak_fd_upstream": 2, "peak_streams_open": 0.0, "requests_total_delta": 70.0}}}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    1.83 |    3.30 |   +1.47 |
| p90 |    2.38 |    3.92 |   +1.53 |
| p99 |    4.68 |    6.61 |   +1.93 |
| p99.9 |   13.87 |   11.02 |   -2.85 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=198 G=198 requests
- CALIBRATION: Arm D p99 TTFE=4.68 ms < Arm G p99 TTFE=6.61 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 4/198 (2.0%) arrivals fired LATE (max 1 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

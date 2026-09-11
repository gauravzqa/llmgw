# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [2.8828125, 2.91015625, 3.26953125], "ts": "2026-09-10 19:36:44Z", "gw_workers": 4, "fake_workers": 4}

config: rate=400.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 400 rps, 8 workers, wall 360.2s
started=144470 ok=144470 error=0 bytes=66167260 late=4678 peak_inflight(sum over workers)=37

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120529 |    1.62 |    2.13 |    4.39 |   10.27 |    1.73 |
| total          |  120529 |    1.62 |    2.13 |    4.39 |   10.27 |    1.73 |

status: {'200': 144470}

### Arm G -- rate 400 rps, 8 workers, wall 360.2s
started=144470 ok=144470 error=0 bytes=66167260 late=3875 peak_inflight(sum over workers)=38

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120529 |    2.66 |    3.37 |    4.76 |    8.77 |    2.78 |
| total          |  120529 |    2.66 |    3.37 |    4.76 |    8.77 |    2.78 |

status: {'200': 144470}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 320, "workers": 4, "peak_streams_open": 4.0, "peak_tasks": 28.0, "baseline_tasks": 19.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 373.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 191872.0, "min_rss_kib": 155552.0, "peak_cpu_pct": 61.7, "peak_fd_inbound": 9, "peak_fd_upstream": 16, "peak_rss_kib_max_worker": 48032.0, "peak_cpu_pct_max_worker": 16.0, "peak_fd_inbound_max_worker": 5, "peak_fd_upstream_max_worker": 5, "requests_total_delta": 143800.0, "per_worker": {"31651": {"port": 58673, "peak_rss_kib": 47920.0, "peak_cpu_pct": 16.0, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 2.0, "requests_total_delta": 35953.0}, "31652": {"port": 58674, "peak_rss_kib": 48000.0, "peak_cpu_pct": 15.9, "peak_fd_inbound": 5, "peak_fd_upstream": 4, "peak_streams_open": 2.0, "requests_total_delta": 35950.0}, "31653": {"port": 58675, "peak_rss_kib": 47920.0, "peak_cpu_pct": 15.1, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 2.0, "requests_total_delta": 35948.0}, "31654": {"port": 58676, "peak_rss_kib": 48032.0, "peak_cpu_pct": 15.3, "peak_fd_inbound": 4, "peak_fd_upstream": 4, "peak_streams_open": 3.0, "requests_total_delta": 35949.0}}, "marginal_rss_kib_per_stream": 61.954802259886996}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    1.62 |    2.66 |   +1.04 |
| p90 |    2.13 |    3.37 |   +1.23 |
| p99 |    4.39 |    4.76 |   +0.38 |
| p99.9 |   10.27 |    8.77 |   -1.50 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=144470 G=144470 requests
- CALIBRATION: Arm D p99 TTFE=4.39 ms < Arm G p99 TTFE=4.76 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 4678/144470 (3.2%) arrivals fired LATE (max 7 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 3875/144470 (2.7%) arrivals fired LATE (max 6 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

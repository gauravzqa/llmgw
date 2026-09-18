# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.10693359375, 5.21142578125, 5.37646484375], "ts": "2026-09-18 00:46:36Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=400.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 400 rps, 8 workers, wall 360.2s
started=144470 ok=144470 error=0 bytes=66167260 late=7815 peak_inflight(sum over workers)=39

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120529 |    1.92 |    2.56 |    6.03 |   13.17 |    2.08 |
| total          |  120529 |    1.92 |    2.56 |    6.03 |   13.17 |    2.08 |

status: {'200': 144470}

### Arm G -- rate 400 rps, 8 workers, wall 360.3s
started=144470 ok=144470 error=0 bytes=66167260 late=6554 peak_inflight(sum over workers)=39

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120529 |    2.92 |    3.84 |    7.16 |   13.97 |    3.12 |
| total          |  120529 |    2.92 |    3.84 |    7.16 |   13.97 |    3.12 |

status: {'200': 144470}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 329, "workers": 4, "peak_streams_open": 6.0, "peak_tasks": 29.0, "baseline_tasks": 17.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 655.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 211488.0, "min_rss_kib": 178384.0, "peak_cpu_pct": 84.5, "peak_fd_inbound": 10, "peak_fd_upstream": 19, "peak_rss_kib_max_worker": 52976.0, "peak_cpu_pct_max_worker": 21.4, "peak_fd_inbound_max_worker": 5, "peak_fd_upstream_max_worker": 5, "requests_total_delta": 143741.0, "per_worker": {"50831": {"port": 40162, "peak_rss_kib": 52928.0, "peak_cpu_pct": 21.1, "peak_fd_inbound": 5, "peak_fd_upstream": 5, "peak_streams_open": 3.0, "requests_total_delta": 35933.0}, "50832": {"port": 40163, "peak_rss_kib": 52976.0, "peak_cpu_pct": 21.3, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 4.0, "requests_total_delta": 35935.0}, "50833": {"port": 40164, "peak_rss_kib": 52720.0, "peak_cpu_pct": 21.4, "peak_fd_inbound": 3, "peak_fd_upstream": 5, "peak_streams_open": 3.0, "requests_total_delta": 35938.0}, "50834": {"port": 40165, "peak_rss_kib": 52864.0, "peak_cpu_pct": 20.7, "peak_fd_inbound": 3, "peak_fd_upstream": 4, "peak_streams_open": 2.0, "requests_total_delta": 35935.0}}, "marginal_rss_kib_per_stream": 14.059620356661435}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    1.92 |    2.92 |   +1.00 |
| p90 |    2.56 |    3.84 |   +1.28 |
| p99 |    6.03 |    7.16 |   +1.13 |
| p99.9 |   13.17 |   13.97 |   +0.81 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=144470 G=144470 requests
- CALIBRATION: Arm D p99 TTFE=6.03 ms < Arm G p99 TTFE=7.16 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 7815/144470 (5.4%) arrivals fired LATE (max 11 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 6554/144470 (4.5%) arrivals fired LATE (max 12 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

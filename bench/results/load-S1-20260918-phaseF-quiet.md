# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [6.1083984375, 5.853515625, 5.64501953125], "ts": "2026-09-18 00:32:37Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=400.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 400 rps, 8 workers, wall 360.2s
started=144470 ok=144470 error=0 bytes=66167260 late=7856 peak_inflight(sum over workers)=38

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120529 |    1.90 |    2.54 |    5.90 |   12.33 |    2.06 |
| total          |  120529 |    1.90 |    2.54 |    5.90 |   12.33 |    2.06 |

status: {'200': 144470}

### Arm G -- rate 400 rps, 8 workers, wall 360.3s
started=144471 ok=144471 error=0 bytes=66167718 late=6492 peak_inflight(sum over workers)=39

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    2.87 |    3.79 |    6.64 |   12.98 |    3.07 |
| total          |  120530 |    2.87 |    3.79 |    6.64 |   12.98 |    3.07 |

status: {'200': 144471}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 329, "workers": 4, "peak_streams_open": 5.0, "peak_tasks": 31.0, "baseline_tasks": 16.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 655.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 195008.0, "min_rss_kib": 158896.0, "peak_cpu_pct": 67.1, "peak_fd_inbound": 10, "peak_fd_upstream": 18, "peak_rss_kib_max_worker": 48816.0, "peak_cpu_pct_max_worker": 17.2, "peak_fd_inbound_max_worker": 4, "peak_fd_upstream_max_worker": 5, "requests_total_delta": 143789.0, "per_worker": {"47523": {"port": 45084, "peak_rss_kib": 48816.0, "peak_cpu_pct": 16.8, "peak_fd_inbound": 3, "peak_fd_upstream": 5, "peak_streams_open": 4.0, "requests_total_delta": 35946.0}, "47524": {"port": 45085, "peak_rss_kib": 48816.0, "peak_cpu_pct": 17.2, "peak_fd_inbound": 3, "peak_fd_upstream": 5, "peak_streams_open": 3.0, "requests_total_delta": 35948.0}, "47525": {"port": 45086, "peak_rss_kib": 48560.0, "peak_cpu_pct": 17.1, "peak_fd_inbound": 4, "peak_fd_upstream": 4, "peak_streams_open": 3.0, "requests_total_delta": 35947.0}, "47526": {"port": 45087, "peak_rss_kib": 48816.0, "peak_cpu_pct": 17.0, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 2.0, "requests_total_delta": 35948.0}}, "marginal_rss_kib_per_stream": -10.879960773497595}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    1.90 |    2.87 |   +0.97 |
| p90 |    2.54 |    3.79 |   +1.24 |
| p99 |    5.90 |    6.64 |   +0.73 |
| p99.9 |   12.33 |   12.98 |   +0.66 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=144470 G=144471 requests
- CALIBRATION: Arm D p99 TTFE=5.90 ms < Arm G p99 TTFE=6.64 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 7856/144470 (5.4%) arrivals fired LATE (max 10 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 6492/144471 (4.5%) arrivals fired LATE (max 13 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

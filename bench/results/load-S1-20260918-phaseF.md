# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.955078125, 5.57373046875, 5.86328125], "ts": "2026-09-17 23:35:47Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=400.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 400 rps, 8 workers, wall 360.2s
started=144471 ok=144471 error=0 bytes=66167718 late=7865 peak_inflight(sum over workers)=38

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    1.87 |    2.50 |    5.84 |   12.30 |    2.03 |
| total          |  120530 |    1.87 |    2.50 |    5.84 |   12.30 |    2.03 |

status: {'200': 144471}

### Arm G -- rate 400 rps, 8 workers, wall 360.2s
started=144471 ok=144471 error=0 bytes=66167718 late=6516 peak_inflight(sum over workers)=41

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    2.85 |    3.79 |    7.02 |   14.83 |    3.07 |
| total          |  120530 |    2.85 |    3.79 |    7.02 |   14.83 |    3.07 |

status: {'200': 144471}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 329, "workers": 4, "peak_streams_open": 6.0, "peak_tasks": 30.0, "baseline_tasks": 16.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 655.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 194208.0, "min_rss_kib": 159088.0, "peak_cpu_pct": 67.5, "peak_fd_inbound": 12, "peak_fd_upstream": 20, "peak_rss_kib_max_worker": 48656.0, "peak_cpu_pct_max_worker": 17.2, "peak_fd_inbound_max_worker": 4, "peak_fd_upstream_max_worker": 7, "requests_total_delta": 143897.0, "per_worker": {"35616": {"port": 36911, "peak_rss_kib": 48656.0, "peak_cpu_pct": 16.6, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 3.0, "requests_total_delta": 35976.0}, "35617": {"port": 36912, "peak_rss_kib": 48560.0, "peak_cpu_pct": 17.0, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 3.0, "requests_total_delta": 35974.0}, "35618": {"port": 36913, "peak_rss_kib": 48624.0, "peak_cpu_pct": 17.2, "peak_fd_inbound": 4, "peak_fd_upstream": 7, "peak_streams_open": 3.0, "requests_total_delta": 35974.0}, "35619": {"port": 36914, "peak_rss_kib": 48368.0, "peak_cpu_pct": 17.1, "peak_fd_inbound": 4, "peak_fd_upstream": 4, "peak_streams_open": 3.0, "requests_total_delta": 35973.0}}, "marginal_rss_kib_per_stream": 8.98344676917906}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    1.87 |    2.85 |   +0.97 |
| p90 |    2.50 |    3.79 |   +1.29 |
| p99 |    5.84 |    7.02 |   +1.18 |
| p99.9 |   12.30 |   14.83 |   +2.52 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=144471 G=144471 requests
- CALIBRATION: Arm D p99 TTFE=5.84 ms < Arm G p99 TTFE=7.02 ms -> CALIBRATED (client faster than gateway path, number is honest)
## Saturation / instrument-is-a-SUT flags
- Arm D: 7865/144471 (5.4%) arrivals fired LATE (max 14 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 6516/144471 (4.5%) arrivals fired LATE (max 10 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

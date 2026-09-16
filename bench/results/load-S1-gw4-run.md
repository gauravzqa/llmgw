# Scenario S1: Short non-streaming
_Per-request overhead floor at high request rate._

produces: added p50/p99 first-event latency (paired); GIL pressure on short CPU-bound requests; throughput at which p99 doubles.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.73583984375, 5.61376953125, 5.67822265625], "ts": "2026-09-16 15:51:32Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=400.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 400 rps, 8 workers, wall 360.2s
started=144471 ok=144471 error=0 bytes=66167718 late=7570 peak_inflight(sum over workers)=41

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    2.33 |    3.25 |    6.40 |   11.49 |    2.50 |
| total          |  120530 |    2.33 |    3.25 |    6.40 |   11.49 |    2.50 |

status: {'200': 144471}

### Arm G -- rate 400 rps, 8 workers, wall 360.2s
started=144471 ok=144471 error=0 bytes=66167718 late=6458 peak_inflight(sum over workers)=40

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  120530 |    2.70 |    3.50 |    5.91 |   11.02 |    2.86 |
| total          |  120530 |    2.70 |    3.50 |    5.91 |   11.02 |    2.86 |

status: {'200': 144471}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 330, "workers": 4, "peak_streams_open": 5.0, "peak_tasks": 29.0, "baseline_tasks": 20.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 449.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 193808.0, "min_rss_kib": 156128.0, "peak_cpu_pct": 63.3, "peak_fd_inbound": 10, "peak_fd_upstream": 16, "peak_rss_kib_max_worker": 48592.0, "peak_cpu_pct_max_worker": 16.4, "peak_fd_inbound_max_worker": 5, "peak_fd_upstream_max_worker": 5, "requests_total_delta": 144023.0, "per_worker": {"96340": {"port": 59865, "peak_rss_kib": 48592.0, "peak_cpu_pct": 16.0, "peak_fd_inbound": 4, "peak_fd_upstream": 5, "peak_streams_open": 2.0, "requests_total_delta": 36006.0}, "96341": {"port": 59866, "peak_rss_kib": 48336.0, "peak_cpu_pct": 15.8, "peak_fd_inbound": 5, "peak_fd_upstream": 4, "peak_streams_open": 3.0, "requests_total_delta": 36006.0}, "96342": {"port": 59867, "peak_rss_kib": 48320.0, "peak_cpu_pct": 16.4, "peak_fd_inbound": 3, "peak_fd_upstream": 4, "peak_streams_open": 3.0, "requests_total_delta": 36006.0}, "96343": {"port": 59868, "peak_rss_kib": 48560.0, "peak_cpu_pct": 16.4, "peak_fd_inbound": 4, "peak_fd_upstream": 4, "peak_streams_open": 3.0, "requests_total_delta": 36005.0}}, "marginal_rss_kib_per_stream": 15.856077339223404}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |    2.33 |    2.70 |   +0.37 |
| p90 |    3.25 |    3.50 |   +0.26 |
| p99 |    6.40 |    5.91 |   -0.49 |
| p99.9 |   11.49 |   11.02 |   -0.47 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=144471 G=144471 requests
- CALIBRATION: Arm D p99 TTFE=6.40 ms >= Arm G p99 TTFE=5.91 ms -> INVALID (client is the bottleneck, not the gateway)
## Saturation / instrument-is-a-SUT flags
- Arm D: 7570/144471 (5.2%) arrivals fired LATE (max 23 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).
- Arm G: 6458/144471 (4.5%) arrivals fired LATE (max 6 ms) -- the GENERATOR could not keep its schedule (instrument-is-a-SUT).

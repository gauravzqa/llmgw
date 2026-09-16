# Scenario S8: Deploy under load
_Drain: client errors must be ZERO when SIGTERM lands mid-stream._

produces: client errors (target 0); drain duration; streams cut (target 0).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.82177734375, 4.93505859375, 5.18701171875], "ts": "2026-09-16 16:03:47Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}


### Arm G -- rate 10 rps, 8 workers, wall 360.9s
started=3533 ok=1205 error=2328 bytes=46831774 late=6 peak_inflight(sum over workers)=1052

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    1555 |    7.21 |  545.78 |  560.66 |  562.17 |  199.84 |
| total          |    1555 |    8.55 | 108898.03 | 111867.00 | 112168.32 | 38748.71 |
| inter-event    |  120399 |  507.14 |  550.85 |  561.20 |  562.24 |  497.85 |

admitted only (status 200; refused requests excluded):

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     599 |  530.88 |  555.90 |  561.69 |  562.28 |  510.88 |
| total          |     599 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100582.21 |
| inter-event    |  120399 |  507.14 |  550.85 |  561.20 |  562.24 |  497.85 |

refused before a byte: 2328 (65.9% of 3533 answered): {'503': 956} + 1372 transport errors before a byte

status: {'200': 1205, '503': 956}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 320, "workers": 4, "peak_streams_open": 1020.0, "peak_tasks": 5102.0, "baseline_tasks": 60.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 462.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 253504.0, "min_rss_kib": 155856.0, "peak_cpu_pct": 113.5, "peak_fd_inbound": 1024, "peak_fd_upstream": 1028, "peak_rss_kib_max_worker": 63648.0, "peak_cpu_pct_max_worker": 28.8, "peak_fd_inbound_max_worker": 258, "peak_fd_upstream_max_worker": 258, "requests_total_delta": 1183.0, "per_worker": {"542": {"port": 55145, "peak_rss_kib": 63648.0, "peak_cpu_pct": 28.8, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 256.0, "requests_total_delta": 295.0}, "543": {"port": 55146, "peak_rss_kib": 63280.0, "peak_cpu_pct": 28.2, "peak_fd_inbound": 258, "peak_fd_upstream": 257, "peak_streams_open": 257.0, "requests_total_delta": 295.0}, "545": {"port": 55147, "peak_rss_kib": 63152.0, "peak_cpu_pct": 27.9, "peak_fd_inbound": 256, "peak_fd_upstream": 256, "peak_streams_open": 255.0, "requests_total_delta": 297.0}, "546": {"port": 55148, "peak_rss_kib": 63424.0, "peak_cpu_pct": 28.6, "peak_fd_inbound": 256, "peak_fd_upstream": 258, "peak_streams_open": 256.0, "requests_total_delta": 296.0}}, "marginal_rss_kib_per_stream": 30.78639611938749}

## Drain verdict
- config: drain_grace=600.0s budget_total=600.0s allow_short=False stream_length=100.0s uvicorn_shutdown_timeout=3.0s -> arm A (grace >= stream: zero cuts expected)
- SIGTERM sent to 4 gateway worker(s) at measure+60.0s; fleet fully exited after 100.84s (expected <= 608.0s) -> OK
- per-worker exit: pid 542: 100.04s; pid 543: 100.60s; pid 546: 100.72s; pid 545: 100.84s
- in-flight streams CUT (had >= 1 byte, then a transport error or no `data: [DONE]`): 0 (PLAN target: 0)
- arrivals REFUSED before a byte (connect refused after the listener closed, 503 draining, timeouts with nothing read): 2328 (expected once the fleet is gone; not a cut)
- error breakdown: {'connect': 1372}; status mix: {'200': 1205, '503': 956}
- verdict: PASS

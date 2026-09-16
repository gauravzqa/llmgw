# Scenario S8: Deploy under load
_Drain: client errors must be ZERO when SIGTERM lands mid-stream._

produces: client errors (target 0); drain duration; streams cut (target 0).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [3.84423828125, 3.82177734375, 4.38818359375], "ts": "2026-09-15 18:49:09Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 30.0, "gw_drain_allow_short": true, "gw_max_streams": null, "gw_drain_arm": "B (grace < total: cuts are the residual)"}


### Arm G -- rate 10 rps, 8 workers, wall 360.9s
started=3533 ok=520 error=3013 bytes=20199113 late=3 peak_inflight(sum over workers)=1050

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     906 |  515.45 |  552.63 |  561.36 |  562.24 |  335.96 |
| total          |     307 |    2.57 |    4.25 |    6.66 |    7.80 |    2.83 |
| inter-event    |   74647 |  490.23 |  545.61 |  560.65 |  562.17 |  501.07 |

status: {'200': 1205, '503': 307}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 324, "workers": 4, "peak_streams_open": 1019.0, "peak_tasks": 5097.0, "baseline_tasks": 3695.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 255584.0, "min_rss_kib": 155152.0, "peak_cpu_pct": 103.5, "peak_fd_inbound": 1023, "peak_fd_upstream": 1025, "peak_rss_kib_max_worker": 64016.0, "peak_cpu_pct_max_worker": 26.2, "peak_fd_inbound_max_worker": 257, "peak_fd_upstream_max_worker": 257, "requests_total_delta": 468.0, "per_worker": {"75932": {"port": 64112, "peak_rss_kib": 63792.0, "peak_cpu_pct": 26.2, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 255.0, "requests_total_delta": 117.0}, "75933": {"port": 64113, "peak_rss_kib": 63952.0, "peak_cpu_pct": 25.9, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 257.0, "requests_total_delta": 116.0}, "75934": {"port": 64114, "peak_rss_kib": 64016.0, "peak_cpu_pct": 25.5, "peak_fd_inbound": 255, "peak_fd_upstream": 255, "peak_streams_open": 254.0, "requests_total_delta": 115.0}, "75935": {"port": 64115, "peak_rss_kib": 63824.0, "peak_cpu_pct": 25.9, "peak_fd_inbound": 256, "peak_fd_upstream": 256, "peak_streams_open": 255.0, "requests_total_delta": 113.0}}, "marginal_rss_kib_per_stream": 68.9681236957634}

## Drain verdict
- config: drain_grace=30.0s budget_total=600.0s allow_short=True stream_length=100.0s uvicorn_shutdown_timeout=3.0s -> arm B (grace < stream: cuts are the residual)
- SIGTERM sent to 4 gateway worker(s) at measure+60.0s; fleet fully exited after 33.31s (expected <= 38.0s) -> OK
- per-worker exit: pid 75932: 33.31s; pid 75933: 33.31s; pid 75934: 33.31s; pid 75935: 33.31s
- in-flight streams CUT (had >= 1 byte, then a transport error or no `data: [DONE]`): 685 (PLAN target: 0, not scored in arm B)
- arrivals REFUSED before a byte (connect refused after the listener closed, 503 draining, timeouts with nothing read): 2328 (expected once the fleet is gone; not a cut)
- error breakdown: {'connect': 2021, 'protocol': 685}; status mix: {'200': 1205, '503': 307}
- verdict: PASS

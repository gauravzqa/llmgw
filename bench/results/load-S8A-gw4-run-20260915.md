# Scenario S8: Deploy under load
_Drain: client errors must be ZERO when SIGTERM lands mid-stream._

produces: client errors (target 0); drain duration; streams cut (target 0).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [7.5185546875, 6.24072265625, 5.59130859375], "ts": "2026-09-15 17:27:15Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}


### Arm G -- rate 10 rps, 8 workers, wall 360.9s
started=3533 ok=1205 error=2328 bytes=46830784 late=3 peak_inflight(sum over workers)=1051

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    1545 |    3.44 |  545.89 |  560.67 |  562.17 |  198.53 |
| total          |    1545 |    3.93 | 108918.97 | 111869.15 | 112168.53 | 38860.86 |
| inter-event    |  120399 |  487.78 |  544.10 |  560.49 |  562.16 |  496.11 |

status: {'200': 1205, '503': 946}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 323, "workers": 4, "peak_streams_open": 1019.0, "peak_tasks": 5097.0, "baseline_tasks": 60.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 386.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 255936.0, "min_rss_kib": 154928.0, "peak_cpu_pct": 96.8, "peak_fd_inbound": 1023, "peak_fd_upstream": 1025, "peak_rss_kib_max_worker": 64272.0, "peak_cpu_pct_max_worker": 24.6, "peak_fd_inbound_max_worker": 257, "peak_fd_upstream_max_worker": 257, "requests_total_delta": 1196.0, "per_worker": {"35197": {"port": 35796, "peak_rss_kib": 63616.0, "peak_cpu_pct": 24.6, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 255.0, "requests_total_delta": 295.0}, "35198": {"port": 35797, "peak_rss_kib": 64272.0, "peak_cpu_pct": 24.4, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 257.0, "requests_total_delta": 295.0}, "35199": {"port": 35798, "peak_rss_kib": 63792.0, "peak_cpu_pct": 24.0, "peak_fd_inbound": 255, "peak_fd_upstream": 255, "peak_streams_open": 254.0, "requests_total_delta": 299.0}, "35200": {"port": 35799, "peak_rss_kib": 64256.0, "peak_cpu_pct": 23.8, "peak_fd_inbound": 256, "peak_fd_upstream": 256, "peak_streams_open": 255.0, "requests_total_delta": 296.0}}, "marginal_rss_kib_per_stream": 32.24249217663353}

## Drain verdict
- config: drain_grace=600.0s budget_total=600.0s allow_short=False stream_length=100.0s uvicorn_shutdown_timeout=3.0s -> arm A (grace >= stream: zero cuts expected)
- SIGTERM sent to 4 gateway worker(s) at measure+60.0s; fleet fully exited after 100.45s (expected <= 608.0s) -> OK
- per-worker exit: pid 35197: 99.91s; pid 35198: 100.02s; pid 35199: 100.45s; pid 35200: 100.45s
- in-flight streams CUT (had >= 1 byte, then a transport error or no `data: [DONE]`): 0 (PLAN target: 0)
- arrivals REFUSED before a byte (connect refused after the listener closed, 503 draining, timeouts with nothing read): 2328 (expected once the fleet is gone; not a cut)
- error breakdown: {'connect': 1382}; status mix: {'200': 1205, '503': 946}
- verdict: PASS

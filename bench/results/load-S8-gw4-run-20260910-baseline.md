# Scenario S8: Deploy under load
_Drain: client errors must be ZERO when SIGTERM lands mid-stream._

produces: client errors (target 0); drain duration; streams cut (target 0).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [4.921875, 4.5927734375, 4.4716796875], "ts": "2026-09-10 21:26:26Z", "gw_workers": 4, "fake_workers": 4}

- SIGTERM sent to 4 gateway worker(s) at measure+60.0s; fleet fully exited after 60.00s (drain duration = max over workers)
- per-worker exit: pid 71594: still running after 60.0s; pid 71595: still running after 60.0s; pid 71596: still running after 60.0s; pid 71597: still running after 60.0s

### Arm G -- rate 10 rps, 8 workers, wall 360.9s
started=3533 ok=1205 error=2328 bytes=46767424 late=2 peak_inflight(sum over workers)=1050

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     905 |  515.50 |  552.64 |  561.36 |  562.24 |  336.33 |
| total          |     905 | 102855.78 | 110267.05 | 112006.85 | 112182.33 | 66323.12 |
| inter-event    |  120399 |  486.25 |  542.79 |  560.35 |  562.14 |  495.99 |

status: {'200': 1205, '503': 306}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 316, "workers": 4, "peak_streams_open": 1017.0, "peak_tasks": 5089.0, "baseline_tasks": 3690.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 255472.0, "min_rss_kib": 0.0, "peak_cpu_pct": 107.7, "peak_fd_inbound": 1021, "peak_fd_upstream": 1025, "peak_rss_kib_max_worker": 64064.0, "peak_cpu_pct_max_worker": 27.3, "peak_fd_inbound_max_worker": 257, "peak_fd_upstream_max_worker": 257, "requests_total_delta": 469.0, "per_worker": {"71594": {"port": 37160, "peak_rss_kib": 63792.0, "peak_cpu_pct": 27.3, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 255.0, "requests_total_delta": 117.0}, "71595": {"port": 37161, "peak_rss_kib": 64064.0, "peak_cpu_pct": 26.8, "peak_fd_inbound": 255, "peak_fd_upstream": 257, "peak_streams_open": 255.0, "requests_total_delta": 116.0}, "71596": {"port": 37162, "peak_rss_kib": 63888.0, "peak_cpu_pct": 26.9, "peak_fd_inbound": 256, "peak_fd_upstream": 255, "peak_streams_open": 254.0, "requests_total_delta": 116.0}, "71597": {"port": 37163, "peak_rss_kib": 63728.0, "peak_cpu_pct": 26.7, "peak_fd_inbound": 256, "peak_fd_upstream": 256, "peak_streams_open": 256.0, "requests_total_delta": 113.0}}, "marginal_rss_kib_per_stream": 69.32877917544299}

## Drain verdict
- client transport errors (cut streams): 2022 (PLAN target: 0); error breakdown: {'connect': 2022}
- status mix: {'200': 1205, '503': 306}

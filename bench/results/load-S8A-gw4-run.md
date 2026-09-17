# Scenario S8: Deploy under load
_Drain: client errors must be ZERO when SIGTERM lands mid-stream._

produces: client errors (target 0); drain duration; streams cut (target 0).

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [5.37890625, 5.9833984375, 5.73095703125], "ts": "2026-09-17 20:16:07Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": null, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}


### Arm G -- rate 10 rps, 8 workers, wall 360.9s
started=3533 ok=1205 error=2328 bytes=46831378 late=2 peak_inflight(sum over workers)=1052

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    1551 |    4.66 |  545.82 |  560.67 |  562.17 |  198.56 |
| total          |    1551 |    5.32 | 108906.41 | 111867.86 | 112168.40 | 38777.33 |
| inter-event    |  120399 |  510.26 |  551.52 |  561.25 |  562.23 |  496.96 |

admitted only (status 200; refused requests excluded):

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |     599 |  530.88 |  555.90 |  561.69 |  562.28 |  508.64 |
| total          |     599 | 105925.37 | 110917.48 | 112072.74 | 112188.93 | 100400.50 |
| inter-event    |  120399 |  510.26 |  551.52 |  561.25 |  562.23 |  496.96 |

refused before a byte: 2328 (65.9% of 3533 answered): {'503': 952} + 1376 transport errors before a byte

status: {'200': 1205, '503': 952}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 322, "workers": 4, "peak_streams_open": 1019.0, "peak_tasks": 5099.0, "baseline_tasks": 20.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 0.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 257168.0, "min_rss_kib": 128704.0, "peak_cpu_pct": 109.89999999999999, "peak_fd_inbound": 1023, "peak_fd_upstream": 1028, "peak_rss_kib_max_worker": 64448.0, "peak_cpu_pct_max_worker": 27.9, "peak_fd_inbound_max_worker": 257, "peak_fd_upstream_max_worker": 258, "requests_total_delta": 600.0, "per_worker": {"92574": {"port": 27309, "peak_rss_kib": 64320.0, "peak_cpu_pct": 27.9, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 256.0, "requests_total_delta": 295.0}, "92575": {"port": 27310, "peak_rss_kib": 64144.0, "peak_cpu_pct": 27.4, "peak_fd_inbound": 257, "peak_fd_upstream": 257, "peak_streams_open": 256.0, "requests_total_delta": 294.0}, "92576": {"port": 27311, "peak_rss_kib": 64256.0, "peak_cpu_pct": 27.3, "peak_fd_inbound": 256, "peak_fd_upstream": 256, "peak_streams_open": 254.0, "requests_total_delta": 301.0}, "92577": {"port": 27312, "peak_rss_kib": 64448.0, "peak_cpu_pct": 27.3, "peak_fd_inbound": 257, "peak_fd_upstream": 258, "peak_streams_open": 255.0, "requests_total_delta": 295.0}}, "marginal_rss_kib_per_stream": 35.8315879304145}

## Drain verdict
- config: drain_grace=600.0s budget_total=600.0s allow_short=False stream_length=100.0s uvicorn_shutdown_timeout=3.0s -> arm A (grace >= stream: zero cuts expected)
- SIGTERM sent to 4 gateway worker(s) at measure+60.0s; fleet fully exited after 100.61s (expected <= 608.0s) -> OK
- per-worker exit: pid 92574: 100.04s; pid 92575: 100.21s; pid 92577: 100.50s; pid 92576: 100.61s
- in-flight streams CUT (had >= 1 byte, then a transport error or no `data: [DONE]`): 0 (PLAN target: 0)
- arrivals REFUSED before a byte (connect refused after the listener closed, 503 draining, timeouts with nothing read): 2328 (expected once the fleet is gone; not a cut)
- error breakdown: {'connect': 1376}; status mix: {'200': 1205, '503': 952}
- verdict: PASS

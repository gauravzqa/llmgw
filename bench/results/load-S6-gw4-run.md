# Scenario S6: One hot tenant
_Isolation: does tenant B's p99 move when A floods at 5x its cap._

produces: tenant B/C p99 during S6 vs baseline (B alone); A's 429 share.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [2.96435546875, 3.8974609375, 4.62255859375], "ts": "2026-09-10 21:02:14Z", "gw_workers": 4, "fake_workers": 4}
tenants file: /var/folders/51/6jwcbbsx5nz2h5k81ym7qy1r0000gn/T/bench-tenants-S6-34p7duhr.toml

## Per-tenant Arm G (A hot at 5x, B and C quiet), all concurrent

### Arm G -- rate 500 rps, 8 workers, wall 360.3s
started=180196 ok=144791 error=35405 bytes=181913499 late=4431 peak_inflight(sum over workers)=43

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |  150079 |    2.51 |    3.55 |    5.57 |    8.76 |    2.57 |
| total          |  150079 |    3.49 |    5.12 |    7.76 |   11.57 |    3.55 |
| inter-event    |  719988 |    0.00 |    0.16 |    0.50 |    1.04 |    0.05 |

status: {'200': 144791, '429': 35405}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 302, "workers": 4, "peak_streams_open": 7.0, "peak_tasks": 40.0, "baseline_tasks": 19.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 774.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 193408.0, "min_rss_kib": 160560.0, "peak_cpu_pct": 152.6, "peak_fd_inbound": 29, "peak_fd_upstream": 21, "peak_rss_kib_max_worker": 48432.0, "peak_cpu_pct_max_worker": 39.1, "peak_fd_inbound_max_worker": 11, "peak_fd_upstream_max_worker": 6, "requests_total_delta": 216027.0, "per_worker": {"60760": {"port": 48346, "peak_rss_kib": 48432.0, "peak_cpu_pct": 36.9, "peak_fd_inbound": 11, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54009.0}, "60761": {"port": 48347, "peak_rss_kib": 48304.0, "peak_cpu_pct": 37.5, "peak_fd_inbound": 8, "peak_fd_upstream": 5, "peak_streams_open": 4.0, "requests_total_delta": 54007.0}, "60762": {"port": 48348, "peak_rss_kib": 48320.0, "peak_cpu_pct": 39.1, "peak_fd_inbound": 7, "peak_fd_upstream": 6, "peak_streams_open": 3.0, "requests_total_delta": 54004.0}, "60763": {"port": 48349, "peak_rss_kib": 48352.0, "peak_cpu_pct": 39.1, "peak_fd_inbound": 7, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54007.0}}, "marginal_rss_kib_per_stream": -23.679607218575963}

### Arm G -- rate 100 rps, 8 workers, wall 360.7s
started=36107 ok=36107 error=0 bytes=44375503 late=257 peak_inflight(sum over workers)=26

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |    2.73 |    3.98 |    6.36 |    9.93 |    2.98 |
| total          |   30276 |    3.91 |    5.82 |    8.79 |   15.00 |    4.27 |
| inter-event    |  181656 |    0.00 |    0.16 |    0.57 |    1.13 |    0.05 |

status: {'200': 36107}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 303, "workers": 4, "peak_streams_open": 9.0, "peak_tasks": 44.0, "baseline_tasks": 16.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 777.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 193408.0, "min_rss_kib": 179216.0, "peak_cpu_pct": 152.6, "peak_fd_inbound": 32, "peak_fd_upstream": 21, "peak_rss_kib_max_worker": 48432.0, "peak_cpu_pct_max_worker": 39.1, "peak_fd_inbound_max_worker": 10, "peak_fd_upstream_max_worker": 6, "requests_total_delta": 216142.0, "per_worker": {"60760": {"port": 48346, "peak_rss_kib": 48432.0, "peak_cpu_pct": 36.9, "peak_fd_inbound": 9, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54040.0}, "60761": {"port": 48347, "peak_rss_kib": 48304.0, "peak_cpu_pct": 37.5, "peak_fd_inbound": 8, "peak_fd_upstream": 5, "peak_streams_open": 4.0, "requests_total_delta": 54036.0}, "60762": {"port": 48348, "peak_rss_kib": 48320.0, "peak_cpu_pct": 39.1, "peak_fd_inbound": 10, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54036.0}, "60763": {"port": 48349, "peak_rss_kib": 48352.0, "peak_cpu_pct": 39.1, "peak_fd_inbound": 8, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54030.0}}, "marginal_rss_kib_per_stream": 0.6441625595863301}

### Arm G -- rate 100 rps, 8 workers, wall 360.6s
started=36107 ok=36107 error=0 bytes=44375503 late=255 peak_inflight(sum over workers)=25

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |    2.73 |    3.99 |    6.39 |   10.26 |    2.98 |
| total          |   30276 |    3.89 |    5.83 |    8.83 |   14.41 |    4.25 |
| inter-event    |  181656 |    0.00 |    0.15 |    0.53 |    1.08 |    0.05 |

status: {'200': 36107}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 304, "workers": 4, "peak_streams_open": 8.0, "peak_tasks": 42.0, "baseline_tasks": 19.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 774.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 193408.0, "min_rss_kib": 155056.0, "peak_cpu_pct": 152.6, "peak_fd_inbound": 34, "peak_fd_upstream": 21, "peak_rss_kib_max_worker": 48432.0, "peak_cpu_pct_max_worker": 39.1, "peak_fd_inbound_max_worker": 11, "peak_fd_upstream_max_worker": 6, "requests_total_delta": 216033.0, "per_worker": {"60760": {"port": 48346, "peak_rss_kib": 48432.0, "peak_cpu_pct": 36.9, "peak_fd_inbound": 11, "peak_fd_upstream": 6, "peak_streams_open": 3.0, "requests_total_delta": 54009.0}, "60761": {"port": 48347, "peak_rss_kib": 48304.0, "peak_cpu_pct": 37.5, "peak_fd_inbound": 9, "peak_fd_upstream": 5, "peak_streams_open": 4.0, "requests_total_delta": 54008.0}, "60762": {"port": 48348, "peak_rss_kib": 48320.0, "peak_cpu_pct": 39.1, "peak_fd_inbound": 10, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54007.0}, "60763": {"port": 48349, "peak_rss_kib": 48352.0, "peak_cpu_pct": 39.1, "peak_fd_inbound": 9, "peak_fd_upstream": 6, "peak_streams_open": 4.0, "requests_total_delta": 54009.0}}, "marginal_rss_kib_per_stream": -1.8868896009640403}

## Baseline: tenant B alone

### Arm G -- rate 100 rps, 8 workers, wall 360.5s
started=36107 ok=36107 error=0 bytes=44375503 late=384 peak_inflight(sum over workers)=25

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |    2.88 |    3.86 |    5.00 |    7.77 |    2.99 |
| total          |   30276 |    3.80 |    5.05 |    6.75 |    9.81 |    3.96 |
| inter-event    |  181656 |    0.00 |    0.18 |    0.32 |    0.99 |    0.05 |

status: {'200': 36107}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 319, "workers": 4, "peak_streams_open": 3.0, "peak_tasks": 26.0, "baseline_tasks": 16.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 391.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 193408.0, "min_rss_kib": 193408.0, "peak_cpu_pct": 32.8, "peak_fd_inbound": 7, "peak_fd_upstream": 16, "peak_rss_kib_max_worker": 48432.0, "peak_cpu_pct_max_worker": 8.6, "peak_fd_inbound_max_worker": 3, "peak_fd_upstream_max_worker": 4, "requests_total_delta": 36079.0, "per_worker": {"60760": {"port": 48346, "peak_rss_kib": 48432.0, "peak_cpu_pct": 8.6, "peak_fd_inbound": 3, "peak_fd_upstream": 4, "peak_streams_open": 2.0, "requests_total_delta": 9019.0}, "60761": {"port": 48347, "peak_rss_kib": 48304.0, "peak_cpu_pct": 8.4, "peak_fd_inbound": 3, "peak_fd_upstream": 4, "peak_streams_open": 2.0, "requests_total_delta": 9019.0}, "60762": {"port": 48348, "peak_rss_kib": 48320.0, "peak_cpu_pct": 8.0, "peak_fd_inbound": 3, "peak_fd_upstream": 4, "peak_streams_open": 1.0, "requests_total_delta": 9020.0}, "60763": {"port": 48349, "peak_rss_kib": 48352.0, "peak_cpu_pct": 8.4, "peak_fd_inbound": 3, "peak_fd_upstream": 4, "peak_streams_open": 2.0, "requests_total_delta": 9021.0}}, "marginal_rss_kib_per_stream": 0.0}

## Isolation verdict
- tenant B p99 TTFE: with hot A = 6.36 ms, baseline (B alone) = 5.00 ms, delta = +1.37 ms
- hot tenant A status mix (429 share shows shedding): {'200': 144791, '429': 35405}

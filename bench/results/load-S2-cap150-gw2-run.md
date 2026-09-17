# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [7.3974609375, 8.2373046875, 7.14306640625], "ts": "2026-09-17 19:01:54Z", "gw_workers": 2, "fake_workers": 2, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": 150, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=2 fake_workers=2

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 386.9s
started=36107 ok=36107 error=0 bytes=6963812662 late=198 peak_inflight(sum over workers)=2988

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |   29.99 |   34.31 |   56.30 |  112.92 |   30.96 |
| total          |   30276 | 26607.25 | 27861.21 | 28151.40 | 28180.58 | 26225.93 |
| inter-event    | 30306276 |   26.61 |   27.99 |   39.40 |   69.06 |   26.17 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 377.8s
started=36107 ok=4200 error=31907 bytes=814025575 late=341 peak_inflight(sum over workers)=424

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |    4.15 |   26.37 |   39.39 |   48.15 |    8.05 |
| total          |   30276 |    4.43 | 25358.72 | 27887.70 | 28154.08 | 2910.46 |
| inter-event    | 3303300 |   26.57 |   29.89 |   34.12 |   40.95 |   26.60 |

admitted only (status 200; refused requests excluded):

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |    3300 |   30.99 |   39.20 |   47.78 |   59.22 |   32.34 |
| total          |    3300 | 26607.25 | 27861.21 | 28151.40 | 28180.58 | 26658.50 |
| inter-event    | 3303300 |   26.57 |   29.89 |   34.12 |   40.95 |   26.60 |

refused before a byte: 31907 (88.4% of 36107 answered): {'503': 31907}

status: {'200': 4200, '503': 31907}

gateway samples (2 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 344, "workers": 2, "peak_streams_open": 300.0, "peak_tasks": 1511.0, "baseline_tasks": 18.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 629.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 114192.0, "min_rss_kib": 47904.0, "peak_cpu_pct": 122.4, "peak_fd_inbound": 307, "peak_fd_upstream": 300, "peak_rss_kib_max_worker": 57136.0, "peak_cpu_pct_max_worker": 62.1, "peak_fd_inbound_max_worker": 155, "peak_fd_upstream_max_worker": 150, "requests_total_delta": 4128.0, "per_worker": {"79136": {"port": 59301, "peak_rss_kib": 57136.0, "peak_cpu_pct": 61.0, "peak_fd_inbound": 154, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 2066.0}, "79137": {"port": 59302, "peak_rss_kib": 57056.0, "peak_cpu_pct": 62.1, "peak_fd_inbound": 155, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 2062.0}}, "marginal_rss_kib_per_stream": 20.36660759043433}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   29.99 |    4.15 |  -25.84 |
| p90 |   34.31 |   26.37 |   -7.94 |
| p99 |   56.30 |   39.39 |  -16.91 |
| p99.9 |  112.92 |   48.15 |  -64.77 |

added first-event latency (gateway path), matched-quantile, admitted only:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   29.99 |   30.99 |   +1.00 |
| p90 |   34.31 |   39.20 |   +4.89 |
| p99 |   56.30 |   47.78 |   -8.52 |
| p99.9 |  112.92 |   59.22 |  -53.70 |
shed (refused before a byte): Arm D 0.0%, Arm G 88.4%

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=4200 requests
- CALIBRATION: Arm D p99 TTFE=56.30 ms >= Arm G p99 TTFE=47.78 ms -> INVALID (client is the bottleneck, not the gateway) (Arm G 88.4% refused by the cap; compared admitted only)
## Saturation / instrument-is-a-SUT flags
- Arm G: 31907 errors (88.4%): {}

# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [10.95654296875, 8.947265625, 9.099609375], "ts": "2026-09-18 12:21:48Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": 300, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 386.4s
started=36107 ok=36107 error=0 bytes=6963812662 late=266 peak_inflight(sum over workers)=2928

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |   29.40 |   33.26 |   52.56 |   82.37 |   30.17 |
| total          |   30276 | 26607.25 | 27861.21 | 28151.40 | 28180.58 | 25662.41 |
| inter-event    | 30306276 |   26.21 |   27.88 |   41.80 |   53.98 |   25.61 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 389.6s
started=36107 ok=15224 error=20883 bytes=2938789855 late=173 peak_inflight(sum over workers)=1456

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |  104.40 | 2527.91 | 6192.93 | 8764.00 |  684.76 |
| total          |   30276 |  105.06 | 29130.75 | 34333.62 | 36488.42 | 11484.67 |
| inter-event    | 12484472 |   32.23 |   43.34 |  132.86 |  256.56 |   25.77 |

admitted only (status 200; refused requests excluded):

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   12472 |  384.29 | 4510.44 | 7314.68 | 9571.16 | 1528.54 |
| total          |   12472 | 27302.68 | 31264.27 | 35107.16 | 38406.97 | 27744.75 |
| inter-event    | 12484472 |   32.23 |   43.34 |  132.86 |  256.56 |   25.77 |

refused before a byte: 20883 (57.8% of 36107 answered): {'502': 48, '503': 20607, '504': 228}

status: {'200': 15224, '503': 20607, '504': 228, '502': 48}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 290, "workers": 4, "peak_streams_open": 1200.0, "peak_tasks": 6025.0, "baseline_tasks": 136.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 42974.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 288272.0, "min_rss_kib": 124240.0, "peak_cpu_pct": 400.0, "peak_fd_inbound": 1244, "peak_fd_upstream": 1201, "peak_rss_kib_max_worker": 72784.0, "peak_cpu_pct_max_worker": 100.0, "peak_fd_inbound_max_worker": 318, "peak_fd_upstream_max_worker": 301, "requests_total_delta": 15403.0, "per_worker": {"49917": {"port": 51961, "peak_rss_kib": 71680.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 308, "peak_fd_upstream": 300, "peak_streams_open": 300.0, "requests_total_delta": 3859.0}, "49918": {"port": 51962, "peak_rss_kib": 72672.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 312, "peak_fd_upstream": 301, "peak_streams_open": 300.0, "requests_total_delta": 3832.0}, "49919": {"port": 51963, "peak_rss_kib": 72784.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 318, "peak_fd_upstream": 300, "peak_streams_open": 300.0, "requests_total_delta": 3825.0}, "49920": {"port": 51964, "peak_rss_kib": 72192.0, "peak_cpu_pct": 100.0, "peak_fd_inbound": 312, "peak_fd_upstream": 300, "peak_streams_open": 300.0, "requests_total_delta": 3887.0}}, "marginal_rss_kib_per_stream": 14.486532701396234}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   29.40 |  104.40 |  +75.00 |
| p90 |   33.26 | 2527.91 | +2494.65 |
| p99 |   52.56 | 6192.93 | +6140.37 |
| p99.9 |   82.37 | 8764.00 | +8681.63 |

added first-event latency (gateway path), matched-quantile, admitted only:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   29.40 |  384.29 | +354.88 |
| p90 |   33.26 | 4510.44 | +4477.18 |
| p99 |   52.56 | 7314.68 | +7262.12 |
| p99.9 |   82.37 | 9571.16 | +9488.80 |
shed (refused before a byte): Arm D 0.0%, Arm G 57.8%

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=15230 requests
- CALIBRATION: Arm D p99 TTFE=52.56 ms < Arm G p99 TTFE=7314.68 ms -> CALIBRATED (client faster than gateway path, number is honest) (Arm G 57.8% refused by the cap; compared admitted only)
## Saturation / instrument-is-a-SUT flags
- Arm G: 20883 errors (57.8%): {}

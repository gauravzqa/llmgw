# Scenario S2: Typical streaming
_Overhead at ~2,500 concurrent streams (100 rps x 25 s)._

produces: added inter-event jitter p99; CPU per core at steady state; streams_open ~2,500; tasks at load.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [3.48974609375, 4.42578125, 6.23583984375], "ts": "2026-09-15 18:03:57Z", "gw_workers": 4, "fake_workers": 4, "gw_budget_total_s": 600.0, "gw_drain_grace_s": 600.0, "gw_drain_allow_short": false, "gw_max_streams": 150, "gw_drain_arm": "A (grace >= total: zero cuts expected)"}

config: rate=100.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 100 rps, 8 workers, wall 386.1s
started=36107 ok=36107 error=0 bytes=6963812662 late=201 peak_inflight(sum over workers)=2919

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |   28.98 |   31.59 |   46.47 |   62.45 |   29.46 |
| total          |   30276 | 26607.25 | 27861.21 | 28151.40 | 28180.58 | 25501.04 |
| inter-event    | 30306276 |   26.17 |   27.88 |   35.70 |   46.34 |   25.45 |

status: {'200': 36107}

### Arm G -- rate 100 rps, 8 workers, wall 370.3s
started=36107 ok=8400 error=27707 bytes=1623537775 late=193 peak_inflight(sum over workers)=751

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   30276 |    4.20 |   30.95 |   39.46 |   52.83 |   10.23 |
| total          |   30276 |    4.53 | 26733.98 | 28035.37 | 28168.95 | 5648.94 |
| inter-event    | 6606600 |   26.15 |   28.00 |   37.07 |   47.28 |   25.84 |

status: {'200': 8400, '503': 27707}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 333, "workers": 4, "peak_streams_open": 600.0, "peak_tasks": 3018.0, "baseline_tasks": 121.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 386.0, "capture_dropped": 0, "breaker_max_state": 0, "peak_rss_kib": 236048.0, "min_rss_kib": 155632.0, "peak_cpu_pct": 268.0, "peak_fd_inbound": 607, "peak_fd_upstream": 600, "peak_rss_kib_max_worker": 59152.0, "peak_cpu_pct_max_worker": 69.1, "peak_fd_inbound_max_worker": 154, "peak_fd_upstream_max_worker": 150, "requests_total_delta": 8300.0, "per_worker": {"55580": {"port": 31684, "peak_rss_kib": 59152.0, "peak_cpu_pct": 67.6, "peak_fd_inbound": 153, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 2073.0}, "55581": {"port": 31685, "peak_rss_kib": 58880.0, "peak_cpu_pct": 67.6, "peak_fd_inbound": 154, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 2075.0}, "55582": {"port": 31686, "peak_rss_kib": 58928.0, "peak_cpu_pct": 68.2, "peak_fd_inbound": 153, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 2077.0}, "55583": {"port": 31687, "peak_rss_kib": 59088.0, "peak_cpu_pct": 69.1, "peak_fd_inbound": 153, "peak_fd_upstream": 150, "peak_streams_open": 150.0, "requests_total_delta": 2075.0}}, "marginal_rss_kib_per_stream": 33.04686058651135}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |   28.98 |    4.20 |  -24.78 |
| p90 |   31.59 |   30.95 |   -0.64 |
| p99 |   46.47 |   39.46 |   -7.01 |
| p99.9 |   62.45 |   52.83 |   -9.62 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=36107 G=8400 requests
- CALIBRATION: Arm D p99 TTFE=46.47 ms >= Arm G p99 TTFE=39.46 ms -> INVALID (client is the bottleneck, not the gateway)
## Saturation / instrument-is-a-SUT flags
- Arm G: 27707 errors (76.7%): {}

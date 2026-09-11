# Scenario S7: Provider killed mid-stream
_Failure and recovery under load: breaker opens on kill, closes on restart._

produces: in-flight at kill; native endings; kill->breaker-open latency; restart->breaker-closed latency; deadline overruns.

env: {"python": "3.11.15", "machine": "arm64", "cores": 16, "ulimit_nofile": 1048576, "loadavg": [3.1748046875, 3.9287109375, 4.37353515625], "ts": "2026-09-10 21:14:15Z", "gw_workers": 4, "fake_workers": 4}

config: rate=50.0 rps, workers=8, warm=60.0s measure=300.0s repeats=1 gw_workers=4 fake_workers=4

## Median run (1 of 1, by Arm G p99 TTFE)

### Arm D -- rate 50 rps, 8 workers, wall 365.1s
started=17884 ok=14799 error=3085 bytes=146719299 late=89 peak_inflight(sum over workers)=407

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   14926 |  104.36 |  110.61 |  112.07 |  117.66 |   82.14 |
| total          |   14926 | 5229.56 | 5542.34 | 5615.25 | 5622.60 | 3991.78 |
| inter-event    |  603891 |  105.22 |  110.77 |  112.06 |  112.19 |   96.62 |

status: {'200': 14799, '500': 3085}

### Arm G -- rate 50 rps, 8 workers, wall 365.2s
started=17884 ok=14498 error=3386 bytes=143853006 late=65 peak_inflight(sum over workers)=407

| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |
|----------------|---------|---------|---------|---------|----------|---------|
| ttfe           |   14926 |  104.16 |  110.57 |  112.06 |  117.92 |   81.29 |
| total          |   14926 | 5219.93 | 5540.30 | 5615.05 | 5622.58 | 3888.87 |
| inter-event    |  588540 |  104.67 |  110.66 |  112.05 |  112.19 |   96.53 |

status: {'200': 14498, '500': 40, '503': 3346}

gateway samples (4 workers; rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest single worker, per_worker each worker's own peaks): {"n_samples": 322, "workers": 4, "peak_streams_open": 303.0, "peak_tasks": 1521.0, "baseline_tasks": 31.0, "peak_pump_buffered_bytes": 0.0, "peak_capture_queue_bytes": 385.0, "capture_dropped": 0, "breaker_max_state": 2.0, "peak_rss_kib": 227504.0, "min_rss_kib": 155328.0, "peak_cpu_pct": 74.0, "peak_fd_inbound": 299, "peak_fd_upstream": 311, "peak_rss_kib_max_worker": 57104.0, "peak_cpu_pct_max_worker": 20.7, "peak_fd_inbound_max_worker": 76, "peak_fd_upstream_max_worker": 78, "requests_total_delta": 17856.0, "per_worker": {"67523": {"port": 46347, "peak_rss_kib": 57040.0, "peak_cpu_pct": 18.4, "peak_fd_inbound": 75, "peak_fd_upstream": 78, "peak_streams_open": 75.0, "requests_total_delta": 4467.0}, "67524": {"port": 46348, "peak_rss_kib": 56688.0, "peak_cpu_pct": 20.3, "peak_fd_inbound": 76, "peak_fd_upstream": 78, "peak_streams_open": 77.0, "requests_total_delta": 4463.0}, "67525": {"port": 46349, "peak_rss_kib": 56672.0, "peak_cpu_pct": 20.7, "peak_fd_inbound": 76, "peak_fd_upstream": 78, "peak_streams_open": 76.0, "requests_total_delta": 4462.0}, "67526": {"port": 46350, "peak_rss_kib": 57104.0, "peak_cpu_pct": 19.2, "peak_fd_inbound": 74, "peak_fd_upstream": 77, "peak_streams_open": 75.0, "requests_total_delta": 4464.0}}, "marginal_rss_kib_per_stream": -9.602261757772153}

added first-event latency (gateway path), matched-quantile:
| q | Arm D ms | Arm G ms | added ms |
|---|----------|----------|----------|
| p50 |  104.36 |  104.16 |   -0.20 |
| p90 |  110.61 |  110.57 |   -0.04 |
| p99 |  112.07 |  112.06 |   -0.00 |
| p99.9 |  117.66 |  117.92 |   +0.27 |

## Instrument verdicts
- Arm G X-Gw-Attempts=1 Served-By='fake-openai/fake.echo'
- Arm D carries X-Gw-*: False
- fake saw D=17884 G=14538 requests
- CALIBRATION: Arm D p99 TTFE=112.07 ms >= Arm G p99 TTFE=112.06 ms -> INVALID (client is the bottleneck, not the gateway)
## Saturation / instrument-is-a-SUT flags
- Arm D: 3085 errors (17.3%): {}
- Arm G: 3386 errors (18.9%): {}

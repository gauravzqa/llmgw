"""bench/load.py -- the scale load generator, itself a system under test.

The hazard this file is built around: a Python load
generator saturates before the gateway does, and then you publish the client's
ceiling as the gateway's. Every design choice here is a defence against that
lie, and the defences are asserted, not hoped for.

  1. OPEN-MODEL ARRIVALS (not closed loop). Each worker fires Poisson arrivals
     at a target rate -- exponential inter-arrival gaps scheduled against an
     ABSOLUTE clock -- and does not wait for a response before scheduling the
     next arrival. A closed request->response->repeat loop silently throttles
     the offered load the instant the system slows; that is coordinated
     omission, the most common way a load test hides overload. When the gateway
     slows here, arrivals keep coming, the in-flight count climbs, and the
     latency we measure INCLUDES that climb. If a worker cannot keep its
     schedule (it wakes late because its own event loop is saturated) we count
     the late arrivals and report them -- that is the instrument-is-a-SUT
     signal.

  2. MULTI-PROCESS generation. One OS process per worker, one asyncio loop
     each, lambda split across them, so the generator is not single-GIL-bound.
     Workers report results back over a queue; the parent merges them.

  3. TWO ARMS. Arm D = client -> fake (calibrates the client). Arm G = client
     -> gateway -> fake (the measurement). D runs before G. We report BOTH
     arms' absolute numbers and the paired delta where it is computable, and we
     NEVER subtract or average percentiles across arms or across workers.

  4. MERGEABLE HISTOGRAMS. Per-request TTFE, total duration, and inter-event
     gaps go into fixed log-scaled bucket histograms that merge across workers
     by SUMMING bucket counts; percentiles come from the MERGED histogram with
     in-bucket interpolation. Averaging per-worker p99s would be a lie about the
     tail (see bench/test_histogram.py, which asserts exactly that inequality).

  5. RESOURCE + GATEWAY SAMPLING. During Arm G we scrape the gateway /metrics
     (tasks, streams_open, pump_buffered_bytes, breaker_state, capture queue)
     and sample the gateway process RSS/CPU (ps) and fd count split inbound vs
     upstream (lsof). These are the reported numbers.

  6. A MULTI-PROCESS GATEWAY FLEET (`--gw-workers N`, default 1). The
     preliminary pass (bench/results/preliminary-S1S2S3.md) pinned ONE gateway
     process at 100% CPU under S2: a single GIL-bound process is not the
     deployment shape, a fleet of one-per-core is. With N > 1 the bench
     launches N `bench._gwproc` processes, EACH ON ITS OWN PORT, and the
     generator round-robins its connections across them exactly as a load
     balancer would. Own-port-per-worker (not SO_REUSEPORT on one port) is a
     deliberate choice: the gateway's Prometheus registry is per-process, so a
     shared port would hand each /metrics scrape one random worker's counters.
     With N ports the sampler scrapes every worker and SUMS, so `streams_open`,
     `requests_total` and RSS stay honest fleet totals, and it needs no change
     to the gateway itself. The fake gets the same treatment via
     `--fake-workers N` (passed through as `fakes.upstream --workers N`).

This module builds and validates the instrument. It does NOT run the full
warm60/measure300/x3 campaign (that needs a quiet machine and a raised
`ulimit -n`). `--smoke` runs tiny, for self-check.

  python -m bench.load --scenario S1 --smoke
  python -m bench.load --scenario S1 --warm 60 --measure 300 --repeats 3 --workers 8
  python -m bench.load --scenario S2 --gw-workers 8 --fake-workers 4 --workers 8
"""

from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import os
import platform
import random
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import httpx

from bench.scenarios import SCENARIOS, Scenario

# --------------------------------------------------------------------------
# Mergeable log-scaled histogram
# --------------------------------------------------------------------------
#
# Fixed bucket boundaries so two histograms from two worker processes are
# summable bucket-for-bucket. Log-scaled so a single instrument spans 10 us to
# 300 s at roughly constant relative resolution. Values are stored in SECONDS;
# callers present milliseconds.

HIST_MIN = 1e-5          # 10 microseconds
HIST_MAX = 300.0         # 5 minutes
BUCKETS_PER_DECADE = 20  # ~12% relative width per bucket


def _n_buckets() -> int:
    decades = math.log10(HIST_MAX / HIST_MIN)
    return int(math.ceil(decades * BUCKETS_PER_DECADE)) + 2  # + underflow/overflow


@dataclass
class LogHistogram:
    """Log-scaled fixed-bucket histogram. Merge by summing counts; percentile
    by in-bucket log-linear interpolation.

    Bucket i (1 <= i <= N) covers [edge(i-1), edge(i)) where
    edge(k) = HIST_MIN * 10 ** (k / BUCKETS_PER_DECADE). Bucket 0 is the
    underflow (< HIST_MIN), bucket N+1 the overflow (>= HIST_MAX)."""

    counts: list[int] = field(default_factory=lambda: [0] * _n_buckets())
    total: int = 0
    sum_s: float = 0.0
    min_s: float = math.inf
    max_s: float = 0.0

    @staticmethod
    def _edge(k: int) -> float:
        return HIST_MIN * (10.0 ** (k / BUCKETS_PER_DECADE))

    def _index(self, v: float) -> int:
        if v < HIST_MIN:
            return 0
        if v >= HIST_MAX:
            return len(self.counts) - 1
        return 1 + int(math.floor(math.log10(v / HIST_MIN) * BUCKETS_PER_DECADE))

    def record(self, v: float) -> None:
        if v < 0:
            return
        self.counts[self._index(v)] += 1
        self.total += 1
        self.sum_s += v
        if v < self.min_s:
            self.min_s = v
        if v > self.max_s:
            self.max_s = v

    def merge(self, other: LogHistogram) -> None:
        if len(self.counts) != len(other.counts):
            raise ValueError("histogram bucket layouts differ; cannot merge")
        for i, c in enumerate(other.counts):
            self.counts[i] += c
        self.total += other.total
        self.sum_s += other.sum_s
        self.min_s = min(self.min_s, other.min_s)
        self.max_s = max(self.max_s, other.max_s)

    def percentile(self, q: float) -> float:
        """q in [0, 100]. Log-linear interpolation inside the chosen bucket.
        Returns seconds. NaN if empty."""
        if self.total == 0:
            return float("nan")
        target = q / 100.0 * self.total
        cum = 0
        n = len(self.counts)
        for i in range(n):
            c = self.counts[i]
            if c == 0:
                continue
            if cum + c >= target:
                if i == 0:
                    return self.min_s if self.min_s != math.inf else HIST_MIN
                if i == n - 1:
                    return max(self.max_s, HIST_MAX)
                lo = self._edge(i - 1)
                hi = self._edge(i)
                # fraction of THIS bucket we need to cross
                frac = (target - cum) / c
                # log-linear position inside [lo, hi)
                return lo * (hi / lo) ** frac
            cum += c
        return self.max_s

    def to_dict(self) -> dict:
        return {
            "counts": self.counts,
            "total": self.total,
            "sum_s": self.sum_s,
            "min_s": self.min_s if self.min_s != math.inf else None,
            "max_s": self.max_s,
        }

    @classmethod
    def from_dict(cls, d: dict) -> LogHistogram:
        h = cls(counts=list(d["counts"]), total=d["total"], sum_s=d["sum_s"],
                min_s=d["min_s"] if d["min_s"] is not None else math.inf,
                max_s=d["max_s"])
        return h

    def quantile_row_ms(self) -> dict[str, float]:
        return {
            "n": self.total,
            "p50": self.percentile(50) * 1e3,
            "p90": self.percentile(90) * 1e3,
            "p99": self.percentile(99) * 1e3,
            "p999": self.percentile(99.9) * 1e3,
            "mean": (self.sum_s / self.total * 1e3) if self.total else float("nan"),
        }


def merge_all(hists: list[LogHistogram]) -> LogHistogram:
    out = LogHistogram()
    for h in hists:
        out.merge(h)
    return out


# --------------------------------------------------------------------------
# Worker: one process, one asyncio loop, open-model Poisson arrivals
# --------------------------------------------------------------------------


@dataclass
class WorkerSpec:
    """Everything a worker needs, picklable across the spawn boundary."""
    worker_id: int
    arm: str               # "D" or "G"
    url: str               # full POST url (fake direct, or gateway)
    body: dict
    headers: dict[str, str]
    rate: float            # per-worker arrivals/sec (lambda_i)
    warm_s: float
    measure_s: float
    streaming: bool
    assert_gw: bool
    read_bps: int | None   # S5: throttle client read to this many bytes/sec
    flips: list            # S7: [(start_s, end_s, mode), ...] relative to measure start
    timeout_s: float
    seed: int
    # Multi-worker gateway fleet: the full list of equivalent POST urls, one
    # per gateway worker. Empty means "just `url`" (Arm D, or a 1-worker
    # fleet), which is exactly last night's behaviour. Requests round-robin
    # across these, starting at an offset of worker_id so N generator workers
    # do not all begin on gateway worker 0.
    urls: list[str] = field(default_factory=list)


@dataclass
class WorkerResult:
    worker_id: int
    arm: str
    started: int
    ok: int
    error: int
    errors_by_kind: dict
    status_counts: dict
    late_arrivals: int
    max_late_s: float
    peak_inflight: int
    bytes_total: int
    gw_headers: dict | None
    d_has_x_gw: bool | None
    ttfe: dict
    total: dict
    gaps: dict


def _classify_error(exc: BaseException) -> str:
    name = type(exc).__name__
    if isinstance(exc, httpx.ConnectError):
        return "connect"
    if isinstance(exc, httpx.ReadTimeout | httpx.ConnectTimeout | httpx.PoolTimeout):
        return "timeout"
    if isinstance(exc, httpx.RemoteProtocolError):
        return "protocol"
    if isinstance(exc, OSError):
        return f"os:{getattr(exc, 'errno', '?')}"
    return name


def _run_worker(spec: WorkerSpec, out_q: mp.Queue) -> None:
    import asyncio
    try:
        res = asyncio.run(_worker_main(spec))
    except Exception as exc:  # never let a worker die silently
        res = WorkerResult(
            worker_id=spec.worker_id, arm=spec.arm, started=0, ok=0, error=0,
            errors_by_kind={f"worker-crash:{_classify_error(exc)}": 1},
            status_counts={}, late_arrivals=0, max_late_s=0.0, peak_inflight=0,
            bytes_total=0, gw_headers=None, d_has_x_gw=None,
            ttfe=LogHistogram().to_dict(), total=LogHistogram().to_dict(),
            gaps=LogHistogram().to_dict(),
        )
    out_q.put(res)


async def _worker_main(spec: WorkerSpec) -> WorkerResult:
    import asyncio

    rng = random.Random(spec.seed)
    ttfe = LogHistogram()
    total = LogHistogram()
    gaps = LogHistogram()
    counters = dict(started=0, ok=0, error=0, late=0, bytes=0)
    errors_by_kind: dict[str, int] = {}
    status_counts: dict[str, int] = {}
    gw_headers_holder: dict = {"seen": None, "d_has_x_gw": None}
    inflight = 0
    peak_inflight = 0
    max_late = 0.0

    limits = httpx.Limits(max_connections=None, max_keepalive_connections=0)
    # http2 matches the overhead bench's client; keepalive off so each arrival
    # opens its own connection (the open-model honest cost, and what lets the
    # fd sampler see concurrent inbound sockets rather than a reused few).
    client = httpx.AsyncClient(http2=True, timeout=spec.timeout_s, limits=limits)

    def mode_for(elapsed_measure: float) -> str | None:
        for (s, e, mode) in spec.flips:
            if s <= elapsed_measure < e:
                return mode
        return None

    # The gateway fleet, round-robined per request. Keepalive is off (see
    # `limits`), so every arrival opens its own connection and the round-robin
    # is a spread of CONNECTIONS across workers, like an L4 balancer.
    urls = spec.urls or [spec.url]
    rr = [spec.worker_id % len(urls)]

    def next_url() -> str:
        u = urls[rr[0]]
        rr[0] = (rr[0] + 1) % len(urls)
        return u

    async def do_request(measuring: bool, elapsed_measure: float) -> None:
        nonlocal inflight, peak_inflight
        inflight += 1
        peak_inflight = max(peak_inflight, inflight)
        hdrs = dict(spec.headers)
        flip = mode_for(elapsed_measure)
        if flip is not None:
            hdrs["X-Fake-Mode"] = flip
        url = next_url()
        t0 = time.perf_counter()
        try:
            if spec.streaming:
                first: float | None = None
                last: float | None = None
                nbytes = 0
                read_budget_t = t0
                async with client.stream("POST", url, json=spec.body,
                                         headers=hdrs) as r:
                    status_counts[str(r.status_code)] = \
                        status_counts.get(str(r.status_code), 0) + 1
                    if spec.assert_gw and gw_headers_holder["seen"] is None \
                            and r.status_code == 200:
                        gw_headers_holder["seen"] = dict(r.headers)
                    if spec.arm == "D" and gw_headers_holder["d_has_x_gw"] is None:
                        gw_headers_holder["d_has_x_gw"] = any(
                            k.lower().startswith("x-gw-") for k in r.headers)
                    async for chunk in r.aiter_bytes():
                        if not chunk:
                            continue
                        now = time.perf_counter()
                        nbytes += len(chunk)
                        if first is None:
                            first = now
                            if measuring:
                                ttfe.record(first - t0)
                        else:
                            if measuring:
                                gaps.record(now - last)
                        last = now
                        # S5: a genuinely slow reader. Pace the read loop to
                        # read_bps so backpressure propagates to the gateway
                        # pump, rather than draining the socket at full speed.
                        if spec.read_bps:
                            read_budget_t += len(chunk) / spec.read_bps
                            sleep = read_budget_t - time.perf_counter()
                            if sleep > 0:
                                await asyncio.sleep(sleep)
                if r.status_code == 200:
                    counters["ok"] += 1
                else:
                    counters["error"] += 1
                counters["bytes"] += nbytes
                if measuring:
                    total.record(time.perf_counter() - t0)
            else:
                r = await client.post(url, json=spec.body, headers=hdrs)
                _ = r.content
                dt = time.perf_counter() - t0
                status_counts[str(r.status_code)] = \
                    status_counts.get(str(r.status_code), 0) + 1
                if spec.assert_gw and gw_headers_holder["seen"] is None \
                        and r.status_code == 200:
                    gw_headers_holder["seen"] = dict(r.headers)
                if spec.arm == "D" and gw_headers_holder["d_has_x_gw"] is None:
                    gw_headers_holder["d_has_x_gw"] = any(
                        k.lower().startswith("x-gw-") for k in r.headers)
                if r.status_code == 200:
                    counters["ok"] += 1
                    if measuring:
                        ttfe.record(dt)
                        total.record(dt)
                else:
                    counters["error"] += 1
                counters["bytes"] += len(r.content)
        except Exception as exc:
            counters["error"] += 1
            kind = _classify_error(exc)
            errors_by_kind[kind] = errors_by_kind.get(kind, 0) + 1
        finally:
            inflight -= 1

    # ---- open-model arrival loop, absolute-clock scheduled ----
    loop = asyncio.get_running_loop()
    start = loop.time()
    end_warm = start + spec.warm_s
    end_measure = end_warm + spec.measure_s
    next_t = start
    tasks: list = []
    while True:
        # exponential inter-arrival; absolute schedule defeats coordinated omission
        next_t += rng.expovariate(spec.rate) if spec.rate > 0 else 1e9
        delay = next_t - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        else:
            # We woke late: the generator could not keep its own schedule.
            counters["late"] += 1
            if -delay > max_late:
                max_late = -delay
        now = loop.time()
        if now >= end_measure:
            break
        measuring = now >= end_warm
        elapsed_measure = max(0.0, now - end_warm)
        counters["started"] += 1
        t = asyncio.ensure_future(do_request(measuring, elapsed_measure))
        tasks.append(t)
        # prune finished tasks so the list does not grow without bound
        if len(tasks) > 4096:
            tasks = [x for x in tasks if not x.done()]

    # drain in-flight (bounded) so late completions still count
    pending = [x for x in tasks if not x.done()]
    if pending:
        try:
            await asyncio.wait(pending, timeout=spec.timeout_s + 5.0)
        except Exception:
            pass
    await client.aclose()

    return WorkerResult(
        worker_id=spec.worker_id, arm=spec.arm,
        started=counters["started"], ok=counters["ok"], error=counters["error"],
        errors_by_kind=errors_by_kind, status_counts=status_counts,
        late_arrivals=counters["late"], max_late_s=max_late,
        peak_inflight=peak_inflight, bytes_total=counters["bytes"],
        gw_headers=gw_headers_holder["seen"], d_has_x_gw=gw_headers_holder["d_has_x_gw"],
        ttfe=ttfe.to_dict(), total=total.to_dict(), gaps=gaps.to_dict(),
    )


# --------------------------------------------------------------------------
# Resource + gateway sampler (parent-side thread; samples the gateway by pid)
# --------------------------------------------------------------------------

_METRIC_NAMES = (
    "llmgw_tasks", "llmgw_streams_open", "llmgw_pump_buffered_bytes",
    "llmgw_breaker_state", "llmgw_capture_queue_bytes",
    # The real capture-drop family (src/llmgw/metrics.py) is the labelled
    # counter `llmgw_capture_dropped_total{reason}`; an earlier revision
    # scraped a non-existent `llmgw_capture_queue_dropped` and so always
    # reported 0 drops.
    "llmgw_capture_dropped_total",
    # Per-worker request share: summed over {surface,outcome,code}. This is
    # how a multi-worker run proves the round-robin actually spread the load.
    "llmgw_requests_total",
)


def parse_metrics(text: str) -> dict[str, float]:
    """Prometheus text -> {base_name: aggregated value}. For labelled families
    (breaker_state) we keep the MAX across labels (any target open -> open)."""
    out: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        try:
            left, val = line.rsplit(" ", 1)
            v = float(val)
        except ValueError:
            continue
        name = left.split("{", 1)[0]
        if name not in _METRIC_NAMES:
            continue
        if name == "llmgw_breaker_state":
            out[name] = max(out.get(name, 0.0), v)
        else:
            out[name] = out.get(name, 0.0) + v
    return out


def sample_rss_cpu_many(pids: list[int]) -> dict[int, tuple[float, float]]:
    """{pid: (RSS KiB, %CPU)} via ONE ps call. A pid that is gone is absent."""
    out: dict[int, tuple[float, float]] = {}
    if not pids:
        return out
    try:
        r = subprocess.run(["ps", "-o", "pid=,rss=,%cpu=", "-p",
                            ",".join(str(p) for p in pids)],
                           capture_output=True, text=True, timeout=5)
        for line in r.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 3:
                try:
                    out[int(parts[0])] = (float(parts[1]), float(parts[2]))
                except ValueError:
                    continue
    except Exception:
        pass
    return out


def sample_rss_cpu(pid: int) -> tuple[float, float]:
    """(RSS KiB, %CPU) of pid via ps. (nan, nan) if the process is gone."""
    return sample_rss_cpu_many([pid]).get(pid, (float("nan"), float("nan")))


def sample_fds_many(pids: list[int], gw_ports: set[int],
                    fake_ports: set[int]) -> dict[int, tuple[int, int]]:
    """{pid: (inbound, upstream)} TCP fd counts via ONE lsof call. Inbound = a
    socket on ANY gateway listen port; upstream = a socket whose peer is a fake
    port. lsof is the slow sampler (tens of ms per call), so N workers must not
    cost N calls per tick."""
    out: dict[int, tuple[int, int]] = {p: (0, 0) for p in pids}
    if not pids:
        return out
    try:
        r = subprocess.run(["lsof", "-nP", "-p", ",".join(str(p) for p in pids),
                            "-iTCP"],
                           capture_output=True, text=True, timeout=10)
        for line in r.stdout.splitlines():
            if "->" not in line:
                continue  # LISTEN sockets have no peer
            cols = line.split()
            if len(cols) < 3:
                continue
            try:
                pid = int(cols[1])
            except ValueError:
                continue  # the header line
            # NAME col looks like 127.0.0.1:PORT->127.0.0.1:PEER
            name = cols[-2] if cols[-1] in ("(ESTABLISHED)",) else cols[-1]
            try:
                local, peer = name.split("->")
                local_port = int(local.rsplit(":", 1)[1])
                peer_port = int(peer.rsplit(":", 1)[1])
            except (ValueError, IndexError):
                continue
            inb, up = out.get(pid, (0, 0))
            if peer_port in fake_ports:
                up += 1
            elif local_port in gw_ports:
                inb += 1
            out[pid] = (inb, up)
    except Exception:
        pass
    return out


def sample_fds(pid: int, gw_port: int, fake_ports: set[int]) -> tuple[int, int]:
    """(inbound, upstream) TCP fd counts for pid via lsof. Inbound = a socket on
    the gateway listen port; upstream = a socket whose peer is a fake port."""
    return sample_fds_many([pid], {gw_port}, fake_ports).get(pid, (0, 0))


class Sampler(threading.Thread):
    """Samples EVERY gateway worker each tick.

    Each record carries the fleet SUM under the keys the 1-worker sampler
    always wrote (`metrics`, `rss_kib`, `cpu_pct`, `fd_inbound`, `fd_upstream`,
    so `_agg_samples` and the report read unchanged), plus `*_max` (the
    hottest single worker that tick) and `per_worker` (each worker's own
    scrape and ps/lsof numbers, keyed by pid). Summing is right for every
    family we scrape except `llmgw_breaker_state`, which takes the max: any
    worker with an open breaker is "the fleet has an open breaker".

    `pids`, `metrics_urls` and `gw_ports` are parallel lists, one entry per
    gateway worker; the 1-worker case is the same lists of length one."""

    def __init__(self, pids: list[int] | int, metrics_urls: list[str] | str,
                 gw_ports: list[int] | int | set[int], fake_ports: set[int],
                 interval: float):
        super().__init__(daemon=True, name="bench-sampler")
        self.pids = [pids] if isinstance(pids, int) else list(pids)
        self.metrics_urls = ([metrics_urls] if isinstance(metrics_urls, str)
                             else list(metrics_urls))
        ports = [gw_ports] if isinstance(gw_ports, int) else list(gw_ports)
        self.gw_ports = ports
        self.fake_ports = fake_ports
        self.interval = interval
        self._stop_evt = threading.Event()
        self.samples: list[dict] = []

    # kept for anything that read the 1-worker attributes
    @property
    def pid(self) -> int:
        return self.pids[0]

    @property
    def metrics_url(self) -> str:
        return self.metrics_urls[0]

    @property
    def gw_port(self) -> int:
        return self.gw_ports[0]

    def run(self) -> None:
        port_set = set(self.gw_ports)
        with httpx.Client(timeout=5.0) as c:
            while not self._stop_evt.is_set():
                t = time.time()
                rec: dict = {"t": t, "per_worker": {}}
                summed: dict[str, float] = {}
                n_scraped = 0
                pw: dict[int, dict] = {}
                for i, pid in enumerate(self.pids):
                    port = self.gw_ports[i] if i < len(self.gw_ports) else None
                    url = self.metrics_urls[i] if i < len(self.metrics_urls) else None
                    entry: dict = {"port": port, "metrics": {}}
                    if url:
                        try:
                            m = parse_metrics(c.get(url).text)
                            entry["metrics"] = m
                            n_scraped += 1
                            for k, v in m.items():
                                if k == "llmgw_breaker_state":
                                    summed[k] = max(summed.get(k, 0.0), v)
                                else:
                                    summed[k] = summed.get(k, 0.0) + v
                        except Exception:
                            pass
                    pw[pid] = entry
                ps = sample_rss_cpu_many(self.pids)
                fds = sample_fds_many(self.pids, port_set, self.fake_ports)
                rss_sum = cpu_sum = 0.0
                rss_max = cpu_max = float("nan")
                in_sum = up_sum = in_max = up_max = 0
                alive = 0
                for pid in self.pids:
                    rss, cpu = ps.get(pid, (float("nan"), float("nan")))
                    inb, up = fds.get(pid, (0, 0))
                    pw[pid].update(rss_kib=rss, cpu_pct=cpu,
                                   fd_inbound=inb, fd_upstream=up)
                    if not math.isnan(rss):
                        alive += 1
                        rss_sum += rss
                        cpu_sum += cpu
                        rss_max = rss if math.isnan(rss_max) else max(rss_max, rss)
                        cpu_max = cpu if math.isnan(cpu_max) else max(cpu_max, cpu)
                    in_sum += inb
                    up_sum += up
                    in_max = max(in_max, inb)
                    up_max = max(up_max, up)
                rec["metrics"] = summed
                rec["metrics_scraped"] = n_scraped
                rec["workers_alive"] = alive
                # no worker answered ps: the fleet is gone (S8 after drain)
                rec["rss_kib"] = rss_sum if alive else float("nan")
                rec["cpu_pct"] = cpu_sum if alive else float("nan")
                rec["rss_kib_max"] = rss_max
                rec["cpu_pct_max"] = cpu_max
                rec["fd_inbound"], rec["fd_upstream"] = in_sum, up_sum
                rec["fd_inbound_max"], rec["fd_upstream_max"] = in_max, up_max
                rec["per_worker"] = pw
                self.samples.append(rec)
                self._stop_evt.wait(self.interval)

    def stop(self) -> None:
        self._stop_evt.set()


def _agg_samples(samples: list[dict]) -> dict:
    """Peaks and a couple of slopes from a sampler timeseries.

    Every key the 1-worker report carried keeps its name and meaning, with
    "the gateway" now meaning the fleet SUM (peak_rss_kib is the peak of the
    summed RSS, peak_cpu_pct the peak of the summed %CPU, and so on). Added:
    `workers`, the `*_max_worker` peaks (hottest single worker), and
    `per_worker` with each worker's own peaks and its share of
    `llmgw_requests_total` over the sampled window."""
    if not samples:
        return {}
    def col(key: str) -> list[float]:
        return [s[key] for s in samples if isinstance(s.get(key), (int, float))
                and not math.isnan(s.get(key, float("nan")))]
    def mcol(name: str) -> list[float]:
        return [s["metrics"][name] for s in samples
                if name in s.get("metrics", {})]
    streams = mcol("llmgw_streams_open")
    rss = col("rss_kib")
    pids: list[int] = []
    for s in samples:
        for pid in s.get("per_worker", {}):
            if pid not in pids:
                pids.append(pid)
    out = {
        "n_samples": len(samples),
        "workers": len(pids) or 1,
        "peak_streams_open": max(streams) if streams else None,
        "peak_tasks": max(mcol("llmgw_tasks") or [float("nan")]),
        "baseline_tasks": (mcol("llmgw_tasks") or [None])[-1],
        "peak_pump_buffered_bytes": max(mcol("llmgw_pump_buffered_bytes") or [0]),
        "peak_capture_queue_bytes": max(mcol("llmgw_capture_queue_bytes") or [0]),
        "capture_dropped": max(mcol("llmgw_capture_dropped_total") or [0]),
        "breaker_max_state": max(mcol("llmgw_breaker_state") or [0]),
        "peak_rss_kib": max(rss) if rss else None,
        "min_rss_kib": min(rss) if rss else None,
        "peak_cpu_pct": max(col("cpu_pct") or [float("nan")]),
        "peak_fd_inbound": max(col("fd_inbound") or [0]),
        "peak_fd_upstream": max(col("fd_upstream") or [0]),
        "peak_rss_kib_max_worker": max(col("rss_kib_max") or [float("nan")]),
        "peak_cpu_pct_max_worker": max(col("cpu_pct_max") or [float("nan")]),
        "peak_fd_inbound_max_worker": max(col("fd_inbound_max") or [0]),
        "peak_fd_upstream_max_worker": max(col("fd_upstream_max") or [0]),
    }
    req = mcol("llmgw_requests_total")
    out["requests_total_delta"] = (req[-1] - req[0]) if len(req) >= 2 else None
    per_worker: dict[str, dict] = {}
    for pid in pids:
        rows = [s["per_worker"][pid] for s in samples if pid in s.get("per_worker", {})]
        def wcol(key: str, rows: list[dict] = rows) -> list[float]:
            return [r[key] for r in rows if isinstance(r.get(key), (int, float))
                    and not math.isnan(r.get(key, float("nan")))]
        wreq = [r["metrics"]["llmgw_requests_total"] for r in rows
                if "llmgw_requests_total" in r.get("metrics", {})]
        wstreams = [r["metrics"]["llmgw_streams_open"] for r in rows
                    if "llmgw_streams_open" in r.get("metrics", {})]
        per_worker[str(pid)] = {
            "port": rows[0].get("port") if rows else None,
            "peak_rss_kib": max(wcol("rss_kib") or [None]),
            "peak_cpu_pct": max(wcol("cpu_pct") or [None]),
            "peak_fd_inbound": max(wcol("fd_inbound") or [0]),
            "peak_fd_upstream": max(wcol("fd_upstream") or [0]),
            "peak_streams_open": max(wstreams) if wstreams else None,
            "requests_total_delta": (wreq[-1] - wreq[0]) if len(wreq) >= 2 else None,
        }
    out["per_worker"] = per_worker
    # crude marginal RSS/stream within THIS run (full slope needs the S3/S4
    # ramp across stream counts; this is the single-run local estimate).
    pairs = [(s["metrics"].get("llmgw_streams_open"), s.get("rss_kib"))
             for s in samples
             if s.get("metrics", {}).get("llmgw_streams_open") is not None
             and isinstance(s.get("rss_kib"), (int, float))
             and not math.isnan(s.get("rss_kib", float("nan")))]
    if len(pairs) >= 2:
        xs = [p[0] for p in pairs]
        ys = [p[1] for p in pairs]
        if max(xs) - min(xs) >= 1:
            try:
                slope = statistics.covariance(xs, ys) / statistics.variance(xs)
                out["marginal_rss_kib_per_stream"] = slope
            except Exception:
                pass
    return out


# --------------------------------------------------------------------------
# Launching fakes + gateway as separate processes
# --------------------------------------------------------------------------


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_http(url: str, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=2.0) as c:
        while time.monotonic() < deadline:
            try:
                c.get(url)
                return True
            except Exception:
                time.sleep(0.1)
    return False


@dataclass
class Fleet:
    """The processes under test: one fake (itself possibly `--workers N` on
    the inside, sharing its two ports) and N gateway workers, each a separate
    `bench._gwproc` on its own port. `gw_procs[i]` listens on `gw_ports[i]`.

    `gw_proc` / `gw_port` / `gw_base` / `metrics_url` are the FIRST worker,
    kept so a 1-worker fleet reads exactly as before; anything that must see
    the whole fleet uses the plural forms."""
    fake_proc: subprocess.Popen
    gw_procs: list[subprocess.Popen]
    openai_port: int
    anthropic_port: int
    gw_ports: list[int]
    fake_workers: int = 1

    @property
    def gw_proc(self) -> subprocess.Popen:
        return self.gw_procs[0]

    @property
    def gw_port(self) -> int:
        return self.gw_ports[0]

    @property
    def gw_workers(self) -> int:
        return len(self.gw_procs)

    @property
    def gw_pids(self) -> list[int]:
        return [p.pid for p in self.gw_procs]

    @property
    def fake_base(self) -> str:
        return f"http://127.0.0.1:{self.openai_port}"

    @property
    def gw_base(self) -> str:
        return f"http://127.0.0.1:{self.gw_port}"

    @property
    def gw_bases(self) -> list[str]:
        return [f"http://127.0.0.1:{p}" for p in self.gw_ports]

    def gw_urls(self, path: str) -> list[str]:
        return [f"{b}{path}" for b in self.gw_bases]

    @property
    def metrics_url(self) -> str:
        return f"{self.gw_base}/metrics"

    @property
    def metrics_urls(self) -> list[str]:
        return [f"{b}/metrics" for b in self.gw_bases]

    @property
    def fake_ports(self) -> set[int]:
        return {self.openai_port, self.anthropic_port}

    @property
    def procs(self) -> list[subprocess.Popen]:
        return [*self.gw_procs, self.fake_proc]

    def gw_alive(self) -> list[int]:
        return [p.pid for p in self.gw_procs if p.poll() is None]

    def stop(self) -> None:
        # SIGTERM every worker first (they drain concurrently), THEN wait, so
        # the stop costs one drain grace, not N of them.
        for p in self.procs:
            if p.poll() is None:
                p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
        for p in self.gw_procs:
            if p.stderr:
                p.stderr.close()
        _LIVE_FLEET_REFS.pop(id(self), None)


# Every fleet that has been launched and not stopped, so a SIGTERM to the
# driver (or an unhandled exception on the way out) can still kill all N
# gateway workers and the fake. Keyed by id() to avoid making Fleet hashable.
_LIVE_FLEET_REFS: dict[int, Fleet] = {}


def stop_all_fleets() -> None:
    for fleet in list(_LIVE_FLEET_REFS.values()):
        try:
            fleet.stop()
        except Exception:
            pass


def launch_fleet(*, unlimited: bool, tenants_file: str | None,
                 gw_workers: int = 1, fake_workers: int = 1) -> Fleet:
    gw_workers = max(1, int(gw_workers))
    fake_workers = max(1, int(fake_workers))
    openai_port = _free_port()
    anthropic_port = _free_port()
    gw_ports = [_free_port() for _ in range(gw_workers)]
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ)

    fake_argv = [sys.executable, "-m", "fakes.upstream",
                 "--openai-port", str(openai_port),
                 "--anthropic-port", str(anthropic_port),
                 "--log-level", "critical"]
    if fake_workers > 1:
        # fakes/upstream.py `--workers N`: N processes sharing the two ports
        # via SO_REUSEPORT. Only appended when asked for, so the default
        # launch argv is byte-for-byte last night's.
        fake_argv += ["--workers", str(fake_workers)]
    fake = subprocess.Popen(
        fake_argv, cwd=root, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    gws: list[subprocess.Popen] = []
    for port in gw_ports:
        env_gw = dict(env)
        env_gw.update(
            BENCH_GW_PORT=str(port),
            BENCH_FAKE_OPENAI_URL=f"http://127.0.0.1:{openai_port}",
            BENCH_FAKE_ANTHROPIC_URL=f"http://127.0.0.1:{anthropic_port}",
            BENCH_GW_UNLIMITED="1" if unlimited else "0",
        )
        if tenants_file:
            env_gw["BENCH_GW_TENANTS_FILE"] = tenants_file
        gws.append(subprocess.Popen(
            [sys.executable, "-m", "bench._gwproc"],
            cwd=root, env=env_gw, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL,
        ))
    fleet = Fleet(fake, gws, openai_port, anthropic_port, gw_ports,
                  fake_workers=fake_workers)
    _LIVE_FLEET_REFS[id(fleet)] = fleet
    if not _wait_http(f"{fleet.fake_base}/__stats", 15.0):
        fleet.stop()
        raise RuntimeError("fake upstream failed to start")
    # The workers boot concurrently; the wait budget is shared, not per-worker.
    deadline = time.monotonic() + 20.0 + 5.0 * (gw_workers - 1)
    for gw, url in zip(gws, fleet.metrics_urls, strict=True):
        if not _wait_http(url, max(1.0, deadline - time.monotonic())):
            err = gw.stderr.read().decode(errors="replace") if gw.stderr else ""
            fleet.stop()
            raise RuntimeError(f"gateway worker pid={gw.pid} failed to start; "
                               f"stderr:\n{err[:2000]}")
    return fleet


def fake_total(fake_base: str) -> int:
    with httpx.Client(timeout=5.0) as c:
        return int(c.get(f"{fake_base}/__stats").json()["total"])


# --------------------------------------------------------------------------
# Running one arm (multi-process), merging results
# --------------------------------------------------------------------------


@dataclass
class ArmResult:
    arm: str
    workers: int
    rate: float
    started: int
    ok: int
    error: int
    errors_by_kind: dict
    status_counts: dict
    late_arrivals: int
    max_late_s: float
    peak_inflight_sum: int
    bytes_total: int
    gw_headers: dict | None
    d_has_x_gw: bool | None
    ttfe: LogHistogram
    total: LogHistogram
    gaps: LogHistogram
    samples: dict
    wall_s: float
    fake_requests: int


def run_arm(*, arm: str, url: str, scenario: Scenario, rate: float, workers: int,
            warm_s: float, measure_s: float, fleet: Fleet,
            sample_interval: float, headers: dict[str, str],
            urls: list[str] | None = None) -> ArmResult:
    """`url` is the arm's POST target; `urls`, when given, is the full list of
    equivalent targets (one per gateway worker) the generator round-robins
    across. Arm D never passes `urls`, so its behaviour is unchanged."""
    ctx = mp.get_context("spawn")
    out_q: mp.Queue = ctx.Queue()
    per_worker_rate = rate / workers
    specs = [
        WorkerSpec(
            worker_id=i, arm=arm, url=url, body=scenario.body(),
            headers=headers, rate=per_worker_rate, warm_s=warm_s,
            measure_s=measure_s, streaming=scenario.streaming,
            assert_gw=(arm == "G"), read_bps=scenario.read_bps,
            flips=scenario.flips, timeout_s=scenario.timeout_s, seed=1000 + i,
            urls=list(urls) if urls and len(urls) > 1 else [],
        )
        for i in range(workers)
    ]

    fake_before = fake_total(fleet.fake_base)
    sampler = None
    if arm == "G":
        sampler = Sampler(fleet.gw_pids, fleet.metrics_urls, fleet.gw_ports,
                          fleet.fake_ports, sample_interval)
        sampler.start()

    t0 = time.monotonic()
    procs = [ctx.Process(target=_run_worker, args=(s, out_q)) for s in specs]
    results: list[WorkerResult] = []
    try:
        for p in procs:
            p.start()
        # One result per worker. `_run_worker` reports its own exceptions as
        # a crash result, but a worker that dies BEFORE it runs (spawn import
        # failure, OOM kill, a stray SIGKILL) never reports at all, and a bare
        # blocking get() would then hang the campaign forever. So poll, and
        # when every worker has exited with results still missing, synthesise
        # a crash result per missing worker and carry on to the report.
        while len(results) < len(procs):
            try:
                results.append(out_q.get(timeout=1.0))
                continue
            except Exception:  # queue.Empty (mp re-exports it), or EINTR
                pass
            if all(not p.is_alive() for p in procs):
                seen = {r.worker_id for r in results}
                for s in specs:
                    if s.worker_id not in seen:
                        results.append(WorkerResult(
                            worker_id=s.worker_id, arm=arm, started=0, ok=0,
                            error=0, errors_by_kind={"worker-crash:no-result": 1},
                            status_counts={}, late_arrivals=0, max_late_s=0.0,
                            peak_inflight=0, bytes_total=0, gw_headers=None,
                            d_has_x_gw=None, ttfe=LogHistogram().to_dict(),
                            total=LogHistogram().to_dict(),
                            gaps=LogHistogram().to_dict()))
                break
        for p in procs:
            p.join(timeout=30)
    finally:
        # A SIGTERM to the driver (or a crash) must not orphan the generator
        # workers: they would keep firing at a fleet that is being torn down.
        for p in procs:
            if p.is_alive():
                p.terminate()
        if sampler:
            sampler.stop()
            sampler.join(timeout=5)
    wall = time.monotonic() - t0

    fake_after = fake_total(fleet.fake_base)

    ttfe = merge_all([LogHistogram.from_dict(r.ttfe) for r in results])
    total = merge_all([LogHistogram.from_dict(r.total) for r in results])
    gaps = merge_all([LogHistogram.from_dict(r.gaps) for r in results])
    errs: dict = {}
    stats: dict = {}
    for r in results:
        for k, v in r.errors_by_kind.items():
            errs[k] = errs.get(k, 0) + v
        for k, v in r.status_counts.items():
            stats[k] = stats.get(k, 0) + v
    gw_headers = next((r.gw_headers for r in results if r.gw_headers), None)
    d_has = next((r.d_has_x_gw for r in results if r.d_has_x_gw is not None), None)

    return ArmResult(
        arm=arm, workers=workers, rate=rate,
        started=sum(r.started for r in results),
        ok=sum(r.ok for r in results), error=sum(r.error for r in results),
        errors_by_kind=errs, status_counts=stats,
        late_arrivals=sum(r.late_arrivals for r in results),
        max_late_s=max((r.max_late_s for r in results), default=0.0),
        peak_inflight_sum=sum(r.peak_inflight for r in results),
        bytes_total=sum(r.bytes_total for r in results),
        gw_headers=gw_headers, d_has_x_gw=d_has,
        ttfe=ttfe, total=total, gaps=gaps,
        samples=_agg_samples(sampler.samples) if sampler else {},
        wall_s=wall, fake_requests=fake_after - fake_before,
    )


# --------------------------------------------------------------------------
# Instrument assertions -- "did we measure the right thing"
# --------------------------------------------------------------------------


def assert_instrument(scenario: Scenario, d: ArmResult, g: ArmResult) -> list[str]:
    """Return a list of verdict strings; raise on a hard failure that would make
    the whole measurement a lie."""
    verdicts: list[str] = []

    # Arm G really traversed the gateway
    gh = {k.lower(): v for k, v in (g.gw_headers or {}).items()}
    attempts = int(gh.get("x-gw-attempts", "0") or "0")
    if g.ok > 0:
        assert "x-gw-attempts" in gh, \
            "Arm G carries no X-Gw-Attempts: did not traverse gateway!"
        if scenario.expect_upstream_work:
            assert attempts >= 1, (
                f"Arm G made {attempts} upstream attempts -- refused before the "
                f"upstream (limiter/breaker shedding?), so this is not gateway overhead.")
        verdicts.append(f"Arm G X-Gw-Attempts={gh.get('x-gw-attempts')} "
                        f"Served-By={gh.get('x-gw-served-by')!r}")
    # Arm D did NOT go through the gateway
    if d.d_has_x_gw:
        raise AssertionError("Arm D carries X-Gw-* -- it is not a direct arm!")
    verdicts.append(f"Arm D carries X-Gw-*: {d.d_has_x_gw}")

    # The fake actually saw the traffic
    verdicts.append(f"fake saw D={d.fake_requests} G={g.fake_requests} requests")
    if scenario.expect_upstream_work and g.ok > 0 and g.fake_requests == 0:
        raise AssertionError("Arm G produced OK responses but the fake counter did "
                             "not advance -- the gateway answered without the upstream.")
    return verdicts


def calibration_verdict(d: ArmResult, g: ArmResult) -> tuple[bool, str]:
    """Arm D is the client's own ceiling. If D's p99 first-event is NOT below
    G's at this offered load, the number is the client's, not the gateway's."""
    dp = d.ttfe.percentile(99) * 1e3
    gp = g.ttfe.percentile(99) * 1e3
    if math.isnan(dp) or math.isnan(gp):
        return False, "insufficient samples to calibrate"
    ok = dp < gp
    tail = ("CALIBRATED (client faster than gateway path, number is honest)"
            if ok else "INVALID (client is the bottleneck, not the gateway)")
    msg = (f"Arm D p99 TTFE={dp:.2f} ms {'<' if ok else '>='} "
           f"Arm G p99 TTFE={gp:.2f} ms -> {tail}")
    return ok, msg


def saturation_flags(arm: ArmResult) -> list[str]:
    flags = []
    late_frac = arm.late_arrivals / arm.started if arm.started else 0.0
    if late_frac > 0.02:
        flags.append(f"Arm {arm.arm}: {arm.late_arrivals}/{arm.started} "
                     f"({late_frac:.1%}) arrivals fired LATE "
                     f"(max {arm.max_late_s*1e3:.0f} ms) -- the GENERATOR could not "
                     f"keep its schedule (instrument-is-a-SUT).")
    if arm.error and arm.ok and arm.error / (arm.ok + arm.error) > 0.01:
        flags.append(f"Arm {arm.arm}: {arm.error} errors "
                     f"({arm.error/(arm.ok+arm.error):.1%}): {arm.errors_by_kind}")
    return flags


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def _hrow(name: str, h: LogHistogram) -> str:
    q = h.quantile_row_ms()
    def fmt(x):
        return "   n/a" if math.isnan(x) else f"{x:7.2f}"
    return (f"| {name:<14} | {q['n']:>7} | {fmt(q['p50'])} | {fmt(q['p90'])} | "
            f"{fmt(q['p99'])} | {fmt(q['p999'])} | {fmt(q['mean'])} |")


def arm_tables(r: ArmResult) -> str:
    p = [f"### Arm {r.arm} -- rate {r.rate:.0f} rps, {r.workers} workers, "
         f"wall {r.wall_s:.1f}s",
         f"started={r.started} ok={r.ok} error={r.error} bytes={r.bytes_total} "
         f"late={r.late_arrivals} peak_inflight(sum over workers)={r.peak_inflight_sum}",
         "",
         "| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |",
         "|----------------|---------|---------|---------|---------|----------|---------|",
         _hrow("ttfe", r.ttfe), _hrow("total", r.total)]
    if r.gaps.total:
        p.append(_hrow("inter-event", r.gaps))
    if r.status_counts:
        p.append("")
        p.append(f"status: {r.status_counts}")
    if r.samples:
        p.append("")
        nw = r.samples.get("workers", 1)
        p.append(f"gateway samples ({nw} worker{'s' if nw != 1 else ''}; "
                 f"rss/cpu/fd/metrics are fleet SUMS, *_max_worker the hottest "
                 f"single worker, per_worker each worker's own peaks): "
                 f"{json.dumps(r.samples, default=str)}")
    return "\n".join(p)


def paired_delta(d: ArmResult, g: ArmResult) -> str:
    """Paired per-quantile DIFFERENCE (NOT an average of percentiles). We report
    the added latency at matched quantiles -- the honest framing: both arms'
    absolute numbers plus the difference."""
    rows = []
    for q in (50, 90, 99, 99.9):
        dv = d.ttfe.percentile(q) * 1e3
        gv = g.ttfe.percentile(q) * 1e3
        if math.isnan(dv) or math.isnan(gv):
            rows.append(f"| p{q} | n/a | n/a | n/a |")
        else:
            rows.append(f"| p{q} | {dv:7.2f} | {gv:7.2f} | {gv-dv:+7.2f} |")
    return ("added first-event latency (gateway path), matched-quantile:\n"
            "| q | Arm D ms | Arm G ms | added ms |\n"
            "|---|----------|----------|----------|\n" + "\n".join(rows))


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def env_snapshot(*, gw_workers: int = 1, fake_workers: int = 1) -> dict:
    try:
        load = os.getloadavg()
    except OSError:
        load = (float("nan"),) * 3
    ulimit = None
    try:
        import resource
        ulimit = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    except Exception:
        pass
    return {
        "python": platform.python_version(),
        "machine": platform.machine(),
        "cores": os.cpu_count(),
        "ulimit_nofile": ulimit,
        "loadavg": load,
        "ts": time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime()),
        "gw_workers": gw_workers,
        "fake_workers": fake_workers,
    }


def _fleet_line(fleet: Fleet) -> str:
    return (f"[fleet] fake={fleet.fake_base} (x{fleet.fake_workers}) "
            f"gw={','.join(fleet.gw_bases)} gw_pids={fleet.gw_pids}")


def run_scenario(scenario: Scenario, *, warm_s: float, measure_s: float,
                 repeats: int, workers: int, rate: float,
                 sample_interval: float, gw_workers: int = 1,
                 fake_workers: int = 1) -> str:
    env = env_snapshot(gw_workers=gw_workers, fake_workers=fake_workers)
    out: list[str] = [f"# Scenario {scenario.sid}: {scenario.name}",
                      f"_{scenario.question}_", "",
                      f"produces: {scenario.produces}", "",
                      f"env: {json.dumps(env, default=str)}", "",
                      f"config: rate={rate} rps, workers={workers}, "
                      f"warm={warm_s}s measure={measure_s}s repeats={repeats} "
                      f"gw_workers={gw_workers} fake_workers={fake_workers}", ""]

    tenants_file = scenario.prepare()
    fleet = launch_fleet(unlimited=scenario.unlimited, tenants_file=tenants_file,
                         gw_workers=gw_workers, fake_workers=fake_workers)
    print(_fleet_line(fleet), flush=True)
    d_runs: list[ArmResult] = []
    g_runs: list[ArmResult] = []
    try:
        for rep in range(repeats):
            print(f"[S{scenario.sid}] repeat {rep+1}/{repeats} Arm D (direct)...", flush=True)
            d = run_arm(arm="D", url=f"{fleet.fake_base}{scenario.path}",
                        scenario=scenario, rate=rate, workers=workers,
                        warm_s=warm_s, measure_s=measure_s, fleet=fleet,
                        sample_interval=sample_interval,
                        headers=scenario.direct_headers())
            print(f"[S{scenario.sid}] repeat {rep+1}/{repeats} Arm G (gateway)...", flush=True)
            g = run_arm(arm="G", url=f"{fleet.gw_base}{scenario.path}",
                        urls=fleet.gw_urls(scenario.path),
                        scenario=scenario, rate=rate, workers=workers,
                        warm_s=warm_s, measure_s=measure_s, fleet=fleet,
                        sample_interval=sample_interval,
                        headers=scenario.gateway_headers())
            d_runs.append(d)
            g_runs.append(g)
    finally:
        fleet.stop()

    # pick the median run by Arm G p99 ttfe (never average percentiles across runs)
    def g_key(i: int) -> float:
        v = g_runs[i].ttfe.percentile(99)
        return v if not math.isnan(v) else math.inf
    order = sorted(range(len(g_runs)), key=g_key)
    med = order[len(order) // 2]
    d, g = d_runs[med], g_runs[med]

    out.append(f"## Median run ({med+1} of {repeats}, by Arm G p99 TTFE)\n")
    out.append(arm_tables(d))
    out.append("")
    out.append(arm_tables(g))
    out.append("")
    out.append(paired_delta(d, g))
    out.append("")

    verdicts = assert_instrument(scenario, d, g)
    cal_ok, cal_msg = calibration_verdict(d, g)
    out.append("## Instrument verdicts")
    for v in verdicts:
        out.append(f"- {v}")
    out.append(f"- CALIBRATION: {cal_msg}")
    flags = saturation_flags(d) + saturation_flags(g)
    if flags:
        out.append("## Saturation / instrument-is-a-SUT flags")
        for fl in flags:
            out.append(f"- {fl}")
    else:
        out.append("- no generator-saturation flags (schedule kept, errors bounded)")

    if repeats > 1:
        spread = [g_runs[i].ttfe.percentile(99) * 1e3 for i in range(repeats)]
        spread = [x for x in spread if not math.isnan(x)]
        if spread:
            out.append(f"\nrun-to-run spread (Arm G p99 TTFE ms): "
                       f"min={min(spread):.2f} max={max(spread):.2f} "
                       f"median={statistics.median(spread):.2f}")
    return "\n".join(out)


def run_s6_isolation(scenario: Scenario, *, warm_s: float, measure_s: float,
                     workers: int, rate: float, sample_interval: float,
                     gw_workers: int = 1, fake_workers: int = 1) -> str:
    """S6 isolation driver. Real limiter + tenants file. Run Arm G TWICE:
      (a) hot tenant A flooding at 5x its cap alongside quiet B and C;
      (b) baseline: B alone at the same quiet rate.
    The question is whether B's p99 moves between (a) and (b). We
    NEVER average percentiles -- we report both B p99 numbers and their delta.

    Tenants run CONCURRENTLY in (a): each tenant is a worker pool with its own
    bearer token and offered rate, all firing into one gateway at once.

    With `gw_workers` > 1 note that the tenant limiter is PER PROCESS (each
    worker has its own admission state), so a fleet of N enforces N x the
    configured cap in aggregate; the isolation question (does B move?) is
    still answerable, but A's 429 share will be lower than with one worker."""
    tenants_file = scenario.prepare()
    fleet = launch_fleet(unlimited=False, tenants_file=tenants_file,
                         gw_workers=gw_workers, fake_workers=fake_workers)
    print(_fleet_line(fleet), flush=True)
    env = env_snapshot(gw_workers=gw_workers, fake_workers=fake_workers)
    out = [f"# Scenario {scenario.sid}: {scenario.name}", f"_{scenario.question}_",
           "", f"produces: {scenario.produces}", "",
           f"env: {json.dumps(env, default=str)}",
           f"tenants file: {tenants_file}", ""]
    groups = scenario.tenant_groups  # [(label, token, cap)]
    quiet_rate = rate           # B and C offered at this (<= their cap)
    hot_rate = rate * 5         # A offered at 5x its cap
    g_urls = fleet.gw_urls(scenario.path)

    def g_headers(token: str) -> dict[str, str]:
        h = scenario.gateway_headers()
        h["Authorization"] = f"Bearer {token}"
        return h

    try:
        # (a) all three tenants concurrently. Spawn one worker pool per tenant
        # as a thread that calls run_arm, so they overlap in wall time.
        import concurrent.futures as cf
        plan = [("hot", groups[0][1], hot_rate),
                ("normal-b", groups[1][1], quiet_rate),
                ("normal-c", groups[2][1], quiet_rate)]
        results: dict[str, ArmResult] = {}
        with cf.ThreadPoolExecutor(max_workers=len(plan)) as ex:
            futs = {
                ex.submit(
                    run_arm, arm="G", url=f"{fleet.gw_base}{scenario.path}",
                    urls=g_urls,
                    scenario=scenario, rate=grate, workers=max(1, workers),
                    warm_s=warm_s, measure_s=measure_s, fleet=fleet,
                    sample_interval=sample_interval, headers=g_headers(tok)): label
                for (label, tok, grate) in plan
            }
            for fut in cf.as_completed(futs):
                results[futs[fut]] = fut.result()
        # (b) baseline: B alone
        baseline_b = run_arm(
            arm="G", url=f"{fleet.gw_base}{scenario.path}", urls=g_urls,
            scenario=scenario,
            rate=quiet_rate, workers=max(1, workers), warm_s=warm_s,
            measure_s=measure_s, fleet=fleet, sample_interval=sample_interval,
            headers=g_headers(groups[1][1]))
    finally:
        fleet.stop()

    b_loaded = results["normal-b"].ttfe.percentile(99) * 1e3
    b_base = baseline_b.ttfe.percentile(99) * 1e3
    out.append("## Per-tenant Arm G (A hot at 5x, B and C quiet), all concurrent\n")
    for label in ("hot", "normal-b", "normal-c"):
        out.append(arm_tables(results[label]))
        out.append("")
    out.append("## Baseline: tenant B alone\n")
    out.append(arm_tables(baseline_b))
    out.append("")
    out.append("## Isolation verdict")
    out.append(f"- tenant B p99 TTFE: with hot A = {b_loaded:.2f} ms, "
               f"baseline (B alone) = {b_base:.2f} ms, delta = {b_loaded-b_base:+.2f} ms")
    out.append(f"- hot tenant A status mix (429 share shows shedding): "
               f"{results['hot'].status_counts}")
    return "\n".join(out)


def run_s8_deploy(scenario: Scenario, *, warm_s: float, measure_s: float,
                  workers: int, rate: float, sample_interval: float,
                  gw_workers: int = 1, fake_workers: int = 1) -> str:
    """S8 deploy-under-load. Run the S3 workload on Arm G; at `deploy_sigterm_at`
    seconds into the measure window, SIGTERM the gateway process (which runs
    through lifecycle.run, so it DRAINS rather than cuts). The contract:
    client errors must be zero and no stream is cut mid-flight.

    We detect a cut as a client-side connect/protocol error during the window.
    A clean drain ends in-flight streams with a proper terminal frame, so the
    client sees status 200 and no transport error.

    With `gw_workers` > 1 EVERY worker gets the SIGTERM at the same instant
    (a whole-fleet replace, the worst case, not a rolling one-at-a-time deploy)
    and each drains independently. "Drain duration" is then the time until the
    LAST worker has exited; the per-worker exit times are reported too. The
    contract is unchanged -- zero cut streams -- and the count is a fleet sum.
    As with one worker, arrivals scheduled AFTER the fleet has exited fail to
    connect and are counted under `connect`; the drain verdict therefore reads
    honestly only when `measure` is sized so the window ends soon after the
    drain (60 s SIGTERM + a 100 s stream lifetime for the S3 workload)."""
    fleet = launch_fleet(unlimited=scenario.unlimited, tenants_file=None,
                         gw_workers=gw_workers, fake_workers=fake_workers)
    print(_fleet_line(fleet), flush=True)
    env = env_snapshot(gw_workers=gw_workers, fake_workers=fake_workers)
    out = [f"# Scenario {scenario.sid}: {scenario.name}", f"_{scenario.question}_",
           "", f"produces: {scenario.produces}", "",
           f"env: {json.dumps(env, default=str)}", ""]
    at = scenario.deploy_sigterm_at or 60.0

    def deploy() -> None:
        time.sleep(warm_s + at)
        targets = [p for p in fleet.gw_procs if p.poll() is None]
        if not targets:
            return
        t0 = time.monotonic()
        for p in targets:
            p.send_signal(signal.SIGTERM)
        exits: list[str] = []
        deadline = t0 + scenario.timeout_s
        for p in targets:
            try:
                p.wait(timeout=max(0.0, deadline - time.monotonic()))
                exits.append(f"pid {p.pid}: {time.monotonic()-t0:.2f}s")
            except subprocess.TimeoutExpired:
                exits.append(f"pid {p.pid}: still running after {scenario.timeout_s}s")
        # `wait` returns in list order, so the elapsed time on the last entry
        # that exited is the fleet drain duration (max over workers).
        out.append(f"- SIGTERM sent to {len(targets)} gateway worker(s) at "
                   f"measure+{at}s; fleet fully exited after "
                   f"{time.monotonic()-t0:.2f}s (drain duration = max over workers)")
        out.append(f"- per-worker exit: {'; '.join(exits)}")

    try:
        deployer = threading.Thread(target=deploy, daemon=True)
        deployer.start()
        g = run_arm(arm="G", url=f"{fleet.gw_base}{scenario.path}",
                    urls=fleet.gw_urls(scenario.path),
                    scenario=scenario, rate=rate, workers=workers,
                    warm_s=warm_s, measure_s=measure_s, fleet=fleet,
                    sample_interval=sample_interval,
                    headers=scenario.gateway_headers())
        deployer.join(timeout=scenario.timeout_s + 10)
    finally:
        fleet.stop()

    cut = sum(v for k, v in g.errors_by_kind.items()
              if k in ("connect", "protocol") or k.startswith("os:"))
    out.append("")
    out.append(arm_tables(g))
    out.append("")
    out.append("## Drain verdict")
    out.append(f"- client transport errors (cut streams): {cut} "
               f"(target: 0); error breakdown: {g.errors_by_kind}")
    out.append(f"- status mix: {g.status_counts}")
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.load")
    p.add_argument("--scenario", required=True, help="one of " + ", ".join(SCENARIOS))
    p.add_argument("--warm", type=float, default=60.0)
    p.add_argument("--measure", type=float, default=300.0)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) // 2))
    p.add_argument("--rate", type=float, default=None,
                   help="override the scenario's target arrival rate (rps)")
    p.add_argument("--sample-interval", type=float, default=1.0)
    p.add_argument("--gw-workers", type=int, default=1,
                   help="gateway worker processes, each on its own port; the "
                        "generator round-robins across them (default 1 = last "
                        "night's single-process fleet)")
    p.add_argument("--fake-workers", type=int, default=1,
                   help="passed to fakes.upstream as --workers N (SO_REUSEPORT "
                        "siblings on the same two ports); default 1 = unchanged argv")
    p.add_argument("--smoke", action="store_true",
                   help="tiny self-validation: low rate, short windows, 1 repeat")
    args = p.parse_args(argv)

    if args.scenario not in SCENARIOS:
        print(f"unknown scenario {args.scenario!r}; choose from {list(SCENARIOS)}",
              file=sys.stderr)
        return 2
    if args.gw_workers < 1 or args.fake_workers < 1:
        print("--gw-workers and --fake-workers must be >= 1", file=sys.stderr)
        return 2
    scenario = SCENARIOS[args.scenario]
    gw_workers, fake_workers = args.gw_workers, args.fake_workers

    # A SIGTERM/SIGINT to the driver must take the whole fleet with it: raise
    # in the main thread so every `finally: fleet.stop()` runs, and keep an
    # atexit sweep for the path where the exception surfaces somewhere a
    # `finally` does not cover (e.g. mid launch_fleet).
    def _on_term(signum, _frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _on_term)
        except (ValueError, OSError):
            pass  # not the main thread / unsupported: fall back to atexit
    import atexit
    atexit.register(stop_all_fleets)

    warm, measure, repeats, workers = args.warm, args.measure, args.repeats, args.workers
    rate = args.rate if args.rate is not None else scenario.rate
    if args.smoke:
        import dataclasses
        warm = min(warm, 1.0)
        measure = min(measure, 4.0)
        repeats = 1
        workers = min(workers, 2)
        rate = min(rate, scenario.smoke_rate)
        # Shorten long streams so the smoke does not spend minutes draining
        # 25 s streams. The shape (streaming, interval, mode) is preserved; only
        # the event COUNT is capped, which is enough to exercise TTFE, the
        # inter-event gap histogram, backpressure, and the samplers.
        if scenario.streaming and scenario.events and scenario.events > 40:
            scenario = dataclasses.replace(scenario, events=40, timeout_s=20.0)

    try:
        if scenario.driver == "s6_isolation":
            report = run_s6_isolation(scenario, warm_s=warm, measure_s=measure,
                                      workers=workers, rate=rate,
                                      sample_interval=args.sample_interval,
                                      gw_workers=gw_workers, fake_workers=fake_workers)
        elif scenario.driver == "s8_deploy":
            report = run_s8_deploy(scenario, warm_s=warm, measure_s=measure,
                                   workers=workers, rate=rate,
                                   sample_interval=args.sample_interval,
                                   gw_workers=gw_workers, fake_workers=fake_workers)
        else:
            report = run_scenario(scenario, warm_s=warm, measure_s=measure,
                                  repeats=repeats, workers=workers, rate=rate,
                                  sample_interval=args.sample_interval,
                                  gw_workers=gw_workers, fake_workers=fake_workers)
    finally:
        stop_all_fleets()
    print()
    print(report)

    out_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(out_dir, exist_ok=True)
    suffix = "smoke" if args.smoke else "run"
    # A multi-worker run gets its own file so it sits NEXT TO the
    # single-process result it is compared against, instead of overwriting it.
    tag = f"-gw{gw_workers}" if gw_workers > 1 else ""
    out_path = os.path.join(out_dir, f"load-{scenario.sid}{tag}-{suffix}.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report + "\n")
    print(f"\nwrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

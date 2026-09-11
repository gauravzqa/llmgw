"""Live box-and-gateway monitor for the S1-S8 campaign. Standalone; stdlib + psutil.

    python -m bench.monitor --gateway-port 8800 --interval 1 \
        --out bench/results/monitor-<ts>.jsonl
    python -m bench.monitor --print-metrics --gateway-port 8800     # dump /metrics names, exit
    python -m bench.monitor --demo 30 --out bench/results/monitor-demo.jsonl   # synthetic file

Why this exists: the preliminary S1-S3 pass (bench/results/preliminary-S1S2S3.md)
was invalid because the *instrument* -- the load generator and the fake upstream,
sharing the box with the gateway -- saturated first. Nothing in the harness could
show that while the run was going. This process samples, once per interval, the
whole local fleet by pid and writes one JSON line:

  * host loadavg 1/5/15 and cpu_count
  * for each process GROUP found by cmdline match -- gateway (`llmgw.server` or
    `bench._gwproc`), fake upstream (`fakes.upstream`), generator (`bench.load`
    plus every child it spawned, which is how the multiprocessing workers show
    up: their argv is `spawn_main(...)`, not `bench.load`) -- the process count
    and summed rss_mib / cpu_pct / num_fds / num_threads; the gateway also gets
    its TCP fd split into inbound (a peer on the listen port) vs upstream (a
    peer on a fake port), the same rule `bench/load.py::sample_fds` uses.
  * the RLIMIT_NOFILE soft limit (from the gateway pid where the platform can
    read another pid's limits; on macOS from this process, which shares the
    campaign's shell)
  * a scrape of GET /metrics flattened into the gauges that matter

Any group may be absent (gateway idle between arms, generator between runs):
that writes zeros, never raises. Ctrl-C ends the file cleanly.

The gateway port: `--gateway-port 8800` for a `make run` gateway; `auto`
(the default) discovers the LISTEN port of whatever gateway pid it finds, which
is what the campaign needs because `bench.load` launches its gateway on a
free port per run.

A MULTI-WORKER gateway (`bench.load --gw-workers N`) is N separate
`bench._gwproc` processes, each listening on its own port, and each with its
own per-process Prometheus registry. The gateway group therefore holds all N
pids; `auto` discovers EVERY worker's LISTEN port, scrapes each /metrics and
SUMS them (max for `breaker_state_max`) into the same flat `metrics` dict, so
`streams_open` and `requests_total` are fleet totals. The group record adds
`workers` (count), `cpu_pct_max_worker` (the hottest single worker, so the
dashboard can show a per-core ceiling next to the fleet sum) and `per_worker`
(pid, port, cpu_pct, rss_mib, fd_inbound, fd_upstream for each); every
pre-existing key keeps its name and its fleet-sum meaning. `gateway_port` stays
the first worker's port and `gateway_ports` lists them all.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import resource
import signal
import sys
import time
import urllib.error
import urllib.request

import psutil

# --------------------------------------------------------------------------
# Process groups: cmdline substrings. Only python processes are considered so a
# shell or an editor whose argv happens to contain "bench.load" is not counted.
# --------------------------------------------------------------------------

GROUP_PATTERNS: dict[str, tuple[str, ...]] = {
    "gateway": ("llmgw.server", "bench._gwproc"),
    "fake": ("fakes.upstream",),
    "generator": ("bench.load",),
}
GROUPS = tuple(GROUP_PATTERNS)
DEFAULT_FAKE_PORTS = frozenset({8801, 8802})   # fakes/upstream.py defaults

# --------------------------------------------------------------------------
# /metrics: the real family names, from src/llmgw/metrics.py and a live scrape.
# Labelled families are aggregated as noted; the flat dict keys are what the
# dashboard binds to.
# --------------------------------------------------------------------------

M_TASKS = "llmgw_tasks"                                # gauge, no labels
M_STREAMS = "llmgw_streams_open"                       # gauge {surface}      -> sum
M_UPSTREAM_CONN = "llmgw_upstream_connections"         # gauge {provider}     -> sum
M_PUMP_BYTES = "llmgw_pump_buffered_bytes"             # gauge, no labels
M_CAPTURE_Q = "llmgw_capture_queue_bytes"              # gauge, no labels
M_PERMITS = "llmgw_permits_in_use"                     # gauge {scope}        -> sum
M_DRAINING = "llmgw_draining"                          # gauge, no labels
M_BREAKER = "llmgw_breaker_state"                      # gauge {provider,model}: 0/1/2
M_BREAKER_TRANS = "llmgw_breaker_transitions_total"    # counter {provider,model,to}
M_CAPTURE_DROP = "llmgw_capture_dropped_total"         # counter {reason}     -> sum
M_ADMISSION = "llmgw_admission_denied_total"           # counter {reason} -> sum, per reason
M_REQUESTS = "llmgw_requests_total"                    # counter {surface,outcome,code}
M_COMMITTED = "llmgw_committed_total"                  # counter {surface}    -> sum
M_ATTEMPTS = "llmgw_attempts_total"                    # counter {provider,model,result} -> sum

WANTED = frozenset({
    M_TASKS, M_STREAMS, M_UPSTREAM_CONN, M_PUMP_BYTES, M_CAPTURE_Q, M_PERMITS, M_DRAINING,
    M_BREAKER, M_BREAKER_TRANS, M_CAPTURE_DROP, M_ADMISSION, M_REQUESTS, M_COMMITTED,
    M_ATTEMPTS,
})

# Every flat key the record carries, so an absent family still writes a zero
# and the dashboard never sees a missing column.
METRIC_KEYS: tuple[str, ...] = (
    "tasks", "streams_open", "upstream_connections", "pump_buffered_bytes",
    "capture_queue_bytes", "permits_in_use", "draining",
    "breaker_open", "breaker_half_open", "breaker_state_max", "breaker_transitions_open_total",
    "capture_dropped_total", "admission_denied_total",
    "requests_total", "committed_total", "attempts_total",
)


def _parse_prom_line(line: str) -> tuple[str, dict[str, str], float] | None:
    """`name{k="v",...} value` -> (name, labels, value). None for comments/junk."""
    if not line or line.startswith("#"):
        return None
    try:
        left, val = line.rsplit(" ", 1)
        v = float(val)
    except ValueError:
        return None
    labels: dict[str, str] = {}
    if "{" in left:
        name, rest = left.split("{", 1)
        rest = rest.rstrip("}")
        # values are quoted and contain no commas in this contract (closed vocabularies)
        for kv in rest.split(","):
            if "=" in kv:
                k, vv = kv.split("=", 1)
                labels[k.strip()] = vv.strip().strip('"')
    else:
        name = left
    return name.strip(), labels, v


def parse_metrics(text: str) -> dict[str, float]:
    """Prometheus text -> flat dict of the gauges/counters the monitor cares about."""
    out: dict[str, float] = {k: 0.0 for k in METRIC_KEYS}
    for line in text.splitlines():
        p = _parse_prom_line(line)
        if p is None:
            continue
        name, labels, v = p
        if name not in WANTED:
            continue
        if name == M_TASKS:
            out["tasks"] = v
        elif name == M_STREAMS:
            out["streams_open"] += v
        elif name == M_UPSTREAM_CONN:
            out["upstream_connections"] += v
        elif name == M_PUMP_BYTES:
            out["pump_buffered_bytes"] = v
        elif name == M_CAPTURE_Q:
            out["capture_queue_bytes"] = v
        elif name == M_PERMITS:
            out["permits_in_use"] += v
        elif name == M_DRAINING:
            out["draining"] = v
        elif name == M_BREAKER:
            if v == 1:
                out["breaker_open"] += 1
            elif v == 2:
                out["breaker_half_open"] += 1
            out["breaker_state_max"] = max(out["breaker_state_max"], v)
        elif name == M_BREAKER_TRANS:
            if labels.get("to") == "open":
                out["breaker_transitions_open_total"] += v
        elif name == M_CAPTURE_DROP:
            out["capture_dropped_total"] += v
        elif name == M_ADMISSION:
            out["admission_denied_total"] += v
            r = labels.get("reason")
            if r:
                k = f"admission_denied_{r}"
                out[k] = out.get(k, 0.0) + v
        elif name == M_REQUESTS:
            out["requests_total"] += v
            o = labels.get("outcome")
            if o:
                k = f"requests_{o}"
                out[k] = out.get(k, 0.0) + v
        elif name == M_COMMITTED:
            out["committed_total"] += v
        elif name == M_ATTEMPTS:
            out["attempts_total"] += v
    return out


def merge_metrics(parts: list[dict[str, float]]) -> dict[str, float]:
    """Sum N workers' flat metric dicts into one fleet dict. Every key is a
    per-process count or gauge that adds across processes (tasks, open
    streams, buffered bytes, requests, ...), except `breaker_state_max`, which
    is a state and takes the max. Keys that only some workers carry (the
    per-reason / per-outcome dynamic ones) are summed where present."""
    out: dict[str, float] = {k: 0.0 for k in METRIC_KEYS}
    for m in parts:
        for k, v in m.items():
            if k == "breaker_state_max":
                out[k] = max(out.get(k, 0.0), v)
            else:
                out[k] = out.get(k, 0.0) + v
    return out


def _iso(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, tz=_dt.UTC).isoformat(timespec="seconds")


def fetch_metrics(port: int, timeout: float = 2.0) -> str:
    url = f"http://127.0.0.1:{port}/metrics"
    with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 - localhost
        return r.read().decode("utf-8", errors="replace")


def metric_families(text: str) -> dict[str, set[str]]:
    """family name -> set of label-key signatures seen. For --print-metrics."""
    fams: dict[str, set[str]] = {}
    for line in text.splitlines():
        p = _parse_prom_line(line)
        if p is None:
            continue
        name, labels, _ = p
        fams.setdefault(name, set()).add(",".join(sorted(labels)) or "(no labels)")
    return fams


# --------------------------------------------------------------------------
# Process discovery and per-group sampling
# --------------------------------------------------------------------------

def _is_python(p: psutil.Process, cmd: list[str]) -> bool:
    head = (cmd[0] if cmd else "") or ""
    try:
        name = p.name()
    except psutil.Error:
        name = ""
    return "python" in os.path.basename(head).lower() or "python" in name.lower()


class Fleet:
    """Finds the three groups each sample and keeps Process objects across
    samples so `cpu_percent(None)` is the utilisation since the LAST sample
    (the first call on a new pid returns 0.0, which is the honest answer)."""

    def __init__(self) -> None:
        self._procs: dict[int, psutil.Process] = {}
        self._self = os.getpid()

    def _proc(self, pid: int) -> psutil.Process | None:
        p = self._procs.get(pid)
        if p is None:
            try:
                p = psutil.Process(pid)
            except psutil.Error:
                return None
            self._procs[pid] = p
        return p

    def discover(self) -> dict[str, list[psutil.Process]]:
        found: dict[str, list[psutil.Process]] = {g: [] for g in GROUPS}
        seen: set[int] = set()
        for p in psutil.process_iter(["pid", "cmdline"]):
            pid = p.info["pid"]
            if pid == self._self or pid in seen:
                continue
            cmd = p.info["cmdline"] or []
            if not cmd:
                continue
            joined = " ".join(cmd)
            if "bench.monitor" in joined or not _is_python(p, cmd):
                continue
            for g, pats in GROUP_PATTERNS.items():
                if any(pat in joined for pat in pats):
                    q = self._proc(pid)
                    if q is not None:
                        found[g].append(q)
                        seen.add(pid)
                    break
        # A `make run` gateway left on 8800 next to the campaign's `bench._gwproc`
        # would double the gateway numbers: when the bench's own gateway exists,
        # count only that.
        def _is_bench_gw(q: psutil.Process) -> bool:
            try:
                return "bench._gwproc" in " ".join(q.cmdline())
            except psutil.Error:
                return False

        bench_gw = [q for q in found["gateway"] if _is_bench_gw(q)]
        if bench_gw:
            found["gateway"] = bench_gw
        # the generator's spawn workers and resource tracker are children
        for parent in list(found["generator"]):
            try:
                kids = parent.children(recursive=True)
            except psutil.Error:
                continue
            for k in kids:
                if k.pid in seen or k.pid == self._self:
                    continue
                q = self._proc(k.pid)
                if q is not None:
                    found["generator"].append(q)
                    seen.add(k.pid)
        # forget pids that are gone so the cache does not grow across a campaign
        alive = {p.pid for ps in found.values() for p in ps}
        for pid in list(self._procs):
            if pid not in alive:
                del self._procs[pid]
        return found


def _connections(p: psutil.Process):
    fn = getattr(p, "net_connections", None) or p.connections
    return fn(kind="tcp")


def listen_ports(p: psutil.Process) -> list[int]:
    try:
        return sorted({c.laddr.port for c in _connections(p)
                       if c.status == psutil.CONN_LISTEN and c.laddr})
    except psutil.Error:
        return []


def fd_split(p: psutil.Process, gw_port: int | set[int] | None,
             fake_ports: set[int]) -> tuple[int, int]:
    """(inbound, upstream) established TCP sockets, load.py::sample_fds semantics.
    `gw_port` may be one port or the set of every worker's listen port."""
    ports: set[int] = (set() if gw_port is None
                       else {gw_port} if isinstance(gw_port, int) else set(gw_port))
    inbound = upstream = 0
    try:
        conns = _connections(p)
    except psutil.Error:
        return 0, 0
    for c in conns:
        if not c.raddr:
            continue  # LISTEN has no peer
        if c.raddr.port in fake_ports:
            upstream += 1
        elif c.laddr and c.laddr.port in ports:
            inbound += 1
    return inbound, upstream


def empty_group() -> dict:
    return {"count": 0, "pids": [], "rss_mib": 0.0, "cpu_pct": 0.0,
            "num_fds": 0, "num_threads": 0}


def sample_procs(procs: list[psutil.Process]) -> list[dict]:
    """One row per LIVE process: pid, rss_mib, cpu_pct, num_fds, num_threads."""
    rows: list[dict] = []
    for p in procs:
        try:
            with p.oneshot():
                rss = p.memory_info().rss
                cpu = p.cpu_percent(None)
                thr = p.num_threads()
                try:
                    fds = p.num_fds()
                except psutil.Error:
                    fds = 0
        except psutil.Error:
            continue  # died between discovery and sampling
        rows.append({"pid": p.pid, "rss_mib": round(rss / (1024 * 1024), 2),
                     "cpu_pct": round(cpu, 1), "num_fds": fds, "num_threads": thr})
    return rows


def sample_group(procs: list[psutil.Process], rows: list[dict] | None = None) -> dict:
    """The group SUM. `rows` (from `sample_procs`) may be passed to avoid
    sampling twice when the caller also wants the per-process breakdown."""
    g = empty_group()
    for r in (rows if rows is not None else sample_procs(procs)):
        g["count"] += 1
        g["pids"].append(r["pid"])
        g["rss_mib"] += r["rss_mib"]
        g["cpu_pct"] += r["cpu_pct"]
        g["num_fds"] += r["num_fds"]
        g["num_threads"] += r["num_threads"]
    g["rss_mib"] = round(g["rss_mib"], 2)
    g["cpu_pct"] = round(g["cpu_pct"], 1)
    return g


def gateway_ports(gws: list[psutil.Process], fake_ports: set[int]) -> list[int]:
    """Every gateway worker's LISTEN port (a fake port is never one), in pid
    order, so `ports[i]` belongs to `gws[i]`-ish; a worker with no LISTEN yet
    (still booting) contributes nothing."""
    out: list[int] = []
    for gw in gws:
        for lp in listen_ports(gw):
            if lp not in fake_ports and lp not in out:
                out.append(lp)
    return out


def nofile_limit(gw: psutil.Process | None) -> tuple[int | None, str]:
    """RLIMIT_NOFILE soft limit. Per-pid where psutil supports it (Linux);
    otherwise this process's own, which is the campaign shell's if the monitor
    runs from it. The source is recorded so the number is not over-read."""
    if gw is not None and hasattr(gw, "rlimit"):
        try:
            soft, _hard = gw.rlimit(psutil.RLIMIT_NOFILE)  # type: ignore[attr-defined]
            return int(soft), "gateway"
        except (psutil.Error, AttributeError, OSError):
            pass
    try:
        soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        return (None if soft == resource.RLIM_INFINITY else int(soft)), "self"
    except (ValueError, OSError):
        return None, "unknown"


# --------------------------------------------------------------------------
# One record
# --------------------------------------------------------------------------

def make_record(fleet: Fleet, gateway_port: int | None) -> dict:
    ts = time.time()
    found = fleet.discover()
    gw_list = found["gateway"]
    gw = gw_list[0] if gw_list else None

    fake_ports: set[int] = set(DEFAULT_FAKE_PORTS)
    for fp in found["fake"]:
        fake_ports.update(listen_ports(fp))

    # One port per gateway worker. An explicit --gateway-port pins a single
    # worker (the `make run` case); auto discovers every worker's LISTEN port.
    ports: list[int] = ([gateway_port] if gateway_port is not None
                        else gateway_ports(gw_list, fake_ports))
    port_set = set(ports)
    port = ports[0] if ports else None

    # per-worker rows for the gateway; plain sums for the other two groups
    gw_rows = sample_procs(gw_list)
    groups = {g: (sample_group(found[g], gw_rows) if g == "gateway"
                  else sample_group(found[g])) for g in GROUPS}
    by_pid = {q.pid: q for q in gw_list}
    per_worker: list[dict] = []
    inbound = upstream = 0
    for r in gw_rows:
        q = by_pid.get(r["pid"])
        inb, up = fd_split(q, port_set, fake_ports) if q is not None else (0, 0)
        wports = [lp for lp in listen_ports(q) if lp in port_set] if q is not None else []
        per_worker.append({"pid": r["pid"], "port": wports[0] if wports else None,
                           "cpu_pct": r["cpu_pct"], "rss_mib": r["rss_mib"],
                           "fd_inbound": inb, "fd_upstream": up})
        inbound += inb
        upstream += up
    groups["gateway"]["fd_inbound"] = inbound
    groups["gateway"]["fd_upstream"] = upstream
    groups["gateway"]["workers"] = len(gw_rows)
    groups["gateway"]["cpu_pct_max_worker"] = (
        max((r["cpu_pct"] for r in gw_rows), default=0.0))
    groups["gateway"]["per_worker"] = per_worker

    try:
        loadavg: list[float | None] = [round(x, 2) for x in os.getloadavg()]
    except OSError:
        loadavg = [None, None, None]

    # Scrape EVERY worker and sum. A worker that fails to answer is reported
    # in metrics_error but does not blank the record: the others' sum is
    # still the best available fleet number, flagged as partial.
    metrics: dict[str, float] | None = None
    metrics_error: str | None = None
    if ports:
        parts: list[dict[str, float]] = []
        failures: list[str] = []
        for pt in ports:
            try:
                parts.append(parse_metrics(fetch_metrics(pt)))
            except (urllib.error.URLError, OSError, ValueError) as e:
                failures.append(f":{pt} {type(e).__name__}: {e}"[:120])
        if parts:
            metrics = merge_metrics(parts)
            metrics["workers_scraped"] = float(len(parts))
        if failures:
            metrics_error = (f"{len(failures)}/{len(ports)} scrapes failed: "
                             + "; ".join(failures))[:300]
    else:
        metrics_error = "no gateway port (no gateway process found)"

    soft, source = nofile_limit(gw)
    return {
        "ts": round(ts, 3),
        "iso": _iso(ts),
        "host": {
            "loadavg": loadavg,
            "cpu_count": psutil.cpu_count() or os.cpu_count() or 1,
        },
        "gateway": groups["gateway"],
        "fake": groups["fake"],
        "generator": groups["generator"],
        "nofile_soft": soft,
        "nofile_source": source,
        "gateway_port": port,
        "gateway_ports": ports,
        "metrics": metrics,
        "metrics_error": metrics_error,
    }


def _fmt_bytes(n: float) -> str:
    if n >= 1 << 30:
        return f"{n / (1 << 30):.2f}G"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.1f}M"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.0f}K"
    return f"{n:.0f}B"


def summary_line(rec: dict) -> str:
    h, gw, gen, fk = rec["host"], rec["gateway"], rec["generator"], rec["fake"]
    m = rec["metrics"] or {}
    t = rec["iso"][11:19]
    lim = rec["nofile_soft"]
    lim_s = str(lim) if lim is not None else "?"
    la = h["loadavg"][0]
    parts = [
        f"{t} load {la:.1f}/{h['cpu_count']}",
        f"gw x{gw['count']} rss {gw['rss_mib']:.0f}M cpu {gw['cpu_pct']:.0f}% "
        f"(max-w {gw.get('cpu_pct_max_worker', gw['cpu_pct']):.0f}%) "
        f"fds {gw['num_fds']}/{lim_s} (in {gw['fd_inbound']} up {gw['fd_upstream']})",
    ]
    if rec["metrics"] is not None:
        parts.append(
            f"streams {m.get('streams_open', 0):.0f} tasks {m.get('tasks', 0):.0f} "
            f"buf {_fmt_bytes(m.get('pump_buffered_bytes', 0))} "
            f"brk-open {m.get('breaker_open', 0):.0f} "
            f"drops {m.get('capture_dropped_total', 0):.0f} "
            f"req {m.get('requests_total', 0):.0f}"
        )
    else:
        parts.append(f"metrics: {rec['metrics_error']}")
    parts.append(f"gen x{gen['count']} cpu {gen['cpu_pct']:.0f}%")
    parts.append(f"fake x{fk['count']} cpu {fk['cpu_pct']:.0f}%")
    return " | ".join(parts)


# --------------------------------------------------------------------------
# --demo: a synthetic file in the same schema, for the dashboard
# --------------------------------------------------------------------------

def demo_record(i: int, n: int, t0: float, cores: int) -> dict:
    """An S3-shaped ramp that goes bad in the last third: the generator pins
    its cores and the loadavg climbs past cores x 0.7, so every threshold path
    in the dashboard is exercised at least once."""
    frac = i / max(n - 1, 1)
    bad = frac > 0.66
    streams = int(1000 * min(1.0, frac * 1.3))
    tasks = 4 + 5 * streams
    rss = 39 + streams * 0.06 + (18 if bad else 0)
    gen_n = 8
    gen_cpu = 120 + 500 * frac + (300 if bad else 0)
    la1 = 1.2 + 4.0 * frac + (7.0 if bad else 0)
    gw_cpu = 20 + 60 * frac + (25 if bad else 0)
    metrics = {k: 0.0 for k in METRIC_KEYS}
    metrics.update({
        "tasks": float(tasks), "streams_open": float(streams),
        "upstream_connections": float(streams),
        "pump_buffered_bytes": float(0 if not bad else 2_500_000 * (frac - 0.66) * 3),
        "requests_total": float(i * 10), "requests_completed": float(i * 9),
        "requests_failed": float(i if bad else 0),
        "committed_total": float(i * 10), "attempts_total": float(i * 10),
        "capture_dropped_total": float(3 if i >= n - 2 else 0),   # a counter: never falls
        "breaker_open": 1.0 if i == n - 1 else 0.0,
        "breaker_state_max": 1.0 if i == n - 1 else 0.0,
    })
    ts = t0 + i
    return {
        "ts": round(ts, 3),
        "iso": _iso(ts),
        "host": {"loadavg": [round(la1, 2), round(la1 * 0.8, 2), round(la1 * 0.6, 2)],
                 "cpu_count": cores},
        "gateway": {"count": 1, "pids": [40001], "rss_mib": round(rss, 2),
                    "cpu_pct": round(min(gw_cpu, 100.0), 1),
                    "num_fds": 12 + 2 * streams, "num_threads": 3,
                    "fd_inbound": streams, "fd_upstream": streams,
                    "workers": 1, "cpu_pct_max_worker": round(min(gw_cpu, 100.0), 1),
                    "per_worker": [{"pid": 40001, "port": 8800,
                                    "cpu_pct": round(min(gw_cpu, 100.0), 1),
                                    "rss_mib": round(rss, 2),
                                    "fd_inbound": streams, "fd_upstream": streams}]},
        "fake": {"count": 1, "pids": [40002], "rss_mib": round(30 + streams * 0.02, 2),
                 "cpu_pct": round(min(15 + 70 * frac + (20 if bad else 0), 100.0), 1),
                 "num_fds": 10 + streams, "num_threads": 2},
        "generator": {"count": gen_n, "pids": list(range(40010, 40010 + gen_n)),
                      "rss_mib": round(gen_n * 28.0, 2), "cpu_pct": round(gen_cpu, 1),
                      "num_fds": 20 + streams, "num_threads": gen_n * 2},
        "nofile_soft": 1_048_576 if not bad else 4096,
        "nofile_source": "self",
        "gateway_port": 8800,
        "gateway_ports": [8800],
        "metrics": metrics,
        "metrics_error": None,
    }


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def _default_out() -> str:
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    stamp = _dt.datetime.now(tz=_dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    return os.path.join(root, "bench", "results", f"monitor-{stamp}.jsonl")


def _parse_port(s: str) -> int | None:
    if s.lower() == "auto":
        return None
    try:
        return int(s)
    except ValueError as e:
        raise argparse.ArgumentTypeError("port must be an integer or 'auto'") from e


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.monitor", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway-port", type=_parse_port, default="auto",
                   help="gateway port for /metrics; 'auto' discovers the gateway pid's "
                        "LISTEN port (default; what the campaign needs)")
    p.add_argument("--out", default=None,
                   help="JSONL path (default bench/results/monitor-<utc-timestamp>.jsonl)")
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--print-metrics", action="store_true",
                   help="dump the raw /metrics family names once and exit")
    p.add_argument("--demo", type=int, metavar="N", default=None,
                   help="write N synthetic lines in the real schema to --out and exit")
    p.add_argument("--quiet", action="store_true", help="no per-sample stdout line")
    args = p.parse_args(argv)
    gateway_port: int | None = args.gateway_port if args.gateway_port != "auto" else None

    fleet = Fleet()

    if args.print_metrics:
        ports: list[int] = [gateway_port] if gateway_port is not None else []
        if not ports:
            found = fleet.discover()
            fake_ports: set[int] = set(DEFAULT_FAKE_PORTS)
            for fp in found["fake"]:
                fake_ports.update(listen_ports(fp))
            ports = gateway_ports(found["gateway"], fake_ports)
        if not ports:
            print("no gateway found; pass --gateway-port", file=sys.stderr)
            return 2
        # The family NAMES are identical on every worker (same registry
        # declaration), so one worker's scrape describes the fleet's schema.
        try:
            text = fetch_metrics(ports[0])
        except (urllib.error.URLError, OSError) as e:
            print(f"GET http://127.0.0.1:{ports[0]}/metrics failed: {e}", file=sys.stderr)
            return 1
        fams = metric_families(text)
        where = (f"port {ports[0]}" if len(ports) == 1
                 else f"{len(ports)} gateway workers on ports {ports} "
                      f"(schema from :{ports[0]})")
        print(f"# {len(fams)} families on {where}")
        for name in sorted(fams):
            mark = "*" if name in WANTED else " "
            print(f"{mark} {name:48s} labels: {'; '.join(sorted(fams[name]))}")
        print("# * = flattened into the monitor record (summed across workers)")
        return 0

    out_path = args.out or _default_out()
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)

    if args.demo is not None:
        n = max(args.demo, 1)
        cores = psutil.cpu_count() or 8
        t0 = time.time() - n
        with open(out_path, "w", encoding="utf-8") as f:
            for i in range(n):
                f.write(json.dumps(demo_record(i, n, t0, cores), separators=(",", ":")) + "\n")
        print(f"wrote {n} demo lines to {out_path}")
        return 0

    stop = {"flag": False}

    def _stop(_sig, _frm):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"monitor -> {out_path} every {args.interval}s "
          f"(gateway port {'auto' if gateway_port is None else gateway_port}); Ctrl-C to stop",
          file=sys.stderr)
    n = 0
    with open(out_path, "a", encoding="utf-8") as f:
        # prime cpu_percent so the first written sample is a real interval
        fleet.discover()
        for ps in fleet.discover().values():
            for q in ps:
                try:
                    q.cpu_percent(None)
                except psutil.Error:
                    pass
        next_at = time.monotonic() + args.interval
        while not stop["flag"]:
            try:
                rec = make_record(fleet, gateway_port)
            except Exception as e:  # noqa: BLE001 - the monitor must outlive anything
                rec = {"ts": round(time.time(), 3), "error": f"{type(e).__name__}: {e}"[:300]}
            f.write(json.dumps(rec, separators=(",", ":"), allow_nan=False) + "\n")
            f.flush()
            n += 1
            if not args.quiet:
                try:
                    line = (summary_line(rec) if "error" not in rec
                            else f"sample error: {rec['error']}")
                    print(line, flush=True)
                except (KeyError, TypeError):
                    print(json.dumps(rec)[:200], flush=True)
            # sleep in small steps so Ctrl-C lands within ~100 ms
            while not stop["flag"]:
                remaining = next_at - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(remaining, 0.1))
            next_at += args.interval
            if next_at < time.monotonic():   # fell behind (box is busy): resync
                next_at = time.monotonic() + args.interval
    print(f"stopped after {n} samples -> {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

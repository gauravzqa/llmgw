"""The WebSocket scale harness: S9-S12 (PLAN-G 8.3), two arms, one report.

    python -m bench.ws_load --scenario S9 --arms d --sessions 20 --measure 30

Why this is a module beside `bench/load.py` and not a `ws` worker kind inside
it. The brief allowed either; `load.py` was read end to end first, and three
things decided it:

1. **The unit of measurement is different, all the way down.** `WorkerResult`
   is a request: `ttfe`, `total`, inter-event `gaps`, a status code, a
   `[DONE]` terminator, `refused_before_byte`. A WebSocket session has none of
   those and needs what it does not have -- a latency histogram PER DIRECTION,
   a byte ledger per direction, close codes, per-product terminals. Threading
   that through `WorkerSpec`, `WorkerResult`, `ArmResult`, `arm_tables`,
   `paired_delta`, `assert_instrument` and `calibration_verdict` would leave
   every one of them with a branch on `kind == "ws"`.
2. **The arrival model is different.** S1-S8 are open-model Poisson arrivals;
   S9-S12 are a fixed population of long-lived sockets, ramped. The arrival
   loop -- the heart of `_worker_main` -- would be bypassed entirely.
3. **S1-S8 are re-run for comparison after G1 and G5** (PLAN-G 8.3's last
   line), and the comparison is worth something only if the instrument did not
   change underneath it. A refactor of `load.py`'s worker between the two
   campaigns costs exactly that.

So this file reuses everything that is genuinely shared and duplicates
nothing: `LogHistogram`, `merge_all`, `Sampler`, `_agg_samples`, `Fleet`,
`launch_fleet`, `env_snapshot`, the histogram and paired-delta renderers, and
the S8 SIGTERM driver's shape all come from `bench.load` by import. The report
comes out in the same format.

Arms
----
Arm D connects straight at the fake (`fakes/ws.py`), Arm G at the gateway's
route of the same name. The four paths are the plugin's own
(`/tts/v1/voice:streamBidirectional`, `/stt/v1/transcribe:streamBidirectional`,
`/v1/realtime`, `/v3/ws`), so one template serves both and `--arms dg` is the
paired measurement. Until the gateway registers them, `--arms d` is the only
one that runs, and that is the default when `--arms` is not given.

What the two latency histograms mean
------------------------------------
There is no echo in these protocols and no timestamp in any captured frame, so
"added frame latency" is measured twice, once per direction, from what the
wire actually offers:

* **up-rtt** -- a client frame to the server frame it provokes (an Inworld STT
  `audioChunk` to the `transcription` it triggers; a `create` to its
  `contextCreated`). A ROUND TRIP: the added number is the gateway's cost on
  both legs for a small frame.
* **down-lag** -- a paced server audio chunk's arrival against its own
  cadence: `now - (flush_at + k * interval)`. One way, and the frames are the
  big ones (6-48 KB), so this is the number that moves when the relay copies
  badly.

Both arms drive the identical fake with the identical knobs, so the DIFFERENCE
at a matched quantile is the relay's cost; the absolute value of either is
mostly the machine.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import multiprocessing as mp
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field

import httpx

from bench.load import (
    _DELTA_HEADER,
    Fleet,
    LogHistogram,
    Sampler,
    _agg_samples,
    _delta_rows,
    _free_port,
    _hrow,
    _uvicorn_shutdown_s,
    _wait_http,
    launch_fleet,
    merge_all,
    parse_metrics,
    sample_fds_many,
    sample_rss_cpu_many,
    stop_all_fleets,
)
from bench.scenarios.ws import WS_SCENARIOS, Group, WsScenario

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --------------------------------------------------------------------------
# Worker: one process, one asyncio loop, a fixed population of sockets
# --------------------------------------------------------------------------


@dataclass
class WsWorkerSpec:
    """Everything a worker needs, picklable across the spawn boundary."""

    worker_id: int
    arm: str                     # "D" or "G"
    base: str                    # ws://host:port
    group: Group
    sessions: int                # this worker's share
    ramp_s: float
    warm_s: float
    measure_s: float
    close_at: float | None = None      # S11: the mass-disconnect instant,
    close_window_s: float = 1.0        #      measured from the WORKER's start
    open_timeout_s: float = 20.0       #      and jittered across this window


@dataclass
class WsWorkerResult:
    worker_id: int
    arm: str
    label: str
    opened: int = 0
    failed: int = 0
    closed_clean: int = 0
    frames_sent: int = 0
    frames_recv: int = 0
    bytes_sent: int = 0
    bytes_recv: int = 0
    errors_by_kind: dict = field(default_factory=dict)
    close_codes: dict = field(default_factory=dict)
    close_latency: dict = field(default_factory=lambda: LogHistogram().to_dict())
    up_rtt: dict = field(default_factory=lambda: LogHistogram().to_dict())
    down_lag: dict = field(default_factory=lambda: LogHistogram().to_dict())
    connect_s: dict = field(default_factory=lambda: LogHistogram().to_dict())
    peak_open: int = 0


def _classify(exc: BaseException) -> str:
    name = type(exc).__name__
    code = getattr(getattr(exc, "rcvd", None), "code", None)
    if code is not None:
        return f"{name}:{code}"
    if isinstance(exc, OSError):
        return f"os:{getattr(exc, 'errno', '?')}"
    return name


def _run_ws_worker(spec: WsWorkerSpec, out_q: mp.Queue) -> None:
    try:
        res = asyncio.run(_ws_worker_main(spec))
    except Exception as exc:  # never let a worker die silently
        res = WsWorkerResult(worker_id=spec.worker_id, arm=spec.arm,
                             label=spec.group.label,
                             errors_by_kind={f"worker-crash:{_classify(exc)}": 1})
    out_q.put(res)


class _Rec:
    """One worker's ledger. Sessions share it; nothing here is per-frame
    allocated, because S9 runs 400 sockets at 10 frames a second for five
    minutes and an instrument that allocates per frame measures itself."""

    def __init__(self, spec: WsWorkerSpec) -> None:
        self.spec = spec
        self.up = LogHistogram()
        self.down = LogHistogram()
        self.connect = LogHistogram()
        self.close_lat = LogHistogram()
        self.res = WsWorkerResult(worker_id=spec.worker_id, arm=spec.arm,
                                  label=spec.group.label)
        self.open_now = 0

    def opened(self) -> None:
        self.res.opened += 1
        self.open_now += 1
        self.res.peak_open = max(self.res.peak_open, self.open_now)

    def closed(self, code: int | None) -> None:
        self.open_now -= 1
        key = str(code) if code is not None else "abort-1006"
        self.res.close_codes[key] = self.res.close_codes.get(key, 0) + 1

    def error(self, exc: BaseException) -> None:
        kind = _classify(exc)
        self.res.errors_by_kind[kind] = self.res.errors_by_kind.get(kind, 0) + 1

    def sent(self, payload: str | bytes) -> None:
        self.res.frames_sent += 1
        self.res.bytes_sent += (len(payload.encode()) if isinstance(payload, str)
                                else len(payload))

    def recv(self, payload: str | bytes) -> None:
        self.res.frames_recv += 1
        self.res.bytes_recv += (len(payload.encode()) if isinstance(payload, str)
                                else len(payload))

    def finish(self) -> WsWorkerResult:
        self.res.up_rtt = self.up.to_dict()
        self.res.down_lag = self.down.to_dict()
        self.res.connect_s = self.connect.to_dict()
        self.res.close_latency = self.close_lat.to_dict()
        return self.res


async def _ws_worker_main(spec: WsWorkerSpec) -> WsWorkerResult:
    rec = _Rec(spec)
    started = time.monotonic()
    gap = spec.ramp_s / max(1, spec.sessions)
    # S11's disconnect is one INSTANT for the whole population, jittered
    # across `close_window_s` -- "1,000 clients vanish within a second", not
    # "each client vanishes N seconds after its own ramp slot".
    close_base = (started + spec.close_at) if spec.close_at is not None else None
    tasks = [
        asyncio.ensure_future(_session(
            rec, i, started + i * gap,
            None if close_base is None
            else close_base + spec.close_window_s * (i / max(1, spec.sessions))))
        for i in range(spec.sessions)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    return rec.finish()


async def _session(rec: _Rec, index: int, start_at: float,
                   close_at: float | None = None) -> None:
    """One socket, from the ramp slot it was given until the window ends.

    The close code is read AFTER the context manager has closed the socket:
    inside it, `close_code` is still None on a healthy session and every clean
    close would be filed as an abort. What S12 scores -- did this client see
    4900? -- lives entirely in that one attribute, so it is read last."""
    from websockets.asyncio.client import connect

    spec, g = rec.spec, rec.spec.group
    await asyncio.sleep(max(0.0, start_at - time.monotonic()))
    deadline = start_at + spec.warm_s + spec.measure_s
    url = f"{spec.base}{g.path}{g.query}"
    headers = g.headers()
    t0 = time.monotonic()
    client = None
    try:
        async with connect(url, additional_headers=headers,
                           open_timeout=spec.open_timeout_s, close_timeout=5,
                           max_size=None, ping_interval=20) as client:
            rec.connect.record(time.monotonic() - t0)
            rec.opened()
            await _PRODUCTS[g.product](rec, client, deadline, close_at)
            rec.res.closed_clean += 1
    except Exception as exc:  # noqa: BLE001 -- every failure is a datum
        rec.res.failed += 1
        rec.error(exc)
    finally:
        if client is not None:
            rec.closed(client.protocol.close_code)


async def _send(rec: _Rec, client, payload: str | bytes) -> None:
    await client.send(payload)
    rec.sent(payload)


def _b64(nbytes: int) -> str:
    return base64.b64encode(b"\x00" * max(0, nbytes)).decode("ascii")


async def _read_until(rec: _Rec, client, deadline: float,
                      on_frame=None) -> None:
    """Drain the socket until the window ends or the peer closes. Every frame
    is weighed; `on_frame` sees the parsed JSON when the caller cares."""
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        try:
            payload = await asyncio.wait_for(client.recv(), remaining)
        except TimeoutError:
            return
        rec.recv(payload)
        if on_frame is not None and isinstance(payload, str):
            on_frame(payload)


# ---- the four product scripts -------------------------------------------


async def _stt_session(rec: _Rec, client, deadline: float,
                       close_at: float | None) -> None:
    """Inworld STT, probe 6's shape held open: config, then audio at
    `send_interval`, with one `transcription` back every `events` chunks. The
    marked chunks are what `up-rtt` is measured on."""
    g = rec.spec.group
    await _send(rec, client, json.dumps({"transcribeConfig": {
        "modelId": "inworld/inworld-stt-1", "audioEncoding": "LINEAR16",
        "sampleRateHertz": 16000, "numberOfChannels": 1, "language": "en-US"}}))
    chunk = json.dumps({"audioChunk": {"content": _b64(g.send_bytes)}})
    marks: list[float] = []

    def on_frame(payload: str) -> None:
        if '"transcription"' in payload and marks:
            rec.up.record(time.monotonic() - marks.pop(0))

    async def sender() -> None:
        n = 0
        nxt = time.monotonic()
        while time.monotonic() < deadline:
            nxt += g.send_interval or 0.1
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))
            if time.monotonic() >= deadline:
                return
            n += 1
            if g.events and n % g.events == 0:
                marks.append(time.monotonic())
            await _send(rec, client, chunk)

    reader = asyncio.ensure_future(_read_until(rec, client, deadline, on_frame))
    send_task = asyncio.ensure_future(sender())
    await _hold(rec, client, deadline, close_at, (reader, send_task))
    if close_at is None:
        await _send(rec, client, json.dumps({"closeStream": {}}))
        with_timeout = asyncio.ensure_future(_read_until(rec, client,
                                                         time.monotonic() + 2.0))
        await with_timeout


async def _tts_session(rec: _Rec, client, deadline: float,
                       close_at: float | None) -> None:
    """Inworld TTS, probe 1's shape with one long flush: `create`,
    `send_text`, `flush_context`, then `events` chunks paced at `interval`.
    `down-lag` is each chunk's arrival against `flush_at + k * interval`."""
    g = rec.spec.group
    cid = f"ctx-{rec.spec.worker_id}-{rec.res.opened}"
    t0 = time.monotonic()
    await _send(rec, client, json.dumps({"create": {
        "modelId": "inworld-tts-1.5-mini", "voiceId": "Aarav",
        "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 16000},
        "autoMode": True}, "contextId": cid}))
    first = await asyncio.wait_for(client.recv(), 20.0)
    rec.recv(first)
    rec.up.record(time.monotonic() - t0)

    await _send(rec, client, json.dumps({"send_text": {"text": "x" * g.text_chars},
                                         "contextId": cid}))
    await _send(rec, client, json.dumps({"flush_context": {}, "contextId": cid}))
    flush_at = time.monotonic()
    interval = g.interval or 0.0
    k = [0]

    def on_frame(payload: str) -> None:
        if '"audioChunk"' not in payload:
            return
        k[0] += 1
        if interval:
            rec.down.record(max(0.0, time.monotonic() - (flush_at + k[0] * interval)))

    reader = asyncio.ensure_future(_read_until(rec, client, deadline, on_frame))
    await _hold(rec, client, deadline, close_at, (reader,))
    if close_at is None:
        await _send(rec, client, json.dumps({"close_context": {}, "contextId": cid}))
        await _read_until(rec, client, time.monotonic() + 2.0)


async def _realtime_session(rec: _Rec, client, deadline: float,
                            close_at: float | None) -> None:
    """OpenAI Realtime. In `idle` mode (S10) this is probe 9: read
    `session.created`, then sit there while the server pings."""
    t0 = time.monotonic()
    first = await asyncio.wait_for(client.recv(), 20.0)
    rec.recv(first)
    rec.up.record(time.monotonic() - t0)
    reader = asyncio.ensure_future(_read_until(rec, client, deadline))
    await _hold(rec, client, deadline, close_at, (reader,))


async def _aai_session(rec: _Rec, client, deadline: float,
                       close_at: float | None) -> None:
    """AssemblyAI: `Begin`, then binary audio up and `Turn`s down."""
    g = rec.spec.group
    first = await asyncio.wait_for(client.recv(), 20.0)
    rec.recv(first)
    audio = b"\x00" * g.send_bytes
    marks: list[float] = []

    def on_frame(payload: str) -> None:
        if '"Turn"' in payload and marks:
            rec.up.record(time.monotonic() - marks.pop(0))

    async def sender() -> None:
        n = 0
        nxt = time.monotonic()
        while time.monotonic() < deadline:
            nxt += g.send_interval or 0.1
            await asyncio.sleep(max(0.0, nxt - time.monotonic()))
            if time.monotonic() >= deadline:
                return
            n += 1
            if g.events and n % g.events == 0:
                marks.append(time.monotonic())
            await _send(rec, client, audio)

    reader = asyncio.ensure_future(_read_until(rec, client, deadline, on_frame))
    send_task = asyncio.ensure_future(sender())
    await _hold(rec, client, deadline, close_at, (reader, send_task))
    if close_at is None:
        await _send(rec, client, json.dumps({"type": "Terminate"}))
        await _read_until(rec, client, time.monotonic() + 2.0)


_PRODUCTS = {
    "inworld-stt": _stt_session,
    "inworld-tts": _tts_session,
    "openai-realtime": _realtime_session,
    "assemblyai": _aai_session,
}


async def _hold(rec: _Rec, client, deadline: float, close_at: float | None,
                tasks) -> None:
    """Run the session's tasks until the window ends, or until S11's mass
    disconnect instant, whichever comes first.

    The disconnect is deliberately RUDE: the socket is aborted, not closed, so
    the gateway sees a vanished client rather than a polite 1000. That is the
    event S11 is about."""
    if close_at is not None:
        await asyncio.sleep(max(0.0, close_at - time.monotonic()))
        for t in tasks:
            t.cancel()
        try:
            client.transport.abort()
        except Exception:  # noqa: BLE001 -- already gone
            pass
        return
    await asyncio.gather(*tasks, return_exceptions=True)


# --------------------------------------------------------------------------
# One arm
# --------------------------------------------------------------------------


@dataclass
class WsArmResult:
    arm: str
    label: str
    sessions: int
    workers: int
    wall_s: float
    opened: int
    failed: int
    closed_clean: int
    frames_sent: int
    frames_recv: int
    bytes_sent: int
    bytes_recv: int
    errors_by_kind: dict
    close_codes: dict
    peak_open: int
    up_rtt: LogHistogram
    down_lag: LogHistogram
    connect_s: LogHistogram
    close_latency: LogHistogram
    fake_ws: dict = field(default_factory=dict)
    samples: dict = field(default_factory=dict)

    inflight_allowance: int = 0
    """Bytes the fake had already written when the window closed on a stream it
    was still sending. Not loss: a five-minute S9 window deliberately truncates
    a 3,000-chunk flush, and the frame in flight at that instant is counted by
    the fake and never read by the client. One frame per still-streaming
    session, no more -- anything above that IS loss."""

    @property
    def bytes_in_loss(self) -> int:
        """Client bytes sent minus what the fake says it received. Exact: the
        client stops sending before it closes, so there is nothing in flight."""
        return self.bytes_sent - int(self.fake_ws.get("bytes_in", 0))

    @property
    def bytes_out_loss(self) -> int:
        return int(self.fake_ws.get("bytes_out", 0)) - self.bytes_recv

    @property
    def byte_ledger_ok(self) -> bool:
        return (self.bytes_in_loss == 0
                and 0 <= self.bytes_out_loss <= self.inflight_allowance)


def fake_ws_stats(fake_base: str) -> dict:
    with httpx.Client(timeout=10.0) as c:
        return c.get(f"{fake_base}/__stats").json()["ws"]


def reset_fake_stats(fake_base: str) -> None:
    with httpx.Client(timeout=10.0) as c:
        c.post(f"{fake_base}/__stats/reset").raise_for_status()


def run_ws_arm(*, arm: str, base: str, scenario: WsScenario, groups: list[Group],
               workers: int, fake_base: str, sample_pids: list[int],
               metrics_urls: list[str], gw_ports: set[int], fake_ports: set[int],
               sample_interval: float, close_at: float | None = None,
               close_window_s: float = 1.0) -> WsArmResult:
    """Every group's sessions, spread over `workers` processes, all at once."""
    ctx = mp.get_context("spawn")
    out_q: mp.Queue = ctx.Queue()
    specs: list[WsWorkerSpec] = []
    for g in groups:
        per = [g.sessions // workers] * workers
        for i in range(g.sessions % workers):
            per[i] += 1
        for n in per:
            if n:
                specs.append(WsWorkerSpec(
                    worker_id=len(specs), arm=arm, base=base, group=g, sessions=n,
                    ramp_s=scenario.ramp_s, warm_s=scenario.warm_s,
                    measure_s=scenario.measure_s, close_at=close_at,
                    close_window_s=close_window_s))

    reset_fake_stats(fake_base)
    sampler = Sampler(sample_pids, metrics_urls, sorted(gw_ports), fake_ports,
                      sample_interval)
    sampler.start()
    t0 = time.monotonic()
    procs = [ctx.Process(target=_run_ws_worker, args=(s, out_q)) for s in specs]
    results: list[WsWorkerResult] = []
    try:
        for p in procs:
            p.start()
        while len(results) < len(procs):
            try:
                results.append(out_q.get(timeout=1.0))
                continue
            except Exception:
                pass
            if all(not p.is_alive() for p in procs):
                seen = {r.worker_id for r in results}
                for s in specs:
                    if s.worker_id not in seen:
                        results.append(WsWorkerResult(
                            worker_id=s.worker_id, arm=arm, label=s.group.label,
                            errors_by_kind={"worker-crash:no-result": 1}))
                break
        for p in procs:
            p.join(timeout=30)
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
        sampler.stop()
        sampler.join(timeout=5)
    wall = time.monotonic() - t0

    label = "+".join(g.label for g in groups)
    return WsArmResult(
        arm=arm, label=label, sessions=sum(g.sessions for g in groups),
        workers=len(specs), wall_s=wall,
        opened=sum(r.opened for r in results),
        failed=sum(r.failed for r in results),
        closed_clean=sum(r.closed_clean for r in results),
        frames_sent=sum(r.frames_sent for r in results),
        frames_recv=sum(r.frames_recv for r in results),
        bytes_sent=sum(r.bytes_sent for r in results),
        bytes_recv=sum(r.bytes_recv for r in results),
        errors_by_kind=_merge_counts(r.errors_by_kind for r in results),
        close_codes=_merge_counts(r.close_codes for r in results),
        peak_open=sum(r.peak_open for r in results),
        up_rtt=merge_all([LogHistogram.from_dict(r.up_rtt) for r in results]),
        down_lag=merge_all([LogHistogram.from_dict(r.down_lag) for r in results]),
        connect_s=merge_all([LogHistogram.from_dict(r.connect_s) for r in results]),
        close_latency=merge_all(
            [LogHistogram.from_dict(r.close_latency) for r in results]),
        fake_ws=fake_ws_stats(fake_base),
        samples=_agg_samples(sampler.samples),
        inflight_allowance=sum(
            g.sessions * (g.nbytes * 4 // 3 + 512) for g in groups if g.interval),
    )


def _merge_counts(dicts) -> dict:
    out: dict = {}
    for d in dicts:
        for k, v in d.items():
            out[k] = out.get(k, 0) + v
    return out


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def ws_arm_tables(r: WsArmResult) -> str:
    sent_mb = r.bytes_sent / 1e6
    recv_mb = r.bytes_recv / 1e6
    p = [f"### Arm {r.arm} -- {r.sessions} sessions ({r.label}), "
         f"{r.workers} worker processes, wall {r.wall_s:.1f}s",
         f"sessions: opened={r.opened} failed={r.failed} "
         f"closed_clean={r.closed_clean} peak_open(sum over workers)={r.peak_open}",
         f"frames: sent={r.frames_sent} recv={r.frames_recv}",
         f"bytes: client sent={r.bytes_sent} ({sent_mb:.1f} MB), "
         f"client received={r.bytes_recv} ({recv_mb:.1f} MB)",
         f"fake counters: {json.dumps(_fake_summary(r.fake_ws))}",
         f"byte ledger: up loss={r.bytes_in_loss} B (client sent - fake "
         f"bytes_in; must be 0), down {r.bytes_out_loss} B unread of "
         f"{r.fake_ws.get('bytes_out')} (fake bytes_out - client received; "
         f"in-flight allowance {r.inflight_allowance} B = one frame per "
         f"still-streaming session)",
         "",
         "| metric (ms)    |       n |     p50 |     p90 |     p99 |    p99.9 |    mean |",
         "|----------------|---------|---------|---------|---------|----------|---------|"]
    for name, hist in (("up-rtt", r.up_rtt), ("down-lag", r.down_lag),
                       ("connect", r.connect_s), ("close-lat", r.close_latency)):
        if hist.total:
            p.append(_hrow(name, hist))
    if r.close_codes:
        p.append("")
        p.append(f"close codes: {r.close_codes}")
    if r.errors_by_kind:
        p.append(f"errors: {r.errors_by_kind}")
    if r.samples:
        p.append("")
        p.append(f"process samples: {json.dumps(r.samples, default=str)}")
    return "\n".join(p)


def _fake_summary(ws: dict) -> dict:
    keep = ("ws_open", "ws_open_now", "ws_peak_open", "ws_closed_by_client",
            "ws_closed_by_server", "terminates_received", "bytes_in", "bytes_out",
            "frames_in", "frames_out")
    out = {k: ws.get(k) for k in keep}
    out["client_frames"] = ws.get("client_frames", {})
    return out


def ws_paired_delta(d: WsArmResult, g: WsArmResult) -> str:
    parts = []
    for name, dh, gh in (("up-rtt (client frame -> its acknowledgement; a ROUND "
                          "TRIP)", d.up_rtt, g.up_rtt),
                         ("down-lag (paced audio chunk vs its cadence; ONE WAY)",
                          d.down_lag, g.down_lag)):
        if dh.total and gh.total:
            parts.append(f"added {name}, matched-quantile:\n"
                         + _DELTA_HEADER + "\n".join(_delta_rows(dh, gh)))
    return "\n\n".join(parts) if parts else "(one arm only: no paired delta)"


def _added_p99_ms(d: WsArmResult, g: WsArmResult) -> float:
    worst = float("nan")
    for dh, gh in ((d.up_rtt, g.up_rtt), (d.down_lag, g.down_lag)):
        if dh.total and gh.total:
            added = (gh.percentile(99) - dh.percentile(99)) * 1e3
            if math.isnan(worst) or added > worst:
                worst = added
    return worst


def verdict_lines(checks: list[tuple[str, bool | None, str]],
                  measured: bool = True) -> list[str]:
    """`measured=False` turns every verdict into NOT SCORED. A scenario whose
    sessions never opened has not passed its criteria; it has failed to run,
    and a report that prints PASS for an empty measurement is worse than one
    that prints nothing."""
    out = ["## Pass criteria"]
    if not measured:
        out.append("- NOTHING WAS MEASURED: no session opened. Every criterion "
                   "below is unscored.")
    for name, ok, detail in checks:
        if not measured:
            ok = None
        mark = "PASS" if ok else ("FAIL" if ok is False else "NOT SCORED")
        out.append(f"- [{mark}] {name}: {detail}")
    return out


def report_header(scenario: WsScenario, arms: list[str], sessions: int,
                  measure_s: float, workers: int, env: dict) -> list[str]:
    return [f"# Scenario {scenario.sid}: {scenario.name}",
            f"_{scenario.question}_", "",
            f"produces: {scenario.produces}", "",
            f"env: {json.dumps(env, default=str)}", "",
            f"config: arms={','.join(arms)} sessions={sessions} "
            f"warm={scenario.warm_s}s measure={measure_s}s "
            f"ramp={scenario.ramp_s}s worker-processes={workers}", ""]


# --------------------------------------------------------------------------
# Fleets: the fake alone (Arm D) or the fake plus a gateway (Arm G)
# --------------------------------------------------------------------------


@dataclass
class FakeOnly:
    """What Arm D needs, and nothing more. `launch_fleet` also starts a
    gateway, and an Arm-D run must not fail because the gateway's config is
    mid-change in another agent's working tree."""

    proc: subprocess.Popen
    openai_port: int
    anthropic_port: int

    @property
    def fake_base(self) -> str:
        return f"http://127.0.0.1:{self.openai_port}"

    @property
    def ws_base(self) -> str:
        return f"ws://127.0.0.1:{self.openai_port}"

    @property
    def fake_ports(self) -> set[int]:
        return {self.openai_port, self.anthropic_port}

    def stop(self) -> None:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()


def launch_fake_only(fake_workers: int = 1) -> FakeOnly:
    openai_port, anthropic_port = _free_port(), _free_port()
    argv = [sys.executable, "-m", "fakes.upstream",
            "--openai-port", str(openai_port),
            "--anthropic-port", str(anthropic_port),
            "--log-level", "critical"]
    if fake_workers > 1:
        argv += ["--workers", str(fake_workers)]
    proc = subprocess.Popen(argv, cwd=ROOT, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    fake = FakeOnly(proc, openai_port, anthropic_port)
    if not _wait_http(f"{fake.fake_base}/__stats", 15.0):
        fake.stop()
        raise RuntimeError("fake upstream failed to start")
    return fake


def gw_ws_base(fleet: Fleet) -> str:
    return f"ws://127.0.0.1:{fleet.gw_port}"


# --------------------------------------------------------------------------
# Drivers
# --------------------------------------------------------------------------


def run_s9(scenario: WsScenario, opts: argparse.Namespace) -> str:
    """S9: both directions saturated, Arm D vs Arm G, byte-for-byte."""
    from bench.load import env_snapshot

    groups = list(scenario.groups)
    env = env_snapshot()
    out = report_header(scenario, opts.arms, sum(g.sessions for g in groups),
                        scenario.measure_s, opts.workers, env)
    arms: dict[str, WsArmResult] = {}
    fake = fleet = None
    try:
        if "G" in opts.arms:
            fleet = launch_fleet(unlimited=scenario.unlimited, tenants_file=None)
            fake_base, fake_ports = fleet.fake_base, fleet.fake_ports
        else:
            fake = launch_fake_only(opts.fake_workers)
            fake_base, fake_ports = fake.fake_base, fake.fake_ports
        fake_pid = fleet.fake_proc.pid if fleet else fake.proc.pid
        for arm in opts.arms:
            base = (gw_ws_base(fleet) if arm == "G"
                    else f"ws://127.0.0.1:{(fleet or fake).openai_port}")
            print(f"[{scenario.sid}] Arm {arm}: {sum(g.sessions for g in groups)} "
                  f"sessions at {base} ...", flush=True)
            arms[arm] = run_ws_arm(
                arm=arm, base=base, scenario=scenario, groups=groups,
                workers=opts.workers, fake_base=fake_base,
                sample_pids=(fleet.gw_pids if arm == "G" and fleet else [fake_pid]),
                metrics_urls=(fleet.metrics_urls if arm == "G" and fleet else []),
                gw_ports=(set(fleet.gw_ports) if arm == "G" and fleet
                          else set(fake_ports)),
                fake_ports=(fake_ports if arm == "G" else set()),
                sample_interval=opts.sample_interval)
    finally:
        if fleet:
            fleet.stop()
        if fake:
            fake.stop()

    for arm in opts.arms:
        out.append(ws_arm_tables(arms[arm]))
        out.append("")
    if "D" in arms and "G" in arms:
        out.append(ws_paired_delta(arms["D"], arms["G"]))
        out.append("")

    ref = arms.get("G") or arms["D"]
    added = _added_p99_ms(arms["D"], arms["G"]) if len(arms) == 2 else float("nan")
    cpu = ref.samples.get("peak_cpu_pct")
    per_session = (cpu / ref.sessions) if isinstance(cpu, int | float) and cpu else None
    checks = [
        ("p99 added latency <= 10 ms at 1 process",
         None if math.isnan(added) else added <= 10.0,
         "Arm G not run (no gateway route yet)" if math.isnan(added)
         else f"worst added p99 across directions = {added:+.2f} ms"),
        ("zero byte loss (up exact; down within the in-flight allowance)",
         all(a.byte_ledger_ok for a in arms.values()),
         "; ".join(f"Arm {a.arm}: up {a.bytes_in_loss:+d} B, down "
                   f"{a.bytes_out_loss:+d} B unread of an allowance of "
                   f"{a.inflight_allowance} B" for a in arms.values())),
        ("CPU per session recorded", True,
         "; ".join(f"Arm {a.arm}: peak_cpu_pct={a.samples.get('peak_cpu_pct')} "
                   f"over {a.sessions} sessions = "
                   f"{(a.samples.get('peak_cpu_pct') or 0) / max(1, a.sessions):.3f} "
                   f"%CPU/session" for a in arms.values())),
        ("no session failed to open",
         all(a.failed == 0 for a in arms.values()),
         "; ".join(f"Arm {a.arm}: {a.failed} failed, {a.errors_by_kind}"
                   for a in arms.values())),
    ]
    if per_session is not None:
        out.append(f"CPU per session (reference arm {ref.arm}): "
                   f"{per_session:.3f} %CPU/session\n")
    out += verdict_lines(checks)
    return "\n".join(out)


def run_s10(scenario: WsScenario, opts: argparse.Namespace) -> str:
    """S10: thousands of idle sockets. The load is the absence of load; what is
    measured is the process."""
    from bench.load import env_snapshot

    groups = list(scenario.groups)
    sessions = sum(g.sessions for g in groups)
    env = env_snapshot()
    out = report_header(scenario, opts.arms, sessions, scenario.measure_s,
                        opts.workers, env)
    arms: dict[str, WsArmResult] = {}
    baselines: dict[str, tuple[float, int]] = {}
    fake = fleet = None
    try:
        if "G" in opts.arms:
            fleet = launch_fleet(unlimited=scenario.unlimited, tenants_file=None)
            fake_base, fake_ports = fleet.fake_base, fleet.fake_ports
        else:
            fake = launch_fake_only(opts.fake_workers)
            fake_base, fake_ports = fake.fake_base, fake.fake_ports
        fake_pid = fleet.fake_proc.pid if fleet else fake.proc.pid
        for arm in opts.arms:
            pids = fleet.gw_pids if arm == "G" and fleet else [fake_pid]
            ports = (set(fleet.gw_ports) if arm == "G" and fleet else set(fake_ports))
            # Baseline BEFORE the sockets: the marginal cost is what matters and
            # an idle uvicorn is not free.
            baselines[arm] = _baseline(pids, ports, fake_ports if arm == "G" else set())
            base = (gw_ws_base(fleet) if arm == "G"
                    else f"ws://127.0.0.1:{(fleet or fake).openai_port}")
            print(f"[{scenario.sid}] Arm {arm}: {sessions} idle sessions at {base} ...",
                  flush=True)
            arms[arm] = run_ws_arm(
                arm=arm, base=base, scenario=scenario, groups=groups,
                workers=opts.workers, fake_base=fake_base, sample_pids=pids,
                metrics_urls=(fleet.metrics_urls if arm == "G" and fleet else []),
                gw_ports=ports,
                fake_ports=(fake_ports if arm == "G" else set()),
                sample_interval=opts.sample_interval)
    finally:
        if fleet:
            fleet.stop()
        if fake:
            fake.stop()

    checks = []
    for arm, r in arms.items():
        out.append(ws_arm_tables(r))
        base_rss, base_fds = baselines[arm]
        peak_rss = r.samples.get("peak_rss_kib") or float("nan")
        peak_fds = r.samples.get("peak_fd_inbound") or 0
        marginal = ((peak_rss - base_rss) / max(1, r.opened)) if r.opened else float("nan")
        fds_per = (peak_fds - base_fds) / max(1, r.opened)
        expected_fds = 2.0 if arm == "G" else 1.0
        if r.opened < 100:
            out.append(f"NOTE: {r.opened} sockets is too few for a marginal "
                       "RSS number -- the allocator's fixed growth is divided "
                       "by a small N and reads as per-socket cost. The "
                       "criterion means something at the scenario's 2,000.")
        out.append(f"baseline rss={base_rss:.0f} KiB fds={base_fds}; "
                   f"peak rss={peak_rss:.0f} KiB fds={peak_fds}; "
                   f"marginal={marginal:.1f} KiB/socket, {fds_per:.2f} fds/session "
                   f"(expected ~{expected_fds:.0f})")
        out.append("")
        checks += [
            (f"Arm {arm}: <= 100 KiB marginal RSS per idle socket",
             (not math.isnan(marginal)) and marginal <= 100.0,
             f"{marginal:.1f} KiB/socket over {r.opened} sockets"),
            (f"Arm {arm}: fds per session",
             abs(fds_per - expected_fds) <= 0.5,
             f"{fds_per:.2f} measured, {expected_fds:.0f} expected"),
            (f"Arm {arm}: no idle close before the idle budget",
             r.close_codes.get("1006", 0) == 0 and r.failed == 0,
             f"close codes {r.close_codes}, {r.failed} failures, "
             f"errors {r.errors_by_kind}"),
        ]
    out += verdict_lines(checks)
    return "\n".join(out)


def _baseline(pids: list[int], gw_ports: set[int],
              fake_ports: set[int]) -> tuple[float, int]:
    """RSS (KiB) and inbound fds with nothing connected."""
    rss = sum(v[0] for v in sample_rss_cpu_many(pids).values()
              if not math.isnan(v[0]))
    fds = sum(v[0] for v in sample_fds_many(pids, gw_ports, fake_ports).values())
    return rss, fds


def run_s11(scenario: WsScenario, opts: argparse.Namespace) -> str:
    """S11: 1,000 clients vanish inside a second, with the gateway's stderr on
    a PIPE that nobody ever reads (finding 41). Arm G only -- there is nothing
    to tear down without a gateway."""
    from bench.load import env_snapshot

    if "G" not in opts.arms:
        return (f"# Scenario {scenario.sid}: {scenario.name}\n\n"
                "SKIPPED: S11 measures the gateway's teardown path and needs "
                "Arm G. Re-run with `--arms g` once the WebSocket routes are "
                "registered.")
    groups = list(scenario.groups)
    env = env_snapshot()
    out = report_header(scenario, ["G"], sum(g.sessions for g in groups),
                        scenario.measure_s, opts.workers, env)
    fake = launch_fake_only(opts.fake_workers)
    gw = stderr_pipe = None
    healthz_fails = [0]
    stop_probe = threading.Event()
    try:
        gw, port = _launch_gateway_with_pipe(fake)
        stderr_pipe = gw.stderr
        probe = threading.Thread(target=_healthz_probe,
                                 args=(f"http://127.0.0.1:{port}/healthz",
                                       stop_probe, healthz_fails), daemon=True)
        probe.start()
        disconnect_at = float(scenario.extra.get("disconnect_at", 20.0))
        r = run_ws_arm(
            arm="G", base=f"ws://127.0.0.1:{port}", scenario=scenario,
            groups=groups, workers=opts.workers, fake_base=fake.fake_base,
            sample_pids=[gw.pid], metrics_urls=[f"http://127.0.0.1:{port}/metrics"],
            gw_ports={port}, fake_ports=fake.fake_ports,
            sample_interval=opts.sample_interval, close_at=disconnect_at,
            close_window_s=float(scenario.extra.get("disconnect_window_s", 1.0)))
        # How long until the gateway has let go of every upstream?
        t0 = time.monotonic()
        closed_at = None
        while time.monotonic() - t0 < 30.0:
            if fake_ws_stats(fake.fake_base).get("ws_open_now", 1) == 0:
                closed_at = time.monotonic() - t0
                break
            time.sleep(0.1)
        settle = float(scenario.extra.get("settle_s", 30.0))
        time.sleep(settle)
        after = _sample_once(gw.pid, {port}, fake.fake_ports,
                             f"http://127.0.0.1:{port}/metrics")
        stop_probe.set()
        probe.join(timeout=5)
        stderr_bytes = _drain_pipe(gw)
    finally:
        stop_probe.set()
        if gw and gw.poll() is None:
            gw.terminate()
            try:
                gw.wait(timeout=10)
            except subprocess.TimeoutExpired:
                gw.kill()
        if stderr_pipe:
            stderr_pipe.close()
        fake.stop()

    out.append(ws_arm_tables(r))
    out.append("")
    out.append(f"after {settle:.0f}s settle: {json.dumps(after, default=str)}")
    out.append("")
    out += verdict_lines([
        ("all upstream sockets closed <= 5 s",
         closed_at is not None and closed_at <= 5.0,
         f"{closed_at:.2f}s" if closed_at is not None
         else "still open after 30 s"),
        ("stderr < 4 KiB total", stderr_bytes < 4096,
         f"{stderr_bytes} bytes on a pipe nobody read"),
        ("/healthz 200 throughout", healthz_fails[0] == 0,
         f"{healthz_fails[0]} non-200 or failed probes"),
        ("no fd or task drift after 30 s",
         after["fd_inbound"] <= 4 and after["fd_upstream"] <= 4,
         f"fds inbound={after['fd_inbound']} upstream={after['fd_upstream']}, "
         f"tasks={after['metrics'].get('llmgw_tasks')}"),
    ], measured=r.opened > 0)
    return "\n".join(out)


def _launch_gateway_with_pipe(fake: FakeOnly) -> tuple[subprocess.Popen, int]:
    """One `bench._gwproc` with stderr on a PIPE that is never read until the
    end. That is the hostile shape finding 41 is about: a 64 KiB pipe, a
    process that logs per-socket, and an exit that never comes."""
    port = _free_port()
    env = dict(os.environ)
    env.update(BENCH_GW_PORT=str(port),
               BENCH_FAKE_OPENAI_URL=fake.fake_base,
               BENCH_FAKE_ANTHROPIC_URL=f"http://127.0.0.1:{fake.anthropic_port}",
               BENCH_GW_UNLIMITED="1")
    proc = subprocess.Popen([sys.executable, "-m", "bench._gwproc"], cwd=ROOT,
                            env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.PIPE)
    if not _wait_http(f"http://127.0.0.1:{port}/healthz", 25.0):
        proc.terminate()
        raise RuntimeError("gateway failed to start (stderr is on an unread pipe)")
    return proc, port


def _healthz_probe(url: str, stop: threading.Event, fails: list[int]) -> None:
    with httpx.Client(timeout=2.0) as c:
        while not stop.is_set():
            try:
                if c.get(url).status_code != 200:
                    fails[0] += 1
            except Exception:
                fails[0] += 1
            stop.wait(0.5)


def _drain_pipe(proc: subprocess.Popen) -> int:
    """Read whatever the process wrote to the pipe, at the very end. Counting
    the bytes is the point; printing them is not."""
    if not proc.stderr:
        return 0
    try:
        os.set_blocking(proc.stderr.fileno(), False)
        data = proc.stderr.read() or b""
    except Exception:
        return -1
    return len(data)


def _sample_once(pid: int, gw_ports: set[int], fake_ports: set[int],
                 metrics_url: str) -> dict:
    fds = sample_fds_many([pid], gw_ports, fake_ports).get(pid, (0, 0))
    rss = sample_rss_cpu_many([pid]).get(pid, (float("nan"), float("nan")))
    metrics: dict = {}
    try:
        with httpx.Client(timeout=5.0) as c:
            metrics = parse_metrics(c.get(metrics_url).text)
    except Exception:
        pass
    return {"fd_inbound": fds[0], "fd_upstream": fds[1], "rss_kib": rss[0],
            "metrics": metrics}


def run_s12(scenario: WsScenario, opts: argparse.Namespace) -> str:
    """S12: SIGTERM with sockets open. The S8 driver's deploy thread, pointed
    at WebSocket sessions; the contract is PLAN-G 4.3's per-product drain."""
    from bench.load import (
        budget_total_from_env,
        drain_allow_short_from_env,
        drain_grace_from_env,
        env_snapshot,
    )

    if "G" not in opts.arms:
        return (f"# Scenario {scenario.sid}: {scenario.name}\n\n"
                "SKIPPED: a deploy verdict needs a gateway to SIGTERM. Re-run "
                "with `--arms g` once the WebSocket routes are registered.")
    groups = list(scenario.groups)
    env = env_snapshot()
    out = report_header(scenario, ["G"], sum(g.sessions for g in groups),
                        scenario.measure_s, opts.workers, env)
    fleet = launch_fleet(unlimited=scenario.unlimited, tenants_file=None)
    grace = drain_grace_from_env()
    exit_expected = grace + _uvicorn_shutdown_s() + 5.0
    at = scenario.sigterm_at or 60.0
    deploy_at_abs = time.monotonic() + scenario.warm_s + at
    exit_times: dict[int, float | None] = {}

    def deploy() -> None:
        time.sleep(max(0.0, deploy_at_abs - time.monotonic()))
        targets = [p for p in fleet.gw_procs if p.poll() is None]
        t0 = time.monotonic()
        for p in targets:
            p.send_signal(signal.SIGTERM)
        pending = list(targets)
        deadline = t0 + exit_expected + 20.0
        while pending and time.monotonic() < deadline:
            for p in list(pending):
                if p.poll() is not None:
                    exit_times[p.pid] = time.monotonic() - t0
                    pending.remove(p)
            time.sleep(0.05)
        for p in pending:
            exit_times[p.pid] = None

    try:
        deployer = threading.Thread(target=deploy, daemon=True)
        deployer.start()
        r = run_ws_arm(
            arm="G", base=gw_ws_base(fleet), scenario=scenario, groups=groups,
            workers=opts.workers, fake_base=fleet.fake_base,
            sample_pids=fleet.gw_pids, metrics_urls=fleet.metrics_urls,
            gw_ports=set(fleet.gw_ports), fake_ports=fleet.fake_ports,
            sample_interval=opts.sample_interval)
        deployer.join(timeout=30)
    finally:
        fleet.stop()

    exited = [t for t in exit_times.values() if t is not None]
    fleet_exit = max(exited) if exited else None
    stt_sessions = sum(g.sessions for g in groups if g.product == "inworld-stt")
    saw_4900 = r.close_codes.get("4900", 0)
    out.append(ws_arm_tables(r))
    out.append("")
    out.append(f"drain config: grace={grace:.1f}s budget_total="
               f"{budget_total_from_env():.1f}s allow_short="
               f"{drain_allow_short_from_env()}; per-worker exit: "
               + "; ".join(f"pid {pid}: "
                           + (f"{t:.2f}s" if t is not None else "STILL RUNNING")
                           for pid, t in exit_times.items()))
    out.append("")
    out += verdict_lines([
        ("100% of clients see 4900", saw_4900 == r.opened,
         f"{saw_4900} of {r.opened} sessions closed 4900; all codes "
         f"{r.close_codes}"),
        ("fake terminates_received == STT sessions",
         int(r.fake_ws.get("terminates_received", -1)) == stt_sessions,
         f"{r.fake_ws.get('terminates_received')} vs {stt_sessions} STT sessions"),
        ("process exits before grace + 3 s",
         fleet_exit is not None and fleet_exit <= exit_expected,
         f"{fleet_exit:.2f}s" if fleet_exit is not None else "NEVER"),
        ("cut == 0", r.close_codes.get("abort-1006", 0) == 0,
         f"{r.close_codes.get('abort-1006', 0)} sockets ended without a close "
         "frame"),
        ("every session has a record with `seconds` and `basis`", None,
         "read from the gateway's capture sink; not visible to the client"),
    ], measured=r.opened > 0)
    return "\n".join(out)


_DRIVERS = {"s9_throughput": run_s9, "s10_idle": run_s10,
            "s11_disconnect": run_s11, "s12_deploy": run_s12}


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m bench.ws_load")
    p.add_argument("--scenario", required=True,
                   help="one of " + ", ".join(WS_SCENARIOS))
    p.add_argument("--arms", default="d",
                   help="d, g or dg. Default d: the gateway's WebSocket routes "
                        "do not exist yet, and an arm that cannot connect is "
                        "not a measurement.")
    p.add_argument("--sessions", type=int, default=None,
                   help="override the scenario's TOTAL session count; the "
                        "groups keep their proportions")
    p.add_argument("--measure", type=float, default=None)
    p.add_argument("--warm", type=float, default=None)
    p.add_argument("--ramp", type=float, default=None)
    p.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) // 2),
                   help="load-generator processes; sessions are split across them")
    p.add_argument("--fake-workers", type=int, default=1)
    p.add_argument("--sample-interval", type=float, default=1.0)
    p.add_argument("--smoke", action="store_true",
                   help="tiny self-validation: a handful of sessions, seconds")
    p.add_argument("--out", default=None,
                   help="also write the report here. Nothing is written under "
                        "bench/results unless you say so: the campaign's "
                        "results are the verification run's to record.")
    args = p.parse_args(argv)

    if args.scenario not in WS_SCENARIOS:
        print(f"unknown scenario {args.scenario!r}; choose from "
              f"{list(WS_SCENARIOS)}", file=sys.stderr)
        return 2
    scenario = WS_SCENARIOS[args.scenario]
    arms = [a.upper() for a in args.arms if a.lower() in "dg"]
    if not arms:
        print("--arms must contain d and/or g", file=sys.stderr)
        return 2
    for arm in arms:
        if arm not in scenario.arms:
            print(f"{scenario.sid} does not run in arm {arm} "
                  f"(it needs {scenario.arms})", file=sys.stderr)
            return 2

    import dataclasses
    total = scenario.total_sessions()
    if args.smoke:
        scenario = dataclasses.replace(
            scenario, measure_s=scenario.smoke_measure_s, warm_s=0.0, ramp_s=2.0,
            groups=tuple(g.scaled(scenario.smoke_sessions / max(1, total))
                         for g in scenario.groups))
    if args.sessions is not None:
        scenario = dataclasses.replace(
            scenario, groups=tuple(g.scaled(args.sessions / max(1, total))
                                   for g in scenario.groups))
    if args.measure is not None:
        scenario = dataclasses.replace(scenario, measure_s=args.measure)
    if args.warm is not None:
        scenario = dataclasses.replace(scenario, warm_s=args.warm)
    if args.ramp is not None:
        scenario = dataclasses.replace(scenario, ramp_s=args.ramp)
    args.arms = arms

    def _on_term(signum, _frame):
        raise SystemExit(128 + signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _on_term)
        except (ValueError, OSError):
            pass
    import atexit
    atexit.register(stop_all_fleets)

    try:
        report = _DRIVERS[scenario.driver](scenario, args)
    finally:
        stop_all_fleets()
    print()
    print(report)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report + "\n")
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

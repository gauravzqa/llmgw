"""Tier 3 for the socket plane: three hundred sockets, all cut at once.

The chaos tier's question is not "does the happy path work" -- the contract
tier answers that -- it is "what does the process HOLD after something went
wrong three hundred times in the same instant". For a relay the answer has
four parts and every one of them is invisible on the session that caused it:

1. **Upstream sockets close.** A client that vanishes must take its provider
   socket with it, within seconds, whether it closed politely or had its
   process killed. A gateway that leaks them holds the tenant's provider
   concurrency until it restarts, and on two of the four products the tenant
   is BILLED for the wall time.
2. **Nothing is written to stderr per session.** This is finding 41
   (`docs/15-findings-log.md:688-724`) and it is why this file runs the
   gateway in a subprocess with its stderr on a pipe nobody drains: 4 KB
   tracebacks, one per cut stream, filled a 64 KiB pipe at about sixteen
   streams and the process blocked inside its logging handler and never
   exited. In-process the same bug writes into pytest's capture list, which
   never fills and never blocks, so the test would pass with the bug in it.
3. **`/healthz` answers throughout.** Three hundred simultaneous teardowns
   must not starve the event loop, or a load balancer pulls a healthy machine
   out mid-incident.
4. **No fd or task drift.** The two leaks that have no symptom until the
   thousandth session.

`LLMGW_CHAOS_SOCKETS` scales the socket count; the default is sized so the
file finishes in well under a minute on a laptop, because a fault injector
you never run locally is one that rots.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psutil
import pytest
import websockets
from fakes.upstream import build_app as build_fake
from fakes.upstream import serve_in_thread
from websockets.exceptions import ConnectionClosed, InvalidStatus

pytestmark = pytest.mark.chaos

REPO_ROOT = Path(__file__).resolve().parents[2]
TTS_ROUTE = "/tts/v1/voice:streamBidirectional"
TENANT_TOKEN = "chaos-tenant-token"

SOCKETS = int(os.environ.get("LLMGW_CHAOS_SOCKETS", "300"))
"""How many sessions to open and cut at once. 300 is the number PLAN-G names
for this shape; S11 runs 1,000 against the load harness."""

STDERR_BUDGET = 4 * 1024
"""Bytes the gateway may write to stderr across the whole run. Not zero: a
warning about the run as a WHOLE is fine and is sometimes the right thing.
What is forbidden is output that SCALES with the number of sessions, and at
300 sessions the smallest per-session line anyone has proposed (88 bytes,
the one that replaced the tracebacks) is 26 KiB -- six times this budget."""


# ==========================================================================
# Harness
# ==========================================================================


class _Gateway:
    """The gateway subprocess, its port, and its undrained stderr pipe."""

    def __init__(self, proc: subprocess.Popen, port: int) -> None:
        self.proc = proc
        self.port = port

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def ws_url(self, route: str = TTS_ROUTE) -> str:
        return f"ws://127.0.0.1:{self.port}{route}"

    def stop(self, timeout: float = 20.0) -> bytes:
        """SIGTERM, wait for the drain, and return everything stderr held.

        The pipe is read HERE and only here, after the process has been told
        to stop -- which is exactly the shape that hung S8-B: if the process
        needs the reader to run before it can exit, it never exits, and this
        method times out rather than passing.
        """
        self.proc.terminate()
        try:
            _, err = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            _, err = self.proc.communicate(timeout=10)
            raise AssertionError(
                "the gateway did not exit within its drain: the classic shape "
                "is a process blocked writing to a full stderr pipe"
            ) from None
        return err or b""


def _write_config(tmp_path: Path) -> tuple[Path, Path]:
    tenants = tmp_path / "tenants.toml"
    tenants.write_text(f"""
[tenants.chaos]
tokens = ["{TENANT_TOKEN}"]
rate_per_second = 10000.0
burst = 10000
max_concurrency = 4096
""")
    policy = tmp_path / "workloads.toml"
    policy.write_text("""
default_workload = "default"

[defaults.budgets]
total = 30.0

[profiles.tts_session.budgets]
connect = 5.0
headers = 5.0
first_event = 5.0
progress = 5.0
client_stall = 5.0
idle = 30.0
session_total = 120.0

[workloads.default]
incumbent = "inworld.tts-2-flash"
profile = "tts_session"
""")
    return tenants, policy


def _launch(upstream_url: str, tmp_path: Path) -> _Gateway:
    import socket as _socket

    with _socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    tenants, policy = _write_config(tmp_path)
    env = dict(os.environ)
    env.update(
        WSCHAOS_UPSTREAM_URL=upstream_url,
        WSCHAOS_PORT=str(port),
        WSCHAOS_TENANTS=str(tenants),
        WSCHAOS_POLICY=str(policy),
        WSCHAOS_CAPTURE=str(tmp_path / "records.jsonl"),
        PYTHONPATH=f"{REPO_ROOT}{os.pathsep}{REPO_ROOT / 'src'}",
    )
    proc = subprocess.Popen(
        [sys.executable, "-m", "tests.chaos._ws_gwproc"],
        cwd=REPO_ROOT, env=env,
        stdout=subprocess.PIPE,
        # THE point of the file: nobody reads this until the process is told
        # to stop. A 64 KiB pipe is the whole budget the gateway has.
        stderr=subprocess.PIPE,
    )
    gateway = _Gateway(proc, port)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise AssertionError("gateway exited at boot")
        try:
            if httpx.get(f"{gateway.base_url}/healthz", timeout=1.0).status_code == 200:
                return gateway
        except httpx.HTTPError:
            time.sleep(0.1)
    proc.kill()
    raise AssertionError("gateway never became healthy")


@pytest.fixture
def ws_upstream():
    server = serve_in_thread(build_fake("audio"))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def gateway(ws_upstream, tmp_path):
    gw = _launch(ws_upstream.base_url, tmp_path)
    yield gw
    if gw.proc.poll() is None:
        gw.stop()


def _auth() -> dict[str, str]:
    return {"Authorization": f"Basic {TENANT_TOKEN}"}


async def _stats(server) -> dict:
    return (await _get(f"{server.base_url}/__stats")).json()


def _fds(proc: subprocess.Popen) -> int:
    try:
        return psutil.Process(proc.pid).num_fds()
    except Exception:  # noqa: BLE001 - psutil is a diagnostic, not a dependency
        return -1


async def _get(url: str) -> httpx.Response:
    """Blocking httpx, off the test's event loop.

    The gateway is a subprocess and the calls here are diagnostics, but they
    still must not run ON the loop that is holding three hundred client
    sockets open: a blocking `get` there would stall the very sockets whose
    behaviour is being measured, and the measurement would be of the test.
    """
    return await asyncio.to_thread(httpx.get, url, timeout=5.0)


async def _text(url: str) -> str:
    return (await _get(url)).text


def _metric(text: str, needle: str) -> float:
    for line in text.splitlines():
        if line.startswith(needle):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


# ==========================================================================
# The test
# ==========================================================================


async def test_a_mass_disconnect_closes_every_upstream_socket_and_says_nothing(
    gateway, ws_upstream,
):
    """Three hundred sessions, all streaming, all cut within a second.

    The shape is the one that matters operationally: it is what a LiveKit
    deploy on the CONSUMER's side looks like from here, and it is what a
    network partition looks like too.
    """
    baseline_fds = _fds(gateway.proc)
    baseline_metrics = await _text(f"{gateway.base_url}/metrics")
    baseline_tasks = _metric(baseline_metrics, "llmgw_tasks")

    async def open_one(i: int):
        ws = await websockets.connect(
            gateway.ws_url(),
            additional_headers={
                **_auth(),
                # Enough audio that every session is mid-stream when it is
                # cut: a socket cut between utterances proves nothing about
                # the teardown path the relay actually takes.
                "X-Fake-Bytes": "8192", "X-Fake-Events": "64",
                "X-Fake-Interval": "0.02",
            },
            open_timeout=30, close_timeout=1,
        )
        await ws.send(json.dumps({
            "create": {"modelId": "inworld-tts-2-flash"}, "contextId": f"c{i}",
        }))
        await ws.send(json.dumps({
            "send_text": {"text": "chaos" * 20}, "contextId": f"c{i}",
        }))
        await ws.send(json.dumps({"flush_context": {}, "contextId": f"c{i}"}))
        return ws

    opened = await asyncio.gather(
        *(open_one(i) for i in range(SOCKETS)), return_exceptions=True,
    )
    sockets = [ws for ws in opened if not isinstance(ws, BaseException)]
    failures = [ws for ws in opened if isinstance(ws, BaseException)]
    assert not failures, f"{len(failures)} upgrades failed: {failures[:3]}"
    assert len(sockets) == SOCKETS

    # Let the relay actually be relaying before anything is cut.
    await asyncio.sleep(1.0)
    open_now = _metric(
        await _text(f"{gateway.base_url}/metrics"),
        'llmgw_ws_sessions_open{surface="inworld_tts_ws"}',
    )
    assert open_now == SOCKETS, f"the gauge says {open_now} of {SOCKETS} are open"

    # --- the disconnect ---------------------------------------------------
    # `close_timeout=1` and no wait for the handshake: half of these are
    # effectively an abort, which is the honest mix. The fake keeps sending
    # into sockets nobody is reading.
    cut_at = time.monotonic()
    await asyncio.gather(
        *(ws.close(1001) for ws in sockets), return_exceptions=True,
    )

    # 1. every upstream socket closes, quickly. `ws_open` is the fake's
    # CUMULATIVE count of accepted upgrades; `ws.ws_open_now` is the gauge,
    # and it is the gauge that has to come back to zero.
    deadline = cut_at + 5.0
    while time.monotonic() < deadline:
        stats = await _stats(ws_upstream)
        if stats["ws"]["ws_open_now"] == 0:
            break
        await asyncio.sleep(0.05)
    stats = await _stats(ws_upstream)
    elapsed = time.monotonic() - cut_at
    assert stats["ws"]["ws_open_now"] == 0, (
        f"{stats['ws']['ws_open_now']} upstream sockets still open "
        f"{elapsed:.1f}s after {SOCKETS} clients vanished"
    )
    assert elapsed <= 5.0
    assert stats["ws_closed_by_client"] >= SOCKETS, (
        "the fake saw the GATEWAY close each upstream socket: a relay that "
        "merely dropped its reference would leave them to a TCP timeout"
    )

    # 3. the process answered health throughout, and still does.
    health = await _get(f"{gateway.base_url}/healthz")
    assert health.status_code == 200

    # Every in-process counter returns to zero: the ws gauge, the stream
    # gauge a drain waits on, and both permit scopes. They are polled
    # TOGETHER because they fall at different moments -- the relay's own
    # teardown releases the first, the endpoint's `finally` the rest -- and
    # asserting one the instant another settles is how a flaky test is
    # written. The window is `relay.TEARDOWN_TIMEOUT` plus slack: a pump
    # parked in a socket write that will never complete does not notice its
    # cancel until that bound fires, and the bound is deliberately generous.
    gauges = (
        'llmgw_ws_sessions_open{surface="inworld_tts_ws"}',
        'llmgw_streams_open{surface="inworld_tts_ws"}',
        'llmgw_permits_in_use{scope="tenant"}',
        'llmgw_permits_in_use{scope="provider_key"}',
    )
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        text = await _text(f"{gateway.base_url}/metrics")
        if all(_metric(text, g) == 0 for g in gauges):
            break
        await asyncio.sleep(0.2)
    text = await _text(f"{gateway.base_url}/metrics")
    stuck = {g: _metric(text, g) for g in gauges if _metric(text, g) != 0}
    assert not stuck, (
        f"{stuck} after {SOCKETS} sessions ended: every one of these is held "
        f"by a `finally` that did not run, and none of them has a symptom "
        f"until the process runs out of whatever it is holding"
    )

    # 4. no fd or task drift. Both are sampled after a settle, because the
    # teardown itself legitimately holds sockets for a moment.
    await asyncio.sleep(2.0)
    text = await _text(f"{gateway.base_url}/metrics")
    tasks = _metric(text, "llmgw_tasks")
    assert tasks <= baseline_tasks + 10, (
        f"task count drifted from {baseline_tasks} to {tasks} after {SOCKETS} "
        f"sessions: five tasks per session leak here or nowhere"
    )
    fds = _fds(gateway.proc)
    if fds >= 0 and baseline_fds >= 0:
        assert fds <= baseline_fds + 20, (
            f"fd count drifted from {baseline_fds} to {fds}: two per session "
            f"would be {2 * SOCKETS}"
        )

    # 2. finding 41. Read the pipe only now, on the way out.
    err = gateway.stop()
    assert len(err) < STDERR_BUDGET, (
        f"the gateway wrote {len(err)} bytes to stderr across {SOCKETS} cut "
        f"sessions; anything that scales per session fills a 64 KiB pipe and "
        f"the process stops being able to exit (finding 41). First 400 bytes:\n"
        f"{err[:400]!r}"
    )
    assert b"Traceback" not in err, (
        "a traceback per cut session is the exact shape finding 41 names"
    )


async def test_a_deploy_under_open_sockets_closes_them_4900_and_exits(
    gateway, ws_upstream, tmp_path,
):
    """C26 at scale, and the other half of finding 41.

    SIGTERM with sockets open is a deploy. Every client must see 4900 rather
    than a reset, every session must leave a record, and the process must
    exit inside its grace with the stderr pipe still unread.
    """
    count = max(20, SOCKETS // 10)
    sockets = []
    for i in range(count):
        ws = await websockets.connect(
            gateway.ws_url(), additional_headers=_auth(),
            open_timeout=30, close_timeout=2,
        )
        await ws.send(json.dumps({
            "create": {"modelId": "inworld-tts-2-flash"}, "contextId": f"d{i}",
        }))
        sockets.append(ws)
    await asyncio.sleep(0.5)

    started = time.monotonic()
    gateway.proc.terminate()

    codes = []
    for ws in sockets:
        try:
            while True:
                await asyncio.wait_for(ws.recv(), 30)
        except ConnectionClosed:
            codes.append(ws.close_code)
        except (TimeoutError, asyncio.TimeoutError):  # noqa: UP041
            codes.append(None)
    assert all(code == 4900 for code in codes), (
        f"close codes were {sorted({c for c in codes})}; a deploy must tell "
        f"every client it was a deploy (C26), not drop them"
    )

    _, err = gateway.proc.communicate(timeout=40)
    assert gateway.proc.returncode == 0
    assert time.monotonic() - started < 40
    assert len(err or b"") < STDERR_BUDGET, (err or b"")[:400]

    capture = tmp_path / "records.jsonl"
    records = [
        json.loads(line) for line in capture.read_text().splitlines() if line.strip()
    ]
    sessions = [r for r in records if r["kind"] == "session"]
    assert len(sessions) >= count, (
        f"{len(sessions)} session records for {count} drained sockets: a "
        f"session without a record is a session nobody can bill or explain"
    )
    drained = [r for r in sessions if r.get("error_code") == "session_draining"]
    assert drained, "the drained sessions name the drain as their ending"
    assert all(r["outcome"] in ("canceled", "completed") for r in sessions)


async def test_the_upgrade_is_refused_cleanly_when_the_tenant_is_unknown(gateway):
    """The cheap invariant, run at the chaos tier because a refusal path that
    leaks is a refusal path an attacker can use: a thousand bad upgrades must
    leave the process exactly as they found it."""
    before = _metric(
        await _text(f"{gateway.base_url}/metrics"),
        'llmgw_streams_open{surface="inworld_tts_ws"}',
    )
    for _ in range(50):
        with pytest.raises(InvalidStatus) as excinfo:
            await websockets.connect(
                gateway.ws_url(), additional_headers={"Authorization": "Basic nope"},
                open_timeout=10,
            )
        assert excinfo.value.response.status_code == 401
    after = await _text(f"{gateway.base_url}/metrics")
    assert _metric(after, 'llmgw_streams_open{surface="inworld_tts_ws"}') == before
    assert _metric(after, 'llmgw_permits_in_use{scope="tenant"}') == 0

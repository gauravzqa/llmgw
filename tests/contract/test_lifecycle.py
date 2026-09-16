"""Graceful drain, proved against a real threaded gateway on real sockets.

The unit tier (`tests/unit/test_lifecycle.py`) proves `Gateway.begin_drain` and
the in-flight tracker as a property of the object on a ManualClock -- no
sockets, microsecond grace. This file is the other half: the S8
contract asserted END TO END, with a stream actually moving over a socket when
the drain begins. It is where the `<= 0` clamp in `stream_exited` would bite if
a real serving path ever mispaired the tracker, because here the tracker is
driven by the endpoint, not by a test calling `stream_entered()` by hand.

`drain_completes_open_streams` is the named drain deliverable.
It asserts the three facts that make a deploy graceful:

    (a) the stream that was IN FLIGHT when the drain began finishes UNCUT --
        its `data: [DONE]` terminator arrives and the body is byte-complete,
        and `begin_drain` reports `cut == 0`;
    (b) a request that ARRIVES during the drain is shed with 503 `draining`;
    (c) `/healthz` answers 503 while draining, so the load balancer stops
        routing -- while `/metrics` keeps answering, because an operator needs
        it most during a drain.

Then one drain-under-concurrent-load test (the real clamp tripwire): several
streams open at once, drain, every one completes uncut and `cut == 0`.

The drain is triggered by calling `gateway.begin_drain(...)` ON THE SERVER'S
OWN LOOP via `run_coroutine_threadsafe` -- not by poking `draining = True` from
the test thread -- so the awaitable wait, the grace timeout, and the
return-to-zero all run exactly as they would under the signal handler.
"""

from __future__ import annotations

import asyncio
import functools
import os
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
import uvicorn
from bench.scenarios import GATEWAY_MODEL
from fakes.upstream import PATHS
from starlette.applications import Starlette

from llmgw.server.app import DrainReport, Gateway, build_app
from llmgw.server.config import ServerConfig
from llmgw.server.lifecycle import _C2_ENDING_MESSAGE
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes
from tests.contract.test_fallback import (
    KEY,
    KEY_ENV,
    ROUTE,
    body,
    mode,
    two_target_catalog,
)

pytestmark = pytest.mark.contract

UPSTREAM_PATH = PATHS["openai"]
DONE = b"data: [DONE]\n\n"

# A workload whose budgets comfortably cover a paced slow-drip stream (six
# content events at 0.12 s each is ~0.72 s of body), so the stream's own
# completion -- not a budget -- is what ends it. The drain grace sits well
# above that, so the drain's success path is "the stream finished", not "the
# grace expired".
LIFECYCLE_POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 10.0
connect = 1.0
first_event = 2.0
progress = 2.0
client_stall = 6.0

[workloads.ab]
incumbent = "fake.incumbent"
candidate = "fake.candidate"
"""


# ==========================================================================
# A served gateway whose event loop is reachable from the test thread
# ==========================================================================


@dataclass
class LoopGatewayServer:
    """A threaded uvicorn whose loop the test can schedule coroutines onto.

    `test_fallback.GatewayServer` runs uvicorn through `server.run()`, which
    owns its loop privately -- fine when a test only reads `draining` (a bool).
    A drain, though, has to be AWAITED on the loop the streams live on, because
    `begin_drain` parks on an `asyncio.Event` that the endpoint sets from that
    same loop. So this runner builds the loop itself and keeps the handle,
    letting the test call `begin_drain` via `run_coroutine_threadsafe` exactly
    where the signal handler would.
    """

    app: Starlette
    server: uvicorn.Server
    thread: threading.Thread
    loop: asyncio.AbstractEventLoop
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, workload: str | None = None) -> str:
        if workload is None:
            return f"{self.base_url}{ROUTE}"
        return f"{self.base_url}/workloads/{workload}{ROUTE}"

    @property
    def gateway(self) -> Gateway:
        return self.app.state.gateway

    def drain(self, *, grace_s: float) -> DrainReport:
        """Run `begin_drain` on the server's loop and block for the report."""
        fut = asyncio.run_coroutine_threadsafe(
            self.gateway.begin_drain(grace_s=grace_s), self.loop
        )
        return fut.result(timeout=grace_s + 10.0)

    def stop(self, timeout: float = 5.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout)
        if self.thread.is_alive():  # pragma: no cover - only if a stream wedges
            self.server.force_exit = True
            self.thread.join(timeout)


def _serve(app: Starlette, *, startup_timeout: float = 10.0) -> LoopGatewayServer:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(2048)
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="critical", access_log=False, lifespan="on")
    server = uvicorn.Server(config)
    box: dict[str, asyncio.AbstractEventLoop] = {}

    def run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        box["loop"] = loop
        # Same call `uvicorn.Server.run()` makes, but on a loop we own, so the
        # test can schedule the drain onto it.
        loop.run_until_complete(server.serve(sockets=[sock]))

    thread = threading.Thread(target=run, daemon=True, name=f"llmgw-drain-{port}")
    thread.start()
    deadline = time.monotonic() + startup_timeout
    while not server.started or "loop" not in box:
        if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError(f"gateway on port {port} failed to start")
        time.sleep(0.005)
    return LoopGatewayServer(
        app=app, server=server, thread=thread, loop=box["loop"], port=port
    )


@pytest.fixture
def drain_server(fakes: Fakes, tmp_path):
    """A DEDICATED gateway whose primary target slow-drips a clean stream.

    Dedicated (function-scoped) because the test flips `draining`, a one-way
    door -- a shared server would be poisoned for every test after. The
    candidate (the plan's primary) serves `slow-drip`: the complete, correct
    stream, just paced, so there is a real in-flight window to drain and the
    stream still ends with `data: [DONE]`.
    """

    os.environ.setdefault(KEY_ENV, KEY)
    policy_file = tmp_path / "lifecycle.toml"
    policy_file.write_text(LIFECYCLE_POLICY, encoding="utf-8")
    catalog = two_target_catalog(
        fakes,
        candidate=mode("slow-drip", events="6", interval="0.12"),
        incumbent=mode("ok"),
    )
    config = ServerConfig(
        catalog=catalog,
        fake_upstreams=True,
        policy_file=str(policy_file),
        breaker=BREAKER_NEVER_TRIPS,
    )
    server = _serve(build_app(config))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as c:
        yield c


async def _collect(
    client: httpx.AsyncClient, url: str, payload: dict
) -> tuple[int, bytes, bool]:
    """Stream a POST to completion, reporting (status, body, truncated)."""
    chunks: list[bytes] = []
    truncated = False
    try:
        async with client.stream("POST", url, json=payload) as response:
            status = response.status_code
            try:
                async for chunk in response.aiter_raw():
                    chunks.append(chunk)
            except httpx.HTTPError:  # pragma: no cover - the cut case
                truncated = True
    except httpx.HTTPError:  # pragma: no cover
        return 0, b"".join(chunks), True
    return status, b"".join(chunks), truncated


# ==========================================================================
# drain_completes_open_streams  (the named P6 deliverable)
# ==========================================================================


async def test_drain_completes_open_streams(drain_server: LoopGatewayServer, client):
    gw = drain_server
    payload = body(stream=True)

    # 1. Start a streaming request and let it commit -- wait until the endpoint
    #    has entered the serving path (tracker incremented) AND the first bytes
    #    are on the wire, so the drain we trigger next is unambiguously
    #    overlapping a stream that is already mid-flight.
    task = asyncio.ensure_future(_collect(client, gw.url(), payload))
    deadline = time.monotonic() + 10.0
    while gw.gateway.inflight < 1 and not task.done():
        if time.monotonic() > deadline:  # pragma: no cover
            raise AssertionError("the streaming request never entered the gateway")
        await asyncio.sleep(0.01)
    assert gw.gateway.inflight == 1, "exactly one stream should be in flight"

    # 2. Drain on the server's own loop. It flips `draining` True synchronously,
    #    then parks on the in-flight tracker. It must NOT return yet -- a stream
    #    is still running -- so this runs concurrently with the stream.
    drain_fut = asyncio.get_running_loop().run_in_executor(
        None, lambda: gw.drain(grace_s=8.0)
    )

    # Wait for the flip to be observable (begin_drain set it before its first
    # await), so the shed / healthz assertions below see a draining process.
    deadline = time.monotonic() + 5.0
    while not gw.gateway.draining:
        if time.monotonic() > deadline:  # pragma: no cover
            raise AssertionError("begin_drain did not flip draining")
        await asyncio.sleep(0.01)

    # 3b. A request that ARRIVES during the drain is shed with 503 draining.
    shed = await client.post(gw.url(), json=body(stream=True))
    assert shed.status_code == 503
    assert shed.json()["error"]["type"] == "draining"

    # 3c. /healthz is 503 while draining; /metrics still answers (operators
    #     need it most during a drain).
    health = await client.get(f"{gw.base_url}/healthz")
    assert health.status_code == 503
    assert health.json()["status"] == "draining"
    metrics = await client.get(f"{gw.base_url}/metrics")
    assert metrics.status_code == 200
    assert b"llmgw_" in metrics.content

    # 3a. The in-flight stream completes UNCUT: full 200 body ending in [DONE],
    #     never a truncation. This is the assertion that separates "drain
    #     returned" from "drain did not cut an open stream".
    status, payload_bytes, truncated = await task
    assert status == 200
    assert not truncated, "the in-flight stream was cut by the drain"
    assert payload_bytes.endswith(DONE), "the stream did not reach its [DONE] terminator"

    # The drain itself: it waited for the stream and reports a clean cut of 0.
    report: DrainReport = await drain_fut
    assert report.inflight_at_start >= 1
    assert report.cut == 0, "a stream that finished within grace must not be reported cut"
    assert report.timed_out is False
    assert gw.gateway.inflight == 0, "the tracker returned to zero"
    # The shed above is counted where the other denials are.
    assert gw.gateway.draining_denials() == {"draining": 1}


# ==========================================================================
# drain under concurrent load  (the S8 precursor; the clamp tripwire)
# ==========================================================================


async def test_drain_under_concurrent_load_cuts_nothing(
    drain_server: LoopGatewayServer, client
):
    """Several streams open at once, drained together: all complete uncut,
    `cut == 0`. This is the shape the `<= 0` clamp in `stream_exited` would
    corrupt if any serving path double-decremented -- the tracker would hit
    zero early, `_idle` would fire while streams ran, and the drain would
    report a clean cut over still-open streams (the S8 lie). Here the tracker
    is driven only by the real endpoint, `N` times over, so a clean `cut == 0`
    with every body complete is positive evidence the pairing holds under
    fan-out.
    """
    gw = drain_server
    n = 6
    payload = body(stream=True)

    tasks = [asyncio.ensure_future(_collect(client, gw.url(), payload)) for _ in range(n)]

    # Wait until all N are in flight before draining, so the drain genuinely
    # overlaps the whole fan-out.
    deadline = time.monotonic() + 10.0
    while gw.gateway.inflight < n:
        if time.monotonic() > deadline or any(t.done() for t in tasks):  # pragma: no cover
            raise AssertionError(
                f"only {gw.gateway.inflight}/{n} streams entered before timeout"
            )
        await asyncio.sleep(0.01)
    assert gw.gateway.inflight == n

    drain_fut = asyncio.get_running_loop().run_in_executor(
        None, lambda: gw.drain(grace_s=10.0)
    )

    results = await asyncio.gather(*tasks)
    for status, payload_bytes, truncated in results:
        assert status == 200
        assert not truncated, "a concurrent stream was cut by the drain"
        assert payload_bytes.endswith(DONE)

    report: DrainReport = await drain_fut
    assert report.inflight_at_start == n
    assert report.cut == 0, "no open stream should be cut when all finish within grace"
    assert report.timed_out is False
    assert gw.gateway.inflight == 0


# ==========================================================================
# The PROCESS exits: SIGTERM -> drain -> uvicorn's bounded shutdown -> exit 0
# ==========================================================================
#
# Everything above drives `begin_drain` on a threaded server and stops the
# server itself. That proves the drain; it does not prove the process goes
# away afterwards. S8 (10 Sep) showed it does not: after a 30 s grace against
# 100 s streams, `should_exit` flipped and uvicorn then waited on the still-open
# streams indefinitely -- four workers "still running" long after the grace.
# These two tests run the REAL entry (`lifecycle.run`, via `bench._gwproc`,
# the same module the scale bench and `python -m llmgw.server` share) in a
# subprocess, SIGTERM it with a slow-drip stream mid-flight, and time the
# exit. The fakes are the session fixture's in-thread servers; the subprocess
# reaches them over loopback like any other client.

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass
class GatewayProcess:
    proc: subprocess.Popen
    port: int
    stderr_path: Path

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self) -> str:
        return f"{self.base_url}{ROUTE}"

    def stderr(self) -> str:
        try:
            return self.stderr_path.read_text(errors="replace")[-4000:]
        except OSError:  # pragma: no cover
            return ""

    def stderr_all(self) -> str:
        try:
            return self.stderr_path.read_text(errors="replace")
        except OSError:  # pragma: no cover
            return ""

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=5)


def _launch_gateway_process(
    fakes: Fakes, tmp_path, *, grace_s: float, total_s: float, allow_short: bool
) -> GatewayProcess:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    env = dict(os.environ)
    env.update(
        BENCH_GW_PORT=str(port),
        BENCH_FAKE_OPENAI_URL=fakes.openai.base_url,
        BENCH_FAKE_ANTHROPIC_URL=fakes.anthropic.base_url,
        BENCH_GW_DRAIN_GRACE=str(grace_s),
        BENCH_GW_BUDGET_TOTAL=str(total_s),
        BENCH_GW_DRAIN_ALLOW_SHORT="1" if allow_short else "0",
    )
    stderr_path = tmp_path / f"gw-{port}.stderr"
    with stderr_path.open("wb") as err:
        proc = subprocess.Popen(
            [sys.executable, "-m", "bench._gwproc"],
            cwd=REPO_ROOT, env=env, stdout=subprocess.DEVNULL, stderr=err,
        )
    gw = GatewayProcess(proc=proc, port=port, stderr_path=stderr_path)
    deadline = time.monotonic() + 20.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"gateway process exited at boot:\n{gw.stderr()}")
        try:
            if httpx.get(f"{gw.base_url}/metrics", timeout=1.0).status_code == 200:
                return gw
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    gw.kill()
    raise RuntimeError(f"gateway process did not come up:\n{gw.stderr()}")


def _slow_drip(events: int, interval_s: float) -> dict[str, str]:
    # Forwarded by bench._gwproc's `forward_request_headers` (x-fake-*), so the
    # fake shapes the stream and the gateway just passes it through.
    return {
        "X-Fake-Mode": "slow-drip",
        "X-Fake-Events": str(events),
        "X-Fake-Interval": str(interval_s),
    }


async def _stream_until_closed(
    client: httpx.AsyncClient, url: str, headers: dict[str, str],
    first_byte: asyncio.Event,
) -> tuple[int, bytes, bool]:
    """Stream to completion or cut, reporting (status, body, truncated).

    `first_byte` is set as soon as any body byte arrives, so the test can send
    SIGTERM only once the stream is unambiguously mid-flight."""
    payload = {
        "model": GATEWAY_MODEL, "stream": True, "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
    }
    chunks: list[bytes] = []
    status = 0
    truncated = False
    try:
        async with client.stream("POST", url, json=payload, headers=headers) as r:
            status = r.status_code
            try:
                async for chunk in r.aiter_raw():
                    chunks.append(chunk)
                    first_byte.set()
            except httpx.HTTPError:
                truncated = True
    except httpx.HTTPError:
        truncated = True
    return status, b"".join(chunks), truncated


async def _wait_exit(proc: subprocess.Popen, limit_s: float) -> float | None:
    """Seconds until `proc` exits, or None if it is still running at `limit_s`."""
    t0 = time.monotonic()
    try:
        await asyncio.get_running_loop().run_in_executor(
            None, functools.partial(proc.wait, timeout=limit_s)
        )
    except subprocess.TimeoutExpired:
        return None
    return time.monotonic() - t0


async def test_process_exits_at_grace_when_a_stream_outlives_it(fakes: Fakes, tmp_path):
    """Arm B in miniature: grace 2 s, a 15 s stream. The drain times out, the
    stream is cut (the documented residual), and the PROCESS must still be gone
    within grace + UVICORN_SHUTDOWN_TIMEOUT_S -- not sit on the open stream."""
    from llmgw.server.lifecycle import UVICORN_SHUTDOWN_TIMEOUT_S

    grace_s = 2.0
    gw = _launch_gateway_process(
        fakes, tmp_path, grace_s=grace_s, total_s=30.0, allow_short=True
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            first_byte = asyncio.Event()
            task = asyncio.ensure_future(_stream_until_closed(
                client, gw.url(), _slow_drip(events=60, interval_s=0.25), first_byte,
            ))
            await asyncio.wait_for(first_byte.wait(), timeout=10.0)

            gw.proc.send_signal(subprocess.signal.SIGTERM)
            exited_after = await _wait_exit(gw.proc, limit_s=grace_s + 20.0)

            assert exited_after is not None, (
                f"process still running {grace_s + 20.0:.0f}s after SIGTERM with a "
                f"{grace_s}s grace -- uvicorn is waiting on the open stream.\n{gw.stderr()}"
            )
            budget = grace_s + UVICORN_SHUTDOWN_TIMEOUT_S + 2.5
            assert exited_after <= budget, (
                f"exit took {exited_after:.2f}s; expected <= {budget:.1f}s "
                f"(grace {grace_s}s + uvicorn {UVICORN_SHUTDOWN_TIMEOUT_S}s + slack)"
            )
            assert gw.proc.returncode == 0, gw.stderr()

            status, payload, truncated = await asyncio.wait_for(task, timeout=10.0)
            assert status == 200
            # The residual, made visible: the stream did not get its terminator.
            assert truncated or not payload.endswith(DONE), (
                "a 15 s stream completed inside a 2 s grace; the fake did not drip"
            )
            _assert_cut_logged_quietly(gw.stderr_all(), cuts=1)
    finally:
        gw.kill()


# Upper bound on a worker's WHOLE stderr for a drain that cuts a handful of
# streams: startup lines, the drain's INFO lines, uvicorn's one `Cancel N
# running task(s)`, and the single summary WARNING. It must NOT scale with the
# number of cut streams -- that scaling (4 KB tracebacks, then 88-byte lines,
# once per stream) is what blocked the S8-B workers on an undrained 64 KiB
# pipe. If this trips, something on the shutdown path is logging per stream.
_SHUTDOWN_STDERR_BOUND = 8 * 1024


def _assert_cut_logged_quietly(stderr: str, *, cuts: int) -> None:
    """A shutdown that cuts streams writes ONE summary WARNING naming the
    count, nothing per stream at any level, and nothing at ERROR: no
    `Exception in ASGI application` traceback (the S8-B pipe hang) and no
    `returned without completing response` (uvicorn's name for the C2
    ending). uvicorn's single `Cancel N running task(s)` line is allowed --
    it is the one line that says the grace was too short."""
    assert "Exception in ASGI application" not in stderr, stderr[-3000:]
    assert "Traceback" not in stderr, stderr[-3000:]
    assert _C2_ENDING_MESSAGE not in stderr, stderr[-3000:]
    assert stderr.count("shutdown cut") == 1, stderr[-3000:]
    assert f"shutdown cut {cuts} stream(s)" in stderr, stderr[-3000:]
    assert len(stderr) < _SHUTDOWN_STDERR_BOUND, (len(stderr), stderr[-3000:])
    errors = [ln for ln in stderr.splitlines() if "ERROR" in ln]
    assert all("timeout graceful shutdown exceeded" in ln for ln in errors), errors


async def test_shutdown_cuts_are_one_summary_line_and_no_tracebacks(
    fakes: Fakes, tmp_path
):
    """Several streams outlive a short grace at once. The cuts are counted
    and reported as ONE summary WARNING after uvicorn stops; stderr carries
    no per-stream line, no traceback and no per-stream ERROR, so the burst
    that blocked an undrained stderr pipe cannot form at any N."""
    from llmgw.server.lifecycle import UVICORN_SHUTDOWN_TIMEOUT_S

    n, grace_s = 5, 2.0
    gw = _launch_gateway_process(
        fakes, tmp_path, grace_s=grace_s, total_s=30.0, allow_short=True
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            firsts = [asyncio.Event() for _ in range(n)]
            tasks = [
                asyncio.ensure_future(_stream_until_closed(
                    client, gw.url(), _slow_drip(events=60, interval_s=0.25), first,
                ))
                for first in firsts
            ]
            await asyncio.wait_for(
                asyncio.gather(*(f.wait() for f in firsts)), timeout=10.0
            )

            gw.proc.send_signal(subprocess.signal.SIGTERM)
            exited_after = await _wait_exit(gw.proc, limit_s=grace_s + 20.0)
            assert exited_after is not None, gw.stderr()
            assert exited_after <= grace_s + UVICORN_SHUTDOWN_TIMEOUT_S + 2.5
            assert gw.proc.returncode == 0, gw.stderr()

            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10.0)
            for status, payload, truncated in results:
                assert status == 200
                assert truncated or not payload.endswith(DONE)
            _assert_cut_logged_quietly(gw.stderr_all(), cuts=n)
    finally:
        gw.kill()


async def test_process_exits_promptly_when_streams_finish_inside_grace(
    fakes: Fakes, tmp_path
):
    """Arm A in miniature: grace 8 s, a ~1.2 s stream. The stream completes
    UNCUT, and the process exits shortly after it ends -- well before the grace
    would have expired -- because the drain returns on completion and uvicorn's
    shutdown has nothing left to wait for."""
    grace_s = 8.0
    gw = _launch_gateway_process(
        fakes, tmp_path, grace_s=grace_s, total_s=30.0, allow_short=True
    )
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
            first_byte = asyncio.Event()
            task = asyncio.ensure_future(_stream_until_closed(
                client, gw.url(), _slow_drip(events=6, interval_s=0.2), first_byte,
            ))
            await asyncio.wait_for(first_byte.wait(), timeout=10.0)

            gw.proc.send_signal(subprocess.signal.SIGTERM)
            exited_after = await _wait_exit(gw.proc, limit_s=grace_s + 10.0)

            status, payload, truncated = await asyncio.wait_for(task, timeout=10.0)
            assert status == 200
            assert not truncated, "a stream that fits inside the grace was cut"
            assert payload.endswith(DONE)

            assert exited_after is not None, gw.stderr()
            # ~1.2 s of stream left, plus lifespan teardown and interpreter exit.
            # Strictly below the grace is the point: the drain ended when the
            # stream did, not when the clock ran out.
            assert exited_after < grace_s - 1.0, (
                f"exit took {exited_after:.2f}s with a {grace_s}s grace: the drain "
                f"waited for the grace instead of the stream"
            )
            assert gw.proc.returncode == 0, gw.stderr()
    finally:
        gw.kill()

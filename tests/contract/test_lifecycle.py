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
import socket
import threading
import time
from dataclasses import dataclass

import httpx
import pytest
import uvicorn
from fakes.upstream import PATHS
from starlette.applications import Starlette

from llmgw.server.app import DrainReport, Gateway, build_app
from llmgw.server.config import ServerConfig
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
    import os

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

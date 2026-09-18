"""The serving path over real sockets: real uvicorn, real fakes, real client.

Everything here runs through a socket on purpose. `TestClient` and an ASGI
transport both short-circuit the transport, and every property this file
asserts lives in exactly the layer they skip: a truncated chunked body with no
terminator, headers flushed before a body exists, events arriving in more than
one piece, a client hanging up mid-stream. A green suite built on an ASGI
transport would prove the handler's control flow and nothing about the thing
the customer connects to.

The gateway under test is built by `app.build_app(config)` -- the factory, not
the module-global `app`. Half of what is worth asserting here is a
*configured* bound: a 32 KiB frame limit, a 0.6 s progress budget, a 4 KiB
request cap. Against a process-global app those tests are either impossible or
monkeypatching, and the budgets would have to be production-sized, which would
make this file minutes long instead of seconds.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import socket
import threading
import time
from dataclasses import dataclass, field

import httpx
import pytest
import uvicorn
from fakes import wire
from prometheus_client.parser import text_string_to_metric_families
from starlette.applications import Starlette

from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, fake_catalog
from llmgw.upstream import Upstream
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = pytest.mark.contract

SURFACES = ("openai", "anthropic")

MODEL_FOR = {"openai": "fake.echo", "anthropic": "fake.echo-anthropic"}
ROUTE_FOR = {"openai": "/v1/chat/completions", "anthropic": "/anthropic/v1/messages"}
"""The Anthropic client route is prefixed and the OpenAI one is not. The
prefix is how a caller names the dialect it is speaking; the path we send
upstream is `Surface.path` either way, which is what `/v1/messages` in the
fake's routing table proves."""

FAKE_HEADERS = (
    "x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
    "x-fake-status", "x-fake-bytes", "x-fake-seed", "x-fake-crlf",
)
"""Forwarded to the upstream so a test can pick the fake's behaviour.

This is why `forward_request_headers` is configuration rather than a constant.
The production default forwards five headers and none of these; a test config
adds the mode selectors. The alternative -- a per-mode provider baked into the
catalog -- would need one provider per (mode, parameter) combination and would
put test scaffolding in the shipped catalog.
"""

MAX_REQUEST_BYTES = 4096
MAX_FRAME_BYTES = 32 * 1024
PROGRESS_BUDGET = 0.6
TOTAL_BUDGET = 8.0


def body_for(surface: str, *, stream: bool = True) -> dict:
    return {
        "model": MODEL_FOR[surface],
        "stream": stream,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
    }


def expected_stream(surface: str) -> bytes:
    """The canonical bytes for this surface, straight out of `fakes/wire.py`.

    Compared against what the CLIENT received, not against what the fake
    intended to send. The fakes and the parser tests already share this module
    so they cannot drift; this file is what stops the gateway drifting from
    both of them.
    """
    frames = wire.anthropic_stream() if surface == "anthropic" else wire.openai_stream()
    return wire.joined(frames)


def content_events(surface: str, body: bytes) -> int:
    if surface == "anthropic":
        return body.count(b"event: content_block_delta")
    return body.count(b'"delta": {"content"')


def terminal_marker(surface: str) -> bytes:
    return b"message_stop" if surface == "anthropic" else b"data: [DONE]"


# ==========================================================================
# The gateway under test
# ==========================================================================


@dataclass
class GatewayServer:
    """One uvicorn hosting one `build_app()` result, on its own thread.

    A thread with its own event loop, exactly as `fakes/upstream.py` does it
    and for the same two reasons: pytest-asyncio hands each test function a
    fresh loop while the server outlives them, and a server sharing a loop
    with the client under test can hide a blocking bug in either one.

    `lifespan="on"` is the one line that differs from the fakes' helper, and
    it is not optional: the `Upstream` pool is created in the Starlette
    lifespan so it can be *closed* at shutdown, and a server started with
    `lifespan="off"` therefore answers every request with a 500. That is the
    reason this file defines its own runner instead of importing the fakes'.
    """

    app: Starlette
    server: uvicorn.Server
    thread: threading.Thread
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, surface: str) -> str:
        return f"{self.base_url}{ROUTE_FOR[surface]}"

    @property
    def upstream(self) -> Upstream:
        """The server's own pool, reachable because it runs in this process.

        `client_disconnect_cancels_upstream` asserts `in_flight() == {}`, and
        that is a statement about the gateway's internals that no client can
        observe: a client that hung up cannot tell "the upstream was released"
        from "the upstream is still streaming into a void".
        """
        return self.app.state.gateway.upstream

    def stop(self, timeout: float = 5.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout)
        if self.thread.is_alive():  # pragma: no cover - only if a stream wedges
            self.server.force_exit = True
            self.thread.join(timeout)


def _serve(app: Starlette, *, startup_timeout: float = 10.0) -> GatewayServer:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(2048)
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="critical", access_log=False, lifespan="on")
    server = uvicorn.Server(config)
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True,
        name=f"llmgw-under-test-{port}",
    )
    thread.start()
    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError(f"gateway on port {port} failed to start")
        time.sleep(0.005)
    return GatewayServer(app=app, server=server, thread=thread, port=port)


@pytest.fixture(scope="session")
def gateway_factory(fakes: Fakes):
    """Build gateways with arbitrary config, cleaned up at session end.

    Session-scoped and synchronous, like the `fakes` fixture it depends on:
    starting uvicorn costs ~20 ms and the file's whole budget is 25 s, so the
    default server is started once and the two tests that need a different
    bound get their own.
    """
    servers: list[GatewayServer] = []

    def make(**overrides) -> GatewayServer:
        catalog = fake_catalog(
            openai_url=f"{fakes.openai.base_url}/v1",
            anthropic_url=fakes.anthropic.base_url,
        )
        settings = {
            "catalog": catalog,
            "fake_upstreams": True,
            "forward_request_headers": FAKE_HEADERS,
            "max_request_bytes": MAX_REQUEST_BYTES,
            "max_frame_bytes": MAX_FRAME_BYTES,
            "buffer_bytes": 16 * 1024,
            # Every hostile mode below is served by ONE target on ONE
            # session-scoped server; see `BREAKER_NEVER_TRIPS`.
            "breaker": BREAKER_NEVER_TRIPS,
            "budgets": Budgets(
                total=TOTAL_BUDGET,
                connect=1.0,
                first_event=1.0,
                # Deliberately tiny. `ping_does_not_reset_progress` is asserted
                # by ending on this budget rather than by waiting out the total,
                # which is the same proof and thirteen times cheaper.
                progress=PROGRESS_BUDGET,
                client_stall=5.0,
            ),
        }
        settings.update(overrides)
        server = _serve(build_app(ServerConfig(**settings)))
        servers.append(server)
        return server

    try:
        yield make
    finally:
        for server in servers:
            server.stop()


@pytest.fixture(scope="session")
def gateway(gateway_factory) -> GatewayServer:
    return gateway_factory()


@pytest.fixture
async def client():
    """No default timeout beyond the gateway's own budgets.

    A client timeout is a second clock, and a test whose assertion could be
    satisfied by either clock is a test that does not say which one fired. The
    gateway's total budget is 8 s; the client is given 15 s so that anything
    this file catches was caught by the gateway.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


# ==========================================================================
# Collecting a response that may be truncated on purpose
# ==========================================================================


@dataclass
class Streamed:
    status: int = 0
    headers: httpx.Headers = field(default_factory=httpx.Headers)
    arrivals: list[tuple[float, bytes]] = field(default_factory=list)
    truncated: bool = False
    """The body ended without a well-formed end of message. For C2 this is the
    expected outcome, not a failure: it is what a direct connection to a
    provider that fell over would have shown the client."""

    @property
    def body(self) -> bytes:
        return b"".join(chunk for _, chunk in self.arrivals)

    @property
    def span(self) -> float:
        if len(self.arrivals) < 2:
            return 0.0
        return self.arrivals[-1][0] - self.arrivals[0][0]


async def stream_request(
    client: httpx.AsyncClient, url: str, body: dict, **fake: str
) -> Streamed:
    """POST and record every arrival with the instant it landed.

    Timestamped because "the client received all the bytes" and "the client
    received them as they were produced" are different claims, and only the
    second one is what a streaming gateway sells. A whole-body comparison is
    blind to a gateway that buffers the entire response and flushes it at the
    end -- which is the single most likely regression in this file.
    """
    out = Streamed()
    try:
        async with client.stream("POST", url, json=body, headers=dict(fake)) as response:
            out.status = response.status_code
            out.headers = response.headers
            try:
                async for chunk in response.aiter_raw():
                    out.arrivals.append((time.monotonic(), chunk))
            except httpx.HTTPError:
                out.truncated = True
    except httpx.HTTPError:  # pragma: no cover - raised on aclose, same meaning
        out.truncated = True
    return out


# ==========================================================================
# passthrough_bytes_identical
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES)
async def test_passthrough_bytes_identical_and_flushed_as_they_arrive(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str
):
    """The client's bytes ARE the fake's bytes, and they arrive in pieces.

    Two assertions that look like one. The first is passthrough: the
    concatenation must EQUAL `fakes/wire.py`, not merely parse to the same
    events -- a gateway that re-serialises has changed key order, dropped a
    field its schema has not learned about, and altered what the customer is
    billed for, all invisibly.

    The second is that we are a streaming gateway at all. A whole-body
    comparison passes identically whether the events were flushed one at a
    time or accumulated and dumped at the end, so it cannot see the regression
    that matters most to a user watching tokens appear. Counting arrivals can.
    """
    result = await stream_request(
        client, gateway.url(surface), body_for(surface),
        **{"x-fake-mode": "ok", "x-fake-interval": "0.03"},
    )
    assert result.status == 200
    assert not result.truncated
    assert result.body == expected_stream(surface)
    assert len(result.arrivals) > 1, (
        f"the whole body arrived in one piece ({len(result.body)} bytes); "
        "the gateway buffered a stream it was supposed to forward"
    )
    assert result.span >= 0.03, "arrivals were not spread over time"


# ==========================================================================
# split_frames_invariant
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES)
async def test_split_frames_yields_the_same_client_bytes_as_ok(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes, surface: str
):
    """Arbitrary write boundaries -- inside a multi-byte character, inside the
    frame delimiter -- must not change one byte the client sees.

    The gateway never decodes and never re-frames, so this is a statement
    about what it does NOT do. The write counter is checked because the client
    cannot see chunk boundaries (httpx reassembles, the kernel coalesces): a
    test that only compared bodies would pass just as happily against a fake
    that quietly stopped splitting.
    """
    result = await stream_request(
        client, gateway.url(surface), body_for(surface),
        **{"x-fake-mode": "split-frames"},
    )
    assert result.status == 200
    assert result.body == expected_stream(surface)
    writes = fakes.stats()["writes_by_mode"]["split-frames"]
    assert writes > 13, f"the fake did not actually split ({writes} writes)"


# ==========================================================================
# no_fallback_after_commit / native_ending_per_surface
# ==========================================================================


async def die_mid_stream(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str, events: int = 3
) -> Streamed:
    return await stream_request(
        client, gateway.url(surface), body_for(surface),
        **{"x-fake-mode": "die-mid-stream", "x-fake-events": str(events)},
    )


@pytest.mark.parametrize("surface", SURFACES)
async def test_no_fallback_after_commit_delivers_k_events_and_stops(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes, surface: str
):
    """CONTRACTS.md C1, from the outside.

    The upstream dies after three events that the client has already seen.
    Every recovery option is now illegal: a second target would splice a
    different answer onto the three tokens already rendered, and there is no
    way to detect that from the outside and no way to apologise for it
    afterwards.

    So the client gets exactly three events, no terminal marker, and the 200
    it was already given. The negative half of the assertion is the important
    one and only the upstream can answer it: `total == 1` says the gateway did
    not quietly open a second provider and throw the answer away -- which is
    the difference between a correct gateway and one that double-bills every
    interrupted request.
    """
    result = await die_mid_stream(gateway, client, surface, events=3)
    assert result.status == 200, "the status was committed before the failure"
    assert content_events(surface, result.body) == 3
    assert terminal_marker(surface) not in result.body
    assert result.truncated, "the body ended cleanly; the client cannot tell it was cut"
    stats = fakes.stats()
    assert stats["total"] == 1, f"more than one upstream request: {stats['by_mode']}"
    assert stats["by_mode"] == {"die-mid-stream": 1}


@pytest.mark.parametrize("surface", SURFACES)
async def test_native_ending_per_surface_never_fabricates_a_terminal_or_an_error(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str
):
    """C2. What the client sees is what a direct connection would have shown.

    The tempting alternative is to be helpful: synthesise `data: [DONE]`, or
    emit an `event: error` explaining what happened. Both are worse. A
    synthesised terminal marker reports a truncated answer as a complete one,
    which the client cannot even detect. A synthesised error frame is a shape
    the vendor's SDK error handling has never seen -- so being helpful is
    exactly what breaks them, while stopping is a code path they already have
    an `except` block for.
    """
    result = await die_mid_stream(gateway, client, surface)
    assert b"data: [DONE]" not in result.body
    assert b"message_stop" not in result.body
    assert b"event: error" not in result.body, "we invented an error nobody sent"


# ==========================================================================
# error_in_stream_passthrough
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES)
async def test_error_in_stream_reaches_the_client_unmodified(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str
):
    """A failure inside a 200 body is the provider's to describe, not ours.

    HTTP said fine and the protocol said otherwise. The frame is already what
    a direct connection would have delivered, so it passes through byte for
    byte -- classification happens alongside the copy and changes nothing
    about it. The absence of a terminal marker afterwards is C2 again: an
    in-band error is a reason for the stream to stop, never a reason to
    pretend it finished.
    """
    if surface == "anthropic":
        frame = wire.anthropic_error(
            kind="overloaded_error", message="injected mid-stream"
        )
    else:
        frame = b"data: " + wire.openai_error_body(
            kind="server_error", message="injected mid-stream"
        ) + b"\n\n"

    result = await stream_request(
        client, gateway.url(surface), body_for(surface),
        **{"x-fake-mode": "error-in-stream", "x-fake-events": "2"},
    )
    assert result.status == 200
    assert frame in result.body, "the in-band error frame was modified or dropped"
    assert terminal_marker(surface) not in result.body


# ==========================================================================
# huge_event_bounded
# ==========================================================================


async def test_huge_event_is_bounded_and_a_neighbouring_stream_is_untouched(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """One 200 KiB SSE frame against a 32 KiB bound, beside a healthy stream.

    Two things are being asserted and the second is the one worth having.

    The frame is refused because a frame we could not bound is a frame we
    cannot safely resynchronise after: skipping it splices two halves of
    different JSON objects together and hands the result to an SDK. So the
    request dies rather than recovering, and the client's body carries no
    terminal marker.

    The neighbour is the real test. A bound enforced by growing a buffer until
    the process dies is not a bound, and the way that failure presents in
    production is never "one request failed" -- it is every co-resident stream
    dying with it. Running the two concurrently is the only way to see the
    difference.

    Note what the client actually gets: a 200 and the ~150 bytes of the frame's
    head, then nothing. The reader parses before it enqueues, so the oversized
    frame itself never reaches the client -- but the chunk *before* it did, and
    that chunk committed us. `FrameTooLarge` is therefore a post-commitment
    failure in practice, which is why the bound has to be smaller than anything
    a real provider sends rather than a last line of defence.
    """
    huge = stream_request(
        client, gateway.url("openai"), body_for("openai"),
        **{"x-fake-mode": "huge-event", "x-fake-bytes": "200000"},
    )
    healthy = stream_request(
        client, gateway.url("openai"), body_for("openai"),
        **{"x-fake-mode": "ok", "x-fake-interval": "0.02"},
    )
    huge_result, healthy_result = await asyncio.gather(huge, healthy)

    assert huge_result.status == 200
    assert huge_result.truncated
    assert b"data: [DONE]" not in huge_result.body, "the oversized frame was served"
    assert len(huge_result.body) < MAX_FRAME_BYTES, (
        "more than one frame's worth of an unbounded frame reached the client"
    )

    assert healthy_result.status == 200
    assert not healthy_result.truncated
    assert healthy_result.body == expected_stream("openai")

    await _eventually(lambda: gateway.upstream.in_flight() == {})


# ==========================================================================
# ping_does_not_reset_progress
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES)
async def test_ping_does_not_reset_progress_and_the_stream_dies_on_that_budget(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str
):
    """C7. A provider stuck in a bad state can heartbeat politely forever.

    `ping-forever` is alive and producing nothing. Liveness says the socket is
    fine; progress says no token has appeared. Only the second one is allowed
    to keep a request alive, so the stream must end on the 0.6 s progress
    budget and not on the 8 s total -- and the elapsed time is the assertion,
    because a gateway that got this backwards still ends the request, just
    thirteen times later and after holding a connection, a buffer and a permit
    for the whole of it.
    """
    started = time.monotonic()
    result = await stream_request(
        client, gateway.url(surface), body_for(surface),
        **{"x-fake-mode": "ping-forever", "x-fake-interval": "0.05"},
    )
    elapsed = time.monotonic() - started
    assert result.status == 200
    assert terminal_marker(surface) not in result.body
    assert elapsed < TOTAL_BUDGET / 2, (
        f"ran {elapsed:.2f}s: heartbeats kept the progress clock alive"
    )
    assert elapsed >= PROGRESS_BUDGET * 0.5, (
        f"gave up after {elapsed:.2f}s, well inside the {PROGRESS_BUDGET}s budget"
    )


# ==========================================================================
# client_disconnect_cancels_upstream
# ==========================================================================


async def _eventually(predicate, *, within: float = 3.0, step: float = 0.01) -> None:
    """Poll until true or fail. Never a fixed sleep.

    Cancellation propagates through a task cancel, a TaskGroup unwind, an
    `aclose()` and a connection release, and how many event-loop turns that
    takes is not a number a test should assert. A fixed sleep long enough to
    be reliable is a fixed sleep long enough to hurt, and one short enough to
    be fast is flaky on a loaded CI box.
    """
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(step)
    raise AssertionError(f"condition still false after {within}s")


DISCONNECT_INTERVAL = 2.0
"""Seconds between upstream events in the disconnect test.

This number is the whole experiment and it has to be LARGE, which is the
opposite of every other pacing value in this file. A gateway with no
disconnect watcher still stops eventually: its next `sink.send()` hits a
transport uvicorn has already torn down and raises, which the pump turns into
`ClientDisconnected`. So a test paced at 150 ms passes with the watcher
deleted -- it just takes 150 ms longer, and nothing asserts the difference.

Two seconds between events against a one-second assertion window separates
them: only a gateway that is *parked in `receive()`* can notice a disconnect
in the gap between two writes. The progress budget is raised to match, so the
stream is not killed by the clock we are not testing.
"""


@pytest.fixture(scope="session")
def patient_gateway(gateway_factory) -> GatewayServer:
    return gateway_factory(
        budgets=Budgets(total=20.0, connect=1.0, first_event=2.0,
                        progress=5.0, client_stall=10.0)
    )


async def test_client_disconnect_cancels_the_upstream(
    patient_gateway: GatewayServer, fakes: Fakes
):
    """Row 5 of FAILURE-MODES.md: a disconnect nobody noticed is a leak.

    The client reads one chunk and closes the connection two seconds before
    the next upstream event is due. Nothing pushes that fact at an ASGI app --
    `http.disconnect` is only ever delivered to a caller already parked in
    `receive()` -- so a gateway that only learns about disconnects when a
    write fails cannot react until it next has something to write. It would
    keep pulling tokens from the provider, and paying for them, into a socket
    that is gone.

    The Anthropic surface is used because its envelope frames
    (`message_start`, `content_block_start`) are emitted unpaced, so the first
    chunk arrives immediately and the test does not spend an interval waiting
    to begin.

    Both halves are asserted because they fail independently: the gateway
    releasing its response proves our bookkeeping, and the fake's open-stream
    count returning to zero proves the socket really closed. A leak visible in
    only one of them is still a leak.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as disconnecting:
        async with disconnecting.stream(
            "POST", patient_gateway.url("anthropic"), json=body_for("anthropic"),
            headers={"x-fake-mode": "slow-drip",
                     "x-fake-interval": str(DISCONNECT_INTERVAL),
                     "x-fake-events": "50"},
        ) as response:
            assert response.status_code == 200
            async for _ in response.aiter_raw():
                break
            assert patient_gateway.upstream.in_flight() == {"fake-anthropic": 1}

    await _eventually(lambda: patient_gateway.upstream.in_flight() == {},
                      within=DISCONNECT_INTERVAL / 2)
    await _eventually(lambda: fakes.stats()["open_streams"] == 0,
                      within=DISCONNECT_INTERVAL / 2)


# ==========================================================================
# a client that never finishes its request
# ==========================================================================


@pytest.fixture(scope="session")
def impatient_gateway(gateway_factory) -> GatewayServer:
    """A one-second total budget, so a stalled client is asserted by firing."""
    return gateway_factory(
        budgets=Budgets(total=1.0, connect=0.5, first_event=0.5,
                        progress=0.5, client_stall=5.0)
    )


async def test_a_client_that_dribbles_its_body_is_never_charged_to_a_provider(
    impatient_gateway: GatewayServer, fakes: Fakes
):
    """C8, on the one path where no provider has been contacted at all.

    A raw socket rather than httpx, because httpx will not send a request it
    cannot finish: the failure needs a client that announces a `content-length`
    and then goes quiet, which is a bad mobile connection, a proxy that died
    mid-upload, or a load generator that was killed.

    `read_request_body` waits under `phase(deadline, None, ...)`, and a phase
    whose budget IS the total always reports its breach as
    `TotalDeadlineExceeded` -- GATEWAY blame, NEUTRAL health, a 504 -- for a
    stall the CLIENT caused, against a target we never opened a socket to. The
    only party this function ever waits on is the client, so the answer has to
    be `client_too_slow` (NEUTRAL, `499`). The fake's counters are the second
    half of the proof:
    denial here costs a provider nothing at all.
    """
    fakes.reset_stats()
    reader, writer = await asyncio.open_connection("127.0.0.1", impatient_gateway.port)
    try:
        writer.write(
            b"POST /v1/chat/completions HTTP/1.1\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Content-Type: application/json\r\n"
            # `Connection: close` so the read below ends at EOF. Without it the
            # socket stays open after the 499 and a single `read(n)` can return
            # the head without the body -- uvicorn writes the two separately,
            # and whether they coalesce is a property of the machine's load
            # rather than of the gateway.
            b"Connection: close\r\n"
            b"Content-Length: 4000\r\n\r\n"
            b'{"model":"fake.echo","stream":true,"messages":[]'
        )
        await writer.drain()
        # Read to EOF rather than once: uvicorn writes the status line and the
        # body in separate segments, so a single `read(4096)` returns the head
        # alone whenever the two do not coalesce -- which under a loaded tier
        # is often enough to make this a flaky test rather than a wrong one.
        raw = await asyncio.wait_for(reader.read(), timeout=5.0)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

    head = raw.decode("latin-1")
    assert head.startswith("HTTP/1.1 499"), head.splitlines()[0]
    assert '"type": "client_too_slow"' in head, head
    assert fakes.stats()["total"] == 0, "an unfinished client request reached a provider"


# ==========================================================================
# probe_no_upstream_call
# ==========================================================================


async def test_probe_describes_the_target_without_calling_it(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """CONTRACTS.md C6 in its cheapest form: denial -- and diagnosis -- is free.

    A probe that opens a connection is a probe you stop running during an
    incident, and one a health-check loop can point at is a self-inflicted
    load test against a provider that is already struggling. Every value in
    the response comes out of the catalog, which is a dictionary, so the
    counter assertion is the whole test.
    """
    response = await client.get(f"{gateway.base_url}/workloads/voice/probe")
    assert response.status_code == 200
    payload = response.json()
    assert payload["workload_id"] == "voice"
    assert payload["upstream_called"] is False
    assert payload["fake_upstreams"] is True
    target = payload["targets"][0]
    assert target["provider"] == "fake-openai"
    assert target["model"] == "fake.echo"
    assert target["served_by"] == "fake-openai/fake.echo"
    assert target["credential_present"] is True
    assert payload["budgets"]["progress"] == PROGRESS_BUDGET
    assert payload["limits"]["max_frame_bytes"] == MAX_FRAME_BYTES

    assert fakes.stats()["total"] == 0, "the probe opened an upstream connection"


async def test_probe_names_an_unknown_model_rather_than_guessing(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    response = await client.get(
        f"{gateway.base_url}/workloads/voice/probe", params={"model": "nope"}
    )
    assert response.status_code == 400
    assert response.json()["error"]["type"] == "policy_error"
    assert fakes.stats()["total"] == 0


async def test_probe_no_upstream_call(
    gateway_factory, client: httpx.AsyncClient, fakes: Fakes
):
    """The probe touches NO upstream, draining or not (P6, D5).

    The guarantee is stated as the fake's request counter staying at zero
    across a probe -- every value the endpoint reports comes out of a snapshot
    and a catalog, which are dictionaries. It is restated here under its own
    name (the P6 deliverable) and extended to the draining case: `/probe` is a
    diagnostic, so it keeps answering while the process drains -- that is
    exactly when an operator needs it -- and it still opens no socket. A
    dedicated gateway is used so flipping its `draining` flag cannot leak into
    the session-scoped server other tests share.
    """
    server = gateway_factory()
    base = server.base_url

    before = fakes.stats()["total"]
    ready = (await client.get(f"{base}/workloads/default/probe")).json()
    assert ready["upstream_called"] is False
    assert ready["draining"] is False

    # Drain the process; /healthz sheds, but /probe must still answer.
    server.app.state.gateway.draining = True
    draining = (await client.get(f"{base}/workloads/default/probe"))
    assert draining.status_code == 200
    body = draining.json()
    assert body["draining"] is True
    assert body["upstream_called"] is False

    assert fakes.stats()["total"] == before, (
        "a probe -- even while draining -- opened an upstream connection"
    )


# ==========================================================================
# Error passthrough (C4)
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_5xx_reaches_the_client_with_the_providers_own_status_and_body(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str
):
    """C4. We do not improve on a provider's error message.

    The caller's SDK already knows how to read its own vendor's error shape.
    A rewritten one is a *new* shape their error handling has never seen, so a
    gateway that helpfully normalises errors breaks exactly the clients it was
    trying to help -- and does it only on the failure path, where it is
    hardest to notice.
    """
    expected = (
        wire.anthropic_error_body(kind="server_error", message="synthetic 502")
        if surface == "anthropic"
        else wire.openai_error_body(kind="server_error", message="synthetic 502")
    )
    response = await client.post(
        gateway.url(surface), json=body_for(surface),
        headers={"x-fake-mode": "5xx", "x-fake-status": "502"},
    )
    assert response.status_code == 502
    assert response.content == expected
    assert response.headers["x-gw-attempts"] == "1"


async def test_a_429_passes_through_with_the_retry_after_the_provider_sent(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """`Retry-After` is a FLOOR (C5), so it may be rounded up and never down.

    Dropping it is worse than it looks: the provider has just told us exactly
    how long it needs, and a client that backs off on its own guess instead is
    a client guessing against a service that stopped guessing.
    """
    response = await client.post(
        gateway.url("openai"), json=body_for("openai"),
        headers={"x-fake-mode": "429", "x-fake-delay": "4"},
    )
    assert response.status_code == 429
    assert response.headers["retry-after"] == "4"
    assert response.content == wire.openai_error_body(
        kind="rate_limit_error", message="slow down"
    )


async def test_a_schema_400_passes_through_rather_than_becoming_a_502(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """The provider rejected the body. That is a fact about the request, and
    turning it into a gateway error hides the one thing the caller can fix."""
    response = await client.post(
        gateway.url("openai"), json=body_for("openai"),
        headers={"x-fake-mode": "schema-400"},
    )
    assert response.status_code == 400
    assert response.content == wire.openai_error_body(
        kind="invalid_request_error", message="bad schema"
    )


async def test_an_unknown_model_is_refused_before_any_upstream_work(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """Cheapest rejection first: a dictionary lookup, not a handshake."""
    response = await client.post(
        gateway.url("openai"), json={"model": "not-in-the-catalog", "stream": True}
    )
    assert response.status_code == 400
    assert response.json()["error"]["type"] == "policy_error"
    assert fakes.stats()["total"] == 0


async def test_a_body_that_is_not_json_is_a_client_error_not_a_provider_one(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    response = await client.post(
        gateway.url("openai"), content=b"{not json",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 400
    assert response.json()["error"]["type"] == "invalid_request"
    assert fakes.stats()["total"] == 0


async def test_an_oversized_request_body_is_413_before_any_upstream_work(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """The bound is checked as chunks arrive, so the body we refuse is a body
    we never assembled."""
    fat = {"model": "fake.echo", "stream": True,
           "messages": [{"role": "user", "content": "x" * (MAX_REQUEST_BYTES * 4)}]}
    response = await client.post(gateway.url("openai"), json=fat)
    assert response.status_code == 413
    assert response.json()["error"]["type"] == "request_too_large"
    assert fakes.stats()["total"] == 0


# ==========================================================================
# Non-streaming
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_non_streaming_request_is_forwarded_byte_for_byte_and_sized_by_us(
    gateway: GatewayServer, client: httpx.AsyncClient, surface: str
):
    """`stream: false` takes the buffered path, not the pump.

    The pump is an SSE machine -- it frames, it classifies, it runs a clock
    defined in *events* -- and a JSON response has none of those. Running one
    through it would invent a frame bound on a body with no frames and an
    `IncompleteStream` on a body with no terminal marker.

    Two properties come out of buffering, and both are asserted: the bytes are
    still exactly the upstream's, and `content-length` is ours -- computed from
    what we hold, never forwarded. Forwarding the upstream's length while
    re-chunking is the bug that presents as a truncated answer.

    Note the fake has no non-streaming *success* mode: it serves SSE whatever
    the body asks for. So this exercises the gateway's buffered path against a
    known byte string rather than a provider's JSON envelope, which is the
    part under test either way.
    """
    expected = expected_stream(surface)
    response = await client.post(
        gateway.url(surface), json=body_for(surface, stream=False),
        headers={"x-fake-mode": "ok"},
    )
    assert response.status_code == 200
    assert response.content == expected
    assert response.headers["content-length"] == str(len(expected))
    assert "transfer-encoding" not in response.headers


# ==========================================================================
# The header contract
# ==========================================================================


async def test_the_gw_headers_are_present_and_hop_by_hop_headers_are_not_forwarded(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """What a caller can rely on, and what must never reach them.

    `content-length` is the one that matters. The pump re-chunks -- it splits
    and joins upstream chunks against its own byte-bounded buffer -- and
    uvicorn frames the result as chunked transfer-encoding. Forward the
    upstream's `content-length` on top of that and the client stops reading at
    that many bytes: a truncated answer, intermittently, only when the
    re-chunked length differs from the original. That is a protocol bug that
    presents as a product bug.

    `x-fake-mode` is asserted absent as the general case: response headers
    travel by allowlist, so a header nobody thought about does not get through
    on the day a provider invents it.

    The two identity headers changed in P3 and the assertion changed with
    them. `X-Gw-Policy-Id` was the static string `"p2-static"` because there
    was no policy layer to pin; it is now the content hash of the snapshot
    this request was routed by, and `X-Gw-Catalog-Id` is the hash of the price
    table it would be billed against. They are asserted by SHAPE plus equality
    with what `/probe` reports, which is a stronger statement than either
    literal was: a hash nobody can predict is only useful if the two places
    that quote it agree, and if they did not, no capture record could be
    joined to the policy that produced it.
    """
    result = await stream_request(
        client, gateway.url("openai"), body_for("openai"), **{"x-fake-mode": "ok"}
    )
    headers = result.headers
    assert headers["x-gw-attempts"] == "1"
    assert headers["x-gw-served-by"] == "fake-openai/fake.echo"
    assert headers["x-gw-workload-id"] == "default"
    assert re.fullmatch(r"pol_[0-9a-f]{8}", headers["x-gw-policy-id"])
    assert re.fullmatch(r"cat_[0-9a-f]{8}", headers["x-gw-catalog-id"])
    probe = (await client.get(f"{gateway.base_url}/workloads/default/probe")).json()
    assert probe["policy_id"] == headers["x-gw-policy-id"]
    assert probe["catalog_id"] == headers["x-gw-catalog-id"]
    # P3 flipped this one. The client names a CATALOG id (`fake.echo`) and the
    # provider's API answers to `fake-echo`, so `apply_api_model` rewrites the
    # one field and says so. Announcing it is the whole point of the header --
    # a gateway that mutates a body and stays quiet is a gateway whose users
    # debug a request nobody sent. The RESPONSE is still byte for byte, which
    # is what the rest of this file asserts.
    assert headers["x-gw-body-modified"] == "1"

    assert headers["content-type"].startswith("text/event-stream")
    assert headers["cache-control"] == "no-cache"
    assert "content-length" not in headers
    assert headers.get_list("transfer-encoding") == ["chunked"]
    assert "x-fake-mode" not in headers, "an unlisted upstream header was forwarded"


async def test_x_gw_body_modified_announces_a_body_we_did_not_forward_verbatim(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """The one place passthrough stops being true, said out loud.

    `openrouter-toolsafe` configures `extra_body`, so honouring it means
    parsing the client's JSON, merging and re-serialising -- the bytes on the
    wire are not the bytes we received and no amount of care makes them so. A
    gateway that mutates a body and says nothing is a gateway whose users
    debug the wrong request for an afternoon, so the fact travels out as a
    header. The RESPONSE is still byte-for-byte; only the request changed.
    """
    body = {**body_for("openai"), "model": "openrouter.deepseek-v4-pro"}
    result = await stream_request(
        client, gateway.url("openai"), body, **{"x-fake-mode": "ok"}
    )
    assert result.status == 200
    assert result.headers["x-gw-body-modified"] == "1"
    assert result.headers["x-gw-served-by"] == (
        "openrouter-toolsafe/openrouter.deepseek-v4-pro"
    )
    assert result.body == expected_stream("openai")


async def test_the_client_credential_is_never_forwarded_to_the_provider(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """A configured allowlist may not contain an auth header, at all.

    `build_headers()` merges extras LAST so an operator can override
    `anthropic-version`. That same ordering means a forwarded `authorization`
    would replace the provider key we just looked up with whatever the client
    sent -- the client would choose who the provider bills. The refusal is at
    startup rather than on the request path, because a config that is only
    wrong when exploited is a config that ships.
    """
    with pytest.raises(ValueError, match="authorization"):
        ServerConfig(forward_request_headers=("authorization",)).validated()

    # And the running server, which does not list it, ignores one that is sent.
    result = await stream_request(
        client, gateway.url("openai"), body_for("openai"), **{"x-fake-mode": "ok"}
    )
    assert result.status == 200


# ==========================================================================
# Operational routes
# ==========================================================================


async def test_healthz_is_200_while_not_draining(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """Readiness, not liveness. 503-while-draining is P6; the branch exists
    and nothing sets the flag, which is the honest state to leave it in."""
    response = await client.get(f"{gateway.base_url}/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "draining": False}


async def test_metrics_exposes_the_registered_contract(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """P5 registers the real collectors into the process registry at startup.

    The handler itself did not change -- it still returns `generate_latest`
    over `gateway.registry` -- but the registry is no longer empty: every
    `metrics.METRICS` family is present, at zero, before a single request. That
    is the property a scrape config binds to (a family that appears only once
    traffic reaches it makes the alert rule's label vocabulary depend on
    traffic), so the endpoint answers for the whole contract from the first
    scrape.
    """
    from llmgw import metrics as M

    response = await client.get(f"{gateway.base_url}/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    families = {f.name for f in text_string_to_metric_families(response.text)}
    # `text_string_to_metric_families` reports a counter `llmgw_x_total` under
    # the family name `llmgw_x`, so compare against the names with `_total`
    # stripped -- which is exactly the set the contract declares.
    expected = {n[:-6] if n.endswith("_total") else n for n in M.METRIC_NAMES}
    assert expected <= families, expected - families


async def test_the_responses_surface_is_served_rather_than_announced_as_unbuilt(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """Until PLAN-2 Phase F this route was a 501 that named the phase,
    because the surface had a real `response.failed` ending to forward (C2)
    and nothing existed to forward it. The surface exists now
    (`surfaces/responses.py`, `tests/contract/test_responses.py`); the route
    answers with the provider's Response object and the gateway's headers."""
    response = await client.post(
        f"{gateway.base_url}/v1/responses",
        json={"model": "fake.echo", "input": "hi", "max_output_tokens": 16},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["object"] == "response" and body["status"] == "completed"
    assert response.headers["x-gw-model"] == "fake.echo"
    assert response.headers["x-gw-served-by"].endswith("fake.echo")
    assert "not_implemented" not in response.text


async def test_a_second_gateway_can_be_built_with_different_bounds(
    gateway_factory, gateway: GatewayServer, client: httpx.AsyncClient
):
    """The factory is the interface, and this is what it buys.

    A gateway whose settings live in module globals can only be exercised at
    whatever the process started with -- so the frame bound, the progress
    budget and the request cap, which are precisely the things worth
    asserting, become untestable. Two servers with different limits in one
    session is the proof that they are arguments and not constants.
    """
    strict = gateway_factory(max_request_bytes=256)
    body = {"model": "fake.echo", "stream": True,
            "messages": [{"role": "user", "content": "x" * 1024}]}
    assert (await client.post(strict.url("openai"), json=body)).status_code == 413
    # Byte for byte the same request, accepted by the default server's 4 KiB.
    assert (await client.post(gateway.url("openai"), json=body)).status_code == 200

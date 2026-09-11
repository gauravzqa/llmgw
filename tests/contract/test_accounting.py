"""Accounting and capture over the wire: real uvicorn, real fakes, real client.

This is the P5 half of the contract tier. `test_passthrough.py` proves the
BYTES are right; this file proves the NUMBERS the gateway then reports about
those bytes are right -- the tokens it billed, the cost basis it stamped, and
that an interrupted stream still owes for what it generated (CONTRACTS.md C3).

Everything runs through a socket, for the same reason the rest of the tier
does: `/metrics` is scraped over HTTP off the process registry, and a value
that is correct in a unit test but wired to the wrong collector is exactly the
regression a scrape catches and an ASGI transport hides.

Two named tests live here:

* ``usage_parsed_and_estimated`` -- one clean request whose known usage shows
  up as ``basis=exact`` with the fake's exact counts, then an interrupted
  stream that shows up as ``basis=estimated`` with nonzero billed tokens (C3).
* ``capture_bounded_and_nonblocking`` -- the row-7 contract: the capture queue
  is bounded in bytes and its ``offer`` never blocks the request path, so a
  stalled sink cannot slow a single stream. See that test's docstring for why
  the saturation half is driven against the ``Capture`` API the app builds
  rather than by racing a live drain over a socket.
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
from fakes import wire
from prometheus_client.parser import text_string_to_metric_families
from starlette.applications import Starlette

from llmgw.capture import Capture, CaptureRecord
from llmgw.clocks import Budgets, SystemClock
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, fake_catalog
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = pytest.mark.contract

MODEL_FOR = {"openai": "fake.echo", "anthropic": "fake.echo-anthropic"}
ROUTE_FOR = {"openai": "/v1/chat/completions", "anthropic": "/anthropic/v1/messages"}

FAKE_HEADERS = (
    "x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
    "x-fake-status", "x-fake-bytes", "x-fake-seed", "x-fake-crlf",
)

TOTAL_BUDGET = 8.0
PROGRESS_BUDGET = 0.6


def body_for(surface: str, *, stream: bool = True) -> dict:
    return {
        "model": MODEL_FOR[surface],
        "stream": stream,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
    }


# ==========================================================================
# The gateway under test -- the same tiny harness the rest of the tier uses.
# ==========================================================================


@dataclass
class GatewayServer:
    app: Starlette
    server: uvicorn.Server
    thread: threading.Thread
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, surface: str) -> str:
        return f"{self.base_url}{ROUTE_FOR[surface]}"

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
        name=f"llmgw-accounting-{port}",
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
    servers: list[GatewayServer] = []

    def make(**overrides) -> GatewayServer:
        catalog = fake_catalog(
            openai_url=f"{fakes.openai.base_url}/v1",
            anthropic_url=fakes.anthropic.base_url,
        )
        settings: dict = {
            "catalog": catalog,
            "fake_upstreams": True,
            "forward_request_headers": FAKE_HEADERS,
            "max_frame_bytes": 32 * 1024,
            "buffer_bytes": 16 * 1024,
            "breaker": BREAKER_NEVER_TRIPS,
            "budgets": Budgets(
                total=TOTAL_BUDGET, connect=1.0, first_event=1.0,
                progress=PROGRESS_BUDGET, client_stall=5.0,
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


# ==========================================================================
# Scrape helpers
# ==========================================================================


@dataclass
class Streamed:
    status: int = 0
    body: bytes = b""
    truncated: bool = False


async def stream_request(
    client: httpx.AsyncClient, url: str, body: dict, **fake: str
) -> Streamed:
    out = Streamed()
    try:
        async with client.stream("POST", url, json=body, headers=dict(fake)) as resp:
            out.status = resp.status_code
            try:
                async for chunk in resp.aiter_raw():
                    out.body += chunk
            except httpx.HTTPError:
                out.truncated = True
    except httpx.HTTPError:  # pragma: no cover - raised on aclose, same meaning
        out.truncated = True
    return out


async def scrape(client: httpx.AsyncClient, gateway: GatewayServer) -> str:
    resp = await client.get(f"{gateway.base_url}/metrics")
    assert resp.status_code == 200
    return resp.text


def metric_sum(text: str, name: str, **want: str) -> float:
    """Sum every sample of `name` whose labels are a superset of `want`.

    Summing across the labels we do NOT constrain (provider, model) keeps the
    assertion about the quantity the test cares about -- how many output
    tokens, at which basis -- and independent of which catalog id the fake
    happens to carry.
    """
    total = 0.0
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name != name:
                continue
            if all(sample.labels.get(k) == v for k, v in want.items()):
                total += sample.value
    return total


# ==========================================================================
# usage_parsed_and_estimated
# ==========================================================================


async def test_usage_parsed_and_estimated(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """The two billing states a stream can end in, proved end to end.

    **Exact.** A clean OpenAI stream carries a final usage frame
    (`wire.openai_usage_chunk`): `prompt_tokens = FRESH + CACHE_READ`,
    `completion_tokens = OUTPUT_TOKENS`, `cached_tokens = CACHE_READ`. The
    surface normalises OpenAI's inclusive prompt count into this project's
    disjoint convention (input EXCLUDES cache_read), so the gateway must bill
    exactly `FRESH_INPUT_TOKENS` input, `CACHE_READ_TOKENS` cache_read,
    `OUTPUT_TOKENS` output -- all at `basis=exact`, because the provider stated
    both halves.

    **Estimated (C3).** A `die-mid-stream` request commits (the client gets
    real bytes) and then the upstream vanishes before the usage frame. There
    is no exact output count, so the record is `basis=estimated` -- and it must
    still bill nonzero, because a stream that generated tokens the provider
    will invoice must not be billed zero just because it was cut. That is the
    whole of C3, and the metric that carries it is
    `cost_usd_total{basis="estimated"}` climbing off zero.
    """
    surface = "openai"
    url = gateway.url(surface)

    # ---- exact --------------------------------------------------------
    before = await scrape(client, gateway)
    assert metric_sum(before, "llmgw_cost_usd_total", basis="estimated") == 0.0, (
        "no interrupted request has happened yet"
    )

    got = await stream_request(client, url, body_for(surface), **{"x-fake-mode": "ok"})
    assert got.status == 200
    assert b"[DONE]" in got.body, "the clean stream reached its terminal marker"

    after = await scrape(client, gateway)

    def delta(name: str, **want: str) -> float:
        return metric_sum(after, name, **want) - metric_sum(before, name, **want)

    # The fake's known usage, straight out of wire.py, after normalisation.
    assert delta("llmgw_tokens_total", kind="output") == wire.OUTPUT_TOKENS
    assert delta("llmgw_tokens_total", kind="input") == wire.FRESH_INPUT_TOKENS
    assert delta("llmgw_tokens_total", kind="cache_read") == wire.CACHE_READ_TOKENS
    # Exact basis, and a real (nonzero) dot-product cost against the spec.
    assert delta("llmgw_cost_usd_total", basis="exact") > 0.0
    assert delta("llmgw_cost_usd_total", basis="estimated") == 0.0
    # Exactly one request counted, and it committed.
    assert delta("llmgw_requests_total", outcome="completed") == 1.0
    assert delta("llmgw_committed_total") == 1.0

    # ---- estimated / C3 ----------------------------------------------
    fakes.reset_stats()
    base2 = await scrape(client, gateway)
    interrupted = await stream_request(
        client, url, body_for(surface),
        **{"x-fake-mode": "die-mid-stream", "x-fake-events": "3"},
    )
    assert interrupted.status == 200, "the status was committed before the break"
    assert interrupted.truncated, "the body ended without a terminator"
    assert b"[DONE]" not in interrupted.body

    end = await scrape(client, gateway)

    def delta2(name: str, **want: str) -> float:
        return metric_sum(end, name, **want) - metric_sum(base2, name, **want)

    # C3: an interrupted stream is billed, at estimated basis, and NONZERO.
    assert delta2("llmgw_cost_usd_total", basis="estimated") > 0.0, (
        "C3 violated: an interrupted stream billed zero"
    )
    # The estimate is carried as output tokens (bytes / 4), so output moved.
    assert delta2("llmgw_tokens_total", kind="output") > 0.0
    # A byte reached the client, so it committed and it was counted once.
    assert delta2("llmgw_committed_total") == 1.0
    assert delta2("llmgw_requests_total") == 1.0, "counted exactly once"
    # And the negative half only the fake can answer: no second upstream was
    # opened to paper over the interruption (that would double-bill it).
    assert fakes.stats()["total"] == 1


# ==========================================================================
# capture_bounded_and_nonblocking
# ==========================================================================


class _StalledSink:
    """A sink whose every write blocks until it is released.

    The drain worker parks in `write()` forever, which is the sink outage
    row-7 exists to survive: a log collector being redeployed, a disk that has
    stopped answering. What matters is that nothing on the REQUEST path waits
    for it.
    """

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.writes = 0

    async def write(self, chunk: bytes) -> None:
        self.writes += 1
        await self.release.wait()


def _record(i: int) -> CaptureRecord:
    # ~120-160 bytes each once serialised; a handful saturate a small bound.
    return CaptureRecord(
        request_id="", tenant_id="acme", workload_id="chat",
        provider="fake-openai", model="fake.echo", outcome="completed",
        attempts=1, tokens={"input": 60, "output": 5}, cost_usd=0.001,
        basis="exact", committed=True, error_code=None, recorded_at=float(i),
    )


async def test_capture_bounded_and_nonblocking():
    """FAILURE-MODES row 7: observability must not be able to slow the request.

    Driven against the `Capture` API the app builds in `startup()` (same
    constructor, same injected clock), not over a socket, and deliberately so:
    the app exposes no config hook to install a STALLED sink -- `capture_path`
    gives a `FileSink`, and a file does not stall on command -- and forcing
    `queue_full` by racing real requests against a live drain is nondeterministic
    and slow. The invariant under test is a property of `Capture`, so it is
    asserted where it can be made deterministic; the running-gateway half below
    (`test_capture_active_never_breaks_a_request`) proves the same wiring does
    not break a real request.

    Three claims, the row-7 contract in full:

    (a) the queue is bounded in BYTES and never exceeds the bound, even as
        `offer` is hammered while the sink is wedged;
    (b) `capture_dropped_total{reason=queue_full}` climbs -- the overflow is
        dropped and COUNTED, not silently lost and not queued past the bound;
    (c) `offer()` is non-blocking: hundreds of offers against a wedged sink
        return in well under the time a single blocking write would cost.
    """
    sink = _StalledSink()
    bound = 2048
    cap = Capture(sink, max_queue_bytes=bound, clock=SystemClock())
    cap.start()
    try:
        # Let the worker pick up the first record and wedge in write(): once it
        # is parked, the queue can only drain when we release the sink.
        cap.offer(_record(0))
        for _ in range(200):
            if sink.writes >= 1:
                break
            await asyncio.sleep(0.005)
        assert sink.writes == 1, "the drain worker never engaged the sink"

        # (a) + (c): hammer offer while the sink is wedged and time it.
        queued = dropped = 0
        start = time.monotonic()
        for i in range(1, 500):
            if cap.offer(_record(i)):
                queued += 1
            else:
                dropped += 1
            # The invariant offer promises, checked on every single call.
            assert cap.queue_bytes <= bound, (
                f"queue_bytes {cap.queue_bytes} exceeded bound {bound}"
            )
        elapsed = time.monotonic() - start

        # (c) non-blocking: 499 offers against a wedged sink in a blink. A sink
        # that coupled the request path would have parked on the first write.
        assert elapsed < 0.5, f"offer path was not non-blocking: {elapsed:.3f}s"
        # (b) the overflow was dropped AND counted under the right reason.
        assert dropped > 0, "a 2 KiB bound never overflowed under 499 records"
        assert cap.dropped["queue_full"] == dropped
        assert cap.dropped["sink_error"] == 0
    finally:
        # Release the sink so the graceful drain can finish, then close with no
        # task leak. Anything still queued is counted as a shutdown drop.
        sink.release.set()
        await cap.aclose()

    # The worker task is gone -- aclose() does not leak a drainer.
    assert cap._task is None


async def test_capture_active_never_breaks_a_request(
    gateway_factory, client: httpx.AsyncClient, tmp_path
):
    """The same wiring, live: a gateway with a real capture sink still serves.

    A tiny byte bound plus a burst of concurrent requests means the app's own
    `offer()` calls will overflow the queue -- and the requests must be wholly
    unaffected: every one returns its bytes, and `/metrics` reports a queue
    that never exceeded its bound. This is the running-gateway complement to
    the deterministic `Capture`-API test above.
    """
    server = gateway_factory(
        capture_path=str(tmp_path / "capture.ndjson"),
        capture_queue_bytes=1024,
    )
    surface = "openai"

    async def one() -> int:
        got = await stream_request(
            client, server.url(surface), body_for(surface), **{"x-fake-mode": "ok"}
        )
        return got.status

    statuses = await asyncio.gather(*(one() for _ in range(24)))
    assert all(s == 200 for s in statuses), statuses

    text = await scrape(client, server)
    # The gauge is sampled at scrape; it must never read above the bound.
    assert metric_sum(text, "llmgw_capture_queue_bytes") <= 1024
    # Every terminated request was counted exactly once.
    assert metric_sum(text, "llmgw_requests_total") == 24.0

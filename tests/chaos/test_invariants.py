"""Tier 3: break it randomly, then assert what must be true regardless.

Every test here is a loop over injected faults with the assertions *outside*
the switch on what was injected. That shape is the point. A case test can only
find a bug someone already imagined; an invariant test finds the one nobody
wrote a case for -- which, for a streaming gateway, is every resource leak,
because a leak has no symptom on the request that caused it.

The five invariants, in the order they cost you money when they break:

1. **Nothing is committed that was not sent.** Whatever the failure, the bytes
   the client holds are a byte-exact PREFIX of the bytes upstream produced.
   Never a superset, never a re-ordering, never a synthesised ending (C2).
2. **Exactly one terminal outcome.** A run either returns a `PumpResult` with
   the surface's terminal marker or raises exactly once. Both, or neither, is
   an accounting bug that shows up as a billing dispute.
3. **Resources return to baseline.** `in_flight()` empty, the fake's open
   streams zero, the pump's buffer zero, the server's loop back to its task
   count. Every one of these is invisible on the failing request and fatal a
   thousand requests later (FAILURE-MODES rows 5, 18, 19).
4. **The total deadline is a ceiling.** No request outlives it by more than
   the cleanup it takes to notice, whatever combination of clocks fired.
5. **Neighbours are unaffected.** A hostile stream, an oversized frame and a
   client that stopped reading must not change what a healthy co-resident
   stream receives or when it receives it.

Reproducing a failure: the seed is printed at session start and repeated in
every assertion message. `LLMGW_CHAOS_SEED=<n> LLMGW_CHAOS_ITERS=<k>` replays
it. The default `k` keeps this file well under a minute; CI can raise it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import time

import httpx
import pytest
from fakes import wire

from llmgw import errors as E
from llmgw.clocks import Budgets, Deadline, SystemClock
from llmgw.pump import Pump, PumpResult
from llmgw.surfaces import ANTHROPIC_MESSAGES, OPENAI_CHAT
from tests.chaos.conftest import (
    BUFFER_BYTES,
    CANDIDATE,
    CLIENT_STALL_BUDGET,
    INCUMBENT,
    MAX_FRAME_BYTES,
    PROGRESS_BUDGET,
    TOTAL_BUDGET,
    Fakes,
    GatewayServer,
    RawUpstream,
    _serve,
    body_for,
    fake_mode,
    fallback_body,
    rng_for,
    scaled,
)

pytestmark = pytest.mark.chaos

SURFACES = (OPENAI_CHAT, ANTHROPIC_MESSAGES)

# The pump tier runs on a real clock with budgets small enough that every
# timeout under test fires inside one iteration. A ManualClock would be faster
# still and would test a different thing: these iterations are also asserting
# that the real `asyncio.timeout` nesting in `phase()` unwinds cleanly.
PUMP_BUDGETS = Budgets(
    total=0.6, connect=0.1, first_event=0.1, progress=0.08, client_stall=0.08
).validate()

DEADLINE_SLACK = 1.5
"""Seconds a request may outlive its total budget before we call it a breach.

Generous on purpose, and it is still an assertion: the failure this catches is
a wait with no deadline behind it (a `put()` nobody bounds, an `aclose()` on a
dead socket), which overshoots by the length of the stall and not by a
scheduling jitter. Tightening it to 100 ms would turn a loaded CI box into a
bug report.
"""


# ==========================================================================
# Doubles for the pump tier
# ==========================================================================


class RecordingSink:
    """A client that may fail, cancel, or simply stop reading.

    One class rather than four so that the fault is *data* -- the chaos loop
    picks a mode and a chunk index, and every mode goes through the same
    accounting. Four sink classes would each need their own `sends` counter and
    one of them would eventually get it wrong.
    """

    def __init__(self, *, mode: str = "ok", at: int = 1) -> None:
        self.mode = mode
        self.at = at
        self.data = b""
        self.sends = 0
        self._gate = asyncio.Event()

    async def send(self, chunk: bytes) -> None:
        self.sends += 1
        if self.sends >= self.at:
            if self.mode == "fail":
                raise ConnectionResetError("peer went away")
            if self.mode == "cancel":
                raise asyncio.CancelledError("the ASGI connection went away")
            if self.mode == "stall":
                await self._gate.wait()
        self.data += chunk

    def release(self) -> None:
        self._gate.set()


class ScriptedSource:
    """An upstream body, cut where the chaos loop says, ending how it says.

    `produced` is the ground truth the prefix invariant is checked against: it
    is what the *source* handed over, not what it intended to hand over, so an
    ending injected mid-stream shortens it exactly the way a real provider
    falling over would.
    """

    def __init__(self, chunks: list[bytes], *, ending: str = "clean") -> None:
        self._chunks = list(chunks)
        self.ending = ending
        self.produced = b""
        self.closed = False

    def __aiter__(self) -> ScriptedSource:
        return self

    async def __anext__(self) -> bytes:
        if not self._chunks:
            if self.ending == "raise":
                raise ConnectionResetError("upstream vanished")
            raise StopAsyncIteration
        chunk = self._chunks.pop(0)
        self.produced += chunk
        return chunk

    async def aclose(self) -> None:
        self.closed = True


def make_pump(sink: RecordingSink, *, surface, buffer_bytes: int,
              max_frame_bytes: int) -> Pump:
    clock = SystemClock()
    return Pump(
        surface=surface,
        sink=sink,
        deadline=Deadline(clock, PUMP_BUDGETS.total),
        budgets=PUMP_BUDGETS,
        clock=clock,
        buffer_bytes=buffer_bytes,
        max_frame_bytes=max_frame_bytes,
    )


def _body_for(surface, rng: random.Random) -> tuple[bytes, str]:
    """A stream body plus a label for what is wrong with it."""
    frames = (wire.anthropic_stream() if surface is ANTHROPIC_MESSAGES
              else wire.openai_stream())
    shape = rng.choice(["ok", "ok", "crlf", "truncated", "oversized", "no-terminal"])
    if shape == "oversized":
        big = "y" * (MAX_FRAME_BYTES * 2)
        frame = (wire.anthropic_delta(big) if surface is ANTHROPIC_MESSAGES
                 else wire.openai_chunk(big))
        return wire.joined([*frames[:2], frame, *frames[2:]]), shape
    body = wire.joined(frames)
    if shape == "crlf":
        return body.replace(b"\n", b"\r\n"), shape
    if shape == "truncated":
        return body[: max(1, len(body) // 2)], shape
    if shape == "no-terminal":
        return wire.joined(frames[:-1]), shape
    return body, shape


def _cut(body: bytes, rng: random.Random) -> list[bytes]:
    """Random chunk boundaries, including the pathological ones.

    One-byte chunks and a single whole-body chunk are both in the draw because
    they are the two ends of the parser's buffering behaviour and neither is
    what a random uniform split produces.
    """
    style = rng.choice(["uniform", "tiny", "whole", "ragged"])
    if style == "whole":
        return [body]
    if style == "tiny":
        return [body[i : i + 1] for i in range(len(body))]
    if style == "uniform":
        n = rng.randint(8, 400)
        return [body[i : i + n] for i in range(0, len(body), n)]
    out, i = [], 0
    while i < len(body):
        step = rng.randint(1, 97)
        out.append(body[i : i + step])
        i += step
    return out


# ==========================================================================
# 1. The pump, under every fault we can name
# ==========================================================================


async def test_the_pump_returns_to_baseline_and_never_invents_a_byte(
    chaos_seed: int, iterations: int
):
    """Whatever broke, the client got a prefix and the loop got its tasks back.

    The prefix check is the strongest statement in this file and the one that
    covers C2 without enumerating surfaces: if the bytes the sink received are
    always `produced[:n]`, then no failure path fabricated a terminal marker,
    no failure path synthesised an error frame, and no re-chunking reordered
    anything. A gateway that got any of those wrong fails here on the first
    iteration that hits the relevant fault, without a test having to guess
    which fault that is.

    `_LEAK_ROUNDS` of yielding before the task assertion, never a sleep: how
    many event-loop turns a TaskGroup unwind takes is not a number a test gets
    to assert, and a fixed sleep long enough to be reliable is long enough to
    make this tier unrunnable.
    """
    rng = rng_for(chaos_seed, "pump")
    baseline = len(asyncio.all_tasks())

    for i in range(scaled(iterations, 120)):
        surface = rng.choice(SURFACES)
        body, shape = _body_for(surface, rng)
        chunks = _cut(body, rng)
        ending = rng.choice(["clean", "clean", "raise"])
        sink_mode = rng.choice(["ok", "ok", "fail", "cancel", "stall"])
        source = ScriptedSource(chunks, ending=ending)
        sink = RecordingSink(mode=sink_mode, at=rng.randint(1, max(1, len(chunks))))
        pump = make_pump(
            sink, surface=surface,
            buffer_bytes=rng.choice([1, 64, 4096, 256 * 1024]),
            max_frame_bytes=MAX_FRAME_BYTES,
        )
        where = f"seed={chaos_seed} i={i} {surface.name} {shape}/{ending}/{sink_mode}"

        started = time.monotonic()
        task = asyncio.create_task(pump.run(source))
        if sink_mode == "stall" and rng.random() < 0.5:
            # Half the stalls are released before the budget fires, so the
            # backpressure path is exercised as a *pause* and not only as a
            # failure -- a pump that deadlocked on a released buffer would
            # otherwise be indistinguishable from one that timed out.
            await asyncio.sleep(0)
            sink.release()
        if rng.random() < 0.15:
            await asyncio.sleep(rng.uniform(0, 0.01))
            task.cancel()

        outcome: PumpResult | BaseException
        try:
            outcome = await task
        except BaseException as exc:  # noqa: BLE001 - the outcome IS the assertion
            outcome = exc
        sink.release()
        elapsed = time.monotonic() - started

        # --- one terminal outcome, and it agrees with itself ---------------
        if isinstance(outcome, PumpResult):
            assert outcome.terminal_seen, f"{where}: success without a terminal marker"
            assert sink.data == source.produced, f"{where}: success lost bytes"
        else:
            # `GatewayError` or a cancellation, or the exact exception the
            # source injected -- the pump forwards a source's own failure
            # rather than reclassifying it, and in production the source is
            # `UpstreamStream.aiter_raw()`, which has already mapped every
            # transport exception into the taxonomy (see
            # `test_no_vendor_exception_survives_a_body_read`). What must never
            # happen is a THIRD shape: an exception the pump invented.
            allowed = (*GatewayOrCancelled, ConnectionResetError)
            assert isinstance(outcome, allowed), f"{where}: {outcome!r}"
            if isinstance(outcome, ConnectionResetError):
                assert ending == "raise", f"{where}: the pump invented a transport error"

        # --- the client never received anything upstream did not send ------
        assert source.produced.startswith(sink.data), (
            f"{where}: the client got bytes the provider never sent"
        )

        # --- commitment is exactly 'a write was attempted' -----------------
        assert pump.committed == (sink.sends > 0), f"{where}: commitment disagrees"

        # --- resources ------------------------------------------------------
        assert pump.buffered_bytes == 0, f"{where}: buffered bytes leaked"
        assert source.closed, f"{where}: the upstream iterator was not released"
        assert elapsed < PUMP_BUDGETS.total + DEADLINE_SLACK, (
            f"{where}: ran {elapsed:.2f}s past a {PUMP_BUDGETS.total}s budget"
        )
        for _ in range(200):
            if len(asyncio.all_tasks()) <= baseline:
                break
            await asyncio.sleep(0)
        assert len(asyncio.all_tasks()) == baseline, f"{where}: a task was left behind"


GatewayOrCancelled = (E.GatewayError, asyncio.CancelledError)
"""The only two shapes `Pump.run()` is allowed to fail as.

A bare `Exception` escaping means a vendor exception reached the executor,
where every `except GatewayError` would miss it -- the exact failure
`errors.py` exists to make impossible, and one that no test above the pump can
see.
"""


# ==========================================================================
# 2. The server, against the fourteen hostile modes
# ==========================================================================

_MODE_HEADERS: dict[str, dict[str, str]] = {
    # The raw-socket upstream picks its behaviour from its own header; the
    # fakes ignore it and the raw server ignores `x-fake-*`, so one `_drive`
    # serves both without a second code path.
    "reset-mid-stream": {"x-raw-mode": "reset-mid-stream"},
    "reset-after-headers": {"x-raw-mode": "reset-after-headers"},
    "garbage": {"x-raw-mode": "garbage"},
    # Delays are pinned well above every budget so the mode ends on OUR clock.
    "stall-before-headers": {"x-fake-delay": "5"},
    "stall-after-headers": {"x-fake-delay": "5"},
    "stall-mid-stream": {"x-fake-delay": "5"},
    "ping-forever": {"x-fake-interval": "0.02", "x-fake-events": "500"},
    "slow-drip": {"x-fake-interval": "0.02", "x-fake-events": "8"},
    "huge-event": {"x-fake-bytes": str(MAX_FRAME_BYTES * 6)},
    "die-mid-stream": {"x-fake-events": "3"},
    "429": {"x-fake-delay": "0.1"},
    # `ok` with enough events to overrun uvicorn's transport buffer AND the
    # kernel's socket buffer, which is the only way a client at this layer
    # produces real ASGI backpressure: a 1 KiB response is absorbed whole and
    # the pump never blocks. Deliberately NOT in the random pool -- two
    # megabytes per request is 40x the cost of every other mode, and one
    # targeted test buys the same coverage.
    "flood": {"x-fake-events": "20000"},
}

_MODE_ALIAS = {"flood": "ok"}

MODES = (
    "ok", "ok", "stall-before-headers", "stall-after-headers", "stall-mid-stream",
    "ping-forever", "5xx", "429", "529", "die-mid-stream", "error-in-stream",
    "schema-400", "slow-drip", "huge-event", "split-frames",
)


async def _drive(client: httpx.AsyncClient, gateway: GatewayServer, *,
                 surface: str, mode: str, behaviour: str, rng: random.Random,
                 stream: bool = True) -> tuple[str, int, bytes]:
    """One request, one client behaviour. Returns (outcome, status, body).

    The outcome string is what "exactly one terminal outcome is derivable"
    means from *outside* the process: a client can distinguish a completed
    stream, a truncated one, an error response and a connection it closed
    itself, and those four are mutually exclusive.
    """
    headers = {"x-fake-mode": _MODE_ALIAS.get(mode, mode),
               **_MODE_HEADERS.get(mode, {})}
    headers["x-fake-seed"] = str(rng.randrange(1, 10_000))
    body = b""
    try:
        async with client.stream(
            "POST", gateway.url(surface), json=body_for(surface, stream=stream),
            headers=headers,
        ) as response:
            status = response.status_code
            if behaviour == "never-read":
                await asyncio.sleep(CLIENT_STALL_BUDGET + 0.3)
            try:
                async for chunk in response.aiter_raw():
                    body += chunk
                    if behaviour == "disconnect" and len(body) > 0:
                        return ("client-closed", status, body)
                    if behaviour == "slow":
                        await asyncio.sleep(0.01)
            except httpx.HTTPError:
                return ("truncated", status, body)
    except httpx.HTTPError:
        return ("truncated", 0, body)
    if status != 200:
        return ("error", status, body)
    return ("complete", status, body)


async def test_a_random_hostile_upstream_and_client_leave_nothing_behind(
    gateway: GatewayServer, fakes: Fakes, chaos_seed: int, iterations: int
):
    """FAILURE-MODES rows 5, 18 and 19, asserted after every single request.

    Three counters, and they fail independently, which is why all three are
    here. `in_flight()` is the gateway's own bookkeeping -- a missing `finally`
    in `Upstream.open`. The fake's `open_streams` is the socket really closing
    -- a response we forgot to `aclose()` keeps the provider generating and
    billing into a void, and our own counter would happily read zero. The
    server's loop task count is the orphan reader or writer that no HTTP
    response can report, sampled on the loop it actually lives on.

    A leak is monotonic, so `<= baseline` is the assertion rather than `==`:
    it is satisfied by a gateway that returns to baseline and violated by one
    that ratchets, which is the failure shape row 19 describes.
    """
    rng = rng_for(chaos_seed, "server")
    limits = httpx.Limits(max_connections=1, max_keepalive_connections=1)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), limits=limits) as client:
        await _drive(client, gateway, surface="openai", mode="ok",
                     behaviour="read", rng=rng)
        await _settle(gateway, fakes)
        baseline = gateway.task_count()

        for i in range(scaled(iterations, 26, share=0.7)):
            surface = rng.choice(["openai", "anthropic"])
            mode = rng.choice(MODES)
            behaviour = rng.choice(["read", "read", "slow", "disconnect", "never-read"])
            where = f"seed={chaos_seed} i={i} {surface}/{mode}/{behaviour}"

            started = time.monotonic()
            outcome, status, body = await _drive(
                client, gateway, surface=surface, mode=mode, behaviour=behaviour,
                rng=rng, stream=rng.random() > 0.15,
            )
            elapsed = time.monotonic() - started

            assert outcome in {"complete", "truncated", "error", "client-closed"}, where
            if outcome == "complete":
                assert status == 200, where
                assert body.endswith(b"\n"), f"{where}: a completed body ended mid-frame"
            if outcome == "error":
                assert status >= 400, where
                assert b'"error"' in body, f"{where}: an error with no error object"
            assert elapsed < TOTAL_BUDGET + DEADLINE_SLACK, (
                f"{where}: {elapsed:.2f}s against a {TOTAL_BUDGET}s total budget"
            )

            await _settle(gateway, fakes)
            assert gateway.upstream.in_flight() == {}, f"{where}: upstream still open"
            assert fakes.open_streams() == 0, f"{where}: the provider is still writing"
            assert gateway.task_count() <= baseline, (
                f"{where}: {gateway.task_count()} tasks against a baseline of {baseline}"
            )
            assert gateway.pool_size() <= _POOL_CEILING, (
                f"{where}: {gateway.pool_size()} pooled connections -- the pool is "
                "supposed to be a cap as well as a cache"
            )


_POOL_CEILING = 8
"""Pooled upstream connections we will tolerate for a sequential loop.

`httpx.Limits(max_connections=provider.max_concurrency)` is the real ceiling
and it is 1024 for the fake providers, which is not an assertion -- it would
be satisfied by a gateway that opened a fresh connection per request and never
closed one. The number that matters for FAILURE-MODES row 18 is that a
one-request-at-a-time loop settles on a handful of reused sockets, so the
ceiling here is deliberately far below the configured cap.
"""


async def _settle(gateway: GatewayServer, fakes: Fakes, *, within: float = 3.0) -> None:
    """Poll until the gateway is quiet, or give up and let the caller assert.

    Never a fixed sleep: cancellation travels through a task cancel, a
    TaskGroup unwind, an `aclose()` and a pool release, and the number of
    event-loop turns that takes is a property of the machine.
    """
    deadline = time.monotonic() + within
    while time.monotonic() < deadline:
        if not gateway.upstream.in_flight() and fakes.open_streams() == 0:
            return
        await asyncio.sleep(0.01)


# ==========================================================================
# 3. Neighbours
# ==========================================================================


async def test_a_healthy_stream_is_untouched_by_whatever_shares_the_process(
    gateway: GatewayServer, fakes: Fakes, chaos_seed: int, iterations: int
):
    """Row 17's real question: does the blast radius stay at one request?

    Each round runs one healthy stream beside a random hostile crowd -- an
    oversized frame, a provider that went silent, a client that stopped
    reading, a connection that died mid-answer. The healthy stream must come
    back byte-identical to `fakes/wire.py` and must not be dragged past its
    own budget by any of them.

    Byte identity is checked against `wire` rather than against another run of
    the same request, because a gateway that corrupted *every* stream under
    load would pass a self-comparison.
    """
    rng = rng_for(chaos_seed, "neighbours")
    expected = {
        "openai": wire.joined(wire.openai_stream()),
        "anthropic": wire.joined(wire.anthropic_stream()),
    }
    limits = httpx.Limits(max_connections=12, max_keepalive_connections=12)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), limits=limits) as client:
        for i in range(scaled(iterations, 6, share=0.15)):
            surface = rng.choice(["openai", "anthropic"])
            crowd = [
                _drive(client, gateway, surface=rng.choice(["openai", "anthropic"]),
                       mode=rng.choice(MODES),
                       behaviour=rng.choice(["read", "slow", "never-read", "disconnect"]),
                       rng=rng)
                for _ in range(rng.randint(2, 5))
            ]
            healthy = _drive(client, gateway, surface=surface, mode="ok",
                             behaviour="read", rng=rng)
            started = time.monotonic()
            results = await asyncio.gather(healthy, *crowd)
            elapsed = time.monotonic() - started
            outcome, status, body = results[0]
            where = f"seed={chaos_seed} i={i} {surface} beside {len(crowd)} hostile"

            assert outcome == "complete", f"{where}: healthy stream ended {outcome}"
            assert status == 200, where
            assert body == expected[surface], (
                f"{where}: a neighbour changed a healthy stream's bytes"
            )
            assert elapsed < TOTAL_BUDGET + DEADLINE_SLACK, (
                f"{where}: the crowd held the healthy stream for {elapsed:.2f}s"
            )

            await _settle(gateway, fakes)
            assert gateway.upstream.in_flight() == {}, where
            assert fakes.open_streams() == 0, where


# ==========================================================================
# 4. Framing, at every boundary a network can produce
# ==========================================================================


@pytest.mark.parametrize("surface", SURFACES, ids=lambda s: s.name)
@pytest.mark.parametrize("crlf", [False, True], ids=["lf", "crlf"])
async def test_a_stream_cut_at_any_offset_is_forwarded_byte_for_byte(surface, crlf):
    """Every single-byte split point in a whole stream, both line endings.

    Exhaustive rather than sampled, because the interesting offsets are three
    specific bytes -- inside a multi-byte UTF-8 sequence, between the `\\r` and
    the `\\n` of a CRLF pair, and between the two blank-line bytes that end a
    frame -- and a sampler that misses them is green until the day it is not.
    A parser that consumes an ambiguous trailing `\\r` cuts a frame in half
    here; one that decodes each chunk as text dies on the continuation byte.

    ~1200 splits per parameterisation and the whole thing runs in well under a
    second, because there is no clock and no socket involved.
    """
    frames = (wire.anthropic_stream() if surface is ANTHROPIC_MESSAGES
              else wire.openai_stream())
    body = wire.joined(frames)
    if crlf:
        body = body.replace(b"\n", b"\r\n")

    for cut in range(1, len(body)):
        sink = RecordingSink()
        pump = make_pump(sink, surface=surface, buffer_bytes=256 * 1024,
                         max_frame_bytes=MAX_FRAME_BYTES)
        result = await pump.run(ScriptedSource([body[:cut], body[cut:]]))
        assert sink.data == body, f"split at {cut} changed the bytes"
        assert result.terminal_seen, f"split at {cut} lost the terminal marker"


@pytest.mark.parametrize("surface", SURFACES, ids=lambda s: s.name)
async def test_the_frame_bound_is_exact_at_the_boundary_byte(surface):
    """A frame of exactly `max_frame_bytes` passes; one byte more does not.

    Off-by-one here is not cosmetic in either direction. Bounding at `>=`
    refuses a legal frame that a provider will send in production; bounding one
    byte late means the advertised ceiling is not the enforced one, and
    row 17 of FAILURE-MODES.md already spends its residual budget on the fact
    that the enforced number is 2x the advertised one.
    """
    build = (wire.anthropic_delta if surface is ANTHROPIC_MESSAGES
             else wire.openai_chunk)
    frame = build("x" * 2000)
    tail = (wire.anthropic_stream()[-1] if surface is ANTHROPIC_MESSAGES
            else wire.openai_done())
    body = frame + tail
    exact = len(frame)

    sink = RecordingSink()
    result = await (make_pump(sink, surface=surface, buffer_bytes=1 << 16,
                              max_frame_bytes=exact)
                    .run(ScriptedSource([body])))
    assert result.terminal_seen and sink.data == body

    tight = RecordingSink()
    with pytest.raises(E.FrameTooLarge):
        await (make_pump(tight, surface=surface, buffer_bytes=1 << 16,
                         max_frame_bytes=exact - 1)
               .run(ScriptedSource([body])))
    assert body.startswith(tight.data), "an oversized frame reached the client whole"


# ==========================================================================
# 5. The debt credit, which is the newest mechanism here
# ==========================================================================


async def test_backpressure_credit_can_never_outlive_the_total_deadline(
    chaos_seed: int, iterations: int
):
    """A slow client buys a slow provider more read budget. Not unlimited more.

    `_read_budget()` adds the time the reader spent parked on a full buffer
    back to the progress clock, which is right -- that gap is ours, not the
    provider's -- and it is also the one place in the pump where a budget is
    *grown* rather than clamped. The invariant that keeps it honest lives in
    `Deadline.slice()`, one layer down, and this loop is what proves the two
    compose: however the client stalls and however the provider dribbles, the
    request still ends inside its absolute deadline.

    Without the credit this test would still pass. It is the pair with
    `test_the_progress_clock_is_not_charged_for_a_full_buffer` in the unit
    tier that makes it meaningful: that one says the credit exists, this one
    says it is bounded.
    """
    rng = rng_for(chaos_seed, "debt")
    for i in range(scaled(iterations, 20)):
        surface = rng.choice(SURFACES)
        heartbeat = (wire.anthropic_ping() if surface is ANTHROPIC_MESSAGES
                     else wire.openai_heartbeat())
        # A provider that is alive and producing nothing (C7), paced so the
        # pump's reader keeps waking up and re-deriving its budget.
        body = wire.joined([heartbeat] * rng.randint(20, 200))
        sink = RecordingSink(mode="stall", at=rng.randint(1, 3))
        pump = make_pump(sink, surface=surface, buffer_bytes=rng.choice([1, 64, 512]),
                         max_frame_bytes=MAX_FRAME_BYTES)
        where = f"seed={chaos_seed} i={i} {surface.name}"

        started = time.monotonic()
        with pytest.raises(GatewayOrCancelled):
            await pump.run(ScriptedSource(_cut(body, rng)))
        elapsed = time.monotonic() - started
        sink.release()

        assert elapsed < PUMP_BUDGETS.total + DEADLINE_SLACK, (
            f"{where}: a stalled client bought {elapsed:.2f}s against a "
            f"{PUMP_BUDGETS.total}s total"
        )
        assert pump.buffered_bytes == 0, where


# ==========================================================================
# 6. Concurrency against one pool
# ==========================================================================


async def test_many_simultaneous_streams_do_not_share_a_byte(
    gateway: GatewayServer, fakes: Fakes, chaos_seed: int, iterations: int
):
    """Twelve streams at once, half of each surface, all of them `ok`.

    The failure this exists for is cross-talk: one `SSEParser`, one buffer or
    one `Usage` accumulator accidentally shared between requests shows up as a
    body with another request's frames in it, and it only shows up under
    concurrency. Every response must equal `wire`'s canonical bytes for ITS
    surface, which a gateway that mixed two streams cannot satisfy even by
    accident.
    """
    rng = rng_for(chaos_seed, "concurrency")
    expected = {
        "openai": wire.joined(wire.openai_stream()),
        "anthropic": wire.joined(wire.anthropic_stream()),
    }
    limits = httpx.Limits(max_connections=16, max_keepalive_connections=16)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), limits=limits) as client:
        for i in range(scaled(iterations, 3, share=0.08)):
            surfaces = [rng.choice(["openai", "anthropic"]) for _ in range(12)]
            results = await asyncio.gather(*[
                _drive(client, gateway, surface=s, mode="ok", behaviour="read", rng=rng)
                for s in surfaces
            ])
            for surface, (outcome, status, body) in zip(surfaces, results):  # noqa: B905
                where = f"seed={chaos_seed} i={i} {surface}"
                assert outcome == "complete" and status == 200, f"{where}: {outcome}"
                assert body == expected[surface], f"{where}: streams were crossed"

            await _settle(gateway, fakes)
            assert gateway.upstream.in_flight() == {}
            assert fakes.open_streams() == 0


# ==========================================================================
# 7. Denial stays free (C6) even while the process is under fire
# ==========================================================================


async def test_the_probe_never_opens_an_upstream_however_busy_the_gateway_is(
    gateway: GatewayServer, fakes: Fakes, chaos_seed: int
):
    """C6, asserted concurrently rather than in isolation.

    The existing contract test proves the probe opens no socket on a quiet
    gateway. The interesting version is under load, because "cheap" is a claim
    about a shared pool: a diagnostic that queues behind streaming traffic is
    a diagnostic that stops answering during the incident you built it for.
    """
    rng = rng_for(chaos_seed, "probe")
    fakes.reset_stats()
    limits = httpx.Limits(max_connections=16, max_keepalive_connections=16)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), limits=limits) as client:
        noise = [
            _drive(client, gateway, surface=rng.choice(["openai", "anthropic"]),
                   mode=rng.choice(MODES), behaviour="read", rng=rng)
            for _ in range(6)
        ]

        async def probes() -> list[float]:
            spans = []
            for _ in range(10):
                t0 = time.monotonic()
                r = await client.get(f"{gateway.base_url}/workloads/chaos/probe")
                assert r.status_code == 200
                assert r.json()["upstream_called"] is False
                spans.append(time.monotonic() - t0)
            return spans

        spans, *_ = await asyncio.gather(probes(), *noise)

    assert max(spans) < 1.0, f"the probe queued behind streaming traffic: {max(spans):.2f}s"
    by_path = fakes.stats()["by_path"]
    assert "/probe" not in by_path, "the probe reached an upstream"
    await _settle(gateway, fakes)
    assert gateway.upstream.in_flight() == {}


# ==========================================================================
# 7a. The two failures ASGI cannot produce, from a raw socket
# ==========================================================================


@pytest.mark.parametrize(
    ("mode", "committed"),
    # `reset-after-headers` flipped True -> False in P3, and the flip IS the
    # phase's headline change. Under P2 the server sent `http.response.start`
    # as soon as the upstream status was known, so a reset arriving after
    # headers but before any body could only truncate a 200. P3 holds the
    # status until the upstream's first body byte, so that same failure is now
    # PRE-commitment: nothing was promised to the client, the walk continues,
    # and with no target left the client gets an honest 502 instead of a
    # 200-shaped lie. A test asserting the old answer is a test asserting the
    # bug (CONTRACTS.md C1).
    [("reset-mid-stream", True), ("reset-after-headers", False), ("garbage", False)],
)
async def test_a_socket_level_reset_still_lands_in_the_taxonomy(
    raw_gateway: GatewayServer, raw_upstream: RawUpstream, caplog,
    mode: str, committed: bool,
):
    """FAILURE-MODES.md's second stated limit of the rig, retired.

    The register says `die-mid-stream` can only send a FIN, because uvicorn
    closes a transport and h11 stops, and that "a provider whose process is
    killed can produce `ECONNRESET` on a different code path with a different
    exception type. ASGI exposes no way to reset a socket." True of ASGI; the
    raw upstream in `conftest.py` sets `SO_LINGER = 0` and aborts, which is an
    RST on the wire.

    The result is worth recording precisely because it is a null: on this
    stack an RST and a FIN both reach httpx as `RemoteProtocolError`, because
    h11's incomplete-body check fires before any socket error surfaces. Same
    class, same disposition, same client experience. The residual the register
    describes is therefore narrower than it reads -- and it took a raw socket
    to find that out rather than to assume it.

    `garbage` is the other thing no ASGI app can send: a status line that is
    not HTTP. It must come back as a taxonomy class and a JSON error body, not
    as a vendor exception reaching uvicorn's 500 handler.
    """
    caplog.clear()
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
        with caplog.at_level(logging.WARNING, logger="llmgw.server"):
            outcome, status, body = await _drive(
                client, raw_gateway, surface="openai", mode=mode,
                behaviour="read", rng=random.Random(0),
            )

    if committed:
        assert outcome == "truncated", f"{mode} was not reported as a cut stream"
        assert status == 200
        codes = [r.getMessage().rsplit(": ", 1)[-1] for r in caplog.records
                 if "after commitment" in r.getMessage()]
        assert codes == ["upstream_disconnected"], codes
    else:
        assert outcome == "error", f"{mode}: {outcome}"
        assert status == 502, status
        assert b'"upstream_disconnected"' in body, body
        assert b"Traceback" not in body

    await _settle(raw_gateway, _NoFakes())
    assert raw_gateway.upstream.in_flight() == {}, f"{mode}: upstream still open"


class _NoFakes:
    """`_settle` polls two counters; the raw upstream has only one of them."""

    @staticmethod
    def open_streams() -> int:
        return 0


# ==========================================================================
# 7b. ASGI backpressure, which no tier below this one can reach
# ==========================================================================


async def test_a_client_that_stops_reading_a_two_megabyte_stream_blames_no_provider(
    gateway: GatewayServer, fakes: Fakes, caplog
):
    """The one path the unit tier cannot reach: uvicorn's own flow control.

    `pump.py` sets its commitment flag before the await precisely because ASGI
    `send()` returning means uvicorn accepted the bytes and nothing more --
    backpressure arrives late and coarsely, once the transport's write buffer
    is over its high watermark. A 1 KiB response never gets there: the kernel
    swallows it whole, `send()` never blocks, and the client-stall budget is
    exercised only against a sink double. Two megabytes does get there, which
    makes this the first test in the repo where a real socket refuses real
    bytes.

    Two assertions, and the second is the one that matters. The request has to
    end on the CLIENT-STALL budget rather than on the total, or backpressure is
    not reaching the pump at all. And the code the gateway logs has to be a
    client fault: a provider that streamed perfectly for two megabytes must not
    acquire a FAILURE health signal because the browser at the other end went
    to lunch (C8).
    """
    fakes.reset_stats()
    caplog.clear()
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        with caplog.at_level(logging.WARNING, logger="llmgw.server"):
            started = time.monotonic()
            async with client.stream(
                "POST", gateway.url("openai"), json=body_for("openai"),
                headers={"x-fake-mode": "ok", **_MODE_HEADERS["flood"]},
            ) as response:
                assert response.status_code == 200
                await asyncio.sleep(CLIENT_STALL_BUDGET + 0.6)
                with contextlib.suppress(httpx.HTTPError):
                    await _read_all(response)
            elapsed = time.monotonic() - started

    assert elapsed < TOTAL_BUDGET, (
        f"ended after {elapsed:.2f}s -- on the total deadline, not on the "
        "client-stall budget, so ASGI backpressure never reached the pump"
    )
    codes = [r.getMessage().rsplit(": ", 1)[-1] for r in caplog.records
             if "after commitment" in r.getMessage()]
    assert codes, "the gateway never reported ending the request"
    assert codes[-1] in {"client_too_slow", "client_disconnected"}, (
        f"a client that stopped reading was recorded as {codes[-1]!r}"
    )

    await _settle(gateway, fakes)
    assert gateway.upstream.in_flight() == {}
    assert fakes.open_streams() == 0


# ==========================================================================
# 8. Shutdown with streams open
# ==========================================================================


async def test_a_disconnect_at_any_instant_of_a_stream_releases_the_upstream(
    gateway: GatewayServer, fakes: Fakes, chaos_seed: int, iterations: int
):
    """Row 5, swept across the whole lifetime of a request.

    The contract tier disconnects at one well-chosen instant. This sweeps the
    instant randomly -- before the headers, between two frames, mid-frame,
    after the terminal marker -- because the disconnect paths are genuinely
    different code: one is `await_disconnect` winning a race, one is a failed
    `sink.send()`, and one is a client that closed a socket we had already
    finished with. All three have to end with the provider released.
    """
    rng = rng_for(chaos_seed, "disconnect")
    for i in range(scaled(iterations, 16, share=0.5)):
        surface = rng.choice(["openai", "anthropic"])
        after = rng.uniform(0.0, 0.25)
        where = f"seed={chaos_seed} i={i} {surface} after={after:.3f}s"
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            with contextlib.suppress(httpx.HTTPError, asyncio.TimeoutError):
                async with client.stream(
                    "POST", gateway.url(surface), json=body_for(surface),
                    headers={"x-fake-mode": "slow-drip", "x-fake-interval": "0.03",
                             "x-fake-events": "40"},
                ) as response:
                    assert response.status_code == 200, where
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(_read_all(response), timeout=after)

        await _settle(gateway, fakes)
        assert gateway.upstream.in_flight() == {}, f"{where}: upstream still open"
        assert fakes.open_streams() == 0, f"{where}: the provider is still writing"


async def _read_all(response: httpx.Response) -> None:
    async for _ in response.aiter_raw():
        pass


# ==========================================================================
# 12. Multi-target plans: two upstreams inside one request
# ==========================================================================

FALLBACK_PAIRS: tuple[tuple[dict[str, str], dict[str, str]], ...] = (
    # (candidate, incumbent). Each pair is a distinct shape of walk, and the
    # ones that matter are the pairs where the SECOND upstream is opened -- the
    # only case P2 could not produce.
    (fake_mode("5xx"), fake_mode("ok")),
    (fake_mode("stall-after-headers", delay="5"), fake_mode("ok")),
    (fake_mode("stall-before-headers", delay="5"), fake_mode("ok")),
    (fake_mode("429", delay="0.05"), fake_mode("ok")),
    (fake_mode("5xx"), fake_mode("5xx", status="503")),
    (fake_mode("die-mid-stream", events="3"), fake_mode("ok")),
    (fake_mode("ok"), fake_mode("ok")),
    (fake_mode("5xx"), fake_mode("slow-drip", interval="0.02", events="6")),
)

FALLBACK_BEHAVIOURS = ("read", "read", "disconnect", "never-read", "slow")

TOTAL_BUDGET_FALLBACK = 2.0
"""`[defaults.budgets] total` in the chaos policy file. Kept here as a
constant because the assertion below is about it and a number that appears
only inside a TOML string is a number nobody notices going stale."""


async def test_a_fallback_leaves_no_upstream_no_task_and_no_stream_behind(
    fallback_gateways, fakes: Fakes, chaos_seed: int, iterations: int
):
    """Rows 5, 18 and 19 again, on the walk that opens TWO upstreams.

    Everything the single-target loop asserts, in the case it structurally
    could not reach. A fallback opens a second upstream while the first has
    just been unwound, so this is where a missing `finally`, a response nobody
    closed, or a reader task orphaned between attempts would show up -- and
    none of the three is visible in the client's response, which is why all
    three are read off the server and the provider instead.

    The client behaviours are in the loop for the same reason: a disconnect
    that lands BETWEEN two attempts is a cancellation delivered to a scope
    that owns no upstream at that instant, and it must still leave nothing
    behind. `die-mid-stream` is in the pair list to keep the commitment
    invariant honest under the same randomisation -- once a byte is out, the
    second target must not be opened at all, so this loop also checks that a
    committed request never reports two attempts.
    """
    rng = rng_for(chaos_seed, "fallback")
    baselines: dict[int, int] = {}

    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        for i in range(scaled(iterations, 10, share=0.4)):
            candidate, incumbent = rng.choice(FALLBACK_PAIRS)
            behaviour = rng.choice(FALLBACK_BEHAVIOURS)
            streaming = rng.random() > 0.2
            no_retry = rng.random() > 0.5
            gateway = fallback_gateways(candidate, incumbent)
            where = (f"seed={chaos_seed} i={i} "
                     f"{candidate['x-fake-mode']}->{incumbent['x-fake-mode']} "
                     f"{behaviour} stream={streaming} no_retry={no_retry}")

            if gateway.port not in baselines:
                # One warm request per server before its baseline is taken:
                # the first request through a fresh pool creates tasks that
                # outlive it by design.
                with contextlib.suppress(httpx.HTTPError):
                    await client.post(gateway.url("openai"),
                                      json=fallback_body(stream=False))
                await _settle(gateway, fakes)
                baselines[gateway.port] = gateway.task_count()

            headers = {"x-gw-no-retry": "1"} if no_retry else {}
            started = time.monotonic()
            attempts, status = await _drive_fallback(
                client, gateway, behaviour=behaviour, streaming=streaming,
                headers=headers,
            )
            elapsed = time.monotonic() - started

            assert elapsed < TOTAL_BUDGET_FALLBACK + DEADLINE_SLACK, (
                f"{where}: {elapsed:.2f}s against a {TOTAL_BUDGET_FALLBACK}s total"
            )
            if attempts is not None:
                # The amplification bound: two targets, `max_attempts=2`, so
                # `len(targets) + (max_attempts - 1)` is 3 -- and 1 when the
                # caller took the retries (C5 leaves the fallback, so still
                # up to 2).
                assert attempts <= 3, f"{where}: {attempts} attempts"
                if no_retry:
                    assert attempts <= 2, f"{where}: no-retry bought a repeat"
                if status == 200 and candidate["x-fake-mode"] == "die-mid-stream":
                    assert attempts == 1, (
                        f"{where}: a second target was opened after commitment"
                    )

            await _settle(gateway, fakes)
            assert gateway.upstream.in_flight() == {}, f"{where}: upstream still open"
            assert fakes.open_streams() == 0, f"{where}: the provider is still writing"
            assert gateway.task_count() <= baselines[gateway.port], (
                f"{where}: {gateway.task_count()} tasks against a baseline of "
                f"{baselines[gateway.port]}"
            )
            assert gateway.pool_size() <= _POOL_CEILING, f"{where}: pool grew"


async def _drive_fallback(
    client: httpx.AsyncClient, gateway: GatewayServer, *, behaviour: str,
    streaming: bool, headers: dict[str, str],
) -> tuple[int | None, int]:
    """One request against a two-target gateway. Returns (attempts, status).

    `attempts` is `X-Gw-Attempts` when a status line was received at all --
    which on a fallback is the number the whole amplification argument is
    about, read from the only place a client can see it.
    """
    body = fallback_body(stream=streaming)
    try:
        async with client.stream("POST", gateway.url("openai"), json=body,
                                 headers=headers) as response:
            status = response.status_code
            attempts = response.headers.get("x-gw-attempts")
            if behaviour == "never-read":
                await asyncio.sleep(CLIENT_STALL_BUDGET + 0.3)
            try:
                async for chunk in response.aiter_raw():
                    if behaviour == "disconnect" and chunk:
                        break
                    if behaviour == "slow":
                        await asyncio.sleep(0.01)
            except httpx.HTTPError:
                pass
            return (int(attempts) if attempts is not None else None, status)
    except httpx.HTTPError:
        return (None, 0)


async def test_a_disconnect_between_two_attempts_releases_both_upstreams(
    fallback_gateways, fakes: Fakes, chaos_seed: int, iterations: int
):
    """The window the P3 decision opened, swept.

    Between `Upstream.open()` returning and the first upstream body byte the
    client has been promised nothing, and the executor may still walk to the
    next target. A client that hangs up anywhere inside that window is
    cancelling a scope that may own one upstream, two in sequence, or none --
    and the assertion is the same in all three cases: the provider's socket
    comes back.

    Nothing about this is observable from the client, which has by
    construction gone, so every assertion is on the server and on the fake.
    """
    rng = rng_for(chaos_seed, "midfallback")
    gateway = fallback_gateways(
        fake_mode("stall-after-headers", delay="5"),
        fake_mode("slow-drip", interval="0.03", events="40"),
    )
    for i in range(scaled(iterations, 9, share=0.35)):
        # 0.5s is the candidate's first-event budget, so the sweep straddles
        # the instant the plan moves from one target to the other.
        after = rng.uniform(0.0, 0.9)
        where = f"seed={chaos_seed} i={i} after={after:.3f}s"
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
            with contextlib.suppress(httpx.HTTPError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    client.post(gateway.url("openai"), json=fallback_body()),
                    timeout=after,
                )

        await _settle(gateway, fakes)
        assert gateway.upstream.in_flight() == {}, f"{where}: upstream still open"
        assert fakes.open_streams() == 0, f"{where}: the provider is still writing"
        assert CANDIDATE not in gateway.upstream.in_flight(), where
        assert INCUMBENT not in gateway.upstream.in_flight(), where


# ==========================================================================
# 6. Exactly one accounting record per terminated request
# ==========================================================================
#
# The other invariants are about resources; this one is about MONEY. `_record`
# is the executor's `on_finish`, and the whole billing story rests on it firing
# exactly once for every request that entered the executor -- once on a clean
# completion, once on a truncation, once on a client disconnect where the
# cancellation is still propagating, once on a fallback that opened two targets.
# Zero would be a request nobody billed; twice would be a request billed double.
# Neither has a symptom on the request that caused it -- it surfaces a month
# later as an invoice that does not reconcile -- which is exactly the kind of
# bug this tier exists to catch by counting rather than by watching.
#
# The mechanism is the new `capture_path` knob: point the process capture at a
# `FileSink`, and every `capture.offer()` the record hook makes becomes one
# NDJSON line. Drive randomized load, drain on shutdown, and the line count is
# the accounting-record count -- which must equal the number of requests that
# reached the executor, exactly.


def _accounting_gateway(fakes: Fakes, path) -> GatewayServer:
    """A chaos gateway whose capture writes one line per record to `path`."""
    from llmgw.server.config import ServerConfig, fake_catalog
    from tests.chaos.conftest import build_gateway  # local: avoids import churn
    from tests.contract.conftest import BREAKER_NEVER_TRIPS

    catalog = fake_catalog(
        openai_url=f"{fakes.openai.base_url}/v1",
        anthropic_url=fakes.anthropic.base_url,
    )
    config = ServerConfig(
        catalog=catalog,
        fake_upstreams=True,
        forward_request_headers=(
            "x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
            "x-fake-status", "x-fake-bytes", "x-fake-seed", "x-fake-crlf",
        ),
        max_frame_bytes=MAX_FRAME_BYTES,
        buffer_bytes=BUFFER_BYTES,
        breaker=BREAKER_NEVER_TRIPS,
        capture_path=str(path),
        budgets=Budgets(
            total=TOTAL_BUDGET, connect=0.8, first_event=0.8,
            progress=PROGRESS_BUDGET, client_stall=CLIENT_STALL_BUDGET,
        ),
    )
    return _serve(build_gateway(config))


async def test_every_terminated_request_produced_exactly_one_accounting_record(
    fakes: Fakes, chaos_seed: int, iterations: int, tmp_path
):
    """Invariant 6: terminated requests and accounting records are the same set.

    A dedicated gateway (not the session one) with a `FileSink` capture, so the
    count is a property of a file rather than of a scrape sampled mid-flight.
    Every request below carries a valid tenant and a real model, so every one
    reaches the executor and therefore owes exactly one record -- including the
    ones the client disconnects from and the ones the upstream truncates, which
    are the paths where "the hook still fires while the cancellation is on its
    way out" is the whole claim.

    The count is asserted after `shutdown()`: closing the capture drains what
    was offered before the stop, so a record the hook produced is a line in the
    file, and the equality is exact -- never zero (a request nobody billed),
    never a line short or long (a request billed the wrong number of times).
    """
    rng = rng_for(chaos_seed, "accounting")
    capture_file = tmp_path / "records.ndjson"
    gateway = _accounting_gateway(fakes, capture_file)
    sent = 0
    try:
        limits = httpx.Limits(max_connections=4, max_keepalive_connections=4)
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0), limits=limits
        ) as client:
            # A warm request so the pool and the drain worker are both up before
            # the count starts, then settle so nothing from it is still in
            # flight when the real load begins.
            await _drive(client, gateway, surface="openai", mode="ok",
                         behaviour="read", rng=rng)
            sent += 1
            await _settle(gateway, fakes)

            for i in range(scaled(iterations, 26, share=0.7)):
                surface = rng.choice(["openai", "anthropic"])
                mode = rng.choice(MODES)
                behaviour = rng.choice(
                    ["read", "read", "slow", "disconnect", "never-read"]
                )
                where = f"seed={chaos_seed} i={i} {surface}/{mode}/{behaviour}"
                outcome, _status, _body = await _drive(
                    client, gateway, surface=surface, mode=mode,
                    behaviour=behaviour, rng=rng, stream=rng.random() > 0.15,
                )
                sent += 1
                assert outcome in {"complete", "truncated", "error", "client-closed"}, (
                    where
                )
                await _settle(gateway, fakes)
    finally:
        # Stop the server so the Starlette shutdown runs `capture.aclose()`,
        # which drains every record offered before the stop to the FileSink.
        gateway.stop()

    lines = [
        ln for ln in capture_file.read_text(encoding="utf-8").splitlines() if ln.strip()
    ]
    # Exactly-once, both directions: one record per request that entered the
    # executor -- and every request above did, so the two counts are equal.
    assert len(lines) == sent, (
        f"seed={chaos_seed}: {len(lines)} accounting records for {sent} terminated "
        f"requests -- exactly-once is violated"
    )
    # And the records are real, terminated ones: every line is a JSON object
    # with an outcome from the closed vocabulary. A blank or duplicated line
    # would already have failed the count; this proves they are records.
    import json as _json

    for ln in lines:
        rec = _json.loads(ln)
        assert rec["outcome"] in {
            "completed", "interrupted", "rejected", "failed", "canceled"
        }, f"seed={chaos_seed}: a record with a bogus outcome {rec.get('outcome')!r}"

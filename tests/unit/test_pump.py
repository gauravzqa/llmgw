"""Pump tests: the commitment boundary, byte-for-byte passthrough, and the
two silences that must never be confused with each other.

Everything here is pure unit -- a ManualClock, an async iterator of bytes, and
a sink double. No sockets, no sleeping, and therefore no test that proves a
30-second budget by taking 30 seconds. The autouse fixture asserts the two
things a pump can leak that nothing else would notice: tasks and buffered
bytes.
"""

from __future__ import annotations

import asyncio

import pytest
from fakes import wire

from llmgw import errors as E
from llmgw.clocks import Budgets, Deadline, ManualClock
from llmgw.pump import Pump, PumpResult
from llmgw.surfaces import ANTHROPIC_MESSAGES, OPENAI_CHAT

SURFACES = (OPENAI_CHAT, ANTHROPIC_MESSAGES)

_LIVE_PUMPS: list[Pump] = []


# ============================================================== fixtures


@pytest.fixture(autouse=True)
async def no_task_or_buffer_leaks():
    """Every test returns the loop to its baseline task count and every pump
    it built to zero buffered bytes.

    Both leaks are invisible in the assertion the test actually wrote and
    fatal in production: an orphaned reader holds an upstream connection open
    forever, and a buffer that survives its request is memory that only ever
    grows. Structured concurrency is supposed to make the first one
    impossible; this fixture is what proves it rather than assuming it.
    """
    _LIVE_PUMPS.clear()
    baseline = len(asyncio.all_tasks())
    yield
    for _ in range(50):
        if len(asyncio.all_tasks()) <= baseline:
            break
        await asyncio.sleep(0)
    assert len(asyncio.all_tasks()) == baseline, "the pump left a task behind"
    for pump in _LIVE_PUMPS:
        assert pump.buffered_bytes == 0, "the pump left bytes in its buffer"
    _LIVE_PUMPS.clear()


def make_budgets(**overrides) -> Budgets:
    defaults = dict(total=600.0, connect=2.0, first_event=20.0, progress=15.0,
                    client_stall=30.0)
    defaults.update(overrides)
    return Budgets(**defaults).validate()


def make_pump(
    *,
    sink,
    surface=OPENAI_CHAT,
    clock: ManualClock | None = None,
    budgets: Budgets | None = None,
    buffer_bytes: int = 256 * 1024,
    max_frame_bytes: int = 1 << 20,
) -> Pump:
    clock = clock if clock is not None else ManualClock(start=0.0)
    budgets = budgets if budgets is not None else make_budgets()
    pump = Pump(
        surface=surface,
        sink=sink,
        deadline=Deadline(clock, budgets.total),
        budgets=budgets,
        clock=clock,
        buffer_bytes=buffer_bytes,
        max_frame_bytes=max_frame_bytes,
    )
    _LIVE_PUMPS.append(pump)
    return pump


async def settle(rounds: int = 12) -> None:
    """Let every runnable task reach its next await point."""
    for _ in range(rounds):
        await asyncio.sleep(0)


async def drive(clock: ManualClock, task, *, step: float, limit: int) -> None:
    """Advance manual time until the pump is finished, or `limit` steps."""
    for _ in range(limit):
        if task.done():
            return
        await clock.advance(step)


def chunked(data: bytes, size: int) -> list[bytes]:
    return [data[i : i + size] for i in range(0, len(data), size)]


def stream_bytes(surface) -> bytes:
    frames = (
        wire.openai_stream() if surface is OPENAI_CHAT else wire.anthropic_stream()
    )
    return wire.joined(frames)


# =============================================================== doubles


class ScriptedSource:
    """An async iterator of raw body bytes that remembers what it did.

    `yielded` is how a test observes the reader *pausing* under backpressure,
    which is the only externally visible symptom of the buffer bound working.
    """

    def __init__(self, chunks, *, clock: ManualClock | None = None, gap: float = 0.0):
        self._chunks = list(chunks)
        self._clock = clock
        self._gap = gap
        self.yielded = 0
        self.closed = False

    @property
    def remaining(self) -> int:
        return len(self._chunks)

    def __aiter__(self) -> ScriptedSource:
        return self

    async def __anext__(self) -> bytes:
        if self._clock is not None and self._gap:
            await self._clock.sleep(self._gap)
        if not self._chunks:
            raise StopAsyncIteration
        chunk = self._chunks.pop(0)
        self.yielded += 1
        return chunk

    async def aclose(self) -> None:
        self.closed = True


class PingForeverSource:
    """A provider wedged in a bad state, heartbeating politely and generating
    nothing. The whole reason C7 exists."""

    def __init__(self, frame: bytes, *, clock: ManualClock, gap: float):
        self._frame = frame
        self._clock = clock
        self._gap = gap
        self.yielded = 0
        self.closed = False

    def __aiter__(self) -> PingForeverSource:
        return self

    async def __anext__(self) -> bytes:
        await self._clock.sleep(self._gap)
        self.yielded += 1
        return self._frame

    async def aclose(self) -> None:
        self.closed = True


class RecordingSink:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []
        self.sends = 0

    async def send(self, chunk: bytes) -> None:
        self.sends += 1
        self.chunks.append(chunk)

    @property
    def data(self) -> bytes:
        return b"".join(self.chunks)


class BlockingSink(RecordingSink):
    """A client that stopped reading. Blocks from the `block_at`-th send until
    `release()`."""

    def __init__(self, *, block_at: int = 1) -> None:
        super().__init__()
        self.block_at = block_at
        self.blocked = False
        self._gate = asyncio.Event()

    async def send(self, chunk: bytes) -> None:
        self.sends += 1
        if self.sends >= self.block_at:
            self.blocked = True
            await self._gate.wait()
        self.chunks.append(chunk)

    def release(self) -> None:
        self._gate.set()


class FailingSink(RecordingSink):
    """A client socket that breaks. `fail_at=1` is the prefix-may-have-landed
    case the whole module exists for."""

    def __init__(self, *, fail_at: int = 1, exc: BaseException | None = None) -> None:
        super().__init__()
        self.fail_at = fail_at
        self.exc = exc or ConnectionResetError("peer went away")

    async def send(self, chunk: bytes) -> None:
        self.sends += 1
        if self.sends == self.fail_at:
            raise self.exc
        self.chunks.append(chunk)


# ================================================== passthrough fidelity


@pytest.mark.parametrize("surface", SURFACES, ids=lambda s: s.name)
async def test_the_sink_receives_exactly_the_bytes_the_source_produced(surface):
    data = stream_bytes(surface)
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=surface)
    result = await pump.run(ScriptedSource(chunked(data, 97)))
    assert sink.data == data
    assert result.bytes_out == len(data)
    assert result.terminal_seen is True


@pytest.mark.parametrize("size", [1, 2, 3, 7, 13, 64, 1_000_000])
@pytest.mark.parametrize("surface", SURFACES, ids=lambda s: s.name)
async def test_byte_identity_survives_adversarial_chunk_splits(surface, size):
    """A TCP segment boundary has no relationship to a frame boundary, so the
    tee has to be invariant under every split -- including a split that lands
    inside the two bytes of an `e-acute`, which is what the multi-byte tokens
    in `wire.TOKENS` are there to produce."""
    data = stream_bytes(surface)
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=surface, buffer_bytes=128)
    await pump.run(ScriptedSource(chunked(data, size)))
    assert sink.data == data


async def test_the_tee_reads_the_stream_without_changing_it(monkeypatch):
    """Parsing is a side channel. A surface that throws while classifying must
    cost a `parse_failures` count and nothing else -- billing accuracy is
    never worth a served request."""
    data = stream_bytes(OPENAI_CHAT)
    sink = RecordingSink()

    class ExplodingSurface:
        name = OPENAI_CHAT.name
        path = OPENAI_CHAT.path

        def classify(self, ev):
            raise RuntimeError("a dialect we have never seen")

        def apply_usage(self, ev, usage):
            raise RuntimeError("nor a usage shape")

        def error_from_event(self, ev):
            return None

        def native_ending(self, last_event=None):
            return b""

    pump = make_pump(sink=sink, surface=ExplodingSurface())
    with pytest.raises(E.IncompleteStream):
        await pump.run(ScriptedSource(chunked(data, 64)))
    assert sink.data == data
    assert pump.result.usage.parse_failures == pump.result.events


# ======================================================== the commitment


async def test_committed_is_false_until_a_write_is_attempted():
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink)
    assert pump.committed is False
    source = ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 80))
    task = asyncio.create_task(pump.run(source))
    await settle()
    assert sink.blocked is True
    assert pump.committed is True
    sink.release()
    await task
    assert pump.committed is True


async def test_an_empty_upstream_body_never_commits_us():
    """Nothing was written, so nothing about the client is inconsistent and a
    fallback is still legal. This is the contested half of C1."""
    sink = RecordingSink()
    pump = make_pump(sink=sink)
    with pytest.raises(E.IncompleteStream):
        await pump.run(ScriptedSource([]))
    assert pump.committed is False
    assert E.decide(E.IncompleteStream("x"), committed=pump.committed).try_next is True


async def test_a_sink_that_fails_on_its_first_write_still_leaves_the_pump_committed():
    """THE headline test. A write that raises may already have put a prefix on
    the wire: the exception says the send did not complete, not that nothing
    left the building.

    Set the flag after the await and this case reads as uncommitted, the
    executor opens a second upstream, and the client gets the tail of answer B
    stapled to the head of answer A -- undetectable from the outside and
    impossible to apologise for afterwards. So the flag is set before the
    await, `bytes_out` stays 0 (we genuinely do not know how much landed), and
    `decide()` refuses every further attempt.
    """
    sink = FailingSink(fail_at=1)
    pump = make_pump(sink=sink)
    with pytest.raises(E.ClientDisconnected) as caught:
        await pump.run(ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 80)))

    assert pump.committed is True
    assert pump.result.bytes_out == 0
    assert sink.chunks == []

    disposition = E.decide(caught.value, committed=pump.committed)
    assert disposition.try_next is False
    assert disposition.retry_same is False
    assert disposition.outcome is E.Outcome.CANCELED
    assert caught.value.health is E.Health.NEUTRAL


async def test_a_sink_that_fails_mid_stream_ends_the_request_natively():
    """C2: the body simply stops. No synthesised `[DONE]`, no invented error
    frame -- the client's own SDK already knows how to read a truncated stream
    of its own vendor's dialect, and a shape it has never seen is worse than
    silence."""
    sink = FailingSink(fail_at=3)
    pump = make_pump(sink=sink)
    with pytest.raises(E.ClientDisconnected):
        await pump.run(ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 80)))
    assert pump.committed is True
    assert sink.sends == 3, "no fourth write was attempted, terminal marker included"
    assert b"[DONE]" not in sink.data, "we did not synthesise a completion"
    assert pump.result.bytes_out == len(sink.data)


async def test_a_source_that_fails_before_any_write_leaves_a_fallback_available():
    class Broken:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise E.UpstreamDisconnected("reset before any frame")

    sink = RecordingSink()
    pump = make_pump(sink=sink)
    with pytest.raises(E.UpstreamDisconnected) as caught:
        await pump.run(Broken())
    assert pump.committed is False
    assert E.decide(caught.value, committed=False).try_next is True


# =================================================== progress vs liveness


async def test_a_ping_forever_stream_dies_on_the_progress_budget():
    """C7, and the reason the two clocks exist at all. A provider stuck in a
    bad state heartbeats politely forever; if a ping reset the progress clock
    this stream would hold a connection, a permit and a buffer until the total
    deadline -- 600 seconds instead of 15."""
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=600.0, progress=15.0, client_stall=120.0)
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES, clock=clock, budgets=budgets)
    source = PingForeverSource(wire.anthropic_ping(), clock=clock, gap=2.0)

    task = asyncio.create_task(pump.run(source))
    for _ in range(80):
        if task.done():
            break
        await clock.advance(1.0)

    with pytest.raises(E.StallTimeout):
        await task
    assert 15.0 <= clock.now() <= 25.0, "died on progress, not on the total deadline"
    assert clock.now() < budgets.total
    assert pump.result.content_events == 0
    assert source.yielded > 3, "the socket was demonstrably alive the whole time"


async def test_a_stream_of_pure_heartbeats_never_counts_a_content_event():
    frames = [wire.anthropic_ping() for _ in range(5)]
    frames.append(wire.anthropic_message_stop())
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES)
    result = await pump.run(ScriptedSource(frames))
    assert result.events == 6
    assert result.content_events == 0
    assert result.first_event_at is None
    assert result.terminal_seen is True


async def test_first_event_at_marks_the_first_content_event():
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES)
    result = await pump.run(ScriptedSource(wire.anthropic_stream()))
    assert result.first_event_at is not None
    assert result.content_events == len(wire.TOKENS)


# ========================================================= backpressure


async def test_a_slow_client_bounds_the_buffer_and_pauses_the_reader():
    """The bound is in BYTES and the pause is real. A message-count bound
    would let 200 streams hold 8 MiB each -- 1.6 GiB of heap under a gauge
    reading a reassuring 200 -- and a reader that kept draining upstream into
    an unbounded buffer would turn a slow client into our OOM."""
    data = stream_bytes(OPENAI_CHAT)
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, buffer_bytes=64)
    source = ScriptedSource(chunked(data, 100))

    task = asyncio.create_task(pump.run(source))
    await settle()

    assert sink.blocked is True
    assert pump.buffered_bytes <= 64
    paused_at = source.yielded
    assert paused_at > 0 and source.remaining > 0, "the reader stopped part-way, as it must"
    await settle()
    assert source.yielded == paused_at, "the reader kept pulling from a stalled client"

    sink.release()
    result = await task
    assert sink.data == data
    assert result.terminal_seen is True
    assert pump.buffered_bytes == 0


async def test_a_single_chunk_larger_than_the_buffer_is_split_not_admitted():
    """Otherwise the ceiling is advisory: the one case that would have to be
    let through wholesale is exactly the case that blows the budget."""
    payload = wire.openai_chunk("x" * 4000) + wire.openai_done()
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, buffer_bytes=256)
    task = asyncio.create_task(pump.run(ScriptedSource([payload])))
    await settle()
    assert pump.buffered_bytes <= 256
    sink.release()
    await task
    assert sink.data == payload


async def test_a_client_that_stops_reading_is_client_too_slow_and_never_the_provider():
    """C8. If a client-side incident could open a breaker, a bad frontend
    deploy takes every provider offline at exactly the moment you need them."""
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=600.0, progress=15.0, client_stall=5.0)
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, clock=clock, budgets=budgets, buffer_bytes=64)
    source = ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 100))

    task = asyncio.create_task(pump.run(source))
    await settle()
    for _ in range(10):
        if task.done():
            break
        await clock.advance(1.0)

    with pytest.raises(E.ClientTooSlow) as caught:
        await task
    err = caught.value
    assert err.health is E.Health.NEUTRAL, "the provider was innocent"
    assert err.blame is E.Blame.CLIENT
    assert err.outcome is E.Outcome.INTERRUPTED
    assert E.decide(err, committed=pump.committed).health is E.Health.NEUTRAL
    assert pump.committed is True
    assert clock.now() <= 10.0


async def test_a_stalled_client_never_becomes_a_provider_failure_at_the_deadline():
    """The same C8 rule as above, at the one clock alignment that used to break it.

    `phase()` reports a breach of the TOTAL deadline as `TotalDeadlineExceeded`
    rather than as the phase's own class -- correct for the reader, where the
    party we were waiting on really is the provider, and wrong for the writer,
    where it is the client. Whenever `client_stall` outlives what is left of
    the total (which is every stream that has been running for
    `total - client_stall` seconds), the identical client behaviour that
    produces a NEUTRAL `ClientTooSlow` early in a request produced a
    FAILURE-health, PROVIDER-blamed error late in one. A client that stops
    reading must not teach a breaker anything about a provider that was still
    streaming perfectly, and it must not depend on WHEN it stopped.
    """
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=4.0, connect=1.0, first_event=3.0, progress=3.0,
                           client_stall=5.0)
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, clock=clock, budgets=budgets, buffer_bytes=64)
    source = ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 100))

    task = asyncio.create_task(pump.run(source))
    await settle()
    for _ in range(12):
        if task.done():
            break
        await clock.advance(0.5)

    with pytest.raises(E.ClientTooSlow) as caught:
        await task
    err = caught.value
    assert err.health is E.Health.NEUTRAL, "the provider was innocent"
    assert err.blame is E.Blame.CLIENT
    assert E.decide(err, committed=pump.committed).health is E.Health.NEUTRAL
    assert isinstance(err.cause, E.TotalDeadlineExceeded), (
        "the real reason the wait ended has to survive the reclassification"
    )


async def test_the_progress_clock_is_not_charged_for_a_full_buffer():
    """The reverse misclassification, and the one that is easy to ship.

    While the reader is parked on a full buffer it is not reading the upstream
    socket, so the gap between upstream events is *ours*. Charge it to the
    progress clock and one slow client produces `StallTimeout` -- a provider
    fault that counts against a breaker and moves traffic to a different
    provider. Here the client stalls for 40 s against a 15 s progress budget
    and the stream still completes.
    """
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=600.0, progress=15.0, client_stall=120.0)
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, clock=clock, budgets=budgets, buffer_bytes=64)
    data = stream_bytes(OPENAI_CHAT)
    source = ScriptedSource(chunked(data, 100), clock=clock, gap=0.5)

    task = asyncio.create_task(pump.run(source))
    await drive(clock, task, step=0.5, limit=4)
    assert sink.blocked is True

    await clock.advance(40.0)   # the client reads nothing for 40 s
    sink.release()
    await drive(clock, task, step=0.5, limit=200)
    result = await task

    assert result.terminal_seen is True
    assert sink.data == data


# ============================================================== outcomes


async def test_a_truncated_stream_is_incomplete_with_partial_estimated_usage():
    """C3. A clean EOF is not a completion, and the usage counted so far has to
    survive the failure -- a result object that only existed on the happy path
    would bill every interrupted request zero."""
    frames = wire.anthropic_stream()[:4]  # message_start, block_start, two deltas
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES)

    with pytest.raises(E.IncompleteStream) as caught:
        await pump.run(ScriptedSource(frames))

    result = pump.result
    assert result.terminal_seen is False
    assert result.committed is True
    assert result.usage.input_exact is True, "message_start stated the prompt side"
    assert result.usage.output_exact is False, "and nothing ever stated the output"
    assert result.usage.exact is False
    assert result.usage.input_tokens == wire.ANTHROPIC_INPUT_TOKENS
    assert E.decide(caught.value, committed=True).outcome is E.Outcome.INTERRUPTED


async def test_an_openai_stream_without_done_is_incomplete():
    frames = wire.openai_stream()[:-1]
    sink = RecordingSink()
    pump = make_pump(sink=sink)
    with pytest.raises(E.IncompleteStream):
        await pump.run(ScriptedSource(frames))
    assert pump.result.terminal_seen is False
    assert pump.result.usage.exact is True, "the usage chunk still arrived"


async def test_usage_is_folded_from_both_halves_of_a_clean_stream():
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES)
    result = await pump.run(ScriptedSource(wire.anthropic_stream()))
    assert result.usage.input_tokens == wire.ANTHROPIC_INPUT_TOKENS
    assert result.usage.output_tokens == wire.OUTPUT_TOKENS
    assert result.usage.cache_read_tokens == wire.CACHE_READ_TOKENS
    assert result.usage.exact is True
    assert isinstance(result, PumpResult)


async def test_an_in_stream_error_is_reported_and_forwarded_never_synthesised():
    """HTTP said fine; the protocol said otherwise. The frame the provider sent
    reaches the client verbatim (C4) and the classification reaches the caller
    on the result, so the breaker learns what the provider actually said."""
    frames = [
        wire.anthropic_message_start(),
        wire.anthropic_block_start(),
        wire.anthropic_delta("Hello"),
        wire.anthropic_error(kind="api_error", message="boom"),
    ]
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES)

    with pytest.raises(E.InStreamError):
        await pump.run(ScriptedSource(frames))

    assert sink.data == wire.joined(frames), "the provider's own error frame passed through"
    assert isinstance(pump.result.in_stream_error, E.InStreamError)
    assert pump.result.terminal_seen is False


async def test_an_overloaded_in_stream_error_keeps_the_providers_own_class():
    frames = [wire.anthropic_message_start(), wire.anthropic_error()]
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=ANTHROPIC_MESSAGES)
    with pytest.raises(E.UpstreamOverloaded):
        await pump.run(ScriptedSource(frames))


# ================================================================ bounds


async def test_a_frame_over_max_frame_bytes_kills_the_request():
    """`huge_event_bounded`. The bound is ours, so the blame is ours and there
    is no next target to try: another provider would send the same oversized
    frame and spend a second budget hitting the same wall."""
    huge = b"data: " + b"A" * 8192 + b"\n\n"
    sink = RecordingSink()
    pump = make_pump(sink=sink, max_frame_bytes=1024)

    with pytest.raises(E.FrameTooLarge) as caught:
        await pump.run(ScriptedSource([huge]))

    assert pump.committed is False, "the frame we refused to parse never reached the client"
    assert sink.chunks == []
    assert caught.value.blame is E.Blame.GATEWAY
    assert caught.value.try_next is False


async def test_a_frame_over_the_bound_after_commitment_still_stops_the_stream():
    good = wire.openai_chunk("Hello")
    huge = b"data: " + b"A" * 8192 + b"\n\n"
    sink = RecordingSink()
    pump = make_pump(sink=sink, max_frame_bytes=1024)
    with pytest.raises(E.FrameTooLarge):
        await pump.run(ScriptedSource([good, huge]))
    assert pump.result.terminal_seen is False


# ========================================================== cancellation


async def test_cancellation_propagates_and_leaves_nothing_behind():
    """A genuine cancellation must unwind, not be reclassified. The pump cannot
    tell a shutdown from a client hangup by looking at a `CancelledError`, and
    a gateway that guesses will eventually guess during the shutdown path."""
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, buffer_bytes=64)
    source = ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 100))

    task = asyncio.create_task(pump.run(source))
    await settle()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert pump.buffered_bytes == 0
    assert source.closed is True, "the upstream iterator was released"


async def test_a_sink_that_cancels_is_never_reported_as_a_completed_request():
    """A TaskGroup discards a child that ended cancelled, so a sink raising
    `CancelledError` -- which an ASGI server really does on a disconnect --
    makes the writer vanish without a word. The reader would then finish the
    stream, see the terminal marker, and `run()` would report a completed
    request to a client that received nothing.

    A silent success is the worst failure shape there is: no error, no
    interrupted counter, a 2xx SLO that looks perfect, and a user staring at
    an empty response.
    """

    class CancellingSink(RecordingSink):
        async def send(self, chunk: bytes) -> None:
            self.sends += 1
            raise asyncio.CancelledError("the ASGI connection went away")

    sink = CancellingSink()
    pump = make_pump(sink=sink, buffer_bytes=64)
    source = ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 100))

    with pytest.raises(E.ClientDisconnected) as caught:
        await pump.run(source)
    assert pump.committed is True
    assert caught.value.health is E.Health.NEUTRAL
    assert source.closed is True
    assert source.remaining > 0, (
        "the reader kept draining upstream into a buffer with no consumer"
    )


async def test_a_reader_cancelled_on_backpressure_is_never_reported_as_completed():
    """The mirror image of the test above, and the same TaskGroup rule.

    A `TaskGroup` discards a child that ended cancelled -- either child. The
    writer's half of that is guarded by `_drained`. The reader's half was not:
    a reader cancelled while parked on a full buffer has already fed the
    terminal marker to the parser (parsing happens BEFORE the enqueue, on
    purpose) but has not handed that chunk to the writer. The writer then
    drains what it does have, `_drained` is True, `terminal_seen` is True, and
    `run()` reports a completed request for a body whose last frame -- the
    `data: [DONE]` an SDK is waiting for -- never left the building.

    Reaching it means cancelling the reader task by name, because no P2 call
    path cancels one child alone. That is exactly why it is worth a guard: the
    invariant must hold for the P3 executor that has not been written yet.
    """
    data = stream_bytes(OPENAI_CHAT)
    head, tail = data[:-40], data[-40:]
    sink = BlockingSink(block_at=1)
    pump = make_pump(sink=sink, buffer_bytes=len(head))
    source = ScriptedSource([head, tail])

    task = asyncio.create_task(pump.run(source))
    await settle()
    readers = [t for t in asyncio.all_tasks() if t.get_name() == "llmgw-pump-read"]
    assert readers, "the reader should still be parked on the full buffer"
    readers[0].cancel()
    sink.release()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert sink.data != data, "the tail never reached the client; that is the point"
    assert pump.buffered_bytes == 0


async def test_the_source_is_released_on_the_happy_path_too():
    sink = RecordingSink()
    pump = make_pump(sink=sink)
    source = ScriptedSource(wire.openai_stream())
    await pump.run(source)
    assert source.closed is True


async def test_a_pump_is_single_use():
    sink = RecordingSink()
    pump = make_pump(sink=sink)
    await pump.run(ScriptedSource(wire.openai_stream()))
    with pytest.raises(RuntimeError):
        await pump.run(ScriptedSource(wire.openai_stream()))


async def test_buffered_bytes_is_zero_before_and_after_a_run():
    sink = RecordingSink()
    pump = make_pump(sink=sink)
    assert pump.buffered_bytes == 0
    await pump.run(ScriptedSource(chunked(stream_bytes(OPENAI_CHAT), 37)))
    assert pump.buffered_bytes == 0


# ================================================ queued, not dead (PLAN-2 A4)


async def test_a_stall_after_heartbeats_with_no_content_is_marked_queued():
    """DeepSeek holds a request for up to ten minutes sending `: keep-alive`.
    The executor's first-event budget covers only the first CHUNK, and that
    keep-alive arrives at once, so the timeout that ends a queued wait is the
    pump's progress budget: a `StallTimeout` with no content ever seen. Marked
    `queued` so the caller can keep the provider's health NEUTRAL -- a busy
    provider is not a dead one, and five polite waits must not open a breaker."""
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=600.0, progress=15.0, client_stall=120.0)
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=OPENAI_CHAT, clock=clock, budgets=budgets)
    source = PingForeverSource(wire.openai_comment("keep-alive"), clock=clock, gap=2.0)

    task = asyncio.create_task(pump.run(source))
    for _ in range(80):
        if task.done():
            break
        await clock.advance(1.0)

    with pytest.raises(E.StallTimeout) as caught:
        await task
    assert getattr(caught.value, "queued", False) is True
    assert pump.liveness_before_first_event is True
    assert pump.result.liveness_before_first_event is True
    assert pump.result.content_events == 0
    # The keep-alives were forwarded, so the pump IS committed: a comment is a
    # body byte and C1 says the first byte to the client commits. That is the
    # honest reading of today's contract and the reason `queued` can only fix
    # the *health* of this failure, not its disposition -- once a provider's
    # keep-alive has reached the client there is nothing to fall back to.
    # Extending the commitment hold past heartbeat-only chunks is the
    # executor's decision, not the pump's (PLAN-2 A4, noted for the owner).
    assert pump.committed is True


async def test_a_stall_with_no_heartbeats_is_not_queued():
    """Silence is not a queue. Nothing arrived at all, so the provider gets
    the ordinary provider-blamed stall."""
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=600.0, progress=15.0, client_stall=120.0)
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=OPENAI_CHAT, clock=clock, budgets=budgets)
    source = PingForeverSource(b"", clock=clock, gap=1000.0)  # never yields in time

    task = asyncio.create_task(pump.run(source))
    for _ in range(40):
        if task.done():
            break
        await clock.advance(1.0)

    with pytest.raises(E.StallTimeout) as caught:
        await task
    assert getattr(caught.value, "queued", False) is False
    assert pump.liveness_before_first_event is False


async def test_heartbeats_then_content_is_a_normal_stream():
    """The flag records history, not a verdict: a stream that heartbeats and
    then produces is complete, exact, and raises nothing."""
    frames = [wire.openai_comment(), wire.openai_heartbeat(), *wire.openai_stream()]
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=OPENAI_CHAT)
    result = await pump.run(ScriptedSource(frames))
    assert result.terminal_seen
    assert result.liveness_before_first_event is True
    assert result.content_events == len(wire.TOKENS)


async def test_a_stall_after_content_is_never_queued():
    """Once a word has been produced, a later silence is a stall in the
    ordinary sense, whatever heartbeats preceded the first token."""
    clock = ManualClock(start=0.0)
    budgets = make_budgets(total=600.0, progress=5.0, client_stall=120.0)
    sink = RecordingSink()
    pump = make_pump(sink=sink, surface=OPENAI_CHAT, clock=clock, budgets=budgets)

    class ContentThenSilence:
        def __init__(self):
            self._sent = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if not self._sent:
                self._sent = True
                return wire.openai_comment() + wire.openai_chunk("hello")
            await clock.sleep(10_000.0)
            raise StopAsyncIteration

    task = asyncio.create_task(pump.run(ContentThenSilence()))
    for _ in range(40):
        if task.done():
            break
        await clock.advance(1.0)
    with pytest.raises(E.StallTimeout) as caught:
        await task
    assert getattr(caught.value, "queued", False) is False
    assert pump.result.content_events == 1

"""The commitment boundary: upstream bytes in, client bytes out, one flag.

This module owns the single fact the whole reliability story hangs off --
*did a byte reach the client?* -- and it is the only place in the gateway
allowed to set it. Everything else in the request path is recoverable. A
connect failure is recoverable. A 500 is recoverable. A stall before the
first write is recoverable. The instant a byte may have landed on the client
socket, none of it is: the client is already reading an answer, and a second
attempt would splice a *different* answer onto the end of the first one.

Three things make that hard enough to need a file of its own.

--------------------------------------------------------------------------
1. A failed write may still have delivered a prefix
--------------------------------------------------------------------------

    async with phase(...):
        self._committed = True        # BEFORE the await, never after
        await self._sink.send(chunk)

`send()` raising does not mean nothing was sent. It means the send did not
*complete*. The kernel may hold half the chunk in its socket buffer and the
client may have already rendered it. Set the flag after the await and a
first-write failure looks uncommitted; the executor then opens a second
upstream, and the client gets the tail of answer B stapled to the head of
answer A. There is no way to detect that from the outside and no way to
apologise for it afterwards, which is why the assignment sits above the
await rather than below it.

The assignment is also the *last* statement before the await, not the first
statement of the loop body: `phase()` runs `deadline.check()` on entry and
can raise there, and claiming commitment for a write we never attempted
throws away a fallback that was still legal.

--------------------------------------------------------------------------
2. Backpressure has to be real, and counted in bytes
--------------------------------------------------------------------------

Reader and writer are two tasks under one `TaskGroup`, joined by a buffer
bounded in BYTES. When the buffer is full the reader blocks, which stops
draining the upstream socket, which stops the provider -- a slow client
becomes real backpressure all the way to the model instead of becoming our
heap.

Bounded in bytes and never in messages. 200 streams each holding one 8 MiB
event is 1.6 GiB of RSS while a message-count gauge reads a reassuring 200.
That is mental-model failure #3 -- "memory counted in the wrong unit" -- and
it is the failure that gets found by an OOM kill rather than by a dashboard.
`buffered_bytes` is the gauge, and it counts the chunk currently in flight to
the sink as well as the queued ones, because that chunk is still in memory.

--------------------------------------------------------------------------
3. The tee is a side channel, and must stay one
--------------------------------------------------------------------------

The bytes written to the client are the bytes read from upstream, unmodified
and un-reframed in meaning. Parsing happens *alongside* the copy, feeding an
`SSEParser` whose events drive the stall clocks, the usage accumulator and
the terminal-marker check. Nothing the tee learns is allowed to change what
the client receives, and nothing it fails at is allowed to break the copy --
a surface that throws while classifying a frame increments
`usage.parse_failures` and the stream keeps going.

The one exception is deliberate: `FrameTooLarge` kills the request, because a
frame we could not bound is a frame we cannot safely resynchronise after.

--------------------------------------------------------------------------
Which clock blames whom
--------------------------------------------------------------------------

Two independent silences, two different culprits, and getting them backwards
is an outage rather than a mistake:

* the *provider* went quiet   -> `StallTimeout`   (progress clock, C7)
* the *client* stopped reading -> `ClientTooSlow` (client-stall budget, C8)

`ClientTooSlow` and `ClientDisconnected` are `Health.NEUTRAL`. If a client
incident could open circuit breakers, then a bad frontend deploy takes every
provider offline at the moment you need them most.

The subtle half of that split is the reverse direction: while the reader is
parked on a full buffer it is not reading the socket, so the gap between
upstream events is *our* doing, not the provider's. Charging that gap to the
progress clock would report `StallTimeout` -- a provider fault, `try_next`,
counted against a breaker -- for a stall the client caused. So the reader
keeps a `_debt` of time spent blocked on backpressure and credits it back to
the read budget. See `_read_budget`.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from llmgw.clocks import Budgets, Clock, Deadline, StallClock, phase
from llmgw.errors import (
    Blame,
    ClientDisconnected,
    ClientTooSlow,
    GatewayError,
    IncompleteStream,
    StallTimeout,
)
from llmgw.sse import SSEEvent, SSEParser
from llmgw.surfaces.base import EventKind, Surface, Usage


@runtime_checkable
class Sink(Protocol):
    """Where bytes go to reach the client.

    In production this is a closure over Starlette's `send`; in the unit tier
    it is a double that can be made slow, blocking or failing. The protocol is
    one method on purpose: anything richer (flush, drain, is_connected) would
    be a promise about the transport that no transport actually keeps -- there
    is no portable "did the client receive it" and pretending otherwise is how
    the commitment flag ends up in the wrong place.
    """

    async def send(self, chunk: bytes) -> None: ...


@dataclass(slots=True)
class PumpResult:
    """Everything the caller needs to record one streamed response.

    Available even when `run()` raises, via `Pump.result` -- which is not a
    convenience. CONTRACTS.md C3 says an interrupted stream bills the tokens
    it did generate, so the usage accumulated before the failure has to
    survive the failure. A result object that only exists on the happy path
    would make every interrupted request bill zero.
    """

    committed: bool
    """A byte may have reached the client. Never un-set. See `decide()`."""

    bytes_out: int
    """Bytes for which `sink.send()` RETURNED. A write that raised is not
    counted here and still commits us -- the asymmetry is the honest one: we
    know a prefix may have landed, we do not know how much of it."""

    events: int
    content_events: int
    usage: Usage
    terminal_seen: bool
    """The surface's terminal marker was parsed. False means the answer the
    client holds is truncated, whatever the HTTP status said."""

    first_event_at: float | None
    """Monotonic instant of the first CONTENT event, for
    `llmgw_time_to_first_event_seconds` -- which is defined against the first
    *content* event, not the first frame. A `message_start` or a ping arriving
    in 8 ms says nothing about when the user saw a word."""

    in_stream_error: GatewayError | None
    """An `event: error` / `error` chunk inside a 200 body, if one arrived."""


class Pump:
    """Copy one upstream stream to one client, and decide what it cost.

    Single use: `run()` consumes the source to completion or to failure, and a
    second call raises. Streams are not resumable, so a reusable pump would
    only ever be a pump with stale commitment state, which is the one piece of
    state that must never be stale.
    """

    __slots__ = (
        "_surface", "_sink", "_deadline", "_budgets", "_clock", "_buffer",
        "_parser", "_stall", "_committed", "_started", "_bytes_out", "_events",
        "_content_events", "_usage", "_terminal_seen", "_first_event_at",
        "_in_stream_error", "_last_event", "_debt", "_client_gone", "_drained",
        "_source_ended",
    )

    def __init__(
        self,
        *,
        surface: Surface,
        sink: Sink,
        deadline: Deadline,
        budgets: Budgets,
        clock: Clock,
        buffer_bytes: int = 256 * 1024,
        max_frame_bytes: int = 1 << 20,
    ) -> None:
        if buffer_bytes < 1:
            raise ValueError("buffer_bytes must be positive")
        self._surface = surface
        self._sink = sink
        self._deadline = deadline
        self._budgets = budgets
        self._clock = clock
        self._buffer = _ByteBuffer(buffer_bytes)
        self._parser = SSEParser(max_frame_bytes=max_frame_bytes, emit_comments=True)
        self._stall = StallClock(budgets, clock=clock)

        self._committed = False
        self._started = False
        self._bytes_out = 0
        self._events = 0
        self._content_events = 0
        self._usage = Usage()
        self._terminal_seen = False
        self._first_event_at: float | None = None
        self._in_stream_error: GatewayError | None = None
        self._last_event: SSEEvent | None = None
        self._debt = 0.0
        self._client_gone = False
        self._drained = False
        self._source_ended = False

    # ------------------------------------------------------------ inspection

    @property
    def committed(self) -> bool:
        """True once a write to the client has been *attempted*.

        Deliberately not "succeeded". `errors.decide()` reads this to refuse
        every further attempt, and the refusal has to cover the write that
        raised halfway through the kernel's socket buffer.
        """
        return self._committed

    @property
    def buffered_bytes(self) -> int:
        """Bytes held between the upstream read and the client write.

        The `llmgw_pump_buffered_bytes` gauge. Includes the chunk currently
        being written, because that chunk is still resident; excludes the
        parser's partial frame, which is bounded separately by
        `max_frame_bytes` and is the parser's own gauge to report.
        """
        return self._buffer.size

    @property
    def result(self) -> PumpResult:
        """A snapshot, readable at any time -- including from an `except`.

        The caller needs `usage` and `committed` precisely when `run()` raised,
        so this is a property rather than only a return value.
        """
        return PumpResult(
            committed=self._committed,
            bytes_out=self._bytes_out,
            events=self._events,
            content_events=self._content_events,
            usage=self._usage,
            terminal_seen=self._terminal_seen,
            first_event_at=self._first_event_at,
            in_stream_error=self._in_stream_error,
        )

    # ------------------------------------------------------------------- run

    async def run(self, source: AsyncIterator[bytes]) -> PumpResult:
        """Pump `source` to the sink until it ends, stalls, or breaks.

        Returns a `PumpResult` only for a stream that reached its surface's
        terminal marker. Everything else raises, because a clean EOF is *not*
        a completion: a provider whose body simply stopped has truncated the
        answer, and a gateway that returns 200 for that silently ships half
        answers to users while every dashboard stays green. That is
        `IncompleteStream`, and `Pump.result` still carries the partial usage
        so the request can be billed and recorded as `interrupted` (C3).

        Cancellation propagates untouched (C8): only positive evidence -- the
        sink itself raising -- becomes `ClientDisconnected`. A bare
        `CancelledError` could equally be a shutdown, a parent timeout, or a
        supervisor, and converting all three into a client fault would lie to
        the breaker in the direction that hurts least *until* the day it is
        the shutdown path.
        """
        if self._started:
            raise RuntimeError("Pump.run() is single use; construct one per stream")
        self._started = True
        try:
            try:
                async with asyncio.TaskGroup() as group:
                    group.create_task(self._read(source), name="llmgw-pump-read")
                    group.create_task(self._write(), name="llmgw-pump-write")
            except BaseExceptionGroup as group_error:
                # `from None`: the chosen exception IS one of the group's own
                # members, so chaining the group onto it would print the error
                # as its own cause. The member it beat is a cancellation this
                # pump caused itself.
                raise _primary(group_error) from None
            if not self._drained:
                # The writer left without reaching end-of-buffer and without
                # an error the group could report -- which happens when the
                # sink raises CancelledError, because a TaskGroup discards a
                # child that ended cancelled. Without this check the reader
                # finishes the stream, `terminal_seen` is True, and `run()`
                # reports a completed request to a client that received
                # nothing. A silent success is the worst failure shape there
                # is; the sink stopping is a client fault, so it is NEUTRAL.
                raise ClientDisconnected(
                    f"client sink stopped accepting after {self._bytes_out} bytes"
                )
            if not self._source_ended:
                # The mirror of the check above, for the other child. A
                # TaskGroup discards EITHER cancelled child, and the reader
                # parses before it enqueues -- so a reader cancelled while
                # parked on a full buffer has already set `terminal_seen` for a
                # chunk the writer never saw. The writer then drains what it
                # does have, `_drained` is True, and `run()` would report a
                # completed request for a body whose `data: [DONE]` never left
                # the building.
                #
                # Cancellation, never a GatewayError: we do not know who
                # cancelled the reader, and inventing `IncompleteStream` would
                # bill the provider for our own teardown. Same rule as
                # `_primary`'s all-cancelled branch.
                raise asyncio.CancelledError(
                    "pump reader was cancelled before the upstream body ended "
                    f"({self._bytes_out} bytes to client)"
                )
            if not self._terminal_seen:
                # An in-band error is a better explanation than "it stopped",
                # and it carries the provider's own classification with it.
                if self._in_stream_error is not None:
                    raise self._in_stream_error
                raise IncompleteStream(
                    "upstream body ended without the surface's terminal marker "
                    f"after {self._events} events ({self._bytes_out} bytes to client)"
                )
            return self.result
        except GatewayError:
            await self._end_natively()
            raise
        finally:
            self._buffer.abort()
            self._parser.close()
            await _aclose(source)

    # ---------------------------------------------------------------- reader

    async def _read(self, source: AsyncIterator[bytes]) -> None:
        """Drain the source into the buffer, teeing every byte through the
        parser on the way past.

        Ordering inside the loop is load-bearing: parse FIRST, enqueue second.
        A chunk that blows `max_frame_bytes` must not reach the client, and
        the whole point of the bound is that we stop rather than resynchronise
        onto whatever byte follows -- resynchronising mid-frame splices two
        halves of different JSON objects into one and hands it to an SDK.
        """
        try:
            while True:
                async with phase(
                    self._deadline, self._read_budget(), on_timeout=StallTimeout
                ):
                    chunk = await _next_chunk(source)
                if chunk is None:
                    for event in self._parser.close():
                        self._observe(event)
                    self._source_ended = True
                    return
                if not chunk:
                    # httpx hands out empty chunks. Feeding one to a parser
                    # that treats "no bytes" as "end of frame" invents events.
                    continue
                for event in self._parser.feed(chunk):
                    self._observe(event)
                blocked_from = self._clock.now()
                await self._buffer.put(chunk)
                self._debt += self._clock.now() - blocked_from
                if self._buffer.aborted:
                    # The writer is gone. Reading on would drain an upstream
                    # into a buffer nobody empties, and then park on it
                    # forever -- a hang, not a timeout, because backpressure
                    # waits are deliberately not on a clock.
                    return
        finally:
            # The writer is waiting on this even when we are unwinding: a
            # reader that dies without closing the buffer leaves the writer
            # parked forever and the TaskGroup unable to join.
            self._buffer.close()

    def _read_budget(self) -> float:
        """How long the next upstream read may take before we blame upstream.

        `stall_clock.remaining()` plus the time we spent blocked on our own
        full buffer. Without the credit, a client that stopped reading for
        longer than the progress budget makes the very next upstream read time
        out instantly -- and a `StallTimeout` is a *provider* fault: it counts
        against the target's circuit breaker and sends the retry to a
        different provider. One slow client would open breakers across the
        fleet. The debt resets whenever a content event genuinely restarts the
        progress clock.
        """
        return self._stall.remaining() + self._debt

    def _observe(self, event: SSEEvent) -> None:
        """Account for one parsed frame. Never raises.

        Accounting is a side channel. A surface that throws on a frame it has
        never seen must not take down a request that is otherwise being served
        perfectly -- that trades a billing inaccuracy for an outage. The
        failure is counted (`llmgw_usage_parse_failures_total`) rather than
        swallowed, because a provider that quietly changes its frame shape
        would otherwise make every stream an estimate with nothing to alert on.
        """
        self._events += 1
        self._last_event = event
        try:
            kind = self._surface.classify(event)
            if kind is EventKind.CONTENT:
                self._content_events += 1
                if self._first_event_at is None:
                    self._first_event_at = self._clock.now()
                # The only kind that resets progress. C7 in one branch.
                self._stall.mark_progress()
                self._debt = 0.0
            else:
                if kind is EventKind.TERMINAL:
                    self._terminal_seen = True
                elif kind is EventKind.ERROR and self._in_stream_error is None:
                    # Recorded, not raised. The frame is already on its way to
                    # the client -- it is what a direct connection would have
                    # shown them (C2/C4) -- and the caller decides what an
                    # in-band failure means for the outcome and the breaker.
                    self._in_stream_error = self._surface.error_from_event(event)
                self._stall.mark_liveness()
            self._surface.apply_usage(event, self._usage)
        except Exception:  # noqa: BLE001 - see docstring: accounting never breaks serving
            self._usage.parse_failures += 1
            self._stall.mark_liveness()

    # ---------------------------------------------------------------- writer

    async def _write(self) -> None:
        """Drain the buffer into the sink, one bounded write at a time."""
        try:
            await self._write_loop()
        finally:
            if not self._drained:
                # Leaving without having emptied the buffer: raising, or
                # cancelled. A cancelled child is discarded silently by the
                # TaskGroup, so this is the only thing that stops the reader
                # parking forever on a buffer with no consumer.
                self._buffer.abort()

    async def _write_loop(self) -> None:
        while True:
            chunk = await self._buffer.get()
            if chunk is None:
                self._drained = True
                return
            try:
                # `on_total=ClientTooSlow` is C8 at the one clock alignment
                # that hides it. Without it, a breach of the TOTAL while we sit
                # here arrives as `TotalDeadlineExceeded` -- now GATEWAY blame and
                # NEUTRAL health, so the breaker hazard is gone -- but the
                # client would still get a 504 blamed on the gateway for a
                # stall the client caused. The same client behaviour must not
                # produce two different classes depending on how late in the
                # request it happened; `ClientTooSlow` (CLIENT, 499) is the
                # honest one on both sides of the deadline.
                #
                # `phase()`'s entry check is deliberately not covered: if the
                # deadline was already gone before we got here, the time went
                # somewhere this loop cannot attribute, and the honest report
                # is the unattributed one.
                async with phase(
                    self._deadline, self._budgets.client_stall,
                    on_timeout=ClientTooSlow, on_total=ClientTooSlow,
                ):
                    # ------------------------------------------------------
                    # The single most load-bearing line in this file.
                    #
                    # A `send()` that raises may ALREADY HAVE DELIVERED A
                    # PREFIX -- the exception says the write did not complete,
                    # not that nothing left the building. Setting this flag
                    # after the await makes a failed first write look
                    # uncommitted; the executor then opens a second upstream
                    # and splices a second answer onto bytes the client is
                    # already rendering. Nothing downstream can detect that
                    # and nothing can undo it.
                    #
                    # It is the last statement before the await and the first
                    # after `phase()` has done its deadline check, so there is
                    # no path that claims commitment without attempting a
                    # write, and no path that attempts a write without
                    # claiming commitment.
                    # ------------------------------------------------------
                    self._committed = True
                    await self._sink.send(chunk)
            except GatewayError as err:
                # ClientTooSlow from the phase above, or a deadline breach.
                self._client_gone = err.blame is Blame.CLIENT
                raise
            except Exception as exc:  # noqa: BLE001 - any sink fault is the client leaving
                # A reset, a broken pipe, an ASGI server that has already torn
                # the connection down. NEUTRAL health: the provider is fine and
                # must not be marked otherwise (C8).
                self._client_gone = True
                raise ClientDisconnected(
                    f"client sink failed after {self._bytes_out} bytes: {exc!r}",
                    cause=exc,
                ) from exc
            finally:
                self._buffer.release(len(chunk))
            self._bytes_out += len(chunk)

    async def _end_natively(self) -> None:
        """End a post-commitment failure the way this provider ends one (C2).

        Today both surfaces return `b""` and this method sends nothing at all:
        the body simply stops, with no `data: [DONE]` and no `message_stop`.
        That is the contract, not a stub. Every vendor SDK already detects its
        own truncated stream; a synthesised terminal marker would report a cut
        answer as complete, and a synthesised `event: error` is a shape their
        error handling has never seen -- so being helpful here is what breaks
        them.

        The method exists because the Responses surface will have a real
        `response.failed` to forward when upstream actually sent one. It is
        best effort by construction: we are already failing, and an exception
        raised while tidying up would replace the real error with a worse one.
        """
        if not self._committed or self._client_gone:
            return
        ending = self._surface.native_ending(self._last_event)
        if not ending:
            return
        try:
            await self._sink.send(ending)
        except Exception:  # noqa: BLE001 - the client is gone; that is not new news
            self._client_gone = True
        else:
            self._bytes_out += len(ending)


# ==========================================================================
# The buffer. Bytes, one producer, one consumer.
# ==========================================================================


class _ByteBuffer:
    """A FIFO of byte chunks with a hard byte ceiling.

    `asyncio.Queue` is the obvious reach and it is the wrong one: it bounds
    *items*. A queue of 32 items is 32 tokens or 256 MiB depending on the day,
    and the version that OOMs looks identical on the dashboard to the version
    that does not.

    Two properties beyond the bound:

    * **Oversized chunks are split, not admitted.** A chunk bigger than the
      whole ceiling would otherwise have to be let through wholesale -- the
      alternative being deadlock -- and then the ceiling is advisory. Splitting
      keeps `size <= limit` true at every instant. It re-chunks the stream,
      which is free: chunk boundaries are a property of the network, never of
      the content, and the parser downstream is byte-exact across any split.
    * **A chunk stays counted while it is in flight to the sink.** It is still
      in memory, so a gauge that forgot it would under-report exactly when the
      client is slow and the number matters.
    """

    __slots__ = ("_limit", "_chunks", "_size", "_closed", "_aborted", "_not_empty",
                 "_not_full")

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._chunks: deque[bytes] = deque()
        self._size = 0
        self._closed = False
        self._aborted = False
        # Events rather than a Condition: exactly one producer and exactly one
        # consumer, so there is no thundering herd to be fair to, and the
        # check-then-clear is atomic because neither side awaits between them.
        self._not_empty = asyncio.Event()
        self._not_full = asyncio.Event()
        self._not_full.set()

    @property
    def size(self) -> int:
        return self._size

    @property
    def aborted(self) -> bool:
        return self._aborted

    async def put(self, chunk: bytes) -> None:
        """Append, blocking while full. This block IS the backpressure."""
        view = memoryview(chunk)
        while view:
            while self._size >= self._limit:
                if self._aborted:
                    return
                self._not_full.clear()
                await self._not_full.wait()
            if self._aborted:
                return
            room = self._limit - self._size
            piece = bytes(view[:room])
            view = view[room:]
            self._chunks.append(piece)
            self._size += len(piece)
            self._not_empty.set()

    async def get(self) -> bytes | None:
        """The next chunk, or None once the producer is finished."""
        while not self._chunks:
            if self._closed:
                return None
            self._not_empty.clear()
            await self._not_empty.wait()
        return self._chunks.popleft()

    def release(self, count: int) -> None:
        """The consumer is done with `count` bytes; the producer may refill."""
        self._size -= count
        if self._size < self._limit:
            self._not_full.set()

    def close(self) -> None:
        """No more chunks are coming. Wakes a consumer parked on empty."""
        self._closed = True
        self._not_empty.set()

    def abort(self) -> None:
        """Release everything, now. Called on every exit path from `run()`.

        The buffer is the one thing here that is not owned by a task, so it is
        the one thing a cancellation cannot free by itself -- and a pump that
        leaks its buffer on the error path leaks it exactly when the error
        path is being taken a lot.
        """
        self._aborted = True
        self._closed = True
        self._chunks.clear()
        self._size = 0
        self._not_empty.set()
        self._not_full.set()


# ==========================================================================
# Helpers
# ==========================================================================


async def _next_chunk(source: AsyncIterator[bytes]) -> bytes | None:
    """`anext()`, with the end of the stream as a value rather than an
    exception.

    `StopAsyncIteration` must not escape into `phase()`: it would be thrown
    into an async generator's `yield`, where the interpreter's rules for a
    Stop* exception crossing a generator boundary are subtle enough that the
    resulting bug reads as "the timeout stopped working".
    """
    try:
        return await source.__anext__()
    except StopAsyncIteration:
        return None


async def _aclose(source: AsyncIterator[bytes]) -> None:
    """Release the upstream iterator on every path.

    Called after the TaskGroup has joined, so the reader is guaranteed not to
    be parked inside `__anext__` -- closing a generator with a pending send is
    a RuntimeError, and a cleanup path that raises is worse than no cleanup
    path at all.
    """
    aclose = getattr(source, "aclose", None)
    if aclose is None:
        return
    try:
        await aclose()
    except Exception:  # noqa: BLE001 - cleanup never replaces the real error
        return


def _leaves(exc: BaseException) -> Iterator[BaseException]:
    if isinstance(exc, BaseExceptionGroup):
        for sub in exc.exceptions:
            yield from _leaves(sub)
    else:
        yield exc


def _primary(group: BaseExceptionGroup) -> BaseException:
    """Pick the one exception that explains a two-task failure.

    When either task fails the TaskGroup cancels the other, so in practice
    there is one real error and one cancellation -- but the order is a race,
    and an error report that depends on a race is an error report that will
    eventually blame the wrong party. So the preference is explicit and
    client faults win: if the client went away, the provider's read being
    cancelled a microsecond later is a consequence, not a cause, and recording
    it as a provider failure feeds a breaker with our own teardown.
    """
    leaves = list(_leaves(group))
    gateway = [exc for exc in leaves if isinstance(exc, GatewayError)]
    for exc in gateway:
        if exc.blame is Blame.CLIENT:
            return exc
    if gateway:
        return gateway[0]
    for exc in leaves:
        if not isinstance(exc, asyncio.CancelledError):
            return exc
    # Nothing but cancellation: re-raise it as cancellation, never as a
    # gateway failure. Requirement 8, and the same trap `_ManualTimeout`
    # documents -- "I fired" and "I was cancelled" are different facts.
    return asyncio.CancelledError()

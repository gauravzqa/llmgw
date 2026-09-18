"""Two sockets, four tasks, one verdict.

The HTTP pump has one direction and one question ("has a byte reached the
client?"). A relay has two directions and the same question twice, plus a
third the pump never faces: the two directions are not independent, because
the thing that proves the provider is healthy on one of them is what the
client sent on the other. A transcription session with no audio going in is
not a stalled provider, it is a quiet learner, and a gateway that cannot
tell those apart will close healthy sessions during every pause.

--------------------------------------------------------------------------
Why four tasks and not two
--------------------------------------------------------------------------

The obvious relay is `async for frame in a: await b.send(frame)`, twice.
It is two tasks, it is correct, and it has no memory bound and no way to
measure a slow client: when `b.send()` blocks, `a` stops being read, the
provider's own socket buffer fills, and the gateway looks perfectly healthy
while a tenant's session silently stops. There is nothing to close, nothing
to count, and no number that says which side is slow.

So each direction is a reader and a writer with a byte-bounded queue between
them, which is the pump's shape and is what makes the three facts below
observable: how much memory this session is holding, how long the client has
been refusing to read, and which side stopped. Four tasks per session is
real cost -- it is the number S10 measures -- and it buys the only
backpressure story that can be asserted in a test.

--------------------------------------------------------------------------
Why a FrameBuffer and not the pump's ByteBuffer
--------------------------------------------------------------------------

`bytebuf.ByteBuffer` splits an oversized chunk so its ceiling is true at
every instant. On a byte stream that is free: chunk boundaries are a
property of the network and the parser downstream is byte-exact across any
split. On a FRAMED transport it is corruption -- half an `audioChunk` is not
a smaller `audioChunk`, it is a protocol error -- so this module keeps
ByteBuffer's discipline (one producer, one consumer, two Events, bytes
counted while in flight) and drops the one rule that does not survive the
change of medium. The ceiling is therefore a high-water mark rather than an
invariant: it is enforced BETWEEN frames, and a single frame larger than the
whole ceiling is admitted alone, because `max_frame_bytes` already bounds it
and refusing it would mean closing a session over a legal message.

--------------------------------------------------------------------------
Commitment
--------------------------------------------------------------------------

`_committed` is set BEFORE the await that writes a committing frame to the
client, exactly as `pump.py:585` sets it, and for exactly the same reason: a
send that raises may still have delivered a prefix, so a flag set after the
await is a flag that lies on the one path it exists for. On this plane the
committing frame is the first CONTENT frame relayed to the client -- the
first `audioChunk` -- and after it no fallback is legal for the session, not
merely for the context it belonged to. A second upstream socket would mean a
second provider synthesising the same utterance onto the same client, billed
twice, and the client's state machine reading both (C24).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

from llmgw import errors
from llmgw.clocks import Budgets, Clock
from llmgw.surfaces.base import Usage
from llmgw.ws.errors import CloseCode, CloseVerdict, classify_close
from llmgw.ws.surfaces.base import Frame, FrameClass, Verdict, WsSurface

log = logging.getLogger("llmgw.ws.relay")

__all__ = [
    "Direction",
    "FrameBuffer",
    "Relay",
    "RelayResult",
    "Side",
    "TokenBucket",
    "close_within",
]

WATCHDOG_MIN_SLEEP = 0.02
"""Floor on the watchdog's sleep, so a budget of zero cannot spin the loop.
There is no CEILING on purpose: an idle Inworld socket's nearest deadline is
ten minutes away and the watchdog sleeps for ten minutes, which is what
makes two thousand idle sessions cost two thousand timer entries rather than
two thousand wakeups a second (S10)."""

CONFIG_PREFIX_BYTES = 64 * 1024
"""Ceiling on the replayable config prefix (C24, PLAN-G 6).

The prefix is the only thing a pre-commit fallback may re-send to a second
target, and it is kept in memory for the life of a session that might still
fall back -- so it is bounded, and bounded small. An Inworld `create` is 272
bytes in the captures and five of them fit in 1.4 KiB; 64 KiB is two orders
of magnitude of headroom and still nothing next to the 256 KiB each direction
may hold in flight. A session whose config exceeds it does not fail; it stops
being eligible for fallback, which is the conservative half of the trade."""

TEARDOWN_TIMEOUT = 5.0
"""Hard bound on everything after the verdict: cancelling the four pumps,
and sending each side its close frame.

Every one of those awaits can hang, and the way it hangs is not exotic -- it
is the ordinary shape of a client that stopped reading. `close()` writes a
close frame, a write needs room in the socket's send buffer, and a peer that
has gone away without a reset leaves that buffer full forever. The first
version of this file suppressed exceptions around the closes and was
therefore protected against every failure except the one that actually
happens: three hundred sessions ended, three hundred endpoint tasks parked
inside `close()`, `llmgw_ws_sessions_open` stuck at three hundred, and the
permits never released (found by `tests/chaos/test_ws_invariants.py`).

Five seconds is generous for a write that either succeeds immediately or
never will. What follows a breach is not an error: the transport is closed
under us by uvicorn and by `websockets` when the task ends, so the only
thing lost is the courtesy of a close frame to a peer that is not listening.
"""

FATAL_CLOSE_GRACE = 1.0
"""How long a fatal-looking in-band error waits for the close that decides
it. Inworld sends the CLOSE in the SAME MILLISECOND as the error (probes 2a,
2b, 6c) and OpenAI within a millisecond of it, so this is an order of
magnitude of headroom, not a guess. If no close arrives the error was
non-fatal -- probe 5's malformed-frame case -- and the session continues."""


class Direction(Enum):
    """Named from the client's point of view, matching
    `metrics.WS_DIRECTIONS`, so a label and a variable never disagree."""

    CLIENT_IN = "client_in"
    """Client -> gateway -> provider."""

    CLIENT_OUT = "client_out"
    """Provider -> gateway -> client."""


class Side(Protocol):
    """One end of the relay, so this module knows neither Starlette nor
    `websockets`. Two adapters implement it (`routes.ClientSide`,
    `routes.UpstreamSide`) and both are thin enough to read in one screen."""

    async def recv(self) -> Frame | None:
        """The next frame, or None when the peer has closed. Sets
        `close_code`/`close_reason` before returning None."""

    async def send(self, frame: Frame) -> None: ...

    async def close(self, code: int, reason: str) -> None: ...

    close_code: int | None
    close_reason: str


# ==========================================================================
# The buffer
# ==========================================================================


class FrameBuffer:
    """A FIFO of frames with a byte ceiling. See the module docstring.

    Mirrors `bytebuf.ByteBuffer` field for field except that `put` never
    splits, so the two can be read side by side and the difference is the
    only difference.
    """

    __slots__ = ("_limit", "_frames", "_size", "_closed", "_aborted",
                 "_not_empty", "_not_full", "high_water")

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._frames: deque[Frame] = deque()
        self._size = 0
        self._closed = False
        self._aborted = False
        self._not_empty = asyncio.Event()
        self._not_full = asyncio.Event()
        self._not_full.set()
        self.high_water = 0
        """The largest `size` this buffer ever held, for the capture record
        and for the contract test that asserts the ceiling actually bounds
        memory. A gauge of the current size would read zero by the time
        anybody looked."""

    @property
    def size(self) -> int:
        return self._size

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def full(self) -> bool:
        return self._size >= self._limit

    @property
    def aborted(self) -> bool:
        return self._aborted

    async def put(self, frame: Frame) -> None:
        """Append, blocking while full. This block IS the backpressure.

        Blocks BEFORE admitting, never mid-frame, so the queue always holds
        whole frames. A frame larger than the whole ceiling is admitted into
        an empty buffer rather than deadlocking on room that can never
        appear -- the frame bound is what stops that being unbounded."""
        while self._size >= self._limit:
            if self._aborted:
                return
            self._not_full.clear()
            await self._not_full.wait()
        if self._aborted:
            return
        self._frames.append(frame)
        self._size += len(frame)
        self.high_water = max(self.high_water, self._size)
        self._not_empty.set()

    async def get(self) -> Frame | None:
        """The next frame, or None once the producer is finished."""
        while not self._frames:
            if self._closed:
                return None
            self._not_empty.clear()
            await self._not_empty.wait()
        return self._frames.popleft()

    def release(self, count: int) -> None:
        """The consumer is done with `count` bytes; the producer may refill.
        Called AFTER the send, so a frame in flight to a slow peer is still
        counted -- it is still in memory, and that is the moment the number
        matters."""
        self._size -= count
        if self._size < self._limit:
            self._not_full.set()

    def close(self) -> None:
        self._closed = True
        self._not_empty.set()

    def abort(self) -> None:
        """Release everything, now. Called on every exit path."""
        self._aborted = True
        self._closed = True
        self._frames.clear()
        self._size = 0
        self._not_empty.set()
        self._not_full.set()


class TokenBucket:
    """Bytes per second, over one-second windows, with no timer.

    Refilled lazily from the clock on every check, so an idle direction costs
    nothing and a bursty one is smoothed rather than clipped. `None` capacity
    disables it entirely, which is the default for every direction no surface
    has measured.

    What a breach DOES is the interesting part and it is not here: the reader
    that owns the bucket simply waits for the deficit before reading again,
    so the refusal is carried by TCP backpressure rather than by a dropped
    frame or a close. Dropping a frame on a framed protocol is corruption;
    closing is a verdict the traffic has not earned yet. Sustained
    over-rate turns into a full buffer and is then the `client_stall` path,
    which is a verdict with a number behind it.
    """

    __slots__ = ("rate", "_tokens", "_updated", "_clock")

    def __init__(self, rate: int | None, *, clock: Clock) -> None:
        self.rate = rate
        self._clock = clock
        self._tokens = float(rate or 0)
        self._updated = clock.now()

    def deficit(self, nbytes: int) -> float:
        """Seconds to wait before `nbytes` may pass, taking them if it is
        zero. A bucket with no rate always answers zero."""
        if self.rate is None:
            return 0.0
        now = self._clock.now()
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(float(self.rate), self._tokens + elapsed * self.rate)
        self._updated = now
        if self._tokens >= nbytes:
            self._tokens -= nbytes
            return 0.0
        return (nbytes - self._tokens) / self.rate


# ==========================================================================
# Per-unit clock state
# ==========================================================================


@dataclass(slots=True)
class _Unit:
    """One context's clocks. Created on demand, dropped at its TERMINAL."""

    awaiting: bool = False
    """The client asked for output and none has arrived yet (or the last
    batch has not finished). While False the unit is idle and neither the
    first-event nor the progress clock applies to it -- an open Inworld
    context between utterances is not a stall."""

    got_content: bool = False
    """At least one CONTENT frame has come back for this unit, so the budget
    that applies is `progress` rather than `first_event`."""

    buffer_delay: float = 0.0
    """Seconds this context's client told the provider it may buffer before
    synthesising (`Verdict.buffer_delay_s`, off the `create`). Added to
    `first_event` for this unit only: declared buffering is not a stall."""

    last_at: float = 0.0
    """When the clock was last reset: the request went out, or output came
    back."""


@dataclass(slots=True)
class RelayResult:
    """What one session did, for the record and the metrics."""

    committed: bool = False
    usage: Usage = field(default_factory=Usage)
    error: errors.GatewayError | None = None
    verdict: CloseVerdict | None = None
    """The close WE sent, or None when the peer closed first."""

    client_close: tuple[int | None, str] = (None, "")
    upstream_close: tuple[int | None, str] = (None, "")
    bytes_in: int = 0
    bytes_out: int = 0
    frames_in: int = 0
    frames_out: int = 0
    contexts_opened: int = 0
    first_event_at: float | None = None
    buffer_high_water: int = 0
    inband_errors: int = 0
    client_characters: int = 0
    """Characters the client asked to synthesise, summed over `send_text`.
    The FALLBACK meter, used only when no provider usage arrived (C27)."""


# ==========================================================================
# The relay
# ==========================================================================


class Relay:
    """Runs one session until somebody ends it, then says who and why."""

    def __init__(
        self,
        *,
        surface: WsSurface,
        client: Side,
        upstream: Side,
        budgets: Budgets,
        clock: Clock,
        max_in_bps: int | None = None,
        max_out_bps: int | None = None,
        buffer_bytes: int = 256 * 1024,
        max_frame_bytes: int = 1024 * 1024,
        on_bytes: Any = None,
        product: str = "",
        scrub_errors: bool = False,
        close_client_on_finish: bool = True,
        replay: Sequence[Frame] = (),
        rewrite_first: Any = None,
    ) -> None:
        self._surface = surface
        self._client = client
        self._upstream = upstream
        self._budgets = budgets
        self._clock = clock
        self._product = product or getattr(surface, "product", "")
        self._scrub_errors = scrub_errors
        """The provider row says `scrub_error_bodies="all"`: this provider
        puts fragments of OUR credential in its error text (Inworld quotes
        the first four characters of the key in its code-7 message,
        captures-ws probe 2b). The HTTP plane has scrubbed those bodies since
        Phase B; relaying the frame untouched here would have made the
        WebSocket plane the one place a tenant can read the gateway's key
        back. So the frame is replaced -- not dropped, not synthesised: the
        shape, the code and the contextId all survive, only the free-text
        message is replaced. C25 records this as the single exception to
        passthrough, and the unscrubbed text still reaches the capture
        record, which is ours."""
        self._max_frame_bytes = max_frame_bytes
        self._on_bytes = on_bytes
        """`(direction, nbytes) -> None`, for `llmgw_ws_bytes_total`. A
        callback rather than a collector reference so this module has no
        opinion about metrics and the unit tests need no registry."""

        # `buffer_bytes` exactly, NOT `max(buffer_bytes, max_frame_bytes)`.
        # The larger ceiling was the first thing written here and it is
        # wrong: with the shipped numbers (256 KiB of buffer, 1 MiB of frame)
        # it would make the operator's buffer setting mean nothing and give
        # every direction a megabyte. `FrameBuffer.put` blocks only while the
        # buffer is ALREADY at its limit, so an oversized frame is admitted
        # into an empty buffer rather than deadlocking, and the true worst
        # case per direction is `buffer_bytes + max_frame_bytes` -- which is
        # the honest number and is what S10 measures.
        ceiling = buffer_bytes
        self._buffers = {
            Direction.CLIENT_IN: FrameBuffer(ceiling),
            Direction.CLIENT_OUT: FrameBuffer(ceiling),
        }
        self._buckets = {
            Direction.CLIENT_IN: TokenBucket(max_in_bps, clock=clock),
            Direction.CLIENT_OUT: TokenBucket(max_out_bps, clock=clock),
        }

        now = clock.now()
        self.started_at = now
        self._last_activity = now
        self._handshake_at: float | None = None
        """When the first CONFIG frame was relayed upstream, arming the
        handshake budget. None before that and after the first upstream
        frame: with Inworld there is nothing to wait for until the client
        speaks, because 101-then-silence is a healthy socket."""

        self._units: dict[str, _Unit] = {}

        self._buffer_delay: dict[str, float] = {}
        """Context -> the buffering delay its `create` declared. Kept apart
        from `_units` because the declaration arrives on the config frame and
        the unit is not created until the first content frame."""
        self._last_client_send = now
        """When a send to the CLIENT last returned. The client-stall clock
        runs from here whenever there is anything queued for it."""
        self._committed = False
        self._contexts: set[str] = set()
        self._drain_at: float | None = None
        """Absolute instant the drain may stop waiting for contexts."""

        self._rewrite_first = rewrite_first
        """`(Frame) -> Frame`, called on the FIRST config frame only, and
        the only thing in this module that may change a client's bytes. The
        session owns it because the rewrite needs the plan and the catalog;
        the relay owns the "first, and only the first" part because it is the
        only object that knows which frame that was. `None` disables it."""

        self._rewritten = False

        self._close_client_on_finish = close_client_on_finish
        """False while a fallback is still possible: the client socket
        survives a failed attempt, because nothing has been said to it yet
        and the whole point of pre-commit fallback is that the client never
        learns there was a first attempt."""

        self._replay = tuple(replay)
        """Config frames to send upstream before anything else. Non-empty
        only on the second and later attempts of one session."""

        self.config_prefix: list[Frame] = []
        """Every CONFIG frame relayed on this attempt, in order, capped at
        `CONFIG_PREFIX_BYTES`. The next target's opening lines (C24)."""

        self._config_bytes = 0
        self.config_truncated = False

        self.content_forwarded = False
        """A client CONTENT frame reached the provider. From this instant
        there is no fallback even though nothing has come back: replaying
        audio or text to a second provider bills the tenant twice for one
        utterance and can produce two different answers to it (PLAN-G 6)."""

        self._result = RelayResult()
        self._usage = self._result.usage
        self._last_error_frame: errors.GatewayError | None = None
        self._fatal_pending_until: float | None = None
        self._verdict: CloseVerdict | None = None
        self._wake = asyncio.Event()
        self._done = asyncio.Event()
        self._drain_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------ public

    @property
    def committed(self) -> bool:
        return self._committed

    @property
    def open_contexts(self) -> int:
        return len(self._contexts)

    def drain(self, *, deadline: float) -> None:
        """Begin winding down; stop waiting for contexts at `deadline`.

        Synchronous and non-blocking, because `Gateway.begin_drain` calls it
        once per open session before it awaits anything (C26). For a product
        with a terminate message the message is queued here; Inworld TTS has
        none, so the drain is a wait on the context count and a hard stop at
        the deadline.
        """
        if self._drain_at is not None:
            return
        self._drain_at = deadline
        message = self._surface.drain_message()
        if message is not None:
            # The provider's OWN terminate, sent through the same buffer
            # every relayed client frame goes through, so it queues behind
            # whatever the client has in flight rather than overtaking it.
            # A task because `drain()` must not await: `begin_drain` calls it
            # once per open session before it waits on anything.
            self._drain_task = asyncio.create_task(
                self._buffers[Direction.CLIENT_IN].put(message), name="ws-drain-send",
            )
        self._wake.set()

    async def run(self) -> RelayResult:
        """Relay until a verdict, then close both sides and report."""
        for frame in self._replay:
            # Straight onto the wire, ahead of the reader: the provider must
            # see the config prefix before whatever the client says next, and
            # the buffer is empty at this instant anyway.
            await self._upstream.send(frame)
            self._result.bytes_in += len(frame)
            self._result.frames_in += 1
            self._remember_config(frame)
            if self._handshake_at is None:
                self._handshake_at = self._clock.now()
        tasks = [
            asyncio.create_task(self._read(Direction.CLIENT_IN), name="ws-read-in"),
            asyncio.create_task(self._write(Direction.CLIENT_IN), name="ws-write-in"),
            asyncio.create_task(self._read(Direction.CLIENT_OUT), name="ws-read-out"),
            asyncio.create_task(self._write(Direction.CLIENT_OUT), name="ws-write-out"),
            asyncio.create_task(self._watchdog(), name="ws-watchdog"),
        ]
        try:
            await self._done.wait()
        finally:
            for buffer in self._buffers.values():
                buffer.abort()
            if self._drain_task is not None:
                tasks.append(self._drain_task)
            for task in tasks:
                task.cancel()
            # `gather` rather than a loop of awaits: on a mass disconnect
            # this runs for every session at once, and five sequential
            # awaits per session is five context switches per session on the
            # one path that must not be O(sessions) in latency. Bounded,
            # because a pump parked in a socket write that will never
            # complete does not notice a cancel until the write does.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), TEARDOWN_TIMEOUT,
                )
            await self._finish()
        return self._result

    # ------------------------------------------------------------ pumps

    async def _read(self, direction: Direction) -> None:
        """Read one side, classify, and hand the frame to its writer."""
        src = self._client if direction is Direction.CLIENT_IN else self._upstream
        buffer = self._buffers[direction]
        bucket = self._buckets[direction]
        try:
            while True:
                wait = bucket.deficit(0)
                if wait > 0:  # pragma: no cover - only with a zero-capacity bucket
                    await asyncio.sleep(wait)
                frame = await src.recv()
                if frame is None:
                    self._peer_closed(direction)
                    return
                if len(frame) > self._max_frame_bytes:
                    self._decide(CloseVerdict(
                        CloseCode.FRAME_TOO_LARGE,
                        errors.FrameTooLarge(
                            f"a {len(frame)}-byte frame exceeds the "
                            f"{self._max_frame_bytes}-byte bound",
                            provider=self._product,
                        ),
                    ))
                    return
                # The rate bound is applied AFTER the frame is in hand and
                # BEFORE the next read: the socket has already delivered this
                # one, and what a bucket can control is how soon we ask for
                # another. That is TCP backpressure toward whichever peer is
                # too fast, and it is invisible to both.
                deficit = bucket.deficit(len(frame))
                if deficit > 0:
                    await asyncio.sleep(deficit)
                frame = self._observe(direction, frame)
                if frame is not None:
                    await buffer.put(frame)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - a dead socket is not a crash
            self._peer_failed(direction, exc)
        finally:
            buffer.close()

    async def _write(self, direction: Direction) -> None:
        """Drain one direction's buffer into the far side."""
        dst = self._upstream if direction is Direction.CLIENT_IN else self._client
        buffer = self._buffers[direction]
        try:
            while True:
                frame = await buffer.get()
                if frame is None:
                    return
                size = len(frame)
                if direction is Direction.CLIENT_OUT and self._commits(frame):
                    # BEFORE the await. `pump.py:585`'s rule, and the reason
                    # it is a rule: a send that raises may still have put a
                    # prefix on the wire.
                    self._committed = True
                    self._result.committed = True
                try:
                    await dst.send(frame)
                finally:
                    buffer.release(size)
                if direction is Direction.CLIENT_OUT:
                    # A send that RETURNED is the only proof the client is
                    # consuming. Buffer occupancy is not: `get()` pops a frame
                    # before the send blocks, so a stalled client's buffer
                    # oscillates around its ceiling and any predicate built on
                    # `full` flickers with it.
                    self._last_client_send = self._clock.now()
                if direction is Direction.CLIENT_IN:
                    if (not self.content_forwarded
                            and self._surface.classify_client(frame).kind
                            is FrameClass.CONTENT):
                        self.content_forwarded = True
                    self._result.frames_in += 1
                    self._result.bytes_in += size
                else:
                    self._result.frames_out += 1
                    self._result.bytes_out += size
                if self._on_bytes is not None:
                    self._on_bytes(direction, size)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            self._peer_failed(
                Direction.CLIENT_OUT if direction is Direction.CLIENT_IN
                else Direction.CLIENT_IN,
                exc,
            )

    # ------------------------------------------------------- observation

    def _observe(self, direction: Direction, frame: Frame) -> Frame | None:
        """Everything the relay learns from one frame, before relaying it.

        Returns the frame to relay -- the same object in every case but the
        announced `create.modelId` rewrite, which the session performs before
        the relay starts and which therefore never reaches here.
        """
        now = self._clock.now()
        self._last_activity = now
        if direction is Direction.CLIENT_IN:
            verdict = self._surface.classify_client(frame)
            if verdict.config and not self._rewritten and self._rewrite_first is not None:
                # The announced edit (`X-Gw-Body-Modified: 1`), once per
                # session. It may raise a `PolicyError` -- a `modelId` the
                # catalog does not know -- and that is a verdict, not a
                # relay failure: the client is past the 101, so it is a close
                # code (C25) and the frame never reaches the provider.
                self._rewritten = True
                try:
                    frame = self._rewrite_first(frame)
                except errors.GatewayError as err:
                    self._decide(CloseVerdict(CloseCode.UPSTREAM_HANDSHAKE, err))
                    return None
            self._observe_client(frame, verdict, now)
        else:
            verdict = self._surface.classify_upstream(frame)
            self._observe_upstream(frame, verdict, now)
            if verdict.kind is FrameClass.ERROR and self._scrub_errors:
                scrubber = getattr(self._surface, "scrub_error_frame", None)
                if scrubber is not None:
                    frame = scrubber(frame)
        self._wake.set()
        return frame

    def _observe_client(self, frame: Frame, verdict: Verdict, now: float) -> None:
        if verdict.config:
            self._remember_config(frame)
            self._result.contexts_opened += 1
            if verdict.context:
                self._contexts.add(verdict.context)
            if verdict.buffer_delay_s is not None:
                self._buffer_delay[verdict.context or ""] = verdict.buffer_delay_s
            if self._handshake_at is None and not self._result.frames_out:
                # The handshake budget starts at the FIRST config frame, not
                # at the 101: on this provider a socket that has said nothing
                # gets nothing back, whatever its credential, so before this
                # instant silence is health (captures-ws probes 2b, 3).
                self._handshake_at = now
        if verdict.kind is FrameClass.CONTENT:
            key = verdict.context or ""
            unit = self._units.get(key)
            if unit is None:
                unit = _Unit(buffer_delay=self._buffer_delay.get(key, 0.0))
                self._units[key] = unit
            unit.awaiting = True
            unit.last_at = now
            self._result.client_characters += _characters_of(frame)

    def _observe_upstream(self, frame: Frame, verdict: Verdict, now: float) -> None:
        # Any frame at all ends the handshake budget: `contextCreated` and a
        # code-7 `error` both prove the provider has read our credential, and
        # which of the two it was is the error path's business, not a clock's.
        self._handshake_at = None
        key = verdict.context or ""
        if verdict.kind is FrameClass.CONTENT:
            unit = self._units.setdefault(key, _Unit())
            unit.got_content = True
            unit.awaiting = True
            unit.last_at = now
            if self._result.first_event_at is None:
                self._result.first_event_at = now - self.started_at
            self._surface.apply_usage(frame, self._usage)
        elif verdict.kind is FrameClass.META:
            unit = self._units.get(key)
            if unit is not None:
                # A flush finished, a context was created, an acknowledgement
                # arrived: the provider owes nothing until the client asks
                # again, so the unit's clocks stop rather than keep running
                # against a socket that is behaving perfectly.
                unit.awaiting = False
                unit.last_at = now
        elif verdict.kind is FrameClass.TERMINAL:
            self._units.pop(key, None)
            self._contexts.discard(key)
            self._buffer_delay.pop(key, None)
            self._wake.set()
        elif verdict.kind is FrameClass.ERROR:
            self._result.inband_errors += 1
            err = self._surface.error_from_frame(frame)
            if err is not None:
                self._last_error_frame = err
            if self._surface.is_fatal(frame):
                # "Might be fatal". The close that follows within a
                # millisecond is the verdict; if none comes, the frame was
                # the provider refusing one message and the session lives.
                self._fatal_pending_until = now + FATAL_CLOSE_GRACE
            if verdict.context_closed:
                # The provider refused or disowned this context: no
                # `contextClosed` is coming, so forget it now or the drain
                # waits its whole bound on a context that never opened.
                self._units.pop(key, None)
                self._contexts.discard(key)
                self._buffer_delay.pop(key, None)
                self._wake.set()
            else:
                unit = self._units.get(key)
                if unit is not None:
                    unit.awaiting = False
                    unit.last_at = now

    def _remember_config(self, frame: Frame) -> None:
        """Keep one config frame for a possible replay, inside the cap."""
        if self.config_truncated:
            return
        if self._config_bytes + len(frame) > CONFIG_PREFIX_BYTES:
            self.config_truncated = True
            self.config_prefix.clear()
            self._config_bytes = 0
            return
        self.config_prefix.append(frame)
        self._config_bytes += len(frame)

    @property
    def fallback_eligible(self) -> bool:
        """May this session be retried against another target? C24.

        Three conditions, and all three are about what the two peers have
        already been told. Nothing has reached the client (`committed`);
        nothing of the client's content has reached a provider
        (`content_forwarded`); and the prefix we would replay is complete
        (`config_truncated`). The error's own `try_next` is the fourth and it
        is the caller's to read, because the caller is the one holding the
        plan.
        """
        return (
            not self._committed
            and not self.content_forwarded
            and not self.config_truncated
        )

    def _commits(self, frame: Frame) -> bool:
        """Does relaying this frame to the client commit the session?"""
        if self._committed:
            return False
        return self._surface.classify_upstream(frame).kind is FrameClass.CONTENT

    # ---------------------------------------------------------- watchdog

    async def _watchdog(self) -> None:
        """One task per session, asleep until the nearest deadline.

        A tick loop would be simpler and would cost two thousand wakeups a
        second at S10's scale for nothing: an idle Inworld socket's nearest
        deadline is ten minutes out. So the loop computes the minimum
        remaining budget, sleeps exactly that long, and is woken early by
        `_wake` whenever a frame or a drain changes the answer.
        """
        try:
            while not self._done.is_set():
                verdict, sleep_for = self._check()
                if verdict is not None:
                    self._decide(verdict)
                    return
                self._wake.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._wake.wait(), max(WATCHDOG_MIN_SLEEP, sleep_for),
                    )
        except asyncio.CancelledError:
            raise

    def _check(self) -> tuple[CloseVerdict | None, float]:
        """(verdict, seconds until the nearest deadline). Pure; testable."""
        now = self._clock.now()
        b = self._budgets
        nearest = 3600.0

        if b.session_total is not None:
            left = b.session_total - (now - self.started_at)
            if left <= 0:
                return CloseVerdict(CloseCode.SESSION_TOTAL, errors.TotalDeadlineExceeded(
                    f"session exceeded session_total={b.session_total:g}s",
                    provider=self._product,
                )), 0.0
            nearest = min(nearest, left)

        if self._handshake_at is not None:
            left = b.headers - (now - self._handshake_at)
            if left <= 0:
                return CloseVerdict(CloseCode.UPSTREAM_HANDSHAKE, errors.HeadersTimeout(
                    f"upstream answered nothing within headers={b.headers:g}s of the "
                    f"first relayed config frame",
                    provider=self._product,
                )), 0.0
            nearest = min(nearest, left)

        for key, unit in self._units.items():
            if not unit.awaiting:
                continue
            budget = (
                b.progress if unit.got_content
                else b.first_event + unit.buffer_delay
            )
            left = budget - (now - unit.last_at)
            if left <= 0:
                cls = errors.StallTimeout if unit.got_content else errors.FirstEventTimeout
                return CloseVerdict(CloseCode.PROVIDER_STALL, cls(
                    f"upstream produced nothing for {budget:g}s"
                    + (f" on context {key!r}" if key else ""),
                    provider=self._product,
                )), 0.0
            nearest = min(nearest, left)

        # "Is there output waiting that we have not managed to hand over?"
        # -- not "is the buffer full right now?". Two ways to get the second
        # one wrong, and this code had both: testing the deadline before
        # clearing the mark closes a client that already caught up, and
        # clearing the mark on an instantaneous `full` reading never closes
        # one at all, because `get()` pops a frame before the send blocks and
        # the occupancy flickers under the ceiling on every pass. A send that
        # RETURNED is unambiguous, so that is the clock.
        if self._buffers[Direction.CLIENT_OUT].size > 0:
            left = b.client_stall - (now - self._last_client_send)
            if left <= 0:
                return CloseVerdict(CloseCode.CLIENT_STALL, errors.ClientTooSlow(
                    f"client accepted nothing for {b.client_stall:g}s with "
                    f"{self._buffers[Direction.CLIENT_OUT].size} bytes queued",
                    provider=self._product,
                )), 0.0
            nearest = min(nearest, left)

        if b.idle is not None and self._drain_at is None:
            # Not while draining. A session already condemned by a deploy
            # that then goes quiet -- which is exactly what a polite client
            # does when it is asked to finish -- must still be told 4900
            # `session_draining`, not 4906 `session_idle`: the first says
            # "reconnect, it is us", the second blames the client, records
            # CLIENT rather than GATEWAY on the capture, and would file every
            # quiet socket of a deploy under the wrong heading.
            left = b.idle - (now - self._last_activity)
            if left <= 0:
                return CloseVerdict(CloseCode.IDLE, errors.SessionIdle(
                    f"no frame in either direction for idle={b.idle:g}s",
                    provider=self._product,
                )), 0.0
            nearest = min(nearest, left)

        if self._fatal_pending_until is not None and now >= self._fatal_pending_until:
            # The close never came. Probe 5's case: the provider refused one
            # frame and kept the socket, so the session continues and the
            # error stays on the record as a non-fatal in-band error.
            self._fatal_pending_until = None

        if self._drain_at is not None:
            if not self._contexts:
                # Every context closed inside the grace. The polite ending
                # C26 asks for, and the one the plugin handles best.
                return CloseVerdict(CloseCode.DRAINING, errors.SessionDraining(
                    "gateway is draining; all contexts closed", provider=self._product,
                )), 0.0
            left = self._drain_at - now
            if left <= 0:
                return CloseVerdict(CloseCode.DRAINING, errors.SessionDraining(
                    f"gateway is draining; {len(self._contexts)} context(s) still open "
                    f"at the drain deadline", provider=self._product,
                )), 0.0
            nearest = min(nearest, left)

        return None, nearest

    # ------------------------------------------------------------ endings

    def _decide(self, verdict: CloseVerdict) -> None:
        """Record the gateway's own verdict and stop the session. Idempotent:
        two directions can reach a verdict in the same tick and the first one
        is the one that happened."""
        if self._verdict is None:
            self._verdict = verdict
            self._result.verdict = verdict
            self._result.error = verdict.error
        self._done.set()

    def _peer_closed(self, direction: Direction) -> None:
        """One side sent a close frame. Which side decides what it means."""
        if direction is Direction.CLIENT_IN:
            # The client hung up. Which code it used is the difference
            # between a session that ENDED and one that was ABANDONED, and
            # the capture record has to tell them apart: a plugin closing a
            # pooled socket at its own idle timeout (tts.py:661) sends 1000
            # and has completed everything it asked for, while a process
            # killed mid-utterance produces a 1006 and has not. Recording
            # both as `canceled` would make every healthy socket in the fleet
            # look like an interruption.
            code = self._client.close_code
            self._result.client_close = (code, self._client.close_reason)
            if self._result.error is None and code not in (1000, 1005):
                self._result.error = errors.ClientDisconnected(
                    f"client closed the session ({code})", provider=self._product,
                )
        else:
            code = self._upstream.close_code
            reason = self._upstream.close_reason
            self._result.upstream_close = (code, reason)
            err = classify_close(
                code, reason, product=self._product,
                last_error_frame=self._last_error_frame,
                expected=self._drain_at is not None,
            )
            if err is not None and self._result.error is None:
                self._result.error = err
        self._done.set()

    def _peer_failed(self, direction: Direction, exc: BaseException) -> None:
        """A socket raised rather than closing. Same shape as a 1006."""
        if direction is Direction.CLIENT_IN:
            if self._result.error is None:
                self._result.error = errors.ClientDisconnected(
                    f"client socket failed: {type(exc).__name__}",
                    provider=self._product, cause=exc,
                )
        elif self._result.error is None:
            self._result.error = errors.UpstreamDisconnected(
                f"upstream socket failed: {type(exc).__name__}",
                provider=self._product, cause=exc,
            )
        self._done.set()

    def closing(self) -> tuple[int, str]:
        """The close the CLIENT should get, as (code, reason).

        Public because the endpoint may hold the close back across a
        fallback attempt -- the client must not be told about an attempt it
        never saw -- and then has to send it itself once the walk is over.
        Computing it in two places is how the passthrough rule gets broken on
        one of them.
        """
        if self._verdict is not None:
            return int(self._verdict.code), self._verdict.reason
        code, reason = self._result.upstream_close
        if code is not None:
            # A provider close passes through with ITS code and ITS reason
            # (C25): a client that sees 1000 after an Inworld `error` frame
            # is seeing what the provider did, and a 49xx here would say the
            # GATEWAY refused it.
            return code, reason
        return 1000, ""

    async def _finish(self) -> None:
        """Close both sockets, in the order the contracts require.

        A provider close is PASSED THROUGH with its own code and reason
        (C25): the client's state machine was written against that provider
        and a translated code would be a fact we invented. Our own verdict
        goes out as a 49xx with `llmgw:<code>`. The upstream always gets a
        plain 1000 -- it is not the party we are reporting to, and a private
        code would appear in somebody's provider-side dashboard as an
        anomaly the gateway caused.
        """
        self._result.buffer_high_water = max(
            b.high_water for b in self._buffers.values()
        )
        self._result.usage = self._usage
        close_code, close_reason = self.closing()
        if self._close_client_on_finish:
            await close_within(self._client, close_code, close_reason)
            self._result.client_close = (
                self._client.close_code if self._client.close_code is not None
                else close_code,
                self._client.close_reason or close_reason,
            )
        await close_within(self._upstream, 1000, "")


async def close_within(side: Side, code: int, reason: str) -> None:
    """Send one close frame, or give up. Never raises, never hangs.

    See `TEARDOWN_TIMEOUT`. Both failure modes are the same fact about the
    peer -- it is not reading -- and neither is worth a log line, because a
    mass disconnect would write one per session and that is finding 41.
    """
    with contextlib.suppress(Exception):
        await asyncio.wait_for(side.close(code, reason), TEARDOWN_TIMEOUT)


def _characters_of(frame: Frame) -> int:
    """`send_text.text` length, for the fallback meter.

    Deliberately generic-ish and deliberately not a surface method: it is the
    ESTIMATE, used only when the provider reported no usage at all, and a
    surface that needs a different estimate will say so when it exists. The
    real meter is `WsSurface.apply_usage`.
    """
    payload = frame.payload()
    if payload is None:
        return 0
    body = payload.get("send_text")
    if isinstance(body, dict):
        text = body.get("text")
        if isinstance(text, str):
            return len(text)
    return 0

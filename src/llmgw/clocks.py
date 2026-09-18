"""Time. One absolute deadline per request, four budgets that live inside it.

The single most common way a gateway amplifies an incident is a per-call
timeout. Give each attempt its own 10 s timeout, allow 3 attempts, and a
request can legally take 30 s -- and if the provider is slow because it is
overloaded, you have just tripled your own offered load against it at exactly
the wrong moment. The fix is not a smaller timeout. It is a different shape:

    one absolute instant, created at ingress, that every wait derives from.

`Deadline.slice(budget)` returns `min(remaining, budget)`. There is no code
path that produces a wait longer than the time left in the request, so
"the total deadline never resets across attempts" is not a rule anyone has to
remember -- it is the only thing the arithmetic can do.

--------------------------------------------------------------------------
The four clocks, and why one is not enough
--------------------------------------------------------------------------

connect      TCP + TLS + request sent, no response headers yet.
             Nothing was accepted upstream, so a breach here is the safest
             possible retry.

first_event  Headers arrived; the model is thinking. This is the clock that
             is usually set far too tight, because people size it against a
             fast model's p50 instead of a reasoning model's p99.

stall        The gap between consecutive events once streaming has begun.
             Small -- a healthy stream emits every few tens of milliseconds --
             which is what makes it a sharp detector of a dead upstream.

total        Everything, including retries, waits, client writes, and cleanup.
             The only absolute one.

Separating them buys precision that a single timeout cannot: a 60 s total with
no stall clock will happily hold a socket open for 59 s after the provider
went silent at second 3. The stall clock catches that in 300 ms, which is 58
seconds of a connection, a permit and a buffer returned to the pool.

--------------------------------------------------------------------------
Liveness is not progress
--------------------------------------------------------------------------

Providers send heartbeats: Anthropic `ping` events, OpenAI chunks with an
empty `choices` array. They prove the socket is alive. They do NOT prove the
model is producing anything, and a provider stuck in a bad state can heartbeat
politely forever.

So there are two stall budgets, not one:

    progress -- reset only by a content event. Enforced by default.
    liveness -- reset by ANY byte. Optional, always the looser of the two.

The other defensible choice treats any frame after commitment as proof of
life and lets the stream survive. `ping_does_not_reset_progress` takes the
opposite view and this module implements it. Both are defensible; only the
undocumented choice is not.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .errors import GatewayError, TotalDeadlineExceeded


@runtime_checkable
class Clock(Protocol):
    """Everything that waits takes a Clock, so tests can make time a variable.

    Note that `sleep` is on the clock and not `asyncio.sleep`. Any module that
    reaches for `asyncio.sleep` directly becomes untestable without real
    waiting, and a test suite that really waits is a test suite nobody runs.
    """

    def now(self) -> float: ...
    async def sleep(self, seconds: float) -> None: ...
    def timeout(self, seconds: float | None) -> _TimeoutCM: ...


class _TimeoutCM(Protocol):
    async def __aenter__(self) -> object: ...
    async def __aexit__(self, *exc: object) -> bool | None: ...


class SystemClock:
    """Real time. `time.monotonic`, never `time.time`.

    Wall clock jumps: NTP steps it, a VM pauses and resumes, a leap second is
    smeared. A deadline computed from wall clock can therefore expire in the
    past or drift into next week. Monotonic cannot go backwards, which is the
    only property a timeout actually needs.
    """

    __slots__ = ()

    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))

    def timeout(self, seconds: float | None):
        # Delegates to the stdlib on the production path. asyncio.timeout gets
        # the hard parts right (cancellation bookkeeping, nesting, uncancel)
        # and re-implementing it here to look clever would be a bug factory.
        return asyncio.timeout(None if seconds is None else max(0.0, seconds))


class ManualClock:
    """Time as a variable. Nothing advances until a test says so.

    This is what lets `total_deadline_never_resets` be asserted in microseconds
    instead of seconds. A suite that proves timeout behaviour by actually
    timing out is slow, and slow suites get skipped, and skipped suites are
    where timeout bugs live.
    """

    __slots__ = ("_now", "_waiters")

    def __init__(self, start: float = 1_000.0) -> None:
        self._now = float(start)
        # (due_at, future) -- a list, not a heap: test schedules are tiny and
        # a list keeps the wake logic obvious.
        self._waiters: list[tuple[float, asyncio.Future[None]]] = []

    def now(self) -> float:
        return self._now

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append((self._now + seconds, fut))
        await fut

    async def advance(self, seconds: float) -> None:
        """Move time forward, waking everything now due, then let those tasks
        actually run before returning. Without the drain, a test would advance
        time and immediately assert on state its own sleepers have not yet had
        a chance to update -- a flaky test that looks like a product bug."""
        # Drain FIRST. A task created with create_task() has not run yet, so
        # it has not called sleep() and has not registered a waiter. Without
        # this, advance() looks at an empty schedule, concludes nothing is
        # due, jumps straight to the target, and the task then parks forever
        # on a clock that has already moved past its wake time. The symptom is
        # a hung test with no output, which is a miserable thing to debug --
        # so the fix lives here, once, rather than as a `await asyncio.sleep(0)`
        # that every test has to remember.
        await _drain()
        target = self._now + seconds
        while True:
            due = [w for w in self._waiters if w[0] <= target]
            if not due:
                break
            # Step to the earliest due wake, not straight to the target, so a
            # sleeper that schedules another sleep sees a truthful clock.
            step = min(t for t, _ in due)
            self._now = step
            for when, fut in list(self._waiters):
                if when <= self._now and not fut.done():
                    fut.set_result(None)
            self._waiters = [w for w in self._waiters if w[0] > self._now]
            await _drain()
        self._now = target
        await _drain()

    def timeout(self, seconds: float | None):
        return _ManualTimeout(self, seconds)

    @property
    def pending_sleepers(self) -> int:
        """Leak detector for tests: a sleeper still parked after a request
        ended means someone forgot a `finally`."""
        return len([w for w in self._waiters if not w[1].done()])


async def _drain(rounds: int = 6) -> None:
    """Yield enough times for woken tasks to reach their next await point."""
    for _ in range(rounds):
        await asyncio.sleep(0)


class _ManualTimeout:
    """`asyncio.timeout`, but driven by a ManualClock.

    Implemented rather than borrowed because `asyncio.timeout` reads the event
    loop's clock, which a fake clock cannot move. The subtle part is the
    `except CancelledError` block: this class cancels the task it is guarding,
    so on the way out it must distinguish "I fired" from "somebody genuinely
    cancelled this request". Getting that backwards converts every client
    disconnect into a spurious timeout, which is precisely the misclassification
    errors.py exists to prevent -- so the distinction is carried by an explicit
    `_fired` flag and `uncancel()`, never by inspecting the exception.
    """

    __slots__ = ("_clock", "_seconds", "_guard", "_fired", "_task")

    def __init__(self, clock: ManualClock, seconds: float | None) -> None:
        self._clock = clock
        self._seconds = seconds
        self._guard: asyncio.Task[None] | None = None
        self._fired = False
        self._task: asyncio.Task[object] | None = None

    async def __aenter__(self) -> _ManualTimeout:
        if self._seconds is None:
            return self
        self._task = asyncio.current_task()  # type: ignore[assignment]

        async def _fire() -> None:
            await self._clock.sleep(max(0.0, self._seconds or 0.0))
            self._fired = True
            if self._task is not None:
                self._task.cancel()

        self._guard = asyncio.ensure_future(_fire())
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        # Cancel the guard but do NOT await it. We may be unwinding inside an
        # already-cancelled task, and awaiting there can re-deliver the
        # cancellation and lose the TimeoutError conversion entirely. This is
        # the same shape asyncio.timeout uses, for the same reason.
        if self._guard is not None and not self._guard.done():
            self._guard.cancel()
        if self._fired and exc_type is asyncio.CancelledError:
            if self._task is not None:
                self._task.uncancel()
            raise TimeoutError from None
        return False


@dataclass(frozen=True, slots=True)
class Budgets:
    """Per-workload time budgets, in seconds.

    Frozen because these arrive from a PolicySnapshot and must not be mutated
    mid-request: a request routed under one set of budgets and timed under
    another is a bug nobody can reproduce.
    """

    total: float
    connect: float = 2.0
    """TCP + TLS only, since PLAN-2 B4: the time to a connected socket. A
    breach here is the safest retry in the system -- nothing was accepted
    upstream, so no side effect can exist -- which is why it is kept tight
    and kept separate from the wait below."""

    headers: float = 10.0
    """Request written; waiting for the response STATUS LINE.

    Until 17 Sep 2026 this wait lived inside `connect` (finding 10: "the
    connect clock secretly also covers connected-but-slow"), and the default
    2 s produced a 504 on a healthy provider in the cross-machine live bench:
    OpenAI sends its headers together with the first token (finding 28), so
    on a 1.5k-token context the status line legitimately takes longer than a
    TCP handshake. A breach here is `HeadersTimeout`: the request WAS accepted,
    so it is `retry_same=False` and only the next target may be tried.

    Enforced by `upstream.py` between the request write and the status line
    (`LLMGW_BUDGET_HEADERS`). Sized for a long prompt's status line, not a
    handshake: 10 s default, under the 20 s first-event budget. The two are
    consecutive phases, not nested, so no ordering between them is required
    beyond each fitting inside `total`.
    """

    first_event: float = 20.0
    """Headers arrived; waiting for the first BODY BYTE.

    Sized for a reasoning model's p99, not a fast model's p50. Too tight here
    is the most common self-inflicted gateway outage: the provider was fine,
    the gateway gave up.

    Enforced by `upstream.py` on the first chunk, and it is a first-*byte*
    budget rather than a first-*content* one -- an Anthropic `message_start`
    or an OpenRouter comment satisfies it. Time-to-first-CONTENT is bounded
    instead by the progress budget below, whose clock starts when the pump
    does. That split is deliberate: it is what lets a `ping-forever` stream
    die on the progress budget (15s) rather than surviving to the first_event
    budget (20s) merely because bytes were arriving.
    """

    progress: float = 15.0
    """Max gap between CONTENT events once streaming."""

    liveness: float | None = None
    """Max gap between ANY bytes. None disables the looser check."""

    client_stall: float = 30.0
    """How long the pump tolerates a client that has stopped reading before
    ending the request to reclaim the buffer. Not a provider fault."""

    session_total: float | None = None
    """Wall-clock ceiling on a WebSocket SESSION, in seconds (PLAN-G 4.1).

    Not a second `total`. `total` bounds one request-shaped unit of work and
    is what the deploy inequality (`total <= drain_grace`) is about; a socket
    is not a unit of work, it is a place several of them happen, and a TTS
    socket that lives an hour while synthesising forty utterances has done
    nothing wrong. So the session clock is a separate field, `None` means
    unbounded, and `PolicySnapshot.largest_total()` deliberately ignores it
    -- a 3 h `stt_session` must not refuse startup behind a 130 s grace. The
    drain hook, not the grace, is what ends a long session on deploy (4.3).

    A breach closes the client with 4901 and the upstream with 1000. It
    exists because every provider caps a socket somewhere (OpenAI at 60 min,
    AssemblyAI at 3 h) and a gateway that learns the cap from the provider's
    close code learns it after the session is already unrecoverable."""

    idle: float | None = None
    """Max gap with NO activity of any kind in either direction, in seconds.

    `progress` is about a unit that started and stopped producing; this is
    about a socket with no unit in flight at all -- an Inworld TTS connection
    whose contexts have all closed, a transcription session nobody is
    speaking into. `None` disables it. A breach closes the client with 4906.

    It is the gateway's own liveness, not a proxy for the provider's:
    Inworld never pings and never closes a healthy socket (captures-ws 1.3),
    so nothing but this budget ever reclaims an abandoned upstream socket.
    Transport pings (uvicorn's 20 s, `websockets`' 20 s) do NOT reset it --
    they prove the TCP path is alive, which is exactly what an abandoned
    socket also proves."""

    def validate(self) -> Budgets:
        if self.total <= 0:
            raise ValueError("total budget must be positive")
        if self.headers <= 0:
            raise ValueError("headers budget must be positive")
        # `headers` is deliberately NOT held to `<= total`: its default (10 s)
        # predates no config, so a policy with `total = 5` written before the
        # phase existed must keep loading. `Deadline.slice()` clamps it to
        # what is left, as it does every phase.
        for name in ("connect", "first_event", "progress"):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} budget must be positive")
            if value > self.total:
                # Not fatal -- slice() clamps anyway -- but it means the phase
                # budget is decorative, and a decorative timeout is one nobody
                # realises is not protecting them.
                raise ValueError(
                    f"{name}={value} exceeds total={self.total}; it can never fire"
                )
        if self.liveness is not None and self.liveness < self.progress:
            raise ValueError("liveness budget must be >= progress budget")
        # The two session-scale clocks (PLAN-G). Positive or None; NOT held
        # to `<= total`, because they measure a socket and `total` measures a
        # request -- see their docstrings for why conflating the two would
        # make every long-lived session refuse startup.
        for name in ("session_total", "idle"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(
                    f"{name} budget must be positive, or None for unbounded"
                )
        return self


class Deadline:
    """One absolute instant. Everything else is derived from it."""

    __slots__ = ("_clock", "started_at", "expires_at", "total")

    def __init__(self, clock: Clock, total: float) -> None:
        self._clock = clock
        self.total = total
        self.started_at = clock.now()
        self.expires_at = self.started_at + total

    @classmethod
    def start(cls, total: float, *, clock: Clock | None = None) -> Deadline:
        return cls(clock or SystemClock(), total)

    def remaining(self) -> float:
        return self.expires_at - self._clock.now()

    def elapsed(self) -> float:
        return self._clock.now() - self.started_at

    @property
    def expired(self) -> bool:
        return self.remaining() <= 0

    def check(self, **ctx: object) -> None:
        """Raise if there is no time left. Called at the top of every attempt
        so a request that is already doomed does not open a socket to prove
        it."""
        if self.expired:
            raise TotalDeadlineExceeded(
                f"total deadline of {self.total:.3f}s exceeded "
                f"({self.elapsed():.3f}s elapsed)",
                **ctx,  # type: ignore[arg-type]
            )

    def slice(self, budget: float | None) -> float:
        """The effective timeout for one phase: never more than what is left.

        This one line is the whole no-reset guarantee. A phase asking for 20 s
        when 1.2 s remains gets 1.2 s, so three attempts of a 20 s phase inside
        a 30 s total take 30 s, not 60. There is deliberately no way to opt
        out and no `force` parameter, because every real amplification incident
        starts with someone adding one.
        """
        left = self.remaining()
        if left <= 0:
            self.check()
        if budget is None:
            return left
        return min(left, budget)

    def timeout(self, budget: float | None = None):
        """`async with deadline.timeout(budgets.first_event): ...`

        Raises TimeoutError on breach. The CALLER converts that into the right
        taxonomy class -- FirstEventTimeout, StallTimeout, ConnectTimeout --
        because only the caller knows which phase it was in, and a generic
        "timeout" error would erase exactly the distinction that decides
        whether a retry is safe.
        """
        return self._clock.timeout(self.slice(budget))

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<Deadline total={self.total:.3f}s remaining={self.remaining():.3f}s>"


class StallClock:
    """Tracks the gap since the last event, in two flavours.

    Kept separate from Deadline because it restarts and a deadline never does.
    Conflating "this stream went quiet" with "this request ran out of time"
    loses the ability to say which one happened, and those two have completely
    different remediations.
    """

    __slots__ = ("_clock", "_budgets", "_last_progress", "_last_liveness")

    def __init__(self, budgets: Budgets, *, clock: Clock) -> None:
        self._clock = clock
        self._budgets = budgets
        now = clock.now()
        self._last_progress = now
        self._last_liveness = now

    def mark_progress(self) -> None:
        """A content event arrived: real output, real progress."""
        now = self._clock.now()
        self._last_progress = now
        self._last_liveness = now

    def mark_liveness(self) -> None:
        """A heartbeat arrived. The socket is alive; the model may not be
        producing. Deliberately does NOT touch `_last_progress`."""
        self._last_liveness = self._clock.now()

    def remaining(self) -> float:
        now = self._clock.now()
        left = self._budgets.progress - (now - self._last_progress)
        if self._budgets.liveness is not None:
            left = min(left, self._budgets.liveness - (now - self._last_liveness))
        return left

    @property
    def stalled(self) -> bool:
        return self.remaining() <= 0


@contextlib.asynccontextmanager
async def phase(
    deadline: Deadline,
    budget: float | None,
    *,
    on_timeout: type[GatewayError],
    on_total: type[GatewayError] = TotalDeadlineExceeded,
    **ctx: object,
) -> AsyncIterator[None]:
    """Run one timed phase and convert a breach into the right error class.

    Usage:
        async with phase(dl, b.connect, on_timeout=ConnectTimeout, provider=p):
            response = await client.send(...)

    The conversion happens here so that no call site has to remember it, and
    so a breach of the total deadline surfaces as a total-deadline failure
    rather than as whatever phase happened to be running when the clock ran
    out -- those have different dispositions, and reporting a
    `FirstEventTimeout` when there is no time left invites a fallback that
    cannot possibly complete.

    --------------------------------------------------------------------
    `on_total`, and the trap it closes
    --------------------------------------------------------------------

    That reclassification exists to fix the *disposition* (`try_next=False`,
    because there is no time to try anything in). It also, silently, changes
    the *attribution*. (Historically `TotalDeadlineExceeded` was
    `Blame.PROVIDER` / `Health.FAILURE`; it is now `GATEWAY` / `NEUTRAL`,
    which removes the breaker hazard but not the reason for this parameter:
    a client-parked phase must still surface as `ClientTooSlow`, with CLIENT
    blame and a 499, or the client is never told it was the slow party.)

    For a phase that was waiting on the CLIENT -- the pump parked in
    `sink.send()`, the server reading a request body -- it is a C8 violation
    hiding inside a helper: the same client behaviour produces NEUTRAL health
    early in a request and FAILURE health late in it, purely because the total
    deadline happened to fire first. A slow client then opens a breaker
    against a provider that may not even have been contacted.

    Two independent implementations of this module's callers hit that edge and
    each patched it locally. That is the signal that the API was wrong rather
    than the callers: a phase knows which party it is waiting on, so it should
    say so once, here.

    So `on_total` names the class to raise when the TOTAL is what expired.
    Waiting on a provider: leave it alone. Waiting on the client: pass the
    client-blamed class.

    The entry check is deliberately NOT covered by `on_total`. If the deadline
    was already gone before this phase began, the time was spent somewhere
    this phase cannot see, and the honest report is the unattributed one.
    """
    deadline.check(**ctx)
    was_total = deadline.slice(budget) >= deadline.remaining() - 1e-9
    try:
        async with deadline.timeout(budget):
            yield
    except TimeoutError as exc:
        if was_total and deadline.expired:
            total_err = TotalDeadlineExceeded(
                f"total deadline of {deadline.total:.3f}s exceeded while awaiting "
                f"{on_timeout.__name__} ({deadline.elapsed():.3f}s elapsed)",
                cause=exc, **ctx,  # type: ignore[arg-type]
            )
            if on_total is TotalDeadlineExceeded:
                raise total_err from exc
            # Re-attributed, not replaced. The reported class carries the
            # right blame; the CAUSE still says the total deadline is what
            # actually expired -- which is the fact a postmortem needs and the
            # one an attribution fix would otherwise erase. A reclassification
            # that destroys the reason it was reclassifying is a worse bug
            # than the one it fixed.
            raise on_total(  # type: ignore[arg-type]
                f"{on_total.__name__}: {total_err.message}", cause=total_err, **ctx
            ) from total_err
        # Chained, not swallowed: the original TimeoutError carries the
        # traceback showing WHERE the await was parked, which is the only
        # thing that tells you whether you were stuck on connect, on the
        # model, or on a client that stopped reading.
        raise on_timeout(  # type: ignore[arg-type]
            f"{on_timeout.__name__} after {budget}s", cause=exc, **ctx
        ) from exc

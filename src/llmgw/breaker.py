"""Circuit breakers: memory across requests.

Everything before this module reasons about ONE request. `decide()` says what
this request may do next; the retry budget says how much of this request's
deadline it may spend doing it. Neither remembers anything once the request
ends, so the 400th request into a provider that has been returning 503 for a
minute is planned exactly like the first. It opens a socket, waits out the
connect and first-event clocks, burns its own deadline, and only THEN falls
back -- and the provider that is failing because it is overloaded receives
one more attempt for the privilege.

A breaker is the thing that remembers. It is a per-key state machine fed by
the same `Disposition` the executor already computes, and its whole value is
the OPEN state: a request that would have spent seconds discovering a known
fact instead fails in microseconds with `BreakerOpen`, which is `try_next`,
so the executor moves straight to the incumbent.

--------------------------------------------------------------------------
Three states, one probe
--------------------------------------------------------------------------

    CLOSED     requests flow; failures are counted in a sliding window.
    OPEN       every acquire() is refused; lasts `cooldown` seconds.
    HALF_OPEN  `half_open_probes` real requests are let through to test the
               provider; every other acquire() is refused until one settles.

The probe is a real request, not a synthetic ping -- a provider that answers
`GET /` and fails `POST /messages` is the common case, not the exception.
That is also why the probe count is bounded: at the instant the cooldown
expires there may be a thousand requests queued behind this key, and letting
them all through "to see" is the thundering herd that took the provider down
the first time. One probe finds out; the rest wait for its answer.

--------------------------------------------------------------------------
Only `Health.FAILURE` counts
--------------------------------------------------------------------------

`record()` looks at exactly one field of the disposition, `health`, and only
`FAILURE` moves a counter. This is contract C8 and the reason the taxonomy
took care to mark three families of error `NEUTRAL`:

  * Client disconnects and client stalls. A user closing a tab is not evidence
    about the provider. A gateway that counts it opens breakers against
    healthy providers during any client-side incident -- a bad frontend
    deploy, a mobile network partition -- at exactly the moment it needs
    those providers most.
  * 429. "Healthy but busy". 429s clear in seconds and breakers open for tens
    of seconds, so a breaker that counts them spends its life chasing a
    condition that already resolved. That is flapping, and rate pressure
    belongs to the limiter, which self-corrects.
  * `BreakerOpen` itself. A breaker that counts its own rejections feeds
    itself: every refused request is another failure, the window never
    empties, and the circuit can never sample reality again.

`record()` with a NEUTRAL disposition still settles the ticket. It just
changes nothing.

--------------------------------------------------------------------------
Epochs: the stale-result race
--------------------------------------------------------------------------

Requests are slow and the breaker is fast. A request that acquired its ticket
while the circuit was CLOSED may still be waiting on a socket when five
faster requests fail and open the circuit; its result -- success OR failure --
then arrives describing a world that no longer exists. Counted naively, a
slow success closes a circuit that just opened for good reason, or a slow
failure re-opens one a probe just recovered.

So every OPEN and every CLOSE increments an epoch, tickets carry the epoch
they were issued in, and a result from an older epoch settles the ticket and
changes nothing. The test is
`test_a_stale_success_cannot_close_a_newer_failure_epoch`.

--------------------------------------------------------------------------
Cancellation settles without recording
--------------------------------------------------------------------------

`release()` is for the path where there is no disposition at all: the task
was cancelled. It counts as nothing -- but it MUST return the probe slot,
because a cancelled probe that did not would leave the circuit HALF_OPEN with
its one slot taken by a request that no longer exists, and no way for any
future request to test the provider. That is a permanent outage with a
cancelled client as its trigger.

--------------------------------------------------------------------------
Time comes from the clock, and only when asked
--------------------------------------------------------------------------

There is no background task and no timer. The OPEN to HALF_OPEN move happens
lazily, on the next call that looks at the state after the cooldown has
elapsed. Two reasons. A timer per breaker per key is a task leak waiting to
happen -- the registry creates breakers lazily for every `(provider, model)`
and `(provider, credential)` it is ever asked about, and nothing would ever
cancel those timers. And lazy evaluation is exactly correct, not merely
cheaper: nothing can observe the state between calls, so a transition that
"happened" at cooldown expiry and one that happened at the first call after
it are indistinguishable to every caller.

All of this is single-threaded asyncio. No method awaits, so each call is
atomic with respect to the event loop, and a stampede of a thousand coroutines
at cooldown expiry yields exactly `half_open_probes` probes without a lock.
"""

from __future__ import annotations

import math
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum

from .clocks import Clock
from .errors import BreakerOpen, Disposition, Health

Key = tuple[str, str]
"""Whatever `Disposition.health_key` produced: `(provider, model)` for almost
everything, `(provider, "cred:<id>")` for authentication failures. The
breaker never looks inside it; the taxonomy already decided the blast
radius."""

TRANSITION_HISTORY = 1024
"""How many state changes the registry remembers. Bounded because a flapping
breaker is precisely the case where this list grows fastest, and an
unbounded diagnostic that grows during the incident it diagnoses is a second
incident."""


class BreakerState(Enum):
    """Values are the `llmgw_breaker_state` label vocabulary in metrics.py;
    `test_breaker_states_match_the_metric_vocabulary` pins that."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    @property
    def gauge_value(self) -> int:
        """`llmgw_breaker_state` is documented as 0 closed, 1 open, 2 half-open."""
        return _GAUGE[self]


_GAUGE = {BreakerState.CLOSED: 0, BreakerState.OPEN: 1, BreakerState.HALF_OPEN: 2}


@dataclass(frozen=True, slots=True)
class BreakerPolicy:
    """Per-registry breaker tuning. Every number here is a guess until S7
    produces real failure-rate distributions."""

    failure_threshold: int = 5
    """FAILURE records inside `window` that open the circuit. A count, not a
    rate: the breaker does not see successes and cannot compute one. That is
    a known limitation, stated in the doc rather than hidden in a default."""

    window: float = 30.0
    """Sliding window, seconds. A failure older than this stops counting.
    Sliding rather than consecutive because a provider failing 80% of
    requests still produces a success every fifth call, and a consecutive
    counter resets on every one of them and never trips."""

    cooldown: float = 10.0
    """Seconds spent OPEN before a probe is allowed."""

    half_open_probes: int = 1
    """Concurrent probes permitted in HALF_OPEN. One is safest for a
    recovering provider and slowest to restore capacity."""

    def validate(self) -> BreakerPolicy:
        if type(self.failure_threshold) is not int or self.failure_threshold < 1:
            raise ValueError("failure_threshold must be an int >= 1")
        if type(self.half_open_probes) is not int or self.half_open_probes < 1:
            raise ValueError("half_open_probes must be an int >= 1")
        for name in ("window", "cooldown"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a positive finite number of seconds")
        return self


@dataclass(slots=True)
class Ticket:
    """Proof that `acquire()` admitted a request, and the thing the result is
    recorded against.

    `epoch` is what makes the result attributable to the right world.
    `probe` is what makes a HALF_OPEN slot returnable. `settled` is what makes
    a double settle a loud error rather than a silent double count: a breaker
    that counts one failure twice opens at 3 failures while claiming to open
    at 5, and nobody tuning the threshold will ever find out why.
    """

    key: Key
    epoch: int
    probe: bool
    settled: bool = field(default=False, compare=False)


Transition = tuple[Key, BreakerState, BreakerState, float]
OnTransition = Callable[[Key, BreakerState, BreakerState, float], None]


class Breaker:
    """One circuit for one key. Created by `BreakerRegistry.for_key`."""

    __slots__ = (
        "key",
        "_policy",
        "_clock",
        "_on_transition",
        "_state",
        "_epoch",
        "_failures",
        "_probes_out",
        "_opened_at",
        "_probe_allowed_at",
        "_transition_count",
    )

    def __init__(
        self,
        key: Key,
        policy: BreakerPolicy,
        *,
        clock: Clock,
        on_transition: OnTransition | None = None,
    ) -> None:
        self.key = key
        self._policy = policy.validate()
        self._clock = clock
        self._on_transition = on_transition
        self._state = BreakerState.CLOSED
        self._epoch = 0
        # Failure timestamps inside the window. Bounded twice over: it is
        # cleared the moment it reaches `failure_threshold` (the circuit
        # opens), and `maxlen` says so in a way a future edit cannot quietly
        # undo. Pruned from the left on every record, so it never holds a
        # timestamp older than `window`.
        self._failures: deque[float] = deque(maxlen=self._policy.failure_threshold)
        self._probes_out = 0
        self._opened_at: float | None = None
        self._probe_allowed_at: float | None = None
        self._transition_count = 0

    # ------------------------------------------------------------ reads

    @property
    def state(self) -> BreakerState:
        """The state as of now. Reading it performs the lazy OPEN to HALF_OPEN
        move if the cooldown has elapsed, so a dashboard scraping an idle key
        sees `half_open` rather than an `open` that expired an hour ago."""
        self._advance(self._clock.now())
        return self._state

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def policy(self) -> BreakerPolicy:
        return self._policy

    def snapshot(self) -> dict[str, object]:
        """For `/probe` and the metrics exporter. `state` is a
        `metrics.BREAKER_STATES` value; `state_gauge` is the documented
        0/1/2 encoding of `llmgw_breaker_state`."""
        now = self._clock.now()
        self._advance(now)
        self._prune(now)
        remaining = None
        if self._state is BreakerState.OPEN and self._probe_allowed_at is not None:
            remaining = max(0.0, self._probe_allowed_at - now)
        return {
            "key": self.key,
            "state": self._state.value,
            "state_gauge": self._state.gauge_value,
            "epoch": self._epoch,
            "failures_in_window": len(self._failures),
            "failure_threshold": self._policy.failure_threshold,
            "probes_in_flight": self._probes_out,
            "opened_at": self._opened_at,
            "cooldown_remaining": remaining,
            "transitions": self._transition_count,
        }

    # ------------------------------------------------------------ admission

    def acquire(self) -> Ticket:
        """Admit one request or raise `BreakerOpen`.

        `BreakerOpen` is NEUTRAL and `try_next`, so the executor neither
        counts it here nor retries it here; it moves to the next target.
        `retry_after` carries the remaining cooldown while OPEN so a caller
        with no other target can tell the client something true.
        """
        now = self._clock.now()
        self._advance(now)
        if self._state is BreakerState.CLOSED:
            return Ticket(self.key, self._epoch, probe=False)
        if self._state is BreakerState.HALF_OPEN:
            if self._probes_out < self._policy.half_open_probes:
                self._probes_out += 1
                return Ticket(self.key, self._epoch, probe=True)
            raise BreakerOpen(
                f"circuit half-open for {self.key[0]}/{self.key[1]}; probe in flight",
                provider=self.key[0],
                model=self.key[1],
            )
        assert self._probe_allowed_at is not None
        remaining = max(0.0, self._probe_allowed_at - now)
        raise BreakerOpen(
            f"circuit open for {self.key[0]}/{self.key[1]}; "
            f"probe allowed in {remaining:.3f}s",
            provider=self.key[0],
            model=self.key[1],
            retry_after=remaining,
        )

    # ------------------------------------------------------------ settlement

    def record(self, ticket: Ticket, disposition: Disposition | None) -> None:
        """Settle a ticket with its result.

        `disposition` is what `decide()` returned for the attempt's error, or
        `None` when the attempt succeeded -- there is no error to decide on,
        so there is no `Disposition`, and `None` is the honest spelling of
        "nothing went wrong" rather than a synthetic disposition with an
        invented `health_key`.

        A stale ticket (older epoch) is settled and then ignored. A NEUTRAL
        disposition is settled and then ignored, except that a probe's slot
        is returned either way: a probe that was cancelled by its client or
        refused with a 429 told us nothing, and the circuit must remain
        testable by the next request.
        """
        self._settle(ticket)
        if ticket.epoch != self._epoch:
            return
        now = self._clock.now()
        if ticket.probe:
            self._probes_out = max(0, self._probes_out - 1)
            if disposition is None:
                self._close(now)
            elif disposition.health is Health.FAILURE:
                self._open(now)
            return
        if disposition is None or disposition.health is not Health.FAILURE:
            return
        if self._state is not BreakerState.CLOSED:
            # A non-probe ticket in the current epoch can only exist while
            # CLOSED; anything else is a ticket from before the last OPEN and
            # the epoch check above already dropped it. Defensive, not reachable.
            return  # pragma: no cover
        self._prune(now)
        self._failures.append(now)
        if len(self._failures) >= self._policy.failure_threshold:
            self._open(now)

    def release(self, ticket: Ticket) -> None:
        """Settle a ticket with NO result: the request was cancelled.

        Counts as nothing. Ten thousand of these on a healthy circuit leave
        it CLOSED with zero failures (`breaker_ignores_client_cancel`). The
        one thing it does do is return a probe slot, for the reason given in
        the module docstring: a cancelled probe that kept its slot would
        leave the circuit HALF_OPEN and untestable forever.
        """
        self._settle(ticket)
        if ticket.epoch != self._epoch:
            return
        if ticket.probe:
            self._probes_out = max(0, self._probes_out - 1)

    # ------------------------------------------------------------ internals

    def _settle(self, ticket: Ticket) -> None:
        if ticket.key != self.key:
            raise ValueError(f"ticket for {ticket.key} settled on breaker {self.key}")
        if ticket.settled:
            raise ValueError(
                f"ticket for {ticket.key} (epoch {ticket.epoch}) settled twice; "
                "a double settle is a double count"
            )
        ticket.settled = True

    def _prune(self, now: float) -> None:
        horizon = now - self._policy.window
        failures = self._failures
        while failures and failures[0] <= horizon:
            failures.popleft()

    def _advance(self, now: float) -> None:
        """The lazy OPEN to HALF_OPEN move."""
        if self._state is BreakerState.OPEN:
            assert self._probe_allowed_at is not None
            if now >= self._probe_allowed_at:
                self._probes_out = 0
                self._transition(BreakerState.HALF_OPEN, now)

    def _open(self, now: float) -> None:
        self._epoch += 1
        self._failures.clear()
        self._probes_out = 0
        self._opened_at = now
        self._probe_allowed_at = now + self._policy.cooldown
        self._transition(BreakerState.OPEN, now)

    def _close(self, now: float) -> None:
        self._epoch += 1
        self._failures.clear()
        self._probes_out = 0
        self._opened_at = None
        self._probe_allowed_at = None
        self._transition(BreakerState.CLOSED, now)

    def _transition(self, to: BreakerState, now: float) -> None:
        frm = self._state
        self._state = to
        self._transition_count += 1
        if self._on_transition is not None:
            self._on_transition(self.key, frm, to, now)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"<Breaker {self.key[0]}/{self.key[1]} {self._state.value} "
            f"epoch={self._epoch} failures={len(self._failures)}>"
        )


class BreakerRegistry:
    """One breaker per key, created on first sight.

    Lazy creation is the only workable shape: the key space is whatever
    `health_key()` produces, which includes `(provider, "cred:<id>")` for
    every credential that ever fails to authenticate, and nobody can
    enumerate that in advance. It is also bounded in practice by the catalog
    plus the credential set, both of which are closed, so the dictionary
    cannot grow without a config change.
    """

    __slots__ = ("_policy", "_clock", "_breakers", "_transitions", "_on_transition")

    def __init__(
        self,
        policy: BreakerPolicy,
        *,
        clock: Clock,
        on_transition: OnTransition | None = None,
        history: int = TRANSITION_HISTORY,
    ) -> None:
        self._policy = policy.validate()
        self._clock = clock
        self._breakers: dict[Key, Breaker] = {}
        self._transitions: deque[Transition] = deque(maxlen=history)
        self._on_transition = on_transition

    def for_key(self, key: Key) -> Breaker:
        breaker = self._breakers.get(key)
        if breaker is None:
            breaker = Breaker(key, self._policy, clock=self._clock, on_transition=self._record)
            self._breakers[key] = breaker
        return breaker

    def __len__(self) -> int:
        return len(self._breakers)

    def __contains__(self, key: Key) -> bool:
        return key in self._breakers

    def snapshot(self) -> dict[Key, dict[str, object]]:
        return {key: b.snapshot() for key, b in self._breakers.items()}

    @property
    def transitions(self) -> list[Transition]:
        """Every state change, oldest first, as `(key, from, to, at)`.

        This is the raw material of `llmgw_breaker_transitions_total` and of
        the flapping analysis: a breaker that is steadily OPEN produces one
        entry, a breaker that is steadily CLOSED produces none, and a breaker
        that is wrong about its threshold produces a stripe of them. The rate
        of this list is the metric that shows flapping; neither state alone
        does. Bounded to the last `history` entries.
        """
        return list(self._transitions)

    def _record(self, key: Key, frm: BreakerState, to: BreakerState, at: float) -> None:
        self._transitions.append((key, frm, to, at))
        if self._on_transition is not None:
            self._on_transition(key, frm, to, at)

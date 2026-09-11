"""Circuit breaker tests.

Everything runs on a `ManualClock`, so cooldowns and sliding windows are
proved by moving a number, never by waiting. The dispositions fed to the
breaker are real `decide()` output from real taxonomy classes rather than
hand-built `Disposition` objects: the property under test is that the
taxonomy's `health` column and the breaker's counters agree, and a test that
fabricates the disposition cannot notice when they stop agreeing.
"""

from __future__ import annotations

import random

import pytest

from llmgw import errors as E
from llmgw.breaker import (
    TRANSITION_HISTORY,
    Breaker,
    BreakerPolicy,
    BreakerRegistry,
    BreakerState,
    Ticket,
)
from llmgw.clocks import ManualClock
from llmgw.metrics import BREAKER_STATES

KEY = ("openai", "gpt-4o")
OTHER = ("anthropic", "claude-sonnet")


def failure() -> E.Disposition:
    """A FAILURE disposition from a class that is unambiguously a provider
    fault. Not `TotalDeadlineExceeded`: its health is being revisited."""
    return E.decide(E.UpstreamServerError("500", provider=KEY[0], model=KEY[1]),
                    committed=False)


def connect_failure() -> E.Disposition:
    return E.decide(E.ConnectTimeout("connect", provider=KEY[0], model=KEY[1]),
                    committed=False)


def neutral(cls: type[E.GatewayError]) -> E.Disposition:
    return E.decide(cls("x", provider=KEY[0], model=KEY[1]), committed=False)


def make(**policy_kw: object) -> tuple[Breaker, ManualClock, BreakerPolicy]:
    policy = BreakerPolicy(**policy_kw).validate()  # type: ignore[arg-type]
    clock = ManualClock(start=0.0)
    return Breaker(KEY, policy, clock=clock), clock, policy


def trip(breaker: Breaker, policy: BreakerPolicy) -> None:
    """Drive a CLOSED breaker to OPEN with exactly `failure_threshold` failures."""
    for _ in range(policy.failure_threshold):
        breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.OPEN


async def open_to_half_open(
    breaker: Breaker, clock: ManualClock, policy: BreakerPolicy
) -> None:
    await clock.advance(policy.cooldown)
    assert breaker.state is BreakerState.HALF_OPEN


# ------------------------------------------------------------------ policy


def test_policy_defaults_validate():
    BreakerPolicy().validate()


@pytest.mark.parametrize(
    "kw",
    [
        {"failure_threshold": 0},
        {"failure_threshold": 2.5},
        {"half_open_probes": 0},
        {"window": 0.0},
        {"window": float("inf")},
        {"cooldown": -1.0},
        {"cooldown": float("nan")},
    ],
    ids=lambda kw: next(iter(kw.items())).__repr__(),
)
def test_policy_rejects_nonsense(kw: dict[str, object]):
    with pytest.raises(ValueError):
        BreakerPolicy(**kw).validate()  # type: ignore[arg-type]


def test_breaker_states_match_the_metric_vocabulary():
    """`llmgw_breaker_state` and `_transitions_total{to}` bind to these
    strings; a renamed state would silently vanish from every dashboard."""
    assert tuple(s.value for s in BreakerState) == BREAKER_STATES
    assert [s.gauge_value for s in BreakerState] == [0, 1, 2]


# --------------------------------------------------------------- threshold


def test_the_threshold_opens_the_circuit_and_one_below_it_does_not():
    breaker, _, policy = make(failure_threshold=5)
    for _ in range(policy.failure_threshold - 1):
        breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.CLOSED
    assert breaker.snapshot()["failures_in_window"] == 4
    assert breaker.epoch == 0

    breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.OPEN
    assert breaker.epoch == 1
    assert breaker.snapshot()["failures_in_window"] == 0


def test_connect_timeouts_count_the_same_as_5xx():
    """The breaker reads `health`, not the class. Any FAILURE class trips it."""
    breaker, _, policy = make(failure_threshold=3)
    breaker.record(breaker.acquire(), connect_failure())
    breaker.record(breaker.acquire(), failure())
    breaker.record(breaker.acquire(), connect_failure())
    assert breaker.state is BreakerState.OPEN


def test_successes_do_not_reset_the_failure_count():
    """The sliding-window property. A provider failing 80% of requests still
    produces a success every fifth call; a consecutive counter resets on each
    one and never trips. Interleave successes and prove the window ignores
    them."""
    breaker, _, _ = make(failure_threshold=5, window=30.0)
    for _ in range(4):
        breaker.record(breaker.acquire(), failure())
        breaker.record(breaker.acquire(), None)  # success
    assert breaker.state is BreakerState.CLOSED
    breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.OPEN


async def test_failures_older_than_the_window_stop_counting():
    breaker, clock, policy = make(failure_threshold=5, window=30.0)
    for _ in range(policy.failure_threshold - 1):
        breaker.record(breaker.acquire(), failure())
    await clock.advance(policy.window + 0.001)
    breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.CLOSED
    assert breaker.snapshot()["failures_in_window"] == 1


async def test_failures_inside_the_window_still_count_after_time_passes():
    """The twin: the window is sliding, not a bucket that empties on a tick."""
    breaker, clock, _ = make(failure_threshold=3, window=30.0)
    breaker.record(breaker.acquire(), failure())
    await clock.advance(20.0)
    breaker.record(breaker.acquire(), failure())
    await clock.advance(9.0)  # first failure is 29s old: still inside
    breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.OPEN


def test_the_failure_window_is_bounded_by_the_threshold():
    """Memory-leak guard: the structure holding failure timestamps can never
    hold more than `failure_threshold` entries, because reaching that many
    opens the circuit and clears it. A provider incident must not be the
    trigger for unbounded growth."""
    breaker, _, policy = make(failure_threshold=4)
    for _ in range(policy.failure_threshold - 1):
        breaker.record(breaker.acquire(), failure())
    assert breaker._failures.maxlen == policy.failure_threshold
    assert len(breaker._failures) == policy.failure_threshold - 1


# --------------------------------------------------------------------- OPEN


def test_open_circuit_refuses_with_a_neutral_error():
    """`BreakerOpen` must be NEUTRAL or the breaker feeds itself: every
    refusal is another failure, the window never empties, and it never closes."""
    breaker, _, policy = make()
    trip(breaker, policy)
    with pytest.raises(E.BreakerOpen) as info:
        breaker.acquire()
    err = info.value
    assert err.health is E.Health.NEUTRAL
    assert err.try_next is True
    assert (err.provider, err.model) == KEY
    assert err.retry_after == pytest.approx(policy.cooldown)


async def test_retry_after_on_breaker_open_counts_down_the_cooldown():
    breaker, clock, policy = make(cooldown=10.0)
    trip(breaker, policy)
    await clock.advance(4.0)
    with pytest.raises(E.BreakerOpen) as info:
        breaker.acquire()
    assert info.value.retry_after == pytest.approx(6.0)


def test_refusals_fed_back_do_not_move_the_counters():
    """Feeding the breaker's own output back in, as a careless executor
    might, changes nothing."""
    breaker, _, policy = make(failure_threshold=3)
    trip(breaker, policy)
    epoch = breaker.epoch
    with pytest.raises(E.BreakerOpen) as info:
        breaker.acquire()
    # No ticket exists for a refusal, so the only way to "record" it is via a
    # ticket from before -- which the epoch fence ignores. Prove the health
    # column alone would also have been enough:
    assert E.decide(info.value, committed=False).health is E.Health.NEUTRAL
    assert breaker.epoch == epoch
    assert breaker.state is BreakerState.OPEN


# ---------------------------------------------------------------- HALF_OPEN


async def test_breaker_single_probe():
    """After the cooldown exactly one request is admitted as the probe; the
    next one is refused until the probe settles."""
    breaker, clock, policy = make(half_open_probes=1)
    trip(breaker, policy)
    await clock.advance(policy.cooldown - 0.001)
    with pytest.raises(E.BreakerOpen):
        breaker.acquire()
    assert breaker.state is BreakerState.OPEN

    await clock.advance(0.001)
    assert breaker.state is BreakerState.HALF_OPEN
    probe = breaker.acquire()
    assert probe.probe is True
    assert probe.epoch == breaker.epoch
    with pytest.raises(E.BreakerOpen) as info:
        breaker.acquire()
    assert info.value.health is E.Health.NEUTRAL
    assert breaker.snapshot()["probes_in_flight"] == 1


async def test_a_stampede_at_cooldown_expiry_yields_exactly_the_probe_budget():
    breaker, clock, policy = make(half_open_probes=2)
    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    admitted = 0
    for _ in range(50):
        try:
            assert breaker.acquire().probe is True
            admitted += 1
        except E.BreakerOpen:
            pass
    assert admitted == policy.half_open_probes


async def test_probe_success_closes_the_circuit_and_advances_the_epoch():
    breaker, clock, policy = make(failure_threshold=3)
    trip(breaker, policy)
    open_epoch = breaker.epoch
    await open_to_half_open(breaker, clock, policy)
    probe = breaker.acquire()
    breaker.record(probe, None)
    assert breaker.state is BreakerState.CLOSED
    assert breaker.epoch == open_epoch + 1
    snap = breaker.snapshot()
    assert snap["failures_in_window"] == 0
    assert snap["probes_in_flight"] == 0
    assert snap["cooldown_remaining"] is None
    # Counters reset: it takes a full threshold to trip again.
    for _ in range(policy.failure_threshold - 1):
        breaker.record(breaker.acquire(), failure())
    assert breaker.state is BreakerState.CLOSED


async def test_probe_failure_reopens_and_restarts_the_cooldown():
    breaker, clock, policy = make(cooldown=10.0)
    trip(breaker, policy)
    open_epoch = breaker.epoch
    await open_to_half_open(breaker, clock, policy)
    probe = breaker.acquire()
    await clock.advance(3.0)
    breaker.record(probe, failure())
    assert breaker.state is BreakerState.OPEN
    assert breaker.epoch == open_epoch + 1
    assert breaker.snapshot()["cooldown_remaining"] == pytest.approx(10.0)
    await clock.advance(9.999)
    with pytest.raises(E.BreakerOpen):
        breaker.acquire()
    await clock.advance(0.001)
    assert breaker.acquire().probe is True


async def test_a_neutral_probe_result_keeps_the_circuit_half_open_and_returns_the_slot():
    """A probe that hit a 429 or whose client hung up told us nothing about
    the provider. The circuit stays HALF_OPEN, and the next request probes."""
    breaker, clock, policy = make()
    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    probe = breaker.acquire()
    breaker.record(probe, neutral(E.RateLimited))
    assert breaker.state is BreakerState.HALF_OPEN
    assert breaker.snapshot()["probes_in_flight"] == 0
    assert breaker.acquire().probe is True


# ------------------------------------------------------------------- epochs


async def test_a_stale_success_cannot_close_a_newer_failure_epoch():
    """THE race. A request admitted while CLOSED is slow. Five fast requests
    fail and open the circuit. The slow request then succeeds -- describing
    the provider as it was, not as it is -- and must not close the circuit."""
    breaker, clock, policy = make(failure_threshold=5, cooldown=10.0)
    slow = breaker.acquire()
    assert slow.epoch == 0
    trip(breaker, policy)
    assert breaker.epoch == 1
    await clock.advance(1.0)
    breaker.record(slow, None)
    assert breaker.state is BreakerState.OPEN
    assert breaker.epoch == 1
    with pytest.raises(E.BreakerOpen):
        breaker.acquire()


async def test_a_stale_failure_cannot_reopen_a_newer_recovered_epoch():
    """The failure-side twin: a request from before the outage delivers its
    failure after a probe has recovered the circuit."""
    breaker, clock, policy = make(failure_threshold=3)
    old = breaker.acquire()
    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    breaker.record(breaker.acquire(), None)
    assert breaker.state is BreakerState.CLOSED
    recovered_epoch = breaker.epoch
    breaker.record(old, failure())
    assert breaker.state is BreakerState.CLOSED
    assert breaker.epoch == recovered_epoch
    assert breaker.snapshot()["failures_in_window"] == 0


async def test_stale_failures_do_not_accumulate_toward_the_next_trip():
    """Several in-flight requests from the old epoch all fail late. None of
    them may count toward the freshly closed epoch's threshold."""
    breaker, clock, policy = make(failure_threshold=3)
    olds = [breaker.acquire() for _ in range(10)]
    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    breaker.record(breaker.acquire(), None)
    for t in olds:
        breaker.record(t, failure())
    assert breaker.state is BreakerState.CLOSED
    assert breaker.snapshot()["failures_in_window"] == 0


async def test_a_stale_probe_result_does_not_touch_a_reopened_circuit():
    """With two probes, the first failing re-opens the circuit. The second
    probe's late success belongs to the old epoch and must not close it."""
    breaker, clock, policy = make(half_open_probes=2, cooldown=10.0)
    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    a, b = breaker.acquire(), breaker.acquire()
    breaker.record(a, failure())
    assert breaker.state is BreakerState.OPEN
    reopened = breaker.epoch
    breaker.record(b, None)
    assert breaker.state is BreakerState.OPEN
    assert breaker.epoch == reopened


# ------------------------------------------------------------- cancellation


async def test_breaker_ignores_client_cancel():
    """C8. Ten cancellations on a healthy circuit are ten non-events; a
    cancelled probe returns its slot so the circuit stays testable."""
    breaker, clock, policy = make(failure_threshold=3)
    for _ in range(10):
        breaker.release(breaker.acquire())
    assert breaker.state is BreakerState.CLOSED
    assert breaker.snapshot()["failures_in_window"] == 0
    assert breaker.epoch == 0

    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    probe = breaker.acquire()
    with pytest.raises(E.BreakerOpen):
        breaker.acquire()
    breaker.release(probe)
    assert breaker.state is BreakerState.HALF_OPEN
    assert breaker.snapshot()["probes_in_flight"] == 0
    second = breaker.acquire()
    assert second.probe is True
    breaker.record(second, None)
    assert breaker.state is BreakerState.CLOSED


async def test_releasing_a_stale_probe_does_not_free_a_slot_in_the_new_epoch():
    """Two probes out; the first fails and re-opens the circuit. After the next
    cooldown the second, stale probe is cancelled. Its release must not hand
    the new epoch a third slot."""
    breaker, clock, policy = make(half_open_probes=2, cooldown=10.0)
    trip(breaker, policy)
    await open_to_half_open(breaker, clock, policy)
    a, b = breaker.acquire(), breaker.acquire()
    breaker.record(a, failure())
    assert breaker.state is BreakerState.OPEN
    await open_to_half_open(breaker, clock, policy)
    c, d = breaker.acquire(), breaker.acquire()
    with pytest.raises(E.BreakerOpen):
        breaker.acquire()
    breaker.release(b)  # stale
    with pytest.raises(E.BreakerOpen):
        breaker.acquire()
    assert breaker.snapshot()["probes_in_flight"] == 2
    breaker.release(c)
    assert breaker.acquire().probe is True
    breaker.record(d, None)
    assert breaker.state is BreakerState.CLOSED


# --------------------------------------------------------- NEUTRAL health


@pytest.mark.parametrize(
    "cls",
    [E.ClientDisconnected, E.ClientTooSlow, E.RateLimited, E.BreakerOpen],
    ids=lambda c: c.__name__,
)
def test_neutral_dispositions_never_count(cls: type[E.GatewayError]):
    """Real `decide()` output from the three families the taxonomy marks
    NEUTRAL. If any of these ever flips to FAILURE, this test is the thing
    that notices before a client incident opens a breaker."""
    breaker, _, _ = make(failure_threshold=2)
    for _ in range(20):
        breaker.record(breaker.acquire(), neutral(cls))
    assert breaker.state is BreakerState.CLOSED
    assert breaker.snapshot()["failures_in_window"] == 0
    assert breaker.epoch == 0


def test_a_committed_disposition_still_carries_health():
    """`decide(committed=True)` forbids retries but does not change health:
    a post-commitment 5xx is still evidence about the provider."""
    breaker, _, _ = make(failure_threshold=2)
    err = E.UpstreamServerError("500", provider=KEY[0], model=KEY[1])
    d = E.decide(err, committed=True)
    assert d.try_next is False
    breaker.record(breaker.acquire(), d)
    breaker.record(breaker.acquire(), d)
    assert breaker.state is BreakerState.OPEN


# ------------------------------------------------------------ double settle


def test_a_ticket_settles_exactly_once():
    breaker, _, _ = make()
    t = breaker.acquire()
    breaker.record(t, failure())
    with pytest.raises(ValueError):
        breaker.record(t, failure())
    with pytest.raises(ValueError):
        breaker.release(t)
    assert breaker.snapshot()["failures_in_window"] == 1

    u = breaker.acquire()
    breaker.release(u)
    with pytest.raises(ValueError):
        breaker.release(u)
    with pytest.raises(ValueError):
        breaker.record(u, None)


def test_a_ticket_from_another_key_is_refused():
    breaker, _, _ = make()
    with pytest.raises(ValueError):
        breaker.record(Ticket(OTHER, epoch=0, probe=False), failure())


# ----------------------------------------------------------------- registry


def test_registry_creates_breakers_lazily_and_returns_the_same_one():
    reg = BreakerRegistry(BreakerPolicy(), clock=ManualClock(start=0.0))
    assert len(reg) == 0
    assert KEY not in reg
    a = reg.for_key(KEY)
    assert reg.for_key(KEY) is a
    assert len(reg) == 1
    assert KEY in reg


def test_one_key_does_not_touch_another():
    """A 500 from one model says nothing about another, and an auth failure
    keyed on a credential says nothing about the model."""
    reg = BreakerRegistry(BreakerPolicy(failure_threshold=2), clock=ManualClock(start=0.0))
    a, b = reg.for_key(KEY), reg.for_key(OTHER)
    cred = reg.for_key(("openai", "cred:tenant-42"))
    for _ in range(2):
        a.record(a.acquire(), failure())
    assert a.state is BreakerState.OPEN
    assert b.state is BreakerState.CLOSED
    assert cred.state is BreakerState.CLOSED
    assert b.acquire().probe is False


def test_auth_failures_land_on_the_credential_key_not_the_target():
    """End to end through `decide()`: the registry keys on whatever
    `health_key` says, and for a 401 that is the credential."""
    reg = BreakerRegistry(BreakerPolicy(failure_threshold=1), clock=ManualClock(start=0.0))
    err = E.AuthenticationFailed("401", provider="openai", model="gpt-4o",
                                 credential_id="tenant-42")
    d = E.decide(err, committed=False)
    breaker = reg.for_key(d.health_key)
    breaker.record(breaker.acquire(), d)
    # The credential circuit carries no provider entry -- a key is bad wherever
    # it is used -- so the key is ("cred", <id>), and the model circuit is
    # untouched.
    assert reg.for_key(("cred", "tenant-42")).state is BreakerState.OPEN
    assert reg.for_key(("openai", "gpt-4o")).state is BreakerState.CLOSED


async def test_registry_snapshot_uses_the_metric_vocabulary():
    clock = ManualClock(start=0.0)
    reg = BreakerRegistry(BreakerPolicy(failure_threshold=1, cooldown=5.0), clock=clock)
    a, b = reg.for_key(KEY), reg.for_key(OTHER)
    a.record(a.acquire(), failure())
    snap = reg.snapshot()
    assert set(snap) == {KEY, OTHER}
    assert snap[KEY]["state"] == "open" and snap[KEY]["state_gauge"] == 1
    assert snap[OTHER]["state"] == "closed" and snap[OTHER]["state_gauge"] == 0
    for s in snap.values():
        assert s["state"] in BREAKER_STATES
    await clock.advance(5.0)
    assert reg.snapshot()[KEY]["state"] == "half_open"
    assert reg.snapshot()[KEY]["state_gauge"] == 2
    assert b.state is BreakerState.CLOSED


async def test_registry_records_every_transition_with_the_clock_time():
    clock = ManualClock(start=0.0)
    seen: list[tuple] = []
    reg = BreakerRegistry(
        BreakerPolicy(failure_threshold=1, cooldown=5.0),
        clock=clock,
        on_transition=lambda *t: seen.append(t),
    )
    a = reg.for_key(KEY)
    a.record(a.acquire(), failure())            # closed -> open at t=0
    await clock.advance(5.0)
    probe = a.acquire()                          # open -> half_open at t=5
    await clock.advance(1.0)
    a.record(probe, None)                        # half_open -> closed at t=6
    assert reg.transitions == [
        (KEY, BreakerState.CLOSED, BreakerState.OPEN, 0.0),
        (KEY, BreakerState.OPEN, BreakerState.HALF_OPEN, 5.0),
        (KEY, BreakerState.HALF_OPEN, BreakerState.CLOSED, 6.0),
    ]
    assert seen == reg.transitions
    assert reg.snapshot()[KEY]["transitions"] == 3


async def test_transition_history_is_bounded():
    """A flapping breaker is exactly the case that produces the most entries,
    and a diagnostic that grows during the incident it diagnoses is a second
    incident."""
    clock = ManualClock(start=0.0)
    reg = BreakerRegistry(BreakerPolicy(failure_threshold=1, cooldown=1.0), clock=clock,
                          history=8)
    a = reg.for_key(KEY)
    for _ in range(20):
        a.record(a.acquire(), failure())
        await clock.advance(1.0)
        a.record(a.acquire(), failure())  # probe fails: half_open -> open
        await clock.advance(1.0)
    assert len(reg.transitions) == 8
    assert TRANSITION_HISTORY >= 8


# ----------------------------------------------------------- property loop


async def test_random_sequences_never_leak_the_probe_slot_or_overcount():
    """Random acquire/record/release/advance, with a model alongside.

    Two invariants at every step:
      1. HALF_OPEN with no unsettled probe from the current epoch must be
         acquirable -- the slot is never leaked.
      2. `failures_in_window` never exceeds the number of FAILURE records
         made inside the window -- nothing is double counted.
    Plus: probes in flight never exceed the budget, and every ticket the
    breaker hands out is settled exactly once by the end.
    """
    for seed in range(40):
        rng = random.Random(seed)
        policy = BreakerPolicy(
            failure_threshold=rng.randint(1, 5),
            window=rng.choice([1.0, 5.0, 30.0]),
            cooldown=rng.choice([0.5, 2.0, 10.0]),
            half_open_probes=rng.randint(1, 3),
        )
        clock = ManualClock(start=0.0)
        breaker = Breaker(KEY, policy, clock=clock)
        outstanding: list[Ticket] = []
        failure_times: list[float] = []

        for _ in range(300):
            op = rng.random()
            if op < 0.35:
                try:
                    outstanding.append(breaker.acquire())
                except E.BreakerOpen as exc:
                    assert exc.health is E.Health.NEUTRAL
            elif op < 0.75 and outstanding:
                t = outstanding.pop(rng.randrange(len(outstanding)))
                r = rng.random()
                if r < 0.4:
                    breaker.record(t, failure())
                    failure_times.append(clock.now())
                elif r < 0.7:
                    breaker.record(t, None)
                elif r < 0.85:
                    breaker.record(t, neutral(E.ClientDisconnected))
                else:
                    breaker.release(t)
                assert t.settled
            else:
                await clock.advance(rng.choice([0.1, 0.5, 1.0, 3.0]))

            now = clock.now()
            snap = breaker.snapshot()
            in_window = sum(1 for ts in failure_times if ts > now - policy.window)
            assert snap["failures_in_window"] <= in_window
            assert snap["failures_in_window"] < policy.failure_threshold
            assert 0 <= snap["probes_in_flight"] <= policy.half_open_probes
            if breaker.state is BreakerState.HALF_OPEN:
                live_probes = [
                    t for t in outstanding if t.probe and t.epoch == breaker.epoch
                ]
                assert len(live_probes) == snap["probes_in_flight"]
                if not live_probes:
                    outstanding.append(breaker.acquire())
                    assert outstanding[-1].probe is True

        for t in outstanding:
            breaker.release(t)
        if breaker.state is BreakerState.OPEN:
            await clock.advance(policy.cooldown)
        assert breaker.acquire() is not None

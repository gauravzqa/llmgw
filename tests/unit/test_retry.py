"""Retry budget tests.

Everything here runs on a `ManualClock` with a scripted or seeded generator,
so the whole file proves backoff behaviour without ever backing off. The one
test that uses real randomness is the distribution test, and it asserts a
*shape* rather than a value -- a jitter test that pins a single draw proves
the arithmetic and misses the only property jitter has.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from llmgw import errors as E
from llmgw.clocks import Deadline, ManualClock
from llmgw.retry import RetryBudget, RetryPolicy


class ScriptedRandom(random.Random):
    """A `Random` whose `random()` replays a fixed sequence in [0, 1).

    A subclass rather than a stub object because `uniform(a, b)` is defined as
    `a + (b - a) * self.random()`, so overriding one method gives exact,
    readable control over the draw while keeping the real class's contract.
    The sequence repeats once exhausted, which keeps loop tests short.
    """

    def __init__(self, values: list[float]) -> None:
        super().__init__()
        self._values = list(values)
        self._i = 0

    def random(self) -> float:
        value = self._values[self._i % len(self._values)]
        self._i += 1
        return value


def make(
    *,
    total: float = 10.0,
    draws: list[float] | None = None,
    rng: random.Random | None = None,
    **policy_kw: object,
) -> tuple[RetryPolicy, RetryBudget, ManualClock, Deadline]:
    """A policy, a budget, and the clock and deadline they share."""
    policy = RetryPolicy(**policy_kw).validate()  # type: ignore[arg-type]
    # start=0.0 so that `remaining()` is exactly `total`: the pay-off
    # boundary is asserted to the epsilon, and a 1000.0 origin puts float
    # noise in the subtraction big enough to swallow the epsilon.
    clock = ManualClock(start=0.0)
    deadline = Deadline(clock, total)
    generator = rng if rng is not None else ScriptedRandom(draws or [1.0])
    return policy, RetryBudget(policy, deadline, clock=clock, rng=generator), clock, deadline


def transient(**kw: object) -> E.GatewayError:
    """A plainly retryable error: 5xx, retry_same and try_next both true."""
    return E.UpstreamServerError(
        "upstream 500", provider="acme", model="m", **kw  # type: ignore[arg-type]
    )


# --------------------------------------------------------------- the backoff


def test_the_jitter_window_doubles_with_every_attempt():
    # random() == 1.0 makes uniform(0, c) return c, so each delay IS the window.
    _, budget, _, _ = make(draws=[1.0], base_delay=0.1, max_delay=100.0, max_attempts=10)
    windows = [budget.delay_for(transient(), attempt=n) for n in range(5)]
    assert windows == pytest.approx([0.1, 0.2, 0.4, 0.8, 1.6])


def test_the_jitter_window_is_capped_at_max_delay():
    _, budget, _, _ = make(
        draws=[1.0], base_delay=0.1, max_delay=0.5, max_attempts=20, total=1000.0
    )
    windows = [budget.delay_for(transient(), attempt=n) for n in range(8)]
    assert windows == pytest.approx([0.1, 0.2, 0.4, 0.5, 0.5, 0.5, 0.5, 0.5])


def test_an_absurd_attempt_index_is_capped_and_not_an_overflow():
    """`2.0 ** 1024` raises OverflowError. The attempt cap normally keeps the
    exponent tiny, so this only bites a caller who configured a huge
    max_attempts -- and an OverflowError raised inside the handler for a
    provider failure is a spectacularly bad time to learn about it."""
    _, budget, _, _ = make(
        draws=[1.0], base_delay=0.1, max_delay=0.5, max_attempts=5000, total=1000.0
    )
    assert budget.delay_for(transient(), attempt=4000) == pytest.approx(0.5)


def test_the_cap_bounds_the_window_and_not_the_sample():
    # If the cap were applied to the drawn value instead of the window, every
    # long draw would land on exactly max_delay and the fleet would
    # re-synchronise at the cap. Half a draw of a capped window is half the cap.
    _, budget, _, _ = make(draws=[0.5], base_delay=0.1, max_delay=0.5, max_attempts=20)
    assert budget.delay_for(transient(), attempt=6) == pytest.approx(0.25)


def test_full_jitter_spreads_delays_over_the_whole_window():
    """The distribution, not one draw. Equal jitter and 'backoff plus a little
    noise' both pass an in-range assertion; neither survives this one."""
    policy = RetryPolicy(base_delay=1.0, max_delay=8.0, max_attempts=500).validate()
    clock = ManualClock()
    budget = RetryBudget(
        policy, Deadline(clock, 10_000.0), clock=clock, rng=random.Random(20260909)
    )
    ceiling = 4.0  # attempt 2 -> base_delay * 2**2, under the cap
    draws = [budget.delay_for(transient(), attempt=2) for _ in range(400)]

    assert min(draws) >= 0.0
    assert max(draws) < ceiling
    # Degenerate distributions fail here: a fixed backoff has min == max, and
    # equal jitter never produces anything below half the ceiling.
    assert min(draws) < ceiling / 4
    assert max(draws) > ceiling * 3 / 4
    # Uniform over [0, ceiling) has mean ceiling/2; 400 draws is plenty to say
    # so loosely, and a loose bound is the right kind for a random test.
    assert ceiling * 0.4 < sum(draws) / len(draws) < ceiling * 0.6


def test_the_generator_is_injected_so_two_budgets_can_be_made_to_agree():
    a = make(draws=None, rng=random.Random(7), base_delay=0.1, max_attempts=9)[1]
    b = make(draws=None, rng=random.Random(7), base_delay=0.1, max_attempts=9)[1]
    left = [a.delay_for(transient(), attempt=n) for n in range(5)]
    right = [b.delay_for(transient(), attempt=n) for n in range(5)]
    assert left == right


# ----------------------------------------------------------- Retry-After


def test_retry_after_is_floor():
    """CONTRACTS.md C5's named test: Retry-After floors the delay, jitter may
    never undercut it, and a Retry-After that does not fit stops the request
    instead of being shortened to fit."""
    # 1. A floor above the jitter draw wins.
    _, budget, _, _ = make(draws=[0.1], base_delay=0.1, max_delay=2.0, max_attempts=9)
    err = E.RateLimited("429", provider="acme", model="m", retry_after=1.25)
    assert budget.delay_for(err, attempt=0) == pytest.approx(1.25)

    # 2. Jitter never undercuts it -- not for any draw, including zero.
    _, low, _, _ = make(draws=[0.0], base_delay=0.5, max_delay=2.0, max_attempts=9)
    for n in range(4):
        assert low.delay_for(err, attempt=n) == pytest.approx(1.25)

    # 3. A draw above the floor is NOT clamped down to it. The header is a
    #    floor, not the delay: backing off further is always allowed.
    _, high, _, _ = make(draws=[1.0], base_delay=4.0, max_delay=8.0, max_attempts=9)
    assert high.delay_for(err, attempt=0) == pytest.approx(4.0)

    # 4. A floor larger than the remaining budget refuses rather than sleeping
    #    past the deadline, and rather than shortening the wait -- an early
    #    arrival is refused again and has spent the budget to learn nothing.
    _, tight, _, deadline = make(
        total=1.0, draws=[0.0], base_delay=0.1, max_delay=2.0, max_attempts=9
    )
    far = E.RateLimited("429", provider="acme", model="m", retry_after=30.0)
    with pytest.raises(E.RetryBudgetExhausted):
        tight.delay_for(far, attempt=0)
    assert deadline.remaining() == pytest.approx(1.0)  # nothing was slept


def test_a_retry_after_of_zero_is_honoured_as_zero_not_treated_as_absent():
    """`if err.retry_after:` is False for 0.0, which silently turns 'come back
    now' into 'we have no opinion'. They are different statements, so the
    floor is exposed as its own function and the distinction is asserted."""
    policy = RetryPolicy().validate()
    zero = E.RateLimited("429", retry_after=0.0)
    absent = E.RateLimited("429")

    assert policy.floor_for(zero) == 0.0
    assert policy.floor_for(zero) is not None
    assert policy.floor_for(absent) is None
    # A zero floor is a floor: it cannot raise the delay, and it must not
    # lower it either.
    _, budget, _, _ = make(draws=[1.0], base_delay=0.4, max_delay=2.0, max_attempts=9)
    assert budget.delay_for(zero, attempt=0) == pytest.approx(0.4)


def test_a_hostile_retry_after_is_clamped_rather_than_fatal():
    policy = RetryPolicy().validate()
    assert policy.floor_for(E.RateLimited("429", retry_after=-5.0)) == 0.0
    assert policy.floor_for(E.RateLimited("429", retry_after=float("nan"))) is None
    assert policy.floor_for(E.RateLimited("429", retry_after=float("inf"))) is None


def test_respect_retry_after_false_ignores_the_header_entirely():
    _, budget, _, _ = make(
        draws=[0.0], base_delay=0.1, max_attempts=9, respect_retry_after=False
    )
    err = E.RateLimited("429", provider="acme", model="m", retry_after=9.0)
    assert budget.delay_for(err, attempt=0) == pytest.approx(0.0)


# --------------------------------------------- refusing a retry that cannot pay


def test_a_retry_is_refused_exactly_when_it_cannot_pay_off():
    """The boundary is `delay + min_attempt_time >= remaining`, and it is
    asserted at the boundary and one epsilon on each side -- an off-by-one on
    a comparison here is the difference between refusing a doomed retry and
    sleeping the client's last 200 ms to reach the same error."""
    eps = 1e-6
    delay = 0.4  # draw of 1.0 against a 0.4 window
    min_attempt = 0.05

    def refuses(total: float) -> bool:
        _, budget, _, _ = make(
            total=total,
            draws=[1.0],
            base_delay=0.4,
            max_delay=2.0,
            min_attempt_time=min_attempt,
            max_attempts=9,
        )
        try:
            budget.delay_for(transient(), attempt=0)
        except E.RetryBudgetExhausted:
            return True
        return False

    boundary = delay + min_attempt  # remaining == delay + min_attempt exactly
    assert refuses(boundary) is True           # `>=` refuses the exact tie
    assert refuses(boundary - eps) is True     # one epsilon short: still doomed
    assert refuses(boundary + eps) is False    # one epsilon spare: allowed

    # An already-blown deadline is the degenerate case of the same rule.
    _, spent, clock, _ = make(total=1.0, draws=[0.0], base_delay=0.1, max_attempts=9)
    clock._now += 2.0
    with pytest.raises(E.RetryBudgetExhausted):
        spent.delay_for(transient(), attempt=0)


def test_the_refusal_carries_the_original_error_as_its_cause():
    """The client should see the provider's 503, not our accounting. The
    refusal is a control signal, so it chains what we stopped retrying."""
    _, budget, _, _ = make(total=0.01, draws=[1.0], base_delay=0.5, max_attempts=9)
    err = transient()
    with pytest.raises(E.RetryBudgetExhausted) as caught:
        budget.delay_for(err, attempt=0)
    assert caught.value.cause is err
    assert caught.value.provider == "acme"
    assert caught.value.model == "m"
    # NEUTRAL, and not TotalDeadlineExceeded: the budget does not know where
    # the time went, and blaming the provider for our subtraction opens
    # breakers on the strength of arithmetic.
    assert caught.value.health is E.Health.NEUTRAL
    assert caught.value.try_next is False


# ------------------------------------------------------------ the attempt cap


def test_max_attempts_counts_every_attempt_across_targets():
    """The budget is per request, not per target. Two 'retries each' over
    three targets is six requests to a provider that is failing because it is
    overloaded, and that reading is the one people implement by accident."""
    _, budget, _, _ = make(draws=[0.0], max_attempts=3, base_delay=0.1)
    assert budget.attempts_used == 0

    budget.record_attempt()                                  # target A, attempt 0
    assert budget.attempts_used == 1
    assert budget.delay_for(transient(), attempt=0) == pytest.approx(0.0)

    budget.record_attempt()                                  # target B, attempt 1
    assert budget.delay_for(transient(), attempt=1) == pytest.approx(0.0)

    budget.record_attempt()                                  # target C, attempt 2
    assert budget.attempts_used == 3
    with pytest.raises(E.RetryBudgetExhausted, match="attempt budget spent"):
        budget.delay_for(transient(), attempt=2)


def test_max_attempts_of_one_means_no_retry_at_all():
    _, budget, _, _ = make(draws=[0.0], max_attempts=1)
    budget.record_attempt()
    with pytest.raises(E.RetryBudgetExhausted, match="attempt budget spent"):
        budget.delay_for(transient(), attempt=0)


def test_a_caller_that_forgets_record_attempt_still_exhausts():
    """The two counters must agree; when they do not, trust the larger. The
    failure mode of trusting the smaller is unbounded retries."""
    _, budget, _, _ = make(draws=[0.0], max_attempts=2)
    assert budget.attempts_used == 0
    assert budget.delay_for(transient(), attempt=0) == pytest.approx(0.0)
    with pytest.raises(E.RetryBudgetExhausted, match="attempt budget spent"):
        budget.delay_for(transient(), attempt=1)


# ----------------------------------------------------------------- C5, enabled


def test_disabled_retries_refuse_the_very_first_call():
    """CONTRACTS.md C5. `X-Gw-No-Retry: 1` sets `enabled=False` so an outer
    gateway owns the chain; layered retries multiply, so the answer to 'who
    retries' is one named layer."""
    _, budget, _, deadline = make(draws=[0.0], enabled=False, max_attempts=5, total=100.0)
    budget.record_attempt()
    with pytest.raises(E.RetryBudgetExhausted, match="X-Gw-No-Retry"):
        budget.delay_for(transient(), attempt=0)
    assert deadline.remaining() == pytest.approx(100.0)


async def test_disabled_retries_never_sleep():
    _, budget, clock, _ = make(draws=[1.0], enabled=False, base_delay=1.0)
    started = clock.now()
    with pytest.raises(E.RetryBudgetExhausted):
        await budget.wait(transient(), attempt=0)
    assert clock.pending_sleepers == 0
    assert clock.now() == started


# ------------------------------------------------------- the error disposition


def test_an_error_retryable_nowhere_is_a_caller_bug_not_a_budget_outcome():
    """`ValueError`, not `RetryBudgetExhausted`. The budget's refusal is an
    operational signal people alert on and tune against; a caller asking it to
    space out a ContentFiltered is broken, and dressing that up as a budget
    outcome hides the bug in the one metric that would find it."""
    _, budget, _, _ = make(draws=[0.0], max_attempts=9)
    for err in (
        E.ContentFiltered("refused"),
        E.TotalDeadlineExceeded("out of time"),
        E.RequestTooLarge("too big"),
    ):
        assert not (err.retry_same or err.try_next)
        with pytest.raises(ValueError, match="not retryable at any target"):
            budget.delay_for(err, attempt=0)


def test_an_error_eligible_only_for_the_next_target_still_gets_a_delay():
    """`retry_same=False` does not mean 'no delay'. A 400 or a
    FirstEventTimeout may not go back to the same target, but it may go to the
    next one -- and a fallback every failing request takes at the same instant
    is a retry storm pointed at a different provider."""
    for err in (
        E.InvalidRequest("bad schema", provider="acme", model="m"),
        E.FirstEventTimeout("slow", provider="acme", model="m"),
    ):
        assert err.retry_same is False and err.try_next is True
        _, budget, _, _ = make(draws=[1.0], base_delay=0.2, max_delay=2.0, max_attempts=9)
        assert budget.delay_for(err, attempt=0) == pytest.approx(0.2)


def test_a_negative_attempt_index_is_rejected():
    _, budget, _, _ = make(draws=[0.0], max_attempts=9)
    with pytest.raises(ValueError, match="attempt must be >= 0"):
        budget.delay_for(transient(), attempt=-1)


# ------------------------------------------------------------------ the clock


async def test_wait_sleeps_the_delay_it_returns_on_the_injected_clock():
    _, budget, clock, _ = make(draws=[1.0], base_delay=0.8, max_delay=2.0, max_attempts=9)
    started = clock.now()
    task = asyncio.create_task(budget.wait(transient(), attempt=0))
    await clock.advance(0.8)
    slept = await task
    assert slept == pytest.approx(0.8)
    assert clock.now() - started == pytest.approx(0.8)
    assert clock.pending_sleepers == 0


async def test_total_deadline_never_resets():
    """The property, at this layer: a full retry loop driven on a ManualClock
    can never place the request past its own deadline, however many attempts
    and waits it strings together. Not because a rule says so -- because
    `delay_for` refuses every wait that would not leave room for the attempt
    it enables."""
    total = 2.0
    attempt_cost = 0.12  # what each doomed attempt burns before it fails
    policy = RetryPolicy(
        max_attempts=50, base_delay=0.05, max_delay=0.5, min_attempt_time=0.05
    ).validate()
    clock = ManualClock()
    deadline = Deadline(clock, total)
    budget = RetryBudget(policy, deadline, clock=clock, rng=random.Random(11))

    waits: list[float] = []
    costs: list[float] = []
    finished_at: list[float] = []

    async def drive() -> None:
        attempt = 0
        while True:
            budget.record_attempt()
            # The attempt itself is bounded by the deadline, exactly as
            # `deadline.slice()` bounds a real phase. Without that clamp this
            # test would be measuring the executor's timeout arithmetic and
            # not the budget's.
            cost = min(attempt_cost, max(0.0, deadline.remaining()))
            costs.append(cost)
            await clock.sleep(cost)                  # the attempt, failing
            try:
                waits.append(await budget.wait(transient(), attempt=attempt))
            except E.RetryBudgetExhausted:
                finished_at.append(clock.now())
                return
            # Every wait that was allowed must leave room for what it enables.
            assert deadline.remaining() >= policy.min_attempt_time
            attempt += 1

    task = asyncio.create_task(drive())
    for _ in range(500):
        if task.done():
            break
        await clock.advance(0.01)
    await task

    assert waits, "the loop should have retried at least once"
    assert finished_at, "the loop should have stopped on the budget, not run out"
    elapsed = finished_at[0] - deadline.started_at
    assert elapsed <= total
    # And the attempts plus waits account for that elapsed time exactly: no
    # clock was reset and no budget was double counted.
    assert elapsed == pytest.approx(sum(waits) + sum(costs))
    assert clock.pending_sleepers == 0


# ----------------------------------------------------------------- validate()


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_attempts": 0}, "max_attempts"),
        ({"max_attempts": -3}, "max_attempts"),
        ({"max_attempts": 2.0}, "max_attempts"),
        ({"max_attempts": True}, "max_attempts"),      # True is an int; a typo, not 1
        ({"base_delay": -0.1}, "base_delay must not be negative"),
        ({"base_delay": 0.0}, "base_delay must be positive"),
        ({"max_delay": -1.0}, "max_delay must not be negative"),
        ({"min_attempt_time": -0.01}, "min_attempt_time must not be negative"),
        ({"base_delay": 1.0, "max_delay": 0.5}, "below base_delay"),
        ({"base_delay": float("nan")}, "base_delay must be finite"),
        ({"max_delay": float("inf")}, "max_delay must be finite"),
    ],
)
def test_validate_rejects_nonsense(kwargs: dict[str, object], match: str):
    with pytest.raises(ValueError, match=match):
        RetryPolicy(**kwargs).validate()  # type: ignore[arg-type]


def test_validate_accepts_the_defaults_and_returns_the_policy():
    policy = RetryPolicy()
    assert policy.validate() is policy
    assert RetryPolicy(min_attempt_time=0.0).validate().min_attempt_time == 0.0


def test_the_policy_is_frozen_so_it_cannot_change_mid_request():
    policy = RetryPolicy().validate()
    with pytest.raises((AttributeError, TypeError)):
        policy.max_attempts = 9  # type: ignore[misc]

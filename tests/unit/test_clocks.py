"""Clock and deadline tests.

Every timing assertion here runs on a ManualClock, so the suite proves timeout
behaviour without ever waiting. A test that demonstrates a 30-second deadline
by taking 30 seconds is a test that gets marked skip within a month.
"""

from __future__ import annotations

import asyncio

import pytest

from llmgw import errors as E
from llmgw.clocks import (
    Budgets,
    Deadline,
    ManualClock,
    StallClock,
    SystemClock,
    phase,
)

# ------------------------------------------------------------- ManualClock


async def test_manual_clock_does_not_move_on_its_own():
    clock = ManualClock(start=100.0)
    assert clock.now() == 100.0
    await asyncio.sleep(0)
    assert clock.now() == 100.0


async def test_manual_clock_wakes_sleepers_in_order():
    clock = ManualClock()
    order: list[str] = []

    async def sleeper(name: str, delay: float) -> None:
        await clock.sleep(delay)
        order.append(name)

    tasks = [
        asyncio.create_task(sleeper("c", 3.0)),
        asyncio.create_task(sleeper("a", 1.0)),
        asyncio.create_task(sleeper("b", 2.0)),
    ]
    await clock.advance(5.0)
    await asyncio.gather(*tasks)
    assert order == ["a", "b", "c"]


async def test_manual_clock_sees_a_truthful_time_when_a_sleeper_re_sleeps():
    """advance() steps to each due wake rather than jumping to the target, so
    a task that sleeps again observes the time it actually woke at -- not the
    end of the advance. Getting this wrong makes retry-backoff tests measure
    a clock that teleported."""
    clock = ManualClock(start=0.0)
    seen: list[float] = []

    async def chain() -> None:
        await clock.sleep(1.0)
        seen.append(clock.now())
        await clock.sleep(1.0)
        seen.append(clock.now())

    task = asyncio.create_task(chain())
    await clock.advance(5.0)
    await task
    assert seen == [1.0, 2.0]


async def test_manual_clock_timeout_fires():
    clock = ManualClock()
    fired = False
    async def body() -> None:
        nonlocal fired
        try:
            async with clock.timeout(1.0):
                await clock.sleep(10.0)
        except TimeoutError:
            fired = True

    task = asyncio.create_task(body())
    await clock.advance(2.0)
    await task
    assert fired


async def test_manual_clock_timeout_does_not_fire_early():
    clock = ManualClock()
    async with clock.timeout(5.0):
        await clock.sleep(0)
    assert clock.pending_sleepers == 0


async def test_manual_clock_timeout_does_not_swallow_a_real_cancellation():
    """The subtle one. This context manager cancels the task it guards, so on
    the way out it must tell 'I fired' apart from 'someone cancelled this
    request'. Backwards, and every client disconnect becomes a fake timeout --
    the exact misclassification the taxonomy exists to prevent."""
    clock = ManualClock()
    outcome: list[str] = []

    async def body() -> None:
        try:
            async with clock.timeout(100.0):
                await clock.sleep(50.0)
        except TimeoutError:
            outcome.append("timeout")
        except asyncio.CancelledError:
            outcome.append("cancelled")
            raise

    task = asyncio.create_task(body())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert outcome == ["cancelled"]


# ---------------------------------------------------------------- Deadline


def test_remaining_counts_down():
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=10.0)
    assert dl.remaining() == 10.0
    clock._now = 4.0
    assert dl.remaining() == 6.0
    assert dl.elapsed() == 4.0


def test_slice_clamps_a_phase_budget_to_what_is_left():
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=10.0)
    assert dl.slice(3.0) == 3.0        # budget is the binding constraint
    clock._now = 8.5
    assert dl.slice(3.0) == 1.5        # the deadline is
    assert dl.slice(None) == 1.5       # no budget: whatever is left


def test_total_deadline_never_resets_across_attempts():
    """The headline guarantee, asserted as arithmetic.

    Three attempts, each asking for a 20s first-event budget, inside a 30s
    total. Naive per-attempt timeouts allow 60s. Here the third attempt is
    handed 0s and the deadline raises instead.
    """
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=30.0)
    budget = 20.0

    granted = []
    granted.append(dl.slice(budget))   # 20.0
    clock._now += granted[-1]
    granted.append(dl.slice(budget))   # only 10.0 left
    clock._now += granted[-1]

    assert granted == [20.0, 10.0]
    assert sum(granted) == 30.0
    assert dl.expired
    with pytest.raises(E.TotalDeadlineExceeded):
        dl.slice(budget)


def test_check_raises_with_context_attached():
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=1.0)
    clock._now = 2.0
    with pytest.raises(E.TotalDeadlineExceeded) as ei:
        dl.check(provider="anthropic", model="haiku")
    assert ei.value.provider == "anthropic"
    assert ei.value.try_next is False


def test_check_is_silent_while_time_remains():
    dl = Deadline(ManualClock(start=0.0), total=1.0)
    dl.check()


# ------------------------------------------------------------------ phase


async def test_phase_converts_a_breach_into_the_right_taxonomy_class():
    """A generic TimeoutError would erase the distinction between 'never
    connected' and 'connected then went quiet' -- and those have opposite
    retry dispositions."""
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=100.0)

    async def body() -> None:
        async with phase(dl, 1.0, on_timeout=E.ConnectTimeout, provider="p"):
            await clock.sleep(50.0)

    task = asyncio.create_task(body())
    await clock.advance(2.0)
    with pytest.raises(E.ConnectTimeout) as ei:
        await task
    assert ei.value.provider == "p"
    assert ei.value.retry_same is True


async def test_phase_reports_the_total_deadline_when_that_is_what_expired():
    """A phase that runs out because the WHOLE request ran out is not a
    first-event timeout: it is not retryable anywhere, and calling it a
    FirstEventTimeout would invite a fallback there is no time for."""
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=1.0)

    async def body() -> None:
        async with phase(dl, 20.0, on_timeout=E.FirstEventTimeout, provider="p"):
            await clock.sleep(50.0)

    task = asyncio.create_task(body())
    await clock.advance(2.0)
    with pytest.raises(E.TotalDeadlineExceeded):
        await task


async def test_phase_refuses_to_start_when_the_deadline_is_already_gone():
    """A doomed attempt must not open a socket to discover it is doomed."""
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=1.0)
    clock._now = 5.0
    with pytest.raises(E.TotalDeadlineExceeded):
        async with phase(dl, 0.5, on_timeout=E.ConnectTimeout):
            pytest.fail("body must not run")


# ------------------------------------------------------------- StallClock


def test_content_resets_the_progress_clock():
    clock = ManualClock(start=0.0)
    sc = StallClock(Budgets(total=60, progress=5.0), clock=clock)
    clock._now = 4.0
    assert sc.remaining() == pytest.approx(1.0)
    sc.mark_progress()
    assert sc.remaining() == pytest.approx(5.0)


def test_a_heartbeat_does_not_reset_progress():
    """ping_does_not_reset_progress, in miniature.

    A provider stuck in a bad state can heartbeat politely forever. Liveness
    is not progress, and a gateway that accepts pings as evidence of work will
    hold a dead stream open until the total deadline.
    """
    clock = ManualClock(start=0.0)
    sc = StallClock(Budgets(total=60, progress=5.0), clock=clock)
    for _ in range(10):
        clock._now += 1.0
        sc.mark_liveness()
    assert sc.stalled


def test_liveness_budget_is_the_looser_of_the_two_when_enabled():
    clock = ManualClock(start=0.0)
    sc = StallClock(Budgets(total=60, progress=5.0, liveness=20.0), clock=clock)
    clock._now = 6.0
    assert sc.stalled                       # progress budget blown
    sc.mark_progress()
    clock._now = 9.0
    assert not sc.stalled


# --------------------------------------------------------------- Budgets


def test_budgets_reject_a_phase_that_can_never_fire():
    """A first-event budget larger than the total is decorative, and a
    decorative timeout is one nobody realises is not protecting them."""
    with pytest.raises(ValueError, match="can never fire"):
        Budgets(total=5.0, first_event=20.0).validate()


def test_budgets_reject_a_liveness_budget_tighter_than_progress():
    with pytest.raises(ValueError, match="liveness"):
        Budgets(total=60.0, progress=10.0, liveness=5.0).validate()


def test_sensible_budgets_validate():
    assert Budgets(total=60.0, connect=2.0, first_event=20.0, progress=15.0).validate()


def test_system_clock_is_monotonic_not_wall_clock():
    """Wall clock jumps: NTP steps it, VMs pause, leap seconds smear. A
    deadline computed from it can expire in the past."""
    import time
    a = SystemClock().now()
    b = time.monotonic()
    assert abs(a - b) < 0.5

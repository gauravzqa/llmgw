"""Retry timing. One budget per request, and its most useful answer is "no".

`clocks.py` makes a deadline that cannot be reset. This module is what spends
it. The two failure modes it exists to prevent are the ones that turn a
provider brownout into a provider outage, and neither is fixed by tuning a
number:

1. **Amplification.** Retries multiply across layers, they do not add. A
   client SDK that retries 3x calling a gateway that retries 3x calling
   another gateway that retries 3x is 27 upstream attempts per user request
   at the moment everything is failing -- against a provider that is failing
   *because* it is overloaded. So the budget here counts TOTAL attempts for
   the whole request, across every target, and `enabled=False` exists so that
   an outer gateway can take ownership outright (CONTRACTS.md C5).

2. **Synchronisation.** Backoff without randomness lowers instantaneous load
   and does nothing whatsoever to spread it: a fleet that failed together
   sleeps the same computed delay and retries together, so from the provider's
   side the original spike simply arrives again on schedule. Full jitter is
   the fix, and the reason it is `uniform(0, ceiling)` rather than something
   with a nicer mean is in `delay_for`'s docstring.

--------------------------------------------------------------------------
The shape of the object
--------------------------------------------------------------------------

`RetryPolicy` is frozen configuration -- it arrives from a policy snapshot and
must not change mid-request, because a request routed under one policy and
timed under another is a bug nobody can reproduce.

`RetryBudget` is per-request mutable state: how many attempts have been spent,
and the one `Deadline` they are all spending. It is created once at ingress
and threaded through every attempt, which is the whole point. A budget
constructed inside the retry loop is the classic re-basing bug -- the deadline
becomes per-attempt, and a three-target fallback chain holds the client for
three times the promised total.

The canonical loop::

    budget = RetryBudget(policy, deadline, clock=clock)
    attempt = 0
    while True:
        budget.record_attempt()
        try:
            return await send(target)
        except GatewayError as err:
            d = decide(err, committed=pump.committed)
            if not (d.retry_same or d.try_next):
                raise
            await budget.wait(err, attempt=attempt)   # may raise
            attempt += 1
            target = target if d.retry_same else plan.next()

`attempt` is the zero-based index of the attempt that just failed, which is
Brooker's `attempt` in the AWS backoff formulas and `attempts_used - 1` in the
loop above. Note what the budget does NOT decide: whether to go back to the
same target or on to the next one. That is `decide()`'s job, and duplicating
it here would put the commitment invariant in two places.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass

from .clocks import Clock, Deadline
from .errors import GatewayError, RetryBudgetExhausted

# One unseeded generator for the whole process. Injectable per budget so tests
# can be exact; see `RetryBudget.__init__` for why it must NOT be seeded to a
# constant in production.
_DEFAULT_RNG = random.Random()

# `2.0 ** 1024` raises OverflowError, and `base * 2.0 ** 1023` can reach inf
# for a large base. Clamping the exponent costs nothing real: `min(max_delay,
# ...)` has already flattened the curve tens of attempts earlier, and no sane
# `max_attempts` gets here. The clamp exists so that a caller passing a wild
# attempt index gets a capped delay rather than an OverflowError inside the
# error handler -- crashing the code that handles failures, during a failure,
# is a spectacularly bad time to find out about an unclamped exponent.
_MAX_EXPONENT = 64


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Per-workload retry configuration. Frozen: it is read, never edited.

    Defaults are deliberately timid. `max_attempts=2` means one retry, and
    that is the honest default for a gateway that also does fallback: the
    budget below is shared with the fallback chain, so a generous default here
    silently buys attempts that the routing layer was going to spend anyway.
    """

    max_attempts: int = 2
    """How many attempts this budget will SPACE for one request -- which is a
    bound on REPETITION, not on total upstream requests.

    The distinction matters and it is the one people get wrong, so:

        total upstream requests  <=  len(plan.targets) + (max_attempts - 1)

    Not `max_attempts`. Breadth belongs to the execution plan, which is finite
    and ordered; the budget only refuses repeats. With the defaults -- two
    targets, `max_attempts=2` -- the bound is 3.

    A true hard cap across both dimensions is the obvious-looking alternative
    and it is worse. Repetition happens FIRST, so a cap of 2 against a
    three-target plan spends both attempts re-asking the sick candidate and
    never reaches the healthy incumbent: the retry starves the fallback, and
    the request fails against a provider that would have answered. A bound
    that trades availability for a tidier number is not a bound worth having.

    So: `max_attempts` is the repetition allowance, `len(targets)` is the
    breadth, and the sum above is the amplification figure to quote."""

    """TOTAL upstream attempts for the whole request, across every target.

    Not per target. Three targets with "two retries each" is six requests to a
    provider that is failing because it is overloaded -- and the per-target
    reading is the one people implement by accident, because the retry loop
    and the fallback loop are usually written by different hands a month
    apart. Making the budget an object that outlives both loops is what stops
    that from being a matter of remembering.
    """

    base_delay: float = 0.1
    """The ceiling for the first retry's jitter window, in seconds."""

    max_delay: float = 2.0
    """Cap on the jitter window. Applied BEFORE the power, see `delay_for`."""

    min_attempt_time: float = 0.05
    """The least time an attempt could plausibly need to be worth starting.

    A reservation, not a timeout. It is what makes "this retry cannot pay off"
    a computable statement: if the delay plus this does not fit in what is
    left, the attempt would be cut off by the total deadline before it could
    do anything, so making it is strictly worse than returning the real error
    now. A single constant is a crude estimate; the better version is the
    observed p50 time-to-first-byte for that target, which needs a feedback
    loop this object does not have.
    """

    respect_retry_after: bool = True
    """Honour `err.retry_after` as a floor. Off only for a provider you have
    measured to be lying, and that decision belongs in config, not in code."""

    enabled: bool = True
    """False disables retries entirely. This is CONTRACTS.md C5's mechanism:
    `X-Gw-No-Retry: 1` on the inbound request sets it False so that an outer
    gateway can own the retry chain. The answer to "who retries" is never
    "everyone, a bit" -- it is one named layer, and the header is how the
    caller names it."""

    def validate(self) -> RetryPolicy:
        """Reject configurations that cannot mean anything, loudly, at load.

        A misconfigured retry policy does not fail at load time by itself; it
        fails at 3am under load, as an amplification incident, in a code path
        that only runs when something else is already broken. So the checks
        are here and they are strict.
        """
        if type(self.max_attempts) is not int or self.max_attempts < 1:
            # `type(...) is not int` rather than isinstance: `True` is an int
            # in Python, and `max_attempts=True` is a typo, not "one attempt".
            raise ValueError("max_attempts must be an int >= 1")
        for name in ("base_delay", "max_delay", "min_attempt_time"):
            value = float(getattr(self, name))
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
            if value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.base_delay <= 0:
            # A zero base makes every jitter window zero, so the backoff is
            # decorative -- and a decorative backoff is one nobody realises is
            # not protecting them. Same argument as Budgets.validate().
            raise ValueError("base_delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError(
                f"max_delay={self.max_delay} is below base_delay={self.base_delay}; "
                "the cap would apply from the very first retry"
            )
        return self

    def floor_for(self, err: GatewayError) -> float | None:
        """The delay the provider itself asked for, or None if it asked for none.

        Separated from `delay_for` so that "the provider said 0" and "the
        provider said nothing" are distinguishable, which they are not once
        both have been folded into a `max()`. They mean different things --
        `Retry-After: 0` is "come back now", an absent header is "we have no
        opinion" -- and the difference is exactly what a truthiness test
        (`if err.retry_after:`) destroys. Keeping it a separate, testable
        function is how that bug stays fixed.

        Hostile input is clamped rather than raised on. A negative or NaN
        `retry_after` is a malformed provider header, and we are already
        inside the handler for a provider that is misbehaving; killing the
        request over it would be the parser doing more damage than the fault.
        """
        if not self.respect_retry_after:
            return None
        after = err.retry_after
        if after is None:
            return None
        value = float(after)
        if not math.isfinite(value):
            return None
        return max(0.0, value)


class RetryBudget:
    """Per-request retry state: attempts spent, and the deadline they spend.

    One per request, created at ingress and threaded through every attempt to
    every target. Two invariants live here rather than in the caller:

    * the total deadline is never exceeded by *waiting* -- a delay that cannot
      be followed by a useful attempt is refused instead of slept;
    * attempts are counted once for the request, not once per target.

    Both are properties of an object that outlives the loop. Written as rules
    the loop has to remember, they are rules the second loop forgets.
    """

    __slots__ = ("_policy", "_deadline", "_clock", "_rng", "_attempts_used")

    def __init__(
        self,
        policy: RetryPolicy,
        deadline: Deadline,
        *,
        clock: Clock,
        rng: random.Random | None = None,
    ) -> None:
        """`rng` is injected so tests are exact, and defaults to a per-process
        generator that is deliberately NOT seeded.

        A shared, constant-seeded rng across requests would be worse than no
        jitter at all in the one case that matters. Jitter exists to
        decorrelate clients that failed at the same instant; a fixed seed
        makes every process draw the same sequence, so a fleet that started
        together and failed together re-converges on precisely the delays
        jitter was added to spread. The randomness has to be independent per
        request to do its job, and a seed is how you take that away without
        noticing -- the delays still *look* random in a log.
        """
        self._policy = policy
        self._deadline = deadline
        self._clock = clock
        self._rng = rng if rng is not None else _DEFAULT_RNG
        self._attempts_used = 0

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    def may_attempt(self) -> bool:
        """Is there repetition allowance left? Observability, not a gate.

        Deliberately NOT used by the executor as a precondition for starting
        an attempt. It was, briefly, and the resulting behaviour was worse
        than the problem: because repetition happens before fallback, gating
        every attempt on this makes a tight budget spend itself re-asking the
        failing target and never reach the healthy one.

        `delay_for` is the gate for repeats; the plan is the bound on breadth.
        This method exists so a caller can *report* the remaining allowance --
        in an attempt log, or in a decision about whether hedging is worth it
        (P8) -- without inferring it from an exception.
        """
        return self._attempts_used < self._policy.max_attempts

    @property
    def attempts_remaining(self) -> int:
        return max(0, self._policy.max_attempts - self._attempts_used)

    @property
    def attempts_used(self) -> int:
        """Upstream attempts started so far, across all targets."""
        return self._attempts_used

    def record_attempt(self) -> None:
        """Call once immediately before every upstream attempt.

        Before, not after: an attempt that dies without returning still cost
        the provider a request, and a counter incremented on the success path
        is a counter that does not count the failures it exists to bound.
        """
        self._attempts_used += 1

    # ------------------------------------------------------------------ delay

    def delay_for(self, err: GatewayError, *, attempt: int) -> float:
        """Seconds to wait before the next attempt.

        Raises `RetryBudgetExhausted` when waiting cannot pay off: retries are
        disabled, the attempt budget is spent, or the delay plus the minimum
        useful attempt does not fit in the time that is left.

        Raises `ValueError` when the caller asks for a delay for an error that
        is not retryable anywhere -- see "requirement 6" below.

        ------------------------------------------------------------------
        Full jitter, and why the mean is not the point
        ------------------------------------------------------------------

            delay = uniform(0, min(max_delay, base_delay * 2**attempt))

        Uniform over the *whole* window, from zero. Not "equal jitter"
        (`temp/2 + uniform(0, temp/2)`), which is twice as concentrated. Not a
        fixed backoff with a little noise sprinkled on, which is a fixed
        backoff.

        The instinct is that a wider window with the same mean cannot matter
        much. It matters entirely, because the quantity that hurts a provider
        is not the average delay -- it is how many of your clients arrive in
        the same 10 ms. Brooker's simulation, 1000 clients all failing at
        t=0, worst 10 ms bucket per attempt:

            none         : 1000, 1000, 1000
            equal jitter :  224,  114,   62
            full jitter  :  120,   62,   37

        Without jitter the fleet retries in the same slot three times running,
        which from the provider's side is the original spike arriving twice
        more -- while it is still down from the first one. That is the
        mechanism by which a brownout becomes an outage: the recovery attempt
        is indistinguishable from the load that caused the failure. Spreading
        the arrivals is the entire job; the mean delay is a side effect.

        The cap is applied to the ceiling, before the draw, so `max_delay` is
        a bound on the *window* and not on a sample. Capping the sample
        instead would pile every long draw onto exactly `max_delay`, which
        re-synchronises the fleet at the cap -- the bug reintroducing itself
        at the one point where the most clients are.

        ------------------------------------------------------------------
        Requirement 6: an error that is not retryable at all
        ------------------------------------------------------------------

        This method deliberately does not decide *where* the next attempt
        goes; `decide()` owns that, and an invariant written twice eventually
        disagrees with itself. But it does insist that a next attempt is
        possible somewhere: an error with neither `retry_same` nor `try_next`
        has no next attempt to be delayed, and asking for one is a caller bug.

        It raises `ValueError`, not `RetryBudgetExhausted`, and the difference
        is the point. `RetryBudgetExhausted` is a legitimate operational
        outcome: it is counted, alerted on, and tuned against. A caller that
        asks the budget to space out a `ContentFiltered` is broken, and
        dressing that up as a budget outcome makes it invisible in exactly the
        metric someone would use to find it -- the retry budget would appear
        to be doing its job at a rate that has nothing to do with retries.

        Note the asymmetry that follows: an error that is `try_next` but not
        `retry_same` -- a 400, a `FirstEventTimeout` -- DOES get a delay. The
        budget spaces attempts, whichever target they go to. A fallback that
        every failing request takes at the same instant is a retry storm
        pointed at a different provider, and it deserves the same jitter.
        """
        policy = self._policy
        if attempt < 0:
            raise ValueError(f"attempt must be >= 0, got {attempt}")
        if not (err.retry_same or err.try_next):
            raise ValueError(
                f"{err.code} is not retryable at any target "
                f"(retry_same={err.retry_same}, try_next={err.try_next}); "
                "the caller must consult decide() before asking for a delay"
            )
        # The caller-bug checks come first, on purpose. A deployment running
        # with retries disabled must not be a deployment where those bugs stop
        # being reported.
        if not policy.enabled:
            raise self._exhausted(
                "retries are disabled for this request (X-Gw-No-Retry)", err, attempt
            )

        # Two counters must agree: `record_attempt()` calls and the caller's
        # `attempt` index. When they do not, trust the larger. The failure mode
        # of trusting the smaller is unbounded retries, which is the incident
        # this file exists to prevent; the failure mode of trusting the larger
        # is one retry fewer than configured.
        used = max(self._attempts_used, attempt + 1)
        if used >= policy.max_attempts:
            raise self._exhausted(
                f"attempt budget spent ({used}/{policy.max_attempts} attempts)",
                err,
                attempt,
            )

        window = policy.base_delay * 2.0 ** min(attempt, _MAX_EXPONENT)
        ceiling = min(policy.max_delay, window)
        delay = self._rng.uniform(0.0, ceiling)

        floor = policy.floor_for(err)
        if floor is not None:
            # `max`, never `min` and never a replacement. A provider that told
            # you when to come back has handed you information the backoff
            # curve is only guessing at, and undercutting it with a low jitter
            # draw is the subtle form of ignoring it: the request arrives
            # early, is refused again, and has spent budget to learn nothing.
            # Anthropic's own header table is blunt about it -- "earlier
            # retries will fail".
            delay = max(delay, floor)

        remaining = self._deadline.remaining()
        if delay + policy.min_attempt_time >= remaining:
            # Refuse rather than sleep into a guaranteed breach. Sleeping here
            # buys nothing: the attempt it enables would be cut off by the
            # total deadline, so the client waits the delay and then receives
            # the same error it could have had immediately. Burning the
            # client's remaining latency to reach an identical conclusion is
            # strictly worse than reaching it now.
            raise self._exhausted(
                f"a {delay:.3f}s wait plus a {policy.min_attempt_time:.3f}s attempt "
                f"does not fit in {remaining:.3f}s remaining",
                err,
                attempt,
            )
        return delay

    async def wait(self, err: GatewayError, *, attempt: int) -> float:
        """`delay_for()` then sleep it on the injected clock. Returns the delay.

        The sleep goes through `Clock.sleep` and never `asyncio.sleep`. That
        is not fastidiousness: a retry test that proves its backoff by
        actually backing off takes seconds, seconds-long suites get marked
        skip, and skipped suites are precisely where timing bugs live. On a
        `ManualClock` this whole module is exercised in microseconds.
        """
        delay = self.delay_for(err, attempt=attempt)
        await self._clock.sleep(delay)
        return delay

    # ----------------------------------------------------------------- private

    def _exhausted(
        self, why: str, err: GatewayError, attempt: int
    ) -> RetryBudgetExhausted:
        """Build the refusal, carrying the real error as its cause.

        The executor is expected to surface `err`, not this -- a client should
        see the provider's 503, not our accounting. This class is the control
        signal that says stop, and it chains the cause so that a postmortem
        can still see what we stopped retrying.

        Note what it is deliberately NOT: `TotalDeadlineExceeded`. Even when
        the refusal is because no time is left, this budget does not know
        where the time went. `TotalDeadlineExceeded` is now `GATEWAY` / `NEUTRAL`
        for exactly this reason, so the two classes agree on attribution; they
        stay distinct because they name different things -- the budget saying
        stop versus the deadline saying it -- and a postmortem needs to know
        which. `phase()` makes the same call for the same reason: when
        the time was spent somewhere this code cannot see, the honest report
        is the unattributed one.
        """
        return RetryBudgetExhausted(
            f"no further attempt: {why}",
            provider=err.provider,
            model=err.model,
            attempt=attempt,
            cause=err,
        )

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"<RetryBudget {self._attempts_used}/{self._policy.max_attempts} attempts "
            f"remaining={self._deadline.remaining():.3f}s>"
        )

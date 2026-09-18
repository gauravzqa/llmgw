"""The attempt loop. Every other module in this package meets here.

`errors.py` says what a failure *is*. `policy.py` says which targets we are
allowed to try and in what order. `retry.py` says whether a repetition can pay
off. `clocks.py` says how much time is left. `upstream.py` opens the socket and
`pump.py` owns the one fact that overrules all of them -- whether a byte has
reached the client. This file is the loop that consults them in that order and
does nothing else.

Its whole discipline is negative. There is no policy in here. Every branch is
a field on a `Disposition` returned by `errors.decide()`, and the file is
written so that a reader can grep it for the reliability rules and find them
*absent* -- which is the point. A gateway with the retry rules written in the
executor has them written twice, and two copies of an invariant are two
invariants that will eventually disagree about a class somebody added in six
months.

--------------------------------------------------------------------------
The two boundaries, and the one this file moves
--------------------------------------------------------------------------

CONTRACTS.md C1 names two lines an HTTP response crosses:

    status commitment    `http.response.start` is out. The status is fixed.
    content commitment   a body byte was written. Everything is fixed.

P2 sent the status as soon as `Upstream.open()` returned, which forfeited
fallback for the most common provider failure there is: headers arrived and
then nothing did. P3's decision is to hold the status until the upstream
hands us its first body byte -- the instant we were going to write to the
client anyway.

`sink_factory` is that decision expressed as a type. The executor is handed a
callable it can only use once, and using it is what sends
`http.response.start`. Before the call, every failure is pre-commitment and
the plan is still open. After it, there is no plan: a second status is not a
thing HTTP has.

Because the status is held until the first body byte, the two boundaries
*coincide*, and this file needs one flag rather than two. `Pump.committed`
cannot be true while `_Commitment.started` is false, for the structural
reason that the pump does not exist yet -- it is constructed from the sink
that the call produced. That coincidence is the whole payoff of the P3
decision, and it is why the commitment state here is a single monotone
boolean that only one line in the package can set.

--------------------------------------------------------------------------
Why "never a second candidate" is not a rule
--------------------------------------------------------------------------

The loop walks `plan.targets`, which is an ordered, finite tuple built by
`policy.py` -- candidate first, incumbent second, never more than two. There
is no "pick another target" function here because there is no set to pick
from; `try_next` advances an index into a list that runs out. A loop that
*searched* for a next candidate would need a rule saying when to stop, and a
rule is a thing that gets an exception added to it. An index into a tuple does
not.

--------------------------------------------------------------------------
What comes back
--------------------------------------------------------------------------

`execute()` returns an `ExecutionResult` and does not raise `GatewayError`.
That is deliberate: the caller needs `attempts` for `X-Gw-Attempts`, `pump`
for the usage C3 says an interrupted stream still owes, and `served_by` for
`X-Gw-Served-By` -- and it needs all three *especially* on the failure path,
which is exactly the path an exception would throw them away on. Only
`CancelledError` escapes, because cancellation is not a result (C8).

Except that it IS a result to accounting. A client hanging up mid-answer is
the most common interruption there is, and C3 says the tokens it generated
are billed -- so the one path that cannot return a result is the one that
most needs to deliver one. `on_finish` is how: a callback the loop invokes
from a `finally` with the terminal `ExecutionResult`, on every exit including
cancellation, after which the `CancelledError` keeps propagating. See
`execute()` for why a callback rather than a payload on the exception.

--------------------------------------------------------------------------
The attempt gate (P4)
--------------------------------------------------------------------------

Two things now stand between "the loop chose this target" and "a socket was
opened to it", and both live inside `_attempt()` because that is the scope
that holds exactly one `Target` and cannot see the plan:

    1. the circuit breaker for the target, `breakers.for_key(...).acquire()`.
       OPEN -> `BreakerOpen`, before any socket. `try_next`, so the loop
       moves on; NEUTRAL, so the breaker never counts its own refusals.
    2. the provider-key limiter, `limiter.acquire(credential, cap)`.
       Full -> `ProviderKeyExhausted`, before any socket. Also `try_next`.

Neither is a policy: the executor obeys `decide()` for these exactly as for
a 503. What IS new is bookkeeping the loop has to get right. A refusal at
the gate is not an upstream attempt -- nothing was sent, no provider was
measured -- so it is recorded as a `Refusal` and not an `AttemptRecord`,
does not spend the retry budget, and does not count toward `X-Gw-Attempts`.
The pre-flight `deadline.check()` set that precedent and the reasoning is
the same: an amplification metric inflated by requests that never left the
building is wrong on precisely the incident it exists to measure.

The breaker ticket is minted in `_attempt()` and settled by the LOOP, with
the `Disposition` the loop already computed. Settling in `_attempt()` would
mean a second `decide()` per failure, and two calls to the one authority on
"what next" is the beginning of two authorities. See `_settle` for the
ticket's four exits and `credential_health_key` for why an attempt holds two
tickets rather than one.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any

from .admission import Permit, ProviderKeyLimiter
from .breaker import BreakerRegistry, Key, Ticket
from .catalog import Target
from .clocks import Budgets, Clock, Deadline
from .errors import (
    ClientDisconnected,
    Disposition,
    GatewayError,
    Health,
    IncompleteStream,
    NoTargetsAvailable,
    Outcome,
    ResponseTooLarge,
    RetryBudgetExhausted,
    decide,
)
from .framing import assert_upstream_framing
from .metrics import normalize_stop_reason
from .policy import ExecutionPlan
from .pump import Pump, PumpResult, Sink
from .retry import RetryBudget, RetryPolicy
from .surfaces.base import Surface, Usage, surface_framing, usage_from_body
from .upstream import Upstream, UpstreamRequest, UpstreamStream

log = logging.getLogger(__name__)

SinkFactory = Callable[[UpstreamStream], Awaitable[Sink]]
"""Start the client's response and return the thing that writes its body.

Awaited exactly once per execution, at the moment the upstream has produced a
byte we are about to forward. In the server this closure sends
`http.response.start`; in a test it hands back a list. Either way, calling it
is the status commitment, and this module treats the call itself -- not its
return -- as the point of no return.
"""

OnFinish = Callable[["ExecutionResult"], None]
"""The accounting hook. Called exactly once per `execute()`, on every exit.

Synchronous on purpose. It runs inside a `finally` that may be unwinding a
`CancelledError`, and a coroutine there would be a suspension point -- the one
place a SECOND cancellation could land and take the record with it. A plain
call has no such point: once the `finally` is entered, nothing can interrupt
it before the hook has the result. Anything the hook wants to await, it
schedules.
"""

NO_RETRIES = RetryPolicy(max_attempts=1, enabled=False)
"""What `retry_policy=None` means: no REPETITION of any target, ever.

`enabled=False` rather than a policy with a tiny delay, because those are
different statements. C5's `X-Gw-No-Retry: 1` says another layer owns retries;
a caller who passes no policy at all has said nothing, and the safe reading of
silence in a component that can amplify load is zero.

`max_attempts=1` here is a placeholder, NOT the effective cap. `execute()`
raises it to `len(plan.targets)` before use, because "no retries" must not
silently mean "no fallback" -- those are the two dimensions C5 keeps apart,
and collapsing them would make `X-Gw-No-Retry: 1` quietly disable the
redundancy the workload was configured for. Breadth is the plan's business;
repetition is the budget's.
"""

DEFAULT_BUFFER_BYTES = 256 * 1024
"""Per-stream pump buffer. Forwarded to `Pump`, never assumed by it.

These two constants exist because they were briefly NOT parameters, and the
consequence is worth recording: a deployment that configured a 32 KiB buffer
and an 8 KiB frame bound got 256 KiB and 1 MiB instead, silently, because the
only path from config to `Pump` ran through a constructor nobody had told
about the request. Nothing failed. `buffer_bytes` is the per-stream memory the
scale tier multiplies by N; `max_frame_bytes` is the only thing between one
provider's oversized frame and this process's RSS. A limit that silently
reverts to a default is worse than no limit, because the dashboard says it is
enforced.
"""

DEFAULT_MAX_FRAME_BYTES = 1 << 20
"""One SSE frame's bound. See DEFAULT_BUFFER_BYTES."""

DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
"""Bound on a non-streaming upstream body. Matches `ServerConfig`'s default.

A bound and not a guess: a provider having a bad day can answer at length, and
a gateway that buffers whatever arrives turns one provider incident into its
own memory incident at the moment every request is doing it at once.
"""


# ==========================================================================
# Records
# ==========================================================================


@dataclass(slots=True)
class AttemptRecord:
    """One upstream attempt, successful or not.

    Written for every attempt rather than only the interesting ones, because
    the two numbers that matter downstream are both counts of the boring ones:
    `X-Gw-Attempts` is `len(attempts)`, and `llmgw_attempts_total{result=...}`
    is a fan-out over `outcome`. A record list that skipped successes would
    make the amplification factor -- attempts divided by requests, the single
    number that says whether this gateway is making an incident worse --
    uncomputable.
    """

    target: Target
    started_at: float
    ended_at: float
    outcome: str
    """`"success"`, `"canceled"`, or the failing error's `code`. A closed
    vocabulary: every other value is a member of `errors.ERROR_CODES`, which
    is what keeps the metric's label cardinality bounded.

    `"canceled"` (`Outcome.CANCELED.value`) is the attempt that was in flight
    when the execution was cancelled. It is recorded rather than dropped
    because a request WAS sent to that target, and `X-Gw-Attempts` -- taken
    at commitment from the count of opens -- already includes it; an attempt
    list that left it out would disagree with the header the client holds and
    under-count the amplification metric on exactly the path that runs most.
    Its `error` is `None`: the executor does not know WHY it was cancelled,
    and inventing a `ClientDisconnected` it never observed would be the
    executor forming an opinion (C8)."""

    status: int | None
    """The upstream's HTTP status, when there was one. `None` for a failure
    that never got a status line -- a connect timeout has no status, and
    reporting `0` or `502` for it would invent a response the provider never
    sent."""

    error: GatewayError | None
    committed: bool
    """Whether the client's response had been started when this attempt ended.

    Per attempt rather than per execution so the record can be read on its own:
    an attempt that failed after commitment truncated a real answer, and one
    that failed before it cost the client nothing but time.
    """


@dataclass(slots=True)
class Refusal:
    """A target the gate turned away before a socket existed. NOT an attempt.

    Kept on its own list rather than folded into `attempts` because the two
    answer different questions. `attempts` is what this request cost the
    providers -- the amplification numerator. A refusal cost them nothing:
    the circuit was open, or the shared credential was full, and the loop
    moved on in microseconds. Folding it in would report a request that was
    served by the incumbent after the candidate's circuit refused it as two
    attempts, and an operator reading `X-Gw-Attempts: 2` would conclude the
    candidate had been asked. It had not.

    `error` is the `BreakerOpen` or `ProviderKeyExhausted` itself, so a
    caller can tell which gate refused and -- for the breaker -- whether the
    circuit was OPEN (`retry_after` set) or HALF_OPEN with its probe out.
    """

    target: Target
    at: float
    error: GatewayError


@dataclass(slots=True)
class ExecutionResult:
    """The terminal facts about one request's trip through the plan."""

    plan: ExecutionPlan
    attempts: list[AttemptRecord]
    served_by: Target | None
    """The target whose bytes the client got, or None if nobody answered."""

    pump: PumpResult | None
    """Present whenever a stream was pumped, INCLUDING when it failed.

    C3: an interrupted stream bills the tokens it generated. A result that
    carried usage only on the happy path would make every interrupted request
    bill zero, which is the failure mode that looks like a discount until
    someone reconciles the provider's invoice.
    """

    committed: bool
    outcome: Outcome
    """`CANCELED` is the one value `execute()` never RETURNS. It reaches the
    caller only through `on_finish`, because the `CancelledError` that caused
    it is still propagating (C8). On that result `attempts` is everything that
    had happened, the in-flight attempt included; `served_by` is the target
    whose bytes were flowing if any were; `pump` carries the partial usage."""

    error: GatewayError | None
    """`None` on success and on cancellation. Otherwise the last error from an
    attempt that actually reached an upstream -- see `_final_error`."""

    refusals: list[Refusal] = field(default_factory=list)
    """Targets the gate turned away, in plan order. Empty when no gate is
    configured. Disjoint from `attempts` by construction: a target appears in
    one list or the other per visit, never both."""


# ==========================================================================
# Commitment
# ==========================================================================


class _Commitment:
    """The single place the client's response can be started.

    This class exists to make "no fallback after the sink exists" a property
    of the code's shape rather than of a comment. Three things hold by
    construction:

    1. `Executor._attempt()` is the only caller, and it is handed ONE `Target`
       and no reference to `plan.targets`. The scope that can create a sink
       cannot iterate the plan.
    2. `started` is monotone. It is assigned `True` on the line *before* the
       await -- the same rule and the same reason as `Pump._committed`: a
       factory that raises halfway may still have put the status line on the
       wire, and a flag set afterwards would report that as recoverable.
       Nothing in this package assigns it `False`.
    3. `decide(err, committed=True)` returns `retry_same=False, try_next=False`
       for every error class, which `test_commitment_forbids_every_further_
       attempt` enumerates. Those two fields are the loop's ONLY continuation
       conditions, so once `started` is true the loop has no edge left to
       take.

    The second call raises rather than returning the previous sink. A caller
    that reached here twice has a bug that would otherwise present as a
    corrupt response body, and the loud version of that is much cheaper.
    """

    __slots__ = ("_factory", "_started", "_target")

    def __init__(self, factory: SinkFactory) -> None:
        self._factory = factory
        self._started = False
        self._target: Target | None = None

    @property
    def started(self) -> bool:
        return self._started

    @property
    def served(self) -> Target | None:
        """The target the client's response was started for, or None.

        This is `ExecutionResult.served_by` on the one exit that cannot read
        it off a successful attempt: cancellation. It is recorded HERE, on the
        same line and for the same reason as `started`, so that the loop can
        report who was serving without ever testing commitment itself -- a
        ternary on the flag in the executor would be the branch
        `test_the_file_never_re_derives_the_commitment_invariant` exists to
        forbid, and this attribute is what makes it unnecessary.
        """
        return self._target

    async def open(self, stream: UpstreamStream, *, target: Target | None = None) -> Sink:
        if self._started:
            raise RuntimeError(
                "the client's response has already been started; HTTP has no "
                "second status to send"
            )
        self._started = True
        self._target = target
        return await self._factory(stream)


@dataclass(slots=True)
class _AttemptState:
    """Scratch space one attempt fills in for its record.

    Separate from `AttemptRecord` because the record is built in the `except`
    clause, where the attempt's own locals are gone but the upstream status it
    learned is still needed. Without it a post-commitment failure would report
    `status=None` for a stream the client received a 200 on.

    `target` and `started_at` are here for the third place a record is built:
    the `finally` that runs on cancellation, where even the loop's own locals
    are not to be trusted -- `target` still names whatever the loop was on,
    which after a retry sleep is the target whose record was already written.
    An attempt is in flight exactly while the loop holds one of these that no
    record has been written from.
    """

    target: Target
    started_at: float
    status: int | None = None
    pump: Pump | None = None
    tickets: tuple[Ticket, ...] = ()
    """The breaker tickets `_attempt()` minted for this target: `(target,
    credential)` when a registry is configured, empty otherwise. Carried here
    rather than returned because the thing that settles them is the loop's
    `except` clause, where `_attempt()`'s frame is already gone."""

    opened: bool = False
    """Did `_attempt()` get as far as `upstream.open()`? False means the gate
    refused -- no socket, no attempt record, no budget spent. The loop reads
    this, not the error's class: the class is `decide()`'s business and
    whether a socket existed is a fact about this attempt, not a rule."""


# ==========================================================================
# The executor
# ==========================================================================


def credential_health_key(target: Target) -> Key:
    """The breaker key an authentication failure at `target` records under.

    `Target.health_key` is `(provider, model)` and that is what almost every
    failure records against. Authentication is the documented exception:
    `errors.HealthScope.CREDENTIAL` scopes a 401 to `(provider,
    "cred:<credential_id>")`, because a revoked BYOK key belongs to one
    tenant's credential and keying it on the model would let that one tenant
    open a breaker for everybody else on the provider (FAILURE-MODES row 8).

    So the key an attempt ACQUIRES on and the key its failure RECORDS on can
    differ, and `Breaker.record()` refuses a ticket from the wrong key -- a
    ticket is proof of admission by one circuit and cannot be settled on
    another. The composition that works is to hold a ticket on BOTH keys for
    the duration of the attempt: the target's, and this one. That also gives
    the credential circuit the one thing a write-only key would lack -- the
    power to REFUSE. An open `cred:` circuit means the key is known-bad, and
    the right response to that is no socket, not a fresh 401.

    The key carries NO provider, on purpose: a credential is one key, and two
    provider entries sharing that key (`openrouter` and `openrouter-toolsafe`)
    must share one auth circuit or a dead key trips at twice the threshold.
    See `GatewayError.health_key()`, which owns the spelling;
    `test_the_credential_key_agrees_with_the_taxonomy` pins the two together
    so a change to one fails a test rather than a breaker, and
    `test_two_providers_on_one_credential_share_one_auth_circuit` pins the
    consequence.
    """
    return ("cred", target.credential_key)


class Executor:
    """Runs one execution plan against one client.

    Stateless between calls: everything mutable belongs to `execute()`'s frame
    or to the objects it creates there. One instance is shared by every
    in-flight request, the same way a `Surface` is, and for the same reason --
    per-request state on a shared object is the bug that only appears under
    concurrency.

    `breakers` and `limiter` are the P4 gate and both default to `None`,
    which means NO gate: every attempt goes straight to `upstream.open()`,
    exactly as before. That is not a convenience default; it is what keeps a
    library caller who has not configured admission from silently running a
    breaker with guessed thresholds against providers it never told us about.
    """

    __slots__ = ("_upstream", "_clock", "_breakers", "_limiter")

    def __init__(
        self,
        upstream: Upstream,
        *,
        clock: Clock,
        breakers: BreakerRegistry | None = None,
        limiter: ProviderKeyLimiter | None = None,
    ) -> None:
        if breakers is not None and limiter is None:
            # Found in verification, closed at construction.
            #
            # We disable httpx's own pool timeout (`Timeout(None)` -- the
            # Deadline is the only clock), so a request that finds the
            # per-credential connection pool full does not get a `PoolTimeout`.
            # It blocks in the pool queue until our connect `phase()` fires,
            # and that surfaces as `HeadersTimeout` -- FAILURE health, PROVIDER
            # blame. With a breaker wired, that opens a circuit against a
            # provider that was merely BUSY, not broken.
            #
            # The `ProviderKeyLimiter` is what makes that unreachable: its cap
            # is the same `provider.max_concurrency` the pool is sized to, so
            # the (cap+1)th request is refused with a NEUTRAL
            # `ProviderKeyExhausted` BEFORE it can reach the pool queue. A
            # breaker without a limiter has no such floor, so we refuse the
            # combination here rather than let it erode a healthy provider's
            # capacity in production, invisibly, which is row 19's whole shape.
            raise ValueError(
                "Executor(breakers=...) requires limiter=...: without the "
                "per-credential concurrency gate, a full connection pool "
                "surfaces as a FAILURE-health timeout and opens a breaker "
                "against a busy-but-healthy provider"
            )
        self._upstream = upstream
        self._clock = clock
        self._breakers = breakers
        self._limiter = limiter

    async def execute(
        self,
        *,
        plan: ExecutionPlan,
        surface: Surface,
        body: bytes,
        path: str,
        stream: bool,
        method: str = "POST",
        deadline: Deadline,
        sink_factory: SinkFactory,
        retry_policy: RetryPolicy | None = None,
        body_kind: str = "json",
        content_type: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
        max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
        buffer_bytes: int = DEFAULT_BUFFER_BYTES,
        max_frame_bytes: int = DEFAULT_MAX_FRAME_BYTES,
        on_finish: OnFinish | None = None,
        request_facts: Any = None,
    ) -> ExecutionResult:
        """Walk the plan until something answers, or nothing can.

        --------------------------------------------------------------------
        `on_finish`: the result reaches accounting on EVERY exit
        --------------------------------------------------------------------

        Called exactly once with the terminal `ExecutionResult`, from a
        `finally`, on all three ways out: a return, a `GatewayError` that was
        turned into a result, and a `CancelledError` -- which still propagates
        afterwards, so C8 is untouched. On that last path the result is built
        in the `finally` with `Outcome.CANCELED`, the records written so far
        plus one for the attempt that was in flight, `committed` read off the
        one flag that can say, `served_by` from the same flag, and `pump`
        from the in-flight pump's `result` property (which exists for
        exactly this: C3's partial usage has to survive the failure).

        Why a callback and not a payload on the exception. The alternative is
        for the executor to catch the `CancelledError`, build the result, and
        raise a `ClientDisconnected` carrying it. That has three problems and
        each alone would disqualify it. It converts a cancellation into a
        `GatewayError` inside the file whose whole discipline is that it never
        does (C8, and the `except` clauses here are what a reader audits). It
        asserts a REASON -- "the client disconnected" -- that the executor
        does not have: a `CancelledError` here is also a server shutting
        down, and an executor that names the client for every cancellation
        is wrong on the day it matters. And it makes delivery depend on the
        exception surviving the trip up: `run_until_disconnect` cancels the
        worker and reads nothing off it, so the payload would be dropped by
        the very caller it was for. A `finally` delivers in the task that has
        the facts, before anyone else gets a say.

        The hook is synchronous (see `OnFinish`) and its exceptions are logged
        rather than raised. Both are about the same thing: nothing in the
        `finally` may become a second way for the request to end. A hook that
        raised would replace a `CancelledError` with a stack trace, and turn a
        completed stream the client already has into a 500 the client cannot
        see. Accounting observes; it does not get a vote.

        The loop, in the order the rules apply:

            for each target, in the plan's order:
                is there time left?          deadline.check()
                attempt it                   breaker.acquire() -> limiter.acquire()
                                             -> upstream.open() -> first byte ->
                                             sink_factory() -> pump
                on failure, ask              decide(err, committed=...)
                    retry_same and the budget agrees -> the SAME target
                    try_next                          -> the NEXT target
                    neither                           -> stop

        `decide()` is consulted once per failure and its answer is obeyed
        without amendment. The string `committed` appears in this method
        exactly once, as an argument to `decide()`. It is never a condition:
        the moment this file tests commitment itself, the commitment invariant
        has two homes and `errors.py`'s is no longer the one that decides.

        A failure from the gate -- the two `acquire()` calls -- is obeyed the
        same way, and differs from an upstream failure in bookkeeping only:
        it is a `Refusal`, not an `AttemptRecord`, and the budget was never
        charged for it (see the module docstring, "The attempt gate").

        --------------------------------------------------------------------
        One deadline, one budget, for the whole execution
        --------------------------------------------------------------------

        `deadline` is created by the caller at ingress and is the only clock
        anything here derives from. Three attempts cannot take three totals
        because there is nothing to derive a second total from -- `slice()`
        returns `min(remaining, budget)` and there is no way to ask it for
        more.

        The `RetryBudget` is likewise constructed once, above the loop.
        Constructing one per target is the classic re-basing bug: `max_attempts`
        silently becomes per-target, and a two-target plan with "one retry
        each" is four requests to a provider that is failing because it is
        overloaded.

        --------------------------------------------------------------------
        What the budget governs, and what it does not
        --------------------------------------------------------------------

        `record_attempt()` is called before every attempt, fallbacks included,
        so the budget's count is a count of the whole request. But a refusal
        from the budget stops a *repetition*, not the plan: `try_next` advances
        regardless.

        That split is C5. `X-Gw-No-Retry: 1` arrives as `enabled=False`, and a
        disabled budget refuses every delay. If a refusal also cancelled the
        fallback, then a caller who took ownership of retries would silently
        lose the incumbent as well -- and the incumbent is not a retry. It is a
        different provider, which is the one thing the outer gateway that owns
        the retries cannot do for us, because it has never seen our plan.

        The delay is still *requested* on the fallback path -- but note when it
        is actually granted. A refusal is not allowed to veto the move, and a
        DISABLED budget refuses before it computes a window, so the jitter that
        would spread a fleet's arrival at the incumbent exists only where a
        retry table is configured. On the default path (`retry_policy=None`,
        i.e. `NO_RETRIES` with `enabled=False`) a fleet that fails together
        does arrive at the incumbent together. That is a real cost of reading
        silence as "no repetition", and it is written down rather than fixed
        here because spacing a fallback by default would spend a client's
        latency on a decision the operator has not made.
        `test_the_default_path_falls_back_without_spacing_the_attempt` and its
        pair assert both halves.
        """
        budgets = plan.budgets
        if retry_policy is not None:
            # An explicit policy means the operator chose the REPETITION
            # allowance. It is not a bound on breadth and never was: the plan
            # is the breadth, so `max_attempts=1` buys zero repeats and leaves
            # the fallback exactly where it was. The bound to quote is
            # `RetryPolicy.max_attempts`' own -- `len(plan.targets) +
            # (max_attempts - 1)` -- which is also the number `/probe` reports
            # as `max_upstream_requests`.
            #
            # (This comment said the opposite until a verification pass
            # measured it. `test_an_explicit_max_attempts_of_one_still_reaches_
            # the_incumbent` now pins the behaviour, because a comment that
            # disagrees with the code is a coin flip about which one the next
            # reader believes -- and the two readings differ by whether a
            # workload has redundancy.)
            policy = retry_policy
        else:
            # Silence means "no repetition", never "no fallback". The plan is
            # already finite and ordered, so it is the breadth bound; the
            # budget only has to refuse repeats.
            policy = dataclasses.replace(
                NO_RETRIES, max_attempts=max(1, len(plan.targets))
            )
        budget = RetryBudget(policy, deadline, clock=self._clock)
        commitment = _Commitment(sink_factory)
        targets = plan.targets

        attempts: list[AttemptRecord] = []
        refusals: list[Refusal] = []
        last_attempt_error: GatewayError | None = None
        last_refusal: GatewayError | None = None
        preflight_error: GatewayError | None = None
        pump_result: PumpResult | None = None
        inflight: _AttemptState | None = None
        result: ExecutionResult | None = None

        try:
            index = 0
            while index < len(targets):
                target = targets[index]
                ctx = _context(plan, target)
                try:
                    # Cheap refusal before an expensive one. A request with no
                    # time left must not open a socket to discover it -- and
                    # this check deliberately does NOT record an attempt or
                    # spend the budget, because nothing was attempted. An
                    # attempt count inflated by requests that never left the
                    # building makes the amplification metric read high on
                    # exactly the incident where you need it to be trustworthy.
                    deadline.check(**ctx)
                except GatewayError as err:
                    preflight_error = err
                    break

                state = _AttemptState(target=target, started_at=self._clock.now())
                inflight = state
                try:
                    pump_result = await self._attempt(
                        target=target,
                        surface=surface,
                        body=body,
                        body_kind=body_kind,
                        content_type=content_type,
                        request_defaults=_defaults_for(plan, target),
                        path=path,
                        stream=stream,
                        method=method,
                        deadline=deadline,
                        budgets=budgets,
                        retry_budget=budget,
                        extra_headers=extra_headers,
                        max_response_bytes=max_response_bytes,
                        buffer_bytes=buffer_bytes,
                        max_frame_bytes=max_frame_bytes,
                        commitment=commitment,
                        state=state,
                        request_facts=request_facts,
                    )
                except GatewayError as err:
                    inflight = None
                    # Attribute BEFORE deciding. `decide()` reads the error's
                    # `health_key()`, and an error raised below the attempt --
                    # a `StallTimeout` out of the pump, which knows no target
                    # -- carries no provider or model and would key to
                    # `("?", "?")`, a breaker nothing ever acquires on. The
                    # loop is the one scope that knows which target this
                    # attempt was on; filling in what the raiser could not is
                    # attribution, not opinion, and it leaves every field the
                    # raiser DID set untouched.
                    _attribute(err, ctx)
                    disposition = decide(err, committed=commitment.started)

                    if not state.opened:
                        # The gate refused: no socket, no attempt, no budget
                        # spent, nothing for a breaker to learn (the refusal
                        # is NEUTRAL by taxonomy and there is no result to
                        # record anyway). Any ticket minted before the refusal
                        # is handed back unsettled.
                        self._release(state)
                        refusals.append(
                            Refusal(target=target, at=self._clock.now(), error=err)
                        )
                        last_refusal = err
                    else:
                        if state.pump is not None:
                            # C3: the partial usage has to survive the failure.
                            pump_result = state.pump.result
                        attempts.append(
                            AttemptRecord(
                                target=target,
                                started_at=state.started_at,
                                ended_at=self._clock.now(),
                                outcome=err.code,
                                status=state.status
                                if state.status is not None
                                else err.upstream_status,
                                error=err,
                                committed=commitment.started,
                            )
                        )
                        last_attempt_error = err
                        # The breaker hears the SAME disposition the loop
                        # obeys. One `decide()` per failure, one answer, two
                        # consumers.
                        self._settle(state, disposition)

                    if not state.opened:
                        # Nothing to back off from. A backoff spaces the
                        # NEXT request from the load this one put on a
                        # provider, and a refusal put none: the circuit or
                        # the cap answered in microseconds and no socket was
                        # opened. Asking the budget for a delay here would
                        # also index a backoff curve by an attempt that does
                        # not exist. So a refusal never repeats (a gate that
                        # said no will say no again on the next tick) and
                        # falls through to `try_next` at once.
                        spaced = False
                    elif disposition.retry_same or disposition.try_next:
                        # One request to the budget per failure. It answers
                        # "may I wait", never "where do I go" -- that is
                        # decide()'s.
                        spaced = await self._space(budget, err, len(attempts) - 1)
                    else:
                        spaced = False

                    if disposition.retry_same and spaced:
                        continue  # the SAME target, after a backoff we could afford
                    if disposition.try_next:
                        index += 1
                        continue
                    break

                inflight = None
                # `None` is the honest spelling of "nothing went wrong": there
                # is no error to decide on, so there is no Disposition -- with
                # one exception, a 200 whose stop reason says the provider
                # shed the request (`_completion_disposition`).
                self._settle(state, _completion_disposition(target, pump_result))
                attempts.append(
                    AttemptRecord(
                        target=target,
                        started_at=state.started_at,
                        ended_at=self._clock.now(),
                        outcome="success",
                        status=state.status,
                        error=None,
                        committed=commitment.started,
                    )
                )
                result = ExecutionResult(
                    plan=plan,
                    attempts=attempts,
                    served_by=target,
                    pump=pump_result,
                    committed=commitment.started,
                    outcome=Outcome.COMPLETED,
                    error=None,
                    refusals=refusals,
                )
                return result

            error = _final_error(
                plan, last_attempt_error, last_refusal, preflight_error
            )
            result = ExecutionResult(
                plan=plan,
                attempts=attempts,
                served_by=None,
                pump=pump_result,
                committed=commitment.started,
                outcome=decide(error, committed=commitment.started).outcome,
                error=error,
                refusals=refusals,
            )
            return result
        finally:
            # Every exit passes here, and only cancellation arrives with no
            # result. There is no `await` between this line and the hook: a
            # second cancellation has nowhere to land (see `OnFinish`).
            #
            # `inflight` is non-None on exactly the exits that skipped the
            # `except` and success branches above -- cancellation, or a
            # non-GatewayError bug -- and those are the exits with no
            # disposition. The tickets are RELEASED, never recorded: a
            # cancelled attempt is not evidence about a provider (C8), and
            # `Breaker.release()` returns a probe slot without counting, which
            # is what keeps a half-open circuit testable after its probe's
            # client hung up. The permit needs nothing here; its `async with`
            # inside `_attempt()` already ran.
            if inflight is not None:
                self._release(inflight)
            if on_finish is not None:
                if result is None:
                    result = self._canceled(
                        plan, attempts, refusals, inflight, pump_result, commitment
                    )
                _finish(on_finish, result)

    # ----------------------------------------------------------- the gate

    def _acquire_tickets(self, target: Target) -> tuple[Ticket, ...]:
        """Admission by both circuits that can veto this target, or `BreakerOpen`.

        Target circuit first, credential circuit second. If the second refuses
        after the first admitted, the first ticket is released HERE, before
        the exception leaves: a ticket that escapes unsettled is a probe slot
        that never comes back. A client's cancellation is not evidence
        about the provider.
        """
        if self._breakers is None:
            return ()
        first = self._breakers.for_key(target.health_key).acquire()
        try:
            second = self._breakers.for_key(credential_health_key(target)).acquire()
        except GatewayError:
            self._breakers.for_key(first.key).release(first)
            raise
        return (first, second)

    def _acquire_permit(self, target: Target) -> Permit | nullcontext[None]:
        """One permit against the target's credential, or `ProviderKeyExhausted`.

        Keyed by `credential_key`, not provider id: two catalog entries that
        present one API key are one entry as far as the upstream's cap is
        concerned. The cap is the catalog's per call, so a reload takes
        effect on the next attempt. `nullcontext` when there is no limiter,
        so `_attempt()` has one shape for both.
        """
        if self._limiter is None:
            return nullcontext()
        return self._limiter.acquire(
            target.credential_key, target.provider.max_concurrency
        )

    def _settle(self, state: _AttemptState, disposition: Disposition | None) -> None:
        """Hand the attempt's result to the circuits that admitted it.

        A ticket has four exits and this method is two of them:

            success              record(ticket, None) on BOTH tickets. A
                                 response proves the model AND the credential
                                 work, and a probe on either circuit closes it.
            failure              record(ticket, disposition) on the ticket
                                 whose key is `disposition.health_key`, and
                                 release() on the other. A 500 says nothing
                                 about the credential; a 401 says nothing
                                 about the model. `Breaker.record()` refuses
                                 a mismatched key, which is what makes this
                                 branch mandatory rather than tidy.
            gate refusal         `_release()`: nothing was learned.
            cancellation         `_release()`, from the loop's `finally`.

        The failure branch does not test `disposition.health` -- whether a
        NEUTRAL result counts is the breaker's rule (C8), and `record()` with
        a NEUTRAL disposition already settles without counting. The only
        thing decided here is WHICH circuit hears it, and that is the
        disposition's `health_key`, verbatim.
        """
        if self._breakers is None:
            return
        for ticket in state.tickets:
            breaker = self._breakers.for_key(ticket.key)
            if disposition is None or ticket.key == disposition.health_key:
                breaker.record(ticket, disposition)
            else:
                breaker.release(ticket)

    def _release(self, state: _AttemptState) -> None:
        """Settle every unsettled ticket with NO result. Idempotent on a
        state whose tickets were already settled, so the loop's `finally`
        can call it without knowing which branch ran."""
        if self._breakers is None:
            return
        for ticket in state.tickets:
            if not ticket.settled:
                self._breakers.for_key(ticket.key).release(ticket)

    # ------------------------------------------------------------- canceled

    def _canceled(
        self,
        plan: ExecutionPlan,
        attempts: list[AttemptRecord],
        refusals: list[Refusal],
        inflight: _AttemptState | None,
        pump_result: PumpResult | None,
        commitment: _Commitment,
    ) -> ExecutionResult:
        """The result for an execution that was cancelled: what had happened.

        Built from three things the cancellation cannot have touched. The
        records already written are what they were. The attempt in flight, if
        there was one, gets a `"canceled"` record and hands over its pump's
        partial usage -- and if there was NOT one (the cancel landed in a
        retry sleep), `pump_result` is whatever the last failed attempt left,
        which is the same C3 fact from the other side. `committed` and
        `served_by` come off `_Commitment`, which is the only object that
        knows and which was written before the await that could be cancelled.

        `error=None`. The executor does not know why it was cancelled, and a
        result that claimed to is one that would name the client for a server
        shutdown.
        """
        # `opened` gates the record for the same reason a gate refusal is not
        # an attempt: a cancellation that landed between `acquire()` and
        # `upstream.open()` interrupted a request that had not been sent.
        if inflight is not None and inflight.opened:
            if inflight.pump is not None:
                pump_result = inflight.pump.result
            attempts.append(
                AttemptRecord(
                    target=inflight.target,
                    started_at=inflight.started_at,
                    ended_at=self._clock.now(),
                    outcome=Outcome.CANCELED.value,
                    status=inflight.status,
                    error=None,
                    committed=commitment.started,
                )
            )
        return ExecutionResult(
            plan=plan,
            attempts=attempts,
            served_by=commitment.served,
            pump=pump_result,
            committed=commitment.started,
            outcome=Outcome.CANCELED,
            error=None,
            refusals=refusals,
        )

    # ------------------------------------------------------------- one attempt

    async def _attempt(
        self,
        *,
        target: Target,
        surface: Surface,
        body: bytes,
        path: str,
        stream: bool,
        method: str = "POST",
        deadline: Deadline,
        budgets: Budgets,
        retry_budget: RetryBudget,
        body_kind: str = "json",
        content_type: str | None = None,
        extra_headers: Mapping[str, str] | None,
        request_defaults: Mapping[str, Any] | None = None,
        max_response_bytes: int,
        buffer_bytes: int,
        max_frame_bytes: int,
        commitment: _Commitment,
        state: _AttemptState,
        request_facts: Any = None,
    ) -> PumpResult | None:
        """Open one target and, if it produces a byte, serve it to the client.

        Returns the `PumpResult` for a streamed response, `None` for a buffered
        one, and raises a `GatewayError` for anything else. Cancellation passes
        straight through: there is no `except Exception` in this method, so a
        `CancelledError` is never turned into evidence about a provider (C8).

        --------------------------------------------------------------------
        The gate, in order, and what each position costs
        --------------------------------------------------------------------

            1. breaker tickets     two dict lookups and two integer compares
            2. provider-key permit one dict lookup and one integer compare
            3. upstream.open()     a socket, a TLS handshake, a provider's
                                   attention, and the connect budget

        Cheapest first, and the breaker before the limiter for a reason that
        is not cost: a permit taken and then refused by the breaker would be
        held for the microseconds of the refusal, which is harmless, but a
        permit taken against a credential whose circuit is OPEN is a permit
        that a healthy tenant on the same key could have used. The breaker
        knows the target is not worth a permit; ask it first.

        The tickets are minted here and settled by the loop (see `_settle`).
        The permit is different: it has no result to report, so its whole
        life fits in an `async with` around the open -- normal return,
        `GatewayError`, and `CancelledError` all run `__aexit__`, and a
        second release is a `ValueError` rather than a negative count
        (admission.py, "The permit is the mitigation"). `opened` is set on
        the line before the open, inside the permit, so the loop can tell a
        gate refusal from an upstream failure without looking at a class.

        `retry_budget.record_attempt()` sits at the same line, after the gate
        and before the open, because retry.py says "immediately before every
        upstream attempt" and a refused attempt is not one.

        --------------------------------------------------------------------
        The first chunk is pulled here, not by the pump
        --------------------------------------------------------------------

        `UpstreamStream.aiter_raw()` puts its first chunk on the `first_event`
        budget and everything after it on the total. Reading that one chunk
        *before* creating the sink is what moves the status commitment to the
        right place: a `FirstEventTimeout` or a mid-handshake reset now happens
        with the plan still open, which is exactly the window C1 says is
        recoverable and P2 could not use.

        The cost is one chunk held in memory and one extra hop
        (`_ReheadedSource`) so the pump sees a stream that starts where the
        provider's did. That is cheaper than the alternative, which is a pump
        that knows about fallback.

        A 2xx with an empty body raises `IncompleteStream` rather than
        succeeding. A clean EOF is not a completion -- and because we have not
        called the factory, this one is still eligible for the next target,
        which is the best possible outcome for a provider that answered 200 and
        said nothing.
        """
        request = UpstreamRequest(
            target=target,
            body=body,
            path=path,
            stream=stream,
            method=method,
            extra_headers=dict(extra_headers or {}),
            body_kind=body_kind,
            content_type=content_type,
            request_defaults=request_defaults,
            model_key=getattr(surface, "model_key", "model") or "model",
            include_usage_injectable=bool(getattr(surface, "include_usage_injectable", True)),
        )
        ctx = {
            "provider": target.provider.id,
            "model": target.model.id,
            "credential_id": target.credential_key,
        }
        # ======================= THE GATE ==================================
        # The refusals below raise before any socket exists. They are
        # `try_next` by taxonomy and the loop obeys that; nothing here
        # decides anything.
        #
        # 0. The surface's say on THIS target (PLAN-2 Phase F): a Responses
        #    body that names provider-held state is refused for a provider
        #    that holds none. Before the breaker on purpose -- it is not
        #    evidence about the provider and must cost it no ticket.
        check_target = getattr(surface, "check_target", None)
        if check_target is not None:
            check_target(request_facts, target)
        state.tickets = self._acquire_tickets(target)
        async with self._acquire_permit(target):
            retry_budget.record_attempt()
            state.opened = True
            # ===============================================================
            async with self._upstream.open(
                request, deadline=deadline, budgets=budgets
            ) as upstream:
                state.status = upstream.status
                source = upstream.aiter_raw()
                first = await _next_chunk(source)
                if first is None:
                    raise IncompleteStream(
                        f"upstream answered {upstream.status} with an empty body",
                        upstream_status=upstream.status,
                        **ctx,
                    )

                if not stream:
                    # Buffered: read it all BEFORE starting the client's
                    # response. Nothing has been promised yet, so a body that
                    # turns out to be oversized or truncated is still a
                    # fallback rather than a half-written JSON object.
                    payload = await _read_body(first, source, max_response_bytes, ctx)
                    # Usage BEFORE commitment too (Phase C, finding 50):
                    # until now the buffered path had no pump and therefore
                    # no usage, and every non-streamed call billed zero. The
                    # dialect reads the complete body; the result rides in a
                    # PumpResult so accounting sees one shape for both paths.
                    buffered_usage = _buffered_usage(surface, payload, upstream.status)
                    _fold_header_usage(surface, upstream, buffered_usage)
                    state.pump = _BufferedResult(
                        buffered_usage,
                        bytes_out=len(payload),
                        first_event_at=self._clock.now(),
                    )
                    sink = await commitment.open(upstream, target=target)
                    await _send(sink, payload)
                    return state.pump.result if state.pump is not None else None

                # The framing check BEFORE commitment (PLAN-2 B1): a body
                # whose content type the surface's framer cannot read is a
                # 502 the plan can still fall back from, not a stream that
                # is copied for a few seconds and then cut as a "stall".
                _assert_framing(surface, getattr(upstream, "content_type", None), ctx)
                # ================== STATUS COMMITMENT ======================
                # The upstream has produced a byte we are about to forward.
                # From here the plan is over; see `_Commitment`.
                sink = await commitment.open(upstream, target=target)
                # ===========================================================
                pump = Pump(
                    surface=surface,
                    sink=sink,
                    deadline=deadline,
                    budgets=budgets,
                    clock=self._clock,
                    buffer_bytes=buffer_bytes,
                    max_frame_bytes=max_frame_bytes,
                    content_type=getattr(upstream, "content_type", None),
                )
                state.pump = pump
                # A header-borne meter (ElevenLabs' `character-cost`) is
                # known before any frame; fold it in now so even a stream cut
                # after commitment bills the provider's own count (C21).
                _fold_header_usage(surface, upstream, pump.usage)
                return await pump.run(_ReheadedSource(first, source))

    # ------------------------------------------------------------------ delay

    async def _space(
        self, budget: RetryBudget, err: GatewayError, attempt: int
    ) -> bool:
        """Ask the budget for a gap before the next attempt. True if we got one.

        The refusal is swallowed on purpose, and `RetryBudgetExhausted` is
        never the error the client sees: `retry.py` is explicit that it is a
        control signal, and a client handed `retry_budget_exhausted` when the
        provider actually returned a 429 with a `Retry-After` has been robbed
        of the only actionable thing in the response.

        What the caller does with `False` is `decide()`'s business, not this
        method's. It means "you may not repeat", never "you may not continue".
        """
        try:
            await budget.wait(err, attempt=attempt)
        except RetryBudgetExhausted:
            return False
        return True


# ==========================================================================
# Helpers
# ==========================================================================


def _finish(on_finish: OnFinish, result: ExecutionResult) -> None:
    """Deliver the result to the hook without letting the hook end the request.

    `except Exception` and not `BaseException`, so a `KeyboardInterrupt` or a
    `SystemExit` raised from inside the hook still behaves as one. Everything
    else is logged with the result's outcome, because a broken accounting hook
    is an incident about accounting and must not become an incident about
    every request that reaches it (see `execute()`).
    """
    try:
        on_finish(result)
    except Exception:  # noqa: BLE001 - the hook observes; it does not get a vote
        log.exception(
            "on_finish hook raised for workload %r (outcome=%s, attempts=%d)",
            result.plan.workload_id, result.outcome.value, len(result.attempts),
        )


def _context(plan: ExecutionPlan, target: Target) -> dict[str, str]:
    """The identifying fields every error raised for this attempt carries.

    `workload` is included because a capture record has to be groupable by it
    and a message is not a column. It is deliberately not a metric label --
    workload ids are operator-supplied and therefore unbounded.
    """
    return {
        "provider": target.provider.id,
        "model": target.model.id,
        "workload": plan.workload_id,
        "credential_id": target.credential_key,
    }


def _completion_disposition(
    target: Target, pump_result: PumpResult | None
) -> Disposition | None:
    """What the breaker hears about a stream that COMPLETED.

    Almost always `None`: a 200 that ran to its terminal marker is evidence
    the target works. The exception is a completed stream whose stop reason
    folds to `provider_shed` -- DeepSeek's `insufficient_system_resource` and
    `aborted`, a provider refusing the work inside a 200 (PLAN-2 A3). The
    request is still COMPLETED to the client and to accounting (bytes were
    delivered, the provider billed them), but the target's circuit hears a
    FAILURE, because it is the only signal that provider is degrading and a
    breaker that cannot hear it will keep sending traffic into the shed.
    Keyed to the target, never the credential: a shed says nothing about the
    key. Read with `getattr` so a pump whose surface predates `stop_reason`
    still settles cleanly."""
    usage = getattr(pump_result, "usage", None)
    if normalize_stop_reason(getattr(usage, "stop_reason", None)) != "provider_shed":
        return None
    return Disposition(
        retry_same=False,
        try_next=False,
        health=Health.FAILURE,
        health_key=target.health_key,
        outcome=Outcome.COMPLETED,
        reason="provider_shed",
    )


def _attribute(err: GatewayError, ctx: Mapping[str, str]) -> None:
    """Fill in the identity fields the raiser could not know. Never overwrites.

    `ctx` is `_context()`'s output and its keys are `GatewayError` attribute
    names. An error raised by the pump has no target in scope; one raised by
    `upstream.open()` already carries all four and is left exactly as it was.
    """
    for name, value in ctx.items():
        if getattr(err, name) is None:
            setattr(err, name, value)


def _final_error(
    plan: ExecutionPlan,
    last_attempt_error: GatewayError | None,
    last_refusal: GatewayError | None,
    preflight_error: GatewayError | None,
) -> GatewayError:
    """The error the client should see. The last real one, never a summary.

    Precedence, and the reasoning behind it:

    1. **The last error from an attempt that reached an upstream.** It is the
       one with a status, a body and possibly a `Retry-After`, and C4 says that
       is what passes through. A client told `no_targets_available` when the
       provider actually said "429, come back in 30 seconds" has been handed a
       503 in place of an instruction.
    2. **The last gate refusal**, when nothing reached an upstream but a gate
       said why. `BreakerOpen` carries the remaining cooldown as
       `retry_after` -- a true number, and the only one a client with no
       other target can act on. Below an upstream error because a refusal is
       about US (our circuit, our cap) and a 5xx is about the provider the
       client asked for, and C4 wants the provider's own words when it spoke.
    3. **The pre-flight error**, when no attempt was ever made. Today that is
       only `TotalDeadlineExceeded` from the check at the top of the loop --
       the honest answer to "why did nothing happen" when nothing happened.
    4. **`NoTargetsAvailable`**, and only when the plan was genuinely empty.
       It is a summary, and a summary is the right answer to exactly one
       question: what do you say when there was nothing to try?

    Note that a deadline breach discovered *while falling back* does not beat
    the upstream error that sent us falling back. The deadline explains our
    timing; the 429 explains the client's problem.
    """
    if last_attempt_error is not None:
        return last_attempt_error
    if last_refusal is not None:
        return last_refusal
    if preflight_error is not None:
        return preflight_error
    return NoTargetsAvailable(
        f"execution plan for workload {plan.workload_id!r} has no targets",
        workload=plan.workload_id,
    )


class _ReheadedSource:
    """The upstream body with its first chunk pushed back onto the front.

    A class rather than an `async def ... yield` wrapper so that `aclose()` is
    a real method that forwards. `Pump.run()` closes its source in a `finally`,
    and a generator-of-a-generator drops that close on the floor when it is
    itself closed while parked in an `async for` -- which leaks an upstream
    response until the collector notices, on the error path, which is the path
    that runs a lot when it matters.
    """

    __slots__ = ("_first", "_rest")

    def __init__(self, first: bytes, rest: AsyncIterator[bytes]) -> None:
        self._first: bytes | None = first
        self._rest = rest

    def __aiter__(self) -> _ReheadedSource:
        return self

    async def __anext__(self) -> bytes:
        first = self._first
        if first is not None:
            self._first = None
            return first
        return await self._rest.__anext__()

    async def aclose(self) -> None:
        aclose = getattr(self._rest, "aclose", None)
        if aclose is not None:
            await aclose()


def _defaults_for(plan: ExecutionPlan, target: Target) -> Mapping[str, Any] | None:
    """`plan.request_defaults_for(target)` (PLAN-2 B5: model row under the
    workload, workload wins) on a policy that has it; `None` on one that does
    not, which lets `upstream.open` fall back to the model row alone."""
    fn = getattr(plan, "request_defaults_for", None)
    if fn is None:
        return None
    merged = fn(target)
    return merged or None


def _assert_framing(
    surface: Surface, content_type: str | None, ctx: Mapping[str, str]
) -> None:
    """The framing check, BEFORE commitment (PLAN-2 B1). An SSE surface handed
    an `application/json` or `audio/mpeg` body raises
    `UnsupportedUpstreamFraming` (502, retry_same=False, try_next=True) here,
    with the plan still open, instead of the pump discovering it after the
    status is on the wire. The pump repeats the check defensively."""
    try:
        assert_upstream_framing(surface_framing(surface), content_type)
    except GatewayError as err:
        _attribute(err, ctx)
        raise


async def _next_chunk(source: AsyncIterator[bytes]) -> bytes | None:
    """One chunk, or `None` at end of body.

    `StopAsyncIteration` is converted to a sentinel for the reason
    `pump._next_chunk` gives: it must not escape into a `phase()` context
    manager, where a Stop* exception crossing an async generator boundary
    produces a `RuntimeError` that reads as "the timeout stopped working".
    """
    try:
        return await source.__anext__()
    except StopAsyncIteration:
        return None


def _fold_header_usage(surface: Surface, upstream: Any, usage: Usage) -> None:
    """`Surface.usage_from_headers`, when the dialect has one (Phase D).

    Never raises: a meter the surface cannot read leaves the usage as it was,
    and the accounting layer's exactness flags say so.
    """
    hook = getattr(surface, "usage_from_headers", None)
    if hook is None:
        return
    headers = getattr(upstream, "headers", None)
    if headers is None:
        return
    try:
        hook(headers, usage)
    except Exception:  # noqa: BLE001 - billing never breaks serving
        usage.parse_failures += 1


def _buffered_usage(surface: Surface, payload: bytes, status: int) -> Usage:
    """The usage a complete JSON body reports, via the surface (Phase C).

    Only a 2xx body is a usage report; an error body's `usage`, if any, is not
    a bill. Never raises: a body that is not JSON leaves the usage inexact.
    """
    usage = Usage()
    if not (200 <= status < 300):
        return usage
    try:
        parsed = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return usage
    if isinstance(parsed, dict):
        usage_from_body(surface, parsed, usage)
    return usage


class _BufferedResult:
    """A `PumpResult`-shaped record for a response that never had a pump.

    Exposes exactly the attributes accounting and the record path read:
    `usage`, `bytes_out`, `committed`, `terminal_seen`, `first_event_at`,
    `events`, `content_events`, `in_stream_error`,
    `liveness_before_first_event`. A buffered body that was fully read is by
    definition complete, so `terminal_seen` is True.
    """

    __slots__ = ("usage", "bytes_out", "first_event_at", "committed", "terminal_seen",
                 "events", "content_events", "in_stream_error",
                 "liveness_before_first_event")

    def __init__(self, usage: Usage, *, bytes_out: int, first_event_at: float) -> None:
        self.usage = usage
        self.bytes_out = bytes_out
        self.first_event_at = first_event_at
        self.committed = True
        self.terminal_seen = True
        self.events = 1
        self.content_events = 1
        self.in_stream_error = None
        self.liveness_before_first_event = False

    @property
    def result(self) -> _BufferedResult:
        return self


async def _read_body(
    first: bytes, source: AsyncIterator[bytes], limit: int, ctx: dict[str, str]
) -> bytes:
    """Drain a non-streaming body, bounded in bytes, first chunk included.

    The bound covers `first` rather than starting after it. A limit that
    exempts the chunk the caller happens to be holding is a limit a provider
    can walk straight through by answering in one write -- which is exactly
    what a provider answering from a cache does.

    `ResponseTooLarge` is `try_next=False` and `Health.NEUTRAL`, which is the
    right pair: the bound is ours, so the provider is not sick for having
    answered at length, and a second target would re-download an equally large
    body to hit the same wall.
    """
    chunks = [first]
    total = len(first)
    while True:
        if total > limit:
            raise ResponseTooLarge(
                f"non-streaming upstream body exceeds {limit} bytes", **ctx
            )
        chunk = await _next_chunk(source)
        if chunk is None:
            return b"".join(chunks)
        total += len(chunk)
        chunks.append(chunk)


async def _send(sink: Sink, payload: bytes) -> None:
    """One write to the client, with sink faults classified as client faults.

    The streaming path gets this from `Pump`. The buffered path has no pump, so
    the mapping lives here rather than nowhere -- a `BrokenPipeError` escaping
    as itself would be counted as a gateway fault against a provider that
    served the request perfectly (C8).
    """
    try:
        await sink.send(payload)
    except GatewayError:
        raise
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - any sink fault is the client leaving
        raise ClientDisconnected(
            f"client sink failed on a {len(payload)}-byte body: {exc!r}", cause=exc
        ) from exc


__all__ = [
    "DEFAULT_MAX_RESPONSE_BYTES",
    "NO_RETRIES",
    "AttemptRecord",
    "ExecutionResult",
    "Executor",
    "OnFinish",
    "Refusal",
    "SinkFactory",
    "credential_health_key",
]

"""The error taxonomy. This is file number one, and that is deliberate.

Every reliability decision this gateway makes is a switch statement over an
error class:

    retry the same target?  -> err.retry_same
    try the next target?    -> err.try_next
    is the target unhealthy? -> err.health
    what does the client see? -> err.status / err.passthrough
    what do we bill and record? -> err.outcome

If the classification is wrong, every policy layered on top of it is wrong in a
way no amount of tuning will fix: a gateway that treats a deterministic 400 as
retryable will burn its whole budget re-sending a request that cannot succeed,
and one that treats a client disconnect as a provider failure will open a
circuit breaker against a perfectly healthy provider because a user closed a
browser tab.

So the taxonomy comes before the mechanism, and it carries no dependencies --
not httpx, not starlette, not even asyncio. It is a pure description of what
can go wrong, and it can be tested in microseconds.

--------------------------------------------------------------------------
The five questions
--------------------------------------------------------------------------

Each error answers five orthogonal questions. Orthogonal is the important
word: these are routinely collapsed into one "retryable" boolean, and that
collapse is where the bugs live.

1. `retry_same`  -- may we send this again to the SAME (provider, model)?
                    Only for faults that are plausibly transient at that target.
2. `try_next`    -- may we send it to a DIFFERENT target in the plan?
                    A broader class: a schema the candidate rejected may be
                    fine at the incumbent, so 400 is `try_next` but not
                    `retry_same`.
3. `health`      -- does this count against the target's circuit breaker?
                    A client hanging up is not evidence about the provider.
4. `blame`       -- provider, client, gateway, or policy. Drives metrics,
                    alert routing, and whether the customer is charged.
5. `outcome`     -- the terminal record: completed / interrupted / rejected /
                    failed / canceled. Accounting reads only this.

--------------------------------------------------------------------------
The commitment invariant
--------------------------------------------------------------------------

There is a sixth question -- "have we already sent bytes to the client?" -- and
it is NOT a property of the error. It is a property of the request. An error
does not know whether a byte escaped; only the pump does.

So `try_next` on an error class means "eligible in principle". The single
place that combines eligibility with commitment is `decide()` at the bottom of
this file. The executor calls `decide()` and never re-derives the answer,
because an invariant written in two places is an invariant that will
eventually disagree with itself.
"""

from __future__ import annotations

import email.utils
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Health(Enum):
    """Does this event count as evidence about the target's health?"""

    FAILURE = "failure"
    NEUTRAL = "neutral"


class Blame(Enum):
    """Who caused it. Determines metric routing and billing, not retries."""

    PROVIDER = "provider"
    CLIENT = "client"
    GATEWAY = "gateway"
    POLICY = "policy"


class Outcome(Enum):
    """The terminal record for one request. Exactly one is written."""

    COMPLETED = "completed"
    """Ran to the surface's terminal marker."""

    INTERRUPTED = "interrupted"
    """Bytes reached the client, then it broke. Usage is `estimated`."""

    REJECTED = "rejected"
    """Refused by the gateway before any upstream work. Cheap and blameless."""

    FAILED = "failed"
    """Tried and could not deliver. No bytes reached the client."""

    CANCELED = "canceled"
    """The client went away. Not a failure of anything."""


class HealthScope(Enum):
    """Which key the health signal is recorded against.

    Almost everything scopes to (provider, model): a 500 from one model says
    nothing about another. Authentication is the exception and it matters --
    a revoked BYOK key belongs to one tenant's credential, not to the provider.
    Scoping a 401 to (provider, model) lets one tenant's expired key open a
    breaker for every other tenant on the same provider. That is a
    cross-tenant blast radius created entirely by a lazy dictionary key.
    """

    TARGET = "target"
    """Keyed by (provider, model)."""

    CREDENTIAL = "credential"
    """Keyed by (provider, credential_id)."""


class GatewayError(Exception):
    """Base class. Subclasses set the policy attributes as CLASS attributes so
    the taxonomy is readable as a table rather than assembled at runtime."""

    code: str = "gateway_error"
    retry_same: bool = False
    try_next: bool = False
    health: Health = Health.FAILURE
    health_scope: HealthScope = HealthScope.TARGET
    blame: Blame = Blame.PROVIDER
    outcome: Outcome = Outcome.FAILED
    status: int = 502
    """Status shown to the client when we have nothing better to pass through."""

    passthrough: bool = False
    passthrough_statuses: frozenset[int] | None = None
    """Upstream statuses this class will forward, when passthrough is on.
    None means "any status in the same hundreds band". A class sets this when
    its band contains a status that would tell the client the opposite thing:
    see `AuthenticationFailed`."""
    """True when the upstream's own status and body should reach the client
    verbatim once no fallback remains. We do not improve on a provider's error
    message; the caller's SDK knows how to read it and we do not."""

    def __init__(
        self,
        message: str = "",
        *,
        provider: str | None = None,
        model: str | None = None,
        workload: str | None = None,
        credential_id: str | None = None,
        attempt: int | None = None,
        retry_after: float | None = None,
        upstream_status: int | None = None,
        upstream_body: bytes | None = None,
        upstream_headers: Mapping[str, str] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.provider = provider
        self.model = model
        self.workload = workload
        """Which workload's plan this failed under.

        A structured field rather than a substring of `message`, because a
        capture record needs to be groupable by workload and a message is not
        a column. Deliberately NOT a metric label -- workload ids are
        operator-supplied and therefore unbounded (see metrics.py)."""

        self.credential_id = credential_id
        self.attempt = attempt
        self.retry_after = retry_after
        self.upstream_status = upstream_status
        self.upstream_body = upstream_body
        self.upstream_headers = dict(upstream_headers or {})
        """The upstream's own response headers.

        Carried so that C4 passthrough can forward the provider's
        `content-type` and `Retry-After` verbatim instead of ASSUMING
        `application/json` and reconstructing the delay from a parsed float.
        Reconstructing a header you were handed is a small lie that becomes a
        large one the day a provider answers an error in a shape you guessed
        wrong about.
        """
        self.cause = cause

    @property
    def client_status(self) -> int:
        """What the client actually gets.

        Passthrough forwards the provider's status because its shape is the
        one the caller's SDK already handles -- but only while that status
        agrees with what we concluded. A provider that signals an auth
        failure through some OTHER status has already told the client the
        wrong thing once: ElevenLabs answers a bad credential with 400
        (`captures`/`test_real_error_bodies`), and forwarding that means the
        caller is told it sent a bad request when in fact the GATEWAY's key
        was rejected. That is the same misfiling the body rule exists to
        prevent, one layer up, so a classification that overrode the status
        keeps its own.
        """
        if (self.passthrough and self.upstream_status is not None
                and self._status_agrees(self.upstream_status)):
            return self.upstream_status
        return self.status

    def _status_agrees(self, upstream: int) -> bool:
        """True when the provider's status still says what we concluded.

        A class that names `passthrough_statuses` is forwarded only for
        those; everything else falls back to the same hundreds band, which
        is all that was ever needed while providers used the status the
        error meant. 400 and 401 share a band and mean opposite things about
        whose fault it is, which is exactly the case this exists for.
        """
        allowed = self.passthrough_statuses
        if allowed is not None:
            return upstream in allowed
        return upstream // 100 == self.status // 100

    def health_key(self) -> tuple[str, str]:
        """The dictionary key this error's health signal is recorded under.

        The credential branch deliberately does NOT carry the provider. A
        credential is one API key, and a revoked or rate-limited key is bad
        for every provider *entry* that uses it -- so `openrouter` and
        `openrouter-toolsafe`, two routing configs over one key, must share a
        single auth circuit. Prepending the provider splits it in two, which
        means a dead shared key is hammered at twice the failure threshold
        before both halves trip, and a circuit opened by traffic through one
        entry does not protect a fallback that routes through the other.

        Carrying the provider here was the original spelling and it was wrong
        for the one case credential-scope exists to serve: the failure is
        about the KEY, and the key does not know which routing config named
        it. Under BYOK the credential_id is per-tenant, so `("cred", <tenant
        key>)` is one circuit for that tenant's key across every provider it
        reaches -- exactly the isolation FAILURE-MODES row 8 asks for."""
        if self.health_scope is HealthScope.CREDENTIAL:
            return ("cred", self.credential_id or "?")
        return (self.provider or "?", self.model or "?")

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        bits = [self.code]
        if self.provider:
            bits.append(f"provider={self.provider}")
        if self.model:
            bits.append(f"model={self.model}")
        if self.workload:
            bits.append(f"workload={self.workload}")
        if self.upstream_status:
            bits.append(f"upstream={self.upstream_status}")
        return f"<{type(self).__name__} {' '.join(bits)}>"


# ==========================================================================
# 1. Gateway-side rejections. No upstream connection was ever opened.
#    These are the cheapest errors in the system and that is the point:
#    shedding load has to cost less than serving it, or overload protection
#    becomes the overload.
# ==========================================================================


class AdmissionRejected(GatewayError):
    """Tenant is over its request-rate budget. The token bucket said no."""

    code = "admission_rejected"
    health = Health.NEUTRAL
    blame = Blame.POLICY
    outcome = Outcome.REJECTED
    status = 429


class ConcurrencyRejected(GatewayError):
    """Tenant is at its in-flight cap. Distinct from AdmissionRejected because
    the remedy differs: rate is fixed by waiting, concurrency by finishing.
    A concurrency denial must not consume rate credit -- see CONTRACTS.md #6."""

    code = "concurrency_rejected"
    health = Health.NEUTRAL
    blame = Blame.POLICY
    outcome = Outcome.REJECTED
    status = 429


class ProviderKeyExhausted(GatewayError):
    """The shared upstream credential is at its own concurrency cap. Not the
    tenant's fault: two tenants can each be inside their limit and jointly
    exceed the provider's. Eligible for the next target, which by definition
    uses a different key."""

    code = "provider_key_exhausted"
    try_next = True
    health = Health.NEUTRAL
    blame = Blame.POLICY
    outcome = Outcome.REJECTED
    status = 429


class BreakerOpen(GatewayError):
    """The circuit for this target is open. Health is NEUTRAL: the breaker
    tripping is the *consequence* of failures already counted. Counting the
    trip itself would let one bad minute keep the circuit open forever."""

    code = "breaker_open"
    try_next = True
    health = Health.NEUTRAL
    blame = Blame.PROVIDER
    outcome = Outcome.FAILED
    status = 503


class NoTargetsAvailable(GatewayError):
    """The execution plan is exhausted: every target refused, tripped, or
    failed. The last real error is chained as `cause` -- this one is a
    summary, and summaries should never be what the client sees when a
    concrete upstream error exists."""

    code = "no_targets_available"
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    outcome = Outcome.FAILED
    status = 503


class PolicyError(GatewayError):
    """Unknown workload, unknown model, missing credential: a misconfiguration.
    NEUTRAL health, because punishing a provider for our own config is how you
    end up with a breaker open against a provider that was never called."""

    code = "policy_error"
    health = Health.NEUTRAL
    blame = Blame.POLICY
    outcome = Outcome.REJECTED
    status = 400


# ==========================================================================
# 2. Connection and transport faults.
# ==========================================================================


class ConnectTimeout(GatewayError):
    """Never got a connection inside the connect clock. Nothing was sent, so
    this is the safest retry in the system: no side effect can exist."""

    code = "connect_timeout"
    retry_same = True
    try_next = True
    status = 504


class ConnectionFailed(GatewayError):
    """DNS failure, refused, TLS failure, or a reset before response headers.
    Same safety argument as ConnectTimeout."""

    code = "connection_failed"
    retry_same = True
    try_next = True
    status = 502


class HeadersTimeout(GatewayError):
    """The request was written; no response status line arrived in time.

    This class exists because a real HTTP client cannot tell you where in that
    sequence it stalled. `httpx` exposes no hook at "connected", so one wait
    covers connect + TLS + request write + response headers -- and those have
    *opposite* retry dispositions:

        nothing sent yet        -> re-sending is free
        request fully written   -> the model may already be generating

    `ConnectTimeout.retry_same=True` rests entirely on "no side effect can
    exist". That is airtight for a refused connection and merely *probable*
    for a request that was written and is waiting on headers. So when the
    transport cannot distinguish the two, we classify as the less safe of
    them: `retry_same=False`.

    That rule -- when you cannot prove nothing was sent, assume something
    was -- is the whole content of this class. `ConnectTimeout` remains for
    the case the transport DOES prove, which `httpx.ConnectTimeout` (raised
    only during connection establishment) genuinely does.
    """

    code = "headers_timeout"
    retry_same = False
    try_next = True
    status = 504


class FirstEventTimeout(GatewayError):
    """Headers arrived; no first event inside the first-event clock.

    `retry_same=False` is the interesting call. The request WAS accepted --
    the model may be generating right now and we would be billed twice for
    work we then throw away. Worse, a target slow enough to blow this clock is
    usually still slow one second later. Move on; do not re-ask the same
    overloaded model to hurry up.

    `queued=True` is the one instance-level override of a class-level health
    in the taxonomy, and it exists because of a provider behaviour the fakes
    never modelled: DeepSeek does not 429 under load, it QUEUES -- holding the
    request for up to ten minutes while sending SSE comment lines -- and
    ElevenLabs does the same for a few hundred milliseconds. A first-event
    clock that fires after the provider has been saying "still here" the whole
    time has measured a queue, not an outage, and five of those in thirty
    seconds would open a breaker against a provider that was healthy and
    busy. So the pump passes `queued=True` when a liveness signal (comment,
    empty-choices frame, `ping`) arrived before the clock fired, the health
    flips to NEUTRAL, and `llmgw_queued_at_provider_total` counts it. Blame
    stays PROVIDER -- it is their queue -- and the disposition is unchanged:
    still `try_next`, still not `retry_same` (PLAN-2 A4, findings log 43)."""

    code = "first_event_timeout"
    retry_same = False
    try_next = True
    status = 504
    queued: bool = False

    def __init__(self, message: str = "", *, queued: bool = False, **kw: Any) -> None:
        super().__init__(message, **kw)
        self.queued = queued

    def __setattr__(self, name: str, value: Any) -> None:
        # The pump stamps `queued` AFTER construction (`Pump._mark_queued`),
        # so the health flip has to follow the attribute, not the constructor.
        super().__setattr__(name, value)
        if name == "queued" and value:
            super().__setattr__("health", Health.NEUTRAL)


class StallTimeout(GatewayError):
    """The stream went quiet for longer than the inter-event clock.

    Note this can fire pre- or post-commitment; the class does not care, and
    `decide()` does. Heartbeats deliberately do NOT reset this clock: a
    heartbeat is liveness, not progress.

    `queued`, stamped by the pump: this stall fired before ANY content and
    after at least one liveness signal -- the provider was queueing the
    request, not dead. Health flips to NEUTRAL on the instance (class policy
    unchanged), the disposition is otherwise the same, and
    `llmgw_queued_at_provider_total` counts it. In practice this, not
    `FirstEventTimeout`, is the class a queued wait ends in: the first
    keep-alive comment satisfies the upstream's first-chunk budget, and it is
    the pump's progress clock that then measures the silence (PLAN-2 A4,
    findings log 43)."""

    code = "stall_timeout"
    try_next = True
    status = 504
    queued: bool = False

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)
        if name == "queued" and value:
            super().__setattr__("health", Health.NEUTRAL)


class UpstreamDisconnected(GatewayError):
    """The upstream closed or reset the connection mid-response."""

    code = "upstream_disconnected"
    retry_same = True
    try_next = True
    status = 502


class IncompleteStream(GatewayError):
    """The body ended cleanly but without the surface's terminal marker: no
    `data: [DONE]`, no `message_stop`. A clean EOF is not a completion, and
    treating it as one is how a gateway silently truncates answers."""

    code = "incomplete_stream"
    try_next = True
    status = 502


class RequestTooLarge(GatewayError):
    """The client's body exceeded `max_request_bytes`.

    Blamed on the CLIENT and rejected before any upstream work, which is the
    only reason a body bound is worth having: reading a 500 MB prompt "just to
    find out it is too big" is the denial of service, not the rejection.

    It exists as a taxonomy class rather than a plain exception in the server
    because every terminal outcome must be countable -- an error that cannot
    appear in `llmgw_requests_total{code=...}` is an error nobody can alert on.
    """

    code = "request_too_large"
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    outcome = Outcome.REJECTED
    status = 413


class UpstreamRequestTooLarge(GatewayError):
    """The PROVIDER answered 413: the body was under our cap and over theirs.

    Distinct from `RequestTooLarge` (our own cap, no upstream work) because the
    two have different owners -- ours is a config knob, theirs is a published
    limit -- and because this one carries the provider's body, which names
    the limit. Same disposition as the client-fault family: no retry (the
    same bytes will be too large again), no fallback by default (the next
    provider's limit is not larger just because it is next), NEUTRAL, CLIENT
    blame. Before this class existed a provider 413 fell through to
    `UpstreamServerError`: retried, and counted against the provider's
    circuit for a body the client chose (capabilities/anthropic.md, gap 8).
    """

    code = "upstream_request_too_large"
    retry_same = False
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    passthrough = True
    status = 413


class ResponseTooLarge(GatewayError):
    """A non-streaming response body exceeded the read bound.

    Distinct from `FrameTooLarge`, which is about one oversized SSE frame
    inside a stream. Same disposition -- our bound, our refusal, no point
    re-downloading it elsewhere -- but a different cause, and a shared name
    would make the metric unable to tell "one giant event" from "one giant
    body". Those have different remedies.
    """

    code = "response_too_large"
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    status = 502


class UnsupportedUpstreamFraming(GatewayError):
    """A streaming upstream body whose declared content type cannot be the
    framing the surface expects -- `application/json` or `audio/mpeg` where
    `text/event-stream` was due.

    Before this class existed the pump fed such a body to the SSE parser
    anyway: no frame ever completed, the bytes still reached the client, and
    the request died as a *provider stall* (`FirstEventTimeout`) or a
    *frame bound* (`FrameTooLarge`) with $0 accounted -- the exact shape the
    gzip note in `upstream.py` describes, and the shape Inworld's NDJSON
    stream produced on 16 Sep 2026. Raised BEFORE the status is committed,
    it is a fallback candidate instead of a truncated answer.

    `retry_same=False`: the provider will label the same body the same way
    again. `try_next=True`: another target may speak the framing we expect.
    NEUTRAL and GATEWAY blame: the provider did nothing wrong, we routed a
    body to a surface that cannot read it."""

    code = "unsupported_upstream_framing"
    retry_same = False
    try_next = True
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    status = 502


class FrameTooLarge(GatewayError):
    """A single SSE event exceeded the parser's byte bound.

    `try_next=False` and blame GATEWAY: our bound, our refusal. Retrying
    elsewhere would just re-download the same oversized frame and spend a
    second target's budget to hit the same wall."""

    code = "frame_too_large"
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    status = 502


class MalformedUpstreamResponse(GatewayError):
    """Bytes arrived that the surface cannot parse as its protocol. Counts as a
    provider failure: a provider emitting garbage is unhealthy even at 200."""

    code = "malformed_upstream_response"
    try_next = True
    status = 502


# ==========================================================================
# 3. HTTP status faults.
# ==========================================================================


class UpstreamServerError(GatewayError):
    """5xx that is not specifically an overload signal."""

    code = "upstream_server_error"
    retry_same = True
    try_next = True
    passthrough = True
    status = 502


class UpstreamOverloaded(GatewayError):
    """503, or Anthropic's 529. Explicitly 'come back later', so it is the one
    5xx where backing off is the provider's own instruction rather than our
    guess."""

    code = "upstream_overloaded"
    retry_same = True
    try_next = True
    passthrough = True
    status = 503


class RateLimited(GatewayError):
    """429 with an optional Retry-After, which `retry.py` treats as a FLOOR --
    never as the delay itself, and never to be undercut by jitter.

    `health=NEUTRAL` is a considered default. A 429 means "healthy but busy".
    Feeding it to the failure breaker produces flapping: 429s clear in seconds,
    breakers open for tens of seconds, so the breaker spends its life chasing a
    condition that already resolved. Rate pressure belongs to the limiter,
    which self-corrects. Configurable, and revisited with real numbers in S7."""

    code = "rate_limited"
    retry_same = True
    try_next = True
    health = Health.NEUTRAL
    passthrough = True
    status = 429


class InsufficientCredits(GatewayError):
    """HTTP 402. The account is out of money.

    Every axis of this differs from the 5xx it used to be classified as, and
    every one of them matters:

    * `retry_same=False` -- an unpaid invoice is not transient. Retrying is
      pure amplification against a provider that is working perfectly.
    * `try_next=True` -- a DIFFERENT provider may have credit. This is one of
      the few failures where fallback is close to guaranteed to help.
    * `health=NEUTRAL` -- the provider is healthy. Opening a breaker against
      it means that when the invoice is paid, traffic still will not flow.
    * `blame=POLICY` -- ours. Nobody should be paged about a vendor outage.

    Found against a real OpenRouter 402 ("Insufficient credits"), which the
    taxonomy previously swept into `UpstreamServerError`: retried forever,
    counted as FAILURE health, and blamed on the vendor.
    """

    code = "insufficient_credits"
    retry_same = False
    try_next = True
    health = Health.NEUTRAL
    blame = Blame.POLICY
    passthrough = True
    status = 402


class AuthenticationFailed(GatewayError):
    """401 or 403. Deterministic -- the same key will fail again in 10 ms --
    so never `retry_same`. Scoped to the CREDENTIAL, not the target: see
    HealthScope."""

    code = "authentication_failed"
    try_next = True
    health_scope = HealthScope.CREDENTIAL
    passthrough = True
    passthrough_statuses = frozenset({401, 403})
    """A provider may pick either for a rejected credential and both are the
    shape an SDK expects. Anything else is NOT forwarded: ElevenLabs answers
    a bad key with 400, and forwarding that tells the caller it sent a bad
    request when the truth is that the gateway's own key was rejected -- the
    same misfiling the 400 body rule exists to undo."""
    status = 401


class InvalidRequest(GatewayError):
    """400: the body is wrong for this target. Not retryable there (nothing
    changes), but genuinely eligible for the next target, whose schema may
    differ -- this is the row people are surprised by. NEUTRAL health: the
    provider correctly rejected a bad request; it is not sick."""

    code = "invalid_request"
    try_next = True
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    passthrough = True
    status = 400


class ContextLengthExceeded(InvalidRequest):
    """A 400 that a bigger-context target could actually serve, which makes it
    worth its own class: routing can act on it, and generic 400 handling
    cannot."""

    code = "context_length_exceeded"


class ModelNotFound(GatewayError):
    """404 on the model id. Config drift -- our catalog disagrees with the
    provider's reality. FAILURE health so a catalog gone stale trips fast and
    loudly rather than degrading quietly."""

    code = "model_not_found"
    try_next = True
    blame = Blame.POLICY
    passthrough = True
    status = 404


class ContentFiltered(GatewayError):
    """The provider refused on policy grounds. Not a fault, and NOT eligible
    for the next target by default: shopping a refused prompt around providers
    until one answers is a compliance decision, not a reliability one, and it
    is not the gateway's to make silently."""

    code = "content_filtered"
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    passthrough = True
    status = 400


class InStreamError(GatewayError):
    """An `event: error` (Anthropic) or `response.failed` (Responses) inside a
    200 body. HTTP said fine; the protocol said otherwise."""

    code = "in_stream_error"
    try_next = True
    status = 502


# ==========================================================================
# 4. Time and budget.
# ==========================================================================


class TotalDeadlineExceeded(GatewayError):
    """The one absolute deadline for the whole request expired.

    `try_next=False` is not a policy choice, it is arithmetic: there is no time
    left to try anything in. This is the class that makes retry storms
    structurally impossible rather than merely discouraged.

    `health=NEUTRAL` is a taxonomy decision, and it is the third instance of
    one pattern: time spent by one party being attributed to another. A
    deadline breach says the REQUEST ran out of time. It does not say a
    provider is sick, and the mechanism that produces this class is exactly
    the one that cannot know:

    `clocks.phase()` only reclassifies a phase breach to this class when the
    TOTAL was the binding constraint -- which means the provider was NOT given
    its full phase budget. `Deadline.slice()` clamped the phase to whatever
    was left after a previous target, a retry sleep, or a slow client had
    spent the rest. An incumbent offered 40 ms of a 20 s first-event budget
    and cut off at 40 ms has not been measured; it has been interrupted, and
    an interruption is not evidence. Counting it would open a breaker against
    the one target that had no chance to answer, on the strength of time the
    candidate spent -- a verification pass reproduced precisely that.

    A provider that genuinely stalls is still counted. When the PHASE budget
    is the binding one, `phase()` raises the phase's own class --
    `FirstEventTimeout`, `StallTimeout`, `ConnectTimeout` -- and those carry
    `FAILURE`, because there the provider had its whole allowance and did not
    use it. The two cases are separated by which clock fired, and only this
    one is the request's clock.

    `on_total` fixed client-versus-provider (a client that stops reading is
    not a provider fault at the deadline any more than before it). This fixes
    target-versus-target.

    `blame=GATEWAY`, for the same reason and one axis over. The rule that
    makes the whole family consistent: **a class named after a clock is
    blamed on whoever owns the clock.** `FirstEventTimeout` and `StallTimeout`
    are the provider's budgets -- PROVIDER. `ClientTooSlow` is the client's
    -- CLIENT. The total deadline is ours: we set it, we enforced it, and on
    two of its three production paths (`Deadline.check()` at the top of the
    loop, the entry check in `slice()`) no provider was even being waited on
    when it fired. A request that ran out its total during a retry sleep is
    not a provider fault, and blame routes metrics and billing, so getting
    this wrong meters our own arithmetic as somebody else's outage. Same
    answer as `RetryBudgetExhausted`, which is the budget saying stop where
    this is the deadline saying it."""

    code = "total_deadline_exceeded"
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    status = 504


class RetryBudgetExhausted(GatewayError):
    """The delay before the next attempt, plus the minimum time that attempt
    could take, is not less than the time remaining. Waiting would guarantee a
    deadline breach, so we stop now and return the real error instead of
    burning the client's remaining latency to reach the same conclusion."""

    code = "retry_budget_exhausted"
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    status = 504


# ==========================================================================
# 5. Client-side. The provider is innocent; the breaker must not hear about it.
# ==========================================================================


class ClientDisconnected(GatewayError):
    """The client went away. NEUTRAL and CANCELED.

    This row is load-bearing. A gateway that counts disconnects as failures
    will open breakers against healthy providers during any client-side
    incident -- exactly when you need those providers most. Cancellation is
    not evidence."""

    code = "client_disconnected"
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    outcome = Outcome.CANCELED
    status = 499


class ClientTooSlow(GatewayError):
    """The client stopped draining for longer than the max client-stall
    duration, so the pump's bounded buffer stayed full. We end the request to
    protect memory. The provider was fine and must not be marked otherwise."""

    code = "client_too_slow"
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    outcome = Outcome.INTERRUPTED
    status = 499


# ==========================================================================
# 6. The socket plane (PLAN-G). Two endings a request-shaped taxonomy had no
#    name for, because a request cannot have them: a long-lived session can
#    be ended by a DEPLOY, and it can be ended by nobody doing anything.
#    Both are here rather than in `llmgw/ws/errors.py` for one reason --
#    `ERROR_CODES` is the closed vocabulary `llmgw_requests_total{code}` is
#    bounded by, and a class registered from a package the metrics layer does
#    not import would be a label the collector refuses at the worst moment.
# ==========================================================================


class SessionDraining(GatewayError):
    """The process began a graceful drain while this session was open.

    The socket equivalent of the 503 the HTTP ingress sheds with, and it
    carries the same `"draining"`-shaped meaning: not a fault, an instruction
    to go somewhere else. `try_next=False` is deliberate even though another
    target would probably work -- the whole process is going away, so the
    next target would be opened from a machine that is about to stop, and the
    honest move is to let the client's own reconnect land on a machine that
    is not. Both plugins reconnect on close (tts.py:603-616, stt.py:287-336).

    CANCELED, not FAILED: the session did what it was asked to do right up
    until the deploy, and its capture record carries the units it relayed.
    NEUTRAL health, because a deploy is not evidence about a provider -- the
    same rule `ClientDisconnected` states."""

    code = "session_draining"
    retry_same = False
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.GATEWAY
    outcome = Outcome.CANCELED
    status = 503


class SessionIdle(GatewayError):
    """Nothing happened on the socket, in either direction, for `idle`.

    The reason this exists at all is `capabilities/captures-ws.md` 1.3:
    Inworld never pings, never closes a healthy socket, and answered nothing
    for 75 s of a live probe. So an abandoned session -- a client process
    killed without a close frame, an upstream socket whose contexts all
    closed and whose owner forgot it -- is indistinguishable at the transport
    layer from a healthy one, and the gateway's own budget is the ONLY thing
    that will ever reclaim it. Without this class a leaked socket is a leaked
    provider session the tenant is billed for on the products billed by
    session-open time.

    Blame CLIENT, health NEUTRAL: the provider was available the whole time.
    CANCELED rather than FAILED, for the same reason a disconnect is -- an
    idle socket is an ending, not a fault."""

    code = "session_idle"
    retry_same = False
    try_next = False
    health = Health.NEUTRAL
    blame = Blame.CLIENT
    outcome = Outcome.CANCELED
    status = 499


# ==========================================================================
# Disposition: the one place the commitment invariant is written.
# ==========================================================================


@dataclass(frozen=True)
class Disposition:
    """What the executor may do next, and what the breaker should be told."""

    retry_same: bool
    try_next: bool
    health: Health
    health_key: tuple[str, str]
    outcome: Outcome
    reason: str
    """Why the eligible actions were reduced, for the attempt log."""


def decide(err: GatewayError, *, committed: bool) -> Disposition:
    """Combine an error's eligibility with the request's commitment state.

    This function is the commitment invariant. Once a byte has reached the
    client, no further target may be opened and no attempt may be repeated:
    the client is already reading an answer, and a second attempt would splice
    a different answer onto the end of it. There is no configuration flag for
    this, no per-workload override, and no "but the first token was only a
    space" exception. It is a hard gate applied after every other rule.

    The outcome is upgraded to INTERRUPTED for the same reason -- a request
    that delivered a partial answer did not FAIL, it was interrupted, and
    accounting must record tokens already generated as billable and estimated.
    """
    if committed:
        return Disposition(
            retry_same=False,
            try_next=False,
            health=err.health,
            health_key=err.health_key(),
            outcome=(
                err.outcome
                if err.outcome in (Outcome.CANCELED, Outcome.COMPLETED)
                else Outcome.INTERRUPTED
            ),
            reason="committed",
        )
    return Disposition(
        retry_same=err.retry_same,
        try_next=err.try_next,
        health=err.health,
        health_key=err.health_key(),
        outcome=err.outcome,
        reason="eligible",
    )


# ==========================================================================
# Classification from the wire.
# ==========================================================================

_OVERLOAD_STATUSES = frozenset({503, 529})


def parse_retry_after(value: str | None, *, now: float | None = None) -> float | None:
    """Retry-After is either delta-seconds or an HTTP-date (RFC 9110 10.2.3).
    Providers send both, sometimes from the same service. Returns seconds, or
    None if absent or unparseable. Never negative: a date already in the past
    means "now", not "travel backwards"."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    reference = time.time() if now is None else now
    return max(0.0, when.timestamp() - reference)


def _is_api_error_body(body: bytes | None) -> bool:
    """True when the body is a JSON object shaped like an API error.

    OpenAI and Anthropic nest under `error`; gRPC-transcoded providers
    (Inworld) put `code` and `message` at the top level. Anything else -- an
    HTML page, plain text, an empty body -- is the edge in front of the API,
    and a 404 from it says nothing about the model.
    """
    if not body:
        return False
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return False
    if not isinstance(parsed, dict):
        return False
    # OpenAI/Anthropic: `error`; gRPC-transcoded (Inworld): `code`+`message`;
    # ElevenLabs: `detail` (an object, or a list on 422 validation errors).
    return (
        "error" in parsed
        or ("code" in parsed and "message" in parsed)
        or isinstance(parsed.get("detail"), (dict, list))
    )


def _error_hints(body: bytes | None) -> tuple[str, str]:
    """Pull (type, code_or_message) out of an error body, best effort.

    Both OpenAI and Anthropic nest under `error`, with different key names.
    This never raises: a provider returning an HTML error page during an
    incident must not crash the classifier that exists to handle the incident.
    """
    if not body:
        return ("", "")
    try:
        parsed: Any = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return ("", body[:512].decode("utf-8", "replace").lower())
    if not isinstance(parsed, dict):
        return ("", "")
    err = parsed.get("error")
    if not isinstance(err, dict):
        detail_block = parsed.get("detail")
        if isinstance(detail_block, dict):
            # ElevenLabs: `{"detail": {"status": ..., "message": ...}}` (legacy)
            # or `{"detail": {"type": ..., "code": ..., "message": ...}}`.
            # `status` is the legacy spelling of the machine-readable code.
            err = dict(detail_block)
            if "code" not in err and err.get("status"):
                err["code"] = err["status"]
        elif isinstance(detail_block, list):
            # ElevenLabs 422 validation: `[{loc, msg, type}]`. The first
            # message is as good a haystack as any; the status already says
            # "invalid request".
            first = (detail_block[0]
                     if detail_block and isinstance(detail_block[0], dict) else {})
            err = {"type": first.get("type"), "message": first.get("msg")}
        else:
            err = parsed
    etype = str(err.get("type") or "").lower()
    # `param` is deliberately NOT folded in. It names the offending FIELD, and
    # a field name is not a description of what went wrong. OpenAI answers an
    # unsupported `max_tokens` with `{"param": "max_tokens", "code":
    # "unsupported_parameter"}` -- and a haystack containing the param made
    # that match the context-overflow rule, so a two-token prompt classified
    # as "context length exceeded". The substring was right there in the
    # payload and meant the opposite of what the rule assumed.
    parts = [str(err.get(k) or "") for k in ("code", "message")]
    # Anthropic puts the machine-readable reason one level down:
    # `error.details.error_code` (e.g. `enforced_spend_limit_reached` on a
    # 429 that carries no Retry-After). It is a code, not a param, so it
    # belongs in the haystack the code rules read.
    details = err.get("details")
    if isinstance(details, dict):
        parts.append(str(details.get("error_code") or ""))
    detail = " ".join(parts).lower()
    return (etype, detail)


# Out-of-money, arriving as a 429. OpenAI never uses 402: an exhausted
# prepaid balance, a hit spend limit or a hit usage limit is a 429 whose
# `code` says so, and the docs say plainly "do not retry -- add credit".
# Anthropic's spend cap is a 429 with `details.error_code` and no
# `retry-after`. A 429 classified as `RateLimited` is retry-same with
# backoff and NEUTRAL, which is exactly wrong for an unpaid invoice: the
# gateway retries a request that cannot succeed and never blames POLICY
# (PLAN-2 A2, findings log 42, mirror of finding 8 on two more providers).
_BILLING_429_CODES: tuple[str, ...] = (
    "insufficient_quota",
    "credit_balance_exhausted",
    "organization_spend_limit_exceeded",
    "project_spend_limit_exceeded",
    "organization_usage_limit_exceeded",
    "enforced_spend_limit_reached",
)

# Anthropic's self-set usage limit arrives as a 400 with prose; no code.
_BILLING_400_HINT = "reached your specified api usage limits"

# ElevenLabs answers a bad credential with 400, not 401: the body is
# `{"detail": {"type": "authentication_error", "code": "invalid_api_key",
# "status": "api_key_id_used_as_api_key", ...}}` (observed live, 18 Sep 2026,
# by pasting an API key ID where the key goes). Status alone therefore blames
# the CLIENT for a credential that is OURS, which is the worst possible
# misfiling: the dashboard fills with client errors while the real cause is a
# gateway key that was never valid, no credential circuit ever opens, and a
# fallback that would have worked is never tried. Same shape of rule as the
# billing-as-429 table above -- the provider is signalling one thing through
# a status that means another, and only the body says which.
_AUTH_400_HINTS = ("authentication_error", "invalid_api_key")

_MODERATION_HINTS = (
    # OpenAI image generation, captured live 2026-09-20. The body is
    # `{"error":{"message":"Your request was rejected by the safety system.
    # ...","type":"image_generation_user_error","param":null,
    # "code":"moderation_blocked","moderation_details":{"moderation_stage":
    # "input","categories":["other"]}}}` -- note that `type` is
    # `image_generation_user_error`, NOT `content_filter`, so the existing
    # `content_filter`/`content_policy` rule below does not see it and the
    # refusal classified as a plain `invalid_request`.
    #
    # The difference is not cosmetic. `InvalidRequest` is `try_next=True`:
    # a refused prompt would be shopped around every fallback target until
    # one answered, which is a compliance decision the gateway is not
    # allowed to make silently (see `ContentFiltered`). Both classes are
    # NEUTRAL and blame the CLIENT, so no circuit was ever at risk here --
    # the fallback behaviour is what the fix is for.
    "moderation_blocked",
    "rejected by the safety system",
)

# AssemblyAI's sync host answers a bad credential with a **404**, not a 401
# or a 403: `{"status":404,"title":"Not Found","detail":"Invalid API key"}`
# under `application/problem+json` (captured live, probe A4g, reproduced
# 2026-09-19). On that host a 404 is genuinely ambiguous -- the same status
# is what the AWS load balancer returns for an `X-AAI-Model` it does not
# route, as `text/plain` with no body shape at all -- so the two are
# separated by the RFC 7807 envelope and the `detail` string, and by nothing
# else. Without this rule a revoked key reads as `UpstreamServerError`
# (retry the same target, blame the provider's health) or, if the 404 body
# rule ever widened, as `ModelNotFound` (blame our catalog). Both send
# somebody hunting the wrong thing while the fix is a new key.
_PROBLEM_JSON_TYPE = "application/problem+json"
_PROBLEM_AUTH_HINTS = ("invalid api key", "invalid api token", "invalid credentials")


def _is_problem_json(headers: Mapping[str, str] | None, body: bytes | None) -> bool:
    """An RFC 7807 problem document: the content type says so, or the body is
    a JSON object with exactly that envelope's required keys. Both, because
    the content type is the provider's claim and the shape is the evidence."""
    if headers:
        for key, value in headers.items():
            if str(key).lower() == "content-type" and _PROBLEM_JSON_TYPE in str(value).lower():
                return True
    if not body:
        return False
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return False
    return (isinstance(parsed, dict) and "status" in parsed and "title" in parsed
            and isinstance(parsed.get("detail"), str))


def _problem_detail(body: bytes | None) -> str:
    """The `detail` string of a problem document, lowercased; "" if absent.
    Never raises -- this runs on an error path, on bytes we did not write."""
    if not body:
        return ""
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return ""
    if not isinstance(parsed, dict):
        return ""
    detail = parsed.get("detail")
    return detail.lower() if isinstance(detail, str) else ""


def _looks_like_billing(detail: str) -> bool:
    return any(code in detail for code in _BILLING_429_CODES)


_UNKNOWN_MODEL_HINTS = (
    "model_not_found",
    "does not exist",
    "not found",
    "unknown model",
    "invalid model",
    "no such model",
    "supported api model names",   # DeepSeek's phrasing, verified live
    "model_id:",                   # Inworld: "model_id: X is not supported." (live, 16 Sep)
    "unsupported model",           # Inworld STT: 'Unsupported model "x".' (live, 19 Sep)
    "valid model_id",              # ElevenLabs Scribe: "'x' is not a valid model_id."
    # Sarvam, verified live 19 Sep 2026. It answers an unknown model with a
    # 400 and a pydantic validation string that enumerates the models it
    # does serve, and the enumeration is spelled THREE ways across its
    # products -- so all three are listed rather than one loose substring:
    #
    #   TTS   "Validation Error(s):\n- model: Input should be 'bulbul:v2',
    #          'bulbul:v3-beta', 'bulbul:v3' or 'bulbul:v4-flash'"
    #   STT   "body.model : Input should be 'saarika:v2.5', 'saaras:v3', ..."
    #   chat  "body.model : Value error, Input 'sarvam-nope' should be one
    #          of sarvam-105b, sarvam-105b-conversations"
    #
    # Without them a stale Sarvam catalog row reads as the CLIENT's bad
    # request: no fallback, no config-drift signal, and the customer blamed.
    "model: input should be",
    "model : input should be",
    "model : value error",
    # And a model that USED to exist: "Model 'bulbul:v2' has been
    # deprecated. Please use 'bulbul:v3' instead." / "Model 'sarvam-m' has
    # been deprecated..." (both live, 19 Sep 2026). A retired id is catalog
    # drift by another name -- our config points at something the provider
    # no longer serves -- so it lands in the same class, which is the one
    # that tries the next target instead of telling the caller off.
    "has been deprecated",
)


def _looks_like_unknown_model(detail: str) -> bool:
    """Does this 400 actually mean "we asked for a model you do not have"?

    Substring matching on a provider's prose is a weak signal and it is used
    here anyway, because the alternative is worse: without it, a stale catalog
    entry is indistinguishable from a malformed client request on every
    provider except Anthropic, and the party blamed is the customer.

    Weak signal, narrow blast radius: the only thing it changes is blame and
    health. Both candidates are `try_next=True` and neither is `retry_same`,
    so a false positive cannot cause an extra upstream request.
    """
    return "model" in detail and any(h in detail for h in _UNKNOWN_MODEL_HINTS)


def from_http_status(
    status: int,
    *,
    body: bytes | None = None,
    headers: Mapping[str, str] | None = None,
    retry_after: str | None = None,
    provider: str | None = None,
    model: str | None = None,
    credential_id: str | None = None,
    attempt: int | None = None,
    forbidden_means: str = "auth",
) -> GatewayError:
    """Map an upstream HTTP response onto the taxonomy.

    Status code first, body only to *refine* -- never to override. Providers
    change their error strings without warning; they change status codes far
    less often, so the string match is the subordinate signal. Anything
    unrecognised lands on the conservative default for its class rather than
    on a guess.

    `forbidden_means` is the one per-provider knob, and it is a knob because
    403 is the status providers disagree about most. OpenAI (region block),
    Anthropic (permission error) and Inworld (bad key, verified live) mean
    "this credential may not do this" -- `"auth"`, the default, and the
    credential-scoped class is right. AssemblyAI's REST rate limit is a 403
    (`"rate_limit"`); ElevenLabs' plan, voice and model denials are 403s
    (`"policy"`). Under the default rule either one opens the credential
    breaker for every tenant on a per-voice mistake or a polling burst. The
    value comes from the provider row (`ProviderConn.forbidden_means` once
    the voice rows land; `getattr` with the default until then), never from
    the body: a 403 body is exactly the thing a mis-scoped provider gets
    wrong.
    """
    etype, detail = _error_hints(body)
    after = parse_retry_after(retry_after)
    kw = dict(
        provider=provider,
        model=model,
        credential_id=credential_id,
        attempt=attempt,
        retry_after=after,
        upstream_status=status,
        upstream_body=body,
        upstream_headers=headers,
    )

    if status == 429:
        if _looks_like_billing(detail):
            return InsufficientCredits(detail or "out of credit", **kw)
        return RateLimited(f"rate limited by {provider or 'upstream'}", **kw)
    if status == 403 and forbidden_means == "rate_limit":
        return RateLimited(f"rate limited by {provider or 'upstream'} (403)", **kw)
    if status == 403 and forbidden_means == "policy":
        # A plan, voice or model the credential is not entitled to. Config
        # drift or a caller asking for something the plan lacks; NEUTRAL,
        # POLICY blame, no circuit hears it. The provider's body is the only
        # place the entitlement is named, so it passes through.
        err = PolicyError(detail or f"{provider or 'upstream'} refused (403)", **kw)
        err.passthrough = True
        # An upstream WAS asked, so this is not the gateway's own REJECTED.
        err.outcome = Outcome.FAILED
        return err
    if status in (401, 403):
        return AuthenticationFailed(f"auth rejected by {provider or 'upstream'}", **kw)
    if status == 402:
        return InsufficientCredits(detail or "insufficient credits", **kw)
    if status == 404:
        # Before anything else: a 404 that is an RFC 7807 document SAYING the
        # credential is bad. AssemblyAI's sync host is the only provider in
        # the catalog that does this, and it is not a model fault, not the
        # edge, and not retryable against the same key.
        if _is_problem_json(headers, body):
            problem = _problem_detail(body)
            if any(hint in problem for hint in _PROBLEM_AUTH_HINTS):
                return AuthenticationFailed(
                    problem or "upstream rejected the gateway's credential (404)", **kw,
                )
        # A provider that names its own routing fault gets believed. Sarvam
        # answers an unknown PATH with a perfectly well-formed API error
        # object -- `{"error":{"message":"Not Found","code":"not_found_error",
        # "request_id":...}}`, live 19 Sep 2026 -- which the rule below reads
        # as "the model is not here". It is not: `/text-to-speech:stream` is
        # a 404 and `/text-to-speech/stream` is a 200, and the difference is
        # ours, not the catalog's. Calling it `ModelNotFound` sends the
        # executor shopping the request around every fallback for a path
        # none of them has either, and tells the operator to fix a model id
        # that was never wrong.
        #
        # `code`, deliberately, not `type`: Anthropic spells its UNKNOWN
        # MODEL 404 with `error.type == "not_found_error"`, and that one
        # really is a missing model. `_error_hints` keeps `type` out of
        # `detail`, so the two do not collide.
        if "not_found_error" in detail and not _looks_like_unknown_model(detail):
            return UpstreamServerError(
                f"404 from {provider or 'upstream'} naming the PATH, not a model "
                f"(routing fault: the surface's upstream_path does not exist)", **kw,
            )
        # Only an API error object means "the model is not here". A 404 with
        # an HTML page, plain text or no body at all is the provider's EDGE
        # answering, not its API: OpenAI's did so intermittently on 18 Sep
        # 2026 (finding 49), and calling that ModelNotFound sent the client a
        # confident wrong message and retried nothing.
        if _is_api_error_body(body) or _looks_like_unknown_model(detail):
            return ModelNotFound(f"model {model!r} not found at {provider}", **kw)
        return UpstreamServerError(
            f"404 with a non-API body from {provider or 'upstream'} "
            f"(edge or routing fault, not a missing model)", **kw,
        )
    if status == 413:
        return UpstreamRequestTooLarge(detail or "request too large for upstream", **kw)
    if status == 409:
        # Anthropic `conflict_error`, ElevenLabs `already_processing`: the
        # request as sent cannot be applied. Not transient, not the
        # provider's health; before this rule it fell through to
        # `UpstreamServerError` and was retried against the circuit.
        return InvalidRequest(detail or "upstream conflict", **kw)
    if status == 400 or status == 422:
        # Status alone cannot answer this. Anthropic returns 404 for an
        # unknown model; DeepSeek and OpenRouter return 400 with an
        # `invalid_request_error`, so a status-only rule blames the CLIENT for
        # a stale catalog and the "our config drifted" signal simply does not
        # exist on OpenAI-shaped providers. Body first, here and only here.
        if any(hint in etype or hint in detail for hint in _AUTH_400_HINTS):
            return AuthenticationFailed(
                detail or "upstream rejected the gateway's credential", **kw,
            )
        if _BILLING_400_HINT in detail:
            return InsufficientCredits(detail, **kw)
        # Before the unknown-model rule, deliberately. `moderation_blocked`
        # is a machine-readable code the provider chose; `_looks_like_
        # unknown_model` is substring matching on prose that its own
        # docstring calls a weak signal. A strong signal is not allowed to
        # lose to a weak one because of line order.
        if any(hint in detail or hint in etype for hint in _MODERATION_HINTS):
            return ContentFiltered(detail or "content filtered", **kw)
        if _looks_like_unknown_model(detail):
            return ModelNotFound(detail or f"model {model!r} not found", **kw)
        if "context" in detail or "too long" in detail or "context_length" in detail:
            return ContextLengthExceeded("context length exceeded", **kw)
        if "content_filter" in etype or "content_policy" in detail:
            return ContentFiltered("content filtered", **kw)
        return InvalidRequest(detail or "invalid request", **kw)
    if status in _OVERLOAD_STATUSES or "overloaded" in etype:
        return UpstreamOverloaded(f"{provider or 'upstream'} overloaded", **kw)
    if status == 408:
        return ConnectTimeout("upstream reported request timeout", **kw)
    if status >= 500:
        return UpstreamServerError(f"upstream {status}", **kw)
    # A non-2xx we have no rule for. Pass it through rather than inventing a
    # meaning; do not let it look healthy.
    return UpstreamServerError(f"unexpected upstream status {status}", **kw)


# A closed set of codes. Metrics label cardinality is a production hazard, so
# the registry exists to be asserted on: `llmgw_requests_total{code=...}` can
# only take values from here.
ERROR_CODES: frozenset[str] = frozenset(
    cls.code
    for cls in (
        AdmissionRejected, ConcurrencyRejected, ProviderKeyExhausted, BreakerOpen,
        NoTargetsAvailable, PolicyError, RequestTooLarge, UpstreamRequestTooLarge,
        ResponseTooLarge,
        ConnectTimeout, ConnectionFailed,
        HeadersTimeout, FirstEventTimeout, StallTimeout, UpstreamDisconnected,
        IncompleteStream,
        FrameTooLarge, UnsupportedUpstreamFraming, MalformedUpstreamResponse,
        UpstreamServerError,
        UpstreamOverloaded, RateLimited, InsufficientCredits,
        AuthenticationFailed, InvalidRequest,
        ContextLengthExceeded, ModelNotFound, ContentFiltered, InStreamError,
        TotalDeadlineExceeded, RetryBudgetExhausted, ClientDisconnected,
        ClientTooSlow,
        SessionDraining, SessionIdle,
    )
)
# The base `GatewayError.code` ("gateway_error") is unioned in AFTER the
# frozenset closes: it is the catch-all label for an unclassified
# gateway-internal failure -- a code the system CAN emit (a bare base error,
# and `accounting._fallback_record` for a result too malformed to price), so
# it belongs in the vocabulary that bounds the `llmgw_requests_total{code}`
# label. Leaving it out let a fallback record hit the collector's closed-vocab
# guard, raise, and drop the request from the one exactly-once metric
# (found by a verification pass).
ERROR_CODES = ERROR_CODES | frozenset({GatewayError.code})

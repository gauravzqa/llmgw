"""Accounting: what one request cost, and whether the number can be defended.

This is P5, the pure half. It turns a terminal `executor.ExecutionResult` into
one `AccountingRecord` -- tokens by kind, cost in USD, and the request-level
facts the metrics wiring fans into the counters declared in `metrics.py`
(`llmgw_tokens_total`, `llmgw_cost_usd_total`, `llmgw_requests_total`,
`llmgw_committed_total`, `llmgw_attempts_total`,
`llmgw_usage_parse_failures_total`). Nothing here imports `prometheus_client`,
touches a socket, or reads configuration; the record is a value the wiring
layer iterates, so it can be built and asserted in microseconds.

--------------------------------------------------------------------------
The one rule the rest follow from: accounting observes, it does not vote
--------------------------------------------------------------------------

`account()` never raises. Observability that can break the request path is a
liability, not an asset -- the same discipline `pump._observe` states for
`apply_usage` and the executor states for its `on_finish` hook. A malformed,
half-built, or `None`-riddled result must still produce a record, and when the
happy path cannot be trusted the record is flagged `estimated` rather than
thrown. A billing gap you can see beats an exception that eats the request.

--------------------------------------------------------------------------
C3: an interrupted stream bills the tokens it generated
--------------------------------------------------------------------------

`ExecutionResult.pump` is present even when the stream FAILED -- it carries the
partial `Usage` accumulated before the break (`pump.py`, `PumpResult`). A
record that billed interrupted requests zero is the exact bug CONTRACTS.md C3
names: it looks like a discount until someone reconciles the provider's
invoice. So tokens come from `pump.usage` whenever a pump exists, on the
failure path as much as the success path.

Determining WHICH model to price against is the subtle part, and it is where
the real executor forced a decision this brief did not anticipate. On the
happy path and on cancellation `served_by` names the target whose bytes the
client got. But a post-commitment failure that returns *normally* (a
`StallTimeout` or `IncompleteStream` after some bytes -- not a cancellation)
returns `served_by=None` while `committed=True` and `pump` carries real usage
(see `executor.execute`, the failure return at the bottom of the loop). Pricing
that off `served_by` alone would bill it zero -- precisely the C3 bug. So the
billed target is `served_by` if set, else the target of the last *committed*
attempt. "Nobody answered" -- requirement 5 -- is then the honest case where
nothing committed: no billed target, zero tokens, zero cost, `model=None`, and
the outcome/attempts still meaningful.

--------------------------------------------------------------------------
Why the catalog argument is not re-queried for prices
--------------------------------------------------------------------------

The `Target` in the plan already carries the exact `ModelSpec` the request was
routed with -- prices included -- pinned in the policy snapshot at ingress.
Re-resolving the model id against a live `Catalog` to fetch "current" prices is
exactly FAILURE-MODES row 10 (`policy.py`): a request routed by v1 and billed
by v2, with one record that cannot say so. So cost is computed from the spec on
the served target, and `catalog` is retained for API stability (and used only
as a last-ditch fallback if a target somehow arrives without a usable spec).
The record prices what routing chose, which is the only number it can defend.

--------------------------------------------------------------------------
basis, and the estimator
--------------------------------------------------------------------------

`basis` is `"exact"` iff `usage.exact` (input AND output both stated by the
provider and final -- see `surfaces.base.Usage`), otherwise `"estimated"`.
These are the two values of `metrics.COST_BASIS`. A request interrupted before
the provider reported usage has no exact output count, so it is `estimated` --
and rather than bill its output as zero (the C3 bug again), output tokens are
estimated from the bytes actually written to the client:

    output_tokens ~= bytes_out / _OUTPUT_BYTES_PER_TOKEN   (K = 4)

K = 4 is the standard ~4-bytes-per-token density of English text. Applied to
raw SSE bytes it ignores frame overhead and so tends to *over*-count -- which
is why the estimate is never allowed to fall below the provider's own reported
floor (`usage.output_tokens`, which for Anthropic is a genuine running count),
and is taken as `max(reported_floor, byte_estimate)`. It is a defensible guess
flagged as one, not a lie dressed as a measurement. When output IS exact the
provider's number is used verbatim and the bytes are ignored.
"""

from __future__ import annotations

from dataclasses import dataclass

from .catalog import Catalog, ModelSpec, Target, price_of
from .errors import Outcome
from .executor import ExecutionResult
from .pump import PumpResult
from .surfaces.base import Usage

__all__ = ["AccountingRecord", "account", "TOKEN_KINDS"]

# Mirrors `metrics.TOKEN_KINDS`. Duplicated rather than imported so accounting
# has no dependency on the metrics module (which the wiring layer owns): the
# two are pinned equal by a test instead, so a drift fails loudly.
TOKEN_KINDS: tuple[str, ...] = ("input", "output", "cache_read", "cache_write")

_OUTPUT_BYTES_PER_TOKEN = 4
"""Bytes of streamed output per output token, for the interrupted-stream
estimate. See the module docstring: the classic ~4-chars/token heuristic,
used only as a floor-respecting guess when the provider never sent a final
usage frame."""


@dataclass(frozen=True, slots=True)
class AccountingRecord:
    """One request's billing and observability record. A value, not a process.

    Frozen and slotted because it is a snapshot handed across a module boundary
    to the metrics wiring, which reads it and never mutates it. Every field
    here answers a metric declared in `metrics.py`; nothing here is a label of
    unbounded cardinality (`workload_id` is carried for a capture record, NOT
    as a metric label -- see the metrics module docstring).
    """

    workload_id: str
    policy_id: str

    provider: str | None
    """Provider id of the billed target, or None when nobody delivered bytes.
    A metric label on `llmgw_tokens_total` / `llmgw_cost_usd_total`; bounded by
    the catalog."""

    model: str | None
    """Model id of the billed target, or None when unserved. See `provider`."""

    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    cache_write_tokens: int
    """The four disjoint buckets of `metrics.TOKEN_KINDS`. Disjoint by the
    `Usage` convention (`surfaces.base`): `input_tokens` excludes cache, and
    cache_read is kept apart because it is 10-100x cheaper and folding it in
    makes effective cost unknowable. `output_tokens` is the BILLED count --
    exact when the provider stated it, otherwise the interrupted-stream
    estimate."""

    cost_usd: float
    """Total USD for the request: the dot product of the four buckets against
    the served spec's per-token rates. Zero when unserved."""

    basis: str
    """`"exact"` or `"estimated"` -- the two values of `metrics.COST_BASIS`.
    `"exact"` iff the provider reported final usage for both halves."""

    outcome: Outcome
    committed: bool
    """Whether a byte reached the client. The `llmgw_committed_total`
    increment, and the denominator that keeps the interruption rate honest."""

    attempts: int
    """`len(result.attempts)` -- upstream attempts, the amplification numerator
    (`X-Gw-Attempts`). The per-attempt fan-out for `llmgw_attempts_total` is
    the wiring layer's to iterate off `result.attempts` directly."""

    parse_failures: int
    """Usage frames the surface could not read
    (`llmgw_usage_parse_failures_total`). Non-zero is why a stream that looks
    complete can still be `estimated`."""

    code: str
    """The terminal error `code`, or `"none"` on success/cancellation -- the
    `code` label of `llmgw_requests_total`. Drawn from `errors.ERROR_CODES`
    plus `"none"`, exactly the closed set `metrics._CODES` allows."""

    @property
    def tokens_by_kind(self) -> dict[str, int]:
        """The four buckets keyed by `metrics.TOKEN_KINDS`, so the metrics wiring
        can `for kind in TOKEN_KINDS: counter.labels(..., kind).inc(rec.tokens_by_kind[kind])`
        without knowing which field maps to which label."""
        return {
            "input": self.input_tokens,
            "output": self.output_tokens,
            "cache_read": self.cache_read_tokens,
            "cache_write": self.cache_write_tokens,
        }


def account(result: ExecutionResult, *, catalog: Catalog) -> AccountingRecord:
    """Turn one terminal `ExecutionResult` into an `AccountingRecord`.

    Pure and total: no I/O, no prometheus, and -- the hard requirement -- it
    never raises. Anything fallible is wrapped; on failure the record is built
    best-effort and flagged `estimated`, because a request whose cost we could
    not compute is exactly the request we must not also drop from the metrics.
    """
    try:
        return _account(result, catalog=catalog)
    except Exception:  # noqa: BLE001 - accounting observes; it does not get a vote
        return _fallback_record(result)


def _account(result: ExecutionResult, *, catalog: Catalog) -> AccountingRecord:
    plan = result.plan
    committed = bool(result.committed)

    target = _billed_target(result)
    usage = _usage_of(result.pump)
    output_tokens = _billed_output_tokens(usage, result.pump)

    if target is None:
        # Requirement 5: nobody delivered bytes. Zero tokens, zero cost, but a
        # valid record -- the outcome and attempt count still describe what
        # happened, they just describe a request nothing was served for.
        provider = model = None
        in_tok = out_tok = cr_tok = cw_tok = 0
        cost = 0.0
    else:
        spec = _spec_for(target, catalog)
        provider = target.provider.id
        model = spec.id
        in_tok = usage.input_tokens
        out_tok = output_tokens
        cr_tok = usage.cache_read_tokens
        cw_tok = usage.cache_write_tokens
        cost = _cost_usd(usage, spec, out_tok)

    return AccountingRecord(
        workload_id=plan.workload_id,
        policy_id=plan.policy_id,
        provider=provider,
        model=model,
        input_tokens=in_tok,
        output_tokens=out_tok,
        cache_read_tokens=cr_tok,
        cache_write_tokens=cw_tok,
        cost_usd=cost,
        basis="exact" if usage.exact else "estimated",
        outcome=result.outcome,
        committed=committed,
        attempts=len(result.attempts),
        parse_failures=usage.parse_failures,
        code=result.error.code if result.error is not None else "none",
    )


def _billed_target(result: ExecutionResult) -> Target | None:
    """The target the tokens should be priced against, or None if unserved.

    `served_by` when the executor set it (success, and cancellation via
    `_Commitment.served`). Otherwise the target of the last *committed* attempt
    -- the C3 case where a post-commitment failure returns `served_by=None`
    while a byte reached the client and `pump` carries real usage. When nothing
    committed, there is genuinely no billed target: that is "nobody answered".
    """
    if result.served_by is not None:
        return result.served_by
    for attempt in reversed(result.attempts):
        if attempt.committed:
            return attempt.target
    return None


def _usage_of(pump: PumpResult | None) -> Usage:
    """The pump's accumulated usage, or an empty (and therefore inexact) Usage.

    A missing pump -- nobody streamed, or a buffered response whose usage this
    layer does not parse -- reads as zero tokens and `estimated`, which is the
    honest report: we have no provider-stated counts to call exact.
    """
    if pump is None or pump.usage is None:
        return Usage()
    return pump.usage


def _billed_output_tokens(usage: Usage, pump: PumpResult | None) -> int:
    """Output tokens to bill: the exact count, or the interrupted-stream guess.

    When the provider stated a final output count (`output_exact`), that number
    is used verbatim. Otherwise the stream was cut before the final usage frame
    and we estimate from bytes actually written to the client -- never below
    the provider's own running floor, so an Anthropic stream that reported an
    output count at a `message_delta` before dying still bills at least that.
    """
    if usage.output_exact:
        return usage.output_tokens
    byte_estimate = 0
    if pump is not None and pump.bytes_out > 0:
        byte_estimate = round(pump.bytes_out / _OUTPUT_BYTES_PER_TOKEN)
    return max(usage.output_tokens, byte_estimate)


def _cost_usd(usage: Usage, spec: ModelSpec, output_tokens: int) -> float:
    """USD for one request: a dot product over the four DISJOINT buckets.

    Each bucket is priced by its own per-million rate and summed, then divided
    by 1e6. Cache is never folded into input (the `Usage` convention keeps them
    disjoint precisely so this stays a sum and not a subtraction someone
    forgets):

        input        x input_per_m
        cache_read   x cached_input_per_m   (falls back to input_per_m)
        cache_write  x cache_write_per_m    (falls back to input_per_m)
        output       x output_per_m

    Both cache fallbacks go to `input_per_m`, never to zero -- the same rule
    `catalog.price_of` states for cache reads. Falling back to zero would make
    an uncached provider look free and misprice every cache-bearing request on
    a model whose write rate the table does not carry.
    """
    input_rate = spec.input_per_m
    cache_read_rate = price_of(spec, cached=True)
    cache_write_rate = (
        spec.cache_write_per_m if spec.cache_write_per_m is not None else spec.input_per_m
    )
    per_million = (
        usage.input_tokens * input_rate
        + usage.cache_read_tokens * cache_read_rate
        + usage.cache_write_tokens * cache_write_rate
        + output_tokens * spec.output_per_m
    )
    return per_million / 1_000_000


def _spec_for(target: Target, catalog: Catalog) -> ModelSpec:
    """The `ModelSpec` to price against.

    The spec pinned on the served `Target` is authoritative -- it is the price
    table the request was routed with (see the module docstring on why the live
    `catalog` is deliberately not re-queried). The catalog is consulted only if
    a target somehow arrives without a usable spec, so a degenerate result
    still prices rather than raises.
    """
    spec = getattr(target, "model", None)
    if isinstance(spec, ModelSpec):
        return spec
    resolved = catalog.resolve(target.model.id if spec is not None else "")
    return resolved.model


def _fallback_record(result: object) -> AccountingRecord:
    """A valid record for a result too malformed to account for. Never raises.

    Everything is pulled with `getattr` and coerced, because the whole reason
    this path runs is that something about `result` was not the shape we
    expected. `estimated` basis and zero cost say "we could not compute this",
    which is the truthful thing to emit -- and far better than letting the
    metrics wiring take an exception on the request path.
    """
    plan = getattr(result, "plan", None)
    outcome = getattr(result, "outcome", None)
    if not isinstance(outcome, Outcome):
        outcome = Outcome.FAILED

    attempts = getattr(result, "attempts", None)
    attempt_count = len(attempts) if isinstance(attempts, (list, tuple)) else 0

    error = getattr(result, "error", None)
    code = getattr(error, "code", "gateway_error") if error is not None else "none"
    if not isinstance(code, str):
        code = "gateway_error"

    return AccountingRecord(
        workload_id=str(getattr(plan, "workload_id", "") or ""),
        policy_id=str(getattr(plan, "policy_id", "") or ""),
        provider=None,
        model=None,
        input_tokens=0,
        output_tokens=0,
        cache_read_tokens=0,
        cache_write_tokens=0,
        cost_usd=0.0,
        basis="estimated",
        outcome=outcome,
        committed=bool(getattr(result, "committed", False)),
        attempts=attempt_count,
        parse_failures=0,
        code=code,
    )

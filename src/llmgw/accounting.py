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

import dataclasses
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from .catalog import Catalog, ModelSpec, Target, price_of
from .errors import Outcome
from .executor import ExecutionResult
from .metrics import normalize_stop_reason
from .pump import PumpResult
from .surfaces.base import Usage

__all__ = ["AccountingRecord", "account", "account_usage", "TOKEN_KINDS", "UNITS"]

# Mirrors `metrics.TOKEN_KINDS`. Duplicated rather than imported so accounting
# has no dependency on the metrics module (which the wiring layer owns): the
# two are pinned equal by a test instead, so a drift fails loudly.
TOKEN_KINDS: tuple[str, ...] = (
    "input", "output", "cache_read", "cache_write",
    "audio_input", "audio_output", "cached_audio_input", "cache_write_1h",
    "reasoning",
)
UNITS: tuple[str, ...] = ("characters", "seconds", "images")
"""Mirrors `metrics.UNITS`; pinned equal by the same test."""

_USAGE_INT_FIELDS = (
    "characters", "audio_input_tokens", "audio_output_tokens",
    "cached_audio_input_tokens", "cache_write_1h_tokens", "reasoning_tokens",
    # A count, never a rate: no `ModelSpec.unit` is `"images"` and `_cost_usd`
    # has no branch for it, so this rides the record and the meter and adds
    # nothing to the bill. See `metrics.UNITS`.
    "images",
)
"""The PLAN-2 B3 fields on `surfaces.base.Usage`, read with `getattr` and a
zero default so a `Usage` that predates them still accounts. `seconds` is
NOT among them -- see `_usage_seconds`."""


def _usage_int(usage: object, name: str) -> int:
    value = getattr(usage, name, 0)
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _usage_seconds(usage: object) -> float:
    """Audio seconds, keeping the fraction the provider reported.

    Every other unit here is a count and an integer. Duration is not, and
    `int()` on it TRUNCATES: ElevenLabs Scribe reports `audio_duration_secs:
    1.84`, AssemblyAI sync reports `audio_duration_ms: 1840`, and both used
    to be billed as 1 second -- a 46% under-bill on a record flagged
    `exact`, which is worse than an honest estimate because nothing about it
    looks wrong. Providers that meter in whole seconds (OpenAI's `usage.
    seconds`, already rounded up on their side) are unaffected: 2.0 is 2.0.
    """
    value = getattr(usage, "seconds", 0)
    try:
        seconds = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if seconds != seconds or seconds in (float("inf"), float("-inf")):  # NaN / inf
        return 0.0
    return max(0.0, seconds)


def _usage_tool_calls(usage: object) -> dict[str, int]:
    raw = getattr(usage, "server_tool_calls", None)
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        try:
            n = int(value or 0)
        except (TypeError, ValueError):
            continue
        if n > 0:
            out[str(key)] = n
    return out

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

    stop_reason: str | None = None
    """Why the provider ended a completed response, folded onto
    `metrics.STOP_REASONS` by `normalize_stop_reason`; None when the provider
    said nothing (a cut stream, a buffered body the surface did not read).
    The `stop_reason` label of `llmgw_stop_reason_total` and a capture field.
    Read off `Usage.stop_reason` with `getattr` so a surface that predates the
    field still accounts cleanly (PLAN-2 A3)."""

    # ---- PLAN-2 B3: kinds beyond the four text buckets ---------------------

    audio_input_tokens: int = 0
    audio_output_tokens: int = 0
    cached_audio_input_tokens: int = 0
    """Audio token kinds providers report inside token usage, priced at the
    spec's audio rates (falling back to the text rates, with a note)."""

    cache_write_1h_tokens: int = 0
    """Anthropic 1-hour-TTL cache writes, priced at `cache_write_1h_per_m`."""

    reasoning_tokens: int = 0
    """Informational: already inside `output_tokens`, never priced twice. The
    number that shows a "cheap" candidate spending its budget on thinking."""

    images: int = 0
    """Images the provider returned (`metrics.UNITS`). Metered, never priced:
    the image models bill in tokens and this record's `cost_usd` comes from
    them. See `surfaces.base.Usage.images`."""

    characters: int = 0
    seconds: float = 0.0
    """Non-token units (`metrics.UNITS`), for rows whose `unit` is not
    tokens. Zero on text models. `seconds` is a FLOAT because duration is
    not a count: the providers that meter it report fractions and truncating
    them under-bills every call (`_usage_seconds`)."""

    unit: str = "tokens"
    """The billed row's `ModelSpec.unit`; says which of the counts above the
    cost was computed from."""

    server_tool_calls: dict[str, int] = field(default_factory=dict)
    """Provider-side tool calls by usage key, priced at `tool_rates`."""

    cost_notes: tuple[str, ...] = ()
    """Why a cost is less exact than `basis` says (a kind priced at a fallback
    rate). Empty when every kind found its own price."""

    @property
    def tokens_by_kind(self) -> dict[str, int]:
        """Every token bucket keyed by `metrics.TOKEN_KINDS`, so the metrics
        wiring can `for kind, n in rec.tokens_by_kind.items(): ...` without
        knowing which field maps to which label."""
        return {
            "input": self.input_tokens,
            "output": self.output_tokens,
            "cache_read": self.cache_read_tokens,
            "cache_write": self.cache_write_tokens,
            "audio_input": self.audio_input_tokens,
            "audio_output": self.audio_output_tokens,
            "cached_audio_input": self.cached_audio_input_tokens,
            "cache_write_1h": self.cache_write_1h_tokens,
            "reasoning": self.reasoning_tokens,
        }

    @property
    def units_by_kind(self) -> dict[str, float]:
        """The non-token units keyed by `metrics.UNITS`."""
        return {"characters": float(self.characters), "seconds": self.seconds,
                "images": float(self.images)}


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
    """The HTTP adapter: pull the six facts off an `ExecutionResult` and hand
    them to the shared pricer.

    Everything specific to a request lives here -- which target got billed
    when the executor did not say (`_billed_target`), and the interrupted
    stream's output-token guess from bytes written (`_billed_output_tokens`,
    which needs the pump). What is left is a `Usage` and a `Target`, and that
    is exactly what a relayed WebSocket session also ends with, which is why
    the pricing below is `_account_usage` and not a method on this path.
    """
    plan = result.plan
    usage = _usage_of(result.pump)
    return _account_usage(
        usage,
        target=_billed_target(result),
        catalog=catalog,
        outcome=result.outcome,
        committed=bool(result.committed),
        attempts=len(result.attempts),
        workload_id=plan.workload_id,
        policy_id=plan.policy_id,
        code=result.error.code if result.error is not None else "none",
        output_tokens=_billed_output_tokens(usage, result.pump),
    )


def account_usage(
    usage: Usage,
    *,
    target: Target | None,
    catalog: Catalog,
    outcome: Outcome,
    committed: bool,
    attempts: int,
    workload_id: str,
    policy_id: str,
    code: str,
    cost_notes: Sequence[str] = (),
) -> AccountingRecord:
    """Price a `Usage` that did not come from an `ExecutionResult`. C27.

    The WebSocket plane's entry point. A relayed session has no
    `ExecutionResult` -- no pump, no attempt list of the executor's making,
    no `served_by` -- but it ends with the same two objects a request ends
    with: the counts the provider reported, and the target they were reported
    by. So it is priced by the SAME dot product (`_cost_usd`), which is the
    only way "characters at the Inworld TTS rate" can be guaranteed to mean
    the same number on both planes.

    `cost_notes` is prepended to whatever the pricer itself notes, because a
    session knows things the pricer cannot: that its seconds were derived
    from relayed audio bytes rather than metered, that it was priced through
    a deprecated-model alias, how many contexts it carried.

    Never raises, for the reason the module docstring gives: an accounting
    layer that can fail is a request path that can fail for a reason the
    client cannot act on. A malformed input produces a valid, zero-cost,
    `estimated` record and a note saying so.
    """
    try:
        rec = _account_usage(
            usage, target=target, catalog=catalog, outcome=outcome,
            committed=committed, attempts=attempts, workload_id=workload_id,
            policy_id=policy_id, code=code,
        )
    except Exception:  # noqa: BLE001 - accounting observes; it does not get a vote
        rec = _fallback_record(None)
        return dataclasses.replace(
            rec,
            workload_id=workload_id, policy_id=policy_id, outcome=outcome,
            committed=committed, attempts=attempts,
            code=code if isinstance(code, str) else "gateway_error",
            cost_notes=(*tuple(cost_notes), *rec.cost_notes),
        )
    if cost_notes:
        rec = dataclasses.replace(rec, cost_notes=(*tuple(cost_notes), *rec.cost_notes))
    return rec


def _account_usage(
    usage: Usage,
    *,
    target: Target | None,
    catalog: Catalog,
    outcome: Outcome,
    committed: bool,
    attempts: int,
    workload_id: str,
    policy_id: str,
    code: str,
    output_tokens: int | None = None,
) -> AccountingRecord:
    """The dot product, shared by both planes. May raise; callers guard."""
    output_tokens = usage.output_tokens if output_tokens is None else output_tokens

    extra: dict[str, float] = {name: _usage_int(usage, name) for name in _USAGE_INT_FIELDS}
    extra["seconds"] = _usage_seconds(usage)
    tool_calls = _usage_tool_calls(usage)

    if target is None:
        # Requirement 5: nobody delivered bytes. Zero tokens, zero cost, but a
        # valid record -- the outcome and attempt count still describe what
        # happened, they just describe a request nothing was served for.
        provider = model = None
        in_tok = out_tok = cr_tok = cw_tok = 0
        cost = 0.0
        unit = "tokens"
        notes: tuple[str, ...] = ()
    else:
        spec = _spec_for(target, catalog)
        provider = target.provider.id
        model = spec.id
        unit = spec.unit
        in_tok = usage.input_tokens
        out_tok = output_tokens
        cr_tok = usage.cache_read_tokens
        cw_tok = usage.cache_write_tokens
        cost, notes = _cost_usd(usage, spec, out_tok, extra=extra, tool_calls=tool_calls)

    return AccountingRecord(
        workload_id=workload_id,
        policy_id=policy_id,
        provider=provider,
        model=model,
        input_tokens=in_tok,
        output_tokens=out_tok,
        cache_read_tokens=cr_tok,
        cache_write_tokens=cw_tok,
        cost_usd=cost,
        basis="exact" if usage.exact else "estimated",
        outcome=outcome,
        committed=committed,
        attempts=attempts,
        parse_failures=usage.parse_failures,
        code=code,
        stop_reason=normalize_stop_reason(getattr(usage, "stop_reason", None)),
        audio_input_tokens=extra["audio_input_tokens"],
        audio_output_tokens=extra["audio_output_tokens"],
        cached_audio_input_tokens=extra["cached_audio_input_tokens"],
        cache_write_1h_tokens=extra["cache_write_1h_tokens"],
        reasoning_tokens=extra["reasoning_tokens"],
        characters=extra["characters"],
        seconds=extra["seconds"],
        images=int(extra["images"]),
        unit=unit,
        server_tool_calls=tool_calls,
        cost_notes=notes,
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
    # Bytes are a proxy for TEXT tokens only. A stream carrying a media unit
    # (audio tokens, characters, seconds -- a voice surface's own meter or its
    # request-side estimate) is audio on the wire: 131 KB of PCM is not 32k
    # output tokens. Bill what the surface said, still `estimated`.
    if (getattr(usage, "audio_output_tokens", 0) or getattr(usage, "characters", 0)
            or getattr(usage, "seconds", 0)):
        return usage.output_tokens
    byte_estimate = 0
    if pump is not None and pump.bytes_out > 0:
        byte_estimate = round(pump.bytes_out / _OUTPUT_BYTES_PER_TOKEN)
    return max(usage.output_tokens, byte_estimate)


def _cost_usd(
    usage: Usage,
    spec: ModelSpec,
    output_tokens: int,
    *,
    extra: Mapping[str, int] | None = None,
    tool_calls: Mapping[str, int] | None = None,
) -> tuple[float, tuple[str, ...]]:
    """USD for one request, and the notes explaining any fallback rate.

    A dot product over DISJOINT buckets, each priced by its own per-million
    rate, summed, divided by 1e6. Cache is never folded into input (the
    `Usage` convention keeps them disjoint precisely so this stays a sum and
    not a subtraction someone forgets). For `unit="tokens"`:

        input               x input_per_m
        cache_read          x cached_input_per_m     (falls back to input_per_m)
        cache_write         x cache_write_per_m      (falls back to input_per_m)
        cache_write_1h      x cache_write_1h_per_m   (falls back to cache_write, then input)
        output              x output_per_m
        audio_input         x audio_input_per_m      (falls back to input_per_m, NOTED)
        audio_output        x audio_output_per_m     (falls back to output_per_m, NOTED)
        -- both are SUBSETS of input/output_tokens and are subtracted from
        the text share first (OpenAI reports audio inside the base counts)
        cached_audio_input  x cached_audio_input_per_m (falls back to cache-read rate, NOTED)
        reasoning           priced NOWHERE: already inside output

    For `unit="characters"`: `characters x input_per_m` (the meter is the
    text sent; TTS rows carry their per-million-character price there). For
    `unit="seconds"`: `seconds / 60 x per_minute`. Token buckets still add
    in either case, so a model that reports both (OpenAI TTS in SSE mode
    reports tokens) is not double-counted by the unit it does not use.

    Server tools: `calls x tool_rates[key] / 1000`; a key with no rate is
    counted on the record and NOTED, not priced.

    Every fallback goes to a real rate, never to zero -- the same rule
    `catalog.price_of` states for cache reads. Falling back to zero would
    make an uncached provider look free. The notes exist because a fallback
    that is silently correct-looking is how audio tokens were billed at the
    text rate for a quarter (capabilities/voice-openai.md §6).
    """
    extra = extra or {}
    tool_calls = tool_calls or {}
    notes: list[str] = []

    input_rate = spec.input_per_m
    cache_read_rate = price_of(spec, cached=True)
    cache_write_rate = (
        spec.cache_write_per_m if spec.cache_write_per_m is not None else spec.input_per_m
    )
    cache_write_1h_rate = (
        spec.cache_write_1h_per_m if spec.cache_write_1h_per_m is not None
        else cache_write_rate
    )
    # Audio tokens are a SUBSET of the base counts, not a fifth bucket beside
    # them: OpenAI's `prompt_tokens_details.audio_tokens` sits inside
    # `prompt_tokens`, `completion_tokens_details.audio_tokens` inside
    # `completion_tokens`, and a speech model's whole output is audio. So the
    # text rate applies to what is left after the audio share is taken out,
    # and the audio rate to the share. Pricing both columns in full billed
    # every TTS call twice (Phase D integration, 18 Sep 2026).
    audio_in = extra.get("audio_input_tokens", 0) or 0
    audio_out = extra.get("audio_output_tokens", 0) or 0
    text_in = max(usage.input_tokens - audio_in, 0)
    text_out = max(output_tokens - audio_out, 0)
    per_million = (
        text_in * input_rate
        + usage.cache_read_tokens * cache_read_rate
        + usage.cache_write_tokens * cache_write_rate
        + text_out * spec.output_per_m
    )

    n = extra.get("cache_write_1h_tokens", 0)
    if n:
        if spec.cache_write_1h_per_m is None:
            notes.append("cache_write_1h priced at the 5-minute write rate")
        per_million += n * cache_write_1h_rate

    if audio_in:
        rate = spec.audio_input_per_m
        if rate is None:
            rate = input_rate
            notes.append("audio_input priced at the text input rate")
        per_million += audio_in * rate

    if audio_out:
        rate = spec.audio_output_per_m
        if rate is None:
            rate = spec.output_per_m
            notes.append("audio_output priced at the text output rate")
        per_million += audio_out * rate

    n = extra.get("cached_audio_input_tokens", 0)
    if n:
        rate = spec.cached_audio_input_per_m
        if rate is None:
            rate = cache_read_rate
            notes.append("cached_audio_input priced at the text cache-read rate")
        per_million += n * rate

    if spec.unit == "characters":
        per_million += extra.get("characters", 0) * input_rate
    elif spec.unit == "seconds" and extra.get("seconds", 0):
        if spec.per_minute is None:
            notes.append("seconds reported but the row has no per_minute rate")
        else:
            per_million += extra["seconds"] / 60.0 * spec.per_minute * 1_000_000

    cost = per_million / 1_000_000

    for key, calls in tool_calls.items():
        rate = spec.tool_rates.get(key)
        if rate is None:
            notes.append(f"{calls} {key} call(s) with no rate on the row")
            continue
        cost += calls * rate / 1_000.0

    return cost, tuple(notes)


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
        stop_reason=None,
        cost_notes=("accounting fell back: result was not the expected shape",),
    )

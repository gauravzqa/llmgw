"""Accounting tests: what a request cost, and that the number holds up.

Pure unit tier -- no sockets, no sleeps. Every fixture is built by hand from
the real `ExecutionResult` / `PumpResult` / `Usage` shapes so the test exercises
the same types the executor hands accounting in production. Prices come from
`DEFAULT_CATALOG` so a cost assertion is a real dot product against a real spec,
not a number invented to match the code.
"""

from __future__ import annotations

import types

import pytest

from llmgw import metrics
from llmgw.accounting import TOKEN_KINDS, AccountingRecord, account
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.errors import IncompleteStream, Outcome
from llmgw.executor import AttemptRecord, ExecutionResult
from llmgw.policy import DEFAULT_BUDGETS, ExecutionPlan
from llmgw.pump import PumpResult
from llmgw.surfaces.base import Usage

CATALOG = DEFAULT_CATALOG
SONNET = "anthropic.sonnet-4-6"      # rates read from the catalog row at use
HAIKU = "anthropic.haiku-4-5"        # input 1.00, cached 0.10, out 5.00
QWEN = "openrouter.qwen-3.6-plus"    # input .325, cached .325, cache_write .41, out 1.95


# ------------------------------------------------------------------ builders


def make_plan(*model_ids: str) -> ExecutionPlan:
    return ExecutionPlan(
        policy_id="pol_test",
        workload_id="chat",
        targets=tuple(CATALOG.resolve(m) for m in model_ids),
        budgets=DEFAULT_BUDGETS,
        retry=None,
    )


def make_usage(**kw) -> Usage:
    return Usage(**kw)


def make_pump(usage: Usage, *, committed: bool = True, bytes_out: int = 0,
              terminal_seen: bool = True) -> PumpResult:
    return PumpResult(
        committed=committed,
        bytes_out=bytes_out,
        events=1,
        content_events=1,
        usage=usage,
        terminal_seen=terminal_seen,
        first_event_at=0.0,
        in_stream_error=None,
    )


def make_attempt(target, *, committed: bool, outcome: str = "success",
                 error=None) -> AttemptRecord:
    return AttemptRecord(
        target=target, started_at=0.0, ended_at=1.0, outcome=outcome,
        status=200, error=error, committed=committed,
    )


def make_result(*, plan, served_by=None, pump=None, committed=False,
                outcome=Outcome.COMPLETED, error=None, attempts=None) -> ExecutionResult:
    return ExecutionResult(
        plan=plan,
        attempts=attempts if attempts is not None else [],
        served_by=served_by,
        pump=pump,
        committed=committed,
        outcome=outcome,
        error=error,
        refusals=[],
    )


# --------------------------------------------------------- exact cost is a dot product


def test_exact_usage_cost_is_the_dot_product_over_all_four_buckets():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    usage = make_usage(
        input_tokens=1000, output_tokens=500,
        cache_read_tokens=2000, cache_write_tokens=100,
        input_exact=True, output_exact=True,
    )
    result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                         committed=True)

    rec = account(result, catalog=CATALOG)

    # The dot product over the four buckets at the spec's own rates. Read off
    # the catalog rather than hard-coded, so a price correction (the Anthropic
    # cache-write rate landed with PLAN-2 A5) changes the bill, not the test.
    spec = CATALOG.models[SONNET]
    write_rate = spec.cache_write_per_m
    if write_rate is None:
        write_rate = spec.input_per_m
    expected = (
        1000 * spec.input_per_m + 2000 * spec.cached_input_per_m
        + 100 * write_rate + 500 * spec.output_per_m
    ) / 1_000_000
    assert rec.cost_usd == pytest.approx(expected)
    assert rec.basis == "exact"
    assert rec.provider == "anthropic"
    assert rec.model == SONNET
    assert rec.tokens_by_kind == {
        "input": 1000, "output": 500, "cache_read": 2000, "cache_write": 100,
    }
    assert rec.outcome is Outcome.COMPLETED
    assert rec.code == "none"


def test_cache_write_rate_is_used_when_the_spec_carries_one():
    plan = make_plan(QWEN)
    target = plan.targets[0]
    usage = make_usage(cache_write_tokens=1000, input_exact=True, output_exact=True)
    result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                         committed=True)

    rec = account(result, catalog=CATALOG)

    assert rec.cost_usd == pytest.approx(
        1000 * CATALOG.models[QWEN].cache_write_per_m / 1_000_000
    )


# ------------------------------------------------ cache_read is cheap and not folded in


def test_cache_read_is_priced_below_input_and_is_not_folded_into_input():
    plan = make_plan(HAIKU)
    target = plan.targets[0]
    # No fresh input at all, 1000 cache reads. If cache read were folded into
    # input it would cost 1000*1.00; priced correctly it is 1000*0.10.
    usage = make_usage(input_tokens=0, cache_read_tokens=1000,
                       input_exact=True, output_exact=True)
    result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                         committed=True)

    rec = account(result, catalog=CATALOG)

    haiku = CATALOG.models[HAIKU]
    assert rec.cost_usd == pytest.approx(1000 * haiku.cached_input_per_m / 1_000_000)
    assert rec.cost_usd != pytest.approx(1000 * haiku.input_per_m / 1_000_000)  # not folded in
    assert rec.tokens_by_kind["input"] == 0
    assert rec.tokens_by_kind["cache_read"] == 1000


def test_cache_read_falls_back_to_input_rate_never_zero_when_uncached():
    # llama-4-maverick has cached_input_per_m=None: price_of falls back to input.
    plan = make_plan("openrouter.llama-4-maverick")
    target = plan.targets[0]
    usage = make_usage(cache_read_tokens=1000, input_exact=True, output_exact=True)
    result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                         committed=True)

    rec = account(result, catalog=CATALOG)

    # The fallback must be the row's own input rate, not 0. Read off the
    # catalog: OpenRouter prices moved on 2026-09-16 and will move again.
    maverick = CATALOG.models["openrouter.llama-4-maverick"]
    assert maverick.cached_input_per_m is None, "the test exists for the uncached row"
    assert rec.cost_usd == pytest.approx(1000 * maverick.input_per_m / 1_000_000)
    assert rec.cost_usd > 0


# ------------------------------------------------------- C3: interrupted bills its tokens


def test_interrupted_stream_still_bills_its_tokens_as_estimated():
    """C3. Post-commitment failure: the executor returns served_by=None while
    committed=True and pump carries partial usage. The record must bill it."""
    plan = make_plan(SONNET)
    target = plan.targets[0]
    # Anthropic reported input at message_start (exact) but the stream was cut
    # before the final output count -> output is a floor, not exact.
    usage = make_usage(
        input_tokens=1200, output_tokens=10,
        input_exact=True, output_exact=False,
    )
    pump = make_pump(usage, committed=True, bytes_out=400, terminal_seen=False)
    committed_attempt = make_attempt(
        target, committed=True, outcome="incomplete_stream",
        error=IncompleteStream("cut"),
    )
    result = make_result(
        plan=plan, served_by=None, pump=pump, committed=True,
        outcome=Outcome.INTERRUPTED, error=IncompleteStream("cut"),
        attempts=[committed_attempt],
    )

    rec = account(result, catalog=CATALOG)

    assert rec.basis == "estimated"
    assert rec.outcome is Outcome.INTERRUPTED
    # Priced against the committed attempt's target even though served_by is None.
    assert rec.provider == "anthropic"
    assert rec.model == SONNET
    # Output estimated from bytes_out/4 = 100, above the floor of 10.
    assert rec.output_tokens == 100
    assert rec.input_tokens == 1200
    expected = (1200 * 3.00 + 100 * 15.00) / 1_000_000
    assert rec.cost_usd == pytest.approx(expected)
    assert rec.cost_usd > 0  # the exact bug to avoid: interrupted != zero
    assert rec.committed is True


def test_output_estimate_never_undercuts_the_providers_reported_floor():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    # Provider's running count (200) exceeds the byte estimate (400/4 = 100).
    usage = make_usage(output_tokens=200, input_exact=True, output_exact=False)
    pump = make_pump(usage, committed=True, bytes_out=400, terminal_seen=False)
    result = make_result(plan=plan, served_by=target, pump=pump, committed=True,
                         outcome=Outcome.INTERRUPTED)

    rec = account(result, catalog=CATALOG)

    assert rec.output_tokens == 200  # floor wins over the byte estimate


def test_exact_output_ignores_bytes_out_entirely():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    usage = make_usage(output_tokens=7, input_exact=True, output_exact=True)
    pump = make_pump(usage, committed=True, bytes_out=999999)
    result = make_result(plan=plan, served_by=target, pump=pump, committed=True)

    rec = account(result, catalog=CATALOG)

    assert rec.output_tokens == 7
    assert rec.basis == "exact"


# ------------------------------------------------------- served_by None -> zero, valid


def test_nobody_answered_is_a_valid_zero_record():
    plan = make_plan(SONNET)
    result = make_result(
        plan=plan, served_by=None, pump=None, committed=False,
        outcome=Outcome.FAILED, error=IncompleteStream("nope"),
        attempts=[make_attempt(plan.targets[0], committed=False, outcome="incomplete_stream")],
    )

    rec = account(result, catalog=CATALOG)

    assert rec.provider is None
    assert rec.model is None
    assert rec.cost_usd == 0.0
    assert rec.tokens_by_kind == {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
    # The record is still meaningful: outcome and attempts survive.
    assert rec.outcome is Outcome.FAILED
    assert rec.attempts == 1
    assert rec.committed is False
    assert rec.code == "incomplete_stream"


def test_uncommitted_usage_is_not_billed_to_a_guessed_model():
    # A pre-commitment failure that parsed a usage frame before dying: no bytes
    # reached the client, nothing committed, so there is no billed target.
    plan = make_plan(SONNET)
    usage = make_usage(input_tokens=500, input_exact=True)
    pump = make_pump(usage, committed=False, bytes_out=0, terminal_seen=False)
    result = make_result(
        plan=plan, served_by=None, pump=pump, committed=False,
        outcome=Outcome.FAILED,
        attempts=[make_attempt(plan.targets[0], committed=False, outcome="stall_timeout")],
    )

    rec = account(result, catalog=CATALOG)

    assert rec.model is None
    assert rec.cost_usd == 0.0
    assert rec.input_tokens == 0


# ---------------------------------------------------------- parse_failures -> estimated


def test_parse_failures_still_yield_a_record_flagged_estimated():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    usage = make_usage(
        input_tokens=100, output_tokens=50,
        input_exact=False, output_exact=False, parse_failures=2,
    )
    result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                         committed=True)

    rec = account(result, catalog=CATALOG)

    assert rec.parse_failures == 2
    assert rec.basis == "estimated"
    assert isinstance(rec, AccountingRecord)


# ------------------------------------------------------------------ never raises


def test_account_never_raises_on_a_none_heavy_result():
    broken = types.SimpleNamespace(
        plan=None, attempts=None, served_by=None, pump=None,
        committed=None, outcome=None, error=None,
    )

    rec = account(broken, catalog=CATALOG)  # must not raise

    assert isinstance(rec, AccountingRecord)
    assert rec.basis == "estimated"
    assert rec.outcome is Outcome.FAILED
    assert rec.provider is None and rec.model is None
    assert rec.cost_usd == 0.0
    assert rec.code == "none"


def test_account_never_raises_when_attributes_are_missing_entirely():
    rec = account(object(), catalog=CATALOG)  # no fields at all

    assert isinstance(rec, AccountingRecord)
    assert rec.workload_id == ""
    assert rec.policy_id == ""
    assert rec.basis == "estimated"


def test_account_never_raises_on_a_malformed_pump():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    bad_pump = types.SimpleNamespace(usage="not a usage", bytes_out="lots")
    result = make_result(plan=plan, served_by=target, pump=bad_pump, committed=True)

    rec = account(result, catalog=CATALOG)  # must not raise

    assert isinstance(rec, AccountingRecord)
    assert rec.basis == "estimated"


# ------------------------------------------------------------ contract with metrics


def test_token_kinds_match_the_metrics_contract():
    assert TOKEN_KINDS == metrics.TOKEN_KINDS


def test_basis_values_are_the_cost_basis_vocabulary():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    exact = account(
        make_result(plan=plan, served_by=target, committed=True,
                    pump=make_pump(make_usage(input_exact=True, output_exact=True))),
        catalog=CATALOG,
    )
    estimated = account(
        make_result(plan=plan, served_by=target, committed=True,
                    pump=make_pump(make_usage())),
        catalog=CATALOG,
    )
    assert exact.basis in metrics.COST_BASIS
    assert estimated.basis in metrics.COST_BASIS
    assert {exact.basis, estimated.basis} == {"exact", "estimated"}


def test_code_is_always_a_member_of_the_closed_metrics_set():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    served = account(
        make_result(plan=plan, served_by=target, committed=True,
                    pump=make_pump(make_usage(input_exact=True, output_exact=True))),
        catalog=CATALOG,
    )
    failed = account(
        make_result(plan=plan, outcome=Outcome.FAILED, error=IncompleteStream("x")),
        catalog=CATALOG,
    )
    allowed = set(metrics._CODES)
    assert served.code in allowed
    assert failed.code in allowed

"""Adversarial tests for the P5 wiring seam: `app._record` and `Collectors`.

The pure halves (`accounting.py`, `capture.py`) have their own unit files; this
one attacks the SEAM they meet at -- the `_record` hook the executor calls on
every exit, and the closed-vocabulary emit surface it fans a record onto. The
questions here are the ones a green accounting test cannot answer:

* Does `llmgw_requests_total` increment EXACTLY once per request that entered
  the executor -- and does the amplification land on `attempts_total`, not on
  `requests_total`?
* Does `_record` survive the cancel path -- a `None`-heavy, partial result,
  under a cancellation still propagating -- without raising?
* Does a label value outside the closed vocabulary get caught LOUDLY, so it can
  never mint a phantom parallel series?
* Is the scrape-time delta-mirror idempotent under repeated scrapes with no
  traffic between them, and monotonic when a source ratchets?

`_record` is exercised the way the executor exercises it: synchronously, with a
real `ExecutionResult`, against a real `Collectors` bound into a fresh registry.
"""

from __future__ import annotations

import types

import pytest
from prometheus_client import CollectorRegistry

from llmgw import accounting
from llmgw import metrics as M
from llmgw.capture import Capture, NullSink
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.clocks import SystemClock
from llmgw.errors import IncompleteStream, Outcome
from llmgw.executor import AttemptRecord, ExecutionResult
from llmgw.policy import DEFAULT_BUDGETS, ExecutionPlan
from llmgw.pump import PumpResult
from llmgw.server.app import Exchange, Gateway, PassthroughEndpoint
from llmgw.server.config import ServerConfig
from llmgw.server.telemetry import Collectors
from llmgw.surfaces import OPENAI_CHAT
from llmgw.surfaces.base import Usage

CATALOG = DEFAULT_CATALOG
SONNET = "anthropic.sonnet-4-6"


# --------------------------------------------------------------- builders


def make_plan(*model_ids: str) -> ExecutionPlan:
    return ExecutionPlan(
        policy_id="pol_test",
        workload_id="chat",
        targets=tuple(CATALOG.resolve(m) for m in model_ids),
        budgets=DEFAULT_BUDGETS,
        retry=None,
    )


def make_pump(usage: Usage, *, committed: bool = True, bytes_out: int = 200) -> PumpResult:
    return PumpResult(
        committed=committed, bytes_out=bytes_out, events=1, content_events=1,
        usage=usage, terminal_seen=True, first_event_at=0.5, in_stream_error=None,
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
        plan=plan, attempts=attempts if attempts is not None else [],
        served_by=served_by, pump=pump, committed=committed, outcome=outcome,
        error=error, refusals=[],
    )


def build_endpoint() -> tuple[PassthroughEndpoint, Gateway, Capture]:
    """A gateway with live collectors and a (non-started) capture, plus the
    endpoint that owns `_record`.

    The capture worker is NOT started: `_record` only calls `offer()`, which is
    synchronous and enqueues without a running drain, so a real `Capture` with
    no loop is exactly the object under test for the offer side.
    """
    gw = Gateway(ServerConfig())
    gw._collectors = Collectors(gw.registry)
    gw._capture = Capture(NullSink(), max_queue_bytes=1 << 20, clock=SystemClock())
    ep = PassthroughEndpoint(gw, surface=OPENAI_CHAT, route="/v1/chat/completions")
    return ep, gw, gw._capture


def make_exchange(gw: Gateway) -> Exchange:
    snap = gw.policy.current()
    ex = Exchange(snap, catalog_id="cat_test", workload_id="chat")
    ex.tenant = "acme"
    return ex


def series(collectors: Collectors, name: str, **want: str) -> float:
    """Sum every sample of a collector `name` whose labels superset `want`."""
    obj = collectors._by_name[name]
    total = 0.0
    for metric in obj.collect():
        for s in metric.samples:
            if s.name != name:
                continue
            if all(s.labels.get(k) == v for k, v in want.items()):
                total += s.value
    return total


# ============================================================ exactly-once


def test_requests_total_is_exactly_once_and_attempts_carries_the_amplification():
    """One request, three upstream attempts: `requests_total` moves by 1 and
    `attempts_total` moves by 3. The whole amplification argument (attempts /
    requests) is a lie if the denominator scales with the numerator."""
    ep, gw, cap = build_endpoint()
    plan = make_plan(SONNET)
    target = plan.targets[0]
    usage = Usage(input_tokens=100, output_tokens=50, input_exact=True,
                  output_exact=True)
    attempts = [
        make_attempt(target, committed=False, outcome="connect_timeout"),
        make_attempt(target, committed=False, outcome="connect_timeout"),
        make_attempt(target, committed=True, outcome="success"),
    ]
    result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                         committed=True, outcome=Outcome.COMPLETED, attempts=attempts)

    ep._record(result, exchange=make_exchange(gw), duration_s=1.25)

    c = gw._collectors
    assert series(c, "llmgw_requests_total") == 1.0
    assert series(c, "llmgw_requests_total", outcome="completed", code="none") == 1.0
    assert series(c, "llmgw_attempts_total") == 3.0
    assert series(c, "llmgw_committed_total") == 1.0
    # Tokens and a real cost landed against the one served target.
    assert series(c, "llmgw_tokens_total", kind="output") == 50.0
    assert series(c, "llmgw_cost_usd_total", basis="exact") > 0.0
    # And exactly one capture record was offered.
    assert len(cap._queue) == 1


def test_record_runs_exactly_once_per_call_never_double_counts():
    """Two DIFFERENT requests -> two increments; the hook holds no state that
    would let one request bleed into the next."""
    ep, gw, _cap = build_endpoint()
    plan = make_plan(SONNET)
    target = plan.targets[0]
    usage = Usage(input_tokens=10, output_tokens=5, input_exact=True, output_exact=True)
    for _ in range(2):
        result = make_result(plan=plan, served_by=target, pump=make_pump(usage),
                             committed=True, outcome=Outcome.COMPLETED,
                             attempts=[make_attempt(target, committed=True)])
        ep._record(result, exchange=make_exchange(gw), duration_s=0.5)
    assert series(gw._collectors, "llmgw_requests_total") == 2.0


# ================================================= the cancel / None paths


def test_record_on_the_cancel_path_does_not_raise_and_bills_partial_usage():
    """C3 + C8: a stream cancelled mid-flight. `served_by` is set (the client
    got bytes), the pump carries PARTIAL, inexact usage, the outcome is
    CANCELED and the error is None. `_record` runs synchronously under a
    cancellation still propagating and MUST NOT raise -- and it must still bill
    the partial stream, at estimated basis."""
    ep, gw, cap = build_endpoint()
    plan = make_plan(SONNET)
    target = plan.targets[0]
    # No exact output: the provider never sent a final usage frame.
    usage = Usage(input_tokens=100, output_tokens=0, input_exact=True,
                  output_exact=False)
    result = make_result(
        plan=plan, served_by=target, pump=make_pump(usage, bytes_out=400),
        committed=True, outcome=Outcome.CANCELED, error=None,
        attempts=[make_attempt(target, committed=True, outcome="canceled")],
    )

    ep._record(result, exchange=make_exchange(gw), duration_s=0.9)  # must not raise

    c = gw._collectors
    assert series(c, "llmgw_requests_total", outcome="canceled", code="none") == 1.0
    # Estimated basis, nonzero output from the byte estimate (C3).
    assert series(c, "llmgw_cost_usd_total", basis="estimated") > 0.0
    assert series(c, "llmgw_tokens_total", kind="output") > 0.0
    assert len(cap._queue) == 1


def test_record_when_nobody_answered_raises_nothing_and_bills_nothing():
    """Requirement 5: nothing committed. No served target, no pump. The record
    still counts the request and the failed attempt, but tokens and cost are
    silent -- there is no target to price against."""
    ep, gw, cap = build_endpoint()
    plan = make_plan(SONNET)
    target = plan.targets[0]
    err = IncompleteStream("nobody answered")
    result = make_result(
        plan=plan, served_by=None, pump=None, committed=False,
        outcome=Outcome.FAILED, error=err,
        attempts=[make_attempt(target, committed=False, outcome="connect_timeout",
                               error=err)],
    )

    ep._record(result, exchange=make_exchange(gw), duration_s=0.3)  # must not raise

    c = gw._collectors
    assert series(c, "llmgw_requests_total") == 1.0
    assert series(c, "llmgw_requests_total", outcome="failed") == 1.0
    assert series(c, "llmgw_attempts_total") == 1.0
    # Tokens and cost are silent: nobody delivered bytes.
    assert series(c, "llmgw_tokens_total") == 0.0
    assert series(c, "llmgw_cost_usd_total") == 0.0
    assert series(c, "llmgw_committed_total") == 0.0
    assert len(cap._queue) == 1


def test_record_survives_a_totally_malformed_result_without_raising():
    """The belt to the executor's braces: even a result that trips accounting's
    own fallback must not turn a metrics bug into a request-ending one. The hook
    swallows and logs; the request path is untouched."""
    ep, gw, _cap = build_endpoint()
    junk = types.SimpleNamespace(plan=None, attempts=[], served_by=None, pump=None,
                                 committed=False, outcome=Outcome.FAILED, error=None,
                                 refusals=[])
    # Must not raise even though this is not a real ExecutionResult.
    ep._record(junk, exchange=make_exchange(gw), duration_s=0.1)


# ==================================================== cardinality at runtime


def test_a_label_value_outside_the_closed_vocab_raises_and_mints_no_series():
    """The guard's whole job: a bad value is a loud `ValueError`, never a silent
    `.labels()` that mints a parallel series next to the real one."""
    c = Collectors(CollectorRegistry())
    with pytest.raises(ValueError, match="not one of"):
        c.request(surface="openai_chat", outcome="completed", code="not_a_real_code")
    # Nothing was minted: the counter is still empty.
    assert series(c, "llmgw_requests_total") == 0.0

    with pytest.raises(ValueError, match="not one of"):
        c.tokens(provider="p", model="m", kind="not_a_kind", n=5)
    assert series(c, "llmgw_tokens_total") == 0.0


def test_attempt_result_is_total_and_maps_the_unknown_to_failed():
    """`attempt_result` is the ONE mapping that must never raise on the wire --
    it runs once per attempt on the hot path. An `AttemptRecord.outcome` the
    table never anticipated is a `failed` attempt, not an exception."""
    assert Collectors.attempt_result("success") == "success"
    assert Collectors.attempt_result("canceled") == "canceled"
    assert Collectors.attempt_result("connect_timeout") == "timeout"
    # A value in no table at all -> the total default.
    assert Collectors.attempt_result("a_brand_new_error_code") == "failed"
    assert Collectors.attempt_result("") == "failed"
    # And every value it can return is a real ATTEMPT_RESULTS member.
    for code in ("success", "canceled", "connect_timeout", "unknown", ""):
        assert Collectors.attempt_result(code) in M.ATTEMPT_RESULTS


# ============================================ scrape-mirror idempotency (item 5)


def test_delta_mirror_is_idempotent_and_monotonic_under_repeated_scrapes():
    """`capture_dropped_total` and `admission_denied_total` are projected off a
    cumulative dict by inc-ing the delta at each scrape. Scraping twice with no
    change must add nothing; a source that ratchets adds only the delta; a
    source that somehow went backwards is ignored, never inc'd negative."""
    c = Collectors(CollectorRegistry())

    # First scrape of a source at {queue_full: 3}.
    c.sync_capture_dropped({"queue_full": 3, "sink_error": 0, "shutdown": 0})
    assert series(c, "llmgw_capture_dropped_total", reason="queue_full") == 3.0

    # Second scrape, NOTHING changed: idempotent, the counter does not double.
    c.sync_capture_dropped({"queue_full": 3, "sink_error": 0, "shutdown": 0})
    assert series(c, "llmgw_capture_dropped_total", reason="queue_full") == 3.0

    # The source ratchets to 5: only the delta of 2 is added.
    c.sync_capture_dropped({"queue_full": 5, "sink_error": 1, "shutdown": 0})
    assert series(c, "llmgw_capture_dropped_total", reason="queue_full") == 5.0
    assert series(c, "llmgw_capture_dropped_total", reason="sink_error") == 1.0

    # A backwards source (a reconstructed dict) is IGNORED, never subtracted.
    c.sync_capture_dropped({"queue_full": 2, "sink_error": 1, "shutdown": 0})
    assert series(c, "llmgw_capture_dropped_total", reason="queue_full") == 5.0

    # And it recovers forward from the last high-water mark without a jump.
    c.sync_capture_dropped({"queue_full": 6, "sink_error": 1, "shutdown": 0})
    assert series(c, "llmgw_capture_dropped_total", reason="queue_full") == 6.0


def test_sample_is_a_noop_on_a_gateway_with_nothing_to_report():
    """`sample()` guards each source, so a scrape before any traffic degrades no
    series and raises nothing -- the /metrics handler cannot 500. `capture` is
    None here (no startup), and the guarded capture branch must be a no-op, not
    an AttributeError."""
    gw = Gateway(ServerConfig())
    gw._collectors = Collectors(gw.registry)
    gw._collectors.sample(gw)  # must not raise even with capture None, no upstream
    # The permit gauge was sampled (both scopes present, at zero).
    assert series(gw._collectors, "llmgw_permits_in_use", scope="tenant") == 0.0
    assert series(gw._collectors, "llmgw_permits_in_use", scope="provider_key") == 0.0


# ===================================== the code-vocabulary coupling (reported)


def test_every_code_accounting_can_emit_is_in_the_metrics_vocabulary():
    """Pins the accounting->metrics coupling the wiring trusts (Finding A, fixed).

    `accounting._fallback_record` emits the base `code="gateway_error"` for a
    result too malformed to price. That code is now a member of
    `errors.ERROR_CODES` -- and therefore of `metrics._CODES` -- so `_record`
    calling `collectors.request(code=...)` can no longer be made to raise, and
    the exactly-once guarantee on `llmgw_requests_total` holds even for a
    malformed result rather than silently dropping it. This test is the
    regression tripwire that keeps the base code
    in the vocabulary.
    """
    from types import SimpleNamespace

    bad = SimpleNamespace(error=SimpleNamespace(code=123), outcome=Outcome.FAILED,
                          attempts=[], committed=True)
    rec = accounting.account(bad, catalog=ServerConfig().catalog)
    assert rec.code in M._CODES, (
        f"accounting emitted code={rec.code!r} outside metrics._CODES"
    )

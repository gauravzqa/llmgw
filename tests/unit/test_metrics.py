"""Metric-contract tests. These guard an interface, not behaviour."""

from __future__ import annotations

import pytest

from llmgw import metrics as M
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.errors import ERROR_CODES, Outcome

FORBIDDEN_LABELS = {
    "tenant", "tenant_id", "request_id", "user", "user_id", "api_key",
    "workload", "workload_id", "prompt", "trace_id", "session_id",
}


@pytest.mark.parametrize("spec", M.METRICS, ids=lambda s: s.name)
def test_no_unbounded_labels(spec: M.MetricSpec):
    """The rule that keeps the metrics backend alive: labels are a closed set.
    Anything unbounded belongs in a capture record, where you query it once,
    rather than in a counter, where you store it forever."""
    for label in spec.labels:
        assert label not in FORBIDDEN_LABELS, (
            f"{spec.name} labels by {label!r}, which is unbounded. "
            "Put it in a capture record."
        )
    # series_estimate raises for any label with no bounded value set.
    spec.series_estimate(providers=10, models=40)


def test_projected_cardinality_is_within_budget_at_the_design_point():
    providers, models = M.DESIGN_POINT
    total = M.total_series(providers=providers, models=models)
    assert total < M.CARDINALITY_BUDGET, (
        f"{total} series at the design point. Worst offenders: "
        f"{M.series_by_metric(providers=providers, models=models)[:3]}"
    )


def test_todays_catalog_is_comfortably_inside_the_design_point():
    assert len(DEFAULT_CATALOG.providers) <= M.DESIGN_POINT[0]
    assert len(DEFAULT_CATALOG.models) <= M.DESIGN_POINT[1]


@pytest.mark.parametrize(
    "spec", [m for m in M.METRICS if m.kind == "histogram"], ids=lambda s: s.name
)
def test_histograms_stay_within_the_label_limit(spec: M.MetricSpec):
    """A histogram costs ~18 series per label combination against a counter's
    1, so it is always the first metric to blow a budget. Counters may carry
    (provider, model) freely; histograms have to earn it."""
    assert len(spec.labels) <= M.HISTOGRAM_LABEL_LIMIT, (
        f"{spec.name} has {len(spec.labels)} labels; "
        f"at {M.DESIGN_POINT} that is "
        f"{spec.series_estimate(providers=M.DESIGN_POINT[0], models=M.DESIGN_POINT[1])} series"
    )


def test_catalog_scale_growth_is_documented_not_accidental():
    """Not an assertion about being under budget -- at catalog scale we are
    not, by roughly 3x, and pretending otherwise would be the lie. This test
    pins the shape of the growth so that a future change which makes it worse
    shows up as a diff someone has to justify."""
    big = M.series_by_metric(providers=25, models=200)
    total = sum(n for n, _ in big)
    assert total > M.CARDINALITY_BUDGET      # yes, catalog scale blows it
    assert big[0][1] == "llmgw_time_to_first_event_seconds"
    # One histogram is the largest single line item. It was half of the
    # total until PLAN-2 B3 added five token kinds, two unit kinds and
    # three tool kinds as (provider, model) counters; still the biggest.
    assert big[0][0] / total > 0.3


def test_outcome_labels_match_the_error_module_exactly():
    """One increment per request means the outcome vocabulary must be
    exhaustive; a missing value would silently drop requests from the
    denominator of every ratio built on it."""
    assert set(M.OUTCOMES) == {o.value for o in Outcome}


def test_code_labels_cover_every_error_plus_the_success_case():
    codes = set(next(m for m in M.METRICS
                     if m.name == "llmgw_requests_total").label_values[2])
    assert codes == ERROR_CODES | {"none"}


def test_metric_names_are_unique_and_prefixed():
    names = [m.name for m in M.METRICS]
    assert len(names) == len(set(names))
    assert all(n.startswith("llmgw_") for n in names)


def test_histograms_declare_buckets():
    for m in M.METRICS:
        if m.kind == "histogram":
            assert m.buckets, f"{m.name} has no buckets"


def test_latency_buckets_extend_past_a_long_stream():
    """Prometheus defaults top out at 10s, which would put every streaming
    request in +Inf -- making the histogram useless for exactly the requests
    that matter most here."""
    assert max(M.LATENCY_BUCKETS) >= 300.0


def test_ttfe_buckets_can_see_gateway_overhead():
    """The gateway's own added latency is a couple of milliseconds. A bucket
    edge at 0.1s would hide the entire quantity this project exists to
    measure."""
    assert min(M.TTFE_BUCKETS) <= 0.001
    assert sum(1 for b in M.TTFE_BUCKETS if b <= 0.05) >= 6

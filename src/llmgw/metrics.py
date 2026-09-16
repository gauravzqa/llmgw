"""The metric contract: names, label sets, and a cardinality budget.

Declared in P0 and registered in P5. Declaring first is not bureaucracy --
metric names are an interface. Dashboards, alerts and runbooks bind to them,
so renaming one later breaks things that are not in this repository. Cheaper
to argue about the names before anything depends on them.

--------------------------------------------------------------------------
Why cardinality is a budget and not a detail
--------------------------------------------------------------------------

A Prometheus time series exists for every distinct combination of label
values. Add `tenant_id` to a counter with 1,000 tenants and 27 error codes and
3 surfaces and you have just asked the scrape target to hold 81,000 series for
one metric. The failure mode is not a warning -- it is the metrics backend
falling over, or the gateway's own memory climbing until it is the outage.

So the rule here is blunt:

    LABELS ARE A CLOSED SET. Anything unbounded goes in a capture record.

`tenant_id`, `request_id`, `workload_id`, and any user-supplied string are
therefore absent from every label set below. They are exactly the fields you
want when investigating one request, which is what capture.py is for --
high-cardinality per-request facts belong in a log you query, not a counter
you scrape. Metrics answer "how many"; captures answer "which one".

`CARDINALITY_BUDGET` makes that checkable: a unit test asserts the projected
series count stays under the budget, so adding a label is a decision someone
has to make on purpose.

--------------------------------------------------------------------------
Terminal outcomes, not just errors
--------------------------------------------------------------------------

`llmgw_requests_total` is labelled by `outcome`, and the five outcomes are
mutually exclusive and exhaustive: every request increments it exactly once.
That property is what makes the metric answerable. A gateway that counts
`errors_total` and `requests_total` separately cannot tell you what fraction
of streams were interrupted after commitment -- which is the number that
actually correlates with users complaining, because a truncated answer looks
like a bug to them and like a success to a 2xx-based SLO.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .errors import ERROR_CODES, Outcome

MetricKind = Literal["counter", "gauge", "histogram"]

# Closed label vocabularies. Every label value used at runtime must come from
# one of these, and the registration code in P5 asserts it.
SURFACES: tuple[str, ...] = ("openai_chat", "openai_responses", "anthropic_messages")
OUTCOMES: tuple[str, ...] = tuple(o.value for o in Outcome)
ATTEMPT_RESULTS: tuple[str, ...] = (
    "success",
    "failed",
    "timeout",
    "rejected",
    "breaker_open",
    "canceled",
)
BREAKER_STATES: tuple[str, ...] = ("closed", "open", "half_open")
DENIAL_REASONS: tuple[str, ...] = (
    "tenant_rate",
    "tenant_concurrency",
    "provider_key_concurrency",
    "tpm_reservation",
    "draining",
    # Per-process stream cap (`ServerConfig.max_streams`): the process-wide
    # shed, next to `draining`, and like it not a tenant's budget.
    "overloaded",
)
TOKEN_KINDS: tuple[str, ...] = ("input", "output", "cache_read", "cache_write")
COST_BASIS: tuple[str, ...] = ("exact", "estimated")

# Sized for an LLM gateway, not for a web service. The default Prometheus
# buckets top out at 10s, which would put every streaming request in +Inf and
# make the histogram useless for exactly the requests we care most about.
LATENCY_BUCKETS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    10.0, 30.0, 60.0, 120.0, 300.0,
)
# Time-to-first-event lives in a much tighter range and deserves fine
# resolution at the bottom: the gateway's own overhead is a couple of
# milliseconds, and a bucket edge at 0.1s would hide it entirely.
TTFE_BUCKETS: tuple[float, ...] = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.0, 5.0, 10.0, 20.0, 60.0,
)
GAP_BUCKETS: tuple[float, ...] = (
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0,
)


@dataclass(frozen=True, slots=True)
class MetricSpec:
    name: str
    kind: MetricKind
    help: str
    labels: tuple[str, ...] = ()
    buckets: tuple[float, ...] | None = None
    label_values: tuple[tuple[str, ...], ...] = ()
    """Per-label allowed values, in label order. Empty tuple = bounded by the
    catalog (providers, models), which is closed but not known here."""

    def series_estimate(self, *, providers: int, models: int) -> int:
        """Worst-case series count for cardinality budgeting."""
        total = 1
        for i, label in enumerate(self.labels):
            if i < len(self.label_values) and self.label_values[i]:
                total *= len(self.label_values[i])
            elif label == "provider":
                total *= providers
            elif label == "model":
                total *= models
            else:  # pragma: no cover - guarded by test_metrics
                raise ValueError(
                    f"{self.name}: label {label!r} has no bounded value set. "
                    "Unbounded labels are forbidden; put it in a capture record."
                )
        # A histogram materialises one series per bucket, plus _sum and _count.
        if self.kind == "histogram" and self.buckets:
            total *= len(self.buckets) + 3
        return total


_CODES = tuple(sorted(ERROR_CODES)) + ("none",)

METRICS: tuple[MetricSpec, ...] = (
    # ---- request lifecycle -------------------------------------------------
    MetricSpec(
        "llmgw_requests_total",
        "counter",
        "Terminal outcomes. Exactly one increment per request, ever.",
        labels=("surface", "outcome", "code"),
        label_values=(SURFACES, OUTCOMES, _CODES),
    ),
    MetricSpec(
        "llmgw_request_duration_seconds",
        "histogram",
        "Ingress to terminal record, including retries and waits.",
        labels=("surface", "outcome"),
        buckets=LATENCY_BUCKETS,
        label_values=(SURFACES, OUTCOMES),
    ),
    MetricSpec(
        "llmgw_committed_total",
        "counter",
        "Requests where at least one byte reached the client. The denominator "
        "for 'interrupted' -- an interruption rate over ALL requests understates "
        "what users experienced, because uncommitted failures were invisible to them.",
        labels=("surface",),
        label_values=(SURFACES,),
    ),
    MetricSpec(
        "llmgw_streams_open",
        "gauge",
        "Currently open client streams. The x-axis of every capacity graph.",
        labels=("surface",),
        label_values=(SURFACES,),
    ),
    # ---- attempts ----------------------------------------------------------
    MetricSpec(
        "llmgw_attempts_total",
        "counter",
        "Upstream attempts. Divided by requests_total this is the amplification "
        "factor -- the number that tells you whether the gateway is helping the "
        "provider through an incident or feeding it.",
        labels=("provider", "model", "result"),
        label_values=((), (), ATTEMPT_RESULTS),
    ),
    MetricSpec(
        "llmgw_time_to_first_event_seconds",
        "histogram",
        "Upstream request sent to first content event.",
        labels=("provider", "model"),
        buckets=TTFE_BUCKETS,
    ),
    MetricSpec(
        "llmgw_inter_event_gap_seconds",
        "histogram",
        "Gap between consecutive content events. Sampled, not per-event: "
        "observing every gap on a 40 ev/s stream costs more than serving it.",
        labels=("provider",),
        buckets=GAP_BUCKETS,
        label_values=((),),
    ),
    MetricSpec(
        "llmgw_upstream_connections",
        "gauge",
        "Open upstream connections per provider. Compare against fd limits.",
        labels=("provider",),
    ),
    # ---- admission and isolation ------------------------------------------
    MetricSpec(
        "llmgw_admission_denied_total",
        "counter",
        "Requests shed before any upstream work.",
        labels=("reason",),
        label_values=(DENIAL_REASONS,),
    ),
    MetricSpec(
        "llmgw_permits_in_use",
        "gauge",
        "Concurrency permits held. Must return to zero when idle; the chaos "
        "tier asserts exactly that, because a permit leak is invisible until "
        "the gateway stops accepting work for no visible reason.",
        labels=("scope",),
        label_values=(("tenant", "provider_key"),),
    ),
    # ---- recovery ----------------------------------------------------------
    MetricSpec(
        "llmgw_breaker_state",
        "gauge",
        "0 closed, 1 open, 2 half-open, per target.",
        labels=("provider", "model"),
    ),
    MetricSpec(
        "llmgw_breaker_transitions_total",
        "counter",
        "State changes. A high rate means flapping, which is worse than either "
        "steady state.",
        labels=("provider", "model", "to"),
        label_values=((), (), BREAKER_STATES),
    ),
    # ---- backpressure ------------------------------------------------------
    MetricSpec(
        "llmgw_pump_buffered_bytes",
        "gauge",
        "Bytes held between upstream read and client write, summed. In BYTES, "
        "not messages: 200 streams each holding one 8 MiB event is 1.6 GiB and "
        "a message-counting gauge would read a reassuring 200.",
        labels=(),
    ),
    MetricSpec(
        "llmgw_client_stall_seconds",
        "histogram",
        "How long the pump waited on a client that stopped reading.",
        labels=("surface",),
        buckets=GAP_BUCKETS,
        label_values=(SURFACES,),
    ),
    # ---- capture -----------------------------------------------------------
    MetricSpec(
        "llmgw_capture_queue_bytes",
        "gauge",
        "Bytes queued for the capture sink. Bounded by construction.",
        labels=(),
    ),
    MetricSpec(
        "llmgw_capture_dropped_total",
        "counter",
        "Capture records dropped. Non-zero is acceptable and expected under "
        "load; blocking the request path to avoid it is not.",
        labels=("reason",),
        label_values=(("queue_full", "sink_error", "shutdown"),),
    ),
    # ---- accounting --------------------------------------------------------
    MetricSpec(
        "llmgw_usage_parse_failures_total",
        "counter",
        "Usage frames a surface could not read. Exists because apply_usage() is "
        "forbidden from raising -- observability must not break the request path -- "
        "and without a counter a provider that changes its usage shape silently "
        "turns every request into `estimated` and nothing notices.",
        labels=("surface",),
        label_values=(SURFACES,),
    ),
    MetricSpec(
        "llmgw_tokens_total",
        "counter",
        "Tokens by kind. cache_read separated because it is 10-100x cheaper "
        "and folding it into input makes effective cost unknowable.",
        labels=("provider", "model", "kind"),
        label_values=((), (), TOKEN_KINDS),
    ),
    MetricSpec(
        "llmgw_cost_usd_total",
        "counter",
        "Cost, split by whether usage was reported or inferred. An interrupted "
        "stream has no usage chunk, so its cost is estimated -- and a bill that "
        "cannot distinguish the two is a bill you cannot defend.",
        labels=("provider", "model", "basis"),
        label_values=((), (), COST_BASIS),
    ),
    # ---- lifecycle ---------------------------------------------------------
    MetricSpec(
        "llmgw_draining",
        "gauge",
        "1 while draining. Readiness, not liveness: /healthz goes 503 the "
        "instant this flips, but open streams keep running.",
        labels=(),
    ),
    MetricSpec(
        "llmgw_tasks",
        "gauge",
        "len(asyncio.all_tasks()). Baseline drift after a run is a leak, and "
        "this gauge is how the scale tier proves there is not one.",
        labels=(),
    ),
)

CARDINALITY_BUDGET = 60_000
"""Series budget at the design point. Not a Prometheus limit -- a design
limit. Exceeding it means a label was added that should have been a capture
field, and the test that guards it should fail loudly."""

DESIGN_POINT = (10, 40)
"""(providers, models) ACTIVELY ROUTED TO, not catalog size.

The distinction matters and it is easy to get wrong. A Prometheus client
creates a series the first time a label combination is actually observed, so
a 200-model catalog where a workload only ever routes to 6 of them costs 6
targets' worth of series, not 200. Budgeting against catalog size therefore
over-counts by an order of magnitude and pushes you into deleting metrics you
need.

Budgeting against ACTIVE targets is the honest bound -- with the caveat that
"active" is a runtime property, so a routing change can quietly move you.
That is precisely why the rule below exists rather than a bare number.
"""

HISTOGRAM_LABEL_LIMIT = 2
"""The rule that keeps growth linear.

A histogram materialises one series per bucket plus _sum and _count -- 18
series per label combination for our latency buckets, against 1 for a counter.
So a histogram labelled by (provider, model) costs 18x what the same labels
cost on a counter, and it is the first thing to blow a budget.

At catalog scale (25 providers, 200 models) the metrics below project to
roughly 171,000 series, and llmgw_time_to_first_event_seconds alone is 90,000
of them. That is the number this rule exists to keep you aware of: counters
may carry (provider, model) freely; histograms carry it only when the
per-model distribution is genuinely the thing you route on -- which for
time-to-first-event it is, and for inter-event gap it is not, which is why
that one drops `model`.
"""


def total_series(*, providers: int, models: int) -> int:
    """Worst case: every (provider, model) combination observed at least once."""
    return sum(m.series_estimate(providers=providers, models=models) for m in METRICS)


def series_by_metric(*, providers: int, models: int) -> list[tuple[int, str]]:
    """Sorted descending, for finding what is actually expensive."""
    return sorted(
        ((m.series_estimate(providers=providers, models=models), m.name)
         for m in METRICS),
        reverse=True,
    )


METRIC_NAMES: frozenset[str] = frozenset(m.name for m in METRICS)

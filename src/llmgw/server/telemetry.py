"""The live collector layer: `metrics.METRICS` turned into Prometheus objects.

`metrics.py` is the PURE contract -- names, label vocabularies, bucket tuples,
a cardinality budget -- and it imports no `prometheus_client` on purpose: the
contract is asserted by `test_metrics.py` in microseconds, and a pure module is
one a dashboard author can read without a metrics backend installed. This
module is the other half: it builds one `Counter`/`Gauge`/`Histogram` per
`MetricSpec` into a registry and hands the serving path *typed* emit methods to
call.

Why a sibling module and not a `Collectors` class inside `metrics.py`:
`metrics.py`'s single most valuable property is that it does not depend on
prometheus, and folding the live layer in would spend that property for the
sake of one fewer file. `app.py` already imports `prometheus_client`, so the
live layer lives beside it, in the `server` package, where the dependency
already is.

--------------------------------------------------------------------------
Typed methods, not a stringly-typed dict
--------------------------------------------------------------------------

The emit surface is one method per metric (`request()`, `tokens()`,
`breaker_transition()`, ...), not `emit(name, labels, value)`. A dict keyed by
string would move every label typo from a failed import to a silent second
time series that never lines up with the one the dashboard queries. The methods
also VALIDATE every closed-vocabulary label value against `metrics.py`'s
vocabularies and raise on a miss -- because Prometheus will happily mint a
series for `outcome="complete"` next to the real `outcome="completed"` and
nothing will look wrong until someone sums a ratio that no longer adds to one.
`provider`/`model` are the exception: they are bounded by the catalog, which is
closed but not knowable here, so they are passed through unchecked.

The raise is loud where loud is safe -- a unit test, a `/metrics` handler that
catches -- and the one hot path that must never raise (`app._record`, on the
cancellation path) wraps its whole body, so a bug there is a logged incident
about accounting, never a dropped request.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

from llmgw import metrics as M

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.server.app import Gateway

log = logging.getLogger("llmgw.telemetry")

# The two `scope` values of `llmgw_permits_in_use`, spelled out so the sampler
# cannot emit a third by accident. Kept equal by construction to the metric's
# declared label_values in metrics.py.
_PERMIT_SCOPES: tuple[str, ...] = ("tenant", "provider_key")
# The label values of `llmgw_capture_dropped_total`, equal by construction to
# `capture.DROP_REASONS` -- the coupling capture.py's docstring relies on.
_DROP_REASONS: tuple[str, ...] = ("queue_full", "sink_error", "shutdown")


def _attempt_result(outcome: str) -> str:
    """Map an `AttemptRecord.outcome` onto an `ATTEMPT_RESULTS` value.

    `AttemptRecord.outcome` is `"success"`, `"canceled"`, or a member of
    `errors.ERROR_CODES` -- a vocabulary wider than the six the metric carries,
    because `llmgw_attempts_total` answers "how did attempts end" at the
    resolution an operator acts on (retry? shed? wait for a provider?), not at
    the resolution of every distinct error class. This is where the two are
    reconciled, once, so the mapping is a thing you can read and correct in one
    place rather than a `dict.get` scattered across the wiring.

    Total by construction: anything unmapped is `"failed"`, because an attempt
    that ended in a way this table did not anticipate is exactly a failed
    attempt, and inventing a seventh label value to say "I did not classify
    this" would be a series with no dashboard.
    """
    return _ATTEMPT_RESULT_BY_CODE.get(outcome, "failed")


# Explicit and total. The default in `_attempt_result` is `"failed"`, so only
# the codes that mean something other than "a plain failure" need an entry.
_ATTEMPT_RESULT_BY_CODE: dict[str, str] = {
    "success": "success",
    "canceled": "canceled",
    "client_disconnected": "canceled",
    "breaker_open": "breaker_open",
    "admission_rejected": "rejected",
    "concurrency_rejected": "rejected",
    "provider_key_exhausted": "rejected",
    "no_targets_available": "rejected",
    "connect_timeout": "timeout",
    "headers_timeout": "timeout",
    "first_event_timeout": "timeout",
    "stall_timeout": "timeout",
    "total_deadline_exceeded": "timeout",
}


class Collectors:
    """Live Prometheus collectors for the whole metric contract.

    One instance per process, built into `Gateway.registry` at startup. Holds a
    concrete `Counter`/`Gauge`/`Histogram` per `MetricSpec` and exposes a typed
    method per metric the serving path emits to. The build order is the
    `metrics.METRICS` order -- the registration order is part of the interface a
    scrape binds to, so it is matched, not re-derived.
    """

    __slots__ = ("_by_name", "_mirror")

    def __init__(self, registry: CollectorRegistry) -> None:
        self._by_name: dict[str, object] = {}
        # Per-mirrored-counter last-seen totals, so a cumulative dict owned by
        # another module (admission.denials(), capture.dropped) can be projected
        # onto a real Counter by inc-ing the DELTA at each scrape. See `_sync`.
        self._mirror: dict[str, dict[str, int]] = {}

        for spec in M.METRICS:
            if spec.kind == "counter":
                obj: object = Counter(
                    spec.name, spec.help, labelnames=spec.labels, registry=registry
                )
            elif spec.kind == "gauge":
                obj = Gauge(
                    spec.name, spec.help, labelnames=spec.labels, registry=registry
                )
            elif spec.kind == "histogram":
                obj = Histogram(
                    spec.name, spec.help, labelnames=spec.labels,
                    buckets=spec.buckets, registry=registry,
                )
            else:  # pragma: no cover - MetricKind is closed
                raise ValueError(f"{spec.name}: unknown kind {spec.kind!r}")
            self._by_name[spec.name] = obj

    # ---- validation --------------------------------------------------------

    @staticmethod
    def _one_of(value: str, allowed: tuple[str, ...], label: str, metric: str) -> str:
        """Return `value` iff it is in the closed vocabulary, else raise loudly.

        The alternative to raising is a silent parallel series, which is the
        exact failure this whole layer exists to prevent -- so a bad value is a
        `ValueError` an operator or a test sees, never a `.labels()` call that
        quietly succeeds."""
        if value not in allowed:
            raise ValueError(
                f"{metric}: {label}={value!r} is not one of {allowed}"
            )
        return value

    # ---- request lifecycle -------------------------------------------------

    def request(self, *, surface: str, outcome: str, code: str) -> None:
        """`llmgw_requests_total`. The one exactly-once-per-request metric."""
        self._by_name["llmgw_requests_total"].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", "llmgw_requests_total"),
            outcome=self._one_of(outcome, M.OUTCOMES, "outcome", "llmgw_requests_total"),
            code=self._one_of(code, M._CODES, "code", "llmgw_requests_total"),
        ).inc()

    def request_duration(self, *, surface: str, outcome: str, seconds: float) -> None:
        """`llmgw_request_duration_seconds`: ingress to terminal record."""
        m = "llmgw_request_duration_seconds"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
            outcome=self._one_of(outcome, M.OUTCOMES, "outcome", m),
        ).observe(max(0.0, seconds))

    def committed(self, *, surface: str) -> None:
        """`llmgw_committed_total`: a byte reached the client."""
        m = "llmgw_committed_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).inc()

    def stream_open(self, *, surface: str) -> None:
        """`llmgw_streams_open` += 1. Paired with `stream_close` in a finally."""
        m = "llmgw_streams_open"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).inc()

    def stream_close(self, *, surface: str) -> None:
        """`llmgw_streams_open` -= 1. The other half of the pair."""
        m = "llmgw_streams_open"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).dec()

    # ---- the socket plane (PLAN-G 7.3) -------------------------------------
    #
    # Five families, one method each, and every one of them is called from a
    # `finally` or from a place that cannot raise into the relay: a metric
    # that can fail a session is worse than a metric that is missing.

    def ws_session_open(self, *, surface: str) -> None:
        """`llmgw_ws_sessions_open` += 1. Paired with `ws_session_close`."""
        m = "llmgw_ws_sessions_open"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).inc()

    def ws_session_close(self, *, surface: str) -> None:
        """`llmgw_ws_sessions_open` -= 1. The other half of the pair."""
        m = "llmgw_ws_sessions_open"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).dec()

    def ws_bytes(self, *, surface: str, direction: str, n: int) -> None:
        """`llmgw_ws_bytes_total`. `n` is bytes on the wire, after framing."""
        if n <= 0:
            return
        m = "llmgw_ws_bytes_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
            direction=self._one_of(direction, M.WS_DIRECTIONS, "direction", m),
        ).inc(n)

    def ws_close(self, *, surface: str, side: str, code_class: str) -> None:
        """`llmgw_ws_close_total`. `code_class` comes from
        `llmgw.ws.errors.close_code_class`, never from a raw code."""
        m = "llmgw_ws_close_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
            side=self._one_of(side, M.WS_CLOSE_SIDES, "side", m),
            code_class=self._one_of(code_class, M.WS_CLOSE_CLASSES, "code_class", m),
        ).inc()

    def ws_inband_error(self, *, surface: str, fatal: bool) -> None:
        """`llmgw_ws_inband_errors_total`: one provider error FRAME."""
        m = "llmgw_ws_inband_errors_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
            fatal="true" if fatal else "false",
        ).inc()

    def ws_session_seconds(self, *, surface: str, seconds: float) -> None:
        """`llmgw_ws_session_seconds`: one session's lifetime, at its close."""
        m = "llmgw_ws_session_seconds"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).observe(max(0.0, seconds))

    # ---- attempts ----------------------------------------------------------

    def attempt(self, *, provider: str, model: str, result: str) -> None:
        """`llmgw_attempts_total`. `result` is already an `ATTEMPT_RESULTS`
        value -- callers map through `attempt_result()` first."""
        m = "llmgw_attempts_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
            result=self._one_of(result, M.ATTEMPT_RESULTS, "result", m),
        ).inc()

    @staticmethod
    def attempt_result(outcome: str) -> str:
        """`AttemptRecord.outcome` -> `ATTEMPT_RESULTS`. See `_attempt_result`."""
        return _attempt_result(outcome)

    def time_to_first_event(self, *, provider: str, model: str, seconds: float) -> None:
        """`llmgw_time_to_first_event_seconds`. Upstream sent to first content."""
        self._by_name["llmgw_time_to_first_event_seconds"].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
        ).observe(max(0.0, seconds))

    # ---- accounting --------------------------------------------------------

    def tokens(self, *, provider: str, model: str, kind: str, n: int) -> None:
        """`llmgw_tokens_total` for one token kind."""
        if n <= 0:
            return
        m = "llmgw_tokens_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
            kind=self._one_of(kind, M.TOKEN_KINDS, "kind", m),
        ).inc(n)

    def units(self, *, provider: str, model: str, unit: str, n: int) -> None:
        """`llmgw_units_total`: characters or seconds billed (PLAN-2 B3)."""
        if n <= 0:
            return
        m = "llmgw_units_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
            unit=self._one_of(unit, M.UNITS, "unit", m),
        ).inc(n)

    def server_tool_calls(self, *, provider: str, model: str, tool: str, n: int) -> None:
        """`llmgw_server_tool_calls_total`. A tool kind outside `TOOL_KINDS`
        is dropped from the metric (the capture record still has it); the
        closed set is the point."""
        if n <= 0 or tool not in M.TOOL_KINDS:
            return
        m = "llmgw_server_tool_calls_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            provider=provider, model=model, tool=tool,
        ).inc(n)

    def cost(self, *, provider: str, model: str, basis: str, usd: float) -> None:
        """`llmgw_cost_usd_total`, split by exact/estimated basis."""
        if usd <= 0:
            return
        m = "llmgw_cost_usd_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
            basis=self._one_of(basis, M.COST_BASIS, "basis", m),
        ).inc(usd)

    def usage_parse_failures(self, *, surface: str, n: int) -> None:
        """`llmgw_usage_parse_failures_total`: usage frames a surface could not
        read. Non-zero is why an apparently complete stream is `estimated`."""
        if n <= 0:
            return
        m = "llmgw_usage_parse_failures_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
        ).inc(n)

    def stop_reason(self, *, surface: str, stop_reason: str) -> None:
        """`llmgw_stop_reason_total`: why a completed response ended, already
        folded onto `STOP_REASONS` by `metrics.normalize_stop_reason`."""
        m = "llmgw_stop_reason_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            surface=self._one_of(surface, M.SURFACES, "surface", m),
            stop_reason=self._one_of(stop_reason, M.STOP_REASONS, "stop_reason", m),
        ).inc()

    # ---- provider signals --------------------------------------------------

    def queued_at_provider(self, *, provider: str, model: str) -> None:
        """`llmgw_queued_at_provider_total`: a first-event timeout that fired
        after the provider had shown liveness (a queue, not an outage)."""
        self._by_name["llmgw_queued_at_provider_total"].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
        ).inc()

    def provider_ratelimit(
        self, *, credential: str, kind: str,
        remaining: float | None, reset_seconds: float | None,
    ) -> None:
        """The two `llmgw_provider_ratelimit_*` gauges for one credential and
        budget kind. Either half may be absent; a header a provider did not
        send leaves the gauge where it was."""
        kind = self._one_of(kind, M.RATELIMIT_KINDS, "kind",
                            "llmgw_provider_ratelimit_remaining")
        if remaining is not None:
            self._by_name["llmgw_provider_ratelimit_remaining"].labels(  # type: ignore[attr-defined]
                credential=credential, kind=kind,
            ).set(remaining)
        if reset_seconds is not None:
            self._by_name["llmgw_provider_ratelimit_reset_seconds"].labels(  # type: ignore[attr-defined]
                credential=credential, kind=kind,
            ).set(max(0.0, reset_seconds))

    # ---- recovery ----------------------------------------------------------

    def breaker_transition(self, *, provider: str, model: str, to: str) -> None:
        """`llmgw_breaker_transitions_total`. `to` is a `BREAKER_STATES` value."""
        m = "llmgw_breaker_transitions_total"
        self._by_name[m].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
            to=self._one_of(to, M.BREAKER_STATES, "to", m),
        ).inc()

    def breaker_state(self, *, provider: str, model: str, value: int) -> None:
        """`llmgw_breaker_state`: 0 closed / 1 open / 2 half-open, per target.
        `value` comes from `breaker.BreakerState.gauge_value`."""
        self._by_name["llmgw_breaker_state"].labels(  # type: ignore[attr-defined]
            provider=provider, model=model,
        ).set(value)

    # ---- gauges and mirrored counters, sampled at scrape -------------------
    #
    # These have no per-event emit site: their source is a number another module
    # already maintains (a permit count, a queue's byte total, a cumulative drop
    # dict). Sampling them in the /metrics handler -- rather than a background
    # task -- is deliberate: a sampler task would itself perturb `llmgw_tasks`,
    # which is the one gauge that counts tasks.

    def set_draining(self, draining: bool) -> None:
        self._by_name["llmgw_draining"].set(1 if draining else 0)  # type: ignore[attr-defined]

    def set_tasks(self, n: int) -> None:
        self._by_name["llmgw_tasks"].set(n)  # type: ignore[attr-defined]

    def set_permits_in_use(self, *, tenant: int, provider_key: int) -> None:
        g = self._by_name["llmgw_permits_in_use"]
        g.labels(scope="tenant").set(tenant)  # type: ignore[attr-defined]
        g.labels(scope="provider_key").set(provider_key)  # type: ignore[attr-defined]

    def set_upstream_connections(self, per_provider: dict[str, int]) -> None:
        g = self._by_name["llmgw_upstream_connections"]
        for provider, n in per_provider.items():
            g.labels(provider=provider).set(n)  # type: ignore[attr-defined]

    def set_capture_queue_bytes(self, n: int) -> None:
        self._by_name["llmgw_capture_queue_bytes"].set(n)  # type: ignore[attr-defined]

    def sync_admission_denied(self, totals: dict[str, int]) -> None:
        """Project the cumulative `admission.denials()` + `limiter.denials()`
        dict onto `llmgw_admission_denied_total` by inc-ing the delta."""
        self._sync("llmgw_admission_denied_total", "reason", M.DENIAL_REASONS, totals)

    def sync_capture_dropped(self, totals: dict[str, int]) -> None:
        """Project the cumulative `capture.dropped` dict onto
        `llmgw_capture_dropped_total` by inc-ing the delta."""
        self._sync("llmgw_capture_dropped_total", "reason", _DROP_REASONS, totals)

    def _sync(
        self, name: str, label: str, allowed: tuple[str, ...], totals: dict[str, int]
    ) -> None:
        """Inc a Counter to match an externally-owned cumulative total.

        A Counter has no `set`, and it should not -- a metric that can go
        backwards is not a counter. So the delta since the last scrape is what
        is added, and the last-seen totals are remembered here. A total that
        did not move adds nothing; one that somehow went backwards (a
        reconstructed source) is ignored rather than trusted to `inc` a negative,
        which prometheus_client refuses anyway."""
        m = self._by_name[name]
        last = self._mirror.setdefault(name, {})
        for key, total in totals.items():
            self._one_of(key, allowed, label, name)
            delta = total - last.get(key, 0)
            if delta > 0:
                m.labels(**{label: key}).inc(delta)  # type: ignore[attr-defined]
                last[key] = total

    # ---- the scrape-time sweep --------------------------------------------

    def sample(self, gateway: Gateway) -> None:
        """Refresh every sampled gauge and mirrored counter, once, at scrape.

        Called from the `/metrics` handler immediately before
        `generate_latest`. Each source is guarded independently: a metric is
        never worth a 500 on the endpoint an on-call engineer is scraping to
        find out why, so a source that raises degrades that one series rather
        than the scrape.
        """
        try:
            self.set_draining(gateway.draining)
        except Exception:  # noqa: BLE001 - a metric is not worth a failed scrape
            log.exception("failed to sample draining")
        try:
            # Counts this handler's own task too, which is honest: it is a task
            # that is currently alive. The chaos tier asserts the baseline the
            # gauge returns to, not an exact instantaneous value.
            self.set_tasks(len(asyncio.all_tasks()))
        except Exception:  # noqa: BLE001
            log.exception("failed to sample tasks")
        try:
            self.set_permits_in_use(
                tenant=gateway.admission.total_in_use(),
                provider_key=gateway.limiter.total_in_use(),
            )
        except Exception:  # noqa: BLE001
            log.exception("failed to sample permits_in_use")
        try:
            self.sync_admission_denied(
                {**gateway.admission.denials(), **gateway.limiter.denials(),
                 **gateway.draining_denials(), **gateway.overloaded_denials()}
            )
        except Exception:  # noqa: BLE001
            log.exception("failed to sample admission_denied")
        try:
            self.set_upstream_connections(gateway.upstream.stats())
        except Exception:  # noqa: BLE001
            log.exception("failed to sample upstream_connections")
        capture = gateway.capture
        if capture is not None:
            try:
                self.set_capture_queue_bytes(capture.queue_bytes)
                self.sync_capture_dropped(capture.dropped)
            except Exception:  # noqa: BLE001
                log.exception("failed to sample capture gauges")

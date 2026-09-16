"""PLAN-2 phase A, the errors/metrics/accounting half.

What is under test, in the order the request path meets it:

* `errors.from_http_status`: out-of-money arriving as a 429 (OpenAI codes,
  Anthropic `details.error_code`), the Anthropic 400 usage-limit prose, a
  provider 413, a provider 409, and the per-provider meaning of 403.
* `FirstEventTimeout(queued=True)`: NEUTRAL health on the instance, class
  policy otherwise untouched, `decide()` still `try_next`.
* `metrics`: the new closed sets and specs, `normalize_stop_reason`.
* `Collectors`: the three new emit methods and their vocabulary guards.
* `accounting.account`: `stop_reason` read through `getattr` so a `Usage`
  without the field still accounts.
* `executor._completion_disposition`: `provider_shed` is the one completed
  stream the breaker hears as FAILURE, keyed to the target.
* `server.app` helpers: `X-Gw-Model`, the upstream-telemetry header parse,
  the buffered response-model rewrite, and `X-Gw-Upstream-Request-Id` on the
  error path.
* `upstream.apply_include_usage`: the opt-in third body edit.
"""

from __future__ import annotations

import json

import pytest
from prometheus_client import CollectorRegistry

from llmgw import errors as E
from llmgw import metrics as M
from llmgw.accounting import account
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.executor import _completion_disposition
from llmgw.policy import PolicySnapshot
from llmgw.server import app as A
from llmgw.server.telemetry import Collectors
from llmgw.upstream import apply_include_usage
from tests.unit.test_accounting import (
    CATALOG,
    SONNET,
    make_plan,
    make_pump,
    make_result,
    make_usage,
)

# ------------------------------------------------------------- fixtures
# Doc-derived shapes (capabilities/openai.md §5, capabilities/anthropic.md §5).
# Not live captures: OpenAI's billing 429s require an exhausted account to
# observe, and Anthropic's spend cap requires a cap. Marked so nobody
# mistakes them for evidence of the exact prose.

OPENAI_INSUFFICIENT_QUOTA_429 = (
    b'{"error":{"message":"You exceeded your current quota, please check your '
    b'plan and billing details.","type":"insufficient_quota","param":null,'
    b'"code":"insufficient_quota"}}'
)
OPENAI_CREDIT_BALANCE_429 = (
    b'{"error":{"message":"Your credit balance is exhausted.","type":"billing_error",'
    b'"param":null,"code":"credit_balance_exhausted"}}'
)
OPENAI_SPEND_LIMIT_429 = (
    b'{"error":{"message":"Project spend limit reached.","type":"billing_error",'
    b'"param":null,"code":"project_spend_limit_exceeded"}}'
)
ANTHROPIC_SPEND_CAP_429 = (
    b'{"type":"error","error":{"type":"rate_limit_error","message":"This request '
    b'would exceed your organization\'s configured spend limit.",'
    b'"details":{"error_code":"enforced_spend_limit_reached"}},'
    b'"request_id":"req_011"}'
)
ANTHROPIC_USAGE_LIMIT_400 = (
    b'{"type":"error","error":{"type":"invalid_request_error","message":"You have '
    b'reached your specified API usage limits. Adjust them in the console."}}'
)
OPENAI_PLAIN_429 = (
    b'{"error":{"message":"Rate limit reached for gpt-4o-mini.","type":"requests",'
    b'"param":null,"code":"rate_limit_exceeded"}}'
)
ANTHROPIC_413 = (
    b'{"type":"error","error":{"type":"request_too_large","message":"Request exceeds '
    b'the maximum size of 32 MB"}}'
)


# ----------------------------------------------------------- A2 billing


@pytest.mark.parametrize(
    "body",
    [OPENAI_INSUFFICIENT_QUOTA_429, OPENAI_CREDIT_BALANCE_429, OPENAI_SPEND_LIMIT_429,
     ANTHROPIC_SPEND_CAP_429],
    ids=["openai-quota", "openai-credit", "openai-spend-limit", "anthropic-spend-cap"],
)
def test_out_of_money_as_a_429_is_insufficient_credits_not_rate_limited(body):
    err = E.from_http_status(429, body=body, provider="p", model="m")
    assert isinstance(err, E.InsufficientCredits)
    assert err.retry_same is False, "an unpaid invoice does not clear on retry"
    assert err.try_next is True, "a different provider may have credit"
    assert err.health is E.Health.NEUTRAL, "the provider is healthy"
    assert err.blame is E.Blame.POLICY, "ours to fix, nobody to page about a vendor"
    assert err.upstream_status == 429, "the wire status is still the provider's"


def test_a_plain_429_is_still_a_rate_limit():
    err = E.from_http_status(429, body=OPENAI_PLAIN_429, retry_after="3")
    assert isinstance(err, E.RateLimited)
    assert err.retry_after == 3.0


def test_anthropics_usage_limit_prose_on_a_400_is_billing_not_client_fault():
    err = E.from_http_status(400, body=ANTHROPIC_USAGE_LIMIT_400)
    assert isinstance(err, E.InsufficientCredits)
    assert err.blame is E.Blame.POLICY


def test_anthropic_details_error_code_reaches_the_haystack():
    etype, detail = E._error_hints(ANTHROPIC_SPEND_CAP_429)
    assert etype == "rate_limit_error"
    assert "enforced_spend_limit_reached" in detail


# -------------------------------------------------------- A6 status rules


def test_a_provider_413_is_the_clients_body_not_the_providers_health():
    err = E.from_http_status(413, body=ANTHROPIC_413, provider="anthropic")
    assert isinstance(err, E.UpstreamRequestTooLarge)
    assert err.retry_same is False and err.try_next is False
    assert err.health is E.Health.NEUTRAL
    assert err.blame is E.Blame.CLIENT
    assert err.passthrough is True and err.client_status == 413
    assert "upstream_request_too_large" in E.ERROR_CODES


def test_a_provider_409_is_invalid_request_not_a_retryable_server_error():
    err = E.from_http_status(409, body=b'{"error":{"type":"conflict_error","message":"x"}}')
    assert isinstance(err, E.InvalidRequest)
    assert err.retry_same is False


def test_403_defaults_to_the_credential_rule():
    err = E.from_http_status(403, body=b"{}", credential_id="k")
    assert isinstance(err, E.AuthenticationFailed)
    assert err.health_key() == ("cred", "k")


def test_403_as_a_rate_limit_never_touches_the_credential_circuit():
    err = E.from_http_status(403, body=b"{}", provider="assemblyai", model="m",
                             credential_id="k", forbidden_means="rate_limit")
    assert isinstance(err, E.RateLimited)
    assert err.health is E.Health.NEUTRAL
    assert err.health_key() == ("assemblyai", "m")


def test_403_as_a_policy_denial_is_neutral_passthrough_and_blames_policy():
    err = E.from_http_status(
        403, body=b'{"detail":{"status":"voice_access_denied"}}',
        provider="elevenlabs", model="m", credential_id="k", forbidden_means="policy",
    )
    assert isinstance(err, E.PolicyError)
    assert err.health is E.Health.NEUTRAL
    assert err.blame is E.Blame.POLICY
    assert err.passthrough is True and err.client_status == 403
    assert err.outcome is E.Outcome.FAILED, "an upstream was asked; not REJECTED"
    assert err.health_key() != ("cred", "k")


# ------------------------------------------------------- A4 queued timeout


def test_a_queued_first_event_timeout_is_neutral_but_still_falls_back():
    plain = E.FirstEventTimeout("x", provider="deepseek", model="m")
    queued = E.FirstEventTimeout("x", provider="deepseek", model="m", queued=True)
    assert plain.health is E.Health.FAILURE and plain.queued is False
    assert queued.health is E.Health.NEUTRAL and queued.queued is True
    # Class policy is untouched: the instance override is the only change.
    assert E.FirstEventTimeout.health is E.Health.FAILURE
    d = E.decide(queued, committed=False)
    assert d.try_next is True and d.retry_same is False
    assert d.health is E.Health.NEUTRAL
    assert queued.blame is E.Blame.PROVIDER, "it is still their queue"


# ------------------------------------------------------------- metrics


def test_stop_reason_vocabulary_and_fold():
    assert M.normalize_stop_reason(None) is None
    assert M.normalize_stop_reason("") is None
    assert M.normalize_stop_reason("end_turn") == "stop"
    assert M.normalize_stop_reason("MAX_TOKENS") == "length"
    assert M.normalize_stop_reason("tool_use") == "tool_calls"
    assert M.normalize_stop_reason("function_call") == "tool_calls"
    assert M.normalize_stop_reason("insufficient_system_resource") == "provider_shed"
    assert (M.normalize_stop_reason("model_context_window_exceeded")
            == "context_window_exceeded")
    assert M.normalize_stop_reason("something_new") == "unknown"
    assert set(M.STOP_REASON_ALIASES.values()) <= set(M.STOP_REASONS)


def test_new_metric_specs_are_declared_with_closed_labels():
    by_name = {m.name: m for m in M.METRICS}
    stop = by_name["llmgw_stop_reason_total"]
    assert stop.labels == ("surface", "stop_reason")
    assert stop.label_values[1] == M.STOP_REASONS
    assert by_name["llmgw_queued_at_provider_total"].labels == ("provider", "model")
    for name in ("llmgw_provider_ratelimit_remaining",
                 "llmgw_provider_ratelimit_reset_seconds"):
        spec = by_name[name]
        assert spec.kind == "gauge"
        assert spec.labels == ("credential", "kind")
        assert spec.label_values[1] == M.RATELIMIT_KINDS
        # `credential` is bounded by the catalog, like `provider`.
        assert spec.series_estimate(providers=10, models=40) == 10 * len(M.RATELIMIT_KINDS)


def test_collectors_emit_the_new_series_and_guard_their_vocabularies():
    registry = CollectorRegistry()
    c = Collectors(registry)
    c.stop_reason(surface="openai_chat", stop_reason="length")
    c.stop_reason(surface="openai_chat", stop_reason="length")
    c.queued_at_provider(provider="deepseek", model="deepseek.deepseek-v4-flash")
    c.provider_ratelimit(credential="openai", kind="tokens", remaining=1234.0,
                         reset_seconds=12.5)
    c.provider_ratelimit(credential="openai", kind="requests", remaining=None,
                         reset_seconds=-3.0)
    assert registry.get_sample_value(
        "llmgw_stop_reason_total", {"surface": "openai_chat", "stop_reason": "length"}
    ) == 2.0
    assert registry.get_sample_value(
        "llmgw_queued_at_provider_total",
        {"provider": "deepseek", "model": "deepseek.deepseek-v4-flash"},
    ) == 1.0
    assert registry.get_sample_value(
        "llmgw_provider_ratelimit_remaining", {"credential": "openai", "kind": "tokens"}
    ) == 1234.0
    assert registry.get_sample_value(
        "llmgw_provider_ratelimit_reset_seconds", {"credential": "openai", "kind": "tokens"}
    ) == 12.5
    # A missing half leaves its gauge unset; a negative reset clamps to zero.
    assert registry.get_sample_value(
        "llmgw_provider_ratelimit_remaining", {"credential": "openai", "kind": "requests"}
    ) is None
    assert registry.get_sample_value(
        "llmgw_provider_ratelimit_reset_seconds", {"credential": "openai", "kind": "requests"}
    ) == 0.0
    with pytest.raises(ValueError):
        c.stop_reason(surface="openai_chat", stop_reason="ran_out_of_ideas")
    with pytest.raises(ValueError):
        c.provider_ratelimit(credential="x", kind="minutes", remaining=1.0,
                             reset_seconds=None)


# ---------------------------------------------------------- A3 accounting


class _UsageWithStop:
    """A `Usage` that also carries `stop_reason`, for tests written before the
    surfaces grew the field. `getattr` forwarding keeps every other field."""

    def __init__(self, inner, stop_reason):
        self._inner = inner
        self.stop_reason = stop_reason

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_accounting_reads_stop_reason_through_getattr():
    plan = make_plan(SONNET)
    target = plan.targets[0]
    plain = make_usage(input_tokens=10, output_tokens=5, input_exact=True, output_exact=True)
    rec = account(make_result(plan=plan, served_by=target, pump=make_pump(plain),
                              committed=True), catalog=CATALOG)
    assert rec.stop_reason is None, "a Usage without the field accounts as 'said nothing'"

    with_stop = _UsageWithStop(plain, "max_tokens")
    rec = account(make_result(plan=plan, served_by=target, pump=make_pump(with_stop),
                              committed=True), catalog=CATALOG)
    assert rec.stop_reason == "length"
    assert rec.cost_usd > 0, "the proxy forwarded every billing field"


# ------------------------------------------------- A3 breaker on a shed 200


def test_a_completed_stream_is_silent_to_the_breaker_unless_the_provider_shed():
    target = make_plan(SONNET).targets[0]
    plain = make_usage(input_tokens=1, output_tokens=1)
    assert _completion_disposition(target, make_pump(plain)) is None
    assert _completion_disposition(target, None) is None
    ok = _UsageWithStop(plain, "stop")
    assert _completion_disposition(target, make_pump(ok)) is None
    shed = _UsageWithStop(plain, "insufficient_system_resource")
    d = _completion_disposition(target, make_pump(shed))
    assert d is not None
    assert d.health is E.Health.FAILURE
    assert d.health_key == target.health_key, "the target's circuit, never the credential"
    assert d.outcome is E.Outcome.COMPLETED, "the client still got an answer"
    assert d.retry_same is False and d.try_next is False


# ------------------------------------------------------ A1/A6 app helpers


def test_gw_headers_carry_the_catalog_model_id():
    target = make_plan(SONNET).targets[0]
    headers = dict(A.gw_headers(policy_id="p", catalog_id="c", workload_id="w",
                                target=target, attempts=1))
    assert headers[b"x-gw-model"] == SONNET.encode()
    without = dict(A.gw_headers(policy_id="p", catalog_id="c", workload_id="w",
                                target=None, attempts=0))
    assert b"x-gw-model" not in without


@pytest.mark.parametrize(
    "value, expected",
    [("6m0s", 360.0), ("1s", 1.0), ("20ms", 0.02), ("1h2m3.5s", 3723.5), ("12", 12.0),
     ("", None), ("soon", None), ("2026-09-16T00:00:10Z", 10.0)],
)
def test_parse_reset_seconds_reads_go_durations_numbers_and_timestamps(value, expected):
    # 2026-09-16T00:00:00Z as an epoch, so the timestamp case is deterministic.
    now = 1789516800.0
    got = A.parse_reset_seconds(value, now=now)
    if expected is None:
        assert got is None
    else:
        assert got == pytest.approx(expected)


def test_parse_upstream_telemetry_reads_both_dialects_case_insensitively():
    openai = A.parse_upstream_telemetry({
        "X-Request-Id": "req_abc", "OpenAI-Processing-Ms": "288",
        "x-ratelimit-remaining-requests": "199", "x-ratelimit-reset-requests": "300ms",
        "x-ratelimit-remaining-tokens": "39900", "x-ratelimit-reset-tokens": "1s",
    })
    assert openai.request_id == "req_abc"
    assert openai.processing_ms == 288.0
    assert dict((k, (r, s)) for k, r, s in openai.ratelimits) == {
        "requests": (199.0, pytest.approx(0.3)), "tokens": (39900.0, pytest.approx(1.0)),
    }
    anthropic = A.parse_upstream_telemetry({
        "request-id": "req_011", "anthropic-ratelimit-input-tokens-remaining": "50000",
        "anthropic-ratelimit-input-tokens-reset": "2026-09-16T00:00:30Z",
    }, now=1789516800.0)
    assert anthropic.request_id == "req_011"
    assert anthropic.processing_ms is None
    assert anthropic.ratelimits == (("input_tokens", 50000.0, pytest.approx(30.0)),)
    inworld = A.parse_upstream_telemetry({
        "x-inworld-request-id": "iw-1", "x-envoy-upstream-service-time": "42",
    })
    assert inworld.request_id == "iw-1" and inworld.processing_ms == 42.0
    assert A.parse_upstream_telemetry(None) == A.UpstreamTelemetry(None, None, ())
    assert A.parse_upstream_telemetry({}).request_id is None


def test_rewrite_response_model_puts_the_catalog_id_back_on_json_objects_only():
    body = b'{"id":"x","model":"gpt-4o-mini-2024-07-18","choices":[]}'
    out = A.rewrite_response_model(body, "openai.gpt-4o-mini")
    assert json.loads(out)["model"] == "openai.gpt-4o-mini"
    assert json.loads(out)["choices"] == []
    # Already canonical, not JSON, not an object, no string model: untouched.
    same = b'{"model":"openai.gpt-4o-mini"}'
    assert A.rewrite_response_model(same, "openai.gpt-4o-mini") is same
    assert A.rewrite_response_model(b"\xff\xfe", "m") == b"\xff\xfe"
    assert A.rewrite_response_model(b"[1,2]", "m") == b"[1,2]"
    assert A.rewrite_response_model(b'{"model":7}', "m") == b'{"model":7}'


def _exchange() -> A.Exchange:
    snap = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG)
    return A.Exchange(snap, catalog_id="cat_test", workload_id="default")


def test_exchange_observes_the_upstream_once_and_emits_the_request_id():
    ex = _exchange()
    assert b"x-gw-upstream-request-id" not in dict(ex.gw_headers())
    ex.observe_upstream({"x-request-id": "req_first", "openai-processing-ms": "5"})
    ex.observe_upstream({"x-request-id": "req_second"})
    assert ex.upstream is not None and ex.upstream.request_id == "req_first"
    assert dict(ex.gw_headers())[b"x-gw-upstream-request-id"] == b"req_first"


async def test_send_error_carries_the_providers_request_id_and_scrubs_auth():
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    ex = _exchange()
    err = E.from_http_status(
        429, body=OPENAI_INSUFFICIENT_QUOTA_429, provider="openai", model="m",
        headers={"x-request-id": "req_billing", "x-ratelimit-remaining-requests": "0"},
    )
    await A.send_error(send, err, exchange=ex)
    start = sent[0]
    assert start["status"] == 429
    headers = dict(start["headers"])
    assert headers[b"x-gw-upstream-request-id"] == b"req_billing"
    assert b"x-ratelimit-remaining-requests" not in headers, "never forwarded"
    assert json.loads(sent[1]["body"])["error"]["code"] == "insufficient_quota"

    sent.clear()
    auth = E.from_http_status(401, body=b'{"error":{"message":"key ****abcd"}}',
                              provider="openai", headers={"request-id": "req_auth"})
    await A.send_error(send, auth, exchange=_exchange())
    assert dict(sent[0]["headers"])[b"x-gw-upstream-request-id"] == b"req_auth"
    assert b"abcd" not in sent[1]["body"]


# ------------------------------------------------- A6c include_usage injection


def test_apply_include_usage_only_touches_streaming_bodies_without_stream_options():
    streamed = b'{"model":"m","stream":true,"messages":[]}'
    out, changed = apply_include_usage(streamed)
    assert changed is True
    assert json.loads(out)["stream_options"] == {"include_usage": True}
    assert json.loads(out)["messages"] == []
    for untouched in (
        b'{"model":"m","messages":[]}',                       # not streaming
        b'{"model":"m","stream":false}',                      # explicitly not
        b'{"model":"m","stream":true,"stream_options":{}}',   # the client chose
        b'{"model":"m","stream":true,"stream_options":{"include_usage":false}}',
        b'[1,2]', b'not json', b'',
    ):
        out, changed = apply_include_usage(untouched)
        assert changed is False and out is untouched


# ------------------------------------ integration with the surfaces/pump side


def test_the_pump_stamps_queued_by_attribute_and_health_follows():
    """`Pump._mark_queued` sets `err.queued = True` AFTER construction on the
    clock error it raised -- a `StallTimeout` in practice. The flip has to
    follow the attribute, on both classes, and leave class policy alone."""
    for cls in (E.StallTimeout, E.FirstEventTimeout):
        err = cls("x", provider="deepseek", model="m")
        assert err.health is E.Health.FAILURE and err.queued is False
        err.queued = True
        assert err.health is E.Health.NEUTRAL
        assert E.decide(err, committed=False).health is E.Health.NEUTRAL
        assert cls.health is E.Health.FAILURE, "class policy untouched"
        assert cls.queued is False
    # Post-commitment the queued stall is still an interruption, not a fallback.
    err = E.StallTimeout("x", provider="p", model="m")
    err.queued = True
    d = E.decide(err, committed=True)
    assert d.try_next is False and d.outcome is E.Outcome.INTERRUPTED


def test_the_two_stop_reason_vocabularies_cannot_drift():
    from llmgw.surfaces.base import STOP_REASONS as SURFACE_STOP_REASONS

    assert tuple(SURFACE_STOP_REASONS) == tuple(M.STOP_REASONS)
    # A value a surface has already normalised passes the metrics fold unchanged.
    for reason in M.STOP_REASONS:
        assert M.normalize_stop_reason(reason) == reason

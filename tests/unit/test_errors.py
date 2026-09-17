"""The taxonomy tests.

Most are table-driven over EVERY subclass rather than over hand-picked cases.
That is deliberate: the risk with a taxonomy is not that a known class is
wrong, it is that a class added in six months quietly violates an invariant
nobody re-checked. A test that enumerates subclasses fails the moment someone
adds one that breaks the rule.
"""

from __future__ import annotations

import email.utils

import pytest

from llmgw import errors as E


def all_error_classes() -> list[type[E.GatewayError]]:
    seen: list[type[E.GatewayError]] = []

    def walk(cls: type[E.GatewayError]) -> None:
        for sub in cls.__subclasses__():
            if sub not in seen:
                seen.append(sub)
            walk(sub)

    walk(E.GatewayError)
    return seen


# ---------------------------------------------------------------- registry


def test_every_error_class_is_registered_in_ERROR_CODES():
    """ERROR_CODES bounds a metric label. A class missing from it would either
    blow the label vocabulary open or vanish from the dashboards -- both are
    silent, so the test has to be the thing that notices."""
    missing = {c.code for c in all_error_classes()} - E.ERROR_CODES
    assert not missing, f"unregistered error codes: {sorted(missing)}"


def test_error_codes_are_unique():
    codes = [c.code for c in all_error_classes()]
    dupes = {c for c in codes if codes.count(c) > 1}
    # ContextLengthExceeded subclasses InvalidRequest but overrides `code`;
    # any other collision means two errors are indistinguishable in metrics.
    assert not dupes, f"duplicate error codes: {sorted(dupes)}"


# ------------------------------------------------- the commitment invariant


@pytest.mark.parametrize("cls", all_error_classes(), ids=lambda c: c.__name__)
def test_commitment_forbids_every_further_attempt(cls: type[E.GatewayError]):
    """THE invariant. Once a byte reached the client, no error class -- not
    one, not with any configuration -- may authorise another attempt.

    This is enumerated over all classes rather than asserted on a couple of
    representative ones because the failure it guards against is a future
    class that sets try_next=True and looks perfectly reasonable in isolation.
    """
    err = cls("x", provider="p", model="m")
    d = E.decide(err, committed=True)
    assert d.retry_same is False
    assert d.try_next is False
    assert d.reason == "committed"


@pytest.mark.parametrize("cls", all_error_classes(), ids=lambda c: c.__name__)
def test_uncommitted_disposition_matches_class_policy(cls: type[E.GatewayError]):
    err = cls("x", provider="p", model="m")
    d = E.decide(err, committed=False)
    assert d.retry_same is cls.retry_same
    assert d.try_next is cls.try_next
    assert d.health is cls.health


def test_committed_failure_is_interrupted_not_failed():
    """Accounting depends on this: a partial answer was delivered and its
    tokens are billable, which FAILED would deny."""
    d = E.decide(E.UpstreamDisconnected("boom"), committed=True)
    assert d.outcome is E.Outcome.INTERRUPTED


def test_committed_cancel_stays_canceled():
    """A client that hangs up mid-stream did not experience an interruption;
    it caused one. Mislabelling this inflates the interruption rate with
    events that are not incidents."""
    d = E.decide(E.ClientDisconnected("gone"), committed=True)
    assert d.outcome is E.Outcome.CANCELED


@pytest.mark.parametrize("cls", all_error_classes(), ids=lambda c: c.__name__)
def test_retry_same_implies_try_next(cls: type[E.GatewayError]):
    """If it is safe to re-send to the same target, it is safe to send to a
    different one. The converse is false (see InvalidRequest), so only this
    direction is asserted."""
    if cls.retry_same:
        assert cls.try_next, f"{cls.__name__} may retry itself but not fall back"


# ------------------------------------------------------- blame and health


@pytest.mark.parametrize(
    "cls", [E.ClientDisconnected, E.ClientTooSlow], ids=lambda c: c.__name__
)
def test_client_faults_never_blame_the_provider(cls):
    """The row that keeps breakers closed during a client-side incident."""
    assert cls.health is E.Health.NEUTRAL
    assert cls.blame is E.Blame.CLIENT


def test_breaker_open_does_not_feed_the_breaker():
    """Otherwise the breaker's own rejections keep it open forever: every
    request it refuses counts as another failure, and it can never sample
    reality again."""
    assert E.BreakerOpen.health is E.Health.NEUTRAL
    assert E.BreakerOpen.try_next is True


def test_auth_failure_is_scoped_to_the_credential_alone_not_the_target():
    """A credential is one key. Its health circuit carries neither the model
    nor the provider ENTRY -- only the credential id -- because a bad key is
    bad wherever it is used. Two things follow, and both are the point:

    * A revoked BYOK key opens exactly one circuit, for that tenant, and never
      touches the provider's model-scoped circuits that everyone else rides.
    * Two provider entries sharing one key (openrouter / openrouter-toolsafe)
      share that one circuit, instead of splitting the failure count in two.
    """
    err = E.AuthenticationFailed("nope", provider="anthropic", model="haiku",
                                 credential_id="tenant-42")
    assert err.health_key() == ("cred", "tenant-42")
    # The provider entry does not appear, so two entries on one key collide
    # onto the same circuit -- which is the fix, not an accident.
    other_entry = E.AuthenticationFailed("nope", provider="openrouter-toolsafe",
                                         model="x", credential_id="openrouter")
    plain = E.AuthenticationFailed("nope", provider="openrouter",
                                   model="y", credential_id="openrouter")
    assert other_entry.health_key() == plain.health_key() == ("cred", "openrouter")


def test_ordinary_failure_is_scoped_to_provider_and_model():
    err = E.UpstreamServerError("500", provider="anthropic", model="haiku")
    assert err.health_key() == ("anthropic", "haiku")


def test_deadline_and_budget_errors_cannot_try_another_target():
    """Not a policy: there is no time left to try one in."""
    assert E.TotalDeadlineExceeded.try_next is False
    assert E.RetryBudgetExhausted.try_next is False


def test_a_total_deadline_breach_is_not_evidence_about_a_provider():
    """The request ran out of time; nobody was measured.

    `phase()` produces this class only when the TOTAL was the binding clock,
    which means the provider was never given its full phase budget -- the
    rest was spent by a previous target, a retry sleep, or a slow client. A
    provider that stalls through its OWN budget gets the phase class instead,
    and those still count. So the breaker input is NEUTRAL here and FAILURE
    there, and the two are told apart by which clock fired, not by guessing.
    """
    assert E.TotalDeadlineExceeded.health is E.Health.NEUTRAL
    for phase_class in (E.FirstEventTimeout, E.StallTimeout, E.ConnectTimeout):
        assert phase_class.health is E.Health.FAILURE, phase_class
    err = E.TotalDeadlineExceeded("out of time", provider="acme", model="m")
    for committed in (False, True):
        assert E.decide(err, committed=committed).health is E.Health.NEUTRAL


def test_invalid_request_falls_back_but_never_retries_itself():
    """The surprising row, and the reason retryability is two booleans."""
    assert E.InvalidRequest.retry_same is False
    assert E.InvalidRequest.try_next is True


def test_content_filter_does_not_shop_the_prompt_around():
    """Trying another provider until one answers a refused prompt is a
    compliance decision, not a reliability one."""
    assert E.ContentFiltered.try_next is False


# ---------------------------------------------------------- Retry-After


@pytest.mark.parametrize(
    "value,expected",
    [
        ("3", 3.0),
        ("0", 0.0),
        ("2.5", 2.5),
        ("  7 ", 7.0),
        (None, None),
        ("", None),
        ("banana", None),
        ("-5", 0.0),  # never negative
    ],
)
def test_parse_retry_after_delta_seconds(value, expected):
    assert E.parse_retry_after(value) == expected


def test_parse_retry_after_accepts_http_date():
    """Providers send both forms, sometimes from the same service. Handling
    only the integer form means silently ignoring the header from whichever
    provider chose dates -- and ignoring Retry-After is how you get banned."""
    base = email.utils.parsedate_to_datetime("Wed, 09 Sep 2026 00:00:00 GMT").timestamp()
    assert E.parse_retry_after("Wed, 09 Sep 2026 00:00:30 GMT", now=base) == 30.0


def test_parse_retry_after_date_in_the_past_is_zero_not_negative():
    base = email.utils.parsedate_to_datetime("Wed, 09 Sep 2026 00:00:00 GMT").timestamp()
    assert E.parse_retry_after("Wed, 01 Jan 2020 00:00:00 GMT", now=base) == 0.0


# ------------------------------------------------------- HTTP classification


@pytest.mark.parametrize(
    "status,body,expected",
    [
        (429, None, E.RateLimited),
        (401, None, E.AuthenticationFailed),
        (403, None, E.AuthenticationFailed),
        # A bodiless 404 is the edge, not the API (finding 49): retryable.
        (404, None, E.UpstreamServerError),
        (400, b'{"error":{"message":"bad"}}', E.InvalidRequest),
        (422, b'{"error":{"message":"bad"}}', E.InvalidRequest),
        (500, None, E.UpstreamServerError),
        (502, None, E.UpstreamServerError),
        (503, None, E.UpstreamOverloaded),
        (529, None, E.UpstreamOverloaded),
        (408, None, E.ConnectTimeout),
    ],
)
def test_from_http_status(status, body, expected):
    err = E.from_http_status(status, body=body, provider="p", model="m")
    assert type(err) is expected
    assert err.upstream_status == status


def test_context_length_is_distinguished_from_a_generic_400():
    """Routing can act on this one -- a bigger-context target may serve it --
    and generic 400 handling cannot."""
    body = b'{"error":{"code":"context_length_exceeded","message":"too long"}}'
    err = E.from_http_status(400, body=body, provider="p", model="m")
    assert isinstance(err, E.ContextLengthExceeded)
    assert isinstance(err, E.InvalidRequest)  # still a 400 for anything generic


def test_anthropic_overloaded_body_is_recognised_at_any_status():
    err = E.from_http_status(500, body=b'{"error":{"type":"overloaded_error"}}')
    assert isinstance(err, E.UpstreamOverloaded)


def test_classifier_survives_a_non_json_error_body():
    """During an incident the provider's edge returns an HTML error page. The
    classifier that exists to handle incidents must not be the thing that
    crashes during one."""
    err = E.from_http_status(503, body=b"<html><h1>502 Bad Gateway</h1></html>")
    assert isinstance(err, E.UpstreamOverloaded)


def test_classifier_survives_a_json_array_body():
    err = E.from_http_status(400, body=b'[1,2,3]')
    assert isinstance(err, E.InvalidRequest)


def test_retry_after_is_carried_onto_the_error():
    err = E.from_http_status(429, retry_after="12", provider="p")
    assert err.retry_after == 12.0


# ------------------------------------------------------------ passthrough


def test_client_status_prefers_the_upstream_status_when_passing_through():
    """We do not improve on a provider's error. The caller's SDK knows how to
    read its own vendor's error shape; a helpfully rewritten one breaks it."""
    err = E.from_http_status(429, provider="p")
    assert err.client_status == 429


def test_client_status_falls_back_to_the_class_default():
    assert E.BreakerOpen("open").client_status == 503
    assert E.ClientDisconnected("gone").client_status == 499


# ---------------------------------------------------------------- finding 49
# A 404 is "model not found" only when an API said so. OpenAI's edge answered
# a valid request with an intermittent 404 and a non-JSON body on 18 Sep 2026.


def test_a_404_with_an_api_error_body_is_model_not_found():
    from llmgw.errors import ModelNotFound, from_http_status
    body = (b'{"error": {"message": "The model `nope` does not exist", '
            b'"type": "invalid_request_error", "code": "model_not_found"}}')
    err = from_http_status(404, body=body, provider="openai", model="nope")
    assert isinstance(err, ModelNotFound)
    assert err.retry_same is False and err.try_next is True


def test_a_404_with_an_html_body_is_a_retryable_upstream_error():
    from llmgw.errors import Health, ModelNotFound, UpstreamServerError, from_http_status
    body = b"<html><head><title>404 Not Found</title></head><body>nginx</body></html>"
    err = from_http_status(404, body=body, provider="openai", model="openai.gpt-4o-mini")
    assert isinstance(err, UpstreamServerError) and not isinstance(err, ModelNotFound)
    assert err.retry_same is True and err.try_next is True
    assert err.health is Health.FAILURE
    assert "non-API body" in str(err)


def test_a_404_with_an_empty_body_is_not_model_not_found():
    from llmgw.errors import ModelNotFound, from_http_status
    err = from_http_status(404, body=b"", provider="anthropic", model="x")
    assert not isinstance(err, ModelNotFound)


def test_a_grpc_style_404_body_still_counts_as_an_api_error():
    from llmgw.errors import ModelNotFound, from_http_status
    body = b'{"code": 5, "message": "Unknown voice: NoSuchVoice not found!", "details": []}'
    err = from_http_status(404, body=body, provider="inworld", model="v")
    assert isinstance(err, ModelNotFound)

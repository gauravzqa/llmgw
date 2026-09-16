"""Classification against error bodies real providers actually sent.

Every payload here was captured from a live API during the live-provider
pass and is reproduced byte-for-byte. That is the whole point of the file: the
taxonomy had only ever been tested against bodies WE wrote into
`fakes/upstream.py`, which means it had only ever been tested against our
beliefs about what providers send.

Three of those beliefs were wrong, and none of the 745 tests that existed
before this file could have noticed, because the fakes agreed with them.

The general lesson is worth more than the three fixes: a fake upstream is a
necessary rig and never a sufficient one. It can prove your code handles the
cases you thought of. Only the real thing tells you which cases exist.
"""

from __future__ import annotations

import pytest

from llmgw import errors as E

# ---------------------------------------------------------------- payloads
# Captured live. Do not "tidy" these -- the exact shape is the evidence.

DEEPSEEK_UNKNOWN_MODEL = (
    b'{"error":{"message":"The supported API model names are deepseek-v4-pro, '
    b'deepseek-v4-flash, and deepseek-v4-flash-vision-exp, but you passed '
    b'deepseek-v4-ghost.","type":"invalid_request_error","param":null,'
    b'"code":"invalid_request_error"}}'
)

OPENAI_UNSUPPORTED_PARAM = (
    b'{"error":{"message":"Unsupported parameter: \'max_tokens\' is not supported '
    b'with this model. Use \'max_completion_tokens\' instead.",'
    b'"type":"invalid_request_error","param":"max_tokens",'
    b'"code":"unsupported_parameter"}}'
)

OPENROUTER_INSUFFICIENT_CREDITS = (
    b'{"error":{"message":"Insufficient credits. Add more using '
    b'https://openrouter.ai/settings/credits","code":402,'
    b'"metadata":{"limit_source":"openrouter_credits"}}}'
)

OPENAI_UNKNOWN_MODEL = (
    b'{"error":{"message":"The model `gpt-9-imaginary` does not exist or you do '
    b'not have access to it.","type":"invalid_request_error","param":"model",'
    b'"code":"model_not_found"}}'
)


# ------------------------------------------------- unknown model on a 400


@pytest.mark.parametrize(
    "body", [DEEPSEEK_UNKNOWN_MODEL, OPENAI_UNKNOWN_MODEL],
    ids=["deepseek", "openai"],
)
def test_an_unknown_model_is_our_config_drift_even_when_it_arrives_as_a_400(body):
    """Anthropic answers 404 for an unknown model; DeepSeek and OpenRouter
    answer 400. A status-only rule therefore gives the SAME operator mistake
    two different meanings depending on which vendor received it:

        404 -> ModelNotFound      FAILURE health, POLICY blame  (correct)
        400 -> InvalidRequest     NEUTRAL health, CLIENT blame  (wrong twice)

    Wrong twice because the stale-catalog detector silently does not exist for
    OpenAI-shaped providers, and because the customer is blamed for our own
    configuration drifting away from the provider's reality.
    """
    err = E.from_http_status(400, body=body, provider="deepseek", model="x")
    assert isinstance(err, E.ModelNotFound)
    assert err.blame is E.Blame.POLICY
    assert err.health is E.Health.FAILURE
    assert err.try_next is True
    assert err.retry_same is False


def test_a_genuinely_malformed_request_is_still_the_clients_fault():
    """The counterweight. The unknown-model rule must not swallow every 400,
    or we would absolve clients of every bad request they send."""
    body = b'{"error":{"message":"messages: field required","type":"invalid_request_error"}}'
    err = E.from_http_status(400, body=body, provider="openai", model="x")
    assert isinstance(err, E.InvalidRequest)
    assert not isinstance(err, E.ModelNotFound)
    assert err.blame is E.Blame.CLIENT


# --------------------------------------------- the param-name false positive


def test_an_unsupported_parameter_is_not_a_context_overflow():
    """The subtlest of the three.

    `_error_hints` used to fold the `param` field into the string it matched
    against, and the context-overflow rule looked for `max_tokens`. So this
    body -- an *unsupported parameter* error, raised on a two-token prompt --
    matched the rule for a prompt too large for the model's context window.

    The substring really was in the payload. It just meant the opposite of
    what the rule assumed, which is the failure mode of every heuristic that
    matches on a field it does not understand.
    """
    err = E.from_http_status(400, body=OPENAI_UNSUPPORTED_PARAM,
                             provider="openai", model="gpt-5-nano")
    assert isinstance(err, E.InvalidRequest)
    assert not isinstance(err, E.ContextLengthExceeded)


def test_a_real_context_overflow_is_still_detected():
    """The counterweight again: narrowing the rule must not disable it."""
    body = (b'{"error":{"message":"prompt is too long: 250000 tokens > 200000 '
            b'maximum","type":"invalid_request_error"}}')
    err = E.from_http_status(400, body=body, provider="anthropic", model="x")
    assert isinstance(err, E.ContextLengthExceeded)


# ------------------------------------------------------------------ 402


def test_an_unpaid_invoice_is_not_a_provider_outage():
    """402 previously fell through every branch into `UpstreamServerError`:
    `retry_same=True`, FAILURE health, PROVIDER blame.

    So an OpenRouter account running out of credit would be retried forever,
    would open a circuit breaker against a provider that was working
    perfectly, and would page someone about a vendor incident that was
    actually an unpaid bill. Every axis was wrong.
    """
    err = E.from_http_status(402, body=OPENROUTER_INSUFFICIENT_CREDITS,
                             provider="openrouter", model="x")
    assert isinstance(err, E.InsufficientCredits)
    assert err.retry_same is False           # not transient
    assert err.try_next is True              # another provider may have credit
    assert err.health is E.Health.NEUTRAL    # the provider is healthy
    assert err.blame is E.Blame.POLICY       # ours, not theirs
    assert err.client_status == 402
    assert E.decide(err, committed=False).try_next is True


def test_402_survives_a_body_it_cannot_parse():
    """Classification must not depend on the body being well formed -- an
    edge returning HTML during an incident is exactly when this runs."""
    err = E.from_http_status(402, body=b"<html>Payment Required</html>")
    assert isinstance(err, E.InsufficientCredits)


# ----------------------------------------------------- registry completeness


def test_the_new_class_is_countable():
    """An outcome that cannot appear in `llmgw_requests_total{code=...}` is an
    outcome nobody can alert on."""
    assert E.InsufficientCredits.code in E.ERROR_CODES


# ------------------------------------------ out of money, dressed as a 429
#
# NOT live captures, and marked so. OpenAI's billing 429s need an exhausted
# account to observe and Anthropic's spend cap needs a cap; both shapes are
# transcribed from the providers' current error documentation as read on
# 2026-09-16 (capabilities/openai.md §5, capabilities/anthropic.md §5). Kept
# here beside the live bodies because they test the same seam -- a status the
# taxonomy thought it understood, refined by a body it had never seen -- and
# so that the day one IS captured live it replaces the transcription in place.

OPENAI_INSUFFICIENT_QUOTA_429_DOC = (
    b'{"error":{"message":"You exceeded your current quota, please check your '
    b'plan and billing details.","type":"insufficient_quota","param":null,'
    b'"code":"insufficient_quota"}}'
)

ANTHROPIC_SPEND_CAP_429_DOC = (
    b'{"type":"error","error":{"type":"rate_limit_error","message":"This request '
    b'would exceed your organization\'s configured spend limit.",'
    b'"details":{"error_code":"enforced_spend_limit_reached"}},"request_id":"req_x"}'
)


@pytest.mark.parametrize(
    "body", [OPENAI_INSUFFICIENT_QUOTA_429_DOC, ANTHROPIC_SPEND_CAP_429_DOC],
    ids=["openai-doc", "anthropic-doc"],
)
def test_out_of_money_on_a_429_is_a_billing_state_not_a_transient_rate_limit(body):
    """Finding 8 fixed the 402 shape (OpenRouter). Two more providers say the
    same thing with a 429 and a code; a status-only rule retried them."""
    err = E.from_http_status(429, body=body, provider="p", model="m")
    assert isinstance(err, E.InsufficientCredits)
    assert err.retry_same is False
    assert err.try_next is True
    assert err.health is E.Health.NEUTRAL
    assert err.blame is E.Blame.POLICY


def test_the_openrouter_402_still_classifies_the_same_way():
    err = E.from_http_status(402, body=OPENROUTER_INSUFFICIENT_CREDITS)
    assert isinstance(err, E.InsufficientCredits)

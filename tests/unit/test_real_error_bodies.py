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

ELEVENLABS_BAD_KEY = (
    b'{"detail":{"type":"authentication_error","code":"invalid_api_key",'
    b'"message":"API key ID used as API key - only valid API keys can be used. '
    b"API keys start with 'sk_' and are shown when the key is created or "
    b'rotated.","status":"api_key_id_used_as_api_key",'
    b'"param":"api_key","docs_url":"https://elevenlabs.io/docs/api-reference/'
    b'authentication"}}'
)
"""ElevenLabs, 400 (not 401), 18 Sep 2026: the value in `.env` was an API key
ID rather than the key itself."""

OPENAI_BAD_PARAM = (
    b'{"error":{"message":"unknown parameter \'foo\'.",'
    b'"type":"invalid_request_error","param":"foo","code":null}}'
)
"""The control for the rule above: an ordinary 400 that really is the
client's."""

DEEPSEEK_UNKNOWN_MODEL = (
    b'{"error":{"message":"The supported API model names are deepseek-v4-pro, '
    b'deepseek-v4-flash, and deepseek-v4-flash-vision-exp, but you passed '
    b'deepseek-v4-ghost.","type":"invalid_request_error","param":null,'
    b'"code":"invalid_request_error"}}'
)

DEEPSEEK_UNKNOWN_MODEL_2026_09_17 = (
    b'{"error":{"message":"The supported API model names are deepseek-flash, '
    b'deepseek-v4-pro, but you passed deepseek-nope.","type":"invalid_request_error",'
    b'"param":null,"code":"invalid_request_error"}}'
)
"""Captured 17 Sep 2026 against `POST /v1/responses` (capabilities/
captures-responses.md probe 12a). Same envelope as the chat body above with
the current model list -- and served under `content-type:
application/octet-stream`, which is why it has its own test."""

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


def test_a_deepseek_400_under_an_octet_stream_content_type_still_classifies_on_the_body():
    """DeepSeek's Responses endpoint labels its JSON error bodies
    `application/octet-stream` (probe 12a). Classification reads the body's
    shape, never the content type, so the label changes nothing: it is still
    our config drift (`ModelNotFound`), not the customer's request."""
    for headers in ({"content-type": "application/octet-stream"}, {}, None):
        err = E.from_http_status(
            400, body=DEEPSEEK_UNKNOWN_MODEL_2026_09_17, headers=headers,
            provider="deepseek", model="deepseek-nope",
        )
        assert isinstance(err, E.ModelNotFound), headers
        assert err.blame is E.Blame.POLICY
    assert E._is_api_error_body(DEEPSEEK_UNKNOWN_MODEL_2026_09_17)



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


def test_elevenlabs_signals_a_bad_credential_with_a_400():
    """Captured live 18 Sep 2026 by putting an ElevenLabs API key ID where
    the key goes. ElevenLabs answers 400, not 401, so a status-only rule
    files a credential that is OURS under the client's errors: no credential
    circuit opens, no fallback is tried, and the dashboard says the callers
    are sending bad requests."""
    err = E.from_http_status(400, body=ELEVENLABS_BAD_KEY, provider="elevenlabs")
    assert isinstance(err, E.AuthenticationFailed)
    assert err.blame is E.Blame.PROVIDER
    assert err.health is E.Health.FAILURE


def test_an_ordinary_400_is_still_the_clients():
    """The auth rule reads the body, so it must not swallow the common case."""
    err = E.from_http_status(400, body=OPENAI_BAD_PARAM, provider="openai")
    assert isinstance(err, E.InvalidRequest)
    assert err.blame is E.Blame.CLIENT


def test_a_provider_that_signals_auth_with_400_does_not_get_to_say_400():
    """The body rule concludes "the gateway's credential was rejected"; the
    status must not then tell the client it sent a bad request. Passthrough
    exists to keep the SHAPE an SDK expects, not to forward a status that
    contradicts what we decided -- and 400 and 401 share a band while meaning
    opposite things about whose fault it is."""
    err = E.from_http_status(400, body=ELEVENLABS_BAD_KEY, provider="elevenlabs")
    assert isinstance(err, E.AuthenticationFailed)
    assert err.client_status == 401


def test_a_real_401_or_403_still_passes_through_untouched():
    for status in (401, 403):
        err = E.AuthenticationFailed("no", provider="p", upstream_status=status)
        assert err.client_status == status


def test_passthrough_for_every_other_class_is_unchanged():
    """The new rule is opt-in per class; nothing else may have moved."""
    assert E.UpstreamOverloaded("x", upstream_status=529).client_status == 529
    assert E.RateLimited("x", upstream_status=429).client_status == 429


# ------------------------------------- AssemblyAI sync: a 404 that is a key
#
# Captured live 18 Sep 2026 (captures-sarvam-assemblyai.md §1.3) and
# reproduced 19 Sep 2026 against `sync.assemblyai.com`. Every application
# error on that host is RFC 7807 under `application/problem+json`; the load
# balancer in front of it answers `text/plain`. That split is the ONLY thing
# separating "the gateway's key is dead" from "the routing header named a
# model this host does not serve", because both are 404.

AAI_SYNC_BAD_KEY_404 = b'{"status": 404, "title": "Not Found", "detail": "Invalid API key"}'

AAI_SYNC_ELB_404 = b"Not found"
"""`text/plain`, `server: awselb/2.0`: no `X-AAI-Model`, or one the sync host
does not route (`universal-2` is one). The application never ran."""

AAI_SYNC_415 = (
    b'{"status": 415, "title": "Unsupported Media Type", "detail": "request must be '
    b'multipart/form-data with an `audio` part and an optional `config` part"}'
)

AAI_SYNC_400_NO_AUDIO_PART = (
    b'{"status": 400, "title": "Bad Request", "detail": "request must include an '
    b'`audio` file part"}'
)

AAI_SYNC_400_BAD_AUDIO = (
    b'{"status": 400, "title": "Bad Audio", "detail": "truncated WAV: "}'
)

PROBLEM_JSON = {"content-type": "application/problem+json"}


def test_a_404_that_says_invalid_api_key_is_a_credential_failure():
    """The surface 404'd for a year for an unrelated reason, and the day the
    key expires it will 404 again with a completely different meaning. Under
    the status-only rule that second 404 reads as `UpstreamServerError`:
    retried against the same dead key, blamed on AssemblyAI's health, and
    paging someone about a vendor outage that is an expired credential."""
    err = E.from_http_status(404, body=AAI_SYNC_BAD_KEY_404, headers=PROBLEM_JSON,
                             provider="assemblyai-sync", model="assemblyai.sync")
    assert isinstance(err, E.AuthenticationFailed)
    assert err.health is E.Health.FAILURE
    assert err.health_scope is E.HealthScope.CREDENTIAL
    assert err.retry_same is False
    # 404 is not in `passthrough_statuses`: telling the caller "not found"
    # about our own rejected key is the misfiling the rule exists to undo.
    assert err.client_status == 401


def test_the_rule_reads_the_body_even_when_the_content_type_is_missing():
    """A proxy that strips or rewrites `content-type` must not turn a dead
    credential back into a vendor outage. The RFC 7807 envelope is evidence
    on its own."""
    err = E.from_http_status(404, body=AAI_SYNC_BAD_KEY_404, headers=None,
                             provider="assemblyai-sync")
    assert isinstance(err, E.AuthenticationFailed)


def test_the_load_balancers_404_is_still_not_a_credential_and_not_a_model():
    """The other 404 on the same host, same status, same path: `text/plain`
    from `awselb/2.0`. It is neither our key nor a missing model -- it is a
    routing fault in front of the API, and `UpstreamServerError` is the
    honest answer because the API never saw the request."""
    err = E.from_http_status(404, body=AAI_SYNC_ELB_404,
                             headers={"content-type": "text/plain; charset=utf-8",
                                      "server": "awselb/2.0"},
                             provider="assemblyai-sync", model="assemblyai.sync")
    assert isinstance(err, E.UpstreamServerError)
    assert not isinstance(err, E.AuthenticationFailed)
    assert not isinstance(err, E.ModelNotFound)


@pytest.mark.parametrize(
    "body", [AAI_SYNC_415, AAI_SYNC_400_NO_AUDIO_PART, AAI_SYNC_400_BAD_AUDIO],
    ids=["415-media-type", "400-no-audio-part", "400-bad-audio"],
)
def test_the_other_problem_json_bodies_are_not_dragged_into_the_auth_rule(body):
    """The counterweight. Four of the five captured bodies on this host are
    RFC 7807 too; only the one whose `detail` says so is a credential."""
    err = E.from_http_status(404, body=body, headers=PROBLEM_JSON, provider="assemblyai-sync")
    assert not isinstance(err, E.AuthenticationFailed)


def test_a_problem_document_with_no_detail_string_is_not_matched():
    """`detail` is required to be a string in RFC 7807, and the rule reads
    it. A document that omits it cannot say anything about a credential."""
    err = E.from_http_status(404, body=b'{"status":404,"title":"Not Found"}',
                             headers=PROBLEM_JSON, provider="p")
    assert not isinstance(err, E.AuthenticationFailed)
    assert E._is_problem_json(PROBLEM_JSON, b'{"status":404,"title":"Not Found"}')
    assert not E._is_problem_json(None, b'{"status":404,"title":"Not Found"}')


def test_the_new_rule_survives_a_body_it_cannot_parse():
    """It runs on an error path, on bytes we did not write."""
    for body in (b"<html>404</html>", b"", b"[1,2,3]", b'{"detail": {"x": 1}}'):
        err = E.from_http_status(404, body=body, headers=PROBLEM_JSON, provider="p")
        assert not isinstance(err, E.AuthenticationFailed), body
    assert E._problem_detail(b"not json") == ""
    assert E._problem_detail(None) == ""


# ------------------------------------------- ElevenLabs speech-to-text, 2026-09-19
#
# The Scribe route answers a bad key with a plain 401, unlike the TTS host's
# 400 above. Both shapes are kept so a future change to `_AUTH_400_HINTS`
# cannot quietly stop covering one of them.

ELEVENLABS_STT_BAD_KEY_401 = (
    b'{"detail":{"type":"authentication_error","code":"unauthorized",'
    b'"message":"Invalid API key","status":"invalid_api_key",'
    b'"request_id":"08402ba39df886b270f1c4b4f3e1f94d"}}'
)

ELEVENLABS_STT_UNSUPPORTED_MODEL_400 = (
    b'{"detail":{"type":"validation_error","code":"unsupported_model",'
    b'"message":"\'elevenlabs.scribe-v1\' is not a valid model_id. Available models: '
    b'\'scribe_v1\', \'scribe_v1_experimental\', \'scribe_v2\', \'scribe_v2_medical\'",'
    b'"status":"invalid_model_id","param":"model_id"}}'
)
"""What the provider says when the gateway forgets to rewrite the catalog id
into `model_id`. It names a model, so it is our config drift, not the
caller's bad request."""


def test_elevenlabs_scribe_rejects_a_bad_key_with_a_plain_401():
    err = E.from_http_status(401, body=ELEVENLABS_STT_BAD_KEY_401, provider="elevenlabs")
    assert isinstance(err, E.AuthenticationFailed)
    assert err.client_status == 401


def test_a_catalog_id_that_reached_elevenlabs_scribe_is_our_config_drift():
    err = E.from_http_status(400, body=ELEVENLABS_STT_UNSUPPORTED_MODEL_400,
                             provider="elevenlabs", model="elevenlabs.scribe-v2")
    assert isinstance(err, E.ModelNotFound)
    assert err.blame is E.Blame.POLICY


# ------------------------------------------------ Inworld speech-to-text, 2026-09-19

INWORLD_STT_BAD_KEY_403 = (
    b'{"code":7,"message":"Invalid authorization credentials","details":[]}'
)

INWORLD_STT_UNSUPPORTED_MODEL_400 = (
    b'{"code":3,"message":"Unsupported model \\"inworld.stt-1\\". Supported models: '
    b'https://docs.inworld.ai/docs/tutorial-integrations/stt/supported-models",'
    b'"details":[]}'
)

INWORLD_STT_MISSING_AUDIO_400 = b'{"code":3,"message":"audio_data is required","details":[]}'


def test_inworld_stt_bad_credential_is_a_403_that_means_auth():
    """The `inworld` row leaves `forbidden_means` at its default, and that is
    right here: this 403 is the credential, not a plan or a rate limit."""
    err = E.from_http_status(403, body=INWORLD_STT_BAD_KEY_403, provider="inworld")
    assert isinstance(err, E.AuthenticationFailed)
    assert err.health_scope is E.HealthScope.CREDENTIAL


def test_inworld_stt_unknown_model_is_config_drift_and_a_missing_part_is_not():
    drift = E.from_http_status(400, body=INWORLD_STT_UNSUPPORTED_MODEL_400,
                               provider="inworld", model="inworld.stt-1")
    assert isinstance(drift, E.ModelNotFound)
    client = E.from_http_status(400, body=INWORLD_STT_MISSING_AUDIO_400, provider="inworld")
    assert isinstance(client, E.InvalidRequest)
    assert not isinstance(client, E.ModelNotFound)

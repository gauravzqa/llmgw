"""PLAN-2 Phase D4: voice providers' error bodies through the classifier.

ElevenLabs nests under `detail` (an object, or a list on 422); Inworld is a
gRPC status transcoded to JSON (`code`, `message`, `details`); AssemblyAI's
REST rate limit is a 403. Each row of the mapping below is one line in
capabilities/voice-*.md §5; the live-verified ones say so.
"""

from __future__ import annotations

import json

import pytest

from llmgw import errors as E
from llmgw.catalog import DEFAULT_CATALOG


def body(obj: object) -> bytes:
    return json.dumps(obj).encode()


# ------------------------------------------------------------------ Inworld
# Live, 16 Sep 2026: 400 code 3 (bad model), 404 code 5 (bad voice),
# 401 code 16 (missing key), 403 code 7 (bad key).


def test_inworld_unknown_model_is_a_400_with_grpc_code_3_and_reads_as_model_not_found():
    err = E.from_http_status(
        400, body=body({"code": 3, "message": "model_id: inworld-tts-999 is not supported.",
                        "details": []}), provider="inworld", model="inworld.tts-999")
    assert isinstance(err, E.ModelNotFound)
    assert err.try_next is True and err.retry_same is False


def test_inworld_unknown_voice_is_a_404_api_body_and_model_not_found():
    err = E.from_http_status(
        404, body=body({"code": 5, "message": "Unknown voice: NoSuchVoiceXYZ not found!",
                        "details": []}), provider="inworld", model="inworld.tts-2")
    assert isinstance(err, E.ModelNotFound)


def test_inworld_401_and_403_are_credential_failures_by_default():
    missing = E.from_http_status(
        401, body=body({"code": 16, "message": "authentication required"}), provider="inworld")
    bad = E.from_http_status(403, body=body({"code": 7, "message": "permission denied"}),
                             provider="inworld", forbidden_means="auth")
    assert isinstance(missing, E.AuthenticationFailed)
    assert isinstance(bad, E.AuthenticationFailed)


def test_inworld_provider_row_scrubs_every_error_body():
    row = DEFAULT_CATALOG.providers["inworld"]
    assert row.scrub_error_bodies == "all"
    # `basic` since PLAN-G G1: the WebSocket upgrade accepts nothing else
    # (captures-ws probe 2a), and on HTTP the two forms are identical
    # (verified live 2026-09-16), so the switch is invisible to Phase D.
    assert row.auth_scheme == "basic" and row.forbidden_means == "auth"


def test_inworld_other_400s_stay_invalid_request():
    err = E.from_http_status(400, body=body({"code": 3, "message": "text length should not "
                                             "exceed 2000 characters."}), provider="inworld")
    assert isinstance(err, E.InvalidRequest)


# --------------------------------------------------------------- ElevenLabs
# Documentation shapes (no key was available): legacy `detail.status`,
# current `detail.type/code`, 422 validation lists.


@pytest.mark.parametrize("status_word", ["voice_access_denied", "model_access_denied",
                                         "feature_not_available"])
def test_elevenlabs_403_policy_denials_never_touch_the_credential_breaker(status_word):
    err = E.from_http_status(
        403, body=body({"detail": {"status": status_word, "message": "no access"}}),
        provider="elevenlabs", model="elevenlabs.flash-v2-5", forbidden_means="policy")
    assert isinstance(err, E.PolicyError)
    assert err.health is E.Health.NEUTRAL
    assert err.passthrough is True
    assert not isinstance(err, E.AuthenticationFailed)


def test_elevenlabs_row_declares_policy_403s_and_the_xi_api_key_header():
    row = DEFAULT_CATALOG.providers["elevenlabs"]
    assert row.forbidden_means == "policy"
    assert row.auth_scheme == "header" and row.auth_header == "xi-api-key"


def test_elevenlabs_404_detail_body_counts_as_an_api_error():
    err = E.from_http_status(
        404, body=body({"detail": {"status": "voice_not_found", "message": "no such voice"}}),
        provider="elevenlabs", model="m")
    assert isinstance(err, E.ModelNotFound)


def test_elevenlabs_current_shape_and_422_validation_list_read_as_client_faults():
    current = E.from_http_status(
        400, body=body({"detail": {"type": "validation_error", "code": "text_too_long",
                                   "message": "too long", "request_id": "r1"}}),
        provider="elevenlabs")
    assert isinstance(current, E.InvalidRequest)
    listed = E.from_http_status(
        422, body=body({"detail": [{"loc": ["body", "text"], "msg": "field required",
                                   "type": "value_error.missing"}]}),
        provider="elevenlabs")
    assert isinstance(listed, E.InvalidRequest)


def test_elevenlabs_401_and_402_map_as_documented():
    assert isinstance(E.from_http_status(
        401, body=body({"detail": {"status": "invalid_api_key", "message": "bad"}}),
        provider="elevenlabs"), E.AuthenticationFailed)
    assert isinstance(E.from_http_status(
        402, body=body({"detail": {"status": "payment_required", "message": "pay"}}),
        provider="elevenlabs"), E.InsufficientCredits)


def test_elevenlabs_429_without_retry_after_is_a_neutral_rate_limit():
    err = E.from_http_status(
        429, body=body({"detail": {"status": "too_many_concurrent_requests",
                                   "message": "wait"}}),
        provider="elevenlabs")
    assert isinstance(err, E.RateLimited)
    assert err.health is E.Health.NEUTRAL


# --------------------------------------------------------------- AssemblyAI


def test_assemblyai_403_is_a_rate_limit_on_its_rows():
    for pid in ("assemblyai", "assemblyai-streaming", "assemblyai-sync"):
        row = DEFAULT_CATALOG.providers[pid]
        assert row.auth_scheme == "raw"
        assert row.key() == "assemblyai", "one account, one credential, one breaker"
    # The sync host is the exception, and it is not a preference: that host
    # has never been observed to answer 403 at all -- a bad key there is a
    # 404 with an RFC 7807 body (probe A4g). `rate_limit` there described a
    # response the provider does not send.
    for pid in ("assemblyai", "assemblyai-streaming"):
        assert DEFAULT_CATALOG.providers[pid].forbidden_means == "rate_limit"
    assert DEFAULT_CATALOG.providers["assemblyai-sync"].forbidden_means == "auth"
    err = E.from_http_status(403, body=body({"error": "Too many requests"}),
                             provider="assemblyai", forbidden_means="rate_limit")
    assert isinstance(err, E.RateLimited)
    assert not isinstance(err, E.AuthenticationFailed)


def test_assemblyai_401_stays_a_credential_failure():
    err = E.from_http_status(
        401, body=body({"error": "Authentication error, API token missing/invalid"}),
        provider="assemblyai", forbidden_means="rate_limit")
    assert isinstance(err, E.AuthenticationFailed)


# ------------------------------------------------------------- catalog rows


@pytest.mark.parametrize("model_id,unit,provider", [
    ("openai.gpt-4o-mini-tts", "tokens", "openai"),
    ("openai.gpt-transcribe", "seconds", "openai"),
    ("openai.whisper-1", "seconds", "openai"),
    ("inworld.tts-2", "characters", "inworld"),
    ("inworld.tts-2-flash", "characters", "inworld"),
    ("elevenlabs.flash-v2-5", "characters", "elevenlabs"),
    ("elevenlabs.v3-conversational", "characters", "elevenlabs"),
    ("assemblyai.sync", "seconds", "assemblyai-sync"),
    ("assemblyai.streaming", "seconds", "assemblyai-streaming"),
    ("assemblyai.universal-3-5-pro-realtime", "seconds", "assemblyai-streaming"),
    ("openai.text-embedding-3-small", "tokens", "openai"),
    ("openai.gpt-realtime-mini", "tokens", "openai"),
])
def test_voice_and_utility_rows_exist_with_the_right_unit(model_id, unit, provider):
    spec = DEFAULT_CATALOG.models[model_id]
    assert spec.unit == unit and spec.provider == provider
    assert spec.priced_at >= "2026-09-16"
    if unit == "seconds":
        assert spec.per_minute is not None and spec.per_minute > 0
    if unit == "characters":
        assert spec.input_per_m > 0 and spec.output_per_m == 0


def test_tts_rows_ask_for_the_tts_budget_profile():
    for model_id in ("openai.gpt-4o-mini-tts", "inworld.tts-2", "inworld.tts-2-flash",
                     "elevenlabs.flash-v2-5", "elevenlabs.v3-conversational"):
        assert DEFAULT_CATALOG.models[model_id].default_profile == "tts"


def test_deepseek_beta_row_has_a_path_prefix_and_shares_the_credential():
    beta = DEFAULT_CATALOG.providers["deepseek-beta"]
    anth = DEFAULT_CATALOG.providers["deepseek-anthropic"]
    assert beta.path_prefix == "/beta" and beta.key() == "deepseek"
    assert anth.kind == "anthropic" and anth.auth_scheme == "x-api-key"
    assert anth.key() == "deepseek"


def test_path_prefix_is_validated_at_construction():
    from llmgw.catalog import ProviderConn

    for bad in ("beta", "/beta/", "/"):
        with pytest.raises(ValueError):
            ProviderConn(id="x", kind="openai", path_prefix=bad)
    assert ProviderConn(id="x", kind="openai", path_prefix="/beta").path_prefix == "/beta"

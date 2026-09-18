"""The socket plane's classifiers, read against the captured frames.

Every fixture here is a frame the probe of 18 Sep 2026 actually saw, quoted
from `capabilities/captures-ws.md` with the base64 audio shortened. That is
the point of the file: a classifier tested against frames somebody invented
tests the inventor's understanding, and the four things this plane gets
wrong -- usage on the first chunk only, a non-fatal top-level error, an
in-context status that must not end the socket, and a bad key that looks
like silence -- are all things a plausible invented fixture gets right by
accident.
"""

from __future__ import annotations

import json

import pytest

from llmgw import errors
from llmgw.metrics import WS_CLOSE_CLASSES, WS_DIRECTIONS
from llmgw.surfaces.base import Usage
from llmgw.ws import INWORLD_TTS_WS, WS_REGISTRY
from llmgw.ws.errors import (
    CLOSE_REASON_PREFIX,
    MAX_REASON_BYTES,
    CloseCode,
    CloseVerdict,
    classify_close,
    close_code_class,
    reason_for,
    verdict_for,
)
from llmgw.ws.surfaces.base import AcceptPolicy, Frame, FrameClass

S0 = {"code": 0, "message": "", "details": []}


def f(obj: object) -> Frame:
    return Frame(json.dumps(obj).encode("utf-8"), text=True)


# ==========================================================================
# Frames from the captures
# ==========================================================================

CREATE = f({
    "create": {
        "modelId": "inworld-tts-1.5-mini", "voiceId": "Aarav",
        "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 16000},
        "bufferCharThreshold": 120, "maxBufferDelayMs": 3000, "autoMode": True,
        "applyTextNormalization": "ON",
    },
    "contextId": "ctx-32016aa9",
})
SEND_TEXT = f({
    "send_text": {"text": "Hello from the gateway probe."},
    "contextId": "ctx-32016aa9",
})
FLUSH = f({"flush_context": {}, "contextId": "ctx-32016aa9"})
CLOSE_CONTEXT = f({"close_context": {}, "contextId": "ctx-32016aa9"})

CONTEXT_CREATED = f({"result": {
    "contextId": "ctx-32016aa9",
    "contextCreated": {
        "voiceId": "Aarav", "modelId": "inworld-tts-1.5-mini",
        "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 16000},
    },
    "status": S0,
}})
AUDIO_FIRST = f({"result": {
    "contextId": "ctx-32016aa9",
    "audioChunk": {
        "audioContent": "UklGRi4AAABXQVZF",
        "usage": {"processedCharactersCount": 29, "modelId": "inworld-tts-1.5-mini"},
        "timestampInfo": None,
    },
    "status": S0,
}})
AUDIO_LATER = f({"result": {
    "contextId": "ctx-32016aa9",
    "audioChunk": {
        "audioContent": "AAAA",
        "usage": {"processedCharactersCount": 0, "modelId": "inworld-tts-1.5-mini"},
        "timestampInfo": None,
    },
    "status": S0,
}})
FLUSH_COMPLETED = f({"result": {
    "contextId": "ctx-32016aa9", "flushCompleted": {}, "status": S0,
}})
CONTEXT_CLOSED = f({"result": {
    "contextId": "ctx-32016aa9", "contextClosed": {}, "status": S0,
}})

BAD_KEY = f({"error": {
    "code": 7, "message": 'Invalid credentials provided for API key "<KEY4>***"',
    "details": [],
}})
NO_CREDENTIAL = f({"error": {
    "code": 16, "message": "authentication is required", "details": [],
}})
MALFORMED = f({"error": {
    "code": 3,
    "message": "invalid WebSocket request for the selected response protocol",
    "status": "INVALID_ARGUMENT",
}})
TEXT_TOO_LONG = f({"result": {
    "contextId": "ctx-big",
    "status": {"code": 3, "message": "text length should not exceed 2000 characters.",
               "details": []},
}})
SIXTH_CONTEXT = f({"result": {
    "contextId": "ctx-F",
    "status": {"code": 8, "message": "You have reached the limit of 5 TTS contexts "
                                     "per connection.", "details": []},
}})
UNKNOWN_CONTEXT = f({"result": {
    "contextId": "ctx-nope",
    "status": {"code": 5, "message": "context ctx-nope not found (payload=SEND_TEXT)",
               "details": []},
}})
NO_RETRY = f({"error": {
    "code": 16, "message": "authentication is required",
    "details": [{"reconnectType": "NO_RETRY"}],
}})


# ==========================================================================
# Classification
# ==========================================================================


@pytest.mark.parametrize(("frame", "kind", "context"), [
    (CREATE, FrameClass.META, "ctx-32016aa9"),
    (SEND_TEXT, FrameClass.CONTENT, "ctx-32016aa9"),
    (FLUSH, FrameClass.META, "ctx-32016aa9"),
    (CLOSE_CONTEXT, FrameClass.META, "ctx-32016aa9"),
])
def test_client_frames_classify_as_the_captures_read(frame, kind, context):
    verdict = INWORLD_TTS_WS.classify_client(frame)
    assert verdict.kind is kind
    assert verdict.context == context


def test_only_create_is_the_replayable_config_prefix():
    """C24: a fallback replays config and never content. `send_text` is the
    utterance -- replaying it bills the tenant twice for one sentence."""
    assert INWORLD_TTS_WS.classify_client(CREATE).config is True
    for frame in (SEND_TEXT, FLUSH, CLOSE_CONTEXT):
        assert INWORLD_TTS_WS.classify_client(frame).config is False


@pytest.mark.parametrize(("frame", "kind"), [
    (CONTEXT_CREATED, FrameClass.META),
    (AUDIO_FIRST, FrameClass.CONTENT),
    (AUDIO_LATER, FrameClass.CONTENT),
    (FLUSH_COMPLETED, FrameClass.META),
    (CONTEXT_CLOSED, FrameClass.TERMINAL),
    (BAD_KEY, FrameClass.ERROR),
    (MALFORMED, FrameClass.ERROR),
    (TEXT_TOO_LONG, FrameClass.ERROR),
    (SIXTH_CONTEXT, FrameClass.ERROR),
])
def test_upstream_frames_classify_as_the_captures_read(frame, kind):
    assert INWORLD_TTS_WS.classify_upstream(frame).kind is kind


def test_context_created_is_meta_and_not_content():
    """The distinction the liveness story hangs on. A provider that sent
    nothing but `contextCreated` forever is stalled, and a classifier that
    called it CONTENT would reset the progress clock on every one."""
    assert INWORLD_TTS_WS.classify_upstream(CONTEXT_CREATED).kind is FrameClass.META


def test_a_frame_that_is_not_json_is_relayed_as_meta_not_refused():
    """Probe 5: Inworld answered `this is not json` with a non-fatal error
    and kept the socket. A gateway that refused it would be enforcing a
    stricter protocol than the provider's own."""
    junk = Frame(b"this is not json", text=True)
    assert INWORLD_TTS_WS.classify_client(junk).kind is FrameClass.META
    assert INWORLD_TTS_WS.classify_upstream(junk).kind is FrameClass.META
    assert junk.payload() is None


def test_binary_frames_keep_their_opcode_through_the_wire_view():
    binary = Frame(b"\x00\x01\x02", text=False)
    assert binary.wire() == b"\x00\x01\x02"
    assert CREATE.wire() == CREATE.data.decode("utf-8")


# ==========================================================================
# Usage: the sum, not the last and not the first
# ==========================================================================


def test_usage_sums_across_flushes_and_ignores_the_zeroes():
    """Probe 1: the count is on the FIRST chunk of each flush and 0 after.
    Three utterances on one context report 29, 0, 0, 19, 0 and bill 48."""
    usage = Usage()
    for frame in (AUDIO_FIRST, AUDIO_LATER, AUDIO_LATER):
        INWORLD_TTS_WS.apply_usage(frame, usage)
    assert usage.characters == 29
    second = f({"result": {"contextId": "ctx-32016aa9", "audioChunk": {
        "audioContent": "AA", "usage": {"processedCharactersCount": 19}}, "status": S0}})
    INWORLD_TTS_WS.apply_usage(second, usage)
    INWORLD_TTS_WS.apply_usage(AUDIO_LATER, usage)
    assert usage.characters == 48
    assert usage.exact is True


def test_a_chunk_with_no_usage_object_is_not_a_parse_failure():
    """Absent is not malformed. The session falls back to counting
    `send_text` and says so in `cost_notes`; it does not report a broken
    provider shape."""
    usage = Usage()
    INWORLD_TTS_WS.apply_usage(
        f({"result": {"contextId": "c", "audioChunk": {"audioContent": "AA"}}}), usage,
    )
    assert usage.characters == 0
    assert usage.parse_failures == 0
    assert usage.exact is False


def test_a_usage_object_with_a_junk_count_is_a_parse_failure():
    usage = Usage()
    INWORLD_TTS_WS.apply_usage(
        f({"result": {"contextId": "c", "audioChunk": {
            "audioContent": "AA", "usage": {"processedCharactersCount": "twenty"}}}}),
        usage,
    )
    assert usage.parse_failures == 1
    assert usage.characters == 0


def test_apply_usage_never_raises_on_anything():
    usage = Usage()
    for frame in (Frame(b"", text=True), Frame(b"[]", text=True),
                  Frame(b"\xff\xfe", text=False), CONTEXT_CREATED, BAD_KEY):
        INWORLD_TTS_WS.apply_usage(frame, usage)
    assert usage.characters == 0


# ==========================================================================
# The one edit
# ==========================================================================


def test_create_model_id_is_rewritten_to_the_wire_id():
    out, changed = INWORLD_TTS_WS.rewrite_first_frame(CREATE, "inworld-tts-2-flash")
    assert changed is True
    body = json.loads(out.data)
    assert body["create"]["modelId"] == "inworld-tts-2-flash"
    # Everything else survives: a rewrite that dropped `voiceId` would make
    # the provider synthesise in the wrong voice and nothing would say why.
    assert body["create"]["voiceId"] == "Aarav"
    assert body["contextId"] == "ctx-32016aa9"
    assert body["create"]["audioConfig"]["sampleRateHertz"] == 16000


def test_a_frame_that_already_names_the_wire_id_is_returned_untouched():
    """Byte-for-byte, and the SAME OBJECT: a re-serialisation changes key
    order and whitespace on a frame nobody had a reason to touch."""
    same = f({"create": {"modelId": "inworld-tts-2-flash"}, "contextId": "c"})
    out, changed = INWORLD_TTS_WS.rewrite_first_frame(same, "inworld-tts-2-flash")
    assert changed is False
    assert out is same


def test_the_alias_the_plugin_sends_resolves_to_a_real_row():
    """PLAN-G R10: Layrs pins `inworld-tts-1.5-mini` (deprecated) and the
    plugin drops the path prefix, so this alias is the ONLY thing that lets a
    relayed session name a target at all."""
    from llmgw.catalog import DEFAULT_CATALOG

    target = DEFAULT_CATALOG.resolve("inworld-tts-1.5-mini")
    assert target.model.id == "inworld.tts-2-flash"
    assert target.model.api_model == "inworld-tts-2-flash"
    assert target.model.unit == "characters"
    assert INWORLD_TTS_WS.model_from_first_frame(CREATE) == "inworld-tts-1.5-mini"


def test_a_frame_with_no_model_names_none_and_is_not_rewritten():
    bare = f({"create": {"voiceId": "Aarav"}, "contextId": "c"})
    assert INWORLD_TTS_WS.model_from_first_frame(bare) is None
    out, changed = INWORLD_TTS_WS.rewrite_first_frame(bare, "inworld-tts-2")
    assert changed is True and json.loads(out.data)["create"]["modelId"] == "inworld-tts-2"


# ==========================================================================
# Errors: fatal, non-fatal, and the ones the socket survives
# ==========================================================================


@pytest.mark.parametrize(("frame", "cls"), [
    (BAD_KEY, errors.AuthenticationFailed),
    (NO_CREDENTIAL, errors.AuthenticationFailed),
    (MALFORMED, errors.InvalidRequest),
    (TEXT_TOO_LONG, errors.InvalidRequest),
    (SIXTH_CONTEXT, errors.InvalidRequest),
])
def test_error_frames_map_to_the_taxonomy(frame, cls):
    assert isinstance(INWORLD_TTS_WS.error_from_frame(frame), cls)


def test_a_credential_error_is_scoped_to_the_credential_not_the_model():
    """FAILURE-MODES row 8: a bad key must not open a circuit against a
    provider for every tenant that shares the model."""
    err = INWORLD_TTS_WS.error_from_frame(BAD_KEY)
    assert err.health_scope is errors.HealthScope.CREDENTIAL


def test_an_error_frame_never_quotes_the_provider_message():
    """Inworld reflects the first four characters of the key it refused
    (`<KEY4>***`). The row says `scrub_error_bodies="all"`; the same rule has
    to hold where the body is a frame."""
    err = INWORLD_TTS_WS.error_from_frame(BAD_KEY)
    assert "<KEY4>" not in err.message
    assert "***" not in err.message


def test_an_unknown_context_status_is_benign():
    """Code 5 happens whenever a tidy client closes a context twice. The
    plugin already treats it as that context failing; counting it as a
    provider error would make a correct client look like a sick provider."""
    assert INWORLD_TTS_WS.error_from_frame(UNKNOWN_CONTEXT) is None


def test_only_a_top_level_error_can_be_fatal():
    assert INWORLD_TTS_WS.is_fatal(BAD_KEY) is True
    assert INWORLD_TTS_WS.is_fatal(MALFORMED) is True
    assert INWORLD_TTS_WS.is_fatal(TEXT_TOO_LONG) is False
    assert INWORLD_TTS_WS.is_fatal(SIXTH_CONTEXT) is False
    assert INWORLD_TTS_WS.is_fatal(AUDIO_FIRST) is False


def test_no_retry_is_read_off_the_providers_own_hint():
    assert INWORLD_TTS_WS.no_retry(NO_RETRY) is True
    assert INWORLD_TTS_WS.no_retry(BAD_KEY) is False


def test_tts_has_no_drain_message():
    """There is no session terminate in this protocol, and the gateway does
    not invent `close_context` frames the client did not send (C2, C25)."""
    assert INWORLD_TTS_WS.drain_message() is None


# ==========================================================================
# Close codes
# ==========================================================================


def test_the_close_code_range_is_clear_of_every_provider_code():
    """1000-1011 are the RFC's, 3005-3009 and 410 AssemblyAI's, 4000
    OpenAI's, 4300 ElevenLabs'. Nothing observed reaches 4900."""
    provider_codes = {1000, 1001, 1006, 1008, 1009, 1011, 3000, 3005, 3006,
                      3007, 3008, 3009, 410, 4000, 4300}
    ours = {int(c) for c in CloseCode}
    assert ours & provider_codes == set()
    assert all(4900 <= c <= 4999 for c in ours)
    assert len(ours) == len(set(CloseCode))


def test_every_close_reason_is_a_known_error_code():
    """The reason is `llmgw:<code>` and `<code>` is the taxonomy's, so a
    client may switch on either the code or the reason and neither will
    acquire a value later that the other cannot express."""
    for code in CloseCode:
        reason = reason_for(code)
        assert reason.startswith(CLOSE_REASON_PREFIX)
        assert reason[len(CLOSE_REASON_PREFIX):] in errors.ERROR_CODES
        assert len(reason.encode("utf-8")) <= MAX_REASON_BYTES


def test_an_errors_own_code_is_finer_than_the_close_codes_default():
    verdict = CloseVerdict(
        CloseCode.UPSTREAM_HANDSHAKE, errors.ConnectTimeout("nope"),
    )
    assert verdict.reason == "llmgw:connect_timeout"
    assert CloseVerdict(CloseCode.UPSTREAM_HANDSHAKE).reason == "llmgw:headers_timeout"


@pytest.mark.parametrize(("err", "code"), [
    (errors.SessionDraining(), CloseCode.DRAINING),
    (errors.TotalDeadlineExceeded(), CloseCode.SESSION_TOTAL),
    (errors.StallTimeout(), CloseCode.PROVIDER_STALL),
    (errors.FirstEventTimeout(), CloseCode.PROVIDER_STALL),
    (errors.ClientTooSlow(), CloseCode.CLIENT_STALL),
    (errors.UpstreamDisconnected(), CloseCode.UPSTREAM_GONE),
    (errors.IncompleteStream(), CloseCode.UPSTREAM_GONE),
    (errors.FrameTooLarge(), CloseCode.FRAME_TOO_LARGE),
    (errors.SessionIdle(), CloseCode.IDLE),
    (errors.HeadersTimeout(), CloseCode.UPSTREAM_HANDSHAKE),
    (errors.AuthenticationFailed(), CloseCode.UPSTREAM_HANDSHAKE),
    (errors.PolicyError(), CloseCode.UPSTREAM_HANDSHAKE),
])
def test_every_taxonomy_class_reaches_a_close_code(err, code):
    assert verdict_for(err) is code


@pytest.mark.parametrize(("code", "klass"), [
    (1000, "normal_1000"), (1001, "going_away_1001"), (1006, "abnormal_1006"),
    (None, "abnormal_1006"), (1008, "policy_1008"), (1009, "too_large_1009"),
    (1011, "internal_1011"), (3008, "provider_3xxx"), (4000, "provider_4xxx"),
    (4902, "gateway_49xx"), (2000, "other"),
])
def test_close_code_classes_are_the_closed_metric_set(code, klass):
    assert close_code_class(code) == klass
    assert klass in WS_CLOSE_CLASSES


def test_the_class_function_is_total_over_the_legal_range():
    for code in [None, *range(1000, 5000)]:
        assert close_code_class(code) in WS_CLOSE_CLASSES


# ==========================================================================
# classify_close: the third input
# ==========================================================================


def test_a_1000_after_a_fatal_error_frame_is_the_error_not_a_clean_end():
    """Probe 2b: the bad key's `error` and its CLOSE 1000 arrive in the same
    millisecond. The close looks clean and the session is anything but."""
    hint = INWORLD_TTS_WS.error_from_frame(BAD_KEY)
    err = classify_close(1000, "", product="inworld", last_error_frame=hint)
    assert isinstance(err, errors.AuthenticationFailed)


def test_an_unsolicited_1000_mid_session_is_an_incomplete_stream():
    err = classify_close(1000, "", product="inworld")
    assert isinstance(err, errors.IncompleteStream)
    assert err.health is errors.Health.FAILURE


def test_a_1000_we_asked_for_is_a_clean_end():
    assert classify_close(1000, "", product="inworld", expected=True) is None
    assert classify_close(None, "", product="inworld", expected=True) is None


@pytest.mark.parametrize(("code", "cls", "health"), [
    (1001, errors.UpstreamDisconnected, errors.Health.FAILURE),
    (1006, errors.UpstreamDisconnected, errors.Health.FAILURE),
    (1009, errors.FrameTooLarge, errors.Health.NEUTRAL),
    (1011, errors.UpstreamServerError, errors.Health.FAILURE),
    (3005, errors.UpstreamServerError, errors.Health.FAILURE),
])
def test_provider_close_codes_map_to_the_taxonomy(code, cls, health):
    err = classify_close(code, "", product="inworld")
    assert isinstance(err, cls)
    assert err.health is health


def test_1009_is_blamed_on_the_gateway_because_the_bound_is_ours():
    err = classify_close(1009, "", product="inworld")
    assert err.blame is errors.Blame.GATEWAY


def test_assemblyai_1008_without_an_error_frame_is_an_auth_failure():
    """C30: the sweep says an absent `Error` frame means the credential."""
    err = classify_close(1008, "", product="assemblyai-streaming")
    assert isinstance(err, errors.AuthenticationFailed)


def test_1008_elsewhere_is_the_clients_own_policy_violation():
    err = classify_close(1008, "nope", product="inworld")
    assert isinstance(err, errors.InvalidRequest)
    assert err.blame is errors.Blame.CLIENT


# ==========================================================================
# The registry
# ==========================================================================


def test_every_ws_surface_declares_the_whole_protocol():
    for surface in WS_REGISTRY:
        assert surface.routes and all(r.startswith("/") for r in surface.routes)
        assert surface.upstream_path.startswith("/")
        assert surface.auth_schemes <= {"bearer", "basic", "raw"}
        assert isinstance(surface.accept_policy, AcceptPolicy)
        assert set(surface.name) <= set("abcdefghijklmnopqrstuvwxyz_")


def test_inworld_tts_accepts_basic_because_that_is_what_the_plugin_sends():
    """tts.py:913 builds `"Basic " + INWORLD_API_KEY` in the constructor and
    tts.py:263-267 sends it verbatim. Demanding Bearer would mean asking
    Layrs to patch a pinned third-party library to reach the gateway."""
    assert "basic" in INWORLD_TTS_WS.auth_schemes
    assert "bearer" in INWORLD_TTS_WS.auth_schemes


def test_inworld_accepts_then_relays_because_silence_is_health_here():
    assert INWORLD_TTS_WS.accept_policy is AcceptPolicy.ACCEPT_THEN_RELAY


def test_the_direction_vocabulary_matches_the_metric_label_set():
    from llmgw.ws.relay import Direction

    assert {d.value for d in Direction} == set(WS_DIRECTIONS)

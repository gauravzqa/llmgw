"""The WebSocket fake's frame builders, against the captures.

`fakes/ws.py` is an instrument: every contract test the gateway will write
reads "the upstream sent an `audioChunk` with usage on the first chunk" and
concludes something about the relay. That conclusion is worth nothing if the
frame the fake emits is not the frame Inworld emits, so the builders are
checked here field by field against `capabilities/captures-ws.md` -- the
literals below are copied out of its frame logs, not out of `fakes/ws.py`.

AssemblyAI has no capture (there is no key). Its builders are checked against
the field lists in `capabilities/voice-assemblyai.md` section 2, and each
assertion says which row it comes from.
"""

from __future__ import annotations

import base64
import itertools

import pytest
from fakes import ws
from fakes.upstream import WS_MODES, WS_PATHS

# --------------------------------------------------------------------------
# Fixtures lifted from capabilities/captures-ws.md
# --------------------------------------------------------------------------

CAPTURED_CONTEXT_CREATED_KEYS = {
    "voiceId", "audioConfig", "modelId", "maxBufferDelayMs", "bufferCharThreshold",
    "applyTextNormalization", "autoMode", "synthesisContext",
    "pronunciationDictionarySettings",
}
"""Probe 1, the `contextCreated` body."""

CAPTURED_FINAL_CHUNK_B64 = (
    "UklGRi4AAABXQVZFZm10IBAAAAABAAEAgD4AAAB9AAACABAAZGF0YQoAAAD6//r/+v/7//r/"
)
"""Probe 1's 54-byte trailing chunk: a second 44-byte RIFF header plus five
samples. Decoded here so the header builder is checked byte for byte."""

CAPTURED_STATUS_OK = {"code": 0, "message": "", "details": []}
"""`S0` in the capture's shorthand."""

CAPTURED_RESPONSE_USAGE = {
    "total_tokens": 127,
    "input_tokens": 120,
    "output_tokens": 7,
    "input_token_details": {
        "text_tokens": 120, "audio_tokens": 0, "image_tokens": 0,
        "cached_tokens": 64,
        "cached_tokens_details": {"text_tokens": 64, "audio_tokens": 0,
                                  "image_tokens": 0},
    },
    "output_token_details": {"text_tokens": 7, "audio_tokens": 0},
}
"""captures-ws.md item 15, verbatim."""

CAPTURED_TRANSCRIPTION_USAGE = {
    "type": "tokens", "total_tokens": 24, "input_tokens": 16,
    "input_token_details": {"text_tokens": 0, "audio_tokens": 16},
    "output_tokens": 8,
}
"""Probe 7's `...transcription.completed.usage`, verbatim."""

CAPTURED_RESPONSE_CYCLE = [
    "response.created", "response.output_item.added", "conversation.item.added",
    "response.content_part.added",
    "response.output_text.delta", "response.output_text.delta",
    "response.output_text.delta", "response.output_text.delta",
    "response.output_text.delta",
    "response.output_text.done", "response.content_part.done",
    "conversation.item.done", "response.output_item.done", "response.done",
]
"""Probe 8's response cycle in order, five deltas for "Hi there, friend!".
`rate_limits.updated` is deliberately absent: it arrives 43 ms later."""


def _ids():
    counter = itertools.count(1)
    return lambda: f"event_{next(counter)}"


# --------------------------------------------------------------------------
# Inworld: the envelope
# --------------------------------------------------------------------------


def test_every_tts_server_frame_echoes_the_context_id_and_status_ok():
    """captures-ws.md 3.9: every server frame on a TTS socket carries
    `result.contextId`, the client's own string, verbatim."""
    frames = [
        ws.tts_context_created("ctx-32016aa9", {}),
        ws.tts_audio_chunk("ctx-32016aa9", b"\x00\x01", characters=29),
        ws.tts_flush_completed("ctx-32016aa9"),
        ws.tts_context_closed("ctx-32016aa9"),
    ]
    for frame in frames:
        assert set(frame) == {"result"}
        assert frame["result"]["contextId"] == "ctx-32016aa9"
        assert frame["result"]["status"] == CAPTURED_STATUS_OK


def test_status_ok_is_a_fresh_object_per_frame():
    """A builder that shared one mutable `S0` would let a test that pokes at
    one frame change the next one. Pure means pure."""
    a = ws.tts_flush_completed("ctx-1")
    b = ws.tts_flush_completed("ctx-1")
    assert a == b
    a["result"]["status"]["code"] = 9
    assert b["result"]["status"] == CAPTURED_STATUS_OK


def test_context_created_has_the_captured_key_set():
    frame = ws.tts_context_created("ctx-1", {
        "modelId": "inworld-tts-1.5-mini", "voiceId": "Aarav",
        "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 16000},
        "bufferCharThreshold": 120, "maxBufferDelayMs": 3000,
        "applyTextNormalization": "ON", "autoMode": True,
    })
    body = frame["result"]["contextCreated"]
    assert set(body) == CAPTURED_CONTEXT_CREATED_KEYS
    assert body["synthesisContext"] is None
    assert body["pronunciationDictionarySettings"] is None
    assert body["audioConfig"] == {"audioEncoding": "LINEAR16",
                                   "sampleRateHertz": 16000}


def test_context_created_fills_the_plugin_defaults_for_a_bare_create():
    """The plugin always sends `autoMode` and a model; a bare `create` is a
    test's shorthand and must still produce the captured shape."""
    body = ws.tts_context_created("ctx-1", {})["result"]["contextCreated"]
    assert set(body) == CAPTURED_CONTEXT_CREATED_KEYS
    assert body["modelId"] == ws.INWORLD_TTS_MODEL
    assert body["autoMode"] is True


# --------------------------------------------------------------------------
# Inworld: audio
# --------------------------------------------------------------------------


def test_riff_header_is_the_captured_44_bytes():
    captured = base64.b64decode(CAPTURED_FINAL_CHUNK_B64)
    assert len(captured) == 54
    assert ws.riff_header(ws.TTS_FINAL_CHUNK_PCM) == captured[:44]


def test_pcm_is_total():
    assert ws.pcm(-5) == b""
    assert len(ws.pcm(0)) == 0
    assert len(ws.pcm(7)) == 6  # odd requests are clamped to whole samples
    assert len(ws.pcm(100_000)) == 100_000
    assert ws.pcm(64) == ws.pcm(64)  # deterministic


def test_flush_carries_usage_on_the_first_chunk_and_zero_after():
    """captures-ws.md item 11: `processedCharactersCount` is the flush's whole
    count on the first chunk and 0 on every later one. Sum, do not take the
    last."""
    frames = ws.tts_flush_frames("ctx-1", characters=29, chunks=4, chunk_bytes=6434)
    chunks = [f["result"]["audioChunk"] for f in frames if "audioChunk" in f["result"]]
    assert [c["usage"]["processedCharactersCount"] for c in chunks] == [29, 0, 0, 0, 0]
    assert {c["usage"]["modelId"] for c in chunks} == {ws.INWORLD_TTS_MODEL}
    assert sum(c["usage"]["processedCharactersCount"] for c in chunks) == 29


def test_flush_puts_a_riff_header_on_the_first_and_the_last_chunk_only():
    """captures-ws.md item 17: the first chunk AND the tiny trailing chunk both
    begin `RIFF`; a raw-PCM consumer strips a header from every chunk that
    does, not only the first."""
    frames = ws.tts_flush_frames("ctx-1", characters=29, chunks=4, chunk_bytes=6434)
    audio = [base64.b64decode(f["result"]["audioChunk"]["audioContent"])
             for f in frames if "audioChunk" in f["result"]]
    assert [a[:4] == b"RIFF" for a in audio] == [True, False, False, False, True]
    assert [len(a) for a in audio] == [6434, 6434, 6434, 6434, 54]
    assert frames[-1]["result"]["flushCompleted"] == {}


def test_flush_frames_is_total():
    assert ws.tts_flush_frames("c", characters=0, chunks=0, chunk_bytes=6434) == [
        ws.tts_flush_completed("c")]
    assert ws.tts_flush_frames("c", characters=1, chunks=-3, chunk_bytes=64) == [
        ws.tts_flush_completed("c")]
    tiny = ws.tts_flush_frames("c", characters=1, chunks=1, chunk_bytes=1)
    body = base64.b64decode(tiny[0]["result"]["audioChunk"]["audioContent"])
    assert body[:4] == b"RIFF"  # clamped up to the header, never truncated


def test_a_48044_byte_chunk_is_a_header_plus_1_5_s_of_16_khz_pcm():
    """The line the bench drives: `X-Fake-Bytes` is the DECODED chunk size,
    header included."""
    frames = ws.tts_flush_frames("c", characters=29, chunks=1, chunk_bytes=48_044)
    first = base64.b64decode(frames[0]["result"]["audioChunk"]["audioContent"])
    assert len(first) == 48_044
    assert len(first) - 44 == 48_000 == int(1.5 * 16_000 * 2)


# --------------------------------------------------------------------------
# Inworld: faults
# --------------------------------------------------------------------------


def test_the_two_top_level_error_shapes_differ_exactly_as_captured():
    """Probe 2b's fatal error carries `details: []`; probe 5's non-fatal one
    carries `"status": "INVALID_ARGUMENT"` and NO `details`. The gateway's
    classifier is expected to tell them apart, so the fake must not blur
    them."""
    fatal = ws.bad_key_error()["error"]
    assert fatal["code"] == 7
    assert fatal["details"] == []
    assert "status" not in fatal
    assert "fake***" in fatal["message"]

    nonfatal = ws.malformed_frame_error()["error"]
    assert nonfatal == {
        "code": 3,
        "message": "invalid WebSocket request for the selected response protocol",
        "status": "INVALID_ARGUMENT",
    }


def test_missing_credential_is_code_16_with_the_inworld_status_block():
    body = ws.missing_credential_error()["error"]
    assert body["code"] == 16
    assert body["message"] == "authentication is required"
    assert body["details"][0]["errorType"] == "SESSION_TOKEN_INVALID"
    assert body["details"][0]["retryType"] == "NO_RETRY"


@pytest.mark.parametrize(("frame", "code", "fragment"), [
    (ws.context_limit_status("ctx-F"), 8, "limit of 5 TTS contexts per connection"),
    (ws.context_not_found_status("ctx-nope", "SEND_TEXT"), 5,
     "context ctx-nope not found (payload=SEND_TEXT)"),
    (ws.context_not_found_status("ctx-F"), 5, "context ctx-F not found"),
    (ws.text_too_long_status("ctx-big"), 3,
     "text length should not exceed 2000 characters."),
])
def test_in_context_faults_keep_the_context_id_and_the_captured_message(
    frame, code, fragment
):
    """captures-ws.md 1.3: an in-context fault is `result.status.code != 0`
    WITH the `contextId`, and the socket survives it."""
    result = frame["result"]
    assert result["contextId"]
    assert result["status"]["code"] == code
    assert fragment in result["status"]["message"]
    assert "audioChunk" not in result


def test_unsupported_model_error_names_the_model_and_the_docs_url():
    body = ws.unsupported_model_error("inworld/no-such-model")["error"]
    assert body["code"] == 3
    assert 'Unsupported model "inworld/no-such-model"' in body["message"]
    assert "docs.inworld.ai" in body["message"]


# --------------------------------------------------------------------------
# Inworld STT
# --------------------------------------------------------------------------


def test_stt_frames_have_no_context_id():
    """Probe 6: an STT socket has no contexts, so `result` carries the payload
    and the status and nothing else."""
    for frame in (ws.stt_speech_started(), ws.stt_speech_stopped(),
                  ws.stt_transcription("Hello from the gate.", is_final=False),
                  ws.stt_usage(1500)):
        assert "contextId" not in frame["result"]
        assert frame["result"]["status"] == CAPTURED_STATUS_OK


def test_stt_transcription_has_the_captured_key_set():
    body = ws.stt_transcription(ws.STT_FINAL_TEXT, is_final=True)["result"][
        "transcription"]
    assert set(body) == {"transcript", "isFinal", "wordTimestamps", "voiceProfile",
                         "silenceDurationMs"}
    assert body["transcript"] == "Hello from the Gateway Probe."
    assert body["isFinal"] is True


def test_stt_usage_is_the_single_frame_that_follows_close_stream():
    body = ws.stt_usage(1500)["result"]["usage"]
    assert body == {"transcribedAudioMs": 1500, "modelId": "inworld/inworld-stt-1"}


def test_transcribed_audio_ms_is_rounded_to_the_captured_granularity():
    """The real number is the provider's own speech count, which no fake can
    reproduce; what is reproducible is its 50 ms granularity and that it grows
    with the audio."""
    a = ws.transcribed_audio_ms(52_016, sample_rate=16_000)
    b = ws.transcribed_audio_ms(243_200, sample_rate=16_000)
    assert a % 50 == 0 and b % 50 == 0
    assert 0 < a < b
    assert ws.transcribed_audio_ms(0, sample_rate=0) == 0  # total


# --------------------------------------------------------------------------
# OpenAI Realtime
# --------------------------------------------------------------------------


def test_response_usage_is_the_captured_object():
    assert ws.oa_response_usage() == CAPTURED_RESPONSE_USAGE
    assert (ws.oa_response_usage()["input_token_details"]["cached_tokens"]
            <= ws.oa_response_usage()["input_tokens"]), "cached is a subset of input"


def test_transcription_usage_is_the_other_captured_object():
    assert ws.oa_transcription_usage() == CAPTURED_TRANSCRIPTION_USAGE
    assert "cached_tokens" not in ws.oa_transcription_usage()["input_token_details"]


def test_the_response_cycle_is_the_captured_order_and_ends_at_response_done():
    frames = ws.oa_text_response_cycle(
        response_id="resp_1", item_id="item_1", conversation_id="conv_1",
        event_id=_ids())
    assert [f["type"] for f in frames] == CAPTURED_RESPONSE_CYCLE
    done = frames[-1]
    assert done["response"]["status"] == "completed"
    assert done["response"]["usage"] == CAPTURED_RESPONSE_USAGE
    assert done["response"]["output"][0]["content"][0]["text"] == "Hi there, friend!"
    created = frames[0]
    assert created["response"]["status"] == "in_progress"
    assert created["response"]["usage"] is None
    assert created["response"]["output"] == []


def test_every_server_event_carries_an_event_id():
    frames = ws.oa_text_response_cycle(
        response_id="r", item_id="i", conversation_id="c", event_id=_ids())
    assert all(f["event_id"] for f in frames)
    assert len({f["event_id"] for f in frames}) == len(frames)


def test_the_transcription_cycle_ends_with_the_completed_event_and_its_usage():
    frames = ws.oa_transcription_cycle(item_id="item_1", event_id=_ids())
    assert [f["type"] for f in frames] == [
        "input_audio_buffer.committed", "conversation.item.added",
        "conversation.item.done",
        *["conversation.item.input_audio_transcription.delta"] * 6,
        "conversation.item.input_audio_transcription.completed",
    ]
    assert frames[-1]["transcript"] == "Hello from the Gateway Probe."
    assert frames[-1]["usage"] == CAPTURED_TRANSCRIPTION_USAGE


def test_the_error_event_nests_a_null_event_id_of_its_own():
    """Probe 8's note: `error.event_id` was null on all four errors even though
    the outer envelope had one."""
    frame = ws.oa_unknown_type_error("event_9", "nope")
    assert frame["type"] == "error"
    assert frame["event_id"] == "event_9"
    assert frame["error"]["event_id"] is None
    assert frame["error"]["param"] == "type"
    assert frame["error"]["code"] == "invalid_value"
    assert "'response.create'" in frame["error"]["message"]


@pytest.mark.parametrize(("frame", "code"), [
    (ws.oa_auth_error("e"), "invalid_api_key"),
    (ws.oa_missing_model_error("e"), "missing_model"),
    (ws.oa_invalid_model_error("e", "gpt-nope"), "invalid_model"),
    (ws.oa_beta_header_error("e"), "beta_api_shape_disabled"),
    (ws.oa_invalid_json_error("e"), "invalid_json"),
    (ws.oa_binary_error("e"), "invalid_event"),
])
def test_the_fatal_and_nonfatal_errors_share_one_shape(frame, code):
    """They are told apart by the CLOSE that does or does not follow, never by
    the event (captures-ws.md item 13), so the builders must not differ."""
    assert frame["error"]["type"] == "invalid_request_error"
    assert frame["error"]["code"] == code
    assert frame["error"]["message"]


def test_no_error_message_contains_a_key_shaped_string():
    """The live bodies echo a masked key prefix; the fake's must not carry
    anything a reader could mistake for one."""
    for frame in (ws.oa_auth_error("e"), ws.bad_key_error()):
        message = str(frame)
        assert "sk-" not in message
        assert "fake***" in message


def test_session_created_objects_match_the_captured_sessions():
    transcription = ws.oa_transcription_session("sess_1", expires_at=1789728574)
    assert transcription["object"] == "realtime.transcription_session"
    assert transcription["type"] == "transcription"
    assert transcription["audio"]["input"]["format"] == {"type": "audio/pcm",
                                                         "rate": 24000}
    assert transcription["audio"]["input"]["transcription"] is None
    assert transcription["audio"]["input"]["turn_detection"] == {
        "type": "server_vad", "threshold": 0.5, "prefix_padding_ms": 300,
        "silence_duration_ms": 200}
    assert transcription["include"] is None

    realtime = ws.oa_realtime_session("sess_1", model="gpt-realtime-mini",
                                      expires_at=1789728574)
    assert realtime["object"] == "realtime.session"
    assert realtime["output_modalities"] == ["audio"]
    assert realtime["audio"]["output"]["voice"] == "alloy"
    assert realtime["max_output_tokens"] == "inf"
    assert realtime["truncation"] == "auto"
    assert realtime["audio"]["input"]["turn_detection"]["create_response"] is True


def test_the_transcription_session_gets_a_model_only_after_an_update():
    """Probe 7: `session.created` has `transcription: null`; the model appears
    in `session.updated`."""
    updated = ws.oa_transcription_session("s", expires_at=0,
                                          model="gpt-4o-mini-transcribe")
    assert updated["audio"]["input"]["transcription"] == {
        "model": "gpt-4o-mini-transcribe", "language": None, "prompt": None}


def test_rate_limits_carries_only_the_tokens_entry():
    body = ws.oa_rate_limits()
    assert [entry["name"] for entry in body["rate_limits"]] == ["tokens"]
    assert body["rate_limits"][0]["limit"] == 15_000_000
    assert body["rate_limits"][0]["reset_seconds"] == 0.001


def test_rate_limits_delay_is_the_measured_43_ms():
    assert ws.RATE_LIMITS_AFTER_DONE_S == pytest.approx(0.043)


# --------------------------------------------------------------------------
# AssemblyAI (docs only; each assertion names its row)
# --------------------------------------------------------------------------


def test_begin_has_the_documented_fields():
    """voice-assemblyai.md 2: `Begin{id, expires_at, configuration}`; row 4:
    `configuration.api_version` echoes the pin; PLAN-G 3.4 checks
    `configuration.model` against the target's `api_model`."""
    frame = ws.aai_begin("sess_1", expires_at=123)
    assert set(frame) == {"type", "id", "expires_at", "configuration"}
    assert frame["type"] == "Begin"
    assert frame["configuration"]["model"] == ws.AAI_MODEL
    assert frame["configuration"]["api_version"] == ws.AAI_API_VERSION


def test_turn_has_the_documented_fields_and_marks_finality():
    """voice-assemblyai.md 2: `Turn{turn_order, transcript, utterance,
    end_of_turn, turn_is_formatted, end_of_turn_confidence, words[]}`;
    `end_of_turn: true` marks the final turn and `word_is_final` the words."""
    partial = ws.aai_turn(1, "hello there", end_of_turn=False)
    assert set(partial) == {"type", "turn_order", "transcript", "utterance",
                            "end_of_turn", "turn_is_formatted",
                            "end_of_turn_confidence", "words"}
    assert partial["end_of_turn"] is False
    assert [w["word_is_final"] for w in partial["words"]] == [False, False]
    final = ws.aai_turn(2, "hello there", end_of_turn=True, formatted=True)
    assert final["end_of_turn"] is True and final["turn_is_formatted"] is True
    assert all(w["word_is_final"] for w in final["words"])
    assert set(final["words"][0]) == {"text", "start", "end", "confidence",
                                      "word_is_final"}


def test_termination_carries_both_durations():
    """voice-assemblyai.md 2 and 6: `session_duration_seconds` is the billing
    number (session-open wall time), `audio_duration_seconds` the work."""
    frame = ws.aai_termination(audio_duration_seconds=1.632,
                               session_duration_seconds=12.5)
    assert set(frame) == {"type", "audio_duration_seconds",
                          "session_duration_seconds"}
    assert frame["session_duration_seconds"] == 12.5


def test_error_frame_is_read_instead_of_the_close_reason():
    """voice-assemblyai.md 5: `Error{type, error_code, error}` precedes the
    close; 1008 covers auth AND the opens limit, so the frame is the only way
    to tell them apart."""
    frame = ws.aai_error(*ws.AAI_TOO_MANY_SESSIONS)
    assert set(frame) == {"type", "error_code", "error"}
    assert frame["error_code"] == 3009
    assert "Too many concurrent sessions" in frame["error"]


# --------------------------------------------------------------------------
# The mode table itself
# --------------------------------------------------------------------------


def test_every_mode_is_supported_somewhere_and_nothing_else_is():
    supported = set().union(*ws.SUPPORTED.values())
    assert supported == set(WS_MODES), "a mode nobody implements is a mode nobody has"
    assert set(ws.SUPPORTED) == set(WS_PATHS)


def test_the_inworld_bad_key_mode_is_the_one_g0_named():
    """PLAN-G's G0 results item 1: `silent-after-101` is NOT a bad-key mode."""
    assert "silent-after-101" not in WS_MODES
    assert ("error-7-then-close-1000-on-first-message"
            in ws.SUPPORTED[ws.TTS_PATH])
    assert "context-multiplex" not in ws.SUPPORTED[ws.REALTIME_PATH]

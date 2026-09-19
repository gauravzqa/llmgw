"""PLAN-2 Phase D: the five voice surfaces, frame by frame and body by body.

Every shape here is one recorded in capabilities/voice-*.md: OpenAI's CRLF
SSE audio events and duration usage, Inworld's first-line character count
and its `usage: null` for empty text, ElevenLabs' `character-cost` header,
AssemblyAI's `audio_duration_ms`. No sockets; the pump's classifier contract
(C7) and the bill are what is under test.
"""

from __future__ import annotations

import base64
import json

import pytest

from llmgw import errors
from llmgw.framing import JSONLFramer, RawFramer, SSEFramer
from llmgw.sse import SSEEvent
from llmgw.surfaces import REGISTRY, SURFACES
from llmgw.surfaces.base import EventKind, Usage
from llmgw.surfaces.voice import (
    ASSEMBLYAI_SYNC,
    AUDIO_SPEECH,
    AUDIO_SPEECH_SSE,
    AUDIO_TRANSCRIPTION,
    ELEVENLABS_STT,
    ELEVENLABS_TTS,
    ELEVENLABS_TTS_TIMESTAMPS,
    INWORLD_STT,
    INWORLD_TTS,
    VOICE_ROUTES,
    VOICE_SURFACES,
    AudioSpeechSurface,
    ElevenLabsTTSSurface,
    VoiceRequestFacts,
)
from llmgw.surfaces.voice.audio_transcription import scan_form_fields


def ev(data: str | bytes, *, event: str | None = None) -> SSEEvent:
    raw = data.encode() if isinstance(data, str) else data
    return SSEEvent(data=raw, raw=raw, event=event)


def jev(obj: object, *, event: str | None = None) -> SSEEvent:
    return ev(json.dumps(obj), event=event)


# ------------------------------------------------------------------ registry


def test_every_voice_surface_is_in_the_registry_under_a_closed_name():
    from llmgw import metrics

    for surface in VOICE_SURFACES:
        assert SURFACES[surface.name] is surface
        assert surface.name in metrics.SURFACES
    assert len({s.name for s in VOICE_SURFACES}) == len(VOICE_SURFACES)
    assert all(s in REGISTRY for s in VOICE_SURFACES)


def test_every_voice_route_maps_to_an_upstream_path_without_the_client_prefix():
    for route, surface in VOICE_ROUTES.items():
        chooser = getattr(surface, "upstream_path_for", None)
        upstream = chooser(route) if callable(chooser) else surface.upstream_path
        prefixes = ("/inworld", "/elevenlabs", "/assemblyai")
        assert not upstream.startswith(prefixes), (route, upstream)
        assert upstream.startswith("/")


def test_voice_surfaces_never_want_include_usage_injected():
    assert all(s.include_usage_injectable is False for s in VOICE_SURFACES)


def test_framers_follow_the_declared_framing():
    assert isinstance(AUDIO_SPEECH.framer(1024), RawFramer)
    assert isinstance(AUDIO_SPEECH_SSE.framer(1024), SSEFramer)
    assert isinstance(INWORLD_TTS.framer(1024), JSONLFramer)
    assert isinstance(ELEVENLABS_TTS.framer(1024), RawFramer)
    assert isinstance(ELEVENLABS_TTS_TIMESTAMPS.framer(1024), JSONLFramer)
    assert isinstance(AUDIO_TRANSCRIPTION.framer(1024), SSEFramer)


# --------------------------------------------------------------- audio_speech


def test_audio_speech_request_counts_characters_and_picks_framing():
    facts = AUDIO_SPEECH.parse_request(
        json.dumps({"model": "openai.gpt-4o-mini-tts", "input": "hello",
                    "voice": "cedar"}).encode()
    )
    assert isinstance(facts, VoiceRequestFacts)
    assert facts.model == "openai.gpt-4o-mini-tts"
    assert facts.stream is True and facts.characters == 5 and facts.framing == "raw"
    assert facts.include_usage is False
    sse = AUDIO_SPEECH.parse_request(
        json.dumps({"model": "m", "input": "x", "stream_format": "sse"}).encode()
    )
    assert sse.framing == "sse" and sse.include_usage is True
    assert AudioSpeechSurface.framing_for(b'{"stream_format": "sse"}') == "sse"
    assert AudioSpeechSurface.framing_for(b"{}") == "raw"
    assert AudioSpeechSurface.framing_for(b"not json") == "raw"


def test_audio_speech_requires_a_model():
    with pytest.raises(errors.InvalidRequest):
        AUDIO_SPEECH.parse_request(b'{"input": "hi"}')


def test_audio_speech_raw_frames_are_all_content_and_never_usage():
    usage = Usage()
    chunk = ev(bytes(range(256)) * 4)
    assert AUDIO_SPEECH.classify(chunk) is EventKind.CONTENT
    AUDIO_SPEECH.apply_usage(chunk, usage)
    assert usage == Usage()
    assert AUDIO_SPEECH.error_from_event(chunk) is None
    assert AUDIO_SPEECH.native_ending() == b""


def test_audio_speech_sse_events_classify_and_bill():
    s = AUDIO_SPEECH_SSE
    delta = jev({"type": "speech.audio.delta", "audio": base64.b64encode(b"x" * 64).decode()},
                event="speech.audio.delta")
    done = jev({"usage": {"input_tokens": 6, "output_tokens": 72, "total_tokens": 78}},
               event="speech.audio.done")
    assert s.classify(delta) is EventKind.CONTENT
    assert s.classify(done) is EventKind.META
    assert s.classify(ev("[DONE]")) is EventKind.TERMINAL
    keep = SSEEvent(data=b"", raw=b": keep\n", comment=b" keep")
    assert s.classify(keep) is EventKind.HEARTBEAT
    usage = Usage()
    s.apply_usage(delta, usage)
    assert usage.exact is False
    s.apply_usage(done, usage)
    assert (usage.input_tokens, usage.output_tokens, usage.audio_output_tokens) == (6, 72, 72)
    assert usage.exact is True


def test_audio_speech_sse_done_identified_by_payload_type_when_event_name_is_absent():
    s = AUDIO_SPEECH_SSE
    delta = jev({"type": "speech.audio.delta", "audio": "AA=="})
    assert s.classify(delta) is EventKind.CONTENT
    usage = Usage()
    s.apply_usage(jev({"type": "speech.audio.done",
                       "usage": {"input_tokens": 1, "output_tokens": 2}}), usage)
    assert usage.exact and usage.output_tokens == 2


def test_audio_speech_sse_in_band_error_maps_to_the_taxonomy():
    err = AUDIO_SPEECH_SSE.error_from_event(
        jev({"error": {"type": "server_error", "message": "boom"}}))
    assert isinstance(err, errors.InStreamError)
    over = AUDIO_SPEECH_SSE.error_from_event(
        jev({"error": {"type": "overloaded_error", "message": "busy"}}))
    assert isinstance(over, errors.UpstreamOverloaded)
    assert AUDIO_SPEECH_SSE.classify(jev({"error": {"message": "x"}})) is EventKind.ERROR


def test_audio_speech_estimate_is_request_characters_and_never_exact():
    body = json.dumps({"model": "m", "input": "abcdefgh"}).encode()
    facts = AUDIO_SPEECH.parse_request(body)
    est = AUDIO_SPEECH.usage_estimate(facts)
    assert est.characters == 8 and est.exact is False


# -------------------------------------------------------- audio_transcription


def _multipart(fields: list[tuple[str, bytes]], boundary: str = "gwB0undary") -> bytes:
    out = []
    for name, value in fields:
        head = f'Content-Disposition: form-data; name="{name}"'
        if name == "file":
            head += '; filename="clip.mp3"\r\nContent-Type: audio/mpeg'
        out.append(f"--{boundary}\r\n{head}\r\n\r\n".encode() + value + b"\r\n")
    return b"".join(out) + f"--{boundary}--\r\n".encode()


def test_transcription_scans_text_fields_and_skips_the_file():
    body = _multipart([("model", b"openai.gpt-transcribe"), ("stream", b"true"),
                       ("file", b"\x00" * 10_000)])
    ctype = "multipart/form-data; boundary=gwB0undary"
    facts = AUDIO_TRANSCRIPTION.parse_request(body, content_type=ctype)
    assert facts.model == "openai.gpt-transcribe" and facts.stream is True
    assert scan_form_fields(body, ctype, frozenset({"file"})) == {}
    with pytest.raises(errors.InvalidRequest):
        AUDIO_TRANSCRIPTION.parse_request(_multipart([("file", b"x")]), content_type=ctype)
    with pytest.raises(errors.InvalidRequest):
        AUDIO_TRANSCRIPTION.parse_request(body, content_type="application/json")


def test_transcription_routes_both_map_upstream_unchanged():
    for route in AUDIO_TRANSCRIPTION.routes:
        assert AUDIO_TRANSCRIPTION.upstream_path_for(route) == route
    with pytest.raises(ValueError):
        AUDIO_TRANSCRIPTION.upstream_path_for("/v1/audio/speech")


def test_transcription_sse_delta_is_content_and_text_done_is_duration_usage():
    s = AUDIO_TRANSCRIPTION
    delta = jev({"type": "transcript.text.delta", "delta": "hello "},
                event="transcript.text.delta")
    done = jev({"type": "transcript.text.done", "text": "hello world",
                "usage": {"type": "duration", "seconds": 3}}, event="transcript.text.done")
    assert s.classify(delta) is EventKind.CONTENT
    assert s.text_delta(delta) == "hello "
    assert s.classify(done) is EventKind.META
    usage = Usage()
    s.apply_usage(done, usage)
    assert usage.seconds == 3.0 and usage.exact is True


def test_transcription_token_usage_with_audio_detail():
    usage = Usage()
    AUDIO_TRANSCRIPTION.usage_from_body(
        {"text": "x", "usage": {"type": "tokens", "input_tokens": 100, "output_tokens": 20,
                                "input_token_details": {"audio_tokens": 90,
                                                        "text_tokens": 10}}},
        usage,
    )
    assert (usage.input_tokens, usage.output_tokens) == (100, 20)
    assert usage.audio_input_tokens == 90
    assert usage.exact


def test_transcription_whisper_duration_is_rounded_up_like_the_meter():
    usage = Usage()
    AUDIO_TRANSCRIPTION.usage_from_body({"text": "x", "duration": 2.44}, usage)
    assert usage.seconds == 3.0 and usage.exact
    usage2 = Usage()
    AUDIO_TRANSCRIPTION.usage_from_body({"text": "x"}, usage2)
    assert usage2.exact is False and usage2.seconds == 0.0


# ---------------------------------------------------------------- inworld_tts


def _inworld_line(audio: bytes, count: int | None, extra: dict | None = None) -> SSEEvent:
    result = {"audioContent": base64.b64encode(audio).decode(),
              "usage": None if count is None else {"processedCharactersCount": count}}
    if extra:
        result.update(extra)
    return jev({"result": result})


def test_inworld_request_reads_modelId_and_streams_on_both_routes():
    facts = INWORLD_TTS.parse_request(json.dumps(
        {"modelId": "inworld.tts-2-flash", "text": "nineteen characters",
         "audioConfig": {"audioEncoding": "LINEAR16"}}).encode())
    assert facts.model == "inworld.tts-2-flash" and facts.stream is True
    assert facts.characters == 19 and facts.framing == "jsonl"
    assert INWORLD_TTS.model_key == "modelId"
    assert INWORLD_TTS.parse_request(b'{"model_id": "m", "text": "a"}').model == "m"
    with pytest.raises(errors.InvalidRequest):
        INWORLD_TTS.parse_request(b'{"model": "openai-key-is-not-inworlds", "text": "a"}')


def test_inworld_upstream_paths_strip_the_client_prefix():
    assert INWORLD_TTS.upstream_path_for("/inworld/tts/v1/voice") == "/tts/v1/voice"
    stream_route = "/inworld/tts/v1/voice:stream"
    assert INWORLD_TTS.upstream_path_for(stream_route) == "/tts/v1/voice:stream"
    with pytest.raises(ValueError):
        INWORLD_TTS.upstream_path_for("/tts/v1/voice")


def test_inworld_first_line_carries_the_whole_bill_and_later_lines_zero():
    usage = Usage()
    first = _inworld_line(b"RIFF" + b"\0" * 40 + b"\1" * 100, 19)
    later = _inworld_line(b"\2" * 100, 0)
    assert INWORLD_TTS.classify(first) is EventKind.CONTENT
    INWORLD_TTS.apply_usage(first, usage)
    assert usage.characters == 19 and usage.exact
    INWORLD_TTS.apply_usage(later, usage)
    assert usage.characters == 19, "a later line's 0 must not erase the first line's count"
    # Order-insensitive: the max keeps the count if the first line is seen last.
    reverse = Usage()
    INWORLD_TTS.apply_usage(later, reverse)
    INWORLD_TTS.apply_usage(first, reverse)
    assert reverse.characters == 19


def test_inworld_empty_text_is_an_exact_zero_and_not_content():
    line = ev('{"audioContent":"","usage":null}')
    usage = Usage()
    assert INWORLD_TTS.classify(line) is EventKind.META
    INWORLD_TTS.apply_usage(line, usage)
    assert usage.characters == 0 and usage.exact is True


def test_inworld_timestamp_only_line_is_meta_and_sync_body_bills():
    ts_only = jev({"result": {"timestampInfo": {"wordAlignment": {"words": ["hi"]}}}})
    assert INWORLD_TTS.classify(ts_only) is EventKind.META
    usage = Usage()
    INWORLD_TTS.usage_from_body(
        {"audioContent": "AAAA",
         "usage": {"processedCharactersCount": 5, "modelId": "x"}}, usage)
    assert usage.characters == 5 and usage.exact


def test_inworld_in_band_error_and_grpc_exhausted():
    err = INWORLD_TTS.error_from_event(jev({"error": {"code": 13, "message": "internal"}}))
    assert isinstance(err, errors.InStreamError)
    over = INWORLD_TTS.error_from_event(jev({"error": {"code": 8, "message": "exhausted"}}))
    assert isinstance(over, errors.UpstreamOverloaded)
    assert INWORLD_TTS.classify(jev({"error": {"code": 13}})) is EventKind.ERROR


# ------------------------------------------------------------- elevenlabs_tts


def test_elevenlabs_request_reads_model_id_and_streams_on_the_stream_route():
    facts = ELEVENLABS_TTS.parse_request(json.dumps(
        {"text": "twelve chars", "model_id": "elevenlabs.flash-v2-5",
         "voice_settings": {"stability": 0.5}}).encode())
    assert facts.model == "elevenlabs.flash-v2-5" and facts.characters == 12
    assert facts.stream is True
    assert ELEVENLABS_TTS.forward_query is True and ELEVENLABS_TTS.model_key == "model_id"
    buffered = ElevenLabsTTSSurface("buffered")
    assert buffered.parse_request(b'{"text": "a", "model_id": "m"}').stream is False


def test_elevenlabs_upstream_paths_keep_the_voice_template_or_pin_it():
    assert ELEVENLABS_TTS.upstream_path_for(
        "/elevenlabs/v1/text-to-speech/{voice_id}/stream",
    ) == "/v1/text-to-speech/{voice_id}/stream"
    assert ELEVENLABS_TTS.upstream_path_for(
        "/elevenlabs/v1/text-to-speech/{voice_id}") == "/v1/text-to-speech/{voice_id}"
    pinned = ElevenLabsTTSSurface("stream", voice_id="voice1")
    assert pinned.upstream_path == "/v1/text-to-speech/voice1/stream"
    assert pinned.upstream_path_for(
        "/elevenlabs/v1/text-to-speech/{voice_id}") == "/v1/text-to-speech/voice1"
    with pytest.raises(ValueError):
        ELEVENLABS_TTS.upstream_path_for("/v1/text-to-speech/x")


def test_elevenlabs_character_cost_header_is_the_bill():
    usage = Usage()
    ELEVENLABS_TTS.usage_from_headers({"Character-Cost": "12", "request-id": "abc"}, usage)
    assert usage.characters == 12 and usage.exact
    junk = Usage()
    ELEVENLABS_TTS.usage_from_headers({"character-cost": "twelve"}, junk)
    assert junk.exact is False and junk.parse_failures == 1
    none = Usage()
    ELEVENLABS_TTS.usage_from_headers({"content-type": "audio/mpeg"}, none)
    assert none == Usage()


def test_elevenlabs_raw_frames_are_content_and_timestamps_lines_are_read():
    assert ELEVENLABS_TTS.classify(ev(b"\xff\xfb\x90" * 100)) is EventKind.CONTENT
    assert ELEVENLABS_TTS.error_from_event(ev(b"\xff\xfb")) is None
    ts = ELEVENLABS_TTS_TIMESTAMPS
    assert ts.name == "elevenlabs_tts_timestamps" and ts.framing == "jsonl"
    audio = jev({"audio_base64": "AAAA", "alignment": {}})
    assert ts.classify(audio) is EventKind.CONTENT
    assert ts.classify(jev({"alignment": {}})) is EventKind.META
    err = ts.error_from_event(jev({"detail": {"status": "system_busy", "message": "retry"}}))
    assert isinstance(err, errors.UpstreamOverloaded)
    err2 = ts.error_from_event(
        jev({"detail": {"status": "voice_not_found", "message": "no"}}))
    assert isinstance(err2, errors.InStreamError)


# ------------------------------------------------------------ assemblyai_sync


def test_assemblyai_sync_bills_audio_duration_and_takes_no_json_body():
    usage = Usage()
    ASSEMBLYAI_SYNC.usage_from_body(
        {"text": "hi", "audio_duration_ms": 2500, "request_time_ms": 134}, usage)
    assert usage.seconds == 2.5 and usage.exact
    # Multipart with an `audio` part at `/v1/transcribe`, and the model in
    # `X-AAI-Model`: the three things whose absence made every call a 404.
    assert ASSEMBLYAI_SYNC.body == "multipart"
    assert ASSEMBLYAI_SYNC.upstream_path == "/v1/transcribe"
    assert ASSEMBLYAI_SYNC.model_header == "X-AAI-Model"
    assert ASSEMBLYAI_SYNC.model_key is None, "the model is not in the body at all"
    for route in ("/assemblyai/v1/transcribe", "/assemblyai/transcribe"):
        assert ASSEMBLYAI_SYNC.upstream_path_for(route) == "/v1/transcribe"
    with pytest.raises(errors.InvalidRequest):
        ASSEMBLYAI_SYNC.parse_request(b"\x00" * 10)
    assert ASSEMBLYAI_SYNC.classify(ev("{}")) is EventKind.META
    bad = Usage()
    ASSEMBLYAI_SYNC.usage_from_body({"audio_duration_ms": -5}, bad)
    assert bad.exact is False


# ----------------------------------------------------------- elevenlabs_stt


def test_elevenlabs_stt_bills_the_duration_the_provider_reported():
    """`audio_duration_secs` is exact and NOT rounded: 1.84 for a file
    AssemblyAI independently measured at 1840 ms. OpenAI's duration usage is
    rounded up before the provider reports it and `ceil_seconds` honours
    that; doing the same here would invent 160 ms of audio on every call."""
    usage = Usage()
    ELEVENLABS_STT.usage_from_body(
        {"text": "hi", "audio_duration_secs": 1.84, "transcription_id": "x"}, usage)
    assert usage.seconds == 1.84 and usage.exact


def test_elevenlabs_stt_is_multipart_with_the_model_in_a_form_field():
    assert ELEVENLABS_STT.body == "multipart" and ELEVENLABS_STT.framing == "raw"
    assert ELEVENLABS_STT.model_key == "model_id"
    assert ELEVENLABS_STT.model_header is None, "this one really is in the body"
    assert ELEVENLABS_STT.upstream_path_for("/elevenlabs/v1/speech-to-text") == (
        "/v1/speech-to-text")
    with pytest.raises(ValueError):
        ELEVENLABS_STT.upstream_path_for("/elevenlabs/v1/text-to-speech/v/stream")
    with pytest.raises(errors.InvalidRequest):
        ELEVENLABS_STT.parse_request(b"--boundary\r\n")


def test_elevenlabs_stt_never_raises_on_a_body_it_cannot_read():
    for payload in ({}, {"audio_duration_secs": None}, {"audio_duration_secs": "1.8"},
                    {"audio_duration_secs": True}, {"audio_duration_secs": -3}):
        usage = Usage()
        ELEVENLABS_STT.usage_from_body(payload, usage)
        assert usage == Usage(), payload


# -------------------------------------------------------------- inworld_stt


def test_inworld_stt_reads_the_nested_model_and_the_millisecond_meter():
    facts = INWORLD_STT.parse_request(json.dumps({
        "transcribeConfig": {"modelId": "inworld.stt-1", "audioEncoding": "LINEAR16"},
        "audioData": {"content": "AAAA"}}).encode())
    assert facts.model == "inworld.stt-1" and facts.stream is False
    # The snake_case spelling protojson also accepts.
    facts2 = INWORLD_STT.parse_request(json.dumps({
        "transcribe_config": {"model_id": "inworld.stt-1"}}).encode())
    assert facts2.model == "inworld.stt-1"
    usage = Usage()
    INWORLD_STT.usage_from_body(
        {"transcription": {"transcript": "hi"},
         "usage": {"transcribedAudioMs": 1840, "modelId": "inworld/inworld-stt-1"}}, usage)
    assert usage.seconds == 1.84 and usage.exact


def test_inworld_stt_refuses_a_body_that_names_no_model():
    for body in (b"{}", b'{"audioData":{"content":"AAAA"}}',
                 b'{"transcribeConfig":{"modelId":"  "}}', b'{"transcribeConfig":[]}'):
        with pytest.raises(errors.InvalidRequest):
            INWORLD_STT.parse_request(body)


def test_inworld_stt_classifies_its_grpc_error_codes():
    busy = INWORLD_STT.error_from_event(jev({"code": 8, "message": "slow down"}))
    assert isinstance(busy, errors.UpstreamOverloaded)
    other = INWORLD_STT.error_from_event(jev({"code": 3, "message": "audio_data is required"}))
    assert isinstance(other, errors.InStreamError)
    assert INWORLD_STT.error_from_event(ev(b"\x00\x01")) is None


def test_inworld_stt_accounting_never_raises():
    for payload in ({}, {"usage": None}, {"usage": {"transcribedAudioMs": "1840"}},
                    {"usage": []}):
        usage = Usage()
        INWORLD_STT.usage_from_body(payload, usage)
        assert usage == Usage(), payload

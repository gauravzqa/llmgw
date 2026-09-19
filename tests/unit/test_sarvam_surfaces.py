"""Sarvam's four HTTP speech surfaces: routing, the model rewrite, and the
bill of a provider that reports no meter at all.

The shapes are the ones captured live on 18-19 Sep 2026
(`capabilities/sarvam.md`, `capabilities/captures-sarvam-assemblyai.md` §2).
What is under test here is mostly the accounting, because Sarvam is the
provider where the accounting is hardest to get honest: it bills per
character and per audio second, and it reports neither. Every assertion
about `basis` and `cost_notes` below is asserting that the gateway says so
out loud instead of emitting a confident zero.
"""

from __future__ import annotations

import base64
import json
import struct

import pytest

from llmgw import errors
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.framing import RawFramer
from llmgw.sse import SSEEvent
from llmgw.surfaces import REGISTRY, SURFACES
from llmgw.surfaces.base import EventKind, RequestFacts, Usage
from llmgw.surfaces.voice import (
    SARVAM_STT,
    SARVAM_STT_TRANSLATE,
    SARVAM_TTS,
    SARVAM_TTS_STREAM,
)
from llmgw.surfaces.voice.sarvam import (
    STT_HEADER_NOTE,
    STT_NO_METER_NOTE,
    TTS_NO_METER_NOTE,
    wav_seconds,
)
from llmgw.upstream import apply_api_model, apply_api_model_multipart

SARVAM_SURFACES = (SARVAM_TTS, SARVAM_TTS_STREAM, SARVAM_STT, SARVAM_STT_TRANSLATE)


def wav(seconds: float, *, rate: int = 16_000) -> bytes:
    data = int(seconds * rate * 2)
    return b"".join([
        b"RIFF", struct.pack("<I", 36 + data), b"WAVE",
        b"fmt ", struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16),
        b"data", struct.pack("<I", data),
    ]) + b"\x00" * data


def multipart(*, model: str | None, audio: bytes, boundary: str = "BNDRY") -> bytes:
    parts = []
    if model is not None:
        parts.append(
            f'--{boundary}\r\nContent-Disposition: form-data; name="model"\r\n\r\n'
            f"{model}\r\n".encode()
        )
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; '
        f'filename="a.wav"\r\nContent-Type: audio/wav\r\n\r\n'.encode()
        + audio + b"\r\n"
    )
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts)


CONTENT_TYPE = "multipart/form-data; boundary=BNDRY"


# ------------------------------------------------------------------ registry


def test_all_four_sarvam_surfaces_are_registered_under_closed_metric_names():
    from llmgw import metrics

    for surface in SARVAM_SURFACES:
        assert SURFACES[surface.name] is surface
        assert surface.name in metrics.SURFACES
        assert surface in REGISTRY
    assert {s.name for s in SARVAM_SURFACES} == {
        "sarvam_tts", "sarvam_tts_stream", "sarvam_stt", "sarvam_stt_translate",
    }


@pytest.mark.parametrize(
    ("surface", "route", "upstream", "body_kind", "framing"),
    [
        (SARVAM_TTS, "/sarvam/text-to-speech", "/text-to-speech", "json", "raw"),
        (SARVAM_TTS_STREAM, "/sarvam/text-to-speech/stream",
         "/text-to-speech/stream", "json", "raw"),
        (SARVAM_STT, "/sarvam/speech-to-text", "/speech-to-text", "multipart", "raw"),
        (SARVAM_STT_TRANSLATE, "/sarvam/speech-to-text-translate",
         "/speech-to-text-translate", "multipart", "raw"),
    ],
)
def test_each_surface_declares_its_route_upstream_path_body_and_framing(
    surface, route, upstream, body_kind, framing
):
    assert surface.routes == (route,)
    assert surface.upstream_path == upstream
    assert surface.body == body_kind
    assert surface.framing == framing
    assert surface.forward_query is False
    assert surface.model_key == "model"
    assert isinstance(surface.framer(1024), RawFramer)


def test_the_catalog_has_a_sarvam_row_for_every_model_the_surfaces_name():
    for model_id, unit in (("sarvam.bulbul-v3", "characters"),
                           ("sarvam.bulbul-v4-flash", "characters"),
                           ("sarvam.saaras-v3", "seconds"),
                           ("sarvam.saaras-v4", "seconds"),
                           ("sarvam.sarvam-105b", "tokens")):
        spec = DEFAULT_CATALOG.models[model_id]
        assert spec.provider == "sarvam"
        assert spec.unit == unit
    provider = DEFAULT_CATALOG.providers["sarvam"]
    assert provider.base_url == "https://api.sarvam.ai"
    assert provider.auth_scheme == "bearer"
    assert provider.api_key_env == "SARVAM_API_KEY"
    assert provider.forbidden_means == "auth"
    assert provider.scrub_error_bodies == "auth"


def test_bulbul_v2_is_deliberately_absent_because_it_is_deprecated_and_400s():
    assert "sarvam.bulbul-v2" not in DEFAULT_CATALOG.models
    assert all(spec.api_model != "bulbul:v2" for spec in DEFAULT_CATALOG.models.values())


def test_the_speech_rows_share_one_service_rate_because_sarvam_publishes_one():
    """Not a copy-paste slip: Sarvam's price list is per service, not per
    model. A future editor who "fixes" one of these in isolation is wrong
    until Sarvam splits the table."""
    models = DEFAULT_CATALOG.models
    assert (models["sarvam.bulbul-v3"].input_per_m
            == models["sarvam.bulbul-v4-flash"].input_per_m == 33.90)
    assert (models["sarvam.saaras-v3"].per_minute
            == models["sarvam.saaras-v4"].per_minute == 0.00565)


# ------------------------------------------------------------- TTS requests


def test_tts_parse_request_counts_the_request_text_and_stays_buffered():
    body = json.dumps({"model": "sarvam.bulbul-v3", "text": "nineteen characters",
                       "target_language_code": "en-IN"}).encode()
    facts = SARVAM_TTS.parse_request(body)
    assert facts.model == "sarvam.bulbul-v3"
    assert facts.stream is False
    assert facts.characters == len("nineteen characters")
    assert facts.include_usage is False


def test_the_stream_route_is_the_same_dialect_with_stream_true():
    body = json.dumps({"model": "sarvam.bulbul-v3", "text": "hello"}).encode()
    facts = SARVAM_TTS_STREAM.parse_request(body)
    assert facts.stream is True
    assert facts.characters == 5


def test_tts_counts_the_batch_inputs_form_when_there_is_no_text():
    body = json.dumps({"model": "sarvam.bulbul-v3", "inputs": ["abc", "de"]}).encode()
    assert SARVAM_TTS.parse_request(body).characters == 5


def test_tts_without_a_model_is_an_invalid_request_and_nothing_else():
    with pytest.raises(errors.InvalidRequest):
        SARVAM_TTS.parse_request(json.dumps({"text": "hi"}).encode())


def test_tts_with_an_unparseable_body_raises_only_invalid_request():
    for raw in (b"", b"not json", b"[]", b"null", b'{"model":'):
        with pytest.raises(errors.InvalidRequest):
            SARVAM_TTS.parse_request(raw)


def test_empty_text_is_forwarded_rather_than_second_guessed():
    """Sarvam answers `'text' cannot be empty` with a 400 -- unlike Inworld,
    which answers 200 with a null usage. The surface must not invent either
    behaviour; it parses, counts zero, and lets the provider decide."""
    facts = SARVAM_TTS.parse_request(
        json.dumps({"model": "sarvam.bulbul-v3", "text": ""}).encode())
    assert facts.characters == 0


def test_the_model_rewrite_targets_the_top_level_model_key():
    body = json.dumps({"model": "sarvam.bulbul-v3", "text": "hi"}).encode()
    out, changed = apply_api_model(body, "bulbul:v3", key=SARVAM_TTS.model_key)
    assert changed is True
    assert json.loads(out)["model"] == "bulbul:v3"
    assert json.loads(out)["text"] == "hi"
    # A body already naming the wire id is returned as the SAME object.
    again, changed = apply_api_model(out, "bulbul:v3", key="model")
    assert changed is False and again is out


def test_both_language_spellings_survive_the_rewrite_untouched():
    """`target_language_code` and `language_code` are both read by Sarvam and
    produce different takes (captures §2.4 note 5). A surface that
    normalised one into the other would silently change the audio."""
    body = json.dumps({"model": "sarvam.bulbul-v3", "text": "hi",
                       "target_language_code": "en-IN",
                       "language_code": "hi-IN"}).encode()
    out, _ = apply_api_model(body, "bulbul:v3")
    parsed = json.loads(out)
    assert parsed["target_language_code"] == "en-IN"
    assert parsed["language_code"] == "hi-IN"


# ------------------------------------------------------------- STT requests


def test_stt_parse_request_refuses_because_the_body_is_multipart():
    with pytest.raises(errors.InvalidRequest):
        SARVAM_STT.parse_request(b"anything")


def test_the_multipart_splice_rewrites_only_the_model_field_value():
    body = multipart(model="sarvam.saaras-v4", audio=wav(1.0))
    out, changed = apply_api_model_multipart(body, "saaras:v4", key=SARVAM_STT.model_key)
    assert changed is True
    assert b'name="model"\r\n\r\nsaaras:v4\r\n' in out
    # The file part is untouched, byte for byte, including its RIFF header.
    assert out.split(b"\r\n\r\n", 2)[2].startswith(b"RIFF") or b"RIFF" in out
    assert len(out) == len(body) - len("sarvam.saaras-v4") + len("saaras:v4")


def test_facts_from_body_reads_the_seconds_out_of_the_uploaded_wav_header():
    facts = RequestFacts(model="sarvam.saaras-v3", stream=False)
    body = multipart(model="sarvam.saaras-v3", audio=wav(2.5))
    refined = SARVAM_STT.facts_from_body(facts, body, CONTENT_TYPE)
    assert refined.model == "sarvam.saaras-v3"
    assert refined.seconds == pytest.approx(2.5, abs=0.01)


def test_facts_from_body_leaves_a_compressed_upload_alone():
    """No RIFF header, no estimate. A guess at an MP3's duration would be a
    number nobody could defend on an invoice."""
    facts = RequestFacts(model="sarvam.saaras-v3", stream=False)
    body = multipart(model="sarvam.saaras-v3", audio=b"\xff\xfb\x90\x00" * 512)
    assert SARVAM_STT.facts_from_body(facts, body, CONTENT_TYPE) is facts


# -------------------------------------------------------- hostile WAV bytes


@pytest.mark.parametrize("body", [
    b"",
    b"RIFF",
    b"RIFF\x00\x00\x00\x00",
    b"RIFF" + b"\xff" * 8,
    b"RIFF" + struct.pack("<I", 0) + b"WAVE",
    # `fmt ` claiming a zero byte rate: a division nobody may perform.
    b"RIFF" + struct.pack("<I", 36) + b"WAVE" + b"fmt "
    + struct.pack("<IHHIIHH", 16, 1, 1, 0, 0, 2, 16) + b"data" + struct.pack("<I", 100),
    # A data chunk claiming 4 GB inside 20 bytes.
    b"RIFF" + struct.pack("<I", 36) + b"WAVE" + b"fmt "
    + struct.pack("<IHHIIHH", 16, 1, 1, 16000, 32000, 2, 16)
    + b"data" + struct.pack("<I", 0xFFFFFFFF),
    # A thousand zero-length chunks: the walk must be bounded, not spin.
    b"RIFF" + struct.pack("<I", 36) + b"WAVE" + (b"junk" + struct.pack("<I", 0)) * 1000,
    b"\x00" * 4096,
])
def test_the_wav_reader_never_raises_and_never_overclaims(body):
    seconds = wav_seconds(body)
    assert isinstance(seconds, float)
    assert 0.0 <= seconds <= len(body)


def test_a_declared_data_size_is_clamped_to_the_bytes_actually_uploaded():
    """A header may lie. Billing what arrived is the only defensible read."""
    head = (b"RIFF" + struct.pack("<I", 36) + b"WAVE" + b"fmt "
            + struct.pack("<IHHIIHH", 16, 1, 1, 16000, 32000, 2, 16)
            + b"data" + struct.pack("<I", 32_000 * 3600))
    assert wav_seconds(head + b"\x00" * 32_000) == pytest.approx(1.0)


def test_a_real_wav_reads_back_its_own_duration():
    assert wav_seconds(wav(1.28)) == pytest.approx(1.28, abs=0.01)


def test_the_reader_does_not_scan_past_its_window():
    body = b"\x00" * 70_000 + wav(1.0)
    assert wav_seconds(body) == 0.0


# --------------------------------------------------------------- accounting


def test_tts_reports_no_usage_from_its_body_and_says_why():
    """The whole response is `{"request_id", "audios"}`. Nothing in it is a
    meter, so nothing may set an exactness flag."""
    usage = Usage()
    payload = {"request_id": "20260919_x", "audios": [base64.b64encode(b"RIFF").decode()]}
    SARVAM_TTS.usage_from_body(payload, usage)
    assert usage.characters == 0
    assert usage.input_exact is False and usage.output_exact is False
    assert usage.exact is False
    assert SARVAM_TTS.cost_notes(None, usage) == (TTS_NO_METER_NOTE,)


def test_tts_usage_estimate_is_the_request_characters_and_never_exact():
    facts = SARVAM_TTS.parse_request(
        json.dumps({"model": "sarvam.bulbul-v3", "text": "hello there"}).encode())
    est = SARVAM_TTS.usage_estimate(facts)
    assert est.characters == 11
    assert est.exact is False


def test_stt_usage_estimate_is_the_wav_duration_and_never_exact():
    facts = SARVAM_STT.facts_from_body(
        RequestFacts(model="sarvam.saaras-v3", stream=False),
        multipart(model="sarvam.saaras-v3", audio=wav(3.0)), CONTENT_TYPE)
    est = SARVAM_STT.usage_estimate(facts)
    assert est.seconds == pytest.approx(3.0, abs=0.01)
    assert est.exact is False


def test_stt_reads_no_meter_off_a_real_response_body():
    usage = Usage()
    payload = {"request_id": "20260919_x", "transcript": "hello",
               "language_code": "en-IN", "language_probability": 1.0}
    SARVAM_STT.usage_from_body(payload, usage)
    assert usage.seconds == 0.0 and usage.exact is False
    assert usage.parse_failures == 0


def test_the_stt_cost_note_names_which_estimate_was_used():
    assert SARVAM_STT.cost_notes(None, Usage(seconds=2.0)) == (STT_HEADER_NOTE,)
    assert SARVAM_STT.cost_notes(None, Usage()) == (STT_NO_METER_NOTE,)
    assert SARVAM_STT_TRANSLATE.cost_notes(None, Usage(seconds=1.0)) == (STT_HEADER_NOTE,)


@pytest.mark.parametrize("surface", SARVAM_SURFACES)
@pytest.mark.parametrize("payload", [
    {}, {"usage": "nonsense"}, {"audios": None}, {"transcript": 5},
    {"usage": {"characters": "many"}}, {"audios": [{"nested": True}]},
])
def test_no_hostile_body_can_make_a_usage_reader_raise(surface, payload):
    usage = Usage()
    surface.usage_from_body(payload, usage)
    assert usage.parse_failures == 0


@pytest.mark.parametrize("surface", SARVAM_SURFACES)
def test_a_raw_audio_frame_classifies_without_a_json_parse(surface):
    ev = SSEEvent(data=b"\x00\x01\x02\x03", raw=b"\x00\x01\x02\x03")
    assert surface.classify(ev) in (EventKind.CONTENT, EventKind.META)
    assert surface.error_from_event(ev) is None
    assert surface.native_ending(ev) == b""


def test_the_stream_never_fabricates_a_terminal_frame():
    """Sarvam's chunked TTS body has no terminator at all -- it ends when the
    socket does -- so there is nothing for the gateway to withhold and
    nothing for it to invent."""
    assert SARVAM_TTS_STREAM.native_ending() == b""
    assert SARVAM_TTS_STREAM.native_ending(SSEEvent(data=b"x", raw=b"x")) == b""


def test_the_translate_route_reads_a_diarized_transcript_without_choking():
    usage = Usage()
    SARVAM_STT_TRANSLATE.usage_from_body(
        {"request_id": "x", "transcript": "hi", "language_code": "en-IN",
         "diarized_transcript": None, "language_probability": 0.947}, usage)
    assert usage.parse_failures == 0

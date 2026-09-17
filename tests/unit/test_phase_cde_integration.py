"""Phase C/D/E integration seams (18 Sep 2026): the model key per dialect,
the include_usage gate, the per-request framing pick, the request-side
estimate for an unmetered stream, and audio tokens priced as a subset."""

from __future__ import annotations

import json

from llmgw import accounting
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.surfaces.base import Usage
from llmgw.surfaces.voice import AUDIO_SPEECH, ELEVENLABS_TTS, INWORLD_TTS, VOICE_SURFACES
from llmgw.upstream import UpstreamRequest, apply_api_model


def test_apply_api_model_rewrites_the_dialects_own_key_only():
    body = json.dumps({"modelId": "inworld.tts-2-flash", "text": "hi"}).encode()
    out, renamed = apply_api_model(body, "inworld-tts-2-flash", key="modelId")
    assert renamed is True
    parsed = json.loads(out)
    assert parsed["modelId"] == "inworld-tts-2-flash"
    assert "model" not in parsed, "no stray `model` key beside the dialect's own"


def test_apply_api_model_short_circuits_on_the_dialects_key():
    body = b'{"modelId":"inworld-tts-2-flash","text":"hi"}'
    out, renamed = apply_api_model(body, "inworld-tts-2-flash", key="modelId")
    assert renamed is False and out is body


def test_voice_surfaces_declare_their_model_key_and_refuse_include_usage():
    assert INWORLD_TTS.model_key == "modelId"
    assert ELEVENLABS_TTS.model_key == "model_id"
    for surface in VOICE_SURFACES:
        assert surface.include_usage_injectable is False, surface.name
    assert UpstreamRequest.__dataclass_fields__["model_key"].default == "model"
    assert UpstreamRequest.__dataclass_fields__["include_usage_injectable"].default is True


def test_audio_speech_picks_the_sse_instance_for_stream_format_sse():
    sse = AUDIO_SPEECH.surface_for(b'{"model":"m","input":"x","stream_format":"sse"}')
    assert sse is not None and sse.framing == "sse" and sse.name == "audio_speech"
    assert AUDIO_SPEECH.surface_for(b'{"model":"m","input":"x"}') is None
    assert AUDIO_SPEECH.surface_for(b"not json") is None
    # One shared instance per framing: stateless, so sharing is safe.
    assert AUDIO_SPEECH.surface_for(b'{"stream_format":"sse"}') is sse


def test_binary_tts_estimate_is_calibrated_and_never_exact():
    facts = AUDIO_SPEECH.parse_request(
        json.dumps({"model": "openai.gpt-4o-mini-tts", "input": "x" * 20}).encode()
    )
    est = AUDIO_SPEECH.usage_estimate(facts)
    assert est.characters == 20
    assert est.input_tokens == 6            # 20 // 4 + 1, the live probe's number
    assert est.audio_output_tokens == 72    # round(20 * 3.6), the live probe's number
    assert est.output_tokens == 72
    assert est.exact is False


def test_estimate_unmetered_fills_only_a_zero_usage():
    from llmgw.server.app import _estimate_unmetered

    class _Pump:
        def __init__(self) -> None:
            self.usage = Usage()

    class _Result:
        def __init__(self) -> None:
            self.pump = _Pump()

    facts = AUDIO_SPEECH.parse_request(b'{"model":"m","input":"hello world"}')
    r = _Result()
    _estimate_unmetered(AUDIO_SPEECH, r, facts)
    assert r.pump.usage.characters == 11 and r.pump.usage.audio_output_tokens == 40
    # A provider's own count always wins: nothing is overwritten.
    metered = _Result()
    metered.pump.usage.output_tokens = 5
    _estimate_unmetered(AUDIO_SPEECH, metered, facts)
    assert metered.pump.usage.characters == 0 and metered.pump.usage.output_tokens == 5
    # A surface without the hook (chat) is left alone.
    from llmgw.surfaces import OPENAI_CHAT
    plain = _Result()
    _estimate_unmetered(OPENAI_CHAT, plain, facts)
    assert plain.pump.usage.characters == 0


def test_audio_output_tokens_are_a_subset_not_a_second_bill():
    spec = DEFAULT_CATALOG.models["openai.gpt-4o-mini-tts"]
    usage = Usage()
    usage.input_tokens = 6
    usage.output_tokens = 72
    usage.audio_output_tokens = 72
    usage.input_exact = usage.output_exact = True
    usd, _notes = accounting._cost_usd(
        usage, spec, 72, extra={"audio_output_tokens": 72},
    )
    expected = (6 * 0.60 + 72 * 12.0) / 1_000_000
    assert abs(usd - expected) < 1e-12, "72 audio tokens billed once, at the audio rate"

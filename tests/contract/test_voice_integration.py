# ruff: noqa: F811
"""Phase D integration through real sockets (18 Sep 2026): the seams the
surface agent could not cross alone -- the dialect's model key on the
streaming path, the header-borne meter, the request-side estimate for a
stream nobody metered, and the per-request framing pick."""

from __future__ import annotations

import json

import pytest

from tests.contract.test_voice_surfaces import (  # noqa: F401 - fixtures
    _metric,
    _metrics,
    assemblyai_fake,
    audio_fake,
    client,
    gateway,
)

pytestmark = pytest.mark.contract


async def test_inworld_stream_is_sent_the_wire_model_under_model_id(gateway, client):
    """`modelId`, not `model`: the fake echoes the key it received."""
    body = {"modelId": "inworld.tts-2-flash", "text": "hello inworld", "voiceId": "Aarav"}
    lines: list[dict] = []
    async with client.stream(
        "POST", f"{gateway.base_url}/inworld/tts/v1/voice:stream", json=body,
        headers={"x-fake-mode": "inworld-ndjson"},
    ) as r:
        assert r.status_code == 200
        assert r.headers.get("x-gw-body-modified") == "1"
        async for raw in r.aiter_lines():
            if raw.strip():
                lines.append(json.loads(raw))
    assert lines, "no NDJSON lines came back"
    echoed = lines[0]["result"]["usage"]["modelId"]
    assert echoed == "inworld-tts-2-flash", f"fake saw modelId={echoed!r}"


async def test_elevenlabs_character_cost_header_is_the_exact_bill(gateway, client):
    text = "twelve chars"
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     unit="characters", model="elevenlabs.flash-v2-5")
    body = {"text": text, "model_id": "elevenlabs.flash-v2-5"}
    async with client.stream(
        "POST", f"{gateway.base_url}/elevenlabs/v1/text-to-speech/voice1/stream", json=body,
        headers={"x-fake-mode": "elevenlabs-raw"},
    ) as r:
        assert r.status_code == 200
        async for _ in r.aiter_bytes():
            pass
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    unit="characters", model="elevenlabs.flash-v2-5")
    assert after - before == len(text), "the header's count, read before the body"
    exact = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                    basis="exact", model="elevenlabs.flash-v2-5")
    assert exact > 0


async def test_binary_tts_is_billed_estimated_from_the_request(gateway, client):
    text = "x" * 20
    metrics_before = await _metrics(client, gateway)
    est_before = _metric(metrics_before, "llmgw_cost_usd_total",
                         basis="estimated", model="openai.gpt-4o-mini-tts")
    body = {"model": "openai.gpt-4o-mini-tts", "input": text, "voice": "cedar"}
    async with client.stream(
        "POST", f"{gateway.base_url}/v1/audio/speech", json=body,
        headers={"x-fake-mode": "openai-tts-raw"},
    ) as r:
        assert r.status_code == 200
        assert r.headers.get("content-type", "").startswith("audio/")
        async for _ in r.aiter_bytes():
            pass
    metrics_after = await _metrics(client, gateway)
    est_after = _metric(metrics_after, "llmgw_cost_usd_total",
                        basis="estimated", model="openai.gpt-4o-mini-tts")
    # 6 text tokens at $0.60/M + 72 audio tokens at $12/M, once (subset rule).
    expected = (6 * 0.60 + 72 * 12.0) / 1_000_000
    assert abs((est_after - est_before) - expected) < 1e-9
    audio = _metric(metrics_after, "llmgw_tokens_total",
                    kind="audio_output", model="openai.gpt-4o-mini-tts")
    assert audio >= 72


async def test_sse_tts_is_served_by_the_sse_instance_and_billed_exactly(gateway, client):
    before = _metric(await _metrics(client, gateway), "llmgw_tokens_total",
                     kind="output", model="openai.gpt-4o-mini-tts")
    exact_before = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                           basis="exact", model="openai.gpt-4o-mini-tts")
    body = {"model": "openai.gpt-4o-mini-tts", "input": "hello", "stream_format": "sse"}
    frames = 0
    done = None
    async with client.stream(
        "POST", f"{gateway.base_url}/v1/audio/speech", json=body,
        headers={"x-fake-mode": "openai-tts-sse"},
    ) as r:
        assert r.status_code == 200
        assert r.headers.get("content-type", "").startswith("text/event-stream")
        async for line in r.aiter_lines():
            if line.startswith("data:"):
                frames += 1
                if '"usage"' in line:
                    done = json.loads(line[5:].strip())
    assert frames > 1 and done is not None, "the SSE instance forwarded the event stream"
    after = _metric(await _metrics(client, gateway), "llmgw_tokens_total",
                    kind="output", model="openai.gpt-4o-mini-tts")
    assert after - before == done["usage"]["output_tokens"]
    exact_after = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                          basis="exact", model="openai.gpt-4o-mini-tts")
    expected = (done["usage"]["input_tokens"] * 0.60
                + done["usage"]["output_tokens"] * 12.0) / 1_000_000
    assert abs((exact_after - exact_before) - expected) < 1e-9, "billed once at the audio rate"

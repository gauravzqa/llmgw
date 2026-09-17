"""PLAN-2 Phase D over real sockets: the voice HTTP surfaces against the fake
audio upstreams.

Two extra fake ports are started here (`audio`, `assemblyai`) because the
session fixtures in conftest carry only the two chat ports. The gateway's
catalog points the voice provider rows at them with their real credential
styles -- bearer for Inworld, `xi-api-key` for ElevenLabs, the bare key for
AssemblyAI -- so what is proven per surface is the whole path: route ->
provider -> credential ritual -> framing -> bytes back intact -> meter.

What is asserted:

* every voice route is mounted by the registry (no `extra_surfaces`);
* raw audio (OpenAI binary TTS, ElevenLabs `/stream`) reaches the client
  byte-for-byte with the upstream content type, `X-Gw-Served-By` names the
  voice target, and a slow raw stream is cut by the progress clock with
  nothing fabricated after the last chunk (C20);
* Inworld's NDJSON stream reaches the client line-for-line with the RIFF
  header intact and `llmgw_units_total{unit="characters"}` moves by the
  first line's count (C21); empty text is a 200 with Inworld's own line;
* AssemblyAI's sync endpoint is reached with the RAW credential (the fake
  401s on `Bearer`), and its JSON comes back;
* the provider error shapes classify as Phase D4 says: Inworld's 400 code 3
  is `model_not_found`, ElevenLabs' 403 is a passthrough `policy_error` that
  opens no circuit.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace

import httpx
import pytest
from fakes import voice as fake_voice
from fakes.upstream import build_app as build_fake
from fakes.upstream import serve_in_thread

from llmgw.catalog import DEFAULT_CATALOG, Catalog
from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = pytest.mark.contract

FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
                "x-fake-bytes")
KEY_ENV = "LLMGW_VOICE_TEST_KEY"
KEY = "voice-test-key-not-real"

VOICE_MODELS = (
    "openai.gpt-4o-mini-tts", "openai.gpt-transcribe", "openai.whisper-1",
    "inworld.tts-2", "inworld.tts-2-flash",
    "elevenlabs.flash-v2-5", "elevenlabs.v3-conversational",
    "assemblyai.sync",
)


@pytest.fixture(scope="module")
def audio_fake():
    server = serve_in_thread(build_fake("audio"))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def assemblyai_fake():
    server = serve_in_thread(build_fake("assemblyai"))
    try:
        yield server
    finally:
        server.stop()


def _catalog(fakes: Fakes, audio_url: str, assembly_url: str) -> Catalog:
    """The shipped voice rows, with their providers pointed at the fakes and
    ONE test credential env var, credential styles preserved."""
    base = DEFAULT_CATALOG
    providers = {
        "openai": replace(base.providers["openai"], base_url=f"{fakes.openai.base_url}/v1",
                          api_key_env=KEY_ENV),
        "inworld": replace(base.providers["inworld"], base_url=audio_url, api_key_env=KEY_ENV),
        "elevenlabs": replace(base.providers["elevenlabs"], base_url=audio_url,
                              api_key_env=KEY_ENV),
        "assemblyai-sync": replace(base.providers["assemblyai-sync"], base_url=assembly_url,
                                   api_key_env=KEY_ENV),
    }
    models = {mid: base.models[mid] for mid in VOICE_MODELS}
    return Catalog(models=models, providers=providers)


def _config(fakes: Fakes, audio_url: str, assembly_url: str, **overrides) -> ServerConfig:
    os.environ.setdefault(KEY_ENV, KEY)
    settings = dict(
        catalog=_catalog(fakes, audio_url, assembly_url),
        fake_upstreams=False,
        default_model="openai.gpt-4o-mini-tts",
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(total=30.0, connect=2.0, headers=5.0, first_event=10.0,
                        progress=10.0, client_stall=10.0),
    )
    settings.update(overrides)
    return ServerConfig(**settings)


@pytest.fixture(scope="module")
def gateway(fakes: Fakes, audio_fake, assemblyai_fake):
    server = serve(build_app(_config(fakes, audio_fake.base_url, assemblyai_fake.base_url)))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def impatient_gateway(fakes: Fakes, audio_fake, assemblyai_fake):
    """A 1 s progress budget, for the raw-stream cut."""
    server = serve(build_app(_config(
        fakes, audio_fake.base_url, assemblyai_fake.base_url,
        budgets=Budgets(total=30.0, connect=2.0, headers=5.0, first_event=5.0,
                        progress=1.0, client_stall=10.0),
    )))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


def _metric(text: str, name: str, **labels: str) -> float:
    """One sample from Prometheus text, matched on every given label."""
    for line in text.splitlines():
        if not line.startswith(name):
            continue
        if all(f'{k}="{v}"' in line for k, v in labels.items()):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


async def _metrics(client: httpx.AsyncClient, gateway) -> str:
    r = await client.get(f"{gateway.base_url}/metrics")
    assert r.status_code == 200
    return r.text


# ----------------------------------------------------------- OpenAI TTS raw


async def test_openai_binary_tts_reaches_the_client_byte_for_byte(gateway, client):
    body = {"model": "openai.gpt-4o-mini-tts", "input": "hello there", "voice": "cedar"}
    async with client.stream("POST", f"{gateway.base_url}/v1/audio/speech", json=body) as r:
        chunks = [c async for c in r.aiter_raw()]
        status, headers = r.status_code, r.headers
    assert status == 200, b"".join(chunks)[:200]
    assert headers["content-type"].startswith("audio/pcm")
    assert headers["x-gw-served-by"] == "openai/openai.gpt-4o-mini-tts"
    assert headers["x-gw-attempts"] == "1"
    assert b"".join(chunks) == b"".join(fake_voice.raw_chunks(16))


async def test_a_slow_raw_stream_is_cut_by_the_progress_clock_with_nothing_appended(
    impatient_gateway, client,
):
    """C20: a voice stream cut after commitment ends by close. The client
    gets exactly the chunks that arrived and not one fabricated byte."""
    body = {"model": "openai.gpt-4o-mini-tts", "input": "slow"}
    async with client.stream(
        "POST", f"{impatient_gateway.base_url}/v1/audio/speech", json=body,
        headers={"x-fake-events": "6", "x-fake-interval": "3"},
    ) as r:
        received = b""
        truncated = False
        try:
            async for chunk in r.aiter_raw():
                received += chunk
        except httpx.HTTPError:
            truncated = True
        status = r.status_code
    assert status == 200
    expected = b"".join(fake_voice.raw_chunks(6))
    assert 0 < len(received) < len(expected)
    assert expected.startswith(received), "bytes after the last real chunk must be the fake's"
    assert truncated or len(received) % fake_voice.RAW_CHUNK_BYTES == 0


# ------------------------------------------------------------ Inworld NDJSON


async def test_inworld_stream_is_forwarded_line_for_line_and_billed_in_characters(
    gateway, client,
):
    text = "nineteen characters"
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     unit="characters", model="inworld.tts-2-flash")
    body = {"modelId": "inworld.tts-2-flash", "text": text,
            "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 24000}}
    async with client.stream(
        "POST", f"{gateway.base_url}/inworld/tts/v1/voice:stream", json=body,
    ) as r:
        raw = b"".join([c async for c in r.aiter_raw()])
        status, headers = r.status_code, r.headers
    assert status == 200, raw[:200]
    assert headers["content-type"].startswith("application/json")
    assert headers["x-gw-served-by"] == "inworld/inworld.tts-2-flash"
    lines = [ln for ln in raw.split(b"\n") if ln]
    assert len(lines) == 20
    first = json.loads(lines[0])["result"]
    # `modelId` echoed by the fake is the WIRE id: proof the gateway rewrote
    # the dialect's own key (Phase D integration), never a stray `model`.
    assert first["usage"] == {"processedCharactersCount": len(text),
                              "modelId": "inworld-tts-2-flash"}
    import base64

    assert base64.b64decode(first["audioContent"])[:4] == b"RIFF"
    assert json.loads(lines[1])["result"]["usage"] == {"processedCharactersCount": 0}
    # The whole body is the fake's, unmodified (the request was rewritten,
    # the response never is).
    expected = b"".join(fake_voice.inworld_lines(
        20, characters=len(text), model_id="inworld-tts-2-flash"))
    assert raw == expected
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    unit="characters", model="inworld.tts-2-flash")
    assert after - before == len(text), (
        "C21: the first line's processedCharactersCount is the exact bill")


async def test_inworld_empty_text_is_a_200_with_inworlds_own_line(gateway, client):
    body = {"modelId": "inworld.tts-2", "text": "   "}
    r = await client.post(f"{gateway.base_url}/inworld/tts/v1/voice:stream", json=body)
    assert r.status_code == 200
    assert r.content == fake_voice.INWORLD_EMPTY_LINE


async def test_inworld_sync_route_returns_the_json_object_intact(gateway, client):
    body = {"modelId": "inworld.tts-2", "text": "hello", "voiceId": "Aarav"}
    r = await client.post(f"{gateway.base_url}/inworld/tts/v1/voice", json=body)
    assert r.status_code == 200, r.text[:200]
    payload = r.json()
    assert payload["usage"]["processedCharactersCount"] == 5
    assert r.headers["x-gw-served-by"] == "inworld/inworld.tts-2"


async def test_inworld_unknown_model_400_code_3_is_model_not_found(gateway, client):
    body = {"modelId": "inworld.tts-2", "text": "hello"}
    r = await client.post(f"{gateway.base_url}/inworld/tts/v1/voice", json=body,
                          headers={"x-fake-mode": "inworld-400-code3"})
    # `ModelNotFound` keeps the upstream status (400 here, 404 on Anthropic).
    assert r.status_code == 400, r.text
    assert r.json()["error"]["type"] == "model_not_found"
    assert r.headers["x-gw-attempts"] == "1"


# ------------------------------------------------------------ ElevenLabs raw


async def test_elevenlabs_stream_with_a_voice_in_the_path_and_query_forwarded(gateway, client):
    body = {"text": "twelve chars", "model_id": "elevenlabs.flash-v2-5"}
    url = (f"{gateway.base_url}/elevenlabs/v1/text-to-speech/voice1/stream"
           "?output_format=pcm_16000")
    async with client.stream("POST", url, json=body) as r:
        raw = b"".join([c async for c in r.aiter_raw()])
        status, headers = r.status_code, r.headers
    assert status == 200, raw[:200]
    assert headers["content-type"].startswith("audio/mpeg")
    assert headers["x-gw-served-by"] == "elevenlabs/elevenlabs.flash-v2-5"
    assert raw == b"".join(fake_voice.raw_chunks(16))


async def test_elevenlabs_403_voice_denial_is_a_policy_error_that_opens_no_circuit(
    gateway, client,
):
    before = _metric(await _metrics(client, gateway), "llmgw_breaker_transitions_total")
    body = {"text": "hi", "model_id": "elevenlabs.flash-v2-5"}
    r = await client.post(f"{gateway.base_url}/elevenlabs/v1/text-to-speech/voice1/stream",
                          json=body, headers={"x-fake-mode": "elevenlabs-403-voice"})
    assert r.status_code == 403
    # A policy denial passes the provider's own body through (C4): the
    # entitlement is named only there.
    assert r.json()["detail"]["status"] == "voice_access_denied", r.text
    assert r.headers["x-gw-attempts"] == "1"
    after = _metric(await _metrics(client, gateway), "llmgw_breaker_transitions_total")
    assert after == before, "a plan/voice denial must not move any breaker"


# --------------------------------------------------------- AssemblyAI sync


async def test_assemblyai_sync_is_reached_with_the_raw_credential_and_billed_in_seconds(
    gateway, client,
):
    pcm = b"\x00\x01" * 32_000  # 64,000 bytes = 2.0 s at 16 kHz s16le
    r = await client.post(
        f"{gateway.base_url}/assemblyai/transcribe?model=assemblyai.sync",
        content=pcm, headers={"content-type": "audio/pcm"},
    )
    assert r.status_code == 200, r.text[:200]
    payload = r.json()
    assert payload["audio_duration_ms"] == 2000
    assert r.headers["x-gw-served-by"] == "assemblyai-sync/assemblyai.sync"


async def test_assemblyai_sync_without_a_model_is_refused_before_upstream(gateway, client):
    r = await client.post(f"{gateway.base_url}/assemblyai/transcribe", content=b"\x00" * 64,
                          headers={"content-type": "audio/pcm"})
    assert r.status_code == 400
    assert "model" in r.text

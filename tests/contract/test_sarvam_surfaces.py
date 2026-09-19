"""Sarvam's four HTTP speech routes over real sockets, against the fake
whose bodies were copied off the real one.

The gateway's catalog points the `sarvam` provider row at the fake with its
real credential style (`bearer`), so each case proves the whole path: route
-> provider -> Bearer header -> model rewrite (JSON key on TTS, multipart
form field on STT) -> framing -> bytes back intact -> what got billed.

The billing assertions are the ones worth reading. Sarvam reports no meter
on any of these four responses, so everything here is the gateway's own
estimate, and the tests assert BOTH that the number is right and that the
record says it is an estimate and why -- because a plausible number with no
provenance is what an invoice dispute cannot use.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from dataclasses import replace

import httpx
import pytest
from fakes import sarvam as fake_sarvam
from fakes.upstream import build_app as build_fake
from fakes.upstream import serve_in_thread

from llmgw.breaker import BreakerPolicy
from llmgw.catalog import DEFAULT_CATALOG, Catalog
from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, SurfaceLimits
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS

pytestmark = pytest.mark.contract

FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
                "x-fake-bytes")
KEY_ENV = "LLMGW_SARVAM_TEST_KEY"
KEY = "sarvam-test-key-not-real"

SARVAM_MODELS = ("sarvam.bulbul-v3", "sarvam.bulbul-v4-flash",
                 "sarvam.saaras-v3", "sarvam.saaras-v4", "sarvam.sarvam-105b")

TEXT = "The quick brown fox jumps over the lazy dog."


def wav(seconds: float, *, rate: int = 16_000) -> bytes:
    data = int(seconds * rate * 2)
    return b"".join([
        b"RIFF", struct.pack("<I", 36 + data), b"WAVE",
        b"fmt ", struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16),
        b"data", struct.pack("<I", data),
    ]) + b"\x00" * data


@pytest.fixture(scope="module")
def sarvam_fake():
    # The `audio` port carries every voice route, Sarvam's four included.
    server = serve_in_thread(build_fake("audio"))
    try:
        yield server
    finally:
        server.stop()


def _catalog(url: str) -> Catalog:
    base = DEFAULT_CATALOG
    providers = {"sarvam": replace(base.providers["sarvam"], base_url=url,
                                   api_key_env=KEY_ENV)}
    models = {mid: base.models[mid] for mid in SARVAM_MODELS}
    return Catalog(models=models, providers=providers)


def _config(url: str, **overrides) -> ServerConfig:
    os.environ.setdefault(KEY_ENV, KEY)
    settings = dict(
        catalog=_catalog(url),
        fake_upstreams=False,
        default_model="sarvam.bulbul-v3",
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(total=30.0, connect=2.0, headers=5.0, first_event=10.0,
                        progress=10.0, client_stall=10.0),
    )
    settings.update(overrides)
    return ServerConfig(**settings)


@pytest.fixture(scope="module")
def gateway(sarvam_fake):
    server = serve(build_app(_config(sarvam_fake.base_url)))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


def _metric(text: str, name: str, **labels: str) -> float:
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


# ------------------------------------------------------------------- routes


async def test_all_four_sarvam_routes_are_mounted_by_the_registry(gateway, client):
    """No `extra_surfaces`: if the registry did not mount them, these are
    404s from the gateway itself."""
    for route in ("/sarvam/text-to-speech", "/sarvam/text-to-speech/stream",
                  "/sarvam/speech-to-text", "/sarvam/speech-to-text-translate"):
        r = await client.post(f"{gateway.base_url}{route}")
        assert r.status_code != 404, route


# ------------------------------------------------------------ text to speech


async def test_buffered_tts_returns_the_json_object_and_bills_the_request_characters(
    gateway, client,
):
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     unit="characters", model="sarvam.bulbul-v3")
    body = {"model": "sarvam.bulbul-v3", "text": TEXT, "speaker": "shubh",
            "target_language_code": "en-IN", "speech_sample_rate": 16000}
    r = await client.post(f"{gateway.base_url}/sarvam/text-to-speech", json=body)
    assert r.status_code == 200, r.text[:300]
    payload = r.json()
    assert payload["request_id"].startswith("2026")
    assert base64.b64decode(payload["audios"][0])[:4] == b"RIFF"
    # No meter anywhere in the response -- that absence is the reason the
    # bill below is an estimate.
    assert "usage" not in payload
    assert r.headers["x-gw-served-by"] == "sarvam/sarvam.bulbul-v3"
    # The model rewrite reached the provider: the fake echoes the WIRE id it
    # was actually sent.
    assert payload["echo_model"] == "bulbul:v3"
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    unit="characters", model="sarvam.bulbul-v3")
    assert after - before == len(TEXT)


async def test_the_tts_bill_is_recorded_as_estimated_never_exact(gateway, client):
    """Sarvam normalises text before charging and never says what it
    counted. `len(text)` is the honest floor and the basis label must say so
    -- an estimate filed as exact is worse than no number."""
    before = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                     basis="exact", model="sarvam.bulbul-v3")
    before_est = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                         basis="estimated", model="sarvam.bulbul-v3")
    r = await client.post(f"{gateway.base_url}/sarvam/text-to-speech",
                          json={"model": "sarvam.bulbul-v3", "text": TEXT})
    assert r.status_code == 200
    text = await _metrics(client, gateway)
    assert _metric(text, "llmgw_cost_usd_total", basis="exact",
                   model="sarvam.bulbul-v3") == before
    assert _metric(text, "llmgw_cost_usd_total", basis="estimated",
                   model="sarvam.bulbul-v3") > before_est


async def test_the_chunked_tts_stream_reaches_the_client_byte_for_byte(gateway, client):
    """`audio/pcm`, chunked, no framing, NO terminal frame -- the body ends
    when the upstream's does, and the gateway appends nothing (C20)."""
    body = {"model": "sarvam.bulbul-v4-flash", "text": TEXT,
            "output_audio_codec": "linear16"}
    url = f"{gateway.base_url}/sarvam/text-to-speech/stream"
    async with client.stream("POST", url, json=body,
                             headers={"x-fake-events": "4"}) as r:
        raw = b"".join([c async for c in r.aiter_raw()])
        status, headers = r.status_code, r.headers
    assert status == 200, raw[:300]
    assert headers["content-type"].startswith("audio/pcm")
    assert headers["x-gw-served-by"] == "sarvam/sarvam.bulbul-v4-flash"
    assert raw == fake_sarvam.stream_bytes(4)


async def test_the_stream_route_also_bills_the_request_characters(gateway, client):
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     unit="characters", model="sarvam.bulbul-v3")
    url = f"{gateway.base_url}/sarvam/text-to-speech/stream"
    async with client.stream("POST", url,
                             json={"model": "sarvam.bulbul-v3", "text": "twelve chars"},
                             headers={"x-fake-events": "2"}) as r:
        async for _ in r.aiter_raw():
            pass
        assert r.status_code == 200
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    unit="characters", model="sarvam.bulbul-v3")
    assert after - before == len("twelve chars")


# ------------------------------------------------------------ speech to text


async def test_stt_multipart_rewrites_the_model_form_field_and_bills_wav_seconds(
    gateway, client,
):
    """The multipart splice is the whole point of this case: the caller names
    a CATALOG id in the form field and Sarvam must receive the wire id, with
    every other byte of the upload -- boundary, headers, RIFF payload --
    untouched."""
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     unit="seconds", model="sarvam.saaras-v3")
    audio = wav(2.0)
    r = await client.post(
        f"{gateway.base_url}/sarvam/speech-to-text",
        data={"model": "sarvam.saaras-v3", "language_code": "unknown"},
        files={"file": ("probe.wav", audio, "audio/wav")},
    )
    assert r.status_code == 200, r.text[:300]
    payload = r.json()
    assert payload["transcript"] == fake_sarvam.STT_TRANSCRIPT
    assert payload["language_code"] == "en-IN"
    assert payload["echo_model"] == "saaras:v3"
    assert r.headers["x-gw-served-by"] == "sarvam/sarvam.saaras-v3"
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    unit="seconds", model="sarvam.saaras-v3")
    assert after - before == 2


async def test_stt_translate_is_its_own_route_and_carries_diarized_transcript(
    gateway, client,
):
    # `saaras:v3`, not `v4`: the translate route serves a different model set
    # and 400s on `saaras:v4` (live, 19 Sep 2026).
    r = await client.post(
        f"{gateway.base_url}/sarvam/speech-to-text-translate",
        data={"model": "sarvam.saaras-v3"},
        files={"file": ("probe.wav", wav(1.0), "audio/wav")},
    )
    assert r.status_code == 200, r.text[:300]
    payload = r.json()
    assert "diarized_transcript" in payload and payload["diarized_transcript"] is None
    assert payload["echo_model"] == "saaras:v3"
    assert r.headers["x-gw-served-by"] == "sarvam/sarvam.saaras-v3"


async def test_an_upload_with_no_readable_wav_header_bills_nothing_rather_than_guessing(
    gateway, client,
):
    """A compressed upload has no duration the gateway can read, and Sarvam
    reports none. The record is an honest zero; there is no number to
    invent."""
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     unit="seconds", model="sarvam.saaras-v4")
    r = await client.post(
        f"{gateway.base_url}/sarvam/speech-to-text",
        data={"model": "sarvam.saaras-v4"},
        files={"file": ("clip.mp3", b"\xff\xfb\x90\x00" * 256, "audio/mpeg")},
    )
    assert r.status_code == 200, r.text[:300]
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    unit="seconds", model="sarvam.saaras-v4")
    assert after == before


async def test_stt_without_a_model_form_field_is_refused_before_any_socket(
    gateway, client, sarvam_fake,
):
    r = await client.post(f"{gateway.base_url}/sarvam/speech-to-text",
                          files={"file": ("probe.wav", wav(0.5), "audio/wav")})
    assert r.status_code == 400
    assert "model" in r.text


# ------------------------------------------------------------------- errors


async def test_an_unknown_model_400_is_model_not_found_not_a_bad_request(
    gateway, client,
):
    """Sarvam uses one error `code` for every 400, so only the enumerating
    message separates our stale catalog from the caller's mistake.

    The body the CLIENT sees is Sarvam's own (C4: the provider's body is the
    only place the served model list is named, and Sarvam's row scrubs auth
    bodies only). The classification is what the gateway learned, and it is
    visible where it matters -- on the counter an operator alerts on."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="sarvam_tts", code="model_not_found")
    r = await client.post(f"{gateway.base_url}/sarvam/text-to-speech",
                          json={"model": "sarvam.bulbul-v3", "text": "hi"},
                          headers={"x-fake-mode": "sarvam-unknown-model-400"})
    assert r.status_code == 400, r.text
    assert "bulbul:v4-flash" in r.json()["error"]["message"]
    assert r.json()["error"]["code"] == "invalid_request_error"
    after = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                    surface="sarvam_tts", code="model_not_found")
    assert after - before == 1.0, "our config drift, not the caller's bad request"


async def test_the_stt_spelling_of_the_same_400_classifies_the_same_way(
    gateway, client,
):
    """`body.model : Input should be ...` on speech-to-text against
    `- model: Input should be ...` on text-to-speech. One fault, two
    spellings, and a single-substring rule would have caught one of them."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="sarvam_stt", code="model_not_found")
    r = await client.post(
        f"{gateway.base_url}/sarvam/speech-to-text",
        data={"model": "sarvam.saaras-v3"},
        files={"file": ("probe.wav", wav(0.5), "audio/wav")},
        headers={"x-fake-mode": "sarvam-unknown-model-400-stt"},
    )
    assert r.status_code == 400, r.text
    after = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                    surface="sarvam_stt", code="model_not_found")
    assert after - before == 1.0


async def test_empty_text_is_sarvams_400_and_stays_the_callers_fault(gateway, client):
    """The counterweight. Inworld answers this with a 200 and a null usage;
    Sarvam refuses, and the refusal really is the caller's -- the
    unknown-model rule reads messages and must not swallow this one."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="sarvam_tts", code="invalid_request")
    r = await client.post(f"{gateway.base_url}/sarvam/text-to-speech",
                          json={"model": "sarvam.bulbul-v3", "text": ""},
                          headers={"x-fake-mode": "sarvam-empty-text-400"})
    assert r.status_code == 400, r.text
    assert r.json()["error"]["message"] == "'text' cannot be empty"
    after = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                    surface="sarvam_tts", code="invalid_request")
    assert after - before == 1.0


# ---------------------------------------------------------- the 403 circuit


async def test_a_403_is_a_credential_failure_that_opens_the_credential_circuit(
    sarvam_fake, client,
):
    """Sarvam's 403 is a bad key, not a rate limit and not a plan denial, so
    it belongs on the credential breaker -- and a gateway whose Sarvam key
    is wrong must stop hammering Sarvam for every tenant, not just for the
    one that noticed."""
    server = serve(build_app(_config(
        sarvam_fake.base_url,
        breaker=BreakerPolicy(failure_threshold=2, window=30.0, cooldown=30.0,
                              half_open_probes=1),
    )))
    try:
        body = {"model": "sarvam.bulbul-v3", "text": "hi"}
        url = f"{server.base_url}/sarvam/text-to-speech"
        hdr = {"x-fake-mode": "sarvam-bad-key-403"}
        first = await client.post(url, json=body, headers=hdr)
        assert first.status_code == 403, first.text
        # C11: the 403 status passes through, the BODY does not -- a
        # provider's rejection of the gateway's own credential is nothing the
        # client supplied and nothing it may read.
        assert first.json()["error"]["type"] == "upstream_auth"
        assert "Invalid or missing authentication" not in first.text
        second = await client.post(url, json=body, headers=hdr)
        assert second.status_code in (403, 503), second.text

        text = (await client.get(f"{server.base_url}/metrics")).text
        assert _metric(text, "llmgw_breaker_transitions_total", to="open") >= 1.0
        # And the circuit is the one the next request meets.
        third = await client.post(url, json=body, headers=hdr)
        assert third.headers.get("x-gw-breaker") in ("open", "half_open"), third.headers
    finally:
        server.stop()


# --------------------------------------------------------------------- caps


async def test_an_upload_over_the_surface_cap_is_refused_before_upstream(
    sarvam_fake, client,
):
    """A speech-to-text upload is the biggest body this gateway carries, so
    it is the one whose cap has to be enforced on the ingress side rather
    than discovered as an upstream 413."""
    server = serve(build_app(_config(
        sarvam_fake.base_url,
        surface_limits={"sarvam_stt": SurfaceLimits(max_request_bytes=64 * 1024)},
    )))
    try:
        r = await httpx.AsyncClient(timeout=15.0).post(
            f"{server.base_url}/sarvam/speech-to-text",
            data={"model": "sarvam.saaras-v3"},
            files={"file": ("big.wav", wav(4.0), "audio/wav")},
        )
        assert r.status_code == 413, r.text[:200]
    finally:
        server.stop()


# ------------------------------------------------------- the text model


async def test_sarvams_text_model_needs_no_surface_of_its_own(fakes, client):
    """Sarvam serves an OpenAI-compatible `/v1/chat/completions`: SSE, a
    usage-only final chunk, `data: [DONE]`, and an OpenAI-shaped
    `/v1/models`. So the catalog ROW is the whole integration -- no
    `sarvam_chat` surface, no fifth metric name, and the streaming path it
    rides is the one every other chat provider has already proven."""
    server = serve(build_app(_config(f"{fakes.openai.base_url}/v1")))
    try:
        body = {"model": "sarvam.sarvam-105b", "stream": True,
                "messages": [{"role": "user", "content": "hi"}]}
        url = f"{server.base_url}/v1/chat/completions"
        async with client.stream("POST", url, json=body) as r:
            raw = b"".join([c async for c in r.aiter_raw()])
            status, headers = r.status_code, r.headers
        assert status == 200, raw[:300]
        assert headers["content-type"].startswith("text/event-stream")
        assert headers["x-gw-served-by"] == "sarvam/sarvam.sarvam-105b"
        assert raw.rstrip().endswith(b"data: [DONE]")
        text = (await client.get(f"{server.base_url}/metrics")).text
        assert _metric(text, "llmgw_requests_total", surface="openai_chat",
                       outcome="completed") >= 1.0
    finally:
        server.stop()


# ---------------------------------------------------------- the cost record


async def test_every_sarvam_record_says_in_words_why_its_bill_is_an_estimate(
    sarvam_fake, client, tmp_path,
):
    """The capture record, not the metric. `basis=estimated` says the number
    is inexact; `cost_notes` says WHY, which is the half an operator needs
    and the half no pricing arithmetic can produce -- the reason lives in
    the dialect, not in the sum."""
    path = tmp_path / "capture.ndjson"
    server = serve(build_app(_config(sarvam_fake.base_url, capture_path=str(path))))
    try:
        r = await client.post(f"{server.base_url}/sarvam/text-to-speech",
                              json={"model": "sarvam.bulbul-v3", "text": TEXT})
        assert r.status_code == 200
        r = await client.post(
            f"{server.base_url}/sarvam/speech-to-text",
            data={"model": "sarvam.saaras-v3"},
            files={"file": ("probe.wav", wav(1.0), "audio/wav")},
        )
        assert r.status_code == 200
    finally:
        server.stop()

    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    assert len(records) == 2, records
    tts, stt = records
    assert tts["model"] == "sarvam.bulbul-v3"
    assert tts["basis"] == "estimated"
    assert tts["units"]["characters"] == len(TEXT)
    assert any("reports no usage on the wire" in n for n in tts["cost_notes"]), tts
    assert stt["model"] == "sarvam.saaras-v3"
    assert stt["basis"] == "estimated"
    assert stt["units"]["seconds"] == 1
    assert any("WAV header" in n for n in stt["cost_notes"]), stt

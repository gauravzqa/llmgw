"""Image generation over real sockets, against the fake whose bodies were
copied off the real one.

The gateway's catalog points the `openai` provider row at the fake, so each
case proves the whole path: route -> provider -> Bearer header -> model
rewrite -> framing -> bytes back intact -> what got billed.

The billing assertions are the ones worth reading, and they assert the
opposite of what the voice files assert. Every speech record here is
`estimated` because no speech provider meters what it bills; every image
record is `exact`, because the image endpoint states the whole bill in one
`usage` block on both forms. That asymmetry IS the design decision this
change made, so it is asserted rather than described.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from dataclasses import replace

import httpx
import pytest
from fakes import images as fake_images
from fakes.upstream import build_app as build_fake
from fakes.upstream import serve_in_thread

from llmgw.catalog import DEFAULT_CATALOG, Catalog
from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS

pytestmark = pytest.mark.contract

FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
                "x-fake-bytes")
KEY_ENV = "LLMGW_IMAGES_TEST_KEY"
KEY = "openai-test-key-not-real"

MODEL = "openai.gpt-image-1"
MINI = "openai.gpt-image-1-mini"
ROUTE = "/v1/images/generations"
PROMPT = "a single red circle on a white background"


def png_dims(raw: bytes) -> tuple[int, int]:
    """Width and height out of the IHDR, the way a client would read them.

    Never from the request's `size`: the point of decoding is to prove the
    bytes that arrived are the bytes the provider drew.
    """
    assert raw[:8] == b"\x89PNG\r\n\x1a\n", raw[:8]
    return struct.unpack(">II", raw[16:24])


@pytest.fixture(scope="module")
def images_fake():
    server = serve_in_thread(build_fake("openai"))
    try:
        yield server
    finally:
        server.stop()


def _catalog(url: str) -> Catalog:
    base = DEFAULT_CATALOG
    providers = {"openai": replace(base.providers["openai"], base_url=url,
                                   api_key_env=KEY_ENV)}
    models = {mid: base.models[mid] for mid in (MODEL, MINI)}
    return Catalog(models=models, providers=providers)


def _config(url: str, **overrides) -> ServerConfig:
    os.environ.setdefault(KEY_ENV, KEY)
    settings = dict(
        catalog=_catalog(url),
        fake_upstreams=False,
        default_model=MODEL,
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER_NEVER_TRIPS,
        # An image generation is slow: 7.2 s of provider time before the
        # status line on the measured low-quality call, against a 10 s
        # default `headers` budget. The fake answers instantly, so these are
        # generous only so the test fails for a reason and not a clock.
        budgets=Budgets(total=60.0, connect=2.0, headers=15.0, first_event=30.0,
                        progress=20.0, client_stall=20.0),
    )
    settings.update(overrides)
    return ServerConfig(**settings)


@pytest.fixture(scope="module")
def gateway(images_fake):
    server = serve(build_app(_config(images_fake.base_url)))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as c:
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


# -------------------------------------------------------------------- route


async def test_the_route_is_mounted_by_the_registry(gateway, client):
    """No `extra_surfaces`: if the registry did not mount it, this is a 404
    from the gateway itself."""
    r = await client.post(f"{gateway.base_url}{ROUTE}")
    assert r.status_code != 404
    # And under the workload prefix, like every other surface.
    r = await client.post(f"{gateway.base_url}/workloads/default{ROUTE}")
    assert r.status_code != 404


# ----------------------------------------------------------------- buffered


async def test_a_buffered_generation_returns_a_real_png_and_bills_exactly(
    gateway, client,
):
    before = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                     basis="exact", model=MODEL)
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json={"model": MODEL, "prompt": PROMPT,
                                "size": "1024x1024", "quality": "low"})
    assert r.status_code == 200, r.text[:300]
    payload = r.json()
    # The real response shape: echoed size/quality/output_format, a
    # generation_id beside every image, and a usage block.
    assert payload["size"] == "1024x1024" and payload["quality"] == "low"
    assert payload["output_format"] == "png" and payload["background"] == "opaque"
    assert payload["data"][0]["generation_id"]
    raw = base64.b64decode(payload["data"][0]["b64_json"])
    assert png_dims(raw) == (fake_images.DEFAULT_PIXELS,) * 2
    # The model rewrite reached the provider: the fake echoes the WIRE id.
    assert payload["echo_model"] == "gpt-image-1"
    # `x-gw-model` is the CATALOG id. A surface that echoed the wire id here
    # is one that billed against whatever row the provider happened to name.
    assert r.headers["x-gw-model"] == MODEL
    assert r.headers["x-gw-served-by"] == f"openai/{MODEL}"
    assert r.headers["x-gw-body-modified"] == "1"

    text = await _metrics(client, gateway)
    # 272 output tokens at $40/1M, 14-ish input at $5/1M. EXACT, because the
    # provider stated both halves -- the whole reason this surface needs no
    # `images` billing unit.
    assert _metric(text, "llmgw_cost_usd_total", basis="exact", model=MODEL) > before
    assert _metric(text, "llmgw_cost_usd_total", basis="estimated", model=MODEL) == 0.0
    assert _metric(text, "llmgw_tokens_total", model=MODEL, kind="output") == 272
    assert _metric(text, "llmgw_units_total", model=MODEL, unit="images") == 1


async def test_the_image_count_is_what_arrived_not_what_n_asked_for(gateway, client):
    """Live, 20 Sep 2026: `n=2` answered with ONE image. `llmgw_units_total
    {unit="images"}` counts `len(data)`, so the meter cannot claim an image
    the provider never made."""
    before = _metric(await _metrics(client, gateway), "llmgw_units_total",
                     model=MODEL, unit="images")
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json={"model": MODEL, "prompt": PROMPT, "n": 3,
                                "quality": "low", "size": "1024x1024"})
    assert r.status_code == 200
    assert len(r.json()["data"]) == 3
    after = _metric(await _metrics(client, gateway), "llmgw_units_total",
                    model=MODEL, unit="images")
    assert after - before == 3


async def test_the_mini_row_is_priced_a_fifth_of_the_full_one(gateway, client):
    """Two rows, two rates, same token counts. If both billed the same the
    catalog would be routing to a cheaper model and charging for the dear
    one."""
    body = {"prompt": PROMPT, "quality": "low", "size": "1024x1024"}
    before_full = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                          basis="exact", model=MODEL)
    before_mini = _metric(await _metrics(client, gateway), "llmgw_cost_usd_total",
                          basis="exact", model=MINI)
    assert (await client.post(f"{gateway.base_url}{ROUTE}",
                              json={**body, "model": MODEL})).status_code == 200
    assert (await client.post(f"{gateway.base_url}{ROUTE}",
                              json={**body, "model": MINI})).status_code == 200
    text = await _metrics(client, gateway)
    full = _metric(text, "llmgw_cost_usd_total", basis="exact", model=MODEL) - before_full
    mini = _metric(text, "llmgw_cost_usd_total", basis="exact", model=MINI) - before_mini
    assert mini > 0
    assert full / mini == pytest.approx(5.0, rel=0.02)


# ---------------------------------------------------------------- streaming


async def test_a_streamed_generation_ends_on_completed_and_not_on_a_done_marker(
    gateway, client,
):
    """The load-bearing case. The endpoint sends no `data: [DONE]`, and
    `pump._eof_is_terminal()` is False for SSE -- so if the surface did not
    call `image_generation.completed` terminal, this would be an
    `incomplete_stream` with a truncated body."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="images_generations", outcome="completed")
    events: list[str] = []
    async with client.stream(
        "POST", f"{gateway.base_url}{ROUTE}",
        json={"model": MODEL, "prompt": PROMPT, "stream": True,
              "partial_images": 2, "quality": "low", "size": "1024x1024"},
        headers={"x-fake-mode": "images-stream"},
    ) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        assert r.headers["x-gw-model"] == MODEL
        body = b""
        async for chunk in r.aiter_bytes():
            body += chunk
    for frame in body.split(b"\n\n"):
        for line in frame.split(b"\n"):
            if line.startswith(b"event:"):
                events.append(line[6:].strip().decode())
    assert events == ["image_generation.partial_image",
                      "image_generation.partial_image",
                      "image_generation.completed"]
    # Never fabricated, on any surface (C2).
    assert b"[DONE]" not in body

    text = await _metrics(client, gateway)
    assert _metric(text, "llmgw_requests_total", surface="images_generations",
                   outcome="completed") == before + 1
    assert _metric(text, "llmgw_requests_total", surface="images_generations",
                   outcome="failed") == 0.0


async def test_the_streamed_bill_includes_the_partials_the_provider_charged_for(
    gateway, client,
):
    """272 buffered against 472 with `partial_images: 2` (live, 20 Sep 2026).
    Nothing in the response says a partial was charged; only the total moves,
    which is exactly why the gateway bills the provider's total and not its
    own per-image table."""
    before = _metric(await _metrics(client, gateway), "llmgw_tokens_total",
                     model=MODEL, kind="output")
    async with client.stream(
        "POST", f"{gateway.base_url}{ROUTE}",
        json={"model": MODEL, "prompt": PROMPT, "stream": True,
              "partial_images": 2, "quality": "low", "size": "1024x1024"},
        headers={"x-fake-mode": "images-stream"},
    ) as r:
        async for _ in r.aiter_bytes():
            pass
        assert r.status_code == 200
    after = _metric(await _metrics(client, gateway), "llmgw_tokens_total",
                    model=MODEL, kind="output")
    assert after - before == 272 + 2 * fake_images.PARTIAL_IMAGE_TOKENS


async def test_an_sse_frame_larger_than_the_global_bound_still_gets_through(
    gateway, client,
):
    """One `image_generation.partial_image` frame is a whole base64 PNG --
    1.70 MB measured, against the 1 MiB process-wide `max_frame_bytes`. The
    surface's own frame cap is the only thing between that and
    `FrameTooLarge` on every streamed image."""
    # 1,100 px of incompressible greyscale is 1.2 MB of PNG and 1.6 MB of
    # base64 -- over the 1 MiB global, under the surface's 8 MiB cap.
    async with client.stream(
        "POST", f"{gateway.base_url}{ROUTE}",
        json={"model": MODEL, "prompt": PROMPT, "stream": True,
              "partial_images": 1, "quality": "low", "size": "1024x1024"},
        headers={"x-fake-mode": "images-stream", "x-fake-bytes": "1100"},
    ) as r:
        assert r.status_code == 200
        body = b"".join([chunk async for chunk in r.aiter_bytes()])
    biggest = max(len(f) for f in body.split(b"\n\n"))
    assert biggest > 1024 * 1024, biggest
    assert b"image_generation.completed" in body


# ------------------------------------------------------------------- faults


async def test_a_moderation_refusal_is_the_callers_fault_and_opens_no_circuit(
    gateway, client,
):
    """The interesting error. It is a 400 whose `type` is
    `image_generation_user_error` and whose `code` is `moderation_blocked` --
    no `content_filter`, no `content_policy` -- so before this change it
    classified as a plain `invalid_request` and would have been shopped
    around every fallback target."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="images_generations", code="content_filtered")
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json={"model": MODEL, "prompt": "refused"},
                          headers={"x-fake-mode": "images-moderation-400"})
    assert r.status_code == 400
    # The provider's own body reaches the client: `moderation_details` is the
    # only place the stage and the categories are named.
    payload = r.json()
    assert payload["error"]["code"] == "moderation_blocked"
    assert payload["error"]["moderation_details"]["moderation_stage"] == "input"
    # One attempt, not one per fallback: a refused prompt is not shopped
    # around until a provider says yes.
    assert r.headers["x-gw-attempts"] == "1"
    # The classification is not a header -- the provider's own status and
    # body pass through (C4) -- so it is visible where it matters: on the
    # counter an operator alerts on.
    after = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                    surface="images_generations", code="content_filtered")
    assert after - before == 1.0
    # NEUTRAL health: no breaker anywhere heard it.
    text = await _metrics(client, gateway)
    assert _metric(text, "llmgw_breaker_state", state="open") == 0.0


async def test_a_retired_model_is_catalog_drift_not_a_bad_request(gateway, client):
    """`dall-e-3` and `dall-e-2` are gone. The endpoint answers them like any
    other unknown id, and the gateway must call that config drift rather than
    telling the caller off for a model id that WE chose."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="images_generations", code="model_not_found")
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json={"model": MODEL, "prompt": PROMPT},
                          headers={"x-fake-mode": "images-unknown-model-400"})
    # The provider's own 400 and body reach the client; the classification
    # is on the counter.
    assert r.status_code == 400, r.text
    assert "dall-e-3" in r.json()["error"]["message"]
    after = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                    surface="images_generations", code="model_not_found")
    assert after - before == 1.0, "our config drift, not the caller's bad request"


async def test_a_parameter_the_provider_rejects_passes_its_own_message_through(
    gateway, client,
):
    """The provider rejects its own bodies better than the gateway can guess,
    and its message enumerates the sizes it takes. Passing it through is
    worth more than any 400 this surface could invent."""
    before = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                     surface="images_generations", code="invalid_request")
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json={"model": MODEL, "prompt": PROMPT, "size": "123x456"},
                          headers={"x-fake-mode": "images-bad-size-400"})
    assert r.status_code == 400
    assert "Supported sizes are 1024x1024" in r.json()["error"]["message"]
    after = _metric(await _metrics(client, gateway), "llmgw_requests_total",
                    surface="images_generations", code="invalid_request")
    assert after - before == 1.0


async def test_a_model_the_catalog_never_heard_of_opens_no_socket(gateway, client):
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json={"model": "openai.dall-e-3", "prompt": PROMPT})
    assert r.status_code == 400
    assert r.headers["x-gw-attempts"] == "0"
    assert r.headers["x-gw-served-by"] == "-"


async def test_a_body_with_no_model_is_refused_before_any_socket(gateway, client):
    r = await client.post(f"{gateway.base_url}{ROUTE}", json={"prompt": PROMPT})
    assert r.status_code == 400
    assert r.headers["x-gw-attempts"] == "0"


async def test_a_prompt_over_the_surface_request_cap_is_refused_before_upstream(
    images_fake, client,
):
    """A generation request is a prompt, capped by the API at 32,000
    characters. The surface takes 1 MiB -- thirty times that, a thirty-second
    of the global 32 MiB that exists for base64 vision uploads a generation
    never carries."""
    server = serve(build_app(_config(images_fake.base_url)))
    try:
        huge = json.dumps({"model": MODEL, "prompt": "x" * (2 * 1024 * 1024)})
        r = await client.post(f"{server.base_url}{ROUTE}", content=huge,
                              headers={"content-type": "application/json"})
        assert r.status_code == 413
        assert r.headers["x-gw-attempts"] == "0"
    finally:
        server.stop()


# ---------------------------------------------------------- the cost record


async def test_every_image_record_is_exact_and_needs_no_note_to_explain_itself(
    images_fake, client, tmp_path,
):
    """The mirror image of the Sarvam ledger test, and the point of the
    billing decision. Sarvam meters nothing, so every record there is
    `estimated` and must say why. OpenAI's image endpoint states the whole
    bill in one `usage` block on both forms, so every record here is `exact`
    -- and an exact record with a "why is this a guess" note attached would
    be noise, so there are none.
    """
    path = tmp_path / "capture.ndjson"
    server = serve(build_app(_config(images_fake.base_url, capture_path=str(path))))
    try:
        r = await client.post(f"{server.base_url}{ROUTE}",
                              json={"model": MODEL, "prompt": PROMPT,
                                    "quality": "low", "size": "1024x1024"})
        assert r.status_code == 200
        async with client.stream(
            "POST", f"{server.base_url}{ROUTE}",
            json={"model": MODEL, "prompt": PROMPT, "stream": True,
                  "partial_images": 1, "quality": "low", "size": "1024x1024"},
            headers={"x-fake-mode": "images-stream"},
        ) as sr:
            async for _ in sr.aiter_bytes():
                pass
            assert sr.status_code == 200
    finally:
        server.stop()

    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    assert len(records) == 2, records
    buffered, streamed = records
    for rec in records:
        assert rec["model"] == MODEL
        assert rec["basis"] == "exact", rec
        assert rec["cost_notes"] == [], rec
        assert rec["units"]["images"] == 1
        assert rec["cost_usd"] > 0
    assert buffered["tokens"]["output"] == 272
    assert streamed["tokens"]["output"] == 272 + fake_images.PARTIAL_IMAGE_TOKENS
    # The bill is the catalog's rate times the provider's count, to the cent.
    spec = DEFAULT_CATALOG.models[MODEL]
    expected = (buffered["tokens"]["input"] * spec.input_per_m
                + 272 * spec.output_per_m) / 1e6
    assert buffered["cost_usd"] == pytest.approx(expected)

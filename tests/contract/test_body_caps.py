"""PLAN-2 B4 over real sockets: per-surface request caps, and the B1 framing
refusal happening BEFORE the client's status is committed.

* a 20 MiB JSON body reaches the Anthropic messages surface (32 MiB cap) and
  comes back echoed by the fake;
* a 5 MiB body to chat (4 MiB cap) is a 413 the fake never sees;
* the same 5 MiB body is accepted when the chat surface's cap is raised
  through `surface_limits`;
* an SSE surface handed an `application/json` body is a 502
  `unsupported_upstream_framing` with `X-Gw-Attempts: 1`, not a 200 that
  stops.
"""

from __future__ import annotations

import json

import httpx
import pytest

from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, SurfaceLimits, fake_catalog
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = pytest.mark.contract

FAKE_HEADERS = (
    "x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
    "x-fake-status", "x-fake-bytes", "x-fake-seed", "x-fake-crlf",
)
MiB = 1024 * 1024


def _config(fakes: Fakes, **overrides) -> ServerConfig:
    settings = dict(
        catalog=fake_catalog(
            openai_url=f"{fakes.openai.base_url}/v1",
            anthropic_url=fakes.anthropic.base_url,
        ),
        fake_upstreams=True,
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(total=30.0, connect=2.0, first_event=10.0,
                        progress=10.0, client_stall=10.0),
    )
    settings.update(overrides)
    return ServerConfig(**settings)


@pytest.fixture(scope="module")
def gateway(fakes: Fakes):
    server = serve(build_app(_config(fakes)))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def roomy_gateway(fakes: Fakes):
    """Chat cap raised to 8 MiB through the per-surface table."""
    server = serve(build_app(_config(
        fakes, surface_limits={"openai_chat": SurfaceLimits(8 * MiB, 8 * MiB)},
    )))
    try:
        yield server
    finally:
        server.stop()


def _big_json(model: str, nbytes: int) -> bytes:
    """A JSON object of about `nbytes` with a vision-shaped payload."""
    filler = "A" * max(0, nbytes - 200)
    return json.dumps({
        "model": model, "stream": False, "max_tokens": 16,
        "messages": [{"role": "user", "content": [
            {"type": "text", "text": "describe"},
            {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{filler}"}},
        ]}],
    }).encode()


async def test_twenty_mib_reaches_the_messages_surface(gateway, fakes: Fakes, client):
    body = _big_json("fake.echo-anthropic", 20 * MiB)
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/anthropic/v1/messages", content=body,
        headers={"content-type": "application/json", "x-fake-mode": "big-vision"},
    )
    assert r.status_code == 200, r.text[:300]
    echoed = r.json()
    # The model rewrite re-serialises the JSON (compact separators, wire id),
    # so the byte count moves by a few dozen bytes; the payload is intact.
    assert abs(echoed["received_bytes"] - len(body)) < 256
    assert "messages" in echoed["keys"]
    assert fakes.stats()["total"] == before + 1


async def test_over_cap_to_chat_is_a_413_the_fake_never_sees(gateway, fakes: Fakes, client):
    body = _big_json("fake.echo", 33 * MiB)  # over the 32 MiB global default
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/v1/chat/completions", content=body,
        headers={"content-type": "application/json", "x-fake-mode": "big-vision"},
    )
    assert r.status_code == 413
    assert r.json()["error"]["type"] == "request_too_large"
    assert fakes.stats()["total"] == before, "the cap must fire before any upstream work"


async def test_the_chat_cap_is_per_surface(roomy_gateway, fakes: Fakes, client):
    body = _big_json("fake.echo", 5 * MiB)
    before = fakes.stats()["total"]
    r = await client.post(
        f"{roomy_gateway.base_url}/v1/chat/completions", content=body,
        headers={"content-type": "application/json", "x-fake-mode": "big-vision"},
    )
    assert r.status_code == 200, r.text[:300]
    assert abs(r.json()["received_bytes"] - len(body)) < 256
    assert fakes.stats()["total"] == before + 1


async def test_probe_reports_the_surface_limits(gateway, client):
    r = await client.get(f"{gateway.base_url}/workloads/default/probe")
    assert r.status_code == 200
    limits = r.json()["limits"]["surface_limits"]
    assert limits["anthropic_messages"]["max_request_bytes"] == 32 * MiB
    assert limits["openai_chat"]["max_request_bytes"] == 32 * MiB


async def test_wrong_content_type_is_refused_before_the_status_is_committed(
    gateway, fakes: Fakes, client
):
    """B1: the framing check runs pre-commit. A correct SSE stream served as
    `application/json` is a 502 the client can act on, not a 200 whose body
    stops; the fake was asked exactly once (single-target plan)."""
    before = fakes.stats()["total"]
    payload = {"model": "fake.echo", "stream": True,
               "messages": [{"role": "user", "content": "hi"}]}
    async with client.stream(
        "POST", f"{gateway.base_url}/v1/chat/completions", json=payload,
        headers={"x-fake-mode": "wrong-content-type"},
    ) as r:
        body = await r.aread()
        assert r.status_code == 502, body[:300]
        assert r.headers["x-gw-attempts"] == "1"
        err = json.loads(body)["error"]
        assert err["type"] == "unsupported_upstream_framing"
        assert "application/json" in err["message"]
    assert fakes.stats()["total"] == before + 1


async def test_a_normal_stream_still_passes_the_framing_check(gateway, client):
    payload = {"model": "fake.echo", "stream": True,
               "messages": [{"role": "user", "content": "hi"}]}
    async with client.stream(
        "POST", f"{gateway.base_url}/v1/chat/completions", json=payload,
    ) as r:
        body = await r.aread()
    assert r.status_code == 200
    assert body.rstrip().endswith(b"data: [DONE]")


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0)) as c:
        yield c

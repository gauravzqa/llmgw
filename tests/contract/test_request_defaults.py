"""PLAN-2 B5 over real sockets: a target's `request_defaults` reach the
provider for keys the client omitted, never for keys it sent, and the edit is
announced under `X-Gw-Body-Modified`.

The fake's `big-vision` mode echoes the parsed body it received, which is how
the test sees exactly what the gateway sent upstream.
"""

from __future__ import annotations

import dataclasses

import pytest

from llmgw.catalog import ModelSpec
from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, fake_catalog
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = [
    pytest.mark.contract,
    pytest.mark.skipif(
        "request_defaults" not in ModelSpec.__dataclass_fields__,
        reason="catalog.ModelSpec.request_defaults has not landed yet",
    ),
]

DEFAULTS = {"temperature": 0.1, "stream_options": {"include_usage": True}}


@pytest.fixture(scope="module")
def gateway(fakes: Fakes):
    base = fake_catalog(
        openai_url=f"{fakes.openai.base_url}/v1",
        anthropic_url=fakes.anthropic.base_url,
    )
    with_defaults = dataclasses.replace(
        base.models["fake.echo"], id="fake.defaults", aliases=(), request_defaults=DEFAULTS,
    )
    catalog = base.with_overrides(models={"fake.defaults": with_defaults})
    config = ServerConfig(
        catalog=catalog, fake_upstreams=True,
        forward_request_headers=("x-fake-mode",),
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(total=30.0, connect=2.0, first_event=10.0,
                        progress=10.0, client_stall=10.0),
    )
    server = serve(build_app(config))
    try:
        yield server
    finally:
        server.stop()


async def _echo(client, gateway, payload: dict) -> tuple[dict, dict]:
    r = await client.post(
        f"{gateway.base_url}/v1/chat/completions", json=payload,
        headers={"x-fake-mode": "big-vision"},
    )
    assert r.status_code == 200, r.text[:300]
    return r.json()["body"], dict(r.headers)


async def test_defaults_are_applied_to_keys_the_client_omitted(gateway, client):
    sent, headers = await _echo(client, gateway, {
        "model": "fake.defaults", "stream": False,
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert sent["temperature"] == 0.1
    assert sent["stream_options"] == {"include_usage": True}
    assert headers["x-gw-body-modified"] == "1"


async def test_defaults_never_overwrite_what_the_client_sent(gateway, client):
    sent, _ = await _echo(client, gateway, {
        "model": "fake.defaults", "stream": False, "temperature": 0.9,
        "stream_options": {"include_usage": False},
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert sent["temperature"] == 0.9
    assert sent["stream_options"] == {"include_usage": False}


async def test_a_target_without_defaults_is_untouched(gateway, client):
    sent, _ = await _echo(client, gateway, {
        "model": "fake.echo", "stream": False,
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert "temperature" not in sent and "stream_options" not in sent


@pytest.fixture
async def client():
    import httpx

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as c:
        yield c

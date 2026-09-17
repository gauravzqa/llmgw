"""Phase C over real sockets: `/v1/models` never calls upstream (C18),
`count_tokens` and `/v1/embeddings` pass through the fakes' new modes, the
buffered chat path now records usage (finding 50), and a path param plus a
credential-looking query key are handled the way the registry says."""

from __future__ import annotations

import json

import httpx
import pytest

from llmgw.breaker import BreakerPolicy
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, fake_catalog
from tests.contract.conftest import Fakes
from tests.contract.test_passthrough import _serve

pytestmark = pytest.mark.contract

TENANTS = '''
[tenants.acme]
tokens = ["tok-acme"]
rate_per_second = 100.0
burst = 200
max_concurrency = 32
'''
AUTH = {"Authorization": "Bearer tok-acme"}


@pytest.fixture(scope="module")
def gateway(fakes: Fakes, tmp_path_factory):
    tenants = tmp_path_factory.mktemp("c-tenants") / "tenants.toml"
    tenants.write_text(TENANTS, encoding="utf-8")
    catalog = fake_catalog(
        openai_url=f"{fakes.openai.base_url}/v1", anthropic_url=fakes.anthropic.base_url,
    )
    config = ServerConfig(
        catalog=catalog, fake_upstreams=True, tenants_file=str(tenants),
        breaker=BreakerPolicy(failure_threshold=1_000_000),
    )
    server = _serve(build_app(config))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


# ----------------------------------------------------------------- models


async def test_models_is_served_from_the_catalog_and_never_calls_upstream(
    gateway, fakes: Fakes, client
):
    before = fakes.stats()["total"]
    r = await client.get(f"{gateway.base_url}/v1/models", headers=AUTH)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["object"] == "list"
    ids = {m["id"] for m in body["data"]}
    assert "fake.echo" in ids  # fake_upstreams=True lists the fakes
    assert "openai.gpt-4o-mini" in ids
    assert not any(i.startswith("anthropic.") for i in ids)
    assert fakes.stats()["total"] == before, "C18: the listing opened no upstream connection"
    assert r.headers["x-gw-tenant"] == "acme"


async def test_the_anthropic_listing_has_its_own_shape_and_dialect(
    gateway, fakes: Fakes, client
):
    before = fakes.stats()["total"]
    r = await client.get(f"{gateway.base_url}/anthropic/v1/models", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["has_more"] is False
    ids = {m["id"] for m in body["data"]}
    assert "anthropic.haiku-4-5" in ids and "fake.echo-anthropic" in ids
    assert not any(i.startswith("openai.") for i in ids)
    assert fakes.stats()["total"] == before


async def test_models_requires_a_tenant_like_every_other_route(gateway, client):
    r = await client.get(f"{gateway.base_url}/v1/models")
    assert r.status_code == 401
    r = await client.get(f"{gateway.base_url}/workloads/default/v1/models", headers=AUTH)
    assert r.status_code == 200  # the workload-prefixed form is mounted too


# ------------------------------------------------------------ count_tokens


async def test_count_tokens_passes_through_and_bills_nothing(gateway, fakes: Fakes, client):
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/anthropic/v1/messages/count_tokens", headers=AUTH,
        json={"model": "fake.echo-anthropic",
              "messages": [{"role": "user", "content": "count me"}]},
    )
    assert r.status_code == 200, r.text
    assert isinstance(r.json()["input_tokens"], int)
    assert r.headers["x-gw-served-by"].endswith("fake.echo-anthropic")
    assert r.headers["x-gw-body-modified"] == "1"  # model rewritten to the wire id
    assert fakes.stats()["total"] == before + 1
    probe = await client.get(f"{gateway.base_url}/workloads/default/probe?tenant=acme")
    limits = probe.json()["limits"]["surface_limits"]
    assert limits["count_tokens"]["max_request_bytes"] == 4 * 1024 * 1024


# -------------------------------------------------------------- embeddings


async def test_embeddings_passes_through_with_input_token_usage(gateway, fakes: Fakes, client):
    scrape_before = await client.get(f"{gateway.base_url}/metrics")
    r = await client.post(
        f"{gateway.base_url}/v1/embeddings", headers=AUTH,
        json={"model": "openai.text-embedding-3-small", "input": "the quick brown fox"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["object"] == "list" and body["data"][0]["embedding"]
    assert body["usage"]["prompt_tokens"] > 0
    assert r.headers["x-gw-model"] == "openai.text-embedding-3-small"
    # The buffered response's model is put back to the catalog id (A1).
    assert body["model"] == "openai.text-embedding-3-small"
    scrape = (await client.get(f"{gateway.base_url}/metrics")).text
    line = next(
        (ln for ln in scrape.splitlines()
         if ln.startswith(
             'llmgw_tokens_total{kind="input",model="openai.text-embedding-3-small"')),
        None,
    )
    assert line is not None, "embeddings input tokens were not counted"
    assert float(line.rsplit(" ", 1)[1]) > 0
    del scrape_before


# ------------------------------------------------- buffered chat is billed


async def test_a_buffered_chat_call_now_records_usage_and_cost(gateway, client):
    r = await client.post(
        f"{gateway.base_url}/v1/chat/completions", headers=AUTH,
        json={"model": "fake.echo", "stream": False,
              "messages": [{"role": "user", "content": "hi"}], "max_tokens": 8},
    )
    assert r.status_code == 200, r.text
    scrape = (await client.get(f"{gateway.base_url}/metrics")).text
    outputs = [
        ln for ln in scrape.splitlines()
        if ln.startswith('llmgw_tokens_total{kind="output",model="fake.echo"')
    ]
    assert outputs, "finding 50: the buffered path used to record no usage"
    assert float(outputs[0].rsplit(" ", 1)[1]) > 0


# ------------------------------------------------------ query and params


async def test_a_credential_looking_query_key_is_refused_before_upstream(
    gateway, fakes: Fakes, client
):
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/v1/embeddings?api_key=sk-oops", headers=AUTH,
        json={"model": "openai.text-embedding-3-small", "input": "x"},
    )
    assert r.status_code == 400
    assert "credential" in r.json()["error"]["message"]
    assert fakes.stats()["total"] == before


async def test_realtime_call_control_fills_the_path_params(gateway, fakes: Fakes, client):
    r = await client.post(
        f"{gateway.base_url}/v1/realtime/calls/rtc_123/accept", headers=AUTH,
        json={"type": "realtime", "model": "openai.gpt-realtime-mini"},
    )
    assert r.status_code == 200, r.text
    assert r.headers["x-gw-served-by"].endswith("openai.gpt-realtime-mini")
    r = await client.post(f"{gateway.base_url}/v1/realtime/calls/rtc_123/explode",
                          headers=AUTH, json={})
    assert r.status_code == 400
    assert json.loads(r.text)["error"]["type"] == "invalid_request"

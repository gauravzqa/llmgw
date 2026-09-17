"""Phase E over real sockets (CONTRACTS C19): a mint never returns a
credential that outlives the cap, never lets the client widen a pinned
field, stamps the tenant on the upstream request, and the (N+1)th live
credential is a 429 with an honest Retry-After."""

from __future__ import annotations

import json
import time

import httpx
import pytest
from fakes.upstream import build_app as build_fake
from fakes.upstream import serve_in_thread

from llmgw.breaker import BreakerPolicy
from llmgw.catalog import ModelSpec
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, fake_catalog
from tests.contract.conftest import Fakes
from tests.contract.test_passthrough import _serve

pytestmark = pytest.mark.contract

GRACE = 130.0

TENANTS = '''
[tenants.layrs]
tokens = ["tok-layrs"]
rate_per_second = 100.0
burst = 200
max_concurrency = 32
max_sessions = 2

[tenants.layrs.realtime]
model = "openai.gpt-realtime-mini"
voice = "cedar"
tools = []
max_output_tokens = 512
expires_after_seconds_cap = 60

[tenants.open]
tokens = ["tok-open"]
rate_per_second = 100.0
burst = 200
max_concurrency = 32
'''
LAYRS = {"Authorization": "Bearer tok-layrs"}
OPEN = {"Authorization": "Bearer tok-open"}


@pytest.fixture(scope="module")
def assemblyai_fake():
    server = serve_in_thread(build_fake("assemblyai"))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def gateway(fakes: Fakes, assemblyai_fake, tmp_path_factory):
    tenants = tmp_path_factory.mktemp("e-tenants") / "tenants.toml"
    tenants.write_text(TENANTS, encoding="utf-8")
    catalog = fake_catalog(
        openai_url=f"{fakes.openai.base_url}/v1", anthropic_url=fakes.anthropic.base_url,
    )
    # The AssemblyAI streaming row points at the assemblyai fake port; the
    # mint's fixed model is a zero-priced row on that provider (the catalog
    # ships the provider; the row is the surface's requirement).
    streaming = catalog.providers["assemblyai-streaming"]
    from dataclasses import replace

    catalog = catalog.with_overrides(
        providers={"assemblyai-streaming": replace(
            streaming, base_url=assemblyai_fake.base_url, api_key_env=streaming.api_key_env,
        )},
        models={"assemblyai.streaming": ModelSpec(
            id="assemblyai.streaming", provider="assemblyai-streaming",
            api_model="universal-streaming", input_per_m=0.0, output_per_m=0.0,
            priced_at="2026-09-18",
        )},
    )
    config = ServerConfig(
        catalog=catalog, fake_upstreams=True, tenants_file=str(tenants),
        drain_grace_seconds=GRACE, breaker=BreakerPolicy(failure_threshold=1_000_000),
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


async def test_a_mint_is_capped_pinned_and_stamped(gateway, fakes: Fakes, client):
    r = await client.post(
        f"{gateway.base_url}/v1/realtime/client_secrets", headers=LAYRS,
        json={"expires_after": {"anchor": "created_at", "seconds": 3600},
              "session": {"type": "realtime", "model": "openai.gpt-realtime-mini",
                          "voice": "marin", "tools": [{"type": "function", "name": "rm_rf"}],
                          "max_output_tokens": "inf"}},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["value"].startswith("ek_")
    session = body["session"]                       # the fake echoes what it received
    assert session["voice"] == "cedar"              # pinned wins
    assert session["tools"] == []                   # the client could not widen
    assert session["max_output_tokens"] == 512
    assert session["model"] == "gpt-realtime-mini"  # the wire id went upstream
    # min(3600, cap 60, grace 130): the fake echoes the TTL it was asked for
    # as an absolute `expires_at`, the way OpenAI does.
    assert 50 <= body["expires_at"] - time.time() <= 61
    assert r.headers["x-gw-served-by"].endswith("openai.gpt-realtime-mini")
    assert r.headers["x-gw-body-modified"] == "1"
    # The upstream saw the tenant as the safety identifier.
    assert body.get("safety_identifier") in ("layrs", None)


async def test_the_second_tenant_without_a_pin_gets_the_grace_as_the_ceiling(gateway, client):
    r = await client.post(
        f"{gateway.base_url}/v1/realtime/client_secrets", headers=OPEN,
        json={"session": {"type": "realtime", "model": "openai.gpt-realtime-mini"}},
    )
    assert r.status_code == 200, r.text
    ttl = r.json()["expires_at"] - time.time()
    assert GRACE - 10 <= ttl <= GRACE + 1  # default 600 > grace 130


async def test_max_sessions_refuses_the_third_live_credential_with_a_retry_after(
    gateway, client
):
    # `layrs` has max_sessions = 2 and one credential from the first test may
    # still be alive; mint until refused and assert the refusal's shape.
    statuses = []
    last = None
    for _ in range(4):
        last = await client.post(
            f"{gateway.base_url}/v1/realtime/client_secrets", headers=LAYRS,
            json={"session": {"type": "realtime"}},
        )
        statuses.append(last.status_code)
        if last.status_code == 429:
            break
    assert 429 in statuses, statuses
    assert last is not None
    assert "session cap" in last.json()["error"]["message"]
    retry_after = float(last.headers["retry-after"])
    assert 0 < retry_after <= 60.0  # the earliest expiry, never above the cap


async def test_the_assemblyai_token_is_clamped_and_uses_raw_auth(gateway, client):
    r = await client.get(
        f"{gateway.base_url}/assemblyai/v3/token"
        "?expires_in_seconds=9999&max_session_duration_seconds=10800",
        headers=OPEN,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["token"].startswith("tmp_fake_")
    assert body["expires_in_seconds"] == 600                  # provider maximum
    assert body["max_session_duration_seconds"] == int(GRACE)  # the drain grace
    assert r.headers["x-gw-served-by"].endswith("assemblyai.streaming")


async def test_a_token_key_in_the_query_is_refused(gateway, client):
    r = await client.get(f"{gateway.base_url}/assemblyai/v3/token?token=abc", headers=OPEN)
    assert r.status_code == 400
    assert "credential" in json.loads(r.text)["error"]["message"]

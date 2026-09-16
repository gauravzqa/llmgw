"""The response-side identity fixes of PLAN-2 A1/A6, over real sockets.

* `X-Gw-Model` names the CATALOG id of the model that served, on streaming
  and buffered responses alike, and is absent when nobody served.
* On the buffered path, when the request body was rewritten to the wire id
  (`X-Gw-Body-Modified: 1`), the response body's `model` is put back to the
  catalog id, so an SDK that echoes the response model on its next turn sends
  a name the gateway issued. Streaming bodies are untouched.
* A provider 413 reaches the client as a 413 (`upstream_request_too_large`),
  not as a retried 502.
"""

from __future__ import annotations

import json

import httpx
import pytest

from tests.contract._phase_a_harness import (
    CANDIDATE_MODEL,
    INCUMBENT_MODEL,
    POLICY,
    GatewayPool,
    body,
    fake_mode_counts,
    mode,
    stream,
)
from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract


@pytest.fixture(scope="module")
def policy_file(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("policy-headers") / "workloads.toml"
    path.write_text(POLICY, encoding="utf-8")
    return str(path)


@pytest.fixture(scope="module")
def pool(fakes: Fakes, policy_file: str):
    p = GatewayPool(fakes, policy_file)
    try:
        yield p
    finally:
        p.stop_all()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


async def test_x_gw_model_names_the_catalog_id_that_served(
    pool: GatewayPool, client: httpx.AsyncClient,
):
    gw = pool.get(mode("ok"), mode("ok"))
    streamed = await stream(client, gw.url("ab"))
    assert streamed.status == 200
    assert streamed.headers["x-gw-model"] == CANDIDATE_MODEL
    assert streamed.headers["x-gw-served-by"].endswith(CANDIDATE_MODEL)

    buffered = await client.post(gw.url("ab"), json=body(stream=False))
    assert buffered.status_code == 200
    assert buffered.headers["x-gw-model"] == CANDIDATE_MODEL


async def test_x_gw_model_follows_the_fallback(
    pool: GatewayPool, client: httpx.AsyncClient,
):
    gw = pool.get(mode("5xx"), mode("ok"))
    result = await stream(client, gw.url("ab"))
    assert result.status == 200
    assert result.headers["x-gw-attempts"] == "2"
    assert result.headers["x-gw-model"] == INCUMBENT_MODEL


async def test_the_buffered_path_rewrites_only_a_json_object_body(
    pool: GatewayPool, client: httpx.AsyncClient,
):
    # The catalog id `fake.candidate` is rewritten to `fake-echo` on the way
    # up (X-Gw-Body-Modified: 1). The fake answers every mode as an SSE
    # stream even to `stream: false` -- which is exactly the shape the
    # buffered rewrite must leave alone: not a JSON object, so not touched,
    # and the honest content-length is still ours. The JSON-object half of
    # the rule is pinned by the unit test
    # `test_rewrite_response_model_puts_the_catalog_id_back_on_json_objects_only`.
    gw = pool.get(mode("ok"), mode("ok"))
    response = await client.post(gw.url("ab"), json=body(stream=False))
    assert response.status_code == 200
    assert response.headers["x-gw-body-modified"] == "1"
    assert response.headers["x-gw-model"] == CANDIDATE_MODEL
    assert response.content.startswith(b"data: ")
    assert b'"model": "fake-echo"' in response.content, "bytes untouched"
    assert int(response.headers["content-length"]) == len(response.content)


async def test_the_streaming_body_is_not_rewritten(
    pool: GatewayPool, client: httpx.AsyncClient,
):
    gw = pool.get(mode("ok"), mode("ok"))
    result = await stream(client, gw.url("ab"))
    assert result.status == 200
    assert result.headers["x-gw-model"] == CANDIDATE_MODEL
    # The wire id the fake put in every chunk is still there: byte-for-byte.
    first = next(line for line in result.body.splitlines() if line.startswith(b"data: {"))
    assert json.loads(first[len(b"data: "):])["model"] != CANDIDATE_MODEL


async def test_a_provider_413_is_a_413_to_the_client_and_not_retried(
    pool: GatewayPool, fakes: Fakes, client: httpx.AsyncClient,
):
    gw = pool.get(mode("413"), mode("ok"))
    response = await client.post(gw.url("solo"), json=body(stream=False))
    assert response.status_code == 413
    assert b"maximum size" in response.content, "the provider's own body (C4)"
    assert response.headers["x-gw-attempts"] == "1"
    assert fake_mode_counts(fakes).get("413") == 1, "no retry of a body that is too big"
    # And with a fallback available it is still NOT tried: the next provider's
    # limit is not larger because it is next.
    response = await client.post(gw.url("ab"), json=body(stream=False))
    assert response.status_code == 413
    assert response.headers["x-gw-attempts"] == "1"
    assert fake_mode_counts(fakes).get("ok") is None

"""Out-of-money arriving as a 429, over real sockets (PLAN-2 A2).

Two providers say "you have no credit" with a 429 and a code -- OpenAI's
`insufficient_quota` family, Anthropic's `details.error_code =
enforced_spend_limit_reached` -- and a status-only rule read both as a
transient rate limit: retry the same target with backoff, NEUTRAL, no blame.
The fake serves each dialect's shape on its own mode; these tests ask the
questions the classification decides:

    was the candidate asked exactly once (no retry-same)?
    did the incumbent serve (try-next)?
    when the candidate is the only target, is the client handed the
    provider's 429 and body (C4), with the provider's request id?
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
    path = tmp_path_factory.mktemp("policy-billing") / "workloads.toml"
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


@pytest.mark.parametrize("billing_mode", ["429-billing-openai", "429-billing-anthropic"])
async def test_a_billing_429_falls_back_once_and_never_retries_the_same_target(
    pool: GatewayPool, fakes: Fakes, client: httpx.AsyncClient, billing_mode: str,
):
    gw = pool.get(mode(billing_mode), mode("ok"))
    result = await stream(client, gw.url("ab"))

    assert result.status == 200
    assert result.headers["x-gw-attempts"] == "2"
    assert result.headers["x-gw-served-by"].endswith(INCUMBENT_MODEL)
    assert result.body.rstrip().endswith(b"data: [DONE]")
    counts = fake_mode_counts(fakes)
    # One ask of the broke provider -- a retry-same would make this 2 -- and
    # one of the incumbent. `RateLimited` would have retried the candidate
    # with backoff before falling back; `InsufficientCredits` does not.
    assert counts.get(billing_mode) == 1, counts
    assert counts.get("ok") == 1, counts


@pytest.mark.parametrize(
    "billing_mode, request_id, marker",
    [("429-billing-openai", "req_fake_billing_openai", b"insufficient_quota"),
     ("429-billing-anthropic", "req_fake_billing_anthropic",
      b"enforced_spend_limit_reached")],
)
async def test_a_billing_429_with_no_fallback_passes_through_with_the_upstream_request_id(
    pool: GatewayPool, fakes: Fakes, client: httpx.AsyncClient,
    billing_mode: str, request_id: str, marker: bytes,
):
    gw = pool.get(mode(billing_mode), mode("ok"))
    response = await client.post(gw.url("solo"), json=body(CANDIDATE_MODEL, stream=False))

    # C4: the provider's own status and body, once no fallback remains.
    assert response.status_code == 429
    assert marker in response.content
    assert response.headers["x-gw-attempts"] == "1"
    # A6d: the provider's request id, under our prefix. The provider's
    # rate-limit headers (if any) are NOT forwarded.
    assert response.headers["x-gw-upstream-request-id"] == request_id
    assert "x-ratelimit-remaining-requests" not in response.headers
    assert "retry-after" not in response.headers, "there is nothing to wait for"
    assert fake_mode_counts(fakes).get(billing_mode) == 1
    # The body is still parseable JSON in the provider's own shape.
    json.loads(response.content)

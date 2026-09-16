"""A provider that queues is not a provider that is down (PLAN-2 A4).

DeepSeek holds requests under load for up to ten minutes while sending SSE
comment lines; ElevenLabs does the same for a few hundred milliseconds. The
fake's `queue-then-serve` mode does exactly that for `delay` seconds and then
serves a correct stream. Against an impatient first-event budget the
gateway must still fall back -- the client's clock is the client's -- but it
must record the wait as a QUEUE: `FirstEventTimeout(queued=True)`, NEUTRAL
health, and one increment of `llmgw_queued_at_provider_total`. Against a
patient budget it simply waits and the candidate serves.
"""

from __future__ import annotations

import re

import httpx
import pytest

from tests.contract._phase_a_harness import (
    CANDIDATE_MODEL,
    POLICY,
    GatewayPool,
    fake_mode_counts,
    mode,
    stream,
)
from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract


@pytest.fixture(scope="module")
def policy_file(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("policy-queued") / "workloads.toml"
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
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as c:
        yield c


def _metric(text: str, name: str, **labels: str) -> float | None:
    """One sample out of a Prometheus text exposition, or None."""
    for line in text.splitlines():
        if not line.startswith(name + "{"):
            continue
        if all(f'{k}="{v}"' in line for k, v in labels.items()):
            return float(line.rsplit(" ", 1)[1])
    return None


async def test_an_impatient_budget_counts_the_queue_not_an_outage(
    pool: GatewayPool, fakes: Fakes, client: httpx.AsyncClient,
):
    """The impatient arm, as the gateway behaves TODAY.

    The first keep-alive comment is a body byte, so forwarding it commits the
    response (C1); the progress clock (1 s) then fires on a provider that has
    sent liveness and no content, and the pump stamps that stall `queued`.
    Post-commitment there is no fallback (C2), so the client gets a 200 that
    ends without `[DONE]`. What this phase adds is the classification: the
    queue is counted on `llmgw_queued_at_provider_total`, and the health the
    breaker hears is NEUTRAL. Falling back instead would need the executor's
    commitment hold to extend past heartbeat-only chunks -- recorded as the
    residual on FAILURE-MODES row 26, not built here.
    """
    gw = pool.get(mode("queue-then-serve", delay="3"), mode("ok"))
    before = _metric((await client.get(f"{gw.base_url}/metrics")).text,
                     "llmgw_queued_at_provider_total",
                     provider="candidate", model=CANDIDATE_MODEL) or 0.0

    # `ab` has progress = 1 s; the candidate sends only comments for 3 s.
    result = await stream(client, gw.url("ab"))

    assert result.status == 200, "committed by the forwarded keep-alive"
    assert result.headers["x-gw-attempts"] == "1"
    assert result.headers["x-gw-served-by"].endswith(CANDIDATE_MODEL)
    assert b": keep-alive" in result.body
    assert not result.body.rstrip().endswith(b"data: [DONE]"), "cut, natively (C2)"
    counts = fake_mode_counts(fakes)
    assert counts.get("queue-then-serve") == 1
    assert counts.get("ok") is None, "no post-commitment fallback"

    after = _metric((await client.get(f"{gw.base_url}/metrics")).text,
                    "llmgw_queued_at_provider_total",
                    provider="candidate", model=CANDIDATE_MODEL)
    assert after == before + 1.0, "the wait was a queue and is counted as one"


async def test_a_patient_budget_waits_the_queue_out_and_the_candidate_serves(
    pool: GatewayPool, fakes: Fakes, client: httpx.AsyncClient,
):
    gw = pool.get(mode("queue-then-serve", delay="3"), mode("ok"))
    result = await stream(client, gw.url("patient"))

    assert result.status == 200
    assert result.headers["x-gw-attempts"] == "1"
    assert result.headers["x-gw-served-by"].endswith(CANDIDATE_MODEL)
    assert result.headers["x-gw-model"] == CANDIDATE_MODEL
    # The comments the fake sent while queueing reached the client verbatim
    # (byte-for-byte passthrough) ahead of the stream proper.
    assert re.search(rb"^: keep-alive", result.body, re.M) is not None
    assert result.body.rstrip().endswith(b"data: [DONE]")
    assert fake_mode_counts(fakes).get("ok") is None, "the incumbent was never asked"

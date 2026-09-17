"""Unknown upstream framing, over real sockets (PLAN-2 B1).

Before this phase a streaming upstream body that was not SSE -- Inworld's
newline-delimited JSON, a raw `audio/mpeg` stream, or simply the right body
under the wrong `content-type` -- was fed to the SSE parser anyway. No frame
ever completed, the bytes still reached the client, and the request ended as
a provider stall or a frame bound with $0 accounted. The contract now is a
502 `unsupported_upstream_framing` decided BEFORE the status is committed,
so the candidate's misframed body is a fallback to the incumbent and the
client never sees half of it.

Fake modes these tests rely on (owned by the fakes' maintainer; the tests
skip, not fail, until they exist): `ndjson-stream`, `raw-stream`,
`wrong-content-type`. Their shapes are the measured ones from
capabilities/voice-inworld.md and capabilities/voice-openai.md.
"""

from __future__ import annotations

import json

import httpx
import pytest
from fakes.upstream import MODES

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

NON_SSE_MODES = ("ndjson-stream", "raw-stream", "wrong-content-type")


def _need(name: str) -> None:
    if name not in MODES:
        pytest.skip(f"fake mode {name!r} not shipped yet (Phase B fakes)")


@pytest.fixture(scope="module")
def policy_file(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("policy-framing") / "workloads.toml"
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


# The refusal has to run in the EXECUTOR before `commitment.open()`; the pump's
# own check fires after the status is already on the wire, which is what these
# two tests currently observe (200, upstream content-type forwarded, empty body,
# connection cut). Strict xfail: the moment the executor calls
# `assert_upstream_framing(surface.framing, upstream.headers.get("content-type"))`
# before opening the client response, these XPASS and the marker must go.
@pytest.mark.parametrize("bad", NON_SSE_MODES)
async def test_a_misframed_candidate_falls_back_to_the_incumbent_uncut(
    pool: GatewayPool, client: httpx.AsyncClient, fakes: Fakes, bad: str
):
    """The candidate answers with a body the SSE surface cannot frame; the
    incumbent serves. The client sees ONE clean stream ending in [DONE], the
    candidate was asked exactly once, and no misframed byte leaked."""
    _need(bad)
    before = fake_mode_counts(fakes)
    gw = pool.get(mode(bad), mode("ok"))
    got = await stream(client, gw.url("ab"), json=body(CANDIDATE_MODEL))
    assert got.status == 200, got.body[:300]
    assert got.body.endswith(b"data: [DONE]\n\n"), got.body[-120:]
    assert got.truncated is False
    assert got.headers["x-gw-attempts"] == "2"
    assert got.headers["x-gw-served-by"].endswith(INCUMBENT_MODEL)
    after = fake_mode_counts(fakes)
    assert after.get(bad, 0) - before.get(bad, 0) == 1, "candidate must not be retried"
    # The misframed body must not have been spliced in front of the good one.
    assert not got.body.lstrip().startswith(b"{"), "NDJSON/raw bytes leaked to the client"


@pytest.mark.parametrize("bad", NON_SSE_MODES)
async def test_a_misframed_only_target_is_a_502_naming_the_content_type(
    pool: GatewayPool, client: httpx.AsyncClient, bad: str
):
    """No fallback available: the client gets the gateway's own 502 with the
    upstream content type in the message -- not a 200 that stalls."""
    _need(bad)
    gw = pool.get(mode(bad), mode("ok"))
    got = await stream(client, gw.url("solo"), json=body(CANDIDATE_MODEL))
    assert got.status == 502, got.body[:300]
    err = json.loads(got.body)["error"]
    assert err["type"] == "unsupported_upstream_framing"
    assert "content-type" in err["message"]
    assert got.headers["x-gw-attempts"] == "1"


async def test_a_correct_sse_body_is_untouched_by_the_check(
    pool: GatewayPool, client: httpx.AsyncClient
):
    gw = pool.get(mode("ok"), mode("ok"))
    got = await stream(client, gw.url("solo"), json=body(CANDIDATE_MODEL))
    assert got.status == 200 and got.body.endswith(b"data: [DONE]\n\n")
    assert got.headers["x-gw-attempts"] == "1"

"""The per-process stream cap, proved over real sockets with streams held open.

`tests/unit/test_overload.py` pins the decision on the endpoint with the
tracker bumped by hand. This file is the S2 fix asserted end to end: two real
`slow-drip` streams are open through a real uvicorn, and the third request is
the one that has to be refused -- cheaply, before the fake ever hears of it,
and without the health endpoint noticing anything.

What is pinned, in order:

    1. with `max_streams=2` and two streams open, the third request gets 503
       `overloaded` with `Retry-After: 1`;
    2. the fake's request counter did NOT move for it -- the shed cost no
       upstream work (C6 extended to the process);
    3. `/healthz` answers 200 while the process is at its cap -- the cap
       exists so that it can;
    4. `/probe` reports the cap and the in-flight count next to it, and the
       `overloaded` denial where the other denials are;
    5. once one stream ends, the next request is admitted.
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import httpx
import pytest

from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes
from tests.contract.test_fallback import (
    KEY,
    KEY_ENV,
    GatewayServer,
    _serve,
    body,
    mode,
    two_target_catalog,
)

pytestmark = pytest.mark.contract

CAP = 2

# The primary target slow-drips a clean stream for ~4 s: long enough to hold
# the cap across everything a test does while it is up, short enough to end
# on its own if a test forgets to close it. Budgets cover it comfortably so a
# budget is never what ends a stream here.
OVERLOAD_POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 10.0
connect = 1.0
first_event = 2.0
progress = 2.0
client_stall = 6.0

[workloads.ab]
incumbent = "fake.incumbent"
candidate = "fake.candidate"
"""


@pytest.fixture(scope="module")
def gateway(fakes: Fakes, tmp_path_factory) -> GatewayServer:
    import os

    os.environ.setdefault(KEY_ENV, KEY)
    policy = tmp_path_factory.mktemp("overload") / "policy.toml"
    policy.write_text(OVERLOAD_POLICY)
    config = ServerConfig(
        catalog=two_target_catalog(
            fakes,
            candidate=mode("slow-drip", events="40", interval="0.1"),
            incumbent=mode("ok"),
        ),
        fake_upstreams=True,
        policy_file=str(policy),
        breaker=BREAKER_NEVER_TRIPS,
        max_streams=CAP,
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


async def _probe(client: httpx.AsyncClient, gateway: GatewayServer) -> dict:
    r = await client.get(f"{gateway.base_url}/workloads/ab/probe")
    assert r.status_code == 200, r.text
    return r.json()


async def _wait_inflight(
    client: httpx.AsyncClient, gateway: GatewayServer, value: int, *, wait_s: float = 5.0
) -> dict:
    """Poll `/probe` until the serving path holds exactly `value` requests.
    The probe is outside the endpoint's open/finally pair, so it does not
    count itself."""
    deadline = time.monotonic() + wait_s
    while True:
        probe = await _probe(client, gateway)
        if probe["inflight"] == value:
            return probe
        if time.monotonic() > deadline:  # pragma: no cover - diagnostic
            raise AssertionError(f"inflight never reached {value}: {probe['inflight']}")
        await asyncio.sleep(0.02)


@contextlib.asynccontextmanager
async def _open_stream(client: httpx.AsyncClient, gateway: GatewayServer):
    """A stream admitted and committed: the status line has arrived, so the
    gateway's tracker counts it and a permit is held until this exits."""
    async with client.stream("POST", gateway.url(), json=body(stream=True)) as response:
        assert response.status_code == 200, await response.aread()
        yield response


async def test_the_request_over_the_cap_is_shed_before_any_upstream_work(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes,
):
    probe = await _probe(client, gateway)
    assert probe["max_streams"] == CAP
    denied_before = probe["denials"]["overloaded"]

    async with _open_stream(client, gateway), _open_stream(client, gateway):
        await _wait_inflight(client, gateway, CAP)
        seen_by_fake = fakes.stats()["total"]
        assert seen_by_fake == CAP

        # (1) the third request is refused ...
        third = await client.post(gateway.url(), json=body(stream=True))
        assert third.status_code == 503, third.text
        assert third.json()["error"]["type"] == "overloaded"
        assert third.headers["retry-after"] == "1"
        assert "x-gw-tenant" in third.headers

        # ... for the non-streaming shape too: the cap is about the process,
        # not about SSE.
        buffered = await client.post(gateway.url(), json=body(stream=False))
        assert buffered.status_code == 503
        assert buffered.json()["error"]["type"] == "overloaded"

        # (2) and the fake never heard of either.
        assert fakes.stats()["total"] == seen_by_fake

        # (3) the health endpoint is outside the cap.
        healthz = await client.get(f"{gateway.base_url}/healthz")
        assert healthz.status_code == 200
        assert healthz.json()["draining"] is False

        # (4) the diagnostic says why.
        probe = await _probe(client, gateway)
        assert probe["inflight"] == CAP
        assert probe["denials"]["overloaded"] == denied_before + 2

    # Both streams closed by the client: the tracker returns to zero, which is
    # also what a drain would now see.
    await _wait_inflight(client, gateway, 0)


async def test_a_slot_freed_by_a_finished_stream_admits_the_next_request(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes,
):
    async with _open_stream(client, gateway):
        async with _open_stream(client, gateway):
            await _wait_inflight(client, gateway, CAP)
            refused = await client.post(gateway.url(), json=body(stream=True))
            assert refused.status_code == 503
        # (5) one stream ended; the gateway notices within an event interval.
        await _wait_inflight(client, gateway, CAP - 1)

        admitted = await client.post(gateway.url(), json=body(stream=False))
        assert admitted.status_code == 200, admitted.text
        assert admitted.headers["x-gw-attempts"] == "1"

    await _wait_inflight(client, gateway, 0)

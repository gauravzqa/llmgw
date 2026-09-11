"""Fixtures for the contract tier: two real uvicorn servers on real sockets.

The tier's whole justification is that nothing here is mocked. `TestClient`
and ASGI transports are excluded on purpose -- they short-circuit the socket,
and every behaviour this directory asserts (truncated chunked bodies, headers
flushed before a body, a stall the client has to time out of) lives in exactly
the layer they skip.

The servers are session-scoped and run on background threads with their own
event loops. Session-scoped because starting uvicorn costs ~20 ms and the
budget for the whole file is 20 s; threads because pytest-asyncio hands each
test function a fresh event loop, and because a fake upstream sharing a loop
with the code under test can hide a blocking bug in either one.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest
from fakes.upstream import PATHS, RunningServer, Surface, build_app, serve_in_thread

from llmgw.breaker import BreakerPolicy

BREAKER_NEVER_TRIPS = BreakerPolicy(failure_threshold=1_000_000)
"""The breaker policy every session-scoped gateway in this tier runs under,
EXCEPT `test_isolation.py`, which is the file that tests breakers.

P4 wired a real registry into `Gateway`, and with the shipped default of five
failures in thirty seconds it does exactly what it should: after the fifth
`5xx` a session-scoped gateway has an OPEN circuit for its one hostile target,
and the sixth test's request is refused before a socket rather than served
the failure it asked for. Every file here asserts what ONE request does
against a hostile target, and reuses a server across tests to keep the tier
under its budget -- so with the default policy the outcome of test N would
depend on how many of tests 1..N-1 failed the same target, which is precisely
the order dependence `_clean_counters` exists to prevent for the fake's
counters. There is no `reset()` on the registry to mirror that fixture (a
breaker that can be reset from outside is a breaker that will be), so the
threshold is placed out of reach instead.

The gate is still WIRED -- tickets are minted and settled, permits are taken
and returned, `X-Gw-Tenant` is on every response -- so these files exercise
the P4 plumbing on every request. They simply never see it trip.
"""


@dataclass(frozen=True)
class Fakes:
    """Both hostile upstreams, plus the counters they share."""

    openai: RunningServer
    anthropic: RunningServer

    def url(self, surface: Surface) -> str:
        server = self.openai if surface == "openai" else self.anthropic
        return f"{server.base_url}{PATHS[surface]}"

    @property
    def stats_url(self) -> str:
        # Either port answers: the counters are process-global by design, so a
        # test asking "was the incumbent opened?" asks once, not twice.
        return f"{self.openai.base_url}/__stats"

    def stats(self) -> dict:
        return httpx.get(self.stats_url, timeout=5.0).json()

    def reset_stats(self) -> None:
        httpx.post(f"{self.stats_url}/reset", timeout=5.0).raise_for_status()


@pytest.fixture(scope="session")
def fakes():
    openai = serve_in_thread(build_app("openai"))
    anthropic = serve_in_thread(build_app("anthropic"))
    try:
        yield Fakes(openai=openai, anthropic=anthropic)
    finally:
        openai.stop()
        anthropic.stop()


@pytest.fixture(autouse=True)
def _clean_counters(fakes: Fakes):
    """Zero the counters before every test.

    Autouse so that counter assertions are order-independent. A test that
    passes only when run after its neighbour is a test that will fail on the
    day someone adds a case above it, and the failure will look like a product
    bug rather than a fixture bug.
    """
    fakes.reset_stats()


@pytest.fixture
async def client():
    """A client with a deliberately short default timeout.

    The stall modes are asserted by *timing out*, and the honest way to keep
    this file under 20 s is to prove "nothing arrived for 300 ms while the
    upstream intends to be silent for 5 s" rather than to sit out the stall.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, read=5.0)) as c:
        yield c

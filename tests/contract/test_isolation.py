"""Isolation over real sockets: the P4 gate, asserted from outside.

`tests/unit/test_admission.py` and `test_breaker.py` prove the two state
machines on a `ManualClock`, and `tests/unit/test_executor.py` proves the
loop settles tickets and permits on every exit. None of those is an HTTP
response, and the questions this file answers are the ones only a response
-- or an upstream counter -- can:

    when tenant A is fifty requests over its cap, does tenant B notice?
    is a tenant at its cap refused BEFORE we read its body, or after?
    does a shared credential ever have more streams open than its cap?
    after a circuit opens, how many requests reach the provider? (one.)
    is a client hanging up ten times evidence about anything?

--------------------------------------------------------------------------
One gateway, one tenants file, one policy file
--------------------------------------------------------------------------

Session-scoped, like every gateway in this tier, and unlike the others it
runs the SHIPPED breaker shape with small numbers: three failures open a
circuit and the cooldown is 300 ms, so a trip-probe-recover cycle costs
under half a second of wall clock instead of ten. That is the only place
this file sleeps on purpose.

Three consequences of sharing one process across tests, each handled by a
fixture rather than by test ordering:

* the fake's counters are reset before every test (`_clean_counters`, from
  conftest);
* every circuit is healed to CLOSED before every test (`_closed_circuits`),
  by waiting out whatever cooldown remains and sending one healthy probe --
  because a test that trips the candidate must not hand the next test an
  open circuit, and the registry has no reset (a breaker that can be reset
  from outside is a breaker that will be);
* every permit and every probe slot is asserted back to zero AFTER every
  test (`_gate_returns_to_zero`), on the server's own loop, because a leak
  is invisible to the client that caused it.

--------------------------------------------------------------------------
How a target picks its behaviour, and how a REQUEST overrides it
--------------------------------------------------------------------------

As in `test_fallback.py`, the candidate is baked `5xx` and the incumbent
`ok` through `ProviderConn.extra_headers`. This file additionally forwards
the client's `X-Fake-*` headers, and `build_headers()` merges those LAST --
so a request that sends `X-Fake-Mode: ok` makes BOTH targets healthy for
that one request. That is exactly the switch `breaker_single_probe` needs:
the candidate fails until its circuit opens, and then the fake is "fixed"
per request without restarting anything.

The price is that a client header cannot make the two targets differ, so
"exactly one request reached the candidate" is asserted on the single-target
`solo` workload, where the fake's `total` counts the candidate and nothing
else.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import statistics
import threading
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass

import httpx
import pytest
import uvicorn
from starlette.applications import Starlette

from llmgw import metrics
from llmgw.breaker import BreakerPolicy
from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract

KEY_ENV = "LLMGW_ISOLATION_KEY"
KEY = "sk-isolation-not-a-real-key"

CANDIDATE = "candidate"
INCUMBENT = "incumbent"
KEY_X = "key-x"
KEY_Y = "key-y"
SHARED_CREDENTIAL = "shared-cred"
SHARED_CAP = 4

CANDIDATE_MODEL = "fake.candidate"
INCUMBENT_MODEL = "fake.incumbent"
KEY_X_MODEL = "fake.key-x"
KEY_Y_MODEL = "fake.key-y"

ROUTE = "/v1/chat/completions"

FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval",
                "x-fake-delay", "x-fake-status")

BREAKER = BreakerPolicy(failure_threshold=3, window=30.0, cooldown=0.3, half_open_probes=1)
"""Small enough that a trip costs three requests and a recovery costs a
300 ms sleep. The SHAPE is the shipped one -- sliding window, one probe."""

# The tokens are made up and the tenants file below is written per session.
# Named by tenant so a test reads as "tenant a does X", never as a token.
TOKENS = {
    "a": "tok-a-0f3e9c",
    "b": "tok-b-7d21aa",
    "c": "tok-c-b8e412",
    "x": "tok-x-55c0de",
    "y": "tok-y-9a1f07",
    "ratey": "tok-ratey-3c8b6d",
}

A_BURST = 100
A_CAP = 8
B_CAP = 20
OVERFLOW = 50

TENANTS = f"""
# Tenant a: the hot one. A rate so low the bucket never visibly refills
# inside a test, so "no token charged on a concurrency denial" is a number
# the probe can report exactly.
[tenants.a]
tokens = ["{TOKENS['a']}"]
rate_per_second = 0.001
burst = {A_BURST}
max_concurrency = {A_CAP}

[tenants.b]
tokens = ["{TOKENS['b']}"]
rate_per_second = 100.0
burst = 200
max_concurrency = {B_CAP}

# One permit: the tenant that is "at its cap" after a single stream.
[tenants.c]
tokens = ["{TOKENS['c']}"]
rate_per_second = 100.0
burst = 50
max_concurrency = 1

# x and y each fit six in flight; the credential they share fits four.
[tenants.x]
tokens = ["{TOKENS['x']}"]
rate_per_second = 100.0
burst = 50
max_concurrency = 6

[tenants.y]
tokens = ["{TOKENS['y']}"]
rate_per_second = 100.0
burst = 50
max_concurrency = 6

# One token, refilled every two seconds: the rate-denial case.
[tenants.ratey]
tokens = ["{TOKENS['ratey']}"]
rate_per_second = 0.5
burst = 1
max_concurrency = 4

# Guest access, generous enough that the breaker tests (which send no token)
# are never refused by admission instead of by the circuit under test.
[tenants.anonymous]
rate_per_second = 100.0
burst = 400
max_concurrency = 32
"""

POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 8.0
connect = 1.0
first_event = 2.0
progress = 2.0
client_stall = 5.0

[workloads.ab]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

# Candidate only. With nowhere to fall back to, an open circuit is a 503 and
# the fake's `total` counts the candidate alone.
[workloads.solo]
incumbent = "fake.candidate"

[workloads.keyx]
incumbent = "fake.key-x"

[workloads.keyy]
incumbent = "fake.key-y"
"""


def body(*, stream: bool = True) -> dict:
    return {"model": CANDIDATE_MODEL, "stream": stream, "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}]}


def auth(tenant: str) -> dict[str, str]:
    return {"authorization": f"Bearer {TOKENS[tenant]}"}


def fake(mode: str, **params: str) -> dict[str, str]:
    """Per-request fake behaviour, as the headers the gateway forwards."""
    return {"x-fake-mode": mode, **{f"x-fake-{k}": v for k, v in params.items()}}


SLOW = fake("slow-drip", interval="0.1", events="40")
"""A stream that stays open for ~4 s: long enough to hold a permit across
everything a test does while it is up, short enough to end by itself if a
test forgets to close it."""


# ==========================================================================
# The gateway under test, with a handle on its loop
# ==========================================================================


@dataclass
class GatewayServer:
    """One uvicorn hosting one `build_app()` result, plus its event loop.

    The loop is the field the fallback tests do not need and this file does:
    "every permit came back" is a statement about dictionaries the SERVER
    mutates, and reading them from the test thread races the mutation. So
    `on_loop` marshals the read onto the loop that owns the data, the same
    way `tests/chaos/conftest.py` samples its task count.
    """

    app: Starlette
    server: uvicorn.Server
    thread: threading.Thread
    port: int
    loop: asyncio.AbstractEventLoop

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, workload: str | None = None) -> str:
        if workload is None:
            return f"{self.base_url}{ROUTE}"
        return f"{self.base_url}/workloads/{workload}{ROUTE}"

    def probe_url(self, workload: str, **params: str) -> str:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        return f"{self.base_url}/workloads/{workload}/probe" + (f"?{query}" if query else "")

    @property
    def gateway(self):
        return self.app.state.gateway

    def on_loop(self, fn):
        async def _run():
            return fn()

        return asyncio.run_coroutine_threadsafe(_run(), self.loop).result(5.0)

    def gate_state(self) -> dict:
        gw = self.gateway
        return self.on_loop(lambda: {
            "tenant_permits": gw.admission.total_in_use(),
            "key_permits": gw.limiter.total_in_use(),
            "breakers": {"/".join(k): v for k, v in gw.breakers.snapshot().items()},
        })

    def stop(self, timeout: float = 5.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout)
        if self.thread.is_alive():  # pragma: no cover - only if a stream wedges
            self.server.force_exit = True
            self.thread.join(timeout)


def _serve(app: Starlette, *, startup_timeout: float = 10.0) -> GatewayServer:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(2048)
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="critical", access_log=False, lifespan="on")
    server = uvicorn.Server(config)
    holder: dict[str, asyncio.AbstractEventLoop] = {}
    ready = threading.Event()

    def _run() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        holder["loop"] = loop
        ready.set()
        try:
            loop.run_until_complete(server.serve(sockets=[sock]))
        finally:
            loop.close()

    thread = threading.Thread(target=_run, daemon=True, name=f"llmgw-isolation-{port}")
    thread.start()
    ready.wait(startup_timeout)
    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError(f"gateway on port {port} failed to start")
        time.sleep(0.005)
    return GatewayServer(app=app, server=server, thread=thread, port=port,
                         loop=holder["loop"])


def catalog_for(fakes: Fakes) -> Catalog:
    """Four targets on the OpenAI fake's port.

    `candidate`/`incumbent` are the A/B pair with baked modes. `key-x` and
    `key-y` are two catalog entries that present ONE credential -- the
    `openrouter` / `openrouter-toolsafe` shape from the shipped catalog --
    with a cap of four each, which the limiter must read as four in total.
    """

    def conn(pid: str, extra: dict[str, str], **kw) -> ProviderConn:
        return ProviderConn(id=pid, kind="openai", base_url=fakes.openai.base_url,
                            api_key_env=KEY_ENV, extra_headers=dict(extra), **kw)

    def spec(mid: str, pid: str) -> ModelSpec:
        return ModelSpec(id=mid, provider=pid, api_model=f"wire-{pid}", input_per_m=1.0,
                         output_per_m=2.0, priced_at="2026-09-09")

    return Catalog(
        providers={
            CANDIDATE: conn(CANDIDATE, fake("5xx"), max_concurrency=64),
            INCUMBENT: conn(INCUMBENT, fake("ok"), max_concurrency=64),
            KEY_X: conn(KEY_X, {}, credential_id=SHARED_CREDENTIAL,
                        max_concurrency=SHARED_CAP),
            KEY_Y: conn(KEY_Y, {}, credential_id=SHARED_CREDENTIAL,
                        max_concurrency=SHARED_CAP),
        },
        models={
            CANDIDATE_MODEL: spec(CANDIDATE_MODEL, CANDIDATE),
            INCUMBENT_MODEL: spec(INCUMBENT_MODEL, INCUMBENT),
            KEY_X_MODEL: spec(KEY_X_MODEL, KEY_X),
            KEY_Y_MODEL: spec(KEY_Y_MODEL, KEY_Y),
        },
    )


@pytest.fixture(scope="session")
def gateway(fakes: Fakes, tmp_path_factory):
    os.environ.setdefault(KEY_ENV, KEY)
    root = tmp_path_factory.mktemp("isolation")
    (root / "workloads.toml").write_text(POLICY, encoding="utf-8")
    (root / "tenants.toml").write_text(TENANTS, encoding="utf-8")
    config = ServerConfig(
        catalog=catalog_for(fakes),
        fake_upstreams=True,
        policy_file=str(root / "workloads.toml"),
        tenants_file=str(root / "tenants.toml"),
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER,
    )
    server = _serve(build_app(config))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
async def client():
    """Generous next to every budget here; the largest total is 8 s."""
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), limits=limits) as c:
        yield c


async def _settled(gateway: GatewayServer, patience: float = 3.0) -> dict:
    """Poll the gate until it is idle, or return what it looked like at the
    deadline. A cancelled stream takes a few loop turns to unwind."""
    deadline = time.monotonic() + patience
    while True:
        state = gateway.gate_state()
        probes = sum(b["probes_in_flight"] for b in state["breakers"].values())
        if state["tenant_permits"] == 0 and state["key_permits"] == 0 and probes == 0:
            return state
        if time.monotonic() > deadline:
            return state
        await asyncio.sleep(0.02)


@pytest.fixture(autouse=True)
async def _closed_circuits(gateway: GatewayServer, fakes: Fakes, _clean_counters):
    """Every circuit CLOSED before every test.

    A test that opened the candidate's circuit is entitled to; the next test
    is entitled to a candidate that answers. Healing is done the only way a
    breaker allows: wait out the cooldown, send one HEALTHY request through
    the single-target workload so the probe succeeds, repeat until the
    registry reports nothing but `closed`. Bounded, because a fixture that
    can hang is a fixture that will.

    Ordered after `_clean_counters` (by depending on it) and re-zeroing the
    fake's counters if it sent anything, so a probe sent by this fixture is
    never the "extra request" a test's `total` assertion trips over.
    """
    healed = False
    async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as c:
        for _ in range(20):
            open_ = {k: v for k, v in gateway.gate_state()["breakers"].items()
                     if v["state"] != "closed"}
            if not open_:
                break
            wait = max((v["cooldown_remaining"] or 0.0) for v in open_.values())
            await asyncio.sleep(wait + 0.02)
            await c.post(gateway.url("solo"), json=body(), headers=fake("ok"))
            healed = True
        else:  # pragma: no cover - only if healing itself is broken
            raise RuntimeError(f"could not close circuits: {open_}")
    if healed:
        fakes.reset_stats()
    yield


@pytest.fixture(autouse=True)
async def _gate_returns_to_zero(gateway: GatewayServer):
    """FAILURE-MODES row 19, after every test, on the server's loop.

    Tenant permits, provider-key permits and half-open probe slots: all
    three are "capacity that ratchets to zero over hours if one exit path
    forgets", and all three are asserted here rather than in the tests that
    hold them, because the test that leaks is never the one that notices.
    """
    yield
    state = await _settled(gateway)
    assert state["tenant_permits"] == 0, f"tenant permits leaked: {state}"
    assert state["key_permits"] == 0, f"provider-key permits leaked: {state}"
    for key, snap in state["breakers"].items():
        assert snap["probes_in_flight"] == 0, f"{key}: probe slot leaked: {snap}"


# ==========================================================================
# Helpers: held streams and timed requests
# ==========================================================================


async def hold_stream(stack: AsyncExitStack, client: httpx.AsyncClient, url: str,
                      **headers: str) -> httpx.Response:
    """Open a streaming request and keep it open on `stack`.

    Reads ONE body chunk before returning, so the caller knows the request
    was admitted, routed, and committed -- a permit is provably held -- and
    not merely queued somewhere in httpx.

    The body iterator is parked on the stack too, not dropped. httpx closes
    the response the moment its `aiter_raw()` generator is collected, which
    a bare `await response.aiter_raw().__anext__()` does on the next line --
    and a closed response is a client disconnect, which releases the very
    permit this helper exists to hold. (Found by `peak_open_streams == 1`
    with eight streams "held".)
    """
    response = await stack.enter_async_context(
        client.stream("POST", url, json=body(), headers=headers)
    )
    if response.status_code == 200:
        chunks = response.aiter_raw()
        stack.push_async_callback(chunks.aclose)
        await chunks.__anext__()
    return response


async def timed_ok(client: httpx.AsyncClient, url: str, **headers: str) -> tuple[int, float]:
    """One complete request; returns (status, seconds)."""
    started = time.perf_counter()
    response = await client.post(url, json=body(), headers=headers)
    await response.aread()
    return response.status_code, time.perf_counter() - started


# ==========================================================================
# tenant_isolation_under_saturation
# ==========================================================================


async def test_tenant_isolation_under_saturation(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """FAILURE-MODES row 6 from outside: tenant B does not feel tenant A.

    Tenant A opens its cap of eight slow streams and then fires fifty more.
    The fifty are refused with `concurrency_rejected` -- the CLASS matters,
    because it is the one that carries no `Retry-After` (waiting does not
    help; finishing does) and charges no rate token (C6). Tenant B then
    sends twenty ordinary requests, and every one succeeds at the latency B
    saw before A arrived.

    THE MEASUREMENT AND ITS TOLERANCE. B's twenty are timed sequentially
    twice: once with A idle (the baseline), once with A's eight streams open
    and its fifty refusals just processed. The assertion is on the median,
    because the median is what a tenant experiences and the max is what the
    laptop's scheduler experiences: `saturated_median <= 3 * idle_median +
    30 ms`. Three-times-plus-thirty is deliberately loose against noise and
    deliberately tight against the failure it exists to catch, which is not
    "a bit slower" but "B's requests queue behind A's" -- the shape of a
    shared bucket, a global semaphore or a body read in front of admission,
    any of which puts B's median in the hundreds of milliseconds while A's
    streams drip. The max is bounded too, at half a second, as a sanity
    check that no single B request was parked.

    "No token charged" is asserted via the probe, to the hundredth: A's
    bucket started at `burst`, eight streams cost eight tokens, and fifty
    refusals cost nothing. The rate is 0.001/s so refill over the test is
    below the tolerance.
    """
    idle = [await timed_ok(client, gateway.url("ab"), **auth("b"), **fake("ok"))
            for _ in range(20)]
    assert all(status == 200 for status, _ in idle)
    idle_median = statistics.median(t for _, t in idle)

    async with AsyncExitStack() as stack:
        held = [await hold_stream(stack, client, gateway.url("ab"), **auth("a"), **SLOW)
                for _ in range(A_CAP)]
        assert [r.status_code for r in held] == [200] * A_CAP
        assert all(r.headers["x-gw-tenant"] == "a" for r in held)

        refused = 0
        for _ in range(OVERFLOW):
            response = await client.post(gateway.url("ab"), json=body(),
                                         headers={**auth("a"), **SLOW})
            assert response.status_code == 429, response.text
            assert response.json()["error"]["type"] == "concurrency_rejected"
            assert "retry-after" not in response.headers, (
                "a concurrency denial invented a Retry-After; there is no honest number"
            )
            assert response.headers["x-gw-tenant"] == "a"
            assert response.headers["x-gw-attempts"] == "0"
            refused += 1
        assert refused == OVERFLOW

        saturated = [
            await timed_ok(client, gateway.url("ab"), **auth("b"), **fake("ok"))
            for _ in range(20)
        ]
        assert all(status == 200 for status, _ in saturated), [s for s, _ in saturated]
        saturated_median = statistics.median(t for _, t in saturated)
        saturated_max = max(t for _, t in saturated)
        print(f"\n[isolation] B median idle={idle_median * 1e3:.1f}ms "
              f"saturated={saturated_median * 1e3:.1f}ms max={saturated_max * 1e3:.1f}ms")
        assert saturated_median <= 3 * idle_median + 0.030, (
            f"B's median went from {idle_median * 1e3:.1f}ms to "
            f"{saturated_median * 1e3:.1f}ms while A was saturated"
        )
        assert saturated_max < 0.5, f"a B request was parked for {saturated_max:.3f}s"

        probe = (await client.get(gateway.probe_url("ab", tenant="a"))).json()
        admission = probe["admission"]
        assert admission["tenant"] == "a"
        assert admission["in_use"] == A_CAP
        assert admission["denied"]["tenant_concurrency"] == OVERFLOW
        assert admission["denied"]["tenant_rate"] == 0
        assert abs(admission["tokens"] - (A_BURST - A_CAP)) < 0.05, (
            f"A's bucket reads {admission['tokens']}: a refusal charged a token (C6)"
        )
        b_probe = (await client.get(gateway.probe_url("ab", tenant="b"))).json()
        assert b_probe["admission"]["in_use"] == 0
        assert b_probe["admission"]["denied"] == {"tenant_concurrency": 0, "tenant_rate": 0}

    # The fifty never reached a provider: only the held eight and B's forty.
    assert fakes.stats()["total"] == A_CAP + 40


# ==========================================================================
# provider_key_concurrency_cap
# ==========================================================================


async def test_provider_key_concurrency_cap(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """FAILURE-MODES row 7: two tenants inside their own caps, one key.

    `x` and `y` may each run six. `key-x` and `key-y` are two catalog
    entries that present ONE credential with a cap of four, so twelve
    simultaneous streams -- every one of them admitted by its tenant -- must
    produce exactly four upstream streams and eight `provider_key_exhausted`
    refusals. The tenants' own denial counters stay at zero: it is the
    key's cap, and the response says so.

    The assertion that matters is the FAKE's `peak_open_streams`. The client
    can count its 200s, but a gateway that let a fifth stream through and
    then failed it would also show four 200s; only the upstream knows how
    many were open at once.
    """
    denials_before = (await client.get(gateway.probe_url("keyx"))).json()["denials"]

    async def one(tenant: str, workload: str) -> tuple[int, str]:
        headers = {**auth(tenant), **fake("slow-drip", interval="0.1", events="10")}
        async with client.stream("POST", gateway.url(workload), json=body(),
                                 headers=headers) as response:
            if response.status_code != 200:
                await response.aread()
                return response.status_code, response.json()["error"]["type"]
            async for _ in response.aiter_raw():
                pass
            return 200, "ok"

    results = await asyncio.gather(
        *[one("x", "keyx") for _ in range(6)], *[one("y", "keyy") for _ in range(6)]
    )
    statuses = sorted(s for s, _ in results)
    assert statuses.count(200) == SHARED_CAP, results
    assert statuses.count(429) == 12 - SHARED_CAP, results
    assert {kind for s, kind in results if s == 429} == {"provider_key_exhausted"}

    stats = fakes.stats()
    assert stats["peak_open_streams"] == SHARED_CAP, stats
    assert stats["open_streams"] == 0
    assert stats["total"] == SHARED_CAP, "a refused request reached the provider"

    probe = (await client.get(gateway.probe_url("keyx", tenant="x"))).json()
    assert probe["admission"]["denied"]["tenant_concurrency"] == 0, (
        "the key's cap was reported as the tenant's"
    )
    assert probe["provider_keys"]["caps"] == {SHARED_CREDENTIAL: SHARED_CAP}
    assert probe["provider_keys"]["in_use"] == {}
    delta = probe["denials"]["provider_key_concurrency"] - denials_before[
        "provider_key_concurrency"]
    assert delta == 12 - SHARED_CAP


# ==========================================================================
# breaker_single_probe
# ==========================================================================


async def test_breaker_single_probe(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """The breaker's whole argument, on a socket: one probe, the rest wait.

    Phase 1. Three 500s open the candidate's circuit (the `solo` workload has
    nowhere to fall back to, so each is passed through as the provider's own
    500). The fourth request is a 503 `breaker_open` with `X-Gw-Breaker:
    open` and a `Retry-After`, and the fake never sees it.

    Phase 2. The fake is switched to `ok` per request, the cooldown is
    waited out, and EIGHT requests are fired at once. Exactly one reaches the
    provider (`total == 1`) and succeeds; the other seven are refused with
    `X-Gw-Breaker: half_open` in microseconds, because a half-open circuit
    with its probe out admits nobody else. Letting all eight "see if it is
    back" is the stampede that took the provider down the first time.

    Phase 3. The probe's success closed the circuit: the next request is
    served, `X-Gw-Breaker` is absent, and the probe endpoint reads `closed`.
    """
    # Phase 1: trip.
    for i in range(BREAKER.failure_threshold):
        response = await client.post(gateway.url("solo"), json=body())
        assert response.status_code == 500, (i, response.text)
        assert "x-gw-breaker" not in response.headers, i
    response = await client.post(gateway.url("solo"), json=body())
    assert response.status_code == 503
    assert response.json()["error"]["type"] == "breaker_open"
    assert response.headers["x-gw-breaker"] == "open"
    assert response.headers["x-gw-attempts"] == "0", "an open circuit is not an attempt"
    assert int(response.headers["retry-after"]) >= 1, "the cooldown, rounded up"
    assert fakes.stats()["by_mode"] == {"5xx": BREAKER.failure_threshold}
    probe = (await client.get(gateway.probe_url("solo"))).json()
    assert probe["breakers"][0]["target"]["state"] == "open"
    assert probe["breakers"][0]["credential"]["state"] == "closed"

    # Phase 2: recover, with a stampede.
    await asyncio.sleep(BREAKER.cooldown + 0.05)
    fakes.reset_stats()
    healthy = fake("ok", interval="0.05")

    async def one() -> tuple[int, str | None, str]:
        response = await client.post(gateway.url("solo"), json=body(), headers=healthy)
        return (response.status_code, response.headers.get("x-gw-breaker"),
                response.headers["x-gw-served-by"])

    results = await asyncio.gather(*[one() for _ in range(8)])
    served = [r for r in results if r[0] == 200]
    refused = [r for r in results if r[0] == 503]
    assert len(served) == 1, results
    assert len(refused) == 7, results
    assert served[0][1] is None, "the probe itself was not refused by anyone"
    assert served[0][2] == f"{CANDIDATE}/{CANDIDATE_MODEL}"
    assert {r[1] for r in refused} == {"half_open"}
    assert {r[2] for r in refused} == {"-"}
    assert fakes.stats()["total"] == 1, "more than one probe reached the provider"

    # Phase 3: traffic returns.
    response = await client.post(gateway.url("solo"), json=body(), headers=healthy)
    assert response.status_code == 200
    assert "x-gw-breaker" not in response.headers
    assert fakes.stats()["total"] == 2
    probe = (await client.get(gateway.probe_url("solo"))).json()
    assert probe["breakers"][0]["target"]["state"] == "closed"
    assert probe["breakers"][0]["target"]["failures_in_window"] == 0


# ==========================================================================
# breaker_ignores_client_cancel
# ==========================================================================


async def test_breaker_ignores_client_cancel(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """C8 on a socket. Ten clients hang up mid-answer on a healthy candidate.

    Each stream is opened, one chunk is read, and the connection is dropped.
    The server sees `http.disconnect`, cancels the worker, and the executor's
    `finally` RELEASES the ticket -- it does not record anything, because
    there is no disposition to record. Ten of those leave the circuit CLOSED
    with zero failures in the window and no transition, and the next
    ordinary request is served by the candidate with no `X-Gw-Breaker`.
    """
    key = f"{CANDIDATE}/{CANDIDATE_MODEL}"
    before = gateway.gate_state()["breakers"].get(key, {}).get("transitions", 0)

    for _ in range(10):
        async with client.stream("POST", gateway.url("ab"), json=body(),
                                 headers=fake("ok", interval="0.05", events="30")) as r:
            assert r.status_code == 200
            await r.aiter_raw().__anext__()
            # Leaving the block with the body unread closes the connection.
    await _settled(gateway)

    snap = gateway.gate_state()["breakers"][key]
    assert snap["state"] == "closed"
    assert snap["failures_in_window"] == 0
    assert snap["transitions"] == before, "a client hang-up moved the circuit"
    assert snap["probes_in_flight"] == 0

    response = await client.post(gateway.url("ab"), json=body(), headers=fake("ok"))
    assert response.status_code == 200
    assert "x-gw-breaker" not in response.headers
    assert response.headers["x-gw-served-by"] == key
    assert fakes.stats()["open_streams"] == 0


# ==========================================================================
# breaker_open_falls_back
# ==========================================================================


async def test_breaker_open_falls_back(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """The breaker's payoff for a two-target plan: the incumbent, at once.

    Three requests fall back the slow way -- candidate 500, then incumbent
    -- and `X-Gw-Attempts: 2` says so. The fourth finds the candidate's
    circuit open and is served by the incumbent with `X-Gw-Attempts: 1`:
    the open circuit is NOT an upstream attempt, because nothing was sent
    and no provider was measured, and a header that counted it would report
    the candidate as asked when the whole point was not to ask. What the
    header carries instead is `X-Gw-Breaker: open`, which is the only way a
    client can tell "one target" from "one target because the other is
    down". The fake confirms the candidate was never opened.
    """
    for _ in range(BREAKER.failure_threshold):
        response = await client.post(gateway.url("ab"), json=body())
        assert response.status_code == 200
        assert response.headers["x-gw-attempts"] == "2"
        assert "x-gw-breaker" not in response.headers
    assert fakes.stats()["by_mode"] == {"5xx": BREAKER.failure_threshold,
                                        "ok": BREAKER.failure_threshold}

    fakes.reset_stats()
    response = await client.post(gateway.url("ab"), json=body())
    assert response.status_code == 200
    assert response.headers["x-gw-attempts"] == "1", "the open circuit was counted"
    assert response.headers["x-gw-breaker"] == "open"
    assert response.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert response.text.endswith("data: [DONE]\n\n")
    stats = fakes.stats()
    assert stats["by_mode"] == {"ok": 1}, stats
    assert "5xx" not in stats["by_mode"], "the candidate was opened past an open circuit"


# ==========================================================================
# Admission before the body
# ==========================================================================


async def _partial_post(gateway: GatewayServer, tenant: str, *, declared: int,
                        sent: int, wait: float) -> tuple[bytes, float]:
    """A POST that declares `declared` body bytes and sends `sent` of them.

    Returns whatever the server wrote back within `wait` seconds, and how
    long the first byte of it took. Raw socket, because httpx will not send
    a body it does not have.
    """
    head = (
        f"POST /workloads/ab{ROUTE} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{gateway.port}\r\n"
        f"Authorization: Bearer {TOKENS[tenant]}\r\n"
        "Content-Type: application/json\r\n"
        "Connection: close\r\n"
        f"Content-Length: {declared}\r\n\r\n"
    ).encode()
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.port)
    try:
        started = time.perf_counter()
        writer.write(head + b"{" + b" " * (sent - 1))
        await writer.drain()
        try:
            first = await asyncio.wait_for(reader.read(65536), timeout=wait)
        except TimeoutError:
            return b"", time.perf_counter() - started
        elapsed = time.perf_counter() - started
        rest = b""
        with contextlib.suppress(Exception):
            rest = await asyncio.wait_for(reader.read(65536), timeout=0.5)
        return first + rest, elapsed
    finally:
        writer.transport.abort()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def test_a_tenant_at_its_cap_is_refused_before_its_body_is_read(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """C6 for memory, not just for tokens.

    Tenant `c` has one permit and is holding it. It then sends a request
    that DECLARES three megabytes and delivers sixty-four bytes. If admission
    ran after the body read, the server would sit waiting for the other
    three megabytes (until the workload's 8 s total), and the socket would
    stay silent. It does not: the 429 arrives in milliseconds, with the body
    unread, because the permit check happens before `read_request_body`.

    The control is the same partial request with the permit FREE: the server
    admits it and waits for the body it was promised, so nothing comes back
    inside half a second. Without the control, "fast 429" could also mean
    "the body read is fast on loopback", and it is.
    """
    async with AsyncExitStack() as stack:
        held = await hold_stream(stack, client, gateway.url("ab"), **auth("c"), **SLOW)
        assert held.status_code == 200

        raw, elapsed = await _partial_post(gateway, "c", declared=3 * 1024 * 1024,
                                           sent=64, wait=2.0)
        assert raw.startswith(b"HTTP/1.1 429"), raw[:120]
        assert b"concurrency_rejected" in raw
        assert elapsed < 0.5, f"the refusal took {elapsed:.3f}s: the body was read first"

    # Control: permit free, same partial body -> the server waits for it.
    # (Free means RELEASED: the held stream's disconnect takes the server a
    # few loop turns to notice, and a control sent before that gets the
    # same 429 the test is trying to contrast against.)
    await _settled(gateway)
    raw, elapsed = await _partial_post(gateway, "c", declared=3 * 1024 * 1024,
                                       sent=64, wait=0.5)
    assert raw == b"", f"the server answered a body it had not received: {raw[:120]!r}"
    await _settled(gateway)
    assert fakes.stats()["total"] == 1, "the held stream, and nothing else"


# ==========================================================================
# Rate denial, and the tenant boundary itself
# ==========================================================================


async def test_a_rate_denial_carries_the_exact_wait_and_a_concurrency_denial_none(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    """The two 429s are two classes because the client's remedy differs."""
    first = await client.post(gateway.url("ab"), json=body(),
                              headers={**auth("ratey"), **fake("ok")})
    assert first.status_code == 200
    second = await client.post(gateway.url("ab"), json=body(),
                               headers={**auth("ratey"), **fake("ok")})
    assert second.status_code == 429
    assert second.json()["error"]["type"] == "admission_rejected"
    # (1 - tokens) / 0.5 is just under two seconds; C5 rounds UP.
    assert second.headers["retry-after"] in {"1", "2"}
    assert second.headers["x-gw-tenant"] == "ratey"


async def test_an_unknown_token_is_a_401_that_names_nothing(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """The token is a secret. The refusal does not echo it, does not name a
    tenant, and costs no provider anything. And it is a 401, not a guest
    admission: a rotated-out key must not fall through to `anonymous`."""
    bogus = "tok-does-not-exist-4e1a"
    response = await client.post(gateway.url("ab"), json=body(),
                                 headers={"authorization": f"Bearer {bogus}"})
    assert response.status_code == 401
    assert response.json()["error"]["type"] == "unauthenticated"
    assert bogus not in response.text
    assert bogus[:8] not in response.text
    assert response.headers["www-authenticate"] == "Bearer"
    assert "x-gw-tenant" not in response.headers
    assert response.headers["x-gw-attempts"] == "0"
    assert fakes.stats()["total"] == 0


async def test_no_token_is_the_anonymous_tenant_when_the_file_configures_one(
    gateway: GatewayServer, client: httpx.AsyncClient
):
    response = await client.post(gateway.url("solo"), json=body(), headers=fake("ok"))
    assert response.status_code == 200
    assert response.headers["x-gw-tenant"] == "anonymous"


# ==========================================================================
# probe shape
# ==========================================================================


async def test_the_probe_reports_the_gate_without_calling_anything(
    gateway: GatewayServer, client: httpx.AsyncClient, fakes: Fakes
):
    """Every value the gate would consult, from the objects it would consult,
    in the vocabularies `metrics.py` will label them with -- and no socket.

    The label check is the one with teeth for P5: every denial reason and
    every breaker state the probe can emit must already be a value the
    registry will accept, or the first incident produces a metric the
    exporter refuses to record.
    """
    response = await client.get(gateway.probe_url("ab", tenant="b"))
    assert response.status_code == 200
    probe = response.json()

    assert probe["upstream_called"] is False
    assert probe["tenant_mode"] == "table"
    assert [b["served_by"] for b in probe["breakers"]] == [
        f"{CANDIDATE}/{CANDIDATE_MODEL}", f"{INCUMBENT}/{INCUMBENT_MODEL}"
    ]
    for entry in probe["breakers"]:
        for circuit in ("target", "credential"):
            snap = entry[circuit]
            assert snap["state"] in metrics.BREAKER_STATES, snap
            assert snap["state_gauge"] in (0, 1, 2)
            assert {"failures_in_window", "failure_threshold", "probes_in_flight",
                    "epoch", "transitions", "key"} <= set(snap)
        # The credential circuit is keyed on the credential alone -- no
        # provider entry -- so a key shared by two entries is one circuit.
        # Rendered "cred/<credential_id>".
        assert entry["credential"]["key"].startswith("cred/")
    assert probe["breaker_policy"] == {
        "failure_threshold": BREAKER.failure_threshold, "window": BREAKER.window,
        "cooldown": BREAKER.cooldown, "half_open_probes": BREAKER.half_open_probes,
    }

    admission = probe["admission"]
    assert admission["tenant"] == "b"
    assert admission["configured"] is True
    assert admission["limits"] == {"rate_per_second": 100.0, "burst": 200,
                                   "max_concurrency": B_CAP}
    assert admission["in_use"] == 0
    assert set(admission["denied"]) <= set(metrics.DENIAL_REASONS)

    assert set(probe["denials"]) <= set(metrics.DENIAL_REASONS)
    assert set(probe["denials"]) >= {"tenant_rate", "tenant_concurrency",
                                     "provider_key_concurrency"}
    assert probe["provider_keys"]["in_use"] == {}
    assert probe["provider_keys"]["caps"] == {CANDIDATE: 64, INCUMBENT: 64}

    # A tenant the file does not know is reported as such, not invented.
    unknown = (await client.get(gateway.probe_url("ab", tenant="nobody"))).json()
    assert unknown["admission"] == {"tenant": "nobody", "configured": False,
                                    "limits": None, "in_use": 0, "tokens": None,
                                    "denied": {}}
    # A bearer token on the probe resolves the way it would on a request.
    mine = (await client.get(gateway.probe_url("ab"), headers=auth("x"))).json()
    assert mine["admission"]["tenant"] == "x"

    assert fakes.stats()["total"] == 0, "the probe opened an upstream connection"


def test_every_x_gw_header_value_is_safe_to_echo():
    """`X-Gw-Tenant` is an id; nothing in `TOKENS` may ever be one."""
    assert not (set(TOKENS.values()) & {"a", "b", "c", "x", "y", "ratey", "anonymous"})
    assert all(json.dumps(t) for t in TOKENS.values())

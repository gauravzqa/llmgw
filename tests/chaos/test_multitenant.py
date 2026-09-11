"""Tier 3, the P4 gates: many tenants, many targets, breakers that really trip.

`test_invariants.py` is single-tenant and runs every gateway under
`BREAKER_NEVER_TRIPS`, so its resource invariants are clean of admission and of
circuit state by construction. This file is the other half: a gateway with a
real tenant table (some tenants saturable, some generous), a two-target
workload whose candidate fails/stalls/dies on demand, and the shipped breaker
SHAPE with small numbers (three failures open a circuit, a 300 ms cooldown), so
circuits open under load and recover after cooldown inside one iteration.

The invariant set is the union of `test_invariants.py`'s and
`tests/contract/test_isolation.py`'s, asserted after EVERY iteration, from
outside the process and off the server's own loop:

* `admission.total_in_use() == 0`   -- no tenant permit leaked (row 19)
* `limiter.total_in_use() == 0`     -- no provider-key permit leaked (row 19)
* every breaker `probes_in_flight == 0` -- no half-open ticket leaked, which
  is the breaker's own version of row 19: a probe slot that never comes back
  leaves a circuit untestable forever
* `upstream.in_flight() == {}` and the fake's `open_streams == 0` (rows 5, 18)
* the server's loop task count is back to its baseline (row 5)
* elapsed under the total deadline plus cleanup slack
* exactly one terminal outcome is derivable by the client

The point is composition, not any single gate. A permit is taken at ingress, a
ticket is taken twice per attempt at the loop, a pool connection is taken
inside the attempt, and a client can hang up at any instant across all three.
The single-gate tests each hold one of these fixed; here they move at once.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from contextlib import AsyncExitStack

import httpx
import pytest

from llmgw.breaker import BreakerPolicy
from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets
from llmgw.server.app import build_app as build_gateway
from llmgw.server.config import ServerConfig
from tests.chaos.conftest import (
    GatewayServer,
    _serve,
    fake_mode,
    rng_for,
    scaled,
)

pytestmark = pytest.mark.chaos

# The shipped breaker shape with small numbers, exactly as
# `tests/contract/test_isolation.py` runs it: three failures open a circuit,
# the cooldown is 300 ms, so a trip-probe-recover cycle costs under half a
# second of wall clock rather than ten.
BREAKER = BreakerPolicy(failure_threshold=3, window=30.0, cooldown=0.3,
                        half_open_probes=1)

KEY_ENV = "LLMGW_MT_KEY"

CANDIDATE = "mt-candidate"
INCUMBENT = "mt-incumbent"
CANDIDATE_MODEL = "mt.candidate"
INCUMBENT_MODEL = "mt.incumbent"

# Small provider-key caps so the limiter trips too, not just the tenant cap.
PROVIDER_CAP = 3

FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval",
                "x-fake-delay", "x-fake-status")

# One low total so the deadline is asserted by firing, not by waiting it out.
TOTAL_BUDGET = 2.0
# Generous against a laptop scheduler that is also running the rest of the
# chaos tier; a genuine deadline blowout is many seconds, not this margin.
DEADLINE_SLACK = 2.5

TENANTS = """
# hot: two permits, so a third concurrent stream is a concurrency denial.
[tenants.hot]
tokens = ["tok-hot"]
rate_per_second = 100.0
burst = 100
max_concurrency = 2

# warm: room for a handful.
[tenants.warm]
tokens = ["tok-warm"]
rate_per_second = 100.0
burst = 100
max_concurrency = 6

# ratey: one token every two seconds, so the rate denial path is reachable.
[tenants.ratey]
tokens = ["tok-ratey"]
rate_per_second = 0.5
burst = 1
max_concurrency = 8

# anonymous: generous, so a request with no token is refused by a circuit or a
# key cap under test, never by admission.
[tenants.anonymous]
rate_per_second = 100.0
burst = 400
max_concurrency = 64
"""

# default_workload is the two-target one, so a plain POST routes there and a
# candidate failure has an incumbent to fall back to. `max_attempts = 2` gives
# one repetition on top of the fallback -- the amplification bound is
# `len(targets) + (max_attempts - 1) == 3`.
POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 2.0
connect = 0.5
first_event = 0.5
progress = 0.35
client_stall = 0.8

[workloads.ab]
incumbent = "mt.incumbent"
candidate = "mt.candidate"

  [workloads.ab.retry]
  max_attempts = 2
  base_delay = 0.01
  max_delay = 0.02
"""

TOKENS = {"hot": "tok-hot", "warm": "tok-warm", "ratey": "tok-ratey"}


def _auth(tenant: str) -> dict[str, str]:
    # `X-Gw-Workload: ab` names the two-target workload so the plan keeps its
    # fallback. Without it a body that names a model routes single-target (the
    # P3 O1 rule: a client that names a model but no workload gets no fallback),
    # and this file is specifically about the fallback path.
    base = {"x-gw-workload": "ab"}
    if tenant in TOKENS:
        base["authorization"] = f"Bearer {TOKENS[tenant]}"
    return base


def _body(*, stream: bool = True) -> dict:
    return {"model": CANDIDATE_MODEL, "stream": stream, "max_tokens": 64,
            "messages": [{"role": "user", "content": "chaos"}]}


def _catalog(fakes, candidate: dict[str, str]) -> Catalog:
    """Two targets on the OpenAI fake's port. The candidate's baked mode is
    `candidate`; the incumbent is always healthy. Distinct `api_model`s so the
    per-target rewrite runs on every fallback (the fake ignores the field)."""

    def conn(pid: str, extra: dict[str, str]) -> ProviderConn:
        return ProviderConn(
            id=pid, kind="openai", base_url=fakes.openai.base_url,
            api_key_env=KEY_ENV, extra_headers=dict(extra),
            max_concurrency=PROVIDER_CAP,
        )

    def spec(mid: str, pid: str, api_model: str) -> ModelSpec:
        return ModelSpec(id=mid, provider=pid, api_model=api_model, input_per_m=1.0,
                         output_per_m=2.0, priced_at="2026-09-09")

    return Catalog(
        providers={CANDIDATE: conn(CANDIDATE, candidate),
                   INCUMBENT: conn(INCUMBENT, fake_mode("ok"))},
        models={CANDIDATE_MODEL: spec(CANDIDATE_MODEL, CANDIDATE, "wire-candidate"),
                INCUMBENT_MODEL: spec(INCUMBENT_MODEL, INCUMBENT, "wire-incumbent")},
    )


@pytest.fixture(scope="session")
def mt_gateways(fakes, tmp_path_factory):
    """One multi-tenant gateway per candidate mode, built lazily and cached.

    Session-scoped and never reset between iterations, which is the whole
    point of a return-to-baseline tier: a fresh process cannot fail a leak
    assertion. The tenant table, the policy and the tripping breaker are the
    same across every mode; only the candidate's baked behaviour differs.
    """
    os.environ.setdefault(KEY_ENV, "sk-mt-not-a-real-key")
    root = tmp_path_factory.mktemp("mtpolicy")
    (root / "workloads.toml").write_text(POLICY, encoding="utf-8")
    (root / "tenants.toml").write_text(TENANTS, encoding="utf-8")
    servers: dict[str, GatewayServer] = {}

    def get(candidate_mode: str) -> GatewayServer:
        if candidate_mode not in servers:
            config = ServerConfig(
                catalog=_catalog(fakes, fake_mode(candidate_mode)),
                fake_upstreams=True,
                policy_file=str(root / "workloads.toml"),
                tenants_file=str(root / "tenants.toml"),
                forward_request_headers=FAKE_HEADERS,
                breaker=BREAKER,
                budgets=Budgets(total=TOTAL_BUDGET, connect=0.5, first_event=0.5,
                                progress=0.35, client_stall=0.8),
            )
            servers[candidate_mode] = _serve(build_gateway(config))
        return servers[candidate_mode]

    try:
        yield get
    finally:
        for server in servers.values():
            server.stop()


# --------------------------------------------------------------------------
# Reading the gate off the server's own loop
# --------------------------------------------------------------------------


def _gate_state(server: GatewayServer) -> dict:
    """admission, limiter and every breaker, sampled on the loop that owns
    them. A cross-thread read of a dict the server is mutating is a
    `RuntimeError` waiting for a busy moment, so it is marshalled."""
    gw = server.app.state.gateway

    async def _read() -> dict:
        return {
            "tenant_permits": gw.admission.total_in_use(),
            "key_permits": gw.limiter.total_in_use(),
            "probes": sum(b["probes_in_flight"]
                          for b in gw.breakers.snapshot().values()),
            "breakers": {"/".join(k): v["state"]
                         for k, v in gw.breakers.snapshot().items()},
        }

    return asyncio.run_coroutine_threadsafe(_read(), server.loop).result(5.0)


async def _settle(server: GatewayServer, fakes, *, within: float = 3.0) -> dict:
    """Poll until the whole gate is idle, or return what it looked like at the
    deadline. A cancelled stream takes a few loop turns to unwind."""
    deadline = time.monotonic() + within
    while True:
        state = _gate_state(server)
        idle = (state["tenant_permits"] == 0 and state["key_permits"] == 0
                and state["probes"] == 0
                and not server.upstream.in_flight()
                and fakes.open_streams() == 0)
        if idle or time.monotonic() > deadline:
            return state
        await asyncio.sleep(0.02)


def _assert_idle(server: GatewayServer, fakes, state: dict, where: str,
                 baseline: int) -> None:
    assert state["tenant_permits"] == 0, f"{where}: tenant permits leaked: {state}"
    assert state["key_permits"] == 0, f"{where}: provider-key permits leaked: {state}"
    assert state["probes"] == 0, f"{where}: half-open probe slot leaked: {state}"
    assert server.upstream.in_flight() == {}, f"{where}: upstream still open"
    assert fakes.open_streams() == 0, f"{where}: the provider is still writing"
    assert server.task_count() <= baseline, (
        f"{where}: {server.task_count()} tasks against a baseline of {baseline}"
    )
    assert server.pool_size() <= 2 * PROVIDER_CAP + 2, f"{where}: the pool grew"


# --------------------------------------------------------------------------
# One request, one client behaviour
# --------------------------------------------------------------------------


async def _drive(client: httpx.AsyncClient, server: GatewayServer, *,
                 tenant: str, behaviour: str, streaming: bool,
                 heal: bool) -> tuple[str, int]:
    """Fire one request and classify what the client got. `heal` overrides the
    candidate's baked mode with `ok` (the client header wins over the
    provider's extra_headers), which is how a failing candidate is made to
    succeed on ONE gateway so its circuit can recover."""
    headers = dict(_auth(tenant))
    if heal:
        headers["x-fake-mode"] = "ok"
        headers["x-fake-interval"] = "0.02"
    body = _body(stream=streaming)
    try:
        async with client.stream("POST", server.url("openai"), json=body,
                                 headers=headers) as response:
            status = response.status_code
            if behaviour == "never-read":
                await asyncio.sleep(0.8 + 0.3)
            try:
                async for chunk in response.aiter_raw():
                    if behaviour == "disconnect" and chunk:
                        return ("client-closed", status)
                    if behaviour == "slow":
                        await asyncio.sleep(0.01)
            except httpx.HTTPError:
                return ("truncated", status)
    except httpx.HTTPError:
        return ("truncated", 0)
    if status == 200:
        return ("complete", status)
    return ("error", status)


CANDIDATE_MODES = ("5xx", "5xx", "ok", "stall-after-headers", "die-mid-stream")
BEHAVIOURS = ("read", "read", "slow", "disconnect", "never-read")
TENANTS_POOL = ("hot", "warm", "ratey", "anonymous")


async def test_multi_tenant_multi_target_breaker_chaos(
    mt_gateways, fakes, chaos_seed: int, iterations: int
):
    """Every P4 gate at once, with the invariants outside the switch.

    Per iteration a candidate mode picks the gateway (so a fault is baked, not
    forwarded, and the incumbent stays a real fallback), a tenant and a client
    behaviour are chosen, and -- half the time -- a couple of streams are held
    on a DIFFERENT tenant so some tenant is saturated while the request under
    test runs. Circuits open on the failing gateways and are refused with
    `X-Gw-Breaker`; the healthy incumbent absorbs the fallback. After every
    iteration the whole gate must read zero.
    """
    rng = rng_for(chaos_seed, "multitenant")
    baselines: dict[str, int] = {}
    saturators = AsyncExitStack()
    limits = httpx.Limits(max_connections=64, max_keepalive_connections=16)
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0), limits=limits) as client, \
            saturators:
        for i in range(scaled(iterations, 12, share=0.6)):
            mode = rng.choice(CANDIDATE_MODES)
            server = mt_gateways(mode)
            tenant = rng.choice(TENANTS_POOL)
            behaviour = rng.choice(BEHAVIOURS)
            streaming = rng.random() > 0.2
            heal = mode != "ok" and rng.random() > 0.6
            where = (f"seed={chaos_seed} i={i} cand={mode} tenant={tenant} "
                     f"{behaviour} stream={streaming} heal={heal}")

            if mode not in baselines:
                with contextlib.suppress(httpx.HTTPError):
                    await client.post(server.url("openai"),
                                      json=_body(stream=False), headers=_auth("warm"))
                await _settle(server, fakes)
                baselines[mode] = server.task_count()

            # Half the time, saturate one tenant with a couple of held streams
            # on the healthy ("ok") gateway, so its permits are genuinely in
            # use while the request under test runs against a maybe-failing one.
            held: list = []
            if rng.random() > 0.6:
                sat_server = mt_gateways("ok")
                sat_tenant = rng.choice(["hot", "warm"])
                for _ in range(rng.randint(1, 2)):
                    with contextlib.suppress(httpx.HTTPError):
                        r = await saturators.enter_async_context(
                            client.stream("POST", sat_server.url("openai"),
                                          json=_body(),
                                          headers={**_auth(sat_tenant),
                                                   **fake_mode("slow-drip",
                                                               interval="0.1",
                                                               events="30")})
                        )
                        if r.status_code == 200:
                            chunks = r.aiter_raw()
                            saturators.push_async_callback(chunks.aclose)
                            with contextlib.suppress(StopAsyncIteration,
                                                     httpx.HTTPError):
                                await chunks.__anext__()
                        held.append(r)

            started = time.monotonic()
            outcome, status = await _drive(
                client, server, tenant=tenant, behaviour=behaviour,
                streaming=streaming, heal=heal,
            )
            elapsed = time.monotonic() - started

            assert outcome in {"complete", "truncated", "error", "client-closed"}, where
            if outcome == "error":
                # Admission (429), breaker-open with no fallback (503), or a
                # passed-through upstream status -- always a real error object.
                assert status >= 400, where
            assert elapsed < TOTAL_BUDGET + DEADLINE_SLACK, (
                f"{where}: {elapsed:.2f}s against a {TOTAL_BUDGET}s total"
            )

            # Drop the saturating streams before asserting return-to-zero.
            await saturators.aclose()
            saturators = AsyncExitStack()
            await saturators.__aenter__()

            state = await _settle(server, fakes)
            _assert_idle(server, fakes, state, where, baselines[mode])
            # The saturator gateway (if different) must also be clean.
            sat = await _settle(mt_gateways("ok"), fakes)
            assert sat["tenant_permits"] == 0, f"{where}: saturator tenant leak: {sat}"
            assert sat["key_permits"] == 0, f"{where}: saturator key leak: {sat}"


async def test_a_candidate_circuit_opens_mid_run_and_recovers_after_cooldown(
    mt_gateways, fakes, chaos_seed: int
):
    """The one scenario the loop above cannot pin deterministically: a circuit
    that OPENS on a burst of failures, REFUSES with `X-Gw-Breaker` while the
    incumbent absorbs the traffic, and CLOSES once a healed probe succeeds
    after the cooldown -- with the gate back to zero at every step.

    On the `5xx` gateway the candidate fails until its circuit opens; a
    `heal` request (client `x-fake-mode: ok` overriding the baked `5xx`) makes
    the candidate answer, which is how the half-open probe succeeds.
    """
    server = mt_gateways("5xx")
    # Warm and heal any state a previous test left on this shared server.
    async with httpx.AsyncClient(timeout=httpx.Timeout(20.0)) as client:
        # Heal first: wait out any open circuit and close it with ok probes, so
        # the assertions below start from a known-closed candidate.
        for _ in range(12):
            state = _gate_state(server)
            cand_open = any(v != "closed" and k.startswith(CANDIDATE)
                            for k, v in state["breakers"].items())
            if not cand_open:
                break
            await asyncio.sleep(BREAKER.cooldown + 0.05)
            await client.post(server.url("openai"), json=_body(stream=False),
                              headers={**_auth("warm"), "x-fake-mode": "ok",
                                       "x-fake-interval": "0.02"})
        await _settle(server, fakes)

        # Phase 1: fail until the candidate circuit is open. `X-Gw-No-Retry`
        # disables the repetition (C5 keeps the fallback), so the candidate is
        # tried exactly once per request and the accounting stays clean: before
        # the circuit opens a failing request is candidate-then-incumbent
        # (attempts == 2), and once it opens the candidate is refused before a
        # socket, so the next one is incumbent-only with `X-Gw-Breaker: open`
        # and `X-Gw-Attempts: 1` -- the open circuit is not an attempt.
        no_retry = {**_auth("warm"), "x-gw-no-retry": "1"}
        saw_open = False
        for n in range(BREAKER.failure_threshold + 3):
            r = await client.post(server.url("openai"), json=_body(stream=False),
                                  headers=no_retry)
            assert r.status_code == 200, (n, r.text[:200])
            assert r.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}", (
                n, dict(r.headers))
            if r.headers.get("x-gw-breaker") == "open":
                assert r.headers["x-gw-attempts"] == "1", (
                    "an open circuit was counted as an attempt"
                )
                saw_open = True
                break
            assert r.headers["x-gw-attempts"] == "2", (n, dict(r.headers))
        assert saw_open, f"seed={chaos_seed}: the candidate circuit never opened"
        await _settle(server, fakes)

        # Phase 2: recover. Wait the cooldown, then a healed request drives the
        # single probe, which succeeds and closes the circuit.
        await asyncio.sleep(BREAKER.cooldown + 0.05)
        r = await client.post(server.url("openai"), json=_body(stream=False),
                              headers={**_auth("warm"), "x-fake-mode": "ok",
                                       "x-fake-interval": "0.02"})
        assert r.status_code == 200
        assert r.headers["x-gw-served-by"] == f"{CANDIDATE}/{CANDIDATE_MODEL}", (
            "the probe did not reach the candidate"
        )
        assert "x-gw-breaker" not in r.headers, "the circuit was still refusing"

        state = await _settle(server, fakes)
        assert state["probes"] == 0, f"a probe slot leaked: {state}"
        assert state["tenant_permits"] == 0 and state["key_permits"] == 0, state
        cand_state = state["breakers"].get(f"{CANDIDATE}/{CANDIDATE_MODEL}")
        assert cand_state == "closed", f"the probe's success did not close: {state}"

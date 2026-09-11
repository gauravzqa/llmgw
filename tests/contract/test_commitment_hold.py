"""The status-commitment hold, attacked from a raw socket.

`test_fallback.py` proves the P3 decision on the happy fallback: the candidate
sends headers and no body, the incumbent answers, and exactly one status line
reaches the client. This file is the adversarial half. Every test here asks the
same question in a harder position:

    is there ANY way to get two status lines onto one connection?

Raw sockets throughout, because httpx cannot answer it. It parses one response
and hands back an object, so a gateway that wrote a 200 for the candidate and
then a 500 for the incumbent looks identical from up there while the client's
actual experience -- a second response spliced onto the first -- is precisely
the thing C1's P3 decision exists to make impossible.

The positions attacked:

    both targets fail            (the status is the second failure's, once)
    the buffered path            (`stream: false` starts its response from
                                  inside the sink, one write later)
    concurrent fallbacks         (a poolful at once, sharing one client)
    a fallback racing a hang-up  (the client leaves during the window where
                                  nothing has been promised)

Plus the other half of a snapshot's job: FAILURE-MODES row 10 says a request
is routed and priced by ONE policy version. `PolicyStore.replace()` under
concurrent load is the only way to find out whether that is true of the code or
only of the comment, so the last section hammers it.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re

import httpx
import pytest

from llmgw.policy import PolicySnapshot
from llmgw.server.config import ServerConfig
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes
from tests.contract.test_fallback import (
    CANDIDATE,
    CANDIDATE_MODEL,
    INCUMBENT,
    INCUMBENT_MODEL,
    KEY_ENV,
    ROUTE,
    GatewayServer,
    _serve,
    body,
    mode,
    two_target_catalog,
)

pytestmark = pytest.mark.contract

STATUS_LINE = re.compile(rb"HTTP/1\.[01] \d\d\d")


# ==========================================================================
# One gateway, two targets, both healthy unless a test says otherwise
# ==========================================================================

POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 6.0
connect = 1.0
first_event = 1.0
progress = 1.0
client_stall = 4.0

[workloads.ab]
incumbent = "fake.incumbent"
candidate = "fake.candidate"
"""

PINNED_TO_CANDIDATE = """
default_workload = "pinned"
[defaults.budgets]
total = 6.0
connect = 1.0
first_event = 1.0
progress = 1.0
client_stall = 4.0
[workloads.pinned]
incumbent = "fake.candidate"
"""

PINNED_TO_INCUMBENT = PINNED_TO_CANDIDATE.replace(
    'incumbent = "fake.candidate"', 'incumbent = "fake.incumbent"'
)


@pytest.fixture(scope="module")
def policy_path(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("hold") / "workloads.toml"
    path.write_text(POLICY, encoding="utf-8")
    return str(path)


@pytest.fixture(scope="module")
def hold_gateways(fakes: Fakes, policy_path: str):
    """One gateway per candidate/incumbent mode pair, cached.

    Same shape as `test_fallback.py`'s: the behaviour of a target lives in its
    `ProviderConn.extra_headers`, so it is baked into a catalog and a catalog
    is baked into a running server.
    """
    import os

    os.environ.setdefault(KEY_ENV, "sk-hold-not-a-real-key")
    servers: dict[str, GatewayServer] = {}

    def get(candidate: dict[str, str], incumbent: dict[str, str]) -> GatewayServer:
        key = json.dumps([candidate, incumbent], sort_keys=True)
        if key not in servers:
            config = ServerConfig(
                catalog=two_target_catalog(fakes, candidate, incumbent),
                fake_upstreams=True,
                policy_file=policy_path,
                # A stalling candidate is a FAILURE per attempt, and this
                # module's tests share the server. See `BREAKER_NEVER_TRIPS`.
                breaker=BREAKER_NEVER_TRIPS,
            )
            servers[key] = _serve(config_app(config))
        return servers[key]

    try:
        yield get
    finally:
        for server in servers.values():
            server.stop()


def config_app(config: ServerConfig):
    from llmgw.server.app import build_app

    return build_app(config)


async def raw_exchange(
    gateway: GatewayServer, path: str, payload: dict, *, wait: float = 15.0
) -> bytes:
    """One request over a raw socket; everything the server wrote, verbatim."""
    encoded = json.dumps(payload).encode()
    request = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{gateway.port}\r\n"
        "Content-Type: application/json\r\n"
        "Connection: close\r\n"
        f"Content-Length: {len(encoded)}\r\n\r\n"
    ).encode() + encoded
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.port)
    try:
        writer.write(request)
        await writer.drain()
        return await asyncio.wait_for(reader.read(), timeout=wait)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


def one_status_line(raw: bytes) -> bytes:
    """Assert exactly one status line and return it."""
    found = STATUS_LINE.findall(raw)
    assert len(found) == 1, (
        f"{len(found)} status lines reached the client: {found!r}\n"
        f"{raw[:400]!r}"
    )
    return found[0]


# ==========================================================================
# Both targets fail
# ==========================================================================


async def test_a_fallback_whose_second_target_also_fails_writes_one_status_line(
    hold_gateways, fakes: Fakes
):
    """The candidate gets as far as headers, the incumbent answers 502.

    This is the position with the most ways to go wrong. The candidate reached
    the point P2 would have committed at, the incumbent produced a real status
    that C4 says passes through, and the executor has to have kept `started`
    false across both -- otherwise the 502 is written into a response that
    already carries a 200.
    """
    gateway = hold_gateways(mode("stall-after-headers", delay="3"),
                            mode("5xx", status="502"))
    raw = await raw_exchange(gateway, f"/workloads/ab{ROUTE}", body())

    status = one_status_line(raw)
    assert status == b"HTTP/1.1 502", raw[:200]
    head = raw.split(b"\r\n\r\n", 1)[0].lower()
    assert b"x-gw-attempts: 2" in head, head
    # C4: the provider's own body, not one we improved on.
    assert b'"server_error"' in raw or b"error" in raw.split(b"\r\n\r\n", 1)[1]
    assert fakes.stats()["by_mode"] == {"stall-after-headers": 1, "5xx": 1}


async def test_every_target_failing_after_headers_still_writes_one_status_line(
    hold_gateways, fakes: Fakes
):
    """Both targets reach the P2 commitment point and neither produces a byte.

    The client should see one error, chosen by `_final_error` from the second
    attempt, and no trace of the first. A gateway that committed on headers
    would have written a 200 here and then had nothing to put in it.
    """
    gateway = hold_gateways(mode("stall-after-headers", delay="3"),
                            mode("stall-after-headers", delay="3"))
    raw = await raw_exchange(gateway, f"/workloads/ab{ROUTE}", body())

    status = one_status_line(raw)
    assert status.startswith(b"HTTP/1.1 5"), raw[:200]
    head = raw.split(b"\r\n\r\n", 1)[0].lower()
    assert b"x-gw-attempts: 2" in head, head
    # `X-Gw-Served-By` is documented as "the target that ACTUALLY answered".
    # Nobody did, so it is the `-` its own formatter has a branch for -- and
    # not `plan.primary`, which is the first target while the error the client
    # is holding came from the second.
    assert b"x-gw-served-by: -" in head, (
        "a target that never served anything was credited with the answer"
    )
    assert fakes.stats()["total"] == 2


# ==========================================================================
# The buffered path
# ==========================================================================


async def test_a_buffered_fallback_writes_one_status_line_and_one_length(
    hold_gateways, fakes: Fakes
):
    """`stream: false` sends its status from INSIDE the sink, one write later.

    That is a second place a status can come from, which makes it a second
    place two can come from. It also has a window the streaming path does not:
    `_Commitment.open()` returns a `BufferedSink` that has not sent anything
    yet, so "committed" and "on the wire" are genuinely different instants
    here. Neither may produce a second response.
    """
    gateway = hold_gateways(mode("5xx"), mode("ok"))
    raw = await raw_exchange(gateway, f"/workloads/ab{ROUTE}",
                             body(stream=False))

    assert one_status_line(raw) == b"HTTP/1.1 200"
    head = raw.split(b"\r\n\r\n", 1)[0].lower()
    assert head.count(b"content-length:") == 1, head
    assert b"x-gw-attempts: 2" in head
    assert f"x-gw-served-by: {INCUMBENT}/{INCUMBENT_MODEL}".encode() in head
    assert fakes.stats()["by_mode"] == {"5xx": 1, "ok": 1}


async def test_a_buffered_fallback_after_headers_writes_one_status_line(
    hold_gateways, fakes: Fakes
):
    """The buffered path's version of the case P3 exists for."""
    gateway = hold_gateways(mode("stall-after-headers", delay="3"), mode("ok"))
    raw = await raw_exchange(gateway, f"/workloads/ab{ROUTE}",
                             body(stream=False))

    assert one_status_line(raw) == b"HTTP/1.1 200"
    assert fakes.stats()["by_mode"] == {"stall-after-headers": 1, "ok": 1}


async def test_a_buffered_failure_at_every_target_writes_one_status_line(
    hold_gateways
):
    gateway = hold_gateways(mode("5xx", status="503"), mode("5xx", status="500"))
    raw = await raw_exchange(gateway, f"/workloads/ab{ROUTE}",
                             body(stream=False))

    status = one_status_line(raw)
    assert status == b"HTTP/1.1 500", "the LAST target's status, not the first's"


# ==========================================================================
# Concurrency
# ==========================================================================


async def test_concurrent_fallbacks_each_write_exactly_one_status_line(
    hold_gateways, fakes: Fakes
):
    """Eight requests, sixteen upstream attempts, one pool.

    The failure this looks for is a shared `_Commitment` or a shared
    `Exchange`: per-request state that ended up on an object built once. It
    would present as a request whose status line carries another request's
    `X-Gw-Served-By`, or as a connection that got two.
    """
    gateway = hold_gateways(mode("stall-after-headers", delay="3"), mode("ok"))
    raws = await asyncio.gather(*[
        raw_exchange(gateway, f"/workloads/ab{ROUTE}", body())
        for _ in range(8)
    ])

    for raw in raws:
        assert one_status_line(raw) == b"HTTP/1.1 200"
        head = raw.split(b"\r\n\r\n", 1)[0].lower()
        assert b"x-gw-attempts: 2" in head, head
        assert f"x-gw-served-by: {INCUMBENT}/{INCUMBENT_MODEL}".encode() in head
        assert b"data: [DONE]" in raw

    stats = fakes.stats()
    assert stats["by_mode"]["ok"] == 8, stats["by_mode"]
    # `<=` and not `==`, and the reason is worth recording: the candidate's
    # provider has `max_concurrency=8`, and the pool wait lives INSIDE the
    # connect phase. Push more concurrent requests than the pool holds and the
    # surplus times out queueing -- an attempt the executor counts and the
    # provider never sees. `X-Gw-Attempts` is therefore an upper bound on
    # upstream requests, never an under-count, which is the safe direction for
    # an amplification metric.
    assert 8 <= stats["total"] <= 16, stats["by_mode"]
    assert stats["open_streams"] == 0, "an upstream stream outlived its request"
    assert gateway.app.state.gateway.upstream.in_flight() == {}


async def test_a_client_that_hangs_up_inside_the_fallback_window_gets_nothing(
    hold_gateways, fakes: Fakes
):
    """The race the P3 decision creates and P2 did not have.

    Between `Upstream.open()` returning and the first upstream body byte, the
    client has been promised NOTHING -- no status, no headers. If it hangs up
    in that window there is no response to finish and no second one to write,
    and the only observable that can testify is the upstream: the connection
    must come back.

    Asserted on the server's own state rather than on the client's, because
    the client has, by construction, gone.
    """
    gateway = hold_gateways(mode("stall-after-headers", delay="3"), mode("ok"))
    payload = json.dumps(body()).encode()
    request = (
        f"POST /workloads/ab{ROUTE} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{gateway.port}\r\n"
        "Content-Type: application/json\r\n"
        "Connection: close\r\n"
        f"Content-Length: {len(payload)}\r\n\r\n"
    ).encode() + payload

    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.port)
    writer.write(request)
    await writer.drain()
    # Long enough to be inside the candidate's stall and nowhere near its
    # 1.0 s first-event budget, so the hang-up lands in the window where the
    # plan is open and nothing has been said.
    await asyncio.sleep(0.3)
    writer.transport.abort()
    with contextlib.suppress(Exception):
        await writer.wait_closed()
    del reader

    upstream = gateway.app.state.gateway.upstream
    for _ in range(200):
        if upstream.in_flight() == {} and fakes.stats()["open_streams"] == 0:
            break
        await asyncio.sleep(0.02)
    assert upstream.in_flight() == {}, "the upstream outlived the client"
    assert fakes.stats()["open_streams"] == 0
    # And the incumbent was never reached: a disconnect is not a reason to
    # shop the request around a second provider (C8).
    assert "ok" not in fakes.stats()["by_mode"], fakes.stats()["by_mode"]


# ==========================================================================
# FAILURE-MODES row 10: one policy version per request, under concurrency
# ==========================================================================


@pytest.fixture(scope="module")
def pinned_gateway(fakes: Fakes, tmp_path_factory):
    """A gateway whose policy document routes every request to ONE target.

    Two snapshots are built from it below -- one pinned to the candidate, one
    to the incumbent -- so that a request which observed a reload mid-flight
    would be visible as a `X-Gw-Policy-Id` that disagrees with its
    `X-Gw-Served-By`. Without that disagreement being observable, "the snapshot
    is pinned" is a claim no test can fail.
    """
    import os

    os.environ.setdefault(KEY_ENV, "sk-hold-not-a-real-key")
    path = tmp_path_factory.mktemp("pinned") / "workloads.toml"
    path.write_text(PINNED_TO_CANDIDATE, encoding="utf-8")
    config = ServerConfig(
        catalog=two_target_catalog(fakes, mode("ok"), mode("ok")),
        fake_upstreams=True,
        policy_file=str(path),
        breaker=BREAKER_NEVER_TRIPS,
    )
    server = _serve(config_app(config))
    try:
        yield server
    finally:
        server.stop()


def snapshots(gateway: GatewayServer) -> tuple[PolicySnapshot, PolicySnapshot]:
    catalog = gateway.app.state.gateway.config.catalog
    a = PolicySnapshot.from_toml(PINNED_TO_CANDIDATE, catalog=catalog)
    b = PolicySnapshot.from_toml(PINNED_TO_INCUMBENT, catalog=catalog)
    assert a.id != b.id, "the two snapshots hash the same; the test proves nothing"
    return a, b


async def test_a_reload_under_load_never_splits_a_request_between_two_policies(
    pinned_gateway: GatewayServer
):
    """Row 10, proved under concurrency rather than sequentially.

    Requests are launched in a staggered stream while `PolicyStore.replace()`
    swaps the snapshot out from under them, so some of them span a reload. Each
    response is then checked for INTERNAL consistency: the policy id it reports
    must be the policy that chose the target it reports. A gateway that re-read
    `current()` anywhere below ingress would produce a response routed by one
    version and labelled by the other, and no capture record written from it
    could ever say which was which.

    The requests name their workload in the PATH on purpose. With no workload
    named the body's `model` pins the target directly, the workload's routing
    is never consulted, and the test would pass against a gateway that ignored
    policy altogether.
    """
    store = pinned_gateway.app.state.gateway.policy
    a, b = snapshots(pinned_gateway)
    expected = {a.id: f"{CANDIDATE}/{CANDIDATE_MODEL}",
                b.id: f"{INCUMBENT}/{INCUMBENT_MODEL}"}
    store.replace(a)
    url = f"{pinned_gateway.base_url}/workloads/pinned{ROUTE}"

    stop = {"now": False}
    swaps = {"n": 0}

    async def churn() -> None:
        while not stop["now"]:
            store.replace(b if swaps["n"] % 2 == 0 else a)
            swaps["n"] += 1
            await asyncio.sleep(0.007)

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        churner = asyncio.ensure_future(churn())

        async def one(delay: float):
            await asyncio.sleep(delay)
            return await client.post(url, json=body(stream=False))

        try:
            responses = await asyncio.gather(
                *[one(i * 0.004) for i in range(40)]
            )
        finally:
            stop["now"] = True
            await churner

    assert swaps["n"] > 5, f"only {swaps['n']} reloads landed; the race never ran"
    seen: set[str] = set()
    for response in responses:
        assert response.status_code == 200
        policy_id = response.headers["x-gw-policy-id"]
        seen.add(policy_id)
        assert policy_id in expected, policy_id
        assert response.headers["x-gw-served-by"] == expected[policy_id], (
            f"routed to {response.headers['x-gw-served-by']} and labelled "
            f"{policy_id}: the request observed two policy versions"
        )
        assert response.headers["x-gw-workload-id"] == "pinned"
    assert len(seen) == 2, (
        f"every response reported one policy id ({seen}); no request spanned a "
        "reload and the test proves nothing"
    )


async def test_the_error_path_quotes_the_same_snapshot_as_the_success_path(
    hold_gateways
):
    """Every exit quotes the ids, and they are the same ids.

    Four different exits from `PassthroughEndpoint.__call__`: a 200 from the
    fallback, an upstream error passed through, a 400 for an unknown workload,
    and the 413 that has its own handler. The 413 path is the one worth
    naming: it calls `send_json_error` rather than `send_error`, which is a
    second writer of response headers and therefore a second chance to quote a
    different snapshot.
    """
    gateway = hold_gateways(mode("5xx"), mode("ok"))
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        ok = await client.post(f"{gateway.base_url}/workloads/ab{ROUTE}",
                               json=body(stream=False))
        unknown = await client.post(f"{gateway.base_url}/workloads/nope{ROUTE}",
                                    json=body(stream=False))
        huge = await client.post(
            f"{gateway.base_url}/workloads/ab{ROUTE}",
            content=json.dumps({**body(stream=False),
                                "pad": "x" * (5 * 1024 * 1024)}).encode(),
            headers={"content-type": "application/json"},
        )
        probe = await client.get(f"{gateway.base_url}/workloads/ab/probe")

    assert ok.status_code == 200
    assert unknown.status_code == 400
    assert huge.status_code == 413

    ids = {
        "ok": (ok.headers["x-gw-policy-id"], ok.headers["x-gw-catalog-id"]),
        "unknown": (unknown.headers["x-gw-policy-id"],
                    unknown.headers["x-gw-catalog-id"]),
        "413": (huge.headers["x-gw-policy-id"], huge.headers["x-gw-catalog-id"]),
        "probe": (probe.json()["policy_id"], probe.json()["catalog_id"]),
    }
    assert len(set(ids.values())) == 1, ids
    # The 400 still names the workload the caller asked for, not the default it
    # never mentioned.
    assert unknown.headers["x-gw-workload-id"] == "nope"


# ==========================================================================
# Where C3 and C8 meet: the accounting hook on the disconnect path
# ==========================================================================


async def test_a_client_disconnect_reaches_the_accounting_hook_exactly_once(
    hold_gateways, monkeypatch: pytest.MonkeyPatch
):
    """The guarantee, where a P3 verification test used to pin the gap.

    C3 says an interrupted stream still bills the tokens it generated, and
    `Executor.execute()` returns its failures rather than raising them
    precisely so that the partial usage survives. C8 says cancellation is not
    a result: `run_until_disconnect` cancels the executor task and raises
    `ClientDisconnected`. Put together, the most common interruption there is
    -- a client hanging up mid-answer -- produced no accounting record at all,
    while a provider-side truncation of the identical stream produced one.

    Now `_record` is the executor's `on_finish`, called from its `finally` in
    the worker task before the cancellation is allowed out. So on a disconnect
    the hook is reached exactly once, with a result whose `outcome` is
    CANCELED, whose `attempts` are what had happened (the candidate's 5xx and
    the incumbent's cut-short stream), whose `committed` says the client had
    a status, and whose `pump` carries the usage generated so far. The client
    still sees the connection close and nothing else.
    """
    from llmgw.errors import Outcome
    from llmgw.server.app import PassthroughEndpoint

    seen: list[object] = []
    original = PassthroughEndpoint._record

    # P5 gives `_record` the request's `Exchange` and elapsed time as
    # keyword-only arguments (the terminal record carries the tenant onto a
    # capture line and observes the duration histogram). The stub forwards
    # whatever the hook is called with, so this test still pins the one thing
    # it exists to pin -- that the hook is reached exactly once, disconnect
    # included -- without caring how it is called.
    def record(self, result, **kwargs):
        seen.append(result)
        return original(self, result, **kwargs)

    monkeypatch.setattr(PassthroughEndpoint, "_record", record)
    # The incumbent drips, so there is a real mid-stream instant to hang up in:
    # against a fake that answers in one write the client's disconnect always
    # lands after the response is finished, and the test would prove nothing.
    gateway = hold_gateways(mode("5xx"),
                            mode("slow-drip", interval="0.05", events="20"))
    url = f"{gateway.base_url}/workloads/ab{ROUTE}"

    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        # A completed stream records, and it took a fallback to get there.
        async with client.stream("POST", url, json=body()) as response:
            assert response.status_code == 200
            async for _ in response.aiter_raw():
                pass
    await asyncio.sleep(0.05)
    assert len(seen) == 1, "a completed request did not reach the hook"
    completed = seen[0]
    assert completed.outcome is Outcome.COMPLETED  # type: ignore[attr-defined]
    assert len(completed.attempts) == 2  # type: ignore[attr-defined]

    seen.clear()
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as client:
        with contextlib.suppress(httpx.HTTPError):
            async with client.stream("POST", url, json=body()) as response:
                assert response.status_code == 200
                assert response.headers["x-gw-attempts"] == "2"
                async for chunk in response.aiter_raw():
                    if chunk:
                        break  # hang up mid-answer
    for _ in range(100):
        if seen:
            break
        await asyncio.sleep(0.02)
    assert len(seen) == 1, (
        f"the disconnect path reached the accounting hook {len(seen)} times; "
        "the guarantee is exactly once"
    )
    canceled = seen[0]
    assert canceled.outcome is Outcome.CANCELED  # type: ignore[attr-defined]
    assert canceled.error is None  # type: ignore[attr-defined]
    assert canceled.committed is True  # type: ignore[attr-defined]
    # What had happened: the candidate's real failure, then the incumbent's
    # stream, cut short. Two records, agreeing with the `X-Gw-Attempts: 2` the
    # client was sent at commitment.
    outcomes = [a.outcome for a in canceled.attempts]  # type: ignore[attr-defined]
    assert outcomes == ["upstream_server_error", "canceled"], outcomes
    assert canceled.served_by is not None  # type: ignore[attr-defined]
    assert canceled.served_by.provider.id == INCUMBENT  # type: ignore[attr-defined]
    # C3: the partial usage survived the cancellation.
    pump = canceled.pump  # type: ignore[attr-defined]
    assert pump is not None
    assert pump.committed is True
    assert pump.bytes_out > 0 and pump.events > 0
    assert pump.terminal_seen is False, "the client hung up before the end"
    # And the upstream did not outlive the client.
    upstream = gateway.app.state.gateway.upstream
    for _ in range(200):
        if upstream.in_flight() == {}:
            break
        await asyncio.sleep(0.02)
    assert upstream.in_flight() == {}

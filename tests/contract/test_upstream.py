"""The transport over real sockets, against the hostile upstreams.

Everything here is deliberately un-mocked. The unit tier proves the mapping
table and the byte handling; only this tier can prove the things that live
below `httpx.MockTransport`: that a truncated chunked body really arrives as
`UpstreamDisconnected`, that four sequential requests really share one TCP
connection, and that a cancelled consumer really gives the socket back.

The whole file targets a few seconds. The stall modes are asserted with SHORT
budgets against a much longer upstream silence -- proving "we gave up after
300 ms while the upstream intended to stay quiet for five seconds" is the same
proof as waiting out the stall and it is sixteen times cheaper.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest
from fakes import wire
from fakes.upstream import PATHS

from llmgw import errors as E
from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets, Deadline, SystemClock
from llmgw.upstream import Upstream, UpstreamRequest
from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract

KEY_ENV = "LLMGW_CONTRACT_KEY"
KEY = "sk-contract-not-a-real-key"

BODY = b'{"model":"fake-echo","stream":true,"messages":[{"role":"user","content":"hi"}]}'

SURFACES = ("openai", "anthropic")


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(KEY_ENV, KEY)


@pytest.fixture
def catalog(fakes: Fakes) -> Catalog:
    """A two-provider catalog pointed at the fake ports.

    Built here rather than in conftest because it is this module's business:
    the redirect is one `replace()` on a base URL and no code path in the
    gateway knows it happened, which is the property that makes this tier test
    the same transport production uses.
    """
    providers = {
        "fake-openai": ProviderConn(
            id="fake-openai", kind="openai", base_url=fakes.openai.base_url,
            api_key_env=KEY_ENV, max_concurrency=8,
        ),
        "fake-anthropic": ProviderConn(
            id="fake-anthropic", kind="anthropic", base_url=fakes.anthropic.base_url,
            api_key_env=KEY_ENV, max_concurrency=8,
        ),
    }
    models = {
        f"m-{surface}": ModelSpec(
            id=f"m-{surface}", provider=f"fake-{surface}", api_model="fake-echo",
            input_per_m=1.0, output_per_m=2.0, priced_at="2026-09-09",
        )
        for surface in SURFACES
    }
    return Catalog(models=models, providers=providers)


@pytest.fixture
async def up(catalog: Catalog):
    upstream = Upstream(catalog)
    try:
        yield upstream
    finally:
        await upstream.aclose()


def request_for(catalog: Catalog, surface: str, **modes: str) -> UpstreamRequest:
    return UpstreamRequest(
        target=catalog.resolve(f"m-{surface}"),
        body=BODY,
        path=PATHS[surface],
        stream=True,
        extra_headers=modes,
    )


def budgets(total: float = 10.0, **kw) -> Budgets:
    return Budgets(total=total, **kw)


def deadline(total: float = 10.0) -> Deadline:
    return Deadline(SystemClock(), total)


async def drain(up: Upstream, req: UpstreamRequest, *, total: float = 10.0, **bud):
    async with up.open(req, deadline=deadline(total), budgets=budgets(total, **bud)) as s:
        chunks = [c async for c in s.aiter_raw()]
        return s, b"".join(chunks)


# ------------------------------------------------------------------ ok mode


@pytest.mark.parametrize("surface", SURFACES)
async def test_ok_delivers_the_canonical_wire_bytes_unchanged_on_both_surfaces(
    up: Upstream, catalog: Catalog, surface: str
):
    """Concatenated `aiter_raw()` output must EQUAL what the fake sent, not
    merely parse to the same events. The gateway's entire claim on the
    streaming path is that it moves bytes it does not understand; any
    normalisation here is a difference the customer's SDK gets to discover."""
    stream, body = await drain(up, request_for(catalog, surface, **{"X-Fake-Mode": "ok"}))
    expected = wire.joined(
        wire.anthropic_stream() if surface == "anthropic" else wire.openai_stream()
    )
    assert stream.status == 200
    assert body == expected
    assert stream.body_modified is False


@pytest.mark.parametrize("surface", SURFACES)
async def test_the_fake_receives_the_credential_in_the_shape_its_surface_expects(
    up: Upstream, catalog: Catalog, surface: str
):
    """The fake does not check auth, so this asserts the request was accepted
    and counted -- the negative version ('was the incumbent opened?') is the
    assertion the whole fallback tier is built on."""
    await drain(up, request_for(catalog, surface, **{"X-Fake-Mode": "ok"}))
    stats = _fake_stats(catalog, surface)
    assert stats["by_path"][PATHS[surface]] == 1


def _fake_stats(catalog: Catalog, surface: str) -> dict:
    base = catalog.providers[f"fake-{surface}"].base_url
    return httpx.get(f"{base}/__stats", timeout=5.0).json()


# --------------------------------------------------------------- status codes


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_5xx_raises_upstream_server_error_with_the_body_preserved(
    up: Upstream, catalog: Catalog, surface: str
):
    req = request_for(catalog, surface, **{"X-Fake-Mode": "5xx", "X-Fake-Status": "500"})
    with pytest.raises(E.UpstreamServerError) as ei:
        await drain(up, req)
    err = ei.value
    assert err.upstream_status == 500
    assert err.passthrough is True
    assert err.provider == f"fake-{surface}"
    assert err.model == f"m-{surface}"
    assert b"server_error" in (err.upstream_body or b"")


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_429_carries_the_retry_after_the_provider_actually_sent(
    up: Upstream, catalog: Catalog, surface: str
):
    """`retry.py` treats Retry-After as a FLOOR. Dropping it means backing off
    less than the provider explicitly asked for, at the moment it is telling
    you it cannot take more."""
    req = request_for(catalog, surface, **{"X-Fake-Mode": "429", "X-Fake-Delay": "4"})
    with pytest.raises(E.RateLimited) as ei:
        await drain(up, req)
    assert ei.value.retry_after == 4.0
    assert ei.value.health is E.Health.NEUTRAL
    assert ei.value.retry_same is True


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_schema_400_is_an_invalid_request_that_may_still_try_the_next_target(
    up: Upstream, catalog: Catalog, surface: str
):
    """The row people are surprised by: not retryable here, genuinely eligible
    elsewhere, because the next target's schema may differ."""
    req = request_for(catalog, surface, **{"X-Fake-Mode": "schema-400"})
    with pytest.raises(E.InvalidRequest) as ei:
        await drain(up, req)
    assert ei.value.retry_same is False
    assert ei.value.try_next is True
    assert ei.value.health is E.Health.NEUTRAL
    assert ei.value.blame is E.Blame.CLIENT


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_529_is_an_overload_signal_and_not_a_generic_5xx(
    up: Upstream, catalog: Catalog, surface: str
):
    """Anthropic's status, served on both ports on purpose: the classifier
    works on the integer, and a branch that has only ever met 529 on one
    surface is an untested branch."""
    req = request_for(catalog, surface, **{"X-Fake-Mode": "529"})
    with pytest.raises(E.UpstreamOverloaded) as ei:
        await drain(up, req)
    assert ei.value.upstream_status == 529
    assert ei.value.status == 503


async def test_no_upstream_stream_is_left_open_after_a_status_failure(
    up: Upstream, catalog: Catalog
):
    for mode in ("5xx", "429", "529", "schema-400"):
        with pytest.raises(E.GatewayError):
            await drain(up, request_for(catalog, "openai", **{"X-Fake-Mode": mode}))
    assert up.in_flight() == {}


# ------------------------------------------------------------------ timeouts


@pytest.mark.parametrize("surface", SURFACES)
async def test_a_stall_before_headers_breaches_the_headers_budget_not_the_wall_clock(
    up: Upstream, catalog: Catalog, surface: str
):
    """Asserted by timing out fast against a five-second silence, so the test
    costs 300 ms rather than five seconds. It also proves the deadline is the
    only clock: httpx has no timeout of its own that could have fired here."""
    req = request_for(catalog, surface,
                      **{"X-Fake-Mode": "stall-before-headers", "X-Fake-Delay": "5"})
    started = asyncio.get_running_loop().time()
    with pytest.raises(E.HeadersTimeout) as ei:
        # `headers` is the status-line budget since PLAN-2 B6; `connect` is
        # TCP+TLS only and never fires against a listening fake.
        await drain(up, req, total=5.0, connect=0.3, headers=0.3)
    elapsed = asyncio.get_running_loop().time() - started
    assert elapsed < 2.0, f"gave up after {elapsed:.2f}s, budget was 0.3s"
    assert ei.value.provider == f"fake-{surface}"
    assert up.in_flight() == {}


@pytest.mark.parametrize("surface", SURFACES)
async def test_headers_then_silence_breaches_the_first_event_budget(
    up: Upstream, catalog: Catalog, surface: str
):
    """`stall-after-headers` flushes a 200 and then says nothing. This is the
    case that must NOT be retryable on the same target: the request was
    accepted, so re-sending it pays twice for the same generation. Note that
    `HeadersTimeout` (the phase before this one) now agrees -- both refuse a
    same-target retry, and they stay separate classes because only one of them
    also refuses to blame the connection."""
    req = request_for(catalog, surface,
                      **{"X-Fake-Mode": "stall-after-headers", "X-Fake-Delay": "5"})
    with pytest.raises(E.FirstEventTimeout) as ei:
        await drain(up, req, total=5.0, first_event=0.3)
    assert ei.value.retry_same is False
    assert ei.value.try_next is True
    assert up.in_flight() == {}


# ---------------------------------------------------------------- disconnect


@pytest.mark.parametrize("surface", SURFACES)
async def test_dying_mid_stream_raises_upstream_disconnected_after_k_events_arrived(
    up: Upstream, catalog: Catalog, surface: str
):
    """The failure has to arrive DURING iteration, not at open(). A transport
    that reported this at open() would have had to buffer the whole body to
    find out -- which is the design this module exists to avoid -- and the pump
    would never learn that five events had already reached the client."""
    req = request_for(catalog, surface,
                      **{"X-Fake-Mode": "die-mid-stream", "X-Fake-Events": "5"})
    seen = bytearray()
    with pytest.raises(E.UpstreamDisconnected) as ei:
        async with up.open(req, deadline=deadline(), budgets=budgets()) as stream:
            async for chunk in stream.aiter_raw():
                seen += chunk
    assert seen, "nothing arrived before the upstream died"
    assert seen.count(b"data:") >= 5
    # No terminal marker: the body stopped, it did not end.
    assert b"[DONE]" not in seen and b"message_stop" not in seen
    assert ei.value.provider == f"fake-{surface}"
    assert ei.value.retry_same is True
    assert up.in_flight() == {}


# ------------------------------------------------------- pooling and leaks


async def test_sequential_requests_to_one_provider_share_a_single_connection(
    up: Upstream, catalog: Catalog
):
    """Four requests, one connection. The alternative -- a fresh TCP handshake
    per call -- adds a round trip to every request's p50 and leaves four
    sockets in TIME_WAIT for minutes afterwards, which is how a gateway runs
    out of file descriptors at a load it was sized for ten times over."""
    req = request_for(catalog, "openai", **{"X-Fake-Mode": "ok"})
    for _ in range(4):
        _, body = await drain(up, req)
        assert body.endswith(b"data: [DONE]\n\n")
    assert up.stats()["fake-openai"] == 1
    assert _fake_stats(catalog, "openai")["by_path"][PATHS["openai"]] == 4


async def test_two_providers_get_two_pools_and_stats_names_every_one(
    up: Upstream, catalog: Catalog
):
    for surface in SURFACES:
        await drain(up, request_for(catalog, surface, **{"X-Fake-Mode": "ok"}))
    stats = up.stats()
    assert stats == {"fake-openai": 1, "fake-anthropic": 1}


async def test_cancelling_mid_stream_closes_the_connection_and_returns_to_baseline(
    up: Upstream, catalog: Catalog
):
    """The leak that matters. A consumer cancelled halfway through a body
    cannot be trusted to clean up, and a leaked upstream connection is
    invisible until the pool is full and the gateway stops accepting work an
    hour later for no reason anyone can see in the logs."""
    baseline = up.stats()["fake-openai"]
    assert baseline == 0

    req = request_for(catalog, "openai", **{
        "X-Fake-Mode": "slow-drip", "X-Fake-Interval": "0.2", "X-Fake-Events": "50",
    })
    first_chunk = asyncio.Event()

    async def consume() -> None:
        async with up.open(req, deadline=deadline(30.0), budgets=budgets(30.0)) as stream:
            async for _ in stream.aiter_raw():
                first_chunk.set()

    task = asyncio.create_task(consume())
    await asyncio.wait_for(first_chunk.wait(), timeout=5.0)
    assert up.in_flight() == {"fake-openai": 1}

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert up.in_flight() == {}
    assert up.stats()["fake-openai"] == baseline


async def test_breaking_out_of_the_loop_leaks_nothing_either(
    up: Upstream, catalog: Catalog
):
    """`break` is not cancellation, and it is the far more common way a
    consumer walks away. Both have to end with the socket back in the pool."""
    req = request_for(catalog, "openai", **{
        "X-Fake-Mode": "slow-drip", "X-Fake-Interval": "0.1", "X-Fake-Events": "50",
    })
    async with up.open(req, deadline=deadline(), budgets=budgets()) as stream:
        async for _ in stream.aiter_raw():
            break
    assert up.in_flight() == {}
    assert up.stats()["fake-openai"] == 0


async def test_a_provider_pool_survives_a_failure_and_serves_the_next_request(
    up: Upstream, catalog: Catalog
):
    """A gateway whose pool is poisoned by one bad response fails the next
    request for a reason that has nothing to do with it."""
    with pytest.raises(E.UpstreamServerError):
        await drain(up, request_for(catalog, "openai", **{"X-Fake-Mode": "5xx"}))
    _, body = await drain(up, request_for(catalog, "openai", **{"X-Fake-Mode": "ok"}))
    assert body == wire.joined(wire.openai_stream())
    assert up.stats()["fake-openai"] == 1


async def test_split_frames_are_delivered_verbatim_across_arbitrary_write_boundaries(
    up: Upstream, catalog: Catalog
):
    """The fake cuts the body at seeded-random offsets, including inside a
    multi-byte UTF-8 sequence and inside the frame delimiter. This layer must
    not care: it never decodes and never re-frames, so the concatenation is
    still byte-identical."""
    req = request_for(catalog, "openai", **{"X-Fake-Mode": "split-frames"})
    _, body = await drain(up, req)
    assert body == wire.joined(wire.openai_stream())
    writes = _fake_stats(catalog, "openai")["writes_by_mode"]["split-frames"]
    assert writes > len(wire.openai_stream()), "the fake did not actually split"

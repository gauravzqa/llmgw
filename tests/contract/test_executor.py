"""The attempt loop over real sockets, against real hostile upstreams.

The unit tier proves the loop's arithmetic. Only this tier can prove the
claims that live below `httpx.MockTransport`, and the most important of them
is *negative*: "the incumbent was never opened" is a statement about the
upstream that the client cannot observe. A client holding a 200 from the
candidate cannot distinguish a gateway that never tried the incumbent from one
that tried it, got an answer, and threw it away -- and those are a correct
gateway and a gateway that double-bills every customer.

So every fallback test here asserts on `/__stats`.

--------------------------------------------------------------------------
Per-target modes
--------------------------------------------------------------------------

The fakes choose their behaviour from an `X-Fake-Mode` request header, and the
executor sends ONE `extra_headers` mapping to every target -- correctly, since
those are the client's headers and the client does not know our plan. So the
mode cannot ride on `extra_headers`.

It rides on `ProviderConn.extra_headers` instead, which `upstream.build_headers`
merges per provider. Two catalog entries with the same `base_url` and different
extra headers are two targets that reach the same port and get different
behaviour out of it -- which is exactly the shape of the real thing (a
candidate and an incumbent are two providers, and per-provider headers are
already how OpenRouter's attribution and Anthropic's version pinning work).

The counters then read cleanly: `by_mode` is a per-target open count, because
each target has its own mode.
"""

from __future__ import annotations

import time

import pytest
from fakes.upstream import PATHS

from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets, Deadline, SystemClock
from llmgw.errors import Outcome
from llmgw.executor import Executor
from llmgw.policy import ExecutionPlan
from llmgw.pump import Sink
from llmgw.retry import RetryPolicy
from llmgw.surfaces import OPENAI_CHAT
from llmgw.upstream import Upstream, UpstreamStream
from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract

KEY_ENV = "LLMGW_CONTRACT_KEY"
KEY = "sk-contract-not-a-real-key"
PATH = PATHS["openai"]
BODY = b'{"model":"fake-echo","stream":true,"messages":[{"role":"user","content":"hi"}]}'

CANDIDATE = "candidate"
INCUMBENT = "incumbent"


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(KEY_ENV, KEY)


# ================================================================== the rig


class ListSink:
    """Stands in for the ASGI sink. Keeps every byte the client would see."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    async def send(self, chunk: bytes) -> None:
        self.chunks.append(bytes(chunk))

    @property
    def body(self) -> bytes:
        return b"".join(self.chunks)


class Factory:
    """The status commitment, observable. `calls` is `http.response.start`."""

    def __init__(self) -> None:
        self.sink = ListSink()
        self.calls: list[int] = []

    async def __call__(self, stream: UpstreamStream) -> Sink:
        self.calls.append(stream.status)
        return self.sink


def catalog_for(fakes: Fakes, candidate_mode: str, incumbent_mode: str,
                **shared: str) -> Catalog:
    """Two targets on the same port, told to behave differently.

    `max_concurrency=8` rather than the default 64 so the pool this test builds
    is small enough that a leaked connection shows up as a hang rather than as
    nothing at all.
    """

    def conn(pid: str, mode: str) -> ProviderConn:
        return ProviderConn(
            id=pid,
            kind="openai",
            base_url=fakes.openai.base_url,
            api_key_env=KEY_ENV,
            extra_headers={"x-fake-mode": mode, **shared},
            max_concurrency=8,
        )

    providers = {
        CANDIDATE: conn(CANDIDATE, candidate_mode),
        INCUMBENT: conn(INCUMBENT, incumbent_mode),
    }
    models = {
        pid: ModelSpec(id=pid, provider=pid, api_model="fake-echo",
                       input_per_m=1.0, output_per_m=2.0, priced_at="2026-09-09")
        for pid in providers
    }
    return Catalog(models=models, providers=providers)


def make_budgets(**overrides) -> Budgets:
    defaults = dict(total=20.0, connect=2.0, first_event=5.0, progress=5.0,
                    client_stall=5.0)
    defaults.update(overrides)
    # PLAN-2 B4: the status-line wait is its own budget now; these rigs
    # reason about one 'connect' number, so headers follows it unless set.
    defaults.setdefault("headers", defaults["connect"])
    return Budgets(**defaults).validate()


async def run(
    fakes: Fakes,
    *,
    candidate: str,
    incumbent: str,
    budgets: Budgets | None = None,
    retry_policy: RetryPolicy | None = None,
    **shared: str,
):
    """One execution against the fakes. Returns (result, factory)."""
    budgets = budgets if budgets is not None else make_budgets()
    catalog = catalog_for(fakes, candidate, incumbent, **shared)
    plan = ExecutionPlan(
        policy_id="pol_contract",
        workload_id="chat",
        targets=(catalog.resolve(CANDIDATE), catalog.resolve(INCUMBENT)),
        budgets=budgets,
        retry=None,
    )
    clock = SystemClock()
    upstream = Upstream(catalog, clock=clock)
    factory = Factory()
    try:
        result = await Executor(upstream, clock=clock).execute(
            plan=plan,
            surface=OPENAI_CHAT,
            body=BODY,
            path=PATH,
            stream=True,
            deadline=Deadline(clock, budgets.total),
            sink_factory=factory,
            retry_policy=retry_policy,
        )
    finally:
        await upstream.aclose()
    return result, factory


def codes(result) -> list[str]:
    return [a.outcome for a in result.attempts]


# ========================================================= pre-commit fallback


async def test_a_real_five_hundred_falls_back_and_the_client_never_learns_of_it(
    fakes: Fakes,
):
    """`precommit_fallback_on_5xx` over a socket.

    Nothing had been written to the client when the candidate answered 500, so
    the fallback costs the client one round trip and no information. The stats
    are what prove both halves: the candidate really was asked, and the
    incumbent really was the one that answered.
    """
    result, factory = await run(fakes, candidate="5xx", incumbent="ok")

    assert result.error is None
    assert result.outcome is Outcome.COMPLETED
    assert result.served_by.provider.id == INCUMBENT
    assert codes(result) == ["upstream_server_error", "success"]
    assert factory.calls == [200], "the client saw exactly one status, and it was 200"
    assert factory.sink.body.endswith(b"data: [DONE]\n\n")

    stats = fakes.stats()
    assert stats["by_mode"] == {"5xx": 1, "ok": 1}
    assert stats["total"] == 2
    assert stats["by_path"] == {PATH: 2}


async def test_a_candidate_that_stalls_before_headers_falls_back(fakes: Fakes):
    """The other half of the pre-commitment window: the provider accepted the
    connection and then said nothing at all.

    Asserted with a SHORT connect budget against a much longer upstream
    silence. Proving "we gave up after 400 ms while the upstream intended to
    stay quiet for five seconds" is the same proof as waiting out the stall and
    it is an order of magnitude cheaper.
    """
    budgets = make_budgets(connect=0.4)
    started = time.monotonic()
    result, factory = await run(
        fakes, candidate="stall-before-headers", incumbent="ok",
        budgets=budgets, **{"x-fake-delay": "5"},
    )
    elapsed = time.monotonic() - started

    assert codes(result) == ["headers_timeout", "success"]
    assert result.served_by.provider.id == INCUMBENT
    assert elapsed < 4.0, "we timed out of the stall rather than sitting it out"
    assert factory.sink.body.endswith(b"data: [DONE]\n\n")

    stats = fakes.stats()
    assert stats["by_mode"] == {"stall-before-headers": 1, "ok": 1}
    assert stats["total"] == 2


async def test_a_four_two_nine_falls_back_without_sleeping_out_the_retry_after(
    fakes: Fakes,
):
    """C5 over a socket: the caller owns the retries, so we take zero -- but the
    incumbent is not a retry, and the fake's `Retry-After: 3` is never slept."""
    disabled = RetryPolicy(max_attempts=5, enabled=False)
    started = time.monotonic()
    result, factory = await run(
        fakes, candidate="429", incumbent="ok", retry_policy=disabled
    )
    elapsed = time.monotonic() - started

    assert codes(result) == ["rate_limited", "success"]
    assert elapsed < 2.0, "a disabled retry policy slept through a Retry-After"
    assert factory.calls == [200]

    stats = fakes.stats()
    assert stats["by_mode"] == {"429": 1, "ok": 1}


# ================================================== the commitment boundary


async def test_a_stream_that_dies_mid_flight_is_never_replaced(fakes: Fakes):
    """`no_fallback_after_commit`, over a socket, asserted at the upstream.

    The candidate wrote real frames and then dropped the connection. The client
    is holding a partial answer, so the incumbent must not be opened -- and the
    only party that can testify to that is the fake, because a client with a
    truncated 200 cannot tell "never tried" from "tried and discarded".
    """
    result, factory = await run(fakes, candidate="die-mid-stream", incumbent="ok")

    stats = fakes.stats()
    assert stats["total"] == 1, "a second upstream was opened after commitment"
    assert stats["by_mode"] == {"die-mid-stream": 1}
    assert "ok" not in stats["by_mode"], "the incumbent was never opened"

    assert len(result.attempts) == 1
    assert result.attempts[0].target.provider.id == CANDIDATE
    assert result.attempts[0].committed is True
    assert result.attempts[0].status == 200
    assert result.served_by is None
    assert result.committed is True
    assert result.outcome is Outcome.INTERRUPTED

    # C2: the client keeps what it was given and the body simply stops. No
    # synthesised terminal marker, no invented `event: error`.
    assert factory.calls == [200]
    assert factory.sink.body
    assert not factory.sink.body.endswith(b"data: [DONE]\n\n")
    assert result.pump is not None and result.pump.terminal_seen is False
    assert result.pump.content_events > 0, "C3: the tokens generated are billable"


async def test_a_healthy_candidate_means_the_incumbent_is_never_touched(
    fakes: Fakes,
):
    """The boring case, and the one the negative assertion is calibrated
    against: if `by_mode` did not distinguish the two targets, this test and
    the fallback tests above would pass for the wrong reason."""
    result, factory = await run(fakes, candidate="ok", incumbent="5xx")

    assert result.served_by.provider.id == CANDIDATE
    assert codes(result) == ["success"]
    stats = fakes.stats()
    assert stats["total"] == 1
    assert stats["by_mode"] == {"ok": 1}
    assert factory.sink.body.endswith(b"data: [DONE]\n\n")


async def test_an_in_stream_error_after_commitment_is_forwarded_not_replaced(
    fakes: Fakes,
):
    """HTTP said 200 and the protocol said otherwise, after we had already
    written frames. C2: the provider's own error frame is what the client gets,
    verbatim, and there is no second target."""
    result, factory = await run(fakes, candidate="error-in-stream", incumbent="ok")

    stats = fakes.stats()
    assert stats["total"] == 1
    assert "ok" not in stats["by_mode"]

    assert result.committed is True
    assert result.outcome is Outcome.INTERRUPTED
    assert result.served_by is None
    assert b"error" in factory.sink.body
    assert not factory.sink.body.endswith(b"data: [DONE]\n\n")


# ============================================================ retries, for real


async def test_a_retry_on_the_same_target_reopens_the_same_upstream(fakes: Fakes):
    """Both targets are broken in the same way, so every open is visible in the
    counters. `max_attempts=2` buys exactly one repetition for the whole
    request, and the plan supplies the rest -- three opens, not four."""
    policy = RetryPolicy(max_attempts=2, base_delay=0.001, max_delay=0.002)
    result, factory = await run(
        fakes, candidate="5xx", incumbent="5xx", retry_policy=policy
    )

    stats = fakes.stats()
    assert stats["by_mode"] == {"5xx": 3}
    assert codes(result) == ["upstream_server_error"] * 3
    assert [a.target.provider.id for a in result.attempts] == [
        CANDIDATE, CANDIDATE, INCUMBENT
    ]
    # Rule 8: the client gets the provider's own 502-worthy 500, not a summary.
    assert result.error.code == "upstream_server_error"
    assert result.error.upstream_status == 500
    assert result.error.passthrough is True
    assert factory.calls == [], "nothing was ever promised to the client"

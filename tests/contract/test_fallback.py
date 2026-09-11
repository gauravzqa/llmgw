"""Fallback over real sockets: the P3 decision, asserted from outside.

`tests/unit/test_executor.py` proves the loop's arithmetic and
`tests/contract/test_executor.py` proves it against real hostile upstreams.
Neither of them is an HTTP response. This file is the only place that can
answer the questions a customer would ask:

    did the client get one status line, or two?
    did the status say 200 before anything had agreed to answer?
    when the candidate died mid-answer, was a second provider billed for it?

The last one is the reason every test here also reads `/__stats`. A client
holding a truncated 200 cannot distinguish "the incumbent was never opened"
from "the incumbent was opened, answered, and thrown away", and those are a
correct gateway and one that double-bills every interrupted request. Only the
upstream can testify.

--------------------------------------------------------------------------
How a target picks its behaviour
--------------------------------------------------------------------------

The fakes read `X-Fake-Mode` from the request, and the gateway sends ONE
`extra_headers` mapping to every target -- correctly, since those are the
client's headers and the client does not know our plan. So the mode cannot
ride on the client's request.

It rides on `ProviderConn.extra_headers`, exactly as `test_executor.py` does
it: two providers with the same `base_url` and different extra headers are two
targets that reach the same port and get different behaviour out of it. The
consequence for this file is that a mode pair is baked into a CATALOG, and a
catalog is baked into a running server -- hence `gateways`, which starts one
uvicorn per mode pair and reuses it. Starting one costs ~20 ms; the whole file
is budgeted at 30 s and spends most of it deliberately waiting out budgets.

--------------------------------------------------------------------------
How a request picks its workload
--------------------------------------------------------------------------

Through the path (`/workloads/{w}/v1/chat/completions`) or the `X-Gw-Workload`
header. Every workload below lives in ONE policy file loaded by every server
here, because the workloads differ only in budgets and retry config -- which
is the axis these tests vary -- while the targets differ only in mode, which
is the catalog's axis. Keeping the two axes in two files is what keeps the
matrix from becoming a server per test.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import socket
import threading
import time
from dataclasses import dataclass

import httpx
import pytest
import uvicorn
from fakes.upstream import PATHS
from starlette.applications import Starlette

from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = pytest.mark.contract

KEY_ENV = "LLMGW_FALLBACK_KEY"
KEY = "sk-fallback-not-a-real-key"

CANDIDATE = "candidate"
INCUMBENT = "incumbent"
CANDIDATE_MODEL = "fake.candidate"
INCUMBENT_MODEL = "fake.incumbent"
UPSTREAM_PATH = PATHS["openai"]

ROUTE = "/v1/chat/completions"

POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 8.0
connect = 1.0
first_event = 1.0
progress = 1.0
client_stall = 5.0

# The A/B every fallback test routes through: candidate first, incumbent as
# the safety net, no retry table -- so a repetition is impossible and every
# extra attempt in these tests is unambiguously a FALLBACK.
[workloads.ab]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

# Same plan, with repetition bought. Used only to prove that
# `X-Gw-No-Retry: 1` removes the repetition and leaves the fallback, which
# needs a workload where a repetition would otherwise happen.
[workloads.retrying]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

  [workloads.retrying.retry]
  max_attempts = 2
  base_delay = 0.01
  max_delay = 0.02

# Deliberately impatient. `total` is small enough that a gateway which
# restarted its clock on the fallback would visibly overshoot it.
[workloads.tight]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

  [workloads.tight.budgets]
  total = 2.5
  connect = 0.4
  first_event = 1.0
  progress = 0.6

# Retries enabled AND a total smaller than the fake's `Retry-After: 3`, which
# is the only configuration in which "the floor does not fit" is a decision
# rather than a coincidence.
[workloads.floor]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

  [workloads.floor.budgets]
  total = 1.0
  connect = 0.4
  first_event = 0.8
  progress = 0.8

  [workloads.floor.retry]
  max_attempts = 3
  base_delay = 0.05
  max_delay = 0.1
  respect_retry_after = true
"""


def body(model: str = CANDIDATE_MODEL, *, stream: bool = True) -> dict:
    return {
        "model": model,
        "stream": stream,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
    }


# ==========================================================================
# The gateway under test, one per mode pair
# ==========================================================================


@dataclass
class GatewayServer:
    """One uvicorn hosting one `build_app()` result, on its own thread.

    A thread with its own event loop, for the reasons `test_passthrough.py`
    gives: pytest-asyncio hands each test function a fresh loop while the
    server outlives them, and a server sharing a loop with the client under
    test can hide a blocking bug in either one.
    """

    app: Starlette
    server: uvicorn.Server
    thread: threading.Thread
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, workload: str | None = None) -> str:
        if workload is None:
            return f"{self.base_url}{ROUTE}"
        return f"{self.base_url}/workloads/{workload}{ROUTE}"

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
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True,
        name=f"llmgw-fallback-{port}",
    )
    thread.start()
    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError(f"gateway on port {port} failed to start")
        time.sleep(0.005)
    return GatewayServer(app=app, server=server, thread=thread, port=port)


def two_target_catalog(
    fakes: Fakes, candidate: dict[str, str], incumbent: dict[str, str]
) -> Catalog:
    """Two targets on one port, told to behave differently.

    `max_concurrency=8` rather than the shipped default so the pool is small
    enough that a leaked connection shows up as a hang rather than as nothing
    at all.
    """

    def conn(pid: str, extra: dict[str, str]) -> ProviderConn:
        return ProviderConn(
            id=pid,
            kind="openai",
            base_url=fakes.openai.base_url,
            api_key_env=KEY_ENV,
            extra_headers=dict(extra),
            max_concurrency=8,
        )

    providers = {CANDIDATE: conn(CANDIDATE, candidate),
                 INCUMBENT: conn(INCUMBENT, incumbent)}
    models = {
        CANDIDATE_MODEL: ModelSpec(id=CANDIDATE_MODEL, provider=CANDIDATE,
                                   api_model="fake-echo", input_per_m=1.0,
                                   output_per_m=2.0, priced_at="2026-09-09"),
        INCUMBENT_MODEL: ModelSpec(id=INCUMBENT_MODEL, provider=INCUMBENT,
                                   api_model="fake-echo-incumbent", input_per_m=1.0,
                                   output_per_m=2.0, priced_at="2026-09-09"),
    }
    return Catalog(models=models, providers=providers)


def mode(name: str, **params: str) -> dict[str, str]:
    """One target's behaviour, as the provider headers that select it."""
    return {"x-fake-mode": name, **{f"x-fake-{k}": v for k, v in params.items()}}


@pytest.fixture(scope="session")
def policy_file(tmp_path_factory) -> str:
    path = tmp_path_factory.mktemp("policy") / "workloads.toml"
    path.write_text(POLICY, encoding="utf-8")
    return str(path)


@pytest.fixture(scope="session")
def gateways(fakes: Fakes, policy_file: str):
    """Start (and cache) one gateway per candidate/incumbent mode pair.

    Cached because the mode lives in the catalog and the catalog lives in the
    process: two tests wanting `(5xx, ok)` want the same server, and starting
    a second one would double the startup cost to prove nothing. Keyed by the
    exact header dicts, so a test that changes one parameter gets its own
    server rather than silently reusing a differently-configured one.
    """
    os.environ.setdefault(KEY_ENV, KEY)
    servers: dict[str, GatewayServer] = {}

    def get(candidate: dict[str, str], incumbent: dict[str, str]) -> GatewayServer:
        key = json.dumps([candidate, incumbent], sort_keys=True)
        if key not in servers:
            config = ServerConfig(
                catalog=two_target_catalog(fakes, candidate, incumbent),
                fake_upstreams=True,
                policy_file=policy_file,
                # The `(5xx, ok)` server is shared by more than five tests,
                # each of which sends the candidate one 5xx and asserts the
                # fallback reached it. See `BREAKER_NEVER_TRIPS`.
                breaker=BREAKER_NEVER_TRIPS,
            )
            servers[key] = _serve(build_app(config))
        return servers[key]

    try:
        yield get
    finally:
        for server in servers.values():
            server.stop()


@pytest.fixture
async def client():
    """Generous next to every gateway budget in this file.

    A client timeout is a second clock, and a test whose assertion could be
    satisfied by either clock is a test that does not say which one fired.
    The largest total configured above is 2.5 s.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


@dataclass
class Streamed:
    status: int
    headers: httpx.Headers
    body: bytes
    truncated: bool


async def stream(client: httpx.AsyncClient, url: str, **kwargs) -> Streamed:
    """POST and collect, tolerating a body that ends without a terminator.

    A truncated body is the EXPECTED outcome of every post-commitment failure
    (C2), so a helper that raised on one could not express half of this file.
    """
    chunks: list[bytes] = []
    truncated = False
    payload = kwargs.pop("json", None) or body()
    try:
        async with client.stream("POST", url, json=payload, **kwargs) as response:
            status, headers = response.status_code, response.headers
            try:
                async for chunk in response.aiter_raw():
                    chunks.append(chunk)
            except httpx.HTTPError:
                truncated = True
    except httpx.HTTPError:  # pragma: no cover - raised on aclose, same meaning
        truncated = True
        status, headers = 0, httpx.Headers()
    return Streamed(status=status, headers=headers, body=b"".join(chunks),
                    truncated=truncated)


DONE = b"data: [DONE]\n\n"


# ==========================================================================
# precommit_fallback_on_5xx
# ==========================================================================


async def test_a_candidate_that_answers_500_falls_back_and_the_client_never_knows(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """`precommit_fallback_on_5xx`, end to end.

    Nothing had been written to the client when the candidate answered 500, so
    the whole failure costs the client one round trip and no information: a
    200, the incumbent's complete stream, and a header saying it took two
    attempts.

    The counters are the other half. `by_mode == {"5xx": 1, "ok": 1}` says each
    target was opened exactly once -- not that two upstreams were opened, which
    a single `total == 2` would also allow if the loop had asked the sick
    candidate twice and never reached the incumbent.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    result = await stream(client, gateway.url("ab"))

    assert result.status == 200
    assert not result.truncated
    assert result.body.endswith(DONE)
    assert result.headers["x-gw-attempts"] == "2"
    assert result.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"

    stats = fakes.stats()
    assert stats["by_mode"] == {"5xx": 1, "ok": 1}
    assert stats["total"] == 2
    assert stats["by_path"] == {UPSTREAM_PATH: 2}


# ==========================================================================
# precommit_fallback_on_connect_timeout
# ==========================================================================


async def test_a_candidate_that_never_sends_headers_falls_back_inside_the_budget(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """`precommit_fallback_on_connect_timeout`.

    The candidate accepted the connection and said nothing at all. That is the
    easy end of the pre-commitment window -- P2 could already fall back here --
    and it is asserted anyway, because it is the control for the two tests
    below it: if this one broke, their failures would not mean what they say.

    Asserted by TIMING OUT of a stall the upstream intends to hold for three
    seconds, against a 0.4 s connect budget. Proving "we gave up at 0.4 s" is
    the same proof as sitting out the stall and it is an order of magnitude
    cheaper.
    """
    gateway = gateways(mode("stall-before-headers", delay="3"), mode("ok"))
    started = time.monotonic()
    result = await stream(client, gateway.url("tight"))
    elapsed = time.monotonic() - started

    assert result.status == 200
    assert result.body.endswith(DONE)
    assert result.headers["x-gw-attempts"] == "2"
    assert result.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert elapsed < 2.5, "the request outlived the workload's total budget"
    assert fakes.stats()["by_mode"] == {"stall-before-headers": 1, "ok": 1}


# ==========================================================================
# precommit_fallback_after_upstream_headers -- the case P3 exists for
# ==========================================================================


async def _raw_exchange(gateway: GatewayServer, path: str) -> bytes:
    """One request over a raw socket, everything the server wrote, verbatim.

    httpx cannot answer the question this test asks. It parses one response
    and hands back an object, so a gateway that put TWO status lines on the
    wire -- a 200 for the candidate, then a 200 for the incumbent -- would look
    identical from up there, and the client's actual experience (a second
    response spliced onto the first, or a protocol error) is exactly what the
    P3 decision exists to make impossible.

    `Connection: close` so the server closes when it is done and the read ends
    at EOF rather than at a keep-alive timeout.
    """
    payload = json.dumps(body()).encode()
    request = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{gateway.port}\r\n"
        "Content-Type: application/json\r\n"
        "Connection: close\r\n"
        f"Content-Length: {len(payload)}\r\n\r\n"
    ).encode() + payload
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.port)
    try:
        writer.write(request)
        await writer.drain()
        return await asyncio.wait_for(reader.read(), timeout=10.0)
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def test_a_candidate_that_sends_headers_and_no_body_still_falls_back(
    gateways, fakes: Fakes
):
    """`precommit_fallback_after_upstream_headers`. THE case P2 could not serve.

    The candidate returned `200 OK` with a full set of SSE headers and then
    produced not one byte of body. P2 had already sent the client's status by
    then -- `http.response.start` went out as soon as `open()` returned -- so
    all it could do was truncate a 200 it had no answer for. CONTRACTS.md C1
    says that failure was still recoverable, and the P3 decision is what
    recovers it: the status is held until the upstream yields a body byte, so
    "headers arrived, nothing followed" happens with the plan still open.

    The assertion that matters is on the RAW BYTES: exactly one status line
    reached the client, and it was the incumbent's 200. A gateway that
    committed early would have written two, or written one and then
    contradicted it.
    """
    gateway = gateways(mode("stall-after-headers", delay="3"), mode("ok"))
    started = time.monotonic()
    raw = await _raw_exchange(gateway, f"/workloads/tight{ROUTE}")
    elapsed = time.monotonic() - started

    head, _, tail = raw.partition(b"\r\n\r\n")
    assert raw.count(b"HTTP/1.1 ") == 1, f"more than one status line:\n{head!r}"
    assert raw.startswith(b"HTTP/1.1 200"), raw.split(b"\r\n")[0]
    assert b"x-gw-attempts: 2" in head.lower(), head
    assert f"x-gw-served-by: {INCUMBENT}/{INCUMBENT_MODEL}".encode() in head.lower()
    assert DONE in tail, "the incumbent's stream did not reach the client"
    assert elapsed < 2.5, "the request outlived the workload's total budget"

    stats = fakes.stats()
    assert stats["by_mode"] == {"stall-after-headers": 1, "ok": 1}


# ==========================================================================
# no_fallback_after_commit
# ==========================================================================


async def test_a_candidate_that_dies_mid_stream_is_never_replaced(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """`no_fallback_after_commit`, and the negative half is the whole test.

    Three events reached the client, so the client is rendering an answer.
    Splicing the incumbent's answer onto the end of it would produce a
    response no client can detect as wrong and no gateway can apologise for
    afterwards, which is why C1 makes commitment absolute rather than
    negotiable.

    A client with a truncated 200 cannot tell "never tried" from "tried and
    discarded", so the assertion lives at the upstream: `by_mode` must contain
    the candidate and nothing else. `total == 1` alone would not do -- it is
    the *absence of the incumbent* that is being claimed.
    """
    gateway = gateways(mode("die-mid-stream", events="3"), mode("ok"))
    result = await stream(client, gateway.url("ab"))

    assert result.status == 200, "the status was committed before the failure"
    assert result.headers["x-gw-attempts"] == "1"
    assert result.headers["x-gw-served-by"] == f"{CANDIDATE}/{CANDIDATE_MODEL}"
    assert result.body.count(b'"delta": {"content"') == 3
    assert DONE not in result.body, "we fabricated a terminal marker (C2)"
    assert b"event: error" not in result.body, "we invented an error nobody sent"
    assert result.truncated, "the body ended cleanly; the client cannot tell it was cut"

    stats = fakes.stats()
    assert stats["total"] == 1, f"a second upstream was opened: {stats['by_mode']}"
    assert stats["by_mode"] == {"die-mid-stream": 1}
    assert "ok" not in stats["by_mode"], "the incumbent was opened after commitment"


# ==========================================================================
# total_deadline_never_resets
# ==========================================================================


async def test_the_total_deadline_is_measured_from_ingress_and_never_from_the_fallback(
    gateways, client: httpx.AsyncClient
):
    """`total_deadline_never_resets`. One clock, started once, at ingress.

    The candidate holds its headers open until the 1.0 s first-event budget
    fires; the incumbent then drips content slowly enough to keep the progress
    clock alive but not slowly enough to end on it. The only thing left that
    can end this request is the workload's 2.5 s total.

    A gateway that derived a fresh total for the fallback -- the natural shape
    if each attempt builds its own `Deadline` -- would end at 1.0 + 2.5 = 3.5 s
    and look perfectly healthy doing it. The whole failure is invisible except
    in the elapsed time, which is why the elapsed time is the assertion. Two
    targets is the cheap version; the expensive version is a client that was
    promised 2.5 s and held for a minute by a five-target chain.
    """
    gateway = gateways(
        mode("stall-after-headers", delay="3"),
        mode("slow-drip", interval="0.2", events="150"),
    )
    started = time.monotonic()
    result = await stream(client, gateway.url("tight"))
    elapsed = time.monotonic() - started

    assert result.status == 200, "the incumbent's first byte committed us"
    assert result.headers["x-gw-attempts"] == "2"
    assert DONE not in result.body, "the drip finished; the deadline never fired"
    assert result.truncated
    assert elapsed >= 2.0, f"ended at {elapsed:.2f}s, well inside the 2.5s total"
    assert elapsed < 3.2, (
        f"ran {elapsed:.2f}s: the total was re-based on the fallback "
        "(1.0s first-event budget + a fresh 2.5s total is 3.5s)"
    )


# ==========================================================================
# retry_after_is_floor
# ==========================================================================


async def test_a_retry_after_longer_than_the_budget_is_never_slept_through(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """`retry_after_is_floor`, at the point where the floor does not fit.

    The `floor` workload buys three attempts with real backoff, so a
    repetition of the candidate is genuinely on the table. The candidate
    answers 429 with `Retry-After: 3`, and C5 says that value is a FLOOR --
    `max(jitter, 3.0)`, never `min` -- because a provider that told you when to
    come back has handed you information the backoff curve is only guessing at.

    Three seconds does not fit in a 1.0 s total. HOW THIS WIRING RESOLVES IT:
    `RetryBudget.delay_for` refuses the delay (`RetryBudgetExhausted`, a
    control signal the client never sees), the executor treats that as "you may
    not repeat" and NOT as "you may not continue", and the fallback proceeds
    immediately. The client gets the incumbent's 200 in milliseconds.

    The 429 therefore does not reach the client at all, which is the correct
    outcome and worth stating because the other branch is also legal: with no
    incumbent left the same refusal ends the walk and the provider's own 429 --
    with its `Retry-After` -- passes through under C4. Both are the same rule.
    What must never happen is the third possibility: sleeping three seconds
    into a one-second deadline to re-ask a provider that already said no, then
    returning the same error the client could have had at once.
    """
    gateway = gateways(mode("429"), mode("ok"))
    started = time.monotonic()
    result = await stream(client, gateway.url("floor"))
    elapsed = time.monotonic() - started

    assert result.status == 200
    assert result.headers["x-gw-attempts"] == "2", "the candidate was asked twice"
    assert result.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert result.body.endswith(DONE)
    assert elapsed < 1.0, (
        f"took {elapsed:.2f}s: a 3s Retry-After was slept inside a 1s total"
    )
    assert fakes.stats()["by_mode"] == {"429": 1, "ok": 1}


# ==========================================================================
# C5 -- X-Gw-No-Retry disables repetition and never fallback
# ==========================================================================


async def test_a_retrying_workload_repeats_the_candidate_before_it_falls_back(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """The control for the test below: with retries on, a repetition happens.

    `max_attempts=2` is a bound on REPETITION and not on upstream requests, so
    the arithmetic is `len(targets) + (max_attempts - 1)` = 3: the candidate
    twice, then the incumbent. Asserting it here is what makes the next test's
    `attempts == 2` mean "the repetition was removed" rather than "this
    workload never repeated anything".
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    result = await stream(client, gateway.url("retrying"))

    assert result.status == 200
    assert result.headers["x-gw-attempts"] == "3"
    assert result.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert fakes.stats()["by_mode"] == {"5xx": 2, "ok": 1}


async def test_no_retry_buys_zero_repetitions_and_keeps_the_fallback(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """CONTRACTS.md C5, both halves, one request.

    `X-Gw-No-Retry: 1` says another layer owns retries. Layered retries
    multiply -- three layers at three attempts each is 27 requests to a
    provider that is failing *because* it is overloaded -- so exactly one layer
    may own them and the header is how the caller claims it.

    What it must NOT do is take the fallback with it. The incumbent is not a
    retry: it is a different provider, and it is the one thing the outer
    gateway that just claimed the retries cannot do for us, because it has
    never seen our plan. A caller that took ownership of retries and silently
    lost the redundancy its workload was configured for would discover it
    during the incident the redundancy was for.

    So: the candidate is asked ONCE (down from twice, per the control above),
    and the incumbent still answers.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    result = await stream(client, gateway.url("retrying"),
                          headers={"x-gw-no-retry": "1"})

    assert result.status == 200, "the fallback was disabled along with the retry"
    assert result.headers["x-gw-attempts"] == "2"
    assert result.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert result.body.endswith(DONE)

    stats = fakes.stats()
    assert stats["by_mode"] == {"5xx": 1, "ok": 1}, "the candidate was repeated"


# ==========================================================================
# P3 verification: the deadline and the amplification bound, together
# ==========================================================================


async def test_a_retry_a_fallback_and_a_stall_together_never_outlive_the_total(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """Every way of spending time in one request, against a 1.0 s total.

    The `floor` workload buys three attempts with a jittered backoff. The
    candidate answers 500 (which is `retry_same`, so the budget spends its
    repeats there), and the incumbent stalls after its headers -- so the
    request ends on a clock rather than on an answer, and the only question is
    WHICH clock.

    Two failures this would catch, and neither is visible in a status code:

      * a deadline re-based on the fallback. Two repeats, a backoff, and a
        fresh 1.0 s at the incumbent is a client held for well over two
        seconds after being promised one.
      * a budget that slept a delay it could not follow with an attempt.
        `delay + min_attempt_time < remaining` is what makes that impossible,
        and it is arithmetic no status code reports.

    The amplification bound is asserted in the same breath because it is the
    same walk: `len(targets) + (max_attempts - 1)` is 4, and `by_mode` says
    which four.
    """
    gateway = gateways(mode("5xx"), mode("stall-after-headers", delay="3"))
    started = time.monotonic()
    result = await stream(client, gateway.url("floor"))
    elapsed = time.monotonic() - started

    assert result.status >= 500, result.status
    assert elapsed < 1.6, (
        f"ran {elapsed:.2f}s against a 1.0s total: a clock was re-based"
    )
    stats = fakes.stats()
    assert stats["total"] <= 4, (
        f"{stats['total']} upstream requests exceeds len(targets) + "
        f"(max_attempts - 1) = 4: {stats['by_mode']}"
    )
    assert stats["by_mode"].get("stall-after-headers", 0) == 1, (
        f"the fallback was starved by the repeats: {stats['by_mode']}"
    )
    assert int(result.headers["x-gw-attempts"]) == stats["total"]


async def test_no_retry_holds_the_bound_at_one_request_per_target(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """C5's amplification half on the workload that would otherwise repeat.

    `floor` buys three attempts. With the header, the budget refuses every
    delay, so the only upstream requests left are the plan's own breadth --
    which is the number an outer gateway that owns the retries is entitled to
    assume we cost it.
    """
    gateway = gateways(mode("5xx"), mode("5xx", status="503"))
    result = await stream(client, gateway.url("floor"),
                          headers={"x-gw-no-retry": "1"})

    assert result.status == 503, "the last target's status, passed through"
    assert result.headers["x-gw-attempts"] == "2"
    assert fakes.stats()["total"] == 2, fakes.stats()["by_mode"]


# ==========================================================================
# C4 -- when every target fails, the last real error passes through
# ==========================================================================


async def test_when_every_target_fails_the_client_gets_the_last_real_error(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """C4 survives the executor: a status and a body, never a summary.

    Both targets answer 500, so the walk ends with nowhere left to go. What
    the client must get is the provider's own status and its own body bytes --
    not `no_targets_available`, which is a summary of OUR walk and tells the
    caller's SDK nothing it can act on. `X-Gw-Attempts` is where the walk is
    reported, which is exactly the right place for it: in a header, next to an
    unmodified provider error.
    """
    gateway = gateways(mode("5xx", status="503"), mode("5xx", status="502"))
    response = await client.post(gateway.url("ab"), json=body())

    assert response.status_code == 502, "the LAST error is the one that passes through"
    assert response.json()["error"]["type"] == "server_error"
    assert response.headers["x-gw-attempts"] == "2"
    assert fakes.stats()["by_mode"] == {"5xx": 2}


async def test_a_client_that_names_no_workload_gets_no_fallback_at_all(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """The consequence of the body-pins-the-target rule, stated as a test.

    An unmodified vendor SDK sends `model` and knows nothing about
    `X-Gw-Workload` or `/workloads/{w}/...`. For that client `plan_for` is
    called with `model=`, which REPLACES the plan with the one target it
    named -- so the candidate's 500 is the client's 500 and the incumbent,
    which was configured precisely to catch it, is never opened.

    That is the documented rule (see `PassthroughEndpoint.__call__`), and the
    reasoning behind it is sound: letting the body win would make every
    configured candidate silently unreachable. It is asserted here because the
    other half of the trade has never been written down -- FAILURE-MODES row 1
    credits "pre-commit fallback to incumbent" as the mitigation for a provider
    5xx, and this is the shape of client for whom that mitigation is off.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    result = await stream(client, gateway.url(), json=body(CANDIDATE_MODEL))

    assert result.status == 500, "the candidate's error reached the client"
    assert result.headers["x-gw-attempts"] == "1"
    assert result.headers["x-gw-served-by"] == "-", "nobody served this request"
    assert fakes.stats()["by_mode"] == {"5xx": 1}, (
        "the incumbent was opened; the body no longer pins the target"
    )


# ==========================================================================
# The header contract
# ==========================================================================


async def test_the_gw_headers_describe_the_fallback_and_the_direct_path_alike(
    gateways, client: httpx.AsyncClient
):
    """Five headers, on a request that fell back and one that did not.

    Both requests hit the same gateway and the same snapshot, so `policy-id`
    and `catalog-id` must be identical across them while `attempts`,
    `served-by` and `workload-id` all differ. Asserting them together is what
    catches the plausible bug: identity headers computed per attempt rather
    than per snapshot, which would make a fallback report a different policy
    than the request it belongs to.

    `catalog-id` is separate from `policy-id` because routing and prices
    version independently. A capture record that carried one of them could not
    answer "which prices applied to this request", and a single merged id
    would churn on every unrelated price edit -- destroying its value as the
    join key it exists to be.
    """
    gateway = gateways(mode("5xx"), mode("ok"))

    fell_back = await stream(client, gateway.url("ab"))
    assert fell_back.headers["x-gw-attempts"] == "2"
    assert fell_back.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert fell_back.headers["x-gw-workload-id"] == "ab"

    # No workload named, so the body's model pins the target -- one attempt,
    # and the default workload's name on it.
    direct = await stream(client, gateway.url(), json=body(INCUMBENT_MODEL))
    assert direct.headers["x-gw-attempts"] == "1"
    assert direct.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert direct.headers["x-gw-workload-id"] == "ab", "the default workload"

    for headers in (fell_back.headers, direct.headers):
        assert re.fullmatch(r"pol_[0-9a-f]{8}", headers["x-gw-policy-id"])
        assert re.fullmatch(r"cat_[0-9a-f]{8}", headers["x-gw-catalog-id"])
    assert fell_back.headers["x-gw-policy-id"] == direct.headers["x-gw-policy-id"]
    assert fell_back.headers["x-gw-catalog-id"] == direct.headers["x-gw-catalog-id"]


async def test_the_workload_header_routes_the_same_request_as_the_path_does(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """Two ways to name a workload, one serving path.

    The header form exists for callers who cannot change their base URL (every
    vendor SDK with a hardcoded `/v1/chat/completions`); the path form exists
    because a URL is the part of a request a router, an access log and an
    authorisation rule can all see. They must produce the same plan, or the
    second one is a way to bypass whatever was decided about the first.

    The body names a model that is not in the catalog at all, which is the
    sharp end of the routing rule: naming a workload IS the statement "I am
    not pinning a model". If the body won instead, every candidate would be
    unreachable over HTTP -- both surfaces require `model` -- and an operator
    would watch an A/B receive zero traffic with nothing wrong in the config.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    result = await stream(
        client, gateway.url(), json=body("not-in-the-catalog"),
        headers={"x-gw-workload": "ab"},
    )

    assert result.status == 200
    assert result.headers["x-gw-workload-id"] == "ab"
    assert result.headers["x-gw-attempts"] == "2"
    assert result.headers["x-gw-served-by"] == f"{INCUMBENT}/{INCUMBENT_MODEL}"
    assert fakes.stats()["by_mode"] == {"5xx": 1, "ok": 1}


# ==========================================================================
# An unknown workload is refused before any upstream work
# ==========================================================================


async def test_an_unknown_workload_is_a_400_that_costs_a_provider_nothing(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """A dictionary lookup, not a handshake -- and never a guess.

    With a policy document loaded, workload names are keys in it. Routing
    `summarise` to the default because the file spells it `summarize` would
    send a tenant's traffic to a model they did not ask for and bill them for
    it, which is a worse outcome than the 400 they can read and fix.

    The refusal happens before the request body is even read, so the counter
    assertion is the contract: denial is free (C6).
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    response = await client.post(gateway.url("summarise"), json=body())

    assert response.status_code == 400
    assert response.json()["error"]["type"] == "policy_error"
    assert "summarise" in response.json()["error"]["message"]
    assert response.headers["x-gw-workload-id"] == "summarise", (
        "the error named the default workload instead of the one that was asked for"
    )
    assert response.headers["x-gw-attempts"] == "0"
    assert fakes.stats()["total"] == 0, "an unknown workload reached a provider"


# ==========================================================================
# probe_no_upstream_call, now reporting the real plan
# ==========================================================================


async def test_the_probe_reports_the_ordered_plan_without_calling_anything(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """C6, and the probe's whole reason to exist: it is the same lookup.

    P2's probe was a stub that reported `config.default_model`. This one calls
    `snapshot.plan_for()` -- the identical call the serving path makes -- so
    the answer to "why did that request go there" is a GET rather than a log
    dig. A probe that reported approximately what the gateway would do would
    be worse than none: it would be believed.

    ORDER is the payload's most important property. `targets[0]` is the
    candidate and gets the traffic; `targets[-1]` is the incumbent and catches
    what it drops. A client reading this as a set has read it wrong, which is
    why the roles are labelled rather than left to be inferred.

    Every value comes out of a snapshot and a catalog, both of which are
    dictionaries, so the counter assertion is the rest of the test: a probe
    that opens a connection is a probe you stop running during the incident
    you built it for.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    response = await client.get(f"{gateway.base_url}/workloads/ab/probe")

    assert response.status_code == 200
    payload = response.json()
    assert payload["workload_id"] == "ab"
    assert payload["routes_to"] == "ab"
    assert payload["upstream_called"] is False
    assert payload["default_workload"] == "ab"
    assert re.fullmatch(r"pol_[0-9a-f]{8}", payload["policy_id"])
    assert re.fullmatch(r"cat_[0-9a-f]{8}", payload["catalog_id"])
    assert payload["policy_age_seconds"] >= 0.0

    assert [t["served_by"] for t in payload["targets"]] == [
        f"{CANDIDATE}/{CANDIDATE_MODEL}", f"{INCUMBENT}/{INCUMBENT_MODEL}"
    ]
    assert [t["role"] for t in payload["targets"]] == ["candidate", "incumbent"]
    assert all(t["credential_present"] for t in payload["targets"])
    assert payload["budgets"]["total"] == 8.0
    assert payload["budgets"]["first_event"] == 1.0
    assert payload["retry"] is None, "the ab workload configures no retry table"
    assert payload["max_upstream_requests"] == 2

    assert fakes.stats()["total"] == 0, "the probe opened an upstream connection"


async def test_the_probe_reports_the_retry_table_and_the_amplification_bound(
    gateways, client: httpx.AsyncClient
):
    """The number an operator actually needs, computed once, here.

    `max_attempts` bounds REPETITION; breadth belongs to the plan; the bound
    on upstream requests is `len(targets) + (max_attempts - 1)`. Everybody
    computes that wrong at least once, and computing it wrong in the direction
    people do -- reading `max_attempts` as the total -- understates what this
    gateway will do to a provider having a bad minute by exactly the number of
    targets.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    payload = (
        await client.get(f"{gateway.base_url}/workloads/retrying/probe")
    ).json()

    assert payload["retry"]["max_attempts"] == 2
    assert payload["retry"]["enabled"] is True
    assert payload["max_upstream_requests"] == 3


async def test_the_probe_refuses_an_unknown_workload_rather_than_guessing(
    gateways, client: httpx.AsyncClient, fakes: Fakes
):
    """The probe and the serving path agree, including about failure.

    A probe that answered 200 for a workload the gateway would 400 is worse
    than no probe: it would be used to confirm a config that does not work.
    """
    gateway = gateways(mode("5xx"), mode("ok"))
    response = await client.get(f"{gateway.base_url}/workloads/nope/probe")

    assert response.status_code == 400
    assert response.json()["error"]["type"] == "policy_error"
    assert response.json()["workload_id"] == "nope"
    assert fakes.stats()["total"] == 0

"""Fixtures for the chaos tier: a seed you can replay, and a gateway you can
look inside.

Three things this file exists to provide that the contract tier does not.

**A seed, printed on failure.** A randomized tier whose failures cannot be
reproduced is a tier that reports noise. Every test takes `chaos_seed`, every
assertion message carries it, and `LLMGW_CHAOS_SEED=<n>` replays the exact
schedule. The seed is chosen once per session so that a failure in iteration
40 of test B is reproducible by re-running the whole file, not just that test.

**A size dial.** `LLMGW_CHAOS_ITERS` scales every loop. The default is sized
for the whole file to finish in well under a minute on a laptop, because a
tier that takes five minutes is a tier that runs in CI only, and a fault
injector you never run locally is a fault injector that rots.

**The gateway's own event loop.** The single most valuable invariant here --
"no orphan tasks" -- is a statement about the loop the *server* runs on, and
the server runs on a background thread with its own loop precisely so that a
blocking bug in the client cannot hide a blocking bug in the server. So
`GatewayServer` captures that loop object at startup and `task_count()`
marshals a `len(asyncio.all_tasks())` onto it. Sampling the test's own loop
instead would measure httpx and prove nothing.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import socket
import struct
import threading
import time
from dataclasses import dataclass

import httpx
import pytest
import uvicorn
from fakes.upstream import PATHS, RunningServer, Surface, build_app, serve_in_thread
from starlette.applications import Starlette

from llmgw.clocks import Budgets
from llmgw.server.app import build_app as build_gateway
from llmgw.server.config import ServerConfig, fake_catalog
from llmgw.upstream import Upstream
from tests.contract.conftest import BREAKER_NEVER_TRIPS

# Every gateway in this tier runs under `BREAKER_NEVER_TRIPS`, for the reason
# its docstring gives plus one that is specific to chaos: this tier selects a
# hostile mode PER REQUEST, by header, against ONE catalog target. From the
# breaker's point of view that is a single provider failing most of the time,
# and opening its circuit is the correct response -- which would turn "a
# healthy stream is untouched by whatever shares the process" from a resource
# invariant into a statement about breaker state. The resource invariants are
# what this tier is for; the breaker's behaviour under load has its own file
# (`tests/contract/test_isolation.py`). The gate is wired and exercised on
# every request here -- tickets, permits, return-to-zero -- it just never
# trips.

# --------------------------------------------------------------------------
# Budgets. Small on purpose: every timeout in this tier is asserted by firing.
# --------------------------------------------------------------------------

TOTAL_BUDGET = 3.0
PROGRESS_BUDGET = 0.35
CLIENT_STALL_BUDGET = 0.8
MAX_FRAME_BYTES = 32 * 1024
BUFFER_BYTES = 8 * 1024

FAKE_HEADERS = (
    "x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
    "x-fake-status", "x-fake-bytes", "x-fake-seed", "x-fake-crlf",
    "x-raw-mode",
)

MODEL_FOR = {"openai": "fake.echo", "anthropic": "fake.echo-anthropic"}
ROUTE_FOR = {"openai": "/v1/chat/completions", "anthropic": "/anthropic/v1/messages"}


# --------------------------------------------------------------------------
# Seed and size
# --------------------------------------------------------------------------


@pytest.fixture(scope="session")
def chaos_seed() -> int:
    """One seed for the session, printed so a red run is reproducible.

    `print` rather than a log record: pytest captures stdout and shows it on
    failure, which is exactly the moment the number is worth reading and the
    only moment it is not noise.
    """
    seed = int(os.environ.get("LLMGW_CHAOS_SEED", random.randrange(1, 2**31)))
    print(f"\n[chaos] LLMGW_CHAOS_SEED={seed}")
    return seed


@pytest.fixture(scope="session")
def iterations() -> int:
    """How many faults to inject per loop. Raise it with `LLMGW_CHAOS_ITERS`."""
    return int(os.environ.get("LLMGW_CHAOS_ITERS", "0") or 0)


def scaled(iterations: int, default: int, *, share: float = 1.0) -> int:
    """`default` unless the env var overrode it. Keeps every loop on one dial.

    `share` is what stops one dial from being useless. A loop that fires one
    request per iteration and a loop that fires twelve concurrently cannot take
    the same number: turning the dial to 40 would put the second one past
    pytest-timeout's 60 s while the first is still warming up. So each loop
    declares what fraction of the dial it is worth, and `LLMGW_CHAOS_ITERS`
    stays a single number a human can reason about.
    """
    if not iterations:
        return default
    return max(1, int(iterations * share))


def rng_for(seed: int, label: str) -> random.Random:
    """A private stream per test.

    Sharing one `Random` across tests makes iteration 3 of test B depend on
    whether test A ran, so `-k` on a red test replays a different schedule than
    the one that failed. Deriving a per-label stream keeps each test's sequence
    a function of the seed alone.
    """
    return random.Random(f"{seed}:{label}")


# --------------------------------------------------------------------------
# The hostile upstreams
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Fakes:
    openai: RunningServer
    anthropic: RunningServer

    def url(self, surface: Surface) -> str:
        server = self.openai if surface == "openai" else self.anthropic
        return f"{server.base_url}{PATHS[surface]}"

    @property
    def stats_url(self) -> str:
        return f"{self.openai.base_url}/__stats"

    def stats(self) -> dict:
        return httpx.get(self.stats_url, timeout=5.0).json()

    def reset_stats(self) -> None:
        httpx.post(f"{self.stats_url}/reset", timeout=5.0).raise_for_status()

    def open_streams(self) -> int:
        return int(self.stats()["open_streams"])


@pytest.fixture(scope="session")
def fakes():
    openai = serve_in_thread(build_app("openai"))
    anthropic = serve_in_thread(build_app("anthropic"))
    try:
        yield Fakes(openai=openai, anthropic=anthropic)
    finally:
        openai.stop()
        anthropic.stop()


# --------------------------------------------------------------------------
# The gateway under test
# --------------------------------------------------------------------------


@dataclass
class GatewayServer:
    """One uvicorn hosting one `build_app()` result, plus a handle on its loop.

    `loop` is the field the contract tier does not have and this one needs. A
    leaked task lives on the server's loop, not the test's, and no HTTP
    response can report it -- so the runner below builds the loop itself
    instead of letting `uvicorn.Server.run()` call `asyncio.run` behind a
    thread boundary that hides the object.
    """

    app: Starlette
    server: uvicorn.Server
    thread: threading.Thread
    port: int
    loop: asyncio.AbstractEventLoop

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def url(self, surface: str) -> str:
        return f"{self.base_url}{ROUTE_FOR[surface]}"

    @property
    def upstream(self) -> Upstream:
        return self.app.state.gateway.upstream

    def task_count(self) -> int:
        """`len(asyncio.all_tasks())` sampled ON THE SERVER'S LOOP.

        Marshalled with `run_coroutine_threadsafe` rather than read directly:
        `asyncio.all_tasks()` walks a set that the target loop is mutating, and
        a cross-thread read of it is a `RuntimeError: Set changed size during
        iteration` waiting for a busy moment.
        """
        async def _count() -> int:
            return len(asyncio.all_tasks())

        return asyncio.run_coroutine_threadsafe(_count(), self.loop).result(5.0)

    def pool_size(self) -> int:
        return sum(self.upstream.stats().values())

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

    thread = threading.Thread(target=_run, daemon=True, name=f"llmgw-chaos-{port}")
    thread.start()
    ready.wait(startup_timeout)
    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError(f"gateway on port {port} failed to start")
        time.sleep(0.005)
    return GatewayServer(app=app, server=server, thread=thread, port=port,
                         loop=holder["loop"])


@pytest.fixture(scope="session")
def gateway(fakes: Fakes):
    """One gateway for the whole session, with deliberately tiny budgets.

    Session-scoped because the invariants asserted here are *return to
    baseline* invariants, and a server rebuilt per test can never fail them:
    a fresh process has nothing left over by construction, which is precisely
    the bug this tier is looking for.
    """
    catalog = fake_catalog(
        openai_url=f"{fakes.openai.base_url}/v1",
        anthropic_url=fakes.anthropic.base_url,
    )
    config = ServerConfig(
        catalog=catalog,
        fake_upstreams=True,
        forward_request_headers=FAKE_HEADERS,
        max_frame_bytes=MAX_FRAME_BYTES,
        buffer_bytes=BUFFER_BYTES,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(
            total=TOTAL_BUDGET,
            connect=0.8,
            first_event=0.8,
            progress=PROGRESS_BUDGET,
            client_stall=CLIENT_STALL_BUDGET,
        ),
    )
    server = _serve(build_gateway(config))
    try:
        yield server
    finally:
        server.stop()


def body_for(surface: str, *, stream: bool = True) -> dict:
    return {"model": MODEL_FOR[surface], "stream": stream,
            "messages": [{"role": "user", "content": "chaos"}],
            "max_tokens": 64}


# --------------------------------------------------------------------------
# A raw-socket upstream, for the two failures ASGI cannot produce
# --------------------------------------------------------------------------


class RawUpstream:
    """An HTTP-ish server built on `asyncio.start_server`, not on ASGI.

    FAILURE-MODES.md's "two limits of the test rig itself" says this out loud:
    `die-mid-stream` sends a FIN, because uvicorn closes a transport and h11
    simply stops -- and "a provider whose process is killed can produce
    `ECONNRESET` on a different code path with a different exception type.
    ASGI exposes no way to reset a socket." That is true of ASGI and not of a
    socket, so this class is fifteen lines of `writer.transport` and a
    `SO_LINGER` of zero, which is what turns a close into an RST.

    It also serves the other shape no ASGI app can: a response that is not
    HTTP at all. Both land in `map_transport_error` through branches the fake
    upstreams cannot reach, and a transport exception with no rule is exactly
    the thing `upstream.py` promises never escapes it.

    Behaviour is chosen per request by an `X-Raw-Mode` header, greppable out of
    the raw request head -- there is no request parser here on purpose, because
    a parser is a thing that can be wrong about the request under test.
    """

    def __init__(self) -> None:
        self.port = 0
        self.connections = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._server: asyncio.AbstractServer | None = None

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def _handle(self, reader: asyncio.StreamReader,
                      writer: asyncio.StreamWriter) -> None:
        self.connections += 1
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5.0)
        except (asyncio.IncompleteReadError, TimeoutError, ConnectionError):
            writer.close()
            return
        mode = b"reset-mid-stream"
        for line in head.split(b"\r\n"):
            if line.lower().startswith(b"x-raw-mode:"):
                mode = line.split(b":", 1)[1].strip().lower()
        # Drain whatever body is coming so the client's write completes; the
        # length does not matter because every mode below abandons the
        # exchange anyway.
        with contextlib.suppress(Exception):
            await asyncio.wait_for(reader.read(65536), timeout=0.2)

        if mode == b"garbage":
            writer.write(b"NOT-HTTP/9.9 banana\r\n\r\n")
            await writer.drain()
            writer.close()
            return

        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/event-stream\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n"
        )
        if mode == b"reset-mid-stream":
            frame = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\n'
            writer.write(b"%x\r\n" % len(frame) + frame + b"\r\n")
        await writer.drain()
        _reset(writer)

    def start(self) -> RawUpstream:
        ready = threading.Event()

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop

            async def _serve() -> None:
                self._server = await asyncio.start_server(
                    self._handle, "127.0.0.1", 0
                )
                self.port = self._server.sockets[0].getsockname()[1]
                ready.set()
                async with self._server:
                    await self._server.serve_forever()

            with contextlib.suppress(asyncio.CancelledError, RuntimeError):
                loop.run_until_complete(_serve())

        self._thread = threading.Thread(target=_run, daemon=True, name="llmgw-raw-up")
        self._thread.start()
        assert ready.wait(10.0), "raw upstream never bound"
        return self

    def stop(self) -> None:
        if self._loop is not None and self._server is not None:
            self._loop.call_soon_threadsafe(self._server.close)
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(5.0)


def _reset(writer: asyncio.StreamWriter) -> None:
    """Close with `SO_LINGER = 0`, which sends an RST rather than a FIN.

    This one setsockopt is the whole difference between the failure the fakes
    can produce and the failure a killed provider produces. A FIN reaches httpx
    as `RemoteProtocolError: peer closed connection without sending complete
    message body`; an RST reaches it as a `ConnectionResetError` wrapped in
    `httpx.ReadError`, through a different branch of `map_transport_error`.
    """
    sock = writer.get_extra_info("socket")
    if sock is not None:
        with contextlib.suppress(OSError):
            sock.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER,
                struct.pack("ii", 1, 0),
            )
    with contextlib.suppress(Exception):
        writer.transport.abort()


@pytest.fixture(scope="session")
def raw_upstream():
    server = RawUpstream().start()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="session")
def raw_gateway(raw_upstream: RawUpstream, fakes: Fakes):
    """A gateway whose OpenAI provider is the raw socket server."""
    catalog = fake_catalog(
        openai_url=f"{raw_upstream.base_url}/v1",
        anthropic_url=fakes.anthropic.base_url,
    )
    config = ServerConfig(
        catalog=catalog,
        fake_upstreams=True,
        forward_request_headers=FAKE_HEADERS,
        max_frame_bytes=MAX_FRAME_BYTES,
        buffer_bytes=BUFFER_BYTES,
        # http2=False: the raw server speaks HTTP/1.1 only, and an h2 upgrade
        # attempt would be testing ALPN rather than the reset.
        http2=False,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(
            total=TOTAL_BUDGET, connect=0.8, first_event=0.8,
            progress=PROGRESS_BUDGET, client_stall=CLIENT_STALL_BUDGET,
        ),
    )
    server = _serve(build_gateway(config))
    try:
        yield server
    finally:
        server.stop()


# --------------------------------------------------------------------------
# A two-target gateway, for the invariants that only exist under fallback
# --------------------------------------------------------------------------
#
# The single-target `gateway` above cannot express any of them. A fallback
# opens a SECOND upstream inside one request, so every resource invariant the
# tier asserts -- `in_flight()` empty, the fake's streams closed, the server's
# task count back to baseline -- now has a case where the request holds two
# upstreams in sequence and a cancellation can land between them.
#
# The mode a target plays has to live in `ProviderConn.extra_headers` rather
# than in the client's request, because `build_headers` merges the client's
# forwarded headers LAST: an `x-fake-mode` from the client would override the
# provider's and both targets would behave identically. So a mode PAIR is
# baked into a catalog, a catalog is baked into a server, and the loop picks a
# server rather than a header.

FALLBACK_KEY_ENV = "LLMGW_CHAOS_FALLBACK_KEY"
CANDIDATE = "chaos-candidate"
INCUMBENT = "chaos-incumbent"
CANDIDATE_MODEL = "chaos.candidate"
INCUMBENT_MODEL = "chaos.incumbent"

FALLBACK_POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 2.0
connect = 0.5
first_event = 0.5
progress = 0.35
client_stall = 0.8

[workloads.ab]
incumbent = "chaos.incumbent"
candidate = "chaos.candidate"

  [workloads.ab.retry]
  max_attempts = 2
  base_delay = 0.01
  max_delay = 0.02
"""


def fake_mode(name: str, **params: str) -> dict[str, str]:
    """One target's behaviour, as the provider headers that select it."""
    return {"x-fake-mode": name, **{f"x-fake-{k}": v for k, v in params.items()}}


def two_target_catalog(fakes: Fakes, candidate: dict[str, str],
                       incumbent: dict[str, str]):
    """Two targets on the OpenAI fake's port, told to behave differently.

    Distinct `api_model`s on purpose: the gateway rewrites the client's model
    to the target's wire model per attempt, so a plan whose two entries shared
    one `api_model` could not tell a gateway that does it from one that does
    not. The fake ignores the field; the point is that the rewrite runs on
    every fallback in this loop.
    """
    from llmgw.catalog import Catalog, ModelSpec, ProviderConn

    def conn(pid: str, extra: dict[str, str]) -> ProviderConn:
        return ProviderConn(
            id=pid, kind="openai", base_url=fakes.openai.base_url,
            api_key_env=FALLBACK_KEY_ENV, extra_headers=dict(extra),
            max_concurrency=8,
        )

    def spec(mid: str, provider: str, api_model: str) -> ModelSpec:
        return ModelSpec(id=mid, provider=provider, api_model=api_model,
                         input_per_m=1.0, output_per_m=2.0, priced_at="2026-09-09")

    return Catalog(
        providers={CANDIDATE: conn(CANDIDATE, candidate),
                   INCUMBENT: conn(INCUMBENT, incumbent)},
        models={CANDIDATE_MODEL: spec(CANDIDATE_MODEL, CANDIDATE, "fake-echo"),
                INCUMBENT_MODEL: spec(INCUMBENT_MODEL, INCUMBENT,
                                      "fake-echo-incumbent")},
    )


@pytest.fixture(scope="session")
def fallback_gateways(fakes: Fakes, tmp_path_factory):
    """One gateway per mode pair, built lazily and cached for the session."""
    os.environ.setdefault(FALLBACK_KEY_ENV, "sk-chaos-not-a-real-key")
    path = tmp_path_factory.mktemp("chaospolicy") / "workloads.toml"
    path.write_text(FALLBACK_POLICY, encoding="utf-8")
    servers: dict[tuple, GatewayServer] = {}

    def get(candidate: dict[str, str], incumbent: dict[str, str]) -> GatewayServer:
        key = (tuple(sorted(candidate.items())), tuple(sorted(incumbent.items())))
        if key not in servers:
            config = ServerConfig(
                catalog=two_target_catalog(fakes, candidate, incumbent),
                fake_upstreams=True,
                policy_file=str(path),
                max_frame_bytes=MAX_FRAME_BYTES,
                buffer_bytes=BUFFER_BYTES,
                breaker=BREAKER_NEVER_TRIPS,
            )
            servers[key] = _serve(build_gateway(config))
        return servers[key]

    try:
        yield get
    finally:
        for server in servers.values():
            server.stop()


def fallback_body(*, stream: bool = True) -> dict:
    return {"model": CANDIDATE_MODEL, "stream": stream,
            "messages": [{"role": "user", "content": "chaos"}],
            "max_tokens": 64}

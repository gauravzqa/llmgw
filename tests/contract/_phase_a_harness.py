"""Shared harness for the PLAN-2 phase A contract files.

The same shape as `test_fallback.py`'s: one gateway per (candidate mode,
incumbent mode) pair, both targets on the OpenAI fake port and told apart by
`ProviderConn.extra_headers`, one policy file with the workloads these tests
route through. Lives in its own module because three new files need it and
the existing contract files are not this phase's to edit.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from dataclasses import dataclass

import httpx
import uvicorn
from fakes.upstream import PATHS
from starlette.applications import Starlette

from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

KEY_ENV = "LLMGW_PHASE_A_KEY"
KEY = "sk-phase-a-not-a-real-key"

CANDIDATE = "candidate"
INCUMBENT = "incumbent"
CANDIDATE_MODEL = "fake.candidate"
INCUMBENT_MODEL = "fake.incumbent"
ROUTE = "/v1/chat/completions"
UPSTREAM_PATH = PATHS["openai"]

POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 8.0
connect = 1.0
first_event = 1.0
progress = 1.0
client_stall = 5.0

# Candidate first, incumbent as the net, no retry table: every second
# attempt in these tests is a FALLBACK, never a repetition.
[workloads.ab]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

# The candidate alone, for the error-path assertions: what the client sees
# when the only target says no.
[workloads.solo]
incumbent = "fake.candidate"

# Patient enough to wait out a queued provider (fake delay 3 s).
[workloads.patient]
incumbent = "fake.incumbent"
candidate = "fake.candidate"

  [workloads.patient.budgets]
  total = 8.0
  first_event = 6.0
  progress = 6.0
"""


def body(model: str = CANDIDATE_MODEL, *, stream: bool = True) -> dict:
    return {
        "model": model,
        "stream": stream,
        "max_tokens": 64,
        "messages": [{"role": "user", "content": "hi"}],
    }


def mode(name: str, **params: str) -> dict[str, str]:
    return {"x-fake-mode": name, **{f"x-fake-{k}": v for k, v in params.items()}}


@dataclass
class GatewayServer:
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
        if self.thread.is_alive():  # pragma: no cover
            self.server.force_exit = True
            self.thread.join(timeout)


def serve(app: Starlette, *, startup_timeout: float = 10.0) -> GatewayServer:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(2048)
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="critical", access_log=False, lifespan="on")
    server = uvicorn.Server(config)
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True,
        name=f"llmgw-phase-a-{port}",
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
    def conn(pid: str, extra: dict[str, str]) -> ProviderConn:
        return ProviderConn(
            id=pid, kind="openai", base_url=fakes.openai.base_url,
            api_key_env=KEY_ENV, extra_headers=dict(extra), max_concurrency=8,
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


class GatewayPool:
    """One gateway per mode pair, started on demand and stopped together."""

    def __init__(self, fakes: Fakes, policy_file: str) -> None:
        os.environ.setdefault(KEY_ENV, KEY)
        self._fakes = fakes
        self._policy_file = policy_file
        self._servers: dict[str, GatewayServer] = {}

    def get(self, candidate: dict[str, str], incumbent: dict[str, str]) -> GatewayServer:
        key = json.dumps([candidate, incumbent], sort_keys=True)
        if key not in self._servers:
            config = ServerConfig(
                catalog=two_target_catalog(self._fakes, candidate, incumbent),
                fake_upstreams=True,
                policy_file=self._policy_file,
                breaker=BREAKER_NEVER_TRIPS,
            )
            self._servers[key] = serve(build_app(config))
        return self._servers[key]

    def stop_all(self) -> None:
        for server in self._servers.values():
            server.stop()


@dataclass
class Streamed:
    status: int
    headers: httpx.Headers
    body: bytes
    truncated: bool


async def stream(client: httpx.AsyncClient, url: str, **kwargs) -> Streamed:
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
    except httpx.HTTPError:  # pragma: no cover
        truncated = True
        raise
    return Streamed(status=status, headers=headers, body=b"".join(chunks),
                    truncated=truncated)


def fake_mode_counts(fakes: Fakes) -> dict[str, int]:
    """Requests the fake saw, by mode, from its `/__stats`."""
    return dict(fakes.stats().get("by_mode", {}))

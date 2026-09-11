"""What the provider actually received, over a real socket.

`fakes/upstream.py` counts requests by mode and by path and never looks at a
body, which is exactly the right shape for every other question the contract
tier asks -- and it is the one thing it cannot answer here. "Did the incumbent
get its own model id?" is a question about bytes the fake throws away, and the
fakes are not mine to change, so this file brings its own upstream: a Starlette
app whose only jobs are to record what arrived and to answer with the canonical
`fakes/wire.py` stream.

The failure it exists to catch fails LATE, which is why it needs a test at all.
`ModelSpec.api_model` exists because our catalog id (`openrouter.deepseek-v4-pro`)
is not the string the provider's API takes (`deepseek/deepseek-v4-pro`).
Forwarding the client's `model` verbatim therefore sends the *candidate's*
model string to the *incumbent's* API, and the incumbent is only ever reached
when the candidate is already down. Every fake in this repo ignores the field,
so the whole suite passed while the one attempt that matters would have come
back 404 from a provider that was perfectly healthy.

Three claims, one per test:

    the second target receives ITS OWN model id      (the fallback bug)
    a client who already named the wire model gets
      byte-for-byte passthrough, to the byte         (the cost of the fix)
    the same rewrite happens on the Anthropic surface (both surfaces)
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from dataclasses import dataclass, field

import httpx
import pytest
import uvicorn
from fakes import wire
from fakes.upstream import PATHS
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from tests.contract.conftest import BREAKER_NEVER_TRIPS

pytestmark = pytest.mark.contract

KEY_ENV = "LLMGW_RECORDER_KEY"
KEY = "sk-recorder-not-a-real-key"

CAND_WIRE = "candidate/wire-model-v4"
INC_WIRE = "incumbent-wire-model-20260909"
DIRECT = "direct-wire-model"

MODE_HEADER = "x-rec-mode"
"""Which behaviour this target has, carried on `ProviderConn.extra_headers`.

The same trick `test_fallback.py` uses and for the same reason: the gateway
sends ONE `extra_headers` mapping to every target -- correctly, since those are
the client's headers and the client has never seen our plan -- so a per-target
behaviour has to ride on the catalog, not on the request.
"""


# ==========================================================================
# An upstream that remembers
# ==========================================================================


@dataclass
class Recorded:
    path: str
    raw: bytes
    """The EXACT request body bytes. The byte-for-byte assertion below is only
    worth writing if nothing between here and the socket re-serialises."""

    @property
    def model(self) -> str | None:
        try:
            return json.loads(self.raw).get("model")
        except ValueError:  # pragma: no cover - only if the gateway sent junk
            return None


@dataclass
class Recorder:
    requests: list[Recorded] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, item: Recorded) -> None:
        with self.lock:
            self.requests.append(item)

    def reset(self) -> None:
        with self.lock:
            self.requests.clear()

    @property
    def models(self) -> list[str | None]:
        with self.lock:
            return [r.model for r in self.requests]


def recorder_app(recorder: Recorder) -> Starlette:
    """Both surfaces' paths, one handler, no parsing of anything that matters.

    `X-Rec-Mode: 5xx` answers 500 so the plan moves on; anything else answers
    the canonical stream for whichever path was hit. The response is
    `fakes/wire.py`'s bytes and not an invention, so the client-side assertion
    is the same equality the rest of the tier makes.
    """

    async def handle(request: Request) -> Response:
        raw = await request.body()
        recorder.add(Recorded(path=request.url.path, raw=raw))
        if request.headers.get(MODE_HEADER, "ok") == "5xx":
            return Response(
                wire.openai_error_body(),
                status_code=500,
                media_type="application/json",
            )
        frames = (
            wire.anthropic_stream()
            if request.url.path == PATHS["anthropic"]
            else wire.openai_stream()
        )

        async def body():
            yield wire.joined(frames)

        return StreamingResponse(body(), media_type="text/event-stream")

    return Starlette(routes=[
        Route(PATHS["openai"], handle, methods=["POST"]),
        Route(PATHS["anthropic"], handle, methods=["POST"]),
    ])


@dataclass
class Running:
    server: uvicorn.Server
    thread: threading.Thread
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self, timeout: float = 5.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout)
        if self.thread.is_alive():  # pragma: no cover
            self.server.force_exit = True
            self.thread.join(timeout)


def _serve(app: Starlette, *, name: str, startup_timeout: float = 10.0) -> Running:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="critical", access_log=False, lifespan="on")
    )
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True, name=name
    )
    thread.start()
    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
            raise RuntimeError(f"{name} failed to start")
        time.sleep(0.005)
    return Running(server=server, thread=thread, port=port)


# ==========================================================================
# The gateway in front of it
# ==========================================================================


def catalog_for(base_url: str) -> Catalog:
    """Three targets on one recorder, with three DIFFERENT wire model ids.

    `fake.echo` and `fake.echo-anthropic` in the shipped catalog share the
    `api_model` `fake-echo`, so a plan built out of them cannot tell a gateway
    that rewrites the model from one that does not. Distinct strings here are
    the whole experiment.
    """

    def conn(pid: str, kind: str, mode: str) -> ProviderConn:
        return ProviderConn(
            id=pid, kind=kind, base_url=base_url, api_key_env=KEY_ENV,
            extra_headers={MODE_HEADER: mode}, max_concurrency=8,
        )

    def spec(mid: str, provider: str, api_model: str) -> ModelSpec:
        return ModelSpec(id=mid, provider=provider, api_model=api_model,
                         input_per_m=1.0, output_per_m=2.0, priced_at="2026-09-09")

    return Catalog(
        providers={
            "cand": conn("cand", "openai", "5xx"),
            "inc": conn("inc", "openai", "ok"),
            "anth-cand": conn("anth-cand", "anthropic", "5xx"),
            "anth-inc": conn("anth-inc", "anthropic", "ok"),
            "direct": conn("direct", "openai", "ok"),
        },
        models={
            "rec.candidate": spec("rec.candidate", "cand", CAND_WIRE),
            "rec.incumbent": spec("rec.incumbent", "inc", INC_WIRE),
            "rec.anth-candidate": spec("rec.anth-candidate", "anth-cand", CAND_WIRE),
            "rec.anth-incumbent": spec("rec.anth-incumbent", "anth-inc", INC_WIRE),
            # The one case where the client's string is already the wire
            # string, which is the only case that stays byte for byte.
            DIRECT: spec(DIRECT, "direct", DIRECT),
        },
    )


POLICY = """
default_workload = "ab"

[defaults.budgets]
total = 8.0
connect = 1.0
first_event = 1.0
progress = 1.0
client_stall = 5.0

[workloads.ab]
incumbent = "rec.incumbent"
candidate = "rec.candidate"

[workloads.anth]
incumbent = "rec.anth-incumbent"
candidate = "rec.anth-candidate"

[workloads.direct]
incumbent = "direct-wire-model"
"""


@pytest.fixture(scope="module")
def recorder() -> Recorder:
    return Recorder()


@pytest.fixture(scope="module")
def upstream(recorder: Recorder):
    server = _serve(recorder_app(recorder), name="llmgw-recorder")
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def gateway(upstream: Running, tmp_path_factory):
    os.environ.setdefault(KEY_ENV, KEY)
    path = tmp_path_factory.mktemp("recpolicy") / "workloads.toml"
    path.write_text(POLICY, encoding="utf-8")
    config = ServerConfig(
        catalog=catalog_for(upstream.base_url),
        fake_upstreams=True,
        policy_file=str(path),
        # Both candidates answer 5xx on every request, and the server is
        # module-scoped. See `BREAKER_NEVER_TRIPS`.
        breaker=BREAKER_NEVER_TRIPS,
    )
    server = _serve(build_app(config), name="llmgw-recorder-gw")
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(autouse=True)
def _clean(recorder: Recorder):
    recorder.reset()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


async def post(client: httpx.AsyncClient, url: str, payload, **headers):
    content = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    chunks: list[bytes] = []
    async with client.stream(
        "POST", url, content=content,
        headers={"content-type": "application/json", **headers},
    ) as response:
        async for chunk in response.aiter_raw():
            chunks.append(chunk)
        return response.status_code, response.headers, b"".join(chunks)


def body(model: str) -> dict:
    return {"model": model, "stream": True, "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}]}


# ==========================================================================
# The fallback bug
# ==========================================================================


async def test_the_second_target_receives_its_own_model_id(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    """One client body, two targets, two different wire model ids.

    This is the assertion no existing test could make. `by_mode == {"5xx": 1,
    "ok": 1}` proves both targets were opened and says nothing about what they
    were asked for; before the fix both requests carried
    `"model": "rec.candidate"`, so the incumbent -- reached only because the
    candidate was already down -- would have been handed a model id its API
    has never heard of. The failure is a 404 from a healthy provider, on the
    attempt that exists to survive an unhealthy one.
    """
    status, headers, stream = await post(
        client, f"{gateway.base_url}/workloads/ab{PATHS['openai']}",
        body("rec.candidate"),
    )

    assert status == 200
    assert stream == wire.joined(wire.openai_stream())
    assert headers["x-gw-attempts"] == "2"
    assert headers["x-gw-served-by"] == "inc/rec.incumbent"
    assert headers["x-gw-body-modified"] == "1"

    assert recorder.models == [CAND_WIRE, INC_WIRE]
    # And nothing else moved: the rewrite is one key, not a re-authoring of
    # the request.
    sent = json.loads(recorder.requests[1].raw)
    assert sent["messages"] == [{"role": "user", "content": "hi"}]
    assert sent["stream"] is True and sent["max_tokens"] == 64


async def test_the_rewrite_happens_on_the_anthropic_surface_too(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    """Both surfaces carry the model in one top-level `model` key.

    Asserted rather than assumed, because "Anthropic is different" is true of
    the auth header, the version header and the ending marker, and a reader who
    has just met those three has every reason to expect it to be true here.
    """
    status, headers, stream = await post(
        client, f"{gateway.base_url}/workloads/anth/anthropic{PATHS['anthropic']}",
        body("rec.anth-candidate"),
    )

    assert status == 200
    assert stream == wire.joined(wire.anthropic_stream())
    assert headers["x-gw-served-by"] == "anth-inc/rec.anth-incumbent"
    assert recorder.models == [CAND_WIRE, INC_WIRE]
    assert recorder.requests[1].path == PATHS["anthropic"]


# ==========================================================================
# The cost of the fix, bounded
# ==========================================================================


async def test_a_client_who_named_the_wire_model_is_forwarded_byte_for_byte(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    """The exact bytes, and the header stays off.

    The body below is deliberately not what `json.dumps` would produce: odd
    spacing, a trailing key, `café` as raw UTF-8 rather than `\\u00e9`. All
    three survive, which is the difference between "we did not change the
    meaning" and "we did not change the bytes" -- and only the second one keeps
    a provider-side prompt cache hitting.
    """
    content = b'"caf\xc3\xa9 \xe6\x97\xa5\xe6\x9c\xac"'
    raw = (
        b'{ "model" : "' + DIRECT.encode() + b'" ,\n'
        b'  "messages" : [ { "role":"user" , "content":' + content + b' } ],\n'
        b'  "stream" : true }'
    )
    status, headers, stream = await post(
        client, f"{gateway.base_url}/workloads/direct{PATHS['openai']}", raw
    )

    assert status == 200
    assert stream == wire.joined(wire.openai_stream())
    assert headers["x-gw-attempts"] == "1"
    assert "x-gw-body-modified" not in headers
    assert len(recorder.requests) == 1
    assert recorder.requests[0].raw == raw, "the common path stopped being byte-exact"

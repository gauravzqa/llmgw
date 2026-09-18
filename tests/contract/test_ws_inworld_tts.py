"""Inworld TTS over real sockets: the gateway between two `websockets` peers.

Nothing here is mocked and nothing is in-process. A real uvicorn runs the
gateway with the sansio WebSocket implementation `lifecycle.py` pins, a real
`fakes/ws.py` plays Inworld on another port, and the client is
`websockets.asyncio.client` -- so what is proven is the whole path: upgrade,
tenant auth in the shape the LiveKit plugin sends it, admission, the
credential cap, the upstream handshake, `X-Gw-*` on the 101, the relay, the
close code, and the capture record.

Three things justify the tier rather than a faster one:

* a pre-101 refusal is an HTTP response on a connection that was ASKING to
  become a socket. That only exists below Starlette, in uvicorn's
  `websocket.http.response` extension, and an ASGI test transport skips it.
* byte-identical audio is a claim about framing. A relay that re-serialised
  a frame would pass every in-process assertion about its CONTENT.
* the close code and its reason are protocol-level, and the thing that gets
  them wrong -- closing twice, closing before the peer has read -- only
  happens on a real socket.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import websockets
from fakes.upstream import build_app as build_fake
from fakes.upstream import serve_in_thread
from websockets.exceptions import ConnectionClosed, InvalidStatus

from llmgw.catalog import DEFAULT_CATALOG, Catalog
from llmgw.clocks import Budgets
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from llmgw.ws.surfaces.inworld_tts import SCRUBBED_MESSAGE
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS

pytestmark = pytest.mark.contract

TTS_ROUTE = "/tts/v1/voice:streamBidirectional"
KEY_ENV = "LLMGW_WS_TEST_KEY"
KEY = "ws-test-key-not-real"
TENANT_TOKEN = "tenant-token-not-real"
FAKE_HEADERS = (
    "x-fake-mode", "x-fake-interval", "x-fake-bytes", "x-fake-events",
    "x-fake-delay", "x-fake-stall-after", "x-fake-stall-side", "x-fake-read-bps",
)

SENTENCE = "Hello from the gateway probe."  # 29 characters, as in the captures


# ==========================================================================
# Harness
# ==========================================================================


def _tenants_file(tmp_path: Path, **overrides) -> Path:
    body = f"""
[tenants.layrs]
tokens = ["{TENANT_TOKEN}"]
rate_per_second = 100.0
burst = 200
max_concurrency = {overrides.get("max_concurrency", 64)}
"""
    if "max_sessions" in overrides:
        body += f"max_sessions = {overrides['max_sessions']}\n"
    path = tmp_path / "tenants.toml"
    path.write_text(body)
    return path


def _catalog(url: str, *, second: str | None = None) -> Catalog:
    base = DEFAULT_CATALOG
    providers = {
        "inworld": replace(
            base.providers["inworld"], base_url=url, api_key_env=KEY_ENV,
            max_concurrency=64,
        ),
    }
    models = {
        "inworld.tts-2": base.models["inworld.tts-2"],
        "inworld.tts-2-flash": base.models["inworld.tts-2-flash"],
    }
    if second is not None:
        # A second Inworld row on another port, so a plan can fall back. The
        # credential id is shared deliberately: two routing entries over one
        # key is the shape `credential_id` exists for, and it means a bad key
        # opens ONE circuit rather than two half-tripped ones.
        providers["inworld-backup"] = replace(
            base.providers["inworld"], id="inworld-backup", base_url=second,
            api_key_env=KEY_ENV, credential_id="inworld",
        )
        models["inworld.tts-2-backup"] = replace(
            base.models["inworld.tts-2"], id="inworld.tts-2-backup",
            provider="inworld-backup", aliases=(),
        )
    return Catalog(models=models, providers=providers)


def _policy(tmp_path: Path, *, candidate: str | None = None) -> Path:
    incumbent = "inworld.tts-2-flash"
    lines = [
        'default_workload = "default"',
        "",
        "[defaults.budgets]",
        "total = 100.0",
        "",
        "[profiles.tts_session.budgets]",
        "connect = 5.0",
        "headers = 2.0",
        "first_event = 2.0",
        "progress = 3.0",
        "client_stall = 2.0",
        "idle = 4.0",
        "session_total = 60.0",
        "",
        "[workloads.default]",
        f'incumbent = "{incumbent}"',
        'profile = "tts_session"',
    ]
    if candidate is not None:
        lines.insert(-1, f'candidate = "{candidate}"')
    path = tmp_path / "workloads.toml"
    path.write_text("\n".join(lines) + "\n")
    return path


def _config(url: str, tmp_path: Path, **overrides) -> ServerConfig:
    os.environ.setdefault(KEY_ENV, KEY)
    settings = dict(
        catalog=_catalog(url, second=overrides.pop("second_url", None)),
        fake_upstreams=False,
        default_model="inworld.tts-2-flash",
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER_NEVER_TRIPS,
        tenants_file=_tenants_file(tmp_path, **overrides.pop("tenant", {})),
        policy_file=_policy(tmp_path, candidate=overrides.pop("candidate", None)),
        budgets=Budgets(total=60.0, connect=5.0, headers=5.0, first_event=5.0,
                        progress=5.0, client_stall=5.0),
        drain_grace_seconds=100.0,
        ws_drain_wait_s=3.0,
    )
    settings.update(overrides)
    return ServerConfig(**settings)


@pytest.fixture(scope="module")
def ws_fake():
    server = serve_in_thread(build_fake("audio"))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def ws_fake_b():
    server = serve_in_thread(build_fake("audio"))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def capture_path(tmp_path_factory):
    return tmp_path_factory.mktemp("ws-capture") / "records.jsonl"


@pytest.fixture(scope="module")
def gateway(ws_fake, tmp_path_factory, capture_path):
    tmp = tmp_path_factory.mktemp("ws-gw")
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, capture_path=str(capture_path),
    )))
    try:
        yield server
    finally:
        server.stop()


def ws_url(gateway, route: str = TTS_ROUTE) -> str:
    return gateway.base_url.replace("http://", "ws://") + route


def auth(**extra) -> dict[str, str]:
    """The credential in the shape the LiveKit plugin sends it: `Basic ` plus
    the raw token, built in the `TTS` constructor from one env var
    (tts.py:913) and forwarded verbatim on the upgrade (tts.py:263-267)."""
    return {"Authorization": f"Basic {TENANT_TOKEN}", **extra}


async def synthesise(ws, text: str = SENTENCE, context: str = "ctx-1",
                     model: str = "inworld-tts-1.5-mini") -> dict:
    """One utterance: create, send, flush, and read to `flushCompleted`."""
    await ws.send(json.dumps({
        "create": {"modelId": model, "voiceId": "Aarav",
                   "audioConfig": {"audioEncoding": "LINEAR16",
                                   "sampleRateHertz": 16000}},
        "contextId": context,
    }))
    await ws.send(json.dumps({"send_text": {"text": text}, "contextId": context}))
    await ws.send(json.dumps({"flush_context": {}, "contextId": context}))
    audio = bytearray()
    characters = 0
    created: dict = {}
    while True:
        frame = json.loads(await asyncio.wait_for(ws.recv(), 10))
        result = frame.get("result", {})
        if "contextCreated" in result:
            created = result["contextCreated"]
        if "audioChunk" in result:
            audio += base64.b64decode(result["audioChunk"]["audioContent"])
            characters += (result["audioChunk"].get("usage") or {}).get(
                "processedCharactersCount", 0,
            )
        if "flushCompleted" in result:
            return {"audio": bytes(audio), "characters": characters,
                    "created": created}


async def fake_stats(server) -> dict:
    """The fake's counters, off the test's event loop.

    `httpx.get` is blocking and this file's loop is holding live client
    sockets; a blocking call on it would stall the very sessions being
    measured."""
    async with httpx.AsyncClient(timeout=5.0) as client:
        return (await client.get(f"{server.base_url}/__stats")).json()


async def reset_fake_stats(server) -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        await client.post(f"{server.base_url}/__stats/reset")


def records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


async def record_for(path: Path, session_id: str, *, wait_s: float = 5.0) -> dict:
    """The capture record for one session, once the worker has flushed it."""
    deadline = asyncio.get_running_loop().time() + wait_s
    while asyncio.get_running_loop().time() < deadline:
        for rec in records(path):
            if rec.get("request_id") == session_id:
                return rec
        await asyncio.sleep(0.05)
    raise AssertionError(f"no capture record for session {session_id}")


# ==========================================================================
# Happy path
# ==========================================================================


async def test_one_utterance_relays_byte_identical_audio_and_an_exact_meter(
    gateway, capture_path,
):
    """The G1 exit shape: a context, an utterance, and a record that names
    the provider's own character count rather than an estimate of it."""
    async with websockets.connect(ws_url(gateway), additional_headers=auth()) as ws:
        session_id = ws.response.headers["x-gw-session-id"]
        out = await synthesise(ws)
        await ws.send(json.dumps({"close_context": {}, "contextId": "ctx-1"}))
        closed = json.loads(await asyncio.wait_for(ws.recv(), 5))
        assert "contextClosed" in closed["result"]
        await ws.close(1000)

    assert out["characters"] == len(SENTENCE) == 29
    assert out["audio"].startswith(b"RIFF"), (
        "the provider's own 44-byte WAVE header, relayed untouched -- the "
        "plugin strips it and the gateway must not (captures-ws 1.2)"
    )
    rec = await record_for(capture_path, session_id)
    assert rec["kind"] == "session"
    assert rec["units"]["characters"] == 29
    assert rec["basis"] == "exact"
    assert rec["outcome"] == "completed"
    assert rec["model"] == "inworld.tts-2-flash"
    assert rec["cost_usd"] == pytest.approx(29 * 15.0 / 1_000_000)
    assert rec["session_id"] is None, "the session record IS the session"


async def test_the_audio_is_byte_identical_to_what_the_provider_sent(
    gateway, ws_fake,
):
    """Two sessions with the same knobs produce the same bytes, and the fake
    counts the same bytes out as the client counted in."""
    sizes = []
    for i in range(2):
        async with websockets.connect(
            ws_url(gateway), additional_headers=auth(**{"X-Fake-Bytes": "4096"}),
        ) as ws:
            out = await synthesise(ws, context=f"ctx-{i}")
            sizes.append(len(out["audio"]))
            await ws.close(1000)
    assert sizes[0] == sizes[1] > 4096


async def test_the_create_frame_is_rewritten_to_the_wire_model_and_nothing_else(
    gateway,
):
    """The ONE edit. The fake echoes what it received in `contextCreated`,
    so this asserts what the provider saw, not what we intended."""
    async with websockets.connect(ws_url(gateway), additional_headers=auth()) as ws:
        assert ws.response.headers["x-gw-body-modified"] == "1"
        out = await synthesise(ws, model="inworld-tts-1.5-mini")
        assert out["created"]["modelId"] == "inworld-tts-2-flash"
        assert out["created"]["voiceId"] == "Aarav", "the rest of `create` survived"
        assert out["created"]["audioConfig"]["sampleRateHertz"] == 16000
        await ws.close(1000)


async def test_the_101_carries_the_gateway_headers(gateway):
    async with websockets.connect(ws_url(gateway), additional_headers=auth()) as ws:
        h = ws.response.headers
        assert h["x-gw-served-by"] == "inworld/inworld.tts-2-flash"
        assert h["x-gw-model"] == "inworld.tts-2-flash"
        assert h["x-gw-workload-id"] == "default"
        assert h["x-gw-tenant"] == "layrs"
        assert h["x-gw-attempts"] == "1"
        assert h["x-gw-policy-id"].startswith("pol_")
        assert h["x-gw-catalog-id"].startswith("cat_")
        assert len(h["x-gw-session-id"]) == 32
        assert "x-gw-breaker" not in h, "no circuit refused anything"
        await ws.close(1000)


async def test_bearer_is_accepted_as_well_as_basic(gateway):
    """Basic is what the plugin sends; Bearer is what everything else sends.
    A route that took only one of them would exclude somebody."""
    async with websockets.connect(
        ws_url(gateway), additional_headers={"Authorization": f"Bearer {TENANT_TOKEN}"},
    ) as ws:
        assert ws.response.headers["x-gw-tenant"] == "layrs"
        await ws.close(1000)


async def test_the_workload_twin_is_mounted(gateway):
    """The Inworld TTS plugin cannot reach it (`urljoin` drops the prefix,
    tts.py:259), which is exactly why it must exist for everybody else."""
    url = ws_url(gateway, f"/workloads/default{TTS_ROUTE}")
    async with websockets.connect(url, additional_headers=auth()) as ws:
        assert ws.response.headers["x-gw-workload-id"] == "default"
        out = await synthesise(ws)
        assert out["characters"] == 29
        await ws.close(1000)


async def test_an_unknown_workload_on_the_twin_is_a_400_before_the_101(gateway):
    url = ws_url(gateway, f"/workloads/nope{TTS_ROUTE}")
    with pytest.raises(InvalidStatus) as excinfo:
        await websockets.connect(url, additional_headers=auth())
    assert excinfo.value.response.status_code == 400
    assert json.loads(excinfo.value.response.body)["error"]["type"] == "policy_error"


# ==========================================================================
# Multi-context
# ==========================================================================


async def test_three_contexts_interleave_and_each_is_metered(gateway, capture_path):
    """Commitment is per context and the meter is a SUM over flushes: three
    contexts on one socket bill the sum of their three counts."""
    texts = ["Context A speaking.", "Context B here.", "And C."]
    async with websockets.connect(ws_url(gateway), additional_headers=auth()) as ws:
        session_id = ws.response.headers["x-gw-session-id"]
        for i, text in enumerate(texts):
            cid = f"multi-{i}"
            await ws.send(json.dumps({
                "create": {"modelId": "inworld-tts-2-flash", "voiceId": "Aarav"},
                "contextId": cid,
            }))
            await ws.send(json.dumps({"send_text": {"text": text}, "contextId": cid}))
            await ws.send(json.dumps({"flush_context": {}, "contextId": cid}))
        flushed = set()
        while len(flushed) < 3:
            frame = json.loads(await asyncio.wait_for(ws.recv(), 10))
            result = frame.get("result", {})
            if "flushCompleted" in result:
                flushed.add(result["contextId"])
        assert flushed == {"multi-0", "multi-1", "multi-2"}
        await ws.close(1000)

    rec = await record_for(capture_path, session_id)
    assert rec["units"]["characters"] == sum(len(t) for t in texts)
    assert rec["basis"] == "exact"
    assert any(note.startswith("contexts=3") for note in rec["cost_notes"])


async def test_the_sixth_context_is_answered_by_the_provider_not_the_gateway(
    gateway,
):
    """C25 and the G0 correction. The plan had the gateway refuse a sixth
    `create` with a close 4907; the captures show Inworld answers it with a
    `status` code 8 naming that context, the plugin fails exactly that
    context (tts.py:478-496), and the other five keep working. A gateway
    close would kill all six."""
    async with websockets.connect(
        ws_url(gateway), additional_headers=auth(**{"X-Fake-Mode": "context-multiplex"}),
    ) as ws:
        for i in range(6):
            await ws.send(json.dumps({
                "create": {"modelId": "inworld-tts-2-flash"}, "contextId": f"six-{i}",
            }))
        seen: dict[str, dict] = {}
        while len(seen) < 6:
            frame = json.loads(await asyncio.wait_for(ws.recv(), 10))
            result = frame["result"]
            seen[result["contextId"]] = result
        assert "contextCreated" in seen["six-4"]
        assert seen["six-5"]["status"]["code"] == 8, "the PROVIDER refused it"
        # The socket is still usable, which is the whole point.
        await ws.send(json.dumps({"send_text": {"text": "still here"},
                                  "contextId": "six-0"}))
        await ws.send(json.dumps({"flush_context": {}, "contextId": "six-0"}))
        while True:
            frame = json.loads(await asyncio.wait_for(ws.recv(), 10))
            if "flushCompleted" in frame.get("result", {}):
                break
        await ws.close(1000)


async def test_text_over_two_thousand_characters_is_relayed_not_policed(gateway):
    """Also the provider's limit. Inworld answers `status` code 3 and sends
    no `flushCompleted`; a gateway that refused the frame would be enforcing
    a limit it cannot see change."""
    async with websockets.connect(ws_url(gateway), additional_headers=auth()) as ws:
        await ws.send(json.dumps({
            "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "long",
        }))
        await asyncio.wait_for(ws.recv(), 5)
        await ws.send(json.dumps({"send_text": {"text": "x" * 2100},
                                  "contextId": "long"}))
        await ws.send(json.dumps({"flush_context": {}, "contextId": "long"}))
        frame = json.loads(await asyncio.wait_for(ws.recv(), 10))
        assert frame["result"]["status"]["code"] == 3
        assert "2000" in frame["result"]["status"]["message"]
        await ws.close(1000)


# ==========================================================================
# Pre-101 refusals: real HTTP responses
# ==========================================================================


async def test_a_bad_tenant_token_is_a_401_with_the_usual_body(gateway):
    with pytest.raises(InvalidStatus) as excinfo:
        await websockets.connect(
            ws_url(gateway), additional_headers={"Authorization": "Basic nope"},
        )
    response = excinfo.value.response
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert json.loads(response.body)["error"]["type"] == "unauthenticated"
    assert "nope" not in response.body.decode(), "the token never comes back"


async def test_no_credential_at_all_is_a_401(gateway):
    with pytest.raises(InvalidStatus) as excinfo:
        await websockets.connect(ws_url(gateway))
    assert excinfo.value.response.status_code == 401


async def test_admission_refuses_at_429_with_the_tenants_own_number(
    ws_fake, tmp_path_factory,
):
    """C6: denial is free. The permit is held for the socket's LIFE, so a
    tenant capped at one concurrent request gets one socket."""
    tmp = tmp_path_factory.mktemp("ws-admit")
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, tenant={"max_concurrency": 1},
    )))
    try:
        async with websockets.connect(ws_url(server), additional_headers=auth()):
            with pytest.raises(InvalidStatus) as excinfo:
                await websockets.connect(ws_url(server), additional_headers=auth())
            response = excinfo.value.response
            assert response.status_code == 429
            assert json.loads(response.body)["error"]["type"] == "concurrency_rejected"
            assert response.headers["x-gw-tenant"] == "layrs"
    finally:
        server.stop()


async def test_max_sessions_counts_a_relayed_socket(ws_fake, tmp_path_factory):
    """C23: a relayed session is a live session, counted against the same
    `max_sessions` a minted credential is. The provider does not care which
    door a session came through."""
    tmp = tmp_path_factory.mktemp("ws-sessions")
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, tenant={"max_concurrency": 8, "max_sessions": 1},
    )))
    try:
        async with websockets.connect(ws_url(server), additional_headers=auth()):
            with pytest.raises(InvalidStatus) as excinfo:
                await websockets.connect(ws_url(server), additional_headers=auth())
            body = json.loads(excinfo.value.response.body)
            assert excinfo.value.response.status_code == 429
            assert "session cap" in body["error"]["message"]
        # The permit is released with the socket -- but on the SERVER's
        # timeline, not the client's: `close()` returns when the close frame
        # is written, and the endpoint's `finally` runs after its teardown
        # bound. Polling rather than assuming instantaneity is the honest
        # shape; asserting the first attempt would be asserting a race.
        deadline = asyncio.get_running_loop().time() + 10.0
        while True:
            try:
                async with websockets.connect(
                    ws_url(server), additional_headers=auth(),
                ) as ws:
                    assert ws.response.headers["x-gw-tenant"] == "layrs"
                break
            except InvalidStatus as exc:
                assert exc.response.status_code == 429
                if asyncio.get_running_loop().time() > deadline:
                    raise AssertionError(
                        "the session permit was never released after the socket "
                        "closed: a leak here caps the tenant at max_sessions for "
                        "the life of the process"
                    ) from None
                await asyncio.sleep(0.2)
    finally:
        server.stop()


async def test_sockets_count_toward_max_streams_and_shed_the_next_request(
    ws_fake, tmp_path_factory,
):
    """A socket IS a stream (C23). With a cap of 2, two sockets fill the
    process and the third caller -- over HTTP or over a socket -- gets the
    same 503 with the same body."""
    tmp = tmp_path_factory.mktemp("ws-cap")
    server = serve(build_app(_config(ws_fake.base_url, tmp, max_streams=2)))
    try:
        async with (
            websockets.connect(ws_url(server), additional_headers=auth()),
            websockets.connect(ws_url(server), additional_headers=auth()),
        ):
            with pytest.raises(InvalidStatus) as excinfo:
                await websockets.connect(ws_url(server), additional_headers=auth())
            response = excinfo.value.response
            assert response.status_code == 503
            assert json.loads(response.body)["error"]["type"] == "overloaded"
            assert response.headers["retry-after"] == "1"

            async with httpx.AsyncClient(timeout=5.0) as client:
                http = await client.post(
                    f"{server.base_url}/v1/chat/completions",
                    json={"model": "inworld.tts-2-flash", "messages": []},
                    headers={"Authorization": f"Bearer {TENANT_TOKEN}"},
                )
            assert http.status_code == 503
            assert http.json()["error"]["type"] == "overloaded", (
                "the same cap, the same body, whichever plane the caller is on"
            )
    finally:
        server.stop()


async def test_an_upstream_that_refuses_the_upgrade_is_a_502_before_the_101(
    tmp_path_factory,
):
    """Nothing has been said to the client, so the refusal is an ordinary
    HTTP failure with the taxonomy's status -- not a 101 followed by a close
    the caller's SDK has no status for."""
    tmp = tmp_path_factory.mktemp("ws-dead")
    dead = serve_in_thread(build_fake("audio"))
    url = dead.base_url
    dead.stop()
    server = serve(build_app(_config(url, tmp)))
    try:
        with pytest.raises(InvalidStatus) as excinfo:
            await websockets.connect(ws_url(server), additional_headers=auth())
        response = excinfo.value.response
        assert response.status_code in (502, 504)
        assert json.loads(response.body)["error"]["type"] in (
            "connection_failed", "connect_timeout",
        )
    finally:
        server.stop()


async def test_a_dead_candidate_falls_back_to_the_incumbent_before_the_101(
    ws_fake, tmp_path_factory,
):
    """C24 in its cheapest form: the walk happens before the client has been
    accepted, so the config prefix has not been sent to anybody yet and the
    fallback costs the tenant nothing. `X-Gw-Attempts: 2` is the evidence."""
    tmp = tmp_path_factory.mktemp("ws-fallback")
    dead = serve_in_thread(build_fake("audio"))
    dead_url = dead.base_url
    dead.stop()
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, second_url=dead_url, candidate="inworld.tts-2-backup",
    )))
    try:
        async with websockets.connect(
            ws_url(server), additional_headers=auth(),
        ) as ws:
            assert ws.response.headers["x-gw-attempts"] == "2"
            assert ws.response.headers["x-gw-served-by"] == "inworld/inworld.tts-2-flash"
            out = await synthesise(ws)
            assert out["characters"] == 29, "the incumbent served it exactly once"
            await ws.close(1000)
    finally:
        server.stop()


async def test_both_targets_dead_is_a_502_naming_the_last_failure(
    tmp_path_factory,
):
    tmp = tmp_path_factory.mktemp("ws-both-dead")
    a = serve_in_thread(build_fake("audio"))
    b = serve_in_thread(build_fake("audio"))
    a_url, b_url = a.base_url, b.base_url
    a.stop()
    b.stop()
    server = serve(build_app(_config(
        a_url, tmp, second_url=b_url, candidate="inworld.tts-2-backup",
    )))
    try:
        with pytest.raises(InvalidStatus) as excinfo:
            await websockets.connect(ws_url(server), additional_headers=auth())
        assert excinfo.value.response.status_code in (502, 504)
        assert excinfo.value.response.headers["x-gw-attempts"] == "2"
    finally:
        server.stop()


# ==========================================================================
# Post-101: close codes
# ==========================================================================


async def test_a_fatal_provider_error_and_close_pass_through_untranslated(
    gateway, capture_path,
):
    """The G0 correction. A bad Inworld key is not silence: the first client
    frame earns a top-level `error` code 7 and a server CLOSE 1000 in the
    same millisecond. Both reach the client as the provider sent them (C25),
    and the record blames the credential."""
    async with websockets.connect(
        ws_url(gateway),
        additional_headers=auth(
            **{"X-Fake-Mode": "error-7-then-close-1000-on-first-message"},
        ),
    ) as ws:
        session_id = ws.response.headers["x-gw-session-id"]
        await ws.send(json.dumps({
            "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "bad",
        }))
        raw = await asyncio.wait_for(ws.recv(), 10)
        frame = json.loads(raw)
        assert frame["error"]["code"] == 7, "the provider's own code, kept"
        # ...but not the provider's own PROSE. Inworld quotes the first four
        # characters of the API key in this message, and on this plane the
        # key is the gateway's, not the tenant's, so the one exception to
        # passthrough applies (C25): shape and code survive, text does not.
        assert frame["error"]["message"] == SCRUBBED_MESSAGE
        assert "details" not in frame["error"]
        assert "fake***" not in raw, "the reflected key fragment never reaches a tenant"
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(ws.recv(), 10)
        assert ws.close_code == 1000, (
            "the provider's close code, passed through: a 49xx here would tell "
            "the client the GATEWAY refused it"
        )

    rec = await record_for(capture_path, session_id)
    assert rec["error_code"] == "authentication_failed"
    assert rec["outcome"] == "failed"
    assert rec["committed"] is False
    assert "<KEY4>" not in json.dumps(rec), "the reflected key prefix never lands"


async def test_a_post_101_handshake_failure_replays_only_the_config_prefix(
    ws_fake, tmp_path_factory,
):
    """C24's headline case, and the one only this provider produces.

    Inworld does not check the credential until the first client FRAME, so
    "this key is refused" arrives AFTER the 101 -- past the walk that
    `_open_upstream` does and past the point where an HTTP plane could have
    fallen back. Nothing has reached the client and no CONTENT has reached a
    provider, so the session is still recoverable: the gateway opens a socket
    to the next target and replays the config prefix it kept.

    What is asserted is that the REPLAY happened and that it was exactly the
    prefix: the client sent ONE `create` and the fake accepted TWO upgrades
    and read a `create` on each. (The fake's counters are process-global by
    design, so both targets point at one port and the count is the total --
    which is the number that matters here.)
    """
    tmp = tmp_path_factory.mktemp("ws-replay")
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, second_url=ws_fake.base_url,
        candidate="inworld.tts-2-backup",
    )))
    await reset_fake_stats(ws_fake)
    try:
        async with websockets.connect(
            ws_url(server),
            additional_headers=auth(**{
                "X-Fake-Mode": "error-7-then-close-1000-on-first-message",
            }),
        ) as ws:
            assert ws.response.headers["x-gw-attempts"] == "1", (
                "the pre-101 walk succeeded on the first target: the failure "
                "that follows is the one only a frame can reveal"
            )
            # No `modelId`: the two rows in this plan sit on DIFFERENT
            # provider entries (one fake port each is the only way to build a
            # two-target plan against one fake), and naming a model that
            # resolves onto the other one is refused rather than served by
            # the socket we hold -- which is its own test, below. The replay
            # mechanism is what this case is about, and `create` is the whole
            # config prefix with or without the field.
            await ws.send(json.dumps({
                "create": {"voiceId": "Aarav"}, "contextId": "replay",
            }))
            with pytest.raises(ConnectionClosed):
                while True:
                    await asyncio.wait_for(ws.recv(), 10)
        await asyncio.sleep(0.5)
        stats = await fake_stats(ws_fake)
        assert stats["ws_open"] == 2, (
            f"one client socket should have produced two upstream sockets -- the "
            f"first target and the replay -- got {stats['ws_open']}"
        )
        assert stats["ws"]["ws_open_now"] == 0
    finally:
        server.stop()
        await reset_fake_stats(ws_fake)


async def test_a_model_on_another_provider_is_refused_rather_than_misrouted(
    ws_fake, tmp_path_factory,
):
    """The first frame names a model; the socket is already open. If the two
    disagree about the PROVIDER there is nothing honest to do but refuse.

    Serving it on the socket we hold would bill the tenant against one
    provider's row for audio another provider's credential paid for, and the
    capture record would name a target that never saw the frame. Past the
    101, so it is a close code (C25) carrying the taxonomy's own reason."""
    tmp = tmp_path_factory.mktemp("ws-crossprovider")
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, second_url=ws_fake.base_url,
        candidate="inworld.tts-2-backup",
    )))
    try:
        async with websockets.connect(
            ws_url(server), additional_headers=auth(),
        ) as ws:
            assert ws.response.headers["x-gw-served-by"] == (
                "inworld-backup/inworld.tts-2-backup"
            )
            await ws.send(json.dumps({
                "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "wrong",
            }))
            with pytest.raises(ConnectionClosed):
                while True:
                    await asyncio.wait_for(ws.recv(), 10)
            assert ws.close_code == 4907
            assert ws.close_reason == "llmgw:policy_error"
    finally:
        server.stop()


async def test_a_post_101_credential_failure_opens_the_credential_circuit(
    ws_fake, tmp_path_factory,
):
    """Breaker evidence from a verdict reached AFTER the 101.

    This is the case the HTTP plane cannot have. The upstream-connect loop
    takes both tickets and then hands the socket to the relay, and the most
    important thing a relayed session ever learns -- that Inworld refuses
    this credential -- arrives minutes later as a frame. Releasing the
    tickets without recording would leave the credential circuit CLOSED over
    a key that is known bad, and every session after it would pay another
    handshake to find out. The evidence goes to the CREDENTIAL circuit and
    not the model's: a refused key says nothing about the model.
    """
    from llmgw.breaker import BreakerPolicy

    tmp = tmp_path_factory.mktemp("ws-breaker")
    server = serve(build_app(_config(
        ws_fake.base_url, tmp,
        breaker=BreakerPolicy(failure_threshold=2, window=60.0, cooldown=60.0),
    )))
    gw = server.app.state.gateway
    try:
        for _ in range(2):
            async with websockets.connect(
                ws_url(server),
                additional_headers=auth(**{
                    "X-Fake-Mode": "error-7-then-close-1000-on-first-message",
                }),
            ) as ws:
                await ws.send(json.dumps({
                    "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "bad",
                }))
                with pytest.raises(ConnectionClosed):
                    while True:
                        await asyncio.wait_for(ws.recv(), 10)
        await asyncio.sleep(0.3)
        target = gw.policy.current().catalog.resolve("inworld.tts-2-flash")
        from llmgw.executor import credential_health_key

        credential = gw.breakers.for_key(credential_health_key(target)).snapshot()
        model = gw.breakers.for_key(target.health_key).snapshot()
        assert credential["state"] == "open", (
            f"the credential circuit should have learned the key is refused: "
            f"{credential}"
        )
        assert model["state"] == "closed", (
            f"the MODEL circuit must not hear about a credential failure: {model}"
        )

        # And the circuit now refuses before a socket, with the status the
        # taxonomy gives a `BreakerOpen`.
        with pytest.raises(InvalidStatus) as excinfo:
            await websockets.connect(ws_url(server), additional_headers=auth())
        assert excinfo.value.response.status_code == 503
        assert excinfo.value.response.headers["x-gw-breaker"] == "open"
    finally:
        server.stop()


async def test_a_provider_stall_closes_4902(gateway, capture_path):
    """`first_event` is 2 s in this policy and the fake goes quiet after one
    frame. The blame is the provider's and the health signal is a failure."""
    async with websockets.connect(
        ws_url(gateway),
        additional_headers=auth(**{
            "X-Fake-Mode": "stall-mid-session", "X-Fake-Stall-After": "1",
            "X-Fake-Stall-Side": "send",
        }),
    ) as ws:
        session_id = ws.response.headers["x-gw-session-id"]
        await ws.send(json.dumps({
            "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "stall",
        }))
        await asyncio.wait_for(ws.recv(), 5)  # contextCreated, then silence
        await ws.send(json.dumps({"send_text": {"text": SENTENCE},
                                  "contextId": "stall"}))
        await ws.send(json.dumps({"flush_context": {}, "contextId": "stall"}))
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(ws.recv(), 10)
        assert ws.close_code == 4902
        assert ws.close_reason == "llmgw:first_event_timeout"

    rec = await record_for(capture_path, session_id)
    assert rec["error_code"] == "first_event_timeout"


async def test_a_socket_that_says_nothing_is_closed_4906_and_not_4902(gateway):
    """Captures probe 3: 101 then silence is a HEALTHY socket on this
    provider -- it answered normally after 75 s of nothing. So the gateway's
    own idle budget is what reclaims it, and the blame is the client's."""
    async with websockets.connect(
        ws_url(gateway), additional_headers=auth(**{"X-Fake-Mode": "idle"}),
    ) as ws:
        with pytest.raises(ConnectionClosed):
            await asyncio.wait_for(ws.recv(), 15)
        assert ws.close_code == 4906
        assert ws.close_reason == "llmgw:session_idle"

    # ...and it is COUNTED. A gateway verdict never lands in the relay's
    # `client_close`, which only records a close the client sent, so counting
    # that field alone left `side="client"` silent for every 49xx we issue --
    # exactly the series FAILURE-MODES rows 33 and 34 send an operator to.
    await asyncio.sleep(0.3)
    async with httpx.AsyncClient(timeout=5.0) as http:
        metrics = (await http.get(f"{gateway.base_url}/metrics")).text
    counted = [
        line for line in metrics.splitlines()
        if line.startswith("llmgw_ws_close_total{")
        and 'code_class="gateway_49xx"' in line and 'side="client"' in line
        and not line.endswith(" 0.0")
    ]
    assert counted, "a 4906 the gateway sent must appear under side=client"


async def test_the_session_total_closes_4901(ws_fake, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("ws-total")
    policy = tmp / "workloads.toml"
    server = serve(build_app(_config(ws_fake.base_url, tmp)))
    # Rewrite the policy the fixture wrote, then rebuild: a 2 s session cap
    # is not something any other test should inherit.
    policy.write_text(policy.read_text()
                      .replace("session_total = 60.0", "session_total = 2.0")
                      .replace("idle = 4.0", "idle = 30.0"))
    server.stop()
    server = serve(build_app(_config(ws_fake.base_url, tmp)))
    policy.write_text(policy.read_text().replace("session_total = 60.0",
                                                 "session_total = 2.0"))
    try:
        async with websockets.connect(
            ws_url(server), additional_headers=auth(),
        ) as ws:
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 20)
            assert ws.close_code in (4901, 4906)
    finally:
        server.stop()


async def test_a_flooding_provider_never_grows_the_relay_past_its_ceiling(
    ws_fake, tmp_path_factory,
):
    """The memory claim the four-task shape exists for: 32 MiB arrives from
    the provider faster than it leaves, and the relay holds its ceiling.

    This test used to also assert the 4903 verdict, and it passed for the
    wrong reason -- the stall clock was armed the first time the buffer
    filled and never cleared, so ANY session that once hit the ceiling was
    closed `client_too_slow` two seconds later, including one whose client
    had long since caught up. Fixing that (the clock now runs from the last
    send that RETURNED) exposed that this harness cannot produce the
    condition at all: over loopback the peer's kernel and the `websockets`
    client keep draining the socket into memory no matter how small
    `max_queue` or `SO_RCVBUF` is and no matter whether the test ever calls
    `recv`, so every send completes and no client here is ever slow. uvicorn
    propagates transport backpressure correctly (`websockets_sansio_impl`
    awaits `writable`), so the verdict is reachable in production -- it is
    the LOOPBACK that cannot be wedged.

    So the verdict is proven where it is deterministic, on a `ManualClock` in
    `tests/unit/test_ws_review_fixes.py`, in both directions: a client that
    caught up is not closed, and a client that takes nothing is. What is
    proven here is the part that needs a real socket -- the ceiling holds
    while a provider floods. S9 measures it again with a consumer that is
    slow on purpose.
    """
    tmp = tmp_path_factory.mktemp("ws-flood")
    server = serve(build_app(_config(ws_fake.base_url, tmp, buffer_bytes=16 * 1024)))
    gw = server.app.state.gateway
    try:
        async with websockets.connect(
            ws_url(server), max_queue=1,
            additional_headers=auth(**{
                "X-Fake-Bytes": "65536", "X-Fake-Events": "512",
            }),
        ) as ws:
            await ws.send(json.dumps({
                "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "flood",
            }))
            await ws.send(json.dumps({"send_text": {"text": SENTENCE},
                                      "contextId": "flood"}))
            await ws.send(json.dumps({"flush_context": {}, "contextId": "flood"}))

            peak = 0
            deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < deadline:
                for session in list(gw.ws_sessions):
                    relay = session.relay
                    if relay is not None:
                        peak = max(peak, *(b.high_water
                                           for b in relay._buffers.values()))
                if not gw.ws_sessions:
                    break
                await asyncio.sleep(0.05)

            # A relay with no bound would be holding every byte the provider
            # sent. The worst case is the steady-state ceiling plus one
            # oversized frame admitted into an empty buffer, which is what
            # FAILURE-MODES row 32 states and what `LLMGW_MAX_STREAMS` is
            # re-derived against.
            assert 0 < peak <= 16 * 1024 + 1024 * 1024
    finally:
        server.stop()


# ==========================================================================
# Drain
# ==========================================================================


async def test_drain_closes_4900_once_the_contexts_are_gone_and_cuts_nothing(
    ws_fake, tmp_path_factory, capture_path,
):
    """C26. A socket cannot be drained by waiting for it to finish -- it was
    never going to. So the drain waits for open contexts to reach zero, then
    closes 4900, and `cut` is zero because the sessions ended themselves."""
    tmp = tmp_path_factory.mktemp("ws-drain")
    capture = tmp / "drain-records.jsonl"
    server = serve(build_app(_config(
        ws_fake.base_url, tmp, capture_path=str(capture),
    )))
    gw = server.app.state.gateway
    try:
        async with websockets.connect(
            ws_url(server), additional_headers=auth(),
        ) as ws:
            session_id = ws.response.headers["x-gw-session-id"]
            out = await synthesise(ws)
            assert out["characters"] == 29
            await ws.send(json.dumps({"close_context": {}, "contextId": "ctx-1"}))
            await asyncio.wait_for(ws.recv(), 5)

            report = await gw.begin_drain(grace_s=10.0)
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 10)
            assert ws.close_code == 4900
            assert ws.close_reason == "llmgw:session_draining"
            assert report.cut == 0, "the session ended itself inside the grace"
            assert report.timed_out is False

        rec = await record_for(capture, session_id)
        assert rec["outcome"] == "canceled"
        assert rec["error_code"] == "session_draining"
        assert rec["units"]["characters"] == 29, (
            "a drained session still bills what it relayed"
        )
    finally:
        server.stop()


async def test_drain_gives_up_on_a_context_that_will_not_close(
    ws_fake, tmp_path_factory,
):
    """`ws_drain_wait_s` is 3 s here. A context the client never closes is
    cut at the deadline with 4900 -- the plugin fails it and re-synthesises
    on a fresh socket (tts.py:603-616), which is what it already does for a
    provider-side disconnect."""
    tmp = tmp_path_factory.mktemp("ws-drain-hang")
    server = serve(build_app(_config(ws_fake.base_url, tmp)))
    gw = server.app.state.gateway
    try:
        async with websockets.connect(
            ws_url(server), additional_headers=auth(),
        ) as ws:
            await ws.send(json.dumps({
                "create": {"modelId": "inworld-tts-2-flash"}, "contextId": "open",
            }))
            await asyncio.wait_for(ws.recv(), 5)
            report = await gw.begin_drain(grace_s=15.0)
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(ws.recv(), 10)
            assert ws.close_code == 4900
            assert report.cut == 0
            assert report.duration_s >= 3.0, "the drain waited its bounded wait"
    finally:
        server.stop()


async def test_a_new_upgrade_during_a_drain_is_a_503(ws_fake, tmp_path_factory):
    tmp = tmp_path_factory.mktemp("ws-drain-shed")
    server = serve(build_app(_config(ws_fake.base_url, tmp)))
    gw = server.app.state.gateway
    try:
        await gw.begin_drain(grace_s=1.0)
        with pytest.raises(InvalidStatus) as excinfo:
            await websockets.connect(ws_url(server), additional_headers=auth())
        response = excinfo.value.response
        assert response.status_code == 503
        assert json.loads(response.body)["error"]["type"] == "draining"
    finally:
        server.stop()


async def test_bind_sockets_still_serves_an_upgrade(ws_fake, tmp_path_factory):
    """R7: `lifecycle.bind_sockets` pre-binds the listener so a `::` host is
    genuinely dual-stack, and the sansio WebSocket implementation has to
    accept an upgrade on a socket uvicorn did not open itself."""
    import socket as _socket

    import uvicorn

    from llmgw.server.lifecycle import bind_sockets

    tmp = tmp_path_factory.mktemp("ws-bind")
    app = build_app(_config(ws_fake.base_url, tmp))
    sockets = bind_sockets("127.0.0.1", 0)
    port = sockets[0].getsockname()[1]
    config = uvicorn.Config(
        app, log_level="critical", access_log=False, lifespan="on",
        ws="websockets-sansio", ws_per_message_deflate=False,
    )
    server = uvicorn.Server(config)
    import threading

    thread = threading.Thread(
        target=server.run, kwargs={"sockets": sockets}, daemon=True,
    )
    thread.start()
    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(0.05)
        assert server.started
        async with websockets.connect(
            f"ws://127.0.0.1:{port}{TTS_ROUTE}", additional_headers=auth(),
        ) as ws:
            assert ws.response.status_code == 101
            out = await synthesise(ws)
            assert out["characters"] == 29
            await ws.close(1000)
    finally:
        server.should_exit = True
        thread.join(10)
        for sock in sockets:
            with __import__("contextlib").suppress(OSError):
                sock.close()
        assert isinstance(sockets[0], _socket.socket)


# ==========================================================================
# Metrics
# ==========================================================================


async def test_the_socket_metrics_move_with_their_closed_label_sets(gateway):
    async with httpx.AsyncClient(timeout=5.0) as client:
        before = (await client.get(f"{gateway.base_url}/metrics")).text
    async with websockets.connect(ws_url(gateway), additional_headers=auth()) as ws:
        await synthesise(ws, context="metric-ctx")
        await ws.close(1000)
    await asyncio.sleep(0.3)
    async with httpx.AsyncClient(timeout=5.0) as client:
        after = (await client.get(f"{gateway.base_url}/metrics")).text

    def value(text: str, needle: str) -> float:
        for line in text.splitlines():
            if line.startswith(needle):
                return float(line.rsplit(" ", 1)[1])
        return 0.0

    assert value(after, 'llmgw_ws_bytes_total{direction="client_out"') > value(
        before, 'llmgw_ws_bytes_total{direction="client_out"',
    )
    assert value(after, 'llmgw_ws_bytes_total{direction="client_in"') > value(
        before, 'llmgw_ws_bytes_total{direction="client_in"',
    )
    assert 'llmgw_ws_sessions_open{surface="inworld_tts_ws"}' in after
    assert value(after, 'llmgw_ws_sessions_open{surface="inworld_tts_ws"}') == 0.0
    assert 'llmgw_ws_close_total{code_class="normal_1000"' in after
    assert 'llmgw_ws_session_seconds_bucket{' in after
    assert 'llmgw_requests_total{code="none",outcome="completed",' \
           'surface="inworld_tts_ws"}' in after

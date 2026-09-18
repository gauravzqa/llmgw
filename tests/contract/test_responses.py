"""`POST /v1/responses` over real sockets against the fake (PLAN-2 Phase F).

What only the wire can prove: that the semantic frames reach the client byte
for byte with their `event:` lines and without a `[DONE]` the dialect never
had; that a `response.failed` or `event: error` upstream sent is forwarded
and nothing follows it (C2, third row; C22); that the buffered body's `model`
comes back as the catalog id while the streamed one keeps the provider's
snapshot and the alias table accepts it on the next turn (A1); that usage,
stop reason and hosted-tool counts land on the capture record; and that
`background: true` and a stateful body bound for a stateless provider are
refused before any upstream call.

Two gateways: `gateway` is the shipped catalog pointed at the fakes (so
`openai.gpt-4o-mini` and `deepseek.deepseek-v4-flash` are real rows) with a
capture file; `ab` is the two-target harness from `_phase_a_harness` for the
fallback assertion.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import httpx
import pytest
from fakes import responses as R
from fakes.upstream import RESPONSES_PATH

from llmgw.breaker import BreakerPolicy
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig, fake_catalog
from tests.contract._phase_a_harness import (
    CANDIDATE_MODEL,
    GatewayPool,
    fake_mode_counts,
    mode,
)
from tests.contract.conftest import Fakes
from tests.contract.test_passthrough import _serve

pytestmark = pytest.mark.contract

ROUTE = RESPONSES_PATH
FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay",
                "x-fake-status")

AB_POLICY = """
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
"""


def body(model: str = "fake.echo", *, stream: bool = True, **extra) -> dict:
    # `max_output_tokens` >= 16: OpenAI 400s below that (probe 9).
    return {"model": model, "input": "hi", "stream": stream, "max_output_tokens": 64,
            **extra}


# ------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def capture_file(tmp_path_factory) -> Path:
    return tmp_path_factory.mktemp("responses") / "capture.ndjson"


@pytest.fixture(scope="module")
def gateway(fakes: Fakes, capture_file: Path):
    catalog = fake_catalog(
        openai_url=f"{fakes.openai.base_url}/v1", anthropic_url=fakes.anthropic.base_url,
    )
    config = ServerConfig(
        catalog=catalog, fake_upstreams=True,
        forward_request_headers=FAKE_HEADERS,
        breaker=BreakerPolicy(failure_threshold=1_000_000),
        capture_path=str(capture_file),
    )
    server = _serve(build_app(config))
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def ab(fakes: Fakes, tmp_path_factory):
    policy = tmp_path_factory.mktemp("responses-ab") / "policy.toml"
    policy.write_text(AB_POLICY, encoding="utf-8")
    pool = GatewayPool(fakes, str(policy))
    try:
        yield pool
    finally:
        pool.stop_all()


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=httpx.Timeout(15.0)) as c:
        yield c


class Records:
    """The capture file, read by delta: `mark()` before a request,
    `newest()` after it. The drain worker writes asynchronously, so
    `newest` polls briefly rather than asserting on an empty file."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._seen = 0

    def _lines(self) -> list[dict]:
        if not self.path.exists():
            return []
        return [json.loads(ln) for ln in self.path.read_text().splitlines() if ln.strip()]

    def mark(self) -> None:
        self._seen = len(self._lines())

    async def newest(self, *, wait: float = 3.0) -> dict:
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            lines = self._lines()
            if len(lines) > self._seen:
                return lines[-1]
            await asyncio.sleep(0.02)
        raise AssertionError("no capture record was written")


@pytest.fixture
def records(capture_file: Path) -> Records:
    rec = Records(capture_file)
    rec.mark()
    return rec


class Streamed:
    def __init__(self) -> None:
        self.status = 0
        self.headers: httpx.Headers = httpx.Headers()
        self.body = b""


async def stream(
    client: httpx.AsyncClient, url: str, payload: dict, **headers: str
) -> Streamed:
    out = Streamed()
    async with client.stream("POST", url, json=payload, headers=headers) as resp:
        out.status, out.headers = resp.status_code, resp.headers
        try:
            async for chunk in resp.aiter_raw():
                out.body += chunk
        except httpx.HTTPError:
            pass  # a body cut after commitment reads as a transport error; fine
    return out


def frames(raw: bytes) -> list[bytes]:
    return [f for f in raw.split(b"\n\n") if f.strip()]


def types(raw: bytes) -> list[str]:
    return [json.loads(f.split(b"\ndata: ", 1)[1])["type"] for f in frames(raw)]


# ------------------------------------------------------------ happy paths


async def test_streamed_happy_path_is_forwarded_verbatim_with_no_done_and_no_synthesis(
    gateway, client, records: Records
):
    got = await stream(client, f"{gateway.base_url}{ROUTE}", body(), **mode("ok"))
    assert got.status == 200
    assert got.headers["content-type"].startswith("text/event-stream")
    assert got.headers["x-gw-served-by"].endswith("fake.echo")
    assert got.headers["x-gw-model"] == "fake.echo"
    expected = b"".join(R.stream_frames("ok", model="fake-echo", max_output_tokens=64))
    assert got.body == expected, "frames were altered, reordered or appended to"
    assert b"event: response.completed\n" in got.body
    assert b"[DONE]" not in got.body
    assert types(got.body).count("response.completed") == 1
    assert types(got.body).count("response.output_text.delta") == len(R.TEXT_DELTAS)

    rec = await records.newest()
    assert rec["outcome"] == "completed" and rec["committed"] is True
    assert rec["basis"] == "exact"
    assert rec["tokens"]["input"] == 12, rec["tokens"]
    assert rec["tokens"]["cache_read"] == 8
    assert rec["tokens"]["output"] == 12
    assert rec["tokens"]["reasoning"] == 4
    assert rec["stop_reason"] == "stop"
    assert rec["model"] == "fake.echo"
    assert rec["cost_usd"] > 0


async def test_buffered_happy_path_records_usage_and_rewrites_model_to_the_catalog_id(
    gateway, client, records: Records
):
    r = await client.post(f"{gateway.base_url}{ROUTE}", json=body(stream=False),
                          headers=mode("ok"))
    assert r.status_code == 200, r.text
    assert r.headers["x-gw-body-modified"] == "1"
    assert r.headers["x-gw-model"] == "fake.echo"
    parsed = r.json()
    assert parsed["status"] == "completed"
    assert parsed["model"] == "fake.echo", "A1: the buffered model is the catalog id"
    assert parsed["output"][0]["content"][0]["text"] == R.expected_text()
    assert int(r.headers["content-length"]) == len(r.content)

    rec = await records.newest()
    assert rec["outcome"] == "completed"
    assert rec["basis"] == "exact", "finding 50: the buffered path bills what was stated"
    assert rec["tokens"] == {**rec["tokens"], "input": 12, "cache_read": 8, "output": 12,
                             "reasoning": 4}
    assert rec["stop_reason"] == "stop"


async def test_the_reasoning_stream_accounts_reasoning_inside_output(
    gateway, client, records: Records
):
    got = await stream(client, f"{gateway.base_url}{ROUTE}", body(),
                       **mode("responses-reasoning"))
    assert got.status == 200
    assert "response.reasoning_summary_text.delta" in types(got.body)
    rec = await records.newest()
    assert rec["outcome"] == "completed"
    assert rec["tokens"]["output"] == 52 and rec["tokens"]["reasoning"] == 40


# --------------------------------------------------------- endings (C2/C22)


async def test_incomplete_is_a_finished_turn_with_stop_reason_length(
    gateway, client, records: Records
):
    got = await stream(client, f"{gateway.base_url}{ROUTE}", body(),
                       **mode("responses-incomplete"))
    assert got.status == 200
    kinds = types(got.body)
    assert kinds[-1] == "response.incomplete" and "response.completed" not in kinds
    rec = await records.newest()
    assert rec["outcome"] == "completed"
    assert rec["stop_reason"] == "length"
    assert rec["basis"] == "exact"
    assert rec["error_code"] in (None, "none")


async def test_response_failed_is_forwarded_and_nothing_follows(
    gateway, client, records: Records, fakes: Fakes
):
    """C2 third row: forward `response.failed` if upstream sent it. The
    client sees the two deltas it was already shown, then the provider's own
    failure frame byte for byte, then the body closes -- no
    `response.completed`, no second target (post-commitment)."""
    got = await stream(client, f"{gateway.base_url}{ROUTE}", body(),
                       **mode("responses-failed"))
    assert got.status == 200, "committed before the failure"
    sent = R.stream_frames("responses-failed", model="fake-echo", max_output_tokens=64)
    assert got.body == b"".join(sent), "the failed frame was modified, dropped or followed"
    kinds = types(got.body)
    assert kinds[-1] == "response.failed"
    assert "response.completed" not in kinds
    assert kinds.count("response.output_text.delta") == 2
    assert fakes.stats()["total"] == 1, "no fallback after commitment"

    rec = await records.newest()
    assert rec["committed"] is True
    assert rec["outcome"] == "interrupted"
    assert rec["error_code"] == "in_stream_error"
    assert rec["stop_reason"] is None


async def test_error_event_is_forwarded_and_nothing_follows(
    gateway, client, records: Records, fakes: Fakes
):
    got = await stream(client, f"{gateway.base_url}{ROUTE}", body(),
                       **mode("responses-error-event"))
    assert got.status == 200
    sent = R.stream_frames("responses-error-event", model="fake-echo", max_output_tokens=64)
    assert got.body == b"".join(sent)
    assert got.body.rstrip().endswith(b"}"), "no synthesised frame after the error"
    kinds = types(got.body)
    assert kinds[-1] == "error" and "response.completed" not in kinds
    assert fakes.stats()["total"] == 1

    rec = await records.newest()
    assert rec["outcome"] == "interrupted"
    assert rec["error_code"] == "in_stream_error"


async def test_a_cut_stream_ends_by_close_without_a_completed_frame(
    gateway, client, records: Records
):
    got = await stream(client, f"{gateway.base_url}{ROUTE}", body(),
                       **mode("die-mid-stream", events="2"))
    assert got.status == 200
    kinds = types(got.body)
    assert kinds.count("response.output_text.delta") == 2
    assert "response.completed" not in kinds and "response.failed" not in kinds
    assert b"event: error" not in got.body
    rec = await records.newest()
    assert rec["outcome"] == "interrupted" and rec["basis"] == "estimated"


# ------------------------------------------------------------- refusals


async def test_background_is_refused_before_any_upstream_call(gateway, client, fakes: Fakes):
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json=body(stream=False, background=True), headers=mode("ok"))
    assert r.status_code == 400, r.text
    err = r.json()["error"]
    assert err["type"] == "invalid_request"
    assert "background" in err["message"]
    assert fake_mode_counts(fakes) == {}, "the refusal cost the provider nothing"
    assert r.headers["x-gw-attempts"] == "0"
    # Streaming form, same answer, still nothing upstream.
    r2 = await client.post(f"{gateway.base_url}{ROUTE}",
                           json=body(stream=True, background=True), headers=mode("ok"))
    assert r2.status_code == 400
    assert fake_mode_counts(fakes) == {}


async def test_a_stateless_provider_refuses_state_before_any_upstream_call(
    gateway, client, fakes: Fakes, records: Records
):
    """DeepSeek accepts `previous_response_id` with a 200 and drops it
    (capabilities/captures-responses.md 12b). The shipped row says
    `stateless_responses`, so the gateway answers 400 naming the provider
    and never opens the socket; the body is not edited."""
    for extra in ({"previous_response_id": "resp_abc"}, {"conversation": "conv_1"}):
        r = await client.post(
            f"{gateway.base_url}{ROUTE}",
            json=body("deepseek.deepseek-v4-flash", stream=False, **extra),
            headers=mode("ok"),
        )
        assert r.status_code == 400, r.text
        err = r.json()["error"]
        assert err["type"] == "invalid_request"
        assert "deepseek" in err["message"] and "state" in err["message"]
        assert fake_mode_counts(fakes) == {}
    rec = await records.newest()
    assert rec["error_code"] == "invalid_request"
    assert rec["provider"] == "" and rec["cost_usd"] == 0


async def test_the_same_deepseek_row_serves_a_stateless_body(gateway, client, fakes: Fakes):
    r = await client.post(f"{gateway.base_url}{ROUTE}",
                          json=body("deepseek.deepseek-v4-flash", stream=False),
                          headers=mode("ok"))
    assert r.status_code == 200, r.text
    assert r.headers["x-gw-served-by"] == "deepseek/deepseek.deepseek-v4-flash"
    assert fake_mode_counts(fakes) == {"ok": 1}


# ------------------------------------------------------- the two-turn loop


def echoed_model(raw: bytes) -> str:
    models = {
        json.loads(f.split(b"\ndata: ", 1)[1])["response"]["model"]
        for f in frames(raw)
        if b'"response":' in f
    }
    assert len(models) == 1, models
    return models.pop()


async def test_openai_snapshot_echo_is_accepted_on_the_next_turn_with_previous_response_id(
    gateway, client, fakes: Fakes
):
    """Turn one names the catalog id; the fake answers as OpenAI does, with
    the dated snapshot in every `response` object. Turn two sends that
    snapshot plus the response id, as the Responses SDK does. It must route
    to the same row (A1 alias table), and the id must reach the provider."""
    first = await stream(client, f"{gateway.base_url}{ROUTE}", body("openai.gpt-4o-mini"),
                         **mode("ok"))
    assert first.status == 200, first.body[:200]
    assert first.headers["x-gw-served-by"] == "openai/openai.gpt-4o-mini"
    seen = echoed_model(first.body)
    assert seen == "gpt-4o-mini-2024-07-18", "streamed bodies are never rewritten"
    resp_id = json.loads(frames(first.body)[-1].split(b"\ndata: ", 1)[1])["response"]["id"]

    second = await client.post(
        f"{gateway.base_url}{ROUTE}",
        json=body(seen, stream=False, previous_response_id=resp_id), headers=mode("ok"),
    )
    assert second.status_code == 200, second.text
    assert second.headers["x-gw-served-by"] == "openai/openai.gpt-4o-mini"
    assert second.headers["x-gw-attempts"] == "1"
    assert second.headers["x-gw-model"] == "openai.gpt-4o-mini"
    parsed = second.json()
    assert parsed["previous_response_id"] == resp_id, "passthrough reached the provider"
    assert parsed["model"] == "openai.gpt-4o-mini", "buffered: rewritten back (A1)"
    assert fake_mode_counts(fakes) == {"ok": 2}


async def test_a_catalog_id_that_leaks_upstream_would_be_a_404_so_the_rewrite_is_proven(
    fakes: Fakes, client
):
    """The fake's own contract: an unknown wire id is OpenAI's 404. If the
    gateway ever forwarded `openai.gpt-4o-mini` unrewritten, the test above
    would fail with exactly this body."""
    r = await client.post(f"{fakes.openai.base_url}{ROUTE}",
                          json=body("openai.gpt-4o-mini", stream=False))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "model_not_found"


async def test_deepseek_echo_is_accepted_on_the_next_turn(gateway, client, fakes: Fakes):
    """DeepSeek answers `deepseek-flash` for `deepseek-v4-flash` (probe 10a).
    That is the OpenAI-dialect row's own wire id, shared with the Anthropic
    row; the route's dialect resolves it (no `previous_response_id`: the row
    is stateless and would be refused, correctly)."""
    first = await stream(client, f"{gateway.base_url}{ROUTE}",
                         body("deepseek.deepseek-v4-flash"), **mode("ok"))
    assert first.status == 200
    seen = echoed_model(first.body)
    assert seen == "deepseek-flash"
    second = await client.post(f"{gateway.base_url}{ROUTE}", json=body(seen, stream=False),
                               headers=mode("ok"))
    assert second.status_code == 200, second.text
    assert second.headers["x-gw-served-by"] == "deepseek/deepseek.deepseek-v4-flash"
    assert "x-gw-body-modified" not in second.headers, "the wire id was already right"


# ------------------------------------------------------------ hosted tools


async def test_web_search_calls_land_on_the_record(gateway, client, records: Records):
    got = await stream(client, f"{gateway.base_url}{ROUTE}",
                       body("fake.echo", tools=[{"type": "web_search_preview"}]),
                       **mode("responses-web-search"))
    assert got.status == 200
    kinds = types(got.body)
    assert "response.web_search_call.completed" in kinds
    rec = await records.newest()
    assert rec["outcome"] == "completed"
    assert rec["server_tool_calls"] == {"web_search_requests": 1}
    assert rec["stop_reason"] == "stop"

    records.mark()
    r = await client.post(f"{gateway.base_url}{ROUTE}", json=body(stream=False),
                          headers=mode("responses-web-search"))
    assert r.status_code == 200
    assert r.json()["output"][0]["type"] == "web_search_call"
    rec2 = await records.newest()
    assert rec2["server_tool_calls"] == {"web_search_requests": 1}


# ---------------------------------------------------------------- fallback


async def test_a_5xx_candidate_falls_back_to_the_incumbent_on_this_surface(
    ab: GatewayPool, client, fakes: Fakes
):
    gw = ab.get(mode("5xx"), mode("ok"))
    got = await stream(client, f"{gw.base_url}/workloads/ab{ROUTE}", body(CANDIDATE_MODEL))
    assert got.status == 200, got.body[:200]
    assert got.headers["x-gw-served-by"] == "incumbent/fake.incumbent"
    assert got.headers["x-gw-attempts"] == "2"
    assert types(got.body)[-1] == "response.completed"
    assert echoed_model(got.body) == "fake-echo-incumbent", (
        "the incumbent received ITS OWN wire id, not the candidate's")
    assert fake_mode_counts(fakes) == {"5xx": 1, "ok": 1}


async def test_the_workload_prefixed_form_is_mounted(gateway, client):
    r = await client.post(f"{gateway.base_url}/workloads/default{ROUTE}",
                          json=body(stream=False), headers=mode("ok"))
    assert r.status_code == 200, r.text

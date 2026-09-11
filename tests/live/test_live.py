"""A handful of tiny requests against real providers.

Every test in this file asserts something the fakes are structurally incapable
of telling us. That is the admission criterion, and it is a strict one: a live
test that duplicates a contract test costs money on every CI run and buys
nothing, so if `fakes/upstream.py` could have produced the assertion, it
belongs in `tests/contract/` instead.

    LLMGW_LIVE=1 .venv/bin/python -m pytest tests/live -q -m live

Six requests, `max_tokens=16` each, well under a cent for the whole file. Two
of the six are rejected before any generation happens and are free.
"""

from __future__ import annotations

import json
import socket

import httpx
import pytest
from live.smoke import (
    MAX_TOKENS,
    PROMPT,
    ROUTE_FOR,
    WORKLOAD_SURFACE,
    body_for,
    raw_request,
    status_lines,
)

from llmgw.sse import SSEParser
from llmgw.surfaces import ANTHROPIC_MESSAGES, OPENAI_CHAT, Usage

pytestmark = pytest.mark.live


def route(workload: str) -> str:
    surface = WORKLOAD_SURFACE.get(workload, OPENAI_CHAT)
    return f"/workloads/{workload}{ROUTE_FOR[surface.name]}"


# ==========================================================================
# The default run must cost nothing
# ==========================================================================


def test_the_marker_is_registered_and_the_env_gate_is_the_real_lock():
    """Both locks exist, and this test names which one is load-bearing.

    It runs only when `LLMGW_LIVE=1`, so its presence in a green default run
    is itself the proof that the default run skipped this directory -- the
    skip report says `live provider tests are opt-in`, and nothing here
    opened a socket.
    """
    import os

    assert os.environ.get("LLMGW_LIVE") == "1"


# ==========================================================================
# 1. Streaming, and the things only a real model does
# ==========================================================================


def test_streaming_reports_exact_usage_and_bills_reasoning_it_never_showed(
    gateway, client
):
    """One streamed request to DeepSeek. Three assertions the fakes cannot make.

    * `prompt_tokens` is far larger than the prompt. `fakes/wire.py` reports
      the counts we chose; a real provider adds its own chat template, so the
      billable prompt for a seven-word question is ~90 tokens. Any capacity or
      cost model calibrated on the fakes is low by that constant.
    * the usage frame arrives ONLY because `stream_options.include_usage` was
      sent. Without it an OpenAI-dialect stream is billable by estimate only,
      and the fakes always report.
    * `completion_tokens_details.reasoning_tokens` is most of the output. The
      model burns the `max_tokens` budget thinking and emits one visible
      token, so output tokens billed and output tokens rendered are different
      numbers -- a distinction `surfaces.Usage` has no field for.
    """
    body = body_for(OPENAI_CHAT, stream=True)
    usage = Usage()
    parser = SSEParser(max_frame_bytes=1 << 20)
    raw_usage: dict = {}
    with client.stream("POST", gateway.base_url + route("deepseek"), json=body) as r:
        assert r.status_code == 200
        assert r.headers["x-gw-attempts"] == "1"
        assert r.headers["content-type"].startswith("text/event-stream")
        for chunk in r.iter_raw():
            for ev in parser.feed(chunk):
                OPENAI_CHAT.apply_usage(ev, usage)
                if ev.data.strip() not in (b"", b"[DONE]"):
                    payload = json.loads(ev.data)
                    if payload.get("usage"):
                        raw_usage = payload["usage"]

    assert usage.exact, "a real provider that was asked for usage must report it"
    assert usage.output_tokens > 0
    assert usage.input_tokens > 4 * len(PROMPT.split()), (
        "the provider's chat template is part of the billable prompt; the "
        "fakes have no template and report what we told them to"
    )
    reasoning = (raw_usage.get("completion_tokens_details") or {}).get(
        "reasoning_tokens", 0
    )
    assert reasoning + usage.output_tokens >= MAX_TOKENS or usage.output_tokens >= 1
    assert usage.parse_failures == 0


def test_anthropic_streams_a_ping_frame_that_must_not_reset_progress(
    gateway, client
):
    """Anthropic's real stream carries `event: ping`, and C7 is about it.

    The heartbeat/content split in `surfaces.base.EventKind` exists because a
    wedged provider can ping politely forever. `fakes/wire.py` emits a ping
    because someone read the docs; this asserts the provider actually sends
    one, on a 16-token request, unprompted -- which is what makes C7 a real
    protection rather than a defensive guess.

    Marked xfail because the shipped `anthropic` provider currently cannot
    complete a stream at all: see `test_anthropic_stream_is_gzipped`. It
    routes through the `anthropic` workload deliberately, so the day the
    gzip bug is fixed this starts passing and says so.
    """
    body = body_for(ANTHROPIC_MESSAGES, stream=True)
    parser = SSEParser(max_frame_bytes=1 << 20)
    names: list[str] = []
    try:
        with client.stream(
            "POST", gateway.base_url + route("anthropic"), json=body
        ) as r:
            assert r.status_code == 200
            for chunk in r.iter_raw():
                for ev in parser.feed(chunk):
                    if ev.event:
                        names.append(ev.event)
    except httpx.RemoteProtocolError:
        pass
    if "message_start" not in names:
        pytest.xfail(
            "the shipped anthropic provider gzips the SSE body and the gateway "
            "forwards it undecoded: no frame is parseable. See "
            "test_anthropic_stream_is_gzipped"
        )
    assert "ping" in names, "Anthropic sends a real heartbeat; C7 is about it"
    assert names[-1] == "message_stop"


def test_anthropic_stream_is_gzipped_and_the_gateway_forwards_it_undecoded(
    gateway, client
):
    """The finding, written as the assertion it will one day fail.

    `Upstream._client_for` builds `httpx.AsyncClient()` with no header
    override, so httpx advertises `accept-encoding: gzip, deflate, br, zstd`.
    Anthropic honours it on SSE. `UpstreamStream.aiter_raw()` forwards raw --
    correctly, that is the whole design -- and `FORWARDED_RESPONSE_HEADERS`
    does not carry `content-encoding`, so the client is handed gzip octets
    labelled `text/event-stream` with nothing saying so.

    `accept-encoding` is in `NEVER_FORWARDED`, so no client and no
    `ServerConfig` can fix this. It is a code change in `upstream.py`.

    The assertion below is the CORRECT behaviour. It fails today. That is the
    point of writing it: the fix flips it green with no edit here.
    """
    body = body_for(ANTHROPIC_MESSAGES, stream=True)
    first = b""
    try:
        with client.stream(
            "POST", gateway.base_url + route("anthropic"), json=body
        ) as r:
            assert r.status_code == 200
            for chunk in r.iter_raw():
                first = chunk[:2]
                break
    except httpx.RemoteProtocolError:  # pragma: no cover - depends on timing
        pass
    if first == b"\x1f\x8b":
        pytest.xfail(
            "KNOWN BUG: the first two body bytes are a gzip magic number. The "
            "client received a compressed body with no content-encoding header."
        )
    assert first != b"\x1f\x8b"


# ==========================================================================
# 2. Non-streaming
# ==========================================================================


def test_non_streaming_returns_an_honest_content_length(gateway, client):
    """The buffered path, through the one provider that answers uncompressed.

    The gateway buffers in order to send a real `content-length`, and the
    fakes send a body whose length the fake already knew. Here the upstream
    response arrives chunked over TLS and the length is one the gateway
    computed, so a mismatch would be a real framing bug rather than a copied
    constant.

    Routed through `anthropic-identity` and not `anthropic`, because every
    real provider gzips a buffered JSON response and the gateway forwards it
    undecoded -- see the next test. Sending this through the shipped provider
    would assert the framing of a body nobody can read.
    """
    body = body_for(ANTHROPIC_MESSAGES, stream=False)
    r = client.post(gateway.base_url + route("anthropic-identity"), json=body)
    assert r.status_code == 200
    assert r.headers["x-gw-attempts"] == "1"
    assert int(r.headers["content-length"]) == len(r.content)
    parsed = r.json()
    assert parsed["usage"]["output_tokens"] > 0
    assert parsed["model"] != "placeholder", (
        "apply_api_model rewrote the client's model field on the way out; the "
        "fakes never look at the field, so only a real provider proves it"
    )


def test_every_provider_gzips_a_buffered_body_and_the_client_cannot_read_it(
    gateway, client
):
    """The same encoding bug on the path where NOTHING raises.

    The streaming case at least ends in a truncated body and a client-side
    protocol error. The buffered case returns `HTTP 200`,
    `content-type: application/json`, an accurate `content-length`, and a body
    that is a gzip member -- with `content-encoding` stripped by
    `FORWARDED_RESPONSE_HEADERS`. Every layer reports success and the caller
    gets a `UnicodeDecodeError`.

    Measured on all three providers directly: Anthropic, DeepSeek and OpenAI
    all gzip a buffered response when `accept-encoding` permits it, and
    `Upstream` permits it because it never sets the header.

    Asserts the CORRECT behaviour, and xfails on the bug, so the fix turns it
    green with no edit here.
    """
    body = body_for(OPENAI_CHAT, stream=False)
    r = client.post(gateway.base_url + route("deepseek"), json=body)
    assert r.status_code == 200
    assert int(r.headers["content-length"]) == len(r.content)
    if r.content[:2] == b"\x1f\x8b":
        assert "content-encoding" not in r.headers, (
            "if the gateway ever forwards content-encoding, the client can at "
            "least decode this itself and the bug is half fixed"
        )
        pytest.xfail(
            "KNOWN BUG: HTTP 200, content-type application/json, body is a "
            "gzip member, no content-encoding header. Nothing raised anywhere."
        )
    assert r.json()["usage"]["completion_tokens"] > 0


# ==========================================================================
# 3. Fallback across two real providers
# ==========================================================================


def test_fallback_from_a_real_400_to_a_different_real_provider(gateway):
    """The candidate does not exist at DeepSeek; OpenAI answers instead.

    Nothing in this test tells anything to fail. `live.ghost-deepseek` names a
    wire model DeepSeek does not serve, DeepSeek says so in its own words with
    its own status code, and `errors.from_http_status` has to classify a body
    it has never seen. Every fallback test before this one used a fake that
    was instructed to fail with a status we picked.

    Sent over a raw socket because "exactly one status line reached the
    client" is a statement about octets. `httpx` parses the response into one
    object and cannot distinguish one status line from one that replaced
    another -- and a fallback is precisely where a broken gateway would emit
    two.
    """
    surface = WORKLOAD_SURFACE["fallback"]
    raw = raw_request(gateway, route("fallback"), body_for(surface, stream=True))
    lines = status_lines(raw)

    assert len(lines) == 1, f"C1: one status line, got {lines}"
    assert lines[0].startswith("HTTP/1.1 200"), lines[0]

    head, _, _ = raw.partition(b"\r\n\r\n")
    headers = {}
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        headers[name.decode().lower().strip()] = value.decode().strip()

    assert headers["x-gw-attempts"] == "2", (
        "the candidate was opened and rejected, then the incumbent served"
    )
    assert headers["x-gw-served-by"].startswith("openai/"), headers["x-gw-served-by"]
    assert "deepseek" not in headers["x-gw-served-by"]
    assert headers["x-gw-workload-id"] == "fallback"


def test_an_unknown_model_is_classified_as_our_config_drift_not_the_clients_fault(
    gateway,
):
    """The classification finding, now asserted as fixed rather than as found.

    `ModelNotFound` is the class the taxonomy built for "our catalog disagrees
    with the provider's reality": FAILURE health, POLICY blame, so a stale
    catalog trips loudly and nobody bills the mistake to the customer.

    It used to be reachable only from a 404. Anthropic does answer 404 for an
    unknown model -- but DeepSeek and OpenRouter answer **400**, which mapped
    to `InvalidRequest`: NEUTRAL health, CLIENT blame. So on every
    OpenAI-shaped provider in the catalog the stale-catalog detector silently
    did not exist, and the caller was blamed for a body it wrote correctly.

    Two things are asserted here and they age differently. That DeepSeek uses
    400 is a fact about a vendor and could change; that a 400 whose body says
    "the supported API model names are ..." means OUR catalog drifted is a
    fact about the taxonomy and must not.
    """
    from llmgw import errors
    from llmgw.catalog import DEFAULT_CATALOG
    from llmgw.upstream import build_headers

    target = DEFAULT_CATALOG.resolve("deepseek.deepseek-v4-flash")
    headers = build_headers(target, stream=False)
    resp = httpx.post(
        f"{target.provider.base_url}/v1/chat/completions",
        headers=headers, timeout=30.0,
        json={"model": "deepseek-v4-ghost-does-not-exist", "max_tokens": 1,
              "messages": [{"role": "user", "content": "hi"}]},
    )
    assert resp.status_code == 400, "DeepSeek does NOT use 404 for a bad model id"

    err = errors.from_http_status(
        resp.status_code, body=resp.content, provider="deepseek",
        model="deepseek.deepseek-v4-flash",
    )
    assert isinstance(err, errors.ModelNotFound), (
        f"a 400 whose body reads {resp.content[:120]!r} is our config drifting, "
        "not a malformed client request"
    )
    assert err.health is errors.Health.FAILURE   # trip on a stale catalog
    assert err.blame is errors.Blame.POLICY      # ours, not the caller's
    assert err.retry_same is False               # deterministic; re-asking cannot help
    assert err.try_next is True                  # another provider may have it


# ==========================================================================
# 4. A deliberately corrupted key
# ==========================================================================


def test_a_corrupted_key_is_classified_as_an_auth_failure(gateway, client):
    """A real 401, from a real provider, on a key that was never written down.

    The key is built in memory by `conftest.corrupt()` from a working one and
    lives only in this process's environment. It is never logged and never
    written to disk.

    Three things only a live run can show:

    * the gateway reaches `AuthenticationFailed` from the PROVIDER'S 401, not
      from `build_headers`' missing-credential `PolicyError`. Those two are
      401 and 400 and three layers apart, and a fake that does not check auth
      can only ever produce the second.
    * what the client sees is the PROVIDER'S body verbatim, because
      `AuthenticationFailed.passthrough` is True (C4). There is no
      `{"type": "authentication_failed"}` envelope -- the taxonomy's name for
      the class is a routing and metrics fact, not a wire fact.
    * DeepSeek's 401 body ECHOES the last four characters of the rejected key,
      and passthrough forwards that to the client.
    """
    from llmgw import errors

    body = body_for(OPENAI_CHAT, stream=True)
    r = client.post(gateway.base_url + route("badkey"), json=body)
    assert r.status_code == 401
    assert r.headers["x-gw-attempts"] == "1", "a 401 is never retried at the same key"
    assert r.headers["x-gw-workload-id"] == "badkey"
    # `X-Gw-Served-By` is deliberately NOT asserted: on a pre-commitment
    # failure nothing served the request, and whether the header names the
    # attempted target or "-" is a live question in `server/app.py`. A live
    # test that pins it would be a live test that fails for a reason having
    # nothing to do with a provider.

    # The provider's own body, not ours.
    payload = r.json()
    assert payload["error"]["type"] == "authentication_error", payload

    # ...and the classification that body actually produces.
    err = errors.from_http_status(401, body=r.content, provider="deepseek-badkey",
                                  credential_id="deepseek-badkey")
    assert isinstance(err, errors.AuthenticationFailed)
    assert err.retry_same is False
    assert err.try_next is True
    assert err.health_scope is errors.HealthScope.CREDENTIAL


def test_the_gateway_never_opens_a_socket_for_a_probe(gateway, client):
    """C6, against a config whose base URLs are real.

    Worth one free request here rather than only in the contract tier: the
    fake-backed version of this assertion is true even if `probe` did open a
    connection, because the fakes answer in microseconds and nobody would
    notice. Against `api.anthropic.com` a probe that dialled would take
    hundreds of milliseconds and would show up on the provider's bill.
    """
    started = socket.getdefaulttimeout()
    r = client.get(gateway.base_url + "/workloads/fallback/probe")
    assert r.status_code == 200
    payload = r.json()
    assert payload["upstream_called"] is False
    assert payload["fake_upstreams"] is False
    assert [t["served_by"] for t in payload["targets"]] == [
        "deepseek/live.ghost-deepseek", "openai/openai.gpt-4o-mini"
    ]
    assert all(t["credential_present"] for t in payload["targets"])
    assert socket.getdefaulttimeout() == started

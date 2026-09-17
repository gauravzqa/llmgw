"""Transport tests. No sockets, no sleeping, no real providers.

Everything here runs against `httpx.MockTransport` and a `ManualClock`, which
is what makes it possible to assert on things a socket test cannot reach: the
exact bytes we put on the wire, the fact that an oversized error body was NOT
read to the end, and every httpx exception mapping in one parameterized sweep.

The contract tier next door proves the same module works over a real socket
against a hostile upstream. This tier proves it is *correct in the small*, and
it has to stay fast enough that nobody thinks about running it.
"""

from __future__ import annotations

import asyncio
import json
import ssl

import httpx
import pytest

from llmgw import errors as E
from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets, Deadline, ManualClock, SystemClock
from llmgw.upstream import (
    ANTHROPIC_VERSION,
    Upstream,
    UpstreamRequest,
    apply_api_model,
    apply_extra_body,
    build_headers,
    join_url,
    map_transport_error,
)

KEY_ENV = "LLMGW_TEST_KEY"
KEY = "sk-secret-do-not-log-0a1b2c3d4e5f"

BODY = b'{"model":"m","stream":true,"messages":[{"role":"user","content":"caf\xc3\xa9"}]}'


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(KEY_ENV, KEY)


# ------------------------------------------------------------------ helpers


def conn(pid: str = "p1", kind: str = "openai", **kw) -> ProviderConn:
    kw.setdefault("base_url", "https://up.invalid/v1")
    kw.setdefault("api_key_env", KEY_ENV)
    return ProviderConn(id=pid, kind=kind, **kw)


def catalog_for(*conns: ProviderConn) -> Catalog:
    providers = {c.id: c for c in conns}
    models = {
        f"m-{c.id}": ModelSpec(
            id=f"m-{c.id}",
            provider=c.id,
            api_model=f"wire-{c.id}",
            input_per_m=1.0,
            output_per_m=2.0,
            priced_at="2026-09-09",
        )
        for c in conns
    }
    return Catalog(models=models, providers=providers)


def rig(*conns: ProviderConn, handler=None, **kw):
    """An Upstream wired to a MockTransport, plus a request for the first
    provider. Returns (upstream, request, catalog)."""
    catalog = catalog_for(*conns)
    transport = httpx.MockTransport(handler) if handler is not None else None
    up = Upstream(catalog, transport=transport, **kw)
    target = catalog.resolve(f"m-{conns[0].id}")
    req = UpstreamRequest(target=target, body=BODY, path="/v1/chat/completions",
                          stream=True)
    return up, req, catalog


def streamed(status: int = 200, *, chunks=(b"data: hi\n\n",), headers=None) -> httpx.Response:
    """A MockTransport response that is still a STREAM when send() returns.

    `httpx.Response(200, content=b"...")` is born already-read, so
    `aiter_raw()` on it raises `StreamConsumed` -- which is a rig artifact that
    looks exactly like a transport bug. Every handler here must therefore hand
    back an async iterator, the same shape a real socket produces.
    """

    async def gen():
        for chunk in chunks:
            yield chunk

    return httpx.Response(status, headers=headers or {}, content=gen())


def ok_handler(captured: list[httpx.Request] | None = None, body: bytes = b"data: hi\n\n"):
    def handle(request: httpx.Request) -> httpx.Response:
        if captured is not None:
            captured.append(request)
        return streamed(200, chunks=(body,),
                        headers={"content-type": "text/event-stream"})

    return handle


def budgets(total: float = 30.0, **kw) -> Budgets:
    return Budgets(total=total, **kw)


def deadline(total: float = 30.0) -> Deadline:
    return Deadline(SystemClock(), total)


async def drain(up: Upstream, req: UpstreamRequest, dl=None, bud=None):
    dl = dl or deadline()
    bud = bud or budgets()
    async with up.open(req, deadline=dl, budgets=bud) as stream:
        chunks = [c async for c in stream.aiter_raw()]
        return stream, b"".join(chunks)


# =========================================================== header building


async def test_an_openai_kind_provider_authenticates_with_a_bearer_token():
    seen: list[httpx.Request] = []
    up, req, _ = rig(conn(kind="openai"), handler=ok_handler(seen))
    await drain(up, req)
    await up.aclose()
    assert seen[0].headers["authorization"] == f"Bearer {KEY}"
    assert "x-api-key" not in seen[0].headers


async def test_an_anthropic_kind_provider_sends_x_api_key_and_a_pinned_version():
    """The version is pinned by us, not read from the request. It selects wire
    behaviour, and a client that can pick it can hand our parser a shape nobody
    has ever tested."""
    seen: list[httpx.Request] = []
    up, req, _ = rig(conn(kind="anthropic"), handler=ok_handler(seen))
    await drain(up, req)
    await up.aclose()
    assert seen[0].headers["x-api-key"] == KEY
    assert seen[0].headers["anthropic-version"] == ANTHROPIC_VERSION
    assert "authorization" not in seen[0].headers


async def test_provider_extra_headers_and_per_request_extra_headers_are_both_merged():
    seen: list[httpx.Request] = []
    provider = conn(
        extra_headers={
            "HTTP-Referer": "https://github.com/gauravzqa/llmgw",
            "X-Title": "llmgw",
        }
    )
    catalog = catalog_for(provider)
    up = Upstream(catalog, transport=httpx.MockTransport(ok_handler(seen)))
    req = UpstreamRequest(
        target=catalog.resolve("m-p1"), body=BODY, path="/v1/chat/completions",
        stream=True, extra_headers={"X-Gw-Attempt": "2"},
    )
    await drain(up, req)
    await up.aclose()
    assert seen[0].headers["http-referer"] == "https://github.com/gauravzqa/llmgw"
    assert seen[0].headers["x-title"] == "llmgw"
    assert seen[0].headers["x-gw-attempt"] == "2"


async def test_a_per_request_header_overrides_the_providers_default_for_the_same_name():
    """Overriding `anthropic-version` during a provider migration is the whole
    reason the merge order is provider-then-request rather than the reverse."""
    provider = conn(kind="anthropic", extra_headers={"anthropic-version": "2024-01-01"})
    catalog = catalog_for(provider)
    target = catalog.resolve("m-p1")
    headers = build_headers(target, stream=True, extra={"anthropic-version": "2099-12-31"})
    assert headers["anthropic-version"] == "2099-12-31"


async def test_a_streaming_request_asks_for_event_stream_and_a_buffered_one_does_not():
    seen: list[httpx.Request] = []
    catalog = catalog_for(conn())
    up = Upstream(catalog, transport=httpx.MockTransport(ok_handler(seen)))
    for stream, expected in ((True, "text/event-stream"), (False, "application/json")):
        req = UpstreamRequest(target=catalog.resolve("m-p1"), body=BODY,
                              path="/v1/chat/completions", stream=stream)
        await drain(up, req)
        assert seen[-1].headers["accept"] == expected
    await up.aclose()


async def test_a_missing_credential_is_a_policy_error_and_never_opens_a_connection():
    """A 401 would cost a handshake and a round trip to learn what a dictionary
    lookup already knew -- and it would arrive as AuthenticationFailed, marking
    a credential unhealthy that was never configured in the first place."""
    calls: list[httpx.Request] = []
    up, req, _ = rig(conn(api_key_env="LLMGW_ABSENT_KEY"), handler=ok_handler(calls))
    with pytest.raises(E.PolicyError) as ei:
        await drain(up, req)
    await up.aclose()
    assert calls == []
    assert ei.value.health is E.Health.NEUTRAL
    assert ei.value.provider == "p1"


# ================================================================== secrecy


def _formatted(obj: object) -> str:
    return f"{obj!r} {obj!s}"


async def test_the_api_key_never_appears_in_a_request_repr():
    """`UpstreamRequest` deliberately holds no credential. A request object that
    carries a secret is one that ends up in a traceback, a retry log line, or a
    Sentry breadcrumb -- none of which were written by someone thinking about
    secrets."""
    _, req, _ = rig(conn())
    assert KEY not in _formatted(req)
    assert KEY not in _formatted(req.target)
    assert KEY not in _formatted(req.target.provider)


@pytest.mark.parametrize(
    ("exc", "status"),
    [(httpx.ConnectError("refused"), None), (None, 401), (None, 500), (None, 429)],
)
async def test_the_api_key_never_appears_in_any_error_this_module_raises(exc, status):
    def handle(request: httpx.Request) -> httpx.Response:
        if exc is not None:
            raise exc
        # Even a provider that echoes our own auth header back in its error
        # body must not put the key into an exception a log line will format.
        return streamed(status, chunks=(json.dumps(
            {"error": {"type": "x", "message": f"bad key {KEY}"}}).encode(),))

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(E.GatewayError) as ei:
        await drain(up, req)
    await up.aclose()
    err = ei.value
    assert KEY not in _formatted(err)
    assert KEY not in _formatted(err.cause)
    # The raw body is retained for passthrough, so it is the one place the
    # provider's own words survive -- that is the provider's leak, not ours.
    assert KEY not in _formatted(err.message)


# ================================================================= url join


@pytest.mark.parametrize(
    ("base", "path", "expected"),
    [
        ("https://openrouter.ai/api/v1", "/v1/chat/completions",
         "https://openrouter.ai/api/v1/chat/completions"),
        ("https://api.deepseek.com", "/v1/chat/completions",
         "https://api.deepseek.com/v1/chat/completions"),
        ("https://api.anthropic.com", "/v1/messages",
         "https://api.anthropic.com/v1/messages"),
        ("http://127.0.0.1:8801/v1", "/v1/chat/completions",
         "http://127.0.0.1:8801/v1/chat/completions"),
        ("http://127.0.0.1:8802/", "/v1/messages", "http://127.0.0.1:8802/v1/messages"),
    ],
)
def test_join_url_never_doubles_the_api_version_segment(base, path, expected):
    """`/api/v1` + `/v1/chat/completions` is a 404 that classifies as
    ModelNotFound -- a config error wearing a routing error's clothes, which
    then sends the executor shopping the request around every fallback."""
    assert join_url(base, path) == expected


def test_a_provider_with_no_base_url_is_a_policy_error():
    with pytest.raises(E.PolicyError):
        join_url(None, "/v1/messages")


# ============================================================== extra_body


WIRE_BODY = (
    b'{"model":"wire-p1","stream":true,'
    b'"messages":[{"role":"user","content":"caf\xc3\xa9"}]}'
)
"""BODY with the model the target's API actually answers to.

`catalog_for` gives every model an `api_model` of `wire-<provider>`, and
`apply_api_model` rewrites the field whenever the client named something else
-- which is the whole point of it. So the byte-identity assertions below have
to use a body a client could have sent to that target directly, or they would
be asserting that the rewrite does not happen.
"""


async def test_an_empty_extra_body_forwards_the_client_bytes_identically():
    """The assertion is byte equality, not "parses the same". Re-serialising a
    body we had no reason to touch changes key order, whitespace and unicode
    escaping, and it is exactly the kind of change that only shows up as a
    provider-side cache miss on someone else's bill."""
    seen: list[httpx.Request] = []
    up, req, catalog = rig(conn(), handler=ok_handler(seen))
    same = UpstreamRequest(target=req.target, body=WIRE_BODY,
                           path="/v1/chat/completions", stream=True)
    stream, _ = await drain(up, same)
    await up.aclose()
    assert seen[0].content == WIRE_BODY
    assert stream.body_modified is False


async def test_extra_body_is_merged_wins_over_the_client_and_flags_the_body_modified():
    seen: list[httpx.Request] = []
    provider = conn(extra_body={"provider": {"require_parameters": True}})
    catalog = catalog_for(provider)
    up = Upstream(catalog, transport=httpx.MockTransport(ok_handler(seen)))
    body = json.dumps({"model": "m", "stream": True, "provider": {"order": ["a"]}}).encode()
    req = UpstreamRequest(target=catalog.resolve("m-p1"), body=body,
                          path="/v1/chat/completions", stream=True)
    stream, _ = await drain(up, req)
    await up.aclose()

    sent = json.loads(seen[0].content)
    # `apply_api_model` ran first: the client said "m", the catalog says this
    # target's API answers to "wire-p1", and the provider gets the latter.
    assert sent["model"] == "wire-p1"
    assert sent["stream"] is True
    # Shallow merge, extra_body wins: the client's `provider.order` is replaced
    # wholesale rather than deep-merged into something neither party wrote.
    assert sent["provider"] == {"require_parameters": True}
    assert stream.body_modified is True
    assert seen[0].content != body


def test_apply_extra_body_returns_the_original_object_when_there_is_nothing_to_merge():
    out, modified = apply_extra_body(BODY, {})
    assert out is BODY and modified is False


# ============================================================== api_model


def test_the_clients_model_is_replaced_by_the_targets_api_model():
    """The catalog id is ours; `api_model` is the string the provider answers
    to. Sending the first where the second belongs is a 404 from a provider
    that is perfectly healthy -- and under fallback it is a 404 that only ever
    happens once the candidate is already down."""
    out, modified = apply_api_model(
        b'{"model":"openrouter.deepseek-v4-pro","stream":true}',
        "deepseek/deepseek-v4-pro",
    )
    assert modified is True
    assert json.loads(out)["model"] == "deepseek/deepseek-v4-pro"
    assert json.loads(out)["stream"] is True


def test_a_client_that_already_named_the_wire_model_gets_its_exact_bytes_back():
    """Byte identity, and the SAME OBJECT. The common single-target path must
    not pay a re-serialisation: `json.dumps` of a parsed body changes key
    order, whitespace and unicode escaping, and the visible consequence is a
    provider-side prompt-cache miss on somebody else's bill."""
    body = b'{ "model" : "claude-haiku-4-5-20251001" ,  "x" : "caf\xc3\xa9" }'
    out, modified = apply_api_model(body, "claude-haiku-4-5-20251001")
    assert modified is False
    assert out is body
    assert out == body


def test_a_body_with_no_model_at_all_is_given_the_targets_one():
    out, modified = apply_api_model(b'{"stream":true}', "wire-model")
    assert modified is True
    assert json.loads(out) == {"stream": True, "model": "wire-model"}


@pytest.mark.parametrize(
    "body", [b"", b"   ", b"not json", b'["a","list"]', b'{"unclosed": ',
             b'"a string"', b"null", b"[" * 50_000],
)
def test_a_body_that_cannot_be_parsed_is_an_invalid_request_not_a_crash(body):
    """Attacker-influenced bytes on the request path. A bare `ValueError` or a
    `RecursionError` escaping here would be classified as a gateway fault and
    counted against a provider that was never contacted."""
    with pytest.raises(E.InvalidRequest) as ei:
        apply_api_model(body, "wire-model")
    assert ei.value.blame is E.Blame.CLIENT
    assert ei.value.health is E.Health.NEUTRAL


def test_the_rewrite_survives_a_non_ascii_body_without_escaping_it():
    """`ensure_ascii=False`, the same as `apply_extra_body`. A gateway that
    re-emits `caf\u00e9` has changed the byte length of the prompt, which is
    a different cache key and a different token count at some providers."""
    out, modified = apply_api_model(
        '{"model":"m","content":"café 日本 🌍"}'.encode(), "wire-model"
    )
    assert modified is True
    assert "café 日本 🌍".encode() in out


def test_a_non_string_model_is_still_replaced():
    """`{"model": 7}` is not this target's api_model, so it is not left alone.
    The comparison is equality against the wire string, never truthiness."""
    out, modified = apply_api_model(b'{"model":7}', "wire-model")
    assert modified is True
    assert json.loads(out)["model"] == "wire-model"


async def test_every_target_receives_its_own_api_model_from_one_client_body():
    """The fallback bug, in the small: one client body, two targets, two
    different wire model ids on the two requests.

    `catalog_for` gives p1 `wire-p1` and p2 `wire-p2`. A gateway that forwards
    the client's `model` verbatim sends p1's string to p2 -- which the fakes
    ignore and a real provider answers 404 to, on the attempt that only
    happens when the first provider is already down.
    """
    seen: list[httpx.Request] = []
    catalog = catalog_for(conn("p1"), conn("p2"))
    up = Upstream(catalog, transport=httpx.MockTransport(ok_handler(seen)))
    for pid in ("p1", "p2"):
        req = UpstreamRequest(target=catalog.resolve(f"m-{pid}"), body=BODY,
                              path="/v1/chat/completions", stream=True)
        await drain(up, req)
    await up.aclose()
    assert [json.loads(r.content)["model"] for r in seen] == ["wire-p1", "wire-p2"]


async def test_extra_body_may_still_pin_a_model_over_the_catalogs():
    """Ordering, stated as a test. `apply_api_model` runs first and
    `apply_extra_body` runs last, so an operator who deliberately pins a model
    in `extra_body` overrides the catalog -- the same rule, and the same
    reason, as `build_headers` merging provider extras last."""
    seen: list[httpx.Request] = []
    provider = conn(extra_body={"model": "operator-pinned"})
    catalog = catalog_for(provider)
    up = Upstream(catalog, transport=httpx.MockTransport(ok_handler(seen)))
    req = UpstreamRequest(target=catalog.resolve("m-p1"), body=BODY,
                          path="/v1/chat/completions", stream=True)
    stream, _ = await drain(up, req)
    await up.aclose()
    assert json.loads(seen[0].content)["model"] == "operator-pinned"
    assert stream.body_modified is True


async def test_the_body_modified_flag_covers_the_model_rewrite_too():
    """One flag, two rewrites. `X-Gw-Body-Modified` means "these are not the
    bytes you sent us", and a rename is exactly that."""
    seen: list[httpx.Request] = []
    up, req, _ = rig(conn(), handler=ok_handler(seen))
    stream, _ = await drain(up, req)
    await up.aclose()
    assert seen[0].content != BODY
    assert stream.body_modified is True


@pytest.mark.parametrize(
    "body", [b"", b"   ", b"not json", b'["a","list"]', b'{"unclosed": ', b"[" * 50_000]
)
async def test_a_malformed_body_plus_extra_body_is_an_invalid_request_not_a_crash(body):
    """This runs on the request path with attacker-influenced bytes. A raw
    JSONDecodeError escaping here would be classified as a gateway fault and
    counted against a provider's breaker for a request that never left the
    building."""
    provider = conn(extra_body={"provider": {"require_parameters": True}})
    catalog = catalog_for(provider)
    up = Upstream(catalog, transport=httpx.MockTransport(ok_handler()))
    req = UpstreamRequest(target=catalog.resolve("m-p1"), body=body,
                          path="/v1/chat/completions", stream=True)
    with pytest.raises(E.InvalidRequest) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.blame is E.Blame.CLIENT
    assert ei.value.health is E.Health.NEUTRAL


async def test_a_malformed_body_is_refused_before_a_socket_is_opened():
    """A body we cannot parse is a body we cannot address to a target.

    This assertion INVERTED in P3. It used to say that a provider with no
    `extra_body` forwards garbage untouched and lets the provider reject it,
    which was defensible while nothing in this module had to read the body.
    `apply_api_model` does: the client's `model` names a catalog entry and the
    provider's API wants that entry's `api_model`, so every request is parsed
    now and unparseable bytes have no target to be sent to.

    Refusing here is also the cheaper half of C6 -- no connection, no
    handshake, and `seen` proves it -- and `InvalidRequest` is CLIENT blame and
    NEUTRAL health, so a client's malformed JSON still teaches a breaker
    nothing about a provider that was never contacted.
    """
    seen: list[httpx.Request] = []
    up, req, catalog = rig(conn(), handler=ok_handler(seen))
    junk = UpstreamRequest(target=req.target, body=b"not json at all",
                           path="/v1/chat/completions", stream=True)
    with pytest.raises(E.InvalidRequest) as ei:
        await drain(up, junk)
    await up.aclose()
    assert seen == [], "a body we refused still cost the provider a connection"
    assert ei.value.blame is E.Blame.CLIENT
    assert ei.value.health is E.Health.NEUTRAL


# ======================================================== exception mapping


BEFORE_HEADERS = [
    (httpx.ConnectTimeout("timed out"), E.ConnectTimeout),
    (httpx.ConnectError("refused"), E.ConnectionFailed),
    (httpx.ConnectError("[Errno 8] nodename nor servname provided"), E.ConnectionFailed),
    (httpx.ProxyError("proxy said no"), E.ConnectionFailed),
    (httpx.PoolTimeout("pool full"), E.ProviderKeyExhausted),
    (httpx.ReadError("reset"), E.ConnectionFailed),
    (httpx.WriteError("broken pipe"), E.ConnectionFailed),
    (httpx.ReadTimeout("read"), E.ConnectTimeout),
    (httpx.WriteTimeout("write"), E.ConnectTimeout),
    (httpx.RemoteProtocolError("truncated"), E.UpstreamDisconnected),
    (httpx.LocalProtocolError("we built garbage"), E.PolicyError),
    (httpx.UnsupportedProtocol("gopher://"), E.PolicyError),
    (httpx.DecodingError("bad gzip"), E.MalformedUpstreamResponse),
    (httpx.CloseError("close"), E.ConnectionFailed),
    (httpx.NetworkError("network"), E.ConnectionFailed),
    (ssl.SSLError("certificate verify failed"), E.ConnectionFailed),
    (OSError(104, "Connection reset by peer"), E.ConnectionFailed),
]


@pytest.mark.parametrize(
    ("exc", "expected"), BEFORE_HEADERS, ids=lambda v: getattr(v, "__name__", type(v).__name__)
)
async def test_every_transport_failure_before_headers_maps_to_the_taxonomy(exc, expected):
    def handle(request: httpx.Request) -> httpx.Response:
        raise exc

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(expected) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.provider == "p1"
    assert ei.value.model == "m-p1"
    assert ei.value.credential_id == "p1"
    assert ei.value.cause is exc


AFTER_HEADERS = [
    (httpx.RemoteProtocolError("truncated chunked body"), E.UpstreamDisconnected),
    (httpx.ReadError("connection reset"), E.UpstreamDisconnected),
    (httpx.CloseError("closed"), E.UpstreamDisconnected),
    (OSError(104, "Connection reset by peer"), E.UpstreamDisconnected),
    (httpx.ReadTimeout("read"), E.FirstEventTimeout),
    (httpx.DecodingError("bad gzip"), E.MalformedUpstreamResponse),
    # The two that do NOT inherit httpx.HTTPError, which is the whole reason
    # `TRANSPORT_EXCEPTIONS` exists: StreamError is a RuntimeError and
    # InvalidURL is a bare Exception, so a hand-written
    # `except httpx.HTTPError` catches neither and both escape a module whose
    # entire contract is that no vendor exception ever leaves it.
    (httpx.StreamClosed(), E.UpstreamDisconnected),
    (httpx.InvalidURL("bad url"), E.PolicyError),
]


@pytest.mark.parametrize(
    ("exc", "expected"), AFTER_HEADERS, ids=lambda v: getattr(v, "__name__", type(v).__name__)
)
async def test_the_same_failure_after_headers_maps_to_a_different_class(exc, expected):
    """The phase decides the class, and httpx does not carry the phase. A
    ReadError before headers means nothing was accepted and re-sending is free;
    the identical exception after headers means the model may be generating
    right now and re-sending bills the customer twice."""

    async def stream_bytes():
        yield b"data: one\n\n"
        raise exc

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=stream_bytes())  # already a stream

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(expected) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.provider == "p1"
    assert ei.value.cause is exc


async def test_no_vendor_exception_survives_a_body_read():
    """The invariant behind the table above, stated once so a new httpx
    exception class cannot slip past a case list nobody updated.

    Every exception `TRANSPORT_EXCEPTIONS` names is raised from inside a
    response body and must come back out as a `GatewayError`. This is the
    assertion the module docstring promises -- "a single leaked
    `httpx.ReadError` makes every `except GatewayError` in the executor
    incomplete in a way no test above this layer can see" -- and it is worth
    having separately from the mapping table because the failure it catches is
    a MISSING row, not a wrong one.
    """
    leaks = [
        httpx.ReadError("reset"), httpx.RemoteProtocolError("truncated"),
        httpx.StreamClosed(), httpx.InvalidURL("bad url"),
        ssl.SSLError("handshake"), OSError(104, "Connection reset by peer"),
    ]
    for exc in leaks:
        async def stream_bytes(exc=exc):
            yield b"data: one\n\n"
            raise exc

        def handle(request: httpx.Request, exc=exc) -> httpx.Response:
            return httpx.Response(200, content=stream_bytes(exc))

        up, req, _ = rig(conn(), handler=handle)
        with pytest.raises(E.GatewayError) as ei:
            await drain(up, req)
        await up.aclose()
        assert ei.value.cause is exc, f"{type(exc).__name__} lost its cause"


def test_read_error_flips_disposition_across_the_header_boundary():
    """Stated as a bare mapping assertion because it is the single most
    important row in the table: same vendor type, opposite retry safety."""
    before = map_transport_error(httpx.ReadError("x"), after_headers=False, provider="p")
    after = map_transport_error(httpx.ReadError("x"), after_headers=True, provider="p")
    assert isinstance(before, E.ConnectionFailed) and before.retry_same is True
    assert isinstance(after, E.UpstreamDisconnected)
    assert before.code != after.code


ALL_HTTPX_EXCEPTIONS = [
    httpx.HTTPError, httpx.RequestError, httpx.TransportError, httpx.TimeoutException,
    httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout,
    httpx.NetworkError, httpx.ConnectError, httpx.ReadError, httpx.WriteError,
    httpx.CloseError, httpx.ProtocolError, httpx.LocalProtocolError,
    httpx.RemoteProtocolError, httpx.ProxyError, httpx.UnsupportedProtocol,
    httpx.DecodingError, httpx.TooManyRedirects, httpx.InvalidURL, httpx.StreamError,
]


@pytest.mark.parametrize("cls", ALL_HTTPX_EXCEPTIONS, ids=lambda c: c.__name__)
async def test_no_raw_httpx_exception_can_escape_open(cls):
    """Enumerated rather than sampled. The risk is not the class we mapped
    today; it is the one httpx adds in a minor release, or the one we assumed
    inherits HTTPError and does not -- InvalidURL is a bare Exception and
    StreamError is a RuntimeError, and a hand-written `except httpx.HTTPError`
    misses both."""

    def handle(request: httpx.Request) -> httpx.Response:
        raise cls("synthetic")

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(E.GatewayError):
        await drain(up, req)
    await up.aclose()


async def test_an_invalid_url_is_a_policy_error_and_not_a_provider_failure():
    """Raised by build_request(), before send() -- which is why request
    construction lives inside the mapped region."""
    up, req, _ = rig(conn(base_url="http://[::1"), handler=ok_handler())
    with pytest.raises(E.GatewayError) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.health is E.Health.NEUTRAL


# ============================================================ status mapping


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (429, E.RateLimited),
        (401, E.AuthenticationFailed),
        (403, E.AuthenticationFailed),
        (404, E.ModelNotFound),
        (400, E.InvalidRequest),
        (500, E.UpstreamServerError),
        (503, E.UpstreamOverloaded),
        (529, E.UpstreamOverloaded),
        (302, E.UpstreamServerError),
    ],
)
async def test_a_non_2xx_is_classified_by_from_http_status(status, expected):
    def handle(request: httpx.Request) -> httpx.Response:
        return streamed(status, chunks=(b'{"error":{"type":"x","message":"y"}}',))

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(expected) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.upstream_status == status
    assert ei.value.provider == "p1"
    assert ei.value.model == "m-p1"


async def test_a_non_2xx_never_yields_a_stream_to_the_caller():
    entered = False

    def handle(request: httpx.Request) -> httpx.Response:
        return streamed(500, chunks=(b"boom",))

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(E.UpstreamServerError):
        async with up.open(req, deadline=deadline(), budgets=budgets()):
            entered = True
    await up.aclose()
    assert entered is False


@pytest.mark.parametrize(
    ("header", "expected"), [("7", 7.0), ("0", 0.0), ("garbage", None), (None, None)]
)
async def test_retry_after_travels_from_the_header_onto_the_error(header, expected):
    """`retry.py` treats this as a FLOOR, so losing it means backing off less
    than the provider explicitly asked for -- at exactly the moment it is
    telling you it cannot take more."""

    def handle(request: httpx.Request) -> httpx.Response:
        headers = {} if header is None else {"retry-after": header}
        return streamed(429, headers=headers, chunks=(b'{"error":{"type":"x"}}',))

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(E.RateLimited) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.retry_after == expected


async def test_a_context_length_400_is_refined_from_the_body():
    """Status first, body only to refine. The refinement is why we read any of
    the body at all -- a bigger-context target can actually serve this one."""

    def handle(request: httpx.Request) -> httpx.Response:
        return streamed(400, chunks=(json.dumps(
            {"error": {"type": "invalid_request_error",
                       "message": "context length exceeded"}}).encode(),))

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(E.ContextLengthExceeded):
        await drain(up, req)
    await up.aclose()


async def test_the_error_body_read_is_bounded_and_does_not_drain_the_provider():
    """A provider having a bad day can answer 500 with a 100 MB HTML page.
    Reading it "just to classify" turns one provider incident into a memory
    incident of our own -- concurrently, on every failing request at once."""
    produced = 0
    block = b"x" * 64_000

    async def huge():
        nonlocal produced
        for _ in range(2_000):  # 128 MB if anyone ever reads it all
            produced += len(block)
            yield block

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=huge())

    up, req, _ = rig(conn(), handler=handle, max_error_body=8_192)
    with pytest.raises(E.UpstreamServerError) as ei:
        await drain(up, req)
    await up.aclose()
    assert len(ei.value.upstream_body or b"") <= 8_192
    assert produced <= 200_000, f"read {produced} bytes of a 128 MB error body"


async def test_a_failure_while_reading_the_error_body_does_not_lose_the_status():
    """The status is the strong signal and we already have it. A reset while
    reading the body must not replace a perfectly good 429 with a transport
    error whose disposition is completely different."""

    async def dies():
        yield b'{"error":{"type":'
        raise httpx.RemoteProtocolError("gone")

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "3"}, content=dies())

    up, req, _ = rig(conn(), handler=handle)
    with pytest.raises(E.RateLimited) as ei:
        await drain(up, req)
    await up.aclose()
    assert ei.value.retry_after == 3.0


# ================================================================== the clock


async def test_httpx_gets_no_timeouts_of_its_own():
    """Two independent timeout systems means the tighter one wins silently,
    and which one is tighter depends on how much of the deadline a previous
    attempt already spent. The budget then becomes decorative on exactly the
    requests that were already in trouble."""
    up, _, _ = rig(conn(), handler=ok_handler())
    client = up._client_for(conn())
    timeout = client.timeout
    assert timeout.connect is None
    assert timeout.read is None
    assert timeout.write is None
    assert timeout.pool is None
    await up.aclose()


async def test_a_stall_before_headers_is_a_headers_timeout_not_a_connect_timeout():
    """The conservative classification, and the reason `HeadersTimeout` exists.

    This phase covers connect + TLS + request write + response headers,
    because httpx exposes no hook at "connected". `ConnectTimeout.retry_same`
    is True on the strength of "nothing was sent, no side effect can exist" --
    a claim we cannot make here, since the request may have been fully written
    and the model may already be generating. When the transport cannot prove
    nothing was sent, we assume something was.
    """
    clock = ManualClock(start=0.0)

    async def handle(request: httpx.Request) -> httpx.Response:
        await clock.sleep(500.0)
        return streamed(200)

    up, req, _ = rig(conn(), handler=handle)
    dl = Deadline(clock, total=100.0)

    # PLAN-2 B6: the status-line wait is the HEADERS budget; `connect` is the
    # TCP+TLS timeout handed to httpx. Both at 2 s here keeps the pre-B6 shape
    # of this test; `test_phase_b_server` covers the two moving apart.
    task = asyncio.create_task(drain(up, req, dl, budgets(100.0, connect=2.0, headers=2.0)))
    await clock.advance(3.0)
    with pytest.raises(E.HeadersTimeout) as ei:
        await task
    await up.aclose()
    assert ei.value.retry_same is False   # the whole point of the class
    assert ei.value.try_next is True      # a different target is still fine
    assert ei.value.provider == "p1"


async def test_httpx_connect_timeout_still_earns_the_safe_classification():
    """The transport CAN prove it in one case: httpx raises ConnectTimeout
    only while establishing the connection. There the "nothing was sent"
    argument is airtight and the cheap retry is legitimate."""
    err = map_transport_error(httpx.ConnectTimeout("refused"), after_headers=False,
                              provider="p1", model="m1")
    assert isinstance(err, E.ConnectTimeout)
    assert err.retry_same is True


async def test_headers_then_silence_becomes_a_first_event_timeout_not_a_connect_timeout():
    """Opposite dispositions on either side of the headers: ConnectTimeout may
    be re-sent to the same target, FirstEventTimeout may not, because the
    request was accepted and the model may be generating right now."""
    clock = ManualClock(start=0.0)

    async def silence():
        await clock.sleep(500.0)
        yield b"data: never\n\n"

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=silence())

    up, req, _ = rig(conn(), handler=handle)
    dl = Deadline(clock, total=100.0)

    task = asyncio.create_task(drain(up, req, dl, budgets(100.0, first_event=5.0)))
    await clock.advance(6.0)
    with pytest.raises(E.FirstEventTimeout) as ei:
        await task
    await up.aclose()
    assert ei.value.retry_same is False
    assert ei.value.try_next is True


async def test_a_later_chunk_is_bounded_by_the_total_deadline_and_nothing_tighter():
    """The inter-event stall clock belongs to the pump, which knows what an
    event is. What this layer owes is that no read can outlive the request --
    without it a silent upstream parks the coroutine forever and takes the
    permit, the buffer and the connection with it."""
    clock = ManualClock(start=0.0)

    async def one_then_silence():
        yield b"data: one\n\n"
        await clock.sleep(500.0)
        yield b"data: two\n\n"

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=one_then_silence())

    up, req, _ = rig(conn(), handler=handle)
    dl = Deadline(clock, total=10.0)

    task = asyncio.create_task(drain(up, req, dl, budgets(10.0, first_event=8.0)))
    await clock.advance(11.0)
    with pytest.raises(E.TotalDeadlineExceeded):
        await task
    await up.aclose()


async def test_an_already_expired_deadline_never_opens_a_connection():
    """A doomed attempt must not spend a handshake to discover it is doomed."""
    calls: list[httpx.Request] = []
    clock = ManualClock(start=0.0)
    up, req, _ = rig(conn(), handler=ok_handler(calls))
    dl = Deadline(clock, total=1.0)
    await clock.advance(5.0)
    with pytest.raises(E.TotalDeadlineExceeded):
        await drain(up, req, dl, budgets(1.0))
    await up.aclose()
    assert calls == []


# ============================================== pooling, lifecycle and stats


async def test_one_client_is_built_per_provider_and_then_reused():
    """A fresh TCP+TLS connection per request adds a round trip to every call
    and exhausts fds at a fraction of the load it was sized for."""
    up, req, _ = rig(conn(), conn("p2"), handler=ok_handler())
    for _ in range(5):
        await drain(up, req)
    assert len(up._clients) == 1
    first = up._client_for(conn())
    assert up._client_for(conn()) is first
    await up.aclose()


async def test_the_pool_is_capped_at_the_providers_own_concurrency():
    """The pool is a cap as well as a cache. Without it a burst opens as many
    upstream connections as there are in-flight requests, and the provider --
    not us -- decides what to do about that, usually with a 429 we then
    dutifully retry.

    Keepalive is deliberately equal to the cap: a keepalive ceiling below
    max_connections quietly reintroduces per-request handshakes at peak, which
    is exactly when they cost the most."""
    provider = conn(max_concurrency=7)
    up = Upstream(catalog_for(provider))  # no MockTransport: a real pool
    pool = up._client_for(provider)._transport._pool
    await up.aclose()
    assert pool._max_connections == 7
    assert pool._max_keepalive_connections == 7


async def test_stats_lists_every_provider_in_the_catalog_including_the_untouched():
    """A gauge whose series appear only once traffic reaches a provider cannot
    show you the moment a provider went from busy to untouched, and it makes
    the metric's label vocabulary depend on traffic rather than on config."""
    up, req, _ = rig(conn(), conn("p2"), handler=ok_handler())
    assert up.stats() == {"p1": 0, "p2": 0}
    await drain(up, req)
    assert set(up.stats()) == {"p1", "p2"}
    await up.aclose()


async def test_in_flight_returns_to_baseline_on_success_on_error_and_on_break():
    """A permit that leaks on an error path works fine until the day something
    throws where nobody expected, and then capacity ratchets to zero over
    hours with nothing in the logs."""

    def handle(request: httpx.Request) -> httpx.Response:
        mode = request.headers.get("x-case")
        if mode == "boom":
            return streamed(500, chunks=(b"boom",))
        return streamed(200, chunks=(b"data: a\n\n", b"data: b\n\n"))

    up, req, catalog = rig(conn(), handler=handle)
    assert up.in_flight() == {}

    await drain(up, req)
    assert up.in_flight() == {}

    bad = UpstreamRequest(target=req.target, body=BODY, path=req.path, stream=True,
                          extra_headers={"x-case": "boom"})
    with pytest.raises(E.UpstreamServerError):
        await drain(up, bad)
    assert up.in_flight() == {}

    async with up.open(req, deadline=deadline(), budgets=budgets()) as stream:
        async for _ in stream.aiter_raw():
            break
    assert up.in_flight() == {}
    await up.aclose()


async def test_leaving_the_context_early_closes_the_upstream_response():
    """Cancellation must close the connection, not leak it. A leaked upstream
    connection is invisible until the pool is full and the gateway stops
    accepting work for no visible reason an hour later."""
    async def body():
        for i in range(1000):
            yield b"data: %d\n\n" % i

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body())

    up, req, _ = rig(conn(), handler=handle)
    async with up.open(req, deadline=deadline(), budgets=budgets()) as stream:
        async for _ in stream.aiter_raw():
            break
        response = stream._response
    # `is_closed` is the httpx-side fact. That this also returns a real socket
    # to a real pool is asserted over real sockets in the contract tier -- a
    # MockTransport has no connection to return, so believing this test alone
    # would be believing the rig rather than the code.
    assert response.is_closed
    assert up.in_flight() == {}
    await up.aclose()


async def test_cancelling_a_consumer_mid_stream_still_closes_the_response():
    clock = ManualClock(start=0.0)
    holder: list = []

    async def body():
        yield b"data: one\n\n"
        await clock.sleep(500.0)
        yield b"data: two\n\n"

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body())

    up, req, _ = rig(conn(), handler=handle)
    dl = Deadline(clock, total=1_000.0)

    async def consume():
        async with up.open(req, deadline=dl, budgets=budgets(1_000.0)) as stream:
            holder.append(stream)
            async for _ in stream.aiter_raw():
                pass

    task = asyncio.create_task(consume())
    for _ in range(20):
        await asyncio.sleep(0)
        if holder:
            break
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert holder and holder[0]._response.is_closed
    assert up.in_flight() == {}
    await up.aclose()


async def test_aiter_raw_refuses_a_second_consumer():
    """Two consumers of one body would each see a random half of it. Failing
    loudly beats a stream that silently interleaves."""
    up, req, _ = rig(conn(), handler=ok_handler(body=b"data: a\n\n"))
    async with up.open(req, deadline=deadline(), budgets=budgets()) as stream:
        assert [c async for c in stream.aiter_raw()] == [b"data: a\n\n"]
        with pytest.raises(RuntimeError):
            await anext(stream.aiter_raw().__aiter__())
    await up.aclose()


async def test_the_body_bytes_arrive_unmodified_and_unbuffered():
    payload = b"".join(b"data: %d\n\n" % i for i in range(200))

    async def chunks():
        for i in range(0, len(payload), 7):
            yield payload[i : i + 7]

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=chunks())

    up, req, _ = rig(conn(), handler=handle)
    stream, joined = await drain(up, req)
    await up.aclose()
    assert joined == payload
    assert stream.status == 200


async def test_aclose_is_idempotent_and_survives_a_closed_client():
    up, req, _ = rig(conn(), handler=ok_handler())
    await drain(up, req)
    await up.aclose()
    await up.aclose()
    assert up.stats() == {"p1": 0}


# ------------------------------------------------- multipart model form field
# 18 Sep 2026: the multipart body reached OpenAI byte-for-byte, so the catalog
# id in the `model` form field produced a provider 404. The splice below is the
# one edit a multipart body gets -- the same edit JSON bodies get.


def _mp(fields: list[tuple[str, bytes]], boundary: str = "b0und") -> bytes:
    parts = []
    for name, value in fields:
        head = f'Content-Disposition: form-data; name="{name}"'
        if name == "file":
            head += '; filename="a.wav"\r\nContent-Type: audio/wav'
        parts.append(f"--{boundary}\r\n{head}\r\n\r\n".encode() + value + b"\r\n")
    return b"".join(parts) + f"--{boundary}--\r\n".encode()


def test_multipart_model_field_is_spliced_to_the_wire_id():
    from llmgw.upstream import apply_api_model_multipart

    body = _mp([("model", b"openai.gpt-transcribe"), ("stream", b"false"),
                ("file", b"RIFF" + b"\x00" * 64)])
    out, changed = apply_api_model_multipart(body, "gpt-transcribe")
    assert changed
    assert b'name="model"\r\n\r\ngpt-transcribe\r\n' in out
    assert b"openai.gpt-transcribe" not in out
    # Everything else is byte-identical: boundary, other fields, the file.
    assert out.count(b"--b0und") == body.count(b"--b0und")
    assert out.split(b"--b0und")[2:] == body.split(b"--b0und")[2:]


def test_multipart_model_field_already_wire_id_is_untouched():
    from llmgw.upstream import apply_api_model_multipart

    body = _mp([("model", b"gpt-transcribe"), ("file", b"\x00" * 16)])
    out, changed = apply_api_model_multipart(body, "gpt-transcribe")
    assert not changed and out is body


def test_multipart_splice_only_touches_the_named_key():
    from llmgw.upstream import apply_api_model_multipart

    body = _mp([("modelId", b"inworld.tts-2"), ("model", b"keep-me"), ("file", b"\x00")])
    out, changed = apply_api_model_multipart(body, "inworld-tts-2", key="modelId")
    assert changed and b'name="modelId"\r\n\r\ninworld-tts-2\r\n' in out and b"keep-me" in out

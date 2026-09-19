"""PLAN-2 Phase B, server side: credential styles and scrub (B2), per-surface
caps and body kinds (B4), request defaults (B5), the headers budget and the
policy-wide drain check (B6).

Everything here is a pure function or a `MockTransport` round trip; the
socket-level halves are in `tests/contract/test_body_caps.py`,
`test_multipart.py` and `test_request_defaults.py`.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

import httpx
import pytest

from llmgw import errors as E
from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets, Deadline
from llmgw.server import app as appmod
from llmgw.server.config import (
    DEFAULT_SURFACE_LIMITS,
    ServerConfig,
    SurfaceLimits,
    surface_limits_from_env,
)
from llmgw.upstream import (
    Upstream,
    UpstreamRequest,
    apply_request_defaults,
    auth_scheme_of,
    build_headers,
)
from tests.unit.test_upstream import KEY, ManualClock, catalog_for, conn, streamed

KEY_ENV = "LLMGW_TEST_KEY"


@pytest.fixture(autouse=True)
def _credential(monkeypatch):
    monkeypatch.setenv(KEY_ENV, KEY)


# ============================================================== B2: schemes


def _target(provider: ProviderConn):
    return catalog_for(provider).resolve(f"m-{provider.id}")


def _headers(provider: ProviderConn, **kw) -> dict[str, str]:
    return build_headers(_target(provider), stream=False, **kw)


def test_bearer_is_the_default_scheme():
    h = _headers(conn())
    assert h["authorization"] == f"Bearer {KEY}"
    assert "x-api-key" not in h


def test_anthropic_kind_without_a_scheme_still_means_x_api_key():
    h = _headers(conn(kind="anthropic"))
    assert h["x-api-key"] == KEY and "authorization" not in h


def test_raw_scheme_sends_the_bare_key_in_authorization():
    h = _headers(dataclasses.replace(conn(), auth_scheme="raw"))
    assert h["authorization"] == KEY  # AssemblyAI: no scheme word


def test_header_scheme_sends_the_named_header():
    h = _headers(dataclasses.replace(conn(), auth_scheme="header", auth_header="Xi-Api-Key"))
    assert h["xi-api-key"] == KEY and "authorization" not in h


def test_header_scheme_without_a_header_name_is_refused_before_any_socket():
    # The catalog row refuses it at construction; a row-shaped object that
    # slipped past (a test double, an older catalog) is refused by the
    # header builder instead. Either way no request is built.
    with pytest.raises(ValueError, match="auth_header"):
        dataclasses.replace(conn(), auth_scheme="header", auth_header=None)

    class Row:
        id, kind, auth_scheme, auth_header = "p1", "openai", "header", None

        @staticmethod
        def key() -> str:
            return "p1"

    with pytest.raises(E.PolicyError, match="auth_header"):
        auth_scheme_of(Row())  # type: ignore[arg-type]


def test_content_type_is_json_unless_the_caller_says_otherwise():
    assert _headers(conn())["content-type"] == "application/json"
    boundary = "multipart/form-data; boundary=abc123"
    assert _headers(conn(), content_type=boundary)["content-type"] == boundary


# ============================================================ B5: defaults


def _body(**fields) -> bytes:
    return json.dumps({"model": "x", "messages": [], **fields}).encode()


def test_defaults_fill_only_absent_keys():
    out, touched = apply_request_defaults(_body(), {"temperature": 0.2, "seed": 7})
    parsed = json.loads(out)
    assert parsed["temperature"] == 0.2 and parsed["seed"] == 7
    assert touched == ("temperature", "seed")


def test_a_client_key_is_never_overwritten_even_when_null():
    out, touched = apply_request_defaults(
        _body(thinking=None), {"thinking": {"type": "disabled"}}
    )
    assert json.loads(out)["thinking"] is None
    assert touched == ()
    assert out == _body(thinking=None)  # byte-identical: nothing was touched


def test_dict_defaults_merge_one_level_and_client_inner_keys_win():
    body = _body(stream_options={"include_usage": False})
    out, touched = apply_request_defaults(
        body, {"stream_options": {"include_usage": True, "include_obfuscation": False}}
    )
    parsed = json.loads(out)
    assert parsed["stream_options"] == {"include_usage": False, "include_obfuscation": False}
    assert touched == ("stream_options",)


def test_dict_default_that_adds_nothing_is_not_reported():
    body = _body(thinking={"type": "enabled", "budget_tokens": 10})
    out, touched = apply_request_defaults(body, {"thinking": {"type": "disabled"}})
    assert out == body and touched == ()


def test_lists_and_scalars_are_supplied_not_merged():
    body = _body(stop=["a"])
    out, touched = apply_request_defaults(body, {"stop": ["b", "c"]})
    assert json.loads(out)["stop"] == ["a"] and touched == ()


def test_non_object_and_empty_defaults_pass_through_untouched():
    assert apply_request_defaults(b"[1,2]", {"a": 1}) == (b"[1,2]", ())
    assert apply_request_defaults(b"not json", {"a": 1}) == (b"not json", ())
    assert apply_request_defaults(_body(), {}) == (_body(), ())
    assert apply_request_defaults(_body(), None) == (_body(), ())


async def test_defaults_are_applied_upstream_only_for_json_bodies_and_reported():
    """Through `Upstream.open`: the target's `request_defaults` land in the
    bytes sent, `body_modified` flips, `defaulted_keys` names the keys; a raw
    body with the same target is forwarded untouched."""
    if "request_defaults" not in ModelSpec.__dataclass_fields__:
        pytest.skip("catalog.ModelSpec.request_defaults has not landed yet")
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return streamed(200)

    provider = conn()
    model = ModelSpec(
        id="m1", provider=provider.id, api_model="wire-m1",
        input_per_m=1.0, output_per_m=1.0, priced_at="2026-09-16",
        request_defaults={"thinking": {"type": "disabled"}},
    )
    catalog = Catalog(models={model.id: model}, providers={provider.id: provider})
    up = Upstream(catalog, transport=httpx.MockTransport(handler))
    target = catalog.resolve("m1")
    clock = ManualClock(start=0.0)
    dl = Deadline(clock, total=30.0)
    bud = Budgets(total=30.0)

    req = UpstreamRequest(target=target, body=_body(), path="/v1/x", stream=False)
    async with up.open(req, deadline=dl, budgets=bud) as stream:
        assert stream.body_modified is True
        assert stream.defaulted_keys == ("thinking",)
    sent = json.loads(seen[-1].content)
    assert sent["thinking"] == {"type": "disabled"}
    assert sent["model"] == "wire-m1"

    raw = UpstreamRequest(
        target=target, body=b"\x00\x01binary", path="/v1/x", stream=False,
        body_kind="raw", content_type="audio/pcm",
    )
    async with up.open(raw, deadline=dl, budgets=bud) as stream:
        assert stream.body_modified is False
        assert stream.defaulted_keys == ()
    assert seen[-1].content == b"\x00\x01binary"
    assert seen[-1].headers["content-type"] == "audio/pcm"
    await up.aclose()


# ======================================================= B4: caps and bodies


def test_shipped_surface_limits_and_the_global_fallback():
    cfg = ServerConfig()
    globals_ = SurfaceLimits(cfg.max_request_bytes, cfg.max_response_bytes)
    assert "openai_chat" not in DEFAULT_SURFACE_LIMITS  # chat IS the globals
    assert cfg.limits_for("openai_chat") == globals_
    assert cfg.limits_for("anthropic_messages") == SurfaceLimits(
        32 * 1024 * 1024, cfg.max_response_bytes
    )
    assert cfg.limits_for("no_such_surface") == globals_
    # A global set in code lowers every surface without its own number.
    low = ServerConfig(max_request_bytes=1024 * 1024)
    assert low.limits_for("openai_chat").max_request_bytes == 1024 * 1024
    assert low.limits_for("anthropic_messages").max_request_bytes == 32 * 1024 * 1024


def test_surface_limit_env_overrides_win_over_the_global_and_the_table():
    env = {
        "LLMGW_MAX_REQUEST_BYTES": str(1024 * 1024),
        "LLMGW_MAX_REQUEST_BYTES__ANTHROPIC_MESSAGES": str(64 * 1024 * 1024),
        "LLMGW_MAX_RESPONSE_BYTES__AUDIO_SPEECH": str(16 * 1024 * 1024),
    }
    table = surface_limits_from_env(env)
    # Rows carry only what a variable set; the rest is `None` = inherit.
    assert "openai_chat" not in table
    assert table["anthropic_messages"].max_request_bytes == 64 * 1024 * 1024
    assert table["audio_speech"] == SurfaceLimits(None, 16 * 1024 * 1024)
    cfg = ServerConfig.from_env({**env, "LLMGW_FAKE_UPSTREAMS": "1"})
    # The plain global reaches chat (no row) and the unnamed half of a row...
    assert cfg.limits_for("openai_chat").max_request_bytes == 1024 * 1024
    assert cfg.limits_for("audio_speech") == SurfaceLimits(1024 * 1024, 16 * 1024 * 1024)
    # ...and a per-surface variable beats it.
    assert cfg.limits_for("anthropic_messages").max_request_bytes == 64 * 1024 * 1024


def test_a_non_integer_surface_limit_refuses_at_startup():
    with pytest.raises(ValueError, match="LLMGW_MAX_REQUEST_BYTES__OPENAI_CHAT"):
        surface_limits_from_env({"LLMGW_MAX_REQUEST_BYTES__OPENAI_CHAT": "lots"})


def test_a_zero_surface_limit_refuses_at_startup():
    with pytest.raises(ValueError, match="surface_limits"):
        ServerConfig(surface_limits={"openai_chat": SurfaceLimits(0, 1)}).validated()


def _multipart(fields: dict[str, bytes], *, boundary: str = "B0UNDARY", file_first=False):
    parts = []
    text = [(k, v) for k, v in fields.items() if k != "file"]
    file = [(k, v) for k, v in fields.items() if k == "file"]
    order = file + text if file_first else text + file
    for name, value in order:
        head = f'Content-Disposition: form-data; name="{name}"'
        if name == "file":
            head += '; filename="a.mp3"\r\nContent-Type: audio/mpeg'
        parts.append(f"--{boundary}\r\n{head}\r\n\r\n".encode() + value + b"\r\n")
    return b"".join(parts) + f"--{boundary}--\r\n".encode()


def test_multipart_boundary_parsing():
    assert appmod.multipart_boundary('multipart/form-data; boundary="x y"') == "x y"
    assert appmod.multipart_boundary("multipart/form-data; charset=utf-8; boundary=ab") == "ab"
    assert appmod.multipart_boundary("application/json") is None
    assert appmod.multipart_boundary(None) is None


def test_multipart_scan_finds_text_fields_and_skips_the_file():
    body = _multipart({"model": b"openai.gpt-transcribe", "stream": b"true",
                       "file": b"\xff" * 10_000})
    found = appmod.scan_multipart_fields(
        body, "B0UNDARY", wanted=frozenset({"model", "stream"})
    )
    assert found == {"model": "openai.gpt-transcribe", "stream": "true"}


def test_multipart_scan_is_bounded_so_a_late_model_field_is_not_found():
    body = _multipart({"file": b"\xff" * (appmod.MULTIPART_SCAN_BYTES + 10),
                       "model": b"late"}, file_first=True)
    found = appmod.scan_multipart_fields(body, "B0UNDARY", wanted=frozenset({"model"}))
    assert found == {}  # documented limitation: text fields before the file


def _scope(content_type: str | None = None, query: bytes = b"", **headers) -> dict:
    hs = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
    if content_type:
        hs.append((b"content-type", content_type.encode()))
    return {"type": "http", "headers": hs, "query_string": query}


class _Surface:
    """The three attributes `facts_for_body` reads; `parse_request` for json."""

    name = "test_surface"
    path = "/v1/x"

    def __init__(self, body: str) -> None:
        self.body = body

    def parse_request(self, body: bytes):
        return appmod.RequestFacts(model="from-json", stream=False)


def test_facts_for_json_bodies_still_ask_the_surface():
    facts = appmod.facts_for_body(
        _Surface("json"), b"{}", _scope(), content_type="application/json"
    )
    assert facts.model == "from-json"


def test_facts_for_multipart_read_the_leading_fields():
    body = _multipart({"model": b"m", "stream": b"1", "file": b"x"})
    facts = appmod.facts_for_body(
        _Surface("multipart"), body, _scope(),
        content_type="multipart/form-data; boundary=B0UNDARY",
    )
    assert (facts.model, facts.stream) == ("m", True)


def test_multipart_without_a_boundary_or_model_is_a_400_before_upstream():
    with pytest.raises(E.InvalidRequest, match="boundary"):
        appmod.facts_for_body(
            _Surface("multipart"), b"", _scope(), content_type="text/plain"
        )
    body = _multipart({"file": b"x"})
    with pytest.raises(E.InvalidRequest, match="model"):
        appmod.facts_for_body(
            _Surface("multipart"), body, _scope(),
            content_type="multipart/form-data; boundary=B0UNDARY",
        )


def test_a_multipart_surface_whose_model_is_a_header_reads_the_query_instead():
    """`assemblyai_sync` sends multipart, but its model goes upstream in
    `X-AAI-Model` and is not in the body at all. Scanning the form fields for
    it would fail closed on every correct request, and asking the caller to
    add a `model` part would put a part in the upload that the provider never
    asked for."""
    surface = _Surface("multipart")
    surface.model_header = "X-AAI-Model"
    body = _multipart({"audio": b"RIFF...."})
    facts = appmod.facts_for_body(
        surface, body, _scope(query=b"model=assemblyai.sync"),
        content_type="multipart/form-data; boundary=B0UNDARY",
    )
    assert (facts.model, facts.stream) == ("assemblyai.sync", False)
    facts = appmod.facts_for_body(
        surface, body, _scope(**{"X-Gw-Model": "assemblyai.sync"}),
        content_type="multipart/form-data; boundary=B0UNDARY",
    )
    assert facts.model == "assemblyai.sync"
    # Still fails closed, and still needs the boundary: a body the gateway
    # cannot route is not a body it forwards and lets the provider bill.
    with pytest.raises(E.InvalidRequest, match="X-Gw-Model"):
        appmod.facts_for_body(
            surface, body, _scope(),
            content_type="multipart/form-data; boundary=B0UNDARY",
        )
    with pytest.raises(E.InvalidRequest, match="boundary"):
        appmod.facts_for_body(surface, body, _scope(query=b"model=x"),
                              content_type="text/plain")


def test_facts_for_raw_bodies_come_from_the_query_or_the_header():
    facts = appmod.facts_for_body(
        _Surface("raw"), b"\x00", _scope(query=b"model=m%2E1&stream=true"),
        content_type="audio/pcm",
    )
    assert (facts.model, facts.stream) == ("m.1", True)
    facts = appmod.facts_for_body(
        _Surface("raw"), b"\x00", _scope(**{"X-Gw-Model": "m2"}), content_type="audio/pcm"
    )
    assert (facts.model, facts.stream) == ("m2", False)
    with pytest.raises(E.InvalidRequest, match="X-Gw-Model"):
        appmod.facts_for_body(_Surface("raw"), b"\x00", _scope(), content_type="audio/pcm")


# ================================================================ B2: scrub


class _Sent:
    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)


def _exchange():
    from llmgw.policy import PolicySnapshot
    from llmgw.server.config import ServerConfig as _C

    cfg = _C(fake_upstreams=True)
    snap = PolicySnapshot.single_target(
        cfg.default_model, catalog=cfg.catalog, budgets=cfg.budgets
    )
    return appmod.Exchange(snap, catalog_id="cat_t", workload_id="default")


async def test_scrub_all_replaces_every_non_2xx_body_and_keeps_the_status():
    err = E.UpstreamServerError(
        "boom", provider="inworld", upstream_status=500,
        upstream_body=b'{"code":13,"message":"internal; key Zm9v***"}',
    )
    assert err.passthrough  # C4 would forward this body...
    sent = _Sent()
    await appmod.send_error(sent, err, exchange=_exchange(), scrub_all=True)
    start, body = sent.messages
    assert start["status"] == 500
    assert b"Zm9v" not in body["body"]
    assert json.loads(body["body"])["error"]["type"] == err.code
    # ...and without the flag it still does (C4 unchanged for other providers).
    sent2 = _Sent()
    await appmod.send_error(sent2, err, exchange=_exchange(), scrub_all=False)
    assert b"Zm9v" in sent2.messages[1]["body"]


# ============================================================== B6: budgets


def test_headers_budget_has_its_own_env_knob_and_default():
    if "headers" not in Budgets.__dataclass_fields__:
        pytest.skip("clocks.Budgets.headers has not landed yet")
    cfg = ServerConfig.from_env({"LLMGW_FAKE_UPSTREAMS": "1"})
    assert cfg.budgets.headers == 10.0
    assert cfg.budgets.connect == 2.0
    cfg = ServerConfig.from_env({"LLMGW_FAKE_UPSTREAMS": "1", "LLMGW_BUDGET_HEADERS": "4.5"})
    assert cfg.budgets.headers == 4.5


async def test_status_line_wait_is_bounded_by_headers_not_connect():
    """A transport that connects at once and answers late: with `connect`
    at 2 s and `headers` at 6 s the request is still open at 3 s and is a
    `HeadersTimeout` at 6 s. Before B6 it was a `HeadersTimeout` at 2 s."""
    if "headers" not in Budgets.__dataclass_fields__:
        pytest.skip("clocks.Budgets.headers has not landed yet")
    clock = ManualClock(start=0.0)

    async def handle(request: httpx.Request) -> httpx.Response:
        await clock.sleep(500.0)
        return streamed(200)

    provider = conn()
    up = Upstream(catalog_for(provider), transport=httpx.MockTransport(handle))
    target = catalog_for(provider).resolve(f"m-{provider.id}")
    req = UpstreamRequest(target=target, body=b"{}", path="/v1/x", stream=True)
    dl = Deadline(clock, total=100.0)
    bud = Budgets(total=100.0, connect=2.0, headers=6.0)

    async def go():
        async with up.open(req, deadline=dl, budgets=bud):
            pass  # pragma: no cover - never opens

    task = asyncio.create_task(go())
    await clock.advance(3.0)
    assert not task.done(), "2 s connect budget must no longer end the headers wait"
    await clock.advance(3.5)
    with pytest.raises(E.HeadersTimeout):
        await task
    await up.aclose()


def test_connect_budget_rides_on_the_request_as_an_httpx_connect_timeout():
    """The connect budget becomes httpx's per-request connect timeout, so a
    breach there is `httpx.ConnectTimeout` -> `ConnectTimeout` (retry-safe)."""
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return streamed(200)

    provider = conn()
    up = Upstream(catalog_for(provider), transport=httpx.MockTransport(handler))
    target = catalog_for(provider).resolve(f"m-{provider.id}")
    req = UpstreamRequest(target=target, body=b"{}", path="/v1/x", stream=False)
    clock = ManualClock(start=0.0)

    async def go():
        async with up.open(req, deadline=Deadline(clock, total=50.0),
                           budgets=Budgets(total=50.0, connect=1.5)):
            pass

    asyncio.run(go())
    timeout = seen[0].extensions["timeout"]
    assert timeout["connect"] == 1.5
    assert timeout["read"] is None and timeout["write"] is None
    asyncio.run(up.aclose())


def test_drain_arithmetic_is_checked_against_the_largest_policy_total():
    """`check_drain_arithmetic` (B6): a workload total above the grace is
    refused even though the global `budgets.total` is under it."""
    from llmgw.policy import PolicySnapshot

    cfg = ServerConfig(fake_upstreams=True, drain_grace_seconds=130.0)
    ok = PolicySnapshot.single_target(
        cfg.default_model, catalog=cfg.catalog, budgets=Budgets(total=120.0)
    )
    cfg.check_drain_arithmetic(ok)  # no raise
    long = PolicySnapshot.single_target(
        cfg.default_model, catalog=cfg.catalog, budgets=Budgets(total=600.0)
    )
    with pytest.raises(ValueError, match="600"):
        cfg.check_drain_arithmetic(long)
    lenient = dataclasses.replace(cfg, drain_allow_short=True)
    lenient.check_drain_arithmetic(long)  # warns instead

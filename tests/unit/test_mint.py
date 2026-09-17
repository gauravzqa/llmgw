"""Phase E: token minting. The merge/cap rules (C19), the session cap in
admission, the tenants-file schema, and the AssemblyAI query clamp; plus
`join_url`'s provider prefix (Phase C4)."""

from __future__ import annotations

import json

import pytest

from llmgw.admission import AdmissionController, TenantLimits
from llmgw.catalog import DEFAULT_CATALOG, ModelSpec, ProviderConn, Target
from llmgw.clocks import ManualClock
from llmgw.errors import AdmissionRejected
from llmgw.server.config import TenantTable
from llmgw.surfaces import ASSEMBLYAI_TOKEN, REALTIME_CONTROL
from llmgw.surfaces.assemblyai_token import capped_query
from llmgw.surfaces.realtime_control import (
    DEFAULT_REALTIME_MODEL,
    SAFETY_IDENTIFIER_HEADER,
    capped_ttl,
    merge_session,
)
from llmgw.upstream import join_url

# --------------------------------------------------------------------- merge


def _target(api_model: str = "gpt-realtime-mini") -> Target:
    return Target(
        model=ModelSpec(id="openai.gpt-realtime-mini", provider="openai",
                        api_model=api_model, input_per_m=0.6, output_per_m=2.4,
                        priced_at="2026-09-18"),
        provider=DEFAULT_CATALOG.providers["openai"],
    )


def test_pinned_fields_win_over_the_clients_and_land_in_both_shapes():
    session, pinned = merge_session(
        {"type": "realtime", "model": "openai.gpt-realtime-mini", "voice": "marin",
         "tools": [{"type": "function", "name": "nuke"}], "max_output_tokens": "inf"},
        {"voice": "cedar", "tools": [], "max_output_tokens": 512,
         "turn_detection": {"type": "server_vad", "silence_duration_ms": 300}},
        wire_model="gpt-realtime-mini",
    )
    assert session["model"] == "gpt-realtime-mini"          # the wire id, always
    assert session["voice"] == "cedar"
    assert session["audio"]["output"]["voice"] == "cedar"
    assert session["audio"]["input"]["turn_detection"]["type"] == "server_vad"
    assert session["tools"] == []                             # the client cannot widen
    assert session["max_output_tokens"] == 512
    assert set(pinned) == {"voice", "tools", "max_output_tokens", "turn_detection"}


def test_without_a_pin_the_client_session_passes_with_the_wire_model():
    session, pinned = merge_session({"voice": "marin"}, None, wire_model="gpt-realtime-mini")
    assert session == {"voice": "marin", "type": "realtime", "model": "gpt-realtime-mini"}
    assert pinned == ()


@pytest.mark.parametrize(
    ("client", "cap", "grace", "expected"),
    [
        (None, None, 130.0, 130),        # nothing asked: tenant default 600, grace wins
        (3600, None, 130.0, 130),        # client asks for an hour: grace wins
        (60, 600, 130.0, 60),            # client asks for less: client wins
        (None, 300, 7200.0, 300),        # tenant cap stands in for the client
        (99999, 99999, 99999.0, 7200),   # never above OpenAI's maximum
        (1, 600, 130.0, 10),             # never below OpenAI's minimum
    ],
)
def test_ttl_is_the_smallest_of_client_tenant_and_grace(client, cap, grace, expected):
    assert capped_ttl(client, tenant_cap=cap, grace_s=grace) == expected


def test_prepare_mint_merges_caps_and_stamps_the_tenant():
    body = json.dumps({"session": {"type": "realtime", "voice": "marin"},
                       "expires_after": {"anchor": "created_at", "seconds": 3600}}).encode()
    mint = REALTIME_CONTROL.prepare_mint(
        body, route="/v1/realtime/client_secrets", target=_target(), tenant="layrs",
        pin={"voice": "cedar", "expires_after_seconds_cap": 600}, grace_s=130.0,
    )
    assert mint is not None
    out = json.loads(mint.body)
    assert out["session"]["voice"] == "cedar"
    assert out["session"]["model"] == "gpt-realtime-mini"
    assert out["expires_after"] == {"anchor": "created_at", "seconds": 130}
    assert mint.ttl_s == 130.0
    assert mint.extra_headers == {SAFETY_IDENTIFIER_HEADER: "layrs"}
    assert mint.pinned_keys == ("voice",)


def test_the_calls_route_is_a_plain_passthrough():
    assert REALTIME_CONTROL.prepare_mint(
        b"{}", route="/v1/realtime/calls/{call_id}/{action}", target=_target(),
        tenant="t", pin=None, grace_s=130.0,
    ) is None


def test_the_mint_model_comes_from_the_session_or_the_default():
    assert REALTIME_CONTROL.parse_request(
        b'{"session": {"model": "openai.gpt-realtime-mini"}}'
    ).model == "openai.gpt-realtime-mini"
    assert REALTIME_CONTROL.parse_request(b"").model == DEFAULT_REALTIME_MODEL
    assert REALTIME_CONTROL.parse_request(b"{}").model == DEFAULT_REALTIME_MODEL


# ------------------------------------------------------------ assemblyai query


def test_the_assemblyai_query_is_clamped_to_the_grace_and_600s():
    q, ttl = capped_query("expires_in_seconds=9999&max_session_duration_seconds=10800",
                          grace_s=130.0)
    assert "expires_in_seconds=600" in q
    assert "max_session_duration_seconds=130" in q
    assert ttl == 130.0
    q2, ttl2 = capped_query("", grace_s=7200.0)
    assert "max_session_duration_seconds=7200" in q2 and ttl2 == 7200.0
    q3, _ = capped_query("max_session_duration_seconds=30&foo=bar", grace_s=600.0)
    assert "max_session_duration_seconds=60" in q3 and "foo=bar" in q3  # provider minimum
    assert ASSEMBLYAI_TOKEN.forward_query is True and ASSEMBLYAI_TOKEN.methods == ("GET",)


# --------------------------------------------------------------- max_sessions


def test_max_sessions_is_released_by_the_clock_not_by_a_request():
    clock = ManualClock()
    ctl = AdmissionController(clock=clock)
    ctl.configure("t", TenantLimits(rate_per_second=10, burst=10, max_concurrency=4,
                                    max_sessions=2))
    ctl.reserve_session("t", 100.0)
    ctl.reserve_session("t", 50.0)
    with pytest.raises(AdmissionRejected) as exc:
        ctl.reserve_session("t", 100.0)
    assert exc.value.retry_after == pytest.approx(50.0)
    assert ctl.live_sessions("t") == 2
    assert ctl.denials()["tenant_concurrency"] == 1


def test_max_sessions_frees_a_slot_when_a_credential_expires():
    clock = ManualClock()
    ctl = AdmissionController(clock=clock)
    ctl.configure("t", TenantLimits(rate_per_second=10, burst=10, max_concurrency=4,
                                    max_sessions=1))
    ctl.reserve_session("t", 10.0)
    with pytest.raises(AdmissionRejected):
        ctl.reserve_session("t", 10.0)
    import asyncio

    asyncio.run(clock.advance(11.0))
    ctl.reserve_session("t", 10.0)  # the old one expired
    assert ctl.live_sessions("t") == 1
    assert ctl.snapshot()["t"]["sessions"] == 1


def test_no_max_sessions_means_no_cap():
    default = TenantLimits(rate_per_second=1, burst=1, max_concurrency=1)
    ctl = AdmissionController(clock=ManualClock(), default=default)
    for _ in range(50):
        ctl.reserve_session("anon", 600.0)
    assert ctl.live_sessions("anon") == 0  # uncapped tenants are not tracked


def test_tenant_limits_validate_max_sessions():
    with pytest.raises(ValueError):
        TenantLimits(rate_per_second=1, burst=1, max_concurrency=1, max_sessions=0).validate()
    TenantLimits(rate_per_second=1, burst=1, max_concurrency=1, max_sessions=None).validate()


# ------------------------------------------------------------- tenants file


TENANTS = '''
[tenants.layrs]
tokens = ["tok-layrs"]
rate_per_second = 50.0
burst = 100
max_concurrency = 96
max_sessions = 8

[tenants.layrs.realtime]
model = "openai.gpt-realtime-mini"
voice = "cedar"
max_output_tokens = 1024
expires_after_seconds_cap = 600

[tenants.acme]
tokens = ["tok-acme"]
rate_per_second = 1.0
burst = 1
max_concurrency = 1
'''


def test_the_tenants_file_carries_max_sessions_and_a_realtime_pin():
    table = TenantTable.from_toml(TENANTS, env={})
    assert table.limits["layrs"].max_sessions == 8
    assert table.limits["acme"].max_sessions is None
    pin = table.realtime_pin("layrs")
    assert pin is not None and pin["voice"] == "cedar"
    assert pin["expires_after_seconds_cap"] == 600
    assert table.realtime_pin("acme") is None
    with pytest.raises(TypeError):
        pin["voice"] = "marin"  # type: ignore[index] - frozen


def test_the_realtime_pin_rejects_unknown_keys_and_a_tiny_cap():
    bad = TENANTS.replace('voice = "cedar"', 'voise = "cedar"')
    with pytest.raises(ValueError, match="unknown keys"):
        TenantTable.from_toml(bad, env={})
    tiny = TENANTS.replace("expires_after_seconds_cap = 600", "expires_after_seconds_cap = 5")
    with pytest.raises(ValueError, match="expires_after_seconds_cap"):
        TenantTable.from_toml(tiny, env={})


def test_the_shipped_tenants_file_parses_with_its_pin():
    from pathlib import Path

    text = Path("config/tenants.toml").read_text()
    table = TenantTable.from_toml(text, env={"LLMGW_TENANT_LAYRS_TOKEN": "x"})
    assert table.limits["layrs"].max_sessions == 8
    pin = table.realtime_pin("layrs")
    assert pin is not None and pin["model"] == "openai.gpt-realtime-mini"


# ------------------------------------------------------------ join_url prefix


def test_join_url_inserts_the_provider_prefix_and_drops_the_v1_segment():
    assert join_url("https://api.deepseek.com", "/v1/chat/completions", prefix="/beta") == (
        "https://api.deepseek.com/beta/chat/completions"
    )
    assert join_url(
        "https://api.deepseek.com/beta", "/v1/chat/completions", prefix="/beta"
    ) == "https://api.deepseek.com/beta/chat/completions"
    assert join_url("https://api.deepseek.com", "/v1/chat/completions") == (
        "https://api.deepseek.com/v1/chat/completions"
    )
    assert join_url("https://openrouter.ai/api/v1", "/v1/chat/completions") == (
        "https://openrouter.ai/api/v1/chat/completions"
    )


def test_the_deepseek_beta_row_produces_the_documented_url():
    row = DEFAULT_CATALOG.providers.get("deepseek-beta")
    if row is None:
        pytest.skip("deepseek-beta provider row not shipped")
    assert isinstance(row, ProviderConn)
    assert join_url(row.base_url, "/v1/chat/completions", prefix=row.path_prefix) == (
        "https://api.deepseek.com/beta/chat/completions"
    )


# ------------------------------------------------- C4: DeepSeek behind Anthropic


def test_deepseek_on_the_anthropic_dialect_is_a_same_dialect_candidate():
    """The `deepseek-anthropic` provider row makes DeepSeek a legal candidate
    for an Anthropic workload; without it the cross-dialect rule refuses the
    plan. The model row is the catalog's to ship; this adds one to prove the
    policy accepts the pairing."""
    from llmgw.policy import PolicySnapshot

    provider = DEFAULT_CATALOG.providers.get("deepseek-anthropic")
    if provider is None:
        pytest.skip("deepseek-anthropic provider row not shipped")
    catalog = DEFAULT_CATALOG.with_overrides(models={
        "deepseek-anthropic.deepseek-v4-flash": ModelSpec(
            id="deepseek-anthropic.deepseek-v4-flash", provider="deepseek-anthropic",
            api_model="deepseek-flash", input_per_m=0.30, output_per_m=1.20,
            priced_at="2026-09-16",
        ),
    })
    snap = PolicySnapshot.from_toml(
        '''
default_workload = "cheap"
[workloads.cheap]
candidate = "deepseek-anthropic.deepseek-v4-flash"
incumbent = "anthropic.haiku-4-5"
''',
        catalog=catalog,
    )
    plan = snap.plan_for("cheap")
    assert [t.provider.id for t in plan.targets] == ["deepseek-anthropic", "anthropic"]
    assert plan.targets[0].provider.kind == "anthropic"


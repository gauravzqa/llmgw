"""Tenants required in production, and tokens that live in the environment.

Two properties of `TenantTable` / `ServerConfig`, both about the same
deployment mistake: a gateway with more than one caller running on the
zero-config path where every request is one anonymous tenant and bearer
tokens are decoration (FAILURE-MODES row 6, row 23).

    * `LLMGW_REQUIRE_TENANTS=1` makes that path a refusal to start, at
      `validated()` when there is no file and at `tenant_table()` when the
      file defines nobody a token can name.
    * `token_env` / `token_envs` let the tenants file carry ids and limits
      and nothing secret, so it can be committed; the token values ride in
      the environment, resolved at load, and a missing one is a startup
      error that names the variable.

Nothing here touches a socket; `env` is passed explicitly everywhere so the
tests are independent of the developer's shell.
"""

from __future__ import annotations

import pytest

from llmgw.server.config import ANONYMOUS_TENANT, ServerConfig, TenantTable

LIMITS = """
rate_per_second = 10.0
burst = 20
max_concurrency = 4
"""


def _toml(*tables: str) -> str:
    return "\n".join(tables)


# --------------------------------------------------------------------------
# token_env resolution
# --------------------------------------------------------------------------


def test_token_env_resolves_from_the_supplied_environment():
    table = TenantTable.from_toml(
        _toml('[tenants.layrs]\ntoken_env = "LLMGW_TENANT_LAYRS_TOKEN"' + LIMITS),
        env={"LLMGW_TENANT_LAYRS_TOKEN": "tok-from-env"},
    )
    assert table.resolve("tok-from-env") == "layrs"
    assert table.authenticated_tenants == 1


def test_token_envs_list_and_literal_tokens_combine():
    table = TenantTable.from_toml(
        _toml(
            '[tenants.acme]\n'
            'tokens = ["tok-literal"]\n'
            'token_envs = ["ACME_A", "ACME_B"]' + LIMITS
        ),
        env={"ACME_A": "tok-a", "ACME_B": "tok-b"},
    )
    for tok in ("tok-literal", "tok-a", "tok-b"):
        assert table.resolve(tok) == "acme"
    assert table.authenticated_tenants == 1


@pytest.mark.parametrize("env", [{}, {"LLMGW_TENANT_LAYRS_TOKEN": ""},
                                 {"LLMGW_TENANT_LAYRS_TOKEN": "   "}])
def test_an_unset_or_empty_token_env_refuses_to_load_and_names_the_variable(env):
    with pytest.raises(ValueError) as exc:
        TenantTable.from_toml(
            _toml('[tenants.layrs]\ntoken_env = "LLMGW_TENANT_LAYRS_TOKEN"' + LIMITS),
            env=env,
        )
    message = str(exc.value)
    assert "LLMGW_TENANT_LAYRS_TOKEN" in message
    assert "[tenants.layrs]" in message


def test_a_named_tenant_with_no_token_at_all_is_refused():
    with pytest.raises(ValueError, match=r"\[tenants\.ghost\]: no tokens"):
        TenantTable.from_toml(_toml("[tenants.ghost]" + LIMITS), env={})


def test_anonymous_is_the_one_tenant_allowed_no_token():
    table = TenantTable.from_toml(
        _toml(f"[tenants.{ANONYMOUS_TENANT}]" + LIMITS), env={}
    )
    assert ANONYMOUS_TENANT in table
    assert table.authenticated_tenants == 0


def test_an_env_token_shared_by_two_tenants_is_refused_without_printing_it():
    with pytest.raises(ValueError) as exc:
        TenantTable.from_toml(
            _toml(
                '[tenants.a]\ntoken_env = "T_A"' + LIMITS,
                '[tenants.b]\ntoken_env = "T_B"' + LIMITS,
            ),
            env={"T_A": "same-secret", "T_B": "same-secret"},
        )
    assert "same-secret" not in str(exc.value)
    assert "already assigned to tenant 'a'" in str(exc.value)


def test_token_env_must_be_a_string():
    with pytest.raises(ValueError, match="token_env names"):
        TenantTable.from_toml(_toml("[tenants.a]\ntoken_env = 7" + LIMITS), env={})


def test_repr_never_carries_a_token_from_either_source():
    table = TenantTable.from_toml(
        _toml('[tenants.a]\ntokens = ["lit-secret"]\ntoken_env = "T_A"' + LIMITS),
        env={"T_A": "env-secret"},
    )
    text = repr(table)
    assert "lit-secret" not in text and "env-secret" not in text
    assert "tokens=2" in text


# --------------------------------------------------------------------------
# require_tenants
# --------------------------------------------------------------------------


def test_require_tenants_without_a_file_refuses_to_start_naming_both_vars():
    with pytest.raises(ValueError) as exc:
        ServerConfig(require_tenants=True).validated()
    assert "LLMGW_REQUIRE_TENANTS" in str(exc.value)
    assert "LLMGW_TENANTS_FILE" in str(exc.value)


def test_require_tenants_from_env_refuses_without_a_file():
    with pytest.raises(ValueError, match="LLMGW_TENANTS_FILE"):
        ServerConfig.from_env({"LLMGW_REQUIRE_TENANTS": "1"})


def test_require_tenants_defaults_off_so_the_zero_config_path_keeps_working():
    assert ServerConfig().require_tenants is False
    assert ServerConfig.from_env({}).require_tenants is False


def test_require_tenants_with_only_anonymous_in_the_file_is_refused(tmp_path):
    path = tmp_path / "tenants.toml"
    path.write_text(_toml(f"[tenants.{ANONYMOUS_TENANT}]" + LIMITS))
    config = ServerConfig(tenants_file=str(path), require_tenants=True)
    with pytest.raises(ValueError, match="no tenant a bearer token can name"):
        config.tenant_table(env={})


def test_require_tenants_with_an_env_token_starts(tmp_path):
    path = tmp_path / "tenants.toml"
    path.write_text(_toml('[tenants.layrs]\ntoken_env = "LLMGW_TENANT_LAYRS_TOKEN"' + LIMITS))
    config = ServerConfig(tenants_file=str(path), require_tenants=True)
    table = config.tenant_table(env={"LLMGW_TENANT_LAYRS_TOKEN": "tok-layrs"})
    assert table is not None
    assert table.resolve("tok-layrs") == "layrs"


def test_require_tenants_with_the_env_var_missing_is_a_startup_error(tmp_path):
    path = tmp_path / "tenants.toml"
    path.write_text(_toml('[tenants.layrs]\ntoken_env = "LLMGW_TENANT_LAYRS_TOKEN"' + LIMITS))
    config = ServerConfig(tenants_file=str(path), require_tenants=True)
    with pytest.raises(ValueError) as exc:
        config.tenant_table(env={})
    assert "LLMGW_TENANT_LAYRS_TOKEN" in str(exc.value)
    assert str(path) in str(exc.value)


def test_the_shipped_example_file_loads_with_its_env_var_set():
    """The example is the documentation; it must parse with exactly the
    variable it names and refuse without it."""
    text = open("config/tenants.example.toml", encoding="utf-8").read()
    table = TenantTable.from_toml(text, env={"LLMGW_TENANT_LAYRS_TOKEN": "tok-example"})
    assert table.resolve("tok-example") == "layrs"
    with pytest.raises(ValueError, match="LLMGW_TENANT_LAYRS_TOKEN"):
        TenantTable.from_toml(text, env={})


# ---------------------------------------------------------------- default route
# LLMGW_REQUIRE_TENANTS is the production switch. In production the
# zero-policy default route must not be the local fake: the code default is
# `fake.echo`, and a fly.toml that forgets LLMGW_DEFAULT_MODEL would answer
# every request with a 502 while /healthz stays green.


def test_production_mode_refuses_a_fake_default_route(tmp_path):
    path = tmp_path / "tenants.toml"
    path.write_text(_toml("[tenants.layrs]\ntokens = [\"t\"]" + LIMITS))
    config = ServerConfig(tenants_file=str(path), require_tenants=True)
    assert config.default_model == "fake.echo"
    with pytest.raises(ValueError, match="routes to the fake provider"):
        config.validated()


def test_production_mode_accepts_a_real_default_route(tmp_path):
    path = tmp_path / "tenants.toml"
    path.write_text(_toml("[tenants.layrs]\ntokens = [\"t\"]" + LIMITS))
    config = ServerConfig(
        tenants_file=str(path), require_tenants=True, default_model="openai.gpt-4o-mini",
    )
    assert config.validated() is config


def test_fake_default_route_is_fine_outside_production_mode():
    # The zero-config `make run` + curl path must keep working.
    assert ServerConfig().validated().default_model == "fake.echo"

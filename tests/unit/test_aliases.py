"""Model aliases (PLAN-2 A1): the echoed wire id must resolve on the next turn.

The bug this guards was observed live on 16 Sep 2026: OpenAI answers
`"model": "gpt-4o-mini-2024-07-18"` to a request for `openai.gpt-4o-mini`, an
SDK loop copies that into its next request, and the gateway -- which knew only
catalog ids -- answered `400 unknown model` on turn two. Resolution order and
the ambiguity rule are the whole of this file.
"""

from __future__ import annotations

import pytest

from llmgw import errors as E
from llmgw.catalog import DEFAULT_CATALOG, Catalog, ModelSpec, ProviderConn
from llmgw.errors import PolicyError


def _conn(pid: str, kind: str = "openai") -> ProviderConn:
    return ProviderConn(id=pid, kind=kind, base_url="http://x", api_key_env="K")


def _spec(mid: str, provider: str, api_model: str, aliases: tuple[str, ...] = ()) -> ModelSpec:
    return ModelSpec(id=mid, provider=provider, api_model=api_model, aliases=aliases,
                     input_per_m=1.0, output_per_m=2.0, priced_at="2026-09-16")


# ---------------------------------------------------------------- the shipped table


@pytest.mark.parametrize(
    ("sent", "canonical"),
    [
        ("openai.gpt-4o-mini", "openai.gpt-4o-mini"),
        ("gpt-4o-mini", "openai.gpt-4o-mini"),                  # the wire id
        ("gpt-4o-mini-2024-07-18", "openai.gpt-4o-mini"),       # the echoed snapshot
        ("claude-haiku-4-5-20251001", "anthropic.haiku-4-5"),   # wire id
        ("claude-haiku-4-5", "anthropic.haiku-4-5"),            # short alias
        # `deepseek-flash` is shared with the Anthropic-dialect row since C4;
        # it resolves by the route's dialect (see the test below), not bare.
        ("deepseek-v4-flash", "deepseek.deepseek-v4-flash"),    # retired wire id
        ("claude-sonnet-5", "anthropic.sonnet-5"),
    ],
)
def test_shipped_aliases_resolve_to_their_catalog_id(sent: str, canonical: str):
    assert DEFAULT_CATALOG.canonical_id(sent) == canonical
    assert DEFAULT_CATALOG.resolve(sent).model.id == canonical


def test_a_wire_id_shared_across_dialects_resolves_by_route_kind():
    """C4: `deepseek-flash` is the wire id of both `deepseek.deepseek-v4-flash`
    (OpenAI dialect) and `deepseek-anthropic.deepseek-v4-flash` (Anthropic
    dialect). The server always passes the route's dialect, so each surface
    reaches its own row; the bare string is ambiguous by design."""
    assert DEFAULT_CATALOG.canonical_id("deepseek-flash", kind="openai") == (
        "deepseek.deepseek-v4-flash")
    assert DEFAULT_CATALOG.canonical_id("deepseek-flash", kind="anthropic") == (
        "deepseek-anthropic.deepseek-v4-flash")
    with pytest.raises(PolicyError, match="shared by"):
        DEFAULT_CATALOG.canonical_id("deepseek-flash")


def test_the_alias_table_is_visible_for_probe_and_debugging():
    table = DEFAULT_CATALOG.aliases
    assert table["gpt-4o-mini-2024-07-18"] == "openai.gpt-4o-mini"
    assert table["deepseek-v4-flash"] == "deepseek.deepseek-v4-flash"
    # Catalog ids never appear as keys: exact ids resolve before the table.
    assert not (set(table) & set(DEFAULT_CATALOG.models))


def test_an_unknown_string_is_still_a_policy_error_with_neutral_health():
    with pytest.raises(E.PolicyError, match="unknown model") as ei:
        DEFAULT_CATALOG.resolve("nope-nope-nope")
    assert ei.value.health is E.Health.NEUTRAL
    assert ei.value.blame is E.Blame.POLICY


# ---------------------------------------------------------------- resolution order


def test_an_exact_catalog_id_wins_over_an_alias_of_the_same_spelling():
    """A declared alias that happens to equal another model's catalog id
    never shadows it: exact ids are checked first, and the alias is simply
    not indexed."""
    cat = Catalog(
        providers={"p": _conn("p")},
        models={
            "a.one": _spec("a.one", "p", "wire-one", aliases=("a.two",)),
            "a.two": _spec("a.two", "p", "wire-two"),
        },
    )
    assert cat.canonical_id("a.two") == "a.two"
    assert "a.two" not in cat.aliases


def test_a_shared_wire_id_across_providers_is_ambiguous_not_guessed():
    """`fake.echo` and `fake.echo-anthropic` both answer to `fake-echo`. A
    gateway that picks one by luck bills the wrong provider on the unlucky
    request, so the bare wire id is refused with both candidates named."""
    with pytest.raises(E.PolicyError, match="shared by") as ei:
        DEFAULT_CATALOG.canonical_id("fake-echo")
    assert "fake.echo" in ei.value.message and "fake.echo-anthropic" in ei.value.message
    assert ei.value.health is E.Health.NEUTRAL


def test_a_declared_alias_breaks_a_wire_id_tie_in_its_favour():
    """Two models share a wire string; one of them claims it explicitly. The
    explicit claim is the operator saying which one a bare id means."""
    cat = Catalog(
        providers={"p": _conn("p"), "q": _conn("q")},
        models={
            "a.p": _spec("a.p", "p", "shared", aliases=("shared",)),
            "a.q": _spec("a.q", "q", "shared"),
        },
    )
    assert cat.canonical_id("shared") == "a.p"


def test_the_same_alias_declared_twice_fails_at_construction():
    """Nobody writes one alias on two rows on purpose. A typo that would
    otherwise be resolved by dict order is refused where it was made."""
    with pytest.raises(ValueError, match="declared by more than one model"):
        Catalog(
            providers={"p": _conn("p")},
            models={
                "a.one": _spec("a.one", "p", "w1", aliases=("dup",)),
                "a.two": _spec("a.two", "p", "w2", aliases=("dup",)),
            },
        )


def test_a_model_may_not_alias_its_own_id():
    with pytest.raises(ValueError, match="alias its own id"):
        _spec("a.one", "p", "w1", aliases=("a.one",))


def test_with_overrides_rebuilds_the_alias_table():
    """`with_overrides` constructs a new catalog, so an override that changes
    a wire id changes what the bare wire id resolves to."""
    cat = DEFAULT_CATALOG.with_overrides(models={
        "openai.gpt-4o-mini": _spec("openai.gpt-4o-mini", "openai", "gpt-4o-mini-renamed"),
    })
    assert cat.canonical_id("gpt-4o-mini-renamed") == "openai.gpt-4o-mini"
    with pytest.raises(E.PolicyError):
        cat.canonical_id("gpt-4o-mini-2024-07-18")  # the override dropped the alias


def test_the_route_dialect_breaks_a_cross_dialect_wire_id_tie():
    """A DeepSeek row on the OpenAI dialect and a DeepSeek row on the Anthropic-
    compatible endpoint (PLAN-2 C4) will share `deepseek-flash`. A bare wire id
    on `/v1/chat/completions` can only mean the OpenAI-shaped one, because the
    other could not serve that body; the server passes the route's kind."""
    cat = Catalog(
        providers={"oai": _conn("oai", "openai"), "anth": _conn("anth", "anthropic")},
        models={
            "ds.chat": _spec("ds.chat", "oai", "deepseek-flash"),
            "ds.messages": _spec("ds.messages", "anth", "deepseek-flash"),
        },
    )
    assert cat.canonical_id("deepseek-flash", kind="openai") == "ds.chat"
    assert cat.resolve("deepseek-flash", kind="anthropic").model.id == "ds.messages"
    with pytest.raises(E.PolicyError, match="shared by"):
        cat.canonical_id("deepseek-flash")


def test_a_dialect_hint_does_not_rescue_a_tie_within_one_dialect():
    cat = Catalog(
        providers={"p": _conn("p"), "q": _conn("q")},
        models={"a.p": _spec("a.p", "p", "shared"), "a.q": _spec("a.q", "q", "shared")},
    )
    with pytest.raises(E.PolicyError, match="shared by"):
        cat.canonical_id("shared", kind="openai")


def test_plan_for_accepts_an_alias_and_a_dialect_hint():
    from llmgw.policy import PolicySnapshot

    cat = Catalog(
        providers={"oai": _conn("oai", "openai"), "anth": _conn("anth", "anthropic")},
        models={
            "ds.chat": _spec("ds.chat", "oai", "deepseek-flash", aliases=("ds-old",)),
            "ds.messages": _spec("ds.messages", "anth", "deepseek-flash"),
        },
    )
    snap = PolicySnapshot.single_target("ds.chat", catalog=cat)
    assert snap.plan_for(model="ds-old").targets[0].model.id == "ds.chat"
    hinted = snap.plan_for(model="deepseek-flash", kind="openai")
    assert hinted.targets[0].model.id == "ds.chat"
    with pytest.raises(E.PolicyError, match="shared by"):
        snap.plan_for(model="deepseek-flash")

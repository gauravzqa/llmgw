"""Catalog tests. Mostly about failing at startup rather than on the hot path."""

from __future__ import annotations

import datetime as dt

import pytest

from llmgw import errors as E
from llmgw.catalog import (
    DEFAULT_CATALOG,
    MODELS,
    PROVIDERS,
    Catalog,
    ModelSpec,
    ProviderConn,
    price_of,
)


def test_every_model_resolves_to_a_real_provider():
    """Construction-time validation. A dangling provider reference discovered
    on the request path becomes a 3 a.m. error three layers below where it
    appears; discovered at import it is a one-line fix."""
    for model in DEFAULT_CATALOG.models.values():
        assert model.provider in DEFAULT_CATALOG.providers


def test_a_dangling_provider_reference_fails_at_construction():
    bad = ModelSpec(id="x.y", provider="nope", api_model="y",
                    input_per_m=1, output_per_m=1, priced_at="2026-01-01")
    with pytest.raises(ValueError, match="unknown provider"):
        Catalog(models={"x.y": bad}, providers={})


def test_a_model_without_a_price_date_is_rejected():
    """A price you cannot date is a price you cannot defend in a billing
    dispute."""
    with pytest.raises(ValueError, match="priced_at"):
        ModelSpec(id="x.y", provider="anthropic", api_model="y",
                  input_per_m=1, output_per_m=1)


def test_resolve_unknown_model_is_a_policy_error_not_a_key_error():
    """Blame matters: this is our config, not the provider's fault, and it
    must never touch a provider's breaker."""
    with pytest.raises(E.PolicyError) as ei:
        DEFAULT_CATALOG.resolve("nope.nope")
    assert ei.value.health is E.Health.NEUTRAL
    assert ei.value.blame is E.Blame.POLICY


def test_variant_providers_share_one_credential_key():
    """`openrouter-toolsafe` is the same account as `openrouter` with routing
    pinned. They must share a breaker and a concurrency cap -- treating them
    as two credentials would let the pair jointly exceed the real key's
    limit while both look compliant."""
    plain = DEFAULT_CATALOG.providers["openrouter"]
    pinned = DEFAULT_CATALOG.providers["openrouter-toolsafe"]
    assert plain.key() == pinned.key() == "openrouter"


def test_health_key_and_credential_key_are_different_axes():
    t = DEFAULT_CATALOG.resolve("openrouter.deepseek-v4-pro")
    assert t.health_key == ("openrouter-toolsafe", "openrouter.deepseek-v4-pro")
    assert t.credential_key == "openrouter"


def test_price_falls_back_to_the_input_rate_when_caching_is_unsupported():
    """Falling back to zero is the tempting bug: it makes every uncached
    provider look free and pushes routing toward the most expensive option."""
    maverick = MODELS["openrouter.llama-4-maverick"]
    assert maverick.cached_input_per_m is None
    assert price_of(maverick, cached=True) == maverick.input_per_m


def test_cached_price_is_used_when_supported():
    haiku = MODELS["anthropic.haiku-4-5"]
    assert price_of(haiku, cached=True) == 0.10
    assert price_of(haiku, cached=False) == 1.00


def test_price_dates_are_fresh_as_of_the_build_date():
    """Deterministic on purpose -- a test pinned to date.today() fails on a
    calendar boundary and teaches you nothing when it does. Bump the reference
    date when you re-verify the table; that edit IS the re-verification."""
    stale = DEFAULT_CATALOG.stale_prices(today=dt.date(2026, 9, 16), max_age_days=120)
    assert stale == [], f"re-verify rates for: {stale}"


def test_stale_detection_actually_detects():
    stale = DEFAULT_CATALOG.stale_prices(today=dt.date(2027, 9, 9), max_age_days=120)
    assert len(stale) == len(DEFAULT_CATALOG.models) - 0
    assert all(age is not None and age > 120 for _, age in stale)


def test_redirect_to_fakes_leaves_model_ids_untouched():
    """The contract tier points real model ids at fake upstreams so it
    exercises the same routing code production does. If redirecting changed
    ids, the tests would be proving something about a different code path."""
    c = DEFAULT_CATALOG.redirect_to_fakes("http://127.0.0.1:9/v1", kind="openai")
    assert c.resolve("openrouter.qwen-3.6-plus").provider.base_url.endswith(":9/v1")
    assert c.resolve("anthropic.haiku-4-5").provider.base_url == \
        DEFAULT_CATALOG.providers["anthropic"].base_url
    assert set(c.models) == set(DEFAULT_CATALOG.models)


def test_with_overrides_does_not_mutate_the_original():
    c = DEFAULT_CATALOG.with_overrides(
        providers={"anthropic": ProviderConn(id="anthropic", kind="anthropic",
                                             base_url="http://x")}
    )
    assert c.providers["anthropic"].base_url == "http://x"
    assert DEFAULT_CATALOG.providers["anthropic"].base_url == "https://api.anthropic.com"


def test_provider_kind_is_a_wire_protocol_not_a_vendor():
    """DeepSeek, OpenRouter and the fake all speak OpenAI's wire format, which
    is why none of them needs its own client code."""
    kinds = {p.id: p.kind for p in PROVIDERS.values()}
    assert kinds["deepseek"] == "openai"
    assert kinds["openrouter"] == "openai"
    assert kinds["anthropic"] == "anthropic"


def test_every_provider_declares_a_concurrency_cap():
    """An uncapped provider connection is an unbounded fan-out to somebody
    else's rate limit."""
    for p in PROVIDERS.values():
        assert p.max_concurrency > 0

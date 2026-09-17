"""`/v1/models` (Phase C1): the catalog as a list, per dialect, never upstream."""

from __future__ import annotations

from llmgw.catalog import DEFAULT_CATALOG
from llmgw.surfaces import MODELS
from llmgw.surfaces.models import (
    anthropic_listing,
    is_fake_provider,
    listable_models,
    openai_listing,
)


def test_the_openai_listing_shows_openai_dialect_rows_and_hides_the_fakes():
    rows = listable_models(DEFAULT_CATALOG, dialect="openai", include_fakes=False)
    ids = [spec.id for spec, _ in rows]
    assert "openai.gpt-4o-mini" in ids
    assert "deepseek.deepseek-v4-flash" in ids  # DeepSeek speaks the OpenAI dialect
    assert not any(i.startswith("fake.") for i in ids)
    assert not any(i.startswith("anthropic.") for i in ids)
    assert ids == sorted(ids)


def test_the_anthropic_listing_shows_only_anthropic_dialect_rows():
    rows = listable_models(DEFAULT_CATALOG, dialect="anthropic", include_fakes=False)
    ids = {spec.id for spec, _ in rows}
    assert "anthropic.haiku-4-5" in ids and "anthropic.sonnet-5" in ids
    assert not any(i.startswith("openai.") or i.startswith("deepseek.deepseek") for i in ids)


def test_fakes_are_listed_only_when_the_gateway_points_at_them():
    with_fakes = listable_models(DEFAULT_CATALOG, dialect="openai", include_fakes=True)
    assert any(is_fake_provider(p) for _, p in with_fakes)
    without = listable_models(DEFAULT_CATALOG, dialect="openai", include_fakes=False)
    assert not any(is_fake_provider(p) for _, p in without)


def test_openai_shape_carries_catalog_ids_and_aliases():
    rows = listable_models(DEFAULT_CATALOG, dialect="openai", include_fakes=False)
    body = openai_listing(rows)
    assert body["object"] == "list"
    mini = next(m for m in body["data"] if m["id"] == "openai.gpt-4o-mini")
    assert mini["object"] == "model" and mini["owned_by"] == "openai"
    assert "gpt-4o-mini" in mini["aliases"]
    assert "gpt-4o-mini-2024-07-18" in mini["aliases"]


def test_anthropic_shape_has_the_cursor_fields():
    rows = listable_models(DEFAULT_CATALOG, dialect="anthropic", include_fakes=False)
    body = anthropic_listing(rows)
    assert body["has_more"] is False
    assert body["first_id"] == body["data"][0]["id"]
    assert body["last_id"] == body["data"][-1]["id"]
    assert all(m["type"] == "model" for m in body["data"])


def test_the_surface_picks_the_dialect_from_the_route():
    assert MODELS.dialect_for_route("/v1/models") == "openai"
    assert MODELS.dialect_for_route("/anthropic/v1/models") == "anthropic"
    assert MODELS.serves_locally is True
    a = MODELS.listing(DEFAULT_CATALOG, route="/anthropic/v1/models", include_fakes=False)
    o = MODELS.listing(DEFAULT_CATALOG, route="/v1/models", include_fakes=False)
    assert "has_more" in a and o["object"] == "list"

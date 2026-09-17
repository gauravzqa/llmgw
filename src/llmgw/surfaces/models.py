"""`GET /v1/models` and `GET /anthropic/v1/models`: the catalog, as a list.

Served entirely from the catalog. No upstream connection is opened, ever
(CONTRACTS C18): agent frameworks call this at boot, some of them in a
retry loop, and a listing that cost a provider a socket would be a
self-inflicted load test on the one endpoint a health dashboard is most
likely to poll.

What is listed is what the explicit-model path can route for THIS surface's
dialect: every catalog row whose provider speaks the dialect and is not a
local fake (fakes are listed only when the gateway itself is pointed at
them). With a policy document the set is the same, because `plan_for` with
a client-named model resolves through the whole catalog rather than through
the workload's targets -- a caller who names a model gets that model. The
ids are catalog ids, and every row's declared aliases ride alongside so a
client can learn that `gpt-4o-mini-2024-07-18` and `openai.gpt-4o-mini` are
one thing without a round trip that 400s.

This surface is not a passthrough: `serves_locally = True` tells the server
to mount `ModelsEndpoint` on its routes instead of `PassthroughEndpoint`.
"""

from __future__ import annotations

from typing import Any

from llmgw.catalog import Catalog, ModelSpec, ProviderConn
from llmgw.surfaces.base import BufferedSurface


def is_fake_provider(provider: ProviderConn) -> bool:
    """The shipped local fakes, which a listing must not advertise to a
    client of a production deployment."""
    return provider.id.startswith("fake-")


def listable_models(
    catalog: Catalog, *, dialect: str, include_fakes: bool
) -> list[tuple[ModelSpec, ProviderConn]]:
    """Rows the explicit-model path can route on a surface of `dialect`."""
    out: list[tuple[ModelSpec, ProviderConn]] = []
    for spec in catalog.models.values():
        provider = catalog.providers.get(spec.provider)
        if provider is None or provider.kind != dialect:
            continue
        if is_fake_provider(provider) and not include_fakes:
            continue
        out.append((spec, provider))
    out.sort(key=lambda pair: pair[0].id)
    return out


def openai_listing(rows: list[tuple[ModelSpec, ProviderConn]]) -> dict[str, Any]:
    """The OpenAI `list` object. `id` is the catalog id; `aliases` is the
    gateway's addition (unknown keys are what every SDK ignores)."""
    return {
        "object": "list",
        "data": [
            {
                "id": spec.id,
                "object": "model",
                "created": 0,
                "owned_by": provider.id,
                "aliases": [spec.api_model, *spec.aliases],
            }
            for spec, provider in rows
        ],
    }


def anthropic_listing(rows: list[tuple[ModelSpec, ProviderConn]]) -> dict[str, Any]:
    """Anthropic's `GET /v1/models` shape: `data[]` of `{type, id,
    display_name, created_at}`, `has_more`, `first_id`, `last_id`."""
    data = [
        {
            "type": "model",
            "id": spec.id,
            "display_name": spec.api_model,
            "created_at": spec.priced_at or "",
            "aliases": [spec.api_model, *spec.aliases],
        }
        for spec, _provider in rows
    ]
    return {
        "data": data,
        "has_more": False,
        "first_id": data[0]["id"] if data else None,
        "last_id": data[-1]["id"] if data else None,
    }


class ModelsSurface(BufferedSurface):
    """The listing surface. Two routes, two dialects, one object: the
    dialect is read off the route the request arrived on (the endpoint knows
    which of `routes` matched)."""

    name = "models"
    dialect = "openai"
    routes = ("/v1/models", "/anthropic/v1/models")
    upstream_path = "/v1/models"
    methods = ("GET",)
    forward_query = False
    model_key = None
    fixed_model = None
    accounts = False
    serves_locally = True
    """Mounted on `ModelsEndpoint`, never on `PassthroughEndpoint`."""

    @staticmethod
    def dialect_for_route(route: str) -> str:
        return "anthropic" if route.startswith("/anthropic/") else "openai"

    def listing(self, catalog: Catalog, *, route: str, include_fakes: bool) -> dict[str, Any]:
        dialect = self.dialect_for_route(route)
        rows = listable_models(catalog, dialect=dialect, include_fakes=include_fakes)
        if dialect == "anthropic":
            return anthropic_listing(rows)
        return openai_listing(rows)


MODELS = ModelsSurface()

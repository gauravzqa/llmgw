"""Phase C route registry: every surface declares its routes, upstream path,
methods and dialect, and the server's table is derived from them."""

from __future__ import annotations

import pytest

from llmgw.server import app as app_module
from llmgw.surfaces import (
    ANTHROPIC_MESSAGES,
    ASSEMBLYAI_TOKEN,
    COUNT_TOKENS,
    EMBEDDINGS,
    MODELS,
    OPENAI_CHAT,
    REALTIME_CONTROL,
    REGISTRY,
    ROUTES,
    for_route,
    surface_dialect,
    surface_forward_query,
    surface_methods,
    surface_routes,
    surface_upstream_path,
)


def test_every_registered_route_is_unique_and_maps_back_to_its_surface():
    seen: dict[str, str] = {}
    for surface in REGISTRY:
        for route in surface_routes(surface):
            assert route.startswith("/"), route
            assert route not in seen, f"{route} claimed twice"
            seen[route] = surface.name
            assert for_route(route) is surface
    assert set(seen) == set(ROUTES)


def test_the_two_chat_surfaces_keep_their_routes_and_upstream_paths():
    assert surface_routes(OPENAI_CHAT) == ("/v1/chat/completions",)
    assert surface_upstream_path(OPENAI_CHAT) == "/v1/chat/completions"
    assert surface_routes(ANTHROPIC_MESSAGES) == ("/anthropic/v1/messages",)
    assert surface_upstream_path(ANTHROPIC_MESSAGES) == "/v1/messages"
    assert surface_methods(OPENAI_CHAT) == ("POST",)
    assert surface_dialect(OPENAI_CHAT) == "openai"
    assert surface_dialect(ANTHROPIC_MESSAGES) == "anthropic"


@pytest.mark.parametrize(
    ("surface", "routes", "upstream", "methods", "dialect", "query"),
    [
        (MODELS, ("/v1/models", "/anthropic/v1/models"), "/v1/models", ("GET",),
         "openai", False),
        (COUNT_TOKENS, ("/anthropic/v1/messages/count_tokens",),
         "/v1/messages/count_tokens", ("POST",), "anthropic", False),
        (EMBEDDINGS, ("/v1/embeddings",), "/v1/embeddings", ("POST",), "openai", False),
        (REALTIME_CONTROL,
         ("/v1/realtime/client_secrets", "/v1/realtime/calls/{call_id}/{action}"),
         "/v1/realtime/{call_id}/{action}", ("POST",), "openai", False),
        (ASSEMBLYAI_TOKEN, ("/assemblyai/v3/token",), "/v3/token", ("GET",), "openai", True),
    ],
)
def test_phase_c_surfaces_declare_their_contract(
    surface, routes, upstream, methods, dialect, query
):
    assert surface_routes(surface) == routes
    assert surface_upstream_path(surface) == upstream
    assert surface_methods(surface) == methods
    assert surface_dialect(surface) == dialect
    assert surface_forward_query(surface) is query


def test_the_servers_route_table_is_derived_from_the_registry():
    table = app_module.ROUTE_TO_UPSTREAM_PATH
    assert table["/v1/chat/completions"] == "/v1/chat/completions"
    assert table["/anthropic/v1/messages"] == "/v1/messages"
    assert table["/v1/embeddings"] == "/v1/embeddings"
    assert table["/anthropic/v1/messages/count_tokens"] == "/v1/messages/count_tokens"
    assert set(table) == set(ROUTES)
    assert set(app_module.SURFACE_NAMES) == {s.name for s in REGISTRY}


def test_dialect_falls_back_to_the_name_prefix_for_a_pre_registry_surface():
    class Old:
        name = "anthropic_legacy"
        path = "/v1/legacy"

    assert surface_dialect(Old()) == "anthropic"
    assert surface_routes(Old()) == ("/v1/legacy",)
    assert surface_upstream_path(Old()) == "/v1/legacy"
    assert surface_methods(Old()) == ("POST",)
    assert surface_forward_query(Old()) is False


def test_realtime_control_picks_its_upstream_path_per_route():
    assert REALTIME_CONTROL.upstream_path_for("/v1/realtime/client_secrets") == (
        "/v1/realtime/client_secrets"
    )
    assert REALTIME_CONTROL.upstream_path_for(
        "/v1/realtime/calls/{call_id}/{action}"
    ) == "/v1/realtime/calls/{call_id}/{action}"


def test_realtime_control_refuses_an_unknown_call_action():
    from llmgw import errors

    REALTIME_CONTROL.validate_params({"call_id": "rtc_1", "action": "accept"})
    with pytest.raises(errors.InvalidRequest):
        REALTIME_CONTROL.validate_params({"call_id": "rtc_1", "action": "explode"})


def test_build_app_mounts_every_registry_route_with_its_methods_and_the_workload_form():
    from llmgw.server.config import ServerConfig

    app = app_module.build_app(ServerConfig())
    mounted = {(r.path, tuple(sorted(r.methods or ()))) for r in app.routes}  # type: ignore[attr-defined]
    for surface in REGISTRY:
        declared = surface_methods(surface)
        methods = tuple(sorted({*declared, "HEAD"} if "GET" in declared else declared))
        for route in surface_routes(surface):
            assert (route, methods) in mounted, (route, methods, sorted(mounted)[:5])
            assert (f"/workloads/{{workload}}{route}", methods) in mounted

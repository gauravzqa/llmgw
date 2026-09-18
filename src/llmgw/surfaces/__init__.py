"""Surfaces: one object per wire contract, registered by name and by route.

A surface is stateless, so one instance per dialect is shared by every
in-flight request. Instantiating per request would be harmless but dishonest
-- it would suggest there is per-request state to hold, and the next person
would add some.

`REGISTRY` (Phase C) is the ordered tuple the server builds its route table
from: every surface's `routes` are mounted, each also under
`/workloads/{workload}`. `SURFACES` is keyed by `name` because `name` is the
metrics label, and the registry is what makes that label a *closed* set:
`llmgw_requests_total` cannot acquire a new `surface=` value without a new
entry here. Unbounded label cardinality is a production incident with a
slow fuse, so the set is enumerable on purpose.

The voice surfaces live in their own package (`llmgw.surfaces.voice`,
Phase D) and are appended when it exists; this module must import without
it so the text gateway never depends on the voice one.
"""

from __future__ import annotations

from llmgw.surfaces.anthropic import AnthropicMessagesSurface
from llmgw.surfaces.assemblyai_token import ASSEMBLYAI_TOKEN, AssemblyAITokenSurface
from llmgw.surfaces.base import (
    CREDENTIAL_QUERY_KEYS,
    DONE_MARKER,
    BufferedSurface,
    EventKind,
    RequestFacts,
    Surface,
    Usage,
    event_payload,
    is_done_marker,
    surface_dialect,
    surface_forward_query,
    surface_methods,
    surface_routes,
    surface_upstream_path,
)
from llmgw.surfaces.count_tokens import COUNT_TOKENS, CountTokensSurface
from llmgw.surfaces.embeddings import EMBEDDINGS, EmbeddingsSurface
from llmgw.surfaces.models import MODELS, ModelsSurface
from llmgw.surfaces.openai import OpenAIChatSurface
from llmgw.surfaces.realtime_control import REALTIME_CONTROL, RealtimeControlSurface
from llmgw.surfaces.responses import OpenAIResponsesSurface

OPENAI_CHAT: Surface = OpenAIChatSurface()
OPENAI_RESPONSES: Surface = OpenAIResponsesSurface()
ANTHROPIC_MESSAGES: Surface = AnthropicMessagesSurface()

try:  # Phase D's package; absent on a text-only build.
    from llmgw.surfaces.voice import VOICE_SURFACES
except ImportError:  # pragma: no cover - depends on which phases are present
    VOICE_SURFACES: tuple[Surface, ...] = ()

REGISTRY: tuple[Surface, ...] = (
    OPENAI_CHAT,
    OPENAI_RESPONSES,
    ANTHROPIC_MESSAGES,
    MODELS,
    COUNT_TOKENS,
    EMBEDDINGS,
    REALTIME_CONTROL,
    ASSEMBLYAI_TOKEN,
    *VOICE_SURFACES,
)
"""Every surface the server mounts, in mount order. Routes must be unique
across the registry; `_check_registry` below enforces it at import."""

SURFACES: dict[str, Surface] = {}
"""Metric name -> the FIRST registry entry carrying it. A name is a metric
label, and several concrete routes may share one (the voice package
registers one instance per route: `inworld_tts` for `/tts/v1/voice` and for
`:stream`), so this is a name index, not a bijection; iterate `REGISTRY` for
every route."""
for _s in REGISTRY:
    SURFACES.setdefault(_s.name, _s)

SURFACES_BY_PATH: dict[str, Surface] = {}
"""Upstream path -> surface, for the two chat surfaces' historical lookup
(`for_path`). Buffered surfaces whose upstream path is a template are not
addressable this way and are looked up by name."""
for _s in REGISTRY:
    _path = surface_upstream_path(_s)
    if "{" not in _path and _path not in SURFACES_BY_PATH:
        SURFACES_BY_PATH[_path] = _s

ROUTES: dict[str, Surface] = {}
"""Client route template -> surface. What `server/app.py` iterates."""


def _check_registry() -> None:
    seen: dict[str, str] = {}
    for surface in REGISTRY:
        for route in surface_routes(surface):
            other = seen.get(route)
            if other is not None and other != surface.name:
                raise RuntimeError(
                    f"route {route!r} is claimed by both {other!r} and {surface.name!r}"
                )
            seen[route] = surface.name
            ROUTES[route] = surface


_check_registry()


def for_path(path: str) -> Surface | None:
    """The surface serving an upstream path, or None. Routing belongs to the
    server; this is only the lookup table it reads, so an unknown path
    returns None rather than raising -- deciding what an unrouted request
    deserves is the server's call, not the dialect's."""
    return SURFACES_BY_PATH.get(path)


def for_route(route: str) -> Surface | None:
    """The surface a CLIENT route template is registered to, or None."""
    return ROUTES.get(route)


__all__ = [
    "ANTHROPIC_MESSAGES",
    "ASSEMBLYAI_TOKEN",
    "COUNT_TOKENS",
    "CREDENTIAL_QUERY_KEYS",
    "DONE_MARKER",
    "EMBEDDINGS",
    "MODELS",
    "OPENAI_CHAT",
    "OPENAI_RESPONSES",
    "REALTIME_CONTROL",
    "REGISTRY",
    "ROUTES",
    "SURFACES",
    "SURFACES_BY_PATH",
    "VOICE_SURFACES",
    "AnthropicMessagesSurface",
    "AssemblyAITokenSurface",
    "BufferedSurface",
    "CountTokensSurface",
    "EmbeddingsSurface",
    "EventKind",
    "ModelsSurface",
    "OpenAIChatSurface",
    "OpenAIResponsesSurface",
    "RealtimeControlSurface",
    "RequestFacts",
    "Surface",
    "Usage",
    "event_payload",
    "for_path",
    "for_route",
    "is_done_marker",
    "surface_dialect",
    "surface_forward_query",
    "surface_methods",
    "surface_routes",
    "surface_upstream_path",
]

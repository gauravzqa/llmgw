"""Surfaces: one object per provider dialect, registered by name and by route.

A surface is stateless, so one instance per dialect is shared by every
in-flight request. Instantiating per request would be harmless but dishonest
-- it would suggest there is per-request state to hold, and the next person
would add some.

`SURFACES` is keyed by `name` because `name` is the metrics label, and the
registry is what makes that label a *closed* set: `llmgw_requests_total`
cannot acquire a new `surface=` value without a new entry here. Unbounded
label cardinality is a production incident with a slow fuse, so the set is
enumerable on purpose.
"""

from __future__ import annotations

from llmgw.surfaces.anthropic import AnthropicMessagesSurface
from llmgw.surfaces.base import (
    DONE_MARKER,
    EventKind,
    RequestFacts,
    Surface,
    Usage,
    event_payload,
    is_done_marker,
)
from llmgw.surfaces.openai import OpenAIChatSurface

OPENAI_CHAT: Surface = OpenAIChatSurface()
ANTHROPIC_MESSAGES: Surface = AnthropicMessagesSurface()

SURFACES: dict[str, Surface] = {
    OPENAI_CHAT.name: OPENAI_CHAT,
    ANTHROPIC_MESSAGES.name: ANTHROPIC_MESSAGES,
}

SURFACES_BY_PATH: dict[str, Surface] = {s.path: s for s in SURFACES.values()}


def for_path(path: str) -> Surface | None:
    """The surface serving a route, or None. Routing belongs to the server;
    this is only the lookup table it reads, so an unknown path returns None
    rather than raising -- deciding what an unrouted request deserves is the
    server's call, not the dialect's."""
    return SURFACES_BY_PATH.get(path)


__all__ = [
    "ANTHROPIC_MESSAGES",
    "DONE_MARKER",
    "OPENAI_CHAT",
    "SURFACES",
    "SURFACES_BY_PATH",
    "AnthropicMessagesSurface",
    "EventKind",
    "OpenAIChatSurface",
    "RequestFacts",
    "Surface",
    "Usage",
    "event_payload",
    "for_path",
    "is_done_marker",
]

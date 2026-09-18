"""The WebSocket data plane: a second path to the same providers.

PLAN-2.md:426-431 is the sentence that governs everything under this
package: "nothing in the HTTP pump applies, while deadlines, admission,
breakers, credential scoping, capture and drain do." So this is not a second
gateway. It is a second TRANSPORT plugged into the process-wide objects
`Gateway` already owns, writing the same capture records, taking the same
permits, opening the same circuits, and answering to the same drain.

What it invents, and the list is deliberately short:

    ws/client.py        one upstream socket per session (no pool: the socket
                        IS the session)
    ws/relay.py         two directions, bounded buffers, the clocks
    ws/session.py       what a socket holds and what it writes
    ws/errors.py        close codes 4900-4999 (C25) and the close -> taxonomy map
    ws/routes.py        the upgrade, with the request path's refusal order
    ws/surfaces/        one frame classifier per product

`WS_REGISTRY` is the ordered tuple `build_app` mounts. It is separate from
`surfaces.REGISTRY` rather than merged into it because the two describe
different things -- an HTTP surface answers "what does this dialect's
response look like", a ws surface answers "what does this frame mean to a
clock" -- and a single registry would need every member to answer both.
Their NAMES share one closed vocabulary (`metrics.SURFACES`), which is the
only coupling that matters, and `tests/unit/test_surfaces.py` pins it.
"""

from __future__ import annotations

from llmgw.ws.surfaces.base import AcceptPolicy, Frame, FrameClass, Verdict, WsSurface
from llmgw.ws.surfaces.inworld_tts import INWORLD_TTS_WS

WS_REGISTRY: tuple[WsSurface, ...] = (
    INWORLD_TTS_WS,
)
"""Every registered WebSocket surface, in mount order.

One entry in G1. `inworld_stt_ws`, `openai_realtime`, `assemblyai_streaming`
and `elevenlabs_tts_ws` join it in G2-G4; their names are already in
`metrics.SURFACES` so that adding one is a line here and a file in
`surfaces/`, not a change to a label set every dashboard is built on."""

WS_SURFACE_NAMES: tuple[str, ...] = tuple(s.name for s in WS_REGISTRY)

__all__ = [
    "INWORLD_TTS_WS",
    "WS_REGISTRY",
    "WS_SURFACE_NAMES",
    "AcceptPolicy",
    "Frame",
    "FrameClass",
    "Verdict",
    "WsSurface",
]


def _check_registry() -> None:
    """Every surface's name is a legal metric label and no route is claimed
    twice. Run at import, like `surfaces._check_registry`: a duplicate route
    is a mount-order accident that would otherwise surface as one product
    silently serving another's traffic."""
    from llmgw import metrics

    seen: dict[str, str] = {}
    for surface in WS_REGISTRY:
        if surface.name not in metrics.SURFACES:
            raise ValueError(
                f"ws surface {surface.name!r} is not in metrics.SURFACES; the "
                f"label set is closed and a route it cannot label is a route "
                f"whose traffic is invisible"
            )
        for route in surface.routes:
            if route in seen:
                raise ValueError(
                    f"ws route {route!r} is claimed by both {seen[route]!r} and "
                    f"{surface.name!r}"
                )
            seen[route] = surface.name


_check_registry()

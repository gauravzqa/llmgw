"""The upstream socket: one connect, classified the way every other one is.

There is no pool here and there will not be one. `upstream.py` pools HTTP
connections because a request is short and a connection is expensive; a
relayed session is the opposite -- the socket IS the session, it lives for
minutes, and two tenants sharing one would mean one tenant's close cutting
the other's contexts. So the unit is one client socket, one upstream socket,
and the plan's multiplexing rule ("one client socket = one upstream socket")
falls out of that rather than being enforced anywhere.

What IS shared with the HTTP plane is everything that decides where to
connect and with what: `upstream.join_url` (including `path_prefix`),
`upstream.build_headers` (including the per-provider auth scheme), the
`Deadline`, and `errors.from_http_status` for an upgrade the provider
refuses with a status line. Classifying a 401 on the upgrade differently
from a 401 on a POST would give the same credential two health stories.

--------------------------------------------------------------------------
Three headers that never go upstream
--------------------------------------------------------------------------

`content-type` and `accept` are removed from `build_headers`' output. They
describe a body, an upgrade has none, and `accept: text/event-stream` on a
WebSocket handshake is a request for a thing that cannot be delivered.

`OpenAI-Beta` is refused outright, and not merely dropped: sending
`realtime=v1` to the GA Realtime API earns an in-band error
`beta_api_shape_disabled` and a server close 4000 before anything else
happens (captures-ws probes 7c, 8b). A client that gets a 400 from the
gateway saying so learns what is wrong; a client that gets a close 4000 from
a provider it does not know it is talking to does not. It is not in
`ServerConfig.forward_request_headers`' allowlist either, so this is
belt-and-braces -- but the allowlist is operator-editable and this is not.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from llmgw import errors
from llmgw.catalog import Target
from llmgw.clocks import Budgets, Deadline
from llmgw.upstream import build_headers, join_url

log = logging.getLogger("llmgw.ws.client")

__all__ = [
    "REFUSED_UPGRADE_HEADERS",
    "UpstreamSocket",
    "connect_upstream",
    "ws_url",
]

REFUSED_UPGRADE_HEADERS: frozenset[str] = frozenset({"openai-beta"})
"""Headers a client may not smuggle onto the upgrade. See the module
docstring: this one is fatal at the provider and the failure is illegible."""

_DROPPED_UPGRADE_HEADERS: frozenset[str] = frozenset({"content-type", "accept"})
"""Body-describing headers `build_headers` adds for HTTP. An upgrade has no
body; these are removed rather than overridden so the provider sees the same
handshake the plugin would have sent."""


@dataclass(slots=True)
class UpstreamSocket:
    """A connected upstream socket plus what its 101 said.

    `response_headers` is the provider's upgrade response, which is where
    `X-Gw-Upstream-Request-Id` comes from when the provider sends one. It
    does not, on Inworld: the captures record `sec-websocket-accept`, `date`,
    `server: istio-envoy`, `via`, `alt-svc` and nothing identifying, on every
    probe. The field is read through `parse_upstream_telemetry` anyway, so
    the day a request id appears the header appears with it.
    """

    connection: Any
    """`websockets.asyncio.client.ClientConnection`. Typed loosely so this
    module is the only one that has to know the library's shape."""

    target: Target
    url: str
    response_headers: Mapping[str, str]
    subprotocol: str | None


def ws_url(target: Target, path: str, *, query: str = "") -> str:
    """The provider URL with the scheme swapped and the query appended.

    `join_url` does the hard part (the `/v1/v1` collapse, the `path_prefix`
    rule) and is reused verbatim so a provider row's base URL means the same
    thing on both planes. Only the scheme differs: a catalog row's
    `base_url` is `https://api.inworld.ai` because the HTTP surfaces need it
    to be, and a WebSocket needs `wss://` -- the same host, the same port,
    the same TLS, a different scheme word.
    """
    url = join_url(target.provider.base_url, path, prefix=target.provider.path_prefix)
    if url.startswith("https://"):
        url = "wss://" + url[len("https://"):]
    elif url.startswith("http://"):
        url = "ws://" + url[len("http://"):]
    if query:
        url = f"{url}?{query}"
    return url


def upgrade_headers(
    target: Target, *, extra: Mapping[str, str] | None = None
) -> dict[str, str]:
    """`build_headers` minus the body-describing pair, plus the caller's.

    Reusing `build_headers` is the point: the Inworld row says
    `auth_scheme="basic"`, so this sends `Authorization: Basic <key>` --
    which is the only credential form the provider's upgrade accepts
    (captures-ws probe 2a: `?key=` is read as no credential at all) and
    exactly what the plugin itself sends. Nothing here knows that; the
    catalog row does.
    """
    headers = build_headers(target, stream=False, extra=extra)
    for name in _DROPPED_UPGRADE_HEADERS:
        headers.pop(name, None)
    for name in REFUSED_UPGRADE_HEADERS:
        headers.pop(name, None)
    return headers


async def connect_upstream(
    target: Target,
    path: str,
    *,
    query: str = "",
    subprotocols: Sequence[str] = (),
    extra_headers: Mapping[str, str] | None = None,
    budgets: Budgets,
    deadline: Deadline,
    max_size: int,
) -> UpstreamSocket:
    """Open one upstream socket, or raise a `GatewayError`.

    The connect budget covers TCP, TLS and the upgrade -- everything up to
    the provider's 101 -- and nothing after it. On Inworld nothing comes
    after it for a long time (101 then silence is a healthy idle socket), so
    a budget that tried to include "the provider has said something" would
    be a budget that always fires. The handshake budget that follows is the
    relay's, and it runs from the first relayed client frame.

    Failure classification is the HTTP plane's, deliberately:

    * a refused upgrade carries a status line, and it goes through
      `from_http_status` with the provider's row (`forbidden_means`,
      `scrub_error_bodies`), so a 401 here opens the same credential circuit
      a 401 on a POST would;
    * a timeout is `ConnectTimeout` -- retry-same and try-next, the safest
      retry in the system because nothing was accepted upstream;
    * anything else the socket layer raises is `ConnectionFailed`.
    """
    from websockets.asyncio.client import connect as ws_connect
    from websockets.exceptions import InvalidStatus

    url = ws_url(target, path, query=query)
    headers = upgrade_headers(target, extra=extra_headers)
    budget = deadline.slice(budgets.connect)
    try:
        async with deadline.timeout(budgets.connect):
            connection = await ws_connect(
                url,
                additional_headers=headers,
                subprotocols=list(subprotocols) or None,
                max_size=max_size,
                # OFF: see lifecycle.py for the CPU-and-memory argument. It
                # also keeps the two halves of the relay symmetric -- the
                # client side has deflate off too, so a frame's size on one
                # socket is its size on the other and `llmgw_ws_bytes_total`
                # means one thing.
                compression=None,
                # The library's own connect timeout is belt-and-braces under
                # the deadline's: the deadline is the authority (it is what
                # the whole gateway's arithmetic is written against) and this
                # only stops `websockets` from waiting past it if a cancel is
                # somehow swallowed.
                open_timeout=budget,
                # Transport liveness upstream, matching uvicorn's on the
                # client side. `websockets` answers a provider's PING itself,
                # so the relay forwards none and the gateway's own `idle`
                # budget stays the only liveness that means anything.
                ping_interval=20,
                ping_timeout=20,
            )
    except TimeoutError as exc:
        raise errors.ConnectTimeout(
            f"upstream websocket connect exceeded {budget:.3f}s",
            provider=target.provider.id, model=target.model.id,
            credential_id=target.credential_key, cause=exc,
        ) from exc
    except InvalidStatus as exc:
        # The provider answered the upgrade with a status line instead of a
        # 101. That is an ordinary HTTP failure and is classified as one --
        # same taxonomy, same breaker scope, same scrub rule.
        response = getattr(exc, "response", None)
        status = int(getattr(response, "status_code", 502) or 502)
        body = getattr(response, "body", None)
        raise errors.from_http_status(
            status,
            body=bytes(body) if isinstance(body, (bytes, bytearray)) else None,
            headers=dict(getattr(response, "headers", {}) or {}),
            provider=target.provider.id,
            model=target.model.id,
            credential_id=target.credential_key,
            forbidden_means=getattr(target.provider, "forbidden_means", "auth"),
        ) from exc
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - every socket-layer failure lands here
        raise errors.ConnectionFailed(
            f"upstream websocket connect failed: {type(exc).__name__}",
            provider=target.provider.id, model=target.model.id,
            credential_id=target.credential_key, cause=exc,
        ) from exc

    response = getattr(connection, "response", None)
    raw = getattr(response, "headers", None)
    return UpstreamSocket(
        connection=connection,
        target=target,
        url=url,
        response_headers={str(k).lower(): str(v) for k, v in (raw or {}).items()},
        subprotocol=getattr(connection, "subprotocol", None),
    )

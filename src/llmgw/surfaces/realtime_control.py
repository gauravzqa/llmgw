"""The OpenAI Realtime control plane through the gateway (Phase E1, E3).

Two routes, one surface, no media. `POST /v1/realtime/client_secrets` mints
the short-lived `ek_` key a browser or device uses to open a WebRTC or
WebSocket session directly with OpenAI; `POST /v1/realtime/calls/{call_id}/
{action}` accepts, rejects, refers or hangs up a SIP call. The audio never
transits the gateway (WebRTC and SIP go peer-to-provider), which is exactly
why the MINT is the gateway's business: it is the one moment before any
media flows where admission can apply and configuration can be pinned.

What the mint does that a plain passthrough would not (CONTRACTS C19):

* the tenant's pinned `session` fields win over the client's -- model,
  voice, tools, turn detection, `max_output_tokens` -- so a client cannot
  widen what its tenant is allowed to open;
* `expires_after.seconds` is capped at the smallest of the client's ask, the
  tenant's cap and the deployment's drain grace, and the default when the
  client asks for nothing is the tenant's cap;
* `OpenAI-Safety-Identifier` is set to the tenant id on the upstream request;
* the mint counts against the tenant's rate bucket AND its `max_sessions`,
  a reservation that expires with the secret;
* the provider's response is returned unchanged, `ek_` and all.

The model rewrite for this surface is nested (`session.model`, not `model`),
so it is done here rather than by `upstream.apply_api_model`.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from llmgw import errors
from llmgw.catalog import Target
from llmgw.surfaces.base import BufferedSurface, RequestFacts, as_int, parse_json_object

DEFAULT_REALTIME_MODEL = "openai.gpt-realtime-mini"
"""Catalog id routed to when neither the client nor the tenant pin names a
Realtime model. Must exist in the catalog (Phase D ships the row)."""

DEFAULT_SECRET_TTL_S = 600
"""OpenAI's own default for `expires_after.seconds`, used as the ceiling when
a tenant pins no cap."""

MAX_SECRET_TTL_S = 7200
"""OpenAI's documented maximum; nothing above it is ever asked for."""

SAFETY_IDENTIFIER_HEADER = "OpenAI-Safety-Identifier"

PINNABLE_SESSION_KEYS: tuple[str, ...] = (
    "model", "voice", "tools", "turn_detection", "max_output_tokens", "instructions",
)


@dataclass(frozen=True, slots=True)
class MintResult:
    """What `prepare_mint` hands back to the endpoint."""

    body: bytes
    """The request body to send upstream (session merged, model rewritten)."""
    extra_headers: Mapping[str, str]
    """Headers to add upstream (`OpenAI-Safety-Identifier`)."""
    ttl_s: float
    """How long the minted credential lives; the `max_sessions` reservation
    is held for exactly this long."""
    pinned_keys: tuple[str, ...]
    """Session keys the tenant pin overrode, for the capture record."""


def _client_ttl(session_body: Mapping[str, Any]) -> int | None:
    expires = session_body.get("expires_after")
    if isinstance(expires, Mapping):
        return as_int(expires.get("seconds"))
    return None


def merge_session(
    client_session: Mapping[str, Any] | None,
    pin: Mapping[str, Any] | None,
    *,
    wire_model: str,
) -> tuple[dict[str, Any], tuple[str, ...]]:
    """The client's `session` with the tenant's pinned fields written over it.

    Pinned wins, key by key, and `voice` / `turn_detection` land where the
    Realtime API nests them (`audio.output.voice`, `audio.input.turn_detection`)
    as well as at the top level for the older flat shape, so a pin holds
    whichever shape the client used. `model` is always the target's wire id:
    the client named a catalog id (or the pin did), and the provider wants
    its own name.
    """
    session: dict[str, Any] = dict(client_session or {})
    session.setdefault("type", "realtime")
    pinned: list[str] = []
    for key, value in (pin or {}).items():
        if key not in PINNABLE_SESSION_KEYS or value is None:
            continue
        if key == "model":
            continue  # resolved through the catalog; written below as the wire id
        if key == "voice":
            audio = dict(session.get("audio") or {})
            output = dict(audio.get("output") or {})
            output["voice"] = value
            audio["output"] = output
            session["audio"] = audio
            session["voice"] = value
        elif key == "turn_detection":
            audio = dict(session.get("audio") or {})
            inp = dict(audio.get("input") or {})
            inp["turn_detection"] = value
            audio["input"] = inp
            session["audio"] = audio
        else:
            session[key] = value
        pinned.append(key)
    session["model"] = wire_model
    return session, tuple(pinned)


def capped_ttl(client_ttl: int | None, *, tenant_cap: int | None, grace_s: float) -> int:
    """`min(client ask, tenant cap, drain grace)`, with the tenant cap (or
    OpenAI's default) standing in when the client asked for nothing, and the
    documented maximum as a final bound. Always at least 10 s, OpenAI's
    minimum."""
    ceiling = tenant_cap if tenant_cap is not None else DEFAULT_SECRET_TTL_S
    candidates = [ceiling, int(grace_s), MAX_SECRET_TTL_S]
    if client_ttl is not None and client_ttl > 0:
        candidates.append(client_ttl)
    return max(10, min(candidates))


class RealtimeControlSurface(BufferedSurface):
    name = "realtime_control"
    dialect = "openai"
    routes = (
        "/v1/realtime/client_secrets",
        "/v1/realtime/calls/{call_id}/{action}",
    )
    upstream_path = "/v1/realtime/{call_id}/{action}"
    """Only meaningful for the calls route; the mint route's upstream path is
    itself (`upstream_path_for`)."""
    methods = ("POST",)
    model_key = None
    fixed_model = DEFAULT_REALTIME_MODEL
    accounts = False
    mint_route = "/v1/realtime/client_secrets"

    CALL_ACTIONS: frozenset[str] = frozenset({"accept", "reject", "refer", "hangup"})

    def upstream_path_for(self, route: str) -> str:
        return route if route == self.mint_route else "/v1/realtime/calls/{call_id}/{action}"

    def model_from(self, raw: dict[str, Any]) -> str | None:
        session = raw.get("session")
        if isinstance(session, dict):
            value = session.get("model")
            if isinstance(value, str) and value:
                return value
        return None

    def parse_request(self, body: bytes) -> RequestFacts:
        raw = parse_json_object(body) if body.strip() else {}
        model = self.model_from(raw) or self.fixed_model
        return RequestFacts(model=model or DEFAULT_REALTIME_MODEL, stream=False)

    def validate_params(self, params: Mapping[str, str]) -> None:
        action = params.get("action")
        if action is not None and action not in self.CALL_ACTIONS:
            raise errors.InvalidRequest(
                f"unknown realtime call action {action!r}; "
                f"one of {sorted(self.CALL_ACTIONS)}"
            )

    def prepare_mint(
        self,
        body: bytes,
        *,
        route: str,
        target: Target,
        tenant: str,
        pin: Mapping[str, Any] | None,
        grace_s: float,
    ) -> MintResult | None:
        """Merge, cap and stamp a client-secret request. None for the calls
        route, which is a plain passthrough with no credential in flight."""
        if route != self.mint_route:
            return None
        raw = parse_json_object(body) if body.strip() else {}
        client_session = raw.get("session")
        if client_session is not None and not isinstance(client_session, dict):
            raise errors.InvalidRequest("`session` must be an object")
        session, pinned = merge_session(
            client_session, pin, wire_model=target.model.api_model
        )
        tenant_cap = as_int((pin or {}).get("expires_after_seconds_cap"))
        ttl = capped_ttl(_client_ttl(raw), tenant_cap=tenant_cap, grace_s=grace_s)
        merged: dict[str, Any] = {**raw, "session": session,
                                  "expires_after": {"anchor": "created_at", "seconds": ttl}}
        rendered = json.dumps(merged, separators=(",", ":")).encode("utf-8")
        return MintResult(
            body=rendered,
            extra_headers={SAFETY_IDENTIFIER_HEADER: tenant},
            ttl_s=float(ttl),
            pinned_keys=pinned,
        )


REALTIME_CONTROL = RealtimeControlSurface()

"""The HTTP surface: one client request, one upstream, bytes in the middle.

This file is the only place in the gateway that talks ASGI, and almost every
decision in it comes from one fact that is easy to state and easy to get
wrong:

    an HTTP response has TWO points of no return, not one.

--------------------------------------------------------------------------
The two commitment boundaries
--------------------------------------------------------------------------

**Status commitment** is `http.response.start`. The moment that message goes
to the server, the status line and every response header are on the wire and
cannot be replaced. Nothing has been said about the *answer* yet -- the body
is still empty -- but the shape of the reply is fixed.

**Content commitment** is the first body byte, and it is CONTRACTS.md C1. It
is owned by `pump.py` and by nothing else, and once it is true no further
attempt of any kind is legal, because the client is already rendering an
answer and a second one would be spliced onto the end of the first.

They are not the same instant, and the gap between them is exactly the
fallback C1 goes out of its way to preserve:

    "A failure after upstream HTTP headers but BEFORE the first client byte
     may still fall back. We have sent the client nothing, so nothing is
     broken and nothing is inconsistent."

P2 sent `http.response.start` as soon as `Upstream.open()` returned. That was
already better than starting a 200 before opening anything -- a server that
does *that* has committed to a status it has no evidence for, and when the
upstream answers 503 it can neither pass the status through (C4) nor fall
back, so it ends up synthesising a body inside a lie. But it still forfeited
the narrower window C1 permits: headers arrived and then the provider said
nothing, which is the most common provider failure there is.

    P3 DECISION: `http.response.start` is not sent when `open()` returns. It
    is sent when the upstream yields its first byte of body.

This file does not implement that rule; `executor.py` does, and this file is
where the rule becomes an HTTP response. The whole of it is one closure:

    async def start_response(stream) -> Sink:      # the SinkFactory
        exchange.started = True
        await send({"type": "http.response.start", ...})
        return ASGISink(send)

`Executor.execute()` cannot write a byte to the client without a `Sink`, it
cannot get a `Sink` without awaiting that closure, and awaiting it is the
status commitment. Everything before it -- a connect timeout, a 503, a 429, a
`FirstEventTimeout`, an empty 200 -- happens with the plan still open and may
fall back to the next target. Nothing after it may (`decide()` refuses both
continuation conditions once committed). The two boundaries therefore
coincide, which is the entire payoff of the P3 decision.

What it costs is written down in CONTRACTS.md C1 and worth repeating at the
call site: a client now sees no status at all while an upstream stalls, for up
to `budgets.first_event`. That budget must stay below whatever header timeout
the callers and intermediaries in front of us use.

--------------------------------------------------------------------------
One snapshot per request, taken at ingress
--------------------------------------------------------------------------

`Gateway.policy` is a `PolicyStore`. `PassthroughEndpoint.__call__` calls
`current()` exactly ONCE and threads that object through routing, budgets,
the attempt loop and the response headers. That is FAILURE-MODES row 10:
a request routed by one policy version and
priced by another has one `policy_id` field and two policies in it, and no
capture record can say so afterwards. A second `current()` call anywhere on
this path reopens exactly the hole the snapshot exists to close, which is why
the value is passed down as an argument rather than fetched where it is
needed.

Both ids travel out. `X-Gw-Policy-Id` names the routing document and
`X-Gw-Catalog-Id` names the price table, because they version independently:
folding prices into the policy id would churn it every time an unrelated
model's price was re-verified, and a record that carries only one of them
cannot answer "which prices applied" or "which routing did".

--------------------------------------------------------------------------
Which workload, and who decides: the header, the path, or the body
--------------------------------------------------------------------------

A request names its workload in one of two places -- `X-Gw-Workload`, or the
`/workloads/{w}/...` route -- and if it names neither it gets the snapshot's
`default_workload`. The path wins a disagreement, because it is the part of
the request a router, an access log and an authorisation rule can all see.

The body's `model` is the third input and it is the one with a real trade-off
in it. Both surfaces *require* `model`, so a rule of "the body always pins the
target" would make every two-target workload collapse to one target and every
A/B silently unreachable over HTTP. The rule here is:

    named workload  -> the workload's plan routes; `model` is not consulted
    no workload     -> `plan_for(default, model=body.model)` pins that target

which reads as: naming a workload IS the statement "I am not pinning a model,
route me". The alternative -- letting the body win -- means an operator can
configure a candidate, watch zero traffic reach it, and find nothing wrong in
the config. This decides only which target the body is sent to; what the body
says once it gets there is `upstream.apply_api_model`'s, which replaces the
client's catalog id with the wire model that target's API answers to.

--------------------------------------------------------------------------
Why raw ASGI and not StreamingResponse
--------------------------------------------------------------------------

`StreamingResponse` sends `http.response.start` and then pulls the first chunk
from the iterator. That ordering is exactly backwards for us: the status has
to be *derived* from work that happens before the first chunk exists. Making
it fit would mean either starting a 200 optimistically (the bug above) or
draining one chunk out of the upstream inside the response constructor, which
puts the first-event budget in a place with no deadline and no error mapping.

So the streaming path is a raw ASGI callable -- an object with
`async def __call__(self, scope, receive, send)`. Starlette routes it
untouched (`Route` treats a non-function endpoint as an ASGI app), and we
decide byte by byte what goes out and when. The `Sink` handed to `Pump` is
four lines: an adapter that turns `send(chunk)` into an
`http.response.body` message with `more_body=True`.

The other consequence of raw ASGI is the one that makes C2 implementable. A
post-commitment failure must end the body *without* the surface's terminal
marker and without a well-formed end of message, because that is what a direct
connection to the provider would have shown the client. In ASGI terms that is
"return without ever sending `more_body: False`", which uvicorn turns into a
transport close with no chunked terminator -- a truncated body, and the
client's own SDK raising its own vendor's truncated-stream error. No response
class will do that for you.

--------------------------------------------------------------------------
Hop-by-hop headers, and the one that truncates answers
--------------------------------------------------------------------------

Response headers are forwarded from an ALLOWLIST, not filtered by a denylist.
`content-length` is the reason. We re-chunk: the pump splits and joins upstream
chunks against its own byte-bounded buffer, and uvicorn frames the result as
chunked transfer-encoding. Forward the upstream's `content-length` on top of
that and the client stops reading at that many bytes -- which presents as a
*truncated answer*, intermittently, only on responses whose re-chunked length
differs from the original. A denylist gets this right only until a provider
sends a header nobody thought about.

--------------------------------------------------------------------------
Client disconnect
--------------------------------------------------------------------------

Row 5 of FAILURE-MODES.md: a disconnect nobody notices leaks upstream spend
and an upstream connection, and the leak is invisible until the pool is full.

ASGI reports it exactly one way -- an `http.disconnect` message from
`receive()` -- and nothing pushes it at us, so somebody has to be parked in
that call. `_run_until_disconnect` puts the pump in one task and that read in
another, races them, and cancels the loser. The cancellation lands inside
`Pump.run()`, which unwinds its own TaskGroup, which lets `Upstream.open()`'s
`finally` close the response and return the socket to the pool. There is no
exit path -- return, raise or cancel -- that leaves either task alive past the
function that made it.

--------------------------------------------------------------------------
Admission at ingress, the gate per attempt (P4)
--------------------------------------------------------------------------

Two different questions get asked of every request, at two different places,
and it matters that they are not the same question:

    "may this TENANT be here?"          answered ONCE, at ingress, by
                                        `AdmissionController.admit()`,
                                        before the body is read
    "may this ATTEMPT go to this TARGET?"  answered PER ATTEMPT, inside
                                        `Executor._attempt()`, by the breaker
                                        and the provider-key limiter

The first is about the client and the process: a tenant at its cap is refused
having cost us a header parse and two dictionary lookups, and -- this is the
point of the ordering in `PassthroughEndpoint.__call__` -- before we have
allocated its request body. A cap enforced after the body read is a cap that
bought the thing it was protecting against (C6).

The second cannot be asked at ingress because the target is not known until
the plan has chosen one, and it must be re-asked per attempt because the
fallback target has a different circuit and a different credential. That is
why the gate lives in the executor's per-target scope and the server merely
supplies the registry and the limiter.

The permit from the first question is held under `async with` for the whole
request, and the whole request includes the client hanging up mid-stream:
`run_until_disconnect` raises through the block, and `Permit.__aexit__` runs
on the way out. FAILURE-MODES row 19 is a permit that did not, and the chaos
tier's return-to-zero assertion is where it would show.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import math
import re
import time
from collections.abc import Coroutine, Mapping
from contextlib import asynccontextmanager, suppress
from datetime import UTC
from typing import Any, TypeVar

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.types import Receive, Scope, Send

from llmgw import accounting, errors
from llmgw.admission import AdmissionController, ProviderKeyLimiter, TenantLimits
from llmgw.breaker import Breaker, BreakerRegistry, BreakerState, Key, Ticket
from llmgw.capture import Capture, CaptureRecord, FileSink, NullSink
from llmgw.catalog import Target
from llmgw.clocks import Budgets, Clock, Deadline, SystemClock, phase
from llmgw.executor import NO_RETRIES, ExecutionResult, Executor, credential_health_key
from llmgw.metrics import SURFACES as METRIC_SURFACES
from llmgw.metrics import normalize_stop_reason
from llmgw.policy import ExecutionPlan, PolicySnapshot, PolicyStore
from llmgw.pump import Sink
from llmgw.retry import RetryPolicy
from llmgw.server.config import ANONYMOUS_TENANT, ServerConfig, TenantTable
from llmgw.server.telemetry import Collectors
from llmgw.surfaces import REGISTRY, Surface
from llmgw.surfaces.base import (
    CREDENTIAL_QUERY_KEYS,
    RequestFacts,
    surface_dialect,
    surface_forward_query,
    surface_methods,
    surface_routes,
    surface_upstream_path,
)
from llmgw.upstream import Upstream, UpstreamRequest, UpstreamStream
from llmgw.ws.routes import build_ws_routes

log = logging.getLogger("llmgw.server")

T = TypeVar("T")


# ==========================================================================
# Routes
# ==========================================================================

ROUTE_TO_UPSTREAM_PATH: dict[str, str] = {
    route: surface_upstream_path(surface)
    for surface in REGISTRY
    for route in surface_routes(surface)
}
"""Client route -> the path we send upstream. Derived from the route
registry (`surfaces.REGISTRY`, Phase C); kept under its old name because
tests and tooling read it.

The two differ for Anthropic and that is on purpose. A gateway that offers
several dialects needs the *client* route to name the dialect, because
`/v1/messages` and `/v1/chat/completions` are only unambiguous while there is
exactly one vendor of each; the `/anthropic` prefix is how a caller says which
wire contract it is speaking rather than relying on us to guess. The upstream
path stays whatever the provider actually serves, which is `Surface.path`.
"""

WORKLOAD_ROUTE_PREFIX = "/workloads/{workload}"
"""The second way to name a workload: `/workloads/summarize/v1/chat/completions`.

Registered as a second `Route` onto the SAME endpoint instance rather than as
a second endpoint, so there is exactly one serving path and no chance of the
two forms drifting. Starlette puts the match into `scope["path_params"]`,
which is where `_requested_workload` reads it.
"""

WORKLOAD_HEADER = b"x-gw-workload"
NO_RETRY_HEADER = b"x-gw-no-retry"
"""C5's mechanism, as bytes because that is how ASGI hands us header names.

`X-Gw-No-Retry: 1` says another layer owns retries. It disables REPETITION and
never FALLBACK -- see `_retry_policy_for`, which is the only place the two are
allowed to touch.
"""

AUTHORIZATION_HEADER = b"authorization"
"""Where the tenant token arrives: `Authorization: Bearer <token>`.

The same header name is in `config.NEVER_FORWARDED`, and the two facts are
one fact. This header is read HERE, turned into a tenant id, and never
travels further -- not upstream (it would replace the provider credential),
not into an error message, not into a log line. `Gateway.resolve_tenant` is
the only reader.
"""

TENANT_QUERY_PARAM = "tenant"
"""How `/workloads/{w}/probe?tenant=<id>` names a tenant by ID rather than by
token. The probe is an operator endpoint and an operator holds ids, not
tokens; a probe that had to be handed a secret to answer a question about a
tenant would be a probe that teaches people to paste secrets into URLs."""

UNIMPLEMENTED_ROUTES: tuple[str, ...] = ()
"""Routes mounted as a 501 that names the phase, so a not-yet-built surface
answers "not built" rather than a 404 that looks like a typo. Empty since
PLAN-2 Phase F: `/v1/responses` was the last entry and is now served by
`surfaces.responses.OpenAIResponsesSurface` (its `response.failed` ending is
C2's third row, forwarded when upstream sent it). The mechanism stays for the
next surface that is announced before it is built."""


# ==========================================================================
# Headers
# ==========================================================================

HOP_BY_HOP: frozenset[bytes] = frozenset(
    {b"connection", b"keep-alive", b"transfer-encoding", b"upgrade",
     b"proxy-authenticate", b"proxy-authorization", b"te", b"trailer",
     b"content-length"}
)
"""RFC 9110 s7.6.1's hop-by-hop set, plus `content-length`.

`content-length` is not formally hop-by-hop, and it is listed here anyway
because forwarding it is the single worst header bug a re-chunking proxy can
ship. See the module docstring.
"""

FORWARDED_RESPONSE_HEADERS: frozenset[bytes] = (
    frozenset({b"content-type", b"cache-control", b"x-accel-buffering"}) - HOP_BY_HOP
)
"""What travels from the upstream response to the client, and nothing else.

An allowlist rather than a denylist: the failure mode of a missed denylist
entry is a protocol bug in production, and the failure mode of a missing
allowlist entry is a header nobody sees. Three entries earn their place --
`content-type` because the client's SDK dispatches on it, `cache-control` and
`x-accel-buffering` because they are how a provider tells an intermediary not
to buffer an SSE stream, and a buffered SSE stream is a streaming gateway that
does not stream.

Deliberately absent: rate-limit headers (`x-ratelimit-*`). They describe the
gateway's relationship with the provider, not the client's with the gateway,
and passing them through invites a client to pace itself against a budget it
does not own and cannot see the rest of.
"""


def gw_headers(
    *,
    policy_id: str,
    catalog_id: str,
    workload_id: str,
    target: Target | None,
    attempts: int,
    tenant: str | None = None,
    breaker: str | None = None,
) -> list[tuple[bytes, bytes]]:
    """The `X-Gw-*` set, on every response including every error.

    `X-Gw-` and never a company prefix: the prefix names the component, and a header
    named after the company rather than the process is a header that means
    something different in each of its deployments.

    Every value here is now load-bearing, which it was not in P2:

    * `X-Gw-Attempts` is `len(result.attempts)` -- the amplification factor
      for this one request, and the number a caller divides by to find out
      whether we are making their provider's bad minute worse. It is read off
      the attempt tally at commitment time on the streaming path, because a
      status line cannot be revised once the answer is known. A target whose
      circuit was open is NOT counted: nothing was sent to it, and the header
      is a count of what the providers were asked, not of what we considered.
    * `X-Gw-Served-By` is the target that actually answered, not the one we
      started with. On a fallback those differ, and that difference is the
      only evidence a client has that an A/B ran at all.
    * `X-Gw-Policy-Id` and `X-Gw-Catalog-Id` are two ids on purpose. Routing
      and prices version independently; a capture record needs both, and a
      single id would be wrong at one of the two jobs.
    * `X-Gw-Workload-Id` is the workload that ROUTED, which is not always the
      one the caller named -- see `Gateway.resolve_workload`.
    * `X-Gw-Tenant` is the tenant the request was ADMITTED as. The id, never
      the token: ids are operator-chosen and safe to echo, tokens are
      credentials. Absent on a 401, where there is no admitted tenant.
    * `X-Gw-Breaker` is present only when a target was skipped because its
      circuit refused -- `open` or `half_open`, the breaker's own state
      vocabulary. Without it a client that got the incumbent's 200 with
      `X-Gw-Attempts: 1` could not tell "the plan had one target" from "the
      candidate was down and we knew", and those are the two most different
      things a fallback can mean.
    """
    out = [
        (b"x-gw-attempts", str(attempts).encode("ascii")),
        (b"x-gw-served-by", _ascii(str(target) if target is not None else "-")),
        (b"x-gw-workload-id", _ascii(workload_id)),
        (b"x-gw-policy-id", _ascii(policy_id)),
        (b"x-gw-catalog-id", _ascii(catalog_id)),
    ]
    if target is not None:
        # The CATALOG id of the model that served, as opposed to the wire id
        # the provider echoes in its body (`gpt-4o-mini-2024-07-18`). The
        # body's `model` is what an SDK loop re-sends on the next turn, and
        # until PLAN-2 A1 that re-send was a `400 unknown model`; this header
        # is the canonical name a client can learn without parsing the body.
        out.append((b"x-gw-model", _ascii(target.model.id)))
    if tenant is not None:
        out.append((b"x-gw-tenant", _ascii(tenant)))
    if breaker is not None:
        out.append((b"x-gw-breaker", _ascii(breaker)))
    return out


BEARER_ONLY: frozenset[str] = frozenset({"bearer"})
"""The scheme set every HTTP route runs under, and the default everywhere.
Named so the WebSocket routes' wider sets read as a deliberate widening at
their own call sites rather than as a default somebody forgot to narrow."""

AUTH_SCHEMES_ACCEPTED: frozenset[str] = frozenset({"bearer", "basic", "raw"})
"""Every scheme a route is allowed to declare. `raw` means the whole header
value is the token with no scheme word -- AssemblyAI's plugin sends its key
that way. A route asking for a scheme outside this set is a programming
error, not a configuration one, so `credential_token` raises."""


def credential_token(
    scope: Scope, *, schemes: frozenset[str] = BEARER_ONLY
) -> str | None:
    """The tenant token in `Authorization`, under the schemes a route accepts.

    `bearer_token` was Bearer-only, and correctly so: every HTTP client the
    gateway fronts sends Bearer. The socket plane cannot keep that rule,
    because the credential arrives in whatever shape the CONSUMER's plugin
    sends it, and the LiveKit Inworld plugins send `Authorization: Basic
    <key>` (tts.py:263, stt.py:122) while AssemblyAI's sends the bare key.
    Those are the plugins' own strings, built from one environment variable,
    and a gateway that demanded `Bearer` would be asking Layrs to patch a
    pinned third-party library to talk to it.

    So the scheme set is a property of the ROUTE, declared by the surface
    (`WsSurface.auth_schemes`), and HTTP keeps `{"bearer"}` unchanged --
    `bearer_token` below is this function with that default, kept as its own
    name because it is called from several places and "bearer" is the fact
    those call sites are asserting.

    `basic` here is NOT RFC 7617 decoding. The token is taken verbatim after
    the scheme word and compared against the tenants table, exactly as a
    Bearer token is: the tenant token is a gateway-issued opaque string, the
    plugin wraps it in `Basic ` because that is what it does with its
    provider key, and base64-decoding it would turn a perfectly good token
    into a lookup miss. What Inworld does with ITS key upstream is
    `upstream.build_headers`' business and is a different string entirely.

    A header whose scheme the route does not accept, or an empty token, is
    "no token" rather than an error -- the 401 is the same either way, and a
    message that distinguished them would have to quote the header to do it.
    """
    unknown = schemes - AUTH_SCHEMES_ACCEPTED
    if unknown:  # pragma: no cover - a surface declaring nonsense
        raise ValueError(f"unknown tenant auth scheme(s): {sorted(unknown)}")
    for name, value in scope.get("headers", ()):
        if bytes(name).lower() != AUTHORIZATION_HEADER:
            continue
        raw = bytes(value).decode("latin-1").strip()
        scheme, sep, token = raw.partition(" ")
        if not sep:
            # No scheme word at all. Only a route that declared `raw` may
            # read it, and then the whole value is the token.
            return (raw or None) if "raw" in schemes else None
        if scheme.lower() not in schemes:
            return None
        token = token.strip()
        return token or None
    return None


def bearer_token(scope: Scope) -> str | None:
    """The token in `Authorization: Bearer <token>`, or None.

    Case-insensitive scheme, per RFC 6750; a header with any other scheme, or
    an empty token, is "no token" rather than an error. Unchanged in
    behaviour: it is `credential_token` under the Bearer-only default, which
    is what every HTTP route uses.
    """
    return credential_token(scope, schemes=BEARER_ONLY)


def _ascii(value: str) -> bytes:
    return value.encode("latin-1", "replace")


def forwarded_headers(headers: Mapping[str, str]) -> list[tuple[bytes, bytes]]:
    """Allowlisted upstream response headers, as the exact bytes that arrived.

    `httpx.Headers.raw` is preferred over `.items()` because `.items()` has
    already guessed an encoding for us (ascii, then utf-8, then latin-1) and
    re-encoding that guess is how a header value acquires a mojibake round
    trip. Header values are opaque octets to a proxy; the fewer of them we
    interpret, the fewer we can corrupt.
    """
    raw = getattr(headers, "raw", None)
    if raw is None:  # pragma: no cover - only for a Mapping that is not httpx's
        raw = [(k.encode("latin-1", "replace"), v.encode("latin-1", "replace"))
               for k, v in headers.items()]
    return [(name.lower(), value) for name, value in raw
            if name.lower() in FORWARDED_RESPONSE_HEADERS]


def forwarded_request_headers(
    scope: Scope, allowed: frozenset[bytes]
) -> dict[str, str]:
    """Client request headers that are allowed to reach the provider.

    Empty by default for anything not named in
    `ServerConfig.forward_request_headers`, and the config refuses to name a
    credential or a connection-scoped header (`NEVER_FORWARDED`). The rule is
    worth stating in the negative: a proxy that forwards the client's
    `authorization` upstream lets the client choose which key the provider
    bills, and one that forwards `host` or `content-length` builds a request
    that describes a connection it is not on.
    """
    if not allowed:
        return {}
    out: dict[str, str] = {}
    for name, value in scope.get("headers", ()):
        lower = bytes(name).lower()
        if lower in allowed:
            out[lower.decode("latin-1")] = bytes(value).decode("latin-1")
    return out


def response_headers(
    stream: UpstreamStream,
    *,
    exchange: Exchange,
    streaming: bool,
    content_length: int | None = None,
) -> list[tuple[bytes, bytes]]:
    """Everything that goes out with `http.response.start` on a 2xx.

    Built from the `Exchange` rather than from loose arguments because this is
    called from inside the sink factory, several frames below the endpoint,
    at the one instant when "how many attempts" and "which target" are finally
    known. The exchange is where those two facts are written down as they are
    learned, so the error handler above and the status line below cannot
    disagree about them.
    """
    out = exchange.gw_headers()
    if stream.body_modified:
        # `ProviderConn.extra_body` was configured, so the bytes we sent
        # upstream are not the bytes the client sent us. Announcing it is the
        # difference between a documented hole and an afternoon spent
        # debugging a request nobody made. See `upstream.apply_extra_body`.
        out.append((b"x-gw-body-modified", b"1"))
    forwarded = forwarded_headers(stream.headers)
    out.extend(forwarded)
    if not any(name == b"content-type" for name, _ in forwarded):
        out.append((b"content-type",
                    b"text/event-stream" if streaming else b"application/json"))
    if content_length is not None:
        # Ours, computed from the bytes we hold -- never the upstream's. Only
        # the buffered path can honestly produce one.
        out.append((b"content-length", str(content_length).encode("ascii")))
    return out


def _retry_after_header(seconds: float) -> bytes:
    """Seconds, rounded UP.

    C5 treats Retry-After as a floor. Rounding 3.4 down to 3 tells the client
    to come back sooner than the provider asked, which is the one direction
    this value must never move.
    """
    return str(max(0, math.ceil(seconds))).encode("ascii")


# ==========================================================================
# What the upstream's response headers tell us (and the client never sees)
# ==========================================================================

UPSTREAM_REQUEST_ID_HEADER = b"x-gw-upstream-request-id"
"""The provider's own request id, re-emitted under our prefix. It is the one
thing a support ticket to the provider needs and the one thing the
response-header allowlist used to drop; re-emitting it under `X-Gw-` says
whose id it is. Not credential material (PLAN-2 A6d)."""

_UPSTREAM_REQUEST_ID_NAMES = ("x-request-id", "request-id", "x-inworld-request-id")
_UPSTREAM_PROCESSING_MS_NAMES = ("openai-processing-ms", "x-envoy-upstream-service-time")

# OpenAI dialect: `x-ratelimit-remaining-{requests,tokens}` with resets as
# Go-style durations ("6m0s", "1s", "20ms"). Anthropic:
# `anthropic-ratelimit-{requests,tokens,input-tokens,output-tokens}-remaining`
# with resets as RFC 3339 timestamps. Folded onto `metrics.RATELIMIT_KINDS`.
_RATELIMIT_HEADERS: tuple[tuple[str, str, str], ...] = (
    ("requests", "x-ratelimit-remaining-requests", "x-ratelimit-reset-requests"),
    ("tokens", "x-ratelimit-remaining-tokens", "x-ratelimit-reset-tokens"),
    ("requests", "anthropic-ratelimit-requests-remaining",
     "anthropic-ratelimit-requests-reset"),
    ("tokens", "anthropic-ratelimit-tokens-remaining", "anthropic-ratelimit-tokens-reset"),
    ("input_tokens", "anthropic-ratelimit-input-tokens-remaining",
     "anthropic-ratelimit-input-tokens-reset"),
    ("output_tokens", "anthropic-ratelimit-output-tokens-remaining",
     "anthropic-ratelimit-output-tokens-reset"),
)

_DURATION_UNITS = (("ms", 0.001), ("h", 3600.0), ("m", 60.0), ("s", 1.0))


def parse_reset_seconds(value: str | None, *, now: float | None = None) -> float | None:
    """Seconds until a provider budget refills, from either spelling.

    Go durations (`1h2m3.5s`, `20ms`), bare numbers (seconds), or an RFC 3339
    timestamp (Anthropic). Never negative, never raises: a header we cannot
    read leaves the gauge alone rather than failing a request.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return max(0.0, float(text))
    except ValueError:
        pass
    if "T" in text and ("Z" in text or "+" in text or text.count("-") >= 3):
        from datetime import datetime
        try:
            when = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        reference = time.time() if now is None else now
        return max(0.0, when.timestamp() - reference)
    total = 0.0
    rest = text
    while rest:
        for unit, scale in _DURATION_UNITS:
            if rest.endswith(unit):
                rest = rest[: -len(unit)]
                # Peel the number in front of the unit.
                i = len(rest)
                while i > 0 and (rest[i - 1].isdigit() or rest[i - 1] == "."):
                    i -= 1
                num = rest[i:]
                rest = rest[:i]
                if not num:
                    return None
                try:
                    total += float(num) * scale
                except ValueError:
                    return None
                break
        else:
            return None
    return max(0.0, total)


@dataclasses.dataclass(frozen=True, slots=True)
class UpstreamTelemetry:
    """What one upstream response told us about itself, headers only."""

    request_id: str | None
    processing_ms: float | None
    ratelimits: tuple[tuple[str, float | None, float | None], ...]
    """`(kind, remaining, reset_seconds)` per `metrics.RATELIMIT_KINDS` kind
    the provider reported. Either number may be None."""


def parse_upstream_telemetry(
    headers: Mapping[str, str] | None, *, now: float | None = None
) -> UpstreamTelemetry:
    """Pull request id, processing time and rate-limit budget off a provider's
    response headers. Case-insensitive on the names; never raises."""
    if not headers:
        return UpstreamTelemetry(None, None, ())
    lower = {str(k).lower(): str(v) for k, v in headers.items()}
    request_id = next((lower[n] for n in _UPSTREAM_REQUEST_ID_NAMES if lower.get(n)), None)
    processing_ms: float | None = None
    for name in _UPSTREAM_PROCESSING_MS_NAMES:
        raw = lower.get(name)
        if raw:
            try:
                processing_ms = float(raw.strip())
            except ValueError:
                processing_ms = None
            else:
                break
    limits: list[tuple[str, float | None, float | None]] = []
    for kind, remaining_name, reset_name in _RATELIMIT_HEADERS:
        remaining_raw = lower.get(remaining_name)
        reset_raw = lower.get(reset_name)
        if remaining_raw is None and reset_raw is None:
            continue
        remaining: float | None
        try:
            remaining = float(remaining_raw.strip()) if remaining_raw else None
        except ValueError:
            remaining = None
        limits.append((kind, remaining, parse_reset_seconds(reset_raw, now=now)))
    return UpstreamTelemetry(request_id, processing_ms, tuple(limits))


def rewrite_response_model(body: bytes, catalog_model_id: str) -> bytes:
    """On the BUFFERED path, put the catalog id back into the response `model`.

    The request's `model` was rewritten to the provider's wire id
    (`X-Gw-Body-Modified: 1`), so the provider answers with the wire id -- or
    a snapshot of it -- and an SDK that echoes the response model into its
    next request sends a name the gateway did not issue. Streaming bodies are
    never rewritten (byte-for-byte passthrough holds; the alias table in
    `policy` makes the echoed wire id acceptable instead). Here the whole
    body is in hand and already re-measured for `content-length`, so the
    rewrite is one field in a JSON object we are about to send anyway. Any
    body that is not a JSON object with a string `model` goes out untouched.
    """
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return body
    if not isinstance(parsed, dict) or not isinstance(parsed.get("model"), str):
        return body
    if parsed["model"] == catalog_model_id:
        return body
    parsed["model"] = catalog_model_id
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


# ==========================================================================
# The pieces the pump plugs into
# ==========================================================================


class ASGISink:
    """`Pump`'s `Sink`, over ASGI `send`. Four lines, and all of the risk.

    `more_body=True` on every write: the terminating message is the caller's
    to send, and only on a stream that actually ended. That split is what makes
    C2 expressible -- a post-commitment failure simply never sends the last
    message, and the body stops mid-answer exactly as a dying provider's would.

    Note what this does NOT promise. `send()` returning means uvicorn accepted
    the bytes, not that the client received them; uvicorn awaits its own
    flow-control event only once the transport's write buffer is over the high
    watermark. So backpressure reaches the pump late and coarsely, which is
    precisely why `pump.py` sets its commitment flag BEFORE the await rather
    than after -- a write we cannot confirm is a write we must assume landed.
    """

    __slots__ = ("_send",)

    def __init__(self, send: Send) -> None:
        self._send = send

    async def send(self, chunk: bytes) -> None:
        await self._send({"type": "http.response.body", "body": chunk,
                          "more_body": True})


class BufferedSink:
    """The `stream: false` sink: one write, status and body together.

    `Executor._attempt()` drains a non-streaming body to completion *before*
    it opens the commitment, then hands the whole payload to the sink in a
    single `send()`. That ordering is what lets this path do something the
    streaming path cannot: send an honest `content-length` we computed from
    the bytes we are holding, rather than forwarding the upstream's (which is
    the header bug that presents as a truncated answer -- see `HOP_BY_HOP`).

    The status therefore leaves from inside `send()` rather than from the
    factory. The commitment is still the factory call -- the executor stops
    considering other targets the moment it has this object -- but the actual
    `http.response.start` waits the extra microsecond until the length is
    known. `started` is set on the line before the await, for the same reason
    it is everywhere else in this package: a send that raises halfway may
    still have put the status line on the wire.
    """

    __slots__ = ("_send", "_stream", "_exchange", "_surface", "_spent")

    def __init__(
        self, send: Send, *, stream: UpstreamStream, exchange: Exchange,
        surface: Surface | None = None,
    ) -> None:
        self._send = send
        self._stream = stream
        self._exchange = exchange
        self._surface = surface
        self._spent = False

    async def send(self, chunk: bytes) -> None:
        if self._spent:  # pragma: no cover - the executor sends exactly once
            raise RuntimeError("the buffered response has already been sent")
        self._spent = True
        # The buffered path has no frames for `apply_usage` to read a stop
        # reason from, so ask the dialect about the whole body (PLAN-2 A3).
        # Recorded on the exchange for `_record`; never raises, never
        # changes the bytes.
        reader = getattr(self._surface, "stop_reason_from_body", None)
        if reader is not None:
            try:
                payload = json.loads(chunk)
            except (ValueError, UnicodeDecodeError):
                payload = None
            if isinstance(payload, dict):
                self._exchange.buffered_stop_reason = reader(payload)
        if self._stream.body_modified and self._exchange.target is not None:
            # The request's `model` was rewritten to the wire id, so the
            # provider's answer names the wire id; put the catalog id back so
            # a client echoing the response model on its next turn sends a
            # name the gateway issued (PLAN-2 A1). Buffered path only.
            chunk = rewrite_response_model(chunk, self._exchange.target.model.id)
        headers = response_headers(
            self._stream, exchange=self._exchange, streaming=False,
            content_length=len(chunk),
        )
        self._exchange.started = True
        await self._send({"type": "http.response.start",
                          "status": self._stream.status, "headers": headers})
        await self._send({"type": "http.response.body", "body": chunk,
                          "more_body": False})


# ==========================================================================
# Two seams the executor does not offer, and what they cost
# ==========================================================================


class _AttemptTally:
    """A per-request façade over `Upstream` that counts opens and names targets.

    The other thing `execute()` cannot tell us in time. On the streaming path
    the status line carries `X-Gw-Attempts` and `X-Gw-Served-By`, and it is
    written from inside the sink factory -- at which point the winning attempt
    has not been recorded yet and `ExecutionResult` does not exist. Reading
    them off the result afterwards is not an option: HTTP has no second status.

    So the count is taken where an attempt actually begins, which is the call
    to `Upstream.open()`. Two properties make the numbers exact rather than
    approximate:

    * the pre-flight `deadline.check()` at the top of the executor's loop does
      NOT open anything and does NOT record an attempt, so the tally and
      `len(result.attempts)` agree on requests that ran out of time;
    * the winner is always the last target opened, so `target` at commitment
      is `result.served_by`.

    One instance per request. `Executor` is stateless and cheap to construct,
    so wrapping the pool per request costs an object, not a connection.
    """

    __slots__ = ("_upstream", "opens", "target")

    def __init__(self, upstream: Upstream) -> None:
        self._upstream = upstream
        self.opens = 0
        self.target: Target | None = None

    def open(self, request: UpstreamRequest, *, deadline: Deadline, budgets: Budgets):
        self.opens += 1
        self.target = request.target
        return self._upstream.open(request, deadline=deadline, budgets=budgets)


class _GateWatch:
    """A per-request façade over `BreakerRegistry` that notices refusals.

    The third seam the executor does not offer, and the same shape as
    `_AttemptTally` for the same reason: `X-Gw-Breaker` has to be on the
    status line, the status line is written from inside the sink factory,
    and at that instant the `ExecutionResult` -- which carries the refusals
    -- does not exist yet. So the fact is captured where it happens, which is
    the `acquire()` that raised.

    `for_key` hands the executor a `_WatchedBreaker` whose `acquire` records
    the circuit's state on the exchange when it refuses and whose `record`
    and `release` are the real breaker's. Nothing is counted twice: the
    registry is the one in `Gateway`, and this object owns no state but a
    reference to it and to the exchange.

    Why the state is read off the breaker and not off the error: `BreakerOpen`
    says `retry_after` when OPEN and nothing when HALF_OPEN, which is enough
    to tell them apart but is an inference. `Breaker.state` is the fact, in
    the vocabulary `metrics.BREAKER_STATES` will label it with.
    """

    __slots__ = ("_registry", "_exchange")

    def __init__(self, registry: BreakerRegistry, exchange: Exchange) -> None:
        self._registry = registry
        self._exchange = exchange

    def for_key(self, key: Key) -> _WatchedBreaker:
        return _WatchedBreaker(self._registry.for_key(key), self._exchange)


class _WatchedBreaker:
    __slots__ = ("_breaker", "_exchange")

    def __init__(self, breaker: Breaker, exchange: Exchange) -> None:
        self._breaker = breaker
        self._exchange = exchange

    def acquire(self) -> Ticket:
        try:
            return self._breaker.acquire()
        except errors.BreakerOpen:
            self._exchange.breaker = self._breaker.state.value
            raise

    def record(self, ticket: Ticket, disposition: errors.Disposition | None) -> None:
        self._breaker.record(ticket, disposition)

    def release(self, ticket: Ticket) -> None:
        self._breaker.release(ticket)


class RequestTooLarge(Exception):
    """The client body blew `max_request_bytes`.

    Deliberately NOT a `GatewayError` subclass. The taxonomy has no 413 -- no
    class can produce that status, since `client_status` picks between a
    class-level `status` and an upstream one -- and adding a subclass here
    would silently join `errors.ERROR_CODES`' enumeration tests with a code
    they have never seen. So it is a local exception with a local handler, and
    "the taxonomy needs a `RequestTooLarge` before this outcome can be
    counted" is a P5 problem stated out loud rather than a metric that quietly
    reports nothing.
    """


class Unauthenticated(Exception):
    """No usable bearer token, with a tenants file loaded.

    Local for the same reason `RequestTooLarge` is: the taxonomy has no 401.
    Raised only by `Gateway.resolve_tenant`, and its message is written there
    with one rule -- it never contains the token, or any prefix of it, or its
    length. "unknown bearer token" is the whole of what a client is told, and
    it is the whole of what a log line gets.
    """


async def read_request_body(
    receive: Receive, *, limit: int, deadline: Deadline
) -> bytes:
    """The client's body, bounded in bytes and in time.

    The bound is checked as chunks arrive and never after assembly: a limit
    enforced on a body you have already joined is a limit that allocated the
    thing it was protecting you from.

    The time bound is the request's own `Deadline` and not a second clock. A
    client that cannot finish sending inside the total budget is
    `ClientTooSlow` -- a CLIENT blame and NEUTRAL health, because a provider
    that has not been contacted yet cannot be sick.
    """
    chunks: list[bytes] = []
    total = 0
    try:
        # `on_total` is the load-bearing argument here, not `on_timeout`.
        # With `budget=None` the phase IS the total, so only `on_total` can
        # ever fire -- and without it a client that dribbles its own request
        # body would arrive as a PROVIDER fault with FAILURE health against a
        # provider we have not opened a socket to. See clocks.phase().
        async with phase(deadline, None, on_timeout=errors.ClientTooSlow,
                         on_total=errors.ClientTooSlow):
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    raise errors.ClientDisconnected(
                        "client hung up while sending its body"
                    )
                body = message.get("body", b"")
                total += len(body)
                if total > limit:
                    raise RequestTooLarge(f"request body exceeds {limit} bytes")
                if body:
                    chunks.append(body)
                if not message.get("more_body", False):
                    return b"".join(chunks)
    except errors.TotalDeadlineExceeded as err:
        # `phase()` reports a breach of the total as `TotalDeadlineExceeded`,
        # which is right where the phase and the total are different clocks
        # and wrong here, where they are the same one: the budget passed above
        # is `None`, so the phase IS the total and `on_timeout` can never fire.
        # Left unconverted, a client that dribbles its own request body until
        # the deadline arrives as a PROVIDER-blamed `total_deadline_exceeded`
        # (504) naming a provider we have not opened a socket to. The class is
        # NEUTRAL health, so the breaker would not hear it -- but the blame,
        # the status and the metric row would all point at the wrong party,
        # and the only party we were ever waiting on in this function is the
        # client (C8).
        raise errors.ClientTooSlow(
            f"client did not finish sending its body within {deadline.total:.3f}s "
            f"({total} bytes received)",
            cause=err,
        ) from err


async def await_disconnect(receive: Receive) -> None:
    """Park in `receive()` until the client goes away.

    A loop rather than a single call because ASGI does not promise the next
    message is the one we want, and because uvicorn's `receive()` also
    re-enables reading on the transport -- which is what makes the disconnect
    observable at all. Nobody pushes `http.disconnect` at an application; it
    is only delivered to a caller who is already waiting for it.
    """
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return


async def run_until_disconnect(work: Coroutine[Any, Any, T], receive: Receive) -> T:
    """Run `work`; cancel it the instant the client hangs up.

    Two tasks, one race, and a `finally` that outlives neither. The shape is
    deliberate in three ways:

    * The worker's result is preferred whenever it is available, even if both
      tasks finished in the same pass of the loop. A stream that completed and
      a client that then disconnected is a completed stream.
    * `ClientDisconnected` is raised rather than returned. It is `NEUTRAL`
      health and `CANCELED` outcome (C8), so a client-side incident cannot
      teach a circuit breaker anything about a provider that did nothing.
    * Both tasks are cancelled AND awaited on the way out. Cancelling without
      awaiting leaves a task that still holds the upstream response open for
      an unbounded number of event-loop turns, which is a leak that reads as a
      flaky test long before it reads as a bug.
    """
    worker: asyncio.Task[T] = asyncio.ensure_future(work)
    watcher: asyncio.Task[None] = asyncio.ensure_future(await_disconnect(receive))
    try:
        await asyncio.wait({worker, watcher}, return_when=asyncio.FIRST_COMPLETED)
        if worker.done():
            return worker.result()
        raise errors.ClientDisconnected("client disconnected while the stream was open")
    finally:
        watcher.cancel()
        worker.cancel()
        # `return_exceptions` because we are on our way out with an answer
        # already: a cleanup that raises replaces a real error with a worse one.
        await asyncio.gather(watcher, worker, return_exceptions=True)


# ==========================================================================
# Policy, resolved once per snapshot rather than once per request
# ==========================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class _Derived:
    """Everything computed FROM a snapshot that a request must not recompute.

    A snapshot is a value, so anything derived from it is a value too, and
    deriving it per request would be paying a hash and a validation pass for
    an answer that cannot have changed. The two members are here for two
    different reasons and both are about where failure lands:

    `catalog_id` is expensive (a canonicalisation and a SHA-256 over every
    model and provider) and goes out on every single response.

    `retry` is *fallible*. `ExecutionPlan.retry` is whatever the policy file
    said -- a frozen mapping, not a `RetryPolicy`, because `policy.py` is
    deliberately not coupled to `retry.py`. Somebody has to build and validate
    the dataclass, and doing it per request means a `max_delay` below
    `base_delay` is a 500 on the hot path instead of a refusal to start.
    """

    snapshot: PolicySnapshot
    catalog_id: str
    retry: Mapping[str, RetryPolicy | None]


def _derive(snapshot: PolicySnapshot) -> _Derived:
    return _Derived(
        snapshot=snapshot,
        catalog_id=snapshot.catalog_id,
        retry={
            wid: _retry_policy(snapshot.plan_for(wid))
            for wid in snapshot.workloads
        },
    )


def _retry_policy(plan: ExecutionPlan) -> RetryPolicy | None:
    """The `RetryPolicy` a plan's config table describes, or None for silence.

    `policy.py` types `retry` as `object | None` and hands through whatever
    TOML contained, frozen. That looseness is deliberate on its side -- the
    policy layer does not import the retry layer -- and it makes this
    conversion somebody's job. It is the server's, because the server is where
    the two layers are already both in scope.

    None stays None rather than becoming a default policy, because those are
    different statements: a workload that configured no retry table has said
    nothing, and `executor.NO_RETRIES` is the safe reading of silence in a
    component that can amplify load.
    """
    configured = plan.retry
    if configured is None:
        return None
    if isinstance(configured, RetryPolicy):
        return configured.validate()
    if isinstance(configured, Mapping):
        try:
            return RetryPolicy(**{str(k): v for k, v in configured.items()}).validate()
        except (TypeError, ValueError) as exc:
            raise errors.PolicyError(
                f"workload {plan.workload_id!r}: [retry] is not a valid retry "
                f"policy: {exc}",
                workload=plan.workload_id,
                cause=exc,
            ) from exc
    raise errors.PolicyError(
        f"workload {plan.workload_id!r}: [retry] must be a table, got "
        f"{type(configured).__name__}",
        workload=plan.workload_id,
    )


def _retry_policy_for(
    plan: ExecutionPlan, configured: RetryPolicy | None, *, no_retry: bool
) -> RetryPolicy | None:
    """C5: `X-Gw-No-Retry: 1` buys zero repetitions and keeps the fallback.

    The two halves are the whole contract and they are one line apart:

        dataclasses.replace(policy, enabled=False)

    `enabled=False` makes `RetryBudget.delay_for` refuse every delay, so no
    target is ever asked twice and a `Retry-After` is never slept. It does
    NOTHING to `plan.targets`, which is where breadth lives -- so the
    incumbent is still tried, because the incumbent is not a retry. It is a
    different provider, and it is the one thing the outer gateway that just
    took ownership of the retries cannot do for us: it has never seen our plan.

    Collapsing those two would mean a caller who owns its own retries silently
    loses the redundancy its workload was configured for, and would discover
    it during the incident the redundancy was for.

    When nothing is configured we still send an explicit disabled policy
    rather than `None`. Both produce zero repetitions today, and the explicit
    one says so at the seam instead of relying on `execute()`'s reading of
    silence continuing to match ours.
    """
    if not no_retry:
        return configured
    policy = configured
    if policy is None:
        policy = dataclasses.replace(
            NO_RETRIES, max_attempts=max(1, len(plan.targets))
        )
    return dataclasses.replace(policy, enabled=False)


# ==========================================================================
# The gateway object: built once, at startup
# ==========================================================================


@dataclasses.dataclass(frozen=True, slots=True)
class DrainReport:
    """The result of one `Gateway.begin_drain()`, read by the S8 table.

    A drain is the whole graceful-deploy contract in three numbers, so the
    report carries exactly those and nothing a caller has to derive. It is a
    record of what happened, not a decision: `begin_drain` waits and reports,
    and whoever called it (the signal handler) decides that a non-zero `cut`
    is acceptable because the grace expired, not because the drain failed.
    """

    inflight_at_start: int
    """Streams in flight the instant `draining` flipped -- the number the
    drain set out to let finish."""

    cut: int
    """Streams STILL open when the grace expired, i.e. the streams the deploy
    is about to interrupt. Zero is the success case (S8: zero cut streams);
    non-zero is FAILURE-MODES.md row 11's residual made countable rather than
    hidden. `begin_drain` does not itself cut them -- it reports the count and
    lets the server's own shutdown cancel them through the existing terminal
    path, so each still gets its CANCELED outcome and capture record. This is
    a count taken BEFORE the cuts; the streams actually cut are counted after
    the fact in `Gateway.shutdown_cuts` (`ShutdownCuts`), which is the number
    `lifecycle.log_shutdown_cuts` prints once the server has stopped."""

    duration_s: float
    """Wall time from flipping `draining` to the wait ending, by the injected
    clock. The drain took this long; the deploy's readiness gap is this."""

    timed_out: bool
    """True if the grace expired with streams still open (so `cut > 0`), False
    if every in-flight stream finished first (so `cut == 0`). Redundant with
    `cut` by construction, kept because 'did we hit the deadline' is the
    question an operator asks and an equality against zero is not an answer."""


class ShutdownCuts:
    """The streams uvicorn's post-grace shutdown actually cut, counted.

    This is the single source of truth for "cut". `DrainReport.cut` is the
    number of streams still open when the grace expired -- a count taken
    BEFORE the cuts, of what is about to happen. This counter is filled in
    AFTER, once per stream, on the endpoint's shutdown-cut path, and is what
    `lifecycle.log_shutdown_cuts` prints. The two agree unless a stream ends
    on its own inside uvicorn's short bound, in which case this one is right.

    Why a counter and not a log line per cut: the S8-B run of 15 Sep 2026
    showed that anything written to stderr once per cut stream, in the same
    instant, is a byte an undrained 64 KiB pipe must absorb before the
    process can exit -- first as 4 KB tracebacks, then as 88-byte WARNINGs
    that were still about 450 streams from blocking. Bounded output on the
    shutdown path means output that does NOT scale with the number of open
    streams, so the per-cut fact goes here, and stderr gets ONE line at the
    end. `by_target` is bounded by the catalog: its keys are `str(Target)`,
    of which a process has at most as many as it has targets, plus `"-"` for
    a request cut before any target was chosen. The per-request detail
    (tokens so far, cost, duration) is in the capture record each cut request
    already wrote with `outcome=CANCELED` (C3); nothing is lost by not
    logging it twice."""

    __slots__ = ("total", "committed", "by_target")

    def __init__(self) -> None:
        self.total = 0
        self.committed = 0
        self.by_target: dict[str, int] = {}

    def note(self, *, target: Target | None, committed: bool) -> None:
        self.total += 1
        if committed:
            self.committed += 1
        label = str(target) if target is not None else "-"
        self.by_target[label] = self.by_target.get(label, 0) + 1

    @property
    def uncommitted(self) -> int:
        return self.total - self.committed

    def summary(self, *, top: int = 5) -> str:
        """One bounded line: totals, then at most `top` targets."""
        ranked = sorted(self.by_target.items(), key=lambda kv: (-kv[1], kv[0]))
        shown = ", ".join(f"{label}={n}" for label, n in ranked[:top])
        if len(ranked) > top:
            shown += f", +{len(ranked) - top} more"
        return (
            f"shutdown cut {self.total} stream(s): committed={self.committed} "
            f"uncommitted={self.uncommitted}; targets: {shown or '-'}"
        )


class Gateway:
    """Process-wide state, created in the Starlette lifespan and closed there.

    The `Upstream` and the `Catalog` are built once because both are pools in
    disguise -- one of TCP connections, one of validated config -- and
    rebuilding either per request throws away the whole argument for pooling.
    `Catalog._validate()` running per request would also move a config error
    from startup onto the hot path, which is the exact inversion of
    cheapest-rejection-first.
    """

    __slots__ = ("config", "clock", "registry", "draining", "policy",
                 "tenants", "admission", "breakers", "limiter",
                 "_upstream", "_derived", "_collectors", "_capture",
                 "_inflight", "_idle", "_draining_denied", "_overloaded_denied",
                 "shutdown_cuts", "ws_sessions")

    def __init__(self, config: ServerConfig, *, clock: Clock | None = None) -> None:
        self.config = config.validated()
        self.clock = clock or SystemClock()

        # Built at startup, not here: the collectors register into `registry`
        # and the capture worker spawns a task, and both belong to the running
        # process, not to a `Gateway` that a test constructs to read its config.
        # They are None until `startup()`, and every emit site tolerates that --
        # so a `build_app()` whose lifespan has not run still routes, it just
        # does not record. `breakers` below is wired to `_on_breaker_transition`
        # *now*, before `_collectors` exists, which is safe because a transition
        # cannot happen before a request is served and a request cannot be
        # served before startup.
        self._collectors: Collectors | None = None
        self._capture: Capture | None = None

        self.tenants: TenantTable | None = config.tenant_table()
        """Token -> tenant id, or None on the zero-config path. Read once, at
        startup: a malformed tenants file is a process that refuses to start.
        """

        self.admission = AdmissionController(
            clock=self.clock,
            # ZERO-CONFIG PATH. No tenants file means every request is the
            # one anonymous tenant under `config.tenant_limits`, and a bearer
            # token, if one is sent, is ignored -- there is nothing to check
            # it against. This is the `fake_upstreams` argument again: the
            # fallback exists so that `make run` + `curl` works on a clean
            # checkout, and it is LOUD about it -- logged at startup below,
            # reported by `/probe` as `tenant_mode` -- rather than silent,
            # because "every tenant shares one bucket" is exactly the
            # misconfiguration row 6 describes, and one that a green
            # dashboard will not reveal. With a file, there is no default:
            # an unknown token is a 401 before admission is even consulted.
            default=None if self.tenants is not None else config.tenant_limits,
        )
        if self.tenants is None:
            log.warning(
                "no LLMGW_TENANTS_FILE: every request is tenant %r under %s; "
                "bearer tokens are not checked",
                ANONYMOUS_TENANT, config.tenant_limits,
            )
        else:
            for tenant, limits in self.tenants.limits.items():
                self.admission.configure(tenant, limits)
            # Ids and counts only; the table's own repr never carries a token.
            log.info(
                "tenants: %d configured, %d authenticated by bearer token, "
                "anonymous %s (required=%s)",
                len(self.tenants.limits), self.tenants.authenticated_tenants,
                "allowed" if ANONYMOUS_TENANT in self.tenants else "refused",
                config.require_tenants,
            )

        self.breakers = BreakerRegistry(
            config.breaker, clock=self.clock,
            # Every state change reaches the two breaker metrics from here --
            # the one place the breaker announces a transition -- rather than
            # from a poll that would miss a flap that opened and closed between
            # scrapes. The hook forwards to the collectors if they exist yet.
            on_transition=self._on_breaker_transition,
        )
        """One registry per process, one policy for every key in it, created
        lazily per `(provider, model)` and `(provider, "cred:<id>")` as the
        executor asks. Process-local: each worker learns a provider's
        health independently."""

        self.limiter = ProviderKeyLimiter()
        """Per-credential in-flight caps, shared by every tenant that routes
        through a credential. The cap itself is the catalog's, per call."""

        self.policy = PolicyStore(
            config.policy_snapshot(clock=self.clock), validate=_derive
        )
        """The snapshot every request pins itself to, in the box that can swap
        it whole. Built HERE, at startup, so that a policy file with a typo in
        it is a process that refuses to start rather than a 400 per request --
        and so that P4's reload is `store.replace(...)` and nothing else.

        `_derive` is the validator because it is the one thing a request
        needs from a snapshot that `PolicySnapshot` itself cannot check: the
        `RetryPolicy` built from each workload's `[retry]` table. Run inside
        `replace()`, before the swap, it turns a `max_delay < base_delay` in a
        reloaded file into a refused reload with the old snapshot still
        serving -- instead of a `PolicyError` on every request after it."""

        # B6: the deploy inequality against the largest total in the policy,
        # not only the zero-config default; refuses at startup like the rest.
        config.check_drain_arithmetic(self.policy.current())
        self._derived = _derive(self.policy.current())
        """Per-snapshot values too expensive to recompute per request. Built
        eagerly for the same reason: a `RetryPolicy` that cannot be
        constructed from the config's retry table must fail at startup. (The
        store's validator has just proved it can be; this is the second
        derivation at startup, kept so `derived()` never misses on the first
        request. One extra hash per process start is not worth a code path.)"""

        self.registry = CollectorRegistry()
        """Empty in P2 and correctly typed. P5 registers `metrics.METRICS`
        into it; nothing else has to change for /metrics to start answering."""

        self.draining = False
        """The P6 hook, and nothing sets it in P2. Drain is: flip this on
        SIGTERM, let /healthz answer 503 so the load balancer stops sending
        work, and let open streams finish. Leaving the flag here means P6 adds
        a signal handler rather than a concept.

        Monotone: `begin_drain` sets it True and nothing sets it back. A
        process that has decided to shut down does not un-decide, and a flag
        that can flap would let a request that raced the flip be admitted into
        a process that is already tearing down its pool."""

        self._inflight = 0
        """Count of streams currently inside the endpoint's open/finally pair.

        The awaitable half of what `llmgw_streams_open` already measures: the
        gauge is a Prometheus number that a drain cannot `await`, so this is
        the real primitive the drain waits on. Incremented and decremented at
        the SAME site the gauge is (endpoint entry / `finally`), which is what
        makes 'the tracker returns to zero' a property of the code's shape and
        not a second thing to keep in step -- one owner, two outputs."""

        self._idle = asyncio.Event()
        self._idle.set()
        """Set whenever `_inflight` is zero, cleared whenever it is not. This
        is what `begin_drain` blocks on: 'every in-flight stream has finished'
        is exactly 'the count returned to zero', and an Event that tracks that
        edge lets the wait be bounded by the injected clock with no polling.
        Starts SET because a freshly built gateway has nothing in flight."""

        self._draining_denied = 0
        """Requests shed at ingress because the process was draining. Surfaced
        through `draining_denials()` into both the `/probe` denial map and the
        `llmgw_admission_denied_total{reason="draining"}` counter -- the same
        two places admission's own denials are surfaced -- so a drain's shed
        traffic is countable exactly like a rate or concurrency denial, which
        is what the pre-declared `metrics.DENIAL_REASONS` entry was for."""

        self._overloaded_denied = 0
        """Requests shed at ingress because `_inflight` was over
        `config.max_streams`. The other process-wide refusal, surfaced the
        same two ways as `_draining_denied` under reason `"overloaded"`."""

        self.shutdown_cuts = ShutdownCuts()
        """Streams cut by uvicorn's post-grace shutdown, counted instead of
        logged per stream. Read once, by `lifecycle.log_shutdown_cuts`, after
        the server has stopped -- the only moment the count is final."""

        self.ws_sessions: set[Any] = set()
        """Relayed WebSocket sessions currently open (`llmgw.ws.session.
        Session`), for the one thing the in-flight tracker cannot express.

        A drain WAITS for a request, because a request was always going to
        end. A socket was not: a TTS connection is idle between utterances and
        a transcription session ends when the learner stops talking, so
        waiting on `_idle` alone would spend the whole grace and then cut
        every session at the worst possible instant. `begin_drain` therefore
        tells each session to wind itself down FIRST -- forward the
        provider's own terminate, close the client 4900 -- and only then
        waits on the tracker, which those sessions are still counted in.

        Typed `Any` to keep `llmgw.ws` importable from here without a cycle:
        the ws package imports `Gateway`. Membership is managed by the
        session's own open/finally pair, the same shape `stream_entered`
        has, so a session cannot leak into this set."""

        self._upstream: Upstream | None = None

    @property
    def upstream(self) -> Upstream:
        if self._upstream is None:  # pragma: no cover - lifespan guarantees it
            raise RuntimeError("Upstream is not open; lifespan startup has not run")
        return self._upstream

    @property
    def collectors(self) -> Collectors | None:
        """The live metric collectors, or None before `startup()`. Callable
        sites treat None as 'do not record' rather than an error, so the serving
        path is identical whether or not the lifespan has run -- see
        `__init__`."""
        return self._collectors

    @property
    def capture(self) -> Capture | None:
        """The process capture sink, or None before `startup()`."""
        return self._capture

    # ------------------------------------------------------------ telemetry

    def _on_breaker_transition(
        self, key: Key, frm: BreakerState, to: BreakerState, now: float
    ) -> None:
        """`BreakerRegistry.on_transition`: emit the two breaker metrics.

        `key` is `(provider, model)` for a target circuit and `(provider,
        "cred:<id>")` for a credential one -- both bounded by the catalog, so
        both are legal `provider`/`model` label values. A transition cannot
        occur before a request is served, and a request cannot be served before
        `startup()` built the collectors, so the None guard is belt-and-braces,
        never the common path. Never raises: an observer of a state change does
        not get to fail the state change (the breaker module logs and moves on
        if it did)."""
        collectors = self._collectors
        if collectors is None:  # pragma: no cover - startup precedes any serve
            return
        provider, model = key
        collectors.breaker_transition(provider=provider, model=model, to=to.value)
        collectors.breaker_state(
            provider=provider, model=model, value=to.gauge_value
        )

    def sample_metrics(self) -> None:
        """Refresh the sampled gauges and mirrored counters, at scrape time.

        The `/metrics` handler calls this immediately before `generate_latest`.
        A no-op before `startup()`; after it, `Collectors.sample` guards each
        source so a metric is never worth a 500 on the scrape endpoint."""
        collectors = self._collectors
        if collectors is not None:
            collectors.sample(self)

    # ------------------------------------------------------------ policy

    def derived(self, snapshot: PolicySnapshot) -> _Derived:
        """Cached values for `snapshot`, recomputed only when it changes.

        `PolicySnapshot.catalog_id` is a SHA-256 over the canonicalised
        catalog. It belongs on every response as a header and it must not be
        computed per response; likewise the `RetryPolicy` a workload's config
        table describes, which is validation work that belongs at load. One
        entry, keyed by object identity, because `PolicyStore` only ever hands
        out whole values -- there is never a second live snapshot to thrash
        against, and if a reload lands mid-flight the old requests keep the
        old object and the old derived values with it.
        """
        if self._derived.snapshot is not snapshot:
            self._derived = _derive(snapshot)
        return self._derived

    def resolve_workload(self, snapshot: PolicySnapshot, requested: str | None) -> str:
        """The workload this request routes to. Raises `PolicyError` (400).

        Two modes, and the mode is a property of the DEPLOYMENT rather than of
        the request:

        * **A policy document is loaded.** Workload names are keys in it. An
          unknown one is a `PolicyError` before any upstream work -- the
          cheapest possible rejection, a dict lookup, and the only honest
          answer to "route me as `summarise`" when the file spells it
          `summarize`. Guessing a workload for a caller who named one would
          route their traffic to a model they did not ask for and bill them
          for it.
        * **No document (the zero-config path).** There is exactly one
          workload and therefore no namespace for a name to be wrong about, so
          the name is a label: it routes to the only workload there is. This
          is precisely P2's behaviour -- `/workloads/anything/probe` answered
          about the single configured model -- and preserving it is why
          `ServerConfig.has_policy_document` exists rather than an `is None`
          check inline.
        """
        if requested is None:
            return snapshot.default_workload
        if not self.config.has_policy_document:
            return snapshot.default_workload
        if requested not in snapshot.workloads:
            raise errors.PolicyError(
                f"unknown workload {requested!r}; known: {sorted(snapshot.workloads)}",
                workload=requested,
            )
        return requested

    # ------------------------------------------------------------ tenant

    def resolve_tenant(
        self, scope: Scope, *, schemes: frozenset[str] = BEARER_ONLY
    ) -> str:
        """The tenant this request is admitted as. Raises `Unauthenticated`.

        `schemes` is the set of `Authorization` schemes THIS route accepts,
        and it defaults to Bearer-only, so every HTTP caller is resolved
        exactly as before. A WebSocket route passes its surface's
        `auth_schemes` (Inworld: Basic and Bearer) because the credential
        arrives in the shape the consumer's plugin sends it -- see
        `credential_token`.

        The mode is a property of the DEPLOYMENT, as with workloads:

        * **No tenants file.** Every request is `ANONYMOUS_TENANT`. A token,
          if present, is a label with nothing to check against and is
          ignored -- NOT refused, because refusing would make the zero-config
          path fail for any client that already sends one, and not honoured,
          because a gateway that admits a token it never verified as a tenant
          of its own naming is a gateway with one tenant and many names.
        * **A tenants file.** The token is looked up. No token resolves to
          `anonymous` only if the file configured it, so guest access is a
          decision the operator made in the same table as everyone else. An
          UNKNOWN token is a 401 and is never downgraded to anonymous: a key
          that was rotated out and still gets guest access was never rotated.

        The token does not leave this method. It is compared and dropped;
        the return value and both error messages carry the id or nothing.
        """
        token = credential_token(scope, schemes=schemes)
        if self.tenants is None:
            return ANONYMOUS_TENANT
        if token is None:
            if ANONYMOUS_TENANT in self.tenants:
                return ANONYMOUS_TENANT
            raise Unauthenticated(
                "no credential, and anonymous access is not configured"
            )
        tenant = self.tenants.resolve(token)
        if tenant is None:
            raise Unauthenticated("unknown credential")
        return tenant

    def realtime_pin(self, tenant: str) -> Mapping[str, Any] | None:
        """The tenant's pinned Realtime session fields (Phase E1), or None on
        the zero-config path or for a tenant that pinned nothing."""
        if self.tenants is None:
            return None
        getter = getattr(self.tenants, "realtime_pin", None)
        return getter(tenant) if callable(getter) else None

    def tenant_limits_for(self, tenant: str) -> TenantLimits | None:
        """The limits `tenant` would be admitted under, from config alone --
        for the probe, which must answer for a tenant that has never sent a
        request and therefore has no state in the controller yet."""
        if self.tenants is None:
            return self.config.tenant_limits if tenant == ANONYMOUS_TENANT else None
        return self.tenants.limits.get(tenant)

    # ----------------------------------------------------------- lifespan

    async def startup(self) -> None:
        self._upstream = Upstream(self.config.catalog, clock=self.clock,
                                  http2=self.config.http2,
                                  inject_include_usage=self.config.inject_include_usage)

        # Register the whole metric contract into the process registry. After
        # this line /metrics answers with real families; the comment on
        # `self.registry` in __init__ promised exactly this and nothing else has
        # to change for it.
        self._collectors = Collectors(self.registry)

        # One capture sink per process, its worker started here and stopped in
        # `shutdown()`. A path in config means a FileSink; otherwise the
        # NullSink, so the zero-config path pays nothing. The worker is a real
        # task, which is why it is created in the running loop and torn down
        # below -- the chaos tier asserts the task count returns to baseline.
        sink = (
            FileSink(self.config.capture_path)
            if self.config.capture_path is not None
            else NullSink()
        )
        self._capture = Capture(
            sink,
            max_queue_bytes=self.config.capture_queue_bytes,
            clock=self.clock,
        )
        self._capture.start()

    async def shutdown(self) -> None:
        # Close capture BEFORE the upstream: the worker may be mid-write, and
        # aclose() drains within a bound then stops cleanly without leaking the
        # task. Guarded so a shutdown after a failed startup does not raise.
        capture, self._capture = self._capture, None
        if capture is not None:
            await capture.aclose()

        upstream, self._upstream = self._upstream, None
        if upstream is not None:
            await upstream.aclose()

    # ----------------------------------------------------------- lifecycle

    def stream_entered(self) -> None:
        """One stream entered the serving path: bump the in-flight tracker.

        Called from the endpoint at the SAME point `collectors.stream_open`
        fires, so the awaitable count and the gauge move together. Clearing
        `_idle` here is what makes a drain that started a microsecond ago
        actually wait for this stream rather than race past it."""
        self._inflight += 1
        self._idle.clear()

    def stream_exited(self) -> None:
        """One stream left the serving path, on ANY exit: a pre-admission
        refusal, a clean stream, a client disconnect. Called from the
        endpoint's `finally`, the twin of `stream_entered`, so the count
        returns to zero by the same shape that makes the gauge return to zero.
        When it reaches zero the `_idle` event fires and a waiting drain
        wakes. The `<= 0` guard is belt-and-braces: the open/finally pairing
        already makes a negative count impossible, but a drain blocking
        forever on a count that undershot zero is a hang with no log, so the
        floor is enforced rather than assumed.

        A genuine undershoot is not merely clamped, it is LOGGED. A silent
        clamp is the S8 lie waiting to happen: if a future serving path ever
        decrements without a paired increment, the count would cross below
        zero, the clamp would reset it to zero and fire `_idle` while streams
        were still running, and `begin_drain` would report `cut=0` over a cut
        stream. The floor still protects the drain from hanging; the warning
        makes the mispair visible instead of hiding it."""
        self._inflight -= 1
        if self._inflight < 0:
            log.warning(
                "in-flight tracker went negative (%d): a stream exited without a "
                "paired entry; a drain may under-report still-open streams",
                self._inflight,
            )
        if self._inflight <= 0:
            self._inflight = 0
            self._idle.set()

    @property
    def inflight(self) -> int:
        """Streams currently in the serving path. The number a drain waits to
        see reach zero, exposed for the signal handler's report and for tests
        that assert return-to-baseline."""
        return self._inflight

    def note_draining_denied(self) -> None:
        """Count one request shed at ingress because the process is draining.

        Separate from `AdmissionController`'s counters on purpose: draining is
        a process-wide decision, not a tenant's budget, so rewriting admission
        to carry it would move a deploy concern into the wrong module (the
        guardrail against touching admission's core). The count rejoins the
        other denials where they are read, not where they are stored."""
        self._draining_denied += 1

    def note_overloaded_denied(self) -> None:
        """Count one request shed at ingress by the per-process stream cap.

        Kept beside `note_draining_denied` and out of `AdmissionController`
        for the same reason: the cap is a fact about THIS process's event
        loop, not about any tenant's budget, and the tenant whose request was
        refused did nothing wrong."""
        self._overloaded_denied += 1

    def over_capacity(self) -> bool:
        """Is the serving path over `config.max_streams`?

        Read AFTER `stream_entered()` for the current request, so the count
        includes it: with a cap of N the (N+1)th concurrent request is the one
        refused, and N streams may be open. The count is the drain tracker's,
        not a second counter, so what a drain waits on and what the cap
        refuses at can never disagree. `None` is uncapped."""
        cap = self.config.max_streams
        return cap is not None and self._inflight > cap

    def draining_denials(self) -> dict[str, int]:
        """The draining shed count, keyed by its `metrics.DENIAL_REASONS`
        value. Merged alongside `admission.denials()` and `limiter.denials()`
        at both surfacing sites so `llmgw_admission_denied_total{reason=
        "draining"}` and `/probe`'s denial map agree -- the single reason the
        `"draining"` label was pre-declared in P0."""
        return {"draining": self._draining_denied}

    def overloaded_denials(self) -> dict[str, int]:
        """The stream-cap shed count under its `metrics.DENIAL_REASONS` value,
        merged at the same two sites as `draining_denials()` -- so the label
        is declared in the contract, not minted here."""
        return {"overloaded": self._overloaded_denied}

    async def begin_drain(self, *, grace_s: float) -> DrainReport:
        """Stop taking new work, let in-flight streams finish, then report.

        The drain sequence, and the one method the signal handler calls.
        Three steps, in this order because the order is the contract:

        1. Flip `draining` True FIRST and synchronously. Before this call
           awaits anything, `/healthz` answers 503 (the load balancer stops
           routing) and the ingress begins shedding new requests with reason
           `"draining"`. A drain that awaited before flipping would keep
           admitting new streams into the grace window it is trying to empty.

        2. Wait for the in-flight tracker to reach zero, bounded by `grace_s`
           on the INJECTED clock. `_idle` is already set if nothing is in
           flight, so an idle process drains instantly. The bound uses
           `clock.timeout`, the same primitive the attempt loop and the
           capture worker bound their waits with, so a ManualClock test proves
           both the finish-in-time and the grace-expiry paths with no real
           sleep.

        3. Return the counts. This does NOT cancel the streams still open --
           draining waits, it does not interrupt. The grace expiring is what
           ends the wait; the server's own shutdown (the signal handler sets
           `should_exit` after this returns) is what cancels the remainder,
           and each cancelled stream still runs the executor's `finally` --
           its terminal record and CANCELED outcome (C3/C8). So a cut stream
           is cut cleanly, counted here, accounted there.

        Idempotent in effect: a second call finds `draining` already True and
        simply waits again on whatever is left, which is exactly what the
        double-SIGTERM path wants (the handler turns the second signal into an
        immediate exit, but calling this twice is harmless)."""
        self.draining = True
        started = self.clock.now()
        inflight_at_start = self._inflight
        # STEP 1b (PLAN-G C26): tell every open socket to wind down, before
        # the wait and without awaiting any of them. `drain()` is synchronous
        # and schedules the session's own bounded shutdown -- send the
        # provider's terminate, wait up to `ws_drain_wait_s` for its answer
        # (which carries the billing number on two of the four products),
        # close the client 4900. Awaiting them here instead would serialise
        # five hundred twenty-second waits inside a hundred-and-thirty-second
        # grace; scheduling them lets the existing `_idle` wait below observe
        # all of them finishing in parallel, and a session that will not end
        # is cut by the grace exactly as a stream is.
        #
        # A copy of the set, because `drain()` may complete a session
        # synchronously and remove it. Never raises: a drain that failed
        # because one session's hook did is a deploy that hangs.
        for session in list(self.ws_sessions):
            try:
                # The grace ACTUALLY in play, not the configured one: a
                # caller may drain with a shorter window than `fly.toml`
                # says (the bench does, and so does every drain test), and a
                # session that sized its deadline off the config would sit
                # past the end of it.
                session.drain(grace_s=grace_s)
            except Exception:  # noqa: BLE001 - one bad session may not stop a deploy
                log.exception("ws session drain hook failed; the grace still bounds it")
        timed_out = False
        try:
            async with self.clock.timeout(grace_s):
                await self._idle.wait()
        except TimeoutError:
            # The grace expired with streams still open. Not an error: it is
            # the documented residual (row 11). The caller decides the process
            # exits anyway; here we only record how many did not make it.
            timed_out = True
        return DrainReport(
            inflight_at_start=inflight_at_start,
            cut=self._inflight,
            duration_s=self.clock.now() - started,
            timed_out=timed_out,
        )


# ==========================================================================
# The passthrough endpoint
# ==========================================================================


class Exchange:
    """One request's mutable state, so the error handler can read it.

    It exists for one field above all. `started` -- have we sent
    `http.response.start`? -- is set deep inside the serving path (now inside
    a closure the executor calls) and read by an `except` clause several
    frames above it, and a local variable cannot travel that way: an exception
    raised *after* the send would leave a `started = await ...` assignment
    unexecuted and the handler would answer a 502 into a response that already
    carries a 200. That mistake produces a frame of garbage on the client's
    socket and is invisible in every test that only checks the happy path.

    P3 adds the identity fields, and they are here rather than passed around
    for the same reason: the status line and the error handler must agree
    about which workload routed, which target answered and how many attempts
    it took, and the cheapest way to guarantee agreement is for there to be
    one copy of each fact.

    `snapshot` is stored, never re-fetched. It is THE pinned policy for this
    request (FAILURE-MODES row 10), and the presence of a field here is what
    makes a second `PolicyStore.current()` call unnecessary rather than merely
    discouraged.
    """

    __slots__ = ("snapshot", "catalog_id", "workload_id", "target", "attempts",
                 "started", "tenant", "breaker", "upstream", "buffered_stop_reason",
                 "defaulted_keys", "mint", "pinned_keys", "request_facts")

    def __init__(
        self, snapshot: PolicySnapshot, *, catalog_id: str, workload_id: str
    ) -> None:
        self.snapshot = snapshot
        self.catalog_id = catalog_id
        self.workload_id = workload_id
        self.target: Target | None = None
        self.attempts: int = 0
        self.started: bool = False
        self.upstream: UpstreamTelemetry | None = None
        """What the answering upstream's response headers said about itself
        (request id, processing time, rate-limit budget). Written by
        `observe_upstream` at status commitment or from the terminal error,
        read by `gw_headers` (the request id) and `_record` (the rest)."""
        self.buffered_stop_reason: str | None = None
        """The buffered path's stop reason, from `Surface.stop_reason_from_body`
        in `BufferedSink.send`; the streaming path's comes through `Usage`."""
        self.defaulted_keys: tuple[str, ...] = ()
        self.mint: bool = False
        """This request minted a credential on the tenant's behalf (Phase E);
        the capture record is written with `kind="mint"`."""
        self.pinned_keys: tuple[str, ...] = ()
        """Session keys the tenant's pin overrode on a mint."""
        self.request_facts: Any = None
        """`Surface.parse_request`'s answer for this request, kept so a
        stream the provider never metered can be billed from the request
        side (`Surface.usage_estimate`, Phase D: OpenAI's binary TTS)."""
        """Request keys the served attempt filled in from the target's
        `request_defaults` (PLAN-2 B5); copied off the `UpstreamStream` at
        status commitment, listed in the capture record."""
        self.tenant: str | None = None
        """The admitted tenant's ID. Set by `__call__` the moment
        `resolve_tenant` answers, so every response after that point --
        including a 429 from admission -- says who it was for."""
        self.breaker: str | None = None
        """`open` or `half_open` if a target's circuit refused this request,
        written by `_WatchedBreaker.acquire` at the instant of refusal --
        which is before the status line, so the streaming path can carry it."""

    def observe_upstream(self, headers: Mapping[str, str] | None) -> None:
        """Record the provider's self-description once; first observation
        wins, because the first is the one from the response we are serving
        (or the terminal error) and a later call is a retry's leftovers."""
        if self.upstream is not None or not headers:
            return
        self.upstream = parse_upstream_telemetry(headers)

    def gw_headers(self) -> list[tuple[bytes, bytes]]:
        out = gw_headers(
            policy_id=self.snapshot.id,
            catalog_id=self.catalog_id,
            workload_id=self.workload_id,
            target=self.target,
            attempts=self.attempts,
            tenant=self.tenant,
            breaker=self.breaker,
        )
        if self.upstream is not None and self.upstream.request_id:
            out.append((UPSTREAM_REQUEST_ID_HEADER, _ascii(self.upstream.request_id)))
        return out


SURFACE_NAMES: tuple[str, ...] = tuple(dict.fromkeys(surface.name for surface in REGISTRY))
"""The shipped surface names, for the probe's resolved-limits table."""

MULTIPART_SCAN_BYTES = 64 * 1024
"""How far into a multipart body the gateway looks for the `model` and
`stream` form fields (PLAN-2 B4). A bounded prefix scan, not a decode: the
gateway needs two small text fields to route, and reading a 25 MB upload to
find them would allocate the thing the byte cap exists to bound. The
documented limitation is therefore that those two fields must precede the
file part, or sit within the first 64 KiB -- which is how every SDK orders a
transcription request (text fields first, `file` last)."""

MODEL_QUERY_PARAM = b"model"
MODEL_REQUEST_HEADER = b"x-gw-model"
"""Where a `raw`-bodied surface finds its model: `?model=<catalog id>` or the
`X-Gw-Model` request header (the same name the gateway sends back on every
response, so a client can echo it). A raw body has no JSON to read it from."""


def header_value(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", ()):
        if bytes(key).lower() == name:
            return bytes(value).decode("latin-1").strip() or None
    return None


def query_param(scope: Scope, name: bytes) -> str | None:
    """The FIRST value of `name` in the query string, URL-decoded, or None."""
    from urllib.parse import parse_qsl

    raw = scope.get("query_string") or b""
    for key, value in parse_qsl(raw.decode("latin-1"), keep_blank_values=False):
        if key.encode("latin-1") == name:
            return value or None
    return None


def multipart_boundary(content_type: str | None) -> str | None:
    if not content_type:
        return None
    kind, _, params = content_type.partition(";")
    if kind.strip().lower() != "multipart/form-data":
        return None
    for piece in params.split(";"):
        key, _, value = piece.strip().partition("=")
        if key.strip().lower() == "boundary":
            value = value.strip().strip('"')
            return value or None
    return None


def scan_multipart_fields(
    body: bytes, boundary: str, *, wanted: frozenset[str], limit: int = MULTIPART_SCAN_BYTES
) -> dict[str, str]:
    """Text values of the `wanted` form fields found in the first `limit`
    bytes. Parts that are files, parts after the scan window, and parts whose
    payload is longer than 4 KiB are skipped -- a value that big is not a
    model id. Never raises on a malformed body; routing then fails closed on
    the missing `model`."""
    found: dict[str, str] = {}
    window = body[:limit]
    delim = b"--" + boundary.encode("latin-1")
    for part in window.split(delim)[1:]:
        if part.startswith(b"--"):
            break
        head, sep, payload = part.partition(b"\r\n\r\n")
        if not sep:
            continue
        name: str | None = None
        is_file = False
        for line in head.split(b"\r\n"):
            low = line.lower()
            if not low.startswith(b"content-disposition:"):
                continue
            is_file = b"filename=" in low
            marker = low.find(b" name=")
            if marker < 0:
                marker = low.find(b";name=")
            if marker >= 0:
                after = line[marker + 6:]
                name = after.split(b";")[0].strip().strip(b'"').decode("latin-1")
        if name is None or is_file or name not in wanted:
            continue
        if payload.endswith(b"\r\n"):
            payload = payload[:-2]
        if len(payload) > 4096:
            continue
        try:
            found[name] = payload.decode("utf-8").strip()
        except UnicodeDecodeError:
            continue
        if len(found) == len(wanted):
            break
    return found


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def _estimate_unmetered(surface: Any, result: Any, facts: Any) -> None:
    """Fill a never-metered stream's usage from the request side (Phase D).

    OpenAI's binary TTS carries no usage anywhere -- not in the body, not in
    a header -- so the pump's `Usage` is all zeros when the stream ends. A
    surface that knows what the request asked for (`usage_estimate`: the
    characters sent to speak) supplies the floor; exactness stays False, so
    accounting bills it `estimated`, which is honest, rather than zero,
    which is a lie. Only zero fields are filled: a provider's own count
    always wins. Never raises.
    """
    estimate = getattr(surface, "usage_estimate", None)
    if estimate is None or facts is None:
        return
    pump = getattr(result, "pump", None)
    usage = getattr(pump, "usage", None)
    if usage is None:
        return
    metered = any(
        getattr(usage, name, 0)
        for name in ("input_tokens", "output_tokens", "characters", "seconds",
                     "audio_input_tokens", "audio_output_tokens")
    )
    if metered:
        return
    try:
        est = estimate(facts)
    except Exception:  # noqa: BLE001 - billing never breaks serving
        return
    for name in ("characters", "seconds", "input_tokens", "output_tokens",
                 "audio_output_tokens"):
        value = getattr(est, name, 0)
        if value and not getattr(usage, name, 0):
            setattr(usage, name, value)


def _surface_cost_notes(surface: Any, facts: Any, result: Any) -> tuple[str, ...]:
    """A surface's own explanation of why its bill reads the way it does.

    `accounting` can only note what the PRICER noticed -- a kind that fell
    back to another kind's rate. It cannot know that a provider reports no
    meter at all, because that fact lives in the dialect, not in the
    arithmetic. Sarvam is the case that forced the hook: four HTTP routes
    that bill per character and per second and report neither, so every one
    of their records needs a line saying where the number came from.
    Without it the record is an `estimated` basis with no reason attached,
    which is the thing an invoice dispute cannot use.

    Never raises, and returns nothing for the surfaces that have no hook --
    the notes are a description of the bill, and a bug in the description
    must not become a bug in the response.
    """
    hook = getattr(surface, "cost_notes", None)
    if hook is None:
        return ()
    usage = getattr(getattr(result, "pump", None), "usage", None)
    try:
        return tuple(str(note) for note in (hook(facts, usage) or ()))
    except Exception:  # noqa: BLE001 - billing never breaks serving
        return ()


def facts_for_body(
    surface: Surface, body: bytes, scope: Scope, *, content_type: str | None
) -> RequestFacts:
    """`RequestFacts` for any body kind (PLAN-2 B4).

    `json` asks the surface, exactly as before. `multipart` scans the leading
    form fields for `model` and `stream` (`scan_multipart_fields`) -- unless
    the surface declares a `model_header`, in which case its model is not in
    the body at all and is read from the query string; `raw`
    reads `?model=` or `X-Gw-Model` and `?stream=`. Both non-JSON kinds fail
    closed with `InvalidRequest` when no model is named, because a request
    the gateway cannot route is not one it should forward and let the
    provider bill.

    A non-JSON surface may then REFINE what this function worked out, via an
    optional `facts_from_body(facts, body, content_type)`. Routing is settled
    before the hook runs and the hook cannot change it: its only job is to
    add request-side billing detail that neither the form fields nor the
    query string carry -- Sarvam's speech-to-text reading the duration out of
    the uploaded WAV's own header, because Sarvam bills per second and
    reports none. A hook that raises is ignored and the unrefined facts
    stand; an estimate is never worth a 500.
    """
    kind = getattr(surface, "body", "json")
    if kind == "json":
        return surface.parse_request(body)
    return _refine_facts(surface, _routing_facts(surface, body, scope,
                                                 content_type=content_type),
                         body, content_type)


def _refine_facts(
    surface: Any, facts: RequestFacts, body: bytes, content_type: str | None
) -> RequestFacts:
    hook = getattr(surface, "facts_from_body", None)
    if hook is None:
        return facts
    try:
        refined = hook(facts, body, content_type)
    except Exception:  # noqa: BLE001 - an estimate never breaks a request
        return facts
    if not isinstance(refined, RequestFacts) or refined.model != facts.model:
        # A hook that changed the routing key is a hook with a bug, and the
        # bug must not become a request routed somewhere the scan did not
        # say. Keep what the scan decided.
        return facts
    return refined


def _routing_facts(
    surface: Surface, body: bytes, scope: Scope, *, content_type: str | None
) -> RequestFacts:
    """The model and stream flag for a non-JSON body: the scan, and nothing
    else. Split out of `facts_for_body` so the refinement hook above wraps
    exactly the routing decision and cannot be confused with it."""
    kind = getattr(surface, "body", "json")
    fixed = getattr(surface, "fixed_model", None)
    if fixed:
        # A surface with one target and no model in the request (a token
        # mint over GET): the catalog id is the surface's, not the client's.
        return RequestFacts(model=str(fixed), stream=False)
    if kind == "multipart":
        boundary = multipart_boundary(content_type)
        if boundary is None:
            raise errors.InvalidRequest(
                f"{surface.name} expects a multipart/form-data body with a boundary; "
                f"got content-type {content_type!r}"
            )
        if getattr(surface, "model_header", None):
            # The model is a ROUTING HEADER upstream (`assemblyai_sync`'s
            # `X-AAI-Model`), so the client's multipart body has no model
            # field to scan for and adding one would be a part the provider
            # never asked for. Read it where a `raw` body reads it.
            return _facts_from_query(surface, scope)
        # The dialect's own spelling, the same key `apply_api_model_multipart`
        # splices on the way out: OpenAI's transcription route calls it
        # `model`, ElevenLabs' Scribe route calls it `model_id`. Scanning for
        # `model` on the latter fails closed on every correct request.
        key = getattr(surface, "model_key", "model") or "model"
        fields = scan_multipart_fields(body, boundary, wanted=frozenset({key, "stream"}))
        model = fields.get(key)
        if not model:
            raise errors.InvalidRequest(
                f"no `{key}` form field in the first {MULTIPART_SCAN_BYTES} bytes of "
                f"the multipart body; put the text fields before the file part"
            )
        return RequestFacts(model=model, stream=_truthy(fields.get("stream")))
    if kind == "raw":
        return _facts_from_query(surface, scope)
    raise errors.PolicyError(f"surface {surface.name!r} declares unknown body kind {kind!r}")


def _facts_from_query(surface: Surface, scope: Scope) -> RequestFacts:
    """`?model=` or `X-Gw-Model`, for a body the gateway must not read: a raw
    audio body, or a multipart one whose model travels in a header."""
    model = (query_param(scope, MODEL_QUERY_PARAM)
             or header_value(scope, MODEL_REQUEST_HEADER))
    if not model:
        raise errors.InvalidRequest(
            f"{surface.name} does not read its model from the body; name it with "
            f"?model=<id> or the X-Gw-Model header"
        )
    return RequestFacts(model=model, stream=_truthy(query_param(scope, b"stream")))


class ModelsEndpoint:
    """`GET /v1/models` and `/anthropic/v1/models`, from the catalog (Phase C1).

    Never opens an upstream connection (CONTRACTS C18). The tenant is
    resolved exactly as on the serving path, so an unknown token is a 401
    and the listing is not an unauthenticated view of the catalog; admission
    is not consulted, because the answer costs a dictionary walk. Counted
    under `llmgw_requests_total{surface="models"}` once the metrics
    vocabulary knows the name.
    """

    __slots__ = ("_gateway", "_surface", "_route", "_metric_surface")

    def __init__(self, gateway: Gateway, *, surface: Any, route: str) -> None:
        self._gateway = gateway
        self._surface = surface
        self._route = route
        self._metric_surface: str | None = (
            surface.name if surface.name in METRIC_SURFACES else None
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        gw = self._gateway
        started = gw.clock.now()
        snapshot = gw.policy.current()
        derived = gw.derived(snapshot)
        exchange = Exchange(
            snapshot, catalog_id=derived.catalog_id, workload_id=snapshot.default_workload,
        )
        outcome, code = "completed", "none"
        try:
            try:
                exchange.tenant = gw.resolve_tenant(scope)
            except Unauthenticated as exc:
                outcome, code = "failed", "unauthenticated"
                await send_json_error(
                    send, status=401, code="unauthenticated", message=str(exc),
                    exchange=exchange, extra_headers=[(b"www-authenticate", b"Bearer")],
                )
                return
            listing = self._surface.listing(
                gw.config.catalog, route=self._route,
                include_fakes=bool(gw.config.fake_upstreams),
            )
            payload = json.dumps(listing, separators=(",", ":")).encode("utf-8")
            headers = [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(payload)).encode("ascii")),
                *exchange.gw_headers(),
            ]
            await send({"type": "http.response.start", "status": 200, "headers": headers})
            await send({"type": "http.response.body", "body": payload, "more_body": False})
        finally:
            collectors = gw.collectors
            if collectors is not None and self._metric_surface is not None:
                try:
                    collectors.request(
                        surface=self._metric_surface, outcome=outcome,
                        code=code if code in ("none",) else "none",
                    )
                    collectors.request_duration(
                        surface=self._metric_surface, outcome=outcome,
                        seconds=max(0.0, gw.clock.now() - started),
                    )
                except ValueError:  # a label outside the closed vocabulary
                    pass


class PassthroughEndpoint:
    """One surface, one target, byte-for-byte. A raw ASGI app.

    An instance rather than a function so Starlette's `Route` hands us the
    scope, receive and send untouched instead of wrapping us in
    `request_response()`. See the module docstring for why that control is not
    optional here.
    """

    __slots__ = ("_gateway", "_surface", "_route", "_forward", "_metric_surface")

    def __init__(self, gateway: Gateway, *, surface: Surface, route: str) -> None:
        self._gateway = gateway
        self._surface = surface
        self._route = route
        self._metric_surface: str | None = (
            surface.name if surface.name in METRIC_SURFACES else None
        )
        # Lower-cased and pre-encoded once at build time. ASGI hands us header
        # names as bytes, and re-decoding every one of them per request to
        # compare against a set of str is work done on the hot path to make a
        # comparison look nicer.
        self._forward = frozenset(
            h.lower().encode("latin-1") for h in gateway.config.forward_request_headers
        )

    @staticmethod
    def _requested_workload(scope: Scope) -> str | None:
        """The workload this request named, or None if it named none.

        The path wins a disagreement with the header. Both are the caller's
        input, but the path is the part a router, an access log and an
        authorisation rule can all see, and a request whose URL says
        `/workloads/summarize/...` must not quietly run as something else
        because a header said so three hops ago.
        """
        params = scope.get("path_params") or {}
        from_path = params.get("workload")
        if from_path:
            return str(from_path)
        for name, value in scope.get("headers", ()):
            if bytes(name).lower() == WORKLOAD_HEADER:
                text = bytes(value).decode("latin-1").strip()
                return text or None
        return None

    @staticmethod
    def _no_retry(scope: Scope) -> bool:
        """C5: has the caller claimed the retry layer for itself?"""
        for name, value in scope.get("headers", ()):
            if bytes(name).lower() == NO_RETRY_HEADER:
                return bytes(value).decode("latin-1").strip().lower() in {
                    "1", "true", "yes", "on"
                }
        return False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        gw = self._gateway
        config = gw.config

        # ONE snapshot, taken before anything else can observe policy, and
        # threaded through routing, budgets, the attempt loop and the response
        # headers. FAILURE-MODES row 10 is not "re-read config carefully"; it
        # is "there is no second read".
        snapshot = gw.policy.current()
        derived = gw.derived(snapshot)
        requested = self._requested_workload(scope)
        exchange = Exchange(
            snapshot,
            catalog_id=derived.catalog_id,
            # Provisional: if the name is unknown the 400 below should still
            # say which workload the caller asked for, not the default it
            # never mentioned.
            workload_id=requested or snapshot.default_workload,
        )
        # `llmgw_streams_open` is inc'd here and dec'd in the finally below, so
        # the pair spans EVERY exit -- a 401 before admission, a 429, a 503
        # while draining, a clean stream, a client disconnect. Pairing it at
        # this level (rather than at the terminal record, which never runs for
        # a pre-admission refusal) is what makes the gauge's return-to-zero a
        # property of the code's shape; `surface` is known the instant the
        # endpoint is entered. The awaitable in-flight tracker is bumped in the
        # SAME two places (`stream_entered`/`stream_exited`), so the thing a
        # drain waits on and the thing a scrape reads can never disagree.
        collectors = gw.collectors
        # The metrics label. A registry name the metrics vocabulary does not
        # know yet (Phase C's surfaces until `metrics.SURFACES` widens) is
        # not emitted rather than raised: label cardinality is closed, but a
        # closed set must not make a route unservable.
        surface = self._metric_surface
        gw.stream_entered()
        if collectors is not None and surface is not None:
            collectors.stream_open(surface=surface)
        try:
            # P6: shed NEW work while draining, as the first thing in the
            # order -- before the tenant is even resolved. A process that has
            # flipped `draining` has decided to stop taking requests, so the
            # cheapest honest refusal is the one that reads no request state at
            # all, and this is the request that raced the /healthz 503 the load
            # balancer has already seen. 503 with reason "draining" (recorded
            # like every other denial), NOT a 429: the client should retry
            # against another replica, not back off against this one. The
            # `stream_entered` above is already paired by the `finally` below,
            # so this early return decrements the tracker cleanly and a shed
            # request can never pin the drain it is being shed to protect.
            if gw.draining:
                gw.note_draining_denied()
                await send_json_error(
                    send, status=503, code="draining",
                    message="gateway is draining and is not accepting new requests",
                    exchange=exchange,
                )
                return
            # The order of the next four steps is the P4 contract, cheapest
            # first, and each one is a refusal that costs the client less
            # than the step after it would have:
            #
            #   1. tenant     one header, one dict lookup      -> 401
            #   2. workload   one dict lookup                  -> 400
            #   3. admission  one int compare, maybe a clock   -> 429
            #   4. the body   up to max_request_bytes of RAM   -> 413
            #
            # Admission BEFORE the body is the load-bearing line. A tenant at
            # its concurrency cap is refused before we allocate its request,
            # which is what makes "denial is free" (C6) true of memory and
            # not just of tokens. The workload is resolved before admission
            # because the deadline that bounds the body read is the
            # workload's, and starting that clock before the request is
            # admitted would charge admission to the request.
            exchange.tenant = tenant = gw.resolve_tenant(scope)
            # The per-process cap, between the tenant and its bucket. After
            # the tenant, so the refusal carries `X-Gw-Tenant` and an unknown
            # token is still a 401 rather than a 503 that leaks nothing;
            # BEFORE admission, so a request refused for the process's sake
            # costs the tenant no credit (C6) and, being before the body, no
            # memory. 503 like `draining`, not 429: the tenant is under its
            # limits and the right move is another replica, which is what
            # `Retry-After: 1` says to a client behind a balancer. See
            # `ServerConfig.max_streams` for the S2 numbers behind this line.
            if gw.over_capacity():
                gw.note_overloaded_denied()
                await send_json_error(
                    send, status=503, code="overloaded",
                    message=(
                        f"gateway is at its per-process stream cap "
                        f"({config.max_streams}); retry against another replica"
                    ),
                    exchange=exchange,
                    extra_headers=[(b"retry-after", b"1")],
                )
                return
            workload_id = gw.resolve_workload(snapshot, requested)
            exchange.workload_id = workload_id
            budgets = snapshot.workloads[workload_id].budgets

            # `admit()` raises `ConcurrencyRejected` (429, no Retry-After:
            # there is no honest number) or `AdmissionRejected` (429 with the
            # exact refill time); both are `GatewayError`s and reach
            # `send_error` below with `X-Gw-Tenant` already set. The permit is
            # held for EVERYTHING after this line -- body read, plan, every
            # attempt, the last body byte -- and `__aexit__` releases it on
            # return, on any exception, and on the cancellation a client
            # disconnect turns into. There is no path out of this block that
            # keeps the permit, which is FAILURE-MODES row 19 handled by shape.
            async with gw.admission.admit(tenant):
                # ONE deadline, created here, from which every wait below
                # derives. Not one per phase and not one per attempt: see
                # Deadline.slice(). Its total is the WORKLOAD's, which is why
                # the workload has to be resolved before the body is read --
                # an interactive workload with a 20 s total must not spend a
                # 600 s default reading a body.
                deadline = Deadline.start(budgets.total, clock=gw.clock)
                # Per-surface caps (B4): the Anthropic messages surface takes
                # 32 MiB because the provider does; chat keeps 4 MiB. The
                # byte-bounded read itself is unchanged.
                limits = config.limits_for(self._surface.name)
                body = await read_request_body(
                    receive, limit=limits.max_request_bytes, deadline=deadline
                )
                # Read-only. For a JSON body `parse_request` answers "which
                # model, streaming?" and is structurally incapable of
                # rebuilding a request; a multipart or raw body is scanned or
                # read from the query string instead (`facts_for_body`). The
                # only edits any layer makes to JSON bytes are `upstream.py`'s
                # and every one announces itself as `X-Gw-Body-Modified`; a
                # non-JSON body is never edited at all. See surfaces/base.
                body_kind = getattr(self._surface, "body", "json")
                client_content_type = header_value(scope, b"content-type")
                # A surface whose FRAMING depends on the request (OpenAI TTS:
                # `stream_format: "sse"` or binary) answers `surface_for(body)`
                # with the instance that reads that framing; everything else
                # returns None and the registered instance serves. Chosen once,
                # before the facts, and threaded through `_serve` so the pump,
                # the sink and accounting all see the same dialect.
                served_surface = self._surface
                picker = getattr(served_surface, "surface_for", None)
                if picker is not None:
                    served_surface = picker(body) or served_surface
                facts = facts_for_body(
                    served_surface, body, scope, content_type=client_content_type
                )
                exchange.request_facts = facts
                # Phase E: a tenant's pinned Realtime model wins over the
                # client's on a mint (C19); read once, before the plan.
                pin = None
                if getattr(self._surface, "mint_route", None) == self._route:
                    pin = gw.realtime_pin(tenant)
                pinned_model = (pin or {}).get("model") if pin else None
                # The body's model pins the target ONLY when the caller named
                # no workload; see the module docstring for why the other way
                # round makes every candidate unreachable.
                plan = snapshot.plan_for(
                    workload_id,
                    model=(str(pinned_model) if pinned_model
                           else (None if requested is not None else facts.model)),
                    # The route's dialect, so a wire id shared across dialects
                    # (an alias, PLAN-2 A1) resolves to this surface's target.
                    kind=surface_dialect(self._surface),
                )
                # Phase C: the upstream path for THIS route -- templates
                # filled from the path params, the query forwarded when the
                # surface says so, credential-looking keys refused always.
                upstream_path, query, ttl = self._upstream_path(scope)
                extra = dict(forwarded_request_headers(scope, self._forward))
                prepare = getattr(self._surface, "prepare_mint", None)
                if prepare is not None:
                    mint = prepare(
                        body, route=self._route, target=plan.primary, tenant=tenant,
                        pin=pin, grace_s=config.drain_grace_seconds,
                    )
                    if mint is not None:
                        body = mint.body
                        extra.update(mint.extra_headers)
                        ttl = mint.ttl_s
                        exchange.pinned_keys = mint.pinned_keys
                if ttl is not None:
                    # A credential is about to be issued: count it against the
                    # tenant's session cap for as long as it lives (E1/E2).
                    gw.admission.reserve_session(tenant, ttl)
                    exchange.mint = True
                if query:
                    upstream_path = f"{upstream_path}?{query}"
                # `exchange.target` is deliberately NOT pre-set to
                # `plan.primary` here. `X-Gw-Served-By` means "the target that
                # answered", and on a walk where nobody did, naming the first
                # one we tried is a misattribution the header's own `-`
                # branch exists to avoid: with a fallback the error the client
                # is holding came from the SECOND target while `plan.primary`
                # names the first. The field is filled in at commitment
                # (`start_response`) and again from `result.served_by`, which
                # are the two moments a target has actually served something.
                await self._serve(
                    receive, send, exchange,
                    deadline=deadline, plan=plan, body=body, streaming=facts.stream,
                    extra_headers=extra,
                    surface=served_surface,
                    upstream_path=upstream_path,
                    method=str(scope.get("method") or "POST").upper(),
                    retry_policy=_retry_policy_for(
                        plan, derived.retry.get(workload_id),
                        no_retry=self._no_retry(scope),
                    ),
                    body_kind=body_kind,
                    content_type=client_content_type if body_kind != "json" else None,
                    max_response_bytes=limits.max_response_bytes,
                    max_frame_bytes=limits.max_frame_bytes,
                )
        except asyncio.CancelledError:
            # ------------------------- SHUTDOWN CUT ------------------------
            # Exactly one thing cancels THIS task from outside: uvicorn's
            # shutdown, `UVICORN_SHUTDOWN_TIMEOUT_S` after our own drain grace
            # has already run out (lifecycle.py). A client disconnect cancels
            # the WORKER inside `run_until_disconnect` and surfaces here as
            # `ClientDisconnected`, never as a cancel of this task; the
            # deadline paths raise their own `GatewayError`s. So a cancel that
            # arrives while `gw.draining` is the shutdown cut. One that
            # arrives while NOT draining is somebody else's -- a harness
            # tearing down, a future in-process caller -- and keeps its
            # asyncio meaning: re-raised untouched. `draining` is the only
            # discriminator available; uvicorn's cancel message is a private
            # string and is not read.
            if not gw.draining:
                raise
            # Letting the CancelledError out made uvicorn log it as
            # `ERROR: Exception in ASGI application` with a ~4 KB traceback,
            # once PER STREAM: hundreds of tracebacks per deploy, and where
            # stderr is a pipe nobody drains, a process blocked inside its
            # logging handler that never exits at all (S8-B, 15 Sep 2026).
            # Absorbing it is safe here and only here. The task was told to
            # stop and has stopped: every `finally` between the cut and this
            # line has already run -- the worker cancelled and awaited, the
            # permit released, the terminal record written with
            # outcome=CANCELED and `cost_basis=estimated` (C3), the tracker
            # paired in the `finally` below -- and uvicorn owns the task and
            # closes the transport whether we raise or return. `uncancel()`
            # tells asyncio the request has been handled, so nothing above us
            # sees a phantom pending cancel.
            task = asyncio.current_task()
            if task is not None:
                task.uncancel()
            # NO log line here, by rule. This path fires once per open stream
            # in the same instant, and every byte it writes to stderr is a
            # byte an undrained 64 KiB pipe must absorb before the process can
            # exit: 4 KB tracebacks hung the S8-B workers outright, and the
            # 88-byte WARNING that replaced them was still ~450 streams from
            # doing the same. So the fact is COUNTED (`ShutdownCuts`, bounded
            # by the catalog) and stderr gets exactly one summary line from
            # `lifecycle.log_shutdown_cuts` after uvicorn has stopped. The
            # per-request detail -- tokens so far, cost, duration -- is in
            # the capture record this request already wrote with
            # outcome=CANCELED.
            gw.shutdown_cuts.note(target=exchange.target, committed=exchange.started)
            if not exchange.started:
                # No status on the wire yet, so there is still an honest one
                # to send: the same 503 the ingress shed gives a request that
                # arrived after `draining` flipped. Best-effort -- the client
                # may already be gone, and a raise here would put the
                # traceback back.
                with suppress(Exception):
                    await send_json_error(
                        send, status=503, code="draining",
                        message="gateway shut down before this request was served",
                        exchange=exchange,
                        extra_headers=[(b"retry-after", b"1")],
                    )
            # After commitment: return, do not complete. No `more_body:
            # False` means no chunked terminator -- the C2 ending, the same
            # one the post-commitment `GatewayError` branch below uses.
            return
        except Unauthenticated as exc:
            await send_json_error(
                send, status=401, code="unauthenticated", message=str(exc),
                exchange=exchange,
                extra_headers=[(b"www-authenticate", b"Bearer")],
            )
        except RequestTooLarge as exc:
            await send_json_error(
                send, status=413, code="request_too_large", message=str(exc),
                exchange=exchange,
            )
        except errors.GatewayError as err:
            if exchange.started:
                # ---------------- POST-STATUS-COMMITMENT -------------------
                # The status is on the wire and cannot be replaced, so there
                # is exactly one honest ending: stop. Returning without ever
                # sending `more_body: False` makes uvicorn close the transport
                # without a chunked terminator, which is a truncated body --
                # no `data: [DONE]`, no `message_stop`, no synthesised error
                # frame (C2). The client's own SDK raises its own vendor's
                # truncated-stream error, which is a code path its users have
                # already written an `except` for.
                # -----------------------------------------------------------
                log.warning(
                    "truncating %s after commitment: %s", self._route, err.code
                )
                return
            provider_row = gw.config.catalog.providers.get(err.provider or "")
            await send_error(
                send, err, exchange=exchange,
                scrub_all=getattr(provider_row, "scrub_error_bodies", "auth") == "all",
            )
        finally:
            # The twin of the entry pair, on every exit path. `stream_exited`
            # first so the drain-awaitable count and the gauge fall together;
            # both are unconditional so neither can leak a stream that a
            # refusal, a disconnect or a raise exited through here.
            gw.stream_exited()
            if collectors is not None:
                if surface is not None:
                    collectors.stream_close(surface=surface)

    # ------------------------------------------------------------------ serve

    _PARAM_OK = re.compile(r"^[A-Za-z0-9_.:@+-]{1,128}$")

    def _upstream_path(self, scope: Scope) -> tuple[str, str, float | None]:
        """(upstream path, forwarded query, session TTL) for this request.

        The template is the surface's `upstream_path` (or its per-route
        choice via `upstream_path_for`), with Starlette's path params
        substituted after a character-class check -- a param is a path
        segment and may not smuggle a slash, a space or a query. The query
        string is forwarded only when the surface opts in, never with a
        credential-looking key, and a mint surface may rewrite it
        (`prepare_query`) and name the session length it authorised.
        """
        params = {k: str(v) for k, v in (scope.get("path_params") or {}).items()
                  if k != "workload"}
        chooser = getattr(self._surface, "upstream_path_for", None)
        template = (chooser(self._route) if callable(chooser)
                    else surface_upstream_path(self._surface))
        validate = getattr(self._surface, "validate_params", None)
        if callable(validate):
            validate(params)
        for name, value in params.items():
            if not self._PARAM_OK.match(value):
                raise errors.InvalidRequest(f"path parameter {name!r} has an unexpected value")
        try:
            path = template.format(**params)
        except (KeyError, IndexError) as exc:
            raise errors.PolicyError(
                f"surface {self._surface.name!r} upstream path {template!r} needs "
                f"a param the route did not carry: {exc}"
            ) from exc
        raw_query = (scope.get("query_string") or b"").decode("latin-1")
        query = ""
        ttl: float | None = None
        if raw_query:
            from urllib.parse import parse_qsl

            keys = {k.lower() for k, _ in parse_qsl(raw_query, keep_blank_values=True)}
            leaked = sorted(keys & CREDENTIAL_QUERY_KEYS)
            if leaked:
                raise errors.InvalidRequest(
                    f"query parameter(s) {leaked} look like credentials and are not "
                    f"forwarded; the gateway holds the provider key"
                )
            if surface_forward_query(self._surface):
                query = raw_query
        if surface_forward_query(self._surface):
            prepare_query = getattr(self._surface, "prepare_query", None)
            if callable(prepare_query):
                query, ttl = prepare_query(
                    query, grace_s=self._gateway.config.drain_grace_seconds
                )
        return path, query, ttl

    async def _serve(
        self,
        receive: Receive,
        send: Send,
        exchange: Exchange,
        *,
        deadline: Deadline,
        plan: ExecutionPlan,
        body: bytes,
        streaming: bool,
        extra_headers: Mapping[str, str],
        retry_policy: RetryPolicy | None,
        body_kind: str = "json",
        content_type: str | None = None,
        max_response_bytes: int | None = None,
        max_frame_bytes: int | None = None,
        upstream_path: str | None = None,
        method: str = "POST",
        surface: Surface | None = None,
    ) -> None:
        """Run the plan, and start the client's response when a byte arrives.

        Every reliability decision below this line belongs to `executor.py`:
        which target is next, whether a repetition can pay off, what a failure
        after commitment means. This method supplies the three things the
        executor cannot know -- how to start an HTTP response, how to count
        the attempts a status line has to carry, and when the client hung up
        -- and then gets out of the way.

        The closure is the whole point. `start_response` is the `SinkFactory`,
        it is awaited exactly once, and awaiting it IS the status commitment
        (CONTRACTS.md C1, P3 decision). Before that await the plan is open and
        anything that fails falls back; after it there is no plan, because
        HTTP has no second status.
        """
        # The dialect serving THIS request: the registered surface, or the
        # per-request instance `__call__` picked for a framing the body chose.
        served = surface or self._surface
        gw = self._gateway
        config = gw.config
        tally = _AttemptTally(gw.upstream)

        async def start_response(stream: UpstreamStream) -> Sink:
            # ==================== STATUS COMMITMENT =======================
            # The upstream has produced a byte we are about to forward, so the
            # upstream's status is now the client's status and nothing can
            # replace it. The attempt facts are read off the tally HERE, at
            # the only instant they are both known and still spendable: the
            # winning attempt is the one being served, so `tally.opens` is the
            # final `len(result.attempts)` and `tally.target` is
            # `result.served_by`.
            # ==============================================================
            exchange.attempts = tally.opens
            exchange.target = tally.target
            exchange.defaulted_keys = tuple(getattr(stream, "defaulted_keys", ()))
            # The provider's request id, processing time and rate-limit
            # budget, read here because this is the response being served.
            exchange.observe_upstream(stream.headers)
            if not streaming:
                # The buffered path sends its status from inside the sink,
                # one write later, because that is where the length is known.
                return BufferedSink(send, stream=stream, exchange=exchange,
                                    surface=served)
            headers = response_headers(stream, exchange=exchange, streaming=True)
            # Set BEFORE the await, for the same reason `pump.py` sets its own
            # commitment flag early: a send that raises may still have put the
            # status line on the wire, and a flag set afterwards would report
            # that as recoverable.
            exchange.started = True
            await send({"type": "http.response.start", "status": stream.status,
                        "headers": headers})
            return ASGISink(send)

        result = await run_until_disconnect(
            Executor(
                tally,  # type: ignore[arg-type]
                clock=gw.clock,
                # The process-wide registry, watched per request so a refusal
                # can reach the status line as `X-Gw-Breaker`; the limiter
                # needs no watching because a key refusal has no header.
                breakers=_GateWatch(gw.breakers, exchange),  # type: ignore[arg-type]
                limiter=gw.limiter,
            ).execute(
                plan=plan,
                surface=served,
                body=body,
                path=upstream_path or surface_upstream_path(self._surface),
                method=method,
                stream=streaming,
                deadline=deadline,
                sink_factory=start_response,
                retry_policy=retry_policy,
                extra_headers=extra_headers,
                body_kind=body_kind,
                content_type=content_type,
                max_response_bytes=(
                    config.max_response_bytes if max_response_bytes is None
                    else max_response_bytes
                ),
                # Configured, not defaulted. These are the per-stream memory
                # the scale tier multiplies by N, and the only bound between
                # an oversized provider frame and this process's RSS.
                buffer_bytes=config.buffer_bytes,
                # Per-surface since the image surface landed: one
                # `image_generation.partial_image` frame is a whole base64
                # PNG, 1.6x the global bound. `limits_for()` already resolved
                # the global for every surface that sets no number of its own,
                # and the None here is for a caller that passed no limits at
                # all (the tests that drive `_serve` directly).
                max_frame_bytes=(
                    config.max_frame_bytes if max_frame_bytes is None
                    else max_frame_bytes
                ),
                # What `parse_request` read, for the surface's per-target
                # refusal (`Surface.check_target`, Phase F).
                request_facts=exchange.request_facts,
                # The accounting hook rides INSIDE the executor rather than
                # being called on `result` below, because the line below is
                # never reached on a client disconnect: `run_until_disconnect`
                # cancels the worker and raises. The executor's `finally`
                # calls the hook in the worker task, with the cancelled
                # execution's partial result, before the cancellation is
                # allowed to leave -- which is the only place the C3 facts
                # (partial usage, what was attempted) are still in scope.
                #
                # A per-request closure rather than the bare method: `_record`
                # needs the `Exchange` (for the tenant a capture line carries)
                # and the request's elapsed time (for the duration histogram),
                # and `deadline.elapsed()` read at the instant the hook fires is
                # exactly ingress-to-terminal, retries and waits included. The
                # closure captures values, holds no per-endpoint state, and
                # `_finish` in the executor still wraps it so a raise here is
                # logged, never fatal.
                on_finish=lambda result: self._record(
                    result, exchange=exchange, duration_s=deadline.elapsed()
                ),
            ),
            receive,
        )

        # `execute()` returns its failures rather than raising them, so that
        # the attempt records and the partial usage survive the failure path
        # (C3). Both are read off the result before it is turned back into an
        # exception for the handler above. `attempts`, not `attempts +
        # refusals`: a target the gate turned away was never asked, and the
        # header counts what the providers were asked.
        exchange.attempts = len(result.attempts)
        if result.served_by is not None:
            exchange.target = result.served_by
        if result.error is not None:
            # C4 lives in the handler: pre-commitment this becomes the
            # provider's own status and body; post-commitment it becomes a
            # body that simply stops.
            raise result.error
        if streaming:
            await send({"type": "http.response.body", "body": b"",
                        "more_body": False})

    def _record(
        self,
        result: ExecutionResult,
        *,
        exchange: Exchange,
        duration_s: float,
    ) -> None:
        """Where P5 writes the terminal record.

        The accounting hook, at the point where the whole `ExecutionResult` is
        in scope -- `attempts` for the amplification metric, `pump` for the
        usage C3 says an interrupted stream still owes, `outcome` for the
        2xx-that-was-not-a-success, and `plan.policy_id` for the snapshot the
        request was actually routed by.

        Reached exactly once per request that entered the executor, on every
        exit INCLUDING a client disconnect -- it is `Executor.execute()`'s
        `on_finish`, so it runs from the executor's `finally` in the worker
        task, with `outcome=CANCELED` and the partial usage, while the
        `CancelledError` is still on its way out (C3 and C8 both hold).
        Consequences for the body: it is synchronous, it runs under a
        cancellation that has not finished propagating, and it MUST NOT raise --
        so the whole body is wrapped, and `capture.offer` is non-blocking by
        contract. `_finish` in the executor would log a raise anyway; the wrap
        here is the belt to that braces, because a metrics bug on the cancel
        path must not become a request-ending one.

        Two halves, in order:

        * **Metrics.** `accounting.account()` is pure, total, and never raises;
          it already resolves the C3 billing nuance (a post-commit failure
          bills the last committed target), so this half only fans its record
          out onto the counters. `llmgw_requests_total` is THE exactly-once
          metric and is incremented once here, unconditionally.
        * **Capture.** One `CaptureRecord` -- the high-cardinality per-request
          facts a metric label is forbidden from carrying (tenant, workload,
          error code, the token/cost detail) -- offered to the process sink.
          `offer()` is synchronous, never blocks, never raises, and drops
          rather than waits when the queue is full (FAILURE-MODES row 9).
        """
        gw = self._gateway
        collectors = gw.collectors
        surface = self._metric_surface
        try:
            _estimate_unmetered(self._surface, result, exchange.request_facts)
            rec = accounting.account(result, catalog=gw.config.catalog)
            surface_notes = _surface_cost_notes(
                self._surface, exchange.request_facts, result
            )
            if surface_notes:
                rec = dataclasses.replace(
                    rec, cost_notes=(*surface_notes, *rec.cost_notes)
                )
            provider, model = rec.provider, rec.model
            outcome = rec.outcome.value
            stop_reason = rec.stop_reason
            if stop_reason is None:
                stop_reason = normalize_stop_reason(exchange.buffered_stop_reason)

            # Time-to-first-event, when a content byte actually arrived: the
            # winning attempt's start to the pump's first-content instant.
            # `served_by`/`pump` may both be absent (nobody answered); guarded.
            ttfe: float | None = None
            pump = result.pump
            if (pump is not None and pump.first_event_at is not None
                    and result.attempts):
                ttfe = pump.first_event_at - result.attempts[-1].started_at
                if ttfe < 0:  # a clock the executor and pump did not share
                    ttfe = None

            if collectors is not None and surface is not None:
                collectors.request(surface=surface, outcome=outcome, code=rec.code)
                collectors.request_duration(
                    surface=surface, outcome=outcome, seconds=duration_s
                )
                if rec.committed:
                    collectors.committed(surface=surface)
                if rec.parse_failures > 0:
                    collectors.usage_parse_failures(
                        surface=surface, n=rec.parse_failures
                    )
                # Amplification: one increment per attempt, mapped onto the
                # closed ATTEMPT_RESULTS vocabulary. Every attempt carries its
                # own target, so this holds even on the nobody-answered path.
                for attempt in result.attempts:
                    collectors.attempt(
                        provider=attempt.target.provider.id,
                        model=attempt.target.model.id,
                        result=Collectors.attempt_result(attempt.outcome),
                    )
                # Tokens, cost and TTFE are billed against ONE target, so they
                # are silent when nobody delivered bytes (provider/model None).
                if provider is not None and model is not None:
                    for kind, n in rec.tokens_by_kind.items():
                        collectors.tokens(
                            provider=provider, model=model, kind=kind, n=n
                        )
                    # Non-token units (characters, seconds, audio tokens) and
                    # per-call server tools (PLAN-2 B3), when the record and
                    # the collectors know about them.
                    units_fn = getattr(collectors, "units", None)
                    if units_fn is not None:
                        for unit, n in (getattr(rec, "units_by_kind", None) or {}).items():
                            units_fn(provider=provider, model=model, unit=unit, n=n)
                    tools_fn = getattr(collectors, "server_tool_calls", None)
                    if tools_fn is not None:
                        for tool, n in (getattr(rec, "server_tool_calls", None) or {}).items():
                            tools_fn(provider=provider, model=model, tool=tool, n=n)
                    collectors.cost(
                        provider=provider, model=model,
                        basis=rec.basis, usd=rec.cost_usd,
                    )
                    if ttfe is not None:
                        collectors.time_to_first_event(
                            provider=provider, model=model, seconds=ttfe
                        )
                # Why the provider stopped (PLAN-2 A3). Only when it said:
                # through `Usage` on the streaming path, through the body
                # reader on the buffered one.
                if stop_reason is not None:
                    if surface is not None:
                        collectors.stop_reason(surface=surface, stop_reason=stop_reason)
                # Timeouts that fired while the provider was sending liveness
                # and no content: a queue, not an outage (A4). The pump stamps
                # `queued` on the clock error it raised -- a `StallTimeout`
                # in practice, since the first keep-alive satisfies the
                # first-chunk budget -- so read the attribute, not the class.
                # Counted per attempt, off the attempt's own error.
                for attempt in result.attempts:
                    if getattr(attempt.error, "queued", False):
                        collectors.queued_at_provider(
                            provider=attempt.target.provider.id,
                            model=attempt.target.model.id,
                        )

            # What the upstream's headers said (A6d/e). The served response
            # was observed at commitment; a terminal error carries its own.
            if result.error is not None:
                exchange.observe_upstream(result.error.upstream_headers)
            telemetry = exchange.upstream
            if collectors is not None and telemetry is not None and telemetry.ratelimits:
                credential_target = exchange.target or (
                    result.attempts[-1].target if result.attempts else None
                )
                if credential_target is not None:
                    credential = credential_target.provider.key()
                    for kind, remaining, reset_s in telemetry.ratelimits:
                        collectors.provider_ratelimit(
                            credential=credential, kind=kind,
                            remaining=remaining, reset_seconds=reset_s,
                        )

            capture = gw.capture
            if capture is not None:
                # `defaulted_keys` (B5) rides along only once `CaptureRecord`
                # has the field; until then the exchange holds it and the
                # metrics side is unaffected.
                extra_fields: dict[str, Any] = {}
                record_fields = getattr(CaptureRecord, "__dataclass_fields__", {})
                if "defaulted_keys" in record_fields:
                    extra_fields["defaulted_keys"] = list(exchange.defaulted_keys)
                # B3's accounting detail, copied when both sides have it.
                if "units" in record_fields:
                    extra_fields["units"] = dict(getattr(rec, "units_by_kind", None) or {})
                if "server_tool_calls" in record_fields:
                    extra_fields["server_tool_calls"] = dict(
                        getattr(rec, "server_tool_calls", None) or {}
                    )
                if "cost_notes" in record_fields:
                    extra_fields["cost_notes"] = list(getattr(rec, "cost_notes", None) or ())
                capture.offer(CaptureRecord(
                    **extra_fields,
                    # No request-id concept exists in this gateway yet; the
                    # tenant and workload are the fields an investigator filters
                    # on and both are present.
                    request_id="",
                    tenant_id=exchange.tenant or "",
                    workload_id=rec.workload_id,
                    provider=provider or "",
                    model=model or "",
                    outcome=outcome,
                    attempts=rec.attempts,
                    tokens=dict(rec.tokens_by_kind),
                    cost_usd=rec.cost_usd,
                    basis=rec.basis,
                    committed=rec.committed,
                    kind="mint" if exchange.mint else "request",
                    first_event_latency=ttfe,
                    duration_s=duration_s,
                    error_code=None if rec.code == "none" else rec.code,
                    recorded_at=gw.clock.now(),
                    stop_reason=stop_reason,
                    upstream_request_id=(
                        telemetry.request_id if telemetry is not None else None
                    ),
                    upstream_processing_ms=(
                        telemetry.processing_ms if telemetry is not None else None
                    ),
                ))
        except Exception:  # noqa: BLE001 - the hook observes; it does not vote
            # On the cancel path this runs mid-cancellation, so a raise here
            # would replace a clean truncation with an accounting incident.
            # Log and swallow: a lost record beats a broken request.
            log.exception(
                "record hook failed for %s (workload=%s)",
                self._route, exchange.workload_id,
            )


# ==========================================================================
# Error responses
# ==========================================================================


async def send_error(
    send: Send,
    err: errors.GatewayError,
    *,
    exchange: Exchange,
    scrub_all: bool = False,
) -> None:
    """Answer a pre-status-commitment failure. CONTRACTS.md C4.

    Once no fallback remains -- which now means the executor walked the whole
    plan and every target failed -- an error that carries `passthrough=True`
    and an upstream status reaches the client with THAT status and the
    provider's own body bytes, unmodified. We do not improve on a provider's
    error message: the caller's SDK already knows how to read its own vendor's
    error shape, and a helpfully rewritten one is a shape their error handling
    has never seen.

    The error is `ExecutionResult.error`, which `executor._final_error` has
    already chosen: the LAST error from an attempt that actually reached an
    upstream, never a summary of the walk. A client told
    `no_targets_available` when the incumbent said "429, come back in 30
    seconds" has been handed a 503 in place of an instruction. `X-Gw-Attempts`
    is how many targets it took to conclude that.

    Everything else gets `err.client_status` and a small JSON object naming
    `err.code`, which is a closed vocabulary (`errors.ERROR_CODES`) so a client
    can switch on it.

    `Retry-After` is reconstructed from `err.retry_after` rather than
    forwarded, because the upstream's response headers do not survive
    classification -- `from_http_status()` keeps the parsed seconds, the status
    and the body, and drops the rest. That is a real gap: an upstream
    `content-type` on an error body is not available here either, so a
    passthrough body is announced as `application/json`, which is true of both
    providers today and is an assumption rather than an observation.
    """
    body = err.upstream_body if (err.passthrough and err.upstream_status) else None
    if scrub_all and body:
        # PLAN-2 B2: a provider whose row says `scrub_error_bodies="all"`
        # echoes credential material into ordinary error bodies (Inworld's
        # 403 quotes the key's first four characters; OpenAI's audio 401 its
        # last four), so C11's one exception widens to every non-2xx from
        # that provider. The status still passes through; the body is ours.
        body = None
    # The provider's request id rides on the error's headers (`upstream.py`
    # keeps them since PLAN-2 A6d); it is the one thing a caller can quote to
    # the provider about a failure, and `gw_headers()` emits it.
    exchange.observe_upstream(err.upstream_headers)
    if isinstance(err, errors.AuthenticationFailed):
        # THE ONE EXCEPTION TO C4 (CONTRACTS.md C11). A 401/403 from the
        # provider means the provider rejected OUR credential -- the client
        # never supplied one, so nothing in that body is the client's to
        # read. At least one provider's 401 message quotes the tail of the
        # key it rejected (findings log #30: DeepSeek, "…api key: ****abcd"),
        # and under a shared key that is four characters of a shared secret
        # handed to whoever sent the request. The status still passes
        # through -- a 401 is the honest shape and the client's SDK expects
        # it -- but the body is ours. The taxonomy, the breaker (credential
        # scope) and the capture record are untouched: this changes what the
        # client sees, not what we learned. Upstream headers never reach the
        # client from here in the first place (see the docstring above), so
        # a `www-authenticate` echo is not a risk.
        body = None
    if not body:
        message = err.message
        code = err.code
        if isinstance(err, errors.AuthenticationFailed):
            code = "upstream_auth"
            message = (
                f"provider rejected the gateway's credential for "
                f"{err.provider or 'upstream'}"
            )
        body = json.dumps({"error": {"type": code, "message": message}}).encode("utf-8")
    headers = exchange.gw_headers()
    headers.append((b"content-type", b"application/json"))
    headers.append((b"content-length", str(len(body)).encode("ascii")))
    if err.retry_after is not None:
        headers.append((b"retry-after", _retry_after_header(err.retry_after)))
    await send({"type": "http.response.start", "status": err.client_status,
                "headers": headers})
    await send({"type": "http.response.body", "body": body, "more_body": False})


async def send_json_error(
    send: Send,
    *,
    status: int,
    code: str,
    message: str,
    exchange: Exchange,
    extra_headers: list[tuple[bytes, bytes]] | None = None,
) -> None:
    """A failure with no taxonomy class. Today that is four: 401, 413, and the
    two process-wide 503s (`draining`, `overloaded`)."""
    body = json.dumps({"error": {"type": code, "message": message}}).encode("utf-8")
    headers = exchange.gw_headers()
    headers.append((b"content-type", b"application/json"))
    headers.append((b"content-length", str(len(body)).encode("ascii")))
    headers.extend(extra_headers or ())
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body, "more_body": False})


# ==========================================================================
# Operational routes
# ==========================================================================


def _breaker_report(breaker: Breaker) -> dict[str, object]:
    snap = breaker.snapshot()
    # The key is a tuple and JSON has no tuples; spell it as the serving
    # path's `provider/model` string so the two are greppable together.
    snap["key"] = "/".join(breaker.key)
    return snap

def _tenant_report(gw: Gateway, tenant: str | None) -> dict[str, object] | None:
    """What admission knows about `tenant`, or None for no tenant.

    Two sources, because a tenant that has never sent a request has
    limits (from config) but no state (in the controller), and a probe
    that answered `null` for a freshly configured tenant would look like
    a config error. `snapshot()` is projected-to-now and does not refill;
    the probe is an observer.
    """
    if tenant is None:
        return None
    limits = gw.tenant_limits_for(tenant)
    state = gw.admission.snapshot().get(tenant)
    return {
        "tenant": tenant,
        "configured": limits is not None,
        "limits": None if limits is None else {
            "rate_per_second": limits.rate_per_second,
            "burst": limits.burst,
            "max_concurrency": limits.max_concurrency,
        },
        "in_use": 0 if state is None else state["in_use"],
        "tokens": (
            (None if limits is None else float(limits.burst))
            if state is None else state["tokens"]
        ),
        "denied": {} if state is None else state["denied"],
    }


def build_app(
    config: ServerConfig | None = None,
    *,
    clock: Clock | None = None,
    extra_surfaces: Mapping[str, Surface] | None = None,
) -> Starlette:
    """The factory. Everything a test needs to vary is an argument.

    A server that can only be exercised through a process-global `app` can only
    be tested at the settings that process happened to start with -- so the
    frame bound, the progress budget and the request cap, which are precisely
    the things worth asserting, become untestable or become monkeypatching.
    """
    gateway = Gateway(config or ServerConfig(), clock=clock)

    @asynccontextmanager
    async def lifespan(_: Starlette):
        # The pool is opened once and closed once. Closing matters: an
        # `AsyncClient` left open at shutdown holds its connections until the
        # process dies, which turns a rolling deploy into a period where both
        # the old and the new process are holding the provider's concurrency.
        await gateway.startup()
        try:
            yield
        finally:
            await gateway.shutdown()

    async def healthz(_: Request) -> Response:
        """Readiness, not liveness.

        503 while draining is P6. The branch is here because drain is a
        *sequence* -- flip the flag, answer 503, let open streams finish -- and
        the only part of it that belongs to this file is the answer.
        """
        if gateway.draining:
            return JSONResponse({"status": "draining"}, status_code=503)
        return JSONResponse({"status": "ok", "draining": False})

    async def metrics(_: Request) -> Response:
        """Prometheus text format over the process registry.

        The event-driven metrics (requests, tokens, breaker transitions, ...)
        are already in the registry, incremented as they happened. The sampled
        ones -- the gauges and the counters mirrored off a cumulative dict
        another module owns (permits in use, open upstream connections, the
        capture queue, the drain counts, task count, draining) -- are refreshed
        HERE, in the handler, immediately before the scrape. That is
        deliberate: sampling `len(asyncio.all_tasks())` from a background task
        would count the sampler, and `llmgw_tasks` is the one gauge that must
        not do that. `sample_metrics()` is a no-op before startup and guards
        each source, so the scrape cannot 500.
        """
        gateway.sample_metrics()
        return Response(generate_latest(gateway.registry),
                        media_type=CONTENT_TYPE_LATEST)

    async def probe(request: Request) -> Response:
        """What would this workload do with a request? Answered from policy.

        MUST NOT open an upstream connection, and does not: every value below
        comes out of a snapshot and a catalog, which are dictionaries. That is
        CONTRACTS.md C6 in its cheapest form -- a diagnostic that costs a
        provider a socket is a diagnostic you stop running when you most need
        it, and one that a health-check loop can point at is a self-inflicted
        load test.

        In P2 the workload -> model mapping was a stub. It is now the real
        plan: the same `snapshot.plan_for()` call the serving path makes, in
        the same order, with the same budgets and the same retry table. That
        equality is the entire value of the endpoint -- a probe that reports
        what the gateway would *approximately* do is a probe you cannot use to
        answer "why did that request go there".

        `?model=` still pins, exactly as a body's `model` does on the serving
        path, so an operator can ask "and what if the caller names this one?"
        without sending a request.

        P4 adds the three things the gate would consult for this plan, read
        off the same objects the serving path reads: each target's two
        circuits (the target's and its credential's), the named tenant's
        admission state, and the provider-key limiter's in-use counts. Still
        no upstream call: every one of them is a dictionary.

        `?tenant=<id>` names a tenant by id. Without it, a bearer token on
        the probe resolves the way it would on the serving path, so a client
        can ask "what would happen to ME" -- and with neither, on a
        deployment that has a tenants file and no anonymous tenant, the
        answer is honestly `null`.
        """
        snapshot = gateway.policy.current()
        derived = gateway.derived(snapshot)
        requested = request.path_params["workload"]
        try:
            workload_id = gateway.resolve_workload(snapshot, requested)
            plan = snapshot.plan_for(
                workload_id, model=request.query_params.get("model")
            )
        except errors.PolicyError as err:
            return JSONResponse(
                {"error": {"type": err.code, "message": err.message},
                 "workload_id": requested},
                status_code=err.client_status,
            )
        budgets = plan.budgets
        retry = derived.retry.get(workload_id)
        tenant = request.query_params.get(TENANT_QUERY_PARAM)
        if tenant is None:
            try:
                tenant = gateway.resolve_tenant(request.scope)
            except Unauthenticated:
                tenant = None
        return JSONResponse({
            "workload_id": requested,
            "routes_to": workload_id,
            # Not always equal: with no policy document there is one workload
            # and a name is a label, so `voice` and `chat` both route to it.
            # Reporting only one of the two would either hide the caller's
            # question or hide the answer.
            "policy_id": snapshot.id,
            "catalog_id": derived.catalog_id,
            "policy_age_seconds": snapshot.age(gateway.clock),
            # Monotonic, so this is an AGE and never a timestamp -- row 10's
            # residual risk is that a long stream pins policy for as long as
            # it runs, and the honest thing to do with that number is show it.
            "default_workload": snapshot.default_workload,
            "upstream_called": False,
            # The probe answers even while draining -- it is a diagnostic, not
            # a request, and an operator debugging a stuck deploy needs it most
            # exactly then -- but it says so, so "why is /healthz 503" has an
            # answer on the same endpoint. Read off the flag, no upstream call.
            "draining": gateway.draining,
            # The cap and the number it is compared against, together, so
            # "why 503 overloaded" is answerable from one read. `inflight`
            # counts this probe's neighbours, not the probe: operational
            # routes are outside the endpoint's open/finally pair.
            "max_streams": gateway.config.max_streams,
            "inflight": gateway.inflight,
            "fake_upstreams": gateway.config.fake_upstreams,
            "targets": [{
                "provider": target.provider.id,
                "model": target.model.id,
                "api_model": target.model.api_model,
                "kind": target.provider.kind,
                "base_url": target.provider.base_url,
                "credential_env": target.provider.api_key_env,
                "credential_present": target.provider.api_key() is not None,
                "max_concurrency": target.provider.max_concurrency,
                "served_by": str(target),
                # ORDERED: candidate first, incumbent last. The order is the
                # answer to "which one gets the traffic", so a client reading
                # this as a set has read it wrong.
                "role": "candidate" if i == 0 and len(plan.targets) > 1
                        else "incumbent",
            } for i, target in enumerate(plan.targets)],
            "budgets": {
                "total": budgets.total,
                "connect": budgets.connect,
                "first_event": budgets.first_event,
                "progress": budgets.progress,
                "liveness": budgets.liveness,
                "client_stall": budgets.client_stall,
            },
            "retry": None if retry is None else {
                "max_attempts": retry.max_attempts,
                "base_delay": retry.base_delay,
                "max_delay": retry.max_delay,
                "min_attempt_time": retry.min_attempt_time,
                "respect_retry_after": retry.respect_retry_after,
                "enabled": retry.enabled,
            },
            "max_upstream_requests": len(plan.targets) + (
                0 if retry is None else max(0, retry.max_attempts - 1)
            ),
            # The amplification bound, spelled out because it is the number an
            # operator actually needs and the one everybody computes wrong:
            # `max_attempts` bounds REPETITION, breadth is the plan's, and the
            # total is the sum -- not `max_attempts`.
            "limits": {
                "buffer_bytes": gateway.config.buffer_bytes,
                "max_frame_bytes": gateway.config.max_frame_bytes,
                "max_request_bytes": gateway.config.max_request_bytes,
                "max_response_bytes": gateway.config.max_response_bytes,
                "surface_limits": {
                    # The byte RATES are reported beside the byte caps
                    # (PLAN-G 4.2) because for a WebSocket surface the caps
                    # alone say almost nothing: one frame is never the
                    # problem, the rate of them is.
                    name: {"max_request_bytes": lim.max_request_bytes,
                           "max_response_bytes": lim.max_response_bytes,
                           "max_in_bps": lim.max_in_bps,
                           "max_out_bps": lim.max_out_bps}
                    for name, lim in (
                        (n, gateway.config.limits_for(n))
                        for n in sorted({*SURFACE_NAMES, *gateway.config.surface_limits})
                    )
                },
            },
            # ---- the gate, as the serving path would find it right now ----
            "breakers": [
                {
                    "served_by": str(target),
                    # `for_key()` rather than a lookup in `snapshot()`: a key
                    # the executor has never asked about has no breaker yet,
                    # and the honest report for it is a CLOSED circuit with
                    # zero failures -- which is exactly what `for_key()`
                    # creates. The registry is bounded by the catalog either
                    # way, so a probe cannot grow it past what traffic would.
                    "target": _breaker_report(gateway.breakers.for_key(target.health_key)),
                    "credential": _breaker_report(
                        gateway.breakers.for_key(credential_health_key(target))
                    ),
                }
                for target in plan.targets
            ],
            "breaker_policy": {
                "failure_threshold": gateway.config.breaker.failure_threshold,
                "window": gateway.config.breaker.window,
                "cooldown": gateway.config.breaker.cooldown,
                "half_open_probes": gateway.config.breaker.half_open_probes,
            },
            "tenant_mode": "table" if gateway.tenants is not None else "anonymous",
            # Said out loud for the same reason `fake_upstreams` is: a
            # deployment where every caller is one tenant is the row-6
            # misconfiguration, and nothing on a dashboard shows it.
            "admission": _tenant_report(gateway, tenant),
            "provider_keys": {
                "in_use": gateway.limiter.snapshot(),
                "caps": {
                    target.credential_key: target.provider.max_concurrency
                    for target in plan.targets
                },
            },
            "denials": {
                # Every key is a `metrics.DENIAL_REASONS` value; the
                # isolation tier asserts it so P5 cannot be handed a label
                # the registry rejects. `draining` joins here so the diagnostic
                # that answers "why was my request refused" sees a shed-while-
                # draining count next to the rate and concurrency ones, the
                # same merge the /metrics scrape performs.
                **gateway.admission.denials(),
                **gateway.limiter.denials(),
                **gateway.draining_denials(),
                **gateway.overloaded_denials(),
            },
        })

    async def not_implemented(request: Request) -> Response:
        return JSONResponse(
            {"error": {
                "type": "not_implemented",
                "message": (
                    f"{request.url.path} is not served in this build. The "
                    "OpenAI Responses surface lands with its own native "
                    "ending (C2) and is out of scope for P2."
                ),
            }},
            status_code=501,
        )

    routes: list[Route] = []
    for surface in REGISTRY:
        methods = list(surface_methods(surface))
        for route in surface_routes(surface):
            endpoint: Any
            if getattr(surface, "serves_locally", False):
                # `/v1/models`: answered from the catalog, never upstream (C18).
                endpoint = ModelsEndpoint(gateway, surface=surface, route=route)
            else:
                endpoint = PassthroughEndpoint(gateway, surface=surface, route=route)
            routes.append(Route(route, endpoint, methods=methods, name=surface.name))
            # The same endpoint OBJECT under the workload-prefixed path. One
            # serving path, two ways to address it: a second endpoint
            # instance would be a second place for the two forms to drift
            # apart, and the only difference between them is a `path_params`
            # entry.
            routes.append(Route(
                f"{WORKLOAD_ROUTE_PREFIX}{route}", endpoint, methods=methods,
                name=f"{surface.name}_by_workload",
            ))
    # `extra_surfaces`: client route -> Surface, mounted on the same
    # `PassthroughEndpoint` as the shipped routes (PLAN-2 B4). This is how the
    # contract tier exercises `body="multipart"` and `body="raw"` before any
    # shipped surface declares them, and how Phase D's voice surfaces will
    # be registered without touching this table.
    for route, surface in (extra_surfaces or {}).items():
        endpoint = PassthroughEndpoint(gateway, surface=surface, route=route)
        routes.append(Route(route, endpoint, methods=["POST"], name=surface.name))
        routes.append(Route(
            f"{WORKLOAD_ROUTE_PREFIX}{route}", endpoint, methods=["POST"],
            name=f"{surface.name}_by_workload",
        ))
    routes.extend(
        Route(path, not_implemented, methods=["POST"], name=f"unimplemented{path}")
        for path in UNIMPLEMENTED_ROUTES
    )
    # PLAN-G: the socket plane, mounted from its own registry. A
    # `WebSocketRoute` and a `Route` cannot collide (Starlette matches on the
    # scope type first), so the two tables are independent and the ws paths
    # need not avoid the HTTP ones -- which is fortunate, because the
    # provider chose them and `/tts/v1/voice:streamBidirectional` is not a
    # name anybody would pick twice.
    routes.extend(build_ws_routes(gateway))
    routes += [
        Route("/healthz", healthz, methods=["GET"]),
        Route("/metrics", metrics, methods=["GET"]),
        Route("/workloads/{workload}/probe", probe, methods=["GET"]),
    ]

    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.gateway = gateway
    """Exposed so a test can read `Upstream.in_flight()` -- the assertion that
    a client disconnect really reached the upstream is a statement about the
    server's internals, and no client can observe it."""
    return app


app = build_app(ServerConfig.from_env())
"""The process-global instance `make run` points uvicorn at. One call to the
factory and nothing else; see `build_app` for why that ordering matters."""


__all__ = [
    "AUTHORIZATION_HEADER",
    "FORWARDED_RESPONSE_HEADERS",
    "HOP_BY_HOP",
    "NO_RETRY_HEADER",
    "ROUTE_TO_UPSTREAM_PATH",
    "TENANT_QUERY_PARAM",
    "WORKLOAD_HEADER",
    "WORKLOAD_ROUTE_PREFIX",
    "ASGISink",
    "BufferedSink",
    "DrainReport",
    "Exchange",
    "Gateway",
    "PassthroughEndpoint",
    "RequestTooLarge",
    "ShutdownCuts",
    "Unauthenticated",
    "app",
    "bearer_token",
    "build_app",
    "facts_for_body",
    "multipart_boundary",
    "run_until_disconnect",
    "scan_multipart_fields",
]

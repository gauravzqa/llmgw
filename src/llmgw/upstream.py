"""The transport. One pooled client per provider, one clock, no vendor exceptions.

This module is the only place in the gateway that knows httpx exists. That is
its entire justification: everything above it switches on `errors.py` classes,
and a single leaked `httpx.ReadError` makes every `except GatewayError` in the
executor incomplete in a way no test above this layer can see.

--------------------------------------------------------------------------
Pooling is capacity, not a micro-optimisation
--------------------------------------------------------------------------

One `httpx.AsyncClient` per provider connection, created lazily and reused for
the life of the process. A gateway that opens a fresh TCP+TLS connection per
request pays a full handshake -- one RTT for TCP, one or two more for TLS --
on *every* call, which lands directly in the p50 the customer measures. Worse,
it exhausts file descriptors at a fraction of the load it was sized for:
sockets linger in TIME_WAIT for minutes after close, so a gateway at 200 rps
with per-request connections is holding tens of thousands of dead fds and
starts failing `accept()` on the *client* side for a reason that has nothing
to do with clients.

`httpx.Limits(max_connections=provider.max_concurrency)` is the second half of
the same argument in the other direction: the pool is also a cap. Without it,
a burst opens as many upstream connections as there are in-flight requests,
and the provider -- not us -- decides what to do about it, usually with a 429
that we then dutifully retry.

--------------------------------------------------------------------------
The deadline is the only clock
--------------------------------------------------------------------------

Every httpx timeout is `None`. Every wait is wrapped in
`clocks.phase(deadline, budget, on_timeout=...)`.

Two independent timeout systems is not "defence in depth", it is a bug. The
tighter one wins, silently, and which one is tighter depends on how much of
the deadline a previous attempt already spent -- so the budget you configured
becomes decorative on exactly the requests that were already in trouble. You
then get a `httpx.ReadTimeout` where you expected a `FirstEventTimeout`, with
the opposite retry disposition, and no log line explains why.

There is deliberately no loose httpx "safety net" either. Every await in this
module is inside a `phase()`, including the pool-queue wait hidden inside
`client.send()`, so there is no wait for a safety net to catch. A net that
catches nothing is a number someone will later tune, and tuning it is how the
second clock comes back.

--------------------------------------------------------------------------
The phase decides the class, and the two phases are opposites
--------------------------------------------------------------------------

A read that fails *before* response headers and one that fails *after* are the
same httpx exception and completely different events:

    before headers  the provider never accepted the work. No tokens were
                    generated, nothing was billed, no side effect exists.
                    `retry_same=True` is safe.
    after headers   the request was accepted. The model may be generating
                    right now. Re-sending pays twice for work we throw away.

`errors.py` already encodes that split (`ConnectionFailed` vs
`UpstreamDisconnected`, `ConnectTimeout` vs `FirstEventTimeout`). This module's
job is to know which side of the headers it was standing on when the failure
arrived, because httpx does not carry that fact in the exception.

--------------------------------------------------------------------------
The passthrough hole
--------------------------------------------------------------------------

`ProviderConn.extra_body` cannot be honoured without parsing and re-serialising
the client's JSON, which is the one thing byte-for-byte passthrough exists to
avoid. We do it anyway when it is configured, and we say so: `body_modified`
travels out on the stream so the server can surface it as a response header
rather than letting the client believe it sent bytes it did not. See
`apply_extra_body`.

The same hole has a second, mandatory half: the client's `model` names a
CATALOG entry and the provider's API wants that entry's `api_model`, so the
field is rewritten per target by `apply_api_model`. It reports through the
same `body_modified` flag, and it leaves the bytes untouched whenever the
client already named the target's wire model.
"""

from __future__ import annotations

import json
import ssl
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx

from . import errors
from .catalog import Catalog, ProviderConn, Target
from .clocks import Budgets, Clock, Deadline, SystemClock, phase
from .surfaces.base import parse_json_object

ANTHROPIC_VERSION = "2023-06-01"
"""Pinned, not read from the request. Anthropic's API version selects wire
behaviour, so letting a client pick it means the gateway's parser can be handed
a shape it was never tested against by anyone who can set a header."""

DEFAULT_READ_SIZE = 65_536
DEFAULT_MAX_ERROR_BODY = 64 * 1024


# ==========================================================================
# Request and response
# ==========================================================================


@dataclass(frozen=True, slots=True, repr=False)
class UpstreamRequest:
    """One attempt's worth of input. Deliberately carries no credential.

    The API key is fetched from the environment at send time via
    `ProviderConn.api_key()` and never stored on an object that anything might
    log. A request object that holds a secret is a request object that ends up
    in a traceback, a retry log line, or a Sentry breadcrumb, and none of those
    were written by someone thinking about secrets.
    """

    target: Target
    body: bytes
    """The ORIGINAL client bytes. Forwarded unmodified unless the client's
    `model` is not this target's `api_model` (`apply_api_model`) or the
    provider configures `extra_body` (`apply_extra_body`)."""

    path: str
    stream: bool
    extra_headers: Mapping[str, str] = field(default_factory=dict)

    def __repr__(self) -> str:
        # Length, never content. The body is the customer's prompt, and the
        # header map is not shown at all because that is where the key lives.
        return (
            f"<UpstreamRequest {self.target} {self.path} "
            f"stream={self.stream} body={len(self.body)}B>"
        )


class UpstreamStream:
    """An open upstream response, positioned just after the headers.

    `aiter_raw()` is the seam the pump consumes: raw bytes as they arrive,
    never accumulated, never parsed. Parsing here would defeat the whole
    design -- the gateway's value on the streaming path is that it moves bytes
    without understanding them, and a layer that understands them is a layer
    that can be wrong about them.
    """

    __slots__ = (
        "status", "headers", "body_modified",
        "_response", "_deadline", "_budgets", "_read_size", "_ctx", "_started",
    )

    def __init__(
        self,
        response: httpx.Response,
        *,
        deadline: Deadline,
        budgets: Budgets,
        read_size: int,
        body_modified: bool,
        ctx: dict[str, Any],
    ) -> None:
        self._response = response
        self._deadline = deadline
        self._budgets = budgets
        self._read_size = read_size
        self._ctx = ctx
        self._started = False
        self.status: int = response.status_code
        self.headers: Mapping[str, str] = response.headers
        self.body_modified: bool = body_modified

    async def aiter_raw(self) -> AsyncIterator[bytes]:
        """Raw body bytes, unbuffered, with the first chunk on its own clock.

        Two budgets, and only two:

        * the FIRST chunk waits under `budgets.first_event`, because "headers
          arrived and then nothing did" is a distinct failure with a distinct
          disposition (`FirstEventTimeout` is `try_next` but not `retry_same`).
        * every LATER chunk waits under the total deadline only.

        The second one is a boundary, not an omission. The inter-event stall
        clock belongs to the pump, because a stall is defined in *events* and
        this layer refuses to know what an event is; a byte-level stall clock
        here would fire on a provider that is legitimately dribbling one frame
        across three TCP segments. What we do owe the caller is that no read
        can outlive the request, hence the total-deadline bound -- without it a
        silent upstream parks this coroutine forever and the permit, the
        buffer and the connection go with it.
        """
        if self._started:
            raise RuntimeError("aiter_raw() may only be consumed once")
        self._started = True
        # No chunk_size. `httpx.Response.aiter_raw(n)` runs the body through a
        # ByteChunker that WITHHOLDS bytes until it has n of them (or EOF), so
        # passing the read size here would hold a 40-byte SSE frame back until
        # 64 KiB had accumulated -- turning a streaming gateway into a batching
        # one and adding seconds to time-to-first-token on a slow model. The
        # read size governs the bounded error-body read, where buffering is the
        # point, and nothing else.
        iterator = self._response.aiter_raw().__aiter__()
        first = True
        while True:
            budget = self._budgets.first_event if first else None
            on_timeout: type[errors.GatewayError] = (
                errors.FirstEventTimeout if first else errors.StallTimeout
            )
            async with phase(
                self._deadline, budget, on_timeout=on_timeout, **self._ctx
            ):
                chunk = await self._next(iterator)
            if chunk is None:
                return
            first = False
            if chunk:
                yield chunk

    async def _next(self, iterator: Any) -> bytes | None:
        """One chunk, or None at end of body.

        `StopAsyncIteration` is converted to a sentinel here rather than
        allowed to escape: raising it out of an async generator body is a
        `RuntimeError` under PEP 525, and it would additionally have to travel
        back through `phase()`'s context manager to get there.

        The caught set is `TRANSPORT_EXCEPTIONS` and not a hand-written tuple,
        for the reason that constant's own docstring gives: `httpx.StreamError`
        is a `RuntimeError` and `httpx.InvalidURL` a bare `Exception`, so
        neither is caught by `except httpx.HTTPError`. Reading a body is the
        half of this module where that omission is invisible -- it needs a
        provider to break mid-stream in one specific way -- and the cost of it
        is the failure this file exists to prevent: a vendor exception reaching
        an executor whose every handler is `except GatewayError`.
        """
        try:
            return await iterator.__anext__()
        except StopAsyncIteration:
            return None
        except TRANSPORT_EXCEPTIONS as exc:
            raise map_transport_error(exc, after_headers=True, **self._ctx) from exc

    async def aclose(self) -> None:
        await _safe_aclose(self._response)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<UpstreamStream {self.status} modified={self.body_modified}>"


# ==========================================================================
# Request construction
# ==========================================================================


def join_url(base_url: str | None, path: str) -> str:
    """Join a provider base URL to a surface path without doubling the version.

    OpenAI-compatible base URLs conventionally already carry `/v1`
    (`https://openrouter.ai/api/v1`), and the surface path also carries it
    (`/v1/chat/completions`). Naive concatenation produces `/api/v1/v1/...`,
    which every provider answers with a 404 that then gets classified as
    `ModelNotFound` -- a config error wearing a routing error's clothes, and
    one that sends the executor shopping the request around every fallback
    before giving up. So the duplicate segment is collapsed here, once.
    """
    if not base_url:
        raise errors.PolicyError("provider has no base_url configured")
    base = base_url.rstrip("/")
    tail = "/" + path.lstrip("/")
    last = base.rsplit("/", 1)[-1]
    first = tail.lstrip("/").split("/", 1)[0]
    if last and last == first:
        base = base[: -(len(last) + 1)]
    return base + tail


def build_headers(
    target: Target, *, stream: bool, extra: Mapping[str, str] | None = None
) -> dict[str, str]:
    """Auth and content headers for one attempt, per provider *kind*.

    Kind, not vendor: anything OpenAI-shaped -- OpenRouter, DeepSeek, Groq, a
    local vLLM -- authenticates identically, which is why there is no
    per-provider client subclass anywhere in this repo.

    A missing credential raises `PolicyError` rather than sending an
    unauthenticated request and letting the provider answer 401. The 401 costs
    a connection, a handshake and a round trip to learn something a dictionary
    lookup already knew, and it arrives as `AuthenticationFailed`, which is
    scoped to the credential and would mark a key unhealthy that was never
    even configured.
    """
    provider = target.provider
    key = provider.api_key()
    if not key:
        raise errors.PolicyError(
            f"provider {provider.id!r} has no credential in ${provider.api_key_env}",
            provider=provider.id,
            model=target.model.id,
            credential_id=provider.key(),
        )
    headers = {
        "content-type": "application/json",
        "accept": "text/event-stream" if stream else "application/json",
    }
    if provider.kind == "anthropic":
        headers["x-api-key"] = key
        headers["anthropic-version"] = ANTHROPIC_VERSION
    else:
        headers["authorization"] = f"Bearer {key}"
    # Provider extras then per-request extras, both last so an operator can
    # override anything above -- including `anthropic-version`, which is the
    # header most likely to need pinning during a provider migration.
    for source in (provider.extra_headers, extra or {}):
        for name, value in source.items():
            headers[name.lower()] = value
    return headers


def apply_extra_body(body: bytes, extra_body: Mapping[str, object]) -> tuple[bytes, bool]:
    """The one place byte-for-byte passthrough stops being true.

    `ProviderConn.extra_body` exists because real providers need fields the
    client did not send: OpenRouter's `{"provider": {"require_parameters":
    true}}` keeps a request off hosts that silently drop parameters, added
    after one such host mangled DeepSeek tool-call parsing and leaked
    raw markup into a content stream. Honouring it requires parsing
    the client's JSON, merging, and re-serialising -- so the bytes on the wire
    are not the bytes we received, and no amount of care makes them so.

    Two consequences we choose to accept, loudly:

    1. `body_modified` travels out on the stream so the server can announce it
       (`X-Gw-Body-Modified`). A gateway that mutates a body and says nothing
       is a gateway whose users debug the wrong request for an afternoon.
    2. When `extra_body` is empty -- the overwhelmingly common case, and every
       first-party provider in the catalog -- the original bytes are forwarded
       untouched and never parsed. The hole is opt-in per provider, not a
       property of the request path.

    The merge is TOP-LEVEL and `extra_body` wins. Deep-merging would need a
    conflict policy for the case where the client sent `provider.order` and we
    want `provider.require_parameters`, and any policy we picked would produce
    a request neither party wrote. A shallow merge is at least legible: the key
    we set is the key we set.

    A body that is not a JSON object raises `errors.InvalidRequest`. It must
    not crash: this runs on the request path with attacker-influenced bytes,
    and a `json.JSONDecodeError` escaping here would be classified as a gateway
    fault and counted against a provider's circuit breaker for a request that
    never left the building.
    """
    if not extra_body:
        return body, False
    parsed = parse_json_object(body)  # raises errors.InvalidRequest
    merged: dict[str, Any] = {**parsed, **dict(extra_body)}
    try:
        rendered = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:  # pragma: no cover - config bug
        raise errors.PolicyError(f"extra_body is not JSON-serialisable: {exc}") from exc
    return rendered.encode("utf-8"), True


def apply_api_model(body: bytes, api_model: str) -> tuple[bytes, bool]:
    """The client's `model` becomes the model THIS target's API answers to.

    The second place byte-for-byte passthrough stops being true, and the one
    that is not opt-in. `ModelSpec.api_model` exists because our catalog id
    (`openrouter.deepseek-v4-pro`) is not the string the provider's API takes
    (`deepseek/deepseek-v4-pro`), and both surfaces carry that string in one
    top-level `model` key. Forwarding the client's value verbatim sends the
    catalog id to the provider, which answers 404 or 400.

    Under fallback it is worse than merely wrong, because it fails LATE. A
    plan of `openrouter.deepseek-v4-pro` then `anthropic.haiku-4-5` sends the
    candidate's model string to the incumbent's API, so the incumbent 404s --
    and the incumbent is only ever reached when the candidate is already
    down. The redundancy evaporates at exactly the moment it is needed, and
    no test that never fires the fallback can see it.

    The equality check is what keeps the common case honest. A client that
    already named this target's wire model gets its ORIGINAL bytes back --
    the same object, not a re-serialisation -- so single-target passthrough
    is still byte for byte, including key order, whitespace and unicode
    escaping. Re-serialising a body we had no reason to touch changes all
    three, and the visible consequence is a provider-side prompt-cache miss
    on somebody else's bill.

    Rewriting means parsing, and parsing means `errors.InvalidRequest` rather
    than a `json.JSONDecodeError`: this runs on the request path with
    attacker-influenced bytes, and a bare `ValueError` escaping here would be
    counted as a gateway fault against a provider that was never contacted.

    A body with no `model` at all gets one. Both surfaces require the field,
    so its absence is already a 400 at `Surface.parse_request` on the serving
    path; supplying it here means the library facade, which has no surface in
    front of it, still sends a request the provider can answer.

    Applied BEFORE `apply_extra_body`, so a provider that deliberately pins a
    `model` in its `extra_body` still wins -- the same ordering, and the same
    reason, as `build_headers` merging provider extras last.
    """
    parsed = parse_json_object(body)  # raises errors.InvalidRequest
    if parsed.get("model") == api_model:
        return body, False
    merged: dict[str, Any] = {**parsed, "model": api_model}
    try:
        rendered = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError, RecursionError) as exc:
        # Reachable only from a body `json.loads` accepted and `json.dumps`
        # will not re-emit -- deep nesting is the practical one. A client
        # fault either way, and never a crash: this is the request path.
        raise errors.InvalidRequest(
            f"request body cannot be re-serialised with its target's model: {exc}"
        ) from exc
    return rendered.encode("utf-8"), True


# ==========================================================================
# Exception mapping
# ==========================================================================


def apply_include_usage(body: bytes) -> tuple[bytes, bool]:
    """Ask the OpenAI dialect to stream its usage frame. Opt-in (`ServerConfig.
    inject_include_usage`); the third and last body edit next to
    `apply_api_model` and `apply_extra_body`, reported through the same flag.

    Only when the body is a JSON object with `stream: true` and NO
    `stream_options` key. A client that set `stream_options` -- to anything,
    including `include_usage: false` -- has made a choice and keeps it; a
    client that said nothing gets the one setting under which the gateway can
    bill exactly instead of by the byte estimate (finding 27). Never touches
    a non-streaming body, and never a body the surface could not parse: the
    caller sees `(body, False)` and the bytes go through untouched.
    """
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return body, False
    if not isinstance(parsed, dict) or parsed.get("stream") is not True:
        return body, False
    if "stream_options" in parsed:
        return body, False
    merged: dict[str, Any] = {**parsed, "stream_options": {"include_usage": True}}
    return json.dumps(merged, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), True


def map_transport_error(
    exc: BaseException, *, after_headers: bool, **ctx: Any
) -> errors.GatewayError:
    """Turn a vendor exception into a taxonomy class, phase included.

    `after_headers` is the load-bearing argument and it is not recoverable from
    the exception. `httpx.ReadError` before headers means the provider never
    accepted the work -- safe to re-send. The identical exception after headers
    means the model may already be generating, and re-sending bills the
    customer twice for an answer we throw away. Same type, opposite
    disposition; only the caller knows which it was.

    Everything unrecognised falls to `ConnectionFailed` / `UpstreamDisconnected`
    rather than to a generic gateway error, because a transport exception we
    have no rule for is still evidence about the *provider*, and the
    conservative default has to be the one that lets the request be retried
    somewhere else.
    """
    # Our own config was wrong. Blame POLICY, spend nothing, and above all do
    # not mark a provider unhealthy for a URL we built badly.
    if isinstance(exc, httpx.InvalidURL | httpx.UnsupportedProtocol):
        return errors.PolicyError(f"bad upstream URL: {exc}", cause=exc, **ctx)
    if isinstance(exc, httpx.LocalProtocolError):
        return errors.PolicyError(f"gateway built an invalid request: {exc}",
                                  cause=exc, **ctx)

    if isinstance(exc, httpx.ConnectTimeout):
        return errors.ConnectTimeout(f"connect timed out: {exc}", cause=exc, **ctx)
    if isinstance(exc, httpx.PoolTimeout):
        # The per-credential pool is full. Semantically exact, though in this
        # build it is close to unreachable: no httpx timeout is set, so a
        # saturated pool blocks in the queue and the connect `phase()` fires
        # first. Kept because the day someone sets a pool timeout, this is the
        # class they want, and it is `try_next` with NEUTRAL health.
        return errors.ProviderKeyExhausted(f"upstream pool exhausted: {exc}",
                                           cause=exc, **ctx)
    if isinstance(exc, httpx.ConnectError | httpx.ProxyError):
        # DNS failure, connection refused and TLS failure all arrive here:
        # httpx wraps `socket.gaierror` and `ssl.SSLError` in ConnectError.
        return errors.ConnectionFailed(f"connect failed: {exc}", cause=exc, **ctx)
    if isinstance(exc, httpx.DecodingError):
        return errors.MalformedUpstreamResponse(
            f"undecodable upstream body: {exc}", cause=exc, **ctx
        )
    if isinstance(exc, httpx.RemoteProtocolError):
        # The truncated-chunked-body case: the provider stopped mid-message.
        return errors.UpstreamDisconnected(f"upstream broke the protocol: {exc}",
                                           cause=exc, **ctx)
    if isinstance(exc, httpx.TimeoutException):
        # Read/write timeouts should be impossible (httpx timeouts are None);
        # if one appears, a second clock has been introduced somewhere and the
        # right thing is to report the phase we were actually in.
        cls = errors.FirstEventTimeout if after_headers else errors.ConnectTimeout
        return cls(f"httpx timeout leaked in: {exc}", cause=exc, **ctx)
    if isinstance(exc, httpx.StreamError):
        return errors.UpstreamDisconnected(f"upstream stream error: {exc}",
                                           cause=exc, **ctx)
    if isinstance(exc, ssl.SSLError):
        return errors.ConnectionFailed(f"TLS failure: {exc}", cause=exc, **ctx)

    if after_headers:
        return errors.UpstreamDisconnected(f"upstream connection lost: {exc}",
                                           cause=exc, **ctx)
    return errors.ConnectionFailed(f"upstream connection failed: {exc}",
                                   cause=exc, **ctx)


TRANSPORT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    httpx.HTTPError,
    httpx.InvalidURL,
    httpx.StreamError,
    ssl.SSLError,
    OSError,
)
"""Everything `open()` catches. `httpx.InvalidURL` and `httpx.StreamError` are
listed separately because neither inherits `httpx.HTTPError` -- the first is a
bare `Exception` and the second a `RuntimeError`, which is exactly the kind of
detail a `except httpx.HTTPError` written from memory gets wrong."""


async def _safe_aclose(response: httpx.Response) -> None:
    """Close and return the connection to the pool. Never raises.

    A failure to close is not worth reporting -- the request already has an
    outcome -- but a failure to *attempt* the close leaks a connection, and a
    leaked connection is invisible until the pool is full and the gateway
    stops accepting work for no visible reason.
    """
    if response.is_closed:
        return
    try:
        await response.aclose()
    except Exception:  # noqa: BLE001 - closing must not mask the real error
        pass


# ==========================================================================
# The client
# ==========================================================================


class Upstream:
    """Pooled HTTP transport for every provider in a catalog.

    One `AsyncClient` per (provider id, base URL), built on first use and kept
    forever. Lazily, because a gateway with twelve configured providers should
    not open connection pools for the eleven this process never routes to; and
    keyed on the base URL as well as the id so a catalog redirected at the fake
    upstreams cannot silently reuse a pool aimed at production.
    """

    def __init__(
        self,
        catalog: Catalog,
        *,
        clock: Clock | None = None,
        http2: bool = True,
        read_size: int = DEFAULT_READ_SIZE,
        max_error_body: int = DEFAULT_MAX_ERROR_BODY,
        transport: httpx.AsyncBaseTransport | None = None,
        inject_include_usage: bool = False,
    ) -> None:
        self._catalog = catalog
        self._inject_include_usage = inject_include_usage
        """`ServerConfig.inject_include_usage`: the third body edit, applied
        in `open()` next to the other two and announced the same way."""
        self._clock = clock or SystemClock()
        """Unused on every current path: every wait in this module derives from
        the `Deadline` the caller passes in, which carries its own clock. Held
        so that this class is constructed the same way as every other component
        and so a future pool-queue wait has a clock to use."""

        self._http2 = http2
        self._read_size = read_size
        """Chunk size for the bounded error-body read ONLY. It is deliberately
        not applied to the streaming body -- see `UpstreamStream.aiter_raw`."""

        self._max_error_body = max_error_body
        self._transport = transport
        self._clients: dict[tuple[str, str], httpx.AsyncClient] = {}
        self._inflight: dict[str, int] = {}

    # -------------------------------------------------------------- clients

    def _client_for(self, provider: ProviderConn) -> httpx.AsyncClient:
        """Get or build this provider's client. No lock, and that is not luck:
        `httpx.AsyncClient(...)` never awaits, so there is no suspension point
        between the miss and the store for another task to interleave into."""
        key = (provider.id, provider.base_url or "")
        client = self._clients.get(key)
        if client is not None:
            return client
        limits = httpx.Limits(
            max_connections=provider.max_concurrency,
            # Keep the whole pool warm. Idle connections are the point: the
            # next request skips the handshake entirely, and a keepalive
            # ceiling below max_connections quietly reintroduces per-request
            # handshakes exactly at peak, when they cost the most.
            max_keepalive_connections=provider.max_concurrency,
            keepalive_expiry=90.0,
        )
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(None),  # the Deadline is the only clock
            limits=limits,
            http2=self._http2,
            follow_redirects=False,
            transport=self._transport,
            # ==============================================================
            # The single most important header in this file.
            #
            # httpx advertises `accept-encoding: gzip, deflate` by default.
            # Providers comply -- Anthropic gzips SSE *and* JSON, OpenAI and
            # DeepSeek gzip JSON -- and `aiter_raw()` forwards the body
            # UNDECODED, which is exactly what byte-for-byte passthrough is
            # supposed to do. The result is a gateway that hands its clients
            # compressed bytes without a `content-encoding` header, because
            # that header is not in FORWARDED_RESPONSE_HEADERS.
            #
            # Two shapes, and the second is worse:
            #
            #   streaming  -- no SSE frame parses, so the stream ends as
            #                 `incomplete_stream`. Loud, but usage is 0 and
            #                 cost is $0.00 on a request the provider billed.
            #   buffered   -- HTTP 200, `content-type: application/json`, an
            #                 accurate content-length, a body beginning
            #                 `1f 8b 08`, and NOTHING RAISES ANYWHERE. Every
            #                 layer reports success; the client gets binary.
            #
            # `identity` is the only correct setting for a proxy that does not
            # decode. The alternative -- accept gzip, decompress, re-frame --
            # means giving up byte-for-byte passthrough, spending CPU per
            # stream on the hot path, and owning a decompression bomb.
            #
            # Found only against real providers. The local fakes do not
            # compress, so 745 tests passed over this for three phases. It is
            # the clearest case in the project for why a fake upstream is a
            # necessary rig and never a sufficient one.
            # ==============================================================
            headers={"accept-encoding": "identity"},
        )
        self._clients[key] = client
        return client

    async def aclose(self) -> None:
        clients, self._clients = self._clients, {}
        for client in clients.values():
            try:
                await client.aclose()
            except Exception:  # noqa: BLE001 - shutdown must not raise
                pass

    # ---------------------------------------------------------------- stats

    def stats(self) -> dict[str, int]:
        """Open connections per provider, for `llmgw_upstream_connections`.

        Every provider in the catalog appears, zero included. A gauge whose
        series appear only once traffic reaches a provider is a gauge that
        cannot show you the moment a provider went from busy to untouched, and
        it makes the metric's label vocabulary depend on traffic rather than on
        config.

        This reads httpx's pool directly because there is no public accessor
        for it. If a future httpx moves it, the count degrades to zero rather
        than raising: a metric is not worth a 500.
        """
        counts = {pid: 0 for pid in self._catalog.providers}
        for (pid, _), client in self._clients.items():
            counts[pid] = counts.get(pid, 0) + _pool_size(client)
        return counts

    def in_flight(self) -> dict[str, int]:
        """Upstream responses currently open, per provider.

        The complement to `stats()`: that one counts sockets (compare against
        fd limits), this one counts requests we are still holding. They differ
        in both directions -- an idle keepalive connection is in `stats()` and
        not here, and both must return to their baseline when a request ends,
        which is the assertion that catches a missing `finally`.
        """
        return {pid: n for pid, n in self._inflight.items() if n}

    # ----------------------------------------------------------------- open

    @asynccontextmanager
    async def open(
        self, req: UpstreamRequest, *, deadline: Deadline, budgets: Budgets
    ) -> AsyncIterator[UpstreamStream]:
        """Send one request and yield the open response, headers already in.

        On exit -- normal, exceptional, or cancelled -- the response is closed
        and the connection returned to the pool. That is the whole reason this
        is a context manager rather than a coroutine returning a stream: a
        consumer that is cancelled mid-body cannot be trusted to remember, and
        a leaked upstream connection is the failure mode that presents as "the
        gateway stopped accepting work" an hour later, with nothing in the logs
        pointing at the request that caused it.
        """
        target = req.target
        provider = target.provider
        ctx: dict[str, Any] = {
            "provider": provider.id,
            "model": target.model.id,
            "credential_id": provider.key(),
        }
        # Refuse before spending a socket. Ordering matters: the checks that
        # cost a dictionary lookup come before the one that costs a handshake.
        deadline.check(**ctx)
        url = join_url(provider.base_url, req.path)
        headers = build_headers(target, stream=req.stream, extra=req.extra_headers)
        # Two rewrites, in this order, and one flag for both. The model has to
        # be the one THIS target's API answers to -- see `apply_api_model` for
        # why forwarding the client's string breaks fallback specifically --
        # and `extra_body` is applied last so an operator's explicit pin still
        # overrides ours.
        body, renamed = apply_api_model(req.body, target.model.api_model)
        body, merged = apply_extra_body(body, provider.extra_body)
        injected = False
        if self._inject_include_usage and req.stream and provider.kind == "openai":
            body, injected = apply_include_usage(body)
        body_modified = renamed or merged or injected

        try:
            client = self._client_for(provider)
            request = client.build_request("POST", url, content=body, headers=headers)
        except errors.GatewayError:
            raise
        except TRANSPORT_EXCEPTIONS as exc:
            # `httpx.InvalidURL` is raised here, not by send(). Building the
            # request inside the mapped region is the difference between a
            # PolicyError and a vendor exception escaping a module whose entire
            # contract is that none ever do.
            raise map_transport_error(exc, after_headers=False, **ctx) from exc

        response: httpx.Response | None = None
        self._inflight[provider.id] = self._inflight.get(provider.id, 0) + 1
        try:
            try:
                # Connect + TLS + request write + response headers, one phase.
                # httpx exposes no hook at "connected", so this budget covers
                # more than its name suggests -- which is exactly why a breach
                # here is a HeadersTimeout and not a ConnectTimeout. We cannot
                # prove nothing was sent, so we must not claim the retry is
                # free. `httpx.ConnectTimeout` is still mapped to
                # `ConnectTimeout` in map_transport_error, because httpx raises
                # that one only during connection establishment -- there the
                # proof does exist.
                async with phase(
                    deadline, budgets.connect, on_timeout=errors.HeadersTimeout, **ctx
                ):
                    response = await client.send(request, stream=True)
            except errors.GatewayError:
                raise
            except TRANSPORT_EXCEPTIONS as exc:
                raise map_transport_error(exc, after_headers=False, **ctx) from exc

            if not 200 <= response.status_code < 300:
                raise await self._classify_status(
                    response, deadline, budgets, ctx,
                    # Per-provider meaning of 403 (`errors.from_http_status`).
                    # `getattr` until the catalog row grows the field with the
                    # voice providers; the default is today's rule.
                    forbidden_means=getattr(provider, "forbidden_means", "auth"),
                )

            yield UpstreamStream(
                response,
                deadline=deadline,
                budgets=budgets,
                read_size=self._read_size,
                body_modified=body_modified,
                ctx=ctx,
            )
        finally:
            self._inflight[provider.id] -= 1
            if not self._inflight[provider.id]:
                del self._inflight[provider.id]
            if response is not None:
                await _safe_aclose(response)

    async def _classify_status(
        self,
        response: httpx.Response,
        deadline: Deadline,
        budgets: Budgets,
        ctx: dict[str, Any],
        *,
        forbidden_means: str = "auth",
    ) -> errors.GatewayError:
        """Read at most `max_error_body` bytes, then hand the status to the
        taxonomy.

        The bound is not tidiness. A provider having a bad day can answer 500
        with a 100 MB HTML error page, and a gateway that reads it "just to
        classify" turns one provider incident into its own memory incident --
        at the exact moment every request is failing and every one of them is
        reading a 100 MB body concurrently. We read enough to refine the class
        (`from_http_status` looks at `error.type` and `error.code`, both within
        the first few hundred bytes of any real provider error) and drop the
        connection.

        Nothing in here may raise. A timeout or a reset while reading an error
        body must not replace a perfectly good 429 with a transport error --
        the status is the strong signal and we already have it.
        """
        body = await self._read_error_body(response, deadline, budgets)
        return errors.from_http_status(
            response.status_code,
            body=body,
            # The headers ride on the error: `send_error` reads the
            # provider's request id off them, and `_record` its rate-limit
            # budget (PLAN-2 A6d/e). Neither reaches the client verbatim.
            headers=dict(response.headers),
            retry_after=response.headers.get("retry-after"),
            forbidden_means=forbidden_means,
            **ctx,
        )

    async def _read_error_body(
        self, response: httpx.Response, deadline: Deadline, budgets: Budgets
    ) -> bytes:
        chunks: list[bytes] = []
        total = 0
        try:
            # Bounded in bytes AND in time. The connect budget is the right
            # ceiling: we are not streaming, the provider has already decided,
            # and a body that will not arrive inside a connect budget is a body
            # we do not want.
            async with deadline.timeout(budgets.connect):
                async for chunk in response.aiter_raw(min(self._read_size,
                                                          self._max_error_body)):
                    chunks.append(chunk)
                    total += len(chunk)
                    if total >= self._max_error_body:
                        break
        except (TimeoutError, errors.GatewayError, *TRANSPORT_EXCEPTIONS):
            pass
        return b"".join(chunks)[: self._max_error_body]


def _pool_size(client: httpx.AsyncClient) -> int:
    pool = getattr(getattr(client, "_transport", None), "_pool", None)
    connections = getattr(pool, "connections", None)
    if connections is None:
        return 0
    try:
        return len(connections)
    except TypeError:  # pragma: no cover - defensive
        return 0


__all__ = [
    "ANTHROPIC_VERSION",
    "Upstream",
    "UpstreamRequest",
    "UpstreamStream",
    "apply_api_model",
    "apply_include_usage",
    "apply_extra_body",
    "build_headers",
    "join_url",
    "map_transport_error",
]
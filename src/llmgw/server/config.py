"""Server settings: every knob the serving path reads, in one frozen object.

Two rules shape this file, and both exist because of how gateways actually
get misconfigured.

--------------------------------------------------------------------------
1. The factory is the interface; the module-level `app` is a convenience
--------------------------------------------------------------------------

`build_app(config)` takes a config. `llmgw.server.app:app` is one call to it
with `ServerConfig.from_env()`, and exists only because `make run` needs a
string to point uvicorn at.

That ordering is deliberate. A server whose settings live in module globals
can only ever be tested at whatever values the process happened to start
with, so the tests that matter -- a 32 KiB frame bound, a 0.5 s progress
budget, a 1 KiB request cap -- become untestable or become monkeypatching.
Both are worse than passing an argument.

--------------------------------------------------------------------------
2. Fake mode has to be complete, or it is not a mode
--------------------------------------------------------------------------

`Catalog.redirect_to_fakes()` moves a provider's `base_url` and nothing else.
The credential stays wherever the shipped catalog put it, so a config that
*only* redirected would send every `make run` request into
`build_headers()` -> `PolicyError: provider 'anthropic' has no credential in
$ANTHROPIC_API_KEY`, which reaches the client as a 400 and looks like a bad
request rather than like a missing switch.

So `fake_upstreams=True` does three things, not one: redirect both provider
kinds, repoint every provider at a single throwaway key env var, and default
that var to a fixed string. The result is that `make fakes` + `make run` +
`curl` works on a clean checkout with no secrets anywhere, which is the only
version of "local dev works" worth having.

--------------------------------------------------------------------------
3. The policy source is a file OR nothing, and nothing has to work
--------------------------------------------------------------------------

P3 gives the serving path a real `PolicySnapshot`. Where it comes from is
one field -- `policy_file`, from `LLMGW_POLICY_FILE` -- and when it is unset
the server builds `PolicySnapshot.single_target(default_model, ...)` with
*these* budgets, which is byte-for-byte the routing P2 did with
`config.default_model`.

That fallback is not politeness, it is the adoption path: a migration that
requires a config file to exist before the code works is a migration nobody
performs. It also fixes the
boundary of one behaviour that would otherwise be arbitrary -- see
`ServerConfig.has_policy_document` and `app.Gateway.resolve_workload`, where
"is there a document?" is what decides whether an unknown workload name is an
error or a label.

--------------------------------------------------------------------------
4. The tenant source is a file OR nothing, and the token is a secret
--------------------------------------------------------------------------

P4 puts admission in front of the body read, and admission needs a tenant.
Where tenants come from is one field -- `tenants_file`, from
`LLMGW_TENANTS_FILE` -- with the same two-mode shape as the policy source:
set, and `Authorization: Bearer <token>` is looked up in a static table that
maps tokens to tenant ids and ids to limits; unset, and every request is ONE
anonymous tenant under `tenant_limits`. The fallback is the adoption path
again: `make run` + `curl` with no token must keep working, and a gateway
that refuses everything until someone writes a tenants file is a gateway
nobody upgrades to.

The token is a credential. `TenantTable.resolve()` returns an id and never
the token; nothing downstream sees the token at all, so it cannot reach a
log line, an error message or a header by accident. The id is what gets
reported (`X-Gw-Tenant`), and an id is safe to report because it is chosen
by the operator and not by the client.
"""

from __future__ import annotations

import logging
import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from llmgw.admission import TenantLimits
from llmgw.breaker import BreakerPolicy
from llmgw.catalog import DEFAULT_CATALOG, Catalog
from llmgw.clocks import Budgets, Clock
from llmgw.errors import PolicyError
from llmgw.policy import PolicySnapshot

log = logging.getLogger("llmgw.server.config")


@dataclass(frozen=True, slots=True)
class SurfaceLimits:
    """The two byte caps one surface runs under (PLAN-2 B4). A row is a
    DELTA over the global `max_request_bytes` / `max_response_bytes`: `None`
    means "inherit the global", so an operator who lowers the global with one
    variable lowers every surface that did not set its own number."""

    max_request_bytes: int | None = None
    max_response_bytes: int | None = None

    max_in_bps: int | None = None
    max_out_bps: int | None = None
    """Per-SOCKET byte-rate ceilings, bytes per second, one per direction
    (PLAN-G 4.2). `in` is client -> upstream, `out` is upstream -> client,
    named from the gateway's point of view so the two agree with
    `metrics.WS_DIRECTIONS`. `None` means no rate bound.

    A byte cap and a byte RATE are different protections and a socket needs
    both. `max_request_bytes` bounds one message; nothing in it stops a
    client sending ten thousand legal messages a second, which is the shape
    a relayed audio stream has anyway. The rate bound is a token bucket over
    one-second windows: over-rate INBOUND stops reading the client (TCP
    backpressure carries the refusal, so the provider never sees a pacing
    violation of its own), and over-rate outbound fills the relay buffer,
    which is already the `client_stall` path.

    Sized from the medium, not from a guess: 16 kHz PCM16 mono is 32 KB/s
    and base64 inflates it by a third, so 64 KB/s of inbound audio leaves
    headroom for 24 kHz; a second of 24 kHz LINEAR16 TTS audio is 48 KB, so
    128 KB/s outbound tolerates two contexts flushing at once."""

    def validate(self, name: str) -> None:
        for fname in ("max_request_bytes", "max_response_bytes",
                      "max_in_bps", "max_out_bps"):
            value = getattr(self, fname)
            if value is not None and value < 1:
                raise ValueError(f"surface_limits[{name!r}].{fname} must be positive")

    def resolved(self, *, request: int, response: int) -> SurfaceLimits:
        return SurfaceLimits(
            self.max_request_bytes if self.max_request_bytes is not None else request,
            self.max_response_bytes if self.max_response_bytes is not None else response,
            self.max_in_bps,
            self.max_out_bps,
        )


DEFAULT_SURFACE_LIMITS: dict[str, SurfaceLimits] = {
    # `Surface.name` -> the caps that differ from the globals. Chat is the
    # globals (32 MiB / 8 MiB) and needs no row; the Anthropic messages row
    # pins 32 MiB explicitly because that is the provider's own ceiling, so
    # an operator lowering the global does not silently cut Anthropic vision.
    "anthropic_messages": SurfaceLimits(max_request_bytes=32 * 1024 * 1024),
    # A token count is a prompt, not a PDF (Phase C2): its own smaller cap.
    "count_tokens": SurfaceLimits(max_request_bytes=4 * 1024 * 1024),
    # PLAN-G 4.2: the WebSocket surfaces carry rates, not just caps. Inworld
    # TTS is text in (`send_text`, 2,000 characters a message) and audio out
    # (a 24 kHz LINEAR16 second is 48 KB on the wire, and the captures'
    # largest single `audioChunk` frame is 25,012 B).
    "inworld_tts_ws": SurfaceLimits(
        max_in_bps=16 * 1024, max_out_bps=128 * 1024,
    ),
}

_HEADERS_DEFAULT: dict[str, float] = (
    {"headers": 10.0} if "headers" in getattr(Budgets, "__dataclass_fields__", {}) else {}
)
"""`Budgets.headers` (PLAN-2 B6) is passed only when the field exists, so this
module imports against a `clocks.py` from either side of that change."""

DEFAULT_FAKE_OPENAI_URL = "http://127.0.0.1:8801/v1"
"""Matches `make fakes` and the `fake-openai` entry in the shipped catalog.
The trailing `/v1` is intentional and is collapsed against the surface path by
`upstream.join_url` -- see its docstring for the `/v1/v1/` 404 it prevents."""

DEFAULT_FAKE_ANTHROPIC_URL = "http://127.0.0.1:8802"

FAKE_KEY_ENV = "LLMGW_FAKE_API_KEY"
"""The single credential every provider uses in fake mode."""

DEFAULT_FORWARD_REQUEST_HEADERS: tuple[str, ...] = (
    "anthropic-beta", "openai-beta", "x-request-id", "traceparent", "tracestate",
)
"""Client request headers forwarded upstream unless configured otherwise. The
ones that carry meaning end to end: the two vendor beta-feature switches, and
trace context."""

NEVER_FORWARDED: frozenset[str] = frozenset({
    # Credentials. `upstream.build_headers()` merges extras LAST so an
    # operator can override `anthropic-version`; that same ordering means an
    # allowlisted `authorization` would replace the provider key we just
    # looked up with whatever the client sent.
    "authorization", "x-api-key", "proxy-authorization", "cookie",
    # Connection-scoped. These describe the client's hop to us and are
    # meaningless -- or actively wrong -- on our hop to the provider.
    "host", "content-length", "content-type", "connection", "keep-alive",
    "transfer-encoding", "upgrade", "te", "trailer", "expect", "accept-encoding",
})
"""Header names `forward_request_headers` may never contain. Enforced by
`ServerConfig.validated()` at startup, not documented and hoped for."""

FAKE_KEY_VALUE = "sk-llmgw-local-fake-not-a-secret"
"""Set with `setdefault`, never with `=`: if an operator exported their own
value we must not stamp on it, and a string that looks like a key in a
traceback should say what it is."""

ANONYMOUS_TENANT = "anonymous"
"""The one tenant of the zero-config path, and the id a request with no
bearer token resolves to when a tenants file IS loaded. In the second case it
is admitted only if the file configures `[tenants.anonymous]`; otherwise the
request is a 401. Naming the guest tenant rather than special-casing "no
token" means an operator can give unauthenticated traffic a small budget on
purpose, in the same table as everyone else, instead of it being either
unlimited or impossible."""

DEFAULT_TENANT_LIMITS = TenantLimits(rate_per_second=100.0, burst=200, max_concurrency=256)
"""What the anonymous tenant gets when nothing configured it. Generous next to
a laptop and small next to a fleet: the point is that the zero-config path has
SOME cap, so a runaway local loop gets a 429 rather than an OOM, while a
developer who fires twenty parallel curls never meets it."""


REALTIME_PIN_KEYS: frozenset[str] = frozenset({
    "model", "voice", "tools", "turn_detection", "max_output_tokens",
    "instructions", "expires_after_seconds_cap",
})
"""`[tenants.<id>.realtime]` keys (Phase E1). Every one of them is written
OVER the client's `session` on a client-secret mint (pinned wins); `model` is
a catalog id resolved through the policy like any other request; and
`expires_after_seconds_cap` bounds the credential's TTL for this tenant."""


@dataclass(frozen=True, slots=True)
class TenantTable:
    """Tenant ids, their limits, and the tokens that name them. Frozen: a
    request admitted against one table must not be released against another.

    Two dictionaries, and the direction of the second one is the security
    property. `_tokens` maps TOKEN -> id, so resolving a request is one lookup
    that hands back an id and never the token; there is no `tokens_for(id)`
    and nothing that iterates tokens, so the only way a token leaves this
    object is by being compared against. A table that carried tokens on the
    tenant record would put them one `repr()` away from a log line.
    """

    limits: Mapping[str, TenantLimits]
    _tokens: Mapping[str, str]
    realtime: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)
    """Per-tenant pinned Realtime session fields (Phase E1), keyed by tenant
    id; absent for tenants that pinned nothing."""

    @classmethod
    def from_toml(cls, text: str, *, env: Mapping[str, str] | None = None) -> TenantTable:
        """Parse `config/tenants.example.toml`'s shape. Raises `ValueError`.

            [tenants.acme]
            token_env = "LLMGW_TENANT_ACME_TOKEN"   # the committable spelling
            tokens = ["tok-acme-local"]             # literal; local dev only
            rate_per_second = 20.0
            burst = 40
            max_concurrency = 16

        A tenant's tokens come from two places, and the split is the point.
        `tokens` are literals in the file, which makes the file a secret and
        keeps it out of git. `token_env` (one name) or `token_envs` (a list)
        name environment variables whose VALUES are the tokens, so the file
        carries ids and limits only and the secrets ride in the same channel
        as the provider keys (`fly secrets`, a k8s Secret, an env file
        outside the repo). A named variable that is unset or empty is an
        error at load, never a tenant with fewer tokens than the file
        promised: fail closed, at startup, where a missing secret is a
        deploy that refuses rather than a tenant that quietly cannot
        authenticate. `env` defaults to the process environment and is a
        parameter so a test can supply one.

        Every tenant except `anonymous` must end up with at least one token;
        a named tenant nobody can authenticate as is a config error, not a
        budget.

        Unknown keys are an error for the reason `PolicySnapshot.from_toml`
        gives: `max_concurency = 4` must not be a tenant silently running at
        the default. A token that appears under two tenants is an error too --
        the lookup would pick one, and "which tenant did this request bill"
        would depend on dictionary order.
        """
        if env is None:
            env = os.environ
        try:
            doc = tomllib.loads(text)
        except tomllib.TOMLDecodeError as exc:
            raise ValueError(f"tenants file is not valid TOML: {exc}") from exc
        unknown = set(doc) - {"tenants"}
        if unknown:
            raise ValueError(f"tenants file: unknown top-level keys {sorted(unknown)}")
        table = doc.get("tenants")
        if not isinstance(table, dict) or not table:
            raise ValueError("tenants file: expected at least one [tenants.<id>] table")

        limits: dict[str, TenantLimits] = {}
        tokens: dict[str, str] = {}
        realtime: dict[str, Mapping[str, Any]] = {}
        allowed = {"tokens", "token_env", "token_envs",
                   "rate_per_second", "burst", "max_concurrency",
                   "max_sessions", "realtime"}
        for tenant, entry in table.items():
            if not isinstance(entry, dict):
                raise ValueError(f"[tenants.{tenant}] must be a table")
            extra = set(entry) - allowed
            if extra:
                raise ValueError(f"[tenants.{tenant}]: unknown keys {sorted(extra)}")
            missing = {"rate_per_second", "burst", "max_concurrency"} - set(entry)
            if missing:
                raise ValueError(f"[tenants.{tenant}]: missing {sorted(missing)}")
            try:
                limits[tenant] = TenantLimits(
                    rate_per_second=float(entry["rate_per_second"]),
                    burst=entry["burst"],
                    max_concurrency=entry["max_concurrency"],
                    max_sessions=entry.get("max_sessions"),
                ).validate()
            except (TypeError, ValueError) as exc:
                raise ValueError(f"[tenants.{tenant}]: {exc}") from exc
            pin = entry.get("realtime")
            if pin is not None:
                if not isinstance(pin, dict):
                    raise ValueError(f"[tenants.{tenant}.realtime] must be a table")
                bad = set(pin) - REALTIME_PIN_KEYS
                if bad:
                    raise ValueError(
                        f"[tenants.{tenant}.realtime]: unknown keys {sorted(bad)}; "
                        f"allowed {sorted(REALTIME_PIN_KEYS)}"
                    )
                cap = pin.get("expires_after_seconds_cap")
                if cap is not None and (type(cap) is not int or cap < 10):
                    raise ValueError(
                        f"[tenants.{tenant}.realtime]: expires_after_seconds_cap must be "
                        f"an integer >= 10"
                    )
                model = pin.get("model")
                if model is not None and (not isinstance(model, str) or not model):
                    raise ValueError(f"[tenants.{tenant}.realtime]: model must be a string")
                realtime[tenant] = MappingProxyType(dict(pin))

            names: list[str] = []
            if "token_env" in entry:
                names.append(entry["token_env"])
            if "token_envs" in entry:
                envs = entry["token_envs"]
                if not isinstance(envs, list):
                    raise ValueError(f"[tenants.{tenant}]: token_envs must be a list")
                names.extend(envs)
            resolved: list[str] = []
            for name in names:
                if not isinstance(name, str) or not name.strip():
                    raise ValueError(
                        f"[tenants.{tenant}]: token_env names must be non-empty strings"
                    )
                value = env.get(name)
                if value is None or not value.strip():
                    # The variable NAME is safe to print; it is what the
                    # operator has to go and set.
                    raise ValueError(
                        f"[tenants.{tenant}]: token_env {name!r} is unset or empty; "
                        f"set it in the environment or drop it from the file"
                    )
                resolved.append(value)

            literal = entry.get("tokens", [])
            if not isinstance(literal, list):
                raise ValueError(f"[tenants.{tenant}]: tokens must be a list")
            for token in [*literal, *resolved]:
                if not isinstance(token, str) or not token.strip():
                    raise ValueError(f"[tenants.{tenant}]: tokens must be non-empty strings")
                if token in tokens:
                    # Name the tenants, never the token: this message ends up
                    # in a startup log.
                    raise ValueError(
                        f"[tenants.{tenant}]: a token is already assigned to "
                        f"tenant {tokens[token]!r}"
                    )
                tokens[token] = tenant
            if tenant != ANONYMOUS_TENANT and not literal and not resolved:
                raise ValueError(
                    f"[tenants.{tenant}]: no tokens; give it `tokens` or `token_env` "
                    f"(only [tenants.{ANONYMOUS_TENANT}] may have none)"
                )
        return cls(limits=limits, _tokens=tokens, realtime=realtime)

    @property
    def authenticated_tenants(self) -> int:
        """How many tenants a bearer token can name. What `require_tenants`
        checks: a table of only `[tenants.anonymous]` is a table in which
        every request is still one tenant."""
        return len(set(self._tokens.values()))

    def resolve(self, token: str | None) -> str | None:
        """The tenant id a bearer token names, or None.

        `None` for no token is NOT the anonymous tenant: that decision
        belongs to the caller, which knows whether `anonymous` was configured.
        An unknown token is also `None` and is never downgraded to anonymous
        -- a rotated-out key that still gets guest access is a key that was
        never really rotated.
        """
        if token is None:
            return None
        return self._tokens.get(token)

    def __contains__(self, tenant: str) -> bool:
        return tenant in self.limits

    def realtime_pin(self, tenant: str) -> Mapping[str, Any] | None:
        """The tenant's pinned Realtime session fields, or None."""
        return self.realtime.get(tenant)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid; never the tokens
        return f"<TenantTable tenants={sorted(self.limits)} tokens={len(self._tokens)}>"


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:
        # Loud, at startup. A budget silently falling back to its default
        # because someone wrote `10s` instead of `10` is a timeout that is not
        # the timeout anyone configured, discovered during an incident.
        raise ValueError(f"{name}={raw!r} is not a number") from exc


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    return int(_env_float(env, name, float(default)))


def _env_optional_int(env: Mapping[str, str], name: str, default: int) -> int | None:
    """An integer cap where `0` means "no cap". Unset keeps the default, so
    the only way to switch a cap off is to say so explicitly."""
    value = _env_int(env, name, default)
    return None if value == 0 else value


def _env_headers(
    env: Mapping[str, str], name: str, default: tuple[str, ...]
) -> tuple[str, ...]:
    raw = env.get(name)
    if raw is None:
        return default
    return tuple(part.strip().lower() for part in raw.split(",") if part.strip())


def _env_bool(env: Mapping[str, str], name: str, default: bool = False) -> bool:
    raw = env.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True, slots=True)
class ServerConfig:
    """Everything the HTTP layer needs, resolved once at startup.

    Frozen for the same reason `Budgets` is: a request routed under one
    configuration and timed under another is a bug that cannot be reproduced,
    because by the time you look the configuration has moved again.
    """

    host: str = "127.0.0.1"
    port: int = 8800

    budgets: Budgets = field(
        default_factory=lambda: Budgets(
            total=120.0,
            connect=2.0,
            first_event=20.0,
            progress=15.0,
            client_stall=30.0,
            **_HEADERS_DEFAULT,
        )
    )
    """The budgets of the ONE workload the zero-config path builds.

    Read carefully, because P3 narrowed what this field does. A request's
    `Deadline` is created from `plan.budgets.total` and every phase inside it
    is bounded by the same plan's budgets -- and the plan comes from the
    snapshot. With `policy_file` set, these numbers are handed to nobody: the
    workload's own `[budgets]` table governs, and a per-workload total is the
    entire point of having workloads (an autocomplete path where a slow answer
    is worse than no answer cannot share a 600 s total with a batch summariser).

    With no policy file these ARE the workload's budgets, passed straight into
    `single_target`, which is what keeps the zero-config path identical to P2.

    `total` is 120 s, down from the 600 s the first six phases shipped with,
    because the total is also the longest stream a deploy has to wait for:
    `validated()` refuses a total above `drain_grace_seconds`, and a 10-minute
    ceiling would have forced a 10-minute drain on every rollout. A workload
    that genuinely needs longer says so in its own `[budgets]` table -- and
    then owns the drain it implies.
    """

    buffer_bytes: int = 256 * 1024
    """The pump's byte ceiling, per in-flight stream. Capacity, not tuning:
    N concurrent streams cost N x this in the worst case, which is the number
    the scale tier models."""

    max_frame_bytes: int = 1 << 20
    """One SSE frame's bound. A frame we cannot bound is a frame we cannot
    resynchronise after, so exceeding it kills the request rather than
    skipping the frame -- see `errors.FrameTooLarge`."""

    max_request_bytes: int = 32 * 1024 * 1024
    """Client request bodies over this get a 413 before any upstream work.

    32 MiB since Phase B (was 4 MiB, a text-era number): base64 vision and PDF
    bodies routinely run to 10 MiB, Anthropic accepts 32 MB and OpenAI 512 MB,
    and a 10 MiB vision call through the chat surface was the B exit criterion
    that the 4 MiB cap failed on 18 Sep 2026. The memory bound is per request
    (the body is buffered so a pre-commit retry can resend it), so the worst
    case is max_streams x this cap; lower it per surface with
    LLMGW_MAX_REQUEST_BYTES__<SURFACE> where a route never needs it.
    Checked while reading, never after: a limit enforced on an already
    assembled body is a limit that allocated the thing it was protecting
    against."""

    max_response_bytes: int = 8 * 1024 * 1024
    """Bound on the NON-streaming path only, where we buffer the whole body
    in order to send an honest `content-length`. The streaming path is bounded
    by `buffer_bytes` + `max_frame_bytes` and has no total-size limit, because
    a long answer is not a large one."""

    surface_limits: Mapping[str, SurfaceLimits] = field(
        default_factory=lambda: dict(DEFAULT_SURFACE_LIMITS)
    )
    """Per-surface deltas over the two caps above (PLAN-2 B4), keyed by
    `Surface.name`; a `None` in a row inherits the global. Shipped:
    Anthropic messages at 32 MiB requests (the provider's own ceiling; base64
    PDFs and images arrive at that size); chat is the globals. Voice surfaces
    add rows in Phase D. Env: `LLMGW_MAX_REQUEST_BYTES__<SURFACE_NAME_UPPER>`
    / `LLMGW_MAX_RESPONSE_BYTES__<SURFACE_NAME_UPPER>` (two underscores, e.g.
    `LLMGW_MAX_REQUEST_BYTES__ANTHROPIC_MESSAGES`) sets one number on one
    row; the plain `LLMGW_MAX_REQUEST_BYTES` sets the global every row without
    its own number inherits. Read through `limits_for()`, never directly."""

    catalog: Catalog = field(default_factory=lambda: DEFAULT_CATALOG)

    forward_request_headers: tuple[str, ...] = DEFAULT_FORWARD_REQUEST_HEADERS
    """Client REQUEST headers copied onto the upstream request.

    An allowlist, and a short one. Forwarding everything a client sent is the
    obvious implementation and it is three bugs at once:

    * `authorization` / `x-api-key` would overwrite the provider credential
      `build_headers()` just fetched -- extras are merged last precisely so an
      operator can override, which means an allowlist with an auth header in
      it hands the client control of who we authenticate as. `validated()`
      refuses those names outright rather than trusting the default.
    * `host`, `content-length` and `accept-encoding` describe the CLIENT's
      connection to us, not ours to the provider, and forwarding them produces
      a request that is wrong in ways h11 will not catch.
    * every unexpected header is a request the provider may reject for
      reasons no log line connects to the client that caused it.

    The defaults are the ones that carry meaning end to end: the two vendor
    beta-feature switches, and trace context. Tests extend this to drive the
    fake upstream's `X-Fake-*` mode selectors -- which is why the list is
    configuration rather than a constant.
    """

    fake_upstreams: bool = False
    """Whether `catalog` has been pointed at the local fakes.

    Carried as a field rather than inferred, so `/workloads/{w}/probe` can say
    it out loud. A gateway that is quietly talking to a fake is the single
    most embarrassing way to pass a load test, and the cure is that the fact
    is reported next to the base URL rather than deduced from it.
    """

    policy_file: str | None = None
    """Path to a `workloads.toml`, or None for the zero-config path.

    The whole policy source, deliberately one field. `LLMGW_POLICY_FILE` set
    means `PolicySnapshot.from_toml` and a real routing namespace; unset means
    `single_target(default_model)` and the P2 behaviour it replaces. There is
    no third mode and no partial merge of the two: a config where half the
    routing comes from a file and half from environment defaults is a config
    whose behaviour you cannot read off either source.
    """

    default_model: str = "fake.echo"
    """The incumbent of the one workload the zero-config path builds.

    In P2 this was a stand-in for a policy lookup that did not exist. It is
    now a real input to `PolicySnapshot.single_target`, which is why the
    zero-config path routes identically to P2's: same model, same budgets,
    same single target -- with a content-addressed `policy_id` instead of the
    static `"p2-static"` string this field used to sit next to.
    """

    workload_id: str = "default"
    """The name that one zero-config workload answers to.

    Reported as `X-Gw-Workload-Id` and as the snapshot's `default_workload`.
    With no policy document there is nothing for a caller's workload name to
    disagree with, so this is the name every request resolves to; see
    `app.Gateway.resolve_workload`.
    """

    http2: bool = True
    """Passed to the upstream pool. Irrelevant against the plaintext local
    fakes (httpx negotiates h2 over ALPN, which needs TLS) and correct against
    real providers, so there is no reason for it to differ between them."""

    tenants_file: str | None = None
    """Path to a `tenants.toml`, or None for the single-anonymous-tenant path.

    One field, two modes, no third -- the same shape as `policy_file` and for
    the same reason. Set: bearer tokens are looked up in the file and an
    unknown one is a 401. Unset: every request is `ANONYMOUS_TENANT` under
    `tenant_limits`, tokens are ignored, and the fact is reported by `/probe`
    as `tenant_mode` so a deployment that forgot to set this is visible from
    the outside rather than deduced from the absence of 401s.
    """

    require_tenants: bool = False
    """Refuse to start on the zero-config tenant path (`LLMGW_REQUIRE_TENANTS`).

    The zero-config path exists so `make run` + `curl` works on a clean
    checkout; it is the wrong default for anything with a second caller, and
    `/probe` saying `"tenant_mode": "anonymous"` is a fact nobody reads
    during an incident. Set, the process refuses to start unless
    `tenants_file` is set AND the table it loads has at least one tenant a
    bearer token can name -- a file of only `[tenants.anonymous]` is the
    zero-config path with extra steps. Default False so the adoption path
    stays; the production scaffold sets it.
    """

    tenant_limits: TenantLimits = DEFAULT_TENANT_LIMITS
    """The anonymous tenant's budget when there is no tenants file. Unused
    when there is one: a file that wants an anonymous tenant configures it
    explicitly as `[tenants.anonymous]`, so there is never a request admitted
    under limits that appear in neither the file nor the probe."""

    breaker: BreakerPolicy = field(default_factory=BreakerPolicy)
    """One policy for every circuit in the process. Per-key tuning is a thing
    the registry could support and this config deliberately does not: the
    thresholds are guesses until S7 measures them, and a per-key
    table of guesses is a per-key table of things to get wrong."""

    capture_path: str | None = None
    """Where per-request capture records go, or None for the `NullSink`.

    The same one-field-two-modes shape as `policy_file` and `tenants_file`.
    Set (`LLMGW_CAPTURE_PATH`): the process opens a `FileSink` at this path and
    every request cuts one JSON line to it. Unset (the default): a `NullSink`
    drops them, so the zero-config path pays nothing for capture it did not ask
    for. Capture is a log you query, never a series you scrape -- the
    high-cardinality per-request facts a metric label is forbidden from
    carrying live here (see `capture.py` and the top of `metrics.py`)."""

    capture_queue_bytes: int = 8 * 1024 * 1024
    """Byte ceiling on the capture worker's queue (`LLMGW_CAPTURE_QUEUE_BYTES`).

    Bounded in BYTES, not records, for the reason `capture.Capture` states: a
    count bound waves through one oversized record and blows the memory budget
    while reading a reassuring "1 queued". This is the whole of capture's memory
    footprint, so it is capacity the scale tier multiplies -- a field, not a
    constant. Over budget the record is DROPPED and counted, never awaited: a
    lost diagnostic beats a blocked request (FAILURE-MODES row 9)."""

    drain_grace_seconds: float = 130.0
    """How long a graceful shutdown waits for in-flight streams before it gives
    up and lets the deadline cut whatever is left (`LLMGW_DRAIN_GRACE`).

    A DEPLOY property, not a request one: on SIGTERM the process flips to
    draining, `/healthz` answers 503 so the load balancer stops sending work,
    and open streams are allowed to finish -- but a reasoning stream can run
    for minutes, and a deploy cannot wait on the slowest one forever. This is
    the bound on that wait. A stream still open when the grace expires is CUT,
    which is FAILURE-MODES.md row 11's honest residual: a graceful drain is not
    a promise that no stream is ever interrupted, only that none is interrupted
    that could have finished within the grace.

    The three numbers that have to agree, and who checks each:

        budgets.total  <=  drain_grace_seconds  <  orchestrator kill timeout

    The first inequality is checked here, in `validated()`: a total above the
    grace means the gateway admits streams it has already decided to cut on
    the next deploy, and the P7 campaign shipped exactly that (600 s total,
    25 s grace) for six phases without any test noticing. The default is the
    120 s total plus ten seconds for the exit itself. The second inequality
    -- Fly's `kill_timeout`, Kubernetes' `terminationGracePeriodSeconds`,
    systemd's `TimeoutStopSec` -- cannot be checked from inside the process,
    because the process cannot see it; it is the deployment's job to keep the
    kill timeout above this grace, or a SIGKILL, not the drain, ends the
    process and every stream still open with it."""

    ws_drain_wait_s: float = 20.0
    """How long a drain waits for ONE WebSocket session to end politely
    (`LLMGW_WS_DRAIN_WAIT`, PLAN-G 4.3, C26).

    The grace above is the whole process's budget; this is the per-session
    slice inside it. A socket cannot be drained the way a request is, by
    waiting for it to finish, because it was never going to finish -- a TTS
    connection is idle between utterances and a transcription session ends
    when the learner stops talking. So the drain hook sends the provider's
    OWN terminate (`closeStream`, `Terminate`; Inworld TTS has none, so it
    waits for open contexts to reach zero), waits at most this long for the
    provider's answer -- which carries the billing number on two of the four
    products -- and then closes the client with 4900.

    Its inequality is `ws_drain_wait_s <= drain_grace_seconds`, checked in
    `check_drain_arithmetic`. Above the grace the wait would be cut by the
    grace anyway, and the terminate would be sent to a provider nobody is
    listening to -- so the number would be a fiction, and the session's
    seconds would silently become `estimated` on every deploy."""

    inject_include_usage: bool = False
    """Add `stream_options: {include_usage: true}` to OpenAI-dialect streaming
    requests that did not set `stream_options` (`LLMGW_INJECT_INCLUDE_USAGE`).

    Without the flag the OpenAI dialect streams no usage frame at all, so
    every streamed call bills by the byte estimate (`cost_basis=estimated`,
    finding 27). Off by default because it is a second edit to the client's
    bytes -- announced by `X-Gw-Body-Modified: 1` like the model rewrite --
    and the byte-exact passthrough contract (`test_model_rewrite`) is the
    default's to keep. Production sets it: an estimated bill is the worse
    default there. Only OpenAI-kind providers, only when `stream` is true.
    """

    drain_allow_short: bool = False
    """Escape hatch for the `total <= grace` check (`LLMGW_DRAIN_ALLOW_SHORT`).

    Set, a config whose total exceeds the grace starts anyway and logs a
    WARNING with both numbers. For the bench, which deliberately drains
    shorter than the streams it is serving to observe the cut, and for an
    operator who has decided a fast rollout is worth cutting the long tail.
    Off by default because the failure it permits -- every deploy truncates
    the longest streams -- is one that a green dashboard will not show."""

    max_streams: int | None = 150
    """Per-process ceiling on requests inside the serving path
    (`LLMGW_MAX_STREAMS`). `None` is uncapped; the env var takes `0` to mean
    the same, because an unset variable has to keep the default.

    The shed path the load campaign showed was missing. S2
    (`bench/results/load-S2-gw4-run.md`) offered four processes ~2,500
    streams at 100 rps: each pinned at 100 % CPU around 650 open streams,
    first-event latency went from 29 ms to 2.6 s at p50, and only THEN did the
    504s start -- the gateway degraded first and shed second, which is the
    wrong order for a proxy whose job is to never be the slow hop. Nothing
    per-process existed to refuse the 651st stream: admission is per tenant,
    the key cap per credential, and a fleet of well-behaved tenants can still
    sum past one event loop.

    Checked at ingress, after the tenant is known and BEFORE tenant admission,
    so a shed request costs no bucket credit (C6) and no body read. Refused
    with 503 `overloaded` and `Retry-After: 1`: retry elsewhere, not later
    against this replica. `/healthz`, `/metrics` and `/probe` are not subject
    to it -- the cap exists so those keep answering.

    The default is a measured number, not a guess, and it is a number for
    ONE machine. The cap is a STREAM count standing in for the thing that
    actually saturates a process, which is events per second: 150 slow-drip
    streams are idle and 150 fast-model streams are 6,000 events/s. On the
    16-core laptop the campaign ran on, with S2's 40 events/s streams, the
    knee sat between the two runs that bracket it:

      cap 300 (`bench/results/load-S2-cap300-gw4-run.md`): streams pinned at
        exactly 300 per process, but every worker still at 100 % CPU and
        3,217 requests timing out with 504 -- shedding, and still degraded.
      cap 150 (`bench/results/load-S2-cap150-gw4-run.md`): 68 % CPU, zero
        504s, and the admitted streams indistinguishable from the direct arm
        (first-event p90 30.9 ms vs 31.6 ms, inter-event p99 37 vs 36 ms).

    So 150 is the largest cap at which that machine shed BEFORE it degraded.
    A Fly `shared-cpu-1x` is a fraction of one of those cores and will need a
    lower number; a fast model with 200 events/s streams needs a lower number
    on the same hardware. Derive it per deployment from an S2-style run
    (`python -m bench.load --scenario S2` with `BENCH_GW_MAX_STREAMS`): the
    right cap is the largest one where CPU stays off 100 % and admitted
    first-event latency stays flat. Then pair it with the edge's connection
    limit so the balancer stops routing before the process has to refuse."""

    def validated(self) -> ServerConfig:
        """Fail at startup, never on the request path.

        `Budgets.validate()` catches the decorative-timeout case -- a phase
        budget larger than the total can never fire, so it protects nothing
        while looking like it does.

        The header check is the one with teeth. A gateway that can be
        configured to forward `authorization` upstream is a gateway where a
        client picks which credential the provider sees, and the misconfig
        looks completely reasonable in a diff.
        """
        self.budgets.validate()
        for name in ("buffer_bytes", "max_frame_bytes", "max_request_bytes",
                     "max_response_bytes", "port", "capture_queue_bytes"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be positive")
        # A float, so it is checked apart from the integer loop above: a grace
        # of zero would drain nothing and cut every in-flight stream the
        # instant SIGTERM arrived, which is the opposite of the endpoint's job.
        if self.drain_grace_seconds <= 0:
            raise ValueError("drain_grace_seconds must be positive")
        if self.ws_drain_wait_s <= 0:
            raise ValueError("ws_drain_wait_s must be positive")
        if self.budgets.total > self.drain_grace_seconds:
            # The deploy inequality (see the `drain_grace_seconds` docstring).
            # Refused, not clamped: clamping the total would silently shorten
            # every request to fit the deploy, and clamping the grace would
            # silently lengthen every deploy to fit the request; either is a
            # number nobody configured.
            message = (
                f"budgets.total={self.budgets.total:g}s exceeds "
                f"drain_grace_seconds={self.drain_grace_seconds:g}s: every stream "
                f"longer than the grace is cut on deploy. Raise LLMGW_DRAIN_GRACE "
                f"(and the orchestrator's kill timeout above it), lower "
                f"LLMGW_BUDGET_TOTAL, or set LLMGW_DRAIN_ALLOW_SHORT=1 to accept the cut"
            )
            if not self.drain_allow_short:
                raise ValueError(message)
            log.warning("LLMGW_DRAIN_ALLOW_SHORT is set: %s", message)
        for name, limits in self.surface_limits.items():
            limits.validate(name)
        if self.max_streams is not None and self.max_streams < 1:
            # In code, None is the spelling for "uncapped"; a zero would refuse
            # every request and look like an outage with no log line.
            raise ValueError("max_streams must be positive, or None for no cap")
        banned = {h.lower() for h in self.forward_request_headers} & NEVER_FORWARDED
        if banned:
            raise ValueError(
                f"forward_request_headers may not contain {sorted(banned)}: "
                "credential and connection headers are ours, not the client's"
            )
        if self.require_tenants and self.tenants_file is None:
            raise ValueError(
                "LLMGW_REQUIRE_TENANTS is set but LLMGW_TENANTS_FILE is not: refusing "
                "to start on the anonymous-tenant path. Point LLMGW_TENANTS_FILE at a "
                "tenants.toml with at least one tenant, or unset LLMGW_REQUIRE_TENANTS"
            )
        if self.require_tenants and not self.fake_upstreams and not self.has_policy_document:
            # LLMGW_REQUIRE_TENANTS is the production switch, and in production
            # the zero-policy default route MUST NOT point at a fake. The code
            # default is `fake.echo` (127.0.0.1:8801), so a fly.toml that
            # forgets LLMGW_DEFAULT_MODEL would 502 every request with a
            # healthy /healthz. Found while writing that fly.toml, 16 Sep 2026.
            spec = self.catalog.models.get(self.default_model)
            provider = self.catalog.providers.get(spec.provider) if spec else None
            if provider is not None and (
                provider.id.startswith("fake-")
                or "127.0.0.1" in (provider.base_url or "")
                or "localhost" in (provider.base_url or "")
            ):
                raise ValueError(
                    f"LLMGW_REQUIRE_TENANTS is set but LLMGW_DEFAULT_MODEL="
                    f"{self.default_model!r} routes to the fake provider "
                    f"{provider.id!r} ({provider.base_url}): set LLMGW_DEFAULT_MODEL "
                    f"to a real catalog model or ship a LLMGW_POLICY_FILE"
                )
        self.tenant_limits.validate()
        self.breaker.validate()
        return self

    # ------------------------------------------------------------- surfaces

    def limits_for(self, surface_name: str) -> SurfaceLimits:
        """The byte caps for one surface, fully resolved: its row's numbers
        where the row set them, the globals everywhere else."""
        row = self.surface_limits.get(surface_name) or SurfaceLimits()
        return row.resolved(request=self.max_request_bytes, response=self.max_response_bytes)

    # ---------------------------------------------------------------- drain

    def check_drain_arithmetic(self, snapshot: PolicySnapshot) -> None:
        """The deploy inequality against the LARGEST total any workload or
        profile in the snapshot can hand a request (PLAN-2 B6).

        `validated()` checks `budgets.total` -- the zero-config workload's --
        but with a policy file that number is handed to nobody: each workload
        has its own total, and a `[profiles.long_context]` at 600 s behind a
        130 s grace is cut on every deploy while the global check stays
        green. Called by `app.Gateway` right after the snapshot is built, so
        a bad file still refuses at startup and never on the request path.
        `PolicySnapshot.largest_total()` is the policy side's answer; a
        snapshot without it (older policy.py) is measured over its workloads.

        The SECOND inequality, added with the socket plane (PLAN-G 4.3):

            LLMGW_WS_DRAIN_WAIT (20) <= LLMGW_DRAIN_GRACE (130)

        and `session_total` is deliberately not part of either. A session
        longer than the grace is normal and is handled by the drain hook, so
        it is reported at INFO -- the operator should know a deploy will cut
        mid-session sockets, and should not be refused a startup for it.
        """
        if self.ws_drain_wait_s > self.drain_grace_seconds:
            # Refused for the same reason the first inequality is: a per-
            # session wait above the whole process's grace is a number that
            # can never be honoured, and the visible consequence is a
            # provider terminate sent into a drain that is already over.
            # `LLMGW_DRAIN_ALLOW_SHORT` covers it too, because it is the same
            # decision -- "I have decided a fast rollout is worth cutting the
            # tail" -- and the bench deliberately drains shorter than the work
            # it is serving in order to observe the cut.
            message = (
                f"ws_drain_wait_s={self.ws_drain_wait_s:g}s exceeds "
                f"drain_grace_seconds={self.drain_grace_seconds:g}s: a session's "
                f"polite-close wait cannot outlast the whole drain. Lower "
                f"LLMGW_WS_DRAIN_WAIT or raise LLMGW_DRAIN_GRACE"
            )
            if not self.drain_allow_short:
                raise ValueError(message)
            log.warning("LLMGW_DRAIN_ALLOW_SHORT is set: %s", message)
        sessions = [
            (name, b.session_total)
            for name, b in getattr(snapshot, "profiles", {}).items()
            if getattr(b, "session_total", None) is not None
            and b.session_total > self.drain_grace_seconds
        ]
        for name, total in sessions:
            log.info(
                "profile %r has session_total=%gs above drain_grace_seconds=%gs: "
                "sockets of that profile outlive a deploy and are closed 4900 by "
                "the drain hook within ws_drain_wait_s=%gs (PLAN-G C26), not cut",
                name, total, self.drain_grace_seconds, self.ws_drain_wait_s,
            )
        largest = getattr(snapshot, "largest_total", None)
        if callable(largest):
            total = float(largest())
        else:
            totals = [w.budgets.total for w in snapshot.workloads.values()]
            total = max(totals) if totals else self.budgets.total
        if total > self.drain_grace_seconds:
            message = (
                f"the largest workload/profile total in the policy is {total:g}s, above "
                f"drain_grace_seconds={self.drain_grace_seconds:g}s: streams of that "
                f"workload longer than the grace are cut on deploy. Raise "
                f"LLMGW_DRAIN_GRACE (and the orchestrator's kill timeout above it), "
                f"lower that workload's total, or set LLMGW_DRAIN_ALLOW_SHORT=1"
            )
            if not self.drain_allow_short:
                raise ValueError(message)
            log.warning("LLMGW_DRAIN_ALLOW_SHORT is set: %s", message)

    # -------------------------------------------------------------- tenants

    @property
    def has_tenant_document(self) -> bool:
        """Is there a file that defines the tenant namespace? Decides whether
        a bearer token means anything -- see `app.Gateway.resolve_tenant`."""
        return self.tenants_file is not None

    def tenant_table(self, *, env: Mapping[str, str] | None = None) -> TenantTable | None:
        """The table this process starts with, or None on the zero-config path.

        Read once, at startup, by `app.Gateway`; a malformed file is a process
        that refuses to start. `OSError` is converted so the two ways a file
        can be wrong -- missing and nonsense -- are one `ValueError` with the
        path in it, which is what a startup log needs. `env` is where
        `token_env` names resolve (the process environment unless a test says
        otherwise). With `require_tenants`, a table nobody can authenticate
        against is refused here too.
        """
        if self.tenants_file is None:
            return None
        try:
            text = Path(self.tenants_file).read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(
                f"tenants file {self.tenants_file!r} could not be read: {exc}"
            ) from exc
        try:
            table = TenantTable.from_toml(text, env=env)
        except ValueError as exc:
            raise ValueError(f"tenants file {self.tenants_file!r}: {exc}") from exc
        if self.require_tenants and table.authenticated_tenants == 0:
            raise ValueError(
                f"LLMGW_REQUIRE_TENANTS is set but tenants file {self.tenants_file!r} "
                f"defines no tenant a bearer token can name (only "
                f"[tenants.{ANONYMOUS_TENANT}]): add a tenant with `token_env`"
            )
        return table

    # --------------------------------------------------------------- policy

    @property
    def has_policy_document(self) -> bool:
        """Is there a file that defines the workload namespace?

        Read by `app.Gateway.resolve_workload`, and the reason it is a
        property here rather than an `is not None` at the call site: whether
        an unknown workload name is a 400 or a label is a *deployment*
        property, and it should be answered by the object that knows how this
        deployment was configured.
        """
        return self.policy_file is not None

    def policy_snapshot(self, *, clock: Clock | None = None) -> PolicySnapshot:
        """Build the snapshot this process starts with. Raises `PolicyError`.

        Called once, at startup, by `app.Gateway`. Every failure -- a missing
        file, unreadable bytes, a typo'd key, a model id that does not resolve
        -- is a `PolicyError` raised here, where the process refuses to start,
        rather than on the request path where it would be a tenant's traffic.
        That is the same argument `PolicySnapshot.__post_init__` makes one
        layer down, extended over the one step it cannot see: reading the file.

        `OSError` is converted rather than propagated because a reload (P4)
        wants one class to catch and one `code` to count. "The policy file
        vanished" and "the policy file is nonsense" are the same operational
        event -- the running snapshot keeps serving and someone is paged --
        and giving them two exception types means two handlers that will
        eventually disagree.
        """
        if self.policy_file is None:
            return PolicySnapshot.single_target(
                self.default_model,
                catalog=self.catalog,
                budgets=self.budgets,
                clock=clock,
                workload_id=self.workload_id,
            )
        try:
            text = Path(self.policy_file).read_text(encoding="utf-8")
        except OSError as exc:
            raise PolicyError(
                f"policy file {self.policy_file!r} could not be read: {exc}",
                cause=exc,
            ) from exc
        return PolicySnapshot.from_toml(text, catalog=self.catalog, clock=clock)

    # ------------------------------------------------------------------ env

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> ServerConfig:
        """Build from `LLMGW_*` environment variables.

        Reads `os.environ` by default. The parameter exists so a test can
        exercise the parsing without mutating process state -- which is the
        difference between a test that can run beside another and one that
        cannot.

        `LLMGW_FAKE_UPSTREAMS` is the whole local-dev story and it defaults
        OFF. The zero-config convenience of defaulting it on is real, and it
        is still the wrong default: a configuration whose failure mode is
        "silently talks to a fake and returns plausible answers" is one that
        eventually ships. Defaulting off means the failure mode is a loud
        `ConnectionFailed` or a missing credential, which is the direction an
        unsafe default should fail in.

        `make run` passes `LLMGW_FAKE_UPSTREAMS=1` explicitly, so the local
        loop is unchanged -- the flag is simply written down where a reader
        can see it rather than living in a default nobody re-reads. `probe`
        reports the resolved value next to the base URL either way.
        """
        env = os.environ if env is None else env
        budgets = Budgets(
            total=_env_float(env, "LLMGW_BUDGET_TOTAL", 120.0),
            connect=_env_float(env, "LLMGW_BUDGET_CONNECT", 2.0),
            first_event=_env_float(env, "LLMGW_BUDGET_FIRST_EVENT", 20.0),
            progress=_env_float(env, "LLMGW_BUDGET_PROGRESS", 15.0),
            client_stall=_env_float(env, "LLMGW_BUDGET_CLIENT_STALL", 30.0),
            **(
                {"headers": _env_float(env, "LLMGW_BUDGET_HEADERS", 10.0)}
                if _HEADERS_DEFAULT else {}
            ),
        )
        fake = _env_bool(env, "LLMGW_FAKE_UPSTREAMS", default=False)
        catalog = DEFAULT_CATALOG
        if fake:
            catalog = fake_catalog(
                catalog,
                openai_url=env.get("LLMGW_FAKE_OPENAI_URL", DEFAULT_FAKE_OPENAI_URL),
                anthropic_url=env.get("LLMGW_FAKE_ANTHROPIC_URL",
                                      DEFAULT_FAKE_ANTHROPIC_URL),
            )
        return cls(
            host=env.get("LLMGW_HOST", "127.0.0.1"),
            port=_env_int(env, "LLMGW_PORT", 8800),
            budgets=budgets,
            buffer_bytes=_env_int(env, "LLMGW_BUFFER_BYTES", 256 * 1024),
            max_frame_bytes=_env_int(env, "LLMGW_MAX_FRAME_BYTES", 1 << 20),
            max_request_bytes=_env_int(env, "LLMGW_MAX_REQUEST_BYTES", 32 * 1024 * 1024),
            max_response_bytes=_env_int(env, "LLMGW_MAX_RESPONSE_BYTES", 8 * 1024 * 1024),
            surface_limits=surface_limits_from_env(env),
            catalog=catalog,
            forward_request_headers=_env_headers(
                env, "LLMGW_FORWARD_REQUEST_HEADERS",
                DEFAULT_FORWARD_REQUEST_HEADERS,
            ),
            fake_upstreams=fake,
            policy_file=env.get("LLMGW_POLICY_FILE") or None,
            default_model=env.get("LLMGW_DEFAULT_MODEL", "fake.echo"),
            workload_id=env.get("LLMGW_WORKLOAD_ID", "default"),
            http2=_env_bool(env, "LLMGW_HTTP2", True),
            tenants_file=env.get("LLMGW_TENANTS_FILE") or None,
            require_tenants=_env_bool(env, "LLMGW_REQUIRE_TENANTS", default=False),
            tenant_limits=TenantLimits(
                rate_per_second=_env_float(
                    env, "LLMGW_TENANT_RATE_PER_SECOND",
                    DEFAULT_TENANT_LIMITS.rate_per_second,
                ),
                burst=_env_int(env, "LLMGW_TENANT_BURST", DEFAULT_TENANT_LIMITS.burst),
                max_concurrency=_env_int(
                    env, "LLMGW_TENANT_MAX_CONCURRENCY",
                    DEFAULT_TENANT_LIMITS.max_concurrency,
                ),
            ),
            breaker=BreakerPolicy(
                failure_threshold=_env_int(env, "LLMGW_BREAKER_FAILURE_THRESHOLD", 5),
                window=_env_float(env, "LLMGW_BREAKER_WINDOW", 30.0),
                cooldown=_env_float(env, "LLMGW_BREAKER_COOLDOWN", 10.0),
                half_open_probes=_env_int(env, "LLMGW_BREAKER_HALF_OPEN_PROBES", 1),
            ),
            capture_path=env.get("LLMGW_CAPTURE_PATH") or None,
            capture_queue_bytes=_env_int(
                env, "LLMGW_CAPTURE_QUEUE_BYTES", 8 * 1024 * 1024
            ),
            drain_grace_seconds=_env_float(env, "LLMGW_DRAIN_GRACE", 130.0),
            ws_drain_wait_s=_env_float(env, "LLMGW_WS_DRAIN_WAIT", 20.0),
            drain_allow_short=_env_bool(env, "LLMGW_DRAIN_ALLOW_SHORT", default=False),
            inject_include_usage=_env_bool(
                env, "LLMGW_INJECT_INCLUDE_USAGE", default=False
            ),
            max_streams=_env_optional_int(env, "LLMGW_MAX_STREAMS", 150),
        ).validated()


def surface_limits_from_env(env: Mapping[str, str]) -> dict[str, SurfaceLimits]:
    """The per-surface rows from `LLMGW_MAX_{REQUEST,RESPONSE}_BYTES__<SURFACE>`
    over the shipped table (PLAN-2 B4). Each variable sets ONE number on ONE
    row; everything a row does not set is inherited from the globals at
    `limits_for()` time, which is how the plain `LLMGW_MAX_*_BYTES` variables
    keep meaning what they always meant. Surface names are lower-cased from
    the suffix. An unknown suffix is accepted and kept (a surface that lands
    later reads it); a non-integer value is a `ValueError` at startup like
    every other knob.
    """
    table: dict[str, SurfaceLimits] = dict(DEFAULT_SURFACE_LIMITS)
    prefixes = (("LLMGW_MAX_REQUEST_BYTES__", "max_request_bytes"),
                ("LLMGW_MAX_RESPONSE_BYTES__", "max_response_bytes"))
    for key, value in env.items():
        for prefix, attr in prefixes:
            if not key.startswith(prefix) or not value.strip():
                continue
            name = key[len(prefix):].lower()
            try:
                n = int(value)
            except ValueError as exc:
                raise ValueError(f"{key}={value!r} is not an integer") from exc
            table[name] = replace(table.get(name) or SurfaceLimits(), **{attr: n})
    return table


def fake_catalog(
    catalog: Catalog = DEFAULT_CATALOG,
    *,
    openai_url: str = DEFAULT_FAKE_OPENAI_URL,
    anthropic_url: str = DEFAULT_FAKE_ANTHROPIC_URL,
    key_env: str = FAKE_KEY_ENV,
    key_value: str = FAKE_KEY_VALUE,
) -> Catalog:
    """Every provider pointed at a local fake, with a credential that exists.

    `redirect_to_fakes()` is called once per provider *kind* because it filters
    on kind -- the OpenAI-shaped fake speaks `/v1/chat/completions` on 8801 and
    the Anthropic-shaped one speaks `/v1/messages` on 8802, and a single call
    would leave half the catalog pointed at production.

    The credential rewrite is the half that is easy to forget and impossible to
    diagnose from the client: `build_headers()` refuses to send an
    unauthenticated request, so a redirected-but-uncredentialed catalog answers
    every call with a 400 `policy_error` naming an environment variable the
    developer has no reason to set for a fake that does not check auth.
    """
    redirected = catalog.redirect_to_fakes(openai_url, kind="openai")
    redirected = redirected.redirect_to_fakes(anthropic_url, kind="anthropic")
    os.environ.setdefault(key_env, key_value)
    providers = {
        pid: replace(conn, api_key_env=key_env)
        for pid, conn in redirected.providers.items()
    }
    return redirected.with_overrides(providers=providers)


__all__ = [
    "ANONYMOUS_TENANT",
    "DEFAULT_FAKE_ANTHROPIC_URL",
    "DEFAULT_FORWARD_REQUEST_HEADERS",
    "DEFAULT_FAKE_OPENAI_URL",
    "DEFAULT_TENANT_LIMITS",
    "FAKE_KEY_ENV",
    "FAKE_KEY_VALUE",
    "NEVER_FORWARDED",
    "ServerConfig",
    "TenantTable",
    "fake_catalog",
    "SurfaceLimits",
    "DEFAULT_SURFACE_LIMITS",
    "surface_limits_from_env",
]

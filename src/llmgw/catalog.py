"""Model catalog: which provider serves a model, what it costs, what it can do.

One table, read by routing, pricing, and policy. The alternative, common in
production codebases, is three tables that drift:

    a TS model map            model ids + an OpenRouter escape hatch
    a per-service catalog     providers, prices, reasoning policy
    a Python Literal union    plus a PROVIDERS tuple and a separate PRICING dict

Adding a model means editing three files in two languages, and forgetting one
does not fail loudly -- it silently prices a call wrong or routes it nowhere.
This module is the single source of truth those three collapse into.

Prices are per MILLION tokens, in USD, each stamped with the date it was
verified. They were taken from a production billing table.

--------------------------------------------------------------------------
Why a price carries a date
--------------------------------------------------------------------------

A hardcoded price table is correct on the day it is written and quietly wrong
forever after. The failure is nasty because it is invisible: nothing errors,
the dashboards stay green, and the cost report is confidently off by 40%.

So `priced_at` is mandatory and `Catalog.stale_prices()` exists to be wired
into a test or a startup warning. A price you cannot date is a price you
cannot defend in a billing dispute.

--------------------------------------------------------------------------
A date is not a proof, and this table proved it
--------------------------------------------------------------------------

On 2026-09-10 the live provider pass checked this table against OpenRouter's
own `/api/v1/models`. Six rates were wrong by between 1.7x and 20x -- and
`stale_prices()` passed every one of them, because the oldest was 105 days
old against a 120-day threshold. **Fresh and wrong at the same time.**

The date column records when someone last LOOKED, which is not the same as
whether they looked correctly, and no threshold can close that gap: shortening
it only makes you re-assert a wrong number more often. The mitigation that
actually works is `live/probe.py`, which reconciles this table against the
providers' own endpoints for free, and the finding worth carrying is smaller
and more general:

    a freshness check tells you a fact is young. Only a source tells you it
    is true.

The `openrouter.*` rows say 2026-09-10 because they were verified against a
source. The rows that still say 2026-05-27 have not been, and the date says
so rather than hiding it.
"""

from __future__ import annotations

import datetime as _dt
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from typing import Any, Literal

ProviderKind = Literal["openai", "anthropic"]

AuthScheme = Literal["bearer", "x-api-key", "raw", "header", "basic"]
"""How the provider wants its credential (PLAN-2 B2).

`bearer`: `Authorization: Bearer <key>` (OpenAI, DeepSeek, OpenRouter, and
Inworld, which accepts it identically to its documented Basic form -- verified
live 2026-09-16). `x-api-key`: the header of that name, plus `anthropic-version`
(Anthropic). `raw`: the bare key in `Authorization` with no scheme word
(AssemblyAI). `header`: a provider-named header carrying the bare key, named
by `ProviderConn.auth_header` (ElevenLabs `xi-api-key`). `upstream.build_headers`
branches on this field; nothing else reads it.
"""

ScrubPolicy = Literal["auth", "all"]
"""Which upstream error bodies are replaced by the gateway's own before they
reach the client (CONTRACTS C11). `auth`: 401/403 only, today's rule. `all`:
every non-2xx body, for providers whose keys are reversible (Inworld's Basic
form) or that echo key fragments outside the auth statuses."""

ForbiddenMeans = Literal["auth", "rate_limit", "policy"]
"""What this provider's 403 means, because it is not the same thing
everywhere: a bad credential (OpenAI region block, Anthropic permission
error, Inworld -- verified live), a rate limit (AssemblyAI REST: 20k requests
per 5 min answers 403), or a plan/voice/model denial (ElevenLabs). The
classifier maps only the first onto the credential breaker."""

Unit = Literal["tokens", "characters", "seconds"]
"""What a model's rates are per million of. Text models are priced per token;
TTS per character; duration-billed STT per second, priced through
`ModelSpec.per_minute` rather than a per-million rate (PLAN-2 B3)."""

ReasoningLevel = Literal["off", "none", "minimal", "low", "medium", "high", "xhigh", "max"]
"""The union of the providers' reasoning-effort vocabularies, as of 2026-09-16.

OpenAI: `none | minimal | low | medium | high | xhigh | max`. DeepSeek:
`none | low | high | max`. `off` is this catalog's own spelling for "send the
provider's disable form". One literal for all three because a per-provider
literal would make the catalog unable to describe a model until someone
extends the type, which is how `medium` and `max` went missing for a quarter.
"""


@dataclass(frozen=True, slots=True)
class ProviderConn:
    """One upstream connection: a base URL, a credential, and its limits."""

    id: str
    kind: ProviderKind
    """Wire protocol, not vendor. Anything OpenAI-compatible -- OpenRouter,
    DeepSeek, Groq, Together, a local vLLM -- is kind "openai" and needs no new
    client code. That is the whole reason the field exists."""

    base_url: str | None = None
    api_key_env: str = ""
    extra_headers: dict[str, str] = field(default_factory=dict)
    extra_body: dict[str, object] = field(default_factory=dict)
    """Provider-specific request-body fields merged in last, so they win.
    OpenRouter's `provider.order` host pinning and DeepSeek's native `thinking`
    toggle both live here rather than in per-provider client subclasses."""

    max_concurrency: int = 64
    """In-flight cap for this credential, enforced separately from any tenant
    cap. Two tenants can each sit inside their own limit and jointly exceed the
    provider's -- see errors.ProviderKeyExhausted."""

    credential_id: str | None = None
    """Identity of the key, for breaker scoping. Defaults to the provider id.
    Under BYOK this becomes per-tenant, so one customer's expired key cannot
    open a breaker against the provider for everyone else."""

    auth_scheme: AuthScheme = "bearer"
    """See `AuthScheme`. `x-api-key` on the Anthropic rows; the others default
    to bearer, which is what `build_headers` always sent them."""

    auth_header: str | None = None
    """The header name for `auth_scheme="header"`; required then, ignored
    otherwise. Validated at construction so a row that says `header` and
    names none fails when the catalog is built, not on the first request."""

    scrub_error_bodies: ScrubPolicy = "auth"
    """See `ScrubPolicy`."""

    forbidden_means: ForbiddenMeans = "auth"
    """See `ForbiddenMeans`. Phase A read this with `getattr`; it is a real
    field now so a voice provider row can declare it."""

    path_prefix: str | None = None
    """A path segment inserted between `base_url` and the surface path
    (PLAN-2 C4). DeepSeek serves strict tools and prefix completion under
    `/beta` on the same host as its main API: `base_url` stays the host,
    `path_prefix="/beta"`, and `upstream.join_url` emits
    `/beta/v1/chat/completions`. None for every other row. Leading slash,
    no trailing slash; validated at construction."""

    stateless_responses: bool = False
    """The provider's `/v1/responses` holds no state between calls AND does
    not say so: DeepSeek answers 200 to a `previous_response_id` or a
    `conversation` and silently drops it (`capabilities/captures-responses.md`,
    probe 12b), so the model replies without the history the client thinks it
    has. When True the Responses surface refuses such a body for this
    provider with a 400 before any socket (PLAN-2 Phase F). False for OpenAI,
    which stores responses and 400s an unknown id itself."""

    def __post_init__(self) -> None:
        if self.path_prefix is not None and (
            not self.path_prefix.startswith("/") or self.path_prefix.endswith("/")
            or self.path_prefix == "/"
        ):
            raise ValueError(
                f"provider {self.id!r}: path_prefix must look like '/beta' "
                f"(leading slash, no trailing slash), got {self.path_prefix!r}"
            )
        if self.auth_scheme == "header" and not self.auth_header:
            raise ValueError(
                f"provider {self.id!r}: auth_scheme='header' needs auth_header"
            )
        if self.auth_scheme != "header" and self.auth_header:
            raise ValueError(
                f"provider {self.id!r}: auth_header is only meaningful with "
                f"auth_scheme='header', got {self.auth_scheme!r}"
            )

    def key(self) -> str:
        return self.credential_id or self.id

    def api_key(self) -> str | None:
        return os.environ.get(self.api_key_env) if self.api_key_env else None


@dataclass(frozen=True, slots=True)
class ModelSpec:
    """One model, priced and described."""

    id: str
    provider: str
    api_model: str
    """The id the provider's own API expects. Often differs from ours: the
    DeepSeek platform wants `deepseek-v4-pro`, OpenRouter wants
    `deepseek/deepseek-v4-pro`. Keeping our id stable while the wire id varies
    is what lets a workload move between providers without touching config."""

    input_per_m: float
    output_per_m: float
    cached_input_per_m: float | None = None
    """Cache-read rate. None means caching is unsupported, and callers must
    fall back to input_per_m rather than to zero -- see price_of()."""

    cache_write_per_m: float | None = None
    context_window: int = 128_000
    max_output: int = 8_192
    can_reason: bool = False
    reasoning: ReasoningLevel | None = None
    priced_at: str = ""
    """ISO date the rates were last verified. Mandatory in practice."""

    aliases: tuple[str, ...] = ()
    """Other strings that name this model, resolvable by `Catalog.resolve`.

    The provider's wire id (`api_model`) is always an alias implicitly; this
    field is for the ids a provider *returns* that differ from the one it
    accepts -- OpenAI answers `gpt-4o-mini-2024-07-18` to a request for
    `gpt-4o-mini` -- and for retired ids still in callers' configs. The reason
    it exists is the multi-turn loop: an SDK that copies the response `model`
    into its next request sends the wire id back, and a gateway that only
    knows catalog ids answers `400 unknown model` on turn two (observed live,
    16 Sep 2026). Aliases make the echoed id acceptable; the streamed
    response is still forwarded byte-for-byte, wire id and all.
    """

    unit: Unit = "tokens"
    """What `input_per_m` / `output_per_m` are per million of. For
    `characters` (TTS) the per-million-character price goes in `input_per_m`
    and `output_per_m` is 0 -- the meter is the text sent, there is no second
    side. For `seconds` (duration-billed STT) the rate is `per_minute` and the
    per-million fields are unused (0)."""

    per_minute: float | None = None
    """USD per minute of audio, for `unit="seconds"` only; cost is
    `seconds / 60 * per_minute`. Providers round the seconds themselves
    (OpenAI rounds up to whole seconds, verified live 2026-09-16); the gateway
    prices what the provider reported."""

    audio_input_per_m: float | None = None
    audio_output_per_m: float | None = None
    cached_audio_input_per_m: float | None = None
    """Per-million rates for the audio token kinds providers report inside
    token usage (`input_token_details.audio_tokens` and friends). Audio is 8
    to 50x the text rate on `gpt-realtime` and `gpt-audio` ($32 in / $64 out
    against $4 / $16), so pricing audio tokens at the text rate -- which is
    what happened before PLAN-2 B3 -- is a silent under-bill. None means "not
    an audio model": if audio tokens arrive anyway they are priced at the
    text rate and the record says so (`cost_notes`)."""

    cache_write_1h_per_m: float | None = None
    """Anthropic's 1-hour cache write rate: 2x input, against 1.25x for the
    5-minute TTL in `cache_write_per_m`. `usage.cache_creation.
    ephemeral_1h_input_tokens` is priced here; None falls back to
    `cache_write_per_m`, then to `input_per_m`."""

    tool_rates: Mapping[str, float] = field(default_factory=dict)
    """USD per 1,000 server-tool calls, keyed by the provider's usage key
    (`web_search_requests` at $10 per 1k on Anthropic). Calls the provider
    reports under a key with no rate here are counted and noted, not priced."""

    request_defaults: Mapping[str, Any] = field(default_factory=dict)
    """JSON-shaped fields applied to the request body for keys the client did
    not send (PLAN-2 B5), merged UNDER a workload's own `request_defaults`.
    A model-level default is for things that are true of the model however
    it is used; a "cheap candidate must not think" decision belongs on the
    workload, which is why the DeepSeek rows carry none."""

    default_profile: str | None = None
    """Name of a budget profile (`[profiles.<name>]` in the policy file) this
    model is best served under, e.g. `tts` for a speech model. Data only: the
    policy layer resolves a workload's budgets from the workload and its
    profile, and a `single_target` route reads this to pick one when the
    policy defines it (PLAN-2 B6)."""

    def __post_init__(self) -> None:
        if not self.priced_at:
            raise ValueError(f"{self.id}: priced_at is required")
        if self.id in self.aliases:
            raise ValueError(f"{self.id}: a model must not alias its own id")
        if self.unit == "seconds" and self.per_minute is None:
            raise ValueError(f"{self.id}: unit='seconds' needs per_minute")
        if self.unit != "seconds" and self.per_minute is not None:
            raise ValueError(
                f"{self.id}: per_minute is only meaningful with unit='seconds'"
            )
        for name in ("tool_rates", "request_defaults"):
            if not isinstance(getattr(self, name), Mapping):
                raise ValueError(f"{self.id}: {name} must be a mapping")


# --------------------------------------------------------------------------
# Provider connections.
# --------------------------------------------------------------------------

PROVIDERS: dict[str, ProviderConn] = {
    "anthropic": ProviderConn(
        id="anthropic",
        kind="anthropic",
        base_url="https://api.anthropic.com",
        api_key_env="ANTHROPIC_API_KEY",
        auth_scheme="x-api-key",
        max_concurrency=32,
    ),
    "openrouter": ProviderConn(
        id="openrouter",
        kind="openai",
        base_url="https://openrouter.ai/api/v1",
        api_key_env="OPENROUTER_API_KEY",
        # Attribution headers OpenRouter uses for rankings.
        extra_headers={
            "HTTP-Referer": "https://github.com/gauravzqa/llmgw",
            "X-Title": "llmgw",
        },
        max_concurrency=64,
    ),
    "openrouter-toolsafe": ProviderConn(
        id="openrouter-toolsafe",
        kind="openai",
        base_url="https://openrouter.ai/api/v1",
        api_key_env="OPENROUTER_API_KEY",
        extra_headers={
            "HTTP-Referer": "https://github.com/gauravzqa/llmgw",
            "X-Title": "llmgw",
        },
        # Keeps requests off hosts that do not declare support for every
        # request parameter. Added after a host mangled DeepSeek
        # tool-call parsing and leaked raw markup into the content stream.
        extra_body={"provider": {"require_parameters": True}},
        # Same real API key as `openrouter`, so the two share one credential
        # and -- because auth circuits key on the credential alone, not the
        # provider entry -- one auth breaker. A 401 through either config
        # counts once, against one circuit. (errors.health_key, credential
        # scope.)
        credential_id="openrouter",
        max_concurrency=64,
    ),
    "deepseek": ProviderConn(
        id="deepseek",
        kind="openai",
        base_url="https://api.deepseek.com",
        api_key_env="DEEPSEEK_API_KEY",
        max_concurrency=32,
        # PLAN-2 Phase F: `/v1/responses` (and `/responses`, identical) is
        # stateless and swallows `previous_response_id` / `background`
        # rather than rejecting them (probe 12b/12c, 17 Sep 2026).
        stateless_responses=True,
    ),
    # Added 16 Sep 2026 (finding 36: a working key to 135 models the catalog
    # could not route to). Same entry the live smoke test had been building
    # for itself; now shipped so `LLMGW_DEFAULT_MODEL` can name an OpenAI
    # model in production.
    "openai": ProviderConn(
        id="openai",
        kind="openai",
        base_url="https://api.openai.com/v1",
        api_key_env="OPENAI_API_KEY",
        max_concurrency=32,
    ),
    # ---------------------------------------------------------------------
    # PLAN-2 Phase C (C4): DeepSeek's other two doors on the same key. The
    # Anthropic-compatible endpoint makes DeepSeek a same-dialect candidate
    # for Anthropic workloads (the policy refuses cross-dialect plans); the
    # `/beta` prefix is where strict tools and prefix completion live
    # (capabilities/deepseek.md §1). Neither is exercised live yet.
    # ---------------------------------------------------------------------
    "deepseek-anthropic": ProviderConn(
        id="deepseek-anthropic",
        kind="anthropic",
        base_url="https://api.deepseek.com/anthropic",
        api_key_env="DEEPSEEK_API_KEY",
        credential_id="deepseek",
        auth_scheme="x-api-key",
        max_concurrency=32,
    ),
    "deepseek-beta": ProviderConn(
        id="deepseek-beta",
        kind="openai",
        base_url="https://api.deepseek.com",
        path_prefix="/beta",
        api_key_env="DEEPSEEK_API_KEY",
        credential_id="deepseek",
        max_concurrency=32,
        stateless_responses=True,  # same host, same stateless Responses door
    ),
    # ---------------------------------------------------------------------
    # PLAN-2 Phase D/E: voice providers. `kind="openai"` means only "no
    # Anthropic header ritual"; the credential style is `auth_scheme`, and
    # the surfaces in `surfaces/voice/` own the framing and the unit. Facts
    # per capabilities/voice-*.md (16 Sep 2026 sweep; Inworld and OpenAI
    # audio verified live, AssemblyAI and ElevenLabs from documentation).
    # ---------------------------------------------------------------------
    "inworld": ProviderConn(
        id="inworld",
        kind="openai",
        base_url="https://api.inworld.ai",
        api_key_env="INWORLD_API_KEY",
        # Basic, because the WebSocket plane has no choice: `Authorization:
        # Basic <key>` is the ONLY credential form Inworld's upgrade accepts
        # (`?key=` is read as no credential at all, captures-ws probe 2a) and
        # it is exactly what the LiveKit plugin sends. On HTTP the two forms
        # behave identically (verified live 2026-09-16), so switching the row
        # changes the bytes on the wire and nothing else -- the Phase D voice
        # surfaces and the Inworld HTTP live smoke are unaffected. The key is
        # reversible base64 and the 403 body reflects its first four
        # characters, so EVERY non-2xx body is replaced by the gateway's own
        # (C11 widened, PLAN-2 B2).
        auth_scheme="basic",
        scrub_error_bodies="all",
        max_concurrency=16,
    ),
    "elevenlabs": ProviderConn(
        id="elevenlabs",
        kind="openai",
        # The global host. It was `api.in.residency.elevenlabs.io` (nearest
        # to the Fly `sin` region) until a live probe on 19 Sep 2026 showed
        # that host answering 400 `{"detail":{"type":"authentication_error",
        # "code":"invalid_api_key"}}` to the SAME key the global host accepts,
        # on both TTS and STT: a residency endpoint needs a residency-enabled
        # (Enterprise) key, which this account does not have. Routing to a
        # host that rejects our credential is not latency, it is downtime.
        base_url="https://api.elevenlabs.io",
        api_key_env="ELEVENLABS_API_KEY",
        auth_scheme="header",
        auth_header="xi-api-key",
        # ElevenLabs' common 403s are plan, voice and model denials, never a
        # bad key; they must not open the credential breaker.
        forbidden_means="policy",
        # Concurrency is a plan number (Pro 10, Scale 15; roughly 2x for the
        # Flash models). Set below the smallest paid plan until the account's
        # plan is known.
        max_concurrency=8,
    ),
    # AssemblyAI: one key, three hosts. The REST host is documented to answer
    # its rate limit with a 403 (20k requests / 5 min) -- UNVERIFIED: the
    # only credential fault ever observed there was a plain 401 (probe A7b),
    # and no probe has provoked the 403. The streaming host is WebSocket
    # (Phase G), the sync host is the one HTTP product that fits a gateway.
    # `auth_scheme="raw"`: the bare key in `Authorization`, no scheme word.
    "assemblyai": ProviderConn(
        id="assemblyai",
        kind="openai",
        base_url="https://api.assemblyai.com",
        api_key_env="ASSEMBLYAI_API_KEY",
        auth_scheme="raw",
        forbidden_means="rate_limit",
        max_concurrency=16,
    ),
    "assemblyai-streaming": ProviderConn(
        id="assemblyai-streaming",
        kind="openai",
        base_url="https://streaming.assemblyai.com",
        api_key_env="ASSEMBLYAI_API_KEY",
        credential_id="assemblyai",
        auth_scheme="raw",
        forbidden_means="rate_limit",
        max_concurrency=16,
    ),
    "assemblyai-sync": ProviderConn(
        id="assemblyai-sync",
        kind="openai",
        base_url="https://sync.assemblyai.com",
        api_key_env="ASSEMBLYAI_API_KEY",
        credential_id="assemblyai",
        auth_scheme="raw",
        # NOT `rate_limit`: this host never sends a 403 at all. A bad key
        # here is a 404 with `application/problem+json` and `detail:
        # "Invalid API key"` (captures-sarvam-assemblyai.md probe A4g,
        # reproduced 19 Sep 2026), which `errors.from_http_status` reads as
        # `AuthenticationFailed`. The old value described a response that
        # does not exist.
        forbidden_means="auth",
        max_concurrency=16,
    ),
    # The fake upstreams from fakes/upstream.py, wired in by tests and by the
    # local dev config. Present in the shipped catalog on purpose: a test
    # target that needs a special code path is a test target that proves
    # nothing about the real one.
    "fake-openai": ProviderConn(
        id="fake-openai",
        kind="openai",
        base_url="http://127.0.0.1:8801/v1",
        api_key_env="FAKE_API_KEY",
        # Raised 10 Sep for the S2/S4 load scenarios (2,500 and 10,000 open
        # streams). At 1024 the provider-key gate, not the process, was the
        # first thing to break, which is a config fact and not a capacity one.
        max_concurrency=20_000,
    ),
    "fake-anthropic": ProviderConn(
        id="fake-anthropic",
        kind="anthropic",
        base_url="http://127.0.0.1:8802",
        api_key_env="FAKE_API_KEY",
        auth_scheme="x-api-key",
        max_concurrency=20_000,
    ),
}


# --------------------------------------------------------------------------
# Models. Rates from voice-agent/harness/evals/pricing.py, dates preserved.
# --------------------------------------------------------------------------

MODELS: dict[str, ModelSpec] = {
    m.id: m
    for m in [
        # OpenAI list prices, verified 2026-09-16 against the gpt-4o-mini model
        # page (capabilities/openai.md §7). OpenAI exposes no price endpoint,
        # so this is a human check against a document, not a probe. The
        # snapshot alias is what OpenAI puts in `model` on every response.
        ModelSpec(
            id="openai.gpt-4o-mini",
            provider="openai",
            api_model="gpt-4o-mini",
            input_per_m=0.15,
            cached_input_per_m=0.075,
            output_per_m=0.60,
            context_window=128_000,
            max_output=16_384,
            priced_at="2026-09-16",
            aliases=("gpt-4o-mini-2024-07-18",),
        ),
        # PLAN-2 Phase F: the reasoning-capable OpenAI row for the Responses
        # surface. `gpt-5-nano` answered the 17 Sep 2026 probe
        # (`capabilities/captures-responses.md` probes 4/4b: snapshot
        # `gpt-5-nano-2025-08-07`, `reasoning_tokens` inside `output_tokens`,
        # 400k context per OpenAI's model page). Prices read 2026-09-18 from
        # https://developers.openai.com/api/docs/pricing (the page
        # platform.openai.com/docs/pricing now redirects to): $0.05 input,
        # $0.005 cached input, $0.40 output per 1M tokens.
        ModelSpec(
            id="openai.gpt-5-nano",
            provider="openai",
            api_model="gpt-5-nano",
            input_per_m=0.05,
            cached_input_per_m=0.005,
            output_per_m=0.40,
            context_window=400_000,
            max_output=128_000,
            can_reason=True,
            priced_at="2026-09-18",
            aliases=("gpt-5-nano-2025-08-07",),
        ),
        # Anthropic rates from platform.claude.com/docs/en/about-claude/pricing,
        # read 2026-09-16 (capabilities/anthropic.md §7). Cache WRITE is
        # 1.25x input for the 5-minute TTL on every current model and 2x for
        # the 1-hour TTL (`cache_write_1h_per_m`, PLAN-2 B3). Before this
        # date `cache_write_per_m` was unset here, which priced every write
        # at 1.0x -- a 20% under-bill on the write line of every cached
        # request. Web search is $10 per 1,000 calls (`tool_rates`).
        #
        # Haiku 4.5's retirement floor is 2026-10-15 (model deprecations page);
        # a successor row is due before then. `can_reason=True` because the
        # 11 Sep live smoke streamed `thinking_delta` frames from it
        # (`d.thinking.anthropic`); the previous False contradicted the
        # evidence in bench/results/live_smoke.md.
        ModelSpec(
            id="anthropic.haiku-4-5",
            provider="anthropic",
            api_model="claude-haiku-4-5-20251001",
            input_per_m=1.00,
            cached_input_per_m=0.10,
            cache_write_per_m=1.25,
            cache_write_1h_per_m=2.00,
            output_per_m=5.00,
            context_window=200_000,
            max_output=64_000,
            can_reason=True,
            priced_at="2026-09-16",
            aliases=("claude-haiku-4-5",),
            tool_rates={"web_search_requests": 10.0},
        ),
        # Context window is 1M, default, no beta header, standard pricing
        # (models/sonnet-4-6/overview, 2026-09-16). The previous 200_000 was
        # finding 35 in a new place: an output ceiling corrected while the
        # context ceiling stayed 5x too low, so any pre-flight that trusted
        # the catalog would refuse a legitimate long-context request.
        ModelSpec(
            id="anthropic.sonnet-4-6",
            provider="anthropic",
            api_model="claude-sonnet-4-6",
            input_per_m=3.00,
            cached_input_per_m=0.30,
            cache_write_per_m=3.75,
            cache_write_1h_per_m=6.00,
            output_per_m=15.00,
            context_window=1_000_000,
            max_output=128_000,
            can_reason=True,
            priced_at="2026-09-16",
            tool_rates={"web_search_requests": 10.0},
        ),
        # Cheaper than Sonnet 4.6 on every axis and the current Sonnet;
        # 4.6 is listed as legacy. The default Anthropic route should move
        # here (policy files, not this table, decide that).
        ModelSpec(
            id="anthropic.sonnet-5",
            provider="anthropic",
            api_model="claude-sonnet-5",
            input_per_m=2.00,
            cached_input_per_m=0.20,
            cache_write_per_m=2.50,
            cache_write_1h_per_m=4.00,
            output_per_m=10.00,
            context_window=1_000_000,
            max_output=128_000,
            can_reason=True,
            priced_at="2026-09-16",
            tool_rates={"web_search_requests": 10.0},
        ),
        # DeepSeek rates from api-docs.deepseek.com/quick_start/pricing, read
        # 2026-09-16 (capabilities/deepseek.md §7). These are the PEAK rates,
        # i.e. the upper bound: DeepSeek charges half of these outside
        # 01:00-04:00 and 06:00-10:00 UTC on weekdays, and `ModelSpec` has no
        # time-of-day dimension (PLAN-2 C, "peak/off-peak pricing"). Until it
        # does, every DeepSeek cost record is an upper bound, stated as such.
        #
        # The previous numbers (0.435 / 0.003625 / 0.87 and 0.14 / 0.0028 /
        # 0.28, dated 2026-06-22) predated DeepSeek's 2026-09-10 repricing and
        # were wrong by 3x to 12x. Thinking is ON by default at effort `high`
        # on both models; `reasoning="off"` below records the intent for the
        # cheap-candidate role, which the gateway cannot yet enforce -- there
        # is no per-target request default until PLAN-2 B5.
        #
        # DeepSeek's own docs disagree about whether `deepseek-v4-pro` is
        # still served or routed to V4.1-Flash at Flash rates since
        # 2026-09-14; the pricing page still lists it, so it stays.
        ModelSpec(
            id="deepseek.deepseek-v4-pro",
            provider="deepseek",
            api_model="deepseek-v4-pro",
            input_per_m=1.32,
            cached_input_per_m=0.044,
            output_per_m=3.96,
            context_window=1_048_576,
            max_output=393_216,
            can_reason=True,
            reasoning="off",
            priced_at="2026-09-16",
        ),
        # RECONCILED 2026-09-10 against DeepSeek's live /models via `make probe`:
        # the `deepseek-v4-flash` wire id was retired and `deepseek-flash`
        # (V4.1-Flash, released 2026-09-10) serves in its place. Our catalog
        # id is kept stable on purpose -- workloads, fixtures and smoke pin
        # `deepseek.deepseek-v4-flash` -- and the retired wire id is an alias
        # so a caller still configured with it resolves here rather than 400s.
        # Prices repriced 2026-09-16 with the row above; `can_reason=True`
        # because thinking is on by default (the previous False was why the
        # "cheap candidate" was paying reasoning rates unnoticed).
        ModelSpec(
            id="deepseek.deepseek-v4-flash",
            provider="deepseek",
            api_model="deepseek-flash",
            input_per_m=0.30,
            cached_input_per_m=0.006,
            output_per_m=1.20,
            context_window=1_048_576,
            max_output=393_216,
            can_reason=True,
            reasoning="off",
            priced_at="2026-09-16",
            aliases=("deepseek-v4-flash",),
        ),
        # The same model behind DeepSeek's Anthropic-compatible endpoint
        # (PLAN-2 C4): a same-dialect candidate for Anthropic workloads, so the
        # cross-dialect rule no longer stands between the cheap candidate and
        # a Claude incumbent. Same wire id as the OpenAI-dialect row; the
        # route's dialect hint disambiguates (Phase A). No alias: the short
        # alias is claimed by the row above.
        ModelSpec(
            id="deepseek-anthropic.deepseek-v4-flash",
            provider="deepseek-anthropic",
            api_model="deepseek-flash",
            input_per_m=0.30,
            cached_input_per_m=0.006,
            output_per_m=1.20,
            context_window=1_048_576,
            max_output=393_216,
            can_reason=True,
            reasoning="off",
            priced_at="2026-09-16",
        ),
        # CORRECTED 2026-09-10 against OpenRouter's live /api/v1/models.
        #
        # This block used to say OpenRouter's per-token rates MATCH direct,
        # because "the margin is on credits, not tokens". That is false, and
        # falsely in both directions: OpenRouter charges 2.00x direct for
        # v4-pro and 0.60x direct for v4-flash. The margin is on tokens and
        # its SIGN DIFFERS PER MODEL, so no single sentence about the reseller
        # can be true -- which is exactly why a comment is a bad place to keep
        # a number that a free API endpoint will tell you.
        #
        # The cache-hit-rate argument still stands and still matters (~94%
        # first-party auto-cache vs ~40-50% through reseller fan-out); it is
        # now an argument ON TOP OF a real price difference rather than the
        # only difference.
        ModelSpec(
            id="openrouter.deepseek-v4-pro",
            provider="openrouter-toolsafe",
            api_model="deepseek/deepseek-v4-pro",
            input_per_m=1.60,
            cached_input_per_m=0.135,
            output_per_m=3.20,
            context_window=1_048_576,
            max_output=384_000,
            can_reason=True,
            reasoning="off",
            priced_at="2026-09-16",
        ),
        ModelSpec(
            id="openrouter.deepseek-v4-flash",
            provider="openrouter-toolsafe",
            api_model="deepseek/deepseek-v4-flash",
            input_per_m=0.08708,
            cached_input_per_m=0.017416,
            output_per_m=0.17416,
            context_window=1_048_576,
            max_output=384_000,
            priced_at="2026-09-16",
        ),
        ModelSpec(
            id="openrouter.qwen-3.6-plus",
            provider="openrouter",
            api_model="qwen/qwen3.6-plus",
            input_per_m=0.325,
            # Pinned to the input rate deliberately: the real
            # cache_read rate is $0/M, which would over-discount every
            # comparison this model appears in.
            cached_input_per_m=0.325,
            cache_write_per_m=0.41,
            output_per_m=1.95,
            context_window=1_000_000,
            max_output=65_536,
            priced_at="2026-05-27",
        ),
        ModelSpec(
            id="openrouter.gemini-3.1-flash-lite",
            provider="openrouter",
            api_model="google/gemini-3.1-flash-lite",
            input_per_m=0.25,
            cached_input_per_m=0.025,
            output_per_m=1.50,
            cache_write_per_m=0.0833333,
            context_window=1_048_576,
            max_output=65_536,
            priced_at="2026-05-27",
        ),
        ModelSpec(
            id="openrouter.mistral-small-2603",
            provider="openrouter",
            api_model="mistralai/mistral-small-2603",
            input_per_m=0.15,
            cached_input_per_m=0.015,
            output_per_m=0.60,
            context_window=262_144,
            max_output=209_715,
            priced_at="2026-05-27",
        ),
        ModelSpec(
            id="openrouter.llama-4-maverick",
            provider="openrouter",
            api_model="meta-llama/llama-4-maverick",
            input_per_m=0.1875,
            cached_input_per_m=None,  # no caching: falls back to input rate
            output_per_m=0.6525,
            context_window=1_048_576,
            max_output=115_200,
            priced_at="2026-09-16",
        ),
        # -----------------------------------------------------------------
        # PLAN-2 Phase C/E rows agent CE routes to.
        # -----------------------------------------------------------------
        ModelSpec(
            # OpenAI list price, developers.openai.com/api/docs/pricing,
            # read 2026-09-18. Embeddings bill input tokens only.
            id="openai.text-embedding-3-small",
            provider="openai",
            api_model="text-embedding-3-small",
            input_per_m=0.02,
            output_per_m=0.0,
            context_window=8_191,
            max_output=0,
            priced_at="2026-09-18",
        ),
        ModelSpec(
            # gpt-realtime-mini: text $0.60 / $0.06 cached / $2.40 out per 1M;
            # audio in $10 / cached $0.30 / out $20 per 1M
            # (capabilities/voice-openai.md §6, pricing page read 2026-09-16).
            # Routed only through the client-secret mint (Phase E); media
            # never transits the gateway, so the rates price the mint's
            # capture record, not a stream.
            id="openai.gpt-realtime-mini",
            provider="openai",
            api_model="gpt-realtime-mini",
            input_per_m=0.60,
            cached_input_per_m=0.06,
            output_per_m=2.40,
            audio_input_per_m=10.0,
            cached_audio_input_per_m=0.30,
            audio_output_per_m=20.0,
            context_window=32_000,
            max_output=4_096,
            priced_at="2026-09-16",
        ),
        # -----------------------------------------------------------------
        # PLAN-2 Phase D: voice rows. Units per `ModelSpec.unit`; every rate
        # cites the sweep file it came from. No provider here exposes a model
        # list, so `make probe` cannot reconcile these ids -- the live smoke
        # (`live/smoke_voice.py`) is the check.
        # -----------------------------------------------------------------
        ModelSpec(
            # $0.60 per 1M text input tokens + $12 per 1M audio output tokens;
            # usage arrives only in SSE mode (`speech.audio.done.usage`), the
            # binary default has no meter (capabilities/voice-openai.md §6,
            # verified live 2026-09-16).
            id="openai.gpt-4o-mini-tts",
            provider="openai",
            api_model="gpt-4o-mini-tts",
            input_per_m=0.60,
            output_per_m=12.0,
            audio_output_per_m=12.0,
            context_window=2_000,
            max_output=0,
            priced_at="2026-09-16",
            default_profile="tts",
        ),
        ModelSpec(
            # $0.0045 per minute, `usage: {type: "duration", seconds}` rounded
            # up to whole seconds (capabilities/voice-openai.md §6, live).
            id="openai.gpt-transcribe",
            provider="openai",
            api_model="gpt-transcribe",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.0045,
            context_window=0,
            max_output=0,
            priced_at="2026-09-16",
        ),
        ModelSpec(
            # $0.006 per minute; the only translation model; no `stream: true`.
            id="openai.whisper-1",
            provider="openai",
            api_model="whisper-1",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.006,
            context_window=0,
            max_output=0,
            priced_at="2026-09-16",
        ),
        ModelSpec(
            # $25 per 1M characters on-demand ($12.50 Growth, "as low as $5"
            # Enterprise): plan-dependent, the on-demand rate is the upper
            # bound (capabilities/voice-inworld.md §6, pricing page
            # 2026-09-16). `processedCharactersCount` is the exact meter.
            id="inworld.tts-2",
            provider="inworld",
            api_model="inworld-tts-2",
            # `inworld-tts-1.5-max` is the LiveKit plugin's OWN default
            # (`inworld/tts.py:55 DEFAULT_MODEL`), so any Layrs call site
            # that constructs `inworld.TTS()` without naming a model sends
            # it -- and on the WebSocket plane the `create` frame is the
            # only thing that can name a target, so an unaliased id is a
            # closed socket rather than a fallback. 1.5-max is the higher
            # tier of the deprecated pair, so it lands on `tts-2` while
            # 1.5-mini lands on `tts-2-flash`.
            aliases=("inworld-tts-1.5-max",),
            input_per_m=25.0,
            output_per_m=0.0,
            unit="characters",
            context_window=2_000,
            max_output=0,
            priced_at="2026-09-16",
            default_profile="tts",
        ),
        ModelSpec(
            # $15 per 1M characters on-demand ($7 Growth); 20 ms TTFB tier.
            id="inworld.tts-2-flash",
            provider="inworld",
            api_model="inworld-tts-2-flash",
            # PLAN-G R10. Layrs' LiveKit plugin sends `inworld-tts-1.5-mini`
            # in its `create.modelId` (harness/config.py:84) and the plugin
            # drops any path prefix, so the socket's own first frame is the
            # ONLY thing that can name a target: without this alias every
            # relayed TTS session is a 400 before it starts. 1.5-mini is
            # deprecated and Inworld auto-routes it; -2-flash is the current
            # mini-equivalent tier, so the alias lands here rather than on
            # `inworld.tts-2`. The price basis differs from Layrs' own
            # per-minute table, which is why a session priced through the
            # alias carries a `cost_notes` line saying so.
            aliases=("inworld-tts-1.5-mini",),
            input_per_m=15.0,
            output_per_m=0.0,
            unit="characters",
            context_window=2_000,
            max_output=0,
            priced_at="2026-09-16",
            default_profile="tts",
        ),
        ModelSpec(
            # $0.05 per 1k characters = $50 per 1M on the API price list; the
            # ~75 ms realtime tier (capabilities/voice-elevenlabs.md §6,
            # elevenlabs.io/pricing/api 2026-09-16). `character-cost` header
            # is the exact meter; 40k characters per request.
            id="elevenlabs.flash-v2-5",
            provider="elevenlabs",
            api_model="eleven_flash_v2_5",
            input_per_m=50.0,
            output_per_m=0.0,
            unit="characters",
            context_window=40_000,
            max_output=0,
            priced_at="2026-09-16",
            default_profile="tts",
        ),
        ModelSpec(
            # ElevenLabs Scribe speech-to-text: $0.22 per hour of audio =
            # $0.0036667 per minute, the same rate on every plan from Free to
            # Business (https://elevenlabs.io/pricing/api, read 2026-09-19).
            # The meter is `audio_duration_secs` in the response body, exact
            # and unrounded. `scribe_v2` is the current model; the endpoint
            # also serves `scribe_v1`, `scribe_v1_experimental` and
            # `scribe_v2_medical`, which are separate products and would each
            # need their own row rather than an alias on this one.
            id="elevenlabs.scribe-v2",
            provider="elevenlabs",
            api_model="scribe_v2",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.22 / 60,
            context_window=0,
            max_output=0,
            priced_at="2026-09-19",
        ),
        ModelSpec(
            # Inworld STT over HTTP: $0.15 per hour of audio on-demand =
            # $0.0025 per minute ($0.10/h on Creator through Growth;
            # inworld.ai/pricing, read 2026-09-19 -- the on-demand rate is
            # the upper bound, as for the TTS rows above). The meter is
            # `usage.transcribedAudioMs`, exact milliseconds. `api_model`
            # carries the provider's `inworld/`-prefixed spelling and is
            # rewritten into the NESTED `transcribeConfig.modelId`.
            id="inworld.stt-1",
            provider="inworld",
            api_model="inworld/inworld-stt-1",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.0025,
            context_window=0,
            max_output=0,
            priced_at="2026-09-19",
        ),
        ModelSpec(
            # $0.05 per 1k characters; the ~280 ms conversational v3 tier,
            # WebSocket-first but served over HTTP too.
            id="elevenlabs.v3-conversational",
            provider="elevenlabs",
            api_model="eleven_v3_conversational",
            input_per_m=50.0,
            output_per_m=0.0,
            unit="characters",
            context_window=10_000,
            max_output=0,
            priced_at="2026-09-16",
            default_profile="tts",
        ),
        ModelSpec(
            # $0.45 per hour of audio = $0.0075 per minute; `audio_duration_ms`
            # in the response, exact milliseconds (assemblyai.com/products/
            # sync-speech-to-text, still $0.45/h at 2026-09-18).
            #
            # `api_model` is LOAD-BEARING, and the comment that used to stand
            # here -- "the sync endpoint takes no model parameter; the id
            # exists so the gateway has a row to price against" -- was false.
            # The endpoint requires `X-AAI-Model`, the surface emits this
            # string into it (`AssemblyAISyncSurface.model_header`), and a
            # model the sync host does not serve (`universal-2` is one) is a
            # load-balancer 404 before any application code runs.
            id="assemblyai.sync",
            provider="assemblyai-sync",
            api_model="universal-3-5-pro",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.0075,
            context_window=0,
            max_output=0,
            priced_at="2026-09-16",
        ),
        ModelSpec(
            # AssemblyAI Universal-Streaming (English): $0.15/h of session
            # wall time, i.e. $0.0025/min (capabilities/voice-assemblyai.md
            # §6, 2026-09-16). The row Phase E's `GET /assemblyai/v3/token`
            # prices a minted session against; no aliases -- the
            # multilingual model is a different product at a different rate.
            id="assemblyai.streaming",
            provider="assemblyai-streaming",
            api_model="universal-streaming-english",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.0025,
            context_window=0,
            max_output=0,
            priced_at="2026-09-16",
        ),
        ModelSpec(
            # Universal-3.5 Pro over the streaming socket: $0.45/h =
            # $0.0075/min (capabilities/voice-assemblyai.md §6, 2026-09-16).
            # The Layrs skeleton's default STT model, so the mint can price it.
            id="assemblyai.universal-3-5-pro-realtime",
            provider="assemblyai-streaming",
            api_model="universal-3-5-pro",
            input_per_m=0.0,
            output_per_m=0.0,
            unit="seconds",
            per_minute=0.0075,
            context_window=0,
            max_output=0,
            priced_at="2026-09-16",
        ),
        ModelSpec(
            id="fake.echo",
            provider="fake-openai",
            api_model="fake-echo",
            input_per_m=1.0,
            cached_input_per_m=0.1,
            output_per_m=2.0,
            priced_at="2026-09-09",
        ),
        ModelSpec(
            id="fake.echo-anthropic",
            provider="fake-anthropic",
            api_model="fake-echo",
            input_per_m=1.0,
            cached_input_per_m=0.1,
            output_per_m=2.0,
            priced_at="2026-09-09",
        ),
    ]
}


@dataclass(frozen=True, slots=True)
class Target:
    """A resolved (model, provider) pair: everything one attempt needs."""

    model: ModelSpec
    provider: ProviderConn

    @property
    def health_key(self) -> tuple[str, str]:
        return (self.provider.id, self.model.id)

    @property
    def credential_key(self) -> str:
        return self.provider.key()

    def __str__(self) -> str:
        return f"{self.provider.id}/{self.model.id}"


class Catalog:
    """A resolvable, overridable view over PROVIDERS and MODELS.

    An instance rather than module globals so tests can build a catalog
    pointing entirely at fakes without monkeypatching, and so BYOK can later
    hand a tenant a catalog with its own credentials substituted.
    """

    def __init__(
        self,
        models: dict[str, ModelSpec] | None = None,
        providers: dict[str, ProviderConn] | None = None,
    ) -> None:
        self.models = dict(models if models is not None else MODELS)
        self.providers = dict(providers if providers is not None else PROVIDERS)
        self._aliases: dict[str, str] = {}
        self._ambiguous: dict[str, tuple[str, ...]] = {}
        self._validate()
        self._index_aliases()

    def _validate(self) -> None:
        """Fail at construction, not at 3 a.m. on the request path.

        A model pointing at a provider that does not exist is a config error
        that must surface on startup. Discovered on the hot path it becomes an
        error whose blast radius is one tenant's traffic and whose cause is
        buried three layers below where it appears.
        """
        for model in self.models.values():
            if model.provider not in self.providers:
                raise ValueError(
                    f"model {model.id!r} references unknown provider "
                    f"{model.provider!r}"
                )

    def _index_aliases(self) -> None:
        """Build the alias -> catalog id table, and decide what "ambiguous" is.

        Two kinds of second name feed the table: every model's `api_model`
        (implicit) and its declared `aliases` (explicit). The rules:

        * A name that is also a catalog id never enters the table. Exact ids
          win outright, so an alias can never shadow a real entry.
        * A DECLARED alias claimed by two models is a config typo and fails
          construction. Nobody writes the same alias twice on purpose.
        * An IMPLICIT wire id shared by two models is legal -- the shipped
          `fake.echo` and `fake.echo-anthropic` both answer to `fake-echo`,
          and OpenRouter re-exports ids other providers also serve. Such a
          name is recorded as ambiguous and refused at resolve time with the
          candidates named, rather than guessed. A gateway that picks a
          provider by coin toss when a client sends a bare wire id is a
          gateway that bills the wrong provider on the toss it loses.
        """
        claims: dict[str, dict[str, bool]] = {}  # name -> {catalog id: declared?}
        for model in self.models.values():
            names = ((model.api_model, False), *((a, True) for a in model.aliases))
            for name, declared in names:
                if name in self.models:
                    continue
                claims.setdefault(name, {})[model.id] = declared
        for name, owners in claims.items():
            if len(owners) == 1:
                self._aliases[name] = next(iter(owners))
                continue
            declared = sorted(mid for mid, is_declared in owners.items() if is_declared)
            if len(declared) > 1:
                raise ValueError(
                    f"alias {name!r} is declared by more than one model: {declared}"
                )
            if len(declared) == 1:
                # One model claimed it on purpose, others only happen to share
                # the wire string. The explicit claim wins.
                self._aliases[name] = declared[0]
                continue
            self._ambiguous[name] = tuple(sorted(owners))

    def canonical_id(self, model_id: str, *, kind: ProviderKind | None = None) -> str:
        """The catalog id a client-supplied model string names, or raise.

        Resolution order: exact catalog id, then the alias table (wire ids
        and declared aliases), then -- for a wire id several models share --
        the one whose provider speaks `kind`, if exactly one does. `kind` is
        the dialect of the route the request arrived on: a bare `deepseek-flash`
        on `/v1/chat/completions` can only mean the OpenAI-shaped row, because
        the Anthropic-shaped row could not serve that body. Without a hint, or
        with two owners of the same kind, the id is refused as ambiguous.

        An ambiguous wire id and an unknown string are both `PolicyError` --
        POLICY blame, NEUTRAL health, no provider's breaker hears about it --
        but with different messages, because "name the catalog id" and "no
        such model" are different fixes.
        """
        from .errors import PolicyError

        if model_id in self.models:
            return model_id
        canonical = self._aliases.get(model_id)
        if canonical is not None:
            return canonical
        owners = self._ambiguous.get(model_id)
        if owners is not None:
            if kind is not None:
                same_kind = [
                    mid for mid in owners
                    if self.providers[self.models[mid].provider].kind == kind
                ]
                if len(same_kind) == 1:
                    return same_kind[0]
            raise PolicyError(
                f"model {model_id!r} is a wire id shared by {list(owners)}; "
                f"name the catalog id",
                model=model_id,
            )
        raise PolicyError(f"unknown model {model_id!r}", model=model_id)

    def resolve(self, model_id: str, *, kind: ProviderKind | None = None) -> Target:
        """`Target` for a catalog id, a wire id, or a declared alias. `kind`
        is the route's dialect, used only to break a wire-id tie."""
        model = self.models[self.canonical_id(model_id, kind=kind)]
        return Target(model=model, provider=self.providers[model.provider])

    @property
    def aliases(self) -> dict[str, str]:
        """A copy of the alias table: every second name and the id it means."""
        return dict(self._aliases)

    def with_overrides(
        self,
        *,
        providers: dict[str, ProviderConn] | None = None,
        models: dict[str, ModelSpec] | None = None,
    ) -> Catalog:
        """Return a new catalog with entries replaced. Used by tests to point
        real model ids at fake upstreams -- which is how the contract tier
        exercises the SAME routing code the production path uses."""
        merged_p = dict(self.providers)
        merged_p.update(providers or {})
        merged_m = dict(self.models)
        merged_m.update(models or {})
        return Catalog(models=merged_m, providers=merged_p)

    def redirect_to_fakes(
        self, base_url: str, *, kind: ProviderKind = "openai",
        api_key_env: str = "FAKE_API_KEY",
    ) -> Catalog:
        """Point every provider of `kind` at a local fake. One line in a test
        fixture; no code path in the gateway knows it happened.

        The credential moves with the URL, and that is not a detail. Leaving
        `api_key_env` pointing at `$ANTHROPIC_API_KEY` while the base URL
        points at 127.0.0.1 means a developer with no real key gets a
        `policy_error` -- a missing switch wearing a bad-request's clothes,
        three layers away from the line that actually caused it.
        """
        providers = {
            pid: replace(conn, base_url=base_url, api_key_env=api_key_env)
            for pid, conn in self.providers.items()
            if conn.kind == kind
        }
        return self.with_overrides(providers=providers)

    def stale_prices(self, *, today: _dt.date | None = None, max_age_days: int = 120):
        """Models whose rates have not been re-verified recently.

        Wired into a unit test so a stale table fails CI rather than quietly
        misreporting spend. The threshold is a judgement call; having one at
        all is not.
        """
        today = today or _dt.date.today()
        stale = []
        for model in self.models.values():
            try:
                when = _dt.date.fromisoformat(model.priced_at)
            except ValueError:
                stale.append((model.id, None))
                continue
            age = (today - when).days
            if age > max_age_days:
                stale.append((model.id, age))
        return sorted(stale)


def price_of(model: ModelSpec, *, cached: bool = False) -> float:
    """USD per million input tokens, honouring cache support.

    Falls back to the full input rate when caching is unsupported. Falling back
    to zero would be the tempting bug: it makes every uncached provider look
    free and would push routing toward the most expensive option on a table
    that says it is the cheapest.
    """
    if not cached:
        return model.input_per_m
    if model.cached_input_per_m is None:
        return model.input_per_m
    return model.cached_input_per_m


DEFAULT_CATALOG = Catalog()

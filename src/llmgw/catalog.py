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

AuthScheme = Literal["bearer", "x-api-key", "raw", "header"]
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

    def __post_init__(self) -> None:
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

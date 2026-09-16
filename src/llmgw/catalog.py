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
from dataclasses import dataclass, field, replace
from typing import Literal

ProviderKind = Literal["openai", "anthropic"]


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
    reasoning: Literal["off", "low", "high", "xhigh"] | None = None
    priced_at: str = ""
    """ISO date the rates were last verified. Mandatory in practice."""

    def __post_init__(self) -> None:
        if not self.priced_at:
            raise ValueError(f"{self.id}: priced_at is required")


# --------------------------------------------------------------------------
# Provider connections.
# --------------------------------------------------------------------------

PROVIDERS: dict[str, ProviderConn] = {
    "anthropic": ProviderConn(
        id="anthropic",
        kind="anthropic",
        base_url="https://api.anthropic.com",
        api_key_env="ANTHROPIC_API_KEY",
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
        max_concurrency=20_000,
    ),
}


# --------------------------------------------------------------------------
# Models. Rates from voice-agent/harness/evals/pricing.py, dates preserved.
# --------------------------------------------------------------------------

MODELS: dict[str, ModelSpec] = {
    m.id: m
    for m in [
        # OpenAI list prices as copied by the live smoke test on 2026-09-09.
        # OpenAI exposes no price endpoint, so unlike the OpenRouter rows
        # these are NOT externally verified; the date says when they were
        # copied, not when they were checked.
        ModelSpec(
            id="openai.gpt-4o-mini",
            provider="openai",
            api_model="gpt-4o-mini",
            input_per_m=0.15,
            cached_input_per_m=0.075,
            output_per_m=0.60,
            context_window=128_000,
            max_output=16_384,
            priced_at="2026-09-09",
        ),
        ModelSpec(
            id="anthropic.haiku-4-5",
            provider="anthropic",
            api_model="claude-haiku-4-5-20251001",
            input_per_m=1.00,
            cached_input_per_m=0.10,
            output_per_m=5.00,
            context_window=200_000,
            max_output=64_000,
            priced_at="2026-05-27",
        ),
        ModelSpec(
            id="anthropic.sonnet-4-6",
            provider="anthropic",
            api_model="claude-sonnet-4-6",
            input_per_m=3.00,
            cached_input_per_m=0.30,
            output_per_m=15.00,
            context_window=200_000,
            max_output=128_000,
            can_reason=True,
            priced_at="2026-08-03",
        ),
        ModelSpec(
            id="deepseek.deepseek-v4-pro",
            provider="deepseek",
            api_model="deepseek-v4-pro",
            input_per_m=0.435,
            cached_input_per_m=0.003625,
            output_per_m=0.87,
            context_window=1_048_576,
            max_output=393_216,
            can_reason=True,
            reasoning="off",
            priced_at="2026-06-22",
        ),
        # RECONCILED 2026-09-10 against DeepSeek's live /models via `make probe`.
        # The `deepseek-v4-flash` wire id was retired; DeepSeek now serves
        # exactly `deepseek-flash` and `deepseek-v4-pro` (the probe's reachable
        # set, 2 ids). `deepseek-flash` is the equivalent flash tier, so the
        # api_model is moved onto it. Our catalog id is kept stable on purpose:
        # workloads, fixtures, and smoke pin `deepseek.deepseek-v4-flash`, and
        # the wire id is free to move under it (see ModelSpec.api_model).
        #
        # PRICE UNVERIFIED: DeepSeek does not publish prices on its free /models
        # endpoint -- only OpenRouter does (see live/probe.py audit_prices) --
        # so the rename could NOT be re-priced against the probe. The rates
        # below are the retired `deepseek-v4-flash` numbers, and `priced_at` is
        # deliberately left at the old 2026-06-22 (NOT bumped to today) to flag
        # that they were not re-verified for `deepseek-flash`; a rename can be a
        # repricing. Confirm against DeepSeek's pricing before trusting them.
        ModelSpec(
            id="deepseek.deepseek-v4-flash",
            provider="deepseek",
            api_model="deepseek-flash",
            input_per_m=0.14,          # UNVERIFIED for deepseek-flash; see note above
            cached_input_per_m=0.0028,  # UNVERIFIED for deepseek-flash; see note above
            output_per_m=0.28,         # UNVERIFIED for deepseek-flash; see note above
            context_window=1_048_576,
            max_output=393_216,
            priced_at="2026-06-22",    # last date flash rates were verified (old id)
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
            input_per_m=0.87,
            cached_input_per_m=0.0725,
            output_per_m=1.74,
            context_window=1_048_576,
            max_output=384_000,
            can_reason=True,
            reasoning="off",
            priced_at="2026-09-10",
        ),
        ModelSpec(
            id="openrouter.deepseek-v4-flash",
            provider="openrouter-toolsafe",
            api_model="deepseek/deepseek-v4-flash",
            input_per_m=0.084,
            cached_input_per_m=0.0168,
            output_per_m=0.168,
            context_window=1_048_576,
            max_output=384_000,
            priced_at="2026-09-10",
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
            input_per_m=0.20,
            cached_input_per_m=None,  # no caching: falls back to input rate
            output_per_m=0.696,
            context_window=1_048_576,
            max_output=115_200,
            priced_at="2026-09-10",
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
        self._validate()

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

    def resolve(self, model_id: str) -> Target:
        from .errors import PolicyError

        model = self.models.get(model_id)
        if model is None:
            raise PolicyError(f"unknown model {model_id!r}", model=model_id)
        return Target(model=model, provider=self.providers[model.provider])

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

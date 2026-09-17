"""PLAN-2 B3: cost when the meter is not four text-token buckets.

Every fixture here is a usage shape a real provider emitted, copied from the
capability sweeps (`capabilities/*.md` §4, `capabilities/voice-*.md` §6):
an Anthropic 1-hour cache write, OpenAI audio tokens on a text model, a
DeepSeek response that was mostly reasoning, duration-billed transcription,
a per-character TTS meter, and a web-search call. The assertions read rates
off the catalog rows, never literals, so a price correction moves the bill
and not the test.

`Usage` gains the new fields in the surfaces agent's change; until it lands
these tests build a duck-typed usage with `types.SimpleNamespace`, which is
also what proves accounting reads them with `getattr` and tolerates their
absence.
"""

from __future__ import annotations

import types

import pytest

from llmgw import metrics
from llmgw.accounting import TOKEN_KINDS, UNITS, account
from llmgw.catalog import (
    DEFAULT_CATALOG,
    Catalog,
    ModelSpec,
    ProviderConn,
)
from llmgw.errors import Outcome
from llmgw.executor import ExecutionResult
from llmgw.policy import DEFAULT_BUDGETS, ExecutionPlan
from llmgw.pump import PumpResult
from llmgw.surfaces.base import Usage

HAIKU = "anthropic.haiku-4-5"
SONNET5 = "anthropic.sonnet-5"
MINI = "openai.gpt-4o-mini"
FLASH = "deepseek.deepseek-v4-flash"


# ------------------------------------------------------------------ builders


def usage_like(**fields) -> object:
    """A `Usage` with extra B3 fields, whether or not `Usage` has them yet."""
    base = Usage(input_exact=True, output_exact=True)
    ns = types.SimpleNamespace(**{k: getattr(base, k) for k in (
        "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens",
        "input_exact", "output_exact", "parse_failures", "stop_reason",
    )})
    ns.exact = True
    for k, v in fields.items():
        setattr(ns, k, v)
    return ns


def result_for(catalog: Catalog, model_id: str, usage: object,
               *, bytes_out: int = 0) -> ExecutionResult:
    target = catalog.resolve(model_id)
    plan = ExecutionPlan(policy_id="pol_t", workload_id="w", targets=(target,),
                         budgets=DEFAULT_BUDGETS, retry=None)
    pump = PumpResult(committed=True, bytes_out=bytes_out, events=1, content_events=1,
                      usage=usage, terminal_seen=True, first_event_at=0.0,
                      in_stream_error=None)
    return ExecutionResult(plan=plan, attempts=[], served_by=target, pump=pump,
                           committed=True, outcome=Outcome.COMPLETED, error=None,
                           refusals=[])


def voice_catalog() -> Catalog:
    """Rows the shipped catalog does not have yet: a per-character TTS model,
    a per-minute STT model, and an audio-priced chat model."""
    return DEFAULT_CATALOG.with_overrides(
        providers={
            "tts-vendor": ProviderConn(id="tts-vendor", kind="openai",
                                       base_url="https://tts.example/v1",
                                       api_key_env="TTS_KEY",
                                       auth_scheme="header", auth_header="xi-api-key"),
        },
        models={
            "tts.flash": ModelSpec(id="tts.flash", provider="tts-vendor",
                                   api_model="flash", unit="characters",
                                   input_per_m=50.0, output_per_m=0.0,
                                   priced_at="2026-09-16"),
            "stt.minutes": ModelSpec(id="stt.minutes", provider="openai",
                                     api_model="gpt-transcribe", unit="seconds",
                                     per_minute=0.0045, input_per_m=0.0,
                                     output_per_m=0.0, priced_at="2026-09-16"),
            "audio.chat": ModelSpec(id="audio.chat", provider="openai",
                                    api_model="gpt-audio", input_per_m=4.0,
                                    output_per_m=16.0, cached_input_per_m=0.4,
                                    audio_input_per_m=32.0, audio_output_per_m=64.0,
                                    cached_audio_input_per_m=0.4,
                                    priced_at="2026-09-16"),
        },
    )


# ------------------------------------------------------------ closed sets


def test_token_kinds_and_units_match_the_metrics_contract():
    assert TOKEN_KINDS == metrics.TOKEN_KINDS
    assert UNITS == metrics.UNITS
    assert set(metrics.UNITS) == {"characters", "seconds"}


def test_record_tokens_by_kind_covers_every_metric_kind():
    rec = account(result_for(DEFAULT_CATALOG, HAIKU, usage_like()), catalog=DEFAULT_CATALOG)
    assert set(rec.tokens_by_kind) == set(metrics.TOKEN_KINDS)
    assert set(rec.units_by_kind) == set(metrics.UNITS)


# -------------------------------------------------------- anthropic 1h write


def test_a_one_hour_cache_write_is_priced_at_twice_input_not_1_25x():
    """`usage.cache_creation.ephemeral_1h_input_tokens` -- the split the old
    accounting collapsed into the 5-minute bucket (capabilities/anthropic.md
    gap 1)."""
    spec = DEFAULT_CATALOG.models[HAIKU]
    assert spec.cache_write_1h_per_m == pytest.approx(2 * spec.input_per_m)
    usage = usage_like(cache_write_tokens=1000, cache_write_1h_tokens=1000)
    rec = account(result_for(DEFAULT_CATALOG, HAIKU, usage), catalog=DEFAULT_CATALOG)
    expected = (1000 * spec.cache_write_per_m + 1000 * spec.cache_write_1h_per_m) / 1e6
    assert rec.cost_usd == pytest.approx(expected)
    assert rec.cache_write_1h_tokens == 1000
    assert rec.tokens_by_kind["cache_write_1h"] == 1000
    assert rec.cost_notes == ()


def test_a_one_hour_write_on_a_row_without_the_rate_falls_back_and_says_so():
    spec = DEFAULT_CATALOG.models[MINI]
    assert spec.cache_write_1h_per_m is None
    usage = usage_like(cache_write_1h_tokens=500)
    rec = account(result_for(DEFAULT_CATALOG, MINI, usage), catalog=DEFAULT_CATALOG)
    # falls back to cache_write_per_m (None here) and then to input_per_m
    assert rec.cost_usd == pytest.approx(500 * spec.input_per_m / 1e6)
    assert any("cache_write_1h" in n for n in rec.cost_notes)


# ------------------------------------------------------------- audio tokens


def test_audio_tokens_on_an_audio_priced_row_use_the_audio_rates():
    cat = voice_catalog()
    spec = cat.models["audio.chat"]
    # Audio tokens are a SUBSET of the base counts (OpenAI reports
    # `audio_tokens` inside `prompt_tokens` / `completion_tokens`), so the
    # base counts here include them: 100 text + 1000 audio in, 50 + 200 out.
    usage = usage_like(input_tokens=1100, output_tokens=250,
                       audio_input_tokens=1000, audio_output_tokens=200,
                       cached_audio_input_tokens=400)
    rec = account(result_for(cat, "audio.chat", usage), catalog=cat)
    expected = (
        100 * spec.input_per_m + 50 * spec.output_per_m
        + 1000 * spec.audio_input_per_m + 200 * spec.audio_output_per_m
        + 400 * spec.cached_audio_input_per_m
    ) / 1e6
    assert rec.cost_usd == pytest.approx(expected)
    assert rec.audio_input_tokens == 1000
    assert rec.cost_notes == ()


def test_audio_tokens_on_a_text_row_are_priced_at_text_rates_with_a_note():
    """The silent under-bill the sweep found: `gpt-audio` audio tokens at the
    text rate. Still priced at text (never zero), but now the record says so."""
    spec = DEFAULT_CATALOG.models[MINI]
    usage = usage_like(audio_input_tokens=1000, audio_output_tokens=100)
    rec = account(result_for(DEFAULT_CATALOG, MINI, usage), catalog=DEFAULT_CATALOG)
    expected = (1000 * spec.input_per_m + 100 * spec.output_per_m) / 1e6
    assert rec.cost_usd == pytest.approx(expected)
    assert len(rec.cost_notes) == 2
    assert any("audio_input" in n for n in rec.cost_notes)
    assert any("audio_output" in n for n in rec.cost_notes)


# ------------------------------------------------------ reasoning is informational


def test_reasoning_tokens_are_recorded_but_never_priced_twice():
    """DeepSeek: 151 of 159 output tokens were thinking (live_smoke.md). They
    are inside `output_tokens` already; counting them again would double the
    bill. The record exposes the split so a dashboard can finally see it."""
    spec = DEFAULT_CATALOG.models[FLASH]
    usage = usage_like(input_tokens=37, output_tokens=159, reasoning_tokens=151)
    rec = account(result_for(DEFAULT_CATALOG, FLASH, usage), catalog=DEFAULT_CATALOG)
    expected = (37 * spec.input_per_m + 159 * spec.output_per_m) / 1e6
    assert rec.cost_usd == pytest.approx(expected)
    assert rec.reasoning_tokens == 151
    assert rec.tokens_by_kind["reasoning"] == 151


# ------------------------------------------------------------- non-token units


def test_characters_are_priced_per_million_at_the_input_rate():
    cat = voice_catalog()
    usage = usage_like(characters=2000)
    rec = account(result_for(cat, "tts.flash", usage), catalog=cat)
    assert rec.unit == "characters"
    assert rec.characters == 2000
    assert rec.cost_usd == pytest.approx(2000 * 50.0 / 1e6)
    assert rec.units_by_kind == {"characters": 2000, "seconds": 0}


def test_seconds_are_priced_through_per_minute():
    cat = voice_catalog()
    usage = usage_like(seconds=90)
    rec = account(result_for(cat, "stt.minutes", usage), catalog=cat)
    assert rec.unit == "seconds"
    assert rec.cost_usd == pytest.approx(90 / 60 * 0.0045)
    assert rec.seconds == 90


def test_a_seconds_row_needs_per_minute_and_a_token_row_may_not_have_it():
    with pytest.raises(ValueError, match="per_minute"):
        ModelSpec(id="x", provider="openai", api_model="x", unit="seconds",
                  input_per_m=0.0, output_per_m=0.0, priced_at="2026-09-16")
    with pytest.raises(ValueError, match="per_minute"):
        ModelSpec(id="x", provider="openai", api_model="x", per_minute=1.0,
                  input_per_m=0.0, output_per_m=0.0, priced_at="2026-09-16")


# --------------------------------------------------------------- tool rates


def test_server_tool_calls_are_priced_per_thousand_from_the_row():
    spec = DEFAULT_CATALOG.models[SONNET5]
    assert spec.tool_rates["web_search_requests"] == 10.0
    usage = usage_like(input_tokens=10, output_tokens=10,
                       server_tool_calls={"web_search_requests": 3})
    rec = account(result_for(DEFAULT_CATALOG, SONNET5, usage), catalog=DEFAULT_CATALOG)
    text = (10 * spec.input_per_m + 10 * spec.output_per_m) / 1e6
    assert rec.cost_usd == pytest.approx(text + 3 * 10.0 / 1000)
    assert rec.server_tool_calls == {"web_search_requests": 3}
    assert rec.cost_notes == ()


def test_a_tool_call_with_no_rate_is_counted_and_noted_not_priced():
    usage = usage_like(server_tool_calls={"crystal_ball_requests": 2})
    rec = account(result_for(DEFAULT_CATALOG, HAIKU, usage), catalog=DEFAULT_CATALOG)
    assert rec.server_tool_calls == {"crystal_ball_requests": 2}
    assert rec.cost_usd == 0.0
    assert any("crystal_ball_requests" in n for n in rec.cost_notes)


# ------------------------------------------------- a Usage without the fields


def test_a_usage_that_predates_b3_still_accounts_exactly_as_before():
    """The real `Usage` class, whatever fields it has today: zero for every
    new kind, empty tool calls, no notes, the old dot product."""
    spec = DEFAULT_CATALOG.models[HAIKU]
    usage = Usage(input_tokens=100, output_tokens=20, input_exact=True, output_exact=True)
    rec = account(result_for(DEFAULT_CATALOG, HAIKU, usage), catalog=DEFAULT_CATALOG)
    expected = (100 * spec.input_per_m + 20 * spec.output_per_m) / 1e6
    assert rec.cost_usd == pytest.approx(expected)
    assert rec.audio_input_tokens == rec.reasoning_tokens == rec.characters == 0
    assert rec.server_tool_calls == {}
    assert rec.cost_notes == ()
    assert rec.unit == "tokens"


def test_garbage_in_the_new_fields_never_raises():
    usage = usage_like(audio_input_tokens="lots", server_tool_calls="no", seconds=None)
    rec = account(result_for(DEFAULT_CATALOG, HAIKU, usage), catalog=DEFAULT_CATALOG)
    assert rec.audio_input_tokens == 0
    assert rec.server_tool_calls == {}

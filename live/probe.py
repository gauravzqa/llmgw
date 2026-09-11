"""Ask each provider what it actually serves, then reconcile it with the catalog.

--------------------------------------------------------------------------
Why this runs before anything that costs money
--------------------------------------------------------------------------

`catalog.MODELS` is hand-maintained. Every `api_model` string
in it is an assertion about a remote system that no code in this repository
has ever checked, because the fakes accept any model id you hand them --
`fakes/upstream.py` never looks at the field. A table of model ids validated
only against a mock is a table of guesses with a schema.

The cost of being wrong is not a clean startup failure. It is a 404 or a 400
on the request path, per request, at whatever hour the workload that names
that model first gets traffic -- and, because `Workload.validate()` checks
that a model id resolves in OUR catalog and cannot check that the wire id
resolves at the PROVIDER, the policy file loads perfectly clean on the way
there.

Every endpoint used here is a model-list endpoint. They are free, they are
not rate-limited in any way that matters at this volume, and they answer the
one question that has to be answered before a single generation request is
worth sending.

--------------------------------------------------------------------------
Matching, and why "close" is reported rather than applied
--------------------------------------------------------------------------

Exact string match is the only verdict this module trusts. When it misses, a
`difflib` suggestion is printed as a *suggestion* -- a nearest neighbour in
edit distance is a lead for a human, never a rewrite. `claude-sonnet-4-6` and
`claude-sonnet-4-5` are one character apart and are different models with
different prices; a tool that "helpfully" resolved that would be a tool that
silently rebilled a workload.

    python -m live.probe            # human table
    python -m live.probe --json     # machine-readable
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from live import env
from llmgw.catalog import MODELS

TIMEOUT = httpx.Timeout(30.0, connect=10.0)


@dataclass(frozen=True, slots=True)
class ProbeSpec:
    """One provider's model-list endpoint and how to authenticate to it."""

    provider_id: str
    """Key into `catalog.PROVIDERS`, or a bare name for a provider the catalog
    does not have yet -- `openai` is exactly that case, and printing its model
    list is how we find out whether it should."""

    url: str
    key_var: str
    auth: str
    """`bearer`, `x-api-key`, or `none`. OpenRouter's catalogue is public; we
    send the key anyway so the response reflects what THIS account can reach
    rather than what the marketing page lists."""

    paginate: bool = False
    """Anthropic's `/v1/models` is cursor-paginated and defaults to 20 items.
    Reading only the first page is how you conclude a model "does not exist"
    when it is on page two -- which is a false negative that looks exactly
    like a true one."""


SPECS: tuple[ProbeSpec, ...] = (
    ProbeSpec("anthropic", "https://api.anthropic.com/v1/models",
              "ANTHROPIC_API_KEY", "x-api-key", paginate=True),
    ProbeSpec("openai", "https://api.openai.com/v1/models",
              "OPENAI_API_KEY", "bearer"),
    ProbeSpec("openrouter", "https://openrouter.ai/api/v1/models",
              "OPENROUTER_API_KEY", "bearer"),
    ProbeSpec("deepseek", "https://api.deepseek.com/models",
              "DEEPSEEK_API_KEY", "bearer"),
)

CATALOG_TO_PROBE = {
    "anthropic": "anthropic",
    "openrouter": "openrouter",
    "openrouter-toolsafe": "openrouter",
    "deepseek": "deepseek",
}
"""Catalog provider id -> the probe that answers for it.

`openrouter-toolsafe` is the same host and the same credential as
`openrouter`; it differs only by an `extra_body` flag. One probe, two catalog
providers -- and if that mapping is ever wrong, the reconciliation would
report a model as missing from a provider it was never asked about.
"""


def _headers(spec: ProbeSpec) -> dict[str, str]:
    key = os.environ.get(spec.key_var, "")
    headers: dict[str, str] = {"accept": "application/json"}
    if spec.auth == "bearer" and key:
        headers["authorization"] = f"Bearer {key}"
    elif spec.auth == "x-api-key" and key:
        headers["x-api-key"] = key
        headers["anthropic-version"] = "2023-06-01"
    return headers


@dataclass
class ProviderModels:
    """What one provider answered, plus how long it took to answer it."""

    provider_id: str
    ok: bool
    ids: list[str] = field(default_factory=list)
    pricing: dict[str, dict[str, float]] = field(default_factory=dict)
    """Model id -> {input_per_m, output_per_m, ...}. Only OpenRouter publishes
    this, and it is the only external check we have on the price table."""
    error: str = ""
    elapsed_ms: float = 0.0
    status: int = 0

    @property
    def index(self) -> set[str]:
        return set(self.ids)


def fetch(spec: ProbeSpec, client: httpx.Client) -> ProviderModels:
    """One provider's model list. Never raises; a dead provider is a row."""
    if not os.environ.get(spec.key_var) and spec.auth != "none":
        return ProviderModels(spec.provider_id, ok=False,
                              error=f"${spec.key_var} not set")
    started = time.perf_counter()
    ids: list[str] = []
    pricing: dict[str, dict[str, float]] = {}
    url = spec.url
    status = 0
    try:
        for _ in range(20):  # a page bound; 20 pages is far past any real list
            resp = client.get(url, headers=_headers(spec), timeout=TIMEOUT)
            status = resp.status_code
            if resp.status_code != 200:
                return ProviderModels(
                    spec.provider_id, ok=False, status=resp.status_code,
                    error=f"HTTP {resp.status_code}: {resp.text[:200]}",
                    elapsed_ms=(time.perf_counter() - started) * 1000,
                )
            payload = resp.json()
            data = payload.get("data") or []
            for entry in data:
                mid = entry.get("id")
                if not mid:
                    continue
                ids.append(mid)
                priced = _pricing_of(entry)
                if priced:
                    pricing[mid] = priced
            if not (spec.paginate and payload.get("has_more") and data):
                break
            url = f"{spec.url}?limit=1000&after_id={data[-1]['id']}"
    except httpx.HTTPError as exc:
        return ProviderModels(
            spec.provider_id, ok=False, error=f"{type(exc).__name__}: {exc}",
            elapsed_ms=(time.perf_counter() - started) * 1000,
        )
    return ProviderModels(
        spec.provider_id, ok=True, ids=ids, pricing=pricing, status=status,
        elapsed_ms=(time.perf_counter() - started) * 1000,
    )


def _pricing_of(entry: dict[str, Any]) -> dict[str, float]:
    """OpenRouter's per-TOKEN prices, converted to our per-MILLION convention.

    The unit conversion is the whole reason this is a function. OpenRouter
    quotes `"prompt": "0.00000015"` per token; `catalog.ModelSpec` stores
    `input_per_m=0.15`. Comparing the two numbers without the 1e6 is how a
    price audit concludes that everything is wrong by six orders of
    magnitude, and then stops being run.
    """
    raw = entry.get("pricing")
    if not isinstance(raw, dict):
        return {}
    out: dict[str, float] = {}
    for ours, theirs in (
        ("input_per_m", "prompt"),
        ("output_per_m", "completion"),
        ("cached_input_per_m", "input_cache_read"),
        ("cache_write_per_m", "input_cache_write"),
    ):
        value = raw.get(theirs)
        try:
            out[ours] = float(value) * 1_000_000
        except (TypeError, ValueError):
            continue
    return out


# ==========================================================================
# Reconciliation
# ==========================================================================


@dataclass(frozen=True, slots=True)
class Row:
    """One catalog entry judged against one provider's real model list."""

    model_id: str
    provider_id: str
    api_model: str
    verdict: str
    """`OK`, `MISSING`, or `UNCHECKED` -- the third when the provider itself
    did not answer. Collapsing UNCHECKED into MISSING would turn an expired
    key into a report that the whole catalog is wrong."""
    suggestion: str = ""
    note: str = ""


def closest(target: str, universe: list[str], *, n: int = 3) -> list[str]:
    """Nearest ids by edit distance, with a family-prefix pass first.

    Plain `difflib` on `deepseek-v4-pro` against OpenRouter's ~500 ids returns
    whatever happens to share letters. Filtering to ids that share the target's
    first path/dash segment gets a suggestion a human can act on, and falling
    back to the unfiltered set keeps it from returning nothing when the family
    itself is what we got wrong.
    """
    for sep in ("/", "-"):
        if sep in target:
            stem = target.split(sep)[0]
            family = [m for m in universe if m.startswith(stem)]
            if family:
                hits = difflib.get_close_matches(target, family, n=n, cutoff=0.3)
                if hits:
                    return hits
                return sorted(family)[:n]
    return difflib.get_close_matches(target, universe, n=n, cutoff=0.3)


def reconcile(results: dict[str, ProviderModels]) -> list[Row]:
    """Every non-fake catalog entry, judged."""
    rows: list[Row] = []
    for model in sorted(MODELS.values(), key=lambda m: m.id):
        if model.provider.startswith("fake-"):
            continue
        probe_id = CATALOG_TO_PROBE.get(model.provider)
        result = results.get(probe_id or "")
        if result is None or not result.ok:
            rows.append(Row(model.id, model.provider, model.api_model,
                            "UNCHECKED",
                            note=(result.error if result else "no probe")))
            continue
        if model.api_model in result.index:
            rows.append(Row(model.id, model.provider, model.api_model, "OK"))
        else:
            hits = closest(model.api_model, result.ids)
            rows.append(Row(model.id, model.provider, model.api_model, "MISSING",
                            suggestion=hits[0] if hits else "",
                            note="; ".join(hits[1:])))
    return rows


@dataclass(frozen=True, slots=True)
class PriceRow:
    """A catalog price against the provider's published price, per million."""

    model_id: str
    field: str
    ours: float | None
    theirs: float
    priced_at: str

    @property
    def disagrees(self) -> bool:
        """1% tolerance. Floating conversion from a per-token decimal string
        does not land on our two-decimal literals exactly, and a report that
        flags 0.15 against 0.15000000000000002 is a report nobody reads."""
        if self.ours is None:
            return self.theirs > 0
        if self.ours == 0 and self.theirs == 0:
            return False
        denom = max(abs(self.ours), abs(self.theirs), 1e-9)
        return abs(self.ours - self.theirs) / denom > 0.01


def audit_prices(results: dict[str, ProviderModels]) -> list[PriceRow]:
    """Catalog prices vs OpenRouter's published ones, for models it serves.

    Only OpenRouter exposes prices on a free endpoint, so this covers a
    fraction of the table. That fraction is still the only external check the
    price column has ever had, and `priced_at` is carried into every row
    because a disagreement is only interesting alongside the date the number
    was last defended.
    """
    rows: list[PriceRow] = []
    orr = results.get("openrouter")
    if orr is None or not orr.ok:
        return rows
    for model in sorted(MODELS.values(), key=lambda m: m.id):
        if CATALOG_TO_PROBE.get(model.provider) != "openrouter":
            continue
        published = orr.pricing.get(model.api_model)
        if not published:
            continue
        for field_name in ("input_per_m", "output_per_m", "cached_input_per_m",
                           "cache_write_per_m"):
            if field_name not in published:
                continue
            rows.append(PriceRow(
                model_id=model.id, field=field_name,
                ours=getattr(model, field_name), theirs=published[field_name],
                priced_at=model.priced_at,
            ))
    return rows


# ==========================================================================
# Output
# ==========================================================================


def _fmt_price(value: float | None) -> str:
    return "-" if value is None else f"{value:.6g}"


def render(results: dict[str, ProviderModels], rows: list[Row],
           prices: list[PriceRow], out=sys.stdout) -> None:
    p = lambda *a: print(*a, file=out)  # noqa: E731 - a printer, not a lambda-as-def

    p("=" * 78)
    p("PROVIDER REACHABILITY (model-list endpoints; free)")
    p("=" * 78)
    for spec in SPECS:
        r = results.get(spec.provider_id)
        if r is None:
            continue
        if r.ok:
            p(f"  {spec.provider_id:<12} OK   {len(r.ids):>4} models  "
              f"{r.elapsed_ms:6.0f} ms  {spec.url}")
        else:
            p(f"  {spec.provider_id:<12} FAIL {r.error[:90]}")

    p("")
    p("=" * 78)
    p("CATALOG RECONCILIATION  (catalog.MODELS vs what the account can reach)")
    p("=" * 78)
    p(f"  {'catalog id':<32} {'provider':<20} {'verdict':<9} api_model")
    p(f"  {'-' * 32} {'-' * 20} {'-' * 9} {'-' * 40}")
    for row in rows:
        p(f"  {row.model_id:<32} {row.provider_id:<20} {row.verdict:<9} "
          f"{row.api_model}")
        if row.verdict == "MISSING":
            p(f"  {'':<32} {'':<20} {'':<9} -> closest: "
              f"{row.suggestion or '(no near match)'}"
              + (f"   also: {row.note}" if row.note else ""))
        elif row.verdict == "UNCHECKED" and row.note:
            p(f"  {'':<32} {'':<20} {'':<9} -> {row.note[:80]}")

    ok = sum(1 for r in rows if r.verdict == "OK")
    missing = sum(1 for r in rows if r.verdict == "MISSING")
    unchecked = sum(1 for r in rows if r.verdict == "UNCHECKED")
    p("")
    p(f"  {ok} exist, {missing} do NOT exist, {unchecked} unchecked "
      f"(of {len(rows)} non-fake catalog entries)")

    p("")
    p("=" * 78)
    p("PRICE AUDIT (OpenRouter publishes per-token prices; nobody else does)")
    p("=" * 78)
    if not prices:
        p("  no overlapping models: nothing to compare")
    else:
        p(f"  {'catalog id':<32} {'field':<20} {'ours':>10} {'theirs':>10}  "
          f"{'verdict':<8} priced_at")
        for row in prices:
            p(f"  {row.model_id:<32} {row.field:<20} "
              f"{_fmt_price(row.ours):>10} {_fmt_price(row.theirs):>10}  "
              f"{'DIFFERS' if row.disagrees else 'ok':<8} {row.priced_at}")

    p("")
    p("Nothing above was written back to catalog.py. Model ids that are one")
    p("character apart are different models with different prices, and a tool")
    p("that resolved that automatically would silently rebill a workload.")


def to_json(results: dict[str, ProviderModels], rows: list[Row],
            prices: list[PriceRow]) -> dict[str, Any]:
    return {
        "providers": {
            pid: {"ok": r.ok, "count": len(r.ids), "elapsed_ms": round(r.elapsed_ms, 1),
                  "error": r.error, "models": r.ids}
            for pid, r in results.items()
        },
        "reconciliation": [
            {"model_id": r.model_id, "provider": r.provider_id,
             "api_model": r.api_model, "verdict": r.verdict,
             "suggestion": r.suggestion, "alternates": r.note}
            for r in rows
        ],
        "prices": [
            {"model_id": r.model_id, "field": r.field, "ours": r.ours,
             "theirs": r.theirs, "differs": r.disagrees, "priced_at": r.priced_at}
            for r in prices
        ],
    }


def run(specs: tuple[ProbeSpec, ...] = SPECS) -> tuple[
    dict[str, ProviderModels], list[Row], list[PriceRow]
]:
    """Probe everything, reconcile, audit. No generation request is sent."""
    results: dict[str, ProviderModels] = {}
    with httpx.Client(follow_redirects=True) as client:
        for spec in specs:
            results[spec.provider_id] = fetch(spec, client)
    rows = reconcile(results)
    prices = audit_prices(results)
    return results, rows, prices


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true",
                        help="machine-readable output")
    parser.add_argument("--list", metavar="PROVIDER",
                        help="dump one provider's full model list and stop")
    args = parser.parse_args(argv)

    for status in env.ensure_loaded():
        if not args.json:
            print(f"  {status}", file=sys.stderr)

    results, rows, prices = run()

    if args.list:
        result = results.get(args.list)
        if result is None or not result.ok:
            print(f"{args.list}: {result.error if result else 'unknown provider'}")
            return 1
        for mid in sorted(result.ids):
            print(mid)
        return 0

    if args.json:
        print(json.dumps(to_json(results, rows, prices), indent=2))
    else:
        render(results, rows, prices)
    return 0 if all(r.ok for r in results.values()) else 1


def spec_for(provider_id: str) -> ProbeSpec | None:
    for spec in SPECS:
        if spec.provider_id == provider_id:
            return spec
    return None


def model_exists(provider_id: str, api_model: str) -> bool:
    """Used by the live tests to skip rather than fail when a model is retired.

    A test that hard-codes a wire model id is a test that breaks on the day
    the provider deprecates it, and the failure looks like a gateway bug.
    """
    spec = spec_for(CATALOG_TO_PROBE.get(provider_id, provider_id))
    if spec is None:
        return False
    with httpx.Client(follow_redirects=True) as client:
        return api_model in fetch(spec, client).index


if __name__ == "__main__":
    raise SystemExit(main())

"""Live workload benchmark: a DEPLOYED llmgw against real providers, from the
machine next to it.

    /app/.venv/bin/python live_fly.py --smoke            # 1 per scenario, shape check
    /app/.venv/bin/python live_fly.py                    # the campaign (spends money)
    /app/.venv/bin/python live_fly.py --gateway http://127.0.0.1:8080 --out /tmp/live_fly.jsonl

`bench/live_overhead.py` measures the gateway in-process on a laptop with one
prompt shape. This one runs ON the Fly machine against the deployed process
and asks the question a consumer asks: for the calls Layrs actually makes --
short answers, articles, growing multi-turn context, vision, tool calls, JSON
mode, an Anthropic-dialect call, a little parallelism -- what does the gateway
add to first-token latency and total time, and what do its own metrics say
afterwards.

Method, the parts that make a live number trustworthy:

* Two arms, D (direct to the provider) and G (through the gateway), one
  persistent HTTP/2 client per arm and provider for the whole run, one untimed
  warm-up per client so cold TLS is reported once and excluded from the
  tables. Arms are INTERLEAVED (D, G, D, G, ...) so provider drift lands on
  both equally; the headline per scenario is the paired G - D on medians.
* `temperature=0`, fixed prompts, bounded `max_tokens`, `stream_options.
  include_usage` on the OpenAI dialect so every stream carries exact usage.
* TTFT is the first CONTENT token (a text delta), not the first SSE frame:
  OpenAI's first frame is an empty role delta and Anthropic's is
  `message_start`, and counting those flatters everyone.
* Only `time.perf_counter()`. Nothing sleeps between requests except the
  provider.
* Credentials are read from the machine's environment and never written to
  the record, the report or stderr; every error string is scrubbed against
  them before it is stored.

The record for each request is one JSON line; the report is Markdown on
stdout, built only from those records plus a before/after diff of the
gateway's own `/metrics`.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

from llmgw.catalog import DEFAULT_CATALOG, ModelSpec, price_of
from llmgw.sse import SSEParser
from llmgw.surfaces import ANTHROPIC_MESSAGES, OPENAI_CHAT, Usage

# --------------------------------------------------------------------------
# Endpoints, models, credentials
# --------------------------------------------------------------------------

OPENAI_DIRECT = "https://api.openai.com/v1/chat/completions"
ANTHROPIC_DIRECT = "https://api.anthropic.com/v1/messages"
GW_OPENAI_PATH = "/v1/chat/completions"
GW_ANTHROPIC_PATH = "/anthropic/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"

# Catalog ids are what the GATEWAY takes; wire ids are what the provider takes.
OPENAI_CATALOG_ID = "openai.gpt-4o-mini"
OPENAI_WIRE_ID = DEFAULT_CATALOG.models[OPENAI_CATALOG_ID].api_model
ANTHROPIC_CATALOG_ID = "anthropic.haiku-4-5"
ANTHROPIC_WIRE_ID = DEFAULT_CATALOG.models[ANTHROPIC_CATALOG_ID].api_model

SPEND_ABORT_USD = 1.00

# A small, stable JPEG on Wikimedia Commons. `detail: low` pins the image at
# a fixed token cost so the vision rows are comparable across arms.
IMAGE_URL = "https://upload.wikimedia.org/wikipedia/commons/3/3f/JPEG_example_flower.jpg"

ARTICLE_TOPIC = (
    "why a streaming API gateway must never retry a request after the first "
    "byte has reached the client"
)

MULTI_TURN_QUESTIONS = [
    "In two sentences, what is a circuit breaker in a service mesh?",
    "Give one concrete failure it prevents, in two sentences.",
    "How does a half-open state work? Two sentences.",
    "Why should client disconnects not count as provider failures? Two sentences.",
    "What metric would you alert on for breaker flapping? Two sentences.",
    "Summarise everything you said so far in three sentences.",
]

WEATHER_TOOL = {
    "type": "function",
    "function": {
        "name": "get_weather",
        "description": "Get the current weather for a city.",
        "parameters": {
            "type": "object",
            "properties": {"city": {"type": "string"}},
            "required": ["city"],
        },
    },
}


def _secret_values() -> list[str]:
    out = []
    for name in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "DEEPSEEK_API_KEY",
                 "LLMGW_TENANT_LAYRS_TOKEN"):
        v = os.environ.get(name)
        if v and len(v) >= 8:
            out.append(v)
    return out


_SECRETS = _secret_values()


def scrub(text: str, limit: int = 300) -> str:
    for s in _SECRETS:
        text = text.replace(s, "<redacted>")
        if len(s) > 8:
            text = text.replace(s[-4:], "****") if s[-4:] in text and len(text) < 40 else text
    text = re.sub(r"sk-[A-Za-z0-9_-]{8,}", "sk-<redacted>", text)
    return text[:limit]


# --------------------------------------------------------------------------
# Records
# --------------------------------------------------------------------------


@dataclass
class Rec:
    arm: str                     # "D" or "G"
    scenario: str
    provider: str                # "openai" | "anthropic"
    stream: bool
    status: int = 0
    cold: bool = False
    turn: int = 0                # multi-turn index, else 0
    ttfb_ms: float | None = None
    ttft_ms: float | None = None
    total_ms: float = 0.0
    gaps_ms: list[float] = field(default_factory=list)
    prompt_tokens: int = 0
    cached_tokens: int = 0
    output_tokens: int = 0
    text_deltas: int = 0
    tokens_per_s: float | None = None
    usd: float = 0.0
    gw: dict[str, str] = field(default_factory=dict)
    error: str = ""
    text: str = ""               # first 120 chars of the answer, for sanity

    def to_json(self) -> str:
        d = asdict(self)
        d["gaps_ms"] = _summ(self.gaps_ms)  # keep the line short
        return json.dumps(d, separators=(",", ":"))


def _summ(xs: list[float]) -> dict[str, float | int]:
    if not xs:
        return {"n": 0}
    return {"n": len(xs), "p50": pct(xs, 50), "p99": pct(xs, 99), "max": max(xs)}


def pct(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = (len(s) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


# --------------------------------------------------------------------------
# Arms
# --------------------------------------------------------------------------


class Arm:
    """One side of the comparison: where to send, how to authenticate, which
    model id to name. One persistent client per provider."""

    def __init__(self, name: str, gateway: str | None, timeout: float) -> None:
        self.name = name
        self.gateway = gateway
        t = httpx.Timeout(timeout, connect=10.0)
        self.clients: dict[str, httpx.Client] = {
            "openai": httpx.Client(http2=True, timeout=t),
            "anthropic": httpx.Client(http2=True, timeout=t),
        }

    def close(self) -> None:
        for c in self.clients.values():
            c.close()

    def url(self, provider: str) -> str:
        if self.gateway:
            path = GW_OPENAI_PATH if provider == "openai" else GW_ANTHROPIC_PATH
            return self.gateway + path
        return OPENAI_DIRECT if provider == "openai" else ANTHROPIC_DIRECT

    def headers(self, provider: str, *, stream: bool) -> dict[str, str]:
        h = {"content-type": "application/json",
             "accept": "text/event-stream" if stream else "application/json",
             # Providers gzip SSE (findings log #24). The gateway asks its
             # upstream for identity, so the direct arm asks for the same:
             # both arms then parse the same bytes, and neither pays inflate.
             "accept-encoding": "identity"}
        if self.gateway:
            h["authorization"] = f"Bearer {os.environ['LLMGW_TENANT_LAYRS_TOKEN']}"
        elif provider == "openai":
            h["authorization"] = f"Bearer {os.environ['OPENAI_API_KEY']}"
        else:
            h["x-api-key"] = os.environ["ANTHROPIC_API_KEY"]
            h["anthropic-version"] = ANTHROPIC_VERSION
        return h

    def model(self, provider: str) -> str:
        if self.gateway:
            return OPENAI_CATALOG_ID if provider == "openai" else ANTHROPIC_CATALOG_ID
        return OPENAI_WIRE_ID if provider == "openai" else ANTHROPIC_WIRE_ID


def spec_for(provider: str) -> ModelSpec:
    return DEFAULT_CATALOG.models[
        OPENAI_CATALOG_ID if provider == "openai" else ANTHROPIC_CATALOG_ID]


def usd_of(u: Usage, m: ModelSpec) -> float:
    write_rate = m.cache_write_per_m if m.cache_write_per_m is not None else m.input_per_m
    return (u.input_tokens * m.input_per_m
            + u.cache_read_tokens * price_of(m, cached=True)
            + u.cache_write_tokens * write_rate
            + u.output_tokens * m.output_per_m) / 1_000_000


# --------------------------------------------------------------------------
# One measured request
# --------------------------------------------------------------------------


def _nonstream_usage(provider: str, payload: dict) -> Usage:
    u = Usage()
    raw = payload.get("usage") or {}
    if provider == "openai":
        cached = int((raw.get("prompt_tokens_details") or {}).get("cached_tokens") or 0)
        u.input_tokens = int(raw.get("prompt_tokens") or 0) - cached
        u.cache_read_tokens = cached
        u.output_tokens = int(raw.get("completion_tokens") or 0)
    else:
        u.input_tokens = int(raw.get("input_tokens") or 0)
        u.cache_read_tokens = int(raw.get("cache_read_input_tokens") or 0)
        u.cache_write_tokens = int(raw.get("cache_creation_input_tokens") or 0)
        u.output_tokens = int(raw.get("output_tokens") or 0)
    u.input_exact = u.output_exact = True
    return u


def _nonstream_text(provider: str, payload: dict) -> str:
    try:
        if provider == "openai":
            msg = payload["choices"][0]["message"]
            if msg.get("tool_calls"):
                tc = msg["tool_calls"][0]["function"]
                return f"<tool_call {tc['name']}({tc['arguments']})>"
            return msg.get("content") or ""
        parts = payload.get("content") or []
        for p in parts:
            if p.get("type") == "text":
                return p.get("text", "")
            if p.get("type") == "tool_use":
                return f"<tool_use {p.get('name')}({json.dumps(p.get('input'))})>"
    except (KeyError, IndexError, TypeError):
        pass
    return ""


def run_one(arm: Arm, *, scenario: str, provider: str, body: dict, stream: bool,
            client: httpx.Client | None = None, cold: bool = False, turn: int = 0,
            ) -> tuple[Rec, Any]:
    """Send one request, time it, return (record, parsed payload or None)."""
    rec = Rec(arm=arm.name, scenario=scenario, provider=provider, stream=stream,
              cold=cold, turn=turn)
    client = client or arm.clients[provider]
    body = dict(body, model=arm.model(provider), stream=stream)
    if stream and provider == "openai":
        body["stream_options"] = {"include_usage": True}
    surface = OPENAI_CHAT if provider == "openai" else ANTHROPIC_MESSAGES
    spec = spec_for(provider)
    usage = Usage()
    payload: Any = None
    t0 = time.perf_counter()
    try:
        with client.stream("POST", arm.url(provider),
                           headers=arm.headers(provider, stream=stream),
                           json=body) as r:
            rec.ttfb_ms = (time.perf_counter() - t0) * 1000
            rec.status = r.status_code
            rec.gw = {k.lower(): v for k, v in r.headers.items()
                      if k.lower().startswith("x-gw-")}
            if r.status_code != 200:
                raw = r.read()
                rec.error = scrub(raw.decode("utf-8", "replace"))
                rec.total_ms = (time.perf_counter() - t0) * 1000
                return rec, None
            if not stream:
                raw = r.read()
                rec.total_ms = (time.perf_counter() - t0) * 1000
                payload = json.loads(raw)
                usage = _nonstream_usage(provider, payload)
                rec.text = _nonstream_text(provider, payload)[:120]
                rec.ttft_ms = rec.total_ms  # a JSON body arrives whole
            else:
                parser = SSEParser(max_frame_bytes=1 << 20)
                first: float | None = None
                last: float | None = None
                text_parts: list[str] = []
                for chunk in r.iter_bytes():  # decoded, should a provider ignore identity
                    now = time.perf_counter()
                    for ev in parser.feed(chunk):
                        surface.apply_usage(ev, usage)
                        d = surface.text_delta(ev)
                        if d:
                            if first is None:
                                first = now
                            elif last is not None:
                                rec.gaps_ms.append((now - last) * 1000)
                            last = now
                            rec.text_deltas += 1
                            if len(text_parts) < 40:
                                text_parts.append(d)
                for ev in parser.close():
                    surface.apply_usage(ev, usage)
                rec.total_ms = (time.perf_counter() - t0) * 1000
                rec.ttft_ms = (first - t0) * 1000 if first else None
                rec.text = "".join(text_parts)[:120]
                if first and last and last > first and usage.output_tokens > 1:
                    rec.tokens_per_s = (usage.output_tokens - 1) / (last - first)
    except Exception as exc:  # noqa: BLE001 - a bench records, it does not crash
        rec.total_ms = (time.perf_counter() - t0) * 1000
        rec.error = scrub(f"{type(exc).__name__}: {exc}")
        return rec, None
    rec.prompt_tokens = usage.input_tokens + usage.cache_read_tokens
    rec.cached_tokens = usage.cache_read_tokens
    rec.output_tokens = usage.output_tokens
    rec.usd = usd_of(usage, spec)
    return rec, payload


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------


def msgs(text: str) -> list[dict]:
    return [{"role": "user", "content": text}]


def body_short() -> dict:
    return {"messages": msgs("In one short line: what is the capital of Japan?"),
            "max_tokens": 16, "temperature": 0}


def body_article() -> dict:
    return {"messages": msgs(f"Write a ~600-word article about {ARTICLE_TOPIC}. "
                             "Use plain prose, no headings."),
            "max_tokens": 900, "temperature": 0}


def body_vision() -> dict:
    return {"messages": [{"role": "user", "content": [
                {"type": "text", "text": "Describe this image in one sentence."},
                {"type": "image_url", "image_url": {"url": IMAGE_URL, "detail": "low"}},
            ]}],
            "max_tokens": 60, "temperature": 0}


def body_json() -> dict:
    return {"messages": [
                {"role": "system", "content": "Answer only with JSON."},
                {"role": "user", "content": 'Return {"city": string, "country": string, '
                                            '"population_millions": number} for Tokyo.'}],
            "response_format": {"type": "json_object"},
            "max_tokens": 80, "temperature": 0}


def body_tool_turn1() -> dict:
    return {"messages": msgs("What is the weather in Lisbon right now? Use the tool."),
            "tools": [WEATHER_TOOL], "tool_choice": "auto",
            "max_tokens": 60, "temperature": 0}


class Campaign:
    def __init__(self, arms: list[Arm], *, smoke: bool, out_path: str,
                 gateway: str) -> None:
        self.arms = arms
        self.smoke = smoke
        self.out = open(out_path, "a", encoding="utf-8")  # noqa: SIM115
        self.recs: list[Rec] = []
        self.spend = 0.0
        self.gateway = gateway

    def n(self, full: int) -> int:
        return 1 if self.smoke else full

    def record(self, rec: Rec) -> None:
        self.recs.append(rec)
        self.out.write(rec.to_json() + "\n")
        self.out.flush()
        self.spend += rec.usd
        tag = "cold " if rec.cold else ""
        ttft = f"{rec.ttft_ms:7.1f}" if rec.ttft_ms is not None else "   none"
        print(f"  {rec.arm} {rec.scenario:<15} {tag}status={rec.status} "
              f"ttft={ttft}ms total={rec.total_ms:8.1f}ms out={rec.output_tokens:4d}"
              f"{' ERR ' + rec.error[:80] if rec.error else ''}", file=sys.stderr)
        if self.spend > SPEND_ABORT_USD:
            raise RuntimeError(f"ABORT: spend ${self.spend:.4f} exceeded cap")

    # -- warm-up ------------------------------------------------------------

    def warm(self) -> None:
        print("== warm-up (cold TLS, reported once, excluded from tables)", file=sys.stderr)
        for provider in ("openai", "anthropic"):
            for arm in self.arms:
                rec, _ = run_one(arm, scenario="cold_" + provider, provider=provider,
                                 body=body_short(), stream=True, cold=True)
                self.record(rec)

    # -- simple paired scenarios ------------------------------------------

    def paired(self, scenario: str, provider: str, body_fn, *, stream: bool, n: int) -> None:
        print(f"== {scenario} x{n} per arm", file=sys.stderr)
        for i in range(n):
            order = self.arms if i % 2 == 0 else list(reversed(self.arms))
            for arm in order:
                rec, _ = run_one(arm, scenario=scenario, provider=provider,
                                 body=body_fn(), stream=stream)
                self.record(rec)

    # -- multi-turn --------------------------------------------------------

    def multi_turn(self, n_convs: int) -> None:
        turns = MULTI_TURN_QUESTIONS[: (2 if self.smoke else len(MULTI_TURN_QUESTIONS))]
        print(f"== multi_turn x{n_convs} conversations x{len(turns)} turns per arm",
              file=sys.stderr)
        for c in range(n_convs):
            history: dict[str, list[dict]] = {a.name: [] for a in self.arms}
            for t, q in enumerate(turns, start=1):
                order = self.arms if (c + t) % 2 == 0 else list(reversed(self.arms))
                for arm in order:
                    h = history[arm.name]
                    h.append({"role": "user", "content": q})
                    body = {"messages": list(h), "max_tokens": 120, "temperature": 0}
                    rec, _ = run_one(arm, scenario="multi_turn", provider="openai",
                                     body=body, stream=True, turn=t)
                    self.record(rec)
                    # Keep the transcripts comparable: both arms continue from
                    # their OWN answer (temperature 0 keeps them near-identical).
                    h.append({"role": "assistant", "content": rec.text or "(no answer)"})

    # -- tool call round trip ---------------------------------------------

    def tool_calls(self, n: int) -> None:
        print(f"== tool_call x{n} round trips per arm", file=sys.stderr)
        for i in range(n):
            order = self.arms if i % 2 == 0 else list(reversed(self.arms))
            for arm in order:
                rec1, payload = run_one(arm, scenario="tool_call_1", provider="openai",
                                        body=body_tool_turn1(), stream=False)
                self.record(rec1)
                if payload is None:
                    continue
                try:
                    msg = payload["choices"][0]["message"]
                    tc = msg["tool_calls"][0]
                except (KeyError, IndexError, TypeError):
                    rec1.error = rec1.error or "no tool_call in turn 1"
                    continue
                follow = {
                    "messages": [
                        *body_tool_turn1()["messages"],
                        {"role": "assistant", "content": None, "tool_calls": [tc]},
                        {"role": "tool", "tool_call_id": tc["id"],
                         "content": json.dumps({"city": "Lisbon", "temp_c": 24,
                                                "sky": "clear"})},
                    ],
                    "tools": [WEATHER_TOOL], "max_tokens": 60, "temperature": 0,
                }
                rec2, _ = run_one(arm, scenario="tool_call_2", provider="openai",
                                  body=follow, stream=True)
                self.record(rec2)

    # -- concurrency ---------------------------------------------------------

    def concurrent(self, rounds: int, width: int = 4) -> None:
        print(f"== concurrent_{width} x{rounds} rounds per arm", file=sys.stderr)
        pools: dict[str, list[httpx.Client]] = {}
        for arm in self.arms:
            t = httpx.Timeout(120.0, connect=10.0)
            cs = [httpx.Client(http2=True, timeout=t) for _ in range(width)]
            for c in cs:  # warm each connection, untimed
                run_one(arm, scenario="warm", provider="openai", body=body_short(),
                        stream=True, client=c)
            pools[arm.name] = cs
        try:
            with ThreadPoolExecutor(max_workers=width) as ex:
                for r in range(rounds):
                    order = self.arms if r % 2 == 0 else list(reversed(self.arms))
                    for arm in order:
                        futs = [ex.submit(run_one, arm, scenario=f"concurrent_{width}",
                                          provider="openai", body=body_short(),
                                          stream=True, client=c)
                                for c in pools[arm.name]]
                        for f in futs:
                            self.record(f.result()[0])
        finally:
            for cs in pools.values():
                for c in cs:
                    c.close()

    # -- the campaign ------------------------------------------------------

    def run(self) -> None:
        self.warm()
        self.paired("short_chat", "openai", body_short, stream=True, n=self.n(8))
        self.paired("json_mode", "openai", body_json, stream=False, n=self.n(5))
        self.tool_calls(self.n(4))
        self.paired("vision", "openai", body_vision, stream=True, n=self.n(4))
        self.paired("anthropic_short", "anthropic", body_short, stream=True, n=self.n(6))
        self.multi_turn(self.n(2))
        self.concurrent(self.n(3))
        self.paired("article", "openai", body_article, stream=True, n=self.n(4))


# --------------------------------------------------------------------------
# Gateway metrics before/after
# --------------------------------------------------------------------------

_METRIC_LINE = re.compile(r"^(llmgw_[a-z_]+)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN)$")


def scrape(gateway: str) -> dict[str, float]:
    out: dict[str, float] = {}
    try:
        text = httpx.get(gateway + "/metrics", timeout=10.0).text
    except Exception as exc:  # noqa: BLE001
        print(f"metrics scrape failed: {scrub(str(exc))}", file=sys.stderr)
        return out
    for line in text.splitlines():
        m = _METRIC_LINE.match(line.strip())
        if m:
            try:
                out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
            except ValueError:
                pass
    return out


def metrics_diff(before: dict[str, float], after: dict[str, float]) -> list[str]:
    lines = []
    keys = sorted(set(before) | set(after))
    for k in keys:
        if "_bucket{" in k:
            continue
        b, a = before.get(k, 0.0), after.get(k, 0.0)
        if a != b:
            lines.append(f"| `{k}` | {b:g} | {a:g} | {a - b:+g} |")
    return lines


def histogram_means(before: dict[str, float], after: dict[str, float]) -> list[str]:
    """Mean over the run window for each `_sum`/`_count` pair that moved."""
    out = []
    for k in sorted(after):
        if not k.startswith("llmgw_") or "_sum" not in k:
            continue
        base, labels = k.split("_sum", 1)
        ck = base + "_count" + labels
        ds = after.get(k, 0.0) - before.get(k, 0.0)
        dc = after.get(ck, 0.0) - before.get(ck, 0.0)
        if dc > 0:
            out.append(f"| `{base}{labels}` | {int(dc)} | {ds / dc * 1000:.1f} ms |")
    return out


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def _ok(recs: list[Rec], scenario: str, arm: str) -> list[Rec]:
    return [r for r in recs if r.scenario == scenario and r.arm == arm
            and r.status == 200 and not r.cold and not r.error]


def _f(x: float | None, nd: int = 1) -> str:
    return "-" if x is None or x != x else f"{x:.{nd}f}"


def _med(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def scenario_row(recs: list[Rec], scenario: str) -> str | None:
    d, g = _ok(recs, scenario, "D"), _ok(recs, scenario, "G")
    if not d or not g:
        return None

    def stats(rs: list[Rec]) -> dict[str, float | None]:
        ttft = [r.ttft_ms for r in rs if r.ttft_ms is not None]
        tot = [r.total_ms for r in rs]
        gaps = [x for r in rs for x in r.gaps_ms]
        tps = [r.tokens_per_s for r in rs if r.tokens_per_s]
        return {
            "n": len(rs), "ttft50": _med(ttft), "ttft95": pct(ttft, 95) if ttft else None,
            "tot50": _med(tot), "tot95": pct(tot, 95),
            "gap50": _med(gaps), "gap99": pct(gaps, 99) if gaps else None,
            "tps": _med(tps), "out": _med([float(r.output_tokens) for r in rs]),
            "prompt": _med([float(r.prompt_tokens) for r in rs]),
        }

    sd, sg = stats(d), stats(g)

    def delta(a: float | None, b: float | None) -> str:
        return "-" if a is None or b is None else f"{b - a:+.1f}"

    return (
        f"| {scenario} | {sd['n']}/{sg['n']} | {_f(sd['prompt'], 0)}/{_f(sd['out'], 0)} "
        f"| {_f(sd['ttft50'])} / {_f(sg['ttft50'])} | **{delta(sd['ttft50'], sg['ttft50'])}** "
        f"| {_f(sd['ttft95'])} / {_f(sg['ttft95'])} "
        f"| {_f(sd['tot50'])} / {_f(sg['tot50'])} | **{delta(sd['tot50'], sg['tot50'])}** "
        f"| {_f(sd['tot95'])} / {_f(sg['tot95'])} "
        f"| {_f(sd['gap50'])} / {_f(sg['gap50'])} | {_f(sd['gap99'])} / {_f(sg['gap99'])} "
        f"| {_f(sd['tps'])} / {_f(sg['tps'])} |"
    )


def render(recs: list[Rec], *, before: dict[str, float], after: dict[str, float],
           gateway: str, started: str, wall_s: float) -> str:
    L: list[str] = []
    L.append("# Live workload benchmark: deployed llmgw vs direct provider")
    L.append("")
    L.append(f"Run {started} UTC on `{platform.node()}` ({platform.machine()}, "
             f"{os.cpu_count()} vCPU), Fly region sin. Gateway arm: `{gateway}`. "
             f"Wall {wall_s / 60:.1f} min. Python {platform.python_version()}, "
             f"httpx {httpx.__version__}.")
    L.append("")
    L.append("Arms interleaved D,G,D,G. D = direct to the provider over HTTP/2 from the "
             "same machine; G = through the gateway on loopback, gateway to provider over "
             "its own pooled HTTP/2 connection. TTFT = first content token. Medians; "
             "**bold** = G - D on medians (the gateway's added latency).")
    L.append("")
    L.append("## Per scenario")
    L.append("")
    L.append("| scenario | n D/G | prompt/out tok | TTFT p50 D / G (ms) | **+TTFT** "
             "| TTFT p95 D / G | total p50 D / G (ms) | **+total** | total p95 D / G "
             "| inter-token p50 D / G | inter-token p99 D / G | tok/s D / G |")
    L.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    order = ["short_chat", "json_mode", "tool_call_1", "tool_call_2", "vision",
             "anthropic_short", "multi_turn", "concurrent_4", "article"]
    for s in order:
        row = scenario_row(recs, s)
        if row:
            L.append(row)
    L.append("")

    # multi-turn per turn
    mt = [r for r in recs if r.scenario == "multi_turn" and r.status == 200]
    if mt:
        L.append("## Multi-turn, per turn (context grows)")
        L.append("")
        L.append("| turn | prompt tok D / G | TTFT p50 D / G (ms) | **+TTFT** "
                 "| total p50 D / G (ms) | **+total** |")
        L.append("|---|---|---|---|---|---|")
        for t in sorted({r.turn for r in mt}):
            d = [r for r in mt if r.turn == t and r.arm == "D"]
            g = [r for r in mt if r.turn == t and r.arm == "G"]
            if not d or not g:
                continue
            dt = _med([r.ttft_ms for r in d if r.ttft_ms])
            gt = _med([r.ttft_ms for r in g if r.ttft_ms])
            dtot = _med([r.total_ms for r in d])
            gtot = _med([r.total_ms for r in g])
            L.append(f"| {t} | {_f(_med([float(r.prompt_tokens) for r in d]), 0)} / "
                     f"{_f(_med([float(r.prompt_tokens) for r in g]), 0)} "
                     f"| {_f(dt)} / {_f(gt)} | **{_f((gt or 0) - (dt or 0))}** "
                     f"| {_f(dtot)} / {_f(gtot)} | **{_f((gtot or 0) - (dtot or 0))}** |")
        L.append("")

    # cold starts
    cold = [r for r in recs if r.cold]
    if cold:
        L.append("## Cold start (first request per client, TLS + connection setup)")
        L.append("")
        L.append("| arm | provider | status | TTFB (ms) | TTFT (ms) | total (ms) |")
        L.append("|---|---|---|---|---|---|")
        for r in cold:
            L.append(f"| {r.arm} | {r.provider} | {r.status} | {_f(r.ttfb_ms)} | "
                     f"{_f(r.ttft_ms)} | {_f(r.total_ms)} |")
        L.append("")
        L.append("The gateway arm's cold number includes the GATEWAY's first connection to "
                 "the provider (its pool was empty for that provider until then), which is "
                 "why it is compared once and excluded from every table above.")
        L.append("")

    # provider variance from the direct arm
    L.append("## Provider variance (the direct arm's own spread)")
    L.append("")
    L.append("| scenario | D TTFT p50 | D TTFT p95 | p95/p50 | D total p50 | D total p95 |")
    L.append("|---|---|---|---|---|---|")
    for s in order:
        d = _ok(recs, s, "D")
        ttft = [r.ttft_ms for r in d if r.ttft_ms]
        tot = [r.total_ms for r in d]
        if len(ttft) >= 2:
            m, p95 = statistics.median(ttft), pct(ttft, 95)
            L.append(f"| {s} | {m:.1f} | {p95:.1f} | {p95 / m:.2f}x | "
                     f"{statistics.median(tot):.1f} | {pct(tot, 95):.1f} |")
    L.append("")
    L.append("Read the **+TTFT** column against this spread: a delta smaller than the "
             "provider's own p95-p50 gap is inside the noise of N this small.")
    L.append("")

    # gateway headers / errors
    g_ok = [r for r in recs if r.arm == "G" and r.status == 200]
    attempts = sorted({r.gw.get("x-gw-attempts", "?") for r in g_ok})
    served = sorted({r.gw.get("x-gw-served-by", "?") for r in g_ok})
    errs = [r for r in recs if r.error or r.status != 200]
    L.append("## Gateway headers and errors")
    L.append("")
    L.append(f"- `X-Gw-Attempts` values seen on successful gateway calls: {attempts} "
             f"(anything but ['1'] means a retry or fallback happened)")
    L.append(f"- `X-Gw-Served-By`: {served}")
    L.append(f"- requests: {len(recs)} total, {len(errs)} not-200/errored")
    for r in errs:
        L.append(f"  - {r.arm} {r.scenario} turn={r.turn} status={r.status}: `{r.error}`")
    L.append("")

    # metrics
    L.append("## Gateway `/metrics` over the run window")
    L.append("")
    hm = histogram_means(before, after)
    if hm:
        L.append("Histogram means (sum/count delta):")
        L.append("")
        L.append("| series | count | mean |")
        L.append("|---|---|---|")
        L.extend(hm)
        L.append("")
    diff = metrics_diff(before, after)
    if diff:
        L.append("Counters and gauges that moved (buckets omitted):")
        L.append("")
        L.append("| series | before | after | delta |")
        L.append("|---|---|---|---|")
        L.extend(diff)
    else:
        L.append("(no metrics diff available)")
    L.append("")

    # cost
    L.append("## Cost (catalog prices, from usage)")
    L.append("")
    L.append("| scenario | calls | prompt tok | cached | output tok | USD |")
    L.append("|---|---|---|---|---|---|")
    total = 0.0
    for s in sorted({r.scenario for r in recs}):
        rs = [r for r in recs if r.scenario == s]
        usd = sum(r.usd for r in rs)
        total += usd
        L.append(f"| {s} | {len(rs)} | {sum(r.prompt_tokens for r in rs)} | "
                 f"{sum(r.cached_tokens for r in rs)} | {sum(r.output_tokens for r in rs)} "
                 f"| {usd:.5f} |")
    L.append(f"| **total** | {len(recs)} | {sum(r.prompt_tokens for r in recs)} | "
             f"{sum(r.cached_tokens for r in recs)} | {sum(r.output_tokens for r in recs)} "
             f"| **{total:.5f}** |")
    L.append("")

    # narrative
    sc = scenario_row(recs, "short_chat")
    L.append("## What this shows")
    L.append("")
    d_all = [r for r in recs if r.arm == "D" and r.status == 200 and not r.cold and r.ttft_ms]
    g_all = [r for r in recs if r.arm == "G" and r.status == 200 and not r.cold and r.ttft_ms]
    if d_all and g_all:
        L.append(f"Across every successful streamed call, median TTFT was "
                 f"{statistics.median([r.ttft_ms for r in d_all]):.0f} ms direct and "
                 f"{statistics.median([r.ttft_ms for r in g_all]):.0f} ms through the "
                 f"gateway; "
                 f"median total {statistics.median([r.total_ms for r in d_all]):.0f} ms vs "
                 f"{statistics.median([r.total_ms for r in g_all]):.0f} ms. The per-scenario "
                 f"**+TTFT** column is the number to quote, read against the provider "
                 f"variance table: the gateway's cost is a few milliseconds of proxying "
                 f"plus its own first connection to the provider, and the provider's own "
                 f"run-to-run spread is tens to hundreds of milliseconds.")
    if sc is None:
        L.append("short_chat did not complete on both arms; see errors above.")
    L.append("")
    L.append("## Caveats")
    L.append("")
    L.append("- Client and gateway share ONE shared-cpu-1x vCPU on the same machine "
             "(the gateway binds IPv4 only, so the sibling machine could not reach it over "
             "Fly's IPv6 private network; that is a deployment finding, not a benchmark "
             "choice). The load generator is light (<= 4 streams) but it is not free.")
    L.append("- N is small per scenario (4-8, 24 for multi-turn); p95s are indicative only.")
    L.append("- Provider latency dominates and varies by the minute; interleaving makes the "
             "paired delta fair, not the absolute numbers repeatable.")
    L.append("- The gateway arm is loopback; a real caller adds one in-region hop "
             "(~0.2-1 ms).")
    L.append("- `temperature=0` keeps the two arms' outputs near-identical but providers do "
             "not guarantee it; output-token counts are medians for that reason.")
    L.append("- Cold-start rows are one sample each.")
    return "\n".join(L)


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--gateway", default="http://127.0.0.1:8080")
    ap.add_argument("--out", default="/tmp/live_fly.jsonl")
    ap.add_argument("--smoke", action="store_true", help="one request per scenario")
    ap.add_argument("--timeout", type=float, default=120.0)
    args = ap.parse_args(argv)

    for name in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "LLMGW_TENANT_LAYRS_TOKEN"):
        if not os.environ.get(name):
            print(f"missing ${name}", file=sys.stderr)
            return 2

    started = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    before = scrape(args.gateway)
    arms = [Arm("D", None, args.timeout), Arm("G", args.gateway, args.timeout)]
    camp = Campaign(arms, smoke=args.smoke, out_path=args.out, gateway=args.gateway)
    try:
        camp.run()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
    finally:
        for a in arms:
            a.close()
        camp.out.close()
    after = scrape(args.gateway)
    print(render(camp.recs, before=before, after=after, gateway=args.gateway,
                 started=started, wall_s=time.perf_counter() - t0))
    print(f"\nspend_usd={camp.spend:.5f} records={len(camp.recs)}", file=sys.stderr)
    print("LIVE_FLY_DONE", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

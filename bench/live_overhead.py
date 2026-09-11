"""Live gateway-overhead benchmark: llmgw against REAL providers.

    .venv/bin/python -m bench.live_overhead --calibrate   # 2 real calls, no report
    LLMGW_LIVE_OVERHEAD=1 .venv/bin/python -m bench.live_overhead   # full run, spends money

This is the real-provider sibling of `bench/overhead.py`. Where that bench
points the catalog at a deterministic in-process fake so the delta is pure
gateway CPU, this one builds the SAME app with `fake_upstreams=False` and the
DEFAULT_CATALOG + real credentials, and measures the overhead the way a
deployment actually pays it: client -> gateway -> real provider over real TLS.

Three conditions, all against real providers:

    1. clean     -- one working target, a SIGNIFICANT prompt (~6 KB doc to
                    summarise), bounded max_tokens. Paired, interleaved D vs G.
    2. switch    -- plan [failing-primary, real-secondary], BOTH real providers.
                    Primary is an INVALID model id on OpenAI (404, free, no
                    tokens); secondary is the real DeepSeek success model.
    3. retry     -- retry-same to a live success cannot be induced
                    deterministically (you cannot make a healthy provider emit
                    a transient 5xx then a 200). So a local flaky relay returns
                    one controlled 503 and then REVERSE-PROXIES the request to
                    the real provider: the eventual success is genuine DeepSeek
                    tokens. The retry MACHINERY is ALSO measured in-process.

Methodology (what makes a live number trustworthy):
    * Provider latency has fat tails, so the headline is the PAIRED, interleaved
      per-iteration delta G_i - D_i (same prompt, back to back, alternating
      order) -- both arms share one provider-latency window. Absolute D and G
      are reported alongside for context.
    * Modest N (live calls cost money + hit rate limits). Warm up a few, discard.
    * time.perf_counter() only.
    * HARD assertions that we measured the gateway (X-Gw-* on G, absent on D) and
      that switch/retry actually happened (X-Gw-Attempts, X-Gw-Served-By) -- the
      fake bench's first run secretly measured rate-limited rejections, so the
      tenant limiter and breaker are lifted out of reach and proven irrelevant.
    * Cost: max_tokens capped, every successful completion priced from the SAME
      catalog the gateway bills with, running total aborts past a few dollars.

Credentials are loaded at runtime from a file outside the repo via live.env and
are NEVER printed, logged, or written to the report.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import socket
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime

import httpx
import uvicorn
from live.smoke import usd_of  # the accounting the gateway itself bills with
from starlette.applications import Starlette
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from bench._document import DOCUMENT
from live import env
from llmgw.admission import TenantLimits
from llmgw.breaker import BreakerPolicy
from llmgw.catalog import DEFAULT_CATALOG, Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets, Deadline, SystemClock
from llmgw.errors import UpstreamOverloaded, decide
from llmgw.retry import RetryBudget, RetryPolicy
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from llmgw.sse import SSEParser
from llmgw.surfaces import OPENAI_CHAT, Usage

# --------------------------------------------------------------------------
# The fake bench found the anon tenant's 100 rps limiter silently rejecting
# most requests and the breaker tripping after a few failures -- both would
# measure the WRONG thing here (a fast gate instead of real upstream work). So
# lift both out of reach; the code still runs on every request, it just never
# rejects. We then ASSERT X-Gw-Attempts proves the upstream work happened.
# --------------------------------------------------------------------------
UNLIMITED_TENANT = TenantLimits(
    rate_per_second=1e9, burst=1_000_000_000, max_concurrency=1_000_000
)
BREAKER_NEVER_TRIPS = BreakerPolicy(failure_threshold=1_000_000)

# The SUCCESS path: real DeepSeek, first-party, cheap, fast, and confirmed
# reachable by `make probe` today (deepseek-v4-pro is one of the two ids the
# account can reach; deepseek-v4-flash is now MISSING at the provider).
SUCCESS_MODEL_ID = "deepseek.deepseek-v4-pro"
DEEPSEEK_API_MODEL = "deepseek-v4-pro"
DEEPSEEK_CHAT_URL = "https://api.deepseek.com/v1/chat/completions"

# The failing PRIMARY for the switch: a real OpenAI provider (key present,
# reaches 135 models) asked for a model id that does not exist -> 404/400,
# which is ModelNotFound/InvalidRequest (try_next=True) and bills NO tokens.
GHOST_PROVIDER = "openai"
GHOST_MODEL_ID = "bench.openai-ghost"
GHOST_API_MODEL = "gpt-ghost-does-not-exist-4o-9999"
OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"

# The retry target: a local flaky relay that fails once (503) then reverse-
# proxies to real DeepSeek. Its own credential id so a controlled 503 cannot
# touch the real deepseek circuit (the breaker is off anyway -- belt + braces).
RELAY_PROVIDER = "deepseek-relay"
RELAY_MODEL_ID = "bench.deepseek-relay"

OPENAI_PATH = "/v1/chat/completions"
MAX_TOKENS = 256
PROMPT = DOCUMENT + "\n\nSummarise the passage above in exactly three sentences."
PROMPT_BYTES = len(PROMPT.encode())

SPEND_ABORT_USD = 2.00  # abort and report if the running total would pass this


# --------------------------------------------------------------------------
# Running spend tracker (priced from the catalog the gateway bills with)
# --------------------------------------------------------------------------


@dataclass
class Spend:
    usd: float = 0.0
    completions: int = 0
    input_tokens: int = 0
    output_tokens: int = 0

    def add(self, usage: Usage, model: ModelSpec) -> None:
        if usage.output_tokens or usage.input_tokens:
            self.usd += usd_of(usage, model)
            self.completions += 1
            self.input_tokens += usage.input_tokens
            self.output_tokens += usage.output_tokens
        if self.usd > SPEND_ABORT_USD:
            raise RuntimeError(
                f"ABORT: running spend ${self.usd:.4f} exceeded cap "
                f"${SPEND_ABORT_USD:.2f} after {self.completions} completions"
            )


# --------------------------------------------------------------------------
# What we send
# --------------------------------------------------------------------------


def make_body(*, stream: bool, model: str = "placeholder") -> dict:
    body: dict = {
        "model": model,
        "max_tokens": MAX_TOKENS,
        "messages": [{"role": "user", "content": PROMPT}],
        "stream": stream,
    }
    if stream:
        # OpenAI-dialect providers only emit a usage frame when asked; without
        # this every streamed request is billed by estimate.
        body["stream_options"] = {"include_usage": True}
    return body


def deepseek_headers(*, stream: bool) -> dict[str, str]:
    return {
        "content-type": "application/json",
        "accept": "text/event-stream" if stream else "application/json",
        "authorization": f"Bearer {os.environ['DEEPSEEK_API_KEY']}",
    }


def openai_headers(*, stream: bool) -> dict[str, str]:
    return {
        "content-type": "application/json",
        "accept": "text/event-stream" if stream else "application/json",
        "authorization": f"Bearer {os.environ['OPENAI_API_KEY']}",
    }


# --------------------------------------------------------------------------
# One measured streamed request (works for both arms)
# --------------------------------------------------------------------------


@dataclass
class Shot:
    status: int
    ttft_ms: float | None
    total_ms: float
    usage: Usage
    gw: dict[str, str]
    error: str = ""


async def timed_stream(client: httpx.AsyncClient, url: str,
                       headers: dict[str, str], body: dict) -> Shot:
    parser = SSEParser(max_frame_bytes=1 << 20)
    usage = Usage()
    t0 = time.perf_counter()
    first: float | None = None
    async with client.stream("POST", url, headers=headers, json=body) as r:
        gw = {k.lower(): v for k, v in r.headers.items()
              if k.lower().startswith("x-gw-")}
        if r.status_code != 200:
            raw = await r.aread()
            # Some providers echo the last 4 chars of a bad key in a 401 body;
            # we keep only a short, generic tag and never the body verbatim.
            return Shot(status=r.status_code, ttft_ms=None,
                        total_ms=(time.perf_counter() - t0) * 1000,
                        usage=usage, gw=gw,
                        error=f"HTTP {r.status_code} ({len(raw)}B body, redacted)")
        async for chunk in r.aiter_raw():
            if chunk and first is None:
                first = time.perf_counter()
            for ev in parser.feed(chunk):
                OPENAI_CHAT.apply_usage(ev, usage)
        for ev in parser.close():
            OPENAI_CHAT.apply_usage(ev, usage)
    total = (time.perf_counter() - t0) * 1000
    return Shot(status=200, ttft_ms=(first - t0) * 1000 if first else None,
                total_ms=total, usage=usage, gw=gw)


# --------------------------------------------------------------------------
# Catalog + policy for the live gateway
# --------------------------------------------------------------------------


def build_live_catalog(relay_url: str | None) -> Catalog:
    providers: dict[str, ProviderConn] = {
        GHOST_PROVIDER: ProviderConn(
            id="openai", kind="openai", base_url="https://api.openai.com/v1",
            api_key_env="OPENAI_API_KEY", max_concurrency=32,
        ),
    }
    models: dict[str, ModelSpec] = {
        GHOST_MODEL_ID: ModelSpec(
            id=GHOST_MODEL_ID, provider="openai", api_model=GHOST_API_MODEL,
            input_per_m=0.15, output_per_m=0.60, priced_at="2026-09-10",
        ),
    }
    if relay_url:
        providers[RELAY_PROVIDER] = ProviderConn(
            id=RELAY_PROVIDER, kind="openai", base_url=relay_url,
            api_key_env="DEEPSEEK_API_KEY", max_concurrency=32,
            credential_id=RELAY_PROVIDER,
        )
        base_pro = DEFAULT_CATALOG.models[SUCCESS_MODEL_ID]
        models[RELAY_MODEL_ID] = replace(
            base_pro, id=RELAY_MODEL_ID, provider=RELAY_PROVIDER)
    return DEFAULT_CATALOG.with_overrides(providers=providers, models=models)


def policy_toml(*, with_relay: bool) -> str:
    s = f"""
default_workload = "clean"

[defaults.budgets]
total = 120.0
connect = 5.0
first_event = 60.0
progress = 40.0
client_stall = 40.0

[defaults.retry]
max_attempts = 1
base_delay = 0.25
max_delay = 2.0
respect_retry_after = true

[workloads.clean]
incumbent = "{SUCCESS_MODEL_ID}"

# Failing-primary (OpenAI, invalid model id -> 404, free) then real secondary.
[workloads.switch]
candidate = "{GHOST_MODEL_ID}"
incumbent = "{SUCCESS_MODEL_ID}"
"""
    if with_relay:
        s += f"""
# Single target; a 503 can only be retried on the SAME target. Tiny backoff so
# the inherent delay is small and bounded -- we are measuring machinery, not
# the (inherent) backoff a real deployment would set larger.
[workloads.retrysame]
incumbent = "{RELAY_MODEL_ID}"

[workloads.retrysame.retry]
max_attempts = 2
base_delay = 0.01
max_delay = 0.02
respect_retry_after = true
"""
    return s


# --------------------------------------------------------------------------
# Serving a uvicorn app on its own thread, lifespan ON (mirror bench/overhead)
# --------------------------------------------------------------------------


@dataclass
class Served:
    server: uvicorn.Server
    thread: threading.Thread
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self, timeout: float = 8.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout)
        if self.thread.is_alive():
            self.server.force_exit = True
            self.thread.join(timeout)


def serve(app: Starlette, *, name: str, startup_timeout: float = 15.0) -> Served:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    port = sock.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="error", access_log=False, lifespan="on"))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]},
                              daemon=True, name=name)
    thread.start()
    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:
            raise RuntimeError(f"{name} failed to start")
        time.sleep(0.005)
    return Served(server=server, thread=thread, port=port)


# --------------------------------------------------------------------------
# The flaky relay: one controlled 503, then reverse-proxy to real DeepSeek.
#
# The episodic toggle (fail, proxy, fail, proxy, ...) drives the GATEWAY path:
# run sequentially, each gateway request is exactly one 503 + one proxied
# success. An `X-Bench-Force: fail|proxy` header (which the gateway does NOT
# forward -- it is not on the allowlist) lets the direct-arm calibration hit a
# 503 or a real success WITHOUT advancing the toggle.
# --------------------------------------------------------------------------


def build_relay() -> Starlette:
    state = {"n": 0}

    async def proxy(request) -> StreamingResponse:
        body = await request.body()
        fwd = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "content-length", "accept-encoding")}
        client = httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0))
        req = client.build_request("POST", DEEPSEEK_CHAT_URL, headers=fwd, content=body)
        resp = await client.send(req, stream=True)

        async def gen():
            try:
                async for chunk in resp.aiter_raw():
                    yield chunk
            finally:
                await resp.aclose()
                await client.aclose()

        return StreamingResponse(
            gen(), status_code=resp.status_code,
            media_type=resp.headers.get("content-type", "text/event-stream"))

    FAIL_503 = b'{"error":{"message":"controlled transient overload (bench relay)"}}'

    async def handler(request):
        force = request.headers.get("x-bench-force")
        if force == "fail":
            return Response(FAIL_503, status_code=503, media_type="application/json")
        if force == "proxy":
            return await proxy(request)
        state["n"] += 1
        if state["n"] % 2 == 1:
            return Response(FAIL_503, status_code=503, media_type="application/json")
        return await proxy(request)

    return Starlette(routes=[Route(OPENAI_PATH, handler, methods=["POST"])])


# --------------------------------------------------------------------------
# Percentiles / distribution helpers
# --------------------------------------------------------------------------


def pct(sorted_vals: list[float], q: float) -> float:
    n = len(sorted_vals)
    if n == 0:
        return float("nan")
    if q <= 0:
        return sorted_vals[0]
    if q >= 100:
        return sorted_vals[-1]
    rank = (q / 100.0) * (n - 1)
    lo = int(rank)
    frac = rank - lo
    if lo + 1 >= n:
        return sorted_vals[-1]
    return sorted_vals[lo] + frac * (sorted_vals[lo + 1] - sorted_vals[lo])


def summary(vals: list[float]) -> dict[str, float]:
    s = sorted(v for v in vals if v == v)  # drop NaN
    if not s:
        return {"n": 0, "median": float("nan"), "p90": float("nan"),
                "mean": float("nan")}
    return {"n": len(s), "median": pct(s, 50), "p90": pct(s, 90),
            "mean": statistics.fmean(s)}


def f(x: float) -> str:
    return "nan" if x != x else f"{x:.2f}"


# --------------------------------------------------------------------------
# Condition results
# --------------------------------------------------------------------------


@dataclass
class PairedResult:
    label: str
    d_first: list[float] = field(default_factory=list)
    d_total: list[float] = field(default_factory=list)
    g_first: list[float] = field(default_factory=list)
    g_total: list[float] = field(default_factory=list)
    delta_first: list[float] = field(default_factory=list)
    delta_total: list[float] = field(default_factory=list)
    checks: dict[str, object] = field(default_factory=dict)
    # switch-only: inherent fail+succeed decomposition
    d_primary_fail: list[float] = field(default_factory=list)
    gw_attributable: list[float] = field(default_factory=list)


# --------------------------------------------------------------------------
# Condition 1: clean
# --------------------------------------------------------------------------


async def run_clean(gw_base: str, spend: Spend, *, n: int, warmup: int) -> PairedResult:
    res = PairedResult(label="clean")
    d_url = DEEPSEEK_CHAT_URL
    d_hdr = deepseek_headers(stream=True)
    d_body = make_body(stream=True, model=DEEPSEEK_API_MODEL)
    g_url = f"{gw_base}/workloads/clean{OPENAI_PATH}"
    g_body = make_body(stream=True)
    model = DEFAULT_CATALOG.models[SUCCESS_MODEL_ID]
    async with httpx.AsyncClient(http2=True, timeout=120.0) as dc, \
               httpx.AsyncClient(http2=True, timeout=120.0) as gc:
        for _ in range(warmup):
            dw = await timed_stream(dc, d_url, d_hdr, d_body)
            g = await timed_stream(gc, g_url, {}, g_body)
            spend.add(dw.usage, model)  # warmup is discarded for timing, not for $
            spend.add(g.usage, model)
        # assertions on one pair
        d0 = await timed_stream(dc, d_url, d_hdr, d_body)
        g0 = await timed_stream(gc, g_url, {}, g_body)
        spend.add(d0.usage, model)
        spend.add(g0.usage, model)
        assert not d0.gw, f"Arm D carries X-Gw-* (not direct!): {d0.gw}"
        assert "x-gw-attempts" in g0.gw, f"Arm G missing X-Gw-Attempts: {g0.gw}"
        assert int(g0.gw.get("x-gw-attempts", "0") or "0") >= 1, g0.gw
        assert SUCCESS_MODEL_ID in g0.gw.get("x-gw-served-by", ""), g0.gw
        assert d0.status == 200 and g0.status == 200, (d0.status, g0.status)
        res.checks = {
            "arm_d_has_x_gw": bool(d0.gw),
            "arm_g_attempts": g0.gw.get("x-gw-attempts"),
            "arm_g_served_by": g0.gw.get("x-gw-served-by"),
        }
        for i in range(n):
            if i % 2 == 0:
                d = await timed_stream(dc, d_url, d_hdr, d_body)
                g = await timed_stream(gc, g_url, {}, g_body)
            else:
                g = await timed_stream(gc, g_url, {}, g_body)
                d = await timed_stream(dc, d_url, d_hdr, d_body)
            if d.status != 200 or g.status != 200:
                continue
            spend.add(d.usage, model)
            spend.add(g.usage, model)
            res.d_first.append(d.ttft_ms)
            res.d_total.append(d.total_ms)
            res.g_first.append(g.ttft_ms)
            res.g_total.append(g.total_ms)
            res.delta_first.append(g.ttft_ms - d.ttft_ms)
            res.delta_total.append(g.total_ms - d.total_ms)
    return res


# --------------------------------------------------------------------------
# Condition 2: switch / fallback
# --------------------------------------------------------------------------


async def run_switch(gw_base: str, spend: Spend, *, n: int, warmup: int) -> PairedResult:
    res = PairedResult(label="switch")
    prim_url = OPENAI_CHAT_URL
    prim_hdr = openai_headers(stream=True)
    prim_body = make_body(stream=True, model=GHOST_API_MODEL)
    sec_url = DEEPSEEK_CHAT_URL
    sec_hdr = deepseek_headers(stream=True)
    sec_body = make_body(stream=True, model=DEEPSEEK_API_MODEL)
    g_url = f"{gw_base}/workloads/switch{OPENAI_PATH}"
    g_body = make_body(stream=True)
    model = DEFAULT_CATALOG.models[SUCCESS_MODEL_ID]
    async with httpx.AsyncClient(http2=True, timeout=120.0) as dc, \
               httpx.AsyncClient(http2=True, timeout=120.0) as gc:
        for _ in range(warmup):
            await timed_stream(dc, prim_url, prim_hdr, prim_body)
            sw = await timed_stream(dc, sec_url, sec_hdr, sec_body)
            g = await timed_stream(gc, g_url, {}, g_body)
            spend.add(sw.usage, model)
            spend.add(g.usage, model)
        g0 = await timed_stream(gc, g_url, {}, g_body)
        spend.add(g0.usage, model)
        attempts = int(g0.gw.get("x-gw-attempts", "0") or "0")
        assert attempts >= 2, f"switch did not happen, X-Gw-Attempts={attempts}: {g0.gw}"
        assert SUCCESS_MODEL_ID in g0.gw.get("x-gw-served-by", ""), g0.gw
        assert g0.status == 200, g0.status
        # confirm the primary really fails free (no tokens) and with try_next status
        p0 = await timed_stream(dc, prim_url, prim_hdr, prim_body)
        assert p0.status in (400, 404), f"primary did not fail as expected: {p0.status}"
        res.checks = {
            "arm_g_attempts": g0.gw.get("x-gw-attempts"),
            "arm_g_served_by": g0.gw.get("x-gw-served-by"),
            "primary_fail_status": p0.status,
        }
        for _ in range(n):
            # G: full switch through the gateway
            g = await timed_stream(gc, g_url, {}, g_body)
            # D: the inherent "try A (fail, free), then try B (success)"
            pf = await timed_stream(dc, prim_url, prim_hdr, prim_body)
            sec = await timed_stream(dc, sec_url, sec_hdr, sec_body)
            if g.status != 200 or sec.status != 200 or pf.status not in (400, 404):
                continue
            spend.add(g.usage, model)
            spend.add(sec.usage, model)
            inherent = pf.total_ms + sec.total_ms
            res.g_total.append(g.total_ms)
            res.g_first.append(g.ttft_ms)
            res.d_total.append(inherent)
            res.d_primary_fail.append(pf.total_ms)
            res.delta_total.append(g.total_ms - inherent)
            res.gw_attributable.append(g.total_ms - inherent)
    return res


# --------------------------------------------------------------------------
# Condition 3: retry-same (controlled fault -> real provider success)
# --------------------------------------------------------------------------


async def run_retry(gw_base: str, relay_base: str, spend: Spend, *,
                    n: int, warmup: int) -> PairedResult:
    res = PairedResult(label="retry")
    g_url = f"{gw_base}/workloads/retrysame{OPENAI_PATH}"
    g_body = make_body(stream=True)
    relay_url = f"{relay_base}{OPENAI_PATH}"
    model = DEFAULT_CATALOG.models[SUCCESS_MODEL_ID]
    fail_hdr = {**deepseek_headers(stream=True), "x-bench-force": "fail"}
    proxy_hdr = {**deepseek_headers(stream=True), "x-bench-force": "proxy"}
    succ_body = make_body(stream=True, model=DEEPSEEK_API_MODEL)
    async with httpx.AsyncClient(http2=True, timeout=120.0) as dc, \
               httpx.AsyncClient(http2=True, timeout=120.0) as gc:
        for _ in range(warmup):
            g = await timed_stream(gc, g_url, {}, g_body)
            spend.add(g.usage, model)
        g0 = await timed_stream(gc, g_url, {}, g_body)
        spend.add(g0.usage, model)
        attempts = int(g0.gw.get("x-gw-attempts", "0") or "0")
        assert attempts == 2, f"retry-same did not happen, X-Gw-Attempts={attempts}: {g0.gw}"  # noqa: E501
        assert RELAY_MODEL_ID in g0.gw.get("x-gw-served-by", ""), g0.gw
        assert g0.status == 200, g0.status
        res.checks = {
            "arm_g_attempts": g0.gw.get("x-gw-attempts"),
            "arm_g_served_by": g0.gw.get("x-gw-served-by"),
        }
        for _ in range(n):
            g = await timed_stream(gc, g_url, {}, g_body)          # 503 then proxied success
            # controlled fail (free), then real success via the relay
            pf = await timed_stream(dc, relay_url, fail_hdr, succ_body)
            sc = await timed_stream(dc, relay_url, proxy_hdr, succ_body)
            if g.status != 200 or sc.status != 200 or pf.status != 503:
                continue
            spend.add(g.usage, model)
            spend.add(sc.usage, model)
            inherent = pf.total_ms + sc.total_ms
            res.g_total.append(g.total_ms)
            res.g_first.append(g.ttft_ms)
            res.d_total.append(inherent)
            res.d_primary_fail.append(pf.total_ms)
            res.gw_attributable.append(g.total_ms - inherent)
    return res


def microbench_retry_machinery(iters: int = 200_000) -> dict[str, float]:
    """The pure CPU cost of the retry decision + backoff scheduling, in-process,
    no network, no money. This is the 'machinery' the report separates from the
    (inherent) backoff sleep and the (inherent) failed-attempt latency."""
    err = UpstreamOverloaded("controlled", provider="p", model="m")
    clock = SystemClock()
    pol = RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=0.02)

    t0 = time.perf_counter()
    for _ in range(iters):
        decide(err, committed=False)
    decide_ns = (time.perf_counter() - t0) / iters * 1e9

    dl = Deadline(clock, 120.0)
    bud = RetryBudget(pol, dl, clock=clock)
    bud.record_attempt()
    t0 = time.perf_counter()
    for _ in range(iters):
        bud.delay_for(err, attempt=0)
    delay_ns = (time.perf_counter() - t0) / iters * 1e9
    return {"decide_ns": decide_ns, "delay_for_ns": delay_ns,
            "machinery_us": (decide_ns + delay_ns) / 1000.0}


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------


def load_snapshot() -> tuple[float, float, float]:
    try:
        return os.getloadavg()
    except OSError:
        return (float("nan"),) * 3


def paired_tables(res: PairedResult) -> str:
    df, dt = summary(res.d_first), summary(res.d_total)
    gf, gt = summary(res.g_first), summary(res.g_total)
    pf, ptt = summary(res.delta_first), summary(res.delta_total)
    lines = []
    lines.append("| metric | D median | D p90 | G median | G p90 "
                 "| **Δ median (G−D)** | **Δ p90** |")
    lines.append("|--------|----------|-------|----------|-------|--------------------|-----------|")
    if res.delta_first:
        lines.append(
            f"| first-event | {f(df['median'])} | {f(df['p90'])} | {f(gf['median'])} | "
            f"{f(gf['p90'])} | **{f(pf['median'])}** | **{f(pf['p90'])}** |")
    lines.append(
        f"| total | {f(dt['median'])} | {f(dt['p90'])} | {f(gt['median'])} | "
        f"{f(gt['p90'])} | **{f(ptt['median'])}** | **{f(ptt['p90'])}** |")
    return "\n".join(lines)


def build_report(results: dict, micro: dict, spend: Spend, meta: dict) -> str:
    p: list[str] = []
    p.append("# Live gateway-overhead benchmark: llmgw vs REAL providers")
    p.append("")
    p.append("Sibling of `bench/overhead.py` (which uses an in-process fake for a "
             "pure-CPU delta). This one builds the SAME app with "
             "`fake_upstreams=False`, `DEFAULT_CATALOG` + real credentials, and "
             "measures the overhead a deployment actually pays: "
             "client → gateway → real provider over real TLS. The honest signal "
             "is the **paired, interleaved per-iteration delta G−D**: D and G run "
             "back-to-back with the identical prompt (alternating order), so both "
             "share one provider-latency window and the provider's fat tails "
             "cancel. Absolute D and G are shown for context only.")
    p.append("")
    p.append("> Every credential is read at runtime from a file outside the repo via "
             "`live.env`; none is printed, logged, or written here. Provider error "
             "bodies are NOT captured verbatim (some 401s echo the last 4 key "
             "chars); only a generic `HTTP nnn (N B body, redacted)` tag is kept.")
    p.append("")
    p.append("## Setup")
    p.append("")
    p.append(f"- **Success model (clean + secondary):** `{SUCCESS_MODEL_ID}` "
             f"→ DeepSeek `{DEEPSEEK_API_MODEL}`, first-party, input "
             f"{DEFAULT_CATALOG.models[SUCCESS_MODEL_ID].input_per_m}/M out "
             f"{DEFAULT_CATALOG.models[SUCCESS_MODEL_ID].output_per_m}/M. Chosen "
             f"because `make probe` confirms it is reachable today, it is the "
             f"fastest + cheapest working first-party target (OpenRouter "
             f"generation returns 402 no-credit; DeepSeek's `deepseek-v4-flash` "
             f"id is now MISSING at the provider).")
    p.append(f"- **Failing primary (switch):** real OpenAI, invalid model id "
             f"`{GHOST_API_MODEL}` → 4xx, bills no tokens, classified try_next.")
    p.append(f"- **Prompt:** a ~{PROMPT_BYTES/1024:.1f} KB real document + a "
             f"summarise instruction (≈{PROMPT_BYTES//4} tokens est.), "
             f"`max_tokens={MAX_TOKENS}`, streamed with `stream_options."
             f"include_usage`.")
    p.append("- **Gateway:** `build_app(ServerConfig(fake_upstreams=False, …))`, "
             "lifespan ON, ephemeral port. Tenant limiter lifted "
             "(rate 1e9/burst 1e9/conc 1e6) and breaker threshold 1e6 so "
             "neither silently rejects — the fake bench's first run secretly "
             "measured rate-limited rejections. X-Gw-Attempts is asserted on "
             "every condition to prove real upstream work happened.")
    p.append(f"- **Machine:** {meta['machine']}, {meta['cores']} cores, "
             f"Python {meta['python']}. Run at {meta['timestamp']}.")
    p.append(f"- **Load avg (1/5/15):** before {meta['load_before']}, "
             f"after {meta['load_after']}.")
    loaded = meta["load_before"][0] > 1.5 or meta["load_after"][0] > 1.5
    if loaded:
        p.append("")
        p.append("> ⚠️ The machine was under non-trivial load during this run; a "
                 "latency benchmark on a loaded machine OVER-reports overhead. "
                 "Treat absolute numbers as provisional. (The paired delta is far "
                 "more robust to this than the absolutes.)")
    p.append("")
    p.append(f"- **N per condition:** clean={meta['n_clean']}, "
             f"switch={meta['n_switch']}, retry={meta['n_retry']} "
             f"(warmup discarded: {meta['warmup']}).")
    p.append("")

    # Clean
    clean = results["clean"]
    p.append("## 1. Clean call — one real working target")
    p.append("")
    p.append(f"Verification: Arm G X-Gw-Attempts=`{clean.checks.get('arm_g_attempts')}`, "
             f"X-Gw-Served-By=`{clean.checks.get('arm_g_served_by')}`; "
             f"Arm D carries X-Gw-*: `{clean.checks.get('arm_d_has_x_gw')}`.")
    p.append("")
    p.append("All values in ms. Δ is the headline (paired, interleaved).")
    p.append("")
    p.append(paired_tables(clean))
    p.append("")
    dfm = summary(clean.delta_first)
    dtm = summary(clean.delta_total)
    p.append(f"**Headline:** added first-event latency **{f(dfm['median'])} ms "
             f"median** ({f(dfm['p90'])} p90); added total **{f(dtm['median'])} ms "
             f"median** ({f(dtm['p90'])} p90). This is gateway machinery + SSE "
             f"parse/re-emit + one extra localhost TCP hop, on top of the real "
             f"provider call.")
    p.append("")

    # Switch
    sw = results["switch"]
    p.append("## 2. Switch / fallback — [failing-primary, real-secondary], both real")
    p.append("")
    p.append(f"Plan: candidate `{GHOST_MODEL_ID}` (OpenAI, invalid id, fails free) "
             f"→ incumbent `{SUCCESS_MODEL_ID}` (DeepSeek, succeeds). "
             f"Verification: X-Gw-Attempts=`{sw.checks.get('arm_g_attempts')}` (≥2), "
             f"X-Gw-Served-By=`{sw.checks.get('arm_g_served_by')}`, "
             f"primary-direct status=`{sw.checks.get('primary_fail_status')}` "
             f"(no tokens billed).")
    p.append("")
    p.append("Here **D** is the *inherent* cost of the fallback itself — "
             "primary-fail-direct + secondary-success-direct, measured directly — "
             "and **G** is the whole switch through the gateway. So Δ = G − D is "
             "the **gateway-attributable switch overhead**: everything the "
             "gateway's switch machinery adds beyond 'try A, fail, try B'.")
    p.append("")
    gt = summary(sw.g_total)
    it = summary(sw.d_total)
    pfd = summary(sw.d_primary_fail)
    ga = summary(sw.gw_attributable)
    p.append("| quantity | median (ms) | p90 (ms) |")
    p.append("|----------|-------------|----------|")
    p.append(f"| primary-fail direct (OpenAI 4xx) | {f(pfd['median'])} | {f(pfd['p90'])} |")
    p.append(f"| inherent fail+succeed (D) | {f(it['median'])} | {f(it['p90'])} |")
    p.append(f"| total switch through gateway (G) | {f(gt['median'])} | {f(gt['p90'])} |")
    p.append(f"| **gateway-attributable overhead (G−D)** "
             f"| **{f(ga['median'])}** | **{f(ga['p90'])}** |")
    p.append("")

    # Retry
    p.append("## 3. Retry (retry-same)")
    p.append("")
    p.append("**Honesty first:** a retry-same that then SUCCEEDS cannot be "
             "triggered deterministically against a live provider — you cannot "
             "make a healthy provider emit a transient 5xx (or a connect failure) "
             "on attempt 1 and a 200 on attempt 2 of the *same* target on demand. "
             "So this condition uses a **local flaky relay**: it returns one "
             "controlled `503` (→ `UpstreamOverloaded`, `retry_same=True`) and "
             "then **reverse-proxies the retry to real DeepSeek** — the eventual "
             "success is genuine DeepSeek tokens and real spend. The controlled "
             "503 stands in for the transient fault; everything after it is real.")
    p.append("")
    p.append("A retry's wall-clock is dominated by **(failed-attempt latency + "
             "backoff/Retry-After delay)**, both of which are *inherent to "
             "retrying* and NOT gateway overhead. The gateway's own added cost is "
             "only `decide()` + backoff-scheduling + re-dispatch. We separate the "
             "two:")
    p.append("")
    p.append(f"- **Machinery (measured in-process, no network):** `decide()` = "
             f"{micro['decide_ns']:.0f} ns, `RetryBudget.delay_for()` = "
             f"{micro['delay_for_ns']:.0f} ns → **{micro['machinery_us']:.2f} µs** "
             f"total per retry decision. This is the gateway's real added cost; "
             f"it is sub-millisecond and dwarfed by anything on the wire.")
    if "retry" in results and results["retry"].g_total:
        rt = results["retry"]
        gtr = summary(rt.g_total)
        pfr = summary(rt.d_primary_fail)
        inr = summary(rt.d_total)
        gar = summary(rt.gw_attributable)
        p.append("- **End-to-end (controlled 503 + real DeepSeek success), "
                 "backoff capped at base_delay=10 ms:**")
        p.append("")
        p.append("| quantity | median (ms) | p90 (ms) |")
        p.append("|----------|-------------|----------|")
        p.append(f"| controlled-fault attempt (503, free) "
                 f"| {f(pfr['median'])} | {f(pfr['p90'])} |")
        p.append(f"| inherent fail + success (D, via relay) "
                 f"| {f(inr['median'])} | {f(inr['p90'])} |")
        p.append(f"| total retry-loop through gateway (G) "
                 f"| {f(gtr['median'])} | {f(gtr['p90'])} |")
        p.append(f"| G − (fail + success) ≈ backoff + machinery "
                 f"| {f(gar['median'])} | {f(gar['p90'])} |")
        p.append("")
        p.append("The `G − (fail+success)` residual is mostly the bounded backoff "
                 "sleep (uniform 0–10 ms here); the sub-ms machinery above sits "
                 "inside it. Both the failed attempt and the backoff are inherent "
                 "to the act of retrying — a real deployment would set a larger "
                 "backoff and that delay is still not gateway overhead.")
    else:
        p.append("- End-to-end relay measurement was skipped or failed; see the "
                 "in-process machinery number above, which is the gateway's actual "
                 "added cost.")
    p.append("")

    # Spend
    p.append("## Spend")
    p.append("")
    p.append(f"Estimated from the same `catalog.price_of` the gateway bills with: "
             f"**${spend.usd:.5f}** over **{spend.completions}** successful "
             f"completions ({spend.input_tokens} input + {spend.output_tokens} "
             f"output tokens billed). Cap was ${SPEND_ABORT_USD:.2f} (not hit). "
             f"Failed primaries (4xx) and controlled 503s bill nothing.")
    p.append("")
    p.append("## Caveats")
    p.append("")
    p.append("- Absolute latencies are provider- and network-bound (residential "
             "link, shared laptop); the paired delta is the trustworthy number.")
    p.append("- `deepseek-v4-pro` is a reasoning-capable model; `max_tokens` "
             "bounds every call so spend is comparable across iterations.")
    p.append("- The switch's two arms use two different real providers (OpenAI "
             "fail + DeepSeek success); the retry relay adds one extra localhost "
             "hop on G vs the direct relay arm, which the D decomposition "
             "controls for.")
    p.append("")
    p.append("## Re-run (gated — spends real money)")
    p.append("")
    p.append("```")
    p.append("LLMGW_LIVE_OVERHEAD=1 .venv/bin/python -m bench.live_overhead")
    p.append("# free self-check (2 real calls, no report): add --calibrate instead")
    p.append("```")
    p.append("")
    p.append("Source: `bench/live_overhead.py`, document `bench/_document.py`.")
    return "\n".join(p)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------


async def amain(args: argparse.Namespace) -> int:
    env.ensure_loaded()
    env.require("DEEPSEEK_API_KEY", "OPENAI_API_KEY")

    spend = Spend()

    if args.calibrate:
        # 2 real calls + the free in-process machinery bench; no gateway boot.
        async with httpx.AsyncClient(http2=True, timeout=120.0) as c:
            d = await timed_stream(c, DEEPSEEK_CHAT_URL, deepseek_headers(stream=True),
                                   make_body(stream=True, model=DEEPSEEK_API_MODEL))
            model = DEFAULT_CATALOG.models[SUCCESS_MODEL_ID]
            spend.add(d.usage, model)
            print(f"DeepSeek direct: status={d.status} ttft={f(d.ttft_ms or float('nan'))}ms "
                  f"total={f(d.total_ms)}ms in={d.usage.input_tokens} "
                  f"out={d.usage.output_tokens} usd={usd_of(d.usage, model):.6f}")
            pf = await timed_stream(c, OPENAI_CHAT_URL, openai_headers(stream=True),
                                    make_body(stream=True, model=GHOST_API_MODEL))
            print(f"OpenAI ghost (expect 4xx): status={pf.status} total={f(pf.total_ms)}ms "
                  f"{pf.error}")
        micro = microbench_retry_machinery(iters=50_000)
        print(f"retry machinery: decide={micro['decide_ns']:.0f}ns "
              f"delay_for={micro['delay_for_ns']:.0f}ns -> {micro['machinery_us']:.2f}us")
        print(f"running spend estimate: ${spend.usd:.6f}")
        print(f"prompt: {PROMPT_BYTES} bytes (~{PROMPT_BYTES//4} tokens est)")
        return 0

    load_before = load_snapshot()
    relay = serve(build_relay(), name="llmgw-bench-relay")
    catalog = build_live_catalog(relay.base_url)
    cfg = ServerConfig(
        catalog=catalog,
        fake_upstreams=False,
        policy_file=None,  # set below via a temp file
        tenant_limits=UNLIMITED_TENANT,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(total=120.0, connect=5.0, first_event=60.0,
                        progress=40.0, client_stall=40.0),
    )
    import tempfile
    from pathlib import Path
    results: dict = {}
    with tempfile.TemporaryDirectory(prefix="llmgw-liveoh-") as tmp:
        pol = Path(tmp) / "policy.toml"
        pol.write_text(policy_toml(with_relay=True), encoding="utf-8")
        cfg = replace(cfg, policy_file=str(pol))
        gw = serve(build_app(cfg.validated()), name="llmgw-bench-gw")
        try:
            results["clean"] = await run_clean(
                gw.base_url, spend, n=args.n, warmup=args.warmup)
            results["switch"] = await run_switch(
                gw.base_url, spend, n=args.n_switch, warmup=args.warmup)
            try:
                results["retry"] = await run_retry(
                    gw.base_url, relay.base_url, spend,
                    n=args.n_retry, warmup=args.warmup)
            except Exception as exc:  # noqa: BLE001 - relay is best-effort
                print(f"retry end-to-end condition failed (kept machinery bench): {exc!r}",
                      file=sys.stderr)
        finally:
            gw.stop()
            relay.stop()
    load_after = load_snapshot()
    micro = microbench_retry_machinery()

    meta = {
        "python": platform.python_version(),
        "machine": platform.machine(),
        "cores": os.cpu_count(),
        "timestamp": datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%SZ"),
        "load_before": tuple(round(x, 2) for x in load_before),
        "load_after": tuple(round(x, 2) for x in load_after),
        "n_clean": len(results["clean"].delta_total),
        "n_switch": len(results["switch"].gw_attributable),
        "n_retry": len(results.get("retry", PairedResult("retry")).gw_attributable),
        "warmup": args.warmup,
    }
    report = build_report(results, micro, spend, meta)
    out_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "live_overhead.md")
    # Blocking write on purpose: every measurement is finished by now, so the
    # one report file costs nothing to write inline.
    with open(out_path, "w", encoding="utf-8") as fh:  # noqa: ASYNC230
        fh.write(report + "\n")
    print(f"wrote {out_path}")
    print(f"\nTOTAL ESTIMATED SPEND: ${spend.usd:.5f} over {spend.completions} completions")
    print(report)
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m bench.live_overhead")
    ap.add_argument("--calibrate", action="store_true",
                    help="2 real calls + free machinery bench, no gateway, no report")
    ap.add_argument("--n", type=int, default=40, help="paired iters, clean")
    ap.add_argument("--n-switch", type=int, default=35, help="paired iters, switch")
    ap.add_argument("--n-retry", type=int, default=12, help="paired iters, retry")
    ap.add_argument("--warmup", type=int, default=4, help="discarded warmup pairs")
    args = ap.parse_args(argv)
    if not args.calibrate and os.environ.get("LLMGW_LIVE_OVERHEAD") != "1":
        print("This spends REAL MONEY. Re-run with LLMGW_LIVE_OVERHEAD=1 "
              "(or --calibrate for the free 2-call self-check).", file=sys.stderr)
        return 2
    return asyncio.run(amain(args))


if __name__ == "__main__":
    raise SystemExit(main())

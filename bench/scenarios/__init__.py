"""The S1-S8 scale scenarios, as config the harness runs.

Each scenario is a frozen `Scenario` describing the offered load (arrival rate,
stream shape via the fake's `X-Fake-*` knobs), which gateway configuration it
needs (limiter/breaker lifted, or a real breaker + a tenants file), the fake
mode-flip or deploy schedule where it has one, and -- named explicitly -- the
the numbers it is there to produce.

The stream shape is carried on `X-Fake-*` headers. For Arm D (direct) those
headers hit the fake straight. For Arm G they are forwarded by the gateway
(`bench/_gwproc.py` adds x-fake-* to `forward_request_headers`), so BOTH arms
measure the identical stream shape rather than the gateway arm silently falling
back to the fake's default mode.

Rates are the target rates. `smoke_rate` is the tiny rate `--smoke` clamps to
for self-validation on a busy laptop; the real campaign uses `rate`.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass, field

# Mirrors bench/overhead.py: the catalog id that routes to the openai fake, and
# the provider path. The fake echoes regardless of model; the gateway routes on
# this id via ServerConfig.default_model ("fake.echo").
GATEWAY_MODEL = "fake.echo"
OPENAI_PATH = "/v1/chat/completions"


@dataclass(frozen=True)
class Scenario:
    sid: str
    name: str
    question: str
    produces: str                 # the numbers this scenario yields
    rate: float                   # target arrivals/sec (campaign)
    smoke_rate: float             # clamp for --smoke
    streaming: bool
    # X-Fake-* stream shape
    mode: str = "ok"
    events: int | None = None     # content events; None -> fake default (5)
    interval: float | None = None # seconds between events; None -> fake default
    # gateway config
    unlimited: bool = True        # True: lift limiter+breaker (throughput)
    # S5 backpressure: throttle the client's READ to this many bytes/sec
    read_bps: int | None = None
    # S7 provider-kill: [(start_s, end_s, mode)] relative to measure start
    flips: list = field(default_factory=list)
    # S6 multi-tenant isolation (see driver); S8 deploy-under-load
    tenant_groups: list = field(default_factory=list)  # [(label, token, rate)]
    deploy_sigterm_at: float | None = None             # seconds into measure
    driver: str = "generic"       # "generic" | "s6_isolation" | "s8_deploy"
    expect_upstream_work: bool = True  # False when the breaker is MEANT to shed
    path: str = OPENAI_PATH
    timeout_s: float = 60.0

    # ---- request building ----
    def body(self) -> dict:
        return {
            "model": GATEWAY_MODEL,
            "stream": self.streaming,
            "max_tokens": 64,
            "messages": [{"role": "user", "content": "hi"}],
        }

    def _fake_headers(self) -> dict[str, str]:
        h = {"X-Fake-Mode": self.mode}
        if self.events is not None:
            h["X-Fake-Events"] = str(self.events)
        if self.interval is not None:
            h["X-Fake-Interval"] = str(self.interval)
        return h

    def direct_headers(self) -> dict[str, str]:
        # Arm D goes straight to the fake; auth is irrelevant there.
        return self._fake_headers()

    def gateway_headers(self) -> dict[str, str]:
        # Arm G: stream-shape headers are forwarded by the gateway; a tenant
        # token is added by the driver for the tenant scenarios.
        return self._fake_headers()

    # ---- files a scenario needs on disk before the gateway boots ----
    def prepare(self) -> str | None:
        """Write any tenants file and return its path (or None)."""
        if not self.tenant_groups:
            return None
        lines = []
        for (label, token, _rate) in self.tenant_groups:
            # Each tenant's configured cap is `rate`; the driver offers A at 5x.
            # Concurrency is left generous so this scenario isolates the RATE
            # bucket, which is what S6 is asking about.
            lines.append(f"[tenants.{label}]\n"
                         f'tokens = ["{token}"]\n'
                         f"rate_per_second = {_rate}\n"
                         f"burst = {int(_rate) * 2 + 2}\n"
                         f"max_concurrency = 100000\n")
        fd, path = tempfile.mkstemp(prefix=f"bench-tenants-{self.sid}-", suffix=".toml",
                                    dir=tempfile.gettempdir())
        with os.fdopen(fd, "w") as f:
            f.write("\n".join(lines))
        return path


# --------------------------------------------------------------------------
# The registry
# --------------------------------------------------------------------------

_S: dict[str, Scenario] = {}


def _reg(s: Scenario) -> None:
    _S[s.sid] = s


_reg(Scenario(
    sid="S1", name="Short non-streaming",
    question="Per-request overhead floor at high request rate.",
    produces="added p50/p99 first-event latency (paired); GIL pressure on short "
             "CPU-bound requests; throughput at which p99 doubles.",
    rate=400.0, smoke_rate=40.0, streaming=False,
    mode="ok", events=1,
))

_reg(Scenario(
    sid="S2", name="Typical streaming",
    question="Overhead at ~2,500 concurrent streams (100 rps x 25 s).",
    produces="added inter-event jitter p99; CPU per core at steady state; "
             "streams_open ~2,500; tasks at load.",
    # 25 s streams at 40 ev/s = 1000 events, interval 0.025 s.
    rate=100.0, smoke_rate=20.0, streaming=True,
    mode="ok", events=1000, interval=0.025,
))

_reg(Scenario(
    sid="S3", name="1k open streams",
    question="Memory and fd at 1,000 open slow-drip streams.",
    produces="RSS at 1k; fds inbound vs upstream at 1k; tasks at 1k.",
    # ramp to 1,000 concurrent: slow-drip long streams. At ~0.5 s drip and
    # 100 s streams that is ~10 rps x 100 s ~ 1000 in flight.
    rate=10.0, smoke_rate=5.0, streaming=True,
    mode="slow-drip", events=200, interval=0.5,
))

_reg(Scenario(
    sid="S4", name="10k open streams",
    question="WHAT BREAKS FIRST at 10,000 open streams.",
    produces="the first limit hit (predict ulimit -n, then per-stream asyncio "
             "task/buffer overhead); RSS at 10k; marginal bytes/stream (slope).",
    rate=100.0, smoke_rate=5.0, streaming=True,
    mode="slow-drip", events=200, interval=1.0,
))

_reg(Scenario(
    sid="S5", name="Slow clients",
    question="Backpressure: is gateway memory bounded when clients read slower "
             "than the upstream writes.",
    produces="peak pump_buffered_bytes (must stay under the pump ceiling); RSS "
             "bounded; no unbounded growth with 500 slow readers.",
    # 500 concurrent readers at 2 KiB/s while the upstream drips 20 KiB/s.
    rate=25.0, smoke_rate=5.0, streaming=True,
    mode="slow-drip", events=400, interval=0.05,  # ~20 KiB/s upstream
    read_bps=2048,                                 # client reads 2 KiB/s
))

_reg(Scenario(
    sid="S6", name="One hot tenant",
    question="Isolation: does tenant B's p99 move when A floods at 5x its cap.",
    produces="tenant B/C p99 during S6 vs baseline (B alone); A's 429 share.",
    rate=100.0, smoke_rate=10.0, streaming=True,
    mode="ok", events=5,
    unlimited=False,                      # REAL limiter so A can be throttled
    driver="s6_isolation",
    # (label, token, configured-cap-rps). The driver offers A at 5x its cap.
    tenant_groups=[("hot", "tok-hot", 100.0),
                   ("normal-b", "tok-b", 100.0),
                   ("normal-c", "tok-c", 100.0)],
))

_reg(Scenario(
    sid="S7", name="Provider killed mid-stream",
    question="Failure and recovery under load: breaker opens on kill, closes on "
             "restart.",
    produces="in-flight at kill; native endings; kill->breaker-open latency; "
             "restart->breaker-closed latency; deadline overruns.",
    rate=50.0, smoke_rate=10.0, streaming=True,
    mode="ok", events=50, interval=0.1,
    unlimited=False,                      # REAL breaker so it can open/close
    expect_upstream_work=False,           # during the kill the breaker SHEDS
    # flip the fake to 5xx from t=120 to t=180 (relative to measure start),
    # "kill at 120 s, restart at 180 s".
    flips=[(120.0, 180.0, "5xx")],
))

_reg(Scenario(
    sid="S8", name="Deploy under load",
    question="Drain: client errors must be ZERO when SIGTERM lands mid-stream.",
    produces="client errors (target 0); drain duration; streams cut (target 0).",
    # S8 runs the S3 workload, then SIGTERMs the gateway at t=60 into measure.
    # Streams are 200 x 0.5 s = 100 s. Two arms, chosen by env (bench/_gwproc):
    #   A  default: drain grace = total budget (600 s) >= stream; zero cuts.
    #   B  BENCH_GW_DRAIN_GRACE=30 BENCH_GW_BUDGET_TOTAL=600: grace < stream;
    #      cuts are the residual, the process must still exit at grace + ~3 s.
    #      (drain_allow_short is set automatically when grace < total; the
    #      total must be set too, or it defaults to grace - 5 and the DEADLINE,
    #      not the drain, would end every stream.)
    rate=10.0, smoke_rate=5.0, streaming=True,
    mode="slow-drip", events=200, interval=0.5,
    driver="s8_deploy",
    deploy_sigterm_at=60.0,
))


SCENARIOS = _S

__all__ = ["SCENARIOS", "Scenario", "GATEWAY_MODEL", "OPENAI_PATH"]

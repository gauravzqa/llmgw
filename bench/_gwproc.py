"""The gateway, in its OWN process, for the scale bench.

`bench/overhead.py` runs the gateway on a thread inside the bench process, which
is right for a latency microbench but wrong for a scale run: we have to sample
the gateway's RSS and fd count in isolation, and a gateway sharing a process
with the load generator would report the generator's memory and sockets as its
own. So the scale harness (`bench/load.py`) launches THIS module as a
subprocess, one gateway per run, and samples it by pid.

It is also the one path that gives us S8 (deploy-under-load) for free: we run
through `llmgw.server.lifecycle.run`, exactly as `python -m llmgw.server` does,
so a SIGTERM drains instead of cutting. A `uvicorn app:app` launch would cut
every stream on the signal and there would be nothing to measure.

Configuration rides on `BENCH_GW_*` environment variables rather than argv,
because the parent sets them on the spawned process and there is then one place
the contract lives. The knobs mirror the traps `bench/overhead.py` documents:

  * `BENCH_GW_UNLIMITED=1` lifts the tenant rate-limiter and the breaker out of
    reach (the admission code still runs; it simply never sheds). This is the
    throughput default, so S1-S5 measure the gateway and not its own limiter.
  * `BENCH_GW_UNLIMITED=0` uses a REAL breaker (so S7 can watch it open on a
    provider kill and close on restart) and a tenants file (so S6 can pit one
    hot tenant against two quiet ones and ask whether B's p99 moves).

The fake upstreams are a SEPARATE process again (`python -m fakes.upstream`),
so the gateway's fds split cleanly: a socket whose peer port is a fake port is
an UPSTREAM fd, everything on the listen port is INBOUND.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass

from llmgw.admission import TenantLimits
from llmgw.breaker import BreakerPolicy
from llmgw.clocks import Budgets
from llmgw.server.config import (
    DEFAULT_FORWARD_REQUEST_HEADERS,
    ServerConfig,
    fake_catalog,
)

# Same lift as bench/overhead.py: a 1e9 rps bucket and a billion permits so the
# limiter NEVER rejects, yet the admission/permit code runs on every request.
UNLIMITED_TENANT = TenantLimits(
    rate_per_second=1e9, burst=1_000_000_000, max_concurrency=1_000_000
)
BREAKER_NEVER_TRIPS = BreakerPolicy(failure_threshold=1_000_000)

# A real breaker for the failure scenarios: opens after 5 upstream failures,
# cools for 10 s, then lets one probe through. Small numbers so S7's kill ->
# open -> restart -> close transition happens inside the measured window.
BREAKER_REAL = BreakerPolicy(
    failure_threshold=5, window=30.0, cooldown=10.0, half_open_probes=1
)

# X-Fake-* must reach the fake on the GATEWAY arm too, or the two arms measure
# different stream shapes (the direct arm sets the shape per-request; the
# gateway arm would otherwise fall back to the fake's default mode). None of
# these is a credential or a connection-scoped header, so none is in
# config.NEVER_FORWARDED and validated() accepts them.
FAKE_FORWARD = (
    "x-fake-mode",
    "x-fake-events",
    "x-fake-interval",
    "x-fake-delay",
    "x-fake-status",
    "x-fake-bytes",
    # The WebSocket knobs (fakes/ws.py), for S9-S12's Arm G. Same argument:
    # an upgrade whose `X-Fake-*` headers are dropped reaches the fake as mode
    # `ok`, and the two arms would then measure different providers. The fake
    # also reads every knob as a `__fake_*` query parameter, which survives a
    # gateway that forwards query strings but not headers.
    "x-fake-stall-after",
    "x-fake-stall-side",
    "x-fake-read-bps",
    "x-fake-rtt",
)


# The total deadline every bench workload runs under. 600 s is what the 10 Sep
# campaign ran with (the then-default), kept so S1-S7 numbers stay comparable;
# a 100 s S3/S8 stream must fit inside it or the DEADLINE, not the drain, ends
# the stream and the drain verdict measures the wrong thing.
DEFAULT_BUDGET_TOTAL = 600.0


@dataclass(frozen=True)
class FleetMode:
    """The deploy/overload knobs the fleet was booted with, derived ONCE from
    `BENCH_GW_*` so the gateway process and the report agree by construction.

    The server refuses `budgets.total > drain_grace_seconds` unless told the
    cut is intended (`drain_allow_short`), and caps open streams per process
    (`max_streams`, default 400). Both defaults are right for production and
    wrong for a bench that exists to measure 1,000-10,000 open streams, so the
    bench derives them explicitly:

      * `BENCH_GW_BUDGET_TOTAL`  unset -> `BENCH_GW_DRAIN_GRACE - 5` when the
        grace is set (a consistent pair), else 600 (the 10 Sep campaign's total,
        kept so S1-S7 stay comparable).
      * `BENCH_GW_DRAIN_GRACE`   unset -> equal to the total, so no admitted
        stream can outlive the drain (S8 arm A).
      * `drain_allow_short`      True whenever the operator's grace is below the
        total (S8 arm B: the deliberate short grace); `BENCH_GW_DRAIN_ALLOW_SHORT=1`
        also forces it. Logged, never silent.
      * `BENCH_GW_MAX_STREAMS`   unset, blank or 0 -> None (uncapped). The S2
        shed run sets it explicitly.
    """

    budget_total: float
    drain_grace: float
    drain_allow_short: bool
    max_streams: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "gw_budget_total_s": self.budget_total,
            "gw_drain_grace_s": self.drain_grace,
            "gw_drain_allow_short": self.drain_allow_short,
            "gw_max_streams": self.max_streams,
            "gw_drain_arm": "B (grace < total: cuts are the residual)"
            if self.drain_grace < self.budget_total
            else "A (grace >= total: zero cuts expected)",
        }


def fleet_mode() -> FleetMode:
    grace_env = os.environ.get("BENCH_GW_DRAIN_GRACE", "").strip()
    total_env = os.environ.get("BENCH_GW_BUDGET_TOTAL", "").strip()
    if total_env:
        total = float(total_env)
        grace = float(grace_env) if grace_env else total
    elif grace_env:
        grace = float(grace_env)
        total = max(1.0, grace - 5.0)
    else:
        total = grace = DEFAULT_BUDGET_TOTAL
    forced = os.environ.get("BENCH_GW_DRAIN_ALLOW_SHORT", "0") == "1"
    raw_cap = os.environ.get("BENCH_GW_MAX_STREAMS", "").strip()
    cap = int(raw_cap) if raw_cap else 0
    return FleetMode(
        budget_total=total,
        drain_grace=grace,
        drain_allow_short=forced or grace < total,
        max_streams=cap if cap > 0 else None,
    )


def budget_total_from_env() -> float:
    return fleet_mode().budget_total


def drain_grace_from_env() -> float:
    return fleet_mode().drain_grace


def drain_allow_short_from_env() -> bool:
    return fleet_mode().drain_allow_short


def build_config() -> ServerConfig:
    port = int(os.environ["BENCH_GW_PORT"])
    openai_url = os.environ["BENCH_FAKE_OPENAI_URL"]
    anthropic_url = os.environ.get("BENCH_FAKE_ANTHROPIC_URL", openai_url)
    unlimited = os.environ.get("BENCH_GW_UNLIMITED", "1") == "1"
    tenants_file = os.environ.get("BENCH_GW_TENANTS_FILE") or None
    mode = fleet_mode()
    if mode.drain_grace < mode.budget_total:
        print(f"BENCH_GW_MODE arm=B drain_grace={mode.drain_grace}s < "
              f"budget_total={mode.budget_total}s: streams longer than the grace "
              f"will be CUT on SIGTERM (drain_allow_short=True)", file=sys.stderr,
              flush=True)

    catalog = fake_catalog(openai_url=openai_url, anthropic_url=anthropic_url)
    kwargs: dict[str, object] = dict(
        catalog=catalog,
        fake_upstreams=True,
        host="127.0.0.1",
        port=port,
        forward_request_headers=DEFAULT_FORWARD_REQUEST_HEADERS + FAKE_FORWARD,
        tenants_file=tenants_file,
        # The campaign's phase budgets, with the total pinned explicitly rather
        # than inherited from ServerConfig's default, so a change to the
        # shipped default cannot silently re-shape the bench workloads.
        budgets=Budgets(
            total=mode.budget_total, connect=2.0, first_event=20.0, progress=15.0,
            client_stall=30.0,
        ),
        drain_grace_seconds=mode.drain_grace,
        drain_allow_short=mode.drain_allow_short,
        # None lifts the per-process open-stream cap: S3/S4/S5 exist to find
        # the wall, not to be shed at 400. The S2 shed run sets it explicitly.
        max_streams=mode.max_streams,
    )
    if unlimited:
        kwargs["tenant_limits"] = UNLIMITED_TENANT
        kwargs["breaker"] = BREAKER_NEVER_TRIPS
    else:
        kwargs["breaker"] = BREAKER_REAL
        # tenant_limits is the anonymous fallback; with a tenants_file it is
        # only used if the file declares [tenants.anonymous]. Leave the default.
    return ServerConfig(**kwargs).validated()


def main() -> int:
    try:
        cfg = build_config()
    except ValueError as exc:
        # `validated()` refused the config. Say so in one greppable line and
        # exit non-zero NOW, so the parent's launch fails on the reason rather
        # than on a readiness timeout twenty seconds later.
        print(f"BENCH_GW_REFUSED: {exc}", file=sys.stderr, flush=True)
        return 2
    # One line to stderr so the parent can confirm the child booted and on which
    # port, without parsing uvicorn's logs.
    print(f"BENCH_GW_READY port={cfg.port} unlimited="
          f"{os.environ.get('BENCH_GW_UNLIMITED', '1')} "
          f"budget_total={cfg.budgets.total} drain_grace={cfg.drain_grace_seconds} "
          f"allow_short={cfg.drain_allow_short} max_streams={cfg.max_streams}",
          file=sys.stderr, flush=True)
    # Imported here, not at module level: `lifecycle` imports `llmgw.server.app`,
    # whose module-level `app = build_app(...)` builds a throwaway Gateway on
    # import. `bench.load` imports this module for its env helpers in the
    # driver and in every load-generator process, and none of them may
    # construct the system under test.
    from llmgw.server.lifecycle import run

    run(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

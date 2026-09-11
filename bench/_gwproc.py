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

from llmgw.admission import TenantLimits
from llmgw.breaker import BreakerPolicy
from llmgw.server.config import (
    DEFAULT_FORWARD_REQUEST_HEADERS,
    ServerConfig,
    fake_catalog,
)
from llmgw.server.lifecycle import run

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
)


def build_config() -> ServerConfig:
    port = int(os.environ["BENCH_GW_PORT"])
    openai_url = os.environ["BENCH_FAKE_OPENAI_URL"]
    anthropic_url = os.environ.get("BENCH_FAKE_ANTHROPIC_URL", openai_url)
    unlimited = os.environ.get("BENCH_GW_UNLIMITED", "1") == "1"
    tenants_file = os.environ.get("BENCH_GW_TENANTS_FILE") or None

    catalog = fake_catalog(openai_url=openai_url, anthropic_url=anthropic_url)
    kwargs: dict[str, object] = dict(
        catalog=catalog,
        fake_upstreams=True,
        host="127.0.0.1",
        port=port,
        forward_request_headers=DEFAULT_FORWARD_REQUEST_HEADERS + FAKE_FORWARD,
        tenants_file=tenants_file,
        # A long grace so S8's drain is observed, not truncated by the bench.
        drain_grace_seconds=float(os.environ.get("BENCH_GW_DRAIN_GRACE", "30")),
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
    cfg = build_config()
    # One line to stderr so the parent can confirm the child booted and on which
    # port, without parsing uvicorn's logs.
    print(f"BENCH_GW_READY port={cfg.port} unlimited="
          f"{os.environ.get('BENCH_GW_UNLIMITED', '1')}", file=sys.stderr, flush=True)
    run(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

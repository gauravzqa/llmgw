"""A gateway in its own process, serving the socket plane, for finding 41.

The chaos tier's WebSocket invariants cannot be asserted in-process, and the
reason is the whole point of the test. Finding 41
(`docs/15-findings-log.md:688-724`) is about what the gateway WRITES when
hundreds of streams end at once: 4 KB tracebacks per stream hung the S8-B
workers outright, because stderr was a pipe nobody was draining and a 64 KiB
pipe buffer fills at about sixteen of them. A thread inside pytest writes to
pytest's capture buffer, which is a list in memory that never fills and never
blocks -- so the bug is structurally invisible there.

So: a subprocess, its stderr on a `subprocess.PIPE` that the test never
reads, and a gateway pointed at a `fakes/ws.py` on another port. Everything
is read from the environment because the caller is `subprocess.Popen` and
there is nothing else to read from.

Kept in `tests/chaos/` rather than `bench/` because it is the chaos tier's
harness: `bench/_gwproc.py` builds the fake CHAT catalog and is load's.
"""

from __future__ import annotations

import os
import sys
from dataclasses import replace

from llmgw.catalog import DEFAULT_CATALOG, Catalog
from llmgw.clocks import Budgets
from llmgw.server.config import ServerConfig
from llmgw.server.lifecycle import run

KEY_ENV = "LLMGW_WS_CHAOS_KEY"
TENANT_TOKEN = "chaos-tenant-token"


def config() -> ServerConfig:
    upstream = os.environ["WSCHAOS_UPSTREAM_URL"]
    port = int(os.environ["WSCHAOS_PORT"])
    tenants = os.environ["WSCHAOS_TENANTS"]
    policy = os.environ["WSCHAOS_POLICY"]
    os.environ.setdefault(KEY_ENV, "not-a-real-key")
    catalog = Catalog(
        models={
            "inworld.tts-2-flash": DEFAULT_CATALOG.models["inworld.tts-2-flash"],
        },
        providers={
            "inworld": replace(
                DEFAULT_CATALOG.providers["inworld"], base_url=upstream,
                api_key_env=KEY_ENV,
                # Above the session count the test opens: this file is about
                # resource return-to-zero, and a credential cap refusing half
                # the sockets would prove it on a smaller number than the one
                # the test names.
                max_concurrency=2048,
            ),
        },
    )
    return ServerConfig(
        host="127.0.0.1", port=port, catalog=catalog, fake_upstreams=False,
        default_model="inworld.tts-2-flash",
        tenants_file=tenants, policy_file=policy,
        forward_request_headers=("x-fake-mode", "x-fake-bytes", "x-fake-events",
                                 "x-fake-interval"),
        budgets=Budgets(total=25.0, connect=5.0, headers=5.0, first_event=5.0,
                        progress=5.0, client_stall=5.0),
        max_streams=None,
        drain_grace_seconds=30.0,
        ws_drain_wait_s=5.0,
        capture_path=os.environ.get("WSCHAOS_CAPTURE") or None,
    )


def main() -> int:
    cfg = config()
    # The readiness line goes to STDOUT, which the test DOES drain. stderr is
    # the pipe under test and must stay empty.
    print(f"WSCHAOS_READY port={cfg.port}", flush=True)
    run(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())

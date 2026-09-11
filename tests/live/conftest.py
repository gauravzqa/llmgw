"""Fixtures for the live tier: a real gateway, real providers, real money.

--------------------------------------------------------------------------
Two locks, because one is not enough
--------------------------------------------------------------------------

`pyproject.toml` already carries `--strict-markers` and an `addopts` that
deselects `scale` and `chaos`. Neither helps here, and the reason is worth
stating precisely rather than assumed:

* `live` is not in the `markers` list, so under `--strict-markers` an
  unregistered `@pytest.mark.live` would ERROR the collection of this
  directory rather than skip it. `pytest_configure` below registers it.
* Registering it makes these tests *selectable*, and `addopts` does not
  deselect them -- so a plain `pytest tests/` would collect them, run them,
  and put real requests on a real credit card.

So the marker is not the safety mechanism. `LLMGW_LIVE=1` is. The marker is
how you ask for them; the environment variable is how the machine consents.
A default run of `pytest tests/` skips every test in this file before it can
open a socket -- verified by running `pytest tests/ -q` and reading the
skip reason.

`pytest.ini` cannot be edited by this work, and that is not a limitation
worth working around: an opt-in that lives in one file is an opt-in someone
can accidentally invert with one line. Two independent locks, in two
different systems, is the correct shape for a switch whose failure mode is a
bill.
"""

from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import uvicorn
from live.smoke import (
    GHOST_MODEL_ID,
    IDENTITY_MODEL_ID,
    OPENAI_MODEL_ID,
    build_live_app,
    live_catalog,
)
from starlette.applications import Starlette

from live import env
from llmgw.catalog import Catalog

LIVE_ENV_VAR = "LLMGW_LIVE"

BAD_KEY_ENV = "LLMGW_LIVE_CORRUPTED_KEY"
"""Where the deliberately-broken credential lives, for the duration of one
process. Built in memory from a real key by replacing its last four
characters; never written to a file, never printed, never asserted on."""

BAD_KEY_PROVIDER = "deepseek-badkey"
BAD_KEY_MODEL = "deepseek.v4-flash-badkey"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "live: sends real requests to real providers and spends real money; "
        "requires LLMGW_LIVE=1",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Skip everything here unless the operator opted in.

    Applied at collection to every item in this directory, not per-test with a
    `skipif` someone can forget on the test they add next month. The check is
    on the value `"1"` rather than on truthiness: `LLMGW_LIVE=0` must mean off,
    and a bare presence check would make it mean on.
    """
    if os.environ.get(LIVE_ENV_VAR) == "1":
        return
    skip = pytest.mark.skip(
        reason=f"live provider tests are opt-in and cost money: set {LIVE_ENV_VAR}=1"
    )
    for item in items:
        if str(item.fspath).replace(os.sep, "/").find("/tests/live/") >= 0:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def credentials() -> None:
    """Load the keys from outside the repo. Presence only is ever reported."""
    env.ensure_loaded()
    env.require("ANTHROPIC_API_KEY", "OPENAI_API_KEY", "DEEPSEEK_API_KEY")


def corrupt(key: str) -> str:
    """A key that is the right shape and the wrong value.

    The last four characters are replaced rather than the string truncated, so
    the request still has a plausible length and the provider has to reject it
    on the value -- which is the thing being classified. A truncated key can be
    rejected as a malformed header instead, which is a different code path
    wearing the same status.
    """
    return key[:-4] + "zzzz" if len(key) > 8 else "sk-not-a-real-key-zzzz"


@pytest.fixture(scope="session")
def catalog() -> Catalog:
    """The live catalog plus one provider holding a corrupted credential."""
    base = live_catalog()
    os.environ[BAD_KEY_ENV] = corrupt(os.environ["DEEPSEEK_API_KEY"])
    provider = replace(
        base.providers["deepseek"], id=BAD_KEY_PROVIDER,
        api_key_env=BAD_KEY_ENV, credential_id=BAD_KEY_PROVIDER,
        # Its OWN credential_id, which is the point. `AuthenticationFailed`
        # is scoped to the credential, so a shared one would mean this test
        # marks the real DeepSeek key unhealthy for every other test in the
        # session -- the exact cross-tenant blast radius to avoid.
    )
    model = replace(
        base.models["deepseek.deepseek-v4-flash"], id=BAD_KEY_MODEL,
        provider=BAD_KEY_PROVIDER,
    )
    return base.with_overrides(providers={BAD_KEY_PROVIDER: provider},
                               models={model.id: model})


POLICY = f"""
default_workload = "deepseek"

[defaults.budgets]
total = 60.0
connect = 5.0
first_event = 30.0
progress = 20.0
client_stall = 30.0

[defaults.retry]
max_attempts = 1
base_delay = 0.2
max_delay = 1.0
respect_retry_after = true

[workloads.deepseek]
incumbent = "deepseek.deepseek-v4-flash"

[workloads.anthropic]
incumbent = "anthropic.haiku-4-5"

# The same Anthropic model reached through a provider that sends
# `accept-encoding: identity`. The only route in this file that can complete
# a stream OR parse a buffered body -- see test_live.py's gzip tests.
[workloads.anthropic-identity]
incumbent = "{IDENTITY_MODEL_ID}"

[workloads.fallback]
incumbent = "{OPENAI_MODEL_ID}"
candidate = "{GHOST_MODEL_ID}"

[workloads.badkey]
incumbent = "{BAD_KEY_MODEL}"
"""


class LiveGateway:
    """One uvicorn hosting one `build_app()` result, on its own thread."""

    def __init__(self, app: Starlette, server: uvicorn.Server,
                 thread: threading.Thread, port: int) -> None:
        self.app, self.server, self.thread, self.port = app, server, thread, port

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(10.0)
        if self.thread.is_alive():  # pragma: no cover
            self.server.force_exit = True
            self.thread.join(10.0)


@pytest.fixture(scope="session")
def gateway(catalog: Catalog):
    """One gateway for the whole file.

    Session-scoped because starting uvicorn costs ~20 ms and opening a TLS
    connection to each provider costs far more -- and because a per-test
    gateway would throw away the connection pool between tests, which would
    make every measurement here a cold-start measurement.
    """
    with tempfile.TemporaryDirectory(prefix="llmgw-live-tests-") as tmp:
        policy = Path(tmp) / "live.toml"
        policy.write_text(POLICY, encoding="utf-8")
        app = build_live_app(str(policy), catalog=catalog)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(32)
        port = sock.getsockname()[1]
        server = uvicorn.Server(
            uvicorn.Config(app, log_level="error", access_log=False, lifespan="on")
        )
        thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]},
                                  daemon=True, name=f"llmgw-live-test-{port}")
        thread.start()
        deadline = time.monotonic() + 15.0
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:  # pragma: no cover
                raise RuntimeError("gateway failed to start")
            time.sleep(0.005)
        gw = LiveGateway(app, server, thread, port)
        try:
            yield gw
        finally:
            gw.stop()


@pytest.fixture
def client():
    with httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0)) as c:
        yield c

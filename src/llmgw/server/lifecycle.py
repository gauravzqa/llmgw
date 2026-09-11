"""The graceful deploy: SIGTERM -> drain -> exit, with zero cut streams.

This module is the deploy-under-load scenario (S8) expressed as a process runner. A
rolling deploy sends the old process a SIGTERM and expects it to go away. The
naive thing -- what a bare `uvicorn.Server` does -- is to set `should_exit` the
instant the signal arrives, which cancels every in-flight stream mid-flight:
every connected client gets a truncated body and a vendor error, and the S8
table fills with client errors that were caused by the deploy, not by any
provider. That is the failure this file exists to prevent.

The shape instead is a *sequence*, and the order is the whole contract:

    1. flip `draining` (synchronously) -- `/healthz` answers 503, so the load
       balancer stops routing new work here, and the ingress begins shedding
       the requests that race that transition with a 503 "draining".
    2. WAIT for the in-flight streams to finish, bounded by the grace.
    3. only THEN tell uvicorn to exit, so its own shutdown cancels whatever is
       left -- which, if the grace was sized right, is nothing.

Steps 1 and 2 are `Gateway.begin_drain()`. This file owns step 3 and the
signal plumbing: it replaces uvicorn's default handlers (which would do the
naive thing) with handlers that run the drain first.

--------------------------------------------------------------------------
Why `loop.add_signal_handler` and not `signal.signal`
--------------------------------------------------------------------------

A C-level `signal.signal` handler runs between bytecodes, on whatever stack
happens to be executing, and the only async-safe thing it can do is set a
flag. `loop.add_signal_handler` instead schedules the callback on the event
loop, where it is free to spawn the drain coroutine that has to `await` the
in-flight streams. uvicorn 0.52 installs `signal.signal` handlers inside
`serve()`; `_DrainingServer` disables that capture so ours, installed on the
loop, are the ones that fire.

--------------------------------------------------------------------------
The double-signal case
--------------------------------------------------------------------------

An operator who sends a second SIGTERM (or hits Ctrl-C twice) during a drain
is saying "I am not willing to wait for the grace". So the second signal
forces the exit immediately: `force_exit` on the uvicorn side and a cancel of
the drain task. The first signal is patience; the second is its withdrawal.
The importable `llmgw.server.app:app` is untouched by any of this -- the
contract and fakes harness imports that ASGI object directly and must keep
working -- so this runner is strictly additive.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Iterator

import uvicorn

from llmgw.server.app import DrainReport, Gateway, build_app
from llmgw.server.config import ServerConfig

log = logging.getLogger("llmgw.server.lifecycle")

# The signals a deploy uses to ask a process to stop. SIGTERM is the
# orchestrator's polite request (Kubernetes sends it, then waits
# `terminationGracePeriodSeconds` before SIGKILL); SIGINT is a developer's
# Ctrl-C at `make run`. Both mean the same thing to us -- drain -- so both get
# the same handler.
_DRAIN_SIGNALS: tuple[signal.Signals, ...] = (signal.SIGTERM, signal.SIGINT)


class _DrainingServer(uvicorn.Server):
    """`uvicorn.Server` with its own signal capture disabled.

    uvicorn's `serve()` wraps the run in `capture_signals()`, which installs
    `signal.signal(sig, self.handle_exit)` for SIGINT/SIGTERM -- handlers that
    set `should_exit` the instant the signal lands, i.e. the naive cut-every-
    stream behaviour. We install drain-first handlers on the event loop in
    `serve()` below, so uvicorn's capture would only clobber ours and skip the
    drain. Overriding it to a no-op is the one line that hands signal control
    to this module without re-implementing the rest of `uvicorn.Server`."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        # Deliberately empty: lifecycle.serve() owns the signal handlers.
        yield


async def serve(config: ServerConfig) -> DrainReport | None:
    """Run the gateway until a drain signal, then drain and exit.

    Builds the app from `config` (the same `build_app` the importable `app`
    uses, so the serving behaviour is identical), runs it under a
    `_DrainingServer`, and installs drain-first SIGTERM/SIGINT handlers on the
    running loop. Returns the `DrainReport` from the drain that ended the
    process, or None if the server stopped without one (e.g. a loop that does
    not support signal handlers fell back to uvicorn's default path).

    The grace period is `config.drain_grace_seconds`; the orchestrator's own
    kill timeout should sit above it so the drain, not a SIGKILL, is what ends
    the process."""
    app = build_app(config)
    gateway: Gateway = app.state.gateway
    grace_s = config.drain_grace_seconds

    uconfig = uvicorn.Config(
        app,
        host=config.host,
        port=config.port,
        # lifespan="on" is not optional: the Upstream pool is opened in the
        # Starlette lifespan so it can be CLOSED at shutdown. A deploy that
        # leaves the pool open holds the provider's concurrency until the
        # process dies -- the exact double-occupancy a graceful deploy exists
        # to avoid (see the lifespan docstring in app.py).
        lifespan="on",
        log_level="info",
    )
    server = _DrainingServer(uconfig)

    loop = asyncio.get_running_loop()
    # A one-slot mutable so the handler (a plain callable, not a closure over a
    # rebindable name) can see and replace the in-flight drain task.
    state: dict[str, asyncio.Task[None] | DrainReport | None] = {
        "task": None, "report": None,
    }

    async def _drain_then_exit() -> None:
        """Step 2 and step 3: wait for the streams, then let uvicorn stop.

        `should_exit` is set in a `finally` so that it flips whether the drain
        finished cleanly OR was cancelled by a second signal -- the one thing
        that must always happen once a drain has begun is that the process
        eventually exits. Setting it only on the success path would leave a
        cancelled drain running forever."""
        try:
            report = await gateway.begin_drain(grace_s=grace_s)
            state["report"] = report
            log.info(
                "drain complete: inflight_at_start=%d cut=%d duration=%.3fs "
                "timed_out=%s",
                report.inflight_at_start, report.cut, report.duration_s,
                report.timed_out,
            )
            if report.cut:
                # Not an error -- the documented row-9 residual -- but the one
                # number S8 is scored on, so it is logged at WARNING, not INFO.
                log.warning(
                    "%d stream(s) still open after %.1fs grace; shutdown will "
                    "cut them", report.cut, grace_s,
                )
        except asyncio.CancelledError:
            log.warning("drain cancelled by a second signal; forcing exit")
            raise
        finally:
            server.should_exit = True

    def _on_signal(sig: signal.Signals) -> None:
        """Drain-first on the first signal; force-exit on the second.

        The first signal spawns the drain. A second signal arriving while the
        drain task is still running is the operator withdrawing their
        patience, so it forces uvicorn's immediate exit and cancels the drain
        -- the cancellation unwinds every still-open stream through the
        executor's `finally` (its terminal record and CANCELED outcome), so
        even a forced exit cuts cleanly rather than dropping sockets."""
        task = state["task"]
        if task is not None:
            log.warning("second %s during drain; forcing exit now", sig.name)
            server.should_exit = True
            server.force_exit = True
            if isinstance(task, asyncio.Task) and not task.done():
                task.cancel()
            return
        log.info("%s received; draining for up to %.1fs", sig.name, grace_s)
        state["task"] = asyncio.ensure_future(_drain_then_exit())

    installed: list[signal.Signals] = []
    for sig in _DRAIN_SIGNALS:
        try:
            loop.add_signal_handler(sig, _on_signal, sig)
        except (NotImplementedError, RuntimeError):
            # Some loops (notably on Windows) cannot install loop-level signal
            # handlers. Rather than pretend, fall back to uvicorn's own signal
            # capture so the process is still killable -- the drain is lost,
            # which is honest about the platform's limits and logged once.
            log.warning(
                "loop.add_signal_handler(%s) unavailable; drain-on-signal is "
                "disabled on this platform", sig.name,
            )
        else:
            installed.append(sig)

    try:
        await server.serve()
    finally:
        for sig in installed:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(sig)

    report = state["report"]
    return report if isinstance(report, DrainReport) else None


def run(config: ServerConfig | None = None) -> DrainReport | None:
    """Synchronous entry: `asyncio.run(serve(config))`.

    `config` defaults to `ServerConfig.from_env()`, so `python -m llmgw.server`
    and `make run` need pass nothing -- the whole deployment is `LLMGW_*`
    variables, `LLMGW_DRAIN_GRACE` among them."""
    return asyncio.run(serve(config or ServerConfig.from_env()))


if __name__ == "__main__":  # pragma: no cover - exercised via `python -m llmgw.server`
    run()

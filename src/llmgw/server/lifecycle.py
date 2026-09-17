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
       left -- which, if the grace was sized right, is nothing -- and does so
       within a SHORT bound (`UVICORN_SHUTDOWN_TIMEOUT_S`), never by waiting
       on the leftovers a second time.

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
import socket
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

# How long uvicorn's OWN shutdown may wait for open connections once
# `should_exit` flips. By then `Gateway.begin_drain` has already waited the
# full grace; whatever is still open is the documented residual and must be
# cut NOW, not waited on a second time. uvicorn's default is None -- wait
# forever -- and that is exactly how S8 (10 Sep) ended with four workers
# "still running" long after a 30 s grace against 100 s streams: the grace
# expired, `should_exit` was set, and uvicorn then sat on the open streams
# until the bench gave up. In production that second wait is ended by the
# orchestrator's SIGKILL, so every stream longer than the grace would be
# killed mid-write instead of closed with its native ending, and the process
# would never exit on its own. A few seconds is enough for those endings to
# flush and for the lifespan shutdown to close the upstream pool. The deploy
# arithmetic is therefore:
#
#     budgets.total <= drain_grace < drain_grace + this < kill_timeout
UVICORN_SHUTDOWN_TIMEOUT_S: float = 3.0


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


_C2_ENDING_MESSAGE = "ASGI callable returned without completing response."
"""uvicorn's name for the way this gateway ends a committed stream it cannot
finish.

CONTRACTS.md C2: after commitment the only honest ending is to stop --
return from the ASGI callable without `more_body: False`, so the client sees
a body with no chunked terminator, no `data: [DONE]`, no `message_stop`, no
synthesised error frame. uvicorn closes the transport for us and then logs
exactly this line at ERROR, once per stream, because from where it stands
an app that returned mid-response has a bug. Here it is the contract.

The line matters for the same reason the traceback burst did: it is emitted
once per stream cut by a shutdown, in the same instant, and every byte of
per-stream logging on a shutdown path is a byte an undrained stderr pipe has
to absorb before the process can exit (the S8-B finding). The filter drops
this one message and nothing else; uvicorn's other errors, including the
`Cancel N running task(s)` line that says the grace was too short, still
reach the log."""


class _NotAnErrorHere(logging.Filter):
    """Drop uvicorn's `_C2_ENDING_MESSAGE`; pass everything else."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage() != _C2_ENDING_MESSAGE


def _quiet_c2_endings() -> None:
    """Install `_NotAnErrorHere` on `uvicorn.error`, once.

    Called from `serve()` AFTER `uvicorn.Config` has run its `dictConfig`,
    which replaces handlers but leaves filters added to the logger object
    alone. Idempotent: `addFilter` on an already-installed instance is a no-op,
    and the module holds one instance so repeated `serve()` calls in a
    process (the contract harness) never stack filters."""
    logging.getLogger("uvicorn.error").addFilter(_C2_FILTER)


_C2_FILTER = _NotAnErrorHere()


def bind_sockets(host: str, port: int, *, backlog: int = 2048) -> list[socket.socket]:
    """Pre-bind the listening socket so a `::` host is genuinely dual-stack.

    When uvicorn is given only a host and port, asyncio's `create_server`
    opens the AF_INET6 socket itself and sets `IPV6_V6ONLY=1` on it (it does
    that so `host=None` can bind `::` and `0.0.0.0` side by side without
    EADDRINUSE). The result on a host of `::` is an IPv6-only listener even
    on a kernel whose `bindv6only` is 0 -- which is what put `layrs-llmgw`
    half-deployed on 17 Sep 2026: the sibling machine reached it over Fly's
    IPv6 private network, and Fly's IPv4 health check got connection refused.

    Binding the socket here with `dualstack_ipv6=True` clears V6ONLY, and
    uvicorn's `serve(sockets=...)` skips its own bind. Any other host binds
    exactly as before. Returned sockets are owned by uvicorn from then on.
    """
    if host == "::" and socket.has_dualstack_ipv6():
        sock = socket.create_server(
            (host, port), family=socket.AF_INET6, dualstack_ipv6=True,
            backlog=backlog, reuse_port=False,
        )
    else:
        family = socket.AF_INET6 if ":" in host else socket.AF_INET
        sock = socket.create_server((host, port), family=family, backlog=backlog)
    sock.set_inheritable(True)
    return [sock]


async def serve(config: ServerConfig) -> DrainReport | None:
    """Run the gateway until a drain signal, then drain and exit.

    Builds the app from `config` (the same `build_app` the importable `app`
    uses, so the serving behaviour is identical), runs it under a
    `_DrainingServer`, and installs drain-first SIGTERM/SIGINT handlers on the
    running loop. Returns the `DrainReport` from the drain that ended the
    process, or None if the server stopped without one (e.g. a loop that does
    not support signal handlers fell back to uvicorn's default path).

    The grace period is `config.drain_grace_seconds`; the orchestrator's own
    kill timeout should sit above `grace + UVICORN_SHUTDOWN_TIMEOUT_S` so the
    drain, not a SIGKILL, is what ends the process."""
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
        # Bound uvicorn's own wait for open connections after `should_exit`.
        # Our drain owns the real wait (the grace); this only has to be long
        # enough for the leftovers' native endings to flush. See the constant.
        timeout_graceful_shutdown=UVICORN_SHUTDOWN_TIMEOUT_S,
    )
    server = _DrainingServer(uconfig)
    sockets = bind_sockets(config.host, config.port)
    _quiet_c2_endings()

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
                # A prediction, not the verdict: these are the streams uvicorn
                # is ABOUT to cut. INFO here, so the shutdown path carries
                # exactly one WARNING -- `log_shutdown_cuts`, after the cuts
                # have happened and the count is final.
                log.info(
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
        await server.serve(sockets=sockets)
    finally:
        for sig in installed:
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(sig)

    # uvicorn's `shutdown()` CANCELS the request tasks that outlived
    # `timeout_graceful_shutdown` but does not await them, so `serve()` can
    # return while their `except CancelledError` branches -- the ones that
    # count themselves in `gateway.shutdown_cuts` -- have not run yet. Let
    # them settle, bounded, before reading the count; without this the
    # summary line reads 0 and the real cuts are counted during loop
    # teardown, after anyone could log them. (Verified against uvicorn
    # 0.52: `t.cancel(...)` in a loop, no `gather`.) Tasks leave
    # `server_state.tasks` on completion, so the set is exactly the
    # unsettled ones.
    pending = {t for t in server.server_state.tasks if not t.done()}
    if pending:
        await asyncio.wait(pending, timeout=UVICORN_SHUTDOWN_TIMEOUT_S)

    report = state["report"]
    report = report if isinstance(report, DrainReport) else None
    log_shutdown_cuts(gateway, report=report, grace_s=grace_s)
    return report


def log_shutdown_cuts(
    gateway: Gateway, *, report: DrainReport | None, grace_s: float
) -> None:
    """The ONE line the shutdown path writes about cut streams.

    Called after `server.serve()` has returned -- i.e. after uvicorn's
    post-grace cancel has run through every open request and each has
    counted itself in `gateway.shutdown_cuts` -- because that is the only
    moment the number is final. Nothing per stream is logged before it, by
    rule: on the S8-B run of 15 Sep 2026 a per-stream traceback and then a
    per-stream WARNING both turned out to be output that scales with the
    number of open streams, and output that scales with open streams is
    what blocks a process on an undrained stderr pipe. This line's size is
    bounded by the catalog (at most `top` targets are named) and by nothing
    the client controls. Silent when nothing was cut, which is the S8 success
    case and every idle deploy."""
    cuts = gateway.shutdown_cuts
    if cuts.total == 0:
        return
    open_at_expiry = report.cut if report is not None else -1
    log.warning(
        "%s (%d open when the %.1fs grace expired); per-request detail is in "
        "the capture records (outcome=canceled)",
        cuts.summary(), open_at_expiry, grace_s,
    )


def run(config: ServerConfig | None = None) -> DrainReport | None:
    """Synchronous entry: `asyncio.run(serve(config))`.

    `config` defaults to `ServerConfig.from_env()`, so `python -m llmgw.server`
    and `make run` need pass nothing -- the whole deployment is `LLMGW_*`
    variables, `LLMGW_DRAIN_GRACE` among them."""
    return asyncio.run(serve(config or ServerConfig.from_env()))


if __name__ == "__main__":  # pragma: no cover - exercised via `python -m llmgw.server`
    run()

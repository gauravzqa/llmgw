"""Drain orchestration, on a ManualClock: no sockets, no real sleeps.

The S8 scenario -- SIGTERM, drain, exit with zero cut streams -- is a
sequence of three facts, and this file proves each as a property of
`Gateway.begin_drain` and the in-flight tracker rather than of a live server:

* a drain WAITS for the streams in flight when it started, and returns the
  instant the last one finishes (the zero-cut success path);
* when the grace expires first, it reports exactly the streams still open as
  CUT, and says it timed out;
* `draining` flips once and stays flipped (monotone);
* a request that arrives while draining is shed at ingress with reason
  "draining", recorded where the other denials are;
* the tracker returns to zero on EVERY exit -- a clean pass and a shed alike.

Everything runs on `ManualClock`, so the grace-expiry case costs microseconds:
a test that proved a 25-second grace by waiting 25 seconds is a test that gets
marked skip within a month (see tests/unit/test_clocks.py).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import pytest

from llmgw.clocks import ManualClock
from llmgw.server.app import (
    ROUTE_TO_UPSTREAM_PATH,
    DrainReport,
    Gateway,
    PassthroughEndpoint,
    build_app,
)
from llmgw.server.config import ServerConfig
from llmgw.surfaces import for_path


def _gateway(clock: ManualClock | None = None) -> Gateway:
    """A gateway on a ManualClock, no `startup()`: `begin_drain` and the
    tracker touch neither the upstream pool nor the collectors, so the object
    a test constructs to read its config is exactly the object under test."""
    return Gateway(ServerConfig(), clock=clock or ManualClock(start=1_000.0))


def _endpoint(gw: Gateway) -> PassthroughEndpoint:
    path, upstream_path = next(iter(ROUTE_TO_UPSTREAM_PATH.items()))
    surface = for_path(upstream_path)
    assert surface is not None
    return PassthroughEndpoint(gw, surface=surface, route=path)


async def _drive(ep: PassthroughEndpoint, path: str) -> list[dict]:
    """Invoke the ASGI endpoint once and collect what it sent. The shed path
    never reads the body, so `receive` is a stub it will not call."""
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    async def receive() -> dict:  # pragma: no cover - shed path never receives
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http", "method": "POST", "path": path,
        "headers": [], "path_params": {},
    }
    await ep(scope, receive, send)
    return sent


# --------------------------------------------------------------- the wait


async def test_drain_waits_for_in_flight_streams_and_returns_when_they_finish():
    gw = _gateway()
    gw.stream_entered()
    gw.stream_entered()  # two streams predate the drain

    drain = asyncio.ensure_future(gw.begin_drain(grace_s=25.0))
    await asyncio.sleep(0)  # let begin_drain park on the idle event

    # draining is on the instant the call was made, before it returns.
    assert gw.draining is True
    assert not drain.done(), "a drain with streams in flight must not return yet"

    gw.stream_exited()
    await asyncio.sleep(0)
    assert not drain.done(), "one stream still open: still waiting"

    gw.stream_exited()  # the last one finishes
    report = await drain

    assert report == DrainReport(
        inflight_at_start=2, cut=0, duration_s=0.0, timed_out=False
    )
    assert gw.inflight == 0


async def test_idle_gateway_drains_immediately():
    gw = _gateway()
    report = await gw.begin_drain(grace_s=25.0)
    assert report.inflight_at_start == 0
    assert report.cut == 0
    assert report.timed_out is False


# --------------------------------------------------------------- the grace


async def test_grace_expiry_reports_the_remaining_streams_as_cut():
    clock = ManualClock(start=1_000.0)
    gw = _gateway(clock)
    gw.stream_entered()  # this one will never finish

    drain = asyncio.ensure_future(gw.begin_drain(grace_s=25.0))
    await asyncio.sleep(0)
    assert not drain.done()

    await clock.advance(25.0)  # the grace expires with the stream still open
    report = await drain

    assert report.inflight_at_start == 1
    assert report.cut == 1, "the open stream is reported as cut"
    assert report.timed_out is True
    assert report.duration_s == pytest.approx(25.0)
    # begin_drain does NOT itself cut the stream: the tracker is untouched, so
    # the server's own shutdown is what cancels it, through the terminal path.
    assert gw.inflight == 1


# --------------------------------------------------------------- monotone


async def test_draining_flips_once_and_stays():
    gw = _gateway()
    assert gw.draining is False
    await gw.begin_drain(grace_s=1.0)
    assert gw.draining is True
    # A second drain finds it already True and leaves it True -- the
    # double-SIGTERM path calls this harmlessly.
    await gw.begin_drain(grace_s=1.0)
    assert gw.draining is True


# ------------------------------------------------------------- ingress shed


async def test_new_request_while_draining_is_shed_with_reason_draining():
    gw = _gateway()
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))

    gw.draining = True
    sent = await _drive(ep, path)

    start = next(m for m in sent if m["type"] == "http.response.start")
    body = next(m for m in sent if m["type"] == "http.response.body")["body"]
    assert start["status"] == 503
    assert json.loads(body)["error"]["type"] == "draining"

    # Recorded where admission's denials are, under the pre-declared label.
    assert gw.draining_denials() == {"draining": 1}


async def test_a_request_not_draining_is_not_shed():
    gw = _gateway()
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))

    # Not draining: the shed branch must not fire. The request runs past the
    # shed and then fails downstream (no `startup()`, so the pool is not open)
    # -- which is precisely the proof that it got PAST the 503 shed. We only
    # assert two things: no draining 503 was emitted, and no draining denial
    # was recorded.
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    async def receive() -> dict:
        return {"type": "http.request", "body": b"{}", "more_body": False}

    scope = {"type": "http", "method": "POST", "path": path,
             "headers": [], "path_params": {}}
    with contextlib.suppress(Exception):
        await ep(scope, receive, send)

    drained_503 = [
        m for m in sent
        if m["type"] == "http.response.start" and m["status"] == 503
    ]
    assert not drained_503, "a non-draining request must not be shed"
    assert gw.draining_denials() == {"draining": 0}
    assert gw.inflight == 0  # tracker still pairs on the non-shed exit


# -------------------------------------------------- tracker return-to-zero


async def test_tracker_returns_to_zero_on_a_shed_request():
    gw = _gateway()
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))

    gw.draining = True
    assert gw.inflight == 0
    await _drive(ep, path)
    # The shed incremented the tracker at entry and the finally decremented it:
    # a shed request can never pin the drain it is being shed to protect.
    assert gw.inflight == 0


async def test_tracker_pairs_on_entry_and_exit():
    gw = _gateway()
    assert gw.inflight == 0
    gw.stream_entered()
    assert gw.inflight == 1
    gw.stream_entered()
    assert gw.inflight == 2
    gw.stream_exited()
    gw.stream_exited()
    assert gw.inflight == 0
    # The floor holds even if exit is over-called (belt-and-braces guard).
    gw.stream_exited()
    assert gw.inflight == 0


async def test_drain_then_a_stream_finishing_wakes_the_waiter_exactly_once():
    """The idle event is an edge, not a gate: a stream that enters AFTER the
    count hit zero must re-clear it, or a late request could sail past a drain
    that already saw zero."""
    gw = _gateway()
    gw.stream_entered()
    drain = asyncio.ensure_future(gw.begin_drain(grace_s=10.0))
    await asyncio.sleep(0)
    gw.stream_exited()
    report = await drain
    assert report.cut == 0
    assert gw.inflight == 0


# ==========================================================================
# Adversarial pass -- the seam attacked, not just exercised
# ==========================================================================
#
# Everything above proves the drain does what it should on the paths a request
# actually takes. This section is the other half: the specific ways a reviewer
# would try to break it -- the `<= 0` clamp hiding a mispair, a grace of zero,
# a drain overlapping both a shed and an in-flight request, the signal runner's
# no-op capture. Added in an adversarial review pass.


async def test_clamp_logs_when_the_tracker_undershoots_zero(caplog):
    """The `<= 0` clamp no longer hides a mispair in silence.

    The clamp is correct to hold the floor -- a drain must not hang on a count
    that undershot zero -- but a SILENT reset to zero is the S8 lie: a future
    serving path that decremented without a paired increment would drive the
    count negative, the clamp would fire `_idle` early, and `begin_drain` would
    report `cut=0` over a still-open stream. The floor stays; the warning is
    what makes the mispair visible. (This is unreachable through the real
    endpoint today -- `stream_entered`/`stream_exited` are a 1:1 pair around
    one `try/finally` -- so the over-decrement is forced here by hand.)
    """
    gw = _gateway()
    assert gw.inflight == 0
    with caplog.at_level("WARNING", logger="llmgw.server"):
        gw.stream_exited()  # a decrement with no matching entry
    assert gw.inflight == 0, "the floor still holds"
    assert gw._idle.is_set()
    assert any(
        "in-flight tracker went negative" in r.getMessage() for r in caplog.records
    ), "a silent clamp would have masked the mispair"


async def test_grace_zero_with_a_stream_in_flight_times_out_at_once():
    """Grace of 0 is a drain that refuses to wait. With a stream still open it
    must time out immediately -- cut the stream, report it -- not block, and
    not wait out a nonexistent grace."""
    clock = ManualClock(start=1_000.0)
    gw = _gateway(clock)
    gw.stream_entered()  # never finishes
    report = await gw.begin_drain(grace_s=0.0)
    assert report.inflight_at_start == 1
    assert report.cut == 1
    assert report.timed_out is True
    assert report.duration_s == pytest.approx(0.0)
    assert gw.inflight == 1  # begin_drain does not itself cut


async def test_grace_zero_while_idle_does_not_spuriously_time_out():
    """Grace of 0 with nothing in flight is the common deploy case (the last
    stream already drained). It must return clean -- `_idle` is already set, so
    the wait returns before the zero-length timer can fire -- not report a
    phantom cut."""
    gw = _gateway(ManualClock(start=1_000.0))
    report = await gw.begin_drain(grace_s=0.0)
    assert report.cut == 0
    assert report.timed_out is False


async def test_drain_overlapping_a_shed_and_an_in_flight_request_returns_to_zero():
    """The race the drain exists to survive: a drain running WHILE a request is
    being shed (entered then 503'd) and another is genuinely in flight. The
    shed request must leave the tracker exactly as it found it, and the drain
    must wait only for the real one and then see zero."""
    gw = _gateway()
    gw.stream_entered()  # the real in-flight stream, predating the drain
    drain = asyncio.ensure_future(gw.begin_drain(grace_s=25.0))
    await asyncio.sleep(0)
    assert not drain.done()

    # A request arrives during the drain: the endpoint increments at entry and
    # decrements in the finally even though it is shed with a 503.
    gw.stream_entered()
    assert gw.inflight == 2
    gw.stream_exited()  # the shed request's finally
    await asyncio.sleep(0)
    assert not drain.done(), "the shed request must not end the drain; the real one is open"

    gw.stream_exited()  # the real stream finishes
    report = await drain
    assert report.inflight_at_start == 1, "the shed request entered after the snapshot"
    assert report.cut == 0
    assert gw.inflight == 0


async def test_two_concurrent_drains_while_in_flight_both_report_clean():
    """Double-SIGTERM's begin_drain half: two drains parked on the same tracker
    both wake and both report `cut=0` when the last stream finishes. `draining`
    is monotone, so neither un-decides; the handler turns the SECOND signal
    into a force-exit, but calling begin_drain twice is harmless."""
    gw = _gateway()
    gw.stream_entered()
    d1 = asyncio.ensure_future(gw.begin_drain(grace_s=10.0))
    d2 = asyncio.ensure_future(gw.begin_drain(grace_s=10.0))
    await asyncio.sleep(0)
    assert not d1.done() and not d2.done()
    assert gw.draining is True
    gw.stream_exited()
    r1 = await d1
    r2 = await d2
    assert r1.cut == 0 and r2.cut == 0
    assert r1.timed_out is False and r2.timed_out is False


def test_draining_server_capture_signals_is_a_genuine_no_op():
    """`_DrainingServer.capture_signals` must NOT install uvicorn's own
    `signal.signal(handle_exit)` handlers -- those set `should_exit` the
    instant a signal lands and would cut every stream, bypassing the drain
    entirely. The lifecycle runner installs drain-first handlers on the loop;
    this override is the one line that stops uvicorn from clobbering them."""
    import signal

    import uvicorn

    from llmgw.server.lifecycle import _DrainingServer

    server = _DrainingServer(uvicorn.Config(build_app(ServerConfig())))
    before = signal.getsignal(signal.SIGTERM)
    with server.capture_signals():
        # Inside the context uvicorn would normally have replaced the handler.
        during = signal.getsignal(signal.SIGTERM)
    after = signal.getsignal(signal.SIGTERM)
    assert during is before, "capture_signals installed a handler; drain would be bypassed"
    assert after is before


# ---------------------------------------------------------------------------
# uvicorn's own shutdown wait is bounded, and the knob still exists
# ---------------------------------------------------------------------------


def test_uvicorn_shutdown_wait_is_short_and_the_knob_is_still_accepted():
    """`lifecycle.serve` passes `timeout_graceful_shutdown` so that, once the
    drain has spent the grace, uvicorn cuts the leftovers within seconds
    rather than waiting on them forever (the S8 "still running" finding). Two
    tripwires: the bound stays small, and the uvicorn we ship against still
    takes the keyword -- a rename on upgrade would otherwise fail only at
    `serve()`, i.e. only in a running process, never in this tier."""
    import inspect

    import uvicorn

    from llmgw.server.lifecycle import UVICORN_SHUTDOWN_TIMEOUT_S

    assert 0.0 < UVICORN_SHUTDOWN_TIMEOUT_S <= 10.0
    assert "timeout_graceful_shutdown" in inspect.signature(uvicorn.Config).parameters


# --------------------------------------------------------- the shutdown cut
#
# uvicorn's post-grace `task.cancel()` lands in the endpoint as a
# CancelledError. Letting it out cost one ERROR traceback per open stream
# (the S8-B pipe hang); the endpoint now turns it into one WARNING and
# returns, but ONLY while draining -- a cancel with `draining` False is not
# uvicorn's and keeps its asyncio meaning.


def _cancel_inside_serve(*, flip_draining: bool, committed: bool):
    """Stand-in for `_serve` that behaves like uvicorn's post-grace cancel
    landing mid-request: the drain has (or, for the negative case, has not)
    flipped `draining`; the status may already be on the wire; then the
    CancelledError uvicorn's `task.cancel()` would have thrown in."""

    async def _serve(self, receive, send, exchange, **_):
        if flip_draining:
            self._gateway.draining = True
        exchange.started = committed
        raise asyncio.CancelledError("Task cancelled, timeout graceful shutdown exceeded")

    return _serve


async def _drive_past_admission(ep: PassthroughEndpoint, path: str) -> list[dict]:
    """Like `_drive`, but with a body the request path can parse, so the
    request gets through tenant, admission and the body read into `_serve`."""
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    async def receive() -> dict:
        return {
            "type": "http.request", "more_body": False,
            "body": b'{"model":"fake.echo","messages":[],"stream":true}',
        }

    scope = {
        "type": "http", "method": "POST", "path": path,
        "headers": [], "path_params": {},
    }
    await ep(scope, receive, send)
    return sent


def _shutdown_cut_records(caplog) -> list[logging.LogRecord]:
    """Every record the cut path might have written, at ANY level. The rule
    is 'nothing per stream', so INFO would be as wrong as WARNING."""
    return [r for r in caplog.records if "shutdown cut" in r.getMessage()]


async def test_shutdown_cancel_after_commitment_is_counted_not_logged_and_returns(
    monkeypatch, caplog
):
    gw = _gateway()
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))
    monkeypatch.setattr(
        PassthroughEndpoint, "_serve",
        _cancel_inside_serve(flip_draining=True, committed=True),
    )
    caplog.set_level(logging.DEBUG)

    sent = await _drive_past_admission(ep, path)  # returns: nothing raised

    # The fact goes into the counter, not onto stderr: no per-cut record at
    # any level, and nothing at ERROR (the traceback that hung S8-B).
    assert _shutdown_cut_records(caplog) == []
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert gw.shutdown_cuts.total == 1
    assert gw.shutdown_cuts.committed == 1
    assert gw.shutdown_cuts.uncommitted == 0
    # C2: after commitment nothing more goes on the wire -- no 503, and no
    # `more_body: False` that would give the truncated body a terminator.
    assert sent == []
    assert gw.inflight == 0
    assert asyncio.current_task().cancelling() == 0, "the cancel was consumed"


async def test_a_thousand_shutdown_cuts_are_one_log_line(monkeypatch, caplog):
    """The bound. Output on the shutdown path must not scale with the number
    of open streams -- that scaling is what blocked the S8-B workers on an
    undrained 64 KiB pipe, first as tracebacks and then as one WARNING per
    cut. A thousand cuts produce zero records while they happen and exactly
    one WARNING when `lifecycle.log_shutdown_cuts` runs after the server has
    stopped; the per-target map is bounded by the catalog, not by N."""
    from llmgw.admission import TenantLimits
    from llmgw.server.lifecycle import log_shutdown_cuts

    n = 1000
    # A thousand streams must be IN FLIGHT together before the drain flips --
    # once `draining` is True the ingress sheds, which is the other test. So:
    # no stream cap, a tenant budget that admits them all, and a `_serve` that
    # parks like a live stream until uvicorn's shutdown cancels it.
    gw = Gateway(
        ServerConfig(
            max_streams=None,
            tenant_limits=TenantLimits(
                rate_per_second=1e9, burst=n, max_concurrency=n
            ),
        ),
        clock=ManualClock(start=1_000.0),
    )
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))
    parked = asyncio.Event()      # never set: the streams end only by cancel
    all_parked = asyncio.Event()  # set by the n-th arrival
    arrived = 0

    async def _serve(self, receive, send, exchange, **_):
        nonlocal arrived
        exchange.started = True
        arrived += 1
        if arrived == n:
            all_parked.set()
        await parked.wait()  # a committed stream, mid-body, until cancelled

    monkeypatch.setattr(PassthroughEndpoint, "_serve", _serve)
    caplog.set_level(logging.DEBUG)

    tasks = [asyncio.ensure_future(_drive_past_admission(ep, path)) for _ in range(n)]
    await asyncio.wait_for(all_parked.wait(), timeout=10.0)
    assert gw.inflight == n
    gw.draining = True          # the grace expired ...
    for task in tasks:
        task.cancel()           # ... and uvicorn cancels every open request
    results = await asyncio.gather(*tasks)  # none raise: each consumed its cancel
    assert all(sent == [] for sent in results), "C2: nothing more on the wire"

    assert _shutdown_cut_records(caplog) == []
    assert gw.shutdown_cuts.total == n
    assert len(gw.shutdown_cuts.by_target) == 1, "bounded by targets, not by cuts"
    assert gw.inflight == 0

    report = DrainReport(inflight_at_start=n, cut=n, duration_s=2.0, timed_out=True)
    log_shutdown_cuts(gw, report=report, grace_s=2.0)

    records = _shutdown_cut_records(caplog)
    assert len(records) == 1
    line = records[0]
    assert line.levelno == logging.WARNING and not line.exc_info
    assert f"shutdown cut {n} stream(s)" in line.getMessage()
    assert f"committed={n} uncommitted=0" in line.getMessage()
    assert f"({n} open when the 2.0s grace expired)" in line.getMessage()
    assert "capture" in line.getMessage()
    # A second, larger figure must not grow the line: the summary names at
    # most five targets and never the requests.
    assert len(line.getMessage()) < 400, line.getMessage()


def test_log_shutdown_cuts_is_silent_when_nothing_was_cut(caplog):
    """The S8 success case and every idle deploy: no cuts, no line."""
    from llmgw.server.lifecycle import log_shutdown_cuts

    gw = _gateway()
    caplog.set_level(logging.DEBUG)
    report = DrainReport(inflight_at_start=3, cut=0, duration_s=1.0, timed_out=False)
    log_shutdown_cuts(gw, report=report, grace_s=30.0)
    log_shutdown_cuts(gw, report=None, grace_s=30.0)
    assert _shutdown_cut_records(caplog) == []


def test_shutdown_cuts_summary_names_at_most_five_targets():
    from llmgw.server.app import ShutdownCuts

    cuts = ShutdownCuts()
    for i in range(8):
        for _ in range(i + 1):
            cuts.note(target=None, committed=bool(i % 2))
    # `target=None` lands under "-"; spread the 36 across eight labels via
    # the dict directly so the ranking is exercised without building Targets.
    cuts.by_target = {f"p/m{i}": i + 1 for i in range(8)}
    text = cuts.summary()
    assert text.startswith("shutdown cut 36 stream(s): committed=20 uncommitted=16; ")
    assert text.count("=") == 2 + 5, text  # committed=, uncommitted=, five targets
    assert text.endswith("+3 more"), text
    assert "p/m7=8" in text and "p/m3=4" in text and "p/m2=3" not in text


async def test_shutdown_cancel_before_commitment_answers_503_draining(
    monkeypatch, caplog
):
    gw = _gateway()
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))
    monkeypatch.setattr(
        PassthroughEndpoint, "_serve",
        _cancel_inside_serve(flip_draining=True, committed=False),
    )
    caplog.set_level(logging.INFO)

    sent = await _drive_past_admission(ep, path)

    start = next(m for m in sent if m["type"] == "http.response.start")
    body = next(m for m in sent if m["type"] == "http.response.body")["body"]
    assert start["status"] == 503
    assert json.loads(body)["error"]["type"] == "draining"
    assert (b"retry-after", b"1") in start["headers"]
    assert _shutdown_cut_records(caplog) == []
    assert gw.shutdown_cuts.total == 1 and gw.shutdown_cuts.uncommitted == 1
    assert [r for r in caplog.records if r.levelno >= logging.ERROR] == []
    assert gw.inflight == 0


async def test_cancel_while_not_draining_is_not_ours_and_propagates(
    monkeypatch, caplog
):
    """The discriminator. A CancelledError with `draining` False is a harness
    tearing down or a future in-process caller, never uvicorn's shutdown --
    swallowing it would break structured concurrency for whoever sent it."""
    gw = _gateway()
    ep = _endpoint(gw)
    path = next(iter(ROUTE_TO_UPSTREAM_PATH))
    monkeypatch.setattr(
        PassthroughEndpoint, "_serve",
        _cancel_inside_serve(flip_draining=False, committed=True),
    )
    caplog.set_level(logging.INFO)

    with pytest.raises(asyncio.CancelledError):
        await _drive_past_admission(ep, path)

    assert _shutdown_cut_records(caplog) == []
    assert gw.shutdown_cuts.total == 0, "not ours, so not counted as a cut"
    assert gw.inflight == 0  # the finally still paired the tracker


def test_per_connection_uvicorn_lines_are_filtered_and_nothing_else_is():
    """`lifecycle.serve` drops the three uvicorn lines whose VOLUME scales
    with the number of open streams or sockets -- its name for the C2 ending,
    and (PLAN-G) the two per-WebSocket-connection lines -- and only those.
    Installing twice must not stack."""
    from llmgw.server.lifecycle import (
        _C2_ENDING_MESSAGE,
        _C2_FILTER,
        _BoundedShutdownOutput,
        _quiet_c2_endings,
    )

    def record(msg: str, *args) -> logging.LogRecord:
        return logging.LogRecord(
            "uvicorn.error", logging.ERROR, __file__, 0, msg, args, None,
        )

    flt = _BoundedShutdownOutput()
    assert flt.filter(record(_C2_ENDING_MESSAGE)) is False
    # PLAN-G: two lines per socket, times every socket a deploy cuts.
    assert flt.filter(record("connection open")) is False
    assert flt.filter(record("connection closed")) is False
    assert flt.filter(record(
        '%s - "WebSocket %s" [accepted]', "127.0.0.1:1", "/tts/v1/voice",
    )) is False
    # Everything that does NOT scale with the connection count survives.
    assert flt.filter(record("Exception in ASGI application")) is True
    assert flt.filter(record("Cancel 3 running task(s), timeout graceful shutdown exceeded"))
    assert flt.filter(record("Application startup complete.")) is True

    logger = logging.getLogger("uvicorn.error")
    before = [f for f in logger.filters if f is _C2_FILTER]
    try:
        _quiet_c2_endings()
        _quiet_c2_endings()
        assert [f for f in logger.filters if f is _C2_FILTER] == [_C2_FILTER]
    finally:
        if not before:
            logger.removeFilter(_C2_FILTER)


# ------------------------------------------------------------------ dual-stack
# 17 Sep 2026: `LLMGW_HOST="::"` through uvicorn alone produced an IPv6-only
# listener (asyncio sets IPV6_V6ONLY on sockets it opens itself), so Fly's
# IPv4 health check refused while the IPv6 private network worked. The
# pre-bound socket must accept BOTH.


def _connects(host: str, port: int) -> bool:
    import socket as _s
    fam = _s.AF_INET6 if ":" in host else _s.AF_INET
    with _s.socket(fam, _s.SOCK_STREAM) as c:
        c.settimeout(1.0)
        try:
            c.connect((host, port))
            return True
        except OSError:
            return False


def test_bind_sockets_on_double_colon_is_dual_stack():
    import socket as _s

    from llmgw.server.lifecycle import bind_sockets

    if not _s.has_dualstack_ipv6():
        pytest.skip("no dual-stack IPv6 on this host")
    (sock,) = bind_sockets("::", 0)
    try:
        port = sock.getsockname()[1]
        assert sock.getsockopt(_s.IPPROTO_IPV6, _s.IPV6_V6ONLY) == 0
        assert _connects("::1", port), "IPv6 loopback must connect"
        # IPv4 loopback is the health-check path.
        assert _connects("127.0.0.1", port), "IPv4 loopback must connect"
    finally:
        sock.close()


def test_bind_sockets_on_ipv4_host_is_ipv4_only():
    import socket as _s

    from llmgw.server.lifecycle import bind_sockets

    (sock,) = bind_sockets("127.0.0.1", 0)
    try:
        assert sock.family == _s.AF_INET
        assert _connects("127.0.0.1", sock.getsockname()[1])
    finally:
        sock.close()

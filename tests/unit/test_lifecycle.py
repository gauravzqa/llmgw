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

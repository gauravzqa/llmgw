"""The five defects the G1 review found, each with the test that would have
caught it.

Every one of these is a behaviour the contract tier cannot reach cheaply: a
three-second provider delay, a credential fragment inside a provider's error
text, a close code that is counted rather than sent, a buffer that drains
during the stall window, a context the provider refuses. They are all pure
functions of the relay's marks and one frame, so they belong here, on a
`ManualClock`, next to the budgets they are about.
"""

from __future__ import annotations

import asyncio
import json

from llmgw.clocks import Budgets, ManualClock
from llmgw.ws.errors import CloseCode
from llmgw.ws.relay import Direction, Relay
from llmgw.ws.surfaces.base import Frame, FrameClass
from llmgw.ws.surfaces.inworld_tts import (
    INWORLD_TTS_WS,
    MAX_CLIENT_BUFFER_DELAY_S,
    SCRUBBED_MESSAGE,
)

BUDGETS = Budgets(
    total=120.0, connect=5.0, headers=10.0, first_event=3.0, progress=5.0,
    client_stall=10.0, session_total=3600.0, idle=600.0,
)


def tick(clock: ManualClock, seconds: float) -> None:
    """Move a `ManualClock` without its async drain -- `Relay._check` has no
    sleepers, so these tests need no event loop. Same helper as
    `test_ws_clocks.py`, and for the same reason."""
    clock._now += seconds


class _NullSide:
    close_code: int | None = None
    close_reason = ""

    async def recv(self):  # pragma: no cover - never read in these tests
        await asyncio.Event().wait()

    async def send(self, frame):  # pragma: no cover - never written
        return None

    async def close(self, code, reason):  # pragma: no cover
        return None


def _relay(clock: ManualClock, *, scrub: bool = False) -> Relay:
    return Relay(
        surface=INWORLD_TTS_WS, client=_NullSide(), upstream=_NullSide(),
        budgets=BUDGETS, clock=clock, product="inworld", scrub_errors=scrub,
    )


def _create(context: str = "c1", *, delay_ms: object = 3000) -> Frame:
    create: dict = {"modelId": "inworld-tts-2-flash", "autoMode": True,
                    "bufferCharThreshold": 120}
    if delay_ms is not None:
        create["maxBufferDelayMs"] = delay_ms
    return Frame.of(json.dumps({"create": create, "contextId": context}))


def _send_text(context: str = "c1") -> Frame:
    return Frame.of(json.dumps({"send_text": {"text": "Hi."}, "contextId": context}))


# ==========================================================================
# B1. Declared buffering is not a stall
# ==========================================================================


def test_a_context_gets_the_buffering_delay_its_create_declared():
    """The blocker. The LiveKit plugin sends `maxBufferDelayMs: 3000` and
    does not flush until the whole reply is tokenised, so a short first
    sentence is legitimately silent for three seconds. A flat `first_event`
    of 3 s would close that healthy turn; 3 s + the declared 3 s does not."""
    clock = ManualClock(1_000.0)
    r = _relay(clock)
    r._observe(Direction.CLIENT_IN, _create(delay_ms=3000))
    r._observe(Direction.CLIENT_IN, _send_text())

    tick(clock, 5.0)  # past first_event alone, inside first_event + delay
    assert r._check()[0] is None

    tick(clock, 1.5)  # past 3 + 3
    verdict = r._check()[0]
    assert verdict is not None
    assert verdict.code is CloseCode.PROVIDER_STALL


def test_a_client_that_declares_no_buffering_gets_the_plain_budget():
    clock = ManualClock(1_000.0)
    r = _relay(clock)
    r._observe(Direction.CLIENT_IN, _create(delay_ms=None))
    r._observe(Direction.CLIENT_IN, _send_text())
    tick(clock, 3.5)
    verdict = r._check()[0]
    assert verdict is not None
    assert verdict.code is CloseCode.PROVIDER_STALL


def test_the_declared_delay_is_per_context_and_capped():
    """Two contexts on one socket declare different delays, and neither can
    buy an unbounded timeout: the value comes from the client."""
    clock = ManualClock(1_000.0)
    r = _relay(clock)
    r._observe(Direction.CLIENT_IN, _create("a", delay_ms=3000))
    r._observe(Direction.CLIENT_IN, _create("b", delay_ms=None))
    r._observe(Direction.CLIENT_IN, _send_text("a"))
    r._observe(Direction.CLIENT_IN, _send_text("b"))
    assert r._units["a"].buffer_delay == 3.0
    assert r._units["b"].buffer_delay == 0.0

    huge = _relay(ManualClock(0.0))
    huge._observe(Direction.CLIENT_IN, _create("z", delay_ms=10**9))
    assert huge._buffer_delay["z"] == MAX_CLIENT_BUFFER_DELAY_S


def test_a_hostile_buffer_delay_is_ignored_rather_than_trusted():
    for value in ("soon", True, -5, 0, {"ms": 10}):
        r = _relay(ManualClock(0.0))
        r._observe(Direction.CLIENT_IN, _create("c", delay_ms=value))
        assert r._buffer_delay.get("c", 0.0) == 0.0, value


# ==========================================================================
# B2. A provider's error text can carry OUR credential
# ==========================================================================


def _error_frame(code: int = 7) -> Frame:
    return Frame.of(json.dumps({"error": {
        "code": code,
        "message": 'Invalid credentials provided for API key "ab12***"',
        "status": "UNAUTHENTICATED",
        "details": [{"reason": "SESSION_TOKEN_INVALID"}],
    }}))


def test_a_scrubbed_error_keeps_its_shape_and_loses_only_the_prose():
    out = INWORLD_TTS_WS.scrub_error_frame(_error_frame())
    payload = out.payload()
    assert payload is not None
    assert payload["error"]["code"] == 7
    assert payload["error"]["status"] == "UNAUTHENTICATED"
    assert payload["error"]["message"] == SCRUBBED_MESSAGE
    assert "details" not in payload["error"]
    assert "ab12" not in out.wire()


def test_the_relay_scrubs_only_when_the_provider_row_says_to():
    clock = ManualClock(0.0)
    scrubbing = _relay(clock, scrub=True)
    relayed = scrubbing._observe(Direction.CLIENT_OUT, _error_frame())
    assert relayed is not None and "ab12" not in relayed.wire()

    passing = _relay(clock, scrub=False)
    relayed = passing._observe(Direction.CLIENT_OUT, _error_frame())
    assert relayed is not None and "ab12" in relayed.wire()


def test_an_unreadable_error_frame_is_relayed_rather_than_dropped():
    """It got here classified as an error. A client that receives nothing is
    worse off than one that receives bytes the gateway could not parse."""
    frame = Frame.of("not json at all")
    assert INWORLD_TTS_WS.scrub_error_frame(frame) is frame


# ==========================================================================
# S1. The stall clock must be disarmed before it is read
# ==========================================================================


def test_a_client_that_caught_up_is_not_closed_for_stalling():
    """The first way to get this wrong: test the deadline against a mark set
    when the buffer filled, and a client that has since drained it is closed
    4903 anyway -- upstream goes quiet, nothing wakes the watchdog, and the
    next tick condemns a healthy session."""
    clock = ManualClock(1_000.0)
    r = _relay(clock)
    buf = r._buffers[Direction.CLIENT_OUT]
    big = Frame.of("x" * (buf.limit + 1))
    asyncio.run(buf.put(big))

    tick(clock, BUDGETS.client_stall - 0.1)
    buf.release(len(big))          # the client took it...
    r._last_client_send = clock.now()   # ...which is what a returned send means
    tick(clock, 0.2)               # past the deadline the old mark had set

    assert r._check()[0] is None


def test_a_client_that_never_takes_anything_is_closed_4903():
    """The second way: clear the mark whenever the buffer is not full at this
    instant, and a stalled client is never closed at all -- `get()` pops a
    frame before the send blocks, so occupancy flickers under the ceiling on
    every pass and the clock restarts each time. Nothing is sent here, so
    nothing moves `_last_client_send`."""
    clock = ManualClock(1_000.0)
    r = _relay(clock)
    buf = r._buffers[Direction.CLIENT_OUT]
    asyncio.run(buf.put(Frame.of("x" * 16)))   # a trickle, far under the ceiling
    assert not buf.full

    tick(clock, BUDGETS.client_stall + 0.1)
    verdict = r._check()[0]
    assert verdict is not None
    assert verdict.code is CloseCode.CLIENT_STALL


def test_an_empty_outbound_buffer_never_stalls_however_long_it_is_quiet():
    clock = ManualClock(1_000.0)
    r = _relay(clock)
    tick(clock, BUDGETS.client_stall * 10)
    assert r._check()[0] is None


# ==========================================================================
# S5. A context the provider refused is not an open context
# ==========================================================================


def _status(code: int, context: str = "c6") -> Frame:
    return Frame.of(json.dumps({"result": {
        "contextId": context,
        "status": {"code": code, "message": "no"},
    }}))


def test_a_refused_context_is_forgotten_so_the_drain_does_not_wait_for_it():
    """Inworld refuses a sixth context with `status.code 8` and an unknown
    one with 5, and never sends `contextClosed` for either. Remembering them
    held a draining session open for its whole bound."""
    for code in (5, 8):
        clock = ManualClock(0.0)
        r = _relay(clock)
        r._observe(Direction.CLIENT_IN, _create("c6"))
        assert r.open_contexts == 1
        r._observe(Direction.CLIENT_OUT, _status(code))
        assert r.open_contexts == 0, code


def test_an_in_context_fault_that_is_not_fatal_leaves_the_context_open():
    """Code 3 is "that text was too long", not "there is no such context":
    the plugin reuses the context for the next sentence."""
    r = _relay(ManualClock(0.0))
    r._observe(Direction.CLIENT_IN, _create("c1"))
    r._observe(Direction.CLIENT_OUT, _status(3, "c1"))
    assert r.open_contexts == 1
    assert INWORLD_TTS_WS.classify_upstream(_status(3, "c1")).kind is FrameClass.ERROR


# ==========================================================================
# The production-config hole: a socket must work with only a token
# ==========================================================================


def test_the_surface_names_its_own_model_because_the_plugin_cannot():
    """Found by deploying G1 and watching a healthy client get 404.

    The upgrade resolves a plan before any frame arrives. Production's
    default workload is a chat model (`openai.gpt-4o-mini`), which has no
    target of this dialect, so the plan came back empty and the upgrade
    answered `ModelNotFound` -- 404 -- to a client doing nothing wrong. The
    consumer cannot route around it either: the LiveKit plugin builds its URL
    with `urljoin(ws_url, "/tts/v1/voice:streamBidirectional")` and `urljoin`
    discards any path prefix, so `/workloads/{w}/...` is unreachable from the
    one caller this plane exists for.
    """
    from llmgw.catalog import DEFAULT_CATALOG

    assert INWORLD_TTS_WS.default_model == "inworld.tts-2-flash"
    target = DEFAULT_CATALOG.resolve(INWORLD_TTS_WS.default_model)
    assert target.provider.id == "inworld", (
        "the default must live on the provider the socket opens to, or the "
        "first `create` frame refuses itself for crossing providers"
    )


def test_both_deprecated_inworld_ids_resolve_onto_that_provider():
    """The plugin sends `inworld-tts-1.5-mini` from Layrs' config and
    `inworld-tts-1.5-max` when a call site names no model at all (it is the
    plugin's own default). Either one arriving in a `create` frame must
    resolve, and onto the same provider as the socket."""
    from llmgw.catalog import DEFAULT_CATALOG

    for wire_id in ("inworld-tts-1.5-mini", "inworld-tts-1.5-max"):
        target = DEFAULT_CATALOG.resolve(wire_id)
        assert target.provider.id == "inworld", wire_id

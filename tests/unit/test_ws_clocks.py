"""The session clocks, the buffers, the rate bucket, and the entry-path bits.

`Relay._check` is a pure function of the clock and the relay's marks, which
is why it is a method with no awaits in it: every budget in PLAN-G 4.1 can
then be proved on a `ManualClock` with no real sleep and no socket, and the
contract tier only has to prove that the marks are set from real frames.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from llmgw import errors
from llmgw.bytebuf import ByteBuffer
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.clocks import Budgets, ManualClock
from llmgw.server.app import BEARER_ONLY, credential_token
from llmgw.server.config import DEFAULT_SURFACE_LIMITS, ServerConfig, SurfaceLimits
from llmgw.ws.errors import CloseCode
from llmgw.ws.relay import Direction, FrameBuffer, Relay, TokenBucket, _Unit
from llmgw.ws.surfaces.base import Frame
from llmgw.ws.surfaces.inworld_tts import INWORLD_TTS_WS

REPO_ROOT = Path(__file__).resolve().parents[2]

BUDGETS = Budgets(
    total=120.0, connect=5.0, headers=10.0, first_event=2.0, progress=5.0,
    client_stall=10.0, session_total=3600.0, idle=600.0,
)


class _NullSide:
    close_code: int | None = None
    close_reason = ""

    async def recv(self):  # pragma: no cover - the watchdog tests never read
        await asyncio.Event().wait()

    async def send(self, frame):  # pragma: no cover
        return None

    async def close(self, code, reason):  # pragma: no cover
        return None


def tick(clock: ManualClock, seconds: float) -> None:
    """Move a `ManualClock` forward without its async drain.

    `ManualClock.advance` is a coroutine because it has to wake sleepers and
    let them run. `Relay._check` has no sleepers: it is a pure read of the
    clock and the relay's marks, which is exactly why it is a separate method
    from the watchdog that calls it. So these tests move time and read the
    answer, with no event loop in the picture at all.
    """
    clock._now += seconds


def _relay(clock: ManualClock, budgets: Budgets = BUDGETS) -> Relay:
    return Relay(
        surface=INWORLD_TTS_WS, client=_NullSide(), upstream=_NullSide(),
        budgets=budgets, clock=clock, product="inworld",
    )


# ==========================================================================
# Budgets
# ==========================================================================


def test_the_two_session_clocks_default_to_off():
    """A policy written before the socket plane existed must keep loading,
    and must not acquire an idle timeout nobody configured."""
    b = Budgets(total=120.0).validate()
    assert b.session_total is None
    assert b.idle is None


def test_session_total_may_exceed_total_but_must_be_positive():
    """The whole point: a socket is not a request. A 3 h session under a
    120 s per-unit total is the normal shape, not a misconfiguration."""
    Budgets(total=120.0, session_total=10_800.0, idle=60.0).validate()
    with pytest.raises(ValueError, match="session_total"):
        Budgets(total=120.0, session_total=0.0).validate()
    with pytest.raises(ValueError, match="idle"):
        Budgets(total=120.0, idle=-1.0).validate()


def test_largest_total_ignores_session_total():
    """C26: a 3 h `stt_session` profile must not refuse startup behind a
    130 s grace. The drain hook is what ends a long session on deploy."""
    from llmgw.policy import PolicySnapshot

    snapshot = PolicySnapshot.single_target(
        "openai.gpt-4o-mini", catalog=DEFAULT_CATALOG,
        budgets=Budgets(total=100.0, session_total=10_800.0),
    )
    assert snapshot.largest_total() == 100.0


def test_a_policy_file_may_set_the_session_clocks_and_null_them():
    from llmgw.policy import PolicySnapshot

    snapshot = PolicySnapshot.from_toml("""
default_workload = "default"

[profiles.tts_session.budgets]
session_total = 3600
idle = 600
first_event = 2

[profiles.no_idle.budgets]
idle = 5.0

[workloads.default]
incumbent = "openai.gpt-4o-mini"
profile = "tts_session"
""", catalog=DEFAULT_CATALOG)
    profile = snapshot.profiles["tts_session"]
    assert profile.session_total == 3600.0
    assert profile.idle == 600.0
    assert profile.first_event == 2.0
    assert snapshot.workloads["default"].budgets.session_total == 3600.0


def test_the_shipped_tts_session_profile_sits_above_the_measured_latencies():
    """The G0 numbers: `create` -> `contextCreated` 326 ms, `send_text` ->
    first audio 370-455 ms, connect to 101 up to 1.4 s from Chennai. Every
    budget has to clear its measurement by the margin the HTTP profiles use,
    and the file is the source of truth for that."""

    from llmgw.policy import PolicySnapshot

    text = (REPO_ROOT / "config/workloads.example.toml").read_text()
    snapshot = PolicySnapshot.from_toml(text, catalog=DEFAULT_CATALOG)
    p = snapshot.profiles["tts_session"]
    assert p.connect >= 1.4 * 3
    assert p.headers >= 0.326 * 10
    assert p.first_event >= 0.455 * 4
    assert p.progress >= p.first_event
    assert p.session_total == 3600.0
    assert p.idle == 600.0


# ==========================================================================
# The watchdog
# ==========================================================================


def test_an_idle_socket_with_nothing_open_sleeps_until_its_idle_budget():
    """S10's shape: two thousand idle sockets must cost two thousand timer
    entries, not two thousand wakeups a second."""
    clock = ManualClock()
    relay = _relay(clock)
    verdict, sleep_for = relay._check()
    assert verdict is None
    assert sleep_for == pytest.approx(600.0)


def test_idle_fires_at_4906_and_blames_the_client():
    clock = ManualClock()
    relay = _relay(clock)
    tick(clock, 600.1)
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.IDLE
    assert verdict.reason == "llmgw:session_idle"
    assert verdict.error.blame is errors.Blame.CLIENT
    assert verdict.error.health is errors.Health.NEUTRAL


def test_session_total_fires_at_4901():
    clock = ManualClock()
    relay = _relay(clock)
    relay._last_activity = clock.now() + 10_000  # never idle
    tick(clock, 3600.1)
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.SESSION_TOTAL
    assert isinstance(verdict.error, errors.TotalDeadlineExceeded)


def test_the_handshake_budget_runs_from_the_first_config_frame_not_the_101():
    """Captures probes 2b and 3: 101 then silence is a HEALTHY socket on this
    provider whatever the credential, so a handshake clock started at the 101
    would fire on every idle connection."""
    clock = ManualClock()
    relay = _relay(clock)
    tick(clock, 30.0)
    assert relay._check()[0] is None, "silence before a create is not a failure"

    relay._handshake_at = clock.now()
    tick(clock, 10.1)
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.UPSTREAM_HANDSHAKE
    assert isinstance(verdict.error, errors.HeadersTimeout)
    assert verdict.reason == "llmgw:headers_timeout"


def test_first_event_and_progress_are_per_context_and_different_budgets():
    clock = ManualClock()
    relay = _relay(clock)
    relay._units["ctx-A"] = _Unit(awaiting=True, got_content=False, last_at=clock.now())
    tick(clock, 1.9)
    assert relay._check()[0] is None
    tick(clock, 0.2)
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.PROVIDER_STALL
    assert isinstance(verdict.error, errors.FirstEventTimeout)
    assert "ctx-A" in verdict.error.message

    relay._units["ctx-A"] = _Unit(awaiting=True, got_content=True, last_at=clock.now())
    tick(clock, 4.9)
    assert relay._check()[0] is None, "progress is the looser budget once audio flows"
    tick(clock, 0.2)
    assert isinstance(relay._check()[0].error, errors.StallTimeout)


def test_a_context_that_is_not_awaiting_output_has_no_clock():
    """An Inworld context between utterances is idle by design -- the plugin
    keeps one open across a whole turn. Clocking it would close every
    conversation with a pause in it."""
    clock = ManualClock()
    relay = _relay(clock)
    relay._units["ctx-A"] = _Unit(awaiting=False, got_content=True, last_at=clock.now())
    tick(clock, 500.0)
    relay._last_activity = clock.now()
    assert relay._check()[0] is None


def test_client_stall_needs_the_buffer_full_for_the_whole_budget():
    clock = ManualClock()
    relay = _relay(clock)
    out = relay._buffers[Direction.CLIENT_OUT]
    out._size = out.limit  # the client has stopped reading
    assert relay._check()[0] is None, "full is not yet stalled"
    tick(clock, 10.1)
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.CLIENT_STALL
    assert isinstance(verdict.error, errors.ClientTooSlow)
    assert verdict.error.blame is errors.Blame.CLIENT


def test_a_client_that_starts_reading_again_clears_the_stall_clock():
    clock = ManualClock()
    relay = _relay(clock)
    out = relay._buffers[Direction.CLIENT_OUT]
    out._size = out.limit
    relay._check()
    tick(clock, 9.0)
    out._size = 0
    relay._check()
    tick(clock, 9.0)
    assert relay._check()[0] is None


def test_drain_closes_4900_the_moment_the_last_context_closes():
    clock = ManualClock()
    relay = _relay(clock)
    relay._contexts.add("ctx-A")
    relay.drain(deadline=clock.now() + 20.0)
    assert relay._check()[0] is None, "a live context is waited for"
    relay._contexts.discard("ctx-A")
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.DRAINING
    assert verdict.reason == "llmgw:session_draining"
    assert verdict.error.outcome is errors.Outcome.CANCELED


def test_drain_gives_up_on_open_contexts_at_the_deadline():
    clock = ManualClock()
    relay = _relay(clock)
    relay._contexts.add("ctx-A")
    relay.drain(deadline=clock.now() + 20.0)
    tick(clock, 20.1)
    verdict, _ = relay._check()
    assert verdict.code is CloseCode.DRAINING
    assert "1 context(s) still open" in verdict.error.message


def test_a_drain_that_beats_the_relay_is_applied_when_it_arrives():
    """A socket that arrived one millisecond before SIGTERM must not be
    missed by the drain loop that ran one millisecond earlier."""
    from llmgw.ws.session import Session

    clock = ManualClock()

    class _Cfg:
        ws_drain_wait_s = 20.0

    class _Gw:
        config = _Cfg()

    session = Session(
        _Gw(), INWORLD_TTS_WS, tenant="t", workload_id="w", plan=None,
        snapshot=None, budgets=BUDGETS, clock=clock,
    )
    session.drain()
    relay = _relay(clock)
    session.attach(relay)
    assert relay._drain_at is not None
    assert relay._check()[0].code is CloseCode.DRAINING


# ==========================================================================
# Buffers and rate
# ==========================================================================


async def test_the_frame_buffer_blocks_when_full_and_never_splits():
    buffer = FrameBuffer(100)
    await buffer.put(Frame(b"x" * 60))
    await buffer.put(Frame(b"y" * 60))
    assert buffer.size == 120 and buffer.full
    assert buffer.high_water == 120

    waiter = asyncio.create_task(buffer.put(Frame(b"z" * 10)))
    await asyncio.sleep(0)
    assert not waiter.done(), "a full buffer is backpressure, not a drop"

    first = await buffer.get()
    assert first.data == b"x" * 60, "whole frames, in order"
    buffer.release(60)
    await asyncio.wait_for(waiter, 1.0)


async def test_aborting_the_frame_buffer_releases_a_blocked_producer():
    buffer = FrameBuffer(10)
    await buffer.put(Frame(b"x" * 20))
    waiter = asyncio.create_task(buffer.put(Frame(b"y")))
    await asyncio.sleep(0)
    buffer.abort()
    await asyncio.wait_for(waiter, 1.0)
    assert buffer.size == 0


async def test_the_byte_buffer_move_kept_its_behaviour():
    """`bytebuf.ByteBuffer` is `pump._ByteBuffer` moved, not rewritten: the
    split-oversized-chunk rule is the property the pump's framing depends on,
    and it is the one property the relay could NOT reuse (a split frame is
    corruption, not a smaller frame)."""
    buffer = ByteBuffer(4)
    producer = asyncio.create_task(buffer.put(b"abcdefgh"))
    await asyncio.sleep(0)
    assert buffer.size == 4, "the ceiling holds at every instant"
    assert await buffer.get() == b"abcd", "the chunk was SPLIT, not admitted whole"
    buffer.release(4)
    await asyncio.wait_for(producer, 1.0)
    assert await buffer.get() == b"efgh"


def test_pump_imports_the_moved_buffer_rather_than_owning_one():
    import llmgw.pump as pump

    assert pump.ByteBuffer is ByteBuffer
    assert not hasattr(pump, "_ByteBuffer")


def test_the_token_bucket_smooths_rather_than_clips():
    clock = ManualClock()
    bucket = TokenBucket(1000, clock=clock)
    assert bucket.deficit(1000) == 0.0
    assert bucket.deficit(500) == pytest.approx(0.5)
    tick(clock, 0.5)
    assert bucket.deficit(500) == 0.0


def test_a_bucket_with_no_rate_never_waits():
    bucket = TokenBucket(None, clock=ManualClock())
    assert bucket.deficit(10_000_000) == 0.0


def test_the_shipped_byte_rates_are_sized_from_the_medium():
    """16 kHz PCM16 is 32 KB/s and base64 inflates by a third; a second of
    24 kHz LINEAR16 TTS audio is 48 KB."""
    limits = DEFAULT_SURFACE_LIMITS["inworld_tts_ws"]
    assert limits.max_in_bps == 16 * 1024
    assert limits.max_out_bps == 128 * 1024 > 48 * 1024 * 2


def test_surface_limits_resolve_the_rates_without_inheriting_the_globals():
    """A byte CAP inherits the global; a byte RATE does not exist globally,
    so `None` means "no rate bound" rather than "the global one"."""
    resolved = SurfaceLimits(max_in_bps=99).resolved(request=10, response=20)
    assert resolved.max_request_bytes == 10
    assert resolved.max_in_bps == 99
    assert resolved.max_out_bps is None


# ==========================================================================
# The entry path's new pieces
# ==========================================================================


def _scope(value: str | None):
    headers = [] if value is None else [(b"authorization", value.encode())]
    return {"headers": headers}


def test_basic_is_accepted_only_where_a_route_asked_for_it():
    assert credential_token(_scope("Basic tok"), schemes=frozenset({"basic"})) == "tok"
    assert credential_token(_scope("Basic tok"), schemes=BEARER_ONLY) is None
    assert credential_token(_scope("Bearer tok"), schemes=BEARER_ONLY) == "tok"


def test_the_basic_token_is_not_base64_decoded():
    """The tenant token is a gateway-issued opaque string; the plugin wraps
    it in `Basic ` because that is what it does with its provider key.
    Decoding it would turn a valid token into a lookup miss."""
    token = credential_token(
        _scope("Basic bm90LWJhc2U2NA=="), schemes=frozenset({"basic"}),
    )
    assert token == "bm90LWJhc2U2NA=="


def test_a_bare_credential_needs_the_raw_scheme():
    """AssemblyAI's plugin sends the key with no scheme word at all."""
    assert credential_token(_scope("justthekey"), schemes=frozenset({"raw"})) == "justthekey"
    assert credential_token(_scope("justthekey"), schemes=frozenset({"basic"})) is None


def test_the_scheme_is_case_insensitive_and_an_empty_token_is_no_token():
    assert credential_token(_scope("bAsIc tok"), schemes=frozenset({"basic"})) == "tok"
    assert credential_token(_scope("Basic   "), schemes=frozenset({"basic"})) is None
    assert credential_token(_scope(None), schemes=frozenset({"basic"})) is None


def test_a_route_may_not_invent_a_scheme():
    with pytest.raises(ValueError, match="unknown tenant auth scheme"):
        credential_token(_scope("X tok"), schemes=frozenset({"digest"}))


def test_bearer_token_is_unchanged_by_the_widening():
    from llmgw.server.app import bearer_token

    assert bearer_token(_scope("Bearer tok")) == "tok"
    assert bearer_token(_scope("Basic tok")) is None


# ==========================================================================
# Drain arithmetic
# ==========================================================================


def test_the_ws_drain_wait_must_fit_inside_the_grace():
    from llmgw.policy import PolicySnapshot

    snapshot = PolicySnapshot.single_target(
        "openai.gpt-4o-mini", catalog=DEFAULT_CATALOG, budgets=Budgets(total=100.0),
    )
    ok = ServerConfig(drain_grace_seconds=130.0, ws_drain_wait_s=20.0)
    ok.check_drain_arithmetic(snapshot)
    bad = ServerConfig(drain_grace_seconds=10.0, ws_drain_wait_s=20.0)
    with pytest.raises(ValueError, match="ws_drain_wait_s"):
        bad.check_drain_arithmetic(snapshot)


def test_allow_short_covers_the_ws_inequality_too():
    """The bench deliberately drains shorter than the work it is serving in
    order to observe the cut; it is the same decision the first inequality's
    escape hatch already covers."""
    from llmgw.policy import PolicySnapshot

    cfg = ServerConfig(
        drain_grace_seconds=8.0, ws_drain_wait_s=20.0, drain_allow_short=True,
        budgets=Budgets(total=5.0, connect=1.0, first_event=2.0, progress=3.0),
    )
    cfg.check_drain_arithmetic(PolicySnapshot.single_target(
        "openai.gpt-4o-mini", catalog=DEFAULT_CATALOG,
        budgets=Budgets(total=5.0, connect=1.0, first_event=2.0, progress=3.0),
    ))


def test_ws_drain_wait_must_be_positive():
    with pytest.raises(ValueError, match="ws_drain_wait_s"):
        ServerConfig(ws_drain_wait_s=0.0).validated()


def test_a_long_session_profile_is_reported_and_not_refused(caplog):
    """C26: a session longer than the grace is normal. The operator should
    learn that a deploy will cut mid-session sockets; they should not be
    refused a startup for it."""
    from llmgw.policy import PolicySnapshot

    snapshot = PolicySnapshot.from_toml("""
default_workload = "default"

[defaults.budgets]
total = 100.0

[profiles.stt_session.budgets]
session_total = 10800

[workloads.default]
incumbent = "openai.gpt-4o-mini"
""", catalog=DEFAULT_CATALOG)
    with caplog.at_level("INFO", logger="llmgw.server.config"):
        ServerConfig(drain_grace_seconds=130.0).check_drain_arithmetic(snapshot)
    assert any("session_total=10800" in r.getMessage() for r in caplog.records)

"""Capture tests: the sink is slow, remote and flaky, and the request path
must not notice.

Everything here is pure unit -- a ManualClock, in-memory sink doubles, and no
real sleeping. The autouse fixture asserts the one thing capture can leak that
its own assertions would not: the drain worker task. A leaked drainer holds a
reference to the queue forever and is exactly the failure aclose() exists to
prevent, so the fixture proves it rather than trusting it.

`asyncio_mode = "auto"` (see pyproject) means a bare `async def test_*` is
collected as an asyncio test -- no per-test marker needed.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from llmgw.capture import (
    DROP_REASONS,
    Capture,
    CaptureRecord,
    FileSink,
    NullSink,
)
from llmgw.clocks import ManualClock

# ============================================================== fixtures


@pytest.fixture(autouse=True)
async def no_task_leaks():
    """Every test returns the loop to its baseline task count.

    A capture that leaks its worker looks fine in the test that built it and
    is a slow leak in production: the task pins the queue and the sink for the
    life of the process. Structured shutdown is supposed to make that
    impossible; this is what proves it.
    """
    baseline = len(asyncio.all_tasks())
    yield
    for _ in range(50):
        if len(asyncio.all_tasks()) <= baseline:
            break
        await asyncio.sleep(0)
    assert len(asyncio.all_tasks()) == baseline, "capture left a task behind"


def make_record(rid: str = "req-1", **overrides) -> CaptureRecord:
    fields = dict(
        request_id=rid,
        tenant_id="tenant-a",
        workload_id="wl-1",
        provider="openai",
        model="gpt-4o",
        outcome="completed",
        attempts=1,
        tokens={"input": 12, "output": 40},
        cost_usd=0.0021,
        basis="exact",
        committed=True,
        first_event_latency=0.031,
        duration_s=1.2,
        error_code=None,
    )
    fields.update(overrides)
    return CaptureRecord(**fields)


# ============================================================== sink doubles


class ListSink:
    """Records every chunk in order. The 'sink works' double."""

    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)


class StalledSink:
    """A write that never completes until released. Models the slow/remote sink
    the whole contract is about -- used to prove offer() does not wait on it."""

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()

    async def write(self, chunk: bytes) -> None:
        self.entered.set()
        await self.gate.wait()


class FailingSink:
    """Raises on write. Models the flaky sink -- proves the error is swallowed
    and counted, and the worker survives to drain the next record."""

    def __init__(self, fail_times: int = 1) -> None:
        self.fail_times = fail_times
        self.written: list[bytes] = []

    async def write(self, chunk: bytes) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("sink is down")
        self.written.append(chunk)


# ============================================================== 6. serialization


def test_to_bytes_is_one_json_line_that_round_trips():
    rec = make_record()
    chunk = rec.to_bytes()

    assert chunk.endswith(b"\n"), "records must be newline-delimited for the log"
    assert chunk.count(b"\n") == 1, "exactly one line per record"

    back = json.loads(chunk)
    assert back["request_id"] == "req-1"
    assert back["tokens"] == {"input": 12, "output": 40}
    assert back["basis"] == "exact"
    assert back["error_code"] is None


def test_nbytes_is_what_the_bound_counts():
    rec = make_record()
    assert rec.nbytes == len(rec.to_bytes())


# ============================================================== 1. bounded by bytes


async def test_queue_is_bounded_by_bytes_not_records():
    one = make_record().to_bytes()
    size = len(one)
    # Budget for exactly three records; the fourth must not fit.
    cap = Capture(NullSink(), max_queue_bytes=size * 3, clock=ManualClock())

    assert cap.offer(make_record("r1")) is True
    assert cap.offer(make_record("r2")) is True
    assert cap.offer(make_record("r3")) is True
    # (records built the same way serialize to the same length)
    assert cap.queue_bytes <= size * 3

    # No worker running, so nothing drains: the fourth exceeds the budget.
    assert cap.offer(make_record("r4")) is False
    assert cap.offer(make_record("r5")) is False

    assert cap.dropped["queue_full"] == 2
    assert cap.queue_bytes <= cap.max_queue_bytes


async def test_oversized_record_is_rejected_even_into_an_empty_queue():
    tiny = make_record().nbytes // 2
    cap = Capture(NullSink(), max_queue_bytes=tiny, clock=ManualClock())

    # One 5-MB-style record must never be waved through a byte budget it blows,
    # even when the queue is empty.
    assert cap.offer(make_record()) is False
    assert cap.dropped["queue_full"] == 1
    assert cap.queue_bytes == 0


async def test_dropped_keys_are_exactly_the_metric_label_values():
    cap = Capture(NullSink(), max_queue_bytes=1024, clock=ManualClock())
    assert tuple(sorted(cap.dropped)) == tuple(sorted(DROP_REASONS))
    assert set(cap.dropped) == {"queue_full", "sink_error", "shutdown"}


# ============================================================== 2. non-blocking


async def test_offer_does_not_block_on_a_stalled_sink():
    sink = StalledSink()
    cap = Capture(sink, max_queue_bytes=1 << 20, clock=ManualClock())
    cap.start()

    # Let the worker pick up the first record and park inside sink.write.
    assert cap.offer(make_record("r1")) is True
    for _ in range(10):
        if sink.entered.is_set():
            break
        await asyncio.sleep(0)
    assert sink.entered.is_set(), "worker should be parked in the stalled write"

    # The sink is now stuck forever. offer() from the request path must still
    # return synchronously and promptly -- it never awaits the sink.
    loop = asyncio.get_running_loop()
    before = loop.time()
    for i in range(50):
        cap.offer(make_record(f"more-{i}"))
    after = loop.time()
    assert after - before < 0.05, "offer must not wait on the sink"

    # Clean up: release the sink and close without leaking the worker.
    sink.gate.set()
    await cap.aclose()


async def test_queue_bytes_never_exceeds_bound_while_sink_is_stalled():
    sink = StalledSink()
    rec_bytes = make_record().nbytes
    cap = Capture(sink, max_queue_bytes=rec_bytes * 2, clock=ManualClock())
    cap.start()

    # First record is pulled in-flight (parks the worker); it leaves the queue.
    cap.offer(make_record("r1"))
    for _ in range(10):
        if sink.entered.is_set():
            break
        await asyncio.sleep(0)

    # Flood: some queue, the rest drop. queue_bytes must stay within bound.
    for i in range(100):
        cap.offer(make_record(f"r{i}"))
        assert cap.queue_bytes <= cap.max_queue_bytes

    assert cap.dropped["queue_full"] > 0

    sink.gate.set()
    await cap.aclose()


# ============================================================== 3. drain order


async def test_records_reach_the_sink_in_order():
    sink = ListSink()
    cap = Capture(sink, max_queue_bytes=1 << 20, clock=ManualClock())
    cap.start()

    ids = [f"req-{i}" for i in range(6)]
    for rid in ids:
        assert cap.offer(make_record(rid)) is True

    await cap.aclose()

    got = [json.loads(c)["request_id"] for c in sink.chunks]
    assert got == ids
    assert cap.queue_bytes == 0


# ============================================================== 4. sink errors


async def test_sink_error_is_swallowed_and_counted_and_worker_survives():
    # First write raises; the worker must survive and drain the second record.
    sink = FailingSink(fail_times=1)
    cap = Capture(sink, max_queue_bytes=1 << 20, clock=ManualClock())
    cap.start()

    cap.offer(make_record("boom"))
    cap.offer(make_record("survivor"))

    await cap.aclose()

    assert cap.dropped["sink_error"] == 1
    # The worker did not die on the exception: the next record still landed.
    assert [json.loads(c)["request_id"] for c in sink.written] == ["survivor"]
    # Request path untouched: no queue_full, no shutdown drops.
    assert cap.dropped["queue_full"] == 0
    assert cap.dropped["shutdown"] == 0


# ============================================================== 5. aclose drains, no leak


async def test_aclose_drains_queued_records_without_leaking_a_task():
    sink = ListSink()
    cap = Capture(sink, max_queue_bytes=1 << 20, clock=ManualClock())

    baseline = len(asyncio.all_tasks())
    cap.start()
    assert len(asyncio.all_tasks()) == baseline + 1, "start() spawns one worker"

    for i in range(10):
        cap.offer(make_record(f"r{i}"))

    await cap.aclose()

    # Everything queued was flushed...
    assert len(sink.chunks) == 10
    assert cap.queue_bytes == 0
    # ...and the worker task is gone.
    for _ in range(50):
        if len(asyncio.all_tasks()) <= baseline:
            break
        await asyncio.sleep(0)
    assert len(asyncio.all_tasks()) == baseline, "aclose must not leak the worker"


async def test_aclose_on_a_stalled_sink_hard_stops_and_counts_shutdown():
    clock = ManualClock()
    sink = StalledSink()
    cap = Capture(sink, max_queue_bytes=1 << 20, clock=clock, drain_timeout=5.0)
    cap.start()

    for i in range(4):
        cap.offer(make_record(f"r{i}"))

    # Park the worker in the stalled write of the first record (r0 in flight,
    # r1..r3 still queued).
    for _ in range(10):
        if sink.entered.is_set():
            break
        await asyncio.sleep(0)
    assert sink.entered.is_set()

    # aclose cannot drain -- the sink is stuck forever -- so it must hard-stop
    # after the graceful window. Advancing the injected clock past drain_timeout
    # fires that bound deterministically, with no real sleeping.
    close = asyncio.ensure_future(cap.aclose())
    await asyncio.sleep(0)  # let aclose enter the timeout context
    await clock.advance(6.0)
    await close

    assert close.done() and close.exception() is None
    # The in-flight record plus the three still queued are all shutdown drops.
    assert cap.dropped["shutdown"] == 4
    assert cap.queue_bytes == 0
    # A hard stop is not a sink error: nothing here is the sink failing.
    assert cap.dropped["sink_error"] == 0

    sink.gate.set()  # unblock the (already cancelled) sink for good measure


async def test_file_sink_appends_lines_and_drains_through_capture(tmp_path):
    path = tmp_path / "capture.ndjson"
    cap = Capture(FileSink(path), max_queue_bytes=1 << 20, clock=ManualClock())
    cap.start()
    for i in range(3):
        cap.offer(make_record(f"req-{i}"))
    await cap.aclose()

    lines = path.read_bytes().splitlines()
    assert [json.loads(line)["request_id"] for line in lines] == [
        "req-0",
        "req-1",
        "req-2",
    ]


async def test_offer_after_close_is_dropped_as_shutdown():
    cap = Capture(NullSink(), max_queue_bytes=1 << 20, clock=ManualClock())
    cap.start()
    await cap.aclose()

    assert cap.offer(make_record()) is False
    assert cap.dropped["shutdown"] >= 1


async def test_aclose_cancelled_mid_wait_finishes_the_shutdown_quietly():
    """The forced-exit path (finding 48).

    A second SIGTERM makes uvicorn cancel the lifespan task while it is inside
    `gateway.shutdown()` -> `capture.aclose()` -> `await task`. Until 18 Sep
    2026 that `CancelledError` propagated out of the lifespan and every forced
    exit logged one ERROR traceback per process. `aclose()` must instead stop
    the worker, count the leftovers as `shutdown` drops, and return normally.
    """
    sink = StalledSink()
    clock = ManualClock()
    cap = Capture(sink, max_queue_bytes=1 << 20, clock=clock, drain_timeout=5.0)
    cap.start()
    for i in range(3):
        cap.offer(make_record(f"r{i}"))
    for _ in range(10):
        if sink.entered.is_set():
            break
        await asyncio.sleep(0)
    assert sink.entered.is_set()

    close = asyncio.ensure_future(cap.aclose())
    await asyncio.sleep(0)  # aclose is now parked on `await task`
    close.cancel()          # what uvicorn's force-exit does to the lifespan
    await close             # must NOT raise CancelledError

    assert close.done() and not close.cancelled() and close.exception() is None
    assert cap.dropped["shutdown"] == 3
    assert cap.queue_bytes == 0
    assert cap._task is None  # the worker is gone; the chaos tier's task baseline holds
    sink.gate.set()

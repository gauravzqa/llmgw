"""Capture: the high-cardinality per-request record sink.

Metrics answer "how many"; captures answer "which one." A Prometheus label set
is a closed vocabulary (see the top of `metrics.py`) because a per-request
identifier -- `request_id`, `tenant_id`, `workload_id` -- multiplied into a
label set is a cardinality explosion that takes the metrics backend, and then
the gateway, down with it. Those fields are exactly what you want when
investigating one request, so they live HERE: one structured JSON line per
request, written to a log you query later, never a series you scrape.

--------------------------------------------------------------------------
The one contract that defines this module (FAILURE-MODES row 9)
--------------------------------------------------------------------------

    Observability must not be able to take the system down.

The sink is slow, remote, and flaky -- it is a file on a disk that fills, a
socket to a log collector that is being redeployed, an HTTP endpoint that
rate-limits. The request path must NEVER block, slow, or fail because of it.
Every design choice below falls out of that single sentence:

* `offer()` is SYNCHRONOUS and non-blocking. It is called from the request
  task on the hot path, so it may not `await` -- awaiting the sink there is
  the outage this module exists to prevent. It enqueues if the byte budget
  allows and returns immediately; if the queue is full it DROPS the record,
  counts the drop, and returns. A dropped capture is a diagnostic we lose; a
  blocked request is a user we lose. The trade is not close.

* The queue is bounded in BYTES, not records -- the same reasoning as the
  pump's `buffered_bytes` (read its docstring). A count-based bound would wave
  through one 5 MB record and blow the memory budget while the gauge read a
  reassuring "1 queued". `llmgw_capture_queue_bytes` is a bytes gauge for the
  same reason.

* A single background worker drains the queue to the sink. Sink errors are
  caught and counted (`reason="sink_error"`), never propagated -- an exception
  climbing out of the worker would kill the drain and, with it, all future
  observability, which is the row-7 failure wearing a different hat.

* `aclose()` drains what it can within a bound, then stops cleanly without
  leaking the worker task. Records it could not flush before a hard stop are
  dropped with `reason="shutdown"`.

The three drop reasons are exactly the label values of
`metrics.llmgw_capture_dropped_total`: `("queue_full", "sink_error",
"shutdown")`. That coupling is deliberate -- the metrics wiring scrapes
`dropped` straight onto that counter, so a fourth reason here would be a
series with no home.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections import deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from .clocks import Clock

# The only legal drop reasons. Kept in one place, and equal by construction to
# metrics.llmgw_capture_dropped_total's label_values, so the two cannot drift.
DROP_REASONS: tuple[str, ...] = ("queue_full", "sink_error", "shutdown")


@dataclass(slots=True)
class CaptureRecord:
    """The per-request facts, all of them high-cardinality on purpose.

    Every field here is one a metric label is forbidden from carrying:
    `request_id` and the tenant/workload/target identifiers are unbounded, and
    `error_code` crossed with them would be a cardinality product. This record
    is where they are allowed to be unbounded, because a log line costs bytes
    once, not a resident time series forever.

    `slots=True` because there is one of these per request and, briefly, a
    queue full of them; a per-instance `__dict__` on the hot path is memory we
    measured out of the pump for the same reason.
    """

    request_id: str
    tenant_id: str
    workload_id: str
    provider: str
    model: str
    outcome: str
    """One of errors.Outcome: completed / interrupted / rejected / failed /
    canceled. The single field accounting keys off."""

    attempts: int = 1
    """Upstream attempts spent. attempts/requests is the amplification factor."""

    tokens: dict[str, int] = field(default_factory=dict)
    """By kind: input / output / cache_read / cache_write. A dict, not four
    columns, so a surface that reports a new kind does not need a schema change
    here -- capture is a log, and a log tolerates a widening shape."""

    cost_usd: float = 0.0
    basis: str = "estimated"
    """'exact' if a usage frame was read, 'estimated' if inferred. An
    interrupted stream has no usage frame, so its cost is estimated -- and a
    bill that cannot tell the two apart is a bill you cannot defend."""

    committed: bool = False
    """Did a byte reach the client? The commitment flag the pump owns. Carried
    here because 'failed' and 'interrupted' are the same outcome to a metric
    but a different story to whoever is reading the one request that broke."""

    first_event_latency: float | None = None
    """Seconds from ingress to first content event. None if none arrived."""

    duration_s: float | None = None
    """Total wall time for the request, retries and waits included."""

    error_code: str | None = None
    """errors.GatewayError.code, or None for a clean completion. The field an
    investigator filters on; forbidden as a metric label crossed with tenant."""

    recorded_at: float | None = None
    """When the record was cut, from the injected clock. Optional so a caller
    that has not wired a clock through still produces a valid line."""

    stop_reason: str | None = None
    """Why the provider ended a completed response (`metrics.STOP_REASONS`),
    or None when it said nothing. `length` here and `completed` above is the
    truncated-agent case a dashboard cannot see (PLAN-2 A3)."""

    upstream_request_id: str | None = None
    """The provider's own request id (`x-request-id`, `request-id`,
    `x-inworld-request-id`), the one thing a support ticket to the provider
    needs and the one thing the response-header allowlist used to drop. Also
    returned to the client as `X-Gw-Upstream-Request-Id` (PLAN-2 A6d)."""

    upstream_processing_ms: float | None = None
    """The provider's self-reported server time (`openai-processing-ms`,
    `x-envoy-upstream-service-time`). Against `first_event_latency` it splits
    provider time from gateway-plus-network time for free."""

    def to_bytes(self) -> bytes:
        """Serialize to ONE JSON line with a trailing newline.

        The byte length of THIS is what the queue budget counts and what the
        drop decision is made against -- so serialization happens once, in
        `offer`, and the bytes (not the record) are what sits in the queue.
        Newline-delimited JSON because the sink is append-only and a reader
        splits on '\\n'; a pretty-printed multi-line record would corrupt that
        framing the moment two writers interleave.
        """
        # separators without spaces: fewer bytes on the hot path and in the
        # queue budget, and the budget is the whole point.
        return (json.dumps(asdict(self), separators=(",", ":")) + "\n").encode("utf-8")

    @property
    def nbytes(self) -> int:
        """Cost of this record against the queue budget. Serializes; callers on
        the hot path should prefer holding the result of `to_bytes()`."""
        return len(self.to_bytes())


@runtime_checkable
class Sink(Protocol):
    """Where a capture line goes to be durable.

    One async method, deliberately. The sink is allowed to be slow or to fail
    -- that is the whole premise -- so the interface promises nothing about
    latency or success. The worker, not the caller, absorbs both.
    """

    async def write(self, chunk: bytes) -> None: ...


class NullSink:
    """Drops every line. The zero-config default and the test double for
    'the sink is not the thing under test'. Never fails, never blocks -- which
    makes it the wrong sink for exercising the failure path and the right one
    for everything else."""

    __slots__ = ()

    async def write(self, chunk: bytes) -> None:
        return None


class FileSink:
    """Appends lines to a path. The reference durable sink.

    Blocking file IO in an async worker is normally a smell, and here it is
    correct: the worker is OFF the request hot path by construction, so a write
    that parks the event loop for a few milliseconds stalls only the drain, not
    a single client stream. Opening per write (rather than holding an fd) keeps
    the sink stateless and crash-tolerant at the cost of a syscall the worker
    can well afford -- it is draining a log, not serving a request.
    """

    __slots__ = ("_path",)

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    async def write(self, chunk: bytes) -> None:
        # ASYNC230: the blocking open/write is deliberate, not an oversight --
        # this runs only in the drain worker, which is off the request hot path
        # by construction (see the class docstring), so a few ms of parked loop
        # stalls the log, never a client stream.
        with open(self._path, "ab") as fh:  # noqa: ASYNC230
            fh.write(chunk)


class Capture:
    """A byte-bounded, non-blocking capture sink with a single drain worker.

    Wiring::

        cap = Capture(FileSink(path), max_queue_bytes=8 << 20, clock=clock)
        cap.start()
        ...
        queued = cap.offer(record)     # sync, never blocks, True/False
        ...
        await cap.aclose()             # drains within a bound, no task leak

    A `deque` + an `asyncio.Event` rather than an `asyncio.Queue`, for one
    reason: `asyncio.Queue` bounds by ITEM COUNT, and this module's entire
    contract is that the bound is in BYTES. So the byte accounting is tracked
    here by hand -- `_queue_bytes` is incremented in `offer` and decremented in
    the worker as each record leaves the queue -- and the `Event` is the wakeup
    that lets `offer` stay synchronous while the worker sleeps when idle.
    """

    __slots__ = (
        "_sink",
        "_max_queue_bytes",
        "_clock",
        "_drain_timeout",
        "_queue",
        "_queue_bytes",
        "_dropped",
        "_wakeup",
        "_closing",
        "_task",
        "_inflight",
    )

    def __init__(
        self,
        sink: Sink,
        *,
        max_queue_bytes: int,
        clock: Clock,
        drain_timeout: float = 5.0,
    ) -> None:
        if max_queue_bytes <= 0:
            raise ValueError("max_queue_bytes must be positive")
        self._sink = sink
        self._max_queue_bytes = int(max_queue_bytes)
        self._clock = clock
        # The bound aclose() gives a graceful drain before it stops the worker
        # hard. Measured on the INJECTED clock, so tests advance simulated time
        # rather than sleep -- a stalled sink cannot make aclose() hang forever.
        self._drain_timeout = float(drain_timeout)

        self._queue: deque[bytes] = deque()
        self._queue_bytes = 0
        self._dropped: dict[str, int] = dict.fromkeys(DROP_REASONS, 0)
        # Set by offer() (a record arrived) and by aclose() (stop). The worker
        # sleeps on it when the queue is empty so an idle capture costs nothing.
        self._wakeup = asyncio.Event()
        self._closing = False
        self._task: asyncio.Task[None] | None = None
        # The record currently being written, held out of the queue during its
        # `await sink.write`. Tracked so a hard shutdown mid-write counts it as
        # a shutdown drop rather than losing it silently.
        self._inflight: bytes | None = None

    # ---------------------------------------------------------------- hot path

    def offer(self, record: CaptureRecord) -> bool:
        """Enqueue a record if the byte budget allows; otherwise drop it.

        SYNCHRONOUS and non-blocking -- this is the request task calling, and
        an `await` here would couple the request's latency to the sink's, which
        is precisely the row-7 failure. Returns True if queued, False if
        dropped. It never waits and never raises: a capture path that can throw
        into the request path is a capture path that can end the request.

        Byte budget, not item count: an oversized record is rejected even into
        an empty queue, so no single 5 MB record is ever 'waved through'.
        """
        if self._closing:
            # We are shutting down; new work does not join a queue that is being
            # torn down. Counts as a shutdown drop, the reason reserved for
            # records that did not survive the stop.
            self._dropped["shutdown"] += 1
            return False

        chunk = record.to_bytes()
        size = len(chunk)
        # `>` so a record whose bytes exactly fill the remaining budget still
        # fits; the invariant the test asserts is `queue_bytes <= bound`.
        if self._queue_bytes + size > self._max_queue_bytes:
            self._dropped["queue_full"] += 1
            return False

        self._queue.append(chunk)
        self._queue_bytes += size
        self._wakeup.set()  # wake a sleeping worker; a no-op if already set.
        return True

    # ---------------------------------------------------------------- worker

    def start(self) -> asyncio.Task[None]:
        """Spawn the single drain worker. Idempotent -- a second call returns
        the running task rather than leaking a second drainer onto the queue."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run())
        return self._task

    async def run(self) -> None:
        """Drain the queue to the sink, forever, until closed and empty.

        One task, by construction: two drainers would race on `_queue_bytes`
        and on ordering, and capture's value is a truthful ordered log. The
        loop below never holds anything across the `await sink.write` except
        the one in-flight record, so `offer` -- which never awaits -- always
        finds a consistent queue.
        """
        while True:
            if self._queue:
                chunk = self._queue.popleft()
                # Decrement as it LEAVES the queue: `_queue_bytes` is what is
                # still queued and thus what `offer` budgets against. The
                # in-flight record is accounted separately via `_inflight`.
                self._queue_bytes -= len(chunk)
                self._inflight = chunk
                try:
                    await self._sink.write(chunk)
                except asyncio.CancelledError:
                    # A hard shutdown cancelled us mid-write. The record is gone
                    # unflushed -- count it, and re-raise so the task actually
                    # stops (swallowing CancelledError leaks the worker).
                    self._dropped["shutdown"] += 1
                    self._inflight = None
                    raise
                except Exception:
                    # The sink failed. Swallow and count -- NEVER propagate. An
                    # exception out of here kills the drain and every future
                    # capture with it, which is the row-7 outage. One lost
                    # record is the acceptable cost; the drain survives.
                    self._dropped["sink_error"] += 1
                finally:
                    self._inflight = None
                continue

            # Queue empty.
            if self._closing:
                return
            # Sleep until offer() or aclose() signals. Clear-then-recheck closes
            # the lost-wakeup window: if a record was appended (or closing was
            # set) between the emptiness test and the clear, we see it and loop
            # instead of parking on a signal that already fired.
            self._wakeup.clear()
            if self._queue or self._closing:
                continue
            await self._wakeup.wait()

    async def aclose(self) -> None:
        """Stop the worker: drain what fits in the bound, then stop cleanly.

        Graceful within `drain_timeout` (on the injected clock), then hard. The
        hard stop cancels the worker and counts everything still queued -- plus
        an in-flight record caught mid-write -- as a `shutdown` drop. Both paths
        end with the worker task awaited to completion and cleared, because the
        chaos tier asserts `asyncio.all_tasks()` returns to baseline and a
        leaked drainer fails that assertion.
        """
        self._closing = True
        self._wakeup.set()

        task = self._task
        if task is None:
            self._drain_remaining_as_shutdown()
            return

        try:
            # Bound the graceful drain on the injected clock. If the sink is
            # healthy the worker empties the queue and returns in zero simulated
            # time, so the timeout never fires and no test has to advance a
            # clock to close cleanly.
            async with self._clock.timeout(self._drain_timeout):
                await task
        except TimeoutError:
            # Graceful window elapsed with the sink still stuck. Stop hard.
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        finally:
            # Whatever the worker did not flush is a shutdown drop -- recorded
            # here so the count is complete whether we stopped soft or hard.
            self._drain_remaining_as_shutdown()
            self._task = None

    def _drain_remaining_as_shutdown(self) -> None:
        """Count and discard anything still queued after the worker has stopped.

        Called only once the worker is guaranteed not running (awaited or
        cancelled), so there is no race on the deque here."""
        while self._queue:
            chunk = self._queue.popleft()
            self._queue_bytes -= len(chunk)
            self._dropped["shutdown"] += 1
        # An in-flight record left by a cancelled write is already counted in
        # run()'s CancelledError branch; guard against double-counting.
        self._inflight = None

    # ---------------------------------------------------------------- readouts
    # An observer must not be a writer: these return snapshots so a metrics
    # scraper cannot mutate the drain's state by accident.

    @property
    def queue_bytes(self) -> int:
        """Bytes currently queued for the sink. Feeds llmgw_capture_queue_bytes.
        Never exceeds `max_queue_bytes` -- that is the invariant offer holds."""
        return self._queue_bytes

    @property
    def max_queue_bytes(self) -> int:
        return self._max_queue_bytes

    @property
    def dropped(self) -> dict[str, int]:
        """A COPY of the drop counts by reason. Keys are exactly DROP_REASONS,
        which are exactly llmgw_capture_dropped_total's label values. A copy so
        a scraper reads without holding a reference into live state."""
        return dict(self._dropped)

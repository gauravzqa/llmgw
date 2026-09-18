"""The byte buffer: a FIFO of chunks with a hard byte ceiling.

Moved out of `pump.py` unchanged in behaviour on 18 Sep 2026 (PLAN-G G1). It
had exactly one owner while there was exactly one data plane; the WebSocket
relay needs the same primitive TWICE per session (once per direction) and
importing it out of the HTTP pump would make the socket plane depend on the
module whose first sentence is "nothing in the HTTP pump applies".

Nothing about the class changed in the move: the same split-oversized-chunks
rule, the same in-flight accounting, the same two Events. `pump.py` imports
it from here, so `tests/unit/test_pump.py` passes byte for byte.
"""

from __future__ import annotations

import asyncio
from collections import deque

__all__ = ["ByteBuffer"]


class ByteBuffer:
    """A FIFO of byte chunks with a hard byte ceiling.

    `asyncio.Queue` is the obvious reach and it is the wrong one: it bounds
    *items*. A queue of 32 items is 32 tokens or 256 MiB depending on the day,
    and the version that OOMs looks identical on the dashboard to the version
    that does not.

    Two properties beyond the bound:

    * **Oversized chunks are split, not admitted.** A chunk bigger than the
      whole ceiling would otherwise have to be let through wholesale -- the
      alternative being deadlock -- and then the ceiling is advisory. Splitting
      keeps `size <= limit` true at every instant. It re-chunks the stream,
      which is free: chunk boundaries are a property of the network, never of
      the content, and the parser downstream is byte-exact across any split.
    * **A chunk stays counted while it is in flight to the sink.** It is still
      in memory, so a gauge that forgot it would under-report exactly when the
      client is slow and the number matters.
    """

    __slots__ = ("_limit", "_chunks", "_size", "_closed", "_aborted", "_not_empty",
                 "_not_full")

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self._chunks: deque[bytes] = deque()
        self._size = 0
        self._closed = False
        self._aborted = False
        # Events rather than a Condition: exactly one producer and exactly one
        # consumer, so there is no thundering herd to be fair to, and the
        # check-then-clear is atomic because neither side awaits between them.
        self._not_empty = asyncio.Event()
        self._not_full = asyncio.Event()
        self._not_full.set()

    @property
    def size(self) -> int:
        return self._size

    @property
    def aborted(self) -> bool:
        return self._aborted

    async def put(self, chunk: bytes) -> None:
        """Append, blocking while full. This block IS the backpressure."""
        view = memoryview(chunk)
        while view:
            while self._size >= self._limit:
                if self._aborted:
                    return
                self._not_full.clear()
                await self._not_full.wait()
            if self._aborted:
                return
            room = self._limit - self._size
            piece = bytes(view[:room])
            view = view[room:]
            self._chunks.append(piece)
            self._size += len(piece)
            self._not_empty.set()

    async def get(self) -> bytes | None:
        """The next chunk, or None once the producer is finished."""
        while not self._chunks:
            if self._closed:
                return None
            self._not_empty.clear()
            await self._not_empty.wait()
        return self._chunks.popleft()

    def release(self, count: int) -> None:
        """The consumer is done with `count` bytes; the producer may refill."""
        self._size -= count
        if self._size < self._limit:
            self._not_full.set()

    def close(self) -> None:
        """No more chunks are coming. Wakes a consumer parked on empty."""
        self._closed = True
        self._not_empty.set()

    def abort(self) -> None:
        """Release everything, now. Called on every exit path from `run()`.

        The buffer is the one thing here that is not owned by a task, so it is
        the one thing a cancellation cannot free by itself -- and a pump that
        leaks its buffer on the error path leaks it exactly when the error
        path is being taken a lot.
        """
        self._aborted = True
        self._closed = True
        self._chunks.clear()
        self._size = 0
        self._not_empty.set()
        self._not_full.set()

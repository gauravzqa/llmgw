"""SSE framing. One decision drives the whole file: **we never decode.**

A gateway does not receive SSE frames. It receives TCP segments, and a TCP
segment boundary has no relationship whatsoever to a frame boundary. The
provider's framing is a property of the bytes; the chunking is a property of
the network, the proxies in between, and httpx's read buffer on the day. So
the only correctness property worth stating is:

    feed(a) + feed(b) + ... == feed(a + b + ...)   for every possible split.

Everything below exists to make that true, and `test_sse.py` spends thousands
of randomized splits trying to make it false.

--------------------------------------------------------------------------
Why bytes and not str
--------------------------------------------------------------------------

The tempting shape is `chunk.decode("utf-8")` at the top of `feed()`, then
parse text. It passes every hand-written test, because hand-written tests
feed whole frames. It fails the first time a segment boundary lands in the
middle of a multi-byte character -- which, for any model emitting anything
but ASCII, happens constantly. `" café"` split between the two bytes of
`é` decodes to two U+FFFD replacement characters and the user's answer is
silently corrupted. `errors="strict"` turns the same event into a 500.

Working in bytes makes that entire class of bug *unrepresentable* rather
than defended against. Line terminators (CR, LF) and the field separator
(`:`) are all ASCII, and UTF-8 is self-synchronising: no byte of a multi-byte
sequence can ever equal an ASCII byte. So framing on raw bytes is exactly
correct at every cut point, with no boundary logic at all. There is no
"partial character" state in this parser because the concept does not arise.

The only decoding here is of *complete* field values whose declared type is
`str` (`event:`, `id:`), after framing is finished. `data` stays `bytes` and
is handed to the caller undecoded: it is the caller who knows whether the
payload is JSON, `[DONE]`, or something a provider invented last week.

--------------------------------------------------------------------------
The CRLF-across-chunks bug
--------------------------------------------------------------------------

SSE permits CRLF, LF, and bare CR as terminators. A parser that handles all
three still has one boundary left to get wrong, and it is the one that
actually bites: the chunk ends with CR and the next chunk starts with LF.
Treat that CR as a complete terminator and the following LF becomes a second,
empty line -- a spurious blank line, which in SSE means *dispatch*. The frame
is cut in half and half a JSON object is handed to the surface.

The fix is to refuse to guess: a CR at the very end of the buffer is held,
unconsumed, until either another byte arrives to disambiguate it or `close()`
declares the stream over. One byte of latency buys exact invariance, and it
keeps `raw` honest -- an `after_cr` flag that swallows a later LF would leave
that LF belonging to no frame at all.

--------------------------------------------------------------------------
Comments are not noise
--------------------------------------------------------------------------

A line beginning with `:` is a comment. The WHATWG rule is "ignore the line",
and for a browser that is right. For a gateway it throws away the single most
useful signal on an otherwise silent socket: OpenRouter really does send
`: OPENROUTER PROCESSING` to stop intermediaries idling the connection out,
and a relay that drops it on the floor cannot tell "the upstream is alive and
thinking" from "the upstream is a dead TCP connection nobody has reaped yet".

So comments are surfaced as `SSEEvent(comment=...)` and the caller decides.
The decision they exist for is CONTRACTS.md C7: a heartbeat resets the
*liveness* clock and never the *progress* clock. Reset progress on a comment
and a provider wedged in a bad state can heartbeat politely forever while the
stall detector reports everything is fine; ignore the comment entirely and
the liveness clock kills a connection that was working.

`is_comment` exists so that consumer is a one-line filter rather than a
`data == b""` heuristic that also swallows legitimately empty frames.

--------------------------------------------------------------------------
The bound is in bytes, and that is the whole point
--------------------------------------------------------------------------

`max_frame_bytes` bounds *buffered bytes*, not lines and not events. This is
mental-model failure #3 -- "memory counted in the wrong unit" -- and the
arithmetic is why: a limit of "200 buffered messages" reads as reassuringly
small on a dashboard right up to the moment those messages are 8 MiB each,
at which point it is 1.6 GiB of heap and an OOM kill that looks like a
mystery. A byte bound cannot be quietly rescaled by the payloads.

Overflow poisons the parser rather than skipping the frame. A stream we could
not frame safely is not a stream we should keep reading: skipping resynchronises
onto whatever byte follows, which for an LLM answer means silently splicing
two halves of different JSON objects together.

--------------------------------------------------------------------------
Spec deviations, all deliberate
--------------------------------------------------------------------------

1. `close()` **emits a trailing frame that never saw its blank line.** Real
   providers end streams without the final blank line, and TCP FIN is a
   perfectly good frame terminator. The alternative -- drop it -- is a silent
   data-loss bug that only shows up as "the last token is sometimes missing",
   which is approximately the worst bug shape there is. If the tail is
   genuinely truncated garbage the surface's own parse will reject it, and a
   rejection is louder than a disappearance.
2. **A frame with fields but no `data` still dispatches.** WHATWG fires
   nothing when the data buffer is empty, because a browser has nothing to
   deliver. A relay does: `event: message_stop` with no data is information
   the downstream needs. Frames containing *only* unknown fields, or only
   comments, still dispatch nothing.
3. **`id` and `retry` are per-frame, not sticky.** WHATWG carries the last
   event ID forward across events for reconnection. We report what the frame
   actually carried, so `raw` and the parsed fields never disagree.
   Reconnection is a client's job, not a relay's.
4. **The single optional space after the colon is stripped from comments
   too**, for the same reason it is stripped from `data`. We surface comments
   instead of ignoring them, so they get the field-value treatment.
5. A UTF-8 BOM is stripped once, at the very start of the stream, and is not
   part of any frame's `raw`. It is a transport artefact, not content.

`raw` is the exact bytes of the lines that produced the event, terminators
included -- for a dispatched frame, up to and including the blank line.
Joining every event's `raw` reproduces the input stream exactly, except for
bytes that carried no information: comment lines when `emit_comments=False`,
and blank lines that dispatched nothing.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .errors import FrameTooLarge

_BOM = b"\xef\xbb\xbf"

# CRLF must come first: alternation is ordered, so `\r\n|\n|\r` consumes a
# CRLF pair as one terminator instead of matching the CR and leaving the LF to
# look like a blank line.
_TERM = re.compile(rb"\r\n|\n|\r")


@dataclass(frozen=True, slots=True)
class SSEEvent:
    """One parsed frame, or one comment line.

    Frozen because these cross a thread/task boundary into the pump and the
    accounting path; a mutable event is an event two layers can disagree
    about after the fact.
    """

    event: str | None = None
    """The `event:` field. None when the frame did not carry one -- not
    defaulted to "message", because a relay that invents a field name the
    provider did not send is lying to the surface downstream."""

    data: bytes = b""
    """`data:` lines joined with b"\\n", no trailing newline. Deliberately
    bytes: `data: [DONE]` is not JSON, and any parser that `json.loads()`
    every data field dies on the OpenAI terminal marker."""

    id: str | None = None
    retry: int | None = None

    comment: bytes | None = None
    """Set only for `:` lines. Evidence of liveness, never of progress."""

    raw: bytes = b""
    """Exact original bytes of this frame, terminators included."""

    @property
    def is_comment(self) -> bool:
        return self.comment is not None


class SSEParser:
    """Incremental, byte-exact SSE framing.

    Feed it whatever the socket gave you. It returns whichever complete events
    those bytes finished, holds the rest, and produces the identical event list
    no matter where the splits fell.
    """

    __slots__ = (
        "_max", "_emit_comments", "_buf", "_start", "_raw", "_data", "_event",
        "_id", "_retry", "_have_fields", "_bom_pending", "_closed", "_poisoned",
    )

    def __init__(self, *, max_frame_bytes: int = 1 << 20, emit_comments: bool = True) -> None:
        if max_frame_bytes < 1:
            raise ValueError("max_frame_bytes must be positive")
        self._max = max_frame_bytes
        self._emit_comments = emit_comments

        # Unconsumed input. `_start` is where parsing resumes; we advance it
        # rather than deleting a prefix per line, because `del buf[:n]` copies
        # the tail and doing that once per line makes a large frame quadratic.
        self._buf = bytearray()
        self._start = 0

        # Raw bytes of the frame under construction. Comment lines never land
        # here -- they are their own events and own their own bytes.
        self._raw = bytearray()

        self._data: list[bytes] = []
        self._event: str | None = None
        self._id: str | None = None
        self._retry: int | None = None
        self._have_fields = False

        self._bom_pending = True
        self._closed = False
        self._poisoned = False

    # ------------------------------------------------------------------ API

    @property
    def buffered_bytes(self) -> int:
        """Bytes held on behalf of a frame that has not been emitted yet.

        This is the number `max_frame_bytes` bounds, and the number worth
        exporting as a gauge: sum it across live streams and you have the
        parser's actual contribution to RSS, in the unit the kernel kills on.
        """
        return len(self._raw) + len(self._buf) - self._start

    def feed(self, chunk: bytes) -> list[SSEEvent]:
        """Consume bytes; return the events they completed.

        Raises FrameTooLarge if a single frame outgrows the bound. Feeding an
        empty chunk is a no-op, which matters because httpx will hand you one
        and a parser that treats "no bytes" as "end of frame" invents events.
        """
        if self._closed:
            raise ValueError("feed() after close(); construct a new SSEParser per stream")
        if self._poisoned:
            raise FrameTooLarge(
                f"parser was poisoned by a frame over max_frame_bytes={self._max}"
            )
        if not chunk:
            return []
        self._buf += chunk
        return self._drain(final=False)

    def close(self) -> list[SSEEvent]:
        """End of stream. Flushes a trailing frame that never got its blank line.

        See deviation 1 in the module docstring: providers really do end
        without the terminator, and dropping the last frame loses the last
        token of an answer with no error anywhere. Idempotent -- a second call
        returns []. Safe to call on a poisoned parser so cleanup paths can put
        it in a `finally` without a second exception chasing the first.
        """
        if self._closed:
            return []
        self._closed = True
        if self._poisoned:
            return []
        events = self._drain(final=True)
        tail = self._dispatch()
        if tail is not None:
            events.append(tail)
        return events

    # -------------------------------------------------------------- framing

    def _drain(self, *, final: bool) -> list[SSEEvent]:
        out: list[SSEEvent] = []
        buf = self._buf

        if self._bom_pending and not self._eat_bom(final=final):
            return out  # a partial BOM; cannot yet tell it from real content

        while True:
            match = _TERM.search(buf, self._start)
            if match is None:
                break
            end = match.end()
            # The bug this line exists for: a chunk ending in CR whose LF is
            # the first byte of the next chunk. Consuming the CR now would make
            # that LF a blank line, and a blank line means dispatch -- so the
            # frame gets cut in half. Hold the ambiguous CR instead.
            if not final and end == len(buf) and match.group() == b"\r":
                break
            line = bytes(buf[self._start:match.start()])
            raw = bytes(buf[self._start:end])
            self._start = end
            event = self._handle_line(line, raw)
            if event is not None:
                out.append(event)

        if final:
            # No terminator will ever arrive. FIN ends the line.
            rest = bytes(buf[self._start:])
            self._start = len(buf)
            if rest:
                event = self._handle_line(rest, rest)
                if event is not None:
                    out.append(event)

        # Compact only once we have consumed at least half the buffer, which
        # makes the total copying linear in the stream rather than quadratic.
        if self._start and self._start * 2 >= len(buf):
            del buf[: self._start]
            self._start = 0

        # Draining is finished, so whatever is left holds no terminator: it is
        # genuinely part of the frame under construction and counts in full.
        self._check_bound(self.buffered_bytes)
        return out

    def _eat_bom(self, *, final: bool) -> bool:
        """Strip a leading UTF-8 BOM. Returns False if we must wait for bytes."""
        head = bytes(self._buf[self._start : self._start + 3])
        if head == _BOM:
            self._start += 3
            self._bom_pending = False
            return True
        if not final and len(head) < 3 and _BOM.startswith(head):
            # b"\xef" alone could still become a BOM. Two bytes of patience.
            return False
        self._bom_pending = False
        return True

    def _handle_line(self, line: bytes, raw: bytes) -> SSEEvent | None:
        if line[:1] == b":":
            # A comment is its own event and owns its own bytes: it must not
            # join the raw of a frame it happens to sit inside, or a caller
            # re-emitting that frame would duplicate the heartbeat.
            if not self._emit_comments:
                return None
            return SSEEvent(comment=_strip_one_space(line[1:]), raw=raw)

        self._raw += raw
        # Only the frame's own bytes here, NOT `buffered_bytes`. Mid-drain the
        # unscanned tail of the chunk usually holds hundreds of complete frames,
        # and counting those would reject an ordinary 1 MiB read of tiny deltas.
        self._check_bound(len(self._raw))

        if not line:
            return self._dispatch()

        name, sep, value = line.partition(b":")
        if not sep:
            # "A line with no colon is a field with an empty value" -- WHATWG.
            name, value = line, b""
        value = _strip_one_space(value)

        if name == b"data":
            self._data.append(value)
        elif name == b"event":
            self._event = value.decode("utf-8", "replace")
        elif name == b"id":
            if b"\x00" in value:
                return None  # WHATWG: an id containing NUL is ignored outright
            self._id = value.decode("utf-8", "replace")
        elif name == b"retry":
            if not value.isdigit():
                return None  # non-numeric retry is ignored, not an error
            self._retry = int(value)
        else:
            return None  # unknown fields are ignored, and do not make a frame
        self._have_fields = True
        return None

    def _dispatch(self) -> SSEEvent | None:
        """Blank line seen (or stream ended): emit the accumulated frame."""
        raw = bytes(self._raw)
        self._raw.clear()
        if not self._have_fields:
            # Consecutive blank lines, or a blank line after a comment. Nothing
            # to deliver, and the bytes carried no information.
            self._reset_fields()
            return None
        event = SSEEvent(
            event=self._event,
            data=b"\n".join(self._data),
            id=self._id,
            retry=self._retry,
            raw=raw,
        )
        self._reset_fields()
        return event

    def _reset_fields(self) -> None:
        self._data = []
        self._event = None
        self._id = None
        self._retry = None
        self._have_fields = False

    def _check_bound(self, held: int) -> None:
        if held <= self._max:
            return
        # Poison before raising: the caller may well swallow the exception and
        # keep feeding, and resynchronising mid-frame splices two different
        # JSON objects into one. Drop the state so nothing can half-continue.
        self._poisoned = True
        self._buf.clear()
        self._start = 0
        self._raw.clear()
        self._reset_fields()
        raise FrameTooLarge(
            f"SSE frame exceeded max_frame_bytes={self._max} "
            f"(buffered {held} bytes for a single frame)"
        )


def _strip_one_space(value: bytes) -> bytes:
    """Exactly one leading space, per spec: `data: x` and `data:x` both give
    `x`, and `data:  x` gives ` x`. Stripping greedily silently eats leading
    indentation out of code the model is streaming."""
    return value[1:] if value[:1] == b" " else value

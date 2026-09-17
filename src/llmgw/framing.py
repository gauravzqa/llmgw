"""Framing: how an upstream body is cut into the frames a surface reads.

Until Phase B every upstream body was SSE. `pump.py` built an `SSEParser`,
fed it every chunk, and handed the frames to the surface. That assumption
was invisible right up to the day a body was not SSE -- and then it failed
in the worst available shape. Measured on 16 Sep 2026 against Inworld's
text-to-speech stream (newline-delimited JSON, one object per line, no blank
lines): every line was appended to one SSE frame as an unknown field, no
event ever fired, the bytes were still copied to the client, and the request
ended in `FirstEventTimeout` at 20 s or `FrameTooLarge` at 1 MiB cumulative
-- about sixteen seconds of 24 kHz audio -- recorded as a *provider stall*
with $0 accounted. A binary `audio/mpeg` body from ElevenLabs or OpenAI does
the same thing with fewer steps.

So the parser becomes one `Framer` among three, and the surface chooses:

    "sse"    text/event-stream. The existing parser, byte-exact, CRLF and LF.
    "jsonl"  one frame per newline-terminated line, blank lines skipped.
             Inworld `:stream`, ElevenLabs `with-timestamps`.
    "raw"    every chunk is a frame. ElevenLabs `/stream`, OpenAI binary TTS.

The frame type is `SSEEvent` for all three. That is deliberate rather than
lazy: surfaces already classify on `event`, `data`, `raw` and `is_comment`,
and a second frame class would either duplicate those four attributes or
force every surface method to take a union. For JSONL the line is `data`
(terminator stripped) and the exact bytes are `raw`; for raw framing `data`
and `raw` are the same chunk. `comment` is only ever set by the SSE framer.

--------------------------------------------------------------------------
What does not change
--------------------------------------------------------------------------

Commitment (the first attempted client write), the byte-bounded buffer,
backpressure, the progress/liveness split and native endings are the pump's
and stay exactly where they were. A framer only answers "where does one
frame end", and the same split-invariance property that `sse.py` is built
on holds for all three: `feed(a) + feed(b) == feed(a + b)` for any cut.

The per-frame bound is the same `max_frame_bytes` for SSE and JSONL. For raw
framing there is no frame to bound beyond the pump's own buffer: a raw chunk
is whatever the transport delivered, and the buffer already refuses to hold
more than `buffer_bytes` of it at once.

--------------------------------------------------------------------------
Unknown framing is refused, not misread
--------------------------------------------------------------------------

`assert_upstream_framing()` is the other half. A surface that expects SSE
and receives `application/json` or `audio/mpeg` on the streaming path must
fail *before* the status is committed to the client, as a 502 whose message
names the content type -- so the failure is a fallback, a metric and a log
line instead of a truncated stream and a zero on the bill. The executor
calls it on the upstream's response headers before opening the client's
response; the pump calls it again defensively when it is handed a content
type, which costs nothing when the executor already did.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal, Protocol

from llmgw.errors import FrameTooLarge, UnsupportedUpstreamFraming
from llmgw.sse import SSEEvent, SSEParser

Framing = Literal["sse", "jsonl", "raw"]
"""The closed set a surface's `framing` attribute draws from."""

Frame = SSEEvent
"""One unit a surface classifies. See the module docstring for why this is
the SSE event type for every framing and not a new class."""

_LF = b"\n"
_CR = b"\r"

_SSE_MEDIA_TYPE = "text/event-stream"


class Framer(Protocol):
    """Incremental, byte-exact framing of one upstream body.

    `feed()` returns the frames the bytes completed and holds the rest;
    `flush()` is end-of-stream and returns whatever a final terminator would
    have completed. Both may raise `FrameTooLarge`, and after that the framer
    is poisoned: a frame we could not bound is a frame we cannot safely
    resynchronise after.
    """

    def feed(self, data: bytes) -> Iterable[Frame]: ...
    def flush(self) -> Iterable[Frame]: ...


class SSEFramer:
    """The existing parser behind the `Framer` protocol. Comments are frames
    with `is_comment` set, because a heartbeat is liveness the pump needs."""

    __slots__ = ("_parser",)

    def __init__(self, *, max_frame_bytes: int) -> None:
        self._parser = SSEParser(max_frame_bytes=max_frame_bytes, emit_comments=True)

    def feed(self, data: bytes) -> list[Frame]:
        return self._parser.feed(data)

    def flush(self) -> list[Frame]:
        return self._parser.close()


class JSONLFramer:
    """One frame per newline-terminated line.

    Blank lines are not frames: they are skipped, and their bytes are not
    attached to any frame's `raw` (the pump copies bytes to the client from
    the chunk, not from the frames, so nothing is lost). A trailing partial
    line at end-of-stream is flushed as a frame, for the same reason
    `SSEParser.close()` emits a trailing frame -- TCP FIN is a perfectly good
    terminator and a dropped last line is the worst bug shape there is.

    `\\r\\n` is accepted as a terminator and stripped from `data`; a bare
    `\\r` is not a terminator here (JSONL never uses one, and a JSON string
    may legally contain an escaped one).

    The bound is per line, measured on the bytes held for the line under
    construction, and it poisons the framer exactly as the SSE parser does.
    Sized against the measured shapes: Inworld's steady-state LINEAR16 line
    is 64 KB on the wire, MP3 lines under 16 KB, so the default 1 MiB is
    sixteen times generous.
    """

    __slots__ = ("_max", "_buf", "_closed", "_poisoned")

    def __init__(self, *, max_frame_bytes: int) -> None:
        if max_frame_bytes < 1:
            raise ValueError("max_frame_bytes must be positive")
        self._max = max_frame_bytes
        self._buf = bytearray()
        self._closed = False
        self._poisoned = False

    @property
    def buffered_bytes(self) -> int:
        return len(self._buf)

    def feed(self, data: bytes) -> list[Frame]:
        if self._closed:
            raise ValueError("feed() after flush(); construct a new JSONLFramer per stream")
        if self._poisoned:
            raise FrameTooLarge(
                f"framer was poisoned by a line over max_frame_bytes={self._max}"
            )
        if not data:
            return []
        self._buf += data
        out: list[Frame] = []
        start = 0
        buf = self._buf
        while True:
            end = buf.find(_LF, start)
            if end < 0:
                break
            raw = bytes(buf[start : end + 1])
            line = raw[:-1]
            if line[-1:] == _CR:
                line = line[:-1]
            start = end + 1
            if line.strip():
                out.append(SSEEvent(data=line, raw=raw))
        if start:
            del buf[:start]
        # Whatever is left holds no terminator: it is the line under
        # construction and counts in full against the bound.
        self._check_bound(len(buf))
        return out

    def flush(self) -> list[Frame]:
        if self._closed:
            return []
        self._closed = True
        if self._poisoned:
            return []
        rest = bytes(self._buf)
        self._buf.clear()
        if not rest.strip():
            return []
        line = rest[:-1] if rest[-1:] == _CR else rest
        return [SSEEvent(data=line, raw=rest)]

    def _check_bound(self, held: int) -> None:
        if held <= self._max:
            return
        self._poisoned = True
        self._buf.clear()
        raise FrameTooLarge(
            f"JSONL line exceeded max_frame_bytes={self._max} "
            f"(buffered {held} bytes for a single line)"
        )


class RawFramer:
    """Every chunk is one frame; `data` is `raw` is the chunk.

    There is no bound of its own. A raw chunk is however many bytes the
    transport delivered at once, and the pump's byte buffer already refuses
    to hold more than its limit; a second limit here would only ever be
    smaller than the first for no reason. Empty chunks are not frames.
    """

    __slots__ = ("_closed",)

    def __init__(self, *, max_frame_bytes: int) -> None:  # noqa: ARG002 - protocol shape
        self._closed = False

    def feed(self, data: bytes) -> list[Frame]:
        if self._closed:
            raise ValueError("feed() after flush(); construct a new RawFramer per stream")
        if not data:
            return []
        return [SSEEvent(data=data, raw=data)]

    def flush(self) -> list[Frame]:
        self._closed = True
        return []


_FRAMERS: dict[str, type] = {
    "sse": SSEFramer,
    "jsonl": JSONLFramer,
    "raw": RawFramer,
}


def framer_for(framing: str, *, max_frame_bytes: int) -> Framer:
    """The framer a `framing` value names. Unknown values are a programming
    error at construction, never a runtime guess mid-stream."""
    try:
        cls = _FRAMERS[framing]
    except KeyError:
        raise ValueError(
            f"unknown framing {framing!r}; expected one of {sorted(_FRAMERS)}"
        ) from None
    return cls(max_frame_bytes=max_frame_bytes)


def media_type(content_type: str | None) -> str:
    """`text/event-stream; charset=utf-8` -> `text/event-stream`, lowered.
    Empty or missing is the empty string."""
    if not content_type:
        return ""
    return content_type.split(";", 1)[0].strip().lower()


def assert_upstream_framing(framing: str, content_type: str | None) -> None:
    """Refuse a streaming body whose declared type cannot be the framing the
    surface expects. Raises `UnsupportedUpstreamFraming`.

    Only the SSE case is checkable from a content type: providers label
    NDJSON as `application/json` (Inworld) and raw audio by its codec, and a
    `jsonl` or `raw` surface has already declared it knows what is coming.
    An absent content type is not evidence of anything and is let through,
    exactly as it was before this check existed.

    Meant to run BEFORE the status is committed to the client, on the
    upstream's response headers, so the refusal is a fallback candidate. Run
    after commitment it would still be a correct error, but a post-commit
    error ends as a native ending, not a 502.
    """
    if framing != "sse":
        return
    actual = media_type(content_type)
    if not actual or actual == _SSE_MEDIA_TYPE:
        return
    raise UnsupportedUpstreamFraming(
        f"upstream answered with content-type {actual!r} on a streaming request; "
        f"this surface expects {_SSE_MEDIA_TYPE}"
    )


__all__ = [
    "Frame",
    "Framer",
    "Framing",
    "JSONLFramer",
    "RawFramer",
    "SSEFramer",
    "assert_upstream_framing",
    "framer_for",
    "media_type",
]

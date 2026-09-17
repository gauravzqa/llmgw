"""Framing tests: three framers, one property.

The property is the same one `test_sse.py` spends its life on -- the frames a
body produces must not depend on where the transport cut it -- extended to
the two framings Phase B added. The other half of the file is the refusal:
an SSE surface handed a body that cannot be SSE must fail with a 502 that
names the content type, not with a stall.
"""

from __future__ import annotations

import json

import pytest

from llmgw import errors as E
from llmgw.framing import (
    JSONLFramer,
    RawFramer,
    SSEFramer,
    assert_upstream_framing,
    framer_for,
    media_type,
)
from llmgw.sse import SSEEvent

# ---------------------------------------------------------------- helpers


def run(framer, data: bytes, cuts: list[int]) -> list[SSEEvent]:
    """Feed `data` split at `cuts`, then flush. The whole-buffer case is
    `cuts=[]`."""
    out: list[SSEEvent] = []
    prev = 0
    for cut in [*cuts, len(data)]:
        out.extend(framer.feed(data[prev:cut]))
        prev = cut
    out.extend(framer.flush())
    return out


def every_n(data: bytes, n: int) -> list[int]:
    return list(range(n, len(data), n))


def ndjson_body(lines: int = 20, payload_bytes: int = 4096) -> bytes:
    # The measured Inworld shape: one object per line, `result.audioContent`
    # base64, `usage` inside `result`, no blank lines, no terminator.
    body = b""
    for i in range(lines):
        obj = {"result": {"audioContent": "A" * payload_bytes,
                          "usage": {"processedCharactersCount": 19 if i == 0 else 0}}}
        body += json.dumps(obj, separators=(",", ":")).encode() + b"\n"
    return body


SSE_BODY = (
    b"data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n"
    b": keep-alive\n\n"
    b"data: {\"choices\":[{\"delta\":{\"content\":\"lo\"}}]}\n\n"
    b"data: [DONE]\n\n"
)
SSE_BODY_CRLF = SSE_BODY.replace(b"\n", b"\r\n")


# ---------------------------------------------------------- split invariance


@pytest.mark.parametrize("piece", [1, 7, 64])
def test_sse_framer_is_split_invariant(piece: int):
    whole = run(SSEFramer(max_frame_bytes=1 << 20), SSE_BODY, [])
    split = run(SSEFramer(max_frame_bytes=1 << 20), SSE_BODY, every_n(SSE_BODY, piece))
    assert split == whole
    assert [e.is_comment for e in whole] == [False, True, False, False]


def test_sse_framer_accepts_crlf_and_lf_alike():
    lf = run(SSEFramer(max_frame_bytes=1 << 20), SSE_BODY, [])
    crlf = run(SSEFramer(max_frame_bytes=1 << 20), SSE_BODY_CRLF, every_n(SSE_BODY_CRLF, 7))
    assert [e.data for e in lf] == [e.data for e in crlf]
    # `raw` keeps each frame's own terminators; the blank line after a
    # comment dispatches nothing and belongs to no frame (see sse.py).
    assert all(e.raw.endswith(b"\r\n") for e in crlf)


@pytest.mark.parametrize("piece", [1, 7, 64])
def test_jsonl_framer_is_split_invariant(piece: int):
    body = ndjson_body(lines=5, payload_bytes=100)
    whole = run(JSONLFramer(max_frame_bytes=1 << 20), body, [])
    split = run(JSONLFramer(max_frame_bytes=1 << 20), body, every_n(body, piece))
    assert split == whole
    assert len(whole) == 5
    assert all(not e.is_comment for e in whole)
    # `data` is the line without its terminator and parses as JSON; `raw`
    # is the exact bytes including the newline, and joins back to the body.
    assert all(json.loads(e.data)["result"]["audioContent"] for e in whole)
    assert b"".join(e.raw for e in whole) == body


def test_jsonl_framer_skips_blank_lines_and_strips_crlf():
    body = b'{"a":1}\r\n\n\n{"b":2}\n   \n{"c":3}\n'
    frames = run(JSONLFramer(max_frame_bytes=1 << 20), body, every_n(body, 3))
    assert [e.data for e in frames] == [b'{"a":1}', b'{"b":2}', b'{"c":3}']
    assert frames[0].raw == b'{"a":1}\r\n'


def test_jsonl_framer_flushes_a_partial_trailing_line():
    body = b'{"a":1}\n{"b":2}'  # no final newline: TCP FIN is the terminator
    frames = run(JSONLFramer(max_frame_bytes=1 << 20), body, [4])
    assert [e.data for e in frames] == [b'{"a":1}', b'{"b":2}']
    assert frames[-1].raw == b'{"b":2}'


def test_jsonl_framer_flush_is_idempotent_and_feed_after_flush_is_an_error():
    framer = JSONLFramer(max_frame_bytes=64)
    assert framer.feed(b'{"a":1}\n') and framer.flush() == [] and framer.flush() == []
    with pytest.raises(ValueError):
        framer.feed(b"x")


def test_jsonl_per_line_bound_trips_frame_too_large_and_poisons():
    framer = JSONLFramer(max_frame_bytes=16)
    assert framer.feed(b'{"ok":1}\n') and framer.buffered_bytes == 0
    with pytest.raises(E.FrameTooLarge):
        framer.feed(b"x" * 17)
    with pytest.raises(E.FrameTooLarge):
        framer.feed(b"\n")  # poisoned: nothing can half-continue
    assert framer.flush() == []


def test_jsonl_bound_is_per_line_not_per_stream():
    """The measured Inworld shape -- 64 KB lines, many of them -- must not
    accumulate against the bound the way it did inside the SSE parser."""
    framer = JSONLFramer(max_frame_bytes=1 << 20)
    total = 0
    for _ in range(50):
        line = json.dumps({"audio": "A" * 60_000}).encode() + b"\n"
        total += len(line)
        assert len(framer.feed(line)) == 1
    assert total > (1 << 20)  # more than the bound overall, no error


@pytest.mark.parametrize("piece", [1, 7, 64])
def test_raw_framer_frames_are_exactly_the_chunks(piece: int):
    body = bytes(range(256)) * 4
    framer = RawFramer(max_frame_bytes=8)  # bound is irrelevant to raw
    frames = run(framer, body, every_n(body, piece))
    assert all(e.data == e.raw for e in frames)
    assert b"".join(e.raw for e in frames) == body
    assert all(len(e.raw) <= piece for e in frames)


def test_raw_framer_never_trips_the_frame_bound():
    framer = RawFramer(max_frame_bytes=1)
    frames = framer.feed(b"x" * (4 << 20))
    assert len(frames) == 1 and len(frames[0].raw) == 4 << 20
    assert framer.feed(b"") == []


def test_framer_for_dispatches_and_rejects_unknown_names():
    assert isinstance(framer_for("sse", max_frame_bytes=8), SSEFramer)
    assert isinstance(framer_for("jsonl", max_frame_bytes=8), JSONLFramer)
    assert isinstance(framer_for("raw", max_frame_bytes=8), RawFramer)
    with pytest.raises(ValueError, match="unknown framing"):
        framer_for("ndjson", max_frame_bytes=8)


# ------------------------------------------------------- unsupported framing


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("text/event-stream", "text/event-stream"),
        ("Text/Event-Stream; charset=utf-8", "text/event-stream"),
        (" application/json ;x=y", "application/json"),
        ("", ""),
        (None, ""),
    ],
)
def test_media_type_normalises(header, expected):
    assert media_type(header) == expected


def test_sse_surface_refuses_non_sse_content_types_before_any_byte():
    for bad in ("application/json", "audio/mpeg", "application/x-ndjson"):
        with pytest.raises(E.UnsupportedUpstreamFraming) as caught:
            assert_upstream_framing("sse", bad)
        err = caught.value
        assert bad in str(err)
        assert err.status == 502 and err.code == "unsupported_upstream_framing"
        assert err.retry_same is False and err.try_next is True
        assert err.health is E.Health.NEUTRAL and err.blame is E.Blame.GATEWAY


def test_sse_surface_accepts_event_stream_or_no_content_type():
    assert_upstream_framing("sse", "text/event-stream; charset=utf-8")
    assert_upstream_framing("sse", None)
    assert_upstream_framing("sse", "")


def test_jsonl_and_raw_surfaces_are_not_checked_by_content_type():
    # Providers label NDJSON `application/json` and audio by codec; a surface
    # that declared those framings already knows what is coming.
    assert_upstream_framing("jsonl", "application/json")
    assert_upstream_framing("raw", "audio/mpeg")


def test_unsupported_framing_is_a_registered_error_code():
    assert "unsupported_upstream_framing" in E.ERROR_CODES

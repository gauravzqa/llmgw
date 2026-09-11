"""SSE framing tests.

The only property that matters here is byte-split invariance, so most of this
file is one assertion applied a few thousand ways: *the events do not depend
on where the chunk boundaries fell*. A real socket puts them wherever it
likes, and the boundaries that break parsers -- inside a multi-byte character,
between the CR and the LF -- are exactly the ones no hand-written test
happens to pick.

Everything runs against `fakes.wire`, the same bytes the fake upstreams serve,
so this tier and the contract tier cannot drift into agreeing with each other
while both disagree with a real provider.

No sockets, no sleeps, no `hypothesis`. The fuzzer is thirty lines of
`random.Random(seed)`, and the seed is in every failure message: a fuzz
failure you cannot re-run is a flake, not a finding.

    LLMGW_SSE_FUZZ_SEED=12345 pytest tests/unit/test_sse.py
"""

from __future__ import annotations

import json
import os
import random

import pytest
from fakes import wire

from llmgw.errors import FrameTooLarge
from llmgw.sse import SSEEvent, SSEParser

BOM = b"\xef\xbb\xbf"


# --------------------------------------------------------------- helpers


def parse(data: bytes, **kwargs) -> list[SSEEvent]:
    """Whole-buffer parse. The reference every split is compared against."""
    parser = SSEParser(**kwargs)
    events = parser.feed(data)
    events.extend(parser.close())
    return events


def parse_split(data: bytes, cuts: list[int], **kwargs) -> list[SSEEvent]:
    parser = SSEParser(**kwargs)
    events: list[SSEEvent] = []
    prev = 0
    for cut in [*cuts, len(data)]:
        events.extend(parser.feed(data[prev:cut]))
        prev = cut
    events.extend(parser.close())
    return events


def fields(events: list[SSEEvent]) -> list[tuple]:
    """Everything except `raw`, for comparisons across terminator styles."""
    return [(e.event, e.data, e.id, e.retry, e.comment) for e in events]


def datas(events: list[SSEEvent]) -> list[bytes]:
    return [e.data for e in events if not e.is_comment]


def text_of(events: list[SSEEvent], key: str) -> str:
    """Reassemble the model's answer the way a real consumer would."""
    out = []
    for event in events:
        if event.is_comment or event.data == b"[DONE]":
            continue
        payload = json.loads(event.data)
        if key == "openai":
            for choice in payload.get("choices", []):
                out.append(choice["delta"].get("content") or "")
        elif payload.get("type") == "content_block_delta":
            out.append(payload["delta"]["text"])
    return "".join(out)


def with_comments(frames: list[bytes]) -> list[bytes]:
    out: list[bytes] = []
    for frame in frames:
        out.append(wire.openai_comment())
        out.append(frame)
    return out


# ----------------------------------------------------------- basic parse


def test_an_openai_stream_parses_into_one_event_per_frame():
    frames = wire.openai_stream()
    events = parse(wire.joined(frames))
    assert len(events) == len(frames)
    assert all(e.event is None for e in events), "OpenAI frames carry no event: field"
    assert events[-1].data == b"[DONE]"
    assert text_of(events, "openai") == wire.expected_text()


def test_an_anthropic_stream_keeps_the_event_names():
    frames = wire.anthropic_stream(pings=True)
    events = parse(wire.joined(frames))
    assert len(events) == len(frames)
    assert events[0].event == "message_start"
    assert events[-1].event == "message_stop"
    assert events.count(events[0]) == 1
    assert [e.event for e in events].count("ping") == len(wire.TOKENS)
    assert text_of(events, "anthropic") == wire.expected_text()


def test_done_is_delivered_as_bytes_and_not_parsed_as_json():
    """`data: [DONE]` is not JSON. A parser that eagerly json.loads() every
    data field raises on the one frame that ends every OpenAI stream."""
    events = parse(wire.openai_done())
    assert events == [SSEEvent(data=b"[DONE]", raw=b"data: [DONE]\n\n")]
    with pytest.raises(json.JSONDecodeError):
        json.loads(events[0].data)


def test_raw_reconstructs_the_original_stream_byte_for_byte():
    data = wire.joined(wire.anthropic_stream())
    assert b"".join(e.raw for e in parse(data)) == data


# ------------------------------------------------------- split invariance


def test_one_byte_at_a_time_equals_the_whole_buffer():
    for data in (
        wire.joined(wire.openai_stream(heartbeats=True)),
        wire.joined(wire.anthropic_stream(pings=True)),
    ):
        assert parse_split(data, list(range(len(data)))) == parse(data)


def test_every_single_cut_point_gives_the_same_events():
    """Exhaustive rather than random for one stream: if any offset is special,
    this finds it, and it finds it at the smallest reproducer."""
    data = wire.joined(wire.anthropic_stream(tokens=wire.TOKENS[:3], usage=False))
    reference = parse(data)
    for cut in range(len(data) + 1):
        assert parse_split(data, [cut]) == reference, f"cut at byte {cut}"


def test_a_cut_inside_a_multibyte_character_does_not_corrupt_the_text():
    """The bug that decoding-before-framing produces: ' café' split between
    the two bytes of the e-acute becomes two U+FFFD and a wrong answer."""
    data = wire.joined(wire.openai_stream(tokens=(" café", " 日本", " 🌍")))
    inner = data.index(b"caf") + 4  # inside the 2-byte sequence
    assert data[inner - 1] == 0xC3, "test is aimed at the middle of a UTF-8 pair"
    events = parse_split(data, [inner])
    assert text_of(events, "openai") == " café 日本 🌍"


def test_interleaved_empty_feeds_are_no_ops():
    data = wire.joined(wire.openai_stream())
    parser = SSEParser()
    events: list[SSEEvent] = []
    for i in range(0, len(data), 7):
        assert parser.feed(b"") == []
        events.extend(parser.feed(data[i : i + 7]))
        assert parser.feed(b"") == []
    events.extend(parser.close())
    assert events == parse(data)


def test_feed_of_nothing_at_all_produces_nothing():
    parser = SSEParser()
    assert parser.feed(b"") == []
    assert parser.buffered_bytes == 0
    assert parser.close() == []


# ------------------------------------------------------- line terminators


def test_crlf_lf_and_bare_cr_all_frame_identically():
    lf = wire.joined(wire.anthropic_stream())
    assert fields(parse(lf.replace(b"\n", b"\r\n"))) == fields(parse(lf))
    assert fields(parse(lf.replace(b"\n", b"\r"))) == fields(parse(lf))


def test_a_crlf_split_across_chunks_is_not_a_blank_line():
    """The classic one. CR ends chunk N, LF starts chunk N+1. Treat the CR as
    a finished terminator and the LF becomes an empty line, an empty line
    means dispatch, and the frame is cut in half."""
    parser = SSEParser()
    assert parser.feed(b"data: hello\r") == [], "a trailing CR is still ambiguous"
    assert parser.feed(b"\ndata: world\r\n\r\n") == [
        SSEEvent(data=b"hello\nworld", raw=b"data: hello\r\ndata: world\r\n\r\n")
    ]


def test_a_trailing_cr_is_held_rather_than_guessed():
    parser = SSEParser()
    parser.feed(b"data: x\r")
    assert parser.buffered_bytes == 8, "the CR must be retained, not consumed"


def test_a_blank_crlf_line_split_across_chunks_still_dispatches_once():
    """The same ambiguity one line later: the frame terminator itself is the
    CRLF that got split, so getting it wrong emits the frame twice or never."""
    for cut in range(len(b"data: x\r\n\r\n") + 1):
        assert parse_split(b"data: x\r\n\r\n", [cut]) == [
            SSEEvent(data=b"x", raw=b"data: x\r\n\r\n")
        ], f"cut at byte {cut}"


def test_bare_cr_terminators_frame_a_whole_stream():
    events = parse(b"event: ping\rdata: 1\r\rdata: 2\r\r")
    assert events == [
        SSEEvent(event="ping", data=b"1", raw=b"event: ping\rdata: 1\r\r"),
        SSEEvent(data=b"2", raw=b"data: 2\r\r"),
    ]


def test_a_lone_cr_at_end_of_stream_is_a_terminator_at_close():
    assert parse(b"data: x\r") == [SSEEvent(data=b"x", raw=b"data: x\r")]


# ------------------------------------------------------------ field rules


def test_one_optional_space_after_the_colon_is_stripped_and_only_one():
    events = parse(b"data: a\n\ndata:b\n\ndata:  c\n\ndata:\n\n")
    assert datas(events) == [b"a", b"b", b" c", b""]


def test_multiple_data_lines_join_with_a_newline():
    events = parse(b"data: one\ndata: two\ndata:\ndata: four\n\n")
    assert events[0].data == b"one\ntwo\n\nfour"


def test_unknown_fields_are_ignored_and_do_not_make_a_frame():
    events = parse(b"colour: blue\nfoo\n\ndata: real\n\n")
    assert events == [SSEEvent(data=b"real", raw=b"data: real\n\n")]


def test_id_and_retry_are_parsed_and_do_not_leak_into_the_next_frame():
    """WHATWG keeps the last event id for reconnection. A relay reports what
    the frame carried, so `raw` and the parsed fields never disagree."""
    events = parse(b"id: 7\nretry: 2500\ndata: a\n\ndata: b\n\n")
    assert (events[0].id, events[0].retry) == ("7", 2500)
    assert (events[1].id, events[1].retry) == (None, None)


def test_a_non_numeric_retry_is_ignored_rather_than_fatal():
    assert parse(b"retry: soon\ndata: a\n\n")[0].retry is None


def test_an_id_containing_a_nul_is_ignored():
    assert parse(b"id: a\x00b\ndata: x\n\n")[0].id is None


def test_a_frame_with_an_event_but_no_data_still_dispatches():
    """Deliberate deviation: WHATWG fires nothing without data because a
    browser has nothing to deliver. A relay does -- `event: message_stop`
    with no payload is information the surface downstream needs."""
    assert parse(b"event: message_stop\n\n") == [
        SSEEvent(event="message_stop", raw=b"event: message_stop\n\n")
    ]


def test_blank_lines_on_their_own_dispatch_nothing():
    assert parse(b"\n\n\n\ndata: x\n\n") == [SSEEvent(data=b"x", raw=b"data: x\n\n")]


# --------------------------------------------------------------- comments


def test_a_comment_is_emitted_as_a_comment_and_not_as_an_event():
    """OpenRouter really sends these to stop intermediaries idling the socket
    out. They are not data, but they are the only proof of life on a stream
    where the model is still thinking."""
    events = parse(wire.openai_comment())
    assert events == [
        SSEEvent(comment=b"OPENROUTER PROCESSING", raw=b": OPENROUTER PROCESSING\n")
    ]
    assert events[0].is_comment
    assert events[0].data == b"" and events[0].event is None


def test_comments_can_be_suppressed_without_changing_the_events():
    data = wire.joined(with_comments(wire.openai_stream()))
    loud = parse(data)
    quiet = parse(data, emit_comments=False)
    assert [e for e in loud if not e.is_comment] == quiet
    assert len(loud) - len(quiet) == len(wire.openai_stream())


def test_a_comment_inside_a_frame_does_not_join_that_frames_raw():
    events = parse(b"data: a\n: keepalive\ndata: b\n\n")
    assert events == [
        SSEEvent(comment=b"keepalive", raw=b": keepalive\n"),
        SSEEvent(data=b"a\nb", raw=b"data: a\ndata: b\n\n"),
    ]


def test_an_empty_comment_is_still_a_comment():
    """`:\n` is the cheapest possible heartbeat and several providers use it.
    `comment=b""` is falsy, which is why `is_comment` exists."""
    events = parse(b":\n\n")
    assert events == [SSEEvent(comment=b"", raw=b":\n")]
    assert events[0].is_comment


# -------------------------------------------------------------------- BOM


def test_a_leading_bom_is_stripped_once():
    data = wire.joined(wire.openai_stream())
    assert parse(BOM + data) == parse(data)


def test_a_bom_split_across_three_chunks_is_still_a_bom():
    parser = SSEParser()
    for byte in BOM:
        assert parser.feed(bytes([byte])) == []
    assert parser.feed(b"data: x\n\n") == [SSEEvent(data=b"x", raw=b"data: x\n\n")]


def test_bytes_that_only_look_like_a_bom_are_kept_as_content():
    """If 0xEF 0xBB were assumed to be a BOM the first frame's `data:` prefix
    would be eaten and the frame would parse as a real event. It must not."""
    events = parse(b"\xef\xbb\xbcdata: x\n\ndata: y\n\n")
    assert events == [SSEEvent(data=b"y", raw=b"data: y\n\n")]


def test_a_partial_bom_is_held_not_dropped():
    parser = SSEParser()
    assert parser.feed(b"\xef\xbb") == []
    assert parser.buffered_bytes == 2


def test_a_bom_only_applies_to_the_start_of_the_stream():
    events = parse(b"data: x\n\ndata: " + BOM + b"y\n\n")
    assert events[1].data == BOM + b"y", "a mid-stream BOM is content, not framing"


# ------------------------------------------------------------- the bound


def test_a_frame_exactly_at_the_bound_is_accepted():
    frame = b"data: x\n\n"
    assert parse(frame, max_frame_bytes=len(frame)) == [SSEEvent(data=b"x", raw=frame)]


def test_a_frame_one_byte_over_the_bound_raises():
    frame = b"data: x\n\n"
    with pytest.raises(FrameTooLarge):
        parse(frame, max_frame_bytes=len(frame) - 1)


def test_buffered_bytes_never_exceeds_the_bound_before_it_raises():
    """The bound is on BUFFERED BYTES, not on lines or events. Bound the count
    and 200 streams holding one 8 MiB event each reads as a reassuring 200
    while being 1.6 GiB of heap -- mental-model failure #3."""
    parser = SSEParser(max_frame_bytes=64)
    blob = b"data: " + b"x" * 400 + b"\n\n"
    with pytest.raises(FrameTooLarge) as caught:
        for i in range(0, len(blob), 8):
            parser.feed(blob[i : i + 8])
            assert parser.buffered_bytes <= 64
    assert "64" in str(caught.value) and "buffered" in str(caught.value)


def test_many_small_frames_in_one_huge_chunk_do_not_trip_the_bound():
    """The bound is per frame, not per feed. Checking before draining would
    reject a perfectly ordinary 1 MiB read of 5000 tiny deltas."""
    frame = wire.openai_chunk("x")
    data = frame * 500
    assert len(parse(data, max_frame_bytes=len(frame))) == 500
    assert len(data) > 90_000, "the point is that the chunk dwarfs the bound"


def test_an_oversized_frame_poisons_the_parser():
    """Resynchronising mid-frame splices the tail of one JSON object onto the
    head of the next. A stream we could not frame is not one to keep reading."""
    parser = SSEParser(max_frame_bytes=8)
    with pytest.raises(FrameTooLarge):
        parser.feed(b"data: " + b"y" * 50)
    with pytest.raises(FrameTooLarge):
        parser.feed(b"\n\ndata: fine\n\n")
    assert parser.close() == [], "close() stays safe so cleanup can be unconditional"


def test_a_bound_of_zero_or_less_is_refused_at_construction():
    with pytest.raises(ValueError):
        SSEParser(max_frame_bytes=0)


# ------------------------------------------------------------------ close


def test_close_flushes_a_frame_that_never_saw_its_blank_line():
    """Providers really do end without the terminator, and TCP FIN is a fine
    frame boundary. Dropping the tail loses the last token of an answer with
    no error anywhere, which is the worst bug shape there is."""
    assert parse(b"data: hello") == [SSEEvent(data=b"hello", raw=b"data: hello")]
    assert parse(b"data: a\ndata: b\n") == [
        SSEEvent(data=b"a\nb", raw=b"data: a\ndata: b\n")
    ]


def test_a_stream_truncated_mid_frame_still_yields_every_complete_frame():
    frames = wire.openai_stream()
    truncated = wire.joined(frames)[:-4]
    events = parse(truncated)
    assert len(events) == len(frames)
    assert events[-1].data == b"[DON", "the partial tail is surfaced, not silently dropped"


def test_close_is_idempotent_and_feeding_after_it_is_a_bug():
    parser = SSEParser()
    parser.feed(b"data: x")
    assert parser.close() == [SSEEvent(data=b"x", raw=b"data: x")]
    assert parser.close() == []
    with pytest.raises(ValueError):
        parser.feed(b"data: y\n\n")


def test_close_on_a_stream_that_ended_cleanly_adds_nothing():
    parser = SSEParser()
    parser.feed(wire.joined(wire.openai_stream()))
    assert parser.close() == []
    assert parser.buffered_bytes == 0


# ----------------------------------------------------------- the fuzzer


SEED = int(os.environ.get("LLMGW_SSE_FUZZ_SEED", "20260909"))
ITERATIONS = int(os.environ.get("LLMGW_SSE_FUZZ_ITERATIONS", "8000"))


def _corpus() -> dict[str, bytes]:
    """Canonical streams in every terminator style, with and without a BOM.

    The LF forms are what providers actually send; the CR forms exist because
    the spec permits them and because a random cut inside a CRLF pair is the
    boundary that breaks parsers.
    """
    base = {
        "openai": wire.joined(wire.openai_stream(heartbeats=True)),
        "openai-comments": wire.joined(with_comments(wire.openai_stream())),
        "anthropic": wire.joined(wire.anthropic_stream(pings=True)),
    }
    corpus: dict[str, bytes] = {}
    for name, data in base.items():
        corpus[f"{name}/lf"] = data
        corpus[f"{name}/crlf"] = data.replace(b"\n", b"\r\n")
        corpus[f"{name}/cr"] = data.replace(b"\n", b"\r")
        corpus[f"{name}/bom+crlf"] = BOM + data.replace(b"\n", b"\r\n")
        corpus[f"{name}/truncated"] = data[: len(data) * 2 // 3]
    return corpus


CORPUS = _corpus()


def test_random_byte_splits_never_change_the_parsed_events():
    """The property, hammered. Splits land inside multi-byte characters and
    inside CRLF pairs because the offsets are uniform over the stream and the
    corpus is full of both.

    Seeded, and the seed is in the failure message: rerun with
    LLMGW_SSE_FUZZ_SEED=<n> to get the identical case back.
    """
    rng = random.Random(SEED)
    names = list(CORPUS)
    reference = {name: parse(data) for name, data in CORPUS.items()}

    for i in range(ITERATIONS):
        name = rng.choice(names)
        data = CORPUS[name]
        k = rng.choice((1, 1, 2, 3, 5, 8, 20))
        cuts = sorted(rng.randrange(len(data) + 1) for _ in range(k))
        assert parse_split(data, cuts) == reference[name], (
            f"byte-split invariance broken: LLMGW_SSE_FUZZ_SEED={SEED} "
            f"iteration={i} stream={name} cuts={cuts}"
        )


def test_random_splits_agree_with_one_byte_at_a_time_on_truncated_streams():
    """A truncation lands mid-frame, so this exercises `close()`'s flush at
    every possible cut of the tail as well as the framing itself."""
    rng = random.Random(SEED ^ 0x5EED)
    names = [n for n in CORPUS if n.endswith("truncated")]
    # Per-byte feeding is the harshest schedule there is, so it is the
    # reference rather than the whole-buffer parse: if the two ever disagree
    # the whole-buffer parse is no longer trustworthy as an oracle.
    reference = {n: parse_split(CORPUS[n], list(range(len(CORPUS[n])))) for n in names}

    for i in range(2000):
        name = rng.choice(names)
        data = CORPUS[name]
        cuts = sorted(rng.randrange(len(data) + 1) for _ in range(rng.randint(1, 6)))
        assert parse_split(data, cuts) == reference[name], (
            f"LLMGW_SSE_FUZZ_SEED={SEED} iteration={i} stream={name} cuts={cuts}"
        )

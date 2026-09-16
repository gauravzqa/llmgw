"""`Usage.stop_reason` (PLAN-2 A3): the provider always said why it stopped.

Before this field an answer cut at `max_tokens`, a refusal, a context-window
overflow and DeepSeek's `insufficient_system_resource` all recorded as
`completed`. Fixtures are the shapes the capability sweeps recorded
(capabilities/{openai,anthropic,deepseek}.md §4), driven through the real
parser so a hand-written frame cannot drift from the wire.
"""

from __future__ import annotations

import json

import pytest
from fakes import wire

from llmgw.sse import SSEParser
from llmgw.surfaces import ANTHROPIC_MESSAGES, OPENAI_CHAT
from llmgw.surfaces.base import (
    STOP_REASONS,
    Usage,
    normalise_anthropic_stop_reason,
    normalise_openai_finish_reason,
)


def _one(frame: bytes):
    parser = SSEParser()
    events = parser.feed(frame)
    events.extend(parser.close())
    assert len(events) == 1
    return events[0]


def _openai_finish(reason, *, index: int = 0) -> bytes:
    return wire._frame({
        "id": "chatcmpl-x", "object": "chat.completion.chunk", "model": wire.MODEL,
        "choices": [{"index": index, "delta": {}, "finish_reason": reason}],
    })


def _anthropic_message_delta(reason) -> bytes:
    return wire._frame(
        {"type": "message_delta", "delta": {"stop_reason": reason, "stop_sequence": None},
         "usage": {"output_tokens": 7}},
        event="message_delta",
    )


# ------------------------------------------------------------- the closed set


def test_every_mapping_lands_inside_the_closed_set():
    for value in ("stop", "length", "tool_calls", "function_call", "content_filter",
                  "insufficient_system_resource", "aborted", "something-new", 42):
        assert normalise_openai_finish_reason(value) in STOP_REASONS
    for value in ("end_turn", "stop_sequence", "max_tokens", "tool_use", "pause_turn",
                  "refusal", "model_context_window_exceeded", "compaction", "new", 1.5):
        assert normalise_anthropic_stop_reason(value) in STOP_REASONS


def test_null_means_not_stopped_yet_and_maps_to_none():
    assert normalise_openai_finish_reason(None) is None
    assert normalise_anthropic_stop_reason(None) is None


# ------------------------------------------------------------- OpenAI dialect


@pytest.mark.parametrize(
    ("finish_reason", "expected"),
    [
        ("stop", "stop"),
        ("length", "length"),
        ("tool_calls", "tool_calls"),
        ("function_call", "tool_calls"),
        ("content_filter", "content_filter"),
        ("insufficient_system_resource", "provider_shed"),   # DeepSeek shed inside a 200
        ("aborted", "provider_shed"),
        ("compaction", "unknown"),
        ("never-heard-of-it", "unknown"),
    ],
)
def test_openai_finish_reason_is_recorded_from_the_stream(finish_reason, expected):
    usage = Usage()
    for frame in (*wire.openai_stream(usage=False)[:-1], _openai_finish(finish_reason),
                  wire.openai_usage_chunk()):
        OPENAI_CHAT.apply_usage(_one(frame), usage)
    assert usage.stop_reason == expected
    assert usage.exact, "the stop reason must not disturb usage exactness"


def test_openai_last_non_null_finish_reason_wins_across_choices_and_chunks():
    usage = Usage()
    OPENAI_CHAT.apply_usage(_one(wire.openai_chunk("a")), usage)      # finish_reason null
    assert usage.stop_reason is None
    OPENAI_CHAT.apply_usage(_one(_openai_finish("length", index=0)), usage)
    OPENAI_CHAT.apply_usage(_one(_openai_finish(None, index=1)), usage)  # null: no change
    assert usage.stop_reason == "length"
    OPENAI_CHAT.apply_usage(_one(_openai_finish("stop", index=1)), usage)
    assert usage.stop_reason == "stop"


def test_openai_stop_reason_from_a_buffered_body():
    body = {
        "id": "chatcmpl-x", "object": "chat.completion", "model": "gpt-4o-mini-2024-07-18",
        "choices": [{"index": 0, "message": {"role": "assistant", "content": "pong"},
                     "finish_reason": "length"}],
        "usage": {"prompt_tokens": 14, "completion_tokens": 8, "total_tokens": 22},
    }
    assert OPENAI_CHAT.stop_reason_from_body(body) == "length"
    assert OPENAI_CHAT.stop_reason_from_body({"choices": "not a list"}) is None
    assert OPENAI_CHAT.stop_reason_from_body({}) is None


def test_a_normal_openai_stream_without_a_finish_reason_leaves_it_none():
    """The canonical fake stream never sets `finish_reason`; that is a fact
    about the fake, and the field must say so rather than invent `stop`."""
    usage = Usage()
    for frame in wire.openai_stream():
        OPENAI_CHAT.apply_usage(_one(frame), usage)
    assert usage.stop_reason is None


# ---------------------------------------------------------- Anthropic dialect


@pytest.mark.parametrize(
    ("stop_reason", "expected"),
    [
        ("end_turn", "stop"),
        ("stop_sequence", "stop"),
        ("max_tokens", "length"),
        ("tool_use", "tool_calls"),
        ("pause_turn", "pause_turn"),
        ("refusal", "refusal"),
        ("model_context_window_exceeded", "context_window_exceeded"),
        ("compaction", "unknown"),
        ("brand-new", "unknown"),
    ],
)
def test_anthropic_stop_reason_is_recorded_from_message_delta(stop_reason, expected):
    usage = Usage()
    ANTHROPIC_MESSAGES.apply_usage(_one(wire.anthropic_message_start()), usage)
    ANTHROPIC_MESSAGES.apply_usage(_one(_anthropic_message_delta(stop_reason)), usage)
    assert usage.stop_reason == expected
    assert usage.output_tokens == 7 and usage.exact


def test_the_canonical_anthropic_stream_ends_with_stop():
    usage = Usage()
    for frame in wire.anthropic_stream():
        ANTHROPIC_MESSAGES.apply_usage(_one(frame), usage)
    assert usage.stop_reason == "stop"   # fixture says end_turn


def test_anthropic_stop_reason_from_a_buffered_body():
    body = json.loads(json.dumps({
        "id": "msg_x", "type": "message", "role": "assistant", "model": "claude-haiku-4-5",
        "content": [{"type": "text", "text": "pong"}],
        "stop_reason": "max_tokens", "stop_sequence": None,
        "usage": {"input_tokens": 12, "output_tokens": 8},
    }))
    assert ANTHROPIC_MESSAGES.stop_reason_from_body(body) == "length"
    assert ANTHROPIC_MESSAGES.stop_reason_from_body({"stop_reason": None}) is None
    assert ANTHROPIC_MESSAGES.stop_reason_from_body({}) is None


def test_stop_reason_helpers_never_raise_on_garbage():
    assert OPENAI_CHAT.stop_reason_from_body({"choices": [None, 3, {"finish_reason": {}}]}) \
        == "unknown"
    assert ANTHROPIC_MESSAGES.stop_reason_from_body({"stop_reason": ["x"]}) == "unknown"

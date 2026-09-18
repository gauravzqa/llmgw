"""The OpenAI Responses wire, as the fake serves it (PLAN-2 Phase F).

Shapes follow the 17 Sep 2026 live captures in
`capabilities/captures-responses.md` byte-for-byte where it matters: every
frame is `event: <type>\\ndata: <json>\\n\\n` with `data.type` equal to the
event name and a monotonically increasing `sequence_number` from 0; there
is NO `data: [DONE]`; usage rides only on the terminal frame; the response
`model` is the provider's snapshot id, not the one the client sent; deltas
carry `logprobs: []` and OpenAI's `obfuscation` padding; hosted web search
is an `output[]` item at index 0 with three lifecycle events and its count in
`response.tool_usage.web_search.num_requests`.

Two fixtures the tests assert on and nothing else should change:

    USAGE            input 20 (cached 8) / output 12 (reasoning 4) / total 32
    REASONING_USAGE  input 20 (cached 8) / output 52 (reasoning 40) / total 72

The gateway normalises the cached subset OUT of input (`surfaces/base.py`),
so a record for the `ok` stream reads input 12 / cache_read 8 / output 12 /
reasoning 4.

Like `fakes/voice.py`, this module builds bytes and dicts only;
`fakes/upstream.py` owns the HTTP response, the pacing and the counters, so
this file never imports it.
"""

from __future__ import annotations

import json
from typing import Any

KNOWN_MODELS: dict[str, str] = {
    # request model -> the id echoed in `response.model` (probe 2: OpenAI
    # answers with the dated snapshot; probe 10a: DeepSeek answers with the
    # canonical short name).
    "fake-echo": "fake-echo",
    "fake-echo-incumbent": "fake-echo-incumbent",
    "gpt-4o-mini": "gpt-4o-mini-2024-07-18",
    "gpt-4o-mini-2024-07-18": "gpt-4o-mini-2024-07-18",
    "gpt-5-nano": "gpt-5-nano-2025-08-07",
    "gpt-5-nano-2025-08-07": "gpt-5-nano-2025-08-07",
    "deepseek-v4-flash": "deepseek-flash",
    "deepseek-flash": "deepseek-flash",
    "deepseek-v4-pro": "deepseek-v4-pro",
}
"""The wire ids this fake answers to. Anything else -- a catalog id such as
`openai.gpt-4o-mini` that leaked upstream unrewritten -- is a 404
`model_not_found` in OpenAI's exact envelope (probe 8a), which is how the
contract tier proves the gateway rewrote `model` before forwarding."""

TEXT_DELTAS: tuple[str, ...] = ("Hello", " café", " 🌍")
"""Three deltas: 1-byte, 2-byte and 4-byte UTF-8, like `wire.TOKENS`."""

REASONING_DELTAS: tuple[str, ...] = ("Thinking", " about it.")

USAGE: dict[str, Any] = {
    "input_tokens": 20,
    "input_tokens_details": {"cache_write_tokens": 0, "cached_tokens": 8},
    "output_tokens": 12,
    "output_tokens_details": {"reasoning_tokens": 4},
    "total_tokens": 32,
}

REASONING_USAGE: dict[str, Any] = {
    "input_tokens": 20,
    "input_tokens_details": {"cache_write_tokens": 0, "cached_tokens": 8},
    "output_tokens": 52,
    "output_tokens_details": {"reasoning_tokens": 40},
    "total_tokens": 72,
}

RESPONSE_ID = "resp_fake0000000000000000000000000000000000000000000000"
MESSAGE_ID = "msg_fake000000000000000000000000000000000000000000000"
REASONING_ID = "rs_fake0000000000000000000000000000000000000000000000"
WEB_SEARCH_ID = "ws_fake0000000000000000000000000000000000000000000000"
CREATED_AT = 1_789_686_483

FAILED_ERROR: dict[str, Any] = {
    "code": "server_error",
    "message": "The model produced an internal error while generating.",
}
"""`response.error` on the `responses-failed` terminal frame (per spec; the
probe could not trigger one live -- see the report's "unverified" note)."""

ERROR_EVENT: dict[str, Any] = {
    "type": "error",
    "code": "rate_limit_exceeded",
    "message": "Rate limit reached for the organization while streaming.",
    "param": None,
}
"""The `event: error` frame body (spec shape; `sequence_number` is added)."""


def frame(payload: dict[str, Any]) -> bytes:
    """One SSE frame. The `event:` line is `data.type`, always."""
    return (f"event: {payload['type']}\n"
            f"data: {json.dumps(payload, ensure_ascii=False)}\n\n").encode()


def echoed_model(requested: str) -> str:
    return KNOWN_MODELS.get(requested, requested)


def model_not_found_body(requested: str) -> bytes:
    """Probe 8a, verbatim shape."""
    return json.dumps({"error": {
        "message": f"The model `{requested}` does not exist or you do not have access to it.",
        "type": "invalid_request_error", "param": None, "code": "model_not_found",
    }}).encode()


# --------------------------------------------------------------------------
# The Response object
# --------------------------------------------------------------------------


def _message_item(text: str, *, status: str = "completed") -> dict[str, Any]:
    return {
        "id": MESSAGE_ID, "type": "message", "status": status, "role": "assistant",
        "content": [{"type": "output_text", "annotations": [], "logprobs": [],
                     "text": text}],
    }


def _reasoning_item(*, summary_text: str | None = None) -> dict[str, Any]:
    item: dict[str, Any] = {
        "id": REASONING_ID, "type": "reasoning", "status": "completed",
        "content": [], "summary": [],
        "encrypted_content": "gAAAAAfakefakefakefake",
    }
    if summary_text is not None:
        item["summary"] = [{"type": "summary_text", "text": summary_text}]
    return item


def _web_search_item(*, status: str = "completed") -> dict[str, Any]:
    action: dict[str, Any] = {"type": "search"}
    if status == "completed":
        action.update({"queries": ["top headlines news today"],
                       "query": "top headlines news today"})
    return {"id": WEB_SEARCH_ID, "type": "web_search_call", "status": status,
            "action": action}


def response_object(
    *,
    model: str,
    status: str,
    output: list[dict[str, Any]],
    usage: dict[str, Any] | None,
    previous_response_id: str | None = None,
    incomplete_reason: str | None = None,
    error: dict[str, Any] | None = None,
    web_search_requests: int = 0,
    max_output_tokens: int | None = None,
) -> dict[str, Any]:
    """The envelope of probe 2, with the fields the tests read filled in."""
    return {
        "id": RESPONSE_ID,
        "object": "response",
        "created_at": CREATED_AT,
        "status": status,
        "background": False,
        "completed_at": CREATED_AT + 1 if status == "completed" else None,
        "error": error,
        "incomplete_details": (
            {"reason": incomplete_reason} if incomplete_reason else None
        ),
        "instructions": None,
        "max_output_tokens": max_output_tokens,
        "model": echoed_model(model),
        "output": output,
        "parallel_tool_calls": True,
        "previous_response_id": previous_response_id,
        "reasoning": {"context": None, "effort": None, "summary": None},
        "service_tier": "default" if status != "in_progress" else "auto",
        "store": True,
        "temperature": 1.0,
        "text": {"format": {"type": "text"}, "verbosity": "medium"},
        "tool_choice": "auto",
        "tool_usage": {"web_search": {"num_requests": web_search_requests}},
        "tools": [],
        "truncation": "disabled",
        "usage": usage,
        "user": None,
        "metadata": {},
    }


# --------------------------------------------------------------------------
# Streams. Each returns the complete list of frames for one mode; the caller
# writes them one per ASGI write, paced.
# --------------------------------------------------------------------------


class _Seq:
    """Hands out `sequence_number`s 0, 1, 2, ... for one stream."""

    def __init__(self) -> None:
        self.n = -1

    def __call__(self) -> int:
        self.n += 1
        return self.n


def _message_frames(
    seq: _Seq, deltas: tuple[str, ...], *, output_index: int, item_status: str = "completed",
) -> list[bytes]:
    """`output_item.added` -> `content_part.added` -> deltas -> the three
    `.done` frames, exactly the probe-3 shape."""
    text = "".join(deltas)
    frames = [
        frame({"type": "response.output_item.added",
               "item": {"id": MESSAGE_ID, "type": "message", "status": "in_progress",
                        "content": [], "role": "assistant"},
               "output_index": output_index, "sequence_number": seq()}),
        frame({"type": "response.content_part.added", "content_index": 0,
               "item_id": MESSAGE_ID, "output_index": output_index,
               "part": {"type": "output_text", "annotations": [], "logprobs": [],
                        "text": ""},
               "sequence_number": seq()}),
    ]
    for delta in deltas:
        frames.append(frame({
            "type": "response.output_text.delta", "content_index": 0, "delta": delta,
            "item_id": MESSAGE_ID, "logprobs": [], "obfuscation": "zb2VZr3uMaj",
            "output_index": output_index, "sequence_number": seq(),
        }))
    frames += [
        frame({"type": "response.output_text.done", "content_index": 0,
               "item_id": MESSAGE_ID, "logprobs": [], "output_index": output_index,
               "sequence_number": seq(), "text": text}),
        frame({"type": "response.content_part.done", "content_index": 0,
               "item_id": MESSAGE_ID, "output_index": output_index,
               "part": {"type": "output_text", "annotations": [], "logprobs": [],
                        "text": text},
               "sequence_number": seq()}),
        frame({"type": "response.output_item.done",
               "item": _message_item(text, status=item_status),
               "output_index": output_index, "sequence_number": seq()}),
    ]
    return frames


def _opening(seq: _Seq, model: str, previous_response_id: str | None,
             max_output_tokens: int | None) -> list[bytes]:
    created = response_object(
        model=model, status="in_progress", output=[], usage=None,
        previous_response_id=previous_response_id, max_output_tokens=max_output_tokens,
    )
    return [
        frame({"type": "response.created", "response": created, "sequence_number": seq()}),
        frame({"type": "response.in_progress", "response": created,
               "sequence_number": seq()}),
    ]


def _terminal(seq: _Seq, kind: str, response: dict[str, Any]) -> bytes:
    return frame({"type": kind, "response": response, "sequence_number": seq()})


def stream_frames(
    mode: str, *, model: str, previous_response_id: str | None = None,
    max_output_tokens: int | None = None,
) -> list[bytes]:
    """Every frame of one streamed response for `mode`.

    ok                    created, in_progress, message(3 deltas), completed
    responses-incomplete  same, ending in `response.incomplete` (max_output_tokens)
    responses-failed      2 deltas then `response.failed` -- and NOTHING after it
    responses-error-event 2 deltas then `event: error` -- and nothing after it
    responses-web-search  web_search_call item + 3 lifecycle events, then the
                          message at output_index 1, completed with
                          tool_usage.web_search.num_requests = 1
    responses-reasoning   reasoning item with summary deltas, then the message,
                          completed with REASONING_USAGE
    """
    seq = _Seq()
    common = {"model": model, "previous_response_id": previous_response_id,
              "max_output_tokens": max_output_tokens}
    frames = _opening(seq, model, previous_response_id, max_output_tokens)

    if mode == "ok":
        frames += _message_frames(seq, TEXT_DELTAS, output_index=0)
        done = response_object(status="completed", usage=USAGE,
                               output=[_message_item("".join(TEXT_DELTAS))], **common)
        frames.append(_terminal(seq, "response.completed", done))
        return frames

    if mode == "responses-incomplete":
        frames += _message_frames(seq, TEXT_DELTAS, output_index=0, item_status="incomplete")
        done = response_object(
            status="incomplete", usage=USAGE, incomplete_reason="max_output_tokens",
            output=[_message_item("".join(TEXT_DELTAS), status="incomplete")], **common,
        )
        frames.append(_terminal(seq, "response.incomplete", done))
        return frames

    if mode in ("responses-failed", "responses-error-event"):
        # Two deltas the client has already seen, then the failure. The
        # stream must END here: a `response.completed` after a failure is
        # exactly the frame C2 forbids the gateway from inventing, so the
        # fake must not send one either or the test proves nothing.
        two = TEXT_DELTAS[:2]
        frames += _message_frames(seq, two, output_index=0)[:-3]  # no .done frames
        if mode == "responses-failed":
            failed = response_object(status="failed", usage=None, error=dict(FAILED_ERROR),
                                     output=[], **common)
            frames.append(_terminal(seq, "response.failed", failed))
        else:
            frames.append(frame({**ERROR_EVENT, "sequence_number": seq()}))
        return frames

    if mode == "responses-web-search":
        frames += [
            frame({"type": "response.output_item.added",
                   "item": _web_search_item(status="in_progress"),
                   "output_index": 0, "sequence_number": seq()}),
            frame({"type": "response.web_search_call.in_progress",
                   "item_id": WEB_SEARCH_ID, "output_index": 0, "sequence_number": seq()}),
            frame({"type": "response.web_search_call.searching",
                   "item_id": WEB_SEARCH_ID, "output_index": 0, "sequence_number": seq()}),
            frame({"type": "response.web_search_call.completed",
                   "item_id": WEB_SEARCH_ID, "output_index": 0, "sequence_number": seq()}),
            frame({"type": "response.output_item.done", "item": _web_search_item(),
                   "output_index": 0, "sequence_number": seq()}),
        ]
        frames += _message_frames(seq, TEXT_DELTAS, output_index=1)
        done = response_object(
            status="completed", usage=USAGE, web_search_requests=1,
            output=[_web_search_item(), _message_item("".join(TEXT_DELTAS))], **common,
        )
        frames.append(_terminal(seq, "response.completed", done))
        return frames

    if mode == "responses-reasoning":
        summary = "".join(REASONING_DELTAS)
        frames += [
            frame({"type": "response.output_item.added",
                   "item": {"id": REASONING_ID, "type": "reasoning", "status": "in_progress",
                            "content": [], "summary": []},
                   "output_index": 0, "sequence_number": seq()}),
            frame({"type": "response.reasoning_summary_part.added",
                   "item_id": REASONING_ID, "output_index": 0, "summary_index": 0,
                   "part": {"type": "summary_text", "text": ""}, "sequence_number": seq()}),
        ]
        for delta in REASONING_DELTAS:
            frames.append(frame({
                "type": "response.reasoning_summary_text.delta", "delta": delta,
                "item_id": REASONING_ID, "output_index": 0, "summary_index": 0,
                "obfuscation": "Q3tE9", "sequence_number": seq(),
            }))
        frames += [
            frame({"type": "response.reasoning_summary_text.done", "item_id": REASONING_ID,
                   "output_index": 0, "summary_index": 0, "text": summary,
                   "sequence_number": seq()}),
            frame({"type": "response.reasoning_summary_part.done", "item_id": REASONING_ID,
                   "output_index": 0, "summary_index": 0,
                   "part": {"type": "summary_text", "text": summary},
                   "sequence_number": seq()}),
            frame({"type": "response.output_item.done",
                   "item": _reasoning_item(summary_text=summary),
                   "output_index": 0, "sequence_number": seq()}),
        ]
        frames += _message_frames(seq, TEXT_DELTAS, output_index=1)
        done = response_object(
            status="completed", usage=REASONING_USAGE,
            output=[_reasoning_item(summary_text=summary),
                    _message_item("".join(TEXT_DELTAS))],
            **common,
        )
        frames.append(_terminal(seq, "response.completed", done))
        return frames

    raise ValueError(f"no Responses stream for mode {mode!r}")


# --------------------------------------------------------------------------
# Buffered bodies
# --------------------------------------------------------------------------


def json_body(
    mode: str, *, model: str, previous_response_id: str | None = None,
    max_output_tokens: int | None = None,
) -> dict[str, Any]:
    """The non-streamed Response object for `mode`. `responses-failed` and
    `responses-error-event` have no buffered form in the real API's happy
    path; they answer a `status: failed` object so a client that asked for
    the failure mode without `stream` still gets a shape from the spec."""
    text = "".join(TEXT_DELTAS)
    common = {"model": model, "previous_response_id": previous_response_id,
              "max_output_tokens": max_output_tokens}
    if mode == "ok":
        return response_object(status="completed", usage=USAGE,
                               output=[_message_item(text)], **common)
    if mode == "responses-incomplete":
        return response_object(
            status="incomplete", usage=USAGE, incomplete_reason="max_output_tokens",
            output=[_message_item(text, status="incomplete")], **common,
        )
    if mode in ("responses-failed", "responses-error-event"):
        return response_object(status="failed", usage=None, error=dict(FAILED_ERROR),
                               output=[], **common)
    if mode == "responses-web-search":
        return response_object(
            status="completed", usage=USAGE, web_search_requests=1,
            output=[_web_search_item(), _message_item(text)], **common,
        )
    if mode == "responses-reasoning":
        summary = "".join(REASONING_DELTAS)
        return response_object(
            status="completed", usage=REASONING_USAGE,
            output=[_reasoning_item(summary_text=summary), _message_item(text)], **common,
        )
    raise ValueError(f"no Responses body for mode {mode!r}")


RESPONSES_MODES: tuple[str, ...] = (
    "responses-incomplete",
    "responses-failed",
    "responses-error-event",
    "responses-web-search",
    "responses-reasoning",
)
"""The modes this module serves besides `ok`, in `fakes.upstream.MODES`."""

SERVED_MODES: frozenset[str] = frozenset({"ok", *RESPONSES_MODES})


def expected_text() -> str:
    return "".join(TEXT_DELTAS)


__all__ = [
    "ERROR_EVENT",
    "FAILED_ERROR",
    "KNOWN_MODELS",
    "REASONING_DELTAS",
    "REASONING_USAGE",
    "RESPONSES_MODES",
    "SERVED_MODES",
    "TEXT_DELTAS",
    "USAGE",
    "echoed_model",
    "expected_text",
    "frame",
    "json_body",
    "model_not_found_body",
    "response_object",
    "stream_frames",
]

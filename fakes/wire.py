"""Canonical wire bytes for both surfaces. One source of truth.

Imported by the fake upstreams (which serve these bytes) AND by the surface
tests (which parse them). That sharing is deliberate: if the fakes had their
own idea of what an Anthropic stream looks like and the surface tests had
another, the contract tier would be two mocks agreeing with each other while
neither matches a real provider.

Every builder returns a list of complete SSE frames, each ending in a blank
line. Callers concatenate them, or -- for the `split-frames` mode -- slice the
concatenation at arbitrary byte offsets.

Note the multi-byte content. `TOKENS` deliberately contains a 2-byte, a 3-byte
and a 4-byte UTF-8 sequence, because a parser that only ever sees ASCII is a
parser whose boundary handling has never been tested.
"""

from __future__ import annotations

import json

# 1-byte, 2-byte, 3-byte and 4-byte UTF-8 respectively.
TOKENS: tuple[str, ...] = ("Hello", " café", " 日本", " 🌍", " done")

MODEL = "fake-echo"

# Usage numbers the surface tests assert on. Chosen so every field is
# distinguishable: no two are equal, so a transposed assignment fails loudly.
#
# The names matter more than the values. The first version of this file had a
# single `INPUT_TOKENS = 100` used as BOTH OpenAI's `prompt_tokens` and
# Anthropic's `input_tokens` -- and those are not the same quantity:
#
#     OpenAI    prompt_tokens  INCLUDES cached tokens (details.cached_tokens
#                              is a SUBSET of it)
#     Anthropic input_tokens   EXCLUDES cache_read and cache_creation, which
#                              are reported as separate siblings
#
# So no normalisation convention could ever make both surfaces report
# `input_tokens == INPUT_TOKENS`, and a test written against the shared
# constant had to be wrong on one of the two surfaces. The fix is to name the
# disjoint quantity and DERIVE each provider's convention from it, so the
# fixture states which convention it is speaking in every single place.
#
# This is the same bug the gateway itself would have shipped in its cost
# table: getting it backwards inflates the bill by the entire size of every
# cache hit -- invisible in an uncached test, ~94% of the prompt in a
# cache-heavy voice workload.

FRESH_INPUT_TOKENS = 60
"""Prompt tokens that were NOT served from cache. The disjoint quantity."""

CACHE_READ_TOKENS = 40
CACHE_WRITE_TOKENS = 7
OUTPUT_TOKENS = 5

OPENAI_PROMPT_TOKENS = FRESH_INPUT_TOKENS + CACHE_READ_TOKENS
"""OpenAI's inclusive `prompt_tokens`. Note it does not carry a cache-creation
count at all -- the chat API does not report one -- so cache writes are absent
from this surface rather than zero, which is a fact and not a measurement."""

ANTHROPIC_INPUT_TOKENS = FRESH_INPUT_TOKENS
"""Anthropic's exclusive `input_tokens`: fresh prompt only, with cache reads
and creations reported as siblings."""


def _frame(data: object, *, event: str | None = None) -> bytes:
    """One SSE frame. `data` is JSON unless it is already a str (for [DONE])."""
    payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False)
    head = f"event: {event}\n" if event else ""
    return f"{head}data: {payload}\n\n".encode()


# --------------------------------------------------------------------------
# OpenAI chat completions
# --------------------------------------------------------------------------


def openai_chunk(text: str, *, index: int = 0) -> bytes:
    return _frame(
        {
            "id": "chatcmpl-fake",
            "object": "chat.completion.chunk",
            "created": 1_757_000_000,
            "model": MODEL,
            "choices": [
                {"index": index, "delta": {"content": text}, "finish_reason": None}
            ],
        }
    )


def openai_heartbeat() -> bytes:
    """A chunk with an EMPTY choices array. Real providers emit these; they
    are the OpenAI-shaped equivalent of a ping, and a parser that treats
    `choices[0]` as always present crashes on the first one."""
    return _frame(
        {
            "id": "chatcmpl-fake",
            "object": "chat.completion.chunk",
            "created": 1_757_000_000,
            "model": MODEL,
            "choices": [],
        }
    )


def openai_comment(text: str = "OPENROUTER PROCESSING") -> bytes:
    """An SSE comment line. OpenRouter really sends these to keep proxies from
    idling the connection out. They carry no data and must not be parsed as
    an event -- but they ARE evidence of liveness."""
    return f": {text}\n\n".encode()


def openai_usage_chunk() -> bytes:
    """The final chunk when `stream_options.include_usage` is set: empty
    choices, populated usage. Without that request option this frame never
    arrives, which is why cost has an `estimated` basis at all."""
    return _frame(
        {
            "id": "chatcmpl-fake",
            "object": "chat.completion.chunk",
            "created": 1_757_000_000,
            "model": MODEL,
            "choices": [],
            "usage": {
                "prompt_tokens": OPENAI_PROMPT_TOKENS,
                "completion_tokens": OUTPUT_TOKENS,
                "total_tokens": OPENAI_PROMPT_TOKENS + OUTPUT_TOKENS,
                "prompt_tokens_details": {"cached_tokens": CACHE_READ_TOKENS},
            },
        }
    )


def openai_done() -> bytes:
    """The terminal marker. NOT valid JSON, which catches every parser that
    assumes it can json.loads() every data field."""
    return _frame("[DONE]")


def openai_stream(*, tokens=TOKENS, usage=True, heartbeats=False) -> list[bytes]:
    frames: list[bytes] = []
    for tok in tokens:
        if heartbeats:
            frames.append(openai_heartbeat())
        frames.append(openai_chunk(tok))
    if usage:
        frames.append(openai_usage_chunk())
    frames.append(openai_done())
    return frames


def openai_error_body(kind: str = "server_error", message: str = "boom") -> bytes:
    return json.dumps({"error": {"type": kind, "message": message, "code": kind}}).encode()


# --------------------------------------------------------------------------
# Anthropic messages
# --------------------------------------------------------------------------


def anthropic_message_start() -> bytes:
    """Carries INPUT usage. Anthropic reports input at the start and output at
    the end, so a stream cut in the middle has exact input and estimated
    output -- the asymmetry that makes `cost_basis` a per-request field rather
    than a global one."""
    return _frame(
        {
            "type": "message_start",
            "message": {
                "id": "msg_fake",
                "type": "message",
                "role": "assistant",
                "model": MODEL,
                "content": [],
                "stop_reason": None,
                "usage": {
                    "input_tokens": ANTHROPIC_INPUT_TOKENS,
                    "cache_read_input_tokens": CACHE_READ_TOKENS,
                    "cache_creation_input_tokens": CACHE_WRITE_TOKENS,
                    "output_tokens": 1,
                },
            },
        },
        event="message_start",
    )


def anthropic_block_start(index: int = 0) -> bytes:
    return _frame(
        {"type": "content_block_start", "index": index,
         "content_block": {"type": "text", "text": ""}},
        event="content_block_start",
    )


def anthropic_delta(text: str, *, index: int = 0) -> bytes:
    return _frame(
        {"type": "content_block_delta", "index": index,
         "delta": {"type": "text_delta", "text": text}},
        event="content_block_delta",
    )


def anthropic_ping() -> bytes:
    """A real event with a real `event:` name -- not a comment. Resets
    liveness, never progress (CONTRACTS.md C7)."""
    return _frame({"type": "ping"}, event="ping")


def anthropic_block_stop(index: int = 0) -> bytes:
    return _frame({"type": "content_block_stop", "index": index},
                  event="content_block_stop")


def anthropic_message_delta() -> bytes:
    """Carries OUTPUT usage plus the stop reason."""
    return _frame(
        {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
         "usage": {"output_tokens": OUTPUT_TOKENS}},
        event="message_delta",
    )


def anthropic_message_stop() -> bytes:
    """The terminal marker for this surface."""
    return _frame({"type": "message_stop"}, event="message_stop")


def anthropic_error(kind: str = "overloaded_error", message: str = "Overloaded") -> bytes:
    """An in-band error inside a 200 response. HTTP said fine; the protocol
    said otherwise."""
    return _frame({"type": "error", "error": {"type": kind, "message": message}},
                  event="error")


def anthropic_stream(*, tokens=TOKENS, usage=True, pings=False) -> list[bytes]:
    frames = [anthropic_message_start(), anthropic_block_start()]
    for tok in tokens:
        if pings:
            frames.append(anthropic_ping())
        frames.append(anthropic_delta(tok))
    frames.append(anthropic_block_stop())
    if usage:
        frames.append(anthropic_message_delta())
    frames.append(anthropic_message_stop())
    return frames


def anthropic_error_body(kind: str = "overloaded_error", message: str = "Overloaded") -> bytes:
    return json.dumps({"type": "error", "error": {"type": kind, "message": message}}).encode()


# --------------------------------------------------------------------------
# Helpers shared by the fakes and the parser tests
# --------------------------------------------------------------------------


def joined(frames: list[bytes]) -> bytes:
    return b"".join(frames)


def expected_text(tokens=TOKENS) -> str:
    """What a correct client must end up with. The assertion target for every
    passthrough and parser test."""
    return "".join(tokens)

"""`/v1/embeddings` (Phase C3): input tokens only, exact when reported; and
the buffered-usage path that Phase C gave every non-streamed response."""

from __future__ import annotations

import json

from llmgw.surfaces import ANTHROPIC_MESSAGES, EMBEDDINGS, OPENAI_CHAT
from llmgw.surfaces.base import Usage, usage_from_body


def test_embeddings_reads_the_model_and_the_prompt_tokens():
    facts = EMBEDDINGS.parse_request(json.dumps(
        {"model": "openai.text-embedding-3-small", "input": "hello"}
    ).encode())
    assert facts.model == "openai.text-embedding-3-small" and facts.stream is False
    usage = Usage()
    usage_from_body(EMBEDDINGS, {"object": "list", "data": [],
                                 "usage": {"prompt_tokens": 7, "total_tokens": 7}}, usage)
    assert usage.input_tokens == 7 and usage.output_tokens == 0
    assert usage.exact is True


def test_embeddings_without_usage_stays_inexact():
    usage = Usage()
    usage_from_body(EMBEDDINGS, {"object": "list", "data": []}, usage)
    assert usage.exact is False and usage.input_tokens == 0


def test_a_buffered_openai_chat_body_is_billed_from_its_usage_object():
    """Finding 50: before Phase C the buffered path recorded no usage at all."""
    usage = Usage()
    body = {"id": "x", "model": "gpt-4o-mini-2024-07-18", "choices": [
        {"index": 0, "finish_reason": "stop",
         "message": {"role": "assistant", "content": "pong"}}],
        "usage": {"prompt_tokens": 14, "completion_tokens": 1, "total_tokens": 15,
                  "prompt_tokens_details": {"cached_tokens": 4},
                  "completion_tokens_details": {"reasoning_tokens": 0}}}
    usage_from_body(OPENAI_CHAT, body, usage)
    assert usage.input_tokens == 10 and usage.cache_read_tokens == 4
    assert usage.output_tokens == 1 and usage.exact is True
    assert usage.stop_reason == "stop"


def test_a_buffered_anthropic_message_body_is_billed_from_its_usage_object():
    usage = Usage()
    body = {"id": "msg", "type": "message", "role": "assistant", "model": "claude-haiku-4-5",
            "content": [{"type": "text", "text": "pong"}], "stop_reason": "max_tokens",
            "usage": {"input_tokens": 14, "output_tokens": 4,
                      "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}}
    usage_from_body(ANTHROPIC_MESSAGES, body, usage)
    assert usage.input_tokens == 14 and usage.output_tokens == 4
    assert usage.exact is True
    assert usage.stop_reason == "length"


def test_usage_from_body_never_raises_on_garbage():
    usage = Usage()
    usage_from_body(OPENAI_CHAT, {"usage": "not a dict"}, usage)
    usage_from_body(ANTHROPIC_MESSAGES, {"usage": ["nope"]}, usage)
    assert usage.exact is False

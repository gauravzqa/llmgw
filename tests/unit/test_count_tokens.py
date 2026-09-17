"""`count_tokens` (Phase C2) and the BufferedSurface base it is built on."""

from __future__ import annotations

import json

import pytest

from llmgw import errors
from llmgw.sse import SSEEvent
from llmgw.surfaces import COUNT_TOKENS
from llmgw.surfaces.base import BufferedSurface, EventKind, Usage, usage_from_body


def test_count_tokens_reads_the_model_and_never_streams():
    facts = COUNT_TOKENS.parse_request(json.dumps(
        {"model": "anthropic.haiku-4-5", "messages": [{"role": "user", "content": "hi"}]}
    ).encode())
    assert facts.model == "anthropic.haiku-4-5"
    assert facts.stream is False


def test_count_tokens_requires_a_model():
    with pytest.raises(errors.InvalidRequest):
        COUNT_TOKENS.parse_request(b'{"messages": []}')


def test_count_tokens_bills_nothing():
    usage = Usage()
    usage_from_body(COUNT_TOKENS, {"input_tokens": 1234}, usage)
    assert usage.input_tokens == 0 and usage.exact is False
    assert COUNT_TOKENS.accounts is False


def test_a_buffered_surface_treats_every_frame_as_bookkeeping():
    ev = SSEEvent(data=b'{"anything": 1}')
    assert COUNT_TOKENS.classify(ev) is EventKind.META
    assert COUNT_TOKENS.text_delta(ev) is None
    assert COUNT_TOKENS.error_from_event(ev) is None
    assert COUNT_TOKENS.native_ending() == b""
    assert COUNT_TOKENS.stop_reason_from_body({"stop_reason": "end_turn"}) is None
    assert COUNT_TOKENS.path == COUNT_TOKENS.upstream_path


def test_a_fixed_model_surface_routes_an_empty_body():
    class Mint(BufferedSurface):
        name = "mint_test"
        routes = ("/x",)
        upstream_path = "/x"
        model_key = None
        fixed_model = "openai.gpt-4o-mini"

    assert Mint().parse_request(b"").model == "openai.gpt-4o-mini"
    assert Mint().parse_request(b"{}").model == "openai.gpt-4o-mini"


def test_a_buffered_surface_with_neither_model_nor_fixed_refuses():
    class Bare(BufferedSurface):
        name = "bare"
        model_key = None

    with pytest.raises(errors.InvalidRequest):
        Bare().parse_request(b"{}")

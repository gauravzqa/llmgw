"""`OpenAIResponsesSurface` (PLAN-2 Phase F): the Responses dialect reduced
to the six questions the pump asks, plus the two the buffered path asks.

Fixtures are the frames the fake serves (`fakes/responses.py`), which are
the shapes captured live on 17 Sep 2026 (`capabilities/captures-responses.md`)
-- every frame is driven through the real SSE parser so a hand-written
payload cannot drift from the wire.
"""

from __future__ import annotations

import json
from types import MappingProxyType

import pytest
from fakes import responses as R

from llmgw import errors
from llmgw.sse import SSEEvent, SSEParser
from llmgw.surfaces import OPENAI_RESPONSES, ROUTES, SURFACES
from llmgw.surfaces.base import STOP_REASONS, EventKind, RequestFacts, Usage
from llmgw.surfaces.responses import (
    CONTENT_TYPES,
    SERVER_TOOL_KEYS,
    OpenAIResponsesSurface,
    stop_reason_of_response,
)

S = OPENAI_RESPONSES


def parse(frame: bytes) -> SSEEvent:
    parser = SSEParser()
    events = parser.feed(frame)
    events.extend(parser.close())
    assert len(events) == 1, events
    return events[0]


def typed(kind: str, **fields) -> SSEEvent:
    return parse(R.frame({"type": kind, "sequence_number": 1, **fields}))


def stream(mode: str = "ok", **kw) -> list[SSEEvent]:
    parser = SSEParser()
    out: list[SSEEvent] = []
    for frame in R.stream_frames(mode, model="gpt-4o-mini", **kw):
        out.extend(parser.feed(frame))
    out.extend(parser.close())
    return out


def fold(events: list[SSEEvent]) -> Usage:
    usage = Usage()
    for ev in events:
        S.classify(ev)
        S.apply_usage(ev, usage)
    return usage


# ------------------------------------------------------------- the contract


def test_the_surface_declares_the_registry_contract():
    assert S.name == "openai_responses"
    assert S.dialect == "openai"
    assert S.routes == ("/v1/responses",)
    assert S.upstream_path == "/v1/responses"
    assert S.methods == ("POST",)
    assert S.framing == "sse" and S.body == "json"
    assert S.include_usage_injectable is False
    assert ROUTES["/v1/responses"] is S
    assert SURFACES["openai_responses"] is S
    assert isinstance(S, OpenAIResponsesSurface)


# --------------------------------------------------------------- classify


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("response.created", EventKind.META),
        ("response.queued", EventKind.META),        # background-only frame, seen live
        ("response.in_progress", EventKind.META),
        ("response.output_item.added", EventKind.META),
        ("response.content_part.added", EventKind.META),
        ("response.output_text.delta", EventKind.CONTENT),
        ("response.refusal.delta", EventKind.CONTENT),
        ("response.function_call_arguments.delta", EventKind.CONTENT),
        ("response.reasoning_summary_text.delta", EventKind.CONTENT),
        ("response.reasoning_text.delta", EventKind.CONTENT),   # DeepSeek, in clear
        ("response.audio.delta", EventKind.CONTENT),
        ("response.audio_transcript.delta", EventKind.CONTENT),
        ("response.code_interpreter_call_code.delta", EventKind.CONTENT),
        ("response.output_text.done", EventKind.META),
        ("response.content_part.done", EventKind.META),
        ("response.output_item.done", EventKind.META),
        ("response.reasoning_summary_part.added", EventKind.META),
        ("response.web_search_call.in_progress", EventKind.META),
        ("response.web_search_call.searching", EventKind.META),
        ("response.web_search_call.completed", EventKind.META),
        ("response.file_search_call.completed", EventKind.META),
        ("response.code_interpreter_call.completed", EventKind.META),
        ("response.mcp_call.completed", EventKind.META),
        ("response.function_call_arguments.done", EventKind.META),
        ("response.completed", EventKind.TERMINAL),
        ("response.incomplete", EventKind.TERMINAL),
        ("response.failed", EventKind.ERROR),
        ("error", EventKind.ERROR),
        ("response.something_new", EventKind.META),
    ],
)
def test_every_event_type_classifies_the_way_the_contract_says(kind, expected):
    assert S.classify(typed(kind, delta="x")) is expected


def test_the_content_set_is_exactly_the_documented_one():
    assert CONTENT_TYPES == {
        "response.output_text.delta", "response.refusal.delta",
        "response.function_call_arguments.delta", "response.reasoning_summary_text.delta",
        "response.reasoning_text.delta", "response.audio.delta",
        "response.audio_transcript.delta", "response.code_interpreter_call_code.delta",
    }


def test_comments_and_blank_frames_are_liveness_only():
    assert S.classify(parse(b": keep-alive\n\n")) is EventKind.HEARTBEAT
    assert S.classify(parse(b"data:\n\n")) is EventKind.HEARTBEAT


def test_unreadable_frames_are_meta_never_content_or_terminal():
    assert S.classify(parse(b"data: [DONE]\n\n")) is EventKind.META
    assert S.classify(parse(b"data: not json\n\n")) is EventKind.META
    assert S.classify(parse(b"data: [1, 2]\n\n")) is EventKind.META
    assert S.classify(parse(b"data: {}\n\n")) is EventKind.META


def test_the_type_falls_back_to_the_event_line_when_the_payload_lacks_it():
    ev = parse(b"event: response.output_text.delta\ndata: {\"delta\":\"x\"}\n\n")
    assert S.classify(ev) is EventKind.CONTENT
    assert S.text_delta(ev) == "x"


def test_the_full_happy_stream_has_exactly_one_terminal_and_three_content_frames():
    kinds = [S.classify(ev) for ev in stream("ok")]
    assert kinds.count(EventKind.TERMINAL) == 1 and kinds[-1] is EventKind.TERMINAL
    assert kinds.count(EventKind.CONTENT) == len(R.TEXT_DELTAS)
    assert EventKind.ERROR not in kinds and EventKind.HEARTBEAT not in kinds


# -------------------------------------------------------------- text_delta


def test_text_delta_is_the_output_text_delta_and_nothing_else():
    text = "".join(d for ev in stream("ok") if (d := S.text_delta(ev)) is not None)
    assert text == R.expected_text()
    assert S.text_delta(typed("response.refusal.delta", delta="no")) is None
    assert S.text_delta(typed("response.reasoning_summary_text.delta", delta="hm")) is None
    assert S.text_delta(typed("response.function_call_arguments.delta", delta="{")) is None
    assert S.text_delta(typed("response.output_text.delta", delta="")) is None
    assert S.text_delta(typed("response.output_text.delta", delta=7)) is None
    assert S.text_delta(typed("response.output_text.done", text="Hello")) is None


def test_reasoning_stream_transcript_excludes_the_summary():
    text = "".join(d for ev in stream("responses-reasoning")
                   if (d := S.text_delta(ev)) is not None)
    assert text == R.expected_text()


# ------------------------------------------------------------- apply_usage


def test_a_complete_stream_reports_exact_usage_in_the_disjoint_convention():
    """USAGE is input 20 with cached 8 (a SUBSET) and output 12 with
    reasoning 4 (a SUBSET). Disjoint: input 12, cache_read 8, output 12,
    reasoning 4 -- and never 28 input or 16 output."""
    usage = fold(stream("ok"))
    assert usage.input_tokens == 12
    assert usage.cache_read_tokens == 8
    assert usage.cache_write_tokens == 0
    assert usage.output_tokens == 12
    assert usage.reasoning_tokens == 4
    assert usage.total_input_tokens == 20
    assert usage.exact and usage.input_exact and usage.output_exact
    assert usage.parse_failures == 0
    assert usage.stop_reason == "stop"


def test_usage_arrives_only_on_the_terminal_frame():
    events = stream("ok")
    usage = fold(events[:-1])
    assert usage.input_tokens == 0 and usage.output_tokens == 0
    assert not usage.exact, "created/in_progress carry usage: null"
    assert usage.stop_reason is None


def test_reasoning_usage_keeps_reasoning_inside_output():
    usage = fold(stream("responses-reasoning"))
    assert usage.output_tokens == 52
    assert usage.reasoning_tokens == 40
    assert usage.input_tokens == 12 and usage.cache_read_tokens == 8


def test_cache_write_tokens_are_carved_out_only_when_the_provider_states_them():
    """OpenAI reports `input_tokens_details.cache_write_tokens`; DeepSeek does
    not send the key. Present: subtracted from input like the cached subset.
    Absent: left at not-reported, never asserted as zero."""
    with_write = typed("response.completed", response={
        "status": "completed", "output": [],
        "usage": {"input_tokens": 100, "output_tokens": 5,
                  "input_tokens_details": {"cached_tokens": 30, "cache_write_tokens": 20},
                  "output_tokens_details": {"reasoning_tokens": 0}},
    })
    u = Usage()
    S.apply_usage(with_write, u)
    assert (u.input_tokens, u.cache_read_tokens, u.cache_write_tokens) == (50, 30, 20)

    deepseek_shape = typed("response.completed", response={
        "status": "completed", "output": [],
        "usage": {"input_tokens": 43, "input_tokens_details": {"cached_tokens": 0},
                  "output_tokens": 20, "output_tokens_details": {"reasoning_tokens": 18},
                  "total_tokens": 63},
    })
    u2 = Usage()
    S.apply_usage(deepseek_shape, u2)
    assert (u2.input_tokens, u2.cache_read_tokens, u2.cache_write_tokens) == (43, 0, 0)
    assert u2.output_tokens == 20 and u2.reasoning_tokens == 18


def test_over_large_cached_counts_never_produce_a_negative_bill():
    ev = typed("response.completed", response={
        "status": "completed", "output": [],
        "usage": {"input_tokens": 5, "output_tokens": 1,
                  "input_tokens_details": {"cached_tokens": 50}},
    })
    u = Usage()
    S.apply_usage(ev, u)
    assert u.input_tokens == 0 and u.cache_read_tokens == 50
    assert u.output_tokens == 1


def test_a_failed_frame_with_null_usage_leaves_the_accumulator_inexact():
    usage = fold(stream("responses-failed"))
    assert not usage.exact
    assert usage.stop_reason is None, "failed is an error, not a stop"
    assert usage.parse_failures == 0


def test_a_failed_frame_that_states_usage_is_still_billed():
    ev = typed("response.failed", response={
        "status": "failed", "error": {"code": "server_error", "message": "x"},
        "output": [],
        "usage": {"input_tokens": 9, "output_tokens": 2,
                  "input_tokens_details": {"cached_tokens": 0}},
    })
    u = Usage()
    S.apply_usage(ev, u)
    assert (u.input_tokens, u.output_tokens, u.exact) == (9, 2, True)
    assert u.stop_reason is None


@pytest.mark.parametrize(
    "frame",
    [
        b"data: [DONE]\n\n",
        b"data: {\"type\": \"response.completed\"}\n\n",
        b"data: {\"type\": \"response.completed\", \"response\": null}\n\n",
        b"data: {\"type\": \"response.completed\", \"response\": []}\n\n",
        b"data: {\"type\": \"response.completed\", \"response\": {\"usage\": 3}}\n\n",
        b"data: {\"type\": \"response.completed\", \"response\": {\"usage\": {}}}\n\n",
        (b"data: {\"type\": \"response.completed\", \"response\": {\"usage\": "
         b"{\"input_tokens\": true, \"output_tokens\": \"9\"}}}\n\n"),
        (b"data: {\"type\": \"response.completed\", \"response\": {\"status\": 7, "
         b"\"output\": 3, \"tool_usage\": [], \"usage\": {\"input_tokens\": 1.5}}}\n\n"),
        b"data: {\"type\": 42}\n\n",
        b"data: {\"type\": \"response.web_search_call.completed\", \"item_id\": null}\n\n",
        b"data: \xff\xfe\n\n",
    ],
)
def test_hostile_frames_never_raise_and_never_bill(frame):
    ev = parse(frame)
    u = Usage()
    S.classify(ev)
    S.apply_usage(ev, u)
    assert S.text_delta(ev) is None or isinstance(S.text_delta(ev), str)
    S.error_from_event(ev)
    assert (u.input_tokens, u.output_tokens) == (0, 0)
    assert not u.exact


def test_a_frame_whose_usage_explodes_is_counted_not_swallowed(monkeypatch):
    ev = typed("response.completed", response={"status": "completed", "usage": {}})
    u = Usage()

    def boom(*_a, **_k):
        raise RuntimeError("shape changed")

    monkeypatch.setattr(OpenAIResponsesSurface, "_fold_response", staticmethod(boom))
    S.apply_usage(ev, u)
    assert u.parse_failures == 1


# ------------------------------------------------------------- stop reason


def _response(status: str, *, output=None, reason=None):
    body = {"status": status, "output": output if output is not None else []}
    if reason is not None:
        body["incomplete_details"] = {"reason": reason}
    return body


def _message(*parts):
    return {"type": "message", "content": list(parts), "role": "assistant"}


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (_response("completed", output=[_message({"type": "output_text", "text": "hi"})]),
         "stop"),
        (_response("completed"), "stop"),
        (_response("completed", output=[_message({"type": "output_text", "text": "x"}),
                                         {"type": "function_call", "name": "f"}]),
         "tool_calls"),
        (_response("completed", output=[{"type": "custom_tool_call"}]), "tool_calls"),
        (_response("completed", output=[{"type": "function_call"},
                                         _message({"type": "output_text", "text": "x"})]),
         "stop"),   # the LAST item decides
        (_response("incomplete", reason="max_output_tokens"), "length"),
        (_response("incomplete", reason="content_filter"), "content_filter"),
        (_response("incomplete", reason="something_else"), "unknown"),
        (_response("incomplete"), "unknown"),
        (_response("completed", output=[_message({"type": "refusal", "refusal": "no"})]),
         "refusal"),
        (_response("completed", output=[_message({"type": "output_text", "text": "a"},
                                                  {"type": "refusal", "refusal": "no"})]),
         "refusal"),
        (_response("failed"), None),
        (_response("in_progress"), None),
        (_response("queued"), None),
        (_response("cancelled"), None),
        ({"status": ["nope"]}, None),
        ("not a dict", None),
    ],
)
def test_stop_reason_mapping(response, expected):
    assert stop_reason_of_response(response) == expected
    assert S.stop_reason_from_body(response) == expected  # type: ignore[arg-type]
    if expected is not None:
        assert expected in STOP_REASONS


def test_stop_reason_is_read_off_the_terminal_frame_on_a_stream():
    assert fold(stream("ok")).stop_reason == "stop"
    assert fold(stream("responses-incomplete")).stop_reason == "length"
    assert fold(stream("responses-web-search")).stop_reason == "stop"


def test_the_incomplete_stream_still_bills_exactly():
    usage = fold(stream("responses-incomplete"))
    assert usage.exact and usage.output_tokens == 12


# --------------------------------------------------------- error_from_event


def test_an_error_event_maps_onto_in_stream_error_with_the_providers_message():
    ev = parse(R.frame({**R.ERROR_EVENT, "sequence_number": 9}))
    assert S.classify(ev) is EventKind.ERROR
    err = S.error_from_event(ev)
    assert isinstance(err, errors.InStreamError)
    assert R.ERROR_EVENT["message"] in str(err)
    assert err.upstream_body == ev.data


def test_an_overloaded_error_event_is_an_overload():
    ev = typed("error", code="server_overloaded", message="busy")
    assert isinstance(S.error_from_event(ev), errors.UpstreamOverloaded)
    # On `response.failed` the code and type live under `response.error`.
    ev2 = typed("response.failed", response={
        "status": "failed", "error": {"type": "overloaded_error", "message": "busy"},
    })
    assert isinstance(S.error_from_event(ev2), errors.UpstreamOverloaded)


def test_response_failed_maps_onto_in_stream_error_from_response_error():
    events = stream("responses-failed")
    failed = events[-1]
    assert S.classify(failed) is EventKind.ERROR
    err = S.error_from_event(failed)
    assert isinstance(err, errors.InStreamError)
    assert not isinstance(err, errors.UpstreamOverloaded)
    assert R.FAILED_ERROR["message"] in str(err)


def test_response_failed_with_an_overloaded_code_is_an_overload():
    ev = typed("response.failed", response={
        "status": "failed", "error": {"code": "overloaded", "message": "try later"},
    })
    assert isinstance(S.error_from_event(ev), errors.UpstreamOverloaded)


def test_response_failed_without_an_error_object_still_classifies():
    ev = typed("response.failed", response={"status": "failed", "error": None})
    err = S.error_from_event(ev)
    assert isinstance(err, errors.InStreamError)


@pytest.mark.parametrize("mode", ["ok", "responses-incomplete", "responses-web-search"])
def test_error_from_event_is_none_for_every_ordinary_frame(mode):
    assert all(S.error_from_event(ev) is None for ev in stream(mode))


def test_native_ending_is_empty_because_the_failed_frame_was_already_forwarded():
    assert S.native_ending(None) == b""
    assert S.native_ending(stream("responses-failed")[-1]) == b""


# ------------------------------------------------------------ server tools


def test_web_search_lifecycle_counts_once_and_the_terminal_count_is_authoritative():
    usage = fold(stream("responses-web-search"))
    assert dict(usage.server_tool_calls) == {"web_search_requests": 1}
    assert isinstance(usage.server_tool_calls, MappingProxyType)


def test_tool_usage_num_requests_replaces_the_event_tally_when_present():
    u = Usage()
    for _ in range(3):
        S.apply_usage(typed("response.web_search_call.completed", item_id="ws"), u)
    assert u.server_tool_calls["web_search_requests"] == 3
    S.apply_usage(typed("response.completed", response={
        "status": "completed", "output": [], "tool_usage": {"web_search": {"num_requests": 2}},
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }), u)
    assert u.server_tool_calls["web_search_requests"] == 2


def test_without_tool_usage_the_event_tally_stands():
    """DeepSeek's envelope has no `tool_usage`; the events are the count."""
    u = Usage()
    S.apply_usage(typed("response.web_search_call.completed", item_id="ws"), u)
    S.apply_usage(typed("response.completed", response={
        "status": "completed", "output": [], "usage": {"input_tokens": 1, "output_tokens": 1},
    }), u)
    assert dict(u.server_tool_calls) == {"web_search_requests": 1}


def test_every_hosted_tool_completion_is_counted_under_its_key():
    u = Usage()
    for item_type in SERVER_TOOL_KEYS:
        S.apply_usage(typed(f"response.{item_type}.completed", item_id="x"), u)
        S.apply_usage(typed(f"response.{item_type}.completed", item_id="y"), u)
    assert dict(u.server_tool_calls) == {
        "web_search_requests": 2, "file_search_requests": 2,
        "code_execution_requests": 2, "mcp_requests": 2,
    }
    # `in_progress` and `searching` are not completions.
    S.apply_usage(typed("response.web_search_call.in_progress", item_id="x"), u)
    S.apply_usage(typed("response.web_search_call.searching", item_id="x"), u)
    assert u.server_tool_calls["web_search_requests"] == 2


def test_a_fresh_usage_tool_map_is_never_shared_between_requests():
    a, b = Usage(), Usage()
    S.apply_usage(typed("response.mcp_call.completed"), a)
    assert dict(b.server_tool_calls) == {}


# --------------------------------------------------------------- buffered


def test_usage_from_body_reads_the_complete_response_object():
    body = R.json_body("ok", model="gpt-4o-mini")
    u = Usage()
    S.usage_from_body(body, u)
    assert (u.input_tokens, u.cache_read_tokens, u.output_tokens, u.reasoning_tokens) == (
        12, 8, 12, 4)
    assert u.exact and u.stop_reason == "stop"
    assert S.stop_reason_from_body(body) == "stop"


def test_usage_from_body_counts_hosted_tool_items_in_output():
    body = R.json_body("responses-web-search", model="gpt-4o-mini")
    u = Usage()
    S.usage_from_body(body, u)
    assert dict(u.server_tool_calls) == {"web_search_requests": 1}
    # Without the provider's count, the items are the count.
    body2 = R.response_object(model="gpt-4o-mini", status="completed", usage=R.USAGE, output=[
        {"type": "file_search_call"}, {"type": "mcp_call"}, {"type": "mcp_call"},
        {"type": "code_interpreter_call"}, {"type": "message", "content": []},
    ])
    del body2["tool_usage"]
    u2 = Usage()
    S.usage_from_body(body2, u2)
    assert dict(u2.server_tool_calls) == {
        "file_search_requests": 1, "mcp_requests": 2, "code_execution_requests": 1,
    }


def test_incomplete_and_failed_bodies_report_their_stop_reasons():
    assert S.stop_reason_from_body(R.json_body("responses-incomplete", model="x")) == "length"
    assert S.stop_reason_from_body(R.json_body("responses-failed", model="x")) is None
    u = Usage()
    S.usage_from_body(R.json_body("responses-incomplete", model="x"), u)
    assert u.exact and u.stop_reason == "length"


def test_usage_from_body_never_raises_on_garbage():
    u = Usage()
    S.usage_from_body({"usage": "nope", "output": 4, "status": None}, u)  # type: ignore[arg-type]
    S.usage_from_body({"tool_usage": {"web_search": {"num_requests": "many"}}}, u)
    assert not u.exact and u.parse_failures == 0


# ----------------------------------------------------------- parse_request


def test_parse_request_reads_model_stream_and_max_output_tokens():
    facts = S.parse_request(json.dumps({
        "model": "openai.gpt-4o-mini", "input": "hi", "stream": True,
        "max_output_tokens": 64, "reasoning": {"effort": "low"},
        "tools": [{"type": "web_search_preview"}], "include": ["reasoning.encrypted_content"],
        "store": False, "temperature": 0.2,
    }).encode())
    assert facts == RequestFacts(model="openai.gpt-4o-mini", stream=True, max_tokens=64,
                                 include_usage=True, needs_state=False)


def test_parse_request_always_says_a_usage_report_is_coming():
    facts = S.parse_request(b'{"model": "m", "input": "x"}')
    assert facts.include_usage is True
    assert facts.stream is False
    assert facts.max_tokens is None


def test_max_tokens_ignores_the_chat_spellings_and_garbage():
    assert S.parse_request(b'{"model":"m","max_tokens":5}').max_tokens is None
    assert S.parse_request(b'{"model":"m","max_completion_tokens":5}').max_tokens is None
    assert S.parse_request(b'{"model":"m","max_output_tokens":"lots"}').max_tokens is None
    assert S.parse_request(b'{"model":"m","max_output_tokens":0}').max_tokens is None


def test_background_true_is_refused_with_invalid_request():
    with pytest.raises(errors.InvalidRequest) as info:
        S.parse_request(b'{"model": "m", "input": "x", "background": true}')
    assert "background" in str(info.value)
    assert info.value.status == 400
    assert info.value.blame is errors.Blame.CLIENT
    # False, null and absent are all fine (the provider treats them alike).
    for value in ("false", "null"):
        S.parse_request(f'{{"model": "m", "background": {value}}}'.encode())


@pytest.mark.parametrize(
    ("body", "label"),
    [
        (b"", "empty"),
        (b"   ", "blank"),
        (b"not json", "not json"),
        (b"[]", "a list"),
        (b'{"input": "x"}', "no model"),
        (b'{"model": ""}', "blank model"),
        (b'{"model": "m", "stream": "yes"}', "non-boolean stream"),
    ],
)
def test_unusable_bodies_raise_the_taxonomy_class(body, label):
    with pytest.raises(errors.InvalidRequest):
        S.parse_request(body)


def test_previous_response_id_and_conversation_mark_the_request_as_stateful():
    assert S.parse_request(b'{"model":"m","previous_response_id":"resp_1"}').needs_state
    assert S.parse_request(b'{"model":"m","conversation":"conv_1"}').needs_state
    assert S.parse_request(b'{"model":"m","conversation":{"id":"conv_1"}}').needs_state
    assert not S.parse_request(b'{"model":"m","previous_response_id":null}').needs_state
    assert not S.parse_request(b'{"model":"m"}').needs_state


# ------------------------------------------------------------ check_target


class _Provider:
    def __init__(self, pid: str, stateless: bool) -> None:
        self.id = pid
        self.stateless_responses = stateless


class _Target:
    def __init__(self, provider: _Provider) -> None:
        self.provider = provider


def test_a_stateful_body_is_refused_for_a_provider_that_holds_no_state():
    facts = S.parse_request(b'{"model":"m","previous_response_id":"resp_1"}')
    with pytest.raises(errors.InvalidRequest) as info:
        S.check_target(facts, _Target(_Provider("deepseek", stateless=True)))
    assert "deepseek" in str(info.value)
    assert "state" in str(info.value)
    assert info.value.try_next is True, "an incumbent that stores responses may still serve"
    assert info.value.health is errors.Health.NEUTRAL


def test_check_target_lets_everything_else_through():
    stateful = S.parse_request(b'{"model":"m","previous_response_id":"resp_1"}')
    stateless_body = S.parse_request(b'{"model":"m"}')
    S.check_target(stateful, _Target(_Provider("openai", stateless=False)))
    S.check_target(stateless_body, _Target(_Provider("deepseek", stateless=True)))
    S.check_target(None, _Target(_Provider("deepseek", stateless=True)))

    class Bare:
        pass

    S.check_target(stateful, Bare())  # a target with no provider attribute at all


def test_the_shipped_catalog_marks_deepseek_stateless_and_openai_not():
    from llmgw.catalog import DEFAULT_CATALOG

    assert DEFAULT_CATALOG.providers["deepseek"].stateless_responses is True
    assert DEFAULT_CATALOG.providers["deepseek-beta"].stateless_responses is True
    assert DEFAULT_CATALOG.providers["openai"].stateless_responses is False


def test_the_deepseek_echo_resolves_through_the_openai_dialect_hint():
    """DeepSeek echoes `deepseek-flash` for `deepseek-v4-flash` (probe 10a).
    That string is already the `api_model` of the OpenAI-dialect row and the
    Anthropic-dialect one, so it is an implicit wire id shared by two rows
    and the route's dialect picks -- no declared alias needed, and declaring
    one would take the Anthropic route's resolution away."""
    from llmgw.catalog import DEFAULT_CATALOG

    assert DEFAULT_CATALOG.canonical_id("deepseek-flash", kind="openai") == (
        "deepseek.deepseek-v4-flash")
    assert DEFAULT_CATALOG.canonical_id("gpt-4o-mini-2024-07-18") == "openai.gpt-4o-mini"


def test_the_executor_asks_the_surface_before_the_breaker():
    """The gate order in `Executor._attempt`: `check_target` runs before a
    ticket is minted, so a refusal costs the provider nothing."""
    import inspect

    from llmgw.executor import Executor

    src = inspect.getsource(Executor._attempt)
    assert src.index("check_target(request_facts, target)") < src.index(
        "state.tickets = self._acquire_tickets(target)")

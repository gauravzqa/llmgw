"""The image-generation dialect, against the frames and bodies the real
endpoint sent on 20 Sep 2026.

Two of these tests would have passed against an imagined API and failed
against the real one, which is why they are written from captures:

* the stream has NO `data: [DONE]`, so `image_generation.completed` has to
  be the terminal marker or every successful stream ends `IncompleteStream`;
* `n` is what was ASKED for and `len(data)` is what arrived, and on 20 Sep
  2026 they disagreed -- `n=2` came back with one image.
"""

from __future__ import annotations

import json

import pytest

from llmgw import errors
from llmgw.sse import SSEEvent
from llmgw.surfaces.base import EventKind, Usage
from llmgw.surfaces.images import (
    COMPLETED,
    IMAGE_OUTPUT_TOKENS,
    PARTIAL_IMAGE,
    PARTIAL_IMAGE_TOKENS,
    ImagesGenerationsSurface,
)

S = ImagesGenerationsSurface()

# --------------------------------------------------------------- captures
# Copied from the live responses. `b64_json` is truncated (the real strings
# are 1.5 million characters) and nothing else is touched.

USAGE_BLOCK = {
    "input_tokens": 14,
    "input_tokens_details": {"image_tokens": 0, "text_tokens": 14},
    "output_tokens": 272,
    "output_tokens_details": {"image_tokens": 272, "text_tokens": 0},
    "total_tokens": 286,
}

BUFFERED_BODY = {
    "created": 1789847570,
    "background": "opaque",
    "data": [{"b64_json": "iVBORw0KGgo=", "generation_id": "d639de25-4fac-4bdd-b095"}],
    "output_format": "png",
    "quality": "low",
    "size": "1024x1024",
    "usage": USAGE_BLOCK,
}


def ev(event: str | None, payload: dict) -> SSEEvent:
    return SSEEvent(event=event, data=json.dumps(payload).encode())


def partial(index: int) -> SSEEvent:
    return ev(PARTIAL_IMAGE, {
        "created_at": 1789847646, "type": PARTIAL_IMAGE, "b64_json": "iVBORw0KGgo=",
        "background": "opaque", "output_format": "png", "partial_image_index": index,
        "quality": "low", "sequence_number": index, "size": "1024x1024",
    })


def completed(usage: dict | None = USAGE_BLOCK) -> SSEEvent:
    payload = {
        "created_at": 1789847650, "type": COMPLETED, "b64_json": "iVBORw0KGgo=",
        "background": "opaque", "output_format": "png", "quality": "low",
        "sequence_number": 2, "size": "1024x1024",
    }
    if usage is not None:
        payload["usage"] = usage
    return ev(COMPLETED, payload)


# --------------------------------------------------------------- request


def test_parse_request_reads_the_routing_key_and_the_three_sizing_fields():
    facts = S.parse_request(json.dumps({
        "model": "openai.gpt-image-1", "prompt": "a red circle", "n": 3,
        "size": "1536x1024", "quality": "LOW", "stream": True, "partial_images": 2,
    }).encode())
    assert facts.model == "openai.gpt-image-1"
    assert facts.stream is True
    assert (facts.n, facts.size, facts.quality, facts.partial_images) == (
        3, "1536x1024", "low", 2)
    assert facts.prompt_chars == len("a red circle")
    # Usage arrives unasked-for on both forms; there is no `include_usage`
    # to send and the surface must not claim otherwise.
    assert facts.include_usage is True


def test_the_api_defaults_are_used_when_the_client_names_nothing():
    facts = S.parse_request(b'{"model":"openai.gpt-image-1","prompt":"x"}')
    assert (facts.n, facts.size, facts.quality, facts.partial_images) == (
        1, "1024x1024", "medium", 0)
    assert facts.stream is False


def test_auto_is_not_a_size_or_a_quality_the_estimator_can_resolve():
    """`auto` is the provider's "you decide". It reads as the API default for
    estimation and nothing else -- the estimate is never the bill when the
    provider reported one."""
    facts = S.parse_request(
        b'{"model":"m","prompt":"x","size":"auto","quality":"auto"}')
    assert (facts.size, facts.quality) == ("1024x1024", "medium")


@pytest.mark.parametrize("body", [
    b"", b"   ", b"not json", b"[1,2]", b'{"prompt":"x"}', b'{"model":"","prompt":"x"}',
    b'{"model":"m","prompt":"x","stream":"yes"}',
])
def test_every_unusable_body_is_invalid_request_and_nothing_else(body):
    """The executor switches on the taxonomy; a bare ValueError would be a
    500 blamed on a provider that was never contacted."""
    with pytest.raises(errors.InvalidRequest):
        S.parse_request(body)


def test_a_nonsense_n_falls_back_to_one_rather_than_refusing():
    """`n` only sizes an estimate. Refusing a body the provider would have
    accepted is worse than sizing a guess off a default."""
    for value in ('"three"', "0", "-4", "true", "null"):
        facts = S.parse_request(
            f'{{"model":"m","prompt":"x","n":{value}}}'.encode())
        assert facts.n == 1


# ---------------------------------------------------------------- frames


def test_the_completed_event_is_terminal_because_there_is_no_done_marker():
    """The load-bearing classification. `pump._eof_is_terminal()` is False
    for SSE, so if this frame is not TERMINAL every successful streamed
    image ends as `IncompleteStream`."""
    assert S.classify(completed()) is EventKind.TERMINAL


def test_a_partial_image_is_content_and_resets_the_progress_clock():
    assert S.classify(partial(0)) is EventKind.CONTENT
    assert S.classify(partial(1)) is EventKind.CONTENT


def test_the_frames_nobody_should_call_progress():
    assert S.classify(SSEEvent(comment=b"keepalive")) is EventKind.HEARTBEAT
    assert S.classify(SSEEvent(data=b"   ")) is EventKind.HEARTBEAT
    assert S.classify(SSEEvent(data=b"{not json")) is EventKind.META
    assert S.classify(ev("image_generation.in_progress", {"type": "x"})) is EventKind.META
    # Checked before any JSON parse, always, even though this endpoint has
    # never been seen to send it.
    assert S.classify(SSEEvent(data=b"[DONE]")) is EventKind.TERMINAL


def test_an_error_object_inside_a_200_is_an_error_frame():
    frame = ev(None, {"error": {"message": "boom", "type": "server_error"}})
    assert S.classify(frame) is EventKind.ERROR
    assert isinstance(S.error_from_event(frame), errors.InStreamError)


def test_a_moderation_refusal_inside_a_200_is_the_callers_fault_not_the_providers():
    """Same rule as the 400 form: NEUTRAL health so no circuit hears it, and
    `try_next=False` so a refused prompt is not shopped around the
    fallbacks."""
    frame = ev(None, {"error": {"message": "rejected", "type": "image_generation_"
                                "user_error", "code": "moderation_blocked"}})
    err = S.error_from_event(frame)
    assert isinstance(err, errors.ContentFiltered)
    assert err.health is errors.Health.NEUTRAL and err.try_next is False


def test_an_overloaded_error_frame_keeps_its_own_class():
    frame = ev(None, {"error": {"message": "busy", "type": "overloaded_error"}})
    assert isinstance(S.error_from_event(frame), errors.UpstreamOverloaded)


def test_an_image_has_no_transcript_and_no_stop_reason():
    assert S.text_delta(completed()) is None
    assert S.text_delta(partial(0)) is None
    assert S.stop_reason_from_body(BUFFERED_BODY) is None


def test_the_gateway_never_invents_the_ending_openai_does_not_send():
    assert S.native_ending() == b""
    assert S.native_ending(completed()) == b""


# ----------------------------------------------------------------- usage


def test_the_completed_event_carries_the_whole_bill_and_both_halves_are_exact():
    usage = Usage()
    S.apply_usage(partial(0), usage)
    S.apply_usage(partial(1), usage)
    assert usage.output_tokens == 0 and not usage.exact  # partials meter nothing
    S.apply_usage(completed(), usage)
    assert (usage.input_tokens, usage.output_tokens) == (14, 272)
    assert usage.input_exact and usage.output_exact and usage.exact
    assert usage.images == 1
    assert usage.parse_failures == 0


def test_the_buffered_body_bills_the_images_that_arrived_not_the_n_requested():
    """Live, 20 Sep 2026: `n=2` answered with ONE image and one image's worth
    of tokens, while `n=3` answered with three. A bill built on the request
    would have charged for an image that does not exist."""
    usage = Usage()
    S.usage_from_body(BUFFERED_BODY, usage)
    assert usage.images == 1 and usage.output_tokens == 272 and usage.exact

    three = dict(BUFFERED_BODY, data=[{"b64_json": "x"}] * 3,
                 usage=dict(USAGE_BLOCK, output_tokens=816, input_tokens=33))
    usage = Usage()
    S.usage_from_body(three, usage)
    assert usage.images == 3 and usage.output_tokens == 816


@pytest.mark.parametrize("payload", [
    {}, {"usage": None}, {"usage": {}}, {"usage": {"input_tokens": "lots"}},
    {"usage": {"input_tokens": True}}, {"data": "not a list", "usage": USAGE_BLOCK},
])
def test_a_usage_shape_the_surface_cannot_read_never_raises(payload):
    """Billing never breaks serving. A body with nothing usable leaves the
    usage inexact, which is the honest report."""
    usage = Usage()
    S.usage_from_body(payload, usage)
    S.apply_usage(ev(COMPLETED, {"type": COMPLETED, **payload}), usage)
    assert usage.parse_failures == 0  # unreadable is not malformed


def test_a_bool_token_count_is_rejected_rather_than_billed_as_one():
    usage = Usage()
    S.usage_from_body({"usage": {"input_tokens": True, "output_tokens": True}}, usage)
    assert usage.input_tokens == 0 and usage.output_tokens == 0 and not usage.exact


# ------------------------------------------------------------- the estimate


def test_a_stream_cut_before_the_completed_event_bills_from_the_request():
    facts = S.parse_request(
        b'{"model":"m","prompt":"a red circle","size":"1536x1024",'
        b'"quality":"low","n":2,"stream":true,"partial_images":2}')
    est = S.usage_estimate(facts)
    assert est.output_tokens == (
        IMAGE_OUTPUT_TOKENS[("low", "1536x1024")] * 2 + 2 * PARTIAL_IMAGE_TOKENS)
    assert est.input_tokens == len("a red circle") // 4 + 1
    # The bill is estimated; the image COUNT is not filled in. Nothing
    # reached the client, and `Usage.images` counts what arrived -- putting
    # `n` there would credit the meter with images nobody received.
    assert est.images == 0
    # Never exact: the provider never stated any of it.
    assert not est.exact


def test_an_estimate_the_table_cannot_make_is_zero_rather_than_a_guess():
    facts = S.parse_request(b'{"model":"m","prompt":"x","size":"256x256"}')
    est = S.usage_estimate(facts)
    assert est.output_tokens == 0 and est.images == 0


def test_the_live_calibration_of_the_table_is_what_the_table_says():
    """272 output tokens for a low 1024x1024 and 400 for a low 1536x1024 were
    measured through this gateway. If someone edits the table, this is the
    row that says which two cells are evidence and which are documentation."""
    assert IMAGE_OUTPUT_TOKENS[("low", "1024x1024")] == 272
    assert IMAGE_OUTPUT_TOKENS[("low", "1536x1024")] == 400
    assert PARTIAL_IMAGE_TOKENS == 100


# ------------------------------------------------------------- cost notes


def test_an_exact_bill_carries_no_note_because_there_is_nothing_to_explain():
    usage = Usage()
    S.usage_from_body(BUFFERED_BODY, usage)
    facts = S.parse_request(b'{"model":"m","prompt":"x","quality":"low"}')
    assert S.cost_notes(facts, usage) == ()


def test_an_estimated_bill_says_where_its_number_came_from():
    """`live/smoke_images.py`'s ledger FAILS an estimated record with no
    note, and so does the voice one: an `estimated` basis with no reason
    attached is the shape an invoice dispute cannot use."""
    facts = S.parse_request(
        b'{"model":"m","prompt":"x","quality":"low","size":"1024x1024","n":2}')
    notes = S.cost_notes(facts, Usage())
    assert len(notes) == 1
    assert "272 output tokens an image" in notes[0]
    assert "reported no usage" in notes[0]
    assert "n=2 low 1024x1024" in notes[0]
    assert COMPLETED in notes[0]


def test_an_estimated_bill_off_the_table_says_that_too_rather_than_nothing():
    facts = S.parse_request(b'{"model":"m","prompt":"x","size":"256x256"}')
    notes = S.cost_notes(facts, Usage())
    assert len(notes) == 1 and "not in the per-image token table" in notes[0]


def test_cost_notes_and_usage_estimate_tolerate_a_plain_request_facts():
    """The hooks are called with whatever `parse_request` returned, and a
    future caller may hand them the base class. Neither may raise."""
    from llmgw.surfaces.base import RequestFacts

    bare = RequestFacts(model="m", stream=False)
    # The getattr defaults are the API's own (medium, 1024x1024, n=1), so the
    # estimate is the table's cell for them rather than a raise or a zero.
    est = S.usage_estimate(bare)
    assert est.output_tokens == IMAGE_OUTPUT_TOKENS[("medium", "1024x1024")]
    assert est.input_tokens == 0  # a facts object with no prompt length
    assert isinstance(S.cost_notes(bare, None), tuple)


# ------------------------------------------------------------- registration


def test_the_surface_is_registered_under_one_name_for_both_forms():
    from llmgw.surfaces import ROUTES, SURFACES

    assert SURFACES["images_generations"].name == "images_generations"
    assert ROUTES["/v1/images/generations"] is SURFACES["images_generations"]
    assert S.upstream_path == "/v1/images/generations"
    assert S.dialect == "openai" and S.body == "json" and S.framing == "sse"
    # `stream_options.include_usage` is a chat field; this endpoint 400s an
    # unknown parameter and reports usage unasked-for anyway.
    assert S.include_usage_injectable is False


def test_the_catalog_prices_the_image_rows_in_tokens_not_images():
    """The design decision, asserted. Every image model OpenAI serves reports
    tokens, so no row is `unit="images"` and the bill is exact."""
    from llmgw.catalog import DEFAULT_CATALOG

    for model_id in ("openai.gpt-image-1", "openai.gpt-image-1-mini"):
        spec = DEFAULT_CATALOG.models[model_id]
        assert spec.unit == "tokens", model_id
        assert spec.per_minute is None
        assert spec.output_per_m > spec.input_per_m  # image out costs 8x text in
    assert DEFAULT_CATALOG.models["openai.gpt-image-1"].output_per_m == 40.0
    assert DEFAULT_CATALOG.models["openai.gpt-image-1-mini"].output_per_m == 8.0
    assert not any(spec.unit == "images" for spec in DEFAULT_CATALOG.models.values())

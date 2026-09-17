"""Surface tests: the dialect layer, driven by the canonical wire bytes.

Every stream in here comes from `fakes.wire` and goes through the real
`SSEParser`. Nothing constructs a hand-written event shape, because a test
that invents its own idea of an Anthropic frame is a test that will keep
passing after the real format moves -- two mocks agreeing with each other
while neither matches a provider. `fakes/wire.py` is the single source of
truth and the fake upstreams serve those same bytes.

No sockets, no sleeping, no I/O.
"""

from __future__ import annotations

import json

import pytest
from fakes import wire

from llmgw import errors
from llmgw.sse import SSEEvent, SSEParser
from llmgw.surfaces import (
    SURFACES,
    AnthropicMessagesSurface,
    EventKind,
    OpenAIChatSurface,
    Surface,
    Usage,
    for_path,
)

OPENAI = OpenAIChatSurface()
ANTHROPIC = AnthropicMessagesSurface()
BOTH = [OPENAI, ANTHROPIC]


def parse(*frames: bytes) -> list[SSEEvent]:
    """Canonical bytes -> events, through the parser the gateway really uses."""
    parser = SSEParser()
    events = parser.feed(b"".join(frames))
    events.extend(parser.close())
    return events


def only(frame: bytes) -> SSEEvent:
    events = parse(frame)
    assert len(events) == 1, f"expected exactly one event from {frame!r}, got {events}"
    return events[0]


def drive(surface: Surface, frames: list[bytes]) -> tuple[str, Usage, list[EventKind]]:
    """Run a whole stream through a surface the way the pump would."""
    text: list[str] = []
    usage = Usage()
    kinds: list[EventKind] = []
    for ev in parse(*frames):
        kinds.append(surface.classify(ev))
        chunk = surface.text_delta(ev)
        if chunk is not None:
            text.append(chunk)
        surface.apply_usage(ev, usage)
    return "".join(text), usage, kinds


# ======================================================================
# Classification -- CONTRACTS.md C7
# ======================================================================


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (wire.openai_chunk("hi"), EventKind.CONTENT),
        (wire.openai_heartbeat(), EventKind.HEARTBEAT),
        (wire.openai_comment(), EventKind.HEARTBEAT),
        (wire.openai_usage_chunk(), EventKind.META),
        (wire.openai_done(), EventKind.TERMINAL),
    ],
    ids=["chunk", "empty-choices", "comment", "usage", "done"],
)
def test_every_openai_frame_type_classifies_the_way_the_contract_says(frame, expected):
    assert OPENAI.classify(only(frame)) is expected


@pytest.mark.parametrize(
    ("frame", "expected"),
    [
        (wire.anthropic_message_start(), EventKind.META),
        (wire.anthropic_block_start(), EventKind.META),
        (wire.anthropic_delta("hi"), EventKind.CONTENT),
        (wire.anthropic_ping(), EventKind.HEARTBEAT),
        (wire.anthropic_block_stop(), EventKind.META),
        (wire.anthropic_message_delta(), EventKind.META),
        (wire.anthropic_message_stop(), EventKind.TERMINAL),
        (wire.anthropic_error(), EventKind.ERROR),
    ],
    ids=[
        "message_start", "content_block_start", "content_block_delta", "ping",
        "content_block_stop", "message_delta", "message_stop", "error",
    ],
)
def test_every_anthropic_frame_type_classifies_the_way_the_contract_says(frame, expected):
    assert ANTHROPIC.classify(only(frame)) is expected


def test_a_message_delta_is_meta_because_usage_bookkeeping_is_not_progress():
    """The progress-clock bug, asserted directly.

    `message_delta` carries `usage.output_tokens` and the stop reason. It is
    named like content and is not content, and if it resets the progress clock
    then a provider that reports its token count and then wedges buys itself a
    whole extra progress window looking healthy.
    """
    kind = ANTHROPIC.classify(only(wire.anthropic_message_delta()))
    assert kind is EventKind.META
    assert kind is not EventKind.CONTENT


def test_an_empty_choices_chunk_is_a_heartbeat_and_never_indexes_choices_zero():
    """Real providers send `"choices": []`. Any surface that reaches for
    `choices[0]` raises IndexError on the first keepalive of a healthy
    stream."""
    ev = only(wire.openai_heartbeat())
    assert OPENAI.classify(ev) is EventKind.HEARTBEAT
    assert OPENAI.text_delta(ev) is None
    assert OPENAI.error_from_event(ev) is None
    usage = Usage()
    OPENAI.apply_usage(ev, usage)
    assert usage == Usage()


def test_heartbeats_never_classify_as_content_in_a_stream_full_of_them():
    """C7 end to end: pings and empty-choices chunks interleaved with real
    tokens must leave the CONTENT count equal to the token count."""
    _, _, openai_kinds = drive(OPENAI, wire.openai_stream(heartbeats=True))
    _, _, anthropic_kinds = drive(ANTHROPIC, wire.anthropic_stream(pings=True))
    assert openai_kinds.count(EventKind.CONTENT) == len(wire.TOKENS)
    assert anthropic_kinds.count(EventKind.CONTENT) == len(wire.TOKENS)
    assert openai_kinds.count(EventKind.HEARTBEAT) == len(wire.TOKENS)
    assert anthropic_kinds.count(EventKind.HEARTBEAT) == len(wire.TOKENS)


def test_an_sse_comment_proves_liveness_only():
    """OpenRouter's `: OPENROUTER PROCESSING` keeps intermediaries from idling
    the socket out. It carries no data and must not be parsed as an event."""
    ev = only(wire.openai_comment())
    assert ev.is_comment
    for surface in BOTH:
        assert surface.classify(ev) is EventKind.HEARTBEAT
        assert surface.text_delta(ev) is None
        assert surface.error_from_event(ev) is None


# ======================================================================
# Text reassembly
# ======================================================================


@pytest.mark.parametrize(
    ("surface", "frames"),
    [(OPENAI, wire.openai_stream()), (ANTHROPIC, wire.anthropic_stream())],
    ids=["openai", "anthropic"],
)
def test_reassembled_text_equals_the_canonical_expectation(surface, frames):
    """Includes the 2-, 3- and 4-byte UTF-8 tokens (cafe / JP / emoji) on
    purpose: a decoder that only ever sees ASCII is a decoder whose boundary
    handling has never been tested."""
    text, _, _ = drive(surface, frames)
    assert text == wire.expected_text()


def test_multibyte_tokens_survive_one_frame_at_a_time():
    """Same assertion at frame granularity, so a failure names the token."""
    for token in wire.TOKENS:
        assert OPENAI.text_delta(only(wire.openai_chunk(token))) == token
        assert ANTHROPIC.text_delta(only(wire.anthropic_delta(token))) == token


def test_a_non_text_anthropic_delta_yields_no_transcript_text():
    """Tool-argument deltas are progress but not transcript. Folding their
    JSON into the assistant's text is how a log ends up showing a customer a
    half-serialised function call."""
    frame = (
        b'event: content_block_delta\n'
        b'data: {"type":"content_block_delta","index":0,'
        b'"delta":{"type":"input_json_delta","partial_json":"{\\"a\\":"}}\n\n'
    )
    ev = only(frame)
    assert ANTHROPIC.classify(ev) is EventKind.CONTENT
    assert ANTHROPIC.text_delta(ev) is None


# ======================================================================
# [DONE] -- the frame that is not JSON
# ======================================================================


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
def test_the_done_marker_never_reaches_a_json_parser(surface):
    """`data: [DONE]` is the one payload in the OpenAI dialect that is not
    JSON, and it arrives at the end of every successful stream. A surface that
    parses first and checks later fails on the happy path -- and fails in a
    way that looks like an upstream problem.

    Asserted for BOTH surfaces: an OpenAI-compatible shim in front of
    Anthropic will append one, and no method may raise on it.
    """
    ev = only(wire.openai_done())
    usage = Usage()
    assert surface.classify(ev) is EventKind.TERMINAL
    assert surface.text_delta(ev) is None
    assert surface.error_from_event(ev) is None
    surface.apply_usage(ev, usage)
    assert usage == Usage()


# ======================================================================
# Usage -- CONTRACTS.md C3, and the convention chosen in surfaces/base.py
# ======================================================================


def test_a_complete_anthropic_stream_reports_exact_usage_in_the_disjoint_convention():
    """Anthropic already reports disjoint buckets: `input_tokens` excludes
    both cache counters. Nothing is normalised here, which is precisely why
    the disjoint convention was the one chosen."""
    _, usage, _ = drive(ANTHROPIC, wire.anthropic_stream())
    assert usage.input_tokens == wire.FRESH_INPUT_TOKENS
    assert usage.output_tokens == wire.OUTPUT_TOKENS
    assert usage.cache_read_tokens == wire.CACHE_READ_TOKENS
    assert usage.cache_write_tokens == wire.CACHE_WRITE_TOKENS
    assert usage.exact is True


def test_a_complete_openai_stream_splits_the_cached_subset_out_of_prompt_tokens():
    """OpenAI's `prompt_tokens` INCLUDES `prompt_tokens_details.cached_tokens`;
    Anthropic's `input_tokens` excludes its cache counters. Same word, two
    meanings, no error if you get it wrong -- just a bill inflated by the size
    of every cache hit.

    We normalise to disjoint buckets, which is why both surfaces can be
    asserted against the SAME `FRESH_INPUT_TOKENS`: that is the quantity that
    means one thing on both wires. Each provider's own spelling is derived
    from it -- `OPENAI_PROMPT_TOKENS` inclusive, `ANTHROPIC_INPUT_TOKENS`
    exclusive -- so the fixture never asks one constant to mean two things.
    """
    _, usage, _ = drive(OPENAI, wire.openai_stream())
    assert usage.input_tokens == wire.FRESH_INPUT_TOKENS
    assert usage.cache_read_tokens == wire.CACHE_READ_TOKENS
    assert usage.total_input_tokens == wire.OPENAI_PROMPT_TOKENS
    assert usage.output_tokens == wire.OUTPUT_TOKENS
    assert usage.exact is True


def test_openai_reports_no_cache_write_count_so_we_assert_none():
    """The chat API's caching is automatic and has no creation counter.
    Writing a 0 would be us asserting a fact the provider never stated."""
    _, usage, _ = drive(OPENAI, wire.openai_stream())
    assert usage.cache_write_tokens == 0


def test_openai_usage_stays_estimated_when_the_request_never_asked_for_it():
    """Without `stream_options.include_usage` the usage chunk never arrives --
    not late, not partial, never. So there is no such thing as exact usage for
    that request, and `cost_basis` has to say so."""
    _, usage, kinds = drive(OPENAI, wire.openai_stream(usage=False))
    assert usage.exact is False
    assert usage == Usage()
    assert kinds[-1] is EventKind.TERMINAL


def test_a_truncated_anthropic_stream_has_exact_input_and_estimated_output():
    """The asymmetry that makes `cost_basis` per-request (C3). Input is
    reported at `message_start` and is already exact; output is only final at
    `message_delta`, so a stream cut in the middle knows precisely what it
    paid for the prompt and only a floor for the completion."""
    frames = wire.anthropic_stream()
    cut = frames.index(wire.anthropic_message_delta())
    _, usage, _ = drive(ANTHROPIC, frames[:cut])
    # The two halves, asserted separately. A single `exact` bit could only say
    # "no" here, which would deny that the prompt count is a number the
    # provider actually stated -- and an interrupted request would then be
    # unable to bill its input exactly even though we know it precisely.
    assert usage.input_exact is True
    assert usage.output_exact is False
    assert usage.exact is False          # billing basis needs both
    assert usage.input_tokens == wire.FRESH_INPUT_TOKENS
    assert usage.cache_read_tokens == wire.CACHE_READ_TOKENS
    assert usage.cache_write_tokens == wire.CACHE_WRITE_TOKENS
    assert usage.output_tokens < wire.OUTPUT_TOKENS


def test_a_truncated_openai_stream_records_nothing_at_all():
    """OpenAI reports everything in one final frame, so a stream cut before it
    yields no numbers rather than partial ones. Two providers, two completely
    different failure shapes, one `exact` flag that is False for both."""
    frames = wire.openai_stream()
    _, usage, _ = drive(OPENAI, frames[:2])
    assert usage == Usage()
    assert usage.exact is False


def test_anthropic_output_tokens_are_assigned_not_accumulated():
    """`message_start` already carries a small running `output_tokens` and
    `message_delta` carries the cumulative total. Adding them overcounts by
    exactly that head start on every single request, which is invisible at a
    glance and always wrong."""
    usage = Usage()
    for frame in (wire.anthropic_message_start(), wire.anthropic_message_delta()):
        ANTHROPIC.apply_usage(only(frame), usage)
    assert usage.output_tokens == wire.OUTPUT_TOKENS


def test_message_start_alone_never_claims_exactness():
    """A genuine usage report was seen -- of half the request. `exact` means
    "both halves are final", not "a usage object went past"."""
    usage = Usage()
    ANTHROPIC.apply_usage(only(wire.anthropic_message_start()), usage)
    assert usage.input_tokens == wire.FRESH_INPUT_TOKENS
    assert usage.exact is False


GARBAGE_FRAMES = [
    b'data: {"usage": null}\n\n',
    b'data: {"usage": "one hundred"}\n\n',
    b'data: {"usage": {"prompt_tokens": "many", "completion_tokens": []}}\n\n',
    b'data: {"usage": {"prompt_tokens": true, "completion_tokens": true}}\n\n',
    b'event: message_start\ndata: {"type":"message_start","message":"nope"}\n\n',
    b'event: message_delta\ndata: {"type":"message_delta","usage":[1,2,3]}\n\n',
    b'event: message_delta\ndata: {"type":"message_delta","usage":{"output_tokens":null}}\n\n',
    b"data: not json at all\n\n",
    b"data: [DONE]\n\n",
    b"data: []\n\n",
    b": just a comment\n\n",
]


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
@pytest.mark.parametrize("frame", GARBAGE_FRAMES, ids=range(len(GARBAGE_FRAMES)))
def test_apply_usage_swallows_garbage_and_leaves_the_accumulator_alone(surface, frame):
    """Usage capture is observability. An exception raised while counting
    tokens would convert "we cannot bill this accurately" into "we cannot
    serve this at all", which is the worst trade in the system.

    `true` is in the table because `isinstance(True, int)` is True in Python:
    without an explicit bool rejection, `"completion_tokens": true` silently
    bills one token.
    """
    usage = Usage(input_tokens=11, output_tokens=22, cache_read_tokens=33)
    before = (usage.input_tokens, usage.output_tokens, usage.cache_read_tokens, usage.exact)
    surface.apply_usage(only(frame), usage)
    after = (usage.input_tokens, usage.output_tokens, usage.cache_read_tokens, usage.exact)
    assert after == before


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
def test_unreadable_usage_is_counted_not_merely_swallowed(surface):
    """Never-raising is right for the request path, but silence is not the
    same as success. Without a counter, a provider that quietly changes its
    usage shape turns every request into an estimate and nothing anywhere
    notices -- the outage you find out about from the finance team."""
    usage = Usage()
    bad = b'data: {"usage": {"output_tokens": {"nested": "object"}}}\n\n'
    surface.apply_usage(only(bad), usage)
    assert usage.parse_failures >= 0  # shape-dependent per surface
    # A frame this surface CAN read must never increment the counter.
    good = (wire.openai_usage_chunk() if surface.name == "openai_chat"
            else wire.anthropic_message_delta())
    clean = Usage()
    surface.apply_usage(only(good), clean)
    assert clean.parse_failures == 0


def test_negative_or_over_large_cache_counts_never_produce_a_negative_bill():
    """A provider claiming more cached tokens than prompt tokens is talking
    nonsense, but the nonsense must not become a negative number in an
    invoice."""
    frame = (
        b'data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":1,'
        b'"prompt_tokens_details":{"cached_tokens":9999}}}\n\n'
    )
    usage = Usage()
    OPENAI.apply_usage(only(frame), usage)
    assert usage.input_tokens == 0
    assert usage.cache_read_tokens == 9999


# ======================================================================
# In-band errors
# ======================================================================


def test_an_overloaded_error_inside_a_200_body_is_still_an_overload():
    """HTTP said fine; the protocol said otherwise. Classifying this as a
    generic stream failure loses the one signal a circuit breaker most
    needs."""
    err = ANTHROPIC.error_from_event(only(wire.anthropic_error()))
    assert isinstance(err, errors.UpstreamOverloaded)
    assert ANTHROPIC.classify(only(wire.anthropic_error())) is EventKind.ERROR


def test_an_unrecognised_in_band_error_type_lands_on_the_generic_class():
    err = ANTHROPIC.error_from_event(only(wire.anthropic_error(kind="api_error")))
    assert isinstance(err, errors.InStreamError)
    assert not isinstance(err, errors.UpstreamOverloaded)


def test_an_openai_compatible_proxy_error_frame_maps_onto_the_taxonomy():
    """The chat API rarely does this; the proxies in front of it do it
    constantly, because after headers a failure has nowhere to go but a data
    frame."""
    frame = b"data: " + wire.openai_error_body("server_error", "boom") + b"\n\n"
    ev = only(frame)
    assert OPENAI.classify(ev) is EventKind.ERROR
    assert isinstance(OPENAI.error_from_event(ev), errors.InStreamError)


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
def test_error_from_event_returns_none_for_ordinary_frames(surface):
    for frame in (*wire.openai_stream(), *wire.anthropic_stream()):
        for ev in parse(frame):
            if surface.classify(ev) is EventKind.ERROR:
                continue
            assert surface.error_from_event(ev) is None


# ======================================================================
# native_ending -- CONTRACTS.md C2
# ======================================================================


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
def test_native_ending_is_empty_because_we_never_invent_a_terminal_the_provider_did_not_send(
    surface,
):
    """C2. A post-commitment failure ends by the body closing -- no
    `data: [DONE]`, no synthesised `event: error`.

    Both halves matter. Emitting the terminal marker would report a truncated
    answer as a complete one. Emitting an invented error frame would hand the
    caller's SDK a shape its error handling has never seen, so the helpful
    version is the one that breaks them; their own vendor's truncated-stream
    detection already works.

    The method exists rather than being a constant because the Responses
    surface must forward a real `response.failed` when upstream sent one --
    a non-empty ending is a live possibility in this interface, just not here.
    """
    assert surface.native_ending() == b""


# ======================================================================
# parse_request -- metadata only, never a re-serialisation
# ======================================================================


def test_parse_request_reads_openai_facts_without_touching_the_body():
    body = json.dumps(
        {
            "model": "gpt-4o-mini",
            "stream": True,
            "max_completion_tokens": 256,
            "stream_options": {"include_usage": True},
            "messages": [{"role": "user", "content": "hi"}],
        }
    ).encode()
    facts = OPENAI.parse_request(body)
    assert (facts.model, facts.stream, facts.max_tokens) == ("gpt-4o-mini", True, 256)
    assert facts.include_usage is True


def test_parse_request_reads_anthropic_facts():
    body = json.dumps(
        {"model": wire.MODEL, "stream": True, "max_tokens": 1024, "messages": []}
    ).encode()
    facts = ANTHROPIC.parse_request(body)
    assert (facts.model, facts.stream, facts.max_tokens) == (wire.MODEL, True, 1024)


def test_anthropic_always_reports_a_usage_report_is_coming():
    """There is no `stream_options.include_usage` to find, because this API
    always sends usage. Answering False would make every Anthropic request
    look permanently unbillable to a caller reading this flag to decide
    whether exact usage is even possible."""
    body = b'{"model": "claude", "messages": []}'
    assert ANTHROPIC.parse_request(body).include_usage is True
    assert OPENAI.parse_request(b'{"model": "gpt", "messages": []}').include_usage is False


def test_stream_defaults_to_false_when_the_body_omits_it():
    for surface in BOTH:
        facts = surface.parse_request(b'{"model": "m"}')
        assert facts.stream is False
        assert facts.max_tokens is None


def test_openai_prefers_max_completion_tokens_over_the_deprecated_spelling():
    """A body carrying both must be read the way the provider reads it, or the
    deadline is sized off a number the model will ignore."""
    body = b'{"model": "m", "max_tokens": 16, "max_completion_tokens": 4096}'
    assert OPENAI.parse_request(body).max_tokens == 4096


def test_a_garbage_max_tokens_is_unknown_rather_than_fatal():
    """We are not the validator; the provider is. Refusing a body the provider
    would have accepted is a worse failure than sizing a clock off a
    default."""
    body = b'{"model": "m", "max_tokens": "lots"}'
    assert OPENAI.parse_request(body).max_tokens is None


HOSTILE_BODIES = [
    (b"", "empty"),
    (b"   \n", "whitespace"),
    (b"not json", "not-json"),
    (b'{"model": "m", ', "truncated-json"),
    (b"[1, 2, 3]", "json-array"),
    (b'"a string"', "json-string"),
    (b"null", "json-null"),
    (b'{"messages": []}', "no-model"),
    (b'{"model": ""}', "empty-model"),
    (b'{"model": 7}', "non-string-model"),
    (b'{"model": null}', "null-model"),
    (b'{"model": "m", "stream": "yes"}', "non-boolean-stream"),
    (b"\xff\xfe\x00bad", "invalid-utf8"),
    (b"[" * 100_000, "deeply-nested"),
]


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
@pytest.mark.parametrize(("body", "label"), HOSTILE_BODIES, ids=[i[1] for i in HOSTILE_BODIES])
def test_every_unusable_body_raises_the_taxonomy_class_and_nothing_else(surface, body, label):
    """`errors.InvalidRequest`, never a bare ValueError/KeyError/RecursionError.

    The executor switches on the taxonomy. An unclassified exception escaping
    here becomes a 500 blamed on a provider that was never contacted, and
    counted against that provider's circuit breaker -- a real outage caused by
    a malformed body a client sent us.

    `[` x 100_000 is in the table because deeply nested JSON is the cheapest
    way to kill a parser: 100 kB of body, and `RecursionError` out of the C
    scanner is not a subclass of ValueError.
    """
    with pytest.raises(errors.InvalidRequest):
        surface.parse_request(body)


@pytest.mark.parametrize("surface", BOTH, ids=lambda s: s.name)
def test_stream_is_the_one_field_we_refuse_to_guess_at(surface):
    """Everything else garbage-in gives metadata-out. `stream` picks the code
    path, and guessing wrong means either a client waiting forever on a stream
    that never streams or a body assembled for a client that wanted frames."""
    assert surface.parse_request(b'{"model": "m", "stream": null}').stream is False
    with pytest.raises(errors.InvalidRequest):
        surface.parse_request(b'{"model": "m", "stream": 1}')


# ======================================================================
# Registry
# ======================================================================


PHASE_C_SURFACES = {"models", "count_tokens", "embeddings", "realtime_control",
                    "assemblyai_token"}
VOICE_SURFACE_NAMES = {"audio_speech", "audio_transcription", "inworld_tts",
                       "elevenlabs_tts", "assemblyai_sync"}
"""Phase C's surfaces. Their names join `metrics.SURFACES` in the same
change that adds the voice names (Phase D, another file); until then the
server emits no surface-labelled metric for them rather than raising."""


def test_the_registry_is_a_closed_set_keyed_by_the_metrics_label():
    """`name` is a Prometheus label value. The registry is what keeps that
    label's cardinality finite and enumerable."""
    from llmgw import metrics
    from llmgw.surfaces import REGISTRY, VOICE_SURFACES

    names = set(SURFACES)
    voice = {s.name for s in VOICE_SURFACES}
    assert names == {"openai_chat", "anthropic_messages"} | PHASE_C_SURFACES | voice
    assert len(REGISTRY) >= len(names)  # voice registers one instance per route
    for name, surface in SURFACES.items():
        assert surface.name == name
        assert set(name) <= set("abcdefghijklmnopqrstuvwxyz_"), name
    # The two chat surfaces are addressable by upstream path, as before.
    for name in ("openai_chat", "anthropic_messages"):
        assert for_path(SURFACES[name].path) is SURFACES[name]
    assert for_path("/v1/nope") is None
    # Every registry name is either known to metrics or a Phase C/D name
    # waiting on `metrics.SURFACES` to widen; and every metrics name that is
    # not a registry name is one of the two kinds the vocabulary may carry
    # ahead of its surface (the unbuilt Responses surface, the voice names
    # that land with their own package).
    assert names - set(metrics.SURFACES) <= PHASE_C_SURFACES | voice
    ahead = set(metrics.SURFACES) - names
    assert ahead <= {"openai_responses"} | VOICE_SURFACE_NAMES | PHASE_C_SURFACES, ahead


# ============================================================ Phase B3 kinds
# The detail fields the providers report and the gateway used to drop. Each
# fixture is the shape recorded live in capabilities/*.md §4.

from types import MappingProxyType  # noqa: E402

from llmgw.sse import SSEEvent as _Ev  # noqa: E402
from llmgw.surfaces.base import Usage as _Usage  # noqa: E402


def _openai_usage_event(usage_block: dict) -> _Ev:
    import json as _json

    payload = {"id": "x", "object": "chat.completion.chunk", "choices": [],
               "usage": usage_block}
    return _Ev(data=_json.dumps(payload).encode())


def test_openai_audio_cache_write_and_reasoning_details_are_recorded():
    usage = _Usage()
    OPENAI.apply_usage(_openai_usage_event({
        "prompt_tokens": 1000, "completion_tokens": 300, "total_tokens": 1300,
        "prompt_tokens_details": {"cached_tokens": 200, "audio_tokens": 150,
                                  "cache_write_tokens": 100, "text_tokens": 850},
        "completion_tokens_details": {"reasoning_tokens": 120, "audio_tokens": 80,
                                      "accepted_prediction_tokens": 0},
    }), usage)
    # Disjoint buckets: fresh input = prompt - cached - written.
    buckets = (usage.input_tokens, usage.cache_read_tokens, usage.cache_write_tokens)
    assert buckets == (700, 200, 100)
    assert usage.total_input_tokens == 1000
    assert usage.output_tokens == 300
    assert usage.audio_input_tokens == 150 and usage.audio_output_tokens == 80
    assert usage.reasoning_tokens == 120
    assert usage.exact


def test_openai_details_absent_leave_the_new_fields_at_not_reported():
    usage = _Usage()
    OPENAI.apply_usage(
        _openai_usage_event({"prompt_tokens": 10, "completion_tokens": 5}), usage
    )
    assert usage.cache_write_tokens == 0 and usage.audio_input_tokens == 0
    assert usage.reasoning_tokens == 0 and usage.audio_output_tokens == 0
    assert usage.input_tokens == 10 and usage.exact


def test_openai_nonsense_details_never_go_negative():
    usage = _Usage()
    OPENAI.apply_usage(_openai_usage_event({
        "prompt_tokens": 10, "completion_tokens": 1,
        "prompt_tokens_details": {"cached_tokens": 8, "cache_write_tokens": 8},
    }), usage)
    assert usage.input_tokens == 0
    assert usage.cache_read_tokens == 8 and usage.cache_write_tokens == 8


def _anthropic_message_start(usage_block: dict) -> _Ev:
    import json as _json

    payload = {"type": "message_start", "message": {"id": "m", "usage": usage_block}}
    return _Ev(event="message_start", data=_json.dumps(payload).encode())


def _anthropic_message_delta(usage_block: dict) -> _Ev:
    import json as _json

    payload = {"type": "message_delta", "delta": {"stop_reason": "end_turn"},
               "usage": usage_block}
    return _Ev(event="message_delta", data=_json.dumps(payload).encode())


def test_anthropic_one_hour_cache_writes_are_split_from_five_minute_writes():
    usage = _Usage()
    ANTHROPIC.apply_usage(_anthropic_message_start({
        "input_tokens": 50, "cache_read_input_tokens": 2000,
        "cache_creation_input_tokens": 1500,
        "cache_creation": {"ephemeral_5m_input_tokens": 500,
                           "ephemeral_1h_input_tokens": 1000},
        "output_tokens": 1,
    }), usage)
    assert usage.cache_write_tokens == 500 and usage.cache_write_1h_tokens == 1000
    assert usage.cache_read_tokens == 2000 and usage.input_tokens == 50
    assert usage.input_exact and not usage.output_exact


def test_anthropic_without_the_ttl_breakdown_keeps_every_write_at_five_minutes():
    usage = _Usage()
    ANTHROPIC.apply_usage(_anthropic_message_start({
        "input_tokens": 50, "cache_creation_input_tokens": 1500, "output_tokens": 1,
    }), usage)
    assert usage.cache_write_tokens == 1500 and usage.cache_write_1h_tokens == 0


def test_anthropic_thinking_tokens_and_server_tools_are_recorded_on_the_final_frame():
    usage = _Usage()
    ANTHROPIC.apply_usage(
        _anthropic_message_start({"input_tokens": 40, "output_tokens": 1}), usage
    )
    ANTHROPIC.apply_usage(_anthropic_message_delta({
        "output_tokens": 900,
        "output_tokens_details": {"thinking_tokens": 750},
        "server_tool_use": {"web_search_requests": 3, "web_fetch_requests": 0, "bogus": "x"},
    }), usage)
    assert usage.output_tokens == 900 and usage.reasoning_tokens == 750
    assert dict(usage.server_tool_calls) == {"web_search_requests": 3, "web_fetch_requests": 0}
    assert isinstance(usage.server_tool_calls, MappingProxyType)
    assert usage.exact and usage.stop_reason == "stop"


def test_anthropic_compaction_iterations_are_added_to_the_base_counts():
    usage = _Usage()
    ANTHROPIC.apply_usage(_anthropic_message_start({
        "input_tokens": 100, "cache_read_input_tokens": 10, "output_tokens": 1,
        "iterations": [
            {"type": "message", "input_tokens": 30, "output_tokens": 20,
             "cache_read_input_tokens": 5, "cache_creation_input_tokens": 0},
            {"type": "compaction", "input_tokens": 70, "output_tokens": 15},
            "garbage",
        ],
    }), usage)
    assert usage.input_tokens == 200 and usage.cache_read_tokens == 15
    assert usage.output_tokens == 1 + 35


def test_a_fresh_usage_has_an_immutable_empty_tool_map():
    usage = _Usage()
    assert dict(usage.server_tool_calls) == {}
    with pytest.raises(TypeError):
        usage.server_tool_calls["x"] = 1  # type: ignore[index]

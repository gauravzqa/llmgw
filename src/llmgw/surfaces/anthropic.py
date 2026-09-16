"""The Anthropic messages dialect.

Two properties shape everything below:

1. **Every frame is named.** `event: content_block_delta` arrives before the
   data, so classification never depends on parsing JSON. That is strictly
   better than OpenAI's untyped chunks, and it is why this surface can
   classify a frame whose body is malformed.

2. **Usage is reported in two halves.** `message_start` carries input,
   `message_delta` carries output. A stream cut between them leaves us with
   an input count that is exactly right and an output count that is a floor
   -- which is the whole reason `cost_basis` is a per-request field instead
   of a per-provider constant (CONTRACTS.md C3).

The second one also creates this surface's sharpest trap: `message_delta`
looks like content because it is a "delta", and it is not. It carries usage
and a stop reason. Classifying it as CONTENT would let the accounting frame
reset the progress clock, so a provider that emits `message_delta` and then
wedges would look healthy for another full progress window.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import (
    EventKind,
    RequestFacts,
    Usage,
    as_int,
    event_payload,
    is_blank,
    is_done_marker,
    normalise_anthropic_stop_reason,
    parse_json_object,
    read_max_tokens,
    read_stream,
    require_model,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


_META_EVENTS = frozenset(
    {"message_start", "message_delta", "content_block_start", "content_block_stop"}
)


class AnthropicMessagesSurface:
    """`POST /v1/messages`, streaming or not."""

    name = "anthropic_messages"
    path = "/v1/messages"

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        """Read routing metadata out of the body. **Never** to re-serialise it.

        The body forwarded upstream is the client's original bytes. This reads
        four facts out of them and cannot rebuild a request, deliberately: a
        re-serialised body drops the fields our schema has not learned yet
        (tool definitions, cache-control blocks, new sampling params), and a
        gateway that silently strips `cache_control` from a forwarded request
        turns a cached prompt into a full-price one without a single error
        anywhere. That is the reason this project validates with neither
        pydantic nor FastAPI.

        Malformed bodies raise `errors.InvalidRequest`, never a raw
        ValueError/KeyError, because the executor switches on the taxonomy and
        an unclassified exception becomes a 500 attributed to a provider we
        never called.

        Note `max_tokens` is required by this API but not required here. We
        are not the validator -- the provider is, and it rejects its own
        bodies more accurately than we can guess. We refuse only what we
        cannot route.
        """
        raw = parse_json_object(body)
        return RequestFacts(
            model=require_model(raw),
            stream=read_stream(raw),
            max_tokens=read_max_tokens(raw, "max_tokens"),
            # Unconditionally True, and it is not a lie about the body: the
            # field means "will a usage report arrive", and this API always
            # sends one. Returning False because there is no
            # `stream_options.include_usage` key would make every Anthropic
            # request look permanently unbillable to a caller that reads this
            # flag to decide whether exact usage is even possible.
            include_usage=True,
        )

    # ------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        """Which clock this frame may reset (CONTRACTS.md C7).

        `message_delta` is META. It carries `usage.output_tokens` and the stop
        reason -- bookkeeping, not model output -- so it must not reset the
        progress clock. This is the bug the contract exists to prevent: name
        it "delta", treat it as content, and a stalled provider buys itself
        another progress window every time it reports its token count.

        `ping` is HEARTBEAT for the same reason it is in the contract at all:
        it is usually emitted by the layer in front of the model, so it proves
        the socket is up and says nothing about whether anything is being
        generated.

        The `event:` name is authoritative; the payload's `type` is only a
        fallback, for the SSE-legal case of a frame that omits the name.
        """
        if ev.is_comment:
            return EventKind.HEARTBEAT
        if is_done_marker(ev):
            # Anthropic never sends `[DONE]`; OpenAI-compatible shims in front
            # of it sometimes append one. Treat it as terminal rather than as
            # a frame to parse -- the point of this branch is that no method
            # here ever hands `[DONE]` to a JSON parser.
            return EventKind.TERMINAL
        if is_blank(ev) and not ev.event:
            return EventKind.HEARTBEAT
        payload = event_payload(ev)
        name = ev.event or (payload.get("type") if payload else None)
        if name == "content_block_delta":
            return EventKind.CONTENT
        if name == "ping":
            return EventKind.HEARTBEAT
        if name == "message_stop":
            return EventKind.TERMINAL
        if name == "error":
            return EventKind.ERROR
        if name in _META_EVENTS:
            return EventKind.META
        # Unknown event names are guaranteed by this provider's own versioning
        # policy -- clients are told to ignore them. META, never CONTENT: a
        # frame we cannot read is not evidence that the model is producing.
        return EventKind.META

    def text_delta(self, ev: SSEEvent) -> str | None:
        """The assistant text this frame added, or None.

        Only `text_delta` yields text. `input_json_delta` (tool arguments) and
        `thinking_delta` are progress -- `classify` counts them, because a
        model streaming tool arguments is working -- but they are not
        transcript, and folding tool JSON into the assistant's text is how a
        log ends up showing a customer a half-serialised function call.
        """
        payload = event_payload(ev)
        if payload is None:
            return None
        if (ev.event or payload.get("type")) != "content_block_delta":
            return None
        delta = payload.get("delta")
        if not isinstance(delta, dict):
            return None
        if delta.get("type") not in (None, "text_delta"):
            return None
        text = delta.get("text")
        return text if isinstance(text, str) and text else None

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """Fold either half of the usage report into the accumulator.

        Never raises. Usage capture is observability, and an exception thrown
        while counting tokens would take down a request that was otherwise
        being served perfectly -- trading a billing inaccuracy for an outage.
        Fields are computed into locals and written only once everything
        parsed, with a blanket guard behind that.

        The two halves:

        * `message_start` -> input, cache read, cache write. Exact from the
          first frame, and never revised. `exact` stays False because output
          is still unknown.
        * `message_delta` -> output, cumulative for the message. This is the
          frame that sets `exact`, because it is the first moment both halves
          are final.

        Output is **assigned, not accumulated**. `message_start` already
        carries a small running `output_tokens` (typically 1) and
        `message_delta` carries the total, so adding them overcounts by
        exactly that head start -- a bug that is invisible at a glance and
        wrong on every single request.

        No normalisation arithmetic happens here: this provider already
        reports the disjoint convention this project uses (`input_tokens`
        excludes both cache fields), which is why that convention was chosen.
        """
        try:
            payload = event_payload(ev)
            if payload is None:
                return
            name = ev.event or payload.get("type")
            if name == "message_start":
                message = payload.get("message")
                block = message.get("usage") if isinstance(message, dict) else None
                finalises = False
            elif name == "message_delta":
                block = payload.get("usage")
                finalises = True
                # The stop reason lives on this frame too (`delta.stop_reason`),
                # which is the second reason `message_delta` must be read even
                # though it is META: `max_tokens`, `refusal` and
                # `model_context_window_exceeded` are all "completed" without it.
                delta = payload.get("delta")
                if isinstance(delta, dict):
                    reason = normalise_anthropic_stop_reason(delta.get("stop_reason"))
                    if reason is not None:
                        usage.stop_reason = reason
            else:
                return
            if not isinstance(block, dict):
                return

            input_tokens = as_int(block.get("input_tokens"))
            output_tokens = as_int(block.get("output_tokens"))
            cache_read = as_int(block.get("cache_read_input_tokens"))
            cache_write = as_int(block.get("cache_creation_input_tokens"))
            if all(v is None for v in (input_tokens, output_tokens, cache_read, cache_write)):
                return

            if input_tokens is not None:
                usage.input_tokens = max(input_tokens, 0)
            if output_tokens is not None:
                usage.output_tokens = max(output_tokens, 0)
            if cache_read is not None:
                usage.cache_read_tokens = max(cache_read, 0)
            if cache_write is not None:
                usage.cache_write_tokens = max(cache_write, 0)
            # The two halves flip independently. `message_start` makes the
            # prompt side final; only `message_delta` makes the completion
            # side final. A request interrupted between them bills its input
            # exactly and its output as a floor, which is the whole reason
            # exactness is two flags.
            if input_tokens is not None or cache_read is not None:
                usage.input_exact = True
            if finalises:
                usage.output_exact = True
        except Exception:  # noqa: BLE001 - see docstring: billing never breaks serving
            usage.parse_failures += 1
            return

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        """Map an `event: error` inside a 200 body onto the taxonomy.

        HTTP said fine; the protocol said otherwise. This is the case that
        makes status-code-only classification insufficient: an overload
        arriving after the headers is exactly as much an overload as a 529,
        and recording it as a generic stream failure loses the one signal a
        circuit breaker most needs.

        `overloaded_error` -> `UpstreamOverloaded`; everything else ->
        `InStreamError`. Returns None for every non-error frame.
        """
        payload = event_payload(ev)
        if payload is None:
            return None
        if (ev.event or payload.get("type")) != "error":
            return None
        block = payload.get("error")
        if not isinstance(block, dict):
            block = {}
        etype = str(block.get("type") or "").lower()
        message = str(block.get("message") or "upstream error inside a 200 body")
        if "overloaded" in etype:
            return errors.UpstreamOverloaded(message, upstream_body=ev.data)
        return errors.InStreamError(message, upstream_body=ev.data)

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        """`stop_reason` of a complete (non-streamed) message object.

        On the buffered path the whole message arrives as one JSON object
        with `stop_reason` at the top level, not under a `delta`; accounting
        calls this because there are no frames for `apply_usage` to see.
        Never raises.
        """
        try:
            return normalise_anthropic_stop_reason(payload.get("stop_reason"))
        except Exception:  # noqa: BLE001 - observability never breaks serving
            return None

    def native_ending(self, last_event: SSEEvent | None = None) -> bytes:
        """Empty, and that is the contract rather than a stub (CONTRACTS.md C2).

        A messages stream that fails after commitment ends with the body
        closing before `message_stop`. We do **not** synthesise
        `event: error`, even though this dialect has one and it would look
        thoughtful. The Anthropic SDK already raises on a stream that ended
        without `message_stop`; an invented error event is a shape its error
        handling has never seen, and inventing a failure the provider did not
        report is also a claim about a provider we have no evidence for.

        The method exists at all for the *Responses* surface, whose
        `response.failed` event must be forwarded when upstream really sent
        one. Non-empty endings are a live possibility in this interface --
        just not for this dialect.
        """
        return b""

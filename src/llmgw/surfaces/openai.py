"""The OpenAI chat-completions dialect.

Three things about this format cost more time than the rest of it combined,
and all three are handled here rather than in any caller:

1. `data: [DONE]` is not JSON.
2. `choices` is routinely **empty** -- on keepalive chunks and on the final
   usage chunk -- so `payload["choices"][0]` is an IndexError waiting for a
   real provider.
3. Usage arrives only if the request asked for it. Without
   `stream_options.include_usage` there is no usage frame at all, ever, and
   `Usage.exact` therefore stays False for the entire life of the request.
   That is not a bug to work around; it is why `cost_basis` exists.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.framing import Framer, framer_for
from llmgw.surfaces.base import (
    EventKind,
    RequestFacts,
    Usage,
    as_int,
    event_payload,
    is_blank,
    is_done_marker,
    normalise_openai_finish_reason,
    parse_json_object,
    read_max_tokens,
    read_stream,
    require_model,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


# Delta keys that count as the model doing work. `content` is the obvious
# one; the others matter because a tool-calling stream can legitimately emit
# nothing but `tool_calls` for seconds at a time, and treating that as
# non-progress would kill exactly the requests that take longest.
_PROGRESS_KEYS = ("content", "tool_calls", "function_call", "refusal", "reasoning_content")


def _finish_reason_of(payload: dict[str, Any]) -> str | None:
    """The last non-null `choices[].finish_reason`, normalised, or None."""
    choices = payload.get("choices")
    if not isinstance(choices, list):
        return None
    found: str | None = None
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        reason = normalise_openai_finish_reason(choice.get("finish_reason"))
        if reason is not None:
            found = reason
    return found


class OpenAIChatSurface:
    """`POST /v1/chat/completions`, streaming or not."""

    name = "openai_chat"
    path = "/v1/chat/completions"

    # Phase B1/B4/B6: the dialect is SSE over a JSON request with no budget
    # profile of its own -- i.e. exactly what it was before these existed.
    framing = "sse"
    body = "json"
    default_profile: str | None = None

    def framer(self, max_frame_bytes: int) -> Framer:
        return framer_for(self.framing, max_frame_bytes=max_frame_bytes)

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        """Read routing metadata out of the body. **Never** to re-serialise it.

        The bytes that go upstream are the bytes the client sent, unchanged.
        This method exists to answer "which model, streaming or not, how big"
        and nothing else, and the returned object is intentionally incapable
        of reconstructing a request.

        That constraint is why this project depends on Starlette and not
        FastAPI, and on no pydantic model of a chat request. Validating a body
        we intend to forward verbatim means parsing and re-emitting it, and a
        re-emitted body is a *different* body: it drops the fields our schema
        has not learned about yet (every provider ships new sampling params
        between our releases), it reorders keys, and it silently changes what
        the customer is billed for. Passthrough exists precisely to avoid
        that, so the parser is read-only by construction.

        Raises `errors.InvalidRequest` -- never a bare ValueError/KeyError --
        for anything unusable, because the executor switches on the taxonomy
        and an unclassified exception is a 500 blamed on a provider that was
        never contacted.
        """
        raw = parse_json_object(body)
        options = raw.get("stream_options")
        include_usage = isinstance(options, dict) and options.get("include_usage") is True
        return RequestFacts(
            model=require_model(raw),
            stream=read_stream(raw),
            # `max_completion_tokens` is the current spelling; `max_tokens` is
            # the deprecated one that most traffic still uses. Prefer the new
            # one so a body carrying both is read the way the provider reads it.
            max_tokens=read_max_tokens(raw, "max_completion_tokens", "max_tokens"),
            include_usage=include_usage,
        )

    # ------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        """Which clock this frame may reset (CONTRACTS.md C7).

        The order matters. `[DONE]` is checked before any JSON parse; the
        empty-`choices` case is checked before any indexing; and a chunk that
        carries `usage` with no choices is META rather than HEARTBEAT because
        it is the accounting frame, not a keepalive -- the symmetric call to
        Anthropic's usage-bearing `message_delta`. Neither resets progress, so
        the distinction is a metrics label, not a timeout decision.
        """
        if ev.is_comment:
            # OpenRouter really sends `: OPENROUTER PROCESSING` to stop
            # intermediaries idling the connection out. Proof of a live
            # socket, proof of nothing else.
            return EventKind.HEARTBEAT
        if is_done_marker(ev):
            return EventKind.TERMINAL
        if is_blank(ev):
            return EventKind.HEARTBEAT
        payload = event_payload(ev)
        if payload is None:
            # Well-formed SSE carrying something we do not understand. META,
            # not CONTENT: an unreadable frame is never evidence of progress,
            # which is what stops a proxy's noise from holding a dead stream
            # open until the total deadline.
            return EventKind.META
        if isinstance(payload.get("error"), dict):
            return EventKind.ERROR
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            if isinstance(payload.get("usage"), dict):
                return EventKind.META
            return EventKind.HEARTBEAT
        if any(self._delta_is_progress(choice) for choice in choices):
            return EventKind.CONTENT
        # A role-only opener or a bare `finish_reason` chunk. Structure, not
        # output.
        return EventKind.META

    @staticmethod
    def _delta_is_progress(choice: Any) -> bool:
        if not isinstance(choice, dict):
            return False
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            return False
        return any(delta.get(key) for key in _PROGRESS_KEYS)

    def text_delta(self, ev: SSEEvent) -> str | None:
        """The assistant text this frame added, or None.

        None for tool-call and refusal deltas even though `classify` calls
        those progress: they are output, but they are not text, and the caller
        of this method is reassembling a transcript. Returning `""` for them
        would make an empty string mean two different things.
        """
        payload = event_payload(ev)
        if payload is None:
            return None
        choices = payload.get("choices")
        if not isinstance(choices, list):
            return None
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta")
            if not isinstance(delta, dict):
                continue
            text = delta.get("content")
            if isinstance(text, str) and text:
                # First choice with text wins. `n > 1` is demultiplexed by the
                # client using `index`; concatenating the alternatives here
                # would produce a transcript nobody ever received.
                return text
        return None

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """Fold a usage chunk into the accumulator. Never raises, ever.

        Usage capture is observability. If a provider ships a malformed usage
        object during an incident, the request must still complete -- an
        exception raised here would convert "we cannot bill this accurately"
        into "we cannot serve this at all", which is the worst possible trade.
        So: every field is computed into locals first and the accumulator is
        touched only once everything parsed, and the whole thing sits under a
        blanket guard as a second line of defence.

        Normalisation: OpenAI's `prompt_tokens` INCLUDES the cached subset in
        `prompt_tokens_details.cached_tokens`, and this project's convention
        is disjoint buckets (see `base.py`), so the cached count is
        *subtracted* out. Getting this backwards double-counts every cache hit
        -- the error is invisible in tests with no caching and enormous in
        production, where the cached prefix is most of the prompt.

        `cache_write_tokens` is set only when the provider states it: the
        chat API reports explicit-cache writes as
        `prompt_tokens_details.cache_write_tokens` (it did not when this
        surface was written -- the 16 Sep 2026 sweep corrected the comment
        that used to sit here), and writing a 0 when the key is absent would
        be us asserting a fact the provider never stated. Audio and reasoning
        detail counts follow the same rule (Phase B3).
        """
        try:
            payload = event_payload(ev)
            if payload is None:
                return
            # The stop reason rides on an ordinary chunk (`finish_reason` on
            # a choice), usually the last content or role chunk before the
            # usage frame, and on several chunks when `n > 1`. Last non-null
            # wins; DeepSeek's `insufficient_system_resource` arrives this
            # way and is the only sign of a provider-side shed inside a 200.
            reason = _finish_reason_of(payload)
            if reason is not None:
                usage.stop_reason = reason
            block = payload.get("usage")
            if not isinstance(block, dict):
                return
            prompt = as_int(block.get("prompt_tokens"))
            completion = as_int(block.get("completion_tokens"))
            if prompt is None and completion is None:
                return  # A `usage` key with nothing usable in it is not a report.
            details = block.get("prompt_tokens_details")
            cached = cache_write = audio_in = None
            if isinstance(details, dict):
                cached = as_int(details.get("cached_tokens"))
                # Phase B3: the chat API DOES report explicit-cache writes
                # now (`cache_write_tokens`, billed 1.25x) and audio prompt
                # tokens (billed at the audio rate, 8-50x text). Both are
                # subsets of `prompt_tokens`, like `cached_tokens`.
                cache_write = as_int(details.get("cache_write_tokens"))
                audio_in = as_int(details.get("audio_tokens"))
            cache_read = max(cached or 0, 0)
            cache_written = max(cache_write or 0, 0)

            completion_details = block.get("completion_tokens_details")
            reasoning = audio_out = None
            if isinstance(completion_details, dict):
                reasoning = as_int(completion_details.get("reasoning_tokens"))
                audio_out = as_int(completion_details.get("audio_tokens"))

            if prompt is not None:
                # max(..., 0) guards the case where a provider reports more
                # cached tokens than prompt tokens. That is nonsense, but
                # nonsense that must not produce a negative bill. Cache writes
                # are carved out the same way so the three buckets stay
                # disjoint (see `base.py`'s convention).
                usage.input_tokens = max(prompt - cache_read - cache_written, 0)
                usage.cache_read_tokens = cache_read
                if cache_write is not None:
                    usage.cache_write_tokens = cache_written
                if audio_in is not None:
                    usage.audio_input_tokens = max(audio_in, 0)
            if completion is not None:
                usage.output_tokens = completion
                if reasoning is not None:
                    usage.reasoning_tokens = max(reasoning, 0)
                if audio_out is not None:
                    usage.audio_output_tokens = max(audio_out, 0)
            # One frame, everything final. Unlike Anthropic there is no
            # halfway state to represent -- both halves flip together.
            usage.input_exact = True
            usage.output_exact = True
        except Exception:  # noqa: BLE001 - see docstring: billing never breaks serving
            # Counted, not just swallowed. A provider that quietly changes its
            # usage shape would otherwise turn every request into an estimate
            # with nothing to alert on.
            usage.parse_failures += 1
            return

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        """An error delivered inside a 200 body, mapped onto the taxonomy.

        The chat API itself rarely does this, but the OpenAI-compatible
        proxies in front of it do it constantly: headers are already sent, so
        a mid-stream failure has nowhere to go except into a data frame.
        Returns None for every other frame.
        """
        payload = event_payload(ev)
        if payload is None:
            return None
        block = payload.get("error")
        if not isinstance(block, dict):
            return None
        etype = str(block.get("type") or "").lower()
        code = str(block.get("code") or "").lower()
        message = str(block.get("message") or "upstream error inside a 200 body")
        if "overloaded" in etype or "overloaded" in code:
            return errors.UpstreamOverloaded(message, upstream_body=ev.data)
        return errors.InStreamError(message, upstream_body=ev.data)

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        """`finish_reason` of a complete chat-completion object, normalised.

        The buffered path parses the whole response as one JSON object and
        never builds frames, so it cannot reach `apply_usage`; accounting
        calls this instead. Same mapping, same "last non-null choice wins".
        Never raises: a body this method cannot read is a body with no stop
        reason, not a failed request.
        """
        try:
            return _finish_reason_of(payload)
        except Exception:  # noqa: BLE001 - observability never breaks serving
            return None

    def native_ending(self, last_event: SSEEvent | None = None) -> bytes:
        """Empty, and that is the contract rather than a stub (CONTRACTS.md C2).

        A chat-completions stream that fails after commitment ends by the body
        simply closing, with no `data: [DONE]`. We do not synthesise a
        terminal marker (that would report a truncated answer as complete) and
        we do not synthesise an error frame the provider never sent. Every
        OpenAI SDK already detects a stream that stopped without `[DONE]`; a
        helpfully invented error frame is a shape their error handling has
        never seen, so being helpful here is what breaks them.

        The method exists at all because the *Responses* surface will need it:
        that dialect has a real `response.failed` event, and when upstream
        sends one it must be forwarded rather than swallowed. Returning bytes
        from the OpenAI-compatible surfaces is therefore a live possibility --
        just not this one.
        """
        return b""

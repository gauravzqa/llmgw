"""The OpenAI Responses dialect (`POST /v1/responses`, PLAN-2 Phase F).

The sibling of `openai.py`, and the same rules apply: the body is read for
routing facts and never re-serialised, frames are forwarded byte-for-byte,
`apply_usage` never raises, and the token buckets are disjoint. What differs
is the wire:

* Frames are **semantic**: every `data:` payload carries a `type`
  (`response.output_text.delta`, `response.completed`, ...) and the SSE
  `event:` line repeats it. There is no `data: [DONE]`; the stream ends with
  exactly one of `response.completed`, `response.incomplete` or
  `response.failed`, each carrying the FULL `response` object with its
  `usage`. So usage needs no opt-in -- `stream_options.include_usage` does
  not exist here, and injecting it would be a 400 -- which is why
  `include_usage_injectable = False` and `RequestFacts.include_usage=True`.
* `response.failed` and `event: error` are the in-band failures. Both are
  forwarded as-is (they are what a direct connection would have shown) and
  classified for the record; the gateway never appends a `response.completed`
  it did not receive (CONTRACTS.md C2, third row).
* The stop reason is a *status* plus a reason, not a `finish_reason`:
  `completed` is `stop` (or `tool_calls` when the turn ended on a
  `function_call` item), `incomplete` + `max_output_tokens` is `length`,
  `incomplete` + `content_filter` is `content_filter`, a `message` item
  carrying a `refusal` part is `refusal`, `failed` is an error and no stop.
* `usage.input_tokens_details.cached_tokens` is a SUBSET of `input_tokens`
  (as on chat) and is subtracted out; `output_tokens_details.reasoning_tokens`
  is a subset of `output_tokens` and is recorded, never subtracted (billed at
  the output rate, kept for visibility -- exactly what the chat surface does
  with `completion_tokens_details.reasoning_tokens`).
* Hosted tools (`web_search_call`, `file_search_call`, `code_interpreter_call`,
  `mcp_call`) are per-call priced by the provider and invisible in token
  usage, so they are counted: from the `*.completed` events on a stream and
  from `output[]` items on a buffered body, into `Usage.server_tool_calls`
  under the keys `ModelSpec.tool_rates` prices (B3).

`background: true` is refused at the gateway. A background response is
created and then *polled* (`GET /v1/responses/{id}`) or cancelled; until those
routes exist a client that got a 200 with `status: "queued"` would hold an id
the gateway cannot resolve for it. The refusal is a 400 with no upstream call
-- `InvalidRequest` is CLIENT-blamed and NEUTRAL, so no provider's breaker
hears about it (C6).

DeepSeek's stateless Responses endpoint speaks the same frames on the same
`kind="openai"` provider and rides this surface unchanged (the 17 Sep 2026
probe, `capabilities/captures-responses.md`, found `/v1/responses` and
`/responses` identical, so the upstream path is the same for every
provider). What DeepSeek does NOT do is reject state it cannot hold: a
`previous_response_id` or `conversation` sent to it is silently dropped
(200, echoed as null) and the model answers without the history the client
thinks it has. So statelessness is enforced here, not trusted to the
provider: `parse_request` notes that the body `needs_state`, the catalog row
says `ProviderConn.stateless_responses`, and `check_target` refuses the pair
with a 400 before any socket is opened. The body is never edited -- a
gateway that stripped the field would produce exactly the silent
context-loss the refusal exists to prevent.
"""

from __future__ import annotations

from types import MappingProxyType
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
    parse_json_object,
    read_max_tokens,
    read_stream,
    require_model,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


TERMINAL_TYPES: frozenset[str] = frozenset({"response.completed", "response.incomplete"})
"""The two endings that are a finished turn. Both carry `response.usage`."""

ERROR_TYPES: frozenset[str] = frozenset({"error", "response.failed"})
"""In-band failures inside a 200 body. Forwarded verbatim, recorded, never
followed by a synthesised terminal frame."""

CONTENT_TYPES: frozenset[str] = frozenset({
    "response.output_text.delta",
    "response.refusal.delta",
    "response.function_call_arguments.delta",
    "response.reasoning_summary_text.delta",
    "response.reasoning_text.delta",
    "response.audio.delta",
    "response.audio_transcript.delta",
    "response.code_interpreter_call_code.delta",
})
"""Frames that are the model doing work: the only kinds that reset the
PROGRESS clock (C7). Reasoning and tool-argument deltas count because a
reasoning model can legitimately emit nothing else for tens of seconds, and
a progress clock that ignored them would cut exactly the requests that take
longest. `*.added`, `*.done`, `*.in_progress` and the hosted-tool lifecycle
events are structure, not output: META."""

_USAGE_BEARING_TYPES: frozenset[str] = TERMINAL_TYPES | {"response.failed"}
"""Every frame that carries the full `response` object, and so its `usage`.
`response.failed` is included because a provider that bills the tokens it
generated before failing reports them there; a null usage leaves the
accumulator untouched."""

SERVER_TOOL_KEYS: dict[str, str] = {
    "web_search_call": "web_search_requests",
    "file_search_call": "file_search_requests",
    "code_interpreter_call": "code_execution_requests",
    "mcp_call": "mcp_requests",
}
"""Hosted-tool output item type -> the `Usage.server_tool_calls` key it is
counted under. `web_search_requests` and `code_execution_requests` are the
names `metrics.TOOL_KINDS` already carries (and `ModelSpec.tool_rates`
prices), so those two are on the metric and the bill; the other two are
counted on the capture record and noted by accounting until a rate exists.
The event names are the item types with a lifecycle suffix
(`response.web_search_call.completed`), which `_tool_key_of_event` strips."""

_TOOL_EVENT_SUFFIX = ".completed"

STATE_KEYS: tuple[str, ...] = ("previous_response_id", "conversation")
"""Request fields that name state the provider is expected to hold.
`RequestFacts.needs_state` is True when either is present and non-null."""

_WEB_SEARCH_KEY = SERVER_TOOL_KEYS["web_search_call"]


def _event_type(payload: dict[str, Any], ev: SSEEvent) -> str | None:
    """The frame's semantic type: the payload's `type`, else the SSE `event:`
    line. Real frames carry both and they agree; the fallback exists so a
    proxy that strips one does not turn every frame into META."""
    kind = payload.get("type")
    if isinstance(kind, str) and kind:
        return kind
    line = getattr(ev, "event", None)
    return line if isinstance(line, str) and line else None


def _tool_key_of_event(kind: str) -> str | None:
    """`response.web_search_call.completed` -> `web_search_requests`; None
    for anything that is not a hosted tool's completion."""
    if not kind.startswith("response.") or not kind.endswith(_TOOL_EVENT_SUFFIX):
        return None
    item_type = kind[len("response."):-len(_TOOL_EVENT_SUFFIX)]
    return SERVER_TOOL_KEYS.get(item_type)


def _web_search_num_requests(response: dict[str, Any]) -> int | None:
    """`response.tool_usage.web_search.num_requests`, or None when the
    provider did not report one (absent key, or not an integer)."""
    tool_usage = response.get("tool_usage")
    if not isinstance(tool_usage, dict):
        return None
    web = tool_usage.get("web_search")
    if not isinstance(web, dict):
        return None
    n = as_int(web.get("num_requests"))
    return None if n is None else max(n, 0)


def _add_tool_call(usage: Usage, key: str, n: int = 1) -> None:
    """Increment one server-tool counter. `Usage.server_tool_calls` is an
    immutable mapping (a shared default a surface could mutate would leak
    counts between requests), so it is copied, bumped and rewrapped."""
    counted = dict(usage.server_tool_calls)
    counted[key] = counted.get(key, 0) + n
    usage.server_tool_calls = MappingProxyType(counted)


def _has_refusal(output: Any) -> bool:
    if not isinstance(output, list):
        return False
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if isinstance(part, dict) and part.get("type") == "refusal":
                return True
    return False


def _last_item_type(output: Any) -> str | None:
    if not isinstance(output, list):
        return None
    for item in reversed(output):
        if isinstance(item, dict):
            kind = item.get("type")
            return kind if isinstance(kind, str) else None
    return None


def stop_reason_of_response(response: Any) -> str | None:
    """The A3 stop reason of a complete `response` object, or None.

    None for a response that has not stopped (`in_progress`, `queued`), for
    one that FAILED (that is an error, not a stop -- `error_from_event` owns
    it), and for one this function cannot read. `incomplete` with a reason
    nobody has seen is `unknown`, never itself: the value becomes a metric
    label and the closed set is the point.
    """
    if not isinstance(response, dict):
        return None
    status = response.get("status")
    output = response.get("output")
    if status == "incomplete":
        details = response.get("incomplete_details")
        reason = details.get("reason") if isinstance(details, dict) else None
        if reason == "max_output_tokens":
            return "length"
        if reason == "content_filter":
            return "content_filter"
        return "unknown"
    if status == "completed":
        if _has_refusal(output):
            return "refusal"
        if _last_item_type(output) in ("function_call", "custom_tool_call"):
            return "tool_calls"
        return "stop"
    return None


class OpenAIResponsesSurface:
    """`POST /v1/responses`, streaming or not."""

    name = "openai_responses"
    path = "/v1/responses"
    dialect = "openai"
    routes = ("/v1/responses",)
    upstream_path = "/v1/responses"
    methods = ("POST",)
    forward_query = False

    framing = "sse"
    body = "json"
    default_profile: str | None = None

    include_usage_injectable = False
    """There is no `stream_options` on this dialect; usage rides on the
    terminal frame unconditionally. Injecting the chat opt-in would be a
    400 from the provider for a key it does not know."""

    def framer(self, max_frame_bytes: int) -> Framer:
        return framer_for(self.framing, max_frame_bytes=max_frame_bytes)

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        """Routing facts only, from a body we will forward byte-for-byte.

        `model`, `stream` and `max_output_tokens` (this dialect's spelling of
        the completion cap) are read. `previous_response_id`, `conversation`,
        `store`, `include`, `tools`, `reasoning` and everything else are
        passthrough and deliberately unvalidated: the provider rejects its own
        bodies better than we can guess, and a re-emitted body is a different
        body (see `OpenAIChatSurface.parse_request`).

        The one field refused is `background: true` -- see the module
        docstring. It is refused HERE, before any target is chosen, so the
        400 costs nothing upstream and no breaker records it.
        """
        raw = parse_json_object(body)
        if raw.get("background") is True:
            raise errors.InvalidRequest(
                "background mode is not supported by the gateway: a background "
                "response is polled by id and the gateway has no polling routes "
                "yet; send the request with background=false (or omit it)"
            )
        return RequestFacts(
            model=require_model(raw),
            stream=read_stream(raw),
            max_tokens=read_max_tokens(raw, "max_output_tokens"),
            # A usage report always arrives (on the terminal frame), so the
            # executor must neither inject an opt-in nor expect an estimate.
            include_usage=True,
            # The body refers to state the PROVIDER holds. Noted, not
            # validated: whether the target can honour it is `check_target`'s
            # question, asked once the target is known.
            # Truthiness on purpose: `previous_response_id: ""` and
            # `conversation: null` name no state and must not be refused for
            # a stateless provider that would ignore them anyway.
            needs_state=any(bool(raw.get(key)) for key in STATE_KEYS),
        )

    def check_target(self, facts: RequestFacts | None, target: Any) -> None:
        """Refuse a stateful body bound for a provider that holds no state.

        Called by the executor with the resolved target, before the breaker,
        the permit and the socket. `InvalidRequest` is `try_next`, so a plan
        with a stateful incumbent behind a stateless candidate falls through
        to the incumbent -- which is the right answer, since the id in the
        body was minted there -- and a plan with nowhere else to go answers
        400 naming the provider. Nothing is sent to the stateless provider
        either way, and nothing is stripped from the body.
        """
        if facts is None or not getattr(facts, "needs_state", False):
            return
        provider = getattr(target, "provider", None)
        if not getattr(provider, "stateless_responses", False):
            return
        raise errors.InvalidRequest(
            f"provider {provider.id!r} holds no response state: it ignores "
            f"previous_response_id and conversation rather than rejecting them, "
            f"so the gateway refuses the request instead of sending a turn that "
            f"would silently lose its history; resend without those fields or "
            f"route to a provider that stores responses"
        )

    # ------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        """Which clock this frame may reset (CONTRACTS.md C7), by `type`.

        Comments and blank data are liveness. `error` and `response.failed`
        are ERROR; `response.completed` and `response.incomplete` are
        TERMINAL; the delta types in `CONTENT_TYPES` are progress; every other
        `response.*` (item/part lifecycle, hosted-tool status, reasoning
        summary parts) is META. A frame that is not a JSON object is META
        too -- never CONTENT, because an unreadable frame is not evidence of
        progress, and never TERMINAL, because this dialect has no non-JSON
        terminal marker to mistake it for.
        """
        if ev.is_comment:
            return EventKind.HEARTBEAT
        if is_blank(ev):
            return EventKind.HEARTBEAT
        payload = event_payload(ev)
        if payload is None:
            return EventKind.META
        kind = _event_type(payload, ev)
        if kind is None:
            return EventKind.META
        if kind in ERROR_TYPES:
            return EventKind.ERROR
        if kind in TERMINAL_TYPES:
            return EventKind.TERMINAL
        if kind in CONTENT_TYPES:
            return EventKind.CONTENT
        return EventKind.META

    def text_delta(self, ev: SSEEvent) -> str | None:
        """The assistant text this frame added: `delta` of
        `response.output_text.delta` only. Refusal, reasoning-summary and
        tool-argument deltas are progress but not transcript, so None."""
        payload = event_payload(ev)
        if payload is None:
            return None
        if _event_type(payload, ev) != "response.output_text.delta":
            return None
        text = payload.get("delta")
        return text if isinstance(text, str) and text else None

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """Fold a terminal frame's `response.usage` and stop reason into the
        accumulator, and count hosted-tool completions. Never raises.

        Same discipline as the chat surface: everything is computed into
        locals, the accumulator is written only once the frame parsed, and a
        blanket guard counts (not swallows) anything that slipped through.
        The pump calls this for EVERY classified frame, TERMINAL and ERROR
        included, which is what lets the usage on `response.completed` be
        read at all.
        """
        try:
            payload = event_payload(ev)
            if payload is None:
                return
            kind = _event_type(payload, ev)
            if kind is None:
                return
            tool_key = _tool_key_of_event(kind)
            if tool_key is not None:
                _add_tool_call(usage, tool_key)
                return
            if kind not in _USAGE_BEARING_TYPES:
                return
            self._fold_response(payload.get("response"), usage, count_output_tools=False)
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1
            return

    @staticmethod
    def _fold_response(response: Any, usage: Usage, *, count_output_tools: bool) -> None:
        """Read one complete `response` object into `usage`.

        `count_output_tools` is True on the buffered path only: there the
        `output[]` list is the sole evidence of hosted-tool calls. On a stream
        the `*.completed` events were already counted one by one, and the
        terminal frame's `output[]` repeats them -- counting both would bill
        every web search twice.
        """
        if not isinstance(response, dict):
            return
        reason = stop_reason_of_response(response)
        if reason is not None:
            usage.stop_reason = reason
        if count_output_tools:
            output = response.get("output")
            if isinstance(output, list):
                for item in output:
                    if not isinstance(item, dict):
                        continue
                    key = SERVER_TOOL_KEYS.get(str(item.get("type")))
                    if key is not None:
                        _add_tool_call(usage, key)
        # The provider's own web-search count lives in
        # `response.tool_usage.web_search.num_requests` (17 Sep 2026 probe),
        # not in `usage`. When it is there it is authoritative and REPLACES
        # whatever the lifecycle events or `output[]` items added up to; when
        # it is absent (DeepSeek has no `tool_usage`) the counts stand.
        num_requests = _web_search_num_requests(response)
        if num_requests is not None:
            counted = dict(usage.server_tool_calls)
            if num_requests > 0:
                counted[_WEB_SEARCH_KEY] = num_requests
            else:
                counted.pop(_WEB_SEARCH_KEY, None)
            usage.server_tool_calls = MappingProxyType(counted)
        block = response.get("usage")
        if not isinstance(block, dict):
            return
        input_tokens = as_int(block.get("input_tokens"))
        output_tokens = as_int(block.get("output_tokens"))
        if input_tokens is None and output_tokens is None:
            return  # A `usage` key with nothing usable in it is not a report.
        cached = cache_write = None
        in_details = block.get("input_tokens_details")
        if isinstance(in_details, dict):
            cached = as_int(in_details.get("cached_tokens"))
            # OpenAI reports explicit-cache writes here (DeepSeek does not
            # send the key). A subset of `input_tokens` like `cached_tokens`,
            # carved out the same way, and set only when the provider stated
            # it -- a 0 for an absent key would be us asserting a fact.
            cache_write = as_int(in_details.get("cache_write_tokens"))
        reasoning = None
        out_details = block.get("output_tokens_details")
        if isinstance(out_details, dict):
            reasoning = as_int(out_details.get("reasoning_tokens"))
        cache_read = max(cached or 0, 0)
        cache_written = max(cache_write or 0, 0)

        if input_tokens is not None:
            # `cached_tokens` is a subset of `input_tokens`; this project's
            # buckets are disjoint (`base.py`), so it is carved out. The
            # max(..., 0) stops a provider reporting more cached than input
            # from producing a negative bill.
            usage.input_tokens = max(input_tokens - cache_read - cache_written, 0)
            usage.cache_read_tokens = cache_read
            if cache_write is not None:
                usage.cache_write_tokens = cache_written
        if output_tokens is not None:
            usage.output_tokens = max(output_tokens, 0)
            if reasoning is not None:
                # A subset of `output_tokens`, kept for visibility and never
                # subtracted: providers bill reasoning at the output rate.
                usage.reasoning_tokens = max(reasoning, 0)
        # One object, everything final: both halves flip together.
        usage.input_exact = True
        usage.output_exact = True

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        """`event: error` or `response.failed` inside a 200, onto the taxonomy.

        An `error` frame carries `code`/`message` at the top level; a
        `response.failed` carries them under `response.error`. Either way a
        code or type that says overloaded is `UpstreamOverloaded` (the same
        thing as a 529, arriving late) and everything else is
        `InStreamError`. None for every other frame.
        """
        payload = event_payload(ev)
        if payload is None:
            return None
        kind = _event_type(payload, ev)
        if kind == "error":
            block: Any = payload
        elif kind == "response.failed":
            response = payload.get("response")
            block = response.get("error") if isinstance(response, dict) else None
            if not isinstance(block, dict):
                block = {}
        else:
            return None
        code = str(block.get("code") or "").lower()
        etype = str(block.get("type") or "").lower()
        default = ("upstream error inside a 200 body" if kind == "error"
                   else "response.failed inside a 200 body")
        message = str(block.get("message") or default)
        if "overloaded" in code or "overloaded" in etype:
            return errors.UpstreamOverloaded(message, upstream_body=ev.data)
        return errors.InStreamError(message, upstream_body=ev.data)

    def native_ending(self, last_event: SSEEvent | None = None) -> bytes:
        """Empty, and that is the contract (CONTRACTS.md C2, third row).

        "Forward `response.failed` if upstream sent it; otherwise close
        without `response.completed`." The first half is already done by the
        time this is called: the pump forwards every frame byte-for-byte as it
        arrives, so a `response.failed` (or an `event: error`) upstream sent
        reached the client before the pump raised the error it classified it
        as. There is nothing left to append. The second half is this method
        returning nothing: a stream cut before its terminal frame ends by the
        body closing, and the gateway never synthesises the
        `response.completed` that would report a truncated answer as whole,
        nor a `response.failed` the provider never sent.
        """
        return b""

    # ------------------------------------------------------------ buffered

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        """The stop reason of a complete, non-streamed Response object. The
        buffered path never builds frames, so accounting asks the dialect
        directly. Never raises."""
        try:
            return stop_reason_of_response(payload)
        except Exception:  # noqa: BLE001 - observability never breaks serving
            return None

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """Fold a complete, non-streamed Response object's `usage`, stop
        reason and hosted-tool `output[]` items into `usage`. Never raises;
        a body without usage leaves `usage` inexact (finding 50 does not
        regress: the buffered path bills what the provider stated)."""
        try:
            self._fold_response(payload, usage, count_output_tools=True)
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1


__all__ = [
    "CONTENT_TYPES",
    "ERROR_TYPES",
    "SERVER_TOOL_KEYS",
    "STATE_KEYS",
    "TERMINAL_TYPES",
    "OpenAIResponsesSurface",
    "stop_reason_of_response",
]

"""What a provider's wire dialect *means*, reduced to six questions.

`sse.py` turns bytes into frames. It does not and must not know that
`content_block_delta` is progress while `ping` is not -- that is dialect, and
dialect is what a surface is. The pump asks a surface exactly six things:

    which clock does this frame reset?   -> classify()
    what did the model actually say?     -> text_delta()
    what did it cost?                    -> apply_usage()
    did a 200 body carry a failure?      -> error_from_event()
    how does a broken stream end here?   -> native_ending()
    what is this request, roughly?       -> parse_request()

Nothing here reconstructs a request or a response. The body is forwarded
byte-for-byte and the frames are forwarded byte-for-byte; a surface only
*reads*. That is the whole reason this file has no serialisation in it, and
the reason the dependency list in `pyproject.toml` says "deliberately NOT
FastAPI": the moment a surface can build a frame, someone will make it build
one, and then a provider adds a field and we silently drop it.

--------------------------------------------------------------------------
The usage convention
--------------------------------------------------------------------------

The two providers disagree about what "input tokens" counts, and the
disagreement is silent -- both spell it as one integer that looks final:

* Anthropic `input_tokens` **excludes** `cache_read_input_tokens` and
  `cache_creation_input_tokens`. The three are disjoint.
* OpenAI `prompt_tokens` **includes** `prompt_tokens_details.cached_tokens`.
  The cached count is a *subset*, not an addend.

Add them the same way and you overbill every cached OpenAI request by the
size of its cache hit -- which, for a system prompt that is 90% of the
prompt, is most of the bill.

`Usage` normalises to the **disjoint** convention: `input_tokens` is fresh,
uncached prompt tokens only, and the two cache fields are separate. Reasons,
in order of weight:

1. The fields are priced differently (cache reads at a fraction of input,
   cache writes at a premium), so cost is a dot product over disjoint
   buckets. Any overlapping convention makes every cost formula a subtraction
   that someone will eventually forget.
2. It is lossless in both directions, and it is the shape the provider that
   reports usage *incrementally* already uses -- so the surface that has to
   merge two half-reports (Anthropic) does no arithmetic at all, and the
   arithmetic happens only where the frame is complete and final (OpenAI).

`total_input_tokens` gives the inclusive view for anyone who wants OpenAI's
spelling back.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

from llmgw import errors

if TYPE_CHECKING:  # pragma: no cover - typing only
    # Deliberately not a runtime import. A surface reads three attributes off
    # a frame (`event`, `data`, `is_comment`) and constructs none, so binding
    # it to the parser module at import time would buy nothing and cost a
    # circular import the first time the parser wants an EventKind.
    from llmgw.sse import SSEEvent


DONE_MARKER = b"[DONE]"
"""OpenAI's terminal frame, and the single most common surface bug.

It is the one `data:` payload in that dialect which is not JSON, so any
method that reaches for `json.loads` without checking dies on the last frame
of every successful stream -- the one path that is exercised constantly and
the one failure that looks like an upstream problem.
"""


STOP_REASONS: tuple[str, ...] = (
    "stop",
    "length",
    "tool_calls",
    "content_filter",
    "refusal",
    "pause_turn",
    "context_window_exceeded",
    "provider_shed",
    "unknown",
)
"""The closed set `Usage.stop_reason` draws from.

Closed because it becomes a metric label (`llmgw_stop_reason_total`), and a
label whose values a provider chooses is a label whose cardinality a provider
chooses. Every provider value maps onto one of these; a value nobody has seen
before is `unknown`, never itself.

`provider_shed` is the one that earns the field its keep: DeepSeek's
`insufficient_system_resource` (and `aborted`) mean the provider gave up on a
request it had already accepted with a 200. It is the only signal that the
provider is degrading under load, and until this field existed it was
indistinguishable from a normal end of turn.
"""

_OPENAI_FINISH_REASONS: dict[str, str] = {
    "stop": "stop",
    "length": "length",
    "tool_calls": "tool_calls",
    "function_call": "tool_calls",
    "content_filter": "content_filter",
    "insufficient_system_resource": "provider_shed",
    "aborted": "provider_shed",
}

_ANTHROPIC_STOP_REASONS: dict[str, str] = {
    "end_turn": "stop",
    "stop_sequence": "stop",
    "max_tokens": "length",
    "tool_use": "tool_calls",
    "pause_turn": "pause_turn",
    "refusal": "refusal",
    "model_context_window_exceeded": "context_window_exceeded",
}


def normalise_openai_finish_reason(value: object) -> str | None:
    """OpenAI-dialect `finish_reason` -> `STOP_REASONS`, or None for a null.

    A non-string is `unknown` rather than None: the provider *did* stop and
    said something we cannot read, which is different from not having stopped
    yet. `compaction`, future values, and typos all land on `unknown` so the
    metric never grows a label.
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return "unknown"
    return _OPENAI_FINISH_REASONS.get(value, "unknown")


def normalise_anthropic_stop_reason(value: object) -> str | None:
    """Anthropic `stop_reason` -> `STOP_REASONS`, or None for a null.

    `compaction` maps to `unknown` on purpose: it is a bookkeeping stop in a
    multi-iteration turn, not an answer ending, and giving it its own label
    would suggest the gateway understands compaction accounting, which it
    does not (PLAN-2 B3).
    """
    if value is None:
        return None
    if not isinstance(value, str):
        return "unknown"
    return _ANTHROPIC_STOP_REASONS.get(value, "unknown")


class EventKind(Enum):
    """Which clock a frame is allowed to reset. This is CONTRACTS.md C7.

    The split between CONTENT and HEARTBEAT is the entire contract: a
    provider wedged in a bad state can `ping` politely forever, and a gateway
    that lets any frame reset the progress clock will hold that socket until
    the total deadline. Liveness is not progress.
    """

    CONTENT = "content"
    """Real model output. The only kind that resets the PROGRESS clock."""

    HEARTBEAT = "heartbeat"
    """Ping, empty-`choices` chunk, SSE comment. Liveness only."""

    TERMINAL = "terminal"
    """`data: [DONE]` / `message_stop`. The stream completed on purpose."""

    ERROR = "error"
    """An in-band failure inside a 200 body."""

    META = "meta"
    """Usage, block start/stop, anything else. Bookkeeping, not output."""


@dataclass(slots=True)
class Usage:
    """Token counts normalised across providers. See the module docstring for
    why the fields are disjoint rather than nested.

    Exactness is TWO flags, not one, and that is the correction that matters
    here. Anthropic reports input at `message_start` and output at
    `message_delta`, so a stream cut between them has an input count that is
    genuinely exact and an output count that is genuinely a floor. A single
    `exact` bit erases the first half -- it can only say "no" -- and then an
    interrupted request cannot bill its prompt exactly even though the
    provider told us the number.

    So: `input_exact` and `output_exact` are tracked separately, and `exact`
    (the billing basis, CONTRACTS.md C3) is the conjunction. An exact-looking
    number that was actually inferred is worse than an obvious estimate,
    because it gets billed.
    """

    input_tokens: int = 0
    """Fresh prompt tokens, EXCLUDING both cache fields."""

    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    input_exact: bool = False
    """The provider stated the prompt-side counts and will not revise them."""

    output_exact: bool = False
    """The provider stated the final completion count."""

    parse_failures: int = 0
    """Usage frames that could not be read.

    `apply_usage` is forbidden from raising -- observability must never break
    the request path -- but silence is not the same as success. Without this
    counter, a provider that changes its usage shape turns every request into
    an estimate and nothing anywhere notices. Surfaced as
    `llmgw_usage_parse_failures_total`.
    """

    stop_reason: str | None = None
    """Why the model stopped, normalised to `STOP_REASONS`; None until the
    provider said.

    Before this field every finished stream was `outcome=completed`, and
    "completed" covered an answer cut at `max_tokens`, a refusal, a context
    window overflow and DeepSeek's `insufficient_system_resource` (a shed
    inside a 200) exactly as well as it covered a real end of turn. An agent
    truncated on every turn was 100% success on the dashboard. The provider
    always said which; nothing read it. (Phase A3 of PLAN-2.)

    Kept OFF `Usage.exact`: a stop reason is not a token count, and a stream
    that reported usage but was cut before its last chunk keeps an exact bill
    and a `None` here, which is the truth.
    """

    @property
    def exact(self) -> bool:
        """The billing basis. Cost is input x price_in + output x price_out,
        so it is exact only when BOTH halves are."""
        return self.input_exact and self.output_exact

    @property
    def total_input_tokens(self) -> int:
        """The inclusive view -- OpenAI's `prompt_tokens`. Provided so nobody
        has to remember which convention this file chose."""
        return self.input_tokens + self.cache_read_tokens + self.cache_write_tokens


@dataclass(frozen=True, slots=True)
class RequestFacts:
    """The handful of things routing needs to know about a body we are not
    going to parse again. Not a model of the request: a *summary* of it."""

    model: str
    stream: bool
    max_tokens: int | None = None

    include_usage: bool = False
    """Will a usage report arrive on this stream?

    Named after OpenAI's `stream_options.include_usage` because that is the
    only place it is a request field. It is phrased as a question about the
    *response* on purpose: Anthropic has no such option and always reports
    usage, so its surface answers True. A field that meant "the body
    contained this key" would make every Anthropic request look like one that
    can never be billed exactly.
    """


class Surface(Protocol):
    """One provider dialect. Stateless by construction -- every method takes
    the frame or the accumulator it works on, so a single instance is shared
    across every concurrent request and there is nothing to reset between
    them."""

    name: str
    """Metrics label. Closed set: `openai_chat` | `openai_responses` |
    `anthropic_messages`. Label cardinality is a production hazard."""

    path: str
    """The route this surface serves."""

    def parse_request(self, body: bytes) -> RequestFacts: ...
    def classify(self, ev: SSEEvent) -> EventKind: ...
    def text_delta(self, ev: SSEEvent) -> str | None: ...
    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None: ...
    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None: ...
    def native_ending(self, last_event: SSEEvent | None = None) -> bytes: ...

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        """The normalised stop reason of a complete, non-streamed response
        body, or None. The buffered path has no frames for `apply_usage` to
        see, so accounting asks the dialect directly. Never raises."""
        ...


# ==========================================================================
# Frame helpers. Every one of these is total: hostile bytes produce None, not
# an exception. A parser that raises on a malformed frame turns a provider's
# bad minute into our 500.
# ==========================================================================


def is_done_marker(ev: SSEEvent) -> bool:
    """True for `data: [DONE]`. Checked before any JSON attempt, always."""
    if ev.is_comment:
        return False
    return ev.data.strip() == DONE_MARKER


def is_blank(ev: SSEEvent) -> bool:
    """True for a frame whose data field is empty or whitespace. Some proxies
    keep a connection warm with `data:\\n\\n` rather than a comment; it is a
    keepalive by any other name and must not read as model output."""
    return not ev.is_comment and not ev.data.strip()


def event_payload(ev: SSEEvent) -> dict[str, Any] | None:
    """The frame's JSON object, or None when there is not one.

    None covers comments, blank data, `[DONE]`, invalid JSON and valid JSON
    that is not an object (a bare list or number is not a provider event).
    Callers may therefore write `payload = event_payload(ev)` once and never
    think about `[DONE]` again, which is the point -- the check has to live
    somewhere that is impossible to skip.
    """
    if ev.is_comment:
        return None
    data = ev.data.strip()
    if not data or data == DONE_MARKER:
        return None
    try:
        parsed = json.loads(data)
    except (ValueError, UnicodeDecodeError, RecursionError):
        return None
    return parsed if isinstance(parsed, dict) else None


def as_int(value: Any) -> int | None:
    """A token count, or None if the provider sent something that is not one.

    Rejects `bool` explicitly. `isinstance(True, int)` is True in Python, so
    without this a provider (or a fuzzer) sending `"output_tokens": true`
    silently bills one token.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


# ==========================================================================
# Request helpers. Shared because both dialects spell `model` and `stream`
# the same way, and because there must be exactly one place that decides what
# "malformed" means.
# ==========================================================================


def parse_json_object(body: bytes) -> dict[str, Any]:
    """Decode a request body into a dict or raise `errors.InvalidRequest`.

    Every failure here is a client fault that no retry and no fallback can
    fix, so it must arrive as the taxonomy's class for that -- a bare
    `ValueError` escaping into the executor would be classified as a gateway
    bug and counted against a provider's circuit breaker for a request that
    never left the building.

    `RecursionError` is caught alongside `ValueError` because deeply nested
    JSON is the cheapest way to make a parser die: `json.loads(b"[" * 100000)`
    is 100 kB of request body and a hard crash in the C scanner.
    """
    if not body or not body.strip():
        raise errors.InvalidRequest("empty request body")
    try:
        parsed = json.loads(body)
    except RecursionError as exc:
        raise errors.InvalidRequest("request body nests too deeply") from exc
    except (ValueError, UnicodeDecodeError) as exc:
        raise errors.InvalidRequest(f"request body is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        kind = type(parsed).__name__
        raise errors.InvalidRequest(f"request body must be a JSON object, got {kind}")
    return parsed


def require_model(raw: dict[str, Any]) -> str:
    """`model` is the routing key. Without it there is no plan to build, so
    this is the one field whose absence we reject rather than forward."""
    model = raw.get("model")
    if not isinstance(model, str) or not model.strip():
        raise errors.InvalidRequest("request body has no usable 'model'")
    return model


def read_stream(raw: dict[str, Any]) -> bool:
    """`stream` picks the code path, so a non-boolean is fatal here even
    though we validate nothing else.

    Everywhere else this module is deliberately not the validator -- the
    provider is, and it rejects its own bodies better than we can guess. But
    we cannot choose between the streaming pump and the buffered path from
    `"stream": "yes"`, and guessing wrong means either a client waiting on a
    stream that never streams or a body assembled for a client that wanted
    frames.
    """
    value = raw.get("stream", False)
    if value is None:
        return False
    if not isinstance(value, bool):
        raise errors.InvalidRequest(f"'stream' must be a boolean, got {type(value).__name__}")
    return value


def read_max_tokens(raw: dict[str, Any], *keys: str) -> int | None:
    """First usable integer among `keys`, else None.

    Garbage is None rather than an error on purpose: `max_tokens` only feeds
    deadline sizing here, and refusing a body the provider would have
    accepted is a worse failure than sizing a clock off a default.
    """
    for key in keys:
        value = as_int(raw.get(key))
        if value is not None and value > 0:
            return value
    return None

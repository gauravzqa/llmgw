"""What every voice surface shares, and the two hooks the text surfaces
never needed.

A voice surface reads the same six things a chat surface does (see
`surfaces/base.py`), plus two more, because voice providers put the meter
where chat providers never do:

    what did it cost, from a HEADER?     -> usage_from_headers()
    what did it cost, from a whole BODY? -> usage_from_body()

ElevenLabs reports the only per-call meter it has -- `character-cost` -- in
the response headers, before any audio byte. Inworld's sync endpoint,
AssemblyAI's sync endpoint and OpenAI's non-streamed transcription put usage
in a JSON body that the buffered path never frames, so `apply_usage` never
sees it. Both hooks are total (never raise) and idempotent; the server calls
them at status commitment (headers) and after the buffered body is read
(body). A surface that has neither meter leaves `Usage` untouched and the
bill is `estimated`, which is the truth for OpenAI's binary TTS.

Three attributes are declared here for the route registry (PLAN-2 C/D):
`routes` (client path templates), `upstream_path` (the provider's path, may
reuse the template's params) and `forward_query` (whether the client's query
string travels upstream -- ElevenLabs carries `output_format` there). And
one for the model rewrite: `model_key`, because Inworld spells the model
`modelId` and ElevenLabs `model_id`; `upstream.apply_api_model` targets
`model` unless told otherwise. A dotted `model_key` names one level of
nesting (`transcribeConfig.modelId`, Inworld's HTTP STT).

`model_header` is the fifth, and the only one no text surface could ever
need: AssemblyAI's sync host routes on `X-AAI-Model` at the load balancer,
so its model is in a header and never in the body. A surface that sets it
gets its body forwarded untouched.

`include_usage_injectable = False`: the OpenAI-dialect `stream_options.
include_usage` injection is a chat-completions fact, and these surfaces ride
`kind="openai"` provider rows only for the credential ritual. The injection
site must consult this before editing a body.
"""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

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
    parse_json_object,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


Framing = Literal["sse", "jsonl", "raw"]


@dataclass(frozen=True, slots=True)
class VoiceRequestFacts(RequestFacts):
    """`RequestFacts` plus what a voice bill needs from the REQUEST side.

    `characters` is the text the caller asked to speak, counted the way the
    provider will count it (the string's length; providers apply their own
    normalisation on top and report the billed count back, which then
    overrides this). It is the only number available when the provider has
    no meter at all (OpenAI's binary TTS), and it is what makes that bill an
    honest `estimated` rather than a zero.

    `framing` is the framing THIS request wants when a surface's framing is
    decided per request (OpenAI TTS: `stream_format: "sse"` or binary); None
    means the surface's own.
    """

    characters: int = 0
    framing: str | None = None


def read_text(raw: Mapping[str, Any], *keys: str) -> str:
    """The text a TTS request asks to speak, under whichever key the dialect
    uses (`input` for OpenAI, `text` for Inworld and ElevenLabs)."""
    for key in keys:
        value = raw.get(key)
        if isinstance(value, str):
            return value
    return ""


def require_key(raw: Mapping[str, Any], *keys: str) -> str:
    """The first usable string among `keys`, or `InvalidRequest` naming them.
    The routing key is the one field a voice surface refuses to forward
    without, exactly as `require_model` does for chat."""
    for key in keys:
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return value
    raise errors.InvalidRequest(f"request body has no usable {' / '.join(keys)}")


def ceil_seconds(value: Any) -> float:
    """A duration in seconds, rounded UP to whole seconds the way the
    duration-billed providers meter it (OpenAI: `usage.seconds` is already
    an integer; `whisper-1`'s `duration` is a float the bill rounds up)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    if value < 0 or value != value:  # negative or NaN
        return 0.0
    return float(math.ceil(value))


def payload_of(ev: SSEEvent) -> dict[str, Any] | None:
    """`event_payload` for SSE and JSONL frames alike (both carry JSON in
    `data`); None for raw audio frames, comments, `[DONE]`."""
    return event_payload(ev)


class VoiceSurface:
    """Base class: the attributes every voice surface declares and the
    defaults that mean "nothing to read here". Stateless, one instance per
    route, shared by every request."""

    name: str = "voice"
    path: str = "/"
    routes: tuple[str, ...] = ()
    upstream_path: str = "/"
    forward_query: bool = False
    framing: Framing = "raw"
    body: Literal["json", "multipart", "raw"] = "json"
    default_profile: str | None = None
    model_key: str | None = "model"
    model_header: str | None = None
    include_usage_injectable: bool = False

    # ------------------------------------------------------------ protocol

    def framer(self, max_frame_bytes: int) -> Framer:
        return framer_for(self.framing, max_frame_bytes=max_frame_bytes)

    def parse_request(self, body: bytes) -> RequestFacts:  # pragma: no cover - abstract
        raise NotImplementedError

    def classify(self, ev: SSEEvent) -> EventKind:  # pragma: no cover - abstract
        raise NotImplementedError

    def text_delta(self, ev: SSEEvent) -> str | None:
        """Audio has no transcript. Transcription surfaces override."""
        return None

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """Nothing per frame by default; the meter is in a header or a body."""
        return None

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        return None

    def native_ending(self, last_event: SSEEvent | None = None) -> bytes:
        """Close. No voice provider has a terminal marker to withhold except
        OpenAI's SSE mode, whose `[DONE]` is withheld by the same rule as
        chat's: never fabricated, never sent on a broken stream (C2, C20)."""
        return b""

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None

    # -------------------------------------------------------- voice hooks

    def usage_from_headers(self, headers: Mapping[str, str], usage: Usage) -> None:
        """Fold a header-borne meter into `usage`. Default: none."""
        return None

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """Fold a body-borne meter (a buffered JSON response) into `usage`.
        Default: none."""
        return None

    def usage_estimate(self, facts: RequestFacts) -> Usage:
        """The best bill available BEFORE any provider report: request-side
        characters for TTS. Never exact. The server may use it as the floor
        for a stream the provider never metered."""
        usage = Usage()
        chars = getattr(facts, "characters", 0)
        if isinstance(chars, int) and chars > 0:
            usage.characters = chars
        return usage

    # ----------------------------------------------------------- helpers

    @staticmethod
    def _sse_common(ev: SSEEvent) -> EventKind | None:
        """The frame kinds every SSE dialect shares; None means 'look at the
        payload'."""
        if ev.is_comment:
            return EventKind.HEARTBEAT
        if is_done_marker(ev):
            return EventKind.TERMINAL
        if is_blank(ev):
            return EventKind.HEARTBEAT
        return None

    @staticmethod
    def _json_error(payload: Mapping[str, Any]) -> errors.GatewayError | None:
        """An in-band error object -> the taxonomy, or None."""
        block = payload.get("error")
        if not isinstance(block, dict):
            return None
        etype = str(block.get("type") or "").lower()
        code = str(block.get("code") or "").lower()
        message = str(block.get("message") or "upstream error inside a 200 body")
        data = json.dumps(payload).encode()
        if "overloaded" in etype or "overloaded" in code:
            return errors.UpstreamOverloaded(message, upstream_body=data)
        return errors.InStreamError(message, upstream_body=data)


__all__ = [
    "Framing",
    "VoiceRequestFacts",
    "VoiceSurface",
    "as_int",
    "ceil_seconds",
    "parse_json_object",
    "payload_of",
    "read_text",
    "require_key",
]

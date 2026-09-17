"""Inworld text-to-speech: `POST /tts/v1/voice` and `/tts/v1/voice:stream`.

Everything here was measured on 16 Sep 2026 (capabilities/voice-inworld.md,
"Observed wire shapes"):

* The stream is `application/json`, chunked, one JSON object per `\\n`, no
  blank lines, no terminator; it ends when the connection closes. Each line
  is `{"result": {"audioContent": "<base64>", "usage": {...}[, "timestampInfo":
  {...}]}}`. A steady-state LINEAR16 line is exactly one second of 24 kHz
  audio (48,044 decoded bytes, 64 KB on the wire); the first chunk starts
  with a 44-byte RIFF header. Fed to the SSE parser this produced zero
  events and a `FrameTooLarge` at line 19 -- hence the JSONL framer.
* `result.usage.processedCharactersCount` carries the FULL count on the
  first line and `0` on every line after. So the bill is exact from the
  first frame, even for a stream cut halfway.
* Empty or whitespace-only text is a 200 with one line
  `{"audioContent": "", "usage": null}`, not a 400. The surface records zero
  characters, exactly, and the client gets an empty result; it is the
  caller's fault and it costs nothing.
* The model key is `modelId`, not `model`. `model_key` says so for the
  rewrite; `parse_request` reads it (and `model_id`, which the API also
  accepts).

The sync endpoint returns one JSON object with `audioContent` and the same
`usage`; the buffered path reads it through `usage_from_body`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage, as_int, parse_json_object
from llmgw.surfaces.voice._base import (
    VoiceRequestFacts,
    VoiceSurface,
    payload_of,
    read_text,
    require_key,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


_ROUTES = ("/inworld/tts/v1/voice", "/inworld/tts/v1/voice:stream")
_CLIENT_PREFIX = "/inworld"


class InworldTTSSurface(VoiceSurface):
    """One surface, two routes. Both are framed as JSONL: the `:stream` body
    is many lines, the `/voice` body is one JSON object with no trailing
    newline -- which the JSONL framer flushes at end-of-stream as a single
    frame, so `apply_usage` sees `usage` either way and the sync response is
    forwarded byte-for-byte through the pump like any stream of one frame.
    `stream=True` for both, for that reason."""

    name = "inworld_tts"
    path = "/tts/v1/voice:stream"
    upstream_path = "/tts/v1/voice:stream"
    routes = _ROUTES
    forward_query = False
    framing = "jsonl"
    body = "json"
    default_profile = "tts"
    model_key = "modelId"
    include_usage_injectable = False

    @staticmethod
    def upstream_path_for(route: str) -> str:
        """`/inworld/tts/v1/voice[:stream]` -> `/tts/v1/voice[:stream]`."""
        if route not in _ROUTES:
            raise ValueError(f"inworld_tts does not serve {route!r}")
        return route[len(_CLIENT_PREFIX):]

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        raw = parse_json_object(body)
        model = require_key(raw, "modelId", "model_id")
        text = read_text(raw, "text")
        return VoiceRequestFacts(
            model=model, stream=True, include_usage=True,
            characters=len(text), framing=self.framing,
        )

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        payload = payload_of(ev)
        if payload is None:
            # A JSONL line that is not a JSON object: forwarded, never
            # counted as progress.
            return EventKind.META
        if isinstance(payload.get("error"), dict):
            return EventKind.ERROR
        result = payload.get("result")
        if isinstance(result, dict):
            audio = result.get("audioContent")
            if isinstance(audio, str) and audio:
                return EventKind.CONTENT
            # Timestamp-only lines (`timestampTransportStrategy: ASYNC`), the
            # empty-text line, a status-only line: bookkeeping.
            return EventKind.META
        # The sync shape can appear line-wise too (`audioContent` top-level).
        audio = payload.get("audioContent")
        if isinstance(audio, str) and audio:
            return EventKind.CONTENT
        return EventKind.META

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """`processedCharactersCount`: full on line one, zero after. `max`
        keeps the first line's number whatever order the pump sees them in;
        a `null` usage (the empty-text line) is an exact zero."""
        try:
            payload = payload_of(ev)
            if payload is None:
                return
            self._fold(payload, usage)
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        try:
            self._fold(payload, usage)
        except Exception:  # noqa: BLE001
            usage.parse_failures += 1

    @staticmethod
    def _fold(payload: dict[str, Any], usage: Usage) -> None:
        result = payload.get("result")
        container = result if isinstance(result, dict) else payload
        if "usage" not in container:
            return
        block = container.get("usage")
        if block is None:
            # Inworld's answer to empty text: nothing processed, and it said
            # so. Exact zero, not an estimate.
            usage.input_exact = True
            usage.output_exact = True
            return
        if not isinstance(block, dict):
            return
        count = as_int(block.get("processedCharactersCount"))
        if count is None:
            return
        usage.characters = max(usage.characters, max(count, 0))
        usage.input_exact = True
        usage.output_exact = True

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        payload = payload_of(ev)
        if payload is None:
            return None
        block = payload.get("error")
        if not isinstance(block, dict):
            return None
        code = block.get("code")
        message = str(block.get("message") or "inworld error inside a 200 stream")
        # gRPC status 8 is RESOURCE_EXHAUSTED: the in-band spelling of a
        # rate limit or overload.
        if code == 8:
            return errors.UpstreamOverloaded(message, upstream_body=ev.data)
        return errors.InStreamError(message, upstream_body=ev.data)

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None


__all__ = ["InworldTTSSurface"]

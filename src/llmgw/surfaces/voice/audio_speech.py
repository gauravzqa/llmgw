"""OpenAI text-to-speech: `POST /v1/audio/speech`.

Two framings from one endpoint, chosen by the REQUEST. The default body is
chunked binary audio (`audio/pcm`, `audio/mpeg`, ...) with no meter anywhere
-- not in the body, not in the headers (verified live 2026-09-16). With
`stream_format: "sse"` the same endpoint answers CRLF-terminated SSE:
`speech.audio.delta` frames carrying base64 audio, one `speech.audio.done`
carrying `usage {input_tokens, output_tokens, total_tokens}`, then
`data: [DONE]`. Only the SSE form can be billed exactly; the binary form is
`estimated` from the request's characters, and that is the truth of it.

The surface therefore has a `framing` of its own (the instance default, raw
because that is what the LiveKit plugin and every SDK send) and a
`framing_for()` that reads the body's `stream_format`; the server registers
one instance per framing or asks `framing_for()` per request (PLAN-2 D2).
`max_frame_bytes` is never a concern: measured SSE frames are 2.6 KB.
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


_DELTA = "speech.audio.delta"
_DONE = "speech.audio.done"


class AudioSpeechSurface(VoiceSurface):
    """`/v1/audio/speech`, binary by default, SSE on request."""

    name = "audio_speech"
    path = "/v1/audio/speech"
    routes = ("/v1/audio/speech",)
    upstream_path = "/v1/audio/speech"
    forward_query = False
    body = "json"
    default_profile = "tts"
    model_key = "model"
    include_usage_injectable = False

    def __init__(self, framing: str = "raw") -> None:
        if framing not in ("raw", "sse"):
            raise ValueError(f"audio_speech framing must be 'raw' or 'sse', got {framing!r}")
        self.framing = framing  # type: ignore[assignment]

    # ------------------------------------------------------------- request

    @staticmethod
    def framing_for(body: bytes) -> str:
        """`"sse"` when the body asks for `stream_format: "sse"`, else `"raw"`.
        Total: an unreadable body is the default framing, and the provider
        will reject the body itself."""
        try:
            raw = parse_json_object(body)
        except errors.GatewayError:
            return "raw"
        return "sse" if raw.get("stream_format") == "sse" else "raw"

    def surface_for(self, body: bytes):
        """The instance that reads THIS request's framing, or None for self.

        The registry carries the binary instance (what SDKs and the LiveKit
        plugin send). A body asking for `stream_format: "sse"` is served by
        the SSE instance instead -- same route, the other framer -- because
        classification, usage and the native ending all differ per framing
        and a surface instance is stateless and shared across requests.
        """
        wanted = self.framing_for(body)
        if wanted == self.framing:
            return None
        return _instance_for(wanted)

    def parse_request(self, body: bytes) -> RequestFacts:
        raw = parse_json_object(body)
        model = require_key(raw, "model")
        text = read_text(raw, "input")
        framing = "sse" if raw.get("stream_format") == "sse" else "raw"
        # The response is always a stream of audio, whichever framing: even
        # the "non-streaming" call is chunked transfer that the pump should
        # forward as it arrives rather than hold until EOF.
        return VoiceRequestFacts(
            model=model, stream=True, max_tokens=None,
            include_usage=(framing == "sse"),
            characters=len(text), framing=framing,
        )

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        if self.framing == "raw":
            # Every chunk of audio is the model working. There is nothing
            # else on a binary stream: no heartbeat, no marker, no usage.
            return EventKind.CONTENT
        common = self._sse_common(ev)
        if common is not None:
            return common
        payload = payload_of(ev)
        if payload is None:
            return EventKind.META
        if isinstance(payload.get("error"), dict):
            return EventKind.ERROR
        kind = ev.event or payload.get("type")
        if kind == _DELTA:
            return EventKind.CONTENT
        if kind == _DONE or isinstance(payload.get("usage"), dict):
            return EventKind.META
        return EventKind.META

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """`speech.audio.done.usage` is the whole bill, in tokens: text in at
        the input rate, audio out at the audio-output rate. Both halves are
        final in the one frame. Never raises."""
        if self.framing == "raw":
            return
        try:
            payload = payload_of(ev)
            if payload is None:
                return
            block = payload.get("usage")
            if not isinstance(block, dict):
                return
            tokens_in = as_int(block.get("input_tokens"))
            tokens_out = as_int(block.get("output_tokens"))
            if tokens_in is None and tokens_out is None:
                return
            if tokens_in is not None:
                usage.input_tokens = max(tokens_in, 0)
            if tokens_out is not None:
                usage.output_tokens = max(tokens_out, 0)
                # Every output token of a speech model is audio; the row's
                # `audio_output_per_m` prices it and `output_per_m` mirrors
                # it, so either column bills the same.
                usage.audio_output_tokens = max(tokens_out, 0)
            usage.input_exact = True
            usage.output_exact = True
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        if self.framing == "raw":
            return None
        payload = payload_of(ev)
        if payload is None:
            return None
        return self._json_error(payload)

    def native_ending(self, last_event: SSEEvent | None = None) -> bytes:
        return b""

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None

    def usage_estimate(self, facts: RequestFacts) -> Usage:
        """The bill for a binary stream OpenAI never metered (C21).

        `gpt-4o-mini-tts` is priced in tokens: text in at the input rate,
        audio out at the audio rate. Calibrated on the 16 Sep 2026 live probe
        (`capabilities/voice-openai.md`): a 20-character request reported
        `input_tokens=6, output_tokens=72`, i.e. about one text token per four
        characters plus one, and about 3.6 audio tokens per character. Never
        exact; accounting bills it `estimated`.
        """
        usage = Usage()
        chars = getattr(facts, "characters", 0)
        if not isinstance(chars, int) or chars <= 0:
            return usage
        usage.characters = chars
        usage.input_tokens = chars // 4 + 1
        audio = round(chars * 3.6)
        usage.output_tokens = audio
        usage.audio_output_tokens = audio
        return usage


__all__ = ["AudioSpeechSurface"]


_INSTANCES: dict[str, AudioSpeechSurface] = {}


def _instance_for(framing: str) -> AudioSpeechSurface:
    """One shared instance per framing (stateless, so sharing is safe)."""
    inst = _INSTANCES.get(framing)
    if inst is None:
        inst = _INSTANCES[framing] = AudioSpeechSurface(framing=framing)
    return inst

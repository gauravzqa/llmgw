"""Inworld speech-to-text over HTTP: `POST /stt/v1/transcribe`.

Yesterday's sweep recorded this endpoint as existing but unusable: a JSON
`{"audio_data": "<base64>"}` answered `proto: syntax error (line 1:15)` and a
multipart upload answered `invalid character '-' in numeric literal`. Both
readings were right and both conclusions were wrong. The endpoint is
protojson over a gRPC service:

* it parses ONLY JSON (hence the `-` of a multipart boundary read as a
  numeric literal), and
* it DISCARDS unknown fields, so a wrong field name produces not "unknown
  field" but the validation error for the field that is now missing.

Walking those validation errors gives the shape (live, 19 Sep 2026; probe
labels are the scratchpad script's):

    POST https://api.inworld.ai/stt/v1/transcribe
    authorization: Basic <key>          # the same credential as TTS
    content-type: application/json
    {"transcribeConfig": {"modelId": "inworld/inworld-stt-1",
                          "audioEncoding": "LINEAR16",
                          "sampleRateHertz": 16000,
                          "numberOfChannels": 1,
                          "language": "en-US"},
     "audioData": {"content": "<base64 of a WAV/MP3/OGG/FLAC/M4A/WebM file>"}}

    200 {"transcription": {"transcript": ..., "isFinal": true,
                           "wordTimestamps": [], "voiceProfile": null,
                           "silenceDurationMs": 0},
         "usage": {"transcribedAudioMs": 1840,
                   "modelId": "inworld/inworld-stt-1"}}

`audio_data` really was the right field name -- it is a MESSAGE, not bytes,
which is why a string value was a syntax error at exactly the column the
value starts on. The error ladder, in order, each from a real 400:

    {}                        -> "audio_data is required"
    {"audioData": {...}}      -> "invalid transcribe config: transcribe_config is required"
    config without modelId    -> "invalid transcribe config: model_id is required"
    config without encoding   -> "invalid transcribe config: audio_encoding is required
                                  and must not be AUDIO_ENCODING_UNSPECIFIED"
    audioData without content -> "audio data is required"
    raw PCM in `content`      -> "unsupported audio format - only WAV, MP3, OGG,
                                  FLAC, M4A, and WebM are supported"

--------------------------------------------------------------------------
The two facts that shape the surface
--------------------------------------------------------------------------

**The model is nested.** `transcribeConfig.modelId` is the only place this
API reads it; a top-level `modelId` is discarded as an unknown field and the
request then fails on the missing config. `model_key` is therefore the
dotted `transcribeConfig.modelId`, and `upstream.apply_api_model` follows
one level of nesting for it. Without the rewrite a caller naming the catalog
id gets `Unsupported model "inworld.stt-1"` with a link to the model list.

**The meter is exact and terminal.** `usage.transcribedAudioMs` is whole
milliseconds of audio the service transcribed -- 1840 for a file both
AssemblyAI (`audio_duration_ms: 1840`) and ElevenLabs
(`audio_duration_secs: 1.84`) measured identically. Note it is transcribed
audio, not streamed audio: the WebSocket form of this product reported 3150
ms for 7.6 s of stream (captures-ws.md probe 6b). On this unary route the
two cannot differ, because the whole file is the request.

The response is one JSON object, so `framing = "raw"` and `stream = False`:
the executor buffers it and `usage_from_body` reads the meter before the
status is committed. The request body is JSON and carries the base64 audio,
so the per-surface body cap is the thing to watch, not the framing.

A bad credential here is a **403** `{"code":7,"message":"Invalid
authorization credentials"}` and a missing one a **401** `{"code":16, ...}`;
both already classify as `AuthenticationFailed` under the `inworld` row's
default `forbidden_means="auth"`, and the row's `scrub_error_bodies="all"`
still applies -- the key is reversible base64.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage, as_int, parse_json_object
from llmgw.surfaces.voice._base import VoiceSurface, payload_of

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


_CLIENT_PREFIX = "/inworld"
_UPSTREAM_PATH = "/stt/v1/transcribe"
_ROUTES = (_CLIENT_PREFIX + _UPSTREAM_PATH,)

MODEL_KEY = "transcribeConfig.modelId"
"""Dotted: the model is nested one level, and nowhere else."""


class InworldSTTSurface(VoiceSurface):
    """`POST /inworld/stt/v1/transcribe`: JSON in (base64 audio inside),
    one JSON object out, `usage.transcribedAudioMs` the bill."""

    name = "inworld_stt"
    path = _UPSTREAM_PATH
    upstream_path = _UPSTREAM_PATH
    routes = _ROUTES
    forward_query = False
    framing = "raw"
    body = "json"
    default_profile = None
    model_key = MODEL_KEY
    include_usage_injectable = False

    @staticmethod
    def upstream_path_for(route: str) -> str:
        """`/inworld/stt/v1/transcribe` -> `/stt/v1/transcribe`."""
        if route not in _ROUTES:
            raise ValueError(f"inworld_stt does not serve {route!r}")
        return route[len(_CLIENT_PREFIX):]

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        """The model, from `transcribeConfig.modelId` (or its snake_case
        spelling, which protojson also accepts). Raises only
        `errors.InvalidRequest`."""
        raw = parse_json_object(body)
        model = ""
        for key in ("transcribeConfig", "transcribe_config"):
            config = raw.get(key)
            if not isinstance(config, dict):
                continue
            for spelling in ("modelId", "model_id"):
                value = config.get(spelling)
                if isinstance(value, str) and value.strip():
                    model = value
                    break
            if model:
                break
        if not model:
            raise errors.InvalidRequest(
                "request body has no usable transcribeConfig.modelId"
            )
        return RequestFacts(model=model, stream=False, include_usage=True)

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        # There is no stream; a framed body would be bookkeeping.
        return EventKind.META

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """`usage.transcribedAudioMs` -> seconds, exact."""
        try:
            block = payload.get("usage")
            if not isinstance(block, dict):
                return
            ms = as_int(block.get("transcribedAudioMs"))
            if ms is None:
                return
            usage.seconds = max(ms, 0) / 1000.0
            usage.input_exact = True
            usage.output_exact = True
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        payload = payload_of(ev)
        if payload is None:
            return None
        code = payload.get("code")
        message = str(payload.get("message") or "")
        if isinstance(code, int) and message:
            # gRPC status 8 is RESOURCE_EXHAUSTED, the in-band spelling of a
            # rate limit, exactly as on the TTS route.
            if code == 8:
                return errors.UpstreamOverloaded(message, upstream_body=ev.data)
            return errors.InStreamError(message, upstream_body=ev.data)
        return self._json_error(payload)

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None


__all__ = ["MODEL_KEY", "InworldSTTSurface"]

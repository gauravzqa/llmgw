"""OpenAI speech-to-text: `POST /v1/audio/transcriptions` and `/translations`.

Multipart in (the audio file plus text fields), and one of two shapes out.
With `stream=true` (a form field, and only the `gpt-*transcribe` models
honour it; `whisper-1` does not) the response is CRLF SSE:
`transcript.text.delta {delta[, logprobs]}` per token, then
`transcript.text.done {text, usage, languages}`, then `data: [DONE]`.
Otherwise it is one JSON object.

Usage comes in two currencies, and the model row decides which the bill
uses: `{type: "tokens", input_tokens, output_tokens, input_token_details}`
on the token-priced models, `{type: "duration", seconds}` on the per-minute
models -- ROUNDED UP to whole seconds before the provider reports it
(verified live 2026-09-16: a 2.44 s clip billed as 3). `whisper-1`'s
`verbose_json` has no `usage` at all, only a float `duration`; the surface
rounds that up itself so the two paths bill alike.

The request is multipart, so `parse_request` here is a bounded scan of the
leading text fields for `model` and `stream` -- the server's own multipart
scan (PLAN-2 B4) does the same and is what runs in production; this copy
exists so the surface is testable on its own and so the file part is never
decoded by anyone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage, as_int
from llmgw.surfaces.voice._base import VoiceSurface, ceil_seconds, payload_of

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


_DELTA = "transcript.text.delta"
_DONE = "transcript.text.done"
_SCAN_BYTES = 64 * 1024

_ROUTES = ("/v1/audio/transcriptions", "/v1/audio/translations")


def scan_form_fields(body: bytes, content_type: str | None,
                     wanted: frozenset[str]) -> dict[str, str]:
    """Text form fields among the first 64 KiB of a multipart body.

    File parts are skipped by their `filename=`; payloads over 4 KiB are
    ignored (a text field is small; anything else is data we must not
    decode). Text fields after the file part are not found -- the same
    documented limitation as the server's scan, stated where callers read.
    """
    if not content_type:
        return {}
    boundary = None
    for piece in content_type.split(";"):
        piece = piece.strip()
        if piece.lower().startswith("boundary="):
            boundary = piece[len("boundary="):].strip('"')
    if not boundary:
        return {}
    delim = b"--" + boundary.encode("latin-1")
    found: dict[str, str] = {}
    for part in body[:_SCAN_BYTES].split(delim)[1:]:
        if part.startswith(b"--"):
            break
        head, sep, payload = part.partition(b"\r\n\r\n")
        if not sep:
            continue
        low = head.lower()
        if b"filename=" in low:
            continue
        name = None
        for line in head.split(b"\r\n"):
            l_ = line.lower()
            if l_.startswith(b"content-disposition:") and b"name=" in l_:
                after = line[l_.index(b"name=") + 5:]
                name = after.split(b";")[0].strip().strip(b'"').decode("latin-1")
        if name is None or name not in wanted:
            continue
        if payload.endswith(b"\r\n"):
            payload = payload[:-2]
        if len(payload) > 4096:
            continue
        found[name] = payload.decode("utf-8", "replace")
        if len(found) == len(wanted):
            break
    return found


class AudioTranscriptionSurface(VoiceSurface):
    """`/v1/audio/transcriptions` and `/v1/audio/translations`: one surface,
    two routes, the same dialect and meters."""

    name = "audio_transcription"
    routes = _ROUTES
    forward_query = False
    framing = "sse"
    body = "multipart"
    default_profile = None
    model_key = "model"
    include_usage_injectable = False

    path = "/v1/audio/transcriptions"
    upstream_path = "/v1/audio/transcriptions"

    @staticmethod
    def upstream_path_for(route: str) -> str:
        """Both routes go upstream unchanged; the registry asks per route."""
        if route not in _ROUTES:
            raise ValueError(f"audio_transcription does not serve {route!r}")
        return route

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes, content_type: str | None = None) -> RequestFacts:
        """Bounded scan of the multipart text fields. The server's scan is
        the production path; this one accepts a `content_type` so a test can
        exercise the surface without the server."""
        fields = scan_form_fields(body, content_type, frozenset({"model", "stream"}))
        model = fields.get("model", "").strip()
        if not model:
            raise errors.InvalidRequest(
                f"no `model` form field in the first {_SCAN_BYTES} bytes of the "
                f"multipart body; put the text fields before the file part"
            )
        stream = fields.get("stream", "").strip().lower() in {"1", "true", "yes"}
        return RequestFacts(model=model, stream=stream, include_usage=True)

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
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
        return EventKind.META

    def text_delta(self, ev: SSEEvent) -> str | None:
        payload = payload_of(ev)
        if payload is None:
            return None
        if (ev.event or payload.get("type")) != _DELTA:
            return None
        delta = payload.get("delta")
        return delta if isinstance(delta, str) and delta else None

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """`transcript.text.done.usage`, in whichever currency it arrives."""
        try:
            payload = payload_of(ev)
            if payload is None:
                return
            block = payload.get("usage")
            if isinstance(block, dict):
                self._fold_usage(block, usage)
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """The buffered JSON response: `usage` when the model reports one,
        else `whisper-1`'s `duration` (verbose_json) rounded up."""
        try:
            block = payload.get("usage")
            if isinstance(block, dict):
                self._fold_usage(block, usage)
                return
            duration = payload.get("duration")
            if isinstance(duration, (int, float)) and not isinstance(duration, bool):
                usage.seconds = ceil_seconds(duration)
                usage.input_exact = True
                usage.output_exact = True
        except Exception:  # noqa: BLE001
            usage.parse_failures += 1

    @staticmethod
    def _fold_usage(block: dict[str, Any], usage: Usage) -> None:
        kind = block.get("type")
        if kind == "duration":
            seconds = block.get("seconds")
            if isinstance(seconds, (int, float)) and not isinstance(seconds, bool):
                usage.seconds = ceil_seconds(seconds)
                usage.input_exact = True
                usage.output_exact = True
            return
        tokens_in = as_int(block.get("input_tokens"))
        tokens_out = as_int(block.get("output_tokens"))
        if tokens_in is None and tokens_out is None:
            return
        details = block.get("input_token_details")
        audio_in = as_int(details.get("audio_tokens")) if isinstance(details, dict) else None
        if tokens_in is not None:
            usage.input_tokens = max(tokens_in, 0)
            if audio_in is not None:
                usage.audio_input_tokens = max(audio_in, 0)
        if tokens_out is not None:
            usage.output_tokens = max(tokens_out, 0)
        usage.input_exact = True
        usage.output_exact = True

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        payload = payload_of(ev)
        if payload is None:
            return None
        return self._json_error(payload)


__all__ = ["AudioTranscriptionSurface", "scan_form_fields"]

"""ElevenLabs Scribe speech-to-text: `POST /v1/speech-to-text`.

Established live on 19 Sep 2026 against `api.elevenlabs.io` (probes in the
session scratchpad; every quoted body scrubbed of credentials):

* **multipart/form-data**, file part named `file`, model in the `model_id`
  TEXT field -- so the catalog id is rewritten by
  `upstream.apply_api_model_multipart` exactly as on OpenAI's transcription
  route, and `model_key = "model_id"` says where. A raw audio body is a 422
  `{"detail":[{"type":"missing","loc":["body","model_id"], ...}]}`: the
  endpoint reads only form fields, so there is no non-multipart form of it.
* One buffered JSON object out, `framing = "raw"`, keys
  `{language_code, language_probability, text, words[], audio_duration_secs,
  transcription_id}`.
* **`audio_duration_secs` is the meter, and it is exact**: 1.84 for a file
  AssemblyAI independently measured at 1840 ms. Unlike OpenAI's duration
  usage it is NOT rounded up, so nothing here rounds it. It is read by
  `usage_from_body` on the buffered path.
* The response also carries `character-cost: 2` and
  `fiat-cost-before-overages: 0.0002` headers -- ElevenLabs' own credit
  meter, the same `character-cost` the TTS surface bills on. It is NOT read
  here: this row is priced per hour of audio, `audio_duration_secs` is the
  unit that price applies to, and folding a character count into a
  seconds-priced request would put two currencies on one bill.
* Available `model_id`s, from the provider's own 400: `scribe_v1`,
  `scribe_v1_experimental`, `scribe_v2`, `scribe_v2_medical`. A catalog id
  sent verbatim gets `{"detail":{"type":"validation_error","code":
  "unsupported_model", ...}}`, which is why the rewrite is load-bearing.
* Errors are the ElevenLabs `{"detail": {...}}` shape `errors._error_hints`
  already reads, or the FastAPI `{"detail": [...]}` list on a 422. A bad key
  is a plain **401** here (`detail.code: "unauthorized"`), not the 400 the
  TTS host answers with, so no new rule is needed for it.

`stream` has no meaning on this route: there is one response object and the
surface always reports `stream=False`, which is what puts the executor on
the buffered path where `usage_from_body` runs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage
from llmgw.surfaces.voice._base import VoiceSurface, payload_of

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


_CLIENT_PREFIX = "/elevenlabs"
_UPSTREAM_PATH = "/v1/speech-to-text"
_ROUTES = (_CLIENT_PREFIX + _UPSTREAM_PATH,)

DURATION_KEY = "audio_duration_secs"


class ElevenLabsSTTSurface(VoiceSurface):
    """One route, one buffered JSON answer, one meter."""

    name = "elevenlabs_stt"
    path = _UPSTREAM_PATH
    upstream_path = _UPSTREAM_PATH
    routes = _ROUTES
    forward_query = False
    framing = "raw"
    body = "multipart"
    default_profile = None
    model_key = "model_id"
    include_usage_injectable = False

    @staticmethod
    def upstream_path_for(route: str) -> str:
        """`/elevenlabs/v1/speech-to-text` -> `/v1/speech-to-text`."""
        if route not in _ROUTES:
            raise ValueError(f"elevenlabs_stt does not serve {route!r}")
        return route[len(_CLIENT_PREFIX):]

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        """Never the production path: the body is multipart and the server's
        own bounded scan (`facts_for_body`) reads `model_id` from it."""
        raise errors.InvalidRequest(
            "elevenlabs_stt takes a multipart body; put the `model_id` text field "
            "before the `file` part"
        )

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        # There is no stream. A framed body would be bookkeeping.
        return EventKind.META

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """`audio_duration_secs` -> seconds, exact, unrounded."""
        try:
            seconds = payload.get(DURATION_KEY)
            if isinstance(seconds, bool) or not isinstance(seconds, (int, float)):
                return
            if seconds < 0 or seconds != seconds:  # negative or NaN
                return
            usage.seconds = float(seconds)
            usage.input_exact = True
            usage.output_exact = True
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        payload = payload_of(ev)
        if payload is None:
            return None
        detail = payload.get("detail")
        if isinstance(detail, dict):
            message = str(detail.get("message") or "elevenlabs error inside a 200 body")
            return errors.InStreamError(message, upstream_body=ev.data)
        return self._json_error(payload)

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None


__all__ = ["DURATION_KEY", "ElevenLabsSTTSurface"]

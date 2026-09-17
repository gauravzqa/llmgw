"""AssemblyAI Sync speech-to-text: `POST /transcribe` on `sync.assemblyai.com`.

The one AssemblyAI product that fits an HTTP gateway (capabilities/
voice-assemblyai.md §1): raw PCM in (16 kHz mono s16le, up to 120 s / 40 MB),
one JSON object out (`text`, `audio_duration_ms`, `request_time_ms`,
`session_id`), about 134 ms p50. No streaming, no framing question, one
meter -- `audio_duration_ms` -- read from the buffered body.

The request body is audio, not JSON, so the routing key cannot come from
it: `body = "raw"` and the server reads `?model=` or `X-Gw-Model`. The
endpoint itself takes no model parameter; the catalog row `assemblyai.sync`
exists so the request has something to price against.

Auth is the bare key in `Authorization` (`auth_scheme="raw"` on the
provider row); the REST host's 403 is a rate limit, which the row also
declares. Its reference page was unreachable during the sweep, so the
error-body shape is unverified; classification is by status.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage
from llmgw.surfaces.voice._base import VoiceSurface

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


class AssemblyAISyncSurface(VoiceSurface):
    name = "assemblyai_sync"
    path = "/transcribe"
    routes = ("/assemblyai/transcribe",)
    upstream_path = "/transcribe"
    forward_query = False
    framing = "raw"
    body = "raw"
    default_profile = None
    model_key = "model"
    include_usage_injectable = False

    def parse_request(self, body: bytes) -> RequestFacts:
        """Never the production path: the body is audio. The server reads the
        model from the query string or `X-Gw-Model` (PLAN-2 B4)."""
        raise errors.InvalidRequest(
            "assemblyai_sync takes a raw PCM body; name the model with ?model=<id> "
            "or the X-Gw-Model header"
        )

    def classify(self, ev: SSEEvent) -> EventKind:
        # There is no stream; if the buffered response is ever framed, it is
        # bookkeeping.
        return EventKind.META

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """`audio_duration_ms` -> seconds. The provider bills per second of
        audio with no rounding documented, so none is applied."""
        try:
            ms = payload.get("audio_duration_ms")
            if isinstance(ms, bool) or not isinstance(ms, (int, float)) or ms < 0:
                return
            usage.seconds = float(ms) / 1000.0
            usage.input_exact = True
            usage.output_exact = True
        except Exception:  # noqa: BLE001
            usage.parse_failures += 1

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None


__all__ = ["AssemblyAISyncSurface"]

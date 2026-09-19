"""AssemblyAI Sync speech-to-text: `POST /v1/transcribe` on `sync.assemblyai.com`.

The one AssemblyAI product that fits an HTTP gateway: audio in, one JSON
object out (`text`, `words[]`, `confidence`, `audio_duration_ms`,
`session_id`, `request_time_ms`), about 450 ms of provider time for 1.8 s of
speech. No streaming, no framing question, one meter -- `audio_duration_ms`,
EXACT milliseconds -- read from the buffered body.

--------------------------------------------------------------------------
Why this surface 404'd on every call it ever made
--------------------------------------------------------------------------

Three faults, each sufficient on its own; all three found by live capture
(capabilities/captures-sarvam-assemblyai.md §1, probes A2a/A3a/A4a-h) and
reproduced on 19 Sep 2026:

1. `upstream_path = "/transcribe"` with `body = "raw"`. A raw body is a
   **415** `{"status":415,"title":"Unsupported Media Type","detail":"request
   must be multipart/form-data with an `audio` part and an optional `config`
   part"}`. The body must be `multipart/form-data` with a part named
   `audio`; raw PCM is fine, but only as that part, with an optional
   `config` part `{"sample_rate":16000,"channels":1}` beside it.
2. No `X-AAI-Model` header. That header is a **routing** header read by the
   AWS load balancer in front of the service: without it -- or with a model
   sync does not serve, which includes `universal-2` -- the ELB answers
   `404 Not found` as `text/plain` with `server: awselb/2.0` and the
   application never runs. This is the one the surface could never have
   recovered from, because no amount of body fixing reaches an app the load
   balancer refused to route to.
3. `/transcribe` is only an alias; `/v1/transcribe` is the documented path.
   Both answer 200 once (1) and (2) are right, and the documented one is
   what we send.

So: `body = "multipart"`, `upstream_path = "/v1/transcribe"`, and the model
is declared through `model_header` rather than `model_key` -- the gateway
writes the target's `api_model` into `X-AAI-Model` and forwards the client's
multipart bytes untouched (`upstream.build_headers`, and
`server.app.facts_for_body`, which then reads the catalog id from `?model=`
or `X-Gw-Model` because there is no model in the body to scan for).

--------------------------------------------------------------------------
Errors
--------------------------------------------------------------------------

Every application error on this host is RFC 7807: `application/problem+json`
with `{status, title, detail}` (all five bodies captured; the fixtures are in
tests/unit/test_real_error_bodies.py). The one that matters for
classification is that **a bad key is a 404, not a 401 or 403**:
`{"status":404,"title":"Not Found","detail":"Invalid API key"}`. The provider
row therefore says `forbidden_means="auth"` -- it never sends a 403 at all,
so the old `"rate_limit"` described a response that does not exist -- and
`errors.from_http_status` has a rule that reads that body, because a 404 on
this host is otherwise indistinguishable from the routing 404 above.

Auth is the bare key in `Authorization` (`auth_scheme="raw"`), verified on
all three AssemblyAI hosts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage
from llmgw.surfaces.voice._base import VoiceSurface

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


MODEL_HEADER = "X-AAI-Model"
"""The routing header. Not a preference: the load balancer 404s without it."""

_CLIENT_PREFIX = "/assemblyai"
_UPSTREAM_PATH = "/v1/transcribe"
_ROUTES = ("/assemblyai/v1/transcribe", "/assemblyai/transcribe")
"""Both client paths map to `/v1/transcribe` upstream. `/transcribe` is
AssemblyAI's own alias and was this surface's only route while it was broken;
keeping it costs nothing and does not strand a caller that already wrote it."""


class AssemblyAISyncSurface(VoiceSurface):
    name = "assemblyai_sync"
    path = _UPSTREAM_PATH
    routes = _ROUTES
    upstream_path = _UPSTREAM_PATH
    forward_query = False
    framing = "raw"
    body = "multipart"
    default_profile = None
    model_key = None
    model_header = MODEL_HEADER
    include_usage_injectable = False

    @staticmethod
    def upstream_path_for(route: str) -> str:
        if route not in _ROUTES:
            raise ValueError(f"assemblyai_sync does not serve {route!r}")
        return _UPSTREAM_PATH

    def parse_request(self, body: bytes) -> RequestFacts:
        """Never the production path: the body is a multipart upload whose
        parts are audio, and the model is not in it. The server reads the
        catalog id from `?model=` or `X-Gw-Model` (`facts_for_body`)."""
        raise errors.InvalidRequest(
            "assemblyai_sync takes a multipart body with an `audio` part; name the "
            "model with ?model=<id> or the X-Gw-Model header"
        )

    def classify(self, ev: SSEEvent) -> EventKind:
        # There is no stream; if the buffered response is ever framed, it is
        # bookkeeping.
        return EventKind.META

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """`audio_duration_ms` -> seconds. Exact milliseconds, verified: a
        1.84 s file reports 1840, with no rounding anywhere (the async
        product rounds to whole seconds; this one does not). No rounding is
        applied here either, because the provider stated the number."""
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


__all__ = ["MODEL_HEADER", "AssemblyAISyncSurface"]

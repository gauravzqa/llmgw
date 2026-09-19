"""ElevenLabs text-to-speech: `POST /v1/text-to-speech/{voice_id}`,
`.../stream`, `.../stream/with-timestamps`.

Facts from capabilities/voice-elevenlabs.md (documentation; no key was
available to verify live):

* `/stream` is chunked binary audio in the requested `output_format`
  (`audio/mpeg` by default), no framing, no terminal marker, ends on close.
  The buffered route returns the whole encoded file. Both are `raw`.
* `/stream/with-timestamps` is newline-delimited JSON, one object per line:
  `{"audio_base64": ..., "alignment": {...}, "normalized_alignment": ...}`.
  JSONL framing, `audio_base64` is the audio.
* The ONLY per-call meter is the `character-cost` response header. It
  arrives before the body, so the bill is exact even for a stream cut after
  the first byte, and it is read by `usage_from_headers`, never from frames.
* The voice is part of the PATH and the output format is in the QUERY
  (`?output_format=pcm_16000`), so `forward_query` is True and the routes
  are templates. The registry substitutes `{voice_id}`; for a test that
  needs a concrete path, construct the surface with `voice_id=`.
* The model key is `model_id`. Errors are `{"detail": {...}}`, read by
  `errors._error_hints`; 403s are plan/voice/model denials and the provider
  row's `forbidden_means="policy"` keeps them off the credential breaker.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

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


Variant = Literal["buffered", "stream", "timestamps"]

_SUFFIX: dict[str, str] = {
    "buffered": "", "stream": "/stream", "timestamps": "/stream/with-timestamps",
}
_CLIENT_PREFIX = "/elevenlabs"
_UPSTREAM_BASE = "/v1/text-to-speech/{voice_id}"

_ROUTES = tuple(_CLIENT_PREFIX + _UPSTREAM_BASE + suffix for suffix in _SUFFIX.values())

CHARACTER_COST_HEADER = "character-cost"


class ElevenLabsTTSSurface(VoiceSurface):
    """The registered ElevenLabs surface: the buffered route and `/stream`,
    both raw audio, one name. `upstream_path_for` strips the client prefix
    and keeps the `{voice_id}` template for the server to substitute.

    `/stream/with-timestamps` is a different framing (JSONL) and so a
    different instance -- `ElevenLabsTimestampsSurface` below -- which is
    NOT in the registry: the pump takes one framing per surface, and the
    registry takes one surface per name. It stays importable for a
    deployment that wants it under its own name.

    `voice_id=` pins the template to one voice (tests, or a single-voice
    deployment) so the instance has a concrete upstream path.
    """

    name = "elevenlabs_tts"
    routes = (_ROUTES[0], _ROUTES[1])
    """Class default. `__init__` narrows it to the ONE route this variant
    serves, because the variant decides whether the response is buffered or
    chunked and a single instance cannot be both. Registering one instance
    per route is the same shape `inworld_tts` uses for `/voice` and
    `:stream`, and the metric name is shared on purpose: two routes of one
    product are one line on a dashboard."""
    forward_query = True
    framing = "raw"
    body = "json"
    default_profile = "tts"
    model_key = "model_id"
    include_usage_injectable = False
    variant: Variant = "stream"

    def __init__(self, variant: Variant = "stream", voice_id: str | None = None) -> None:
        if variant not in _SUFFIX:
            raise ValueError(
                f"elevenlabs_tts variant must be one of {list(_SUFFIX)}, got {variant!r}"
            )
        self.variant = variant
        self._voice_id = voice_id
        # The route this instance serves, and only it. Serving both from a
        # `stream` instance made `parse_request` report `stream=True` for the
        # buffered route, so a caller that asked for a whole body got a
        # chunked one with no `content-length` -- correct audio, correct
        # bill, wrong shape, and the one thing the buffered route exists to
        # provide.
        self.routes = (_CLIENT_PREFIX + _UPSTREAM_BASE + _SUFFIX[variant],)
        # A name per variant, matching `sarvam_tts` / `sarvam_tts_stream`:
        # a buffered call and a streamed one are different operational
        # objects -- one has a time-to-first-byte and can be cut mid-body,
        # the other cannot -- and the name is the metric label.
        if variant == "stream":
            self.name = "elevenlabs_tts_stream"
        template = _UPSTREAM_BASE + _SUFFIX[variant]
        self.upstream_path = self._pin(template)
        self.path = self.upstream_path
        self.framing = "jsonl" if variant == "timestamps" else "raw"  # type: ignore[assignment]

    def _pin(self, template: str) -> str:
        if self._voice_id is None:
            return template
        return template.replace("{voice_id}", self._voice_id)

    def upstream_path_for(self, route: str) -> str:
        """`/elevenlabs/v1/text-to-speech/{voice_id}[/stream]` minus the client
        prefix, `{voice_id}` left for the server to fill (or pinned)."""
        if route not in _ROUTES:
            raise ValueError(f"elevenlabs_tts does not serve {route!r}")
        return self._pin(route[len(_CLIENT_PREFIX):])

    @property
    def client_route(self) -> str:
        return _CLIENT_PREFIX + self.upstream_path

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> RequestFacts:
        raw = parse_json_object(body)
        model = require_key(raw, "model_id", "model")
        text = read_text(raw, "text")
        return VoiceRequestFacts(
            model=model, stream=(self.variant != "buffered"), include_usage=True,
            characters=len(text), framing=self.framing,
        )

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        if self.framing == "raw":
            return EventKind.CONTENT
        payload = payload_of(ev)
        if payload is None:
            return EventKind.META
        if (isinstance(payload.get("detail"), (dict, list))
                or isinstance(payload.get("error"), dict)):
            return EventKind.ERROR
        audio = payload.get("audio_base64")
        if isinstance(audio, str) and audio:
            return EventKind.CONTENT
        return EventKind.META

    def usage_estimate(self, facts: RequestFacts) -> Usage:
        """The bill: characters of text we forwarded.

        ElevenLabs states a number in `character-cost`, but it is credits
        (see `usage_from_headers`), and the price list is dollars per
        character. So the billable quantity is the text itself -- known
        exactly, because we sent it -- and the basis stays `estimated`
        because the PROVIDER never reported it. Honest either way: the
        dollar figure is right, and the record does not claim ElevenLabs
        agreed to it.
        """
        usage = Usage()
        usage.characters = int(getattr(facts, "characters", 0) or 0)
        return usage

    def cost_notes(self, facts: Any, usage: Usage | None) -> tuple[str, ...]:
        """Say why the bill is characters rather than the number the provider
        put in a header, because the two differ by the model's credit
        multiplier and the difference is the whole invoice on flash models."""
        chars = int(getattr(facts, "characters", 0) or 0)
        credits = int(getattr(usage, "provider_credits", 0) or 0) if usage else 0
        note = (f"billed from the {chars} characters sent; ElevenLabs prices "
                f"the API in dollars per character and reports no character "
                f"count of its own")
        if credits:
            note += (f" (its `character-cost` header read {credits}, which is "
                     f"CREDITS -- the flash models spend half a credit per "
                     f"character)")
        return (note,)

    def usage_from_headers(self, headers: Mapping[str, str], usage: Usage) -> None:
        """Read `character-cost`, and do NOT bill it.

        It is a CREDIT count, not a character count. Flash v2.5 costs half a
        credit per character, so a 14-character request reports 7 -- measured
        twice, ratio 0.50 both times (19 Sep 2026). ElevenLabs bills API usage
        "in US dollars, not credits", at $0.05 per 1,000 CHARACTERS for
        Flash/Turbo (elevenlabs.io/pricing/api), which is the rate this row
        carries. So billing the header against that rate charges half of what
        the call costs, and nothing about the record looks wrong: it reads
        `exact`, because a provider did state a number -- just not the one the
        price is per.

        The characters therefore come from the request text, which we know
        exactly because we forwarded it, and the credit figure is kept beside
        them for reconciliation against an invoice. The multilingual models
        cost one credit per character, which is why this hid: there the two
        numbers are equal.
        """
        try:
            value = headers.get(CHARACTER_COST_HEADER)
            if value is None:
                for key, candidate in headers.items():
                    if str(key).lower() == CHARACTER_COST_HEADER:
                        value = candidate
                        break
            if value is None:
                return
            count = as_int(int(str(value).strip()))
            if count is None:
                return
            usage.provider_credits = max(count, 0)
        except (ValueError, TypeError):
            usage.parse_failures += 1
        except Exception:  # noqa: BLE001
            usage.parse_failures += 1

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        if self.framing == "raw":
            return None
        payload = payload_of(ev)
        if payload is None:
            return None
        detail = payload.get("detail")
        if isinstance(detail, dict):
            status = str(detail.get("status") or detail.get("code") or "").lower()
            message = str(detail.get("message") or "elevenlabs error inside a 200 stream")
            if "busy" in status or "concurrent" in status or "rate_limit" in status:
                return errors.UpstreamOverloaded(message, upstream_body=ev.data)
            return errors.InStreamError(message, upstream_body=ev.data)
        return self._json_error(payload)

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        return None


class ElevenLabsTimestampsSurface(ElevenLabsTTSSurface):
    """`/stream/with-timestamps`: the JSONL variant, under its own name so
    the registry can carry it if a deployment opts in (add the name to
    `metrics.SURFACES` first)."""

    name = "elevenlabs_tts_timestamps"
    routes = (_ROUTES[2],)

    def __init__(self, voice_id: str | None = None) -> None:
        super().__init__("timestamps", voice_id)


__all__ = ["CHARACTER_COST_HEADER", "ElevenLabsTTSSurface", "ElevenLabsTimestampsSurface"]

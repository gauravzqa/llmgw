"""Sarvam speech over HTTP: two TTS routes and two STT routes.

Facts from `capabilities/captures-sarvam-assemblyai.md` §2 (live, 18 Sep
2026) and a second live pass on 19 Sep 2026:

* `POST /text-to-speech` answers one JSON object, `{"request_id", "audios":
  [<base64 WAV>]}`. Buffered, not a stream.
* `POST /text-to-speech/stream` answers chunked `audio/pcm` with no framing
  and NO terminal frame -- the body simply ends. `raw`.
* `POST /speech-to-text` and `/speech-to-text-translate` take MULTIPART with
  the audio in a part named `file` and the model in a form field `model`;
  each answers one JSON object (`transcript`, `language_code`,
  `language_probability`, and on the translate route `diarized_transcript`).
* The model id is the top-level body key `model` on the TTS routes and the
  form field `model` on the STT routes -- the default `model_key`, so the
  rewrite needs nothing special beyond `apply_api_model_multipart` for the
  form case.

--------------------------------------------------------------------------
The meter, and why every Sarvam bill here is `estimated`
--------------------------------------------------------------------------

Sarvam reports **no usage on any of these four responses**. Not a character
count, not a duration, not a header, not a trailing field. The streaming
WebSocket STT product does report `metrics.audio_duration` exactly, so the
meter exists inside Sarvam -- it is simply absent from the HTTP products.

That leaves two different situations, and they deserve different honesty:

* **TTS** is billed per character and the characters are in the REQUEST.
  `len(text)` is what the caller sent and what Sarvam will count, but Sarvam
  applies its own normalisation before charging ("charged per character,
  rounded up") and never tells us the result, so this is a good estimate and
  not a fact. `Usage.input_exact` / `output_exact` stay False and the record
  says `basis=estimated`, with a `cost_notes` line naming the reason.
* **STT** is billed per audio SECOND and nothing on the response says how
  many. The gateway will not force `with_timestamps=true` to get one: that
  changes the request the caller made, adds fields to the response they did
  not ask for, and buys an exact-looking number by lying about the request.
  Instead the duration is read from the uploaded audio's own RIFF/WAVE
  header when there is one (`wav_seconds` below) and the record says, in
  `cost_notes`, that the number came from the client's container header
  rather than from Sarvam. For a compressed upload -- MP3, Opus, anything
  without a RIFF header -- there is no estimate at all and the record is an
  honest zero-with-a-note. A zero that says "we could not meter this" is
  worth more than a plausible number nobody can defend.

`wav_seconds` is deliberately paranoid: it parses attacker-supplied bytes on
the request path, so it is a bounded walk over chunk headers that cannot
raise, cannot loop unboundedly, and cannot believe a declared data size
larger than the bytes actually uploaded.
"""

from __future__ import annotations

import struct
from typing import TYPE_CHECKING, Any, Literal

from llmgw import errors
from llmgw.surfaces.base import EventKind, RequestFacts, Usage
from llmgw.surfaces.voice._base import (
    VoiceRequestFacts,
    VoiceSurface,
    parse_json_object,
    payload_of,
    read_text,
    require_key,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


TTS_NO_METER_NOTE = (
    "sarvam text-to-speech reports no usage on the wire; characters counted "
    "from the request text (estimated)"
)
STT_HEADER_NOTE = (
    "sarvam speech-to-text reports no duration on the wire; seconds estimated "
    "from the uploaded WAV header (estimated)"
)
STT_NO_METER_NOTE = (
    "sarvam speech-to-text reports no duration on the wire and the upload "
    "carried no readable WAV header; billed seconds are zero (estimated)"
)

WAV_SCAN_BYTES = 64 * 1024
"""How far into a body the RIFF header is looked for. Every SDK writes the
text form fields before the file part, and a WAV header is the first 44-ish
bytes of that part, so 64 KiB is generous. Beyond it the estimate is simply
not made -- the alternative is scanning a 25 MB upload for a magic number."""

_MAX_WAV_CHUNKS = 32
"""Chunk headers walked before giving up. A hostile file can declare a
thousand zero-length chunks; the walk is bounded rather than trusting it."""


def wav_seconds(body: bytes, *, limit: int = WAV_SCAN_BYTES) -> float:
    """Seconds of audio a RIFF/WAVE body declares, or 0.0 -- never raises.

    An ESTIMATE, and labelled as one everywhere it is used. It reads the
    `fmt ` chunk's byte rate and the `data` chunk's size and divides. The
    size is clamped to the bytes actually present, so a header claiming four
    hours of audio inside a 10 KB upload is billed as 10 KB of audio.

    Total by construction: every field is bounds-checked before it is
    unpacked, the chunk walk is capped, and the whole body is wrapped. This
    runs on the request path with bytes a client chose.
    """
    try:
        start = body.find(b"RIFF", 0, limit)
        if start < 0 or len(body) < start + 12:
            return 0.0
        if body[start + 8:start + 12] != b"WAVE":
            return 0.0
        byte_rate = 0
        pos = start + 12
        for _ in range(_MAX_WAV_CHUNKS):
            if pos + 8 > len(body) or pos > limit:
                return 0.0
            cid = body[pos:pos + 4]
            (size,) = struct.unpack_from("<I", body, pos + 4)
            payload = pos + 8
            if cid == b"fmt " and size >= 16 and payload + 16 <= len(body):
                # fmt: format(2) channels(2) sample_rate(4) byte_rate(4) ...
                (byte_rate,) = struct.unpack_from("<I", body, payload + 8)
            elif cid == b"data":
                if byte_rate <= 0:
                    return 0.0
                available = max(len(body) - payload, 0)
                actual = min(size, available)
                if actual <= 0:
                    return 0.0
                return actual / byte_rate
            # Chunks are word-aligned; a zero-size chunk would otherwise spin.
            pos = payload + size + (size & 1)
        return 0.0
    except Exception:  # noqa: BLE001 - an estimate never breaks a request
        return 0.0


# ==========================================================================
# Text to speech
# ==========================================================================


class SarvamTTSSurface(VoiceSurface):
    """`POST /sarvam/text-to-speech` -> `/text-to-speech`.

    Buffered: the answer is one JSON object carrying base64 WAV in
    `audios[]`, so `parse_request` reports `stream=False` and the server's
    buffered sink sends it in one write. `framing` is `raw` because there is
    no framing to speak of; the framer is never built for a buffered body.

    Empty text is Sarvam's 400 (`'text' cannot be empty`), NOT a 200 with a
    zero bill the way Inworld answers, so the surface does not special-case
    it -- forwarding the request and letting Sarvam refuse is both cheaper
    and more honest than guessing at a rule that might change.
    """

    name = "sarvam_tts"
    path = "/text-to-speech"
    upstream_path = "/text-to-speech"
    routes = ("/sarvam/text-to-speech",)
    forward_query = False
    framing: Literal["sse", "jsonl", "raw"] = "raw"
    body: Literal["json", "multipart", "raw"] = "json"
    default_profile = "tts"
    model_key = "model"
    include_usage_injectable = False

    stream_mode: bool = False

    def parse_request(self, body: bytes) -> RequestFacts:
        raw = parse_json_object(body)
        model = require_key(raw, "model")
        # `text` is the documented field; `inputs` is the batch form Sarvam
        # also accepts ("Either 'text' or 'inputs' must be provided"). Both
        # are forwarded untouched -- so are `target_language_code` and
        # `language_code`, which Sarvam reads differently and the gateway
        # has no business reconciling.
        text = read_text(raw, "text")
        if not text:
            text = _joined_inputs(raw.get("inputs"))
        return VoiceRequestFacts(
            model=model,
            stream=self.stream_mode,
            include_usage=False,
            characters=len(text),
            framing=self.framing,
        )

    def classify(self, ev: SSEEvent) -> EventKind:
        # Raw audio frames on the streaming route; a buffered body never
        # reaches a framer at all.
        return EventKind.CONTENT

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """There is nothing to read. Stated as an override rather than
        inherited so the absence is a decision in this file and not an
        oversight three classes up."""
        return None

    def cost_notes(self, facts: Any, usage: Usage | None) -> tuple[str, ...]:
        return (TTS_NO_METER_NOTE,)


class SarvamTTSStreamSurface(SarvamTTSSurface):
    """`POST /sarvam/text-to-speech/stream` -> `/text-to-speech/stream`.

    Chunked `audio/pcm` (or `audio/mpeg`, per `output_audio_codec`), no
    framing, and no terminal frame: the body ends when the connection does.
    `raw` framing is exactly that contract, and `native_ending()` returning
    empty means the gateway never invents an end Sarvam did not send.

    A separate NAME, not just a second route on the surface above, because
    the name is the metric label and a buffered call and a streamed one are
    different operational objects -- one has a time-to-first-byte and can be
    cut mid-body, the other cannot.
    """

    name = "sarvam_tts_stream"
    path = "/text-to-speech/stream"
    upstream_path = "/text-to-speech/stream"
    routes = ("/sarvam/text-to-speech/stream",)
    stream_mode = True


def _joined_inputs(value: Any) -> str:
    """The character count of Sarvam's batch `inputs` form, best effort. A
    list of strings is the shape; anything else counts as nothing rather
    than guessing."""
    if not isinstance(value, list):
        return ""
    return "".join(item for item in value if isinstance(item, str))


# ==========================================================================
# Speech to text
# ==========================================================================


class SarvamSTTSurface(VoiceSurface):
    """`POST /sarvam/speech-to-text` -> `/speech-to-text`.

    Multipart in (part `file`, form field `model`), one JSON object out. The
    server scans the leading form fields for `model` to route
    (`app.facts_for_body`) and `upstream.apply_api_model_multipart` splices
    the catalog id into the wire id in place -- the same single edit the
    OpenAI transcription surface gets, and the only edit a multipart body
    ever receives here.

    `facts_from_body` is where the billing estimate is attached: it is the
    one hook that sees both the routing facts and the request bytes, and the
    duration is not derivable from either alone.
    """

    name = "sarvam_stt"
    path = "/speech-to-text"
    upstream_path = "/speech-to-text"
    routes = ("/sarvam/speech-to-text",)
    forward_query = False
    framing: Literal["sse", "jsonl", "raw"] = "raw"
    body: Literal["json", "multipart", "raw"] = "multipart"
    default_profile = None
    model_key = "model"
    include_usage_injectable = False

    def parse_request(self, body: bytes) -> RequestFacts:
        """Never the production path: the body is multipart and the server's
        own bounded scan (PLAN-2 B4) reads the `model` field. Kept explicit
        so nobody wires this surface up expecting a JSON parse."""
        raise errors.InvalidRequest(
            f"{self.name} takes a multipart/form-data body with the audio in a "
            f"part named `file` and the model in a `model` form field"
        )

    def facts_from_body(
        self, facts: RequestFacts, body: bytes, content_type: str | None,
    ) -> RequestFacts:
        """Attach the request-side duration estimate. Never raises; on any
        doubt it returns the facts it was given, and the bill is then a
        noted zero."""
        seconds = wav_seconds(body)
        if seconds <= 0:
            return facts
        return VoiceRequestFacts(
            model=facts.model,
            stream=False,
            max_tokens=facts.max_tokens,
            include_usage=False,
            seconds=seconds,
        )

    def classify(self, ev: SSEEvent) -> EventKind:
        return EventKind.META

    def text_delta(self, ev: SSEEvent) -> str | None:
        payload = payload_of(ev)
        if payload is None:
            return None
        text = payload.get("transcript")
        return text if isinstance(text, str) and text else None

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """Nothing to read: no duration, no usage block, no billed field.
        The seconds come from `usage_estimate` instead, and the bill stays
        `estimated` because of it."""
        return None

    def usage_estimate(self, facts: RequestFacts) -> Usage:
        usage = Usage()
        seconds = getattr(facts, "seconds", 0.0)
        if isinstance(seconds, (int, float)) and not isinstance(seconds, bool):
            if seconds > 0:
                usage.seconds = float(seconds)
        return usage

    def cost_notes(self, facts: Any, usage: Usage | None) -> tuple[str, ...]:
        seconds = getattr(usage, "seconds", 0.0) if usage is not None else 0.0
        return (STT_HEADER_NOTE,) if seconds else (STT_NO_METER_NOTE,)


class SarvamSTTTranslateSurface(SarvamSTTSurface):
    """`POST /sarvam/speech-to-text-translate` -> `/speech-to-text-translate`.

    The same request shape, the same (absent) meter and the same per-second
    rate; the response adds `diarized_transcript`, which is `null` unless
    diarization was asked for. Its own name because it is its own product
    on Sarvam's price list -- today at the same rate, and a shared label
    would make the day they diverge invisible.
    """

    name = "sarvam_stt_translate"
    path = "/speech-to-text-translate"
    upstream_path = "/speech-to-text-translate"
    routes = ("/sarvam/speech-to-text-translate",)


__all__ = [
    "STT_HEADER_NOTE",
    "STT_NO_METER_NOTE",
    "TTS_NO_METER_NOTE",
    "WAV_SCAN_BYTES",
    "SarvamSTTSurface",
    "SarvamSTTTranslateSurface",
    "SarvamTTSStreamSurface",
    "SarvamTTSSurface",
    "wav_seconds",
]

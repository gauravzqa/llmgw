"""Fake Sarvam upstream: the four HTTP speech bodies and the four faults.

Every shape here was captured from `api.sarvam.ai` on 18 and 19 Sep 2026
(`capabilities/captures-sarvam-assemblyai.md` §2 and `capabilities/sarvam.md`)
and is reproduced field for field, including the two things that are easy to
get wrong by imagining them:

* **No meter anywhere.** The TTS response is `{"request_id", "audios"}` and
  the STT response is `{"request_id", "transcript", "language_code",
  "language_probability"}`. There is no character count, no duration, no
  usage object and no usage header on any of the four. A fake that helpfully
  added one would make the gateway's estimated-basis accounting untestable,
  because the tests would be asserting against a meter that does not exist.
* **One error envelope, for everything.** `{"error": {"message", "code",
  "request_id"}}`, with `invalid_request_error`, `invalid_api_key_error` and
  `not_found_error` the codes observed. The unknown-model messages are the
  literal pydantic enumerations Sarvam sends, spelled differently on the TTS
  and STT routes -- which is exactly why the classifier needs both, and why
  copying them verbatim matters more than tidying them.

`fakes/upstream.py` owns ports, modes and counters; this module owns bodies,
and imports nothing from it. The dependency runs one way.
"""

from __future__ import annotations

import base64
import json
import os
import struct
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

# --------------------------------------------------------------------------
# Constants measured on the real thing
# --------------------------------------------------------------------------

STREAM_CHUNK_BYTES = 11_000
"""Steady-state chunk of `POST /text-to-speech/stream`: 11,000 bytes of raw
`audio/pcm`, about 344 ms of 16 kHz s16le (captures §2.1 probe 7a; a 19 Sep
run saw a 12,576 / 2,796 / 28 / 11,000 ... ramp, so the size is a typical
value and not a contract). No terminal frame: the body just ends."""

TTS_SAMPLE_RATE = 16_000
TTS_WAV_BYTES = 8_192
"""Decoded WAV payload the fake synthesises. The real 1.28 s probe clip was
41,004 bytes; the fake is smaller for tier speed and `X-Fake-Bytes` sizes
it."""

STT_TRANSCRIPT = "Hello from the gateway probe."
"""What Sarvam returned for the probe clip (19 Sep 2026)."""

TTS_MODELS = "'bulbul:v2', 'bulbul:v3-beta', 'bulbul:v3' or 'bulbul:v4-flash'"
STT_MODELS = (
    "'saarika:v2.5', 'saaras:v3', 'saaras:v3-realtime', 'saaras:v4', "
    "'saaras:v4-multispk', 'saarika:v1', 'saarika:v2' or 'saarika:flash'"
)


def _request_id() -> str:
    """`20260918_<uuid>` -- Sarvam stamps the date and a uuid on every
    response, in `x-request-id` and in `error.request_id`."""
    raw = os.urandom(16).hex()
    return (f"20260919_{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}")


def _hdr(hdr: dict[str, str]) -> dict[str, str]:
    return {**hdr, "x-request-id": _request_id(), "server": "uvicorn"}


def audio_bytes(n: int, seed: int = 0) -> bytes:
    """Deterministic pseudo-audio. Never random: a byte-for-byte passthrough
    assertion needs the fake to be a pure function of its inputs."""
    return bytes(((i * 7) + seed) % 256 for i in range(n))


def wav(data_bytes: int, *, rate: int = TTS_SAMPLE_RATE) -> bytes:
    """A RIFF/WAVE 16-bit mono PCM file of `data_bytes` samples-worth.

    Sarvam's TTS returns exactly this container (`RIFF....WAVE`, 16 kHz mono
    s16le for `speech_sample_rate: 16000`), and the gateway's speech-to-text
    duration estimate reads its header -- so the fake must produce a header
    a real parser believes, not four magic bytes."""
    head = b"".join([
        b"RIFF", struct.pack("<I", 36 + data_bytes), b"WAVE",
        b"fmt ", struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16),
        b"data", struct.pack("<I", data_bytes),
    ])
    return head + audio_bytes(data_bytes)


# --------------------------------------------------------------------------
# Requests
# --------------------------------------------------------------------------


async def _json_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    try:
        parsed = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def has_bearer_auth(request: Request) -> bool:
    """Sarvam accepts `Authorization: Bearer <key>` and its own documented
    `api-subscription-key: <key>`; the catalog row chooses Bearer. The fake
    accepts either, so a deployment that switches the row keeps working, and
    refuses neither-present the way Sarvam does (403, not 401)."""
    value = request.headers.get("authorization", "").strip()
    if value.lower().startswith("bearer ") and value[7:].strip():
        return True
    return bool(request.headers.get("api-subscription-key", "").strip())


def _form_field(body: bytes, content_type: str | None, name: str) -> str | None:
    """One small text field out of a multipart body, by a bounded scan. The
    fake needs `model` to echo it back; it never decodes the file part."""
    if not content_type or "boundary=" not in content_type:
        return None
    boundary = content_type.split("boundary=", 1)[1].split(";")[0].strip().strip('"')
    delim = b"--" + boundary.encode("latin-1")
    marker = f'name="{name}"'.encode("latin-1")
    for part in body[:64 * 1024].split(delim)[1:]:
        head, sep, payload = part.partition(b"\r\n\r\n")
        if not sep or b"filename=" in head.lower() or marker not in head:
            continue
        if payload.endswith(b"\r\n"):
            payload = payload[:-2]
        if len(payload) > 4096:
            return None
        return payload.decode("utf-8", "replace")
    return None


# --------------------------------------------------------------------------
# Errors -- one envelope for all of them
# --------------------------------------------------------------------------


def error(message: str, code: str, status: int, hdr: dict[str, str]) -> Response:
    """`{"error": {"message", "code", "request_id"}}`, the only error shape
    Sarvam has on HTTP or on a WebSocket upgrade."""
    return JSONResponse(
        {"error": {"message": message, "code": code, "request_id": _request_id()}},
        status_code=status, headers=_hdr(hdr),
    )


def bad_key_403(hdr: dict[str, str]) -> Response:
    """Probes 10b/10c and 19 Sep: a bad key AND a missing header both answer
    403 with this exact body. 403 here really is a credential, which is why
    the catalog row says `forbidden_means="auth"`."""
    return error("Invalid or missing authentication credentials",
                 "invalid_api_key_error", 403, hdr)


def unknown_model_400(hdr: dict[str, str], *, stt: bool = False) -> Response:
    """The enumerating pydantic message, verbatim. Two spellings, because
    Sarvam really does spell it two ways -- `- model: Input should be` on
    text-to-speech and `body.model : Input should be` on speech-to-text --
    and the classifier's unknown-model rule has to match both."""
    if stt:
        return error(f"body.model : Input should be {STT_MODELS}",
                     "invalid_request_error", 400, hdr)
    return error(f"Validation Error(s):\n- model: Input should be {TTS_MODELS}",
                 "invalid_request_error", 400, hdr)


def empty_text_400(hdr: dict[str, str]) -> Response:
    """Sarvam refuses empty text (probe 10h) rather than answering 200 with a
    zero bill the way Inworld does. The contrast is the reason this mode
    exists: the two providers' "nothing to say" paths are different HTTP."""
    return error("'text' cannot be empty", "invalid_request_error", 400, hdr)


def deprecated_model_400(hdr: dict[str, str], model: str = "bulbul:v2") -> Response:
    successor = "sarvam-105b" if model.startswith("sarvam") else "bulbul:v3"
    return error(f"Model '{model}' has been deprecated. Please use "
                 f"'{successor}' instead.", "invalid_request_error", 400, hdr)


def not_found_404(hdr: dict[str, str]) -> Response:
    """An unknown PATH. A well-formed API error object that means the route
    is wrong, not the model -- the case the 404 rule in `errors.py` exists
    to keep out of `ModelNotFound`."""
    return error("Not Found", "not_found_error", 404, hdr)


# --------------------------------------------------------------------------
# Text to speech
# --------------------------------------------------------------------------


def tts_payload(*, wav_bytes: int = TTS_WAV_BYTES) -> dict[str, Any]:
    """`{"request_id", "audios": [<base64 WAV>]}` and nothing else. No usage
    field: that absence is the whole billing story of this surface."""
    return {"request_id": _request_id(),
            "audios": [base64.b64encode(wav(wav_bytes)).decode("ascii")]}


async def tts(request: Request, hdr: dict[str, str], *,
              wav_bytes: int = TTS_WAV_BYTES) -> Response:
    if not has_bearer_auth(request):
        return bad_key_403(hdr)
    body = await _json_body(request)
    text = body.get("text")
    if isinstance(text, str) and not text:
        return empty_text_400(hdr)
    if not isinstance(text, str) and not body.get("inputs"):
        return error("Either 'text' or 'inputs' must be provided",
                     "invalid_request_error", 400, hdr)
    payload = tts_payload(wav_bytes=wav_bytes)
    # The wire model id, echoed so a test can prove the gateway rewrote the
    # catalog id into it. The real API does NOT return this field; it is the
    # one addition here, and it is additive so nothing that reads the real
    # shape breaks on it.
    payload["echo_model"] = body.get("model")
    return JSONResponse(payload, headers=_hdr(hdr))


async def tts_stream(request: Request, hdr: dict[str, str], *, chunks: int,
                     interval: float, chunk_bytes: int,
                     pace: Callable[[float], Awaitable[None]]) -> Response:
    """Chunked `audio/pcm`, no framing, no terminal frame, ends on close."""
    if not has_bearer_auth(request):
        return bad_key_403(hdr)
    body = await _json_body(request)
    text = body.get("text")
    if isinstance(text, str) and not text:
        return empty_text_400(hdr)

    async def gen() -> AsyncIterator[bytes]:
        for i in range(chunks):
            await pace(interval)
            yield audio_bytes(chunk_bytes, seed=i)

    return StreamingResponse(gen(), status_code=200, media_type="audio/pcm",
                             headers=_hdr(hdr))


def stream_bytes(chunks: int, *, chunk_bytes: int = STREAM_CHUNK_BYTES) -> bytes:
    """What `tts_stream` writes, for a byte-for-byte assertion."""
    return b"".join(audio_bytes(chunk_bytes, seed=i) for i in range(chunks))


# --------------------------------------------------------------------------
# Speech to text
# --------------------------------------------------------------------------


async def stt(request: Request, hdr: dict[str, str], *,
              translate: bool = False) -> Response:
    if not has_bearer_auth(request):
        return bad_key_403(hdr)
    raw = await request.body()
    model = _form_field(raw, request.headers.get("content-type"), "model")
    if not raw:
        return error("Failed to read the file, please check the audio format.",
                     "invalid_request_error", 400, hdr)
    payload: dict[str, Any] = {
        "request_id": _request_id(),
        "transcript": STT_TRANSCRIPT,
        "language_code": "en-IN",
    }
    if translate:
        # The one field the translate route adds; `null` unless diarization
        # was asked for (probe 8c).
        payload["diarized_transcript"] = None
    payload["language_probability"] = 1.0
    # Same additive echo as the TTS fake, so a test can see which wire id
    # the multipart splice put in the form field.
    payload["echo_model"] = model
    return JSONResponse(payload, headers=_hdr(hdr))


__all__ = [
    "STREAM_CHUNK_BYTES",
    "STT_MODELS",
    "STT_TRANSCRIPT",
    "TTS_MODELS",
    "TTS_SAMPLE_RATE",
    "TTS_WAV_BYTES",
    "audio_bytes",
    "bad_key_403",
    "deprecated_model_400",
    "empty_text_400",
    "error",
    "has_bearer_auth",
    "not_found_404",
    "stream_bytes",
    "stt",
    "tts",
    "tts_payload",
    "tts_stream",
    "unknown_model_400",
    "wav",
]

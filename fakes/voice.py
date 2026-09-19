"""Fake voice and utility upstreams: the bodies, not the routing.

`fakes/upstream.py` owns ports, modes, counters and the CLI; this module
owns what the new modes SEND, so that file grows a dispatch table rather
than five hundred lines of audio shapes. Every shape here is the one that
was measured or read from the provider on 16 Sep 2026 and written down in
`capabilities/voice-*.md`; where a test wants a smaller body than the real
one (a 48 KB Inworld line, a 40 MB AssemblyAI upload) the size is a
parameter and the comment says what the real number is.

Nothing here imports `fakes.upstream`. The dependency runs one way.
"""

from __future__ import annotations

import base64
import json
import os
import struct
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

# --------------------------------------------------------------------------
# Deterministic audio bytes. Never random: a byte-for-byte assertion on the
# gateway's passthrough needs the fake to be a pure function of its inputs.
# --------------------------------------------------------------------------

RAW_CHUNK_BYTES = 8192
"""OpenAI's binary TTS arrives in <= 8 KiB HTTP/2 data frames; ElevenLabs'
`audio/mpeg` chunks are the same order (capabilities/voice-openai.md,
voice-elevenlabs.md)."""

INWORLD_LINE_BYTES_REAL = 48_044
"""Decoded LINEAR16 bytes per steady-state Inworld `:stream` line: exactly
one second of 24 kHz 16-bit mono (verified live 2026-09-16, 64 KB on the
wire as base64). The fake defaults to a smaller line for tier speed; pass
`X-Fake-Bytes: 48044` for the measured size."""

INWORLD_LINE_BYTES_DEFAULT = 4096

WAV_HEADER_BYTES = 44
"""Inworld's first LINEAR16 chunk starts with a RIFF/WAVE header (verified
live); Pipecat strips it, a gateway must forward it untouched."""


def audio_bytes(n: int, seed: int = 0) -> bytes:
    """`n` deterministic pseudo-audio bytes."""
    return bytes(((i * 7) + seed) % 256 for i in range(n))


def wav_header(data_bytes: int, *, rate: int = 24_000) -> bytes:
    """A 44-byte RIFF/WAVE header for 16-bit mono PCM of `data_bytes`."""
    return b"".join([
        b"RIFF", struct.pack("<I", 36 + data_bytes), b"WAVE",
        b"fmt ", struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16),
        b"data", struct.pack("<I", data_bytes),
    ])


def raw_chunks(n: int, *, size: int = RAW_CHUNK_BYTES) -> list[bytes]:
    return [audio_bytes(size, seed=i) for i in range(n)]


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _hex(n: int = 16) -> str:
    return os.urandom(n).hex()[:n]


# --------------------------------------------------------------------------
# Request helpers. The fakes read as little of a request as the shape needs.
# --------------------------------------------------------------------------


async def json_body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    try:
        parsed = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def text_of(body: dict[str, Any]) -> str:
    """The text a TTS request asks to speak: OpenAI `input`, Inworld and
    ElevenLabs `text`."""
    for key in ("text", "input"):
        value = body.get(key)
        if isinstance(value, str):
            return value
    return ""


def has_raw_auth(request: Request) -> bool:
    """AssemblyAI wants the bare key in `Authorization`, no scheme word. A
    gateway that sent `Bearer <key>` gets AssemblyAI's 401, so the fake
    refuses the same way: present, and not starting with a scheme."""
    value = request.headers.get("authorization", "")
    if not value.strip():
        return False
    first = value.split(None, 1)[0].lower()
    return first not in {"bearer", "basic", "token"}


def _crlf_sse(event: str | None, data: bytes) -> bytes:
    """One CRLF-terminated SSE frame, the way OpenAI's audio endpoints emit
    them (verified live 2026-09-16: `\\r\\n\\r\\n`, unlike chat's `\\n\\n`)."""
    head = f"event: {event}\r\n".encode() if event else b""
    return head + b"data: " + data + b"\r\n\r\n"


# --------------------------------------------------------------------------
# Phase C / E utility modes (agent CE's surfaces route to these)
# --------------------------------------------------------------------------


async def embeddings(request: Request, hdr: dict[str, str]) -> Response:
    body = await json_body(request)
    text = body.get("input")
    n = len(json.dumps(text)) // 4 if text is not None else 0
    return JSONResponse(
        {
            "object": "list",
            "data": [{"object": "embedding", "index": 0,
                      "embedding": [round(0.125 * (i + 1), 3) for i in range(8)]}],
            "model": body.get("model"),
            "usage": {"prompt_tokens": n, "total_tokens": n},
        },
        headers=hdr,
    )


async def client_secrets(request: Request, hdr: dict[str, str]) -> Response:
    body = await json_body(request)
    expires = body.get("expires_after") if isinstance(body.get("expires_after"), dict) else {}
    seconds = expires.get("seconds", 600) if isinstance(expires, dict) else 600
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        seconds = 600
    return JSONResponse(
        {
            "value": f"ek_fake_{_hex(16)}",
            "expires_at": int(time.time()) + seconds,
            "session": body.get("session", {}),
        },
        headers=hdr,
    )


async def realtime_calls(request: Request, hdr: dict[str, str]) -> Response:
    await request.body()
    return JSONResponse({}, headers=hdr)


async def count_tokens(request: Request, hdr: dict[str, str]) -> Response:
    raw = await request.body()
    return JSONResponse({"input_tokens": max(1, len(raw) // 4)}, headers=hdr)


def assemblyai_401(hdr: dict[str, str]) -> Response:
    return JSONResponse(
        {"error": "Authentication error, API token missing/invalid"},
        status_code=401, headers=hdr,
    )


async def assemblyai_token(request: Request, hdr: dict[str, str]) -> Response:
    if not has_raw_auth(request):
        return assemblyai_401(hdr)
    q = request.query_params
    try:
        expires = int(q.get("expires_in_seconds", "600"))
        cap = int(q.get("max_session_duration_seconds", "10800"))
    except ValueError:
        return JSONResponse({"error": "bad query"}, status_code=400, headers=hdr)
    return JSONResponse(
        {"token": f"tmp_fake_{_hex(24)}", "expires_in_seconds": expires,
         "max_session_duration_seconds": cap},
        headers=hdr,
    )


AAI_MODEL_HEADER = "x-aai-model"
AAI_SYNC_MODELS: frozenset[str] = frozenset({"universal-3-5-pro"})
"""The models `sync.assemblyai.com` actually routes. `universal-2` is a real
AssemblyAI model and is NOT one of them -- asking for it is a load-balancer
404, verified live (probe A4e)."""


def _problem(status: int, title: str, detail: str, hdr: dict[str, str]) -> Response:
    """RFC 7807, the shape every application error on the sync host takes."""
    return Response(
        json.dumps({"status": status, "title": title, "detail": detail}),
        status_code=status, headers=hdr, media_type="application/problem+json",
    )


def assemblyai_elb_404(hdr: dict[str, str]) -> Response:
    """What the AWS load balancer answers when `X-AAI-Model` names nothing it
    routes: `text/plain`, two words, and `server: awselb/2.0`. No application
    code ran, so there is no JSON and nothing about the request is echoed."""
    return Response("Not found", status_code=404,
                    headers={**hdr, "server": "awselb/2.0"}, media_type="text/plain")


def assemblyai_sync_badkey(hdr: dict[str, str]) -> Response:
    """A bad key on the sync host is a 404, not a 401 (probe A4g)."""
    return _problem(404, "Not Found", "Invalid API key", hdr)


def multipart_part(body: bytes, content_type: str | None, name: str) -> bytes | None:
    """The payload of the form part called `name`, or None. Small and
    deliberate: the fakes need to see the `audio` part to answer like the
    real service, and nothing more."""
    boundary = None
    for piece in (content_type or "").split(";"):
        piece = piece.strip()
        if piece.lower().startswith("boundary="):
            boundary = piece[len("boundary="):].strip('"')
    if not boundary:
        return None
    needle = f'name="{name}"'.encode()
    for part in body.split(b"--" + boundary.encode("latin-1"))[1:]:
        if part.startswith(b"--"):
            break
        head, sep, payload = part.partition(b"\r\n\r\n")
        if not sep or needle not in head:
            continue
        return payload[:-2] if payload.endswith(b"\r\n") else payload
    return None


async def assemblyai_sync(request: Request, hdr: dict[str, str]) -> Response:
    """The sync transcribe endpoint, in the order the real one refuses:
    routing header at the load balancer, then credential, then body kind,
    then the `audio` part (probes A3a, A4a-h)."""
    model = request.headers.get(AAI_MODEL_HEADER, "")
    if model not in AAI_SYNC_MODELS:
        return assemblyai_elb_404(hdr)
    if not has_raw_auth(request):
        return assemblyai_sync_badkey(hdr)
    raw = await request.body()
    ct = request.headers.get("content-type", "")
    if not ct.lower().startswith("multipart/form-data"):
        return _problem(415, "Unsupported Media Type",
                        "request must be multipart/form-data with an `audio` part and an "
                        "optional `config` part", hdr)
    audio = multipart_part(raw, ct, "audio")
    if audio is None:
        return _problem(400, "Bad Request", "request must include an `audio` file part", hdr)
    if not audio:
        return _problem(400, "Bad Audio", "truncated WAV: ", hdr)
    # 16 kHz mono s16le: 32,000 bytes per second, so ms = bytes / 32. Exact
    # milliseconds, never rounded -- that is the sync product's meter.
    pcm = audio[WAV_HEADER_BYTES:] if audio[:4] == b"RIFF" else audio
    return JSONResponse(
        {"text": "hello from the sync fake",
         "words": [{"text": "hello", "confidence": 0.99}],
         "confidence": 0.99,
         "audio_duration_ms": len(pcm) // 32,
         "session_id": _hex(16), "request_time_ms": 12.5},
        headers=hdr,
    )


def assemblyai_403_ratelimit(hdr: dict[str, str]) -> Response:
    # AssemblyAI's REST rate limit (20k requests / 5 min) is a 403, not a 429
    # (capabilities/voice-assemblyai.md §5).
    return JSONResponse(
        {"error": "Too many requests, please slow down"}, status_code=403, headers=hdr,
    )


# --------------------------------------------------------------------------
# OpenAI audio
# --------------------------------------------------------------------------


def openai_tts_sse_frames(n: int) -> list[bytes]:
    """`speech.audio.delta` x n, `speech.audio.done` with usage, `[DONE]`;
    CRLF-terminated. The done event carries no `type` field (verified live)."""
    out = [
        _crlf_sse("speech.audio.delta", json.dumps(
            {"type": "speech.audio.delta", "audio": _b64(audio_bytes(1200, seed=i))},
            separators=(",", ":")).encode())
        for i in range(n)
    ]
    out.append(_crlf_sse("speech.audio.done", json.dumps(
        {"usage": {"input_tokens": 6, "output_tokens": 72, "total_tokens": 78}},
        separators=(",", ":")).encode()))
    out.append(b"data: [DONE]\r\n\r\n")
    return out


def openai_stt_sse_frames(n: int, *, seconds: int = 3) -> list[bytes]:
    words = ["hello", "from", "the", "transcription", "fake", "over", "sse", "today"]
    out = [
        _crlf_sse("transcript.text.delta", json.dumps(
            {"type": "transcript.text.delta", "delta": words[i % len(words)] + " "},
            separators=(",", ":")).encode())
        for i in range(n)
    ]
    text = " ".join(words[i % len(words)] for i in range(n))
    out.append(_crlf_sse("transcript.text.done", json.dumps(
        {"type": "transcript.text.done", "text": text,
         # Duration usage is rounded UP to whole seconds (verified live).
         "usage": {"type": "duration", "seconds": seconds},
         "languages": [{"code": "en"}]},
        separators=(",", ":")).encode()))
    out.append(b"data: [DONE]\r\n\r\n")
    return out


async def openai_stt_json(request: Request, hdr: dict[str, str]) -> Response:
    raw = await request.body()
    seconds = max(1, -(-len(raw) // 32_000)) if raw else 1
    return JSONResponse(
        {"text": "hello from the transcription fake",
         "usage": {"type": "duration", "seconds": seconds}},
        headers=hdr,
    )


# --------------------------------------------------------------------------
# Inworld
# --------------------------------------------------------------------------


def inworld_lines(
    n: int, *, characters: int, line_bytes: int = INWORLD_LINE_BYTES_DEFAULT,
    timestamps: bool = False, model_id: str | None = None,
) -> list[bytes]:
    """The `:stream` body: one JSON object per `\\n`, no blank lines, no
    terminator. `result.usage.processedCharactersCount` is the FULL count on
    line 1 and 0 after; the first decoded chunk starts with a RIFF header;
    timestamps ride on the same lines when asked for (verified live)."""
    out = []
    for i in range(n):
        if i == 0:
            body_bytes = max(0, line_bytes - WAV_HEADER_BYTES)
            pcm = wav_header(line_bytes) + audio_bytes(body_bytes, seed=i)
        else:
            pcm = audio_bytes(line_bytes, seed=i)
        usage: dict[str, Any] = {"processedCharactersCount": characters if i == 0 else 0}
        if i == 0 and model_id is not None:
            # Echo the key we were sent under, as the real API does
            # (`usage.modelId`, verified live 2026-09-16): a test can prove the
            # gateway rewrote `modelId`, not `model`.
            usage["modelId"] = model_id
        result: dict[str, Any] = {"audioContent": _b64(pcm), "usage": usage}
        if timestamps:
            result["timestampInfo"] = {"wordAlignment": {
                "words": [f"w{i}"], "wordStartTimeSeconds": [float(i)],
                "wordEndTimeSeconds": [float(i) + 0.5]}}
        out.append(json.dumps({"result": result}, separators=(",", ":")).encode() + b"\n")
    return out


INWORLD_EMPTY_LINE = b'{"audioContent":"","usage":null}\n'
"""What Inworld streams for empty or whitespace-only text: a 200 with one
line and a null usage (verified live 2026-09-16), not a 400."""


async def inworld_ndjson(request: Request, hdr: dict[str, str], *, events: int,
                         interval: float, line_bytes: int,
                         pace: Callable[[float], Awaitable[None]]) -> Response:
    body = await json_body(request)
    text = text_of(body)
    timestamps = bool(body.get("timestampType"))

    async def gen() -> AsyncIterator[bytes]:
        if not text.strip():
            yield INWORLD_EMPTY_LINE
            return
        for line in inworld_lines(events, characters=len(text), line_bytes=line_bytes,
                                  timestamps=timestamps, model_id=body.get("modelId")):
            await pace(interval)
            yield line

    return StreamingResponse(gen(), status_code=200, media_type="application/json",
                             headers={**hdr, "x-inworld-request-id": _hex(32),
                                      "x-envoy-upstream-service-time": "42"})


async def inworld_sync(request: Request, hdr: dict[str, str]) -> Response:
    body = await json_body(request)
    text = text_of(body)
    if not text.strip():
        return JSONResponse({"audioContent": "", "usage": None},
                            headers={**hdr, "x-inworld-request-id": _hex(32)})
    pcm = wav_header(8192) + audio_bytes(8192)
    return JSONResponse(
        {"audioContent": _b64(pcm),
         "usage": {"processedCharactersCount": len(text), "modelId": body.get("modelId")}},
        headers={**hdr, "x-inworld-request-id": _hex(32)},
    )


INWORLD_STT_MODELS: frozenset[str] = frozenset({"inworld/inworld-stt-1"})


async def inworld_stt(request: Request, hdr: dict[str, str]) -> Response:
    """`POST /stt/v1/transcribe`: protojson in, one JSON object out with
    `usage.transcribedAudioMs`. The refusal ladder is the real one, walked
    live on 19 Sep 2026 -- unknown fields are DISCARDED, so a wrong name
    surfaces as the validation error for the field that is now missing."""
    body = await json_body(request)
    audio_data = body.get("audioData") or body.get("audio_data")
    if not isinstance(audio_data, dict):
        return JSONResponse({"code": 3, "message": "audio_data is required", "details": []},
                            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    config = body.get("transcribeConfig") or body.get("transcribe_config")
    if not isinstance(config, dict):
        return JSONResponse(
            {"code": 3, "message": "invalid transcribe config: transcribe_config is required",
             "details": []},
            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    model = config.get("modelId") or config.get("model_id")
    if not model:
        return JSONResponse(
            {"code": 3, "message": "invalid transcribe config: model_id is required",
             "details": []},
            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    if model not in INWORLD_STT_MODELS:
        return JSONResponse(
            {"code": 3, "message": f'Unsupported model "{model}". Supported models: '
                                   "https://docs.inworld.ai/docs/tutorial-integrations/stt/"
                                   "supported-models", "details": []},
            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    content = audio_data.get("content")
    if not isinstance(content, str) or not content:
        return JSONResponse({"code": 3, "message": "audio data is required", "details": []},
                            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    try:
        decoded = base64.b64decode(content, validate=True)
    except (ValueError, TypeError):
        return JSONResponse(
            {"code": 3, "message": "proto: invalid value for bytes field content",
             "details": []},
            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    if decoded[:4] != b"RIFF":
        # Raw PCM is refused: the service wants a container it can read.
        return JSONResponse(
            {"code": 3, "message": "unsupported audio format - only WAV, MP3, OGG, FLAC, "
                                   "M4A, and WebM are supported", "details": []},
            status_code=400, headers={**hdr, "x-inworld-request-id": _hex(32)})
    ms = len(decoded[WAV_HEADER_BYTES:]) // 32
    return JSONResponse(
        {"transcription": {"transcript": "hello from the inworld stt fake", "isFinal": True,
                           "wordTimestamps": [], "voiceProfile": None,
                           "silenceDurationMs": 0},
         "usage": {"transcribedAudioMs": ms, "modelId": model}},
        headers={**hdr, "x-inworld-request-id": _hex(32)},
    )


def inworld_stt_403(hdr: dict[str, str]) -> Response:
    """Inworld's bad-credential answer, gRPC status 7 under a 403."""
    return JSONResponse({"code": 7, "message": "Invalid authorization credentials",
                         "details": []}, status_code=403,
                        headers={**hdr, "x-inworld-request-id": _hex(32)})


def inworld_400_code3(hdr: dict[str, str], model_id: str = "inworld-tts-999") -> Response:
    return JSONResponse({"code": 3, "message": f"model_id: {model_id} is not supported.",
                         "details": []}, status_code=400,
                        headers={**hdr, "x-inworld-request-id": _hex(32)})


def inworld_404_code5(hdr: dict[str, str], voice: str = "NoSuchVoiceXYZ") -> Response:
    return JSONResponse({"code": 5, "message": f"Unknown voice: {voice} not found!",
                         "details": []}, status_code=404,
                        headers={**hdr, "x-inworld-request-id": _hex(32)})


# --------------------------------------------------------------------------
# ElevenLabs
# --------------------------------------------------------------------------


async def elevenlabs_raw(request: Request, hdr: dict[str, str], *, events: int,
                         interval: float,
                         pace: Callable[[float], Awaitable[None]]) -> Response:
    """Chunked `audio/mpeg`, close-ended, with the ONLY per-call meter --
    `character-cost` -- in the response headers, BEFORE the body."""
    body = await json_body(request)
    text = text_of(body)

    async def gen() -> AsyncIterator[bytes]:
        for chunk in raw_chunks(events):
            await pace(interval)
            yield chunk

    return StreamingResponse(gen(), status_code=200, media_type="audio/mpeg",
                             headers={**hdr, "character-cost": str(len(text)),
                                      "request-id": _hex(24), "x-trace-id": _hex(16)})


def elevenlabs_ndjson_lines(n: int) -> list[bytes]:
    out = []
    for i in range(n):
        obj = {"audio_base64": _b64(audio_bytes(2048, seed=i)),
               "alignment": {"characters": ["a"], "character_start_times_seconds": [float(i)],
                             "character_end_times_seconds": [float(i) + 0.1]},
               "normalized_alignment": None}
        out.append(json.dumps(obj, separators=(",", ":")).encode() + b"\n")
    return out


async def elevenlabs_ndjson(request: Request, hdr: dict[str, str], *, events: int,
                            interval: float,
                            pace: Callable[[float], Awaitable[None]]) -> Response:
    body = await json_body(request)
    text = text_of(body)

    async def gen() -> AsyncIterator[bytes]:
        for line in elevenlabs_ndjson_lines(events):
            await pace(interval)
            yield line

    return StreamingResponse(gen(), status_code=200, media_type="application/json",
                             headers={**hdr, "character-cost": str(len(text)),
                                      "request-id": _hex(24)})


ELEVENLABS_STT_MODELS: frozenset[str] = frozenset(
    {"scribe_v1", "scribe_v1_experimental", "scribe_v2", "scribe_v2_medical"}
)
"""Enumerated by the provider's own 400 on an unknown `model_id` (live,
19 Sep 2026). A catalog id lands here, which is how we know the gateway's
multipart rewrite ran."""


async def elevenlabs_stt(request: Request, hdr: dict[str, str]) -> Response:
    """`POST /v1/speech-to-text`: multipart in, one JSON object out with
    `audio_duration_secs` -- exact, unrounded -- plus the `character-cost`
    and `fiat-cost-before-overages` headers the real service sends (live
    19 Sep 2026). The duration is derived from the file part so a test can
    assert the gateway billed the provider's own number."""
    raw = await request.body()
    ct = request.headers.get("content-type", "")
    model = (multipart_part(raw, ct, "model_id") or b"").decode("utf-8", "replace").strip()
    if not model:
        return JSONResponse(
            {"detail": [{"type": "missing", "loc": ["body", "model_id"],
                         "msg": "Field required", "input": None}]},
            status_code=422, headers={**hdr, "x-trace-id": _hex(32)},
        )
    if model not in ELEVENLABS_STT_MODELS:
        return JSONResponse(
            {"detail": {"type": "validation_error", "code": "unsupported_model",
                        "message": f"'{model}' is not a valid model_id. Available models: "
                                   "'scribe_v1', 'scribe_v1_experimental', 'scribe_v2', "
                                   "'scribe_v2_medical'",
                        "status": "invalid_model_id", "param": "model_id"}},
            status_code=400, headers={**hdr, "x-trace-id": _hex(32)},
        )
    audio = multipart_part(raw, ct, "file")
    if audio is None:
        return JSONResponse(
            {"detail": {"type": "validation_error", "code": "invalid_parameters",
                        "message": "Must provide either file or a URL parameter.",
                        "status": "invalid_parameters", "param": "file"}},
            status_code=400, headers={**hdr, "x-trace-id": _hex(32)},
        )
    pcm = audio[WAV_HEADER_BYTES:] if audio[:4] == b"RIFF" else audio
    seconds = round(len(pcm) / 32_000, 2)
    return JSONResponse(
        {"language_code": "eng", "language_probability": 0.93,
         "text": "hello from the scribe fake",
         "words": [{"text": "hello", "start": 0.12, "end": 0.46, "type": "word"}],
         "audio_duration_secs": seconds, "transcription_id": _hex(20)},
        headers={**hdr, "character-cost": str(max(1, int(seconds))),
                 "fiat-cost-before-overages": f"{seconds * 0.22 / 3600:.4f}",
                 "fiat-currency": "usd", "x-trace-id": _hex(32)},
    )


def elevenlabs_stt_401(hdr: dict[str, str]) -> Response:
    """A bad key on the speech-to-text route is a plain 401 -- unlike the TTS
    host's 400, which is why both shapes are in the fakes."""
    return JSONResponse(
        {"detail": {"type": "authentication_error", "code": "unauthorized",
                    "message": "Invalid API key", "status": "invalid_api_key"}},
        status_code=401, headers={**hdr, "x-trace-id": _hex(32)},
    )


def elevenlabs_403_voice(hdr: dict[str, str]) -> Response:
    # The legacy `detail.status` shape, still emitted alongside the newer
    # `detail.type/code` (capabilities/voice-elevenlabs.md §5).
    return JSONResponse(
        {"detail": {"status": "voice_access_denied",
                    "message": "You do not have access to this voice."}},
        status_code=403, headers={**hdr, "request-id": _hex(24)},
    )


__all__ = [
    "AAI_MODEL_HEADER",
    "AAI_SYNC_MODELS",
    "ELEVENLABS_STT_MODELS",
    "INWORLD_EMPTY_LINE",
    "INWORLD_LINE_BYTES_DEFAULT",
    "INWORLD_LINE_BYTES_REAL",
    "INWORLD_STT_MODELS",
    "RAW_CHUNK_BYTES",
    "WAV_HEADER_BYTES",
    "assemblyai_403_ratelimit",
    "assemblyai_elb_404",
    "assemblyai_sync",
    "assemblyai_sync_badkey",
    "assemblyai_token",
    "audio_bytes",
    "client_secrets",
    "count_tokens",
    "elevenlabs_403_voice",
    "elevenlabs_ndjson",
    "elevenlabs_ndjson_lines",
    "elevenlabs_raw",
    "elevenlabs_stt",
    "elevenlabs_stt_401",
    "embeddings",
    "has_raw_auth",
    "inworld_400_code3",
    "inworld_404_code5",
    "inworld_lines",
    "inworld_ndjson",
    "inworld_stt",
    "inworld_stt_403",
    "inworld_sync",
    "multipart_part",
    "openai_stt_json",
    "openai_stt_sse_frames",
    "openai_tts_sse_frames",
    "raw_chunks",
    "realtime_calls",
    "wav_header",
]

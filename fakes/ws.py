"""Hostile WebSocket upstreams: Inworld TTS/STT, OpenAI Realtime, AssemblyAI.

The HTTP fake (`fakes/upstream.py`) is one port per provider surface and one
mode per request. This is the same idea on the other transport: four upgrade
routes mounted on the SAME Starlette apps, so the contract tier's
`serve_in_thread` fixture serves them on the port it already has, and one
`X-Fake-Mode` on the upgrade picks the session's whole behaviour.

    /tts/v1/voice:streamBidirectional        Inworld TTS   (captures-ws.md 1)
    /stt/v1/transcribe:streamBidirectional   Inworld STT   (captures-ws.md 1)
    /v1/realtime                             OpenAI        (captures-ws.md 2)
    /v3/ws                                   AssemblyAI    (voice-assemblyai.md)

Every Inworld and OpenAI frame here is copied from `capabilities/captures-ws.md`
-- key names, nesting, the `contextId` echoed on every server frame, the
44-byte RIFF header on the first and last chunk of each flush, the exact
`response.done.response.usage` object. Nothing is invented for those two: if
the captures are silent the fake is silent too. AssemblyAI has no capture (no
key), so every AssemblyAI frame carries a comment naming the row of
`capabilities/voice-assemblyai.md` it comes from.

Selecting a mode
----------------
`X-Fake-Mode` on the upgrade request, exactly as on the HTTP fake; the plugins
send no such header, the tests and the bench do. Because a gateway may relay
headers less freely than query parameters, every knob is ALSO readable as a
`__fake_*` query parameter (`?__fake_mode=idle&__fake_interval=0.02`), which
survives the gateway's query forwarding. The header wins when both are given.

    X-Fake-Mode        the mode, from `upstream.WS_MODES`      (default `ok`)
    X-Fake-Interval    T  seconds between content frames       (default 0)
    X-Fake-Bytes       B  decoded bytes per audio chunk        (per product)
    X-Fake-Events      N  content frames per unit of work      (per product)
    X-Fake-Delay       T  seconds before the first server frame in
                          `queued-before-begin`                (default 2)
    X-Fake-Stall-After M  server frames before `stall-mid-session` goes quiet
                          and before `die-mid-session` aborts  (default 5)
    X-Fake-Stall-Side  S  `send` (keep reading) or `both`/`read` (stop reading
                          too, so the gateway's outbound buffer fills)
    X-Fake-Read-Bps    B  bytes/s the fake drains client frames at in
                          `slow-consumer`                      (default 8 KiB/s)
    X-Fake-Rtt         T  seconds of simulated round trip before each reply
                          (default 0; the live number is ~320 ms)

A malformed knob or a mode the route does not support is refused ON THE
UPGRADE with a 403 and a `x-fake-error` header, never quietly served as `ok`:
a test that thinks it asked for `die-mid-session` and got a clean session is a
test that passes for the wrong reason.

The modes, per product
----------------------
`ok` -- the full success transcript: Inworld TTS create/audio/flush/close,
Inworld STT speechStarted + interim + final + `usage` after `closeStream`,
OpenAI `session.created` then the response or transcription cycle, AssemblyAI
`Begin` then partial and final `Turn`s.

`error-7-then-close-1000-on-first-message` (Inworld) -- 101, then silence
until the client sends ANYTHING, then a top-level `error` code 7 and a server
CLOSE 1000 in the same instant. This is the bad-key shape; "101 then silence"
on its own is a healthy idle socket (captures-ws.md 3.1).

`error-16-missing-credential` (Inworld) -- the same, code 16,
`authentication is required`.

`auth-fail-in-band` -- Inworld: an alias of code 16. OpenAI: `error`
`invalid_api_key` + CLOSE 3000 (probe 10). AssemblyAI: `Error` + CLOSE 1008.

`nonfatal-error` -- Inworld: a top-level code 3 on the first client frame,
socket intact (probe 5). OpenAI: one `error` event, then normal service
(probe 8). AssemblyAI: one `Error` frame with no close.

`context-multiplex` (Inworld TTS) -- up to 5 contexts, `status.code` 8 on the
6th, `code` 5 on an unknown one, socket survives. Identical to `ok`: the limit
is enforced in EVERY mode, because the provider enforces it.

`close-1008-with-error-frame` / `close-1008-without` (AssemblyAI) -- the
ambiguity both ways, on the first client frame.

`queued-before-begin` -- the first server frame is withheld for
`X-Fake-Delay` seconds (the `first_event` budget).

`stall-mid-session` -- stops emitting after `X-Fake-Stall-After` server
frames; `X-Fake-Stall-Side` decides whether it also stops reading.

`die-mid-session` -- writes a truncated frame and aborts the TCP connection
after `X-Fake-Stall-After` frames; the client sees 1006.

`slow-consumer` -- drains client frames at `X-Fake-Read-Bps`.

`usage-in-termination` -- the terminal frame carries the billing number:
AssemblyAI `Termination{session_duration_seconds, audio_duration_seconds}` on
`Terminate`, Inworld STT `result.usage` after `closeStream`, Inworld TTS
`contextClosed` per `close_context`, OpenAI `response.done.response.usage`.
On the Inworld and OpenAI routes that is what `ok` already does; the mode
exists so a test can name the behaviour it is asserting.

`terminate-then-hang` -- never answers `Terminate` / `closeStream` /
`close_context` and never closes, so the drain wait is what ends the session.

`idle` -- accept, the unsolicited first frame if the product has one
(`session.created` for OpenAI, `Begin` for AssemblyAI, nothing for Inworld),
then silence forever. This is S10's mode.

Two behaviours are NOT modes, because the live providers do them on every
socket: Inworld answers a malformed frame with a non-fatal top-level `error`
code 3 and ignores a well-formed frame it does not recognise (probe 5), and
OpenAI answers a bad event with a non-fatal `error` and rejects
`OpenAI-Beta` / a missing `?model=` with `error` + CLOSE 4000 (probes 7c,
8, 11b). Those are unconditional here for the same reason.

What an ASGI WebSocket fake cannot do
-------------------------------------
1. **Backpressure is approximate.** `slow-consumer` stops calling `receive()`,
   but uvicorn has already read whatever the kernel handed it into its own
   queue, so the TCP window closes later than a real slow provider's would.
   The gateway still sees its outbound buffer fill; it sees it at a different
   byte count than production would.
2. **`die-mid-session` writes a truncated frame and aborts the transport.**
   That is an RST-or-FIN decided by the kernel, not by us; what is guaranteed
   is that no CLOSE frame is sent, so the client sees 1006.
3. **The fake never pings.** uvicorn's own keepalive (20 s) is what answers
   OpenAI's "server PING every ~20.3 s" (probe 9). Inworld's "never pings"
   (probe 3) is therefore the one capture fact this file cannot reproduce on
   the Inworld routes; a gateway asserting "no upstream ping" must assert it
   against a real socket, not this one.
"""

from __future__ import annotations

import asyncio
import base64
import json
import math
import struct
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from fakes.upstream import (
    _MAX_STALL_SECONDS,
    WS_CLIENT_FRAMES,
    WS_MODES,
    WS_PATHS,
    BadFakeRequest,
)

TTS_PATH, STT_PATH, REALTIME_PATH, AAI_PATH = WS_PATHS

SUPPORTED: dict[str, frozenset[str]] = {
    TTS_PATH: frozenset({
        "ok", "error-7-then-close-1000-on-first-message",
        "error-16-missing-credential", "auth-fail-in-band", "nonfatal-error",
        "context-multiplex", "queued-before-begin", "stall-mid-session",
        "die-mid-session", "slow-consumer", "usage-in-termination",
        "terminate-then-hang", "idle",
    }),
    STT_PATH: frozenset({
        "ok", "error-7-then-close-1000-on-first-message",
        "error-16-missing-credential", "auth-fail-in-band", "nonfatal-error",
        "queued-before-begin", "stall-mid-session", "die-mid-session",
        "slow-consumer", "usage-in-termination", "terminate-then-hang", "idle",
    }),
    REALTIME_PATH: frozenset({
        "ok", "auth-fail-in-band", "nonfatal-error", "queued-before-begin",
        "stall-mid-session", "die-mid-session", "slow-consumer",
        "usage-in-termination", "terminate-then-hang", "idle",
    }),
    AAI_PATH: frozenset({
        "ok", "auth-fail-in-band", "nonfatal-error",
        "close-1008-with-error-frame", "close-1008-without",
        "queued-before-begin", "stall-mid-session", "die-mid-session",
        "slow-consumer", "usage-in-termination", "terminate-then-hang", "idle",
    }),
}
"""Which mode means something on which route. An unsupported pair is refused on
the upgrade; see the module docstring's table for what each pair does."""


# ==========================================================================
# 1. Inworld TTS frames (captures-ws.md 1.2, probes 1, 2b, 4, 5)
# ==========================================================================

INWORLD_TTS_MODEL = "inworld-tts-1.5-mini"
INWORLD_STT_MODEL = "inworld/inworld-stt-1"
INWORLD_VOICE = "Aarav"
INWORLD_SAMPLE_RATE = 16_000
MAX_CONTEXTS = 5
"""Probe 4: the 6th `create` on one socket gets `status.code` 8."""
MAX_SEND_TEXT_CHARS = 2_000
"""Probe 5: `status.code` 3 "text length should not exceed 2000 characters."."""

UNSUPPORTED_MODEL_MARKER = "no-such-model"
"""A `modelId` containing this is answered exactly as probe 6c was: a
top-level `error` code 3 naming the model, then a server CLOSE 1000. Not a
mode: the fake rejects the model whatever the mode, because the provider does."""

_STATUS_OK: dict[str, Any] = {"code": 0, "message": "", "details": []}
"""`S0` in the capture's frame log: on every well-formed `result`."""


def status_ok() -> dict[str, Any]:
    """A fresh copy of `S0`. Builders never share mutable sub-objects: a caller
    that mutates one frame must not be able to change the next."""
    return dict(_STATUS_OK)


def inworld_result(context_id: str | None, key: str, body: Any) -> dict[str, Any]:
    """`{"result": {"contextId": ..., <key>: <body>, "status": S0}}`.

    `contextId` is the client's own string echoed verbatim and is present on
    EVERY server frame of a TTS socket (captures-ws.md 3.9). The STT socket has
    no contexts, so it passes `None` and the key is omitted."""
    result: dict[str, Any] = {}
    if context_id is not None:
        result["contextId"] = context_id
    result[key] = body
    result["status"] = status_ok()
    return {"result": result}


def inworld_status(context_id: str | None, code: int, message: str) -> dict[str, Any]:
    """An in-context fault: `result.status.code != 0` with the `contextId`.
    The socket survives one of these (captures-ws.md 1.3)."""
    result: dict[str, Any] = {}
    if context_id is not None:
        result["contextId"] = context_id
    result["status"] = {"code": code, "message": message, "details": []}
    return {"result": result}


def inworld_error(code: int, message: str, *, details: list[Any] | None = None,
                  status: str | None = None) -> dict[str, Any]:
    """A connection-level `error`. Two shapes live on the wire and they differ:
    the fatal ones (auth, bad model) carry `details: []` and are followed by a
    server CLOSE 1000; the malformed-frame one carries `"status":
    "INVALID_ARGUMENT"` and no `details`, and the socket survives it
    (captures-ws.md probes 2b, 5)."""
    body: dict[str, Any] = {"code": code, "message": message}
    if status is not None:
        body["status"] = status
    else:
        body["details"] = details if details is not None else []
    return {"error": body}


def bad_key_error() -> dict[str, Any]:
    """Probe 2b. The live message embeds the key's first four characters; the
    fake has no key, so it embeds a constant. Nothing here is a credential."""
    return inworld_error(7, 'Invalid credentials provided for API key "fake***"')


def missing_credential_error() -> dict[str, Any]:
    """Probes 2a/2c: code 16, `authentication is required`. The capture names
    the `InworldStatus` details block's two fields (`SESSION_TOKEN_INVALID`,
    `NO_RETRY`) but not the block's `@type`, so that one string is the fake's
    own and is the only field here a consumer must not depend on."""
    return inworld_error(16, "authentication is required", details=[{
        "@type": "type.googleapis.com/ai.inworld.InworldStatus",
        "errorType": "SESSION_TOKEN_INVALID",
        "retryType": "NO_RETRY",
    }])


def malformed_frame_error() -> dict[str, Any]:
    """Probe 5, verbatim: non-JSON and BINARY both get this, and the socket
    stays open."""
    return inworld_error(
        3, "invalid WebSocket request for the selected response protocol",
        status="INVALID_ARGUMENT",
    )


def unsupported_model_error(model_id: str) -> dict[str, Any]:
    """Probe 6c, verbatim but for the model name."""
    return inworld_error(3, (
        f'Unsupported model "{model_id}". Supported models: '
        "https://docs.inworld.ai/docs/tutorial-integrations/stt/supported-models"
    ))


def context_limit_status(context_id: str) -> dict[str, Any]:
    """Probe 4, verbatim."""
    return inworld_status(context_id, 8, (
        f"You have reached the limit of {MAX_CONTEXTS} TTS contexts per "
        "connection. Please close other contexts on this connection to continue."
    ))


def context_not_found_status(context_id: str, payload: str | None = None) -> dict[str, Any]:
    """Probe 4: `context ctx-F not found` for a `close_context`, and
    `context ctx-nope not found (payload=SEND_TEXT)` for a `send_text`."""
    suffix = f" (payload={payload})" if payload else ""
    return inworld_status(context_id, 5, f"context {context_id} not found{suffix}")


def text_too_long_status(context_id: str) -> dict[str, Any]:
    """Probe 5, verbatim. No `flushCompleted` follows the poisoned flush."""
    return inworld_status(
        context_id, 3, "text length should not exceed 2000 characters.")


def tts_context_created(context_id: str, create: dict[str, Any]) -> dict[str, Any]:
    """The `contextCreated` echo, with the capture's key set and the plugin's
    defaults for what the client left out (probe 1; plugin tts.py:388-414)."""
    audio = create.get("audioConfig") or {}
    return inworld_result(context_id, "contextCreated", {
        "voiceId": create.get("voiceId", INWORLD_VOICE),
        "audioConfig": {
            "audioEncoding": audio.get("audioEncoding", "LINEAR16"),
            "sampleRateHertz": audio.get("sampleRateHertz", INWORLD_SAMPLE_RATE),
        },
        "modelId": create.get("modelId", INWORLD_TTS_MODEL),
        "maxBufferDelayMs": create.get("maxBufferDelayMs", 3000),
        "bufferCharThreshold": create.get("bufferCharThreshold", 120),
        "applyTextNormalization": create.get("applyTextNormalization", "ON"),
        "autoMode": create.get("autoMode", True),
        "synthesisContext": None,
        "pronunciationDictionarySettings": None,
    })


def tts_audio_chunk_b64(context_id: str, content: str, *, characters: int,
                        model_id: str = INWORLD_TTS_MODEL) -> dict[str, Any]:
    """One `audioChunk` from an ALREADY encoded payload. `usage.
    processedCharactersCount` is the flush's whole character count on the FIRST
    chunk and 0 on every later one -- sum per flush, do not take the last
    (captures-ws.md 3.11)."""
    return inworld_result(context_id, "audioChunk", {
        "audioContent": content,
        "usage": {"processedCharactersCount": characters, "modelId": model_id},
        "timestampInfo": None,
    })


def tts_audio_chunk(context_id: str, audio: bytes, *, characters: int,
                    model_id: str = INWORLD_TTS_MODEL) -> dict[str, Any]:
    """The same, from raw PCM."""
    return tts_audio_chunk_b64(
        context_id, base64.b64encode(audio).decode("ascii"),
        characters=characters, model_id=model_id)


def tts_flush_completed(context_id: str) -> dict[str, Any]:
    return inworld_result(context_id, "flushCompleted", {})


def tts_context_closed(context_id: str) -> dict[str, Any]:
    return inworld_result(context_id, "contextClosed", {})


def riff_header(data_len: int, *, sample_rate: int = INWORLD_SAMPLE_RATE,
                channels: int = 1, bits: int = 16) -> bytes:
    """The 44-byte RIFF/WAVE header Inworld puts on the first AND the tiny last
    chunk of every flush (captures-ws.md 3.17). Byte-for-byte the header the
    capture's 54-byte final chunk decodes to at the default arguments."""
    block_align = channels * bits // 8
    return (
        b"RIFF" + struct.pack("<I", 36 + data_len) + b"WAVEfmt "
        + struct.pack("<IHHIIHH", 16, 1, channels, sample_rate,
                      sample_rate * block_align, block_align, bits)
        + b"data" + struct.pack("<I", data_len)
    )


_PCM_PERIOD = 1024
_PCM_BLOCK = b"".join(
    struct.pack("<h", int(8000 * math.sin(2 * math.pi * i / 64)))
    for i in range(_PCM_PERIOD)
)
"""One tile of plausible LINEAR16: a 250 Hz tone at 16 kHz, built once at
import. Audio content is never allocated per chunk -- S9 runs 200 sockets at
64 KB/s and a fake that allocates fresh bytes per frame measures its own
allocator, not the gateway."""


def pcm(nbytes: int) -> bytes:
    """`nbytes` of deterministic LINEAR16, tiled from one block. Pure and
    total: a negative or odd request is clamped to an even, non-negative one."""
    n = max(0, nbytes) & ~1
    if n <= len(_PCM_BLOCK):
        return _PCM_BLOCK[:n]
    reps, rest = divmod(n, len(_PCM_BLOCK))
    return _PCM_BLOCK * reps + _PCM_BLOCK[:rest]


TTS_FINAL_CHUNK_PCM = 10
"""Probe 1's trailing chunk is 54 B = the 44-byte header + 5 samples."""


_AUDIO_B64: dict[tuple[int, int, bool], str] = {}
"""Encoded chunk payloads, keyed by (decoded size, sample rate, RIFF?).

S9 streams 200 sockets at ten 6,400-byte chunks a second for five minutes: two
million base64 encodings of the same handful of distinct payloads. The cache is
bounded because `X-Fake-Bytes` is a knob, not user input -- a run uses two or
three sizes -- and an unbounded dict in a fake is a memory leak with a long
fuse."""
_AUDIO_B64_CAP = 64


def audio_b64(nbytes: int, *, sample_rate: int = INWORLD_SAMPLE_RATE,
              riff: bool = False) -> str:
    """One chunk's `audioContent`: `nbytes` DECODED, RIFF header included in
    that count when `riff` is set. Pure (same arguments, same string) and
    total (a size under the header is clamped up to it)."""
    size = max(nbytes, 46 if riff else 2)
    key = (size, sample_rate, riff)
    hit = _AUDIO_B64.get(key)
    if hit is None:
        body = (riff_header(size - 44, sample_rate=sample_rate) + pcm(size - 44)
                if riff else pcm(size))
        hit = base64.b64encode(body).decode("ascii")
        if len(_AUDIO_B64) < _AUDIO_B64_CAP:
            _AUDIO_B64[key] = hit
    return hit


def tts_flush_iter(context_id: str, *, characters: int, chunks: int,
                   chunk_bytes: int, model_id: str = INWORLD_TTS_MODEL,
                   sample_rate: int = INWORLD_SAMPLE_RATE) -> Iterator[dict[str, Any]]:
    """One flush's server frames, LAZILY: `chunks` audio chunks then
    `flushCompleted`, shaped like probe 1.

    The first chunk carries the 44-byte RIFF header and the flush's whole
    `processedCharactersCount`; the middle chunks are raw PCM with a 0 count;
    the last chunk is the tiny 54-byte second-header chunk (probe 1, and
    captures-ws.md item 17: strip a header from every chunk that begins RIFF,
    not only the first). `chunk_bytes` is the DECODED size of a chunk INCLUDING
    its header, so the 48,044 B line is 44 + 1.5 s of 16 kHz PCM.

    Lazy because one S9 flush is 3,000 chunks: building the list up front cost
    26 MB per session and put 35 ms of the fake's own work in front of the
    first chunk, which the bench then read as latency. Pure and total:
    `chunks <= 0` yields the `flushCompleted` alone."""
    size = max(chunk_bytes, 46)
    for i in range(max(0, chunks)):
        yield tts_audio_chunk_b64(
            context_id,
            audio_b64(size, sample_rate=sample_rate, riff=(i == 0)),
            characters=characters if i == 0 else 0, model_id=model_id)
    if chunks > 0:
        yield tts_audio_chunk_b64(
            context_id,
            audio_b64(44 + TTS_FINAL_CHUNK_PCM, sample_rate=sample_rate, riff=True),
            characters=0, model_id=model_id)
    yield tts_flush_completed(context_id)


def tts_flush_frames(context_id: str, **kw: Any) -> list[dict[str, Any]]:
    """`tts_flush_iter` as a list, for the unit tests and small flushes."""
    return list(tts_flush_iter(context_id, **kw))


# ==========================================================================
# 2. Inworld STT frames (captures-ws.md 1.2, probes 6, 6b, 6c)
# ==========================================================================

STT_INTERIM_TEXT = "Hello from the gate."
STT_FINAL_TEXT = "Hello from the Gateway Probe."
"""Probe 6's actual transcripts, interim and final."""


def stt_speech_started(start_time_ms: int = 0) -> dict[str, Any]:
    return inworld_result(None, "speechStarted",
                          {"startTimeMs": start_time_ms, "confidence": 0})


def stt_speech_stopped(silence_ms: int = 150) -> dict[str, Any]:
    """Probe 6b: the server ends the turn itself after ~150 ms of silence."""
    return inworld_result(None, "speechStopped", {"silenceDurationMs": silence_ms})


def stt_transcription(transcript: str, *, is_final: bool,
                      silence_ms: int = 0) -> dict[str, Any]:
    return inworld_result(None, "transcription", {
        "transcript": transcript,
        "isFinal": is_final,
        "wordTimestamps": [],
        "voiceProfile": None,
        "silenceDurationMs": silence_ms,
    })


def stt_usage(transcribed_audio_ms: int,
              model_id: str = INWORLD_STT_MODEL) -> dict[str, Any]:
    """The single `result.usage` that follows `closeStream` (probe 6). Note the
    absence of a `status` field is NOT a difference: the capture's usage frame
    is 82 B, which is this object with `status` -- see `inworld_result`."""
    return inworld_result(None, "usage", {
        "transcribedAudioMs": transcribed_audio_ms, "modelId": model_id})


def transcribed_audio_ms(audio_bytes: int, *, sample_rate: int,
                         channels: int = 1) -> int:
    """The fake's stand-in for the provider's own count, rounded to the 50 ms
    granularity the capture shows (1500 for 1.63 s, 3150 for 7.6 s).

    It cannot be faithful: the real number counts SPEECH plus an end-of-turn
    tail, not bytes, which is exactly why the gateway must take the frame
    rather than derive it (captures-ws.md 3.11). Deriving it from bytes is the
    only thing a fake with no recogniser can do, and it is why a test must
    assert that the gateway RELAYED the number, never that it equals a
    particular value."""
    rate = max(1, sample_rate) * max(1, channels) * 2
    ms = audio_bytes * 1000 // rate
    return (ms // 50) * 50


# ==========================================================================
# 3. OpenAI Realtime frames (captures-ws.md 2.2, probes 7, 8, 9, 10, 11)
# ==========================================================================

REALTIME_MODEL = "gpt-realtime-mini"
TRANSCRIBE_MODEL = "gpt-4o-mini-transcribe"
REALTIME_TEXT_DELTAS = ("Hi", " there", ",", " friend", "!")
TRANSCRIPT_DELTAS = ("Hello", " from", " the", " Gateway", " Probe", ".")
RATE_LIMITS_AFTER_DONE_S = 0.043
"""Probe 8: `rate_limits.updated` arrives 43 ms AFTER `response.done`, so
`response.done` is a per-response terminal but not the cycle's last frame."""
OPENAI_PING_INTERVAL_S = 20.26
"""Probe 9, recorded for the bench; the fake does not ping (uvicorn does)."""


def _turn_detection(full: bool) -> dict[str, Any]:
    td: dict[str, Any] = {"type": "server_vad", "threshold": 0.5,
                          "prefix_padding_ms": 300, "silence_duration_ms": 200}
    if full:
        td |= {"idle_timeout_ms": None, "create_response": True,
               "interrupt_response": True}
    return td


def oa_event(event_type: str, event_id: str, **fields: Any) -> dict[str, Any]:
    """Every server event is `{type, event_id, ...}` (probe 7/8 frame logs)."""
    return {"type": event_type, "event_id": event_id, **fields}


def oa_transcription_session(session_id: str, *, expires_at: int,
                             model: str | None = None) -> dict[str, Any]:
    """Probe 7's `session.created.session`, verbatim; `model` fills the
    `transcription` slot after a `session.update` (probe 7's `session.updated`)."""
    return {
        "type": "transcription",
        "object": "realtime.transcription_session",
        "id": session_id,
        "expires_at": expires_at,
        "audio": {"input": {
            "format": {"type": "audio/pcm", "rate": 24000},
            "transcription": (None if model is None else
                              {"model": model, "language": None, "prompt": None}),
            "noise_reduction": None,
            "turn_detection": _turn_detection(full=False),
        }},
        "include": None,
    }


def oa_realtime_session(session_id: str, *, model: str, expires_at: int,
                        output_modalities: list[str] | None = None) -> dict[str, Any]:
    """Probe 8's `session.created.session`, from the capture's summary line."""
    return {
        "type": "realtime",
        "object": "realtime.session",
        "id": session_id,
        "model": model,
        "output_modalities": list(output_modalities or ["audio"]),
        "instructions": "",
        "audio": {
            "input": {"format": {"type": "audio/pcm", "rate": 24000},
                      "transcription": None, "noise_reduction": None,
                      "turn_detection": _turn_detection(full=True)},
            "output": {"format": {"type": "audio/pcm", "rate": 24000},
                       "voice": "alloy", "speed": 1.0},
        },
        "max_output_tokens": "inf",
        "truncation": "auto",
        "tools": [],
        "tool_choice": "auto",
        "tracing": None,
        "prompt": None,
        "expires_at": expires_at,
    }


def oa_error(event_id: str, *, code: str | None, message: str,
             param: str | None = None,
             err_type: str = "invalid_request_error") -> dict[str, Any]:
    """Probe 8/10: the inner `error` object always carries its own
    `event_id: null` (the server's id is the outer one)."""
    return oa_event("error", event_id, error={
        "type": err_type, "code": code, "message": message,
        "param": param, "event_id": None,
    })


OA_INVALID_EVENT_TYPES = (
    "'session.update', 'session.close', 'input_audio_buffer.append', "
    "'session.input_audio_buffer.append', 'input_audio_buffer.commit', "
    "'input_audio_buffer.clear', 'conversation.item.create', "
    "'conversation.item.truncate', 'conversation.item.delete', "
    "'conversation.item.retrieve', 'response.create', and 'response.cancel'"
)


def oa_unknown_type_error(event_id: str, value: str) -> dict[str, Any]:
    """Probe 8, verbatim."""
    return oa_error(event_id, code="invalid_value", param="type", message=(
        f"Invalid value: '{value}'. Supported values are: {OA_INVALID_EVENT_TYPES}."))


def oa_invalid_json_error(event_id: str) -> dict[str, Any]:
    return oa_error(event_id, code="invalid_json", message=(
        "Invalid event: failed to parse JSON value. "), param=None)


def oa_binary_error(event_id: str) -> dict[str, Any]:
    return oa_error(event_id, code="invalid_event", message=(
        "Expected a text WebSocket message; binary frames are not supported."))


def oa_auth_error(event_id: str) -> dict[str, Any]:
    """Probe 10. The live message embeds a masked key; the fake embeds a
    constant so that no credential-shaped string exists in this repo."""
    return oa_error(event_id, code="invalid_api_key", message=(
        "Incorrect API key provided: fake***. You can find your API key at "
        "https://platform.openai.com/account/api-keys."))


def oa_missing_model_error(event_id: str) -> dict[str, Any]:
    """Probe 11b, verbatim."""
    return oa_error(event_id, code="missing_model", message=(
        "You must provide a model parameter, for example "
        "wss://api.openai.com/v1/realtime?model=gpt-realtime-1.5"))


def oa_invalid_model_error(event_id: str, model: str) -> dict[str, Any]:
    """Probe 11."""
    return oa_error(event_id, code="invalid_model", message=(
        f'Model "{model}" is not supported in realtime mode. Supported models '
        "are listed at https://platform.openai.com/docs/models."))


def oa_beta_header_error(event_id: str) -> dict[str, Any]:
    """Probes 7c/8b: `OpenAI-Beta: realtime=v1` is fatal on the GA API."""
    return oa_error(event_id, code="beta_api_shape_disabled", message=(
        "The beta API shape is no longer supported. Remove the OpenAI-Beta "
        "header and use the GA event shapes."))


def oa_response_usage(*, input_tokens: int = 120, output_tokens: int = 7,
                      cached_tokens: int = 64) -> dict[str, Any]:
    """`response.done.response.usage`, captures-ws.md item 15 verbatim at the
    default arguments: cached tokens are a SUBSET of input and are broken out
    per modality in `cached_tokens_details`."""
    return {
        "total_tokens": input_tokens + output_tokens,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "input_token_details": {
            "text_tokens": input_tokens, "audio_tokens": 0, "image_tokens": 0,
            "cached_tokens": cached_tokens,
            "cached_tokens_details": {
                "text_tokens": cached_tokens, "audio_tokens": 0, "image_tokens": 0},
        },
        "output_token_details": {"text_tokens": output_tokens, "audio_tokens": 0},
    }


def oa_transcription_usage(*, input_tokens: int = 16,
                           output_tokens: int = 8) -> dict[str, Any]:
    """`conversation.item.input_audio_transcription.completed.usage` (probe 7),
    a different shape from the response one: it has a `type` and no cache or
    image detail."""
    return {
        "type": "tokens",
        "total_tokens": input_tokens + output_tokens,
        "input_tokens": input_tokens,
        "input_token_details": {"text_tokens": 0, "audio_tokens": input_tokens},
        "output_tokens": output_tokens,
    }


def oa_rate_limits(remaining: int = 14_999_716) -> dict[str, Any]:
    """Probe 8: one entry, `tokens`; no `requests` entry. Carries no
    `event_id`-bearing body beyond the standard envelope."""
    return {"rate_limits": [{"name": "tokens", "limit": 15_000_000,
                             "remaining": remaining, "reset_seconds": 0.001}]}


def _obfuscation(n: int) -> str:
    """The deltas carry a random-looking padding string; the capture shows 11
    and 14 character ones. Deterministic here."""
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(alphabet[(i * 7 + n) % len(alphabet)] for i in range(11))


def oa_response_object(response_id: str, *, status: str, conversation_id: str,
                       output: list[Any], modalities: list[str],
                       usage: dict[str, Any] | None) -> dict[str, Any]:
    """The `response` body shared by `response.created` (in_progress, empty
    output, `usage: null`) and `response.done` (completed, with usage)."""
    return {
        "object": "realtime.response",
        "id": response_id,
        "status": status,
        "status_details": None,
        "output": output,
        "conversation_id": conversation_id,
        "output_modalities": list(modalities),
        "max_output_tokens": "inf",
        "audio": {"output": {"format": {"type": "audio/pcm", "rate": 24000},
                             "voice": "alloy"}},
        "usage": usage,
        "metadata": None,
    }


def oa_text_response_cycle(*, response_id: str, item_id: str,
                           conversation_id: str, event_id: Any,
                           deltas: tuple[str, ...] = REALTIME_TEXT_DELTAS,
                           usage: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Probe 8's 14-event response cycle, in order, ending at `response.done`.

    `rate_limits.updated` is NOT in the list: it arrives 43 ms later and the
    handler sends it on its own clock, because a test that asserts the gap is
    asserting the thing the capture actually measured. `event_id` is a callable
    handing out the next id."""
    text = "".join(deltas)
    assistant_item = {
        "id": item_id, "type": "message", "status": "completed", "role": "assistant",
        "content": [{"type": "text", "text": text}],
    }
    frames = [
        oa_event("response.created", event_id(), response=oa_response_object(
            response_id, status="in_progress", conversation_id=conversation_id,
            output=[], modalities=["text"], usage=None)),
        oa_event("response.output_item.added", event_id(), response_id=response_id,
                 output_index=0, item={**assistant_item, "status": "in_progress",
                                       "content": []}),
        oa_event("conversation.item.added", event_id(), previous_item_id=None,
                 item={**assistant_item, "status": "in_progress", "content": []}),
        oa_event("response.content_part.added", event_id(), response_id=response_id,
                 item_id=item_id, output_index=0, content_index=0,
                 part={"type": "text", "text": ""}),
    ]
    for i, delta in enumerate(deltas):
        frames.append(oa_event(
            "response.output_text.delta", event_id(), response_id=response_id,
            item_id=item_id, output_index=0, content_index=0, delta=delta,
            obfuscation=_obfuscation(i)))
    frames += [
        oa_event("response.output_text.done", event_id(), response_id=response_id,
                 item_id=item_id, output_index=0, content_index=0, text=text),
        oa_event("response.content_part.done", event_id(), response_id=response_id,
                 item_id=item_id, output_index=0, content_index=0,
                 part={"type": "text", "text": text}),
        oa_event("conversation.item.done", event_id(), previous_item_id=None,
                 item=assistant_item),
        oa_event("response.output_item.done", event_id(), response_id=response_id,
                 output_index=0, item=assistant_item),
        oa_event("response.done", event_id(), response=oa_response_object(
            response_id, status="completed", conversation_id=conversation_id,
            output=[assistant_item], modalities=["text"],
            usage=usage if usage is not None else oa_response_usage())),
    ]
    return frames


def oa_transcription_cycle(*, item_id: str, event_id: Any,
                           deltas: tuple[str, ...] = TRANSCRIPT_DELTAS,
                           usage: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """Probe 7's post-`commit` cycle: committed, the item, the deltas, and the
    `completed` event that carries the usage."""
    item = {"id": item_id, "type": "message", "status": "completed", "role": "user",
            "content": [{"type": "input_audio", "transcript": None}]}
    frames = [
        oa_event("input_audio_buffer.committed", event_id(),
                 previous_item_id=None, item_id=item_id),
        oa_event("conversation.item.added", event_id(), item=item),
        oa_event("conversation.item.done", event_id(), item=item),
    ]
    for i, delta in enumerate(deltas):
        frames.append(oa_event(
            "conversation.item.input_audio_transcription.delta", event_id(),
            item_id=item_id, content_index=0, delta=delta,
            obfuscation=_obfuscation(i)))
    frames.append(oa_event(
        "conversation.item.input_audio_transcription.completed", event_id(),
        item_id=item_id, content_index=0, transcript="".join(deltas),
        usage=usage if usage is not None else oa_transcription_usage()))
    return frames


# ==========================================================================
# 4. AssemblyAI frames -- DOCS ONLY, no capture exists (no key)
# ==========================================================================
#
# Every builder below cites the row of `capabilities/voice-assemblyai.md` it
# comes from. Where the doc names a field but not its value, the value is the
# fake's own and is flagged; where the doc is silent about a frame altogether
# (an ack for `UpdateConfiguration`, say) the fake sends nothing rather than
# invent one.

AAI_MODEL = "universal-streaming-english"
AAI_API_VERSION = "2025-05-12"
"""voice-assemblyai.md 4: `Begin.configuration.api_version` echoes the pin."""


def aai_begin(session_id: str, *, expires_at: int, model: str = AAI_MODEL,
              sample_rate: int = 16_000,
              encoding: str = "pcm_s16le") -> dict[str, Any]:
    """voice-assemblyai.md 2: `Begin{id, expires_at, configuration}`, the first
    server frame. `configuration.model` is the field PLAN-G 3.4 checks against
    the target's `api_model`; `sample_rate`/`encoding` echo the query (doc 2,
    "binary frames of 50-1000 ms audio ... `pcm_s16le` default")."""
    return {"type": "Begin", "id": session_id, "expires_at": expires_at,
            "configuration": {"model": model, "api_version": AAI_API_VERSION,
                              "sample_rate": sample_rate, "encoding": encoding}}


def aai_turn(turn_order: int, transcript: str, *, end_of_turn: bool,
             formatted: bool = False, confidence: float = 0.9) -> dict[str, Any]:
    """voice-assemblyai.md 2: `Turn{turn_order, transcript, utterance,
    end_of_turn, turn_is_formatted, end_of_turn_confidence, words[]}`. The word
    objects' field names are the doc's (`word_is_final`); their timings are the
    fake's arithmetic."""
    words = []
    at = 0
    for word in transcript.split():
        words.append({"text": word, "start": at, "end": at + 200,
                      "confidence": confidence, "word_is_final": end_of_turn})
        at += 220
    return {
        "type": "Turn", "turn_order": turn_order, "transcript": transcript,
        "utterance": transcript, "end_of_turn": end_of_turn,
        "turn_is_formatted": formatted, "end_of_turn_confidence": confidence,
        "words": words,
    }


def aai_termination(*, audio_duration_seconds: float,
                    session_duration_seconds: float) -> dict[str, Any]:
    """voice-assemblyai.md 2 and 6: the TERMINAL frame, and the only exact
    billing number -- session-open wall time, not audio."""
    return {"type": "Termination",
            "audio_duration_seconds": round(audio_duration_seconds, 3),
            "session_duration_seconds": round(session_duration_seconds, 3)}


def aai_error(error_code: int, message: str) -> dict[str, Any]:
    """voice-assemblyai.md 5: `Error{type, error_code, error}` precedes the
    close, and the close reason is truncated to 123 bytes -- read the frame,
    not the reason."""
    return {"type": "Error", "error_code": error_code, "error": message}


AAI_TOO_MANY_SESSIONS = (
    3009, "Unauthorized connection: Too many concurrent sessions")
"""voice-assemblyai.md 5: 1008 is used for auth AND for the opens limit, which
is why a body sniff is required to tell them apart."""
AAI_BAD_AUTH = (4001, "Not authorized: invalid API key")
"""voice-assemblyai.md 5 names 1008 for "auth/account/limit" but not the
`error_code` for the auth case; 4001 is the fake's own."""
AAI_TRANSCRIPT = "Hello from the gateway probe"


# ==========================================================================
# 5. Knobs
# ==========================================================================


@dataclass(frozen=True)
class WsParams:
    mode: str
    interval: float
    nbytes: int
    events: int
    delay: float
    stall_after: int
    stall_side: str
    read_bps: float
    rtt: float


_DEFAULTS: dict[str, dict[str, float]] = {
    # `nbytes` is the DECODED size of one audio chunk; 6,434 is probe 1's first
    # chunk. `events` is content frames per unit of work: chunks per flush,
    # deltas per response, client frames per Turn.
    TTS_PATH: {"nbytes": 6434, "events": 4},
    STT_PATH: {"nbytes": 3200, "events": 8},
    REALTIME_PATH: {"nbytes": 4800, "events": len(REALTIME_TEXT_DELTAS)},
    AAI_PATH: {"nbytes": 3200, "events": 5},
}
_STALL_SIDES = frozenset({"send", "read", "both"})


def _knob(ws: WebSocket, name: str) -> str | None:
    """`X-Fake-Foo` header, else `?__fake_foo=`. The header wins."""
    raw = ws.headers.get(f"x-fake-{name}")
    if raw is None:
        raw = ws.query_params.get(f"__fake_{name.replace('-', '_')}")
    return raw


def _num(ws: WebSocket, name: str, default: float) -> float:
    raw = _knob(ws, name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError as exc:
        raise BadFakeRequest(f"X-Fake-{name}: {raw!r} is not a number") from exc


def parse_ws_params(ws: WebSocket, path: str) -> WsParams:
    """The knobs for one upgrade. Raises `BadFakeRequest`, which the endpoint
    turns into a 403 on the handshake."""
    mode = (_knob(ws, "mode") or "ok").strip().lower()
    if mode not in WS_MODES:
        raise BadFakeRequest(f"X-Fake-Mode: {mode!r} is not one of {list(WS_MODES)}")
    if mode not in SUPPORTED[path]:
        raise BadFakeRequest(
            f"X-Fake-Mode: {mode!r} means nothing on {path}; "
            f"supported here: {sorted(SUPPORTED[path])}")
    d = _DEFAULTS[path]
    stall_side = (_knob(ws, "stall-side") or "send").strip().lower()
    if stall_side not in _STALL_SIDES:
        raise BadFakeRequest(
            f"X-Fake-Stall-Side: {stall_side!r} is not one of {sorted(_STALL_SIDES)}")
    read_default = 8192.0 if mode == "slow-consumer" else 0.0
    p = WsParams(
        mode=mode,
        interval=_num(ws, "interval", 0.0),
        nbytes=int(_num(ws, "bytes", d["nbytes"])),
        events=int(_num(ws, "events", d["events"])),
        delay=min(_num(ws, "delay", 2.0), _MAX_STALL_SECONDS),
        stall_after=int(_num(ws, "stall-after", 5)),
        stall_side=stall_side,
        read_bps=_num(ws, "read-bps", read_default),
        rtt=_num(ws, "rtt", 0.0),
    )
    if p.interval < 0 or p.delay < 0 or p.rtt < 0 or p.read_bps < 0:
        raise BadFakeRequest("X-Fake-Interval/Delay/Rtt/Read-Bps must be >= 0")
    if p.events < 0 or p.stall_after < 0:
        raise BadFakeRequest("X-Fake-Events and X-Fake-Stall-After must be >= 0")
    if p.nbytes < 64:
        raise BadFakeRequest("X-Fake-Bytes must be >= 64 on a WebSocket route")
    return p


# ==========================================================================
# 6. The connection wrapper: counters, pacing, stalling, dying
# ==========================================================================


class _Gone(Exception):
    """The session is over -- the client vanished, or a mode ended it. Raised
    rather than returned so that every product handler unwinds the same way."""


def _raw_transport(ws: WebSocket) -> Any:
    """uvicorn's transport, found by walking the ASGI `send` chain.

    Starlette hands the endpoint a `send` closed over uvicorn's bound
    `WebSocketsSansIOProtocol.send`, whose `__self__` owns the asyncio
    transport. There is no ASGI message for "abort this connection", and
    `die-mid-session` has to produce a 1006 at the client, so the fake reaches
    for the socket. Returns `None` if the chain ever changes shape; the caller
    then falls back to raising, which uvicorn also turns into a 1006 (at the
    cost of a traceback on stderr)."""
    seen: set[int] = set()
    stack = [ws._send]
    while stack:
        obj = stack.pop()
        if id(obj) in seen:
            continue
        seen.add(id(obj))
        owner = getattr(obj, "__self__", None)
        transport = getattr(owner, "transport", None)
        if transport is not None and hasattr(transport, "abort"):
            return transport
        for cell in getattr(obj, "__closure__", None) or ():
            try:
                stack.append(cell.cell_contents)
            except ValueError:  # pragma: no cover -- empty cell
                continue
    return None


class _Conn:
    """One accepted socket: counters, the mode's pacing, and the two ways a
    session can end badly."""

    def __init__(self, ws: WebSocket, p: WsParams, path: str, stats: Any) -> None:
        self.ws = ws
        self.p = p
        self.path = path
        self.stats = stats
        self.opened_at = time.monotonic()
        self.frames_out = 0
        self.audio_in = 0
        self.client_frames = 0
        self.client_gone = False
        self.stalled = False
        self.accounted = False
        self._next_paced = 0.0
        self._send_lock = asyncio.Lock()
        self._tasks: set[asyncio.Task[None]] = set()

    # -- lifecycle --------------------------------------------------------

    async def accept(self) -> None:
        await self.ws.accept()
        self.stats.ws_opened(self.p.mode, self.path)
        if self.p.mode == "queued-before-begin":
            await asyncio.sleep(self.p.delay)

    def _account_close(self, *, by_client: bool) -> None:
        if not self.accounted:
            self.accounted = True
            self.stats.ws_closed(by_client=by_client)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self._account_close(by_client=False)
        try:
            await self.ws.close(code=code, reason=reason)
        except (RuntimeError, WebSocketDisconnect):  # already gone
            pass
        raise _Gone

    async def finish(self) -> None:
        """Wait for the background emitters, then settle the counter. Called on
        every exit path, including the client hanging up mid-flush."""
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._account_close(by_client=self.client_gone)

    def spawn(self, coro: Any) -> None:
        """Run an emitter concurrently, so two Inworld contexts interleave on
        the wire the way probe 4 shows and the read loop keeps running."""
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # -- sending ----------------------------------------------------------

    async def send(self, frame: dict[str, Any], *, pace: bool = False) -> None:
        """One JSON text frame, counted. `pace=True` marks a content frame, the
        only kind `X-Fake-Interval` slows down.

        The pacing schedule is ABSOLUTE, not `sleep(interval)` per frame. A
        per-frame sleep is `interval + scheduling + send time`, so a five-minute
        stream at 10 frames a second drifts by the accumulated epsilon -- and a
        bench that measures a server frame's lateness against its cadence then
        measures the fake's own drift, which it will happily report as gateway
        latency. Measured before this was fixed: 180 ms of "added lag" after 80
        chunks, all of it the fake's.
        """
        if pace and self.p.interval:
            now = asyncio.get_running_loop().time()
            self._next_paced = (self._next_paced or now) + self.p.interval
            delay = self._next_paced - now
            if delay > 0:
                await asyncio.sleep(delay)
        if self.client_gone or self.stalled:
            return
        if self.p.mode == "stall-mid-session" and self.frames_out >= self.p.stall_after:
            self.stalled = True
            return
        if self.p.mode == "die-mid-session" and self.frames_out >= self.p.stall_after:
            await self.die()
        payload = json.dumps(frame, separators=(",", ":"))
        async with self._send_lock:
            if self.client_gone:
                return
            try:
                await self.ws.send_text(payload)
            except (RuntimeError, WebSocketDisconnect, OSError):
                self.client_gone = True
                raise _Gone from None
        self.frames_out += 1
        self.stats.ws_server_frame(len(payload.encode()))

    async def reply(self, frame: dict[str, Any]) -> None:
        """A frame that answers a client frame: one simulated round trip
        (`X-Fake-Rtt`, 0 by default, ~320 ms live) and no content pacing."""
        if self.p.rtt:
            await asyncio.sleep(self.p.rtt)
        await self.send(frame)

    async def die(self) -> None:
        """Abort mid-frame: write a TEXT frame header promising 4,096 bytes,
        write 32 of them, then abort the transport. The client is left waiting
        for the rest of a frame that never comes and reports 1006."""
        self.client_gone = True
        self._account_close(by_client=False)
        transport = _raw_transport(self.ws)
        if transport is None:  # pragma: no cover -- uvicorn internals changed
            raise _Gone
        try:
            transport.write(b"\x81\x7e\x10\x00" + b'{"result":{"contextId":"ct')
            transport.abort()
        except Exception:  # noqa: BLE001  -- the socket may already be gone
            pass
        raise _Gone

    # -- receiving --------------------------------------------------------

    async def recv(self) -> tuple[str, str | bytes]:
        """The next client frame as `("text"|"bytes", payload)`, counted under
        no kind yet (the product handler classifies it and calls `count`).

        Raises `_Gone` when the client disconnects or a mode stops reading."""
        if self.stalled and self.p.stall_side in ("read", "both"):
            # The point of the mode: the fake stops draining, the gateway's
            # outbound buffer fills, and its client-stall clock is what has to
            # notice. Bounded so a forgotten session cannot outlive the suite.
            await asyncio.sleep(_MAX_STALL_SECONDS)
            raise _Gone
        if self.p.read_bps:
            await asyncio.sleep(self.p.nbytes / self.p.read_bps)
        try:
            message = await self.ws.receive()
        except (RuntimeError, WebSocketDisconnect, OSError):
            self.client_gone = True
            raise _Gone from None
        if message["type"] == "websocket.disconnect":
            self.client_gone = True
            raise _Gone
        if (text := message.get("text")) is not None:
            return "text", text
        return "bytes", message.get("bytes") or b""

    def count(self, kind: str, payload: str | bytes) -> None:
        size = len(payload.encode()) if isinstance(payload, str) else len(payload)
        self.stats.ws_client_frame(kind if kind in WS_CLIENT_FRAMES else "other", size)
        self.client_frames += 1

    def terminate_received(self) -> None:
        self.stats.ws_terminate()

    @property
    def uptime(self) -> float:
        return time.monotonic() - self.opened_at


async def _drain_forever(conn: _Conn) -> None:
    """`idle`, and the tail of every session whose work is done: keep reading
    (so a client close is noticed and counted) and never send again."""
    while True:
        kind, payload = await conn.recv()
        conn.count("binary" if kind == "bytes" else "other", payload)


def _json_or_none(payload: str) -> dict[str, Any] | None:
    try:
        parsed = json.loads(payload)
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


async def _fatal_on_first_message(conn: _Conn, frame: dict[str, Any]) -> None:
    """The Inworld auth shape (probes 2a/2b/2c): silence until the client sends
    anything, then the `error` and a server CLOSE 1000 in the same instant."""
    kind, payload = await conn.recv()
    conn.count(_inworld_kind(kind, payload), payload)
    await conn.reply(frame)
    await conn.close(1000)


# ==========================================================================
# 7. Inworld TTS endpoint
# ==========================================================================


def _inworld_kind(kind: str, payload: str | bytes) -> str:
    if kind == "bytes":
        return "binary"
    data = _json_or_none(payload if isinstance(payload, str) else "")
    if data is None:
        return "invalid-json"
    for name in ("create", "send_text", "flush_context", "close_context",
                 "transcribeConfig", "audioChunk", "endTurn", "closeStream"):
        if name in data:
            return name
    return "other"


@dataclass
class _Ctx:
    """One Inworld TTS context: its `create` payload, the characters buffered
    since the last flush, and whether the pending flush was poisoned by an
    over-long `send_text` (probe 5: no `flushCompleted` follows)."""

    create: dict[str, Any]
    characters: int = 0
    poisoned: bool = False


async def _inworld_tts_session(conn: _Conn) -> None:
    p = conn.p
    if p.mode == "idle":
        await _drain_forever(conn)
    if p.mode == "error-7-then-close-1000-on-first-message":
        await _fatal_on_first_message(conn, bad_key_error())
    if p.mode in ("error-16-missing-credential", "auth-fail-in-band"):
        await _fatal_on_first_message(conn, missing_credential_error())

    contexts: dict[str, _Ctx] = {}
    first = True
    while True:
        kind, payload = await conn.recv()
        frame_kind = _inworld_kind(kind, payload)
        conn.count(frame_kind, payload)
        if first and p.mode == "nonfatal-error":
            # Probe 5's non-fatal shape, on the first frame, socket intact.
            await conn.reply(malformed_frame_error())
        first = False

        if frame_kind in ("binary", "invalid-json"):
            await conn.reply(malformed_frame_error())
            continue
        data = _json_or_none(payload if isinstance(payload, str) else "") or {}
        context_id = data.get("contextId")
        if frame_kind == "other" or not isinstance(context_id, str):
            # Probe 5: `{"foo":"bar"}` got no reply at all, for 4 s.
            continue

        if frame_kind == "create":
            create = data.get("create") or {}
            model = str(create.get("modelId") or INWORLD_TTS_MODEL)
            if UNSUPPORTED_MODEL_MARKER in model:
                await conn.reply(unsupported_model_error(model))
                await conn.close(1000)
            if len(contexts) >= MAX_CONTEXTS:
                await conn.reply(context_limit_status(context_id))
                continue
            contexts[context_id] = _Ctx(create=create)
            await conn.reply(tts_context_created(context_id, create))
        elif frame_kind == "send_text":
            ctx = contexts.get(context_id)
            if ctx is None:
                await conn.reply(context_not_found_status(context_id, "SEND_TEXT"))
                continue
            text = str((data.get("send_text") or {}).get("text") or "")
            if len(text) > MAX_SEND_TEXT_CHARS:
                ctx.poisoned = True
                await conn.reply(text_too_long_status(context_id))
                continue
            ctx.characters += len(text)
        elif frame_kind == "flush_context":
            ctx = contexts.get(context_id)
            if ctx is None:
                await conn.reply(context_not_found_status(context_id))
                continue
            if ctx.poisoned:
                ctx.poisoned = False
                ctx.characters = 0
                continue
            characters, ctx.characters = ctx.characters, 0
            conn.spawn(_emit_flush(conn, context_id, ctx, characters))
        elif frame_kind == "close_context":
            ctx = contexts.pop(context_id, None)
            if ctx is None:
                await conn.reply(context_not_found_status(context_id))
                continue
            if p.mode == "terminate-then-hang":
                continue  # never answers, never closes: the drain-wait bound
            await conn.reply(tts_context_closed(context_id))


async def _emit_flush(conn: _Conn, context_id: str, ctx: _Ctx,
                      characters: int) -> None:
    """One flush's audio, on its own task so contexts interleave (probe 4)."""
    create = ctx.create
    audio = (create.get("audioConfig") or {})
    frames = tts_flush_iter(
        context_id,
        characters=characters,
        chunks=conn.p.events,
        chunk_bytes=conn.p.nbytes,
        model_id=str(create.get("modelId") or INWORLD_TTS_MODEL),
        sample_rate=int(audio.get("sampleRateHertz") or INWORLD_SAMPLE_RATE),
    )
    try:
        if conn.p.rtt:
            await asyncio.sleep(conn.p.rtt)
        for frame in frames:
            await conn.send(frame, pace=True)
    except _Gone:
        return


# ==========================================================================
# 8. Inworld STT endpoint
# ==========================================================================


async def _inworld_stt_session(conn: _Conn) -> None:
    p = conn.p
    if p.mode == "idle":
        await _drain_forever(conn)
    if p.mode == "error-7-then-close-1000-on-first-message":
        await _fatal_on_first_message(conn, bad_key_error())
    if p.mode in ("error-16-missing-credential", "auth-fail-in-band"):
        await _fatal_on_first_message(conn, missing_credential_error())

    sample_rate = INWORLD_SAMPLE_RATE
    channels = 1
    model = INWORLD_STT_MODEL
    speech_started = False
    chunks = 0
    first = True
    while True:
        kind, payload = await conn.recv()
        frame_kind = _inworld_kind(kind, payload)
        conn.count(frame_kind, payload)
        if first and p.mode == "nonfatal-error":
            await conn.reply(malformed_frame_error())
        first = False

        if frame_kind in ("binary", "invalid-json"):
            await conn.reply(malformed_frame_error())
            continue
        data = _json_or_none(payload if isinstance(payload, str) else "") or {}

        if frame_kind == "transcribeConfig":
            # Probe 6: NOT acknowledged. The first server frame is
            # `speechStarted`, and only once audio flows.
            cfg = data.get("transcribeConfig") or {}
            model = str(cfg.get("modelId") or INWORLD_STT_MODEL)
            sample_rate = int(cfg.get("sampleRateHertz") or INWORLD_SAMPLE_RATE)
            channels = int(cfg.get("numberOfChannels") or 1)
            if UNSUPPORTED_MODEL_MARKER in model:  # probe 6c
                await conn.reply(unsupported_model_error(model))
                await conn.close(1000)
        elif frame_kind == "audioChunk":
            content = (data.get("audioChunk") or {}).get("content") or ""
            try:
                conn.audio_in += len(base64.b64decode(content, validate=False))
            except Exception:  # noqa: BLE001  -- a bad base64 body is not a crash
                pass
            chunks += 1
            if not speech_started:
                speech_started = True
                await conn.reply(stt_speech_started())
            elif p.events and chunks % p.events == 0:
                await conn.send(stt_transcription(STT_INTERIM_TEXT, is_final=False),
                                pace=True)
        elif frame_kind == "endTurn":
            await conn.reply(stt_transcription(STT_FINAL_TEXT, is_final=True))
        elif frame_kind == "closeStream":
            conn.terminate_received()
            if p.mode == "terminate-then-hang":
                continue  # no usage, no close: the drain-wait bound
            await conn.reply(stt_usage(
                transcribed_audio_ms(conn.audio_in, sample_rate=sample_rate,
                                     channels=channels),
                model_id=model))
            # Probe 6: the server does NOT close after `usage`; the client did,
            # 8.3 s later. So the session keeps reading.


# ==========================================================================
# 9. OpenAI Realtime endpoint
# ==========================================================================


_OA_CLIENT_EVENTS = frozenset({
    "session.update", "input_audio_buffer.append", "input_audio_buffer.commit",
    "input_audio_buffer.clear", "conversation.item.create", "response.create",
    "response.cancel",
})
_OA_KNOWN_EVENTS = _OA_CLIENT_EVENTS | {
    "session.close", "session.input_audio_buffer.append",
    "conversation.item.truncate", "conversation.item.delete",
    "conversation.item.retrieve",
}


def _merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge a `session.update` into the session object, the way probe 7's
    `session.updated` echoes the whole session with the change applied."""
    out = dict(base)
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = value
    return out


async def _openai_session(conn: _Conn) -> None:
    p = conn.p
    ws = conn.ws
    counter = {"n": 0}

    def event_id() -> str:
        counter["n"] += 1
        return f"event_fake{counter['n']:05d}"

    # Probes 7c/8b: the beta header is fatal whatever the mode, and the error
    # arrives with the upgrade.
    if "openai-beta" in ws.headers:
        await conn.send(oa_beta_header_error(event_id()))
        await conn.close(4000, "invalid_request_error.beta_api_shape_disabled")
    if p.mode == "auth-fail-in-band":  # probe 10
        await conn.send(oa_auth_error(event_id()))
        await conn.close(3000, "invalid_request_error.invalid_api_key")

    intent = ws.query_params.get("intent")
    model = ws.query_params.get("model")
    transcription = intent == "transcription"
    if not transcription:
        if not model:  # probe 11b
            await conn.send(oa_missing_model_error(event_id()))
            await conn.close(4000, "invalid_request_error.missing_model")
        if UNSUPPORTED_MODEL_MARKER in model or model == "gpt-nope":  # probe 11
            await conn.send(oa_invalid_model_error(event_id(), model))
            await conn.close(4000, "invalid_request_error.invalid_model")

    expires_at = int(time.time()) + 3600
    session = (oa_transcription_session(f"sess_fake{id(conn) & 0xFFFFFF:06x}",
                                        expires_at=expires_at)
               if transcription else
               oa_realtime_session(f"sess_fake{id(conn) & 0xFFFFFF:06x}",
                                   model=model or REALTIME_MODEL,
                                   expires_at=expires_at))
    await conn.send(oa_event("session.created", event_id(), session=session))
    if p.mode == "nonfatal-error":  # probe 8: four in a row changed nothing
        await conn.send(oa_unknown_type_error(event_id(), "nope"))
    if p.mode == "idle":
        # Probe 9: `session.created` then nothing but PINGs for 70 s. uvicorn's
        # own 20 s keepalive stands in for the provider's 20.26 s one.
        await _drain_forever(conn)

    items = 0
    responses = 0
    speech_started = False
    while True:
        kind, payload = await conn.recv()
        if kind == "bytes":
            conn.count("binary", payload)
            await conn.reply(oa_binary_error(event_id()))
            continue
        data = _json_or_none(payload if isinstance(payload, str) else "")
        if data is None:
            conn.count("invalid-json", payload)
            await conn.reply(oa_invalid_json_error(event_id()))
            continue
        event_type = str(data.get("type") or "")
        conn.count(event_type if event_type in _OA_CLIENT_EVENTS else "other", payload)
        if event_type not in _OA_KNOWN_EVENTS:
            await conn.reply(oa_unknown_type_error(event_id(), event_type))
            continue

        if event_type == "session.update":
            update = data.get("session")
            if not isinstance(update, dict):
                await conn.reply(oa_error(event_id(), code="invalid_value",
                                          param="session",
                                          message="Invalid value for 'session'."))
                continue
            session = _merge(session, update)
            await conn.reply(oa_event("session.updated", event_id(), session=session))
        elif event_type == "input_audio_buffer.append":
            try:
                conn.audio_in += len(base64.b64decode(data.get("audio") or "",
                                                      validate=False))
            except Exception:  # noqa: BLE001
                pass
            if not speech_started:
                speech_started = True
                await conn.reply(oa_event(
                    "input_audio_buffer.speech_started", event_id(),
                    audio_start_ms=0, item_id=f"item_fake{items:04d}"))
        elif event_type == "input_audio_buffer.commit":
            items += 1
            speech_started = False
            for frame in oa_transcription_cycle(item_id=f"item_fake{items:04d}",
                                                event_id=event_id):
                await conn.send(frame, pace=True)
        elif event_type == "conversation.item.create":
            items += 1
            item = data.get("item") if isinstance(data.get("item"), dict) else {}
            echoed = {**item, "id": f"item_fake{items:04d}", "status": "completed"}
            await conn.reply(oa_event("conversation.item.added", event_id(),
                                      previous_item_id=None, item=echoed))
            await conn.send(oa_event("conversation.item.done", event_id(),
                                     previous_item_id=None, item=echoed))
        elif event_type == "response.create":
            responses += 1
            items += 1
            deltas = REALTIME_TEXT_DELTAS[:p.events] or REALTIME_TEXT_DELTAS[:1]
            for frame in oa_text_response_cycle(
                    response_id=f"resp_fake{responses:04d}",
                    item_id=f"item_fake{items:04d}",
                    conversation_id=f"conv_fake{id(conn) & 0xFFFF:04x}",
                    event_id=event_id, deltas=deltas):
                await conn.send(frame, pace=True)
            conn.spawn(_emit_rate_limits(conn, event_id))
        elif event_type == "response.cancel":
            continue  # the capture never cancelled one; nothing is invented


async def _emit_rate_limits(conn: _Conn, event_id: Any) -> None:
    """Probe 8: 43 ms after `response.done`, on its own task so the read loop
    keeps running through the gap."""
    try:
        await asyncio.sleep(RATE_LIMITS_AFTER_DONE_S)
        await conn.send(oa_event("rate_limits.updated", event_id(),
                                 **oa_rate_limits()))
    except _Gone:
        return


# ==========================================================================
# 10. AssemblyAI endpoint
# ==========================================================================


async def _assemblyai_session(conn: _Conn) -> None:
    p = conn.p
    q = conn.ws.query_params
    if p.mode == "auth-fail-in-band":
        await conn.send(aai_error(*AAI_BAD_AUTH))
        await conn.close(1008, "Not authorized")

    session_id = f"aai_fake{id(conn) & 0xFFFFFF:06x}"
    await conn.send(aai_begin(
        session_id, expires_at=int(time.time()) + 10_800,
        model=q.get("speech_model") or q.get("model") or AAI_MODEL,
        sample_rate=int(q.get("sample_rate") or 16_000),
        encoding=q.get("encoding") or "pcm_s16le"))
    if p.mode == "nonfatal-error":
        # voice-assemblyai.md 5: an `Error` frame precedes a close -- but the
        # doc also lists `Error` cases the session survives (a bad
        # `UpdateConfiguration`), so the fake offers one without a close.
        await conn.send(aai_error(*AAI_TOO_MANY_SESSIONS))
    if p.mode == "idle":
        await _drain_forever(conn)

    turn = 0
    audio_frames = 0
    first = True
    while True:
        kind, payload = await conn.recv()
        if kind == "bytes":
            conn.count("binary", payload)
            conn.audio_in += len(payload)
            audio_frames += 1
            if p.events and audio_frames % p.events == 0:
                turn += 1
                await conn.send(aai_turn(turn, AAI_TRANSCRIPT, end_of_turn=False),
                                pace=True)
        else:
            data = _json_or_none(payload if isinstance(payload, str) else "") or {}
            msg_type = str(data.get("type") or "")
            conn.count(msg_type if msg_type in WS_CLIENT_FRAMES else "other", payload)
            if msg_type == "Terminate":
                conn.terminate_received()
                if p.mode == "terminate-then-hang":
                    continue
                await conn.send(aai_termination(
                    audio_duration_seconds=conn.audio_in / 32_000,
                    session_duration_seconds=conn.uptime))
                await conn.close(1000)
            elif msg_type == "ForceEndpoint":
                turn += 1
                await conn.send(aai_turn(turn, AAI_TRANSCRIPT, end_of_turn=True,
                                         formatted=True))
            # `UpdateConfiguration` and `KeepAlive` get no reply: the doc
            # (voice-assemblyai.md 2) names no acknowledgement for either.
        if first:
            first = False
            if p.mode == "close-1008-with-error-frame":
                await conn.send(aai_error(*AAI_TOO_MANY_SESSIONS))
                await conn.close(1008, "Too many concurrent sessions")
            if p.mode == "close-1008-without":
                await conn.close(1008, "Too many concurrent sessions")


# ==========================================================================
# 11. Routing
# ==========================================================================


_SESSIONS = {
    TTS_PATH: _inworld_tts_session,
    STT_PATH: _inworld_stt_session,
    REALTIME_PATH: _openai_session,
    AAI_PATH: _assemblyai_session,
}


def _endpoint(path: str, stats: Any) -> Any:
    session = _SESSIONS[path]

    async def handle(websocket: WebSocket) -> None:
        try:
            p = parse_ws_params(websocket, path)
        except BadFakeRequest as exc:
            # Before `accept`, a close is an HTTP rejection: the client sees
            # 403 and the reason, not a socket that silently behaves as `ok`.
            await websocket.close(code=1008, reason=str(exc)[:123])
            return
        conn = _Conn(websocket, p, path, stats)
        await conn.accept()
        try:
            await session(conn)
        except _Gone:
            pass
        except WebSocketDisconnect:
            conn.client_gone = True
        finally:
            await conn.finish()

    return handle


def websocket_routes(stats: Any = None) -> list[WebSocketRoute]:
    """The four routes, mounted by `fakes.upstream.build_app` on every port.

    `stats` is passed IN rather than imported, and that is not fussiness.
    `python -m fakes.upstream` runs the module as `__main__`, and this module's
    `from fakes.upstream import ...` then imports a SECOND copy under the real
    name -- with a second `STATS`. The handlers would count into one object
    while `/__stats` served the other, and every WebSocket counter would read
    zero from the CLI while passing every in-process test. `build_app` hands
    over the `STATS` of whichever copy is serving, so the two cannot diverge.
    """
    if stats is None:  # direct users (a test importing this module alone)
        from fakes.upstream import STATS as stats  # noqa: N813
    return [WebSocketRoute(path, _endpoint(path, stats), name=f"ws{i}")
            for i, path in enumerate(WS_PATHS)]

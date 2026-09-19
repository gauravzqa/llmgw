"""Fake OpenAI image generation: the two success bodies and the three faults.

Every shape here was captured from `api.openai.com/v1/images/generations` on
20 Sep 2026 and is reproduced field for field, including the three things
that are easy to get wrong by imagining them:

* **The stream has no `data: [DONE]`.** It is `event:
  image_generation.partial_image` (one per `partial_images`) and then
  `event: image_generation.completed`, and the completed event carries both
  the final image and the `usage` block. A fake that helpfully appended
  `[DONE]` would let a surface that classifies the completed event wrongly
  pass every test and then fail on the real thing with `IncompleteStream`.
* **The response echoes `size`, `quality`, `output_format` and
  `background`** at the top level, and each `data[]` entry carries a
  `generation_id` beside its `b64_json`. There is no `revised_prompt` on
  `gpt-image-1`.
* **The moderation refusal is not a `content_filter`.** It is
  `{"type":"image_generation_user_error","code":"moderation_blocked",
  "moderation_details":{"moderation_stage":"input","categories":[...]}}` at
  status 400 -- which is exactly why the classifier needs the code and not
  the type, and why copying it verbatim matters more than tidying it.

The PNG is really a PNG: `png()` emits a valid signature, IHDR, IDAT and
IEND, so a test can decode the base64 and read the dimensions out of the
header the way a client would. It is small (the real one is 1.1 MB) so the
tier stays fast; `X-Fake-Bytes` sizes the raster when a test wants a body
near a cap.

`fakes/upstream.py` owns ports, modes and counters; this module owns bodies,
and imports nothing from it. The dependency runs one way.
"""

from __future__ import annotations

import base64
import binascii
import json
import struct
import time
import uuid
import zlib
from collections.abc import AsyncIterator
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

DEFAULT_PIXELS = 64
"""Side of the fake raster, in pixels. The real thing is 1024; 64 keeps the
whole tier's image bodies under 20 KB while every byte of the container is
genuine."""

# (quality, size) -> output tokens for one image, as the real API reports
# them. `low`/1024x1024 = 272 and `low`/1536x1024 = 400 were measured through
# the gateway on 20 Sep 2026; the rest are OpenAI's published counts. The
# fake reports them so a contract test can assert that the gateway bills the
# PROVIDER's number and not its own estimate.
OUTPUT_TOKENS: dict[tuple[str, str], int] = {
    ("low", "1024x1024"): 272,
    ("low", "1024x1536"): 408,
    ("low", "1536x1024"): 400,
    ("medium", "1024x1024"): 1056,
    ("medium", "1024x1536"): 1584,
    ("medium", "1536x1024"): 1568,
    ("high", "1024x1024"): 4160,
    ("high", "1024x1536"): 6240,
    ("high", "1536x1024"): 6208,
}

PARTIAL_IMAGE_TOKENS = 100
"""What one partial image added to the bill, measured (272 -> 472 with
`partial_images: 2`). The fake charges it so a test can prove the gateway
bills the provider's total rather than the buffered-form table."""


def _chunk(kind: bytes, data: bytes) -> bytes:
    body = kind + data
    return struct.pack(">I", len(data)) + body + struct.pack(">I", binascii.crc32(body))


def png(width: int = DEFAULT_PIXELS, height: int = DEFAULT_PIXELS, seed: int = 0) -> bytes:
    """A real 8-bit greyscale PNG of `width` x `height`. Deterministic.

    Never random: a byte-for-byte passthrough assertion needs the fake to be
    a pure function of its inputs, and a test that reads the dimensions out
    of the IHDR needs an IHDR that a decoder believes.
    """
    # An LCG rather than a ramp. A smooth gradient is what you reach for
    # first and it is wrong here: zlib squashes it to nothing, so a fake
    # raster sized to exercise a MEGABYTE frame cap arrives as 7 KB and the
    # test proves the opposite of what it says. Incompressible noise makes
    # the PNG's size a function of the pixel count, which is what the caller
    # asked for.
    raw = bytearray()
    state = (seed * 2654435761 + 1) & 0xFFFFFFFF
    for _ in range(height):
        raw.append(0)  # filter byte: none
        for _ in range(width):
            state = (state * 1103515245 + 12345) & 0xFFFFFFFF
            raw.append((state >> 16) & 0xFF)
    return b"".join([
        b"\x89PNG\r\n\x1a\n",
        _chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0)),
        _chunk(b"IDAT", zlib.compress(bytes(raw), 6)),
        _chunk(b"IEND", b""),
    ])


def _request_id() -> str:
    return f"req_{uuid.uuid4().hex}"


def _hdr(hdr: dict[str, str]) -> dict[str, str]:
    return {**hdr, "x-request-id": _request_id(), "openai-processing-ms": "7204"}


def _b64(pixels: int, seed: int) -> str:
    return base64.b64encode(png(pixels, pixels, seed)).decode("ascii")


def _params(raw: dict[str, Any]) -> tuple[str, str, int, int]:
    """(quality, size, n, partial_images) as the API resolves them."""
    quality = raw.get("quality")
    quality = quality.lower() if isinstance(quality, str) else "medium"
    if quality == "auto":
        quality = "medium"
    size = raw.get("size")
    size = size.lower() if isinstance(size, str) else "1024x1024"
    if size == "auto":
        size = "1024x1024"
    n = raw.get("n")
    n = n if isinstance(n, int) and not isinstance(n, bool) and n > 0 else 1
    partials = raw.get("partial_images")
    partials = (partials if isinstance(partials, int) and not isinstance(partials, bool)
                and partials > 0 else 0)
    return quality, size, n, partials


def _usage(quality: str, size: str, n: int, partials: int, prompt: str) -> dict[str, Any]:
    """The `usage` block, in the real shape.

    Input tokens are the prompt at roughly four characters a token, which is
    what the real call reported (a 41-character prompt -> 14 tokens). Output
    is the per-image count times `n`, plus the partials -- which the real API
    charges for and says nothing about.
    """
    text_in = max(len(prompt) // 3, 1)
    out = OUTPUT_TOKENS.get((quality, size), 272) * n + partials * PARTIAL_IMAGE_TOKENS
    return {
        "input_tokens": text_in,
        "input_tokens_details": {"image_tokens": 0, "text_tokens": text_in},
        "output_tokens": out,
        "output_tokens_details": {"image_tokens": out, "text_tokens": 0},
        "total_tokens": text_in + out,
    }


async def generations(
    request: Request, hdr: dict[str, str], *, pixels: int = DEFAULT_PIXELS,
) -> Response:
    """The buffered 200: `{created, background, data[], output_format,
    quality, size, usage}`.

    `data` has `n` entries, because the real API's `n=3` returned three. It
    also echoes the WIRE model id as `echo_model`, which the real one does
    NOT -- it is the fake's only addition, and it is there so a contract test
    can prove the catalog id was rewritten on the way out. Every other field
    is the real shape.
    """
    raw = json.loads(await request.body() or b"{}")
    quality, size, n, _ = _params(raw)
    prompt = raw.get("prompt") if isinstance(raw.get("prompt"), str) else ""
    return JSONResponse({
        "created": int(time.time()),
        "background": "opaque",
        "data": [{"b64_json": _b64(pixels, i), "generation_id": str(uuid.uuid4())}
                 for i in range(n)],
        "output_format": "png",
        "quality": quality,
        "size": size,
        "usage": _usage(quality, size, n, 0, prompt),
        "echo_model": raw.get("model"),
    }, headers=_hdr(hdr))


def _sse(event: str, payload: dict[str, Any]) -> bytes:
    return (f"event: {event}\ndata: ".encode()
            + json.dumps(payload).encode() + b"\n\n")


def stream_frames(
    raw: dict[str, Any], *, pixels: int = DEFAULT_PIXELS,
) -> list[bytes]:
    """The streamed 200's frames, in order and with no `[DONE]`.

    The absence of a terminal marker is the contract, not an omission: the
    real endpoint ends on `image_generation.completed` and closes. See the
    module docstring.
    """
    quality, size, n, partials = _params(raw)
    prompt = raw.get("prompt") if isinstance(raw.get("prompt"), str) else ""
    frames = [
        _sse("image_generation.partial_image", {
            "type": "image_generation.partial_image",
            "created_at": int(time.time()),
            "b64_json": _b64(pixels, i + 1),
            "background": "opaque",
            "output_format": "png",
            "partial_image_index": i,
            "sequence_number": i,
            "quality": quality,
            "size": size,
        })
        for i in range(partials)
    ]
    frames.append(_sse("image_generation.completed", {
        "type": "image_generation.completed",
        "created_at": int(time.time()),
        "b64_json": _b64(pixels, 0),
        "background": "opaque",
        "output_format": "png",
        "sequence_number": partials,
        "quality": quality,
        "size": size,
        "usage": _usage(quality, size, n, partials, prompt),
        "echo_model": raw.get("model"),
    }))
    return frames


async def generations_stream(
    request: Request, hdr: dict[str, str], *, pixels: int = DEFAULT_PIXELS,
) -> StreamingResponse:
    raw = json.loads(await request.body() or b"{}")
    frames = stream_frames(raw, pixels=pixels)

    async def body() -> AsyncIterator[bytes]:
        for frame in frames:
            yield frame

    return StreamingResponse(
        body(), media_type="text/event-stream; charset=utf-8", headers=_hdr(hdr),
    )


# --------------------------------------------------------------------------
# The faults, byte for byte
# --------------------------------------------------------------------------


def _error(body: dict[str, Any], status: int, hdr: dict[str, str]) -> Response:
    return JSONResponse({"error": body}, status_code=status, headers=_hdr(hdr))


def moderation_blocked_400(hdr: dict[str, str], *, stage: str = "input") -> Response:
    """The refusal, captured live 2026-09-20.

    The interesting one, and the reason this module exists at all. Note what
    it is NOT: the `type` is `image_generation_user_error`, not
    `content_filter`, and the word "policy" appears nowhere -- so the
    classifier's pre-existing `content_filter`/`content_policy` rule does not
    see it. It is the CALLER's request being refused, so it must classify
    NEUTRAL (no circuit) and `try_next=False` (a refused prompt is not shopped
    around the fallbacks), which is `errors.ContentFiltered`.
    """
    rid = _request_id()
    return _error({
        "message": ("Your request was rejected by the safety system. If you "
                    "believe this is an error, contact us at help.openai.com "
                    f"and include the request ID {rid}."),
        "type": "image_generation_user_error",
        "param": None,
        "code": "moderation_blocked",
        "moderation_details": {"moderation_stage": stage, "categories": ["other"]},
    }, 400, hdr)


def unknown_model_400(hdr: dict[str, str], model: str = "dall-e-3") -> Response:
    """`dall-e-3` and `dall-e-2` are retired: the endpoint answers them like
    any other unknown id. Catalog drift, not a bad request -- `ModelNotFound`."""
    return _error({
        "message": f"The model '{model}' does not exist.",
        "type": "image_generation_user_error",
        "param": "model",
        "code": "invalid_value",
    }, 400, hdr)


def bad_size_400(hdr: dict[str, str]) -> Response:
    """A parameter the provider rejects better than we could. The message
    enumerates the supported sizes and passes through untouched."""
    return _error({
        "message": ("Invalid size '123x456'. Supported sizes are 1024x1024, "
                    "1024x1536, 1536x1024, and auto."),
        "type": "image_generation_user_error",
        "param": "size",
        "code": "invalid_value",
    }, 400, hdr)


__all__ = [
    "DEFAULT_PIXELS",
    "OUTPUT_TOKENS",
    "PARTIAL_IMAGE_TOKENS",
    "bad_size_400",
    "generations",
    "generations_stream",
    "moderation_blocked_400",
    "png",
    "stream_frames",
    "unknown_model_400",
]

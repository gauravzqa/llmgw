"""OpenAI image generation: `POST /v1/images/generations`.

Everything here was measured against `api.openai.com` on 20 Sep 2026, and
three of the measurements contradict what the endpoint is usually assumed to
do. They are stated first because each one changed a decision below.

1. **`dall-e-3` and `dall-e-2` no longer exist.** The endpoint answers them
   `400 {"error":{"message":"The model 'dall-e-3' does not exist.",
   "type":"image_generation_user_error","param":"model",
   "code":"invalid_value"}}`, and neither appears on the pricing page. They
   were the only per-image-priced models and the only ones with no usage
   block. With them gone, EVERY image model reports exact token usage, and
   the catalog needs no `images` billing unit, no per-image rate table and
   no size x quality price matrix. See `catalog.py`'s image section.

2. **The stream has no `data: [DONE]`.** It is `event:
   image_generation.partial_image` (zero to three of them, per the request's
   `partial_images`) and then `event: image_generation.completed`, which
   carries BOTH the final image and the `usage` block. The completed event is
   therefore the terminal marker -- `_eof_is_terminal()` is False for SSE, so
   a surface that classified it as anything else would raise
   `IncompleteStream` on every successful stream.

3. **Partial images are billed.** The same prompt and size answered 272
   output tokens buffered and 472 with `partial_images: 2` -- 100 output
   tokens per partial at 1024x1024 `low`. Nothing in the response says a
   partial was charged; only the total moves. The gateway does not have to
   model this (the provider states the total), but a caller who turns
   partials on and does not expect the bill to move will be surprised, so it
   is written down here.

--------------------------------------------------------------------------
Sizes
--------------------------------------------------------------------------

An image response is the largest thing this gateway carries. A low-quality
1024x1024 PNG measured 1,138,234 bytes, which is 1,517,648 base64 characters
inside a 1,518,149-byte JSON body -- and a single SSE frame on the streamed
form measured 1,695,214 bytes in the capture and 1,829,149 bytes on the
smoke run, which is 1.6 to 1.7x the process-wide `max_frame_bytes` default
of 1 MiB. Both caps are therefore set per surface
in `server/config.DEFAULT_SURFACE_LIMITS`, with the arithmetic in the comment
there. Without the frame cap every streamed image dies `FrameTooLarge`.

--------------------------------------------------------------------------
What is NOT built, and why it is recorded rather than left ambiguous
--------------------------------------------------------------------------

`/v1/images/edits` and `/v1/images/variations` exist and are multipart. They
are UNSUPPORTED here, deliberately:

* `edits` bills input images at their own rate ($10/M on `gpt-image-1`
  against $5/M for text) and reports them as
  `input_tokens_details.image_tokens`, a kind neither `ModelSpec` nor
  `metrics.TOKEN_KINDS` has a rate or a label for. Mounting the route
  without those would bill every uploaded image at HALF price and say
  nothing -- a silent under-bill, which is the failure mode this repo
  treats as worse than an outage. On a GENERATION that field is always 0
  (verified live), so the gap is unreachable from the route that is built;
  it becomes real the moment `edits` is, and closing it is that change's
  first job, not this one's.
* `variations` is `dall-e-2`-only per the API reference, and `dall-e-2` is
  gone (finding 1 above), so there is nothing left to route it to.

`capabilities/openai.md` records both as unsupported.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from llmgw import errors
from llmgw.framing import Framer, framer_for
from llmgw.surfaces.base import (
    EventKind,
    RequestFacts,
    Usage,
    as_int,
    event_payload,
    is_blank,
    is_done_marker,
    parse_json_object,
    read_stream,
    require_model,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from llmgw.sse import SSEEvent


PARTIAL_IMAGE = "image_generation.partial_image"
COMPLETED = "image_generation.completed"

DEFAULT_SIZE = "1024x1024"
DEFAULT_QUALITY = "medium"
"""The API's own defaults, used when the client names neither and the
estimator has to guess. `quality: "auto"` resolves provider-side and is
treated as `medium` for estimation only -- the estimate is never the bill
when the provider reported one, which is every completed request."""

IMAGE_OUTPUT_TOKENS: dict[tuple[str, str], int] = {
    # (quality, size) -> output tokens for ONE image. Fixed per cell: image
    # output tokens are a function of the raster, not of the prompt.
    #
    # The two cells marked "live" were measured through this gateway on
    # 20 Sep 2026; the rest are OpenAI's published per-image token counts
    # (developers.openai.com image-generation guide, read 2026-09-20) and
    # are used ONLY by `usage_estimate`, i.e. only for a stream that broke
    # before the provider stated the real number. Every completed request
    # bills the provider's own count and ignores this table entirely.
    ("low", "1024x1024"): 272,       # live
    ("low", "1024x1536"): 408,
    ("low", "1536x1024"): 400,       # live
    ("medium", "1024x1024"): 1056,
    ("medium", "1024x1536"): 1584,
    ("medium", "1536x1024"): 1568,
    ("high", "1024x1024"): 4160,
    ("high", "1024x1536"): 6240,
    ("high", "1536x1024"): 6208,
}

PARTIAL_IMAGE_TOKENS = 100
"""Output tokens one partial image added at 1024x1024 `low` (272 buffered
against 472 with `partial_images: 2`, live 2026-09-20). One measurement at
one size, so the estimator uses it flat rather than scaling it -- and the
estimator only ever runs when the provider's own total is missing."""

def _norm(value: Any, default: str) -> str:
    """A request field as a lower-case string, or the API's default.

    `"auto"` is the provider's "you decide", which cannot be resolved here,
    so it reads as the default for estimation purposes and nothing else.
    """
    if not isinstance(value, str) or not value.strip():
        return default
    text = value.strip().lower()
    return default if text == "auto" else text


@dataclass(frozen=True, slots=True)
class ImageRequestFacts(RequestFacts):
    """`RequestFacts` plus the three request fields that decide how many
    output tokens ONE image costs, for the estimator that runs when a stream
    breaks before the provider says. Read-only, like every other facts
    object: it cannot rebuild the request and is never serialised back."""

    n: int = 1
    size: str = DEFAULT_SIZE
    quality: str = DEFAULT_QUALITY
    partial_images: int = 0
    prompt_chars: int = 0
    """Length of the prompt, for the input half of `usage_estimate`. Tiny
    against the output half (14 tokens against 272 on the measured call) and
    carried anyway, because an estimate that silently omits a term is one
    nobody can reconcile against an invoice line."""


class ImagesGenerationsSurface:
    """`POST /v1/images/generations`, buffered or SSE, chosen by `stream`.

    One surface and one metric name for both forms, which is the CHAT
    pattern and not the voice one. The voice package splits
    `sarvam_tts` from `sarvam_tts_stream` because those are two routes with
    two framings and two units; here there is one route, one unit and one
    meter, and the client picks the form with a boolean in the body. A
    second label would split one product's dashboard in half for nothing.

    `framing = "sse"` is safe for the buffered form too: a buffered response
    never reaches a framer at all.
    """

    name = "images_generations"
    path = "/v1/images/generations"
    dialect = "openai"
    routes = ("/v1/images/generations",)
    upstream_path = "/v1/images/generations"
    methods = ("POST",)
    forward_query = False

    framing: Literal["sse", "jsonl", "raw"] = "sse"
    body: Literal["json", "multipart", "raw"] = "json"
    default_profile = "images"
    """A generation is slow in a way no text budget expects: the measured
    low-quality 1024x1024 took 7.2 s of provider time before the status line
    (`openai-processing-ms: 7204`), against a 10 s default `headers` budget,
    and `high` is several times that. Data only -- the policy layer resolves
    profiles -- but a deployment that mounts this surface without a
    `[profiles.images]` will 504 healthy requests at `medium` and above."""

    model_header = None
    include_usage_injectable = False
    """`stream_options.include_usage` is a chat-completions field. This
    endpoint 400s an unknown parameter, and usage arrives unasked-for on
    both forms anyway."""

    def framer(self, max_frame_bytes: int) -> Framer:
        return framer_for(self.framing, max_frame_bytes=max_frame_bytes)

    # ------------------------------------------------------------- request

    def parse_request(self, body: bytes) -> ImageRequestFacts:
        """Read routing metadata and the three sizing fields. Never rebuilds.

        Raises only `errors.InvalidRequest` (from `parse_json_object`,
        `require_model` and `read_stream`). Nothing else is validated: the
        provider rejects its own bodies better than we can guess, and it
        does so precisely -- `"Invalid size '123x456'. Supported sizes are
        1024x1024, 1024x1536, 1536x1024, and auto."` is a better message
        than any this surface could invent, and it passes through.
        """
        raw = parse_json_object(body)
        n = as_int(raw.get("n"))
        partials = as_int(raw.get("partial_images"))
        return ImageRequestFacts(
            model=require_model(raw),
            stream=read_stream(raw),
            max_tokens=None,
            # Both forms report usage without being asked. There is no
            # `include_usage` to send and none is sent.
            include_usage=True,
            n=n if n is not None and n > 0 else 1,
            size=_norm(raw.get("size"), DEFAULT_SIZE),
            quality=_norm(raw.get("quality"), DEFAULT_QUALITY),
            partial_images=partials if partials is not None and partials > 0 else 0,
            prompt_chars=len(raw["prompt"]) if isinstance(raw.get("prompt"), str) else 0,
        )

    # -------------------------------------------------------------- frames

    def classify(self, ev: SSEEvent) -> EventKind:
        """Which clock this frame may reset (C7).

        `image_generation.completed` is TERMINAL and not CONTENT, and that
        choice is load-bearing rather than cosmetic. The stream has no
        `data: [DONE]` and `pump._eof_is_terminal()` is False for SSE, so
        whatever this method calls the last frame is what decides whether a
        perfectly successful generation ends as `completed` or as
        `IncompleteStream`. The frame is genuinely both the output and the
        end; `EventKind` can only say one, and the ending is the half
        nothing else can supply.

        Nothing is lost by it: a partial image already reset the progress
        clock, and with `partial_images: 0` the whole stream is one frame,
        so there is no gap for a progress budget to measure anyway.
        """
        if ev.is_comment:
            return EventKind.HEARTBEAT
        if is_done_marker(ev):
            # Not observed on this endpoint. Checked first anyway, because
            # the one payload in this dialect that is not JSON must never
            # reach a parser (see `base.DONE_MARKER`).
            return EventKind.TERMINAL
        if is_blank(ev):
            return EventKind.HEARTBEAT
        payload = event_payload(ev)
        if payload is None:
            return EventKind.META
        if isinstance(payload.get("error"), dict):
            return EventKind.ERROR
        kind = ev.event or payload.get("type")
        if kind == PARTIAL_IMAGE:
            return EventKind.CONTENT
        if kind == COMPLETED:
            return EventKind.TERMINAL
        return EventKind.META

    def text_delta(self, ev: SSEEvent) -> str | None:
        """An image has no transcript. `revised_prompt` is not one either:
        it is what the provider decided to draw, not something it said, and
        returning it here would put it in a transcript nobody received."""
        return None

    def apply_usage(self, ev: SSEEvent, usage: Usage) -> None:
        """Fold the completed event's `usage` in. Never raises.

        `usage.images` is incremented per completed event rather than set to
        the request's `n`, for the reason `Usage.images` documents: the
        provider decides how many images it actually makes. Only n=1
        streaming has been observed live, so a hypothetical second completed
        event would count a second image and take its token totals as the
        running state -- which is the same "last report wins" rule the chat
        surface applies to its usage chunk.
        """
        try:
            payload = event_payload(ev)
            if payload is None:
                return
            if (ev.event or payload.get("type")) == COMPLETED:
                usage.images += 1
            self._read_usage_block(payload.get("usage"), usage)
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    def error_from_event(self, ev: SSEEvent) -> errors.GatewayError | None:
        """An error object inside a 200 body -> the taxonomy, or None."""
        payload = event_payload(ev)
        if payload is None:
            return None
        block = payload.get("error")
        if not isinstance(block, dict):
            return None
        etype = str(block.get("type") or "").lower()
        code = str(block.get("code") or "").lower()
        message = str(block.get("message") or "upstream error inside a 200 body")
        if "overloaded" in etype or "overloaded" in code:
            return errors.UpstreamOverloaded(message, upstream_body=ev.data)
        if "moderation" in code or "content_policy" in code:
            # A refusal that arrives after the headers is still the caller's,
            # not the provider's: NEUTRAL health, no circuit. Same rule as
            # the 400 form in `errors.from_http_status`.
            return errors.ContentFiltered(message, upstream_body=ev.data)
        return errors.InStreamError(message, upstream_body=ev.data)

    def native_ending(self, last_event: SSEEvent | None = None) -> bytes:
        """Empty, and that is the contract (C2). A generation that fails
        after commitment ends with the body closing and no
        `image_generation.completed`; synthesising one would report a
        half-drawn image as a finished one."""
        return b""

    def stop_reason_from_body(self, payload: dict[str, Any]) -> str | None:
        """This endpoint has no stop reason. A successful generation says
        nothing about why it stopped, and a refusal is an HTTP 400, not a
        field on a 200."""
        return None

    # ------------------------------------------------------------- buffered

    def usage_from_body(self, payload: dict[str, Any], usage: Usage) -> None:
        """The buffered response's own `usage`. Never raises.

        `data` is the authority on the image count, not the request's `n`:
        a live `n=2` on 20 Sep 2026 answered with one image and one image's
        worth of tokens, while `n=3` answered with three. Counting the ask
        rather than the answer would have billed an image that was never
        made.
        """
        try:
            data = payload.get("data")
            if isinstance(data, list):
                usage.images = len(data)
            self._read_usage_block(payload.get("usage"), usage)
        except Exception:  # noqa: BLE001 - billing never breaks serving
            usage.parse_failures += 1

    @staticmethod
    def _read_usage_block(block: Any, usage: Usage) -> None:
        """`{input_tokens, output_tokens, input_tokens_details{...},
        output_tokens_details{...}, total_tokens}` -> `Usage`.

        Identical on both forms -- the streamed `image_generation.completed`
        carries the same object the buffered body does -- so it is written
        once. Both exactness flags flip together: the provider states the
        whole bill in one place and never revises it.

        No caching subtraction. `prompt_tokens_details.cached_tokens` is the
        chat dialect's spelling and this endpoint does not send it; the two
        detail objects it does send (`text_tokens` / `image_tokens`) are a
        breakdown of `input_tokens`, not a subset to carve out.
        """
        if not isinstance(block, dict):
            return
        tokens_in = as_int(block.get("input_tokens"))
        tokens_out = as_int(block.get("output_tokens"))
        if tokens_in is None and tokens_out is None:
            return
        if tokens_in is not None:
            usage.input_tokens = max(tokens_in, 0)
        if tokens_out is not None:
            usage.output_tokens = max(tokens_out, 0)
        usage.input_exact = True
        usage.output_exact = True

    # ------------------------------------------------------------- the bill

    def usage_estimate(self, facts: RequestFacts) -> Usage:
        """The bill for a generation the provider never got to meter.

        Only reached when the stream broke before `image_generation.
        completed` -- every request that finishes, buffered or streamed,
        carries an exact `usage` block and this is not consulted. Image
        output tokens are a fixed function of (quality, size), so the
        estimate is a table lookup times `n`, plus the partials the request
        asked for; input is the prompt, which is small enough that a
        four-characters-per-token guess cannot move the total meaningfully
        against a 272-to-6,240-token output. Exactness stays False, so
        accounting bills it `estimated` and `cost_notes` says where it came
        from.
        """
        usage = Usage()
        per_image = IMAGE_OUTPUT_TOKENS.get(
            (getattr(facts, "quality", DEFAULT_QUALITY),
             getattr(facts, "size", DEFAULT_SIZE))
        )
        if per_image is None:
            # A size or quality this table has never seen. Zero is the honest
            # answer: a made-up number here would be billed, and
            # `cost_notes` says the table missed.
            return usage
        n = max(int(getattr(facts, "n", 1) or 1), 1)
        partials = max(int(getattr(facts, "partial_images", 0) or 0), 0)
        usage.output_tokens = per_image * n + partials * PARTIAL_IMAGE_TOKENS
        # `usage.images` is deliberately NOT set. The bill and the meter are
        # answering different questions here and a cut stream is where they
        # come apart: the provider drew the image and will charge for it, so
        # the TOKENS are estimated; but no image reached the client, so the
        # image COUNT is zero. Filling it with `n` would put images nobody
        # received on the one counter whose documented contract is "what
        # arrived" (`Usage.images`), to make a row look tidy.
        chars = max(int(getattr(facts, "prompt_chars", 0) or 0), 0)
        if chars:
            # The same four-characters-per-token density accounting uses for
            # an interrupted text stream. Calibrated once against the live
            # call: a 41-character prompt reported 14 input tokens.
            usage.input_tokens = chars // 4 + 1
        return usage

    def cost_notes(self, facts: Any, usage: Usage | None) -> tuple[str, ...]:
        """Why this bill reads the way it does.

        Empty on the normal path, which is most of the point: this surface
        bills `exact` from the provider's own counts, and a note on an exact
        record would be noise. Every completed request, buffered or
        streamed, carries a `usage` block, so a note here means something
        went wrong -- a stream cut before `image_generation.completed`, or a
        body whose usage this surface could not read.
        """
        notes: list[str] = []
        exact = bool(usage is not None and usage.exact)
        if not exact:
            quality = getattr(facts, "quality", DEFAULT_QUALITY)
            size = getattr(facts, "size", DEFAULT_SIZE)
            n = max(int(getattr(facts, "n", 1) or 1), 1)
            if (quality, size) in IMAGE_OUTPUT_TOKENS:
                notes.append(
                    f"the generation reported no usage (a stream cut before "
                    f"`{COMPLETED}`, or a body with no readable usage "
                    f"block); billed from the request's n={n} {quality} {size} at "
                    f"{IMAGE_OUTPUT_TOKENS[(quality, size)]} output tokens an "
                    f"image (OpenAI's published per-image token counts, "
                    f"read 2026-09-20) plus "
                    f"{PARTIAL_IMAGE_TOKENS} per partial image"
                )
            else:
                notes.append(
                    f"the generation reported no usage (a stream cut before "
                    f"`{COMPLETED}`, or a body with no readable usage block) "
                    f"and the request's quality/size ({quality} {size}) is not in "
                    f"the per-image token table, so the billed output is "
                    f"zero rather than a guess"
                )
        return tuple(notes)


__all__ = [
    "COMPLETED",
    "DEFAULT_QUALITY",
    "DEFAULT_SIZE",
    "IMAGE_OUTPUT_TOKENS",
    "PARTIAL_IMAGE",
    "PARTIAL_IMAGE_TOKENS",
    "ImageRequestFacts",
    "ImagesGenerationsSurface",
]

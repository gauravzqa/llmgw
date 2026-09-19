"""Live image smoke: five real generations through the gateway, saved to disk.

Spends real money -- about six cents for the default run, which is thirty
times what the voice smoke costs and the reason every number below is
printed rather than assumed. `--no-spend` runs the routing and catalog halves
only.

What is asserted per case: the status, that `x-gw-model` is the CATALOG id
(a surface that echoes the wire id there is one that billed against whatever
row the provider happened to name), that the base64 decodes to a PNG whose
IHDR says the size the request asked for, and that the gateway's own cost
record says `exact` -- because the whole billing decision behind this surface
is that OpenAI states the number and we do not have to guess it.

The ledger at the end is the point. It reads the gateway's capture records,
prints what each request was billed and on what basis, and FAILS any
`estimated` record that does not carry a `cost_notes` line saying where its
number came from. That check is shared with `live/smoke_voice.py`, where
every record is estimated; here every record should be exact, and a single
estimated row is a finding either way.

Run:
    LLMGW_ENV_FILE=/path/to/.env .venv/bin/python -m live.smoke_images
    LLMGW_ENV_FILE=/path/to/.env .venv/bin/python -m live.smoke_images --no-spend
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import struct
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from live import smoke as text_smoke
from live.env import load_env
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig
from llmgw.surfaces.images import COMPLETED, PARTIAL_IMAGE

ROUTE = "/v1/images/generations"
MODEL = "openai.gpt-image-1"
MINI = "openai.gpt-image-1-mini"

OUT_DIR = Path(__file__).resolve().parent / "out" / "images-20260920"
"""Where the PNGs land. Binaries, so the path is in `.git/info/exclude`: a
1 MB image per run is exactly the kind of file that makes a repository
unclonable six months later."""

POLICY_TOML = """
default_workload = "images"

[defaults.budgets]
total = 120.0
connect = 5.0
headers = 10.0
first_event = 20.0
progress = 15.0
client_stall = 30.0

[profiles.images.budgets]
total = 120.0
headers = 90.0
first_event = 100.0
progress = 90.0

[workloads.images]
incumbent = "openai.gpt-image-1"
profile = "images"
"""
"""The budgets an image generation actually needs, and the reason the surface
declares `default_profile = "images"`.

A generation answers nothing at all until the image is drawn: the measured
low-quality 1024x1024 took 7.2 s of provider time before the STATUS LINE
(`openai-processing-ms: 7204`), against a 10 s default `headers` budget, and
`high` at 1536x1024 is an order of magnitude more work. Under the text
defaults a perfectly healthy `medium` request is a 504 that the provider
never hears about -- the most common self-inflicted gateway outage, and here
it is one quality setting away.

The ceiling is not comfort, it is the deploy inequality: the gateway refuses
to start when any workload or profile total exceeds `LLMGW_DRAIN_GRACE`
(130 s here), because a stream longer than the grace is one the next deploy
cuts. 120 s is what an image workload gets until someone raises the grace
AND the orchestrator's kill timeout above it, which is a deployment decision
and not a smoke test's to make. `low` finishes in about nine seconds;
`high` at 1536x1024 would not fit, and that is the honest answer rather than
a number that pretends it would.
"""


@dataclass(frozen=True)
class Shot:
    """One of the five. Distinct subject, colour and composition, so a human
    can tell at a glance that five different images came back rather than one
    image five times."""

    key: str
    model: str
    prompt: str
    size: str
    quality: str = "low"
    stream: bool = False
    partial_images: int = 0


SHOTS: tuple[Shot, ...] = (
    Shot("01-red-apple", MODEL,
         "A single bright red apple on a plain white background, centred, "
         "soft studio lighting, photographic.",
         "1024x1024"),
    Shot("02-yellow-duck", MODEL,
         "A yellow rubber duck floating on calm blue water, seen from "
         "directly overhead, gentle ripples.",
         "1024x1024"),
    Shot("03-cactus-wide", MODEL,
         "A small green cactus in a terracotta pot against a flat orange "
         "wall, wide composition, hard afternoon shadow.",
         # The wide size, on purpose: it is the second cell of the per-image
         # token table this change could verify for free, and it proves the
         # gateway does not assume a square.
         "1536x1024"),
    Shot("04-ink-sketch", MINI,
         "A hand-drawn black ink sketch of a paper aeroplane on cream "
         "paper, loose simple line art, no colour.",
         # The cheap row, on purpose: a fifth of the price for the same token
         # count, and the only way to prove the two rows bill differently
         # against the real provider rather than against a fake.
         "1024x1024"),
    Shot("05-blocks-streamed", MODEL,
         "A stack of three coloured wooden toy blocks -- purple, teal and "
         "pink -- on a plain grey floor, eye level.",
         "1024x1024", stream=True, partial_images=2),
)


@dataclass
class ShotResult:
    shot: Shot
    path: Path | None = None
    png_bytes: int = 0
    dims: tuple[int, int] = (0, 0)
    latency: float = 0.0
    events: list[str] = field(default_factory=list)
    billed: float = 0.0
    basis: str = "-"
    tokens: str = "-"


def _print(line: str, out) -> None:
    print(line, file=out, flush=True)


def _has(env: str) -> bool:
    value = os.environ.get(env, "")
    return bool(value) and not value.startswith("replace-with")


def _metric(text: str, name: str, **labels: str) -> float:
    for line in text.splitlines():
        if line.startswith(name) and all(f'{k}="{v}"' in line for k, v in labels.items()):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def _gw(headers, model: str) -> tuple[bool, str]:
    """The three headers a mis-wired surface gets wrong, checked not printed.

    `x-gw-model` must be the CATALOG id: a surface that echoes the wire id
    here is one that billed against whatever row the provider happened to
    name. `x-gw-body-modified` is the warrant for the model rewrite -- this
    surface carries its model IN the body, so it must always be `1`.
    """
    served = headers.get("x-gw-served-by", "-")
    got = headers.get("x-gw-model", "-")
    mod = headers.get("x-gw-body-modified", "-")
    ok = got == model and served == f"openai/{model}" and mod == "1"
    return ok, f"served_by={served} x-gw-model={got} body-modified={mod}"


def png_dims(raw: bytes) -> tuple[int, int]:
    """Width and height out of the PNG's IHDR -- read, never assumed.

    The request asked for a size and the response echoes one; neither is
    evidence about the bytes. Only the header the decoder reads is.
    """
    if len(raw) < 24 or raw[:8] != b"\x89PNG\r\n\x1a\n":
        return (0, 0)
    return struct.unpack(">II", raw[16:24])


class Case:
    def __init__(self, gw, out) -> None:
        self.gw = gw
        self.out = out
        self.spend = 0.0
        self.failures = 0
        self.results: list[ShotResult] = []

    def _verdict(self, name: str, ok: bool, detail: str) -> None:
        if not ok:
            self.failures += 1
        _print(f"  {name}: {detail} -> {'PASS' if ok else 'FAIL'}", self.out)

    def metrics(self) -> str:
        return httpx.get(f"{self.gw.base_url}/metrics", timeout=10).text

    # ------------------------------------------------------------- routing

    def catalog_rows(self) -> None:
        """Free. The two rows exist, are priced in TOKENS, and no row anywhere
        is priced per image -- which is the design decision, asserted against
        the shipped catalog rather than described in a docstring."""
        rows = [DEFAULT_CATALOG.models.get(m) for m in (MODEL, MINI)]
        ok = all(r is not None and r.unit == "tokens" and r.per_minute is None
                 for r in rows)
        ok = ok and not any(s.unit == "images" for s in DEFAULT_CATALOG.models.values())
        detail = " ".join(
            f"{r.id}=${r.input_per_m:g}/${r.output_per_m:g} per 1M {r.unit}"
            for r in rows if r is not None)
        self._verdict("catalog-rows", ok, detail + " (no row bills per image)")

    def route_mounted(self) -> None:
        """Free. No upstream call: an unknown catalog id is refused before a
        socket, which is also proof the route reached the surface."""
        r = httpx.post(f"{self.gw.base_url}{ROUTE}",
                       json={"model": "openai.dall-e-3", "prompt": "x"}, timeout=20)
        ok = (r.status_code == 400 and r.headers.get("x-gw-attempts") == "0"
              and r.headers.get("x-gw-served-by") == "-")
        self._verdict("route-refuses-retired-model", ok,
                      f"{r.status_code} attempts={r.headers.get('x-gw-attempts')} "
                      f"served_by={r.headers.get('x-gw-served-by')} "
                      f"(dall-e-3 is retired upstream and absent from the catalog)")

    def moderation_refusal(self) -> None:
        """Costs nothing: the safety system refuses before any pixel is drawn.

        The interesting error. It must reach the client as the provider's own
        400 -- `moderation_details` is the only place the stage and the
        categories are named -- and it must be classified `content_filtered`,
        which is NEUTRAL health (no circuit) and `try_next=False` (a refused
        prompt is not shopped around the fallbacks).
        """
        if not _has("OPENAI_API_KEY"):
            _print("  moderation-refusal: SKIP (no OPENAI_API_KEY)", self.out)
            return
        before = _metric(self.metrics(), "llmgw_requests_total",
                         surface="images_generations", code="content_filtered")
        body = {"model": MODEL, "quality": "low", "size": "1024x1024",
                "prompt": ("A photorealistic image of a real, identifiable "
                           "celebrity nude on a bed, explicit sexual content")}
        r = httpx.post(f"{self.gw.base_url}{ROUTE}", json=body, timeout=120)
        try:
            err = r.json().get("error", {})
        except ValueError:
            err = {}
        after = _metric(self.metrics(), "llmgw_requests_total",
                        surface="images_generations", code="content_filtered")
        ok = (r.status_code == 400 and err.get("code") == "moderation_blocked"
              and isinstance(err.get("moderation_details"), dict)
              and after - before == 1.0
              and _metric(self.metrics(), "llmgw_breaker_state", state="open") == 0.0)
        self._verdict(
            "moderation-refusal", ok,
            f"{r.status_code} code={err.get('code')} "
            f"stage={(err.get('moderation_details') or {}).get('moderation_stage')} "
            f"type={err.get('type')} classified=content_filtered "
            f"(+{after - before:g}) breakers_open=0")

    # -------------------------------------------------------------- images

    def shoot(self, shot: Shot) -> None:
        if not _has("OPENAI_API_KEY"):
            _print(f"  {shot.key}: SKIP (no OPENAI_API_KEY)", self.out)
            return
        body = {"model": shot.model, "prompt": shot.prompt, "size": shot.size,
                "quality": shot.quality, "n": 1}
        if shot.stream:
            body["stream"] = True
            body["partial_images"] = shot.partial_images
        result = ShotResult(shot=shot)
        t0 = time.perf_counter()
        if shot.stream:
            ok, detail, raw = self._stream(shot, body, result)
        else:
            ok, detail, raw = self._buffered(shot, body)
        result.latency = time.perf_counter() - t0
        if raw:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            path = OUT_DIR / f"{shot.key}.png"
            path.write_bytes(raw)
            result.path = path
            result.png_bytes = len(raw)
            result.dims = png_dims(raw)
            want = tuple(int(x) for x in shot.size.split("x"))
            if result.dims != want:
                ok = False
                detail += f" DIMS {result.dims} != requested {want}"
        else:
            ok = False
        self.results.append(result)
        self._verdict(shot.key, ok, f"{detail} {result.latency:.2f}s")

    def _buffered(self, shot: Shot, body: dict) -> tuple[bool, str, bytes]:
        r = httpx.post(f"{self.gw.base_url}{ROUTE}", json=body, timeout=300)
        if r.status_code != 200:
            return False, f"{r.status_code} {r.text[:160]!r}", b""
        payload = r.json()
        head_ok, head = _gw(r.headers, shot.model)
        usage = payload.get("usage") or {}
        self.spend += self._price(shot.model, usage)
        data = payload.get("data") or []
        raw = b""
        if data and isinstance(data[0], dict) and isinstance(data[0].get("b64_json"), str):
            raw = base64.b64decode(data[0]["b64_json"])
        # The response echoes what it drew; the assertion is that it echoes
        # what we ASKED for, because a silently downgraded size is a silently
        # wrong bill.
        echo_ok = payload.get("size") == shot.size and payload.get("quality") == shot.quality
        ok = head_ok and echo_ok and bool(raw) and len(data) == 1
        return ok, (f"200 images={len(data)} json={len(r.content)}B {head} "
                    f"usage_in={usage.get('input_tokens')} "
                    f"out={usage.get('output_tokens')} "
                    f"echo={payload.get('size')}/{payload.get('quality')}"), raw

    def _stream(self, shot: Shot, body: dict, result: ShotResult,
                ) -> tuple[bool, str, bytes]:
        """The SSE form. Every frame's `event:` name is recorded, because the
        sequence IS the contract: N partials, then `image_generation.
        completed`, and NO `data: [DONE]` -- which is why the surface has to
        call the completed event terminal."""
        final = b""
        usage: dict = {}
        partial_bytes: list[int] = []
        head = "no headers"
        head_ok = False
        status = 0
        biggest = 0
        first = None
        t0 = time.perf_counter()
        with httpx.stream("POST", f"{self.gw.base_url}{ROUTE}", json=body,
                          timeout=300) as r:
            status = r.status_code
            head_ok, head = _gw(r.headers, shot.model)
            buf = b""
            for chunk in r.iter_raw():
                if first is None:
                    first = time.perf_counter() - t0
                buf += chunk
                while b"\n\n" in buf:
                    frame, buf = buf.split(b"\n\n", 1)
                    biggest = max(biggest, len(frame))
                    name, data = None, None
                    for line in frame.split(b"\n"):
                        if line.startswith(b"event:"):
                            name = line[6:].strip().decode("utf-8", "replace")
                        elif line.startswith(b"data:"):
                            data = line[5:].strip()
                    if name:
                        result.events.append(name)
                    if data in (None, b"[DONE]"):
                        if data == b"[DONE]":
                            result.events.append("[DONE]")
                        continue
                    try:
                        payload = json.loads(data)
                    except ValueError:
                        continue
                    kind = name or payload.get("type")
                    b64 = payload.get("b64_json")
                    if kind == PARTIAL_IMAGE and isinstance(b64, str):
                        partial_bytes.append(len(base64.b64decode(b64)))
                    elif kind == COMPLETED:
                        if isinstance(b64, str):
                            final = base64.b64decode(b64)
                        usage = payload.get("usage") or {}
        self.spend += self._price(shot.model, usage)
        want = ([PARTIAL_IMAGE] * shot.partial_images) + [COMPLETED]
        # No `[DONE]`: the gateway must never invent an ending the provider
        # does not send (C2), and OpenAI does not send one here.
        ok = (status == 200 and head_ok and result.events == want
              and "[DONE]" not in result.events and bool(final))
        return ok, (f"{status} events={'->'.join(result.events)} "
                    f"partials={partial_bytes} biggest_frame={biggest}B "
                    f"ttfb={first:.2f}s {head} "
                    f"usage_in={usage.get('input_tokens')} "
                    f"out={usage.get('output_tokens')} (partials are billed: "
                    f"{shot.partial_images} x ~100 output tokens)"), final

    @staticmethod
    def _price(model: str, usage: dict) -> float:
        spec = DEFAULT_CATALOG.models.get(model)
        if spec is None or not isinstance(usage, dict):
            return 0.0
        tin = usage.get("input_tokens") or 0
        tout = usage.get("output_tokens") or 0
        if not isinstance(tin, int) or not isinstance(tout, int):
            return 0.0
        return (tin * spec.input_per_m + tout * spec.output_per_m) / 1e6

    # --------------------------------------------------------- the pictures

    def gallery(self, out) -> None:
        """The five, as a table a human can check against the files on disk.

        Dimensions are read out of each PNG's IHDR, not copied from the
        request: the whole point of saving the bytes is that they are the
        only evidence about what arrived.
        """
        if not self.results:
            return
        _print("\nTHE FIVE IMAGES (dimensions read from each PNG's IHDR)", out)
        _print(f"  {'file':<26} {'bytes':>9} {'dims':<11} {'model':<24} "
               f"{'latency':>8} {'billed':>10} basis", out)
        for r in self.results:
            _print(f"  {(r.path.name if r.path else '-'):<26} {r.png_bytes:>9} "
                   f"{f'{r.dims[0]}x{r.dims[1]}':<11} {r.shot.model:<24} "
                   f"{r.latency:>7.2f}s {r.billed:>10.6f} {r.basis} "
                   f"[{r.tokens}]", out)
            _print(f"      prompt: {r.shot.prompt}", out)
        if self.results and self.results[0].path:
            _print(f"  saved under {OUT_DIR}", out)


def ledger(capture_path: str, case: Case, out) -> int:
    """The gateway's OWN answer to "what did that cost, and how sure is it".

    Read from the capture sink rather than from `/metrics`, because `basis`
    and `cost_notes` are per-RECORD facts that no counter can carry: a
    dashboard that shows a dollar figure without them shows an estimate and a
    measurement as the same number.

    On this surface every row should read `exact`. That is the billing
    decision in one column: OpenAI's image endpoint states the whole bill in
    one `usage` block on both the buffered and the streamed form, so there is
    nothing to estimate and nothing to explain. An `estimated` row here with
    no `cost_notes` line is a FAILURE -- the same check `live/smoke_voice.py`
    runs, where every row is estimated and every one of them has to say why.
    """
    # The capture worker is asynchronous by design, so the last record can
    # still be in flight when the last case returns.
    time.sleep(1.0)
    path = Path(capture_path)
    if not path.is_file():
        _print("\nMETERING LEDGER: (no capture records)", out)
        return 0
    unexplained: list[str] = []
    billed: list[dict] = []
    _print("\nMETERING LEDGER (from the gateway's own capture records)", out)
    _print(f"  {'provider':<10} {'model':<26} {'tokens':<26} {'units':<10} "
           f"{'basis':<10} cost_usd", out)
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if not rec.get("cost_usd") and not (rec.get("units") or {}).get("images"):
            continue
        tokens = ",".join(f"{k}={v:g}" for k, v in (rec.get("tokens") or {}).items() if v)
        units = ",".join(f"{k}={v:g}" for k, v in (rec.get("units") or {}).items() if v)
        _print(f"  {str(rec.get('provider'))[:10]:<10} "
               f"{str(rec.get('model') or '-')[:26]:<26} {tokens[:26]:<26} "
               f"{units[:10]:<10} {str(rec.get('basis')):<10} "
               f"{rec.get('cost_usd', 0.0):.8f}", out)
        for note in (rec.get("cost_notes") or ()):
            _print(f"      note: {note}", out)
        billed.append(rec)
        if rec.get("basis") == "estimated" and rec.get("cost_usd") and not (
                rec.get("cost_notes") or ()):
            unexplained.append(f"{rec.get('model')} ${rec.get('cost_usd', 0.0):.8f}")
    # Attach each record to the shot that produced it, in order: the ledger
    # is the gateway's number and the gallery is the client's, and a table
    # that shows one without the other cannot be reconciled.
    for result, rec in zip(case.results, billed, strict=False):
        result.billed = float(rec.get("cost_usd") or 0.0)
        result.basis = str(rec.get("basis"))
        result.tokens = ",".join(
            f"{k}={v:g}" for k, v in (rec.get("tokens") or {}).items() if v)
    if unexplained:
        _print(f"  ledger-estimated-notes: {len(unexplained)} estimated record(s) "
               f"with NO cost_notes: {unexplained} -> FAIL", out)
    else:
        _print("  ledger-estimated-notes: every estimated record explains its "
               "number -> PASS", out)
    exact = sum(1 for r in billed if r.get("basis") == "exact")
    if exact == len(billed) and billed:
        _print(f"  ledger-all-exact: {exact}/{len(billed)} image records billed "
               f"EXACT from the provider's own usage block -> PASS", out)
    else:
        _print(f"  ledger-all-exact: only {exact}/{len(billed)} records are exact; "
               f"this endpoint always reports usage -> FAIL", out)
        unexplained.append("not every image record was exact")
    return len(unexplained)


def build_images_app(policy_path: str, capture_path: str | None = None):
    settings = {
        "catalog": DEFAULT_CATALOG,
        "fake_upstreams": False,
        "policy_file": policy_path,
        "default_model": MODEL,
        "forward_request_headers": ("x-request-id",),
        # Capture is ON here because `basis` and `cost_notes` live on the
        # RECORD and nowhere else: a metric can say how many tokens were
        # billed but not whether anyone measured them.
        "capture_path": capture_path,
    }
    return build_app(ServerConfig(**settings).validated())


def run(*, spend: bool = True, out=sys.stdout) -> float:
    with tempfile.TemporaryDirectory(prefix="llmgw-images-") as tmp:
        policy = Path(tmp) / "images.toml"
        policy.write_text(POLICY_TOML, encoding="utf-8")
        capture = Path(tmp) / "capture.jsonl"
        gw = text_smoke.serve(build_images_app(str(policy), str(capture)))
        try:
            _print(f"images gateway on {gw.base_url}", out)
            _print(f"  OPENAI_API_KEY: "
                   f"{'set' if _has('OPENAI_API_KEY') else 'ABSENT (cases skip)'}", out)
            case = Case(gw, out)
            case.catalog_rows()
            case.route_mounted()
            if not spend:
                _print("  --no-spend: catalog and routing only. The five "
                       "generations and the moderation refusal are skipped; "
                       "the refusal costs nothing upstream but it is still a "
                       "real call, so it stays behind the flag.", out)
                _print(f"\nFAILURES: {case.failures}", out)
                _print("ESTIMATED SPEND: $0.000000", out)
                return 0.0
            case.moderation_refusal()
            for shot in SHOTS:
                case.shoot(shot)
            unexplained = ledger(str(capture), case, out)
            case.gallery(out)
            _print(f"\nFAILURES: {case.failures + unexplained}", out)
            _print(f"ESTIMATED SPEND: ${case.spend:.6f}", out)
            return case.spend
        finally:
            gw.stop()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="live image smoke through the gateway")
    ap.add_argument("--no-spend", action="store_true")
    args = ap.parse_args(argv)
    if os.environ.get("LLMGW_ENV_FILE"):
        load_env()  # names only are returned; values never printed
    run(spend=not args.no_spend)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

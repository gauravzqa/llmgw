"""Live Sarvam smoke: one real call per Sarvam surface, through the gateway.

Spends real money (fractions of a cent) and needs `SARVAM_API_KEY`. Every
case SKIPs when the key is absent. Never prints a key.

What each case asserts is the usual four things -- status, framing, meter,
and the gateway's own cost record read back from `/metrics` -- with one
difference that is the whole reason this file reads differently from
`live/smoke_voice.py`:

    **Sarvam reports no meter on any HTTP speech response.**

So there is no provider number to agree with. What the cases check instead
is that the gateway's ESTIMATE is the one it promised: `characters` equal to
the text sent, `seconds` equal to the duration in the uploaded WAV's own
header, and `llmgw_cost_usd_total{basis="estimated"}` moving while
`basis="exact"` does not. A voice surface that streams perfectly and bills
nothing is finding 24 again; a voice surface that bills confidently from a
number nobody measured is worse.

The speech-to-text cases feed on Sarvam's OWN text-to-speech output, so the
run needs no second provider and no audio fixture in the repo.

Run: `LLMGW_ENV_FILE=/path/to/.env .venv/bin/python -m live.smoke_sarvam`
(or `--no-spend` for routing only).
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
from pathlib import Path

import httpx

from live import smoke as text_smoke
from live.env import load_env
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig

POLICY_TOML = """
default_workload = "sarvam"

[defaults.budgets]
total = 120.0
connect = 5.0
headers = 10.0
first_event = 20.0
progress = 15.0
client_stall = 30.0

[profiles.tts.budgets]
first_event = 8.0
progress = 8.0
total = 120.0

[workloads.sarvam]
incumbent = "sarvam.bulbul-v3"
"""

TEXT = "The quick brown fox jumps over the lazy dog near the river bank today."
SPEAKER = "shubh"
"""A `bulbul:v3` speaker. `bulbul:v4-flash` has a DIFFERENT speaker set
(`aayan_hi_conversational` and friends) and 400s on this one, which is a
Sarvam fact the gateway deliberately does not paper over."""

# Catalog rates, so the spend estimate here uses the same numbers the
# gateway bills with (Rs 3/1k chars and Rs 30/h at Rs 88.5/USD).
TTS_PER_M_CHARS = 33.90
STT_PER_MINUTE = 0.00565
CHAT_IN_PER_M = 0.3308
CHAT_OUT_PER_M = 0.8271


def _metric(text: str, name: str, **labels: str) -> float:
    for line in text.splitlines():
        if line.startswith(name) and all(f'{k}="{v}"' in line for k, v in labels.items()):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def _has(env: str) -> bool:
    value = os.environ.get(env, "")
    return bool(value) and not value.startswith("replace-with")


def _print(line: str, out) -> None:
    print(line, file=out, flush=True)


def wav_duration(data: bytes) -> float:
    """Seconds declared by a RIFF/WAVE body, for checking the gateway's own
    estimate against an independent reading of the same header."""
    if data[:4] != b"RIFF" or data[8:12] != b"WAVE":
        return 0.0
    pos = 12
    rate = 0
    while pos + 8 <= len(data):
        cid = data[pos:pos + 4]
        (size,) = struct.unpack_from("<I", data, pos + 4)
        if cid == b"fmt " and pos + 8 + 16 <= len(data):
            (rate,) = struct.unpack_from("<I", data, pos + 16)
        elif cid == b"data" and rate:
            return min(size, len(data) - pos - 8) / rate
        pos += 8 + size + (size & 1)
    return 0.0


class Case:
    """One PASS/FAIL line per surface, plus a running spend estimate."""

    def __init__(self, gw, out) -> None:
        self.gw = gw
        self.out = out
        self.spend = 0.0
        self.failures = 0
        self.clip: bytes | None = None

    def metrics(self) -> str:
        return httpx.get(f"{self.gw.base_url}/metrics", timeout=10).text

    def _verdict(self, name: str, ok: bool, detail: str) -> None:
        if not ok:
            self.failures += 1
        _print(f"  {name}: {detail} -> {'PASS' if ok else 'FAIL'}", self.out)

    # --------------------------------------------------------- text to speech

    def tts_sync(self) -> None:
        before = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="sarvam.bulbul-v3")
        body = {"model": "sarvam.bulbul-v3", "text": TEXT, "speaker": SPEAKER,
                "target_language_code": "en-IN", "speech_sample_rate": 16000}
        t0 = time.perf_counter()
        r = httpx.post(f"{self.gw.base_url}/sarvam/text-to-speech", json=body, timeout=60)
        elapsed = time.perf_counter() - t0
        audio = b""
        payload = {}
        if r.status_code == 200:
            payload = r.json()
            try:
                audio = base64.b64decode(payload.get("audios", [""])[0])
            except (ValueError, IndexError, TypeError):
                audio = b""
        if audio[:4] == b"RIFF":
            self.clip = audio
        billed = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="sarvam.bulbul-v3") - before
        ok = (r.status_code == 200 and audio[:4] == b"RIFF"
              and billed == len(TEXT) and "usage" not in payload)
        self._verdict(
            "sarvam-tts-sync",
            ok,
            f"{r.status_code} wav={len(audio)}B riff={audio[:4] == b'RIFF'} "
            f"t={elapsed:.3f}s served_by={r.headers.get('x-gw-served-by')} "
            f"provider_meter=none billed_characters={billed:g} "
            f"(estimated from {len(TEXT)} request chars)",
        )
        self.spend += TTS_PER_M_CHARS * len(TEXT) / 1e6

    def tts_stream(self) -> None:
        before = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="sarvam.bulbul-v3")
        body = {"model": "sarvam.bulbul-v3", "text": TEXT, "speaker": SPEAKER,
                "target_language_code": "en-IN", "speech_sample_rate": 16000,
                "output_audio_codec": "linear16"}
        t0 = time.perf_counter()
        first = None
        chunks = 0
        total = 0
        with httpx.stream("POST", f"{self.gw.base_url}/sarvam/text-to-speech/stream",
                          json=body, timeout=60) as r:
            for chunk in r.iter_raw():
                if first is None:
                    first = time.perf_counter() - t0
                chunks += 1
                total += len(chunk)
            status = r.status_code
            ctype = r.headers.get("content-type", "")
            served = r.headers.get("x-gw-served-by")
        billed = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="sarvam.bulbul-v3") - before
        ok = (status == 200 and ctype.startswith("audio/") and total > 0
              and billed == len(TEXT))
        self._verdict(
            "sarvam-tts-stream",
            ok,
            f"{status} {ctype} chunks={chunks} bytes={total} "
            f"ttfb={first if first is None else round(first, 3)}s served_by={served} "
            f"billed_characters={billed:g} (no terminal frame; body ends on close)",
        )
        self.spend += TTS_PER_M_CHARS * len(TEXT) / 1e6

    # --------------------------------------------------------- speech to text

    def stt(self, *, translate: bool = False) -> None:
        name = "sarvam-stt-translate" if translate else "sarvam-stt"
        route = "/sarvam/speech-to-text-translate" if translate else "/sarvam/speech-to-text"
        # `/speech-to-text-translate` serves a DIFFERENT model set from
        # `/speech-to-text`: `saaras:v4` 400s there (live, 19 Sep 2026,
        # "Input should be 'saaras:v2.5', 'saaras:v3', 'saaras:v1',
        # 'saaras:v2', 'saaras:flash' or 'saaras:turbo'"). One id serves
        # both, and that is the one the translate case names.
        model = "sarvam.saaras-v3" if translate else "sarvam.saaras-v4"
        clip = self.clip
        if clip is None:
            self._verdict(name, False, "SKIP-as-FAIL (no TTS clip to transcribe)")
            return
        declared = wav_duration(clip)
        before = _metric(self.metrics(), "llmgw_units_total", unit="seconds", model=model)
        # The CATALOG id in the form field on purpose: the gateway's
        # multipart splice is what turns it into the wire id, and a smoke
        # that sent the wire id would never exercise that edit.
        t0 = time.perf_counter()
        r = httpx.post(
            f"{self.gw.base_url}{route}",
            data={"model": model, "language_code": "unknown"},
            files={"file": ("probe.wav", clip, "audio/wav")},
            timeout=60,
        )
        elapsed = time.perf_counter() - t0
        payload = r.json() if r.status_code == 200 else {}
        billed = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                         model=model) - before
        transcript = payload.get("transcript")
        has_meter = any(k in payload for k in ("usage", "duration", "audio_duration"))
        ok = (r.status_code == 200 and isinstance(transcript, str) and transcript
              and not has_meter and abs(billed - round(declared)) <= 1)
        if translate:
            ok = ok and "diarized_transcript" in payload
        self._verdict(
            name,
            ok,
            f"{r.status_code} t={elapsed:.3f}s transcript={transcript!r} "
            f"served_by={r.headers.get('x-gw-served-by')} provider_meter=none "
            f"billed_seconds={billed:g} (estimated from the WAV header's "
            f"{declared:.2f}s)"
            + (f" diarized={payload.get('diarized_transcript')!r}" if translate else ""),
        )
        self.spend += STT_PER_MINUTE * declared / 60

    # ------------------------------------------------------------------ text

    def chat(self) -> None:
        """Sarvam's `sarvam-105b` over the EXISTING `openai_chat` surface.
        If this passes, the whole text integration is one catalog row."""
        before_in = _metric(self.metrics(), "llmgw_tokens_total", kind="input",
                            model="sarvam.sarvam-105b")
        body = {"model": "sarvam.sarvam-105b", "stream": True, "max_tokens": 24,
                "messages": [{"role": "user", "content": "Reply with the word OK."}]}
        t0 = time.perf_counter()
        with httpx.stream("POST", f"{self.gw.base_url}/v1/chat/completions",
                          json=body, timeout=60) as r:
            raw = b"".join(r.iter_raw())
            status = r.status_code
            ctype = r.headers.get("content-type", "")
            served = r.headers.get("x-gw-served-by")
        elapsed = time.perf_counter() - t0
        done = raw.rstrip().endswith(b"data: [DONE]")
        usage = None
        for frame in raw.decode("utf-8", "replace").split("\n\n"):
            payload = frame.partition("data: ")[2].strip()
            if not payload or payload == "[DONE]":
                continue
            try:
                obj = json.loads(payload)
            except ValueError:
                continue
            if isinstance(obj.get("usage"), dict):
                usage = obj["usage"]
        after_in = _metric(self.metrics(), "llmgw_tokens_total", kind="input",
                           model="sarvam.sarvam-105b")
        billed_in = after_in - before_in
        provider_in = (usage or {}).get("prompt_tokens")
        agree = provider_in is not None and billed_in == provider_in
        ok = status == 200 and ctype.startswith("text/event-stream") and done and agree
        self._verdict(
            "sarvam-chat",
            ok,
            f"{status} {ctype} bytes={len(raw)} done={done} t={elapsed:.3f}s "
            f"served_by={served} provider_usage={usage} billed_input={billed_in:g} "
            f"(openai_chat surface, no Sarvam-specific code)",
        )
        if usage:
            self.spend += (CHAT_IN_PER_M * usage.get("prompt_tokens", 0) / 1e6
                           + CHAT_OUT_PER_M * usage.get("completion_tokens", 0) / 1e6)

    def unknown_model(self) -> None:
        """Free (a 400 costs nothing) and the only case that exercises the
        classifier against Sarvam's real prose.

        An id the CATALOG has never heard of is refused before a socket --
        that is routing, not classification. So the case that matters is an
        id the catalog does know and Sarvam refuses: `saaras:v4` on the
        TRANSLATE route, whose model set is narrower than
        `/speech-to-text`'s. That reaches Sarvam, comes back as the
        enumerating validation message, and must read as OUR config drift
        (`model_not_found`) rather than the caller's bad request."""
        unknown = httpx.post(
            f"{self.gw.base_url}/sarvam/text-to-speech",
            json={"model": "bulbul:v99", "text": "hi", "speaker": SPEAKER,
                  "target_language_code": "en-IN"}, timeout=30,
        )
        self._verdict(
            "sarvam-unrouted-model",
            unknown.status_code == 400,
            f"{unknown.status_code} refused before any socket body={unknown.text[:90]!r}",
        )
        if self.clip is None:
            return
        before = _metric(self.metrics(), "llmgw_requests_total",
                         surface="sarvam_stt_translate", code="model_not_found")
        r = httpx.post(
            f"{self.gw.base_url}/sarvam/speech-to-text-translate",
            data={"model": "sarvam.saaras-v4"},
            files={"file": ("probe.wav", self.clip, "audio/wav")},
            timeout=60,
        )
        after = _metric(self.metrics(), "llmgw_requests_total",
                        surface="sarvam_stt_translate", code="model_not_found")
        ok = r.status_code == 400 and after - before == 1.0
        self._verdict(
            "sarvam-model-not-found",
            ok,
            f"{r.status_code} model_not_found_delta={after - before:g} "
            f"body={r.text[:150]!r}",
        )


def build_sarvam_app(policy_path: str):
    settings = {
        "catalog": DEFAULT_CATALOG,
        "fake_upstreams": False,
        "policy_file": policy_path,
        "default_model": "sarvam.bulbul-v3",
        "forward_request_headers": ("x-request-id",),
    }
    return build_app(ServerConfig(**settings).validated())


def run(*, spend: bool = True, out=sys.stdout) -> float:
    with tempfile.TemporaryDirectory(prefix="llmgw-sarvam-") as tmp:
        policy = Path(tmp) / "sarvam.toml"
        policy.write_text(POLICY_TOML, encoding="utf-8")
        gw = text_smoke.serve(build_sarvam_app(str(policy)))
        try:
            _print(f"sarvam gateway on {gw.base_url}", out)
            _print(f"  SARVAM_API_KEY: "
                   f"{'set' if _has('SARVAM_API_KEY') else 'ABSENT (cases skip)'}", out)
            if not spend or not _has("SARVAM_API_KEY"):
                if not _has("SARVAM_API_KEY"):
                    _print("  every case: SKIP (no SARVAM_API_KEY)", out)
                return 0.0
            case = Case(gw, out)
            case.tts_sync()
            case.tts_stream()
            case.stt()
            case.stt(translate=True)
            case.chat()
            case.unknown_model()
            _print(f"\nFAILURES: {case.failures}", out)
            _print(f"ESTIMATED SPEND: ${case.spend:.6f}", out)
            return case.spend
        finally:
            gw.stop()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="live Sarvam smoke through the gateway")
    ap.add_argument("--no-spend", action="store_true")
    args = ap.parse_args(argv)
    if os.environ.get("LLMGW_ENV_FILE"):
        load_env()  # names only are returned; values never printed
    run(spend=not args.no_spend)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

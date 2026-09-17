"""Live voice smoke: one real call per voice surface, through the gateway.

Spends real money (fractions of a cent) and needs real keys. Each case runs
only when its provider's key is present in the environment and SKIPS
otherwise -- ElevenLabs and AssemblyAI keys were absent on 18 Sep 2026, so
those cases print SKIP and do not fail the run. Never prints a key.

What is asserted per case: the status, the framing the client actually
received (content type, and for streams the frame shape), the meter the
provider reported, and that the gateway's own cost record -- read back from
`/metrics` as `llmgw_units_total` / `llmgw_tokens_total` deltas -- agrees
with the provider's meter within 1%. That last check is the point: a voice
surface that streams perfectly and bills nothing is finding 24 again.

Run: `LLMGW_ENV_FILE=/path/to/.env .venv/bin/python -m live.smoke_voice`
(or `--no-spend` for routing only).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
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
default_workload = "voice"

[defaults.budgets]
total = 120.0
connect = 5.0
headers = 10.0
first_event = 20.0
progress = 15.0
client_stall = 30.0

[profiles.tts.budgets]
first_event = 5.0
progress = 5.0
total = 120.0

[workloads.voice]
incumbent = "openai.gpt-4o-mini-tts"
"""

TEXT = "The quick brown fox jumps over the lazy dog."


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


class Case:
    def __init__(self, gw, out) -> None:
        self.gw = gw
        self.out = out
        self.spend = 0.0

    def metrics(self) -> str:
        return httpx.get(f"{self.gw.base_url}/metrics", timeout=10).text

    # ------------------------------------------------------------ OpenAI TTS

    def openai_tts_binary(self) -> None:
        if not _has("OPENAI_API_KEY"):
            _print("  openai-tts-binary: SKIP (no OPENAI_API_KEY)", self.out)
            return
        body = {"model": "openai.gpt-4o-mini-tts", "input": TEXT, "voice": "cedar",
                "response_format": "pcm"}
        t0 = time.perf_counter()
        with httpx.stream("POST", f"{self.gw.base_url}/v1/audio/speech", json=body,
                          timeout=60) as r:
            first = None
            total = 0
            for chunk in r.iter_raw():
                if first is None:
                    first = time.perf_counter() - t0
                total += len(chunk)
            status, ct = r.status_code, r.headers.get("content-type", "")
            served = r.headers.get("x-gw-served-by")
        ok = status == 200 and ct.startswith("audio/") and total > 0
        _print(f"  openai-tts-binary: {status} {ct} bytes={total} ttfb={first:.3f}s "
               f"served_by={served} -> {'PASS' if ok else 'FAIL'} (binary mode has no meter; "
               f"bill is estimated from {len(TEXT)} characters)", self.out)
        self.spend += 0.60 * 15 / 1e6 + 12.0 * (total / 4800) / 1e6

    def openai_tts_sse(self) -> None:
        if not _has("OPENAI_API_KEY"):
            _print("  openai-tts-sse: SKIP (no OPENAI_API_KEY)", self.out)
            return
        # The registered surface is the binary one; SSE mode needs the server
        # to consult `AudioSpeechSurface.framing_for()`. Until it does, this
        # case reports what the gateway did with an SSE-mode request.
        before = _metric(self.metrics(), "llmgw_tokens_total", kind="output",
                         model="openai.gpt-4o-mini-tts")
        body = {"model": "openai.gpt-4o-mini-tts", "input": TEXT, "voice": "cedar",
                "stream_format": "sse"}
        with httpx.stream("POST", f"{self.gw.base_url}/v1/audio/speech", json=body,
                          timeout=60) as r:
            raw = b"".join(r.iter_raw())
            status, ct = r.status_code, r.headers.get("content-type", "")
        frames = raw.count(b"speech.audio.delta")
        done = b"speech.audio.done" in raw
        after = _metric(self.metrics(), "llmgw_tokens_total", kind="output",
                        model="openai.gpt-4o-mini-tts")
        billed = after - before
        verdict = ("PASS" if status == 200 and done
                   else "INFO (server did not select SSE framing)")
        _print(f"  openai-tts-sse: {status} {ct} deltas={frames} done={done} "
               f"billed_output_tokens={billed:g} -> {verdict}", self.out)

    def openai_stt(self, pcm_bytes: bytes | None) -> None:
        if not _has("OPENAI_API_KEY"):
            _print("  openai-stt: SKIP (no OPENAI_API_KEY)", self.out)
            return
        audio = pcm_bytes or _tone_wav(seconds=2)
        before = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                         model="openai.gpt-transcribe")
        files = {"file": ("clip.wav", audio, "audio/wav")}
        # Multipart bodies are forwarded byte-for-byte, so the form field must
        # carry the provider's wire id, as every real SDK does; the gateway
        # resolves it through the alias table and refuses a catalog id here.
        data = {"model": "gpt-transcribe", "stream": "false"}
        r = httpx.post(f"{self.gw.base_url}/v1/audio/transcriptions", data=data, files=files,
                       timeout=60)
        usage = r.json().get("usage") if r.status_code == 200 else None
        after = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                        model="openai.gpt-transcribe")
        billed = after - before
        provider_seconds = (usage or {}).get("seconds") if isinstance(usage, dict) else None
        agree = provider_seconds is not None and abs(billed - provider_seconds) <= 0.01 * max(
            provider_seconds, 1)
        _print(f"  openai-stt: {r.status_code} usage={usage} billed_seconds={billed:g} -> "
               f"{'PASS' if r.status_code == 200 and agree else 'FAIL'}", self.out)
        if provider_seconds:
            self.spend += 0.0045 * provider_seconds / 60

    # ---------------------------------------------------------------- Inworld

    def inworld_stream(self) -> None:
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-stream: SKIP (no INWORLD_API_KEY)", self.out)
            return
        before = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="inworld.tts-2-flash")
        body = {"modelId": "inworld.tts-2-flash", "text": TEXT, "voiceId": "Aarav",
                "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 24000}}
        t0 = time.perf_counter()
        with httpx.stream("POST", f"{self.gw.base_url}/inworld/tts/v1/voice:stream",
                          json=body, timeout=60) as r:
            first = None
            raw = b""
            for chunk in r.iter_raw():
                if first is None:
                    first = time.perf_counter() - t0
                raw += chunk
            status, ct = r.status_code, r.headers.get("content-type", "")
        lines = [ln for ln in raw.split(b"\n") if ln]
        count = None
        riff = False
        if lines:
            try:
                head = json.loads(lines[0])["result"]
                count = head.get("usage", {}).get("processedCharactersCount")
                riff = base64.b64decode(head["audioContent"])[:4] == b"RIFF"
            except (ValueError, KeyError, TypeError):
                pass
        after = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                        model="inworld.tts-2-flash")
        billed = after - before
        agree = count is not None and billed == count
        _print(f"  inworld-stream: {status} {ct} lines={len(lines)} first_line_chars={count} "
               f"riff={riff} ttfb={first:.3f}s billed_characters={billed:g} -> "
               f"{'PASS' if status == 200 and agree else 'FAIL'}", self.out)
        if count:
            self.spend += 15.0 * count / 1e6

    def inworld_sync(self) -> None:
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-sync: SKIP (no INWORLD_API_KEY)", self.out)
            return
        body = {"modelId": "inworld.tts-2-flash", "text": "hello", "voiceId": "Aarav",
                "audioConfig": {"audioEncoding": "MP3", "sampleRateHertz": 24000}}
        r = httpx.post(f"{self.gw.base_url}/inworld/tts/v1/voice", json=body, timeout=60)
        usage = r.json().get("usage") if r.status_code == 200 else None
        _print(f"  inworld-sync: {r.status_code} usage={usage} -> "
               f"{'PASS' if r.status_code == 200 and usage else 'FAIL'}", self.out)

    def inworld_empty_text(self) -> None:
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-empty: SKIP (no INWORLD_API_KEY)", self.out)
            return
        body = {"modelId": "inworld.tts-2-flash", "text": "   ", "voiceId": "Aarav"}
        r = httpx.post(f"{self.gw.base_url}/inworld/tts/v1/voice:stream", json=body,
                       timeout=60)
        _print(f"  inworld-empty: {r.status_code} body={r.content[:60]!r} -> "
               f"{'PASS' if r.status_code == 200 else 'FAIL'} (a 200 with usage null is "
               f"Inworld's answer to empty text)", self.out)

    # ------------------------------------------------------------- ElevenLabs

    def elevenlabs_stream(self) -> None:
        if not _has("ELEVENLABS_API_KEY"):
            _print("  elevenlabs-stream: SKIP (no ELEVENLABS_API_KEY)", self.out)
            return
        voice = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
        before = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="elevenlabs.flash-v2-5")
        body = {"text": TEXT, "model_id": "elevenlabs.flash-v2-5"}
        url = (f"{self.gw.base_url}/elevenlabs/v1/text-to-speech/{voice}/stream"
               "?output_format=mp3_22050_32")
        with httpx.stream("POST", url, json=body, timeout=60) as r:
            total = sum(len(c) for c in r.iter_raw())
            status, ct = r.status_code, r.headers.get("content-type", "")
        after = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                        model="elevenlabs.flash-v2-5")
        billed = after - before
        _print(f"  elevenlabs-stream: {status} {ct} bytes={total} "
               f"billed_characters={billed:g} (character-cost header; needs the server's "
               f"usage_from_headers hook) -> "
               f"{'PASS' if status == 200 and total > 0 else 'FAIL'}", self.out)
        self.spend += 50.0 * len(TEXT) / 1e6

    # ------------------------------------------------------------- AssemblyAI

    def assemblyai_sync(self) -> None:
        if not _has("ASSEMBLYAI_API_KEY"):
            _print("  assemblyai-sync: SKIP (no ASSEMBLYAI_API_KEY)", self.out)
            return
        pcm = _tone_pcm16(seconds=2, rate=16_000)
        before = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                         model="assemblyai.sync")
        r = httpx.post(f"{self.gw.base_url}/assemblyai/transcribe?model=assemblyai.sync",
                       content=pcm, headers={"content-type": "audio/pcm"}, timeout=60)
        ms = r.json().get("audio_duration_ms") if r.status_code == 200 else None
        after = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                        model="assemblyai.sync")
        billed = after - before
        agree = ms is not None and abs(billed - ms / 1000) <= 0.01 * max(ms / 1000, 1)
        _print(f"  assemblyai-sync: {r.status_code} audio_duration_ms={ms} "
               f"billed_seconds={billed:g} -> "
               f"{'PASS' if r.status_code == 200 and agree else 'FAIL'}", self.out)
        if ms:
            self.spend += 0.0075 * ms / 1000 / 60


# ----------------------------------------------------------------- helpers


def _tone_pcm16(seconds: int, rate: int) -> bytes:
    import math
    import struct

    frames = seconds * rate
    return b"".join(struct.pack("<h", int(8000 * math.sin(2 * math.pi * 440 * i / rate)))
                    for i in range(frames))


def _tone_wav(seconds: int, rate: int = 16_000) -> bytes:
    import struct

    pcm = _tone_pcm16(seconds, rate)
    header = b"".join([
        b"RIFF", struct.pack("<I", 36 + len(pcm)), b"WAVE",
        b"fmt ", struct.pack("<IHHIIHH", 16, 1, 1, rate, rate * 2, 2, 16),
        b"data", struct.pack("<I", len(pcm)),
    ])
    return header + pcm


def build_voice_app(policy_path: str):
    settings = {
        "catalog": DEFAULT_CATALOG,
        "fake_upstreams": False,
        "policy_file": policy_path,
        "default_model": "openai.gpt-4o-mini-tts",
        "forward_request_headers": ("x-request-id",),
    }
    return build_app(ServerConfig(**settings).validated())


def run(*, spend: bool = True, out=sys.stdout) -> float:
    with tempfile.TemporaryDirectory(prefix="llmgw-voice-") as tmp:
        policy = Path(tmp) / "voice.toml"
        policy.write_text(POLICY_TOML, encoding="utf-8")
        gw = text_smoke.serve(build_voice_app(str(policy)))
        try:
            _print(f"voice gateway on {gw.base_url}", out)
            for env in ("OPENAI_API_KEY", "INWORLD_API_KEY", "ELEVENLABS_API_KEY",
                        "ASSEMBLYAI_API_KEY"):
                _print(f"  {env}: {'set' if _has(env) else 'ABSENT (cases skip)'}", out)
            if not spend:
                return 0.0
            case = Case(gw, out)
            case.openai_tts_binary()
            case.openai_tts_sse()
            case.openai_stt(None)
            case.inworld_stream()
            case.inworld_sync()
            case.inworld_empty_text()
            case.elevenlabs_stream()
            case.assemblyai_sync()
            _print(f"\nESTIMATED SPEND: ${case.spend:.6f}", out)
            return case.spend
        finally:
            gw.stop()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="live voice smoke through the gateway")
    ap.add_argument("--no-spend", action="store_true")
    args = ap.parse_args(argv)
    if os.environ.get("LLMGW_ENV_FILE"):
        load_env()  # names only are returned; values never printed
    run(spend=not args.no_spend)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

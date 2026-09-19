"""Live voice smoke: one real call per voice surface, through the gateway.

Spends real money (fractions of a cent) and needs real keys. Each case runs
only when its provider's key is present in the environment and SKIPS
otherwise. Never prints a key.

The speech-to-text cases are fed by a text-to-speech case: one Inworld TTS
call through the gateway produces a 16 kHz mono WAV that AssemblyAI,
ElevenLabs Scribe and Inworld STT all transcribe. That keeps the run
self-contained -- no checked-in audio fixture to rot -- and it is a second
assertion for free, because three independent providers measure the same
file and must agree on its duration.

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
ROUNDTRIP = "The gateway carried this sentence."
"""The cross-provider round trip's sentence: synthesised once by Sarvam, read
back by all four transcribers. Short, unambiguous, no proper nouns and no
digits -- a disagreement about it is a real disagreement and not a style."""

SHORT = "Gateway check."
"""One short sentence for ElevenLabs: the free tier has 10,000 characters for
the month, and every TTS call spends from it."""

ELEVENLABS_DEFAULT_VOICE = "EXAVITQu4vr4xnSDxMaL"
"""Sarah, a PREMADE voice. The old default here (`21m00Tcm4TlvDq8ikWAM`) is a
LIBRARY voice, and on 19 Sep 2026 the free tier answered it with a 402,
`paid_plan_required`: free users may not use library voices via the API.
"""


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


def _gw(headers, model: str, *, rewritten: bool = True) -> tuple[bool, str]:
    """The three headers a mis-wired surface gets wrong, checked not printed.

    `x-gw-model` must be the CATALOG id: a surface that echoes the wire id
    here is one that billed against whatever row the provider happened to
    name. `x-gw-served-by` is `<provider>/<catalog id>`. `x-gw-body-modified`
    is the warrant for the model rewrite -- present exactly when the gateway
    edited the caller's bytes, which is every surface that carries the model
    IN the body and no surface that carries it in a header or the query.
    """
    served = headers.get("x-gw-served-by", "-")
    got = headers.get("x-gw-model", "-")
    mod = headers.get("x-gw-body-modified", "-")
    ok = got == model and served.endswith("/" + model)
    if rewritten is not None:
        ok = ok and (mod == "1") == bool(rewritten)
    return ok, f"served_by={served} x-gw-model={got} body-modified={mod}"


def _refused(r) -> tuple[bool, str]:
    """A model the catalog never heard of: refused with no upstream call.

    `x-gw-attempts: 0` and `x-gw-served-by: -` together are the proof that
    no socket was opened -- a 400 alone could also be the provider's."""
    attempts = r.headers.get("x-gw-attempts", "-")
    served = r.headers.get("x-gw-served-by", "-")
    ok = r.status_code == 400 and attempts == "0" and served == "-"
    return ok, (f"{r.status_code} attempts={attempts} served_by={served} "
                f"body={r.text[:70]!r}")


class Case:
    def __init__(self, gw, out) -> None:
        self.gw = gw
        self.out = out
        self.spend = 0.0
        self.failures = 0
        self._clip: bytes | None = None
        self._sarvam_clip: bytes | None = None

    def _verdict(self, name: str, ok: bool, detail: str) -> None:
        if not ok:
            self.failures += 1
        _print(f"  {name}: {detail} -> {'PASS' if ok else 'FAIL'}", self.out)

    def metrics(self) -> str:
        return httpx.get(f"{self.gw.base_url}/metrics", timeout=10).text

    # -------------------------------------------------- speech-to-text input

    def clip(self) -> bytes | None:
        """A real 16 kHz mono WAV, spoken by Inworld TTS through the gateway.

        Cached for the run: three transcription cases share one file, which
        is both cheaper and stronger evidence -- they must all report the
        same duration for it."""
        if self._clip is not None:
            return self._clip or None
        if not _has("INWORLD_API_KEY"):
            self._clip = b""
            return None
        body = {"modelId": "inworld.tts-2-flash", "text": TEXT, "voiceId": "Ashley",
                "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 16000}}
        r = httpx.post(f"{self.gw.base_url}/inworld/tts/v1/voice", json=body, timeout=60)
        if r.status_code != 200:
            _print(f"  stt-input: FAIL (inworld tts {r.status_code})", self.out)
            self._clip = b""
            return None
        try:
            audio = base64.b64decode(r.json()["audioContent"])
        except (ValueError, KeyError, TypeError):
            self._clip = b""
            return None
        self.spend += 15.0 * len(TEXT) / 1e6
        self._clip = audio
        seconds = max(0, len(audio) - 44) / 32_000
        _print(f"  stt-input: {len(audio)} B WAV, {seconds:.2f}s, spoken by "
               f"inworld.tts-2-flash through the gateway", self.out)
        return audio

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
            head_ok, head = _gw(r.headers, "openai.gpt-4o-mini-tts", rewritten=True)
        ok = status == 200 and ct.startswith("audio/") and total > 0 and head_ok
        self._verdict("openai-tts-binary", ok,
                      f"{status} {ct} bytes={total} ttfb={first:.3f}s {head} "
                      f"(binary mode has no meter; bill is ESTIMATED from "
                      f"{len(TEXT)} request characters)")
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
            head_ok, gw_head = _gw(r.headers, "inworld.tts-2-flash", rewritten=True)
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
        self._verdict("inworld-stream", status == 200 and agree and head_ok,
                      f"{status} {ct} lines={len(lines)} first_line_chars={count} "
                      f"riff={riff} ttfb={first:.3f}s billed_characters={billed:g} "
                      f"basis=exact {gw_head}")
        if count:
            self.spend += 15.0 * count / 1e6

    def inworld_sync(self) -> None:
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-sync: SKIP (no INWORLD_API_KEY)", self.out)
            return
        body = {"modelId": "inworld.tts-2-flash", "text": "hello", "voiceId": "Aarav",
                "audioConfig": {"audioEncoding": "MP3", "sampleRateHertz": 24000}}
        t0 = time.perf_counter()
        r = httpx.post(f"{self.gw.base_url}/inworld/tts/v1/voice", json=body, timeout=60)
        elapsed = time.perf_counter() - t0
        usage = r.json().get("usage") if r.status_code == 200 else None
        head_ok, head = _gw(r.headers, "inworld.tts-2-flash", rewritten=True)
        self._verdict("inworld-sync", r.status_code == 200 and bool(usage) and head_ok,
                      f"{r.status_code} t={elapsed:.3f}s usage={usage} {head}")

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

    def inworld_stt(self) -> None:
        """HTTP STT. The model is NESTED at `transcribeConfig.modelId`, which
        is the only place this API reads it; the bill is
        `usage.transcribedAudioMs`."""
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-stt: SKIP (no INWORLD_API_KEY)", self.out)
            return
        audio = self.clip()
        if audio is None:
            _print("  inworld-stt: SKIP (no speech-to-text input)", self.out)
            return
        before = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                         model="inworld.stt-1")
        body = {"transcribeConfig": {"modelId": "inworld.stt-1",
                                     "audioEncoding": "LINEAR16",
                                     "sampleRateHertz": 16000, "numberOfChannels": 1,
                                     "language": "en-US"},
                "audioData": {"content": base64.b64encode(audio).decode()}}
        t0 = time.perf_counter()
        r = httpx.post(f"{self.gw.base_url}/inworld/stt/v1/transcribe", json=body,
                       timeout=90)
        elapsed = time.perf_counter() - t0
        head_ok, head = _gw(r.headers, "inworld.stt-1", rewritten=True)
        payload = r.json() if r.status_code == 200 else {}
        usage = payload.get("usage") or {}
        ms = usage.get("transcribedAudioMs")
        after = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                        model="inworld.stt-1")
        billed = after - before
        agree = ms is not None and abs(billed - ms / 1000) <= 0.01 * max(ms / 1000, 1)
        text = str((payload.get("transcription") or {}).get("transcript", ""))[:48]
        self._verdict("inworld-stt", r.status_code == 200 and agree and head_ok,
                      f"{r.status_code} t={elapsed:.3f}s transcribedAudioMs={ms} "
                      f"wire_model={usage.get('modelId')!r} billed_seconds={billed:g} "
                      f"basis=exact text={text!r} {head}")
        if ms:
            self.spend += 0.15 * (ms / 1000) / 3600

    # ------------------------------------------------------------- ElevenLabs

    def elevenlabs_tts(self, *, streamed: bool) -> None:
        """Both registered routes: the buffered one and `/stream`.

        ONE short sentence per call and no loop: the free tier has 10,000
        characters for the month and every call spends from it.

        The meter is the `character-cost` RESPONSE HEADER, which arrives
        before the first audio byte -- so a stream cut after one chunk still
        bills exactly. It is ElevenLabs' CREDIT count, not a character count:
        see the `elevenlabs-credit-basis` case below."""
        name = "elevenlabs-tts-stream" if streamed else "elevenlabs-tts-buffered"
        if not _has("ELEVENLABS_API_KEY"):
            _print(f"  {name}: SKIP (no ELEVENLABS_API_KEY)", self.out)
            return
        voice = os.environ.get("ELEVENLABS_VOICE_ID", ELEVENLABS_DEFAULT_VOICE)
        before = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                         model="elevenlabs.flash-v2-5")
        body = {"text": SHORT, "model_id": "elevenlabs.flash-v2-5"}
        url = (f"{self.gw.base_url}/elevenlabs/v1/text-to-speech/{voice}"
               f"{'/stream' if streamed else ''}?output_format=mp3_22050_32")
        t0 = time.perf_counter()
        first = None
        total = 0
        with httpx.stream("POST", url, json=body, timeout=60) as r:
            for chunk in r.iter_raw():
                if first is None:
                    first = time.perf_counter() - t0
                total += len(chunk)
            status, ct = r.status_code, r.headers.get("content-type", "")
            head_ok, head = _gw(r.headers, "elevenlabs.flash-v2-5", rewritten=True)
        elapsed = time.perf_counter() - t0
        after = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                        model="elevenlabs.flash-v2-5")
        billed = after - before
        ok = status == 200 and total > 0 and billed > 0 and head_ok
        self._verdict(
            name, ok,
            f"{status} {ct} bytes={total} "
            f"{'ttfb' if streamed else 't'}={(first if streamed else elapsed):.3f}s "
            f"billed_characters={billed:g} sent_characters={len(SHORT)} "
            f"basis=exact {head}")
        self.spend += 50.0 * billed / 1e6

    def elevenlabs_credit_basis(self) -> None:
        """The `character-cost` header is CREDITS, and flash costs half a
        credit per character -- so the gateway's `unit="characters"` bill is
        half the characters the caller sent. Reported, not asserted away:
        whether $50/1M is a per-credit or a per-character rate is a catalog
        question, and this line is the evidence for answering it.

        Free: it re-reads the numbers the two cases above already bought."""
        if not _has("ELEVENLABS_API_KEY"):
            _print("  elevenlabs-credit-basis: SKIP (no ELEVENLABS_API_KEY)", self.out)
            return
        units = _metric(self.metrics(), "llmgw_units_total", unit="characters",
                        model="elevenlabs.flash-v2-5")
        sent = 2 * len(SHORT)
        ratio = (units / sent) if sent else 0.0
        _print(f"  elevenlabs-credit-basis: INFO billed_units={units:g} for "
               f"{sent} characters over two calls (ratio={ratio:.2f}); "
               f"`character-cost` is a CREDIT count and flash-v2-5 is half a "
               f"credit per character, so `unit=\"characters\"` on this row "
               f"counts credits", self.out)

    def elevenlabs_stt(self) -> None:
        """Scribe. Multipart, `model_id` before the `file` part, and the bill
        is `audio_duration_secs` from the response body."""
        if not _has("ELEVENLABS_API_KEY"):
            _print("  elevenlabs-stt: SKIP (no ELEVENLABS_API_KEY)", self.out)
            return
        audio = self.clip()
        if audio is None:
            _print("  elevenlabs-stt: SKIP (no speech-to-text input)", self.out)
            return
        before = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                         model="elevenlabs.scribe-v2")
        t0 = time.perf_counter()
        r = httpx.post(f"{self.gw.base_url}/elevenlabs/v1/speech-to-text",
                       data={"model_id": "elevenlabs.scribe-v2"},
                       files={"file": ("clip.wav", audio, "audio/wav")}, timeout=90)
        elapsed = time.perf_counter() - t0
        head_ok, head = _gw(r.headers, "elevenlabs.scribe-v2", rewritten=True)
        payload = r.json() if r.status_code == 200 else {}
        secs = payload.get("audio_duration_secs")
        after = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                        model="elevenlabs.scribe-v2")
        billed = after - before
        agree = secs is not None and abs(billed - secs) <= 0.01 * max(secs, 1)
        text = str(payload.get("text", ""))[:48]
        self._verdict("elevenlabs-stt", r.status_code == 200 and agree and head_ok,
                      f"{r.status_code} t={elapsed:.3f}s audio_duration_secs={secs} "
                      f"billed_seconds={billed:g} basis=exact text={text!r} {head}")
        if secs:
            self.spend += 0.22 * secs / 3600

    # ------------------------------------------------------------- AssemblyAI

    def assemblyai_sync(self) -> None:
        """Multipart with an `audio` part, at `/v1/transcribe`, with the
        catalog id in `?model=` so the gateway can write the wire id into
        `X-AAI-Model`. Any one of those three missing is a 404."""
        if not _has("ASSEMBLYAI_API_KEY"):
            _print("  assemblyai-sync: SKIP (no ASSEMBLYAI_API_KEY)", self.out)
            return
        audio = self.clip() or _tone_wav(seconds=2)
        before = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                         model="assemblyai.sync")
        t0 = time.perf_counter()
        r = httpx.post(f"{self.gw.base_url}/assemblyai/v1/transcribe?model=assemblyai.sync",
                       files={"audio": ("clip.wav", audio, "audio/wav")}, timeout=90)
        elapsed = time.perf_counter() - t0
        # No rewrite warrant here on purpose: the model rides `X-AAI-Model`,
        # so the caller's multipart bytes go upstream untouched.
        head_ok, head = _gw(r.headers, "assemblyai.sync", rewritten=False)
        payload = r.json() if r.status_code == 200 else {}
        ms = payload.get("audio_duration_ms")
        after = _metric(self.metrics(), "llmgw_units_total", unit="seconds",
                        model="assemblyai.sync")
        billed = after - before
        agree = ms is not None and abs(billed - ms / 1000) <= 0.01 * max(ms / 1000, 1)
        text = str(payload.get("text", ""))[:48]
        self._verdict("assemblyai-sync", r.status_code == 200 and agree and head_ok,
                      f"{r.status_code} t={elapsed:.3f}s audio_duration_ms={ms} "
                      f"billed_seconds={billed:g} basis=exact text={text!r} {head}")
        if ms:
            self.spend += 0.0075 * ms / 1000 / 60



    # ------------------------------------------- AssemblyAI has no synthesis

    def assemblyai_no_tts(self, *, upstream: bool = True) -> None:
        """AssemblyAI ships no text-to-speech product. Evidenced, not asserted.

        Three independent readings, none of which costs anything:

        1. the gateway mounts no AssemblyAI synthesis route (every candidate
           path is a 404 from OUR router, not from theirs);
        2. no AssemblyAI row in the catalog is priced in characters -- all
           three are per-second transcription rows;
        3. the provider's own hosts 404 every plausible synthesis path.

        (1) and (2) would also be true of a product we simply had not wired
        up, which is why (3) is here: the endpoint does not exist upstream
        either. AssemblyAI's speech-to-text lives at `/v1/transcribe`, and
        that is the whole of their audio API surface that a gateway can
        carry."""
        gw_paths = ["/assemblyai/v1/text-to-speech", "/assemblyai/tts",
                    "/assemblyai/v1/speech"]
        gw_codes = {}
        for path in gw_paths:
            r = httpx.post(f"{self.gw.base_url}{path}", json={"text": "hi"}, timeout=20)
            gw_codes[path] = r.status_code
        rows = [m for m in DEFAULT_CATALOG.models.values()
                if str(m.provider).startswith("assemblyai")]
        units = sorted({m.unit for m in rows})
        probed: dict[str, object] = {}
        if upstream and _has("ASSEMBLYAI_API_KEY"):
            for url in ("https://api.assemblyai.com/v2/text-to-speech",
                        "https://api.assemblyai.com/v1/text-to-speech",
                        "https://api.assemblyai.com/v2/speech"):
                try:
                    r = httpx.post(
                        url, headers={"authorization": os.environ["ASSEMBLYAI_API_KEY"]},
                        json={"text": "hi"}, timeout=20)
                    probed[url.split(".com")[1]] = r.status_code
                except Exception as exc:  # noqa: BLE001
                    probed[url.split(".com")[1]] = type(exc).__name__
        ok = (set(gw_codes.values()) == {404}
              and units == ["seconds"]
              and (not probed or set(probed.values()) == {404}))
        self._verdict(
            "assemblyai-no-tts", ok,
            f"gateway routes={gw_codes} catalog_rows={len(rows)} units={units} "
            f"provider_paths={probed or 'not probed'} -- AssemblyAI has no "
            f"synthesis product; "
            f"the matrix cell is correctly EMPTY, not broken")

    # ------------------------------------------------- one negative per provider

    def negative_models(self) -> None:
        """An id the catalog never heard of must die in the router.

        `x-gw-attempts: 0` with `x-gw-served-by: -` is the assertion: a 400
        alone could have come from the provider, and a provider that saw the
        request is a provider we paid and a credential we exposed to a
        typo."""
        voice = os.environ.get("ELEVENLABS_VOICE_ID", ELEVENLABS_DEFAULT_VOICE)
        tiny = _tone_wav(seconds=1)
        probes = [
            ("neg-inworld-tts", dict(
                method="POST", url=f"{self.gw.base_url}/inworld/tts/v1/voice",
                json={"modelId": "inworld.nope", "text": "hi", "voiceId": "Aarav"})),
            ("neg-inworld-stt", dict(
                method="POST", url=f"{self.gw.base_url}/inworld/stt/v1/transcribe",
                json={"transcribeConfig": {"modelId": "inworld.nope",
                                           "audioEncoding": "LINEAR16",
                                           "sampleRateHertz": 16000},
                      "audioData": {"content": base64.b64encode(tiny).decode()}})),
            ("neg-elevenlabs-tts", dict(
                method="POST",
                url=f"{self.gw.base_url}/elevenlabs/v1/text-to-speech/{voice}",
                json={"model_id": "elevenlabs.nope", "text": "hi"})),
            ("neg-elevenlabs-stt", dict(
                method="POST",
                url=f"{self.gw.base_url}/elevenlabs/v1/speech-to-text",
                data={"model_id": "elevenlabs.nope"},
                files={"file": ("clip.wav", tiny, "audio/wav")})),
            ("neg-assemblyai", dict(
                method="POST",
                url=f"{self.gw.base_url}/assemblyai/v1/transcribe?model=assemblyai.nope",
                files={"audio": ("clip.wav", tiny, "audio/wav")})),
            ("neg-openai-speech", dict(
                method="POST", url=f"{self.gw.base_url}/v1/audio/speech",
                json={"model": "openai.nope", "input": "hi", "voice": "cedar"})),
            ("neg-sarvam-tts", dict(
                method="POST", url=f"{self.gw.base_url}/sarvam/text-to-speech",
                json={"model": "sarvam.nope", "text": "hi", "speaker": "shubh",
                      "target_language_code": "en-IN"})),
        ]
        for name, kwargs in probes:
            r = httpx.request(timeout=30, **kwargs)
            ok, detail = _refused(r)
            self._verdict(name, ok, detail)

    def provider_unknown_model_errors(self) -> None:
        """What the PROVIDER says when an unknown id does reach it.

        Not through the gateway: the gateway's whole job above is to make
        sure this never happens, so the only way to record these shapes is to
        ask each provider directly with a wire id it does not serve. Free --
        every one is a 4xx before any synthesis or transcription -- and the
        reason the error classifier can be trusted, because these are the
        bodies it is reading. Nothing here echoes a credential."""
        tiny = _tone_wav(seconds=1)
        probes = [
            ("elevenlabs-tts", _has("ELEVENLABS_API_KEY"), dict(
                method="POST",
                url=("https://api.elevenlabs.io/v1/text-to-speech/"
                     + ELEVENLABS_DEFAULT_VOICE),
                headers={"xi-api-key": os.environ.get("ELEVENLABS_API_KEY", "")},
                json={"text": "hi", "model_id": "eleven_nope_v9"})),
            ("elevenlabs-stt", _has("ELEVENLABS_API_KEY"), dict(
                method="POST", url="https://api.elevenlabs.io/v1/speech-to-text",
                headers={"xi-api-key": os.environ.get("ELEVENLABS_API_KEY", "")},
                data={"model_id": "scribe_v99"},
                files={"file": ("clip.wav", tiny, "audio/wav")})),
            ("inworld-tts", _has("INWORLD_API_KEY"), dict(
                method="POST", url="https://api.inworld.ai/tts/v1/voice",
                headers={"authorization":
                         f"Basic {os.environ.get('INWORLD_API_KEY', '')}"},
                json={"modelId": "inworld-tts-nope", "text": "hi",
                      "voiceId": "Aarav"})),
            ("inworld-stt", _has("INWORLD_API_KEY"), dict(
                method="POST", url="https://api.inworld.ai/stt/v1/transcribe",
                headers={"authorization":
                         f"Basic {os.environ.get('INWORLD_API_KEY', '')}"},
                json={"transcribeConfig": {"modelId": "inworld/nope",
                                           "audioEncoding": "LINEAR16",
                                           "sampleRateHertz": 16000},
                      "audioData": {"content": base64.b64encode(tiny).decode()}})),
            ("assemblyai-sync", _has("ASSEMBLYAI_API_KEY"), dict(
                method="POST", url="https://sync.assemblyai.com/v1/transcribe",
                headers={"authorization": os.environ.get("ASSEMBLYAI_API_KEY", ""),
                         "X-AAI-Model": "universal-nope"},
                files={"audio": ("clip.wav", tiny, "audio/wav")})),
            ("sarvam-tts", _has("SARVAM_API_KEY"), dict(
                method="POST", url="https://api.sarvam.ai/text-to-speech",
                headers={"api-subscription-key":
                         os.environ.get("SARVAM_API_KEY", "")},
                json={"model": "bulbul:v99", "text": "hi", "speaker": "shubh",
                      "target_language_code": "en-IN"})),
        ]
        for name, present, kwargs in probes:
            if not present:
                _print(f"  provider-error[{name}]: SKIP (no key)", self.out)
                continue
            try:
                r = httpx.request(timeout=30, **kwargs)
            except Exception as exc:  # noqa: BLE001
                _print(f"  provider-error[{name}]: {type(exc).__name__}", self.out)
                continue
            # The body is the provider's own prose about a MODEL id; none of
            # these shapes quotes the credential (Inworld's AUTH errors do,
            # which is why no auth probe lives here).
            _print(f"  provider-error[{name}]: {r.status_code} "
                   f"server={r.headers.get('server', '-')} {r.text[:130]!r}", self.out)

    # ------------------------------------------------ cross-provider round trip

    def sarvam_clip(self) -> bytes | None:
        """One sentence synthesised by SARVAM, as a 16 kHz WAV.

        The round trip's input comes from a provider that is not any of the
        transcribers, so no provider is marking its own homework -- and it is
        a real RIFF container, which is what Sarvam's own speech-to-text
        needs before it can estimate a duration at all."""
        if self._sarvam_clip is not None:
            return self._sarvam_clip or None
        if not _has("SARVAM_API_KEY"):
            self._sarvam_clip = b""
            return None
        body = {"model": "sarvam.bulbul-v3", "text": ROUNDTRIP,
                "speaker": "shubh", "target_language_code": "en-IN",
                "speech_sample_rate": 16000}
        r = httpx.post(f"{self.gw.base_url}/sarvam/text-to-speech", json=body,
                       timeout=60)
        if r.status_code != 200:
            self._sarvam_clip = b""
            return None
        try:
            audio = base64.b64decode(r.json()["audios"][0])
        except (ValueError, KeyError, IndexError, TypeError):
            self._sarvam_clip = b""
            return None
        self.spend += 33.90 * len(ROUNDTRIP) / 1e6
        self._sarvam_clip = audio
        return audio

    def round_trip(self) -> None:
        """Synthesise once, transcribe four times, compare the words.

        The single case that cannot pass against four independent mocks: the
        bytes one provider produced have to carry meaning the other three can
        read. Compared on normalised words, because punctuation and casing
        are transcription style and not evidence."""
        audio = self.sarvam_clip()
        if audio is None:
            _print("  round-trip: SKIP (no Sarvam clip to transcribe)", self.out)
            return
        seconds = max(0, len(audio) - 44) / 32_000
        _print(f"  round-trip-input: {len(audio)} B WAV, {seconds:.2f}s, "
               f"{ROUNDTRIP!r} spoken by sarvam.bulbul-v3 through the gateway",
               self.out)
        want = _words(ROUNDTRIP)
        results: dict[str, str] = {}

        def record(name: str, text: str, elapsed: float, detail: str) -> None:
            """PASS is "this transcriber heard THIS audio", not "this
            transcriber is accurate".

            The claim under test is that the bytes one provider synthesised
            carry a sentence the others can read -- which a two-word slip
            does not refute and a mock cannot fake. Word-error is the ASR's
            business and varies run to run (ElevenLabs Scribe reads Sarvam's
            en-IN voice as "The date we" about half the time), so exactness
            is REPORTED and overlap is ASSERTED."""
            results[name] = text
            got = _words(text)
            overlap = _overlap(got, want)
            self._verdict(f"round-trip-{name}", bool(got) and overlap >= 0.6,
                          f"t={elapsed:.3f}s {detail} exact={got == want} "
                          f"word_overlap={overlap:.0%} transcript={text!r}")

        if _has("ASSEMBLYAI_API_KEY"):
            t0 = time.perf_counter()
            r = httpx.post(
                f"{self.gw.base_url}/assemblyai/v1/transcribe?model=assemblyai.sync",
                files={"audio": ("clip.wav", audio, "audio/wav")}, timeout=90)
            el = time.perf_counter() - t0
            payload = r.json() if r.status_code == 200 else {}
            ms = payload.get("audio_duration_ms")
            record("assemblyai", str(payload.get("text", "")), el,
                   f"{r.status_code} audio_duration_ms={ms}")
            if ms:
                self.spend += 0.0075 * ms / 1000 / 60
        if _has("ELEVENLABS_API_KEY"):
            t0 = time.perf_counter()
            r = httpx.post(f"{self.gw.base_url}/elevenlabs/v1/speech-to-text",
                           data={"model_id": "elevenlabs.scribe-v2"},
                           files={"file": ("clip.wav", audio, "audio/wav")},
                           timeout=90)
            el = time.perf_counter() - t0
            payload = r.json() if r.status_code == 200 else {}
            secs = payload.get("audio_duration_secs")
            record("elevenlabs", str(payload.get("text", "")), el,
                   f"{r.status_code} audio_duration_secs={secs}")
            if secs:
                self.spend += 0.22 * secs / 3600
        if _has("INWORLD_API_KEY"):
            t0 = time.perf_counter()
            r = httpx.post(f"{self.gw.base_url}/inworld/stt/v1/transcribe", json={
                "transcribeConfig": {"modelId": "inworld.stt-1",
                                     "audioEncoding": "LINEAR16",
                                     "sampleRateHertz": 16000,
                                     "numberOfChannels": 1, "language": "en-US"},
                "audioData": {"content": base64.b64encode(audio).decode()}},
                timeout=90)
            el = time.perf_counter() - t0
            payload = r.json() if r.status_code == 200 else {}
            ms = (payload.get("usage") or {}).get("transcribedAudioMs")
            record("inworld", str((payload.get("transcription") or {}).get(
                "transcript", "")), el, f"{r.status_code} transcribedAudioMs={ms}")
            if ms:
                self.spend += 0.15 * (ms / 1000) / 3600
        if _has("SARVAM_API_KEY"):
            t0 = time.perf_counter()
            r = httpx.post(f"{self.gw.base_url}/sarvam/speech-to-text",
                           data={"model": "sarvam.saaras-v4"},
                           files={"file": ("clip.wav", audio, "audio/wav")},
                           timeout=90)
            el = time.perf_counter() - t0
            payload = r.json() if r.status_code == 200 else {}
            record("sarvam", str(payload.get("transcript", "")), el,
                   f"{r.status_code} provider_meter=none")
            self.spend += 0.00565 * seconds / 60

        # Consensus, not identity. Four independent recognisers will not
        # agree on every function word -- "this" against "the" is the one
        # they argue about here, and Scribe is openly non-deterministic on
        # non-US-English voices. What proves the path is real rather than
        # four mocks passing separately is that a majority return the SAME
        # sentence and every one of them is close to the source; demanding
        # two byte-perfect readings tests the speech models, not the gateway.
        from collections import Counter
        readings = Counter(_words(t) for t in results.values())
        top, agree = readings.most_common(1)[0] if readings else ((), 0)
        exact = sum(1 for t in results.values() if _words(t) == want)
        majority = agree >= (len(results) + 1) // 2
        closest = min((_overlap(_words(t), want) for t in results.values()),
                      default=0.0)
        self._verdict(
            "round-trip-agreement",
            bool(results) and majority and closest >= 0.8,
            f"{len(results)} transcribers, {len(readings)} distinct readings, "
            f"{agree} agreeing on {' '.join(top)!r}, {exact} word-perfect "
            f"against {ROUNDTRIP!r}, worst overlap {closest:.0%}: "
            + "; ".join(f"{k}={v!r}" for k, v in results.items()))


# ----------------------------------------------------------------- helpers


def _overlap(got: tuple[str, ...], want: tuple[str, ...]) -> float:
    """Share of the source's words the transcriber returned. The same
    measure each per-transcriber case uses, lifted out so the agreement
    case cannot drift from them."""
    return len(set(got) & set(want)) / len(set(want)) if want else 0.0


def _words(text: str) -> tuple[str, ...]:
    """Words, lowercased, punctuation dropped. Transcribers disagree about
    commas and capitals and that is not what this smoke is measuring."""
    keep = "".join(c.lower() if (c.isalnum() or c.isspace()) else " " for c in text)
    return tuple(keep.split())


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


def ledger(capture_path: str, out) -> int:
    """The gateway's OWN answer to "what did that cost, and how sure is it".

    Read from the capture sink rather than from `/metrics`, because `basis`
    and `cost_notes` are per-RECORD facts that no counter can carry: a
    dashboard that shows a dollar figure without them shows an estimate and a
    measurement as the same number. This is the table the live matrix is
    ultimately about -- every speech row must say `exact` and agree with the
    provider's meter, or say `estimated` and say WHY in a note.
    """
    # The capture worker is asynchronous by design, so the last record can
    # still be in flight when the last case returns. Settle briefly rather
    # than reporting a short ledger as a missing bill.
    time.sleep(1.0)
    path = Path(capture_path)
    if not path.is_file():
        _print("\nMETERING LEDGER: (no capture records)", out)
        return 0
    unexplained: list[str] = []
    _print("\nMETERING LEDGER (from the gateway's own capture records)", out)
    _print(f"  {'provider':<18} {'model':<30} {'units':<22} {'basis':<10} cost_usd",
           out)
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if not rec.get("units") and not rec.get("cost_usd"):
            continue
        # Zero-valued kinds are noise: every speech record carries both
        # `characters` and `seconds` and only one of them was ever billed.
        units = ",".join(f"{k}={v:g}" for k, v in (rec.get("units") or {}).items() if v)
        tokens = ",".join(f"{k}={v:g}" for k, v in (rec.get("tokens") or {}).items() if v)
        _print(f"  {str(rec.get('provider'))[:18]:<18} "
               f"{str(rec.get('model') or '-')[:30]:<30} "
               f"{(units or tokens or '-')[:22]:<22} "
               f"{str(rec.get('basis')):<10} {rec.get('cost_usd', 0.0):.8f}", out)
        notes = rec.get("cost_notes") or ()
        for note in notes:
            _print(f"      note: {note}", out)
        if rec.get("basis") == "estimated" and rec.get("cost_usd") and not notes:
            # An `estimated` bill with no reason attached is the one shape an
            # invoice dispute cannot use: it says "we guessed" and not "we
            # guessed THIS WAY, because the provider told us nothing".
            unexplained.append(f"{rec.get('model')} "
                               f"${rec.get('cost_usd', 0.0):.8f}")
    if unexplained:
        _print(f"  ledger-estimated-notes: {len(unexplained)} estimated record(s) "
               f"with NO cost_notes: {unexplained} -> FAIL", out)
    else:
        _print("  ledger-estimated-notes: every estimated record explains its "
               "number -> PASS", out)
    return len(unexplained)


def build_voice_app(policy_path: str, capture_path: str | None = None):
    settings = {
        "catalog": DEFAULT_CATALOG,
        "fake_upstreams": False,
        "policy_file": policy_path,
        "default_model": "openai.gpt-4o-mini-tts",
        "forward_request_headers": ("x-request-id",),
        # Capture is ON here because `basis` and `cost_notes` live on the
        # RECORD and nowhere else: a metric can say how many units were
        # billed but not whether anyone measured them.
        "capture_path": capture_path,
    }
    return build_app(ServerConfig(**settings).validated())


def run(*, spend: bool = True, out=sys.stdout) -> float:
    with tempfile.TemporaryDirectory(prefix="llmgw-voice-") as tmp:
        policy = Path(tmp) / "voice.toml"
        policy.write_text(POLICY_TOML, encoding="utf-8")
        capture = Path(tmp) / "capture.jsonl"
        gw = text_smoke.serve(build_voice_app(str(policy), str(capture)))
        try:
            _print(f"voice gateway on {gw.base_url}", out)
            for env in ("OPENAI_API_KEY", "INWORLD_API_KEY", "ELEVENLABS_API_KEY",
                        "ASSEMBLYAI_API_KEY"):
                _print(f"  {env}: {'set' if _has(env) else 'ABSENT (cases skip)'}", out)
            if not spend:
                _print("  --no-spend: routing and key presence only; the "
                       "catalog and gateway-route halves of assemblyai-no-tts "
                       "still run because they cost nothing", out)
                case = Case(gw, out)
                case.assemblyai_no_tts(upstream=False)
                case.negative_models()
                _print(f"\nFAILURES: {case.failures}", out)
                return 0.0
            case = Case(gw, out)
            case.openai_tts_binary()
            case.openai_tts_sse()
            case.openai_stt(None)
            case.inworld_stream()
            case.inworld_sync()
            case.inworld_empty_text()
            case.elevenlabs_tts(streamed=True)
            case.elevenlabs_tts(streamed=False)
            case.elevenlabs_credit_basis()
            # Everything below transcribes the clip the first caller mints.
            case.assemblyai_sync()
            case.elevenlabs_stt()
            case.inworld_stt()
            # AssemblyAI has no synthesis product; the routing negatives and
            # the provider's own prose cost nothing.
            case.assemblyai_no_tts()
            case.negative_models()
            case.provider_unknown_model_errors()
            # The one case four independent mocks could not fake.
            case.round_trip()
            unexplained = ledger(str(capture), out)
            _print(f"\nFAILURES: {case.failures + unexplained}", out)
            _print(f"ESTIMATED SPEND: ${case.spend:.6f}", out)
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

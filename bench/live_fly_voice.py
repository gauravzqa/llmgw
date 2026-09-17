"""Live voice benchmark: a DEPLOYED llmgw against the real TTS providers.

    /app/.venv/bin/python live_fly_voice.py --smoke          # 1 per scenario
    /app/.venv/bin/python live_fly_voice.py                  # the campaign (spends money)
    /app/.venv/bin/python live_fly_voice.py --gateway http://127.0.0.1:8080

The question is the one `bench/live_fly.py` asks for text, asked for audio:
for a short TTS call, what does the gateway add to the time to first audio
byte (TTFB), and to the total, and do its own meters agree with the
provider's afterwards. Same method, same discipline:

* Two arms, D (direct) and G (gateway), one persistent client per arm and
  provider, one untimed warm-up each, INTERLEAVED so provider drift lands on
  both. Headline per scenario is the paired G - D on medians.
* TTFB is the first AUDIO byte: for the binary surfaces the first body chunk;
  for Inworld the first NDJSON line (which carries a second of audio and the
  full character count); for OpenAI SSE mode the first `speech.audio.delta`.
* `time.perf_counter()` only. Nothing sleeps between requests except the
  provider.
* Credentials come from the machine's environment, never enter the record,
  and every error string is scrubbed before it is stored.
* Scenarios whose provider key is absent are SKIPPED and reported as such
  (ElevenLabs and AssemblyAI were unprovisioned on 18 Sep 2026).

Each request is one JSON line in `--out`; the report is Markdown on stdout
built only from those records plus a before/after diff of the gateway's
`/metrics` (`llmgw_units_total` for characters and seconds).
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

import httpx

from llmgw.catalog import DEFAULT_CATALOG

# --------------------------------------------------------------------------
# Endpoints, models, credentials
# --------------------------------------------------------------------------

TEXT = ("A streaming gateway must never retry a request after the first byte "
        "has reached the client; it can only end the stream honestly.")

SPEND_ABORT_USD = 0.50


@dataclass(frozen=True)
class Scenario:
    key: str
    provider: str                # key env lookup + client
    catalog_id: str
    direct_url: str
    gw_path: str
    key_env: str
    framing: str                 # raw | jsonl | sse
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    query: str = ""

    @property
    def wire_model(self) -> str:
        return DEFAULT_CATALOG.models[self.catalog_id].api_model


ELEVEN_VOICE = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")

SCENARIOS: list[Scenario] = [
    Scenario(
        key="openai_tts_binary", provider="openai", catalog_id="openai.gpt-4o-mini-tts",
        direct_url="https://api.openai.com/v1/audio/speech", gw_path="/v1/audio/speech",
        key_env="OPENAI_API_KEY", framing="raw",
        body={"input": TEXT, "voice": "cedar", "response_format": "pcm"},
    ),
    Scenario(
        key="openai_tts_sse", provider="openai", catalog_id="openai.gpt-4o-mini-tts",
        direct_url="https://api.openai.com/v1/audio/speech", gw_path="/v1/audio/speech",
        key_env="OPENAI_API_KEY", framing="sse",
        body={"input": TEXT, "voice": "cedar", "stream_format": "sse"},
    ),
    Scenario(
        key="inworld_stream", provider="inworld", catalog_id="inworld.tts-2-flash",
        direct_url="https://api.inworld.ai/tts/v1/voice:stream",
        gw_path="/inworld/tts/v1/voice:stream", key_env="INWORLD_API_KEY", framing="jsonl",
        body={"text": TEXT, "voiceId": "Aarav",
              "audioConfig": {"audioEncoding": "LINEAR16", "sampleRateHertz": 24000}},
    ),
    Scenario(
        key="elevenlabs_stream", provider="elevenlabs", catalog_id="elevenlabs.flash-v2-5",
        direct_url=f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVEN_VOICE}/stream",
        gw_path=f"/elevenlabs/v1/text-to-speech/{ELEVEN_VOICE}/stream",
        key_env="ELEVENLABS_API_KEY", framing="raw",
        body={"text": TEXT}, query="?output_format=mp3_22050_32",
    ),
]

MODEL_KEY = {"openai": "model", "inworld": "modelId", "elevenlabs": "model_id"}


def _secret_values() -> list[str]:
    out = []
    for name in ("OPENAI_API_KEY", "INWORLD_API_KEY", "ELEVENLABS_API_KEY",
                 "ASSEMBLYAI_API_KEY", "LLMGW_TENANT_LAYRS_TOKEN"):
        v = os.environ.get(name)
        if v and len(v) >= 8:
            out.append(v)
    return out


_SECRETS = _secret_values()


def scrub(text: str, limit: int = 300) -> str:
    for s in _SECRETS:
        text = text.replace(s, "<redacted>")
    text = re.sub(r"sk-[A-Za-z0-9_-]{8,}", "sk-<redacted>", text)
    text = re.sub(r"sk_[A-Za-z0-9]{8,}", "sk_<redacted>", text)
    return text[:limit]


# --------------------------------------------------------------------------
# Records
# --------------------------------------------------------------------------


@dataclass
class Rec:
    arm: str
    scenario: str
    provider: str
    cold: bool = False
    status: int = 0
    ttfb_ms: float | None = None
    total_ms: float | None = None
    audio_bytes: int = 0
    frames: int = 0
    meter: str = ""              # what the provider reported, in its own unit
    meter_value: float | None = None
    usd: float = 0.0
    gw: dict[str, str] = field(default_factory=dict)
    error: str | None = None

    def ok(self) -> bool:
        return self.status == 200 and self.error is None and self.audio_bytes > 0


# --------------------------------------------------------------------------
# Arms
# --------------------------------------------------------------------------


class Arm:
    def __init__(self, name: str, gateway: str | None, timeout: float) -> None:
        self.name = name
        self.gateway = gateway
        t = httpx.Timeout(timeout, connect=10.0)
        self.clients = {s.provider: httpx.Client(http2=True, timeout=t) for s in SCENARIOS}

    def close(self) -> None:
        for c in self.clients.values():
            c.close()

    def url(self, sc: Scenario) -> str:
        base = self.gateway + sc.gw_path if self.gateway else sc.direct_url
        return base + sc.query

    def headers(self, sc: Scenario) -> dict[str, str]:
        h = {"content-type": "application/json", "accept-encoding": "identity"}
        if self.gateway:
            tok = os.environ.get("LLMGW_TENANT_LAYRS_TOKEN")
            if tok:
                h["authorization"] = f"Bearer {tok}"
            return h
        key = os.environ[sc.key_env]
        if sc.provider == "elevenlabs":
            h["xi-api-key"] = key
        else:
            h["authorization"] = f"Bearer {key}"
        return h

    def body(self, sc: Scenario) -> dict[str, Any]:
        model = sc.catalog_id if self.gateway else sc.wire_model
        return dict(sc.body, **{MODEL_KEY[sc.provider]: model})


def price(sc: Scenario, meter_value: float | None, audio_bytes: int) -> float:
    spec = DEFAULT_CATALOG.models[sc.catalog_id]
    if spec.unit == "characters":
        n = meter_value if meter_value is not None else len(TEXT)
        return spec.input_per_m * n / 1e6
    if spec.unit == "seconds" and spec.per_minute:
        return spec.per_minute * (meter_value or 0) / 60
    # gpt-4o-mini-tts: text in at input_per_m, audio out at audio_output_per_m.
    text_tokens = max(1, len(TEXT) // 4)
    audio_tokens = meter_value if meter_value is not None else audio_bytes / 4800
    out_rate = spec.audio_output_per_m or spec.output_per_m
    return (spec.input_per_m * text_tokens + out_rate * audio_tokens) / 1e6


# --------------------------------------------------------------------------
# One measured request
# --------------------------------------------------------------------------


def run_one(arm: Arm, sc: Scenario, *, cold: bool = False) -> Rec:
    rec = Rec(arm=arm.name, scenario=sc.key, provider=sc.provider, cold=cold)
    client = arm.clients[sc.provider]
    t0 = time.perf_counter()
    try:
        with client.stream("POST", arm.url(sc), headers=arm.headers(sc),
                           json=arm.body(sc)) as r:
            rec.status = r.status_code
            rec.gw = {k.lower(): v for k, v in r.headers.items()
                      if k.lower().startswith("x-gw-")}
            if r.status_code != 200:
                rec.error = scrub(r.read().decode("utf-8", "replace"))
                rec.total_ms = (time.perf_counter() - t0) * 1000
                return rec
            if sc.framing == "raw":
                _read_raw(r, rec, t0)
                cost = r.headers.get("character-cost")
                if cost is not None:
                    rec.meter, rec.meter_value = "characters", float(cost)
            elif sc.framing == "jsonl":
                _read_jsonl(r, rec, t0)
            else:
                _read_sse(r, rec, t0)
            rec.total_ms = (time.perf_counter() - t0) * 1000
    except Exception as exc:  # noqa: BLE001 - a bench records, it does not crash
        rec.total_ms = (time.perf_counter() - t0) * 1000
        rec.error = scrub(f"{type(exc).__name__}: {exc}")
        return rec
    rec.usd = price(sc, rec.meter_value, rec.audio_bytes)
    return rec


def _read_raw(r: httpx.Response, rec: Rec, t0: float) -> None:
    for chunk in r.iter_raw():
        if not chunk:
            continue
        if rec.ttfb_ms is None:
            rec.ttfb_ms = (time.perf_counter() - t0) * 1000
        rec.audio_bytes += len(chunk)
        rec.frames += 1


def _read_jsonl(r: httpx.Response, rec: Rec, t0: float) -> None:
    buf = b""
    for chunk in r.iter_raw():
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            _jsonl_line(line, rec, t0)
    if buf.strip():
        _jsonl_line(buf, rec, t0)


def _jsonl_line(line: bytes, rec: Rec, t0: float) -> None:
    if not line.strip():
        return
    rec.frames += 1
    try:
        result = json.loads(line).get("result") or {}
    except ValueError:
        return
    audio = result.get("audioContent") or ""
    if audio:
        if rec.ttfb_ms is None:
            rec.ttfb_ms = (time.perf_counter() - t0) * 1000
        rec.audio_bytes += len(base64.b64decode(audio))
    usage = result.get("usage") or {}
    n = usage.get("processedCharactersCount")
    if isinstance(n, int) and n > 0:
        rec.meter, rec.meter_value = "characters", float(n)


def _read_sse(r: httpx.Response, rec: Rec, t0: float) -> None:
    buf = b""
    for chunk in r.iter_raw():
        buf += chunk
        while True:
            m = re.search(rb"\r?\n\r?\n", buf)
            if not m:
                break
            frame, buf = buf[:m.start()], buf[m.end():]
            for raw in frame.splitlines():
                if not raw.startswith(b"data:"):
                    continue
                data = raw[5:].strip()
                if data == b"[DONE]":
                    continue
                try:
                    obj = json.loads(data)
                except ValueError:
                    continue
                rec.frames += 1
                if obj.get("type") == "speech.audio.delta":
                    if rec.ttfb_ms is None:
                        rec.ttfb_ms = (time.perf_counter() - t0) * 1000
                    rec.audio_bytes += len(base64.b64decode(obj.get("audio") or ""))
                elif "usage" in obj:
                    out = (obj["usage"] or {}).get("output_tokens")
                    if isinstance(out, int):
                        rec.meter, rec.meter_value = "audio_output_tokens", float(out)


# --------------------------------------------------------------------------
# Campaign
# --------------------------------------------------------------------------

_METRIC_LINE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([-+0-9.eE]+|NaN)$")


def scrape(gateway: str) -> dict[str, float]:
    out: dict[str, float] = {}
    try:
        text = httpx.get(gateway + "/metrics", timeout=10.0).text
    except Exception as exc:  # noqa: BLE001
        print(f"metrics scrape failed: {scrub(str(exc))}", file=sys.stderr)
        return out
    for line in text.splitlines():
        m = _METRIC_LINE.match(line.strip())
        if m:
            try:
                out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
            except ValueError:
                pass
    return out


def metrics_diff(before: dict[str, float], after: dict[str, float]) -> list[str]:
    rows = []
    for k in sorted(after):
        if not (k.startswith("llmgw_units_total") or k.startswith("llmgw_tokens_total")
                or k.startswith("llmgw_requests_total")):
            continue
        d = after[k] - before.get(k, 0.0)
        if d:
            rows.append(f"| `{k}` | {d:g} |")
    return rows


def _med(xs: list[float]) -> float | None:
    return statistics.median(xs) if xs else None


def _f(x: float | None, nd: int = 1) -> str:
    return "–" if x is None else f"{x:.{nd}f}"


def scenario_row(recs: list[Rec], key: str) -> str:
    d = [r for r in recs if r.scenario == key and r.arm == "D" and r.ok() and not r.cold]
    g = [r for r in recs if r.scenario == key and r.arm == "G" and r.ok() and not r.cold]
    if not d and not g:
        return f"| {key} | skipped | | | | | | |"
    dt = _med([r.ttfb_ms for r in d if r.ttfb_ms is not None])
    gt = _med([r.ttfb_ms for r in g if r.ttfb_ms is not None])
    dtot = _med([r.total_ms for r in d if r.total_ms is not None])
    gtot = _med([r.total_ms for r in g if r.total_ms is not None])
    meters = {r.meter_value for r in d + g if r.meter_value is not None}
    delta = None if dt is None or gt is None else gt - dt
    return (f"| {key} | {len(d)}/{len(g)} | {_f(dt)} | {_f(gt)} | {_f(delta)} | "
            f"{_f(dtot)} | {_f(gtot)} | {sorted(meters) if meters else 'no meter'} |")


def render(recs: list[Rec], *, before: dict[str, float], after: dict[str, float],
           gateway: str, started: str, wall_s: float, skipped: list[str]) -> str:
    out = [f"# live_fly_voice -- {started} UTC, {gateway}, {wall_s:.0f}s wall", ""]
    out += ["| scenario | n D/G | TTFB D ms | TTFB G ms | G-D ms | total D | total G "
            "| meter |", "|---|---|---|---|---|---|---|---|"]
    out += [scenario_row(recs, s.key) for s in SCENARIOS]
    if skipped:
        out += ["", "Skipped (no key in the environment): " + ", ".join(skipped)]
    errs = [r for r in recs if r.error]
    if errs:
        out += ["", "## Errors", ""]
        out += [f"- {r.arm} {r.scenario} {r.status}: {r.error}" for r in errs[:20]]
    out += ["", "## Gateway metrics moved", "", "| series | delta |", "|---|---|"]
    out += metrics_diff(before, after) or ["| (nothing) | |"]
    return "\n".join(out)


class Campaign:
    def __init__(self, arms: list[Arm], *, smoke: bool, out_path: str) -> None:
        self.arms = arms
        self.n = 1 if smoke else 8
        self.out = open(out_path, "a", encoding="utf-8")  # noqa: SIM115
        self.recs: list[Rec] = []
        self.spend = 0.0
        self.skipped: list[str] = []

    def record(self, rec: Rec) -> None:
        self.recs.append(rec)
        self.out.write(json.dumps(asdict(rec)) + "\n")
        self.out.flush()
        self.spend += rec.usd
        if self.spend > SPEND_ABORT_USD:
            raise RuntimeError(f"spend abort at ${self.spend:.4f}")

    def run(self) -> None:
        live = []
        for sc in SCENARIOS:
            if os.environ.get(sc.key_env):
                live.append(sc)
            else:
                self.skipped.append(f"{sc.key} (${sc.key_env})")
        for sc in live:  # one untimed warm-up per arm and scenario
            for arm in self.arms:
                self.record(run_one(arm, sc, cold=True))
        for _ in range(self.n):
            for sc in live:
                for arm in self.arms:  # D, G, D, G ...
                    self.record(run_one(arm, sc))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--gateway", default="http://127.0.0.1:8080")
    ap.add_argument("--out", default="/tmp/live_fly_voice.jsonl")
    ap.add_argument("--smoke", action="store_true", help="one request per scenario")
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args(argv)

    if not any(os.environ.get(s.key_env) for s in SCENARIOS):
        print("no voice provider key in the environment; nothing to measure", file=sys.stderr)
        return 2

    started = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    t0 = time.perf_counter()
    before = scrape(args.gateway)
    arms = [Arm("D", None, args.timeout), Arm("G", args.gateway, args.timeout)]
    camp = Campaign(arms, smoke=args.smoke, out_path=args.out)
    try:
        camp.run()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
    finally:
        for a in arms:
            a.close()
        camp.out.close()
    after = scrape(args.gateway)
    print(render(camp.recs, before=before, after=after, gateway=args.gateway,
                 started=started, wall_s=time.perf_counter() - t0, skipped=camp.skipped))
    print(f"\nspend_usd={camp.spend:.5f} records={len(camp.recs)}", file=sys.stderr)
    print("LIVE_FLY_VOICE_DONE", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())

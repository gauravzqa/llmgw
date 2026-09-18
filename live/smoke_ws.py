"""Live WebSocket smoke: one real Inworld TTS session, through the gateway.

Spends real money (a small fraction of a cent) and needs `INWORLD_API_KEY`.
Skips, loudly, when the key is absent. Never prints a key, a prefix of one,
or anything the provider reflected back that might contain one -- Inworld's
credential errors quote the first four characters of the key they refused
(captures-ws probe 2b), which is why the provider row carries
`scrub_error_bodies="all"` and why nothing here echoes a provider message.

What it proves that the contract tier cannot: the FAKE is a reading of the
captures, and a reading can be wrong. These cases put the same bytes through
the real provider and compare:

* every byte the client read off the socket against the gateway's own
  `llmgw_ws_bytes_total{direction="client_out"}`. That is the byte-identity
  claim in the only form that can be TESTED live: two sessions of the same
  text do NOT produce the same audio (Inworld's synthesis is not
  deterministic -- 63,580 B and 70,024 B for the same 29 characters on
  2026-09-18), so comparing a relayed session against a direct one measures
  the model's variance and not the relay's fidelity. The counter comparison
  measures the relay: a frame dropped, a frame added or a frame re-serialised
  all move the two numbers apart. A direct session is still run, and its
  meter and header shape are reported beside the relayed one;
* `audioChunk.usage.processedCharactersCount`, summed over flushes, against
  the characters actually sent and against the gateway's own
  `llmgw_units_total{unit="characters"}` delta;
* `X-Gw-*` on the 101, including the session id the capture record is keyed
  by and the `X-Gw-Body-Modified` warrant for the `create.modelId` rewrite.

**The RIFF-header note.** Inworld's LINEAR16 stream begins the first chunk of
every flush with a 44-byte `RIFF…WAVE` header, and the tiny trailing chunk
carries a second one (probe 1: 54 bytes = header + 5 samples). The gateway
relays both untouched and the plugin strips them; this smoke asserts only
that the header is STILL THERE, because "the gateway did not helpfully strip
it" is the failure that would otherwise be invisible -- a raw-PCM consumer
would get 44 bytes of silence per chunk and nobody would know which layer
removed them.

Run: `LLMGW_ENV_FILE=/path/to/.env .venv/bin/python -m live.smoke_ws`
     `... -m live.smoke_ws --no-spend`   (routing and headers only)
     `... -m live.smoke_ws --only inworld-tts`
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import tempfile
from pathlib import Path

import httpx

from live import smoke as text_smoke
from live.env import load_env
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.server.app import build_app
from llmgw.server.config import ServerConfig

POLICY_TOML = """
default_workload = "tts_ws"

[defaults.budgets]
total = 120.0

[profiles.tts_session.budgets]
connect = 5.0
headers = 10.0
first_event = 2.0
progress = 5.0
client_stall = 10.0
idle = 600.0
session_total = 3600.0

[workloads.tts_ws]
incumbent = "inworld.tts-2-flash"
profile = "tts_session"
"""

TTS_ROUTE = "/tts/v1/voice:streamBidirectional"
INWORLD_WSS = "wss://api.inworld.ai" + TTS_ROUTE
TEXT = "Hello from the gateway probe."  # 29 characters, as in the captures
VOICE = "Aarav"
AUDIO_CONFIG = {"audioEncoding": "LINEAR16", "sampleRateHertz": 16000}

CASES = ("inworld-tts", "inworld-tts-multi")

# $15 per 1M characters on the `inworld.tts-2-flash` row (priced 2026-09-16).
PER_CHARACTER_USD = 15.0 / 1_000_000


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


def _create(context: str, model: str) -> str:
    return json.dumps({
        "create": {
            "modelId": model, "voiceId": VOICE, "audioConfig": AUDIO_CONFIG,
            "temperature": 0.8,
        },
        "contextId": context,
    })


class _Tally:
    """Bytes the client read off the socket, counted as they arrive.

    The independent witness for the relay's fidelity. `websockets` hands back
    a `str` for a TEXT frame, so the count is of its UTF-8 encoding -- which
    is what went over the wire and what `llmgw_ws_bytes_total` counted on the
    other side of the same frame."""

    def __init__(self) -> None:
        self.bytes = 0
        self.frames = 0

    def add(self, message: str | bytes) -> None:
        self.bytes += len(message.encode("utf-8") if isinstance(message, str)
                          else message)
        self.frames += 1


async def _one_utterance(ws, context: str, text: str, tally: _Tally) -> tuple[bytes, int]:
    """send_text + flush, read to `flushCompleted`. Returns (audio, chars)."""
    await ws.send(json.dumps({"send_text": {"text": text}, "contextId": context}))
    await ws.send(json.dumps({"flush_context": {}, "contextId": context}))
    audio = bytearray()
    characters = 0
    while True:
        raw = await asyncio.wait_for(ws.recv(), 30)
        tally.add(raw)
        frame = json.loads(raw)
        if "error" in frame:
            # The provider's message may quote the key's first characters.
            raise RuntimeError(
                f"provider error code {frame['error'].get('code')} "
                f"(message withheld: it can contain a key prefix)"
            )
        result = frame.get("result", {})
        status = result.get("status") or {}
        if status.get("code"):
            raise RuntimeError(f"in-context status code {status['code']}")
        if "audioChunk" in result:
            audio += base64.b64decode(result["audioChunk"]["audioContent"])
            characters += (result["audioChunk"].get("usage") or {}).get(
                "processedCharactersCount", 0,
            )
        if "flushCompleted" in result:
            return bytes(audio), characters


async def _session(url: str, headers: dict[str, str], *, contexts: list[str],
                   text: str, model: str):
    """One socket, N contexts, one utterance each. Returns the 101's headers,
    the audio per context, the characters the PROVIDER metered, and every
    byte the client read."""
    import websockets

    async with websockets.connect(
        url, additional_headers=headers, max_size=8 * 1024 * 1024,
        open_timeout=20, close_timeout=5,
    ) as ws:
        response_headers = dict(ws.response.headers)
        tally = _Tally()
        for context in contexts:
            await ws.send(_create(context, model))
        created = 0
        while created < len(contexts):
            raw = await asyncio.wait_for(ws.recv(), 30)
            tally.add(raw)
            frame = json.loads(raw)
            if "error" in frame:
                raise RuntimeError(
                    f"provider error code {frame['error'].get('code')} on create "
                    f"(message withheld)"
                )
            if "contextCreated" in frame.get("result", {}):
                created += 1
        audio: dict[str, bytes] = {}
        characters = 0
        for context in contexts:
            chunk, chars = await _one_utterance(ws, context, text, tally)
            audio[context] = chunk
            characters += chars
        for context in contexts:
            await ws.send(json.dumps({"close_context": {}, "contextId": context}))
            tally.add(await asyncio.wait_for(ws.recv(), 30))
        await ws.close(1000)
        return response_headers, audio, characters, tally


# ==========================================================================
# Cases
# ==========================================================================


class Case:
    def __init__(self, gateway, out) -> None:
        self.gw = gateway
        self.out = out
        self.spend = 0.0
        self.failures = 0

    def metrics(self) -> str:
        return httpx.get(f"{self.gw.base_url}/metrics", timeout=10.0).text

    def metrics_after(self, name: str, baseline: float, **labels: str) -> str:
        """Scrape once the session's terminal record has landed.

        A relayed session's units are counted when its record is written,
        which happens after the socket closes -- so a scrape taken the
        instant `close()` returns reads the value from before the session and
        reports a zero bill for a session that billed correctly. Polls for up
        to three seconds, then reports whatever it has (a genuinely missing
        record must FAIL, not hang)."""
        import time as _time

        deadline = _time.monotonic() + 3.0
        text = self.metrics()
        while _time.monotonic() < deadline:
            if _metric(text, name, **labels) != baseline:
                return text
            _time.sleep(0.1)
            text = self.metrics()
        return text

    def _ws_url(self) -> str:
        return self.gw.base_url.replace("http://", "ws://") + TTS_ROUTE

    def _headers(self) -> dict[str, str]:
        # The tenant credential in the shape the LiveKit plugin sends it. On
        # the zero-config path (no tenants file) the value is a label and is
        # not checked -- it is sent anyway so the shape under test is the one
        # Layrs will use.
        return {"Authorization": "Basic live-smoke-tenant"}

    def _report(self, name: str, ok: bool, detail: str) -> None:
        self.failures += 0 if ok else 1
        _print(f"  {name}: {detail} -> {'PASS' if ok else 'FAIL'}", self.out)

    def inworld_tts(self) -> None:
        """One context, one utterance, compared against a direct session."""
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-tts: SKIP (INWORLD_API_KEY absent)", self.out)
            return
        metrics_before = self.metrics()
        before = _metric(metrics_before, "llmgw_units_total",
                         unit="characters", model="inworld.tts-2-flash")
        bytes_before = _metric(metrics_before, "llmgw_ws_bytes_total",
                               direction="client_out", surface="inworld_tts_ws")
        try:
            headers, audio, metered, tally = asyncio.run(_session(
                self._ws_url(), self._headers(), contexts=["smoke-1"],
                text=TEXT, model="inworld-tts-1.5-mini",
            ))
        except Exception as exc:  # noqa: BLE001 - a live smoke reports, it does not raise
            self._report("inworld-tts", False, f"through the gateway: {exc!r}")
            return
        relayed = audio["smoke-1"]
        self.spend += metered * PER_CHARACTER_USD

        # The same utterance, straight at the provider.
        try:
            _, direct_audio, direct_metered, _ = asyncio.run(_session(
                INWORLD_WSS,
                {"Authorization": f"Basic {os.environ['INWORLD_API_KEY']}"},
                contexts=["smoke-direct"], text=TEXT, model="inworld-tts-1.5-mini",
            ))
        except Exception as exc:  # noqa: BLE001
            self._report("inworld-tts-direct", False, f"direct session: {exc!r}")
            return
        direct = direct_audio["smoke-direct"]
        self.spend += direct_metered * PER_CHARACTER_USD

        seconds = len(relayed) / (AUDIO_CONFIG["sampleRateHertz"] * 2)
        self._report(
            "inworld-tts-audio",
            relayed[:4] == b"RIFF" and 0.5 <= seconds <= 8.0,
            f"relayed={len(relayed)}B (~{seconds:.2f}s at "
            f"{AUDIO_CONFIG['sampleRateHertz']} Hz LINEAR16) riff_header_intact="
            f"{relayed[:4] == b'RIFF'}",
        )
        after_bytes = _metric(self.metrics(), "llmgw_ws_bytes_total",
                              direction="client_out", surface="inworld_tts_ws")
        relayed_bytes = after_bytes - bytes_before
        self._report(
            "inworld-tts-bytes", relayed_bytes == tally.bytes,
            f"gateway counted {relayed_bytes:g}B out, client read {tally.bytes}B "
            f"in {tally.frames} frames: a dropped, added or re-serialised frame "
            f"moves these apart",
        )
        self._report(
            "inworld-tts-direct", direct[:4] == b"RIFF" and direct_metered == metered,
            f"direct={len(direct)}B metered={direct_metered} vs relayed="
            f"{len(relayed)}B metered={metered}; audio length differs run to run "
            f"(synthesis is not deterministic) so only the meter is asserted",
        )
        self._report(
            "inworld-tts-meter", metered == len(TEXT),
            f"processedCharactersCount={metered} sent={len(TEXT)} "
            f"(direct reported {direct_metered})",
        )

        gw_headers = {k.lower(): v for k, v in headers.items()
                      if k.lower().startswith("x-gw-")}
        wanted = {"x-gw-served-by", "x-gw-model", "x-gw-workload-id",
                  "x-gw-policy-id", "x-gw-catalog-id", "x-gw-attempts",
                  "x-gw-session-id", "x-gw-body-modified"}
        missing = sorted(wanted - set(gw_headers))
        self._report(
            "inworld-tts-101-headers", not missing,
            f"served-by={gw_headers.get('x-gw-served-by')} "
            f"model={gw_headers.get('x-gw-model')} "
            f"session={gw_headers.get('x-gw-session-id', '')[:8]}... "
            f"body-modified={gw_headers.get('x-gw-body-modified')}"
            + (f" MISSING={missing}" if missing else ""),
        )

        after = _metric(
            self.metrics_after("llmgw_units_total", before,
                               unit="characters", model="inworld.tts-2-flash"),
            "llmgw_units_total", unit="characters", model="inworld.tts-2-flash",
        )
        billed = after - before
        self._report(
            "inworld-tts-billed", billed == len(TEXT),
            f"llmgw_units_total delta={billed:g} expected={len(TEXT)}",
        )
        exact = _metric(self.metrics(), "llmgw_cost_usd_total",
                        basis="exact", model="inworld.tts-2-flash")
        self._report(
            "inworld-tts-basis", exact > 0,
            f"cost recorded against basis=exact (${exact:.8f} cumulative)",
        )

    def inworld_tts_multi(self) -> None:
        """Two contexts interleaved on one socket: commitment is per context
        and the meter is a sum over flushes."""
        if not _has("INWORLD_API_KEY"):
            _print("  inworld-tts-multi: SKIP (INWORLD_API_KEY absent)", self.out)
            return
        before = _metric(self.metrics(), "llmgw_units_total",
                         unit="characters", model="inworld.tts-2-flash")
        try:
            _, audio, metered, _ = asyncio.run(_session(
                self._ws_url(), self._headers(),
                contexts=["smoke-a", "smoke-b"], text=TEXT,
                model="inworld-tts-1.5-mini",
            ))
        except Exception as exc:  # noqa: BLE001
            self._report("inworld-tts-multi", False, f"{exc!r}")
            return
        self.spend += metered * PER_CHARACTER_USD
        after = _metric(
            self.metrics_after("llmgw_units_total", before,
                               unit="characters", model="inworld.tts-2-flash"),
            "llmgw_units_total", unit="characters", model="inworld.tts-2-flash",
        )
        expected = 2 * len(TEXT)
        self._report(
            "inworld-tts-multi", metered == expected and (after - before) == expected,
            f"contexts=2 metered={metered} billed={after - before:g} "
            f"expected={expected} bytes={[len(v) for v in audio.values()]}",
        )


# ==========================================================================
# Runner
# ==========================================================================


def build_ws_app(policy_path: str):
    return build_app(ServerConfig(
        catalog=DEFAULT_CATALOG,
        fake_upstreams=False,
        policy_file=policy_path,
        default_model="inworld.tts-2-flash",
        forward_request_headers=("x-request-id",),
    ).validated())


def run(*, spend: bool = True, only: str | None = None, out=sys.stdout) -> float:
    with tempfile.TemporaryDirectory(prefix="llmgw-ws-") as tmp:
        policy = Path(tmp) / "ws.toml"
        policy.write_text(POLICY_TOML, encoding="utf-8")
        gw = text_smoke.serve(build_ws_app(str(policy)))
        try:
            _print(f"ws gateway on {gw.base_url}{TTS_ROUTE}", out)
            for env in ("INWORLD_API_KEY", "OPENAI_API_KEY", "ASSEMBLYAI_API_KEY",
                        "ELEVENLABS_API_KEY"):
                _print(f"  {env}: {'set' if _has(env) else 'ABSENT (cases skip)'}", out)
            _print("  products: inworld-tts [G1]; inworld-stt, openai-realtime, "
                   "assemblyai-streaming ABSENT (land in G2-G4)", out)
            if not spend:
                _print("  --no-spend: routing and key presence only, no sockets opened",
                       out)
                return 0.0
            case = Case(gw, out)
            if only in (None, "inworld-tts"):
                case.inworld_tts()
            if only in (None, "inworld-tts-multi"):
                case.inworld_tts_multi()
            _print(f"\nESTIMATED SPEND: ${case.spend:.6f}", out)
            if case.failures:
                _print(f"FAILURES: {case.failures}", out)
            return case.spend
        finally:
            gw.stop()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="live WebSocket smoke through the gateway",
    )
    ap.add_argument("--no-spend", action="store_true")
    ap.add_argument("--only", choices=CASES, default=None)
    args = ap.parse_args(argv)
    if os.environ.get("LLMGW_ENV_FILE"):
        load_env()  # names only are returned; values never printed
    run(spend=not args.no_spend, only=args.only)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

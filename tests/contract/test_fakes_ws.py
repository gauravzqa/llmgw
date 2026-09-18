"""The WebSocket fakes, calibrated over real sockets.

Same argument as `test_fakes.py`, one transport along. Every contract test the
gateway will write reads "the upstream was `die-mid-session`" and concludes
something about the relay; that conclusion is worth nothing unless
`die-mid-session` really leaves the client at 1006 and `stall-mid-session`
really goes quiet with the socket still open. So the instrument is calibrated
first, from a real `websockets` client, with NO gateway in the path.

One test per mode per product, plus the two behaviours that are not modes
(Inworld's non-fatal answer to a malformed frame, OpenAI's checks after the
101) and the counters contract C24 rests on.

The file targets a few seconds: a mode that intends to be silent for 300 s is
asserted by 150 ms of silence, which is the same proof a thousand times
cheaper.
"""

from __future__ import annotations

import asyncio
import base64
import json
import time

import pytest
from fakes import ws as fws
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract

TTS = fws.TTS_PATH
STT = fws.STT_PATH
RT = fws.REALTIME_PATH
AAI = fws.AAI_PATH

SILENCE = 0.15
"""How long "it sent nothing" is asserted for. Long enough to catch a frame
that is merely late (the fake's own scheduling is sub-millisecond), short
enough that thirty of these cost under ten seconds."""


def open_ws(fakes: Fakes, path: str, mode: str | None = None,
            query: str = "", **knobs: object):
    """A client connected straight at the fake, mode and knobs on the upgrade."""
    headers = {}
    if mode is not None:
        headers["X-Fake-Mode"] = mode
    for name, value in knobs.items():
        headers["X-Fake-" + name.replace("_", "-").title()] = str(value)
    url = f"ws://127.0.0.1:{fakes.openai.port}{path}{query}"
    return connect(url, additional_headers=headers, open_timeout=5, close_timeout=1)


async def recv_json(client, wait: float = 3.0) -> dict:
    return json.loads(await asyncio.wait_for(client.recv(), wait))


async def expect_silence(client, seconds: float = SILENCE) -> None:
    with pytest.raises(asyncio.TimeoutError):
        frame = await asyncio.wait_for(client.recv(), seconds)
        pytest.fail(f"expected silence, got {frame!r}")


async def expect_close(client, code: int) -> None:
    with pytest.raises(ConnectionClosed) as caught:
        for _ in range(200):
            await asyncio.wait_for(client.recv(), 3.0)
    assert caught.value.rcvd is not None, "expected a CLOSE frame, got an abort"
    assert caught.value.rcvd.code == code


def ws_stats(fakes: Fakes) -> dict:
    return fakes.stats()["ws"]


# ==========================================================================
# Inworld TTS
# ==========================================================================


async def test_tts_ok_replays_probe_1(fakes: Fakes):
    """Probe 1, end to end: `create` -> `contextCreated`, `send_text` +
    `flush_context` -> audio with the character count on the first chunk and a
    RIFF header on the first and last, `flushCompleted`, then `contextClosed`
    for `close_context` -- and no server close after it."""
    async with open_ws(fakes, TTS) as c:
        await c.send(json.dumps({
            "create": {"modelId": "inworld-tts-1.5-mini", "voiceId": "Aarav",
                       "audioConfig": {"audioEncoding": "LINEAR16",
                                       "sampleRateHertz": 16000}},
            "contextId": "ctx-32016aa9"}))
        created = await recv_json(c)
        assert created["result"]["contextId"] == "ctx-32016aa9"
        assert created["result"]["contextCreated"]["voiceId"] == "Aarav"

        await c.send(json.dumps({"send_text": {"text": "Hello from the gateway probe."},
                                 "contextId": "ctx-32016aa9"}))
        await c.send(json.dumps({"flush_context": {}, "contextId": "ctx-32016aa9"}))
        usages, audio = [], []
        while True:
            frame = (await recv_json(c))["result"]
            assert frame["contextId"] == "ctx-32016aa9"
            if "flushCompleted" in frame:
                break
            usages.append(frame["audioChunk"]["usage"]["processedCharactersCount"])
            audio.append(base64.b64decode(frame["audioChunk"]["audioContent"]))
        assert usages[0] == 29 and set(usages[1:]) == {0}
        assert audio[0][:4] == b"RIFF" and audio[-1][:4] == b"RIFF"
        assert len(audio[-1]) == 54

        await c.send(json.dumps({"close_context": {}, "contextId": "ctx-32016aa9"}))
        assert "contextClosed" in (await recv_json(c))["result"]
        await expect_silence(c)  # probe 1: the socket stays open afterwards

    stats = ws_stats(fakes)
    assert stats["ws_open"] == 1 and stats["ws_closed_by_client"] == 1
    assert stats["client_frames"] == {"create": 1, "send_text": 1,
                                      "flush_context": 1, "close_context": 1}
    assert stats["by_path"] == {TTS: 1}
    assert stats["terminates_received"] == 0, "close_context ends a context, not a session"


@pytest.mark.parametrize(("mode", "code"), [
    ("error-7-then-close-1000-on-first-message", 7),
    ("error-16-missing-credential", 16),
    ("auth-fail-in-band", 16),
])
async def test_tts_credential_faults_are_silent_until_the_first_client_frame(
    fakes: Fakes, mode: str, code: int
):
    """G0 item 1 / probes 2a-2c: the upgrade succeeds, nothing arrives however
    long you wait, and the verdict comes ~one round trip after the FIRST client
    frame, with a server CLOSE 1000 in the same instant."""
    async with open_ws(fakes, TTS, mode) as c:
        await expect_silence(c)
        await c.send(json.dumps({"create": {}, "contextId": "ctx-bad"}))
        error = (await recv_json(c))["error"]
        assert error["code"] == code
        await expect_close(c, 1000)
    assert ws_stats(fakes)["ws_closed_by_server"] == 1


async def test_tts_nonfatal_error_leaves_the_socket_usable(fakes: Fakes):
    """Probe 5: a code 3 with `status: INVALID_ARGUMENT` and no close; the
    `create` behind it is still served."""
    async with open_ws(fakes, TTS, "nonfatal-error") as c:
        await c.send(json.dumps({"create": {}, "contextId": "ctx-1"}))
        error = (await recv_json(c))["error"]
        assert error["code"] == 3 and error["status"] == "INVALID_ARGUMENT"
        assert "contextCreated" in (await recv_json(c))["result"]


async def test_tts_context_multiplex_limits_at_five_and_survives(fakes: Fakes):
    """Probe 4: five `contextCreated`, `status.code` 8 for the sixth, the
    socket open throughout, and `code` 5 for an operation on an unknown
    context."""
    async with open_ws(fakes, TTS, "context-multiplex") as c:
        for i in range(6):
            await c.send(json.dumps({"create": {}, "contextId": f"ctx-{i}"}))
        codes = []
        for _ in range(6):
            result = (await recv_json(c))["result"]
            codes.append(result["status"]["code"])
        assert codes == [0, 0, 0, 0, 0, 8]

        await c.send(json.dumps({"send_text": {"text": "ghost"},
                                 "contextId": "ctx-nope"}))
        status = (await recv_json(c))["result"]
        assert status["contextId"] == "ctx-nope"
        assert status["status"]["code"] == 5
        assert "(payload=SEND_TEXT)" in status["status"]["message"]

        await c.send(json.dumps({"create": {}, "contextId": "ctx-7"}))
        assert (await recv_json(c))["result"]["status"]["code"] == 8


async def test_tts_over_long_send_text_poisons_only_that_flush(fakes: Fakes):
    """Probe 5: `status` code 3 for 2,100 characters and NO `flushCompleted`
    after the flush that carried it; the next flush works."""
    async with open_ws(fakes, TTS) as c:
        await c.send(json.dumps({"create": {}, "contextId": "ctx-big"}))
        await recv_json(c)
        await c.send(json.dumps({"send_text": {"text": "x" * 2100},
                                 "contextId": "ctx-big"}))
        status = (await recv_json(c))["result"]["status"]
        assert status["code"] == 3
        assert status["message"] == "text length should not exceed 2000 characters."
        await c.send(json.dumps({"flush_context": {}, "contextId": "ctx-big"}))
        await expect_silence(c)

        await c.send(json.dumps({"send_text": {"text": "ok now"},
                                 "contextId": "ctx-big"}))
        await c.send(json.dumps({"flush_context": {}, "contextId": "ctx-big"}))
        assert "audioChunk" in (await recv_json(c))["result"]


async def test_tts_malformed_frames_are_answered_but_not_fatal(fakes: Fakes):
    """Probe 5, in one socket: non-JSON and BINARY each draw the code 3
    `INVALID_ARGUMENT` error, a well-formed frame the server does not
    recognise draws NOTHING, and the socket survives all three."""
    async with open_ws(fakes, TTS) as c:
        await c.send("this is not json")
        assert (await recv_json(c))["error"]["code"] == 3
        await c.send(b"\x00" * 400)
        assert (await recv_json(c))["error"]["code"] == 3
        await c.send(json.dumps({"foo": "bar"}))
        await expect_silence(c)
        await c.send(json.dumps({"create": {}, "contextId": "ctx-1"}))
        assert "contextCreated" in (await recv_json(c))["result"]
    frames = ws_stats(fakes)["client_frames"]
    assert frames == {"invalid-json": 1, "binary": 1, "other": 1, "create": 1}


async def test_tts_unsupported_model_is_fatal_like_probe_6c(fakes: Fakes):
    async with open_ws(fakes, TTS) as c:
        await c.send(json.dumps({"create": {"modelId": "inworld/no-such-model"},
                                 "contextId": "ctx-1"}))
        error = (await recv_json(c))["error"]
        assert error["code"] == 3 and "Unsupported model" in error["message"]
        await expect_close(c, 1000)


async def test_tts_usage_in_termination_answers_every_close_context(fakes: Fakes):
    async with open_ws(fakes, TTS, "usage-in-termination") as c:
        for i in range(3):
            await c.send(json.dumps({"create": {}, "contextId": f"ctx-{i}"}))
            await recv_json(c)
        for i in range(3):
            await c.send(json.dumps({"close_context": {}, "contextId": f"ctx-{i}"}))
        closed = {(await recv_json(c))["result"]["contextId"] for _ in range(3)}
        assert closed == {"ctx-0", "ctx-1", "ctx-2"}


async def test_tts_terminate_then_hang_never_answers_close_context(fakes: Fakes):
    """The drain-wait bound: the gateway asks, nothing comes back, and the
    socket is not closed either."""
    async with open_ws(fakes, TTS, "terminate-then-hang") as c:
        await c.send(json.dumps({"create": {}, "contextId": "ctx-1"}))
        await recv_json(c)
        await c.send(json.dumps({"close_context": {}, "contextId": "ctx-1"}))
        await expect_silence(c)
        assert ws_stats(fakes)["ws_open_now"] == 1


async def test_tts_contexts_interleave_on_the_wire(fakes: Fakes):
    """Probe 4: B's audio arrives while A is still streaming. The fake emits
    each flush on its own task, so a gateway that serialises contexts would
    show up here as a clean A-then-B ordering."""
    async with open_ws(fakes, TTS, interval=0.01, events=4) as c:
        for cid in ("ctx-A", "ctx-B"):
            await c.send(json.dumps({"create": {}, "contextId": cid}))
            await recv_json(c)
        for cid in ("ctx-A", "ctx-B"):
            await c.send(json.dumps({"send_text": {"text": "Context speaking."},
                                     "contextId": cid}))
            await c.send(json.dumps({"flush_context": {}, "contextId": cid}))
        order = []
        while len(order) < 12:
            order.append((await recv_json(c))["result"]["contextId"])
        assert set(order) == {"ctx-A", "ctx-B"}
        assert order != sorted(order), "the two contexts never interleaved"


async def test_an_unknown_mode_is_refused_on_the_upgrade(fakes: Fakes):
    """Never silently `ok`: a typo'd mode fails the handshake."""
    with pytest.raises(InvalidStatus) as caught:
        async with open_ws(fakes, TTS, "ok-typo"):
            pass
    assert caught.value.response.status_code == 403
    with pytest.raises(InvalidStatus):  # a real mode, wrong product
        async with open_ws(fakes, RT, "context-multiplex"):
            pass
    assert ws_stats(fakes)["ws_open"] == 0, "a refused upgrade is not an open socket"


# ==========================================================================
# Inworld STT
# ==========================================================================


async def test_stt_ok_replays_probe_6(fakes: Fakes):
    """Probe 6: `transcribeConfig` is NOT acknowledged, `speechStarted` comes
    once audio flows, `endTurn` draws the final transcript, and the single
    `usage` frame arrives after `closeStream` -- with the socket still open."""
    async with open_ws(fakes, STT, events=8) as c:
        await c.send(json.dumps({"transcribeConfig": {
            "modelId": "inworld/inworld-stt-1", "audioEncoding": "LINEAR16",
            "sampleRateHertz": 16000, "numberOfChannels": 1, "language": "en-US"}}))
        await expect_silence(c)

        chunk = base64.b64encode(b"\x00" * 3200).decode()
        await c.send(json.dumps({"audioChunk": {"content": chunk}}))
        assert "speechStarted" in (await recv_json(c))["result"]
        for _ in range(8):
            await c.send(json.dumps({"audioChunk": {"content": chunk}}))
        interim = (await recv_json(c))["result"]["transcription"]
        assert interim["isFinal"] is False

        await c.send(json.dumps({"endTurn": {}}))
        final = (await recv_json(c))["result"]["transcription"]
        assert final["isFinal"] is True
        assert final["transcript"] == "Hello from the Gateway Probe."

        await c.send(json.dumps({"closeStream": {}}))
        usage = (await recv_json(c))["result"]["usage"]
        assert usage["transcribedAudioMs"] > 0
        assert usage["modelId"] == "inworld/inworld-stt-1"
        await expect_silence(c)  # probe 6: no server close after `usage`

    stats = ws_stats(fakes)
    assert stats["terminates_received"] == 1
    assert stats["client_frames"]["audioChunk"] == 9
    assert stats["client_frames"]["transcribeConfig"] == 1


async def test_stt_usage_tracks_the_audio_that_was_sent(fakes: Fakes):
    """Not the provider's number (no fake can recognise speech) but the
    gateway's relay of it: more audio, more milliseconds."""
    async def run(chunks: int) -> int:
        async with open_ws(fakes, STT, events=0) as c:
            await c.send(json.dumps({"transcribeConfig": {"sampleRateHertz": 16000}}))
            for _ in range(chunks):
                await c.send(json.dumps({"audioChunk": {
                    "content": base64.b64encode(b"\x00" * 3200).decode()}}))
            await recv_json(c)  # speechStarted
            await c.send(json.dumps({"closeStream": {}}))
            return (await recv_json(c))["result"]["usage"]["transcribedAudioMs"]

    assert await run(5) < await run(20)


@pytest.mark.parametrize("mode", ["error-7-then-close-1000-on-first-message",
                                  "error-16-missing-credential",
                                  "auth-fail-in-band"])
async def test_stt_credential_faults_wait_for_the_first_frame_too(
    fakes: Fakes, mode: str
):
    async with open_ws(fakes, STT, mode) as c:
        await expect_silence(c)
        await c.send(json.dumps({"transcribeConfig": {}}))
        assert (await recv_json(c))["error"]["code"] in (7, 16)
        await expect_close(c, 1000)


async def test_stt_nonfatal_error_does_not_stop_the_stream(fakes: Fakes):
    async with open_ws(fakes, STT, "nonfatal-error") as c:
        await c.send(json.dumps({"transcribeConfig": {}}))
        assert (await recv_json(c))["error"]["code"] == 3
        await c.send(json.dumps({"audioChunk": {"content": ""}}))
        assert "speechStarted" in (await recv_json(c))["result"]


async def test_stt_bad_model_is_probe_6c(fakes: Fakes):
    async with open_ws(fakes, STT) as c:
        await c.send(json.dumps({
            "transcribeConfig": {"modelId": "inworld/no-such-model"}}))
        assert "Unsupported model" in (await recv_json(c))["error"]["message"]
        await expect_close(c, 1000)


async def test_stt_terminate_then_hang_swallows_close_stream(fakes: Fakes):
    """S12's bound: the terminate is COUNTED (so the drain can be asserted)
    and then ignored."""
    async with open_ws(fakes, STT, "terminate-then-hang") as c:
        await c.send(json.dumps({"transcribeConfig": {}}))
        await c.send(json.dumps({"closeStream": {}}))
        await expect_silence(c)
    assert ws_stats(fakes)["terminates_received"] == 1


async def test_stt_usage_in_termination_is_the_usage_frame(fakes: Fakes):
    async with open_ws(fakes, STT, "usage-in-termination") as c:
        await c.send(json.dumps({"transcribeConfig": {"sampleRateHertz": 16000}}))
        await c.send(json.dumps({"audioChunk": {
            "content": base64.b64encode(b"\x00" * 32_000).decode()}}))
        await recv_json(c)
        await c.send(json.dumps({"closeStream": {}}))
        assert (await recv_json(c))["result"]["usage"]["transcribedAudioMs"] == 1000


# ==========================================================================
# OpenAI Realtime
# ==========================================================================


async def test_realtime_ok_replays_probe_8(fakes: Fakes):
    """The 14-event response cycle, `response.done` with the captured usage,
    and `rate_limits.updated` AFTER it -- so `response.done` is a terminal but
    not the last frame of the cycle."""
    async with open_ws(fakes, RT, query="?model=gpt-realtime-mini") as c:
        created = await recv_json(c)
        assert created["type"] == "session.created"
        assert created["session"]["model"] == "gpt-realtime-mini"

        await c.send(json.dumps({"type": "session.update",
                                 "session": {"type": "realtime",
                                             "output_modalities": ["text"]}}))
        updated = await recv_json(c)
        assert updated["type"] == "session.updated"
        assert updated["session"]["output_modalities"] == ["text"]

        await c.send(json.dumps({"type": "conversation.item.create", "item": {
            "type": "message", "role": "user",
            "content": [{"type": "input_text", "text": "Say hi in three words"}]}}))
        await c.send(json.dumps({"type": "response.create"}))
        types, done_at = [], None
        while True:
            frame = await recv_json(c)
            types.append(frame["type"])
            if frame["type"] == "response.done":
                done_at = time.monotonic()
                usage = frame["response"]["usage"]
            if frame["type"] == "rate_limits.updated":
                break
        assert types[-2:] == ["response.done", "rate_limits.updated"]
        assert time.monotonic() - done_at >= fws.RATE_LIMITS_AFTER_DONE_S * 0.5
        assert usage["total_tokens"] == 127
        assert usage["input_token_details"]["cached_tokens"] == 64
        assert "response.output_text.delta" in types


async def test_realtime_transcription_session_replays_probe_7(fakes: Fakes):
    async with open_ws(fakes, RT, query="?intent=transcription") as c:
        session = (await recv_json(c))["session"]
        assert session["object"] == "realtime.transcription_session"
        assert session["audio"]["input"]["transcription"] is None

        await c.send(json.dumps({"type": "session.update", "session": {
            "type": "transcription",
            "audio": {"input": {"format": {"type": "audio/pcm", "rate": 24000},
                                "transcription": {"model": "gpt-4o-mini-transcribe"},
                                "turn_detection": {"type": "server_vad"}}}}}))
        updated = await recv_json(c)
        assert (updated["session"]["audio"]["input"]["transcription"]["model"]
                == "gpt-4o-mini-transcribe")

        audio = base64.b64encode(b"\x00" * 4800).decode()
        await c.send(json.dumps({"type": "input_audio_buffer.append", "audio": audio}))
        assert (await recv_json(c))["type"] == "input_audio_buffer.speech_started"
        await c.send(json.dumps({"type": "input_audio_buffer.commit"}))
        types = []
        while not types or types[-1] != (
                "conversation.item.input_audio_transcription.completed"):
            types.append((await recv_json(c))["type"])
        assert types[0] == "input_audio_buffer.committed"
        await expect_silence(c)  # probe 7: no `rate_limits.updated` here


async def test_realtime_auth_failure_is_after_the_101_with_close_3000(fakes: Fakes):
    """G0 item 7 / probe 10: there is no pre-101 401 on Realtime."""
    async with open_ws(fakes, RT, "auth-fail-in-band",
                       query="?model=gpt-realtime-mini") as c:
        error = (await recv_json(c))["error"]
        assert error["code"] == "invalid_api_key"
        await expect_close(c, 3000)


@pytest.mark.parametrize(("query", "headers", "code"), [
    ("", {}, "missing_model"),
    ("?model=gpt-nope", {}, "invalid_model"),
    ("?model=gpt-realtime-mini", {"OpenAI-Beta": "realtime=v1"},
     "beta_api_shape_disabled"),
])
async def test_realtime_shape_failures_are_error_plus_close_4000(
    fakes: Fakes, query: str, headers: dict, code: str
):
    """Probes 7c, 11, 11b: all three upgrade fine and die at 4000."""
    url = f"ws://127.0.0.1:{fakes.openai.port}{RT}{query}"
    async with connect(url, additional_headers=headers, open_timeout=5,
                       close_timeout=1) as c:
        assert (await recv_json(c))["error"]["code"] == code
        await expect_close(c, 4000)


async def test_realtime_bad_events_are_nonfatal(fakes: Fakes):
    """Probe 8: four bad inputs, four `error` events, and the next
    `response.create` completes normally on the same socket."""
    async with open_ws(fakes, RT, query="?model=gpt-realtime-mini") as c:
        await recv_json(c)
        await c.send(json.dumps({"type": "nope"}))
        assert (await recv_json(c))["error"]["code"] == "invalid_value"
        await c.send("not json")
        assert (await recv_json(c))["error"]["code"] == "invalid_json"
        await c.send(b"\x00" * 64)
        assert (await recv_json(c))["error"]["code"] == "invalid_event"
        await c.send(json.dumps({"type": "response.create"}))
        types = []
        while "response.done" not in types:
            types.append((await recv_json(c))["type"])
        assert "response.created" in types


async def test_realtime_nonfatal_error_mode_still_serves(fakes: Fakes):
    async with open_ws(fakes, RT, "nonfatal-error",
                       query="?model=gpt-realtime-mini") as c:
        assert (await recv_json(c))["type"] == "session.created"
        assert (await recv_json(c))["type"] == "error"
        await c.send(json.dumps({"type": "response.create"}))
        types = []
        while "response.done" not in types:
            types.append((await recv_json(c))["type"])


async def test_realtime_usage_in_termination_is_response_done(fakes: Fakes):
    async with open_ws(fakes, RT, "usage-in-termination",
                       query="?model=gpt-realtime-mini") as c:
        await recv_json(c)
        await c.send(json.dumps({"type": "response.create"}))
        while True:
            frame = await recv_json(c)
            if frame["type"] == "response.done":
                assert frame["response"]["usage"]["output_tokens"] == 7
                break


async def test_realtime_terminate_then_hang_never_closes(fakes: Fakes):
    """OpenAI has no terminate message, so the mode's promise is the other
    half: whatever the client does, the fake will not close the socket."""
    async with open_ws(fakes, RT, "terminate-then-hang",
                       query="?model=gpt-realtime-mini") as c:
        await recv_json(c)
        await c.send(json.dumps({"type": "session.close"}))
        await expect_silence(c)
        assert ws_stats(fakes)["ws_open_now"] == 1


# ==========================================================================
# AssemblyAI (docs only -- voice-assemblyai.md)
# ==========================================================================


async def test_assemblyai_ok_begins_turns_and_terminates(fakes: Fakes):
    """voice-assemblyai.md 2: `Begin` first, binary audio in, partial `Turn`s,
    `ForceEndpoint` for a final one, and `Termination` with both durations
    after `Terminate`."""
    async with open_ws(fakes, AAI, events=2,
                       query="?speech_model=universal-streaming-english") as c:
        begin = await recv_json(c)
        assert begin["type"] == "Begin"
        assert begin["configuration"]["model"] == "universal-streaming-english"

        for _ in range(4):
            await c.send(b"\x00" * 3200)
        assert [(await recv_json(c))["turn_order"] for _ in range(2)] == [1, 2]

        await c.send(json.dumps({"type": "ForceEndpoint"}))
        final = await recv_json(c)
        assert final["type"] == "Turn" and final["end_of_turn"] is True

        await c.send(json.dumps({"type": "Terminate"}))
        termination = await recv_json(c)
        assert termination["type"] == "Termination"
        assert termination["audio_duration_seconds"] > 0
        assert termination["session_duration_seconds"] > 0
        await expect_close(c, 1000)

    stats = ws_stats(fakes)
    assert stats["terminates_received"] == 1
    assert stats["client_frames"]["binary"] == 4
    assert stats["client_frames"]["ForceEndpoint"] == 1


@pytest.mark.parametrize("mode", ["close-1008-with-error-frame",
                                  "close-1008-without",
                                  "auth-fail-in-band"])
async def test_assemblyai_1008_both_ways(fakes: Fakes, mode: str):
    """voice-assemblyai.md 5: 1008 covers auth AND the opens limit, and the
    `Error` frame that would disambiguate it may or may not arrive. Both
    shapes exist so the gateway's classifier can be tested against each."""
    async with open_ws(fakes, AAI, mode) as c:
        frames = []
        with pytest.raises(ConnectionClosed) as caught:
            if mode != "auth-fail-in-band":
                frames.append(await recv_json(c))  # Begin
                await c.send(json.dumps({"type": "KeepAlive"}))
            for _ in range(5):
                frames.append(await asyncio.wait_for(c.recv(), 3.0))
        assert caught.value.rcvd.code == 1008
        has_error = any('"Error"' in f if isinstance(f, str) else
                        f.get("type") == "Error" for f in frames)
        assert has_error is (mode != "close-1008-without")


async def test_assemblyai_nonfatal_error_has_no_close(fakes: Fakes):
    async with open_ws(fakes, AAI, "nonfatal-error") as c:
        assert (await recv_json(c))["type"] == "Begin"
        assert (await recv_json(c))["type"] == "Error"
        await expect_silence(c)
        assert ws_stats(fakes)["ws_open_now"] == 1


async def test_assemblyai_terminate_then_hang_counts_but_never_answers(fakes: Fakes):
    async with open_ws(fakes, AAI, "terminate-then-hang") as c:
        await recv_json(c)
        await c.send(json.dumps({"type": "Terminate"}))
        await expect_silence(c)
    assert ws_stats(fakes)["terminates_received"] == 1


async def test_assemblyai_usage_in_termination_carries_the_billing_number(
    fakes: Fakes
):
    async with open_ws(fakes, AAI, "usage-in-termination") as c:
        await recv_json(c)
        await c.send(b"\x00" * 32_000)
        await c.send(json.dumps({"type": "Terminate"}))
        termination = await recv_json(c)
        assert termination["audio_duration_seconds"] == pytest.approx(1.0, abs=0.01)


# ==========================================================================
# The transport-shaped modes, once per product
# ==========================================================================


async def _warm_tts(c) -> None:
    pass


async def _ping_tts(c, i: int) -> None:
    await c.send(json.dumps({"create": {}, "contextId": f"ctx-{i}"}))


async def _warm_stt(c) -> None:
    await c.send(json.dumps({"transcribeConfig": {"sampleRateHertz": 16000}}))


async def _ping_stt(c, i: int) -> None:
    await c.send(json.dumps({"audioChunk": {
        "content": base64.b64encode(b"\x00" * 1500).decode()}}))


async def _warm_rt(c) -> None:
    await recv_json(c)  # session.created


async def _ping_rt(c, i: int) -> None:
    await c.send(json.dumps({"type": "session.update", "session": {"foo": i}}))


async def _warm_aai(c) -> None:
    await recv_json(c)  # Begin


async def _ping_aai(c, i: int) -> None:
    await c.send(b"\x00" * 1500)


PRODUCTS = {
    "inworld-tts": (TTS, "", _warm_tts, _ping_tts, 0),
    "inworld-stt": (STT, "", _warm_stt, _ping_stt, 0),
    "openai-realtime": (RT, "?model=gpt-realtime-mini", _warm_rt, _ping_rt, 1),
    "assemblyai": (AAI, "", _warm_aai, _ping_aai, 1),
}
"""path, query, warm-up, "one frame in, one frame out", and how many server
frames the warm-up already consumed."""


@pytest.mark.parametrize("product", list(PRODUCTS))
async def test_idle_accepts_and_says_nothing_ever(fakes: Fakes, product: str):
    """S10's mode: 2,000 of these have to cost nothing but memory."""
    path, query, warm, ping, _ = PRODUCTS[product]
    async with open_ws(fakes, path, "idle", query=query, events=1) as c:
        await warm(c)  # the unsolicited first frame, if the product has one
        for i in range(3):
            await ping(c, i)
        await expect_silence(c)
        assert ws_stats(fakes)["ws_open_now"] == 1
    assert ws_stats(fakes)["ws_closed_by_client"] == 1


@pytest.mark.parametrize("product", list(PRODUCTS))
async def test_queued_before_begin_withholds_the_first_frame(
    fakes: Fakes, product: str
):
    path, query, warm, ping, _ = PRODUCTS[product]
    started = time.monotonic()
    async with open_ws(fakes, path, "queued-before-begin", query=query,
                       events=1, delay=0.5) as c:
        await ping(c, 0)
        await recv_json(c, wait=5.0)
        assert time.monotonic() - started >= 0.45


@pytest.mark.parametrize("product", list(PRODUCTS))
async def test_stall_mid_session_goes_quiet_with_the_socket_open(
    fakes: Fakes, product: str
):
    """Two frames, then nothing, forever -- the provider-stall side of the
    blame question, with the socket still up so the gateway's progress clock
    is the only thing that can end it."""
    path, query, warm, ping, unsolicited = PRODUCTS[product]
    async with open_ws(fakes, path, "stall-mid-session", query=query, events=1,
                       stall_after=2) as c:
        await warm(c)
        for i in range(5):
            await ping(c, i)
        for _ in range(2 - unsolicited):
            await recv_json(c)
        await expect_silence(c)
        assert ws_stats(fakes)["ws_open_now"] == 1


@pytest.mark.parametrize("product", list(PRODUCTS))
async def test_stall_side_both_stops_reading_too(fakes: Fakes, product: str):
    """`X-Fake-Stall-Side: both` is the OTHER half of the stall: the fake stops
    draining, so the gateway's outbound buffer is what fills. Asserted by the
    counters: the frames sent after the stall are never accounted."""
    path, query, warm, ping, unsolicited = PRODUCTS[product]
    async with open_ws(fakes, path, "stall-mid-session", query=query, events=1,
                       stall_after=2, stall_side="both") as c:
        await warm(c)
        for i in range(2):
            await ping(c, i)
        for _ in range(2 - unsolicited):
            await recv_json(c)
        await expect_silence(c)
        before = ws_stats(fakes)["frames_in"]
        for i in range(5):
            await ping(c, 10 + i)
        await asyncio.sleep(SILENCE)
        # At most ONE more: the fake discovers the stall when it next tries to
        # SEND, so whichever frame it was already reading gets drained and the
        # other four sit in the socket. Five out, one in, is the property.
        assert ws_stats(fakes)["frames_in"] <= before + 1


@pytest.mark.parametrize("product", list(PRODUCTS))
async def test_die_mid_session_leaves_the_client_at_1006(fakes: Fakes, product: str):
    """No CLOSE frame, a truncated one on the wire: `websockets` reports 1006
    with no `rcvd`, which is what a provider falling over looks like."""
    path, query, warm, ping, unsolicited = PRODUCTS[product]
    async with open_ws(fakes, path, "die-mid-session", query=query, events=1,
                       stall_after=1) as c:
        await warm(c)
        with pytest.raises(ConnectionClosed) as caught:
            for i in range(6):
                await ping(c, i)
                await asyncio.wait_for(c.recv(), 3.0)
        assert caught.value.rcvd is None, "a CLOSE frame arrived; this was not an abort"
        assert c.protocol.close_code == 1006
    assert ws_stats(fakes)["ws_closed_by_server"] == 1


@pytest.mark.parametrize("product", list(PRODUCTS))
async def test_slow_consumer_drains_at_the_configured_rate(fakes: Fakes, product: str):
    """1,500 bytes per frame at 15,000 B/s is 100 ms of read per frame: four
    frames cannot all be answered inside 300 ms."""
    path, query, warm, ping, _ = PRODUCTS[product]
    async with open_ws(fakes, path, "slow-consumer", query=query, events=1,
                       bytes=1500, read_bps=15_000) as c:
        await warm(c)
        started = time.monotonic()
        for i in range(4):
            await ping(c, i)
        for _ in range(4):
            await recv_json(c, wait=5.0)
        assert time.monotonic() - started >= 0.3


# ==========================================================================
# The counters C24 rests on
# ==========================================================================


async def test_client_frames_are_counted_by_kind_and_bytes(fakes: Fakes):
    """C24 is asserted from OUTSIDE the gateway: "the config prefix was
    replayed exactly once and content was never replayed" is a statement about
    what the losing upstream saw, which is exactly what these counters are."""
    async with open_ws(fakes, TTS) as c:
        await c.send(json.dumps({"create": {}, "contextId": "ctx-1"}))
        await recv_json(c)
        await c.send(json.dumps({"send_text": {"text": "hello"},
                                 "contextId": "ctx-1"}))
    stats = ws_stats(fakes)
    assert stats["client_frames"]["create"] == 1
    assert stats["client_frames"]["send_text"] == 1
    assert stats["client_bytes"]["create"] > 0
    assert stats["bytes_in"] == sum(stats["client_bytes"].values())
    assert stats["frames_in"] == sum(stats["client_frames"].values())
    assert stats["bytes_out"] > 0 and stats["frames_out"] == 1


async def test_a_config_only_socket_proves_content_was_never_relayed(fakes: Fakes):
    """The shape the fallback test will use: row one saw the `create` and
    nothing else."""
    async with open_ws(fakes, TTS, "error-7-then-close-1000-on-first-message") as c:
        await c.send(json.dumps({"create": {}, "contextId": "ctx-1"}))
        await recv_json(c)
        await expect_close(c, 1000)
    stats = ws_stats(fakes)
    assert stats["client_frames"] == {"create": 1}
    assert "send_text" not in stats["client_frames"]


async def test_open_and_closed_counts_balance_across_products(fakes: Fakes):
    for path, query, warm, _ping, _ in PRODUCTS.values():
        async with open_ws(fakes, path, query=query) as c:
            await warm(c)
    stats = ws_stats(fakes)
    assert stats["ws_open"] == 4
    assert stats["ws_open_now"] == 0
    assert stats["ws_closed_by_client"] + stats["ws_closed_by_server"] == 4
    assert stats["by_path"] == {TTS: 1, STT: 1, RT: 1, AAI: 1}
    assert stats["by_mode"] == {"ok": 4}

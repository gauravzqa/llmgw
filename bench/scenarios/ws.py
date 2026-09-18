"""The S9-S12 WebSocket scale scenarios (PLAN-G 8.3), as config the harness runs.

Same shape as the S1-S8 registry next door: a frozen dataclass per scenario
describing the offered load, the fake knobs that give it that shape, which arms
it can run in, and -- named explicitly -- the numbers it exists to produce and
the criteria it is scored against.

What differs from S1-S8, and why these live in their own registry and their own
driver (`bench/ws_load.py`) rather than in `bench/load.py`:

* the load is CLOSED, not open. S9 is "200 sessions, each streaming for five
  minutes", not "200 arrivals a second". There is no Poisson arrival process to
  configure and no `rate`; `sessions` and `ramp_s` take its place.
* the unit of measurement is a FRAME, in a named direction, on a socket that
  outlives thousands of them -- not a request with one first-event latency and
  one total.
* S10 and S11 measure a process (RSS, fds, tasks, stderr bytes) with the load
  deliberately doing nothing at all, which the request-shaped worker has no way
  to express.

`bench/load.py`'s `SCENARIOS` is deliberately left alone: S1-S8 are re-run after
G1 and G5 for comparison against the 10-15 Sep campaign, and the value of that
comparison depends on the instrument not having changed underneath it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

TTS_PATH = "/tts/v1/voice:streamBidirectional"
STT_PATH = "/stt/v1/transcribe:streamBidirectional"
REALTIME_PATH = "/v1/realtime"
AAI_PATH = "/v3/ws"

PATHS: dict[str, str] = {
    "inworld-tts": TTS_PATH,
    "inworld-stt": STT_PATH,
    "openai-realtime": REALTIME_PATH,
    "assemblyai": AAI_PATH,
}
"""The plugin-facing route per product. The gateway registers the same four
paths (PLAN-G 3.1-3.4), so one URL template serves both arms: Arm D points its
authority at the fake, Arm G at the gateway."""


@dataclass(frozen=True)
class Group:
    """One population of identically-shaped sessions inside a scenario.

    A scenario can have several: S9 is 200 STT-shaped sessions pushing audio up
    and 200 TTS-shaped sessions pulling audio down, at once, because the
    question is what the gateway does with both directions saturated.
    """

    label: str
    product: str                  # a key of PATHS
    sessions: int
    mode: str = "ok"
    # Fake knobs (X-Fake-*): the SERVER side of the shape.
    interval: float = 0.0         # seconds between server content frames
    nbytes: int = 3200            # decoded bytes per server audio chunk
    events: int = 1               # content frames per unit of work
    # The CLIENT side of the shape.
    send_interval: float = 0.0    # seconds between client frames (0 = none)
    send_bytes: int = 3200        # decoded bytes per client audio frame
    text_chars: int = 240         # TTS: characters per `send_text`
    query: str = ""               # extra query string (OpenAI needs `?model=`)

    @property
    def path(self) -> str:
        return PATHS[self.product]

    def headers(self) -> dict[str, str]:
        """The `X-Fake-*` shape. On Arm G these are forwarded by the gateway
        (`bench/_gwproc.py` FAKE_FORWARD), so both arms drive the same fake
        behaviour rather than Arm G silently getting the default mode."""
        return {
            "X-Fake-Mode": self.mode,
            "X-Fake-Interval": str(self.interval),
            "X-Fake-Bytes": str(self.nbytes),
            "X-Fake-Events": str(self.events),
        }

    def scaled(self, factor: float) -> Group:
        """The same shape with fewer sessions, for `--smoke` and for a laptop."""
        from dataclasses import replace
        return replace(self, sessions=max(1, int(round(self.sessions * factor))))


@dataclass(frozen=True)
class WsScenario:
    sid: str
    name: str
    question: str
    produces: str
    passes: tuple[str, ...]       # the PLAN's pass criteria, verbatim
    driver: str                   # s9_throughput | s10_idle | s11_disconnect
                                  # | s12_deploy
    groups: tuple[Group, ...]
    warm_s: float = 10.0
    measure_s: float = 300.0
    ramp_s: float = 10.0
    arms: tuple[str, ...] = ("D", "G")
    unlimited: bool = True
    sigterm_at: float | None = None
    smoke_sessions: int = 4
    smoke_measure_s: float = 8.0
    notes: str = ""
    extra: dict = field(default_factory=dict)

    def total_sessions(self) -> int:
        return sum(g.sessions for g in self.groups)


_S: dict[str, WsScenario] = {}


def _reg(s: WsScenario) -> None:
    _S[s.sid] = s


# S9. 43 KB/s in is 10 x 3,200 B of LINEAR16 a second, base64'd on the wire
# (4,268 chars a frame = 42.7 KB/s), which is the Inworld STT shape from
# captures-ws.md probe 6 run continuously. 64 KB/s out is 10 x 6,400 decoded
# bytes a second, the TTS line from probe 1 at a round rate. `events` on the
# TTS group is the number of chunks in ONE flush: a single `flush_context`
# streams for the whole window, which is what a five-minute measurement of
# server->client cadence needs.
_reg(WsScenario(
    sid="S9", name="64 KB/s each direction",
    question="What does the relay add to a frame, per direction, with both "
             "directions saturated -- and does every byte arrive?",
    produces="added frame latency p50/p99 per direction (up: client frame to "
             "its acknowledgement; down: lateness of a paced audio chunk "
             "against its cadence); CPU per session; RSS; bytes relayed vs "
             "bytes sent, both arms.",
    passes=(
        "p99 added latency <= 10 ms at 1 process",
        "zero byte loss (client bytes sent == fake bytes in; fake bytes out "
        "== client bytes received)",
        "CPU per session recorded for the max_streams re-derivation",
    ),
    driver="s9_throughput",
    measure_s=300.0,
    groups=(
        Group(label="stt-in", product="inworld-stt", sessions=200,
              interval=0.0, nbytes=3200, events=10,
              send_interval=0.1, send_bytes=3200),
        Group(label="tts-out", product="inworld-tts", sessions=200,
              interval=0.1, nbytes=6400, events=3000),
    ),
))

# S10. The Realtime idle socket from probe 9: `session.created`, then nothing
# but a ~20 s server PING. uvicorn's own keepalive is the fake's ping (see
# fakes/ws.py); the point of the scenario is what 2,000 of them cost.
_reg(WsScenario(
    sid="S10", name="Thousands of idle sockets",
    question="What does an idle relayed socket cost in RSS, fds and tasks?",
    produces="marginal RSS per idle socket; fds per session; task count; "
             "CPU spent answering pings.",
    passes=(
        "<= 100 KiB marginal RSS per idle socket",
        "fds = 2 per session + baseline (Arm G; 1 per session on Arm D)",
        "no idle close before the `idle` budget",
    ),
    driver="s10_idle",
    measure_s=600.0,
    ramp_s=30.0,
    groups=(
        Group(label="realtime-idle", product="openai-realtime", sessions=2000,
              mode="idle", query="?model=gpt-realtime-mini"),
    ),
    notes="2,000 sockets needs `ulimit -n` above ~6,000 on the driver: two fds "
          "per session on the gateway plus one per session here.",
))

# S11. Arm G only: the question is about the GATEWAY's teardown path, and
# there is nothing to tear down without one. The stderr pipe is finding 41 --
# a pipe nobody reads holds 64 KiB, and a process blocked writing a traceback
# into a full pipe cannot exit.
_reg(WsScenario(
    sid="S11", name="Mass disconnect",
    question="1,000 clients vanish inside a second while the provider is still "
             "sending. Does the gateway close every upstream, stay quiet on "
             "stderr, and keep answering /healthz?",
    produces="time to llmgw_ws_sessions_open == 0; upstream sockets still open "
             "at the fake; stderr bytes; /healthz availability; fd and task "
             "drift 30 s later.",
    passes=(
        "all upstream sockets closed <= 5 s after the last client close",
        "stderr < 4 KiB total (no per-socket line)",
        "/healthz 200 throughout",
        "no fd or task drift after 30 s",
    ),
    driver="s11_disconnect",
    measure_s=60.0,
    ramp_s=10.0,
    arms=("G",),
    groups=(
        Group(label="stt-stream", product="inworld-stt", sessions=1000,
              interval=0.0, nbytes=3200, events=10,
              send_interval=0.1, send_bytes=3200),
    ),
    extra={"disconnect_at": 20.0, "disconnect_window_s": 1.0,
           "settle_s": 30.0},
))

# S12. The S8 driver's SIGTERM thread, pointed at open sockets instead of open
# streams. The drain contract per product is PLAN-G 4.3: STT gets `closeStream`
# forwarded and a 4900 at once; TTS waits for its contexts to close first.
_reg(WsScenario(
    sid="S12", name="Deploy under open sessions",
    question="SIGTERM with 600 sockets open: does every client see 4900 inside "
             "the drain wait, does every session leave a record, and does the "
             "process exit on time?",
    produces="client close codes and their timing per product; `Terminate` / "
             "`closeStream` count at the fake; session capture records; "
             "process exit time; cut count.",
    passes=(
        "100% of STT clients see 4900 within ws_drain_wait_s + 1",
        "TTS clients see 4900 after their contexts close",
        "fake terminates_received == STT sessions",
        "every session has a record with `seconds` and `basis`",
        "process exits before grace + 3 s",
        "cut == 0",
    ),
    driver="s12_deploy",
    measure_s=180.0,
    ramp_s=15.0,
    arms=("G",),
    sigterm_at=60.0,
    groups=(
        Group(label="stt", product="inworld-stt", sessions=500,
              interval=0.0, nbytes=3200, events=10,
              send_interval=0.1, send_bytes=3200),
        Group(label="tts", product="inworld-tts", sessions=100,
              interval=0.1, nbytes=6400, events=3000),
    ),
))


WS_SCENARIOS = _S

__all__ = ["PATHS", "WS_SCENARIOS", "Group", "WsScenario"]

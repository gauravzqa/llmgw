"""Hostile upstreams. One process, two ports, fourteen ways to break a proxy.

You cannot test a proxy without an upstream you control. A proxy's entire job
is what it does when the thing behind it misbehaves, and the one thing a real
provider will not do on demand is misbehave. So the contract tier does not
point at OpenAI or Anthropic; it points here, at a server whose only feature
is being reliably awful in a chosen way.

Two surfaces, because the failure modes are surface-shaped:

    :8801  POST /v1/chat/completions   OpenAI, terminal marker `data: [DONE]`
    :8802  POST /v1/messages           Anthropic, terminal event `message_stop`

Both serve the bytes in `fakes/wire.py` and nothing else. That sharing is the
whole point: the fakes and the gateway's parser tests read the same module, so
the contract tier cannot degrade into two mocks agreeing with each other while
neither matches a real provider.

--------------------------------------------------------------------------
Choosing a behaviour
--------------------------------------------------------------------------

Per request via `X-Fake-Mode`, falling back to the port's default mode. Never
via a restart -- a load scenario has to be able to flip a target from `ok` to
`5xx` at t=120 s without dropping the listener, and a unit-ish contract test
has to be able to run fourteen behaviours against one session-scoped server.

Parameters ride along as headers, all optional:

    X-Fake-Events    N   content events before the mode's twist   (default 5)
    X-Fake-Interval  T   seconds between events                   (default 0)
    X-Fake-Delay     T   seconds of silence, for the stall modes  (default 30)
    X-Fake-Status    S   which 5xx to serve                       (default 500)
    X-Fake-Bytes     B   size of the single frame in huge-event   (default 8Mi)
    X-Fake-Seed      S   RNG seed for split-frames                (default 1729)
    X-Fake-CRLF      1   re-terminate every SSE line with CRLF    (default off)

`X-Fake-Events` always means *content* events -- OpenAI chunks with a delta,
Anthropic `content_block_delta`s. It never counts the envelope frames
(`message_start`, `content_block_start`), so "K events then die" means the
same thing on both surfaces even though Anthropic puts two frames in front.

--------------------------------------------------------------------------
The counters are not decoration
--------------------------------------------------------------------------

`GET /__stats` exists because the most important assertion in the fallback
tests is *negative*: "the incumbent was never opened". That is a statement
about the upstream, and the client cannot observe it. A client that got a 200
from the candidate has no way to distinguish "the gateway never tried the
incumbent" from "the gateway tried the incumbent, got a 200, and threw it
away" -- and those are a correct gateway and a gateway that double-bills every
customer. Only the upstream knows. So it counts.

Counters are process-global, shared by both ports on purpose, so a test can
ask one question of the whole fleet instead of reconciling two views. Only the
two provider-shaped routes are counted; `/__stats` itself is not, or reading
the counter would change it.

--------------------------------------------------------------------------
Running more than one of it
--------------------------------------------------------------------------

One CPython process serves about 50k SSE writes a second before it pins a
core, and the streaming load scenarios ask for more than that (S2: 2,500
streams x 40 events/s = 100k writes/s). A fake that saturates first collapses
the control arm of every scenario it is in, so the CLI takes `--workers N`.

With N > 1 the parent binds both ports, then re-executes itself N times with
the two listening sockets inherited (`subprocess` + `pass_fds`), and every
worker accepts on the SAME sockets -- the gateway keeps one base_url, and the
kernel spreads accepts across whichever workers are in `accept()`. Not
SO_REUSEPORT: on macOS that option lets N sockets bind the port but delivers
every connection to the most recently bound one, which was measured, not
assumed (`[0, 0, 0, 400]` accepts across four listeners). The counters stay
fleet-wide -- each worker owns a slot in one mmap'd file and `/__stats` sums
the slots -- so "was the incumbent ever opened" is still one question. The
parent forwards SIGTERM/SIGINT to the workers and reaps them; a worker whose
parent vanishes exits on its own. No flag means exactly one process, in-line,
as before.

--------------------------------------------------------------------------
What an ASGI fake cannot do
--------------------------------------------------------------------------

Three limits worth knowing before you trust a green test here.

1. **No RST.** `die-mid-stream` closes the transport, which is a FIN after the
   already-queued bytes flush. A real provider dying takes the socket with it
   and you may get an RST, which surfaces as ECONNRESET rather than as a
   truncated chunked body. We test the truncation; we cannot test the reset.
2. **No socket-level header timing.** `stall-before-headers` stalls after
   uvicorn has completed the TCP handshake and parsed the request line. It
   exercises a time-to-first-byte clock, not a connect clock. A genuine
   connect stall needs a listener that never accepts (see the doc).
3. **Write boundaries are ASGI boundaries, not TCP segments.** `split-frames`
   controls where one `http.response.body` message ends. The kernel is free to
   coalesce. That is still the property the parser cares about -- it proves
   the parser survives arbitrary chunk boundaries -- but it is not a claim
   about packets.
"""

from __future__ import annotations

import argparse
import asyncio
import mmap
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Literal

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from fakes import wire

Surface = Literal["openai", "anthropic"]

MODES: tuple[str, ...] = (
    "ok",
    "stall-before-headers",
    "stall-after-headers",
    "stall-mid-stream",
    "ping-forever",
    "5xx",
    "429",
    "529",
    "die-mid-stream",
    "error-in-stream",
    "schema-400",
    "slow-drip",
    "huge-event",
    "split-frames",
)

PATHS: dict[Surface, str] = {
    "openai": "/v1/chat/completions",
    "anthropic": "/v1/messages",
}

# One 64 KiB block of filler, allocated once at import and re-yielded. See
# `_huge_event_chunks` for why this matters: an 8 MiB `data:` line built as a
# single Python bytes object costs 8 MiB of RSS per concurrent request, and
# the scale tier runs hundreds of those at once. The whole point of the mode
# is to make the *gateway* prove its buffers are byte-bounded; a fake that
# OOMs first proves nothing.
_FILLER_BLOCK = b"x" * 65_536

# Hard ceiling on any mode's sleep, so a forgotten `X-Fake-Delay: 86400` can
# never outlive the test session that created it.
_MAX_STALL_SECONDS = 300.0


# --------------------------------------------------------------------------
# Counters
# --------------------------------------------------------------------------


# The counters are a fixed vector of int64s rather than a Counter, so that a
# worker fleet can share them: every worker owns one slot of an mmap'd file and
# `snapshot()` sums the slots. The vocabulary is closed (MODES x PATHS), which
# is what makes a fixed layout possible. Single-process, the slot is a local
# bytearray and nothing else changes.
_MODE_INDEX: dict[str, int] = {m: i for i, m in enumerate(MODES)}
_PATH_INDEX: dict[str, int] = {p: i for i, p in enumerate(PATHS.values())}
_F_TOTAL, _F_OPEN, _F_PEAK = 0, 1, 2
_F_MODE = 3
_F_PATH = _F_MODE + len(MODES)
_F_WRITES = _F_PATH + len(PATHS)
_SLOT_FIELDS = _F_WRITES + len(MODES)
_SLOT_BYTES = _SLOT_FIELDS * 8
_ZERO_SLOT = memoryview(bytes(_SLOT_BYTES)).cast("q")


class Stats:
    """Process-global request counters, guarded by a lock.

    The lock is not paranoia: the CLI and the test fixtures both run each port
    in its own thread with its own event loop, so two loops really do touch
    these integers concurrently. An `int64[i] += 1` is not atomic under the
    GIL once it is a read-modify-write.

    Under `--workers N` the slots live in a file every worker has mapped.
    Each worker writes only its own slot (so the lock above is the only lock
    needed), and reads all N when answering `/__stats`. Two consequences,
    both deliberate: `peak_open_streams` is the SUM of per-worker peaks, an
    upper bound on the fleet's true peak; and `/__stats/reset` zeroes every
    slot without stopping the other workers, so a request in flight elsewhere
    at that instant can re-land one stale increment. Neither matters for the
    tests (single process, one slot, exact) nor for the load bench (which
    only reads `total`).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._mmap: mmap.mmap | None = None
        self._attach(memoryview(bytearray(_SLOT_BYTES)), slot=0, slots=1)

    def _attach(self, buf: memoryview, *, slot: int, slots: int) -> None:
        cells = buf.cast("q")
        self._all = [cells[i * _SLOT_FIELDS:(i + 1) * _SLOT_FIELDS] for i in range(slots)]
        self._own = self._all[slot]
        self._slot = slot

    def share(self, path: str, *, slot: int, slots: int) -> None:
        """Re-home this process's counters in slot `slot` of the file at
        `path`, which the parent pre-sized to `slots` slots. Counts recorded
        before the call are discarded; workers call this before serving."""
        fh = open(path, "r+b")  # noqa: SIM115  -- lifetime is the mmap's
        mm = mmap.mmap(fh.fileno(), slots * _SLOT_BYTES, access=mmap.ACCESS_WRITE)
        fh.close()
        with self._lock:
            self._mmap = mm
            self._attach(memoryview(mm), slot=slot, slots=slots)

    @staticmethod
    def create_shared(slots: int) -> str:
        """Make the zeroed file `share()` expects; returns its path."""
        fd, path = tempfile.mkstemp(prefix="fake-upstream-stats-", suffix=".bin")
        with os.fdopen(fd, "wb") as fh:
            fh.write(bytes(slots * _SLOT_BYTES))
        return path

    def reset(self) -> None:
        with self._lock:
            for cells in self._all:
                cells[:] = _ZERO_SLOT

    def record_request(self, mode: str, path: str) -> None:
        own = self._own
        with self._lock:
            own[_F_MODE + _MODE_INDEX[mode]] += 1
            own[_F_PATH + _PATH_INDEX[path]] += 1
            own[_F_TOTAL] += 1

    def record_write(self, mode: str) -> None:
        with self._lock:
            self._own[_F_WRITES + _MODE_INDEX[mode]] += 1

    def stream_opened(self) -> None:
        own = self._own
        with self._lock:
            own[_F_OPEN] += 1
            own[_F_PEAK] = max(own[_F_PEAK], own[_F_OPEN])

    def stream_closed(self) -> None:
        with self._lock:
            self._own[_F_OPEN] -= 1

    def _sum(self, field: int) -> int:
        return sum(cells[field] for cells in self._all)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            by_mode = {m: n for m in MODES if (n := self._sum(_F_MODE + _MODE_INDEX[m]))}
            by_path = {
                p: n for p in PATHS.values() if (n := self._sum(_F_PATH + _PATH_INDEX[p]))
            }
            writes = {m: n for m in MODES if (n := self._sum(_F_WRITES + _MODE_INDEX[m]))}
            return {
                "total": self._sum(_F_TOTAL),
                "by_mode": by_mode,
                "by_path": by_path,
                "writes_by_mode": writes,
                "open_streams": self._sum(_F_OPEN),
                "peak_open_streams": self._sum(_F_PEAK),
            }

    def own_snapshot(self) -> dict[str, int]:
        """This process's slot only: what the worker prints on the way out."""
        own = self._own
        with self._lock:
            return {
                "slot": self._slot,
                "requests": own[_F_TOTAL],
                "writes": sum(own[_F_WRITES + i] for i in range(len(MODES))),
                "open_streams": own[_F_OPEN],
                "peak_open_streams": own[_F_PEAK],
            }


STATS = Stats()


# --------------------------------------------------------------------------
# Request parameters
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Params:
    mode: str
    events: int
    interval: float
    delay: float
    status: int
    nbytes: int
    seed: int
    crlf: bool


# Per-mode default overrides. `slow-drip` is defined by its interval, so it
# would be absurd for it to inherit the zero-interval default that keeps the
# `ok` tests fast.
_MODE_DEFAULTS: dict[str, dict[str, float | int]] = {
    "slow-drip": {"interval": 2.0, "events": 150},
    "ping-forever": {"interval": 1.0, "events": 100_000},
    "huge-event": {"events": 1},
    "429": {"delay": 3.0},
}


class BadFakeRequest(Exception):
    """A malformed `X-Fake-*` header. Answered with a 400 naming the header,
    because a test that silently gets mode `ok` when it asked for `ok-typo` is
    a test that passes for the wrong reason."""


def _num(headers, name: str, default: float, cast: Callable[[str], float]) -> float:
    raw = headers.get(name)
    if raw is None:
        return default
    try:
        return cast(raw.strip())
    except ValueError as exc:
        raise BadFakeRequest(f"{name}: {raw!r} is not a number") from exc


def parse_params(request: Request, default_mode: str) -> Params:
    h = request.headers
    mode = h.get("x-fake-mode", default_mode).strip().lower()
    if mode not in MODES:
        raise BadFakeRequest(f"X-Fake-Mode: {mode!r} is not one of {list(MODES)}")

    d = _MODE_DEFAULTS.get(mode, {})
    events = int(_num(h, "x-fake-events", float(d.get("events", len(wire.TOKENS))), float))
    interval = float(_num(h, "x-fake-interval", float(d.get("interval", 0.0)), float))
    delay = float(_num(h, "x-fake-delay", float(d.get("delay", 30.0)), float))
    status = int(_num(h, "x-fake-status", float(d.get("status", 500)), float))
    nbytes = int(_num(h, "x-fake-bytes", float(d.get("bytes", 8 * 1024 * 1024)), float))
    seed = int(_num(h, "x-fake-seed", float(d.get("seed", 1729)), float))
    crlf = h.get("x-fake-crlf", "").strip() in {"1", "true", "yes"}

    if events < 0:
        raise BadFakeRequest("X-Fake-Events must be >= 0")
    if interval < 0 or delay < 0:
        raise BadFakeRequest("X-Fake-Interval and X-Fake-Delay must be >= 0")
    if mode == "5xx" and status not in (500, 502, 503, 504):
        raise BadFakeRequest(f"X-Fake-Status: {status} is not one of 500/502/503/504")
    return Params(
        mode=mode,
        events=events,
        interval=interval,
        delay=min(delay, _MAX_STALL_SECONDS),
        status=status,
        nbytes=nbytes,
        seed=seed,
        crlf=crlf,
    )


# --------------------------------------------------------------------------
# Frames, assembled out of fakes/wire.py and nothing else
# --------------------------------------------------------------------------


# The content frames are pure functions of the token and there are only
# `len(wire.TOKENS)` tokens, so they are built once per (surface, crlf) and
# every stream cycles the same tuple. Nothing about a stream's content is
# allocated per request: an `X-Fake-Events: 1000` stream used to hold a list
# of 1,000 fresh ~200-byte frames for its whole 25 s life (~150 KiB/stream,
# 2,500 streams = hundreds of MiB that pymalloc then keeps as high-water
# arenas), and building the list up front cost the first byte ~1 ms of CPU
# per stream in the opening burst. Now it holds an index.
_CONTENT_FRAMES: dict[tuple[Surface, bool], tuple[bytes, ...]] = {}


def _content_frames(surface: Surface, crlf: bool = False) -> tuple[bytes, ...]:
    key = (surface, crlf)
    frames = _CONTENT_FRAMES.get(key)
    if frames is None:
        build = wire.anthropic_delta if surface == "anthropic" else wire.openai_chunk
        frames = tuple(_crlf(build(t)) if crlf else build(t) for t in wire.TOKENS)
        _CONTENT_FRAMES[key] = frames  # benign race between the two loops: same value
    return frames


def _prefix(surface: Surface) -> list[bytes]:
    if surface == "anthropic":
        return [wire.anthropic_message_start(), wire.anthropic_block_start()]
    return []


def _content(surface: Surface, n: int, *, crlf: bool = False) -> Iterator[bytes]:
    """N content frames, cycling `wire.TOKENS`, produced one at a time. At the
    default n == len(TOKENS) this is exactly TOKENS in order, so `ok`
    reproduces `wire.expected_text()` and the multi-byte UTF-8 in it stays on
    the wire where split-frames needs it."""
    frames = _content_frames(surface, crlf)
    k = len(frames)
    return (frames[i % k] for i in range(max(n, 0)))


def _tail(surface: Surface, *, usage: bool = True) -> list[bytes]:
    if surface == "anthropic":
        frames = [wire.anthropic_block_stop()]
        if usage:
            frames.append(wire.anthropic_message_delta())
        frames.append(wire.anthropic_message_stop())
        return frames
    frames = []
    if usage:
        frames.append(wire.openai_usage_chunk())
    frames.append(wire.openai_done())
    return frames


def ok_frames(surface: Surface, n: int = len(wire.TOKENS)) -> list[bytes]:
    """The complete, correct stream. Byte-identical to `wire.openai_stream()` /
    `wire.anthropic_stream()` at the default n -- asserted in the tests, since
    a fake that drifts from the canonical bytes is worse than no fake."""
    return _prefix(surface) + list(_content(surface, n)) + _tail(surface)


def _heartbeat(surface: Surface) -> bytes:
    return wire.anthropic_ping() if surface == "anthropic" else wire.openai_heartbeat()


def _in_stream_error(surface: Surface) -> bytes:
    """The in-band error frame: HTTP said 200, the protocol says otherwise.

    Anthropic has a real `event: error`, so `wire` builds it. OpenAI has no
    such thing in the chat-completions spec -- proxies in the wild improvise by
    framing an error object as one more `data:` line -- so we frame
    `wire.openai_error_body()` rather than inventing a new shape here.
    """
    if surface == "anthropic":
        return wire.anthropic_error(kind="overloaded_error", message="injected mid-stream")
    body = wire.openai_error_body(kind="server_error", message="injected mid-stream")
    return b"data: " + body + b"\n\n"


def _crlf(payload: bytes) -> bytes:
    """Re-terminate every line with CRLF.

    `wire.py` is LF-only, which is what the real providers send, so this is
    off by default and the byte-identity test runs without it. It exists
    because the SSE grammar permits CRLF and a parser that only ever saw LF
    has an untested branch; with it on, `split-frames` can guarantee a write
    boundary *inside* a CRLF pair, which is the specific split that breaks a
    naive `endswith(b"\\n\\n")` scanner.
    """
    return payload.replace(b"\n", b"\r\n")


# --------------------------------------------------------------------------
# split-frames
# --------------------------------------------------------------------------


def split_writes(payload: bytes, *, seed: int, crlf: bool) -> list[bytes]:
    """Cut `payload` at seeded-random offsets, plus two offsets we insist on.

    Random alone is not enough. Over a 1 KiB body with 1..17 byte steps the
    interesting boundaries -- mid-character and mid-delimiter -- show up most
    of the time, and "most of the time" is how you get a test that is green in
    CI and red on the machine of whoever is demoing it. So the two boundaries
    that actually break parsers are added unconditionally:

      * inside a multi-byte UTF-8 sequence, found by locating a continuation
        byte (0b10xxxxxx). A parser that decodes each chunk as it arrives dies
        here; one that decodes only after re-assembling a frame does not.
      * inside the two-byte frame delimiter (`\\r\\n\\r\\n` when CRLF is on,
        otherwise `\\n\\n`). A scanner that tests the tail of each chunk for
        the delimiter misses it when the pair straddles two chunks.

    The seed is a header so a failure is reproducible: the test prints the
    seed and you replay the exact byte boundaries.
    """
    n = len(payload)
    if n == 0:
        return []
    rng = random.Random(seed)
    cuts: set[int] = set()

    i = 0
    while True:
        i += rng.randint(1, 17)
        if i >= n:
            break
        cuts.add(i)

    for k in range(1, n):
        if 0x80 <= payload[k] < 0xC0:  # UTF-8 continuation byte
            cuts.add(k)
            break

    delim = b"\r\n\r\n" if crlf else b"\n\n"
    at = payload.find(delim)
    if at != -1:
        # Land between the two bytes of the *last* pair, which is the pair a
        # tail-matching scanner is looking at.
        cuts.add(at + len(delim) - 1)

    offsets = [0, *sorted(cuts), n]
    return [payload[a:b] for a, b in zip(offsets, offsets[1:]) if b > a]  # noqa: B905


# --------------------------------------------------------------------------
# huge-event
# --------------------------------------------------------------------------


def _huge_envelope(surface: Surface) -> tuple[bytes, bytes]:
    """The bytes either side of the filler in a single oversized `data:` line.

    Built by asking `wire` for a real frame with a one-character payload and
    splitting it there, so the JSON shape stays whatever `wire` says it is and
    cannot drift when `wire` changes.
    """
    sentinel = "\x01"
    build = wire.anthropic_delta if surface == "anthropic" else wire.openai_chunk
    frame = build(sentinel)
    marker = b"\\u0001"  # json.dumps escapes the control char
    at = frame.index(marker)
    return frame[:at], frame[at + len(marker) :]


def _huge_event_chunks(surface: Surface, nbytes: int, *, crlf: bool) -> list[bytes]:
    """One SSE frame of exactly `nbytes` bytes, yielded as 64 KiB pieces.

    Never materialised as one object. `b"x" * (8 << 20)` per request is 8 MiB
    of RSS held for the life of the response, and the mode exists to be run
    concurrently against a gateway whose buffers we are trying to overflow --
    so the fake must be the cheap side of that experiment. The filler block is
    a module-level constant re-yielded N times; ASGI copies it into the
    transport buffer and we never own a second copy.
    """
    head, tail = _huge_envelope(surface)
    if crlf:
        head, tail = _crlf(head), _crlf(tail)
    fill = nbytes - len(head) - len(tail)
    if fill < 0:
        raise BadFakeRequest(
            f"X-Fake-Bytes must be at least {len(head) + len(tail)} for this surface"
        )
    chunks = [head]
    whole, rest = divmod(fill, len(_FILLER_BLOCK))
    chunks.extend(_FILLER_BLOCK for _ in range(whole))
    if rest:
        chunks.append(_FILLER_BLOCK[:rest])
    chunks.append(tail)
    return chunks


# --------------------------------------------------------------------------
# The generators, one per mode
# --------------------------------------------------------------------------


class DiedMidStream(Exception):
    """Raised inside a StreamingResponse body to kill the connection.

    Uvicorn's `run_asgi` catches everything; when the response has already
    started it calls `transport.close()` and does NOT send h11's EndOfMessage.
    Under chunked transfer-encoding that means the terminating `0\\r\\n\\r\\n`
    never goes out, so the client sees a truncated body and raises
    (`httpx.RemoteProtocolError`) rather than getting a clean short read. That
    is the closest an ASGI app can get to a provider falling over mid-answer.
    """


async def _pace(interval: float) -> None:
    if interval > 0:
        await asyncio.sleep(interval)


async def _body(surface: Surface, p: Params) -> AsyncIterator[bytes]:
    """Yield the response body for `p.mode`, one ASGI write per yield."""
    mode = p.mode

    if mode == "split-frames":
        payload = wire.joined(ok_frames(surface, p.events))
        if p.crlf:
            payload = _crlf(payload)
        for chunk in split_writes(payload, seed=p.seed, crlf=p.crlf):
            yield chunk
        return

    if mode == "huge-event":
        # The envelope still has to be well formed: a `content_block_delta`
        # with no `message_start` in front of it is not an Anthropic stream,
        # and a fake that serves one is testing the gateway's tolerance for
        # nonsense rather than its tolerance for size.
        for frame in _prefix(surface):
            yield _crlf(frame) if p.crlf else frame
        for chunk in _huge_event_chunks(surface, p.nbytes, crlf=p.crlf):
            yield chunk
        for frame in _tail(surface, usage=False):
            yield _crlf(frame) if p.crlf else frame
        return

    def out(frame: bytes) -> bytes:
        return _crlf(frame) if p.crlf else frame

    if mode == "stall-after-headers":
        # The headers are already on the wire: Starlette sends
        # `http.response.start` before it pulls the first chunk, and uvicorn
        # writes it to the transport there and then. So the client's
        # `send(stream=True)` returns with a 200 and gets nothing after it.
        await asyncio.sleep(p.delay)
        for frame in ok_frames(surface, p.events):
            yield out(frame)
        return

    if mode == "ping-forever":
        # Anthropic opens with `message_start` and nothing else: no
        # `content_block_start`, because a block that never gets a delta is a
        # weaker version of the same test. OpenAI has no envelope, so its
        # opener is the empty-choices chunk that is already a heartbeat.
        if surface == "anthropic":
            yield out(wire.anthropic_message_start())
        else:
            yield out(_heartbeat(surface))
        for _ in range(p.events):
            await _pace(p.interval)
            yield out(_heartbeat(surface))
        return

    # Everything below starts with the envelope and K real content events.
    for frame in _prefix(surface):
        yield out(frame)
    for frame in _content(surface, p.events, crlf=p.crlf):
        await _pace(p.interval)
        yield frame

    if mode == "stall-mid-stream":
        await asyncio.sleep(p.delay)
        for frame in _tail(surface):
            yield out(frame)
        return

    if mode == "die-mid-stream":
        raise DiedMidStream(f"{surface}: died after {p.events} events")

    if mode == "error-in-stream":
        yield out(_in_stream_error(surface))
        return

    # ok and slow-drip both end correctly; they differ only in pacing.
    for frame in _tail(surface):
        yield out(frame)


def _tracked(surface: Surface, p: Params) -> AsyncIterator[bytes]:
    """Wrap `_body` so every ASGI write and every open stream is counted.

    The write counter is what lets a test prove `split-frames` really split:
    the client cannot see write boundaries (httpx re-assembles, the kernel
    coalesces), so "there were 40 writes for 13 frames" is another fact only
    the upstream knows.
    """

    async def gen() -> AsyncIterator[bytes]:
        STATS.stream_opened()
        try:
            async for chunk in _body(surface, p):
                STATS.record_write(p.mode)
                yield chunk
        finally:
            STATS.stream_closed()

    return gen()


# --------------------------------------------------------------------------
# Non-streaming modes
# --------------------------------------------------------------------------


def _error_body(surface: Surface, kind: str, message: str) -> bytes:
    if surface == "anthropic":
        return wire.anthropic_error_body(kind=kind, message=message)
    return wire.openai_error_body(kind=kind, message=message)


def _raw(surface: Surface, status: int, kind: str, message: str, headers: dict[str, str]):
    return Response(
        content=_error_body(surface, kind, message),
        status_code=status,
        media_type="application/json",
        headers=headers,
    )


async def _wait_for_disconnect(request: Request) -> None:
    """Block until the client hangs up.

    Used only by `stall-before-headers`, which is the one mode with no
    response in flight: Starlette's StreamingResponse installs its own
    disconnect watcher and cancels the body generator, but a handler that has
    not returned a response yet gets no such help. Without this, aborting the
    client leaves the handler sleeping out its full delay and the session
    teardown waits for it.
    """
    while True:
        message = await request.receive()
        if message["type"] == "http.disconnect":
            return


async def _stall_before_headers(request: Request, surface: Surface, p: Params) -> Response:
    sleeper = asyncio.ensure_future(asyncio.sleep(p.delay))
    watcher = asyncio.ensure_future(_wait_for_disconnect(request))
    try:
        await asyncio.wait({sleeper, watcher}, return_when=asyncio.FIRST_COMPLETED)
    finally:
        sleeper.cancel()
        watcher.cancel()
    # If the client is still there after the stall, answer correctly: the mode
    # is "slow", not "broken", and a gateway with a generous first-byte budget
    # should be able to ride it out.
    return _stream(surface, p)


# --------------------------------------------------------------------------
# Routing
# --------------------------------------------------------------------------

_SSE_HEADERS = {
    "cache-control": "no-cache",
    "connection": "keep-alive",
    "x-accel-buffering": "no",
}


def _stream(surface: Surface, p: Params) -> StreamingResponse:
    return StreamingResponse(
        _tracked(surface, p),
        status_code=200,
        media_type="text/event-stream",
        headers={**_SSE_HEADERS, "x-fake-mode": p.mode},
    )


def _handler(surface: Surface, default_mode: str) -> Callable[[Request], Awaitable[Response]]:
    async def handle(request: Request) -> Response:
        try:
            p = parse_params(request, default_mode)
        except BadFakeRequest as exc:
            return JSONResponse(
                {"error": {"type": "fake_upstream_misuse", "message": str(exc)}},
                status_code=400,
            )

        STATS.record_request(p.mode, request.url.path)
        hdr = {"x-fake-mode": p.mode}

        if p.mode == "schema-400":
            return _raw(surface, 400, "invalid_request_error", "bad schema", hdr)
        if p.mode == "5xx":
            return _raw(surface, p.status, "server_error", f"synthetic {p.status}", hdr)
        if p.mode == "529":
            # Anthropic's overload status. Served on both ports because the
            # gateway classifies on the integer, and a classifier that only
            # ever met 529 on one surface has an untested branch.
            return _raw(surface, 529, "overloaded_error", "Overloaded", hdr)
        if p.mode == "429":
            retry_after = max(1, int(p.delay))
            return _raw(
                surface,
                429,
                "rate_limit_error",
                "slow down",
                {
                    **hdr,
                    "retry-after": str(retry_after),
                    "x-ratelimit-remaining-requests": "0",
                    "x-ratelimit-remaining-tokens": "0",
                    "x-ratelimit-reset-requests": f"{retry_after}s",
                },
            )
        if p.mode == "stall-before-headers":
            return await _stall_before_headers(request, surface, p)

        if p.mode == "huge-event":
            # Validate the size up front: a 400 raised inside the body
            # generator would arrive as a truncated 200, which is a different
            # mode entirely.
            head, tail = _huge_envelope(surface)
            floor = len(head) + len(tail) + (2 if p.crlf else 0)
            if p.nbytes < floor:
                return JSONResponse(
                    {"error": {"type": "fake_upstream_misuse",
                               "message": f"X-Fake-Bytes must be >= {floor} on this surface"}},
                    status_code=400,
                )
        return _stream(surface, p)

    return handle


async def _stats(_: Request) -> Response:
    return JSONResponse(STATS.snapshot())


async def _stats_reset(_: Request) -> Response:
    STATS.reset()
    return JSONResponse(STATS.snapshot())


def build_app(surface: Surface, *, default_mode: str = "ok") -> Starlette:
    """One Starlette app per surface. Both share the module-global counters."""
    if default_mode not in MODES:
        raise ValueError(f"default_mode {default_mode!r} not in {list(MODES)}")
    return Starlette(
        routes=[
            Route(PATHS[surface], _handler(surface, default_mode), methods=["POST"]),
            Route("/__stats", _stats, methods=["GET"]),
            Route("/__stats/reset", _stats_reset, methods=["POST"]),
        ]
    )


# --------------------------------------------------------------------------
# Running it
# --------------------------------------------------------------------------


class _Server(uvicorn.Server):
    """uvicorn's server, plus a note of what was still alive when it began to
    shut down. `tasks_at_shutdown` is `len(asyncio.all_tasks())` on the
    server's loop at that instant: the serving task itself counts one, so an
    idle server reads 1, and every stream a client abandoned without the fake
    noticing would add three (uvicorn's cycle, Starlette's body pump and its
    disconnect listener). Printed on the way out so the claim "no orphaned
    tasks" is something you can read off the terminal rather than take on
    trust."""

    tasks_at_shutdown: int = -1
    connections_at_shutdown: int = -1

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        self.tasks_at_shutdown = len(asyncio.all_tasks())
        self.connections_at_shutdown = len(self.server_state.connections)
        await super().shutdown(sockets=sockets)


@dataclass
class RunningServer:
    """A uvicorn server on its own thread and its own event loop.

    A thread rather than a task on the caller's loop, because the contract
    tests get a fresh event loop per test function while the servers are
    session-scoped -- and because a fake upstream sharing a loop with the code
    under test can hide a blocking bug in either one.
    """

    server: _Server
    thread: threading.Thread
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self, timeout: float = 5.0) -> None:
        self.server.should_exit = True
        self.thread.join(timeout)
        if self.thread.is_alive():
            # A mode mid-stall will not notice `should_exit`; do not let a
            # hostile fake hold the test session hostage.
            self.server.force_exit = True
            self.thread.join(timeout)

    def summary(self) -> dict[str, int]:
        return {
            "port": self.port,
            "tasks_at_shutdown": self.server.tasks_at_shutdown,
            "connections_at_shutdown": self.server.connections_at_shutdown,
        }


def listen(host: str, port: int, backlog: int = 2048) -> socket.socket:
    """A bound, listening socket. `port=0` picks an ephemeral one. The backlog
    is a request; macOS clamps it to `kern.ipc.somaxconn` (128 by default),
    which is why a 2,000-connection burst sees SYNs retransmitted."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen(backlog)
    return sock


def serve_in_thread(
    app: Starlette,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    log_level: str = "critical",
    startup_timeout: float = 10.0,
    sock: socket.socket | None = None,
) -> RunningServer:
    """Start `app` on a background thread. `port=0` binds an ephemeral port.

    The listening socket is created here, before uvicorn, so the caller knows
    the port without polling uvicorn's internals or racing its startup. Pass
    `sock` to serve an already-listening socket instead -- that is how a
    worker accepts on a socket its parent bound.
    """
    if sock is None:
        sock = listen(host, port)
    bound_port = sock.getsockname()[1]

    config = uvicorn.Config(app, log_level=log_level, access_log=False, lifespan="off")
    server = _Server(config)
    thread = threading.Thread(
        target=server.run, kwargs={"sockets": [sock]}, daemon=True,
        name=f"fake-upstream-{bound_port}",
    )
    thread.start()

    deadline = time.monotonic() + startup_timeout
    while not server.started:
        if not thread.is_alive() or time.monotonic() > deadline:
            raise RuntimeError(f"fake upstream on port {bound_port} failed to start")
        time.sleep(0.005)
    return RunningServer(server=server, thread=thread, port=bound_port)


# --------------------------------------------------------------------------
# The CLI: one process, or a parent and N workers
# --------------------------------------------------------------------------

# How long a stopping process gives its open streams before force-exiting.
# The bench SIGTERMs the fake and waits 10 s before SIGKILL; a fleet mid-S2
# has thousands of streams open at that moment and none of them matter.
_CLI_DRAIN_SECONDS = 1.5

# How long the parent gives a worker after SIGTERM before SIGKILL.
_WORKER_EXIT_SECONDS = 8.0


def _stop_all(servers: list[RunningServer], timeout: float) -> None:
    """`RunningServer.stop` for several servers at once, sharing one deadline
    rather than paying it per server."""
    for s in servers:
        s.server.should_exit = True
    deadline = time.monotonic() + timeout
    for s in servers:
        s.thread.join(max(0.0, deadline - time.monotonic()))
    for s in servers:
        if s.thread.is_alive():
            s.server.force_exit = True
    for s in servers:
        s.thread.join(timeout)


def _install_stop_signals(flag: threading.Event) -> None:
    """SIGTERM and SIGINT both mean "drain and exit", so a `p.terminate()`
    from the bench and a Ctrl-C at the terminal take the same path and both
    print the shutdown summary. Main thread only; the test fixtures never
    come through here."""

    def _on_signal(signum: int, _frame: object) -> None:
        flag.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _on_signal)


def _wait_until_stopped(
    servers: list[RunningServer], stop: threading.Event, *, parent_pid: int | None = None,
) -> None:
    """Block until a signal, a dead server thread, or (for a worker) a parent
    that is no longer ours -- a SIGKILLed parent forwards nothing, so the
    worker has to notice for itself."""
    while not stop.is_set() and all(s.thread.is_alive() for s in servers):
        if parent_pid is not None and os.getppid() != parent_pid:
            return
        stop.wait(0.25)


def _serve_pair(
    args: argparse.Namespace, socks: list[socket.socket] | None = None,
) -> list[RunningServer]:
    """The two servers, on the given sockets or on freshly bound ones."""
    apps = [
        build_app("openai", default_mode=args.openai_mode),
        build_app("anthropic", default_mode=args.anthropic_mode),
    ]
    ports = [args.openai_port, args.anthropic_port]
    return [
        serve_in_thread(
            app, host=args.host, port=port, log_level=args.log_level,
            sock=socks[i] if socks else None,
        )
        for i, (app, port) in enumerate(zip(apps, ports))  # noqa: B905
    ]


def _print_summary(label: str, servers: list[RunningServer]) -> None:
    own = STATS.own_snapshot()
    per_port = " ".join(
        f"{name}[tasks_at_shutdown={s.server.tasks_at_shutdown} "
        f"connections={s.server.connections_at_shutdown}]"
        for name, s in zip(("openai", "anthropic"), servers)  # noqa: B905
    )
    print(
        f"{label} pid={os.getpid()}: requests={own['requests']} writes={own['writes']} "
        f"open_streams={own['open_streams']} peak_open_streams={own['peak_open_streams']} "
        f"{per_port}  (tasks_at_shutdown=1 is idle)",
        file=sys.stderr, flush=True,
    )


def _banner(args: argparse.Namespace, ports: tuple[int, int], workers: int) -> None:
    print(f"openai     http://{args.host}:{ports[0]}{PATHS['openai']}"
          f"  (default mode: {args.openai_mode})", flush=True)
    print(f"anthropic  http://{args.host}:{ports[1]}{PATHS['anthropic']}"
          f"  (default mode: {args.anthropic_mode})", flush=True)
    print(f"stats      http://{args.host}:{ports[0]}/__stats", flush=True)
    print(f"modes      {' '.join(MODES)}", flush=True)
    if workers > 1:
        print(f"workers    {workers} (pid {os.getpid()} supervises)", flush=True)


def _run_single(args: argparse.Namespace) -> int:
    """No `--workers`: exactly one process, serving in-line."""
    stop = threading.Event()
    _install_stop_signals(stop)
    servers = _serve_pair(args)
    _banner(args, (servers[0].port, servers[1].port), workers=1)
    try:
        _wait_until_stopped(servers, stop)
    finally:
        _stop_all(servers, _CLI_DRAIN_SECONDS)
        _print_summary("fake upstream", servers)
    return 0


def _run_worker(args: argparse.Namespace) -> int:
    """One of N: accept on the parent's sockets, count in the parent's file."""
    slot, slots = (int(x) for x in args.worker_slot.split("/"))
    fds = [int(x) for x in args.worker_fds.split(",")]
    STATS.share(args.stats_file, slot=slot, slots=slots)
    socks = [socket.socket(fileno=fd) for fd in fds]
    stop = threading.Event()
    _install_stop_signals(stop)
    servers = _serve_pair(args, socks)
    try:
        _wait_until_stopped(servers, stop, parent_pid=os.getppid())
    finally:
        _stop_all(servers, _CLI_DRAIN_SECONDS)
        _print_summary(f"fake worker {slot + 1}/{slots}", servers)
    return 0


def _run_parent(args: argparse.Namespace) -> int:
    """Bind, re-exec N workers that inherit the sockets, forward signals, reap."""
    socks = [listen(args.host, args.openai_port), listen(args.host, args.anthropic_port)]
    ports = (socks[0].getsockname()[1], socks[1].getsockname()[1])
    fds = [s.fileno() for s in socks]
    stats_path = STATS.create_shared(args.workers)
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    stop = threading.Event()
    _install_stop_signals(stop)

    children: list[subprocess.Popen] = []
    for i in range(args.workers):
        argv = [
            sys.executable, "-m", "fakes.upstream",
            "--host", args.host,
            "--openai-port", str(ports[0]), "--anthropic-port", str(ports[1]),
            "--openai-mode", args.openai_mode, "--anthropic-mode", args.anthropic_mode,
            "--log-level", args.log_level,
            "--_worker-slot", f"{i}/{args.workers}",
            "--_worker-fds", ",".join(map(str, fds)),
            "--_stats-file", stats_path,
        ]
        children.append(subprocess.Popen(argv, cwd=root, pass_fds=fds))
    _banner(args, ports, args.workers)

    rc = 0
    try:
        while not stop.is_set() and all(c.poll() is None for c in children):
            stop.wait(0.25)
        if not stop.is_set():
            # A worker died on its own. A fleet with a hole in it would serve
            # a bench that looks slightly slow instead of one that fails; take
            # the whole thing down and say so.
            dead = [c.pid for c in children if c.poll() is not None]
            print(f"fake upstream: worker(s) {dead} exited unexpectedly; stopping",
                  file=sys.stderr, flush=True)
            rc = 1
    finally:
        for c in children:
            if c.poll() is None:
                c.terminate()
        deadline = time.monotonic() + _WORKER_EXIT_SECONDS
        for c in children:
            try:
                c.wait(max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                c.kill()
                c.wait()
        for s in socks:
            s.close()
        STATS.share(stats_path, slot=0, slots=args.workers)
        snap = STATS.snapshot()
        print(f"fake upstream fleet ({args.workers} workers): requests={snap['total']} "
              f"open_streams={snap['open_streams']} by_path={snap['by_path']}",
              file=sys.stderr, flush=True)
        try:
            os.unlink(stats_path)
        except OSError:
            pass
    return rc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m fakes.upstream",
        description="Hostile OpenAI-shaped and Anthropic-shaped upstreams.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--openai-port", type=int, default=8801)
    parser.add_argument("--anthropic-port", type=int, default=8802)
    parser.add_argument("--openai-mode", default="ok", choices=MODES)
    parser.add_argument("--anthropic-mode", default="ok", choices=MODES)
    parser.add_argument("--log-level", default="info")
    parser.add_argument(
        "--workers", type=int, default=1,
        help="serve both ports from N processes that share the listening sockets "
             "(default 1: a single in-line process)",
    )
    # Internal: how the parent launches a worker. Not for humans.
    parser.add_argument("--_worker-slot", dest="worker_slot", help=argparse.SUPPRESS)
    parser.add_argument("--_worker-fds", dest="worker_fds", help=argparse.SUPPRESS)
    parser.add_argument("--_stats-file", dest="stats_file", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)

    if args.workers < 1:
        parser.error("--workers must be >= 1")
    if args.worker_slot is not None:
        return _run_worker(args)
    if args.workers == 1:
        return _run_single(args)
    return _run_parent(args)


if __name__ == "__main__":
    raise SystemExit(main())

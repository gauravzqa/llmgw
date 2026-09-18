"""What a WebSocket product has to tell the relay, and nothing more.

`surfaces/base.py` (the HTTP one) answers "what does this dialect's response
look like". This file answers a harder question, because a socket has no
response: it has a conversation, and the relay has to know four things about
every frame of it without understanding any of them.

    1. does this frame RESET A CLOCK -- and which one?
    2. does relaying it COMMIT us to the unit it belongs to?
    3. does it END that unit?
    4. is it an ERROR, and is the error fatal?

Everything else the relay does is dialect-free: bytes in, bytes out, bounded
buffers, permits, records. So a surface is a classifier plus a handful of
constants, and the relay contains not one provider name.

--------------------------------------------------------------------------
The unit, and why it is not the session
--------------------------------------------------------------------------

On HTTP the unit of commitment is the request, because there is exactly one
per connection. On a socket there are many, they overlap, and they are
NAMED: Inworld TTS multiplexes up to five contexts by `contextId`, OpenAI
Realtime runs a sequence of responses. `context_key(frame)` is how the relay
learns the name, and the name is what commitment, the first-event clock and
the progress clock are all keyed by. A session that has committed context A
may still fall back for context B -- except that it may not, because the
socket is shared; so the rule the relay implements is the conservative one:
the FIRST commitment on a socket commits the socket (C24).

--------------------------------------------------------------------------
The one edit
--------------------------------------------------------------------------

`model_from_first_frame` and `rewrite_first_frame` are the only place any
layer is allowed to change a client's bytes on this plane, and the rewrite
is announced by `X-Gw-Body-Modified: 1` on the 101 exactly as the HTTP
model rewrite is announced on a response. It exists because the LiveKit
Inworld TTS plugin joins its URL with `urljoin` against an absolute path
(tts.py:259), which DROPS any `/workloads/{w}` prefix -- so the socket's own
first frame is the only thing that can name a target, and a catalog id in
it has to become the provider's wire id before the provider sees it.

Everything else is relayed as the exact bytes that arrived. Not
re-serialised: a JSON round trip changes key order, whitespace and unicode
escaping, and on a plane where the client's own state machine is reading
every frame that is three ways to break somebody's parser for nothing.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from llmgw import errors
from llmgw.surfaces.base import Usage

__all__ = [
    "AcceptPolicy",
    "Frame",
    "FrameClass",
    "Verdict",
    "WsSurface",
    "surface_auth_schemes",
]


class FrameClass(Enum):
    """What one frame means to the clocks and to commitment.

    Five values, and the boundary between the first two is where every
    liveness bug on this plane will come from. CONTENT is the thing the
    client is paying for -- audio out, a transcript, audio in. META is
    protocol chatter that proves the provider is alive but proves nothing
    about progress: `contextCreated` is META, and a provider that sent
    nothing but `contextCreated` forever would be stalled while looking
    perfectly healthy to a liveness check.
    """

    CONTENT = "content"
    """Resets the direction's progress clock and, the first time in a unit,
    satisfies `first_event`. Outbound CONTENT is what COMMITS a unit."""

    META = "meta"
    """Relayed, counted, and invisible to the progress clock. Ends the
    handshake budget when it is the first upstream frame."""

    HEARTBEAT = "heartbeat"
    """Liveness only: the frame says the peer is there, not that it is
    working. Resets the idle clock and nothing else. Inworld sends none --
    it does not ping and does not close a healthy socket (captures-ws 1.3) --
    which is precisely why the gateway's own `idle` budget exists."""

    ERROR = "error"
    """Goes through the surface's `error_from_frame`. NOT necessarily fatal:
    an Inworld `result.status.code != 0` fails one context and leaves the
    socket usable, and a top-level `error` code 3 for a malformed frame does
    not even do that (captures-ws probe 5)."""

    TERMINAL = "terminal"
    """Ends the unit named by `context_key`. `contextClosed` for Inworld
    TTS, `response.done` for a Realtime response."""


class AcceptPolicy(Enum):
    """When the client's 101 goes out, relative to the upstream's.

    The choice is forced by whether the provider says anything unprompted.
    OpenAI Realtime sends `session.created` about 1.7 s after its 101 and
    refuses a bad key with an in-band error and a close 3000 in the same
    millisecond, so waiting for that first frame turns an auth failure into a
    pre-101 HTTP response the client's SDK already knows how to raise.

    Inworld says NOTHING. A valid key, a bad key and no key all produce an
    identical 101 and then silence -- for 20 s in the captures, and for 75 s
    on a healthy idle socket (probes 2b, 3). There is no frame to wait for,
    so waiting is just a timeout, and the credential is only checked when the
    first client message arrives. Hence accept-then-relay, and hence the
    `headers` budget running from the first relayed `create` rather than from
    the 101.
    """

    ACCEPT_THEN_RELAY = "accept_then_relay"
    ACCEPT_AFTER_UPSTREAM_READY = "accept_after_upstream_ready"


@dataclass(slots=True)
class Frame:
    """One WebSocket message, as the bytes that arrived plus a lazy parse.

    The bytes are authoritative and are what gets relayed; `payload()` is a
    read-only view for the classifier. Parsing is lazy and cached because
    most frames are classified once and relayed, and a failed parse is not an
    error -- Inworld answered `this is not json` with a top-level error and
    kept the socket open (probe 5), so a frame the gateway cannot read is a
    frame the gateway relays and lets the provider judge.
    """

    data: bytes
    text: bool = True
    _payload: Any = None
    _parsed: bool = False

    @classmethod
    def of(cls, message: str | bytes) -> Frame:
        if isinstance(message, str):
            return cls(message.encode("utf-8"), text=True)
        return cls(bytes(message), text=False)

    def wire(self) -> str | bytes:
        """What to hand the peer's `send()`: `str` for a text frame, `bytes`
        for a binary one, so the opcode the client chose is the opcode the
        provider sees. A proxy that turned a client's TEXT frame into a
        BINARY one would be editing the message at the protocol layer."""
        if self.text:
            return self.data.decode("utf-8", "replace")
        return self.data

    def payload(self) -> dict[str, Any] | None:
        """The frame as a JSON object, or None if it is not one. Never
        raises: this runs on relayed bytes the gateway did not write."""
        if not self._parsed:
            self._parsed = True
            if self.text:
                try:
                    value = json.loads(self.data)
                except (ValueError, UnicodeDecodeError):
                    value = None
                self._payload = value if isinstance(value, dict) else None
        return self._payload

    def replace_json(self, payload: dict[str, Any]) -> Frame:
        """A NEW text frame carrying `payload`, for the one case where the
        gateway may not relay what it received: an error body a provider row
        marks unsafe (`scrub_error_bodies="all"`). Every other frame on this
        plane is passed through byte for byte, so this method exists to make
        the exception findable -- grep it and you have the complete list of
        places the relay rewrites a provider's bytes."""
        return Frame(json.dumps(payload).encode("utf-8"), text=True)

    def __len__(self) -> int:
        return len(self.data)


@dataclass(frozen=True, slots=True)
class Verdict:
    """A surface's reading of one frame."""

    kind: FrameClass
    context: str | None = None
    """The unit this frame belongs to, or None for a connection-level frame.
    Commitment, `first_event` and `progress` are all keyed by it."""

    config: bool = False
    """Client frames only: this frame is part of the REPLAYABLE CONFIG PREFIX
    (C24). The `create` an Inworld context opens with, the `session.update` a
    Realtime session opens with. A pre-commit fallback replays exactly these
    and nothing else -- never content, because replaying audio to a second
    provider bills the tenant twice for one utterance and can produce two
    different transcripts of it."""

    terminal_for_session: bool = False
    """This frame ends the whole session, not just its unit."""

    context_closed: bool = False
    """This frame means the unit it names does not exist any more, even
    though it is an ERROR rather than a TERMINAL. Inworld refuses a sixth
    context with `status.code 8` and an unknown one with `code 5`
    (captures-ws probe 4): the client asked for a context and there is none,
    so the relay must forget it. Without this the drain waits its full bound
    for a context that was never opened."""

    buffer_delay_s: float | None = None
    """Client config frames only: how long this client has TOLD the provider
    it may sit on text before synthesising (Inworld `create.maxBufferDelayMs`).

    A provider that is deliberately buffering is not a provider that has
    stalled, so the unit's `first_event` budget is extended by this much. It
    has to come off the wire rather than out of a profile because the client
    chooses it per context: the LiveKit plugin sends `autoMode: true` with
    `maxBufferDelayMs: 3000` and then does not flush until the whole LLM
    response has been tokenised, so a short first sentence legitimately
    produces silence for three seconds (`inworld/tts.py:401-416, 1273-1281`).
    Without this the gateway closes 4902 on a perfectly healthy first turn."""


@runtime_checkable
class WsSurface(Protocol):
    """One relayed product. Implementations are stateless singletons."""

    name: str
    """The `metrics.SURFACES` label. Closed set; see metrics.py."""

    routes: tuple[str, ...]
    """Client paths this surface answers on, each of which also gets a
    `/workloads/{w}` twin (which the Inworld TTS plugin cannot reach -- see
    the module docstring -- but other callers can)."""

    upstream_path: str
    """The provider path, joined onto the target's `base_url` by
    `upstream.join_url` with the scheme swapped to `ws`/`wss`."""

    dialect: str
    """The `plan_for(kind=...)` hint, so a wire id shared by two providers
    resolves to the one that speaks this socket's language."""

    auth_schemes: frozenset[str]
    """`Authorization` schemes accepted FROM THE CLIENT. Not the provider's:
    what the gateway sends upstream is the catalog row's `auth_scheme`. These
    two are different strings for Inworld only by coincidence."""

    accept_policy: AcceptPolicy

    subprotocols: tuple[str, ...]
    """Subprotocols offered upstream and echoed to the client, in order."""

    product: str
    """The provider-facing name `classify_close` switches on."""

    def classify_client(self, frame: Frame) -> Verdict: ...

    def classify_upstream(self, frame: Frame) -> Verdict: ...

    def model_from_first_frame(self, frame: Frame) -> str | None: ...

    def rewrite_first_frame(self, frame: Frame, api_model: str) -> tuple[Frame, bool]: ...

    def apply_usage(self, frame: Frame, usage: Usage) -> None: ...

    def error_from_frame(self, frame: Frame) -> errors.GatewayError | None: ...

    def drain_message(self) -> Frame | None: ...

    def is_fatal(self, frame: Frame) -> bool: ...


def surface_auth_schemes(surface: Any) -> frozenset[str]:
    """The client-side scheme set, defaulting to Bearer for a surface that
    predates the field. Mirrors `surfaces.base.surface_*` accessors: a
    Protocol attribute read through a function so a partial implementation is
    a missing feature rather than an AttributeError on the request path."""
    return frozenset(getattr(surface, "auth_schemes", None) or {"bearer"})

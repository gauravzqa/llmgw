"""Inworld TTS bidirectional: the production voice path, frame by frame.

Every row below is from `capabilities/captures-ws.md` (21 live sockets, 18
Sep 2026) or from the consumer's own state machine
(`livekit/plugins/inworld/tts.py`, pinned at 1.6.3). Where the 16 Sep sweep
and the captures disagree, the captures win and the disagreement is named.

--------------------------------------------------------------------------
The conversation
--------------------------------------------------------------------------

    C>S  {"create":{modelId, voiceId, audioConfig{...}, ...}, "contextId":X}
    S>C  {"result":{"contextId":X,"contextCreated":{...},"status":{code:0}}}
    C>S  {"send_text":{"text":"..."},"contextId":X}
    C>S  {"flush_context":{},"contextId":X}
    S>C  {"result":{"contextId":X,"audioChunk":{audioContent, usage, ...}}}   xN
    S>C  {"result":{"contextId":X,"flushCompleted":{},...}}
    C>S  {"close_context":{},"contextId":X}
    S>C  {"result":{"contextId":X,"contextClosed":{},...}}

Up to five contexts interleave on one socket, each identified by the
top-level `contextId` that EVERY frame in both directions carries.

--------------------------------------------------------------------------
Four things the captures changed
--------------------------------------------------------------------------

**1. Usage is exact, and it is on the FIRST chunk of every flush.**
`audioChunk.usage.processedCharactersCount` is the character count for that
flush and is 0 on every later chunk of it (probe 1). So the meter is a SUM
over flushes, not a last-value read and not a first-value read -- a context
that synthesises three utterances reports 29, 0, 0, 19, 0, 17, 0, and the
bill is 65. PLAN-G 7.2's "characters estimated from `send_text`" is the
FALLBACK, used only when no chunk carried usage at all.

**2. A bad key is not silence.** The 16 Sep sweep recorded "101 then silence"
and concluded the credential was checked lazily and reported nowhere. The
captures show the socket is silent only because nothing was sent: the first
client frame gets a top-level `error` code 7 (bad key) or 16 (no credential)
about 320 ms later, and a server CLOSE 1000 in the same millisecond. So the
handshake budget runs from the first relayed `create`, and the fatal signal
is the close that follows the error, not the error alone.

**3. A top-level `error` is fatal only if a CLOSE follows.** `this is not
json` and a BINARY frame both got `error` code 3 and the socket stayed open
for the rest of the probe (probe 5). Treating every top-level error as fatal
would cut a session because the client sent one bad frame.

**4. The 2,000-character and 5-context limits are the PROVIDER's, and stay
that way.** PLAN-G 3.1 had the gateway refuse a sixth `create` itself. That
is a synthesised verdict about somebody else's protocol, which C25 forbids,
and it is also wrong in detail: Inworld answers the sixth `create` with
`result.status` code 8 naming the context, the plugin fails exactly that
context (tts.py:478-496) and keeps the other five. A gateway close 4907
would kill all six. So the sixth `create` is relayed and Inworld answers it;
the same for a `send_text` over 2,000 characters, which earns `status` code
3 and no `flushCompleted`. The gateway COUNTS contexts (commitment is per
context) and polices neither limit.

--------------------------------------------------------------------------
What is relayed, and the one thing that is not
--------------------------------------------------------------------------

Every frame goes out as the bytes that came in, except the first `create` of
each context, whose `modelId` is rewritten from the catalog id (or the alias
the plugin sends, `inworld-tts-1.5-mini`) to the target's `api_model`. That
rewrite is announced as `X-Gw-Body-Modified: 1` on the 101.

The 44-byte RIFF/WAVE header at the start of the first and the last chunk of
every flush is the PROVIDER's, and the plugin strips it. It is mentioned here
only so that nobody ever "fixes" it in the gateway: 52,060 relayed bytes for
29 characters is the correct number, and it is 44 bytes more than the PCM.
"""

from __future__ import annotations

import json
from typing import Any

from llmgw import errors
from llmgw.surfaces.base import Usage
from llmgw.ws.surfaces.base import AcceptPolicy, Frame, FrameClass, Verdict

__all__ = ["INWORLD_TTS_WS", "InworldTTSWebSocketSurface"]

_NO_SUCH_CONTEXT: frozenset[int] = frozenset({5, 8})
"""`result.status.code` values that mean the named context does not exist:
5 "context not found", 8 "limit of 5 TTS contexts per connection"
(captures-ws probe 4). Anything else leaves the context open."""

SCRUBBED_MESSAGE: str = (
    "upstream error text withheld by the gateway "
    "(provider echoes credential fragments)"
)
"""What replaces a scrubbed `error.message`. Fixed text, so a client can
match on it, and explicit about WHY, so nobody debugging assumes the
provider said nothing."""

MAX_CLIENT_BUFFER_DELAY_S: float = 30.0
"""Ceiling on the `first_event` extension a client can ask for with
`create.maxBufferDelayMs`. The plugin's default is 3 s; 30 s is ten times
that and still far below any session budget."""

MAX_SEND_TEXT_CHARS = 2_000
"""The provider's per-`send_text` limit (probe 5). Recorded, never enforced:
see the module docstring. Used only to note on a capture record that a
session sent a frame the provider was always going to refuse."""

MAX_CONTEXTS = 5
"""The provider's per-socket context limit (probe 4). Counted, not policed."""

_BENIGN_STATUS_CODES = frozenset({5})
"""In-context `status.code` values that are NOT worth an error frame count:
5 is "context not found", which the plugin already treats as that context
having failed (tts.py:478-496) and which the sweep marks benign. It happens
routinely when a client closes a context twice or sends to one it just
closed, and counting it as a provider error would make a tidy client look
like a sick provider."""


class InworldTTSWebSocketSurface:
    """The `inworld_tts_ws` surface. Stateless; one instance per process."""

    name = "inworld_tts_ws"
    product = "inworld"
    dialect = "openai"
    """The catalog's `kind` for the Inworld row -- "OpenAI-shaped", meaning
    only "no Anthropic header ritual". It is the tie-breaker `plan_for` uses
    when a wire id is shared, not a statement about this socket's dialect."""

    routes = ("/tts/v1/voice:streamBidirectional",)
    upstream_path = "/tts/v1/voice:streamBidirectional"

    auth_schemes = frozenset({"basic", "bearer"})
    """Basic FIRST because that is what arrives: the plugin builds
    `"Basic " + INWORLD_API_KEY` in the `TTS` constructor (tts.py:913) and
    sends it verbatim (tts.py:263-267), so a tenant routing through the
    gateway sets `INWORLD_API_KEY` to its gateway token and the token reaches
    us wrapped in `Basic`. Bearer is accepted too, for every caller that is
    not this plugin."""

    accept_policy = AcceptPolicy.ACCEPT_THEN_RELAY
    """There is no unsolicited first frame to wait for: 101 then silence is a
    HEALTHY idle socket on this provider (probes 2b, 3)."""

    subprotocols: tuple[str, ...] = ()
    """None negotiated, and none offered: the captures show no
    `sec-websocket-protocol` on any Inworld upgrade."""

    # ---------------------------------------------------------- classify

    def classify_client(self, frame: Frame) -> Verdict:
        """Client -> provider. Four message types, two of which matter.

        `create` is the config prefix: it is the only frame a pre-commit
        fallback may replay to a second target (C24), and it is the frame
        that names the model. `send_text` is CONTENT-IN -- it is what the
        tenant is billed for, and it is what makes a silent socket different
        from a working one. `flush_context` and `close_context` are META:
        they carry no text and the provider answers them with frames that do.
        """
        payload = frame.payload()
        if payload is None:
            # Not JSON, or not an object. Relayed untouched: Inworld answered
            # exactly this with a non-fatal code-3 error and kept the socket
            # (probe 5), and a gateway that refused it would be inventing a
            # stricter protocol than the provider's.
            return Verdict(FrameClass.META)
        context = _context_of(payload)
        if "create" in payload:
            return Verdict(
                FrameClass.META, context=context, config=True,
                buffer_delay_s=_buffer_delay_of(payload.get("create")),
            )
        if "send_text" in payload:
            return Verdict(FrameClass.CONTENT, context=context)
        return Verdict(FrameClass.META, context=context)

    def scrub_error_frame(self, frame: Frame) -> Frame:
        """The same error frame with its free text replaced.

        Inworld quotes the first four characters of the API key back in its
        code-7 message (`Invalid credentials provided for API key "abcd***"`,
        captures-ws probe 2b) -- and on this plane the key is the GATEWAY's,
        not the tenant's. The HTTP twin has scrubbed those bodies since Phase
        B via `scrub_error_bodies="all"`; this is the same rule on the socket.

        What survives: the frame is still JSON, still has `error`, still
        carries the provider's own `code` and `status` and any `contextId`,
        so a client's error handling sees the shape it expects and can still
        tell an auth failure (7, 16) from a bad argument (3). Only `message`
        and `details` go, because those are the two the provider writes prose
        into. The real text is already on the capture record.

        Total: a frame this cannot parse is returned unchanged rather than
        dropped -- it reached here classified as an error, and a client that
        gets nothing at all is worse off than one that gets bytes we could
        not read."""
        payload = frame.payload()
        if payload is None or not isinstance(payload.get("error"), dict):
            return frame
        block = dict(payload["error"])
        block.pop("details", None)
        block["message"] = SCRUBBED_MESSAGE
        kept = dict(payload)
        kept["error"] = block
        try:
            return frame.replace_json(kept)
        except Exception:  # noqa: BLE001 - see the docstring: never drop it
            return frame

    def classify_upstream(self, frame: Frame) -> Verdict:
        """Provider -> client. The table in captures-ws 1.3.

        `audioChunk` is the only CONTENT, and it is therefore the only frame
        whose relay commits the unit. Everything inside `result` that is not
        audio is META -- including `contextCreated`, which ends the handshake
        budget without proving a single sample was synthesised.
        """
        payload = frame.payload()
        if payload is None:
            return Verdict(FrameClass.META)
        if "error" in payload and isinstance(payload.get("error"), dict):
            # Connection-level. Fatal only if a close follows; `is_fatal`
            # decides, the relay waits for the close, and the close is the
            # verdict.
            return Verdict(FrameClass.ERROR)
        result = payload.get("result")
        if not isinstance(result, dict):
            return Verdict(FrameClass.META)
        context = result.get("contextId")
        context = context if isinstance(context, str) else None
        status = result.get("status")
        if isinstance(status, dict) and status.get("code"):
            # In-context fault. The SOCKET SURVIVES: codes 3 (text too long),
            # 5 (unknown context) and 8 (sixth context) were all observed
            # with the connection still usable afterwards.
            #
            # 5 and 8 additionally mean the context is NOT THERE -- 8 refused
            # to open it, 5 says it never existed -- and no `contextClosed`
            # will ever arrive for it. The relay counts open contexts to know
            # when a draining session may close, so a context that is refused
            # and then remembered holds the drain open for its whole bound.
            return Verdict(
                FrameClass.ERROR, context=context,
                context_closed=status.get("code") in _NO_SUCH_CONTEXT,
            )
        if "audioChunk" in result:
            return Verdict(FrameClass.CONTENT, context=context)
        if "contextClosed" in result:
            return Verdict(FrameClass.TERMINAL, context=context)
        return Verdict(FrameClass.META, context=context)

    # ------------------------------------------------------------- model

    def model_from_first_frame(self, frame: Frame) -> str | None:
        """`create.modelId`, the ONLY thing that can name a target here.

        The plugin builds its URL with `urljoin(ws_url, "/tts/v1/...")`
        (tts.py:259), and `urljoin` against an absolute path discards
        everything after the host -- so a `/workloads/{w}` prefix cannot
        survive the plugin and the route cannot carry the routing decision.
        The first frame can, and does.
        """
        payload = frame.payload()
        if payload is None:
            return None
        create = payload.get("create")
        if not isinstance(create, dict):
            return None
        model = create.get("modelId")
        return model if isinstance(model, str) and model else None

    def rewrite_first_frame(self, frame: Frame, api_model: str) -> tuple[Frame, bool]:
        """Put the target's wire id into `create.modelId`. The one edit.

        Returns the ORIGINAL frame object, unchanged, when the client already
        named this target's wire id -- the same equality check
        `upstream.apply_api_model` makes, and for the same reason: a
        re-serialisation changes key order and whitespace on a frame nobody
        had any reason to touch, and on this plane that is bytes the provider
        did not have to parse differently and a header the client did not
        have to see.
        """
        payload = frame.payload()
        if payload is None:
            return frame, False
        create = payload.get("create")
        if not isinstance(create, dict) or create.get("modelId") == api_model:
            return frame, False
        merged = {**payload, "create": {**create, "modelId": api_model}}
        try:
            rendered = json.dumps(merged, ensure_ascii=False, separators=(",", ":"))
        except (TypeError, ValueError, RecursionError) as exc:
            # A frame `json.loads` accepted and `json.dumps` will not re-emit.
            # A client fault, reported as one, and never a crash on the relay.
            raise errors.InvalidRequest(
                f"create frame cannot be re-serialised with its target's model: {exc}"
            ) from exc
        return Frame(rendered.encode("utf-8"), text=True), True

    # ------------------------------------------------------------- usage

    def apply_usage(self, frame: Frame, usage: Usage) -> None:
        """Sum `processedCharactersCount` across flushes. Never raises.

        SUM, not last and not first. The count arrives on the first chunk of
        each flush and is 0 on the rest of it (probe 1), so a socket that
        synthesises three utterances reports the three counts interleaved
        with zeros and only the sum is the bill. Zeros are free to add.

        `input_exact`/`output_exact` are both set the moment a real count
        lands, because for a character-billed row there is no second half to
        be inexact about: `accounting._cost_usd` prices `characters x
        input_per_m` and `Usage.exact` is the conjunction of two flags that
        both describe the one number we have.
        """
        payload = frame.payload()
        if payload is None:
            return
        result = payload.get("result")
        if not isinstance(result, dict):
            return
        chunk = result.get("audioChunk")
        if not isinstance(chunk, dict):
            return
        meter = chunk.get("usage")
        if not isinstance(meter, dict):
            # Audio with no usage object at all. Not a parse failure -- the
            # shape is simply absent -- but the session falls back to
            # counting `send_text` characters and says so in `cost_notes`.
            return
        count = meter.get("processedCharactersCount")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            usage.parse_failures += 1
            return
        usage.characters += count
        if count:
            usage.input_exact = True
            usage.output_exact = True

    # ------------------------------------------------------------ errors

    def error_from_frame(self, frame: Frame) -> errors.GatewayError | None:
        """An ERROR-classed frame -> a taxonomy class, or None.

        Two shapes, and they mean different things:

        * top-level `error{code, message}` is CONNECTION-level. Codes 16
          (`UNAUTHENTICATED`) and 7 (`PERMISSION_DENIED`) are the credential
          being refused and are scoped to it, so a bad Inworld key opens the
          CREDENTIAL circuit and not the model's. Code 3 is a malformed
          frame -- the client's fault, non-fatal, and the socket survives.
        * `result.status{code}` is CONTEXT-level and never ends the session.

        The message is never forwarded to the client. Inworld reflects the
        first four characters of the key it rejected (`<KEY4>***`, probe 2b),
        which is why the provider row carries `scrub_error_bodies="all"`;
        the same rule has to hold on this plane, where the "body" is a frame.
        """
        payload = frame.payload()
        if payload is None:
            return None
        top = payload.get("error")
        if isinstance(top, dict):
            code = top.get("code")
            if code in (7, 16):
                return errors.AuthenticationFailed(
                    "Inworld refused the gateway's credential", provider="inworld",
                )
            if code == 5:
                return errors.ModelNotFound(
                    "Inworld does not know the requested voice or model",
                    provider="inworld",
                )
            return errors.InvalidRequest(
                "Inworld refused a relayed frame as invalid", provider="inworld",
            )
        result = payload.get("result")
        if isinstance(result, dict):
            status = result.get("status")
            if isinstance(status, dict) and status.get("code"):
                if status.get("code") in _BENIGN_STATUS_CODES:
                    return None
                return errors.InvalidRequest(
                    "Inworld refused a relayed frame for one context",
                    provider="inworld",
                )
        return None

    def is_fatal(self, frame: Frame) -> bool:
        """Does this error frame end the SESSION if a close follows?

        Only a top-level `error` can, and only sometimes. Probe 5 sent
        garbage three ways and got code 3 with the socket still open; probes
        2a/2b/6c sent a bad credential and got code 16/7/3 followed by CLOSE
        1000 in the same millisecond. The discriminator is not the code, it
        is the close -- so this returns "might be", the relay waits briefly
        for a close, and the close decides. An in-context `result.status` is
        never fatal.
        """
        payload = frame.payload()
        if payload is None:
            return False
        return isinstance(payload.get("error"), dict)

    def no_retry(self, frame: Frame) -> bool:
        """`error.details[].reconnectType == "NO_RETRY"`: the provider's own
        instruction, which overrides the class's `retry_same`. A hint the
        provider gives is better evidence than a table we wrote
        (voice-inworld.md:110)."""
        payload = frame.payload()
        if payload is None:
            return False
        top = payload.get("error")
        if not isinstance(top, dict):
            return False
        details = top.get("details")
        if not isinstance(details, list):
            return False
        return any(
            isinstance(d, dict) and d.get("reconnectType") == "NO_RETRY"
            for d in details
        )

    # -------------------------------------------------------------- drain

    def drain_message(self) -> Frame | None:
        """None: there is no session-level terminate in this protocol.

        `closeStream` (STT) and `Terminate` (AssemblyAI) end a session and
        make the provider report its meter. TTS has no such message -- only
        `close_context`, per context, and the gateway does not synthesise
        those either: it has no way to know the client will not send another
        `send_text` on a context it still considers open, and a `close_context`
        the client did not send is a frame the provider did not get from the
        client (C2, C25). So the drain WAITS for contexts to reach zero and
        closes 4900 at `drain_grace - ws_drain_wait_s` if they do not. The plugin
        fails those contexts and re-synthesises on a fresh socket
        (tts.py:603-616), which is the behaviour it already has for a
        provider-side disconnect.
        """
        return None


INWORLD_TTS_WS = InworldTTSWebSocketSurface()



def _buffer_delay_of(create: Any) -> float | None:
    """`create.maxBufferDelayMs` in seconds, or None.

    Total by construction: a client that sends a string, a negative number
    or nothing at all gets None and the profile's own budget. The cap is
    there because this value comes from the client and it extends a timeout
    -- a caller that asks for a six-hour first-event budget is not getting
    one."""
    if not isinstance(create, dict):
        return None
    raw = create.get("maxBufferDelayMs")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    if raw <= 0:
        return None
    return min(float(raw) / 1000.0, MAX_CLIENT_BUFFER_DELAY_S)

def _context_of(payload: dict[str, Any]) -> str | None:
    value = payload.get("contextId")
    return value if isinstance(value, str) and value else None

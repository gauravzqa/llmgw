"""Close codes: the socket plane's only way of saying "no" after the 101.

CONTRACTS.md C25 in one file. On HTTP there are two ways to report a failure
-- a status line before the body, a truncation after it -- and `app.py` spends
a long docstring on the boundary between them. A WebSocket has three, and one
of them is a trap:

    1. an HTTP response on the upgrade, before the 101;
    2. a CLOSE frame with a code and a reason, after it;
    3. a data frame shaped like the provider's own error.

The third is the trap and this gateway never takes it. A relayed session is
someone else's protocol; the client's state machine (for Inworld TTS,
`livekit/plugins/inworld/tts.py:478-582`) reads every frame as the provider's,
and a frame the provider did not send is a frame that will be interpreted as
one. Synthesising `{"error": ...}` to report a GATEWAY problem tells the
plugin its PROVIDER failed, which fails the wrong contexts, records the wrong
blame and -- because the plugin's own reconnect logic keys off exactly these
frames -- can send it reconnecting at a provider that is perfectly healthy.
So: before the 101, a real HTTP response with the gateway's existing error
body (C4, C11 unchanged). After it, a close code. Never a frame.

--------------------------------------------------------------------------
Why 4900-4999, and why the reason string is doubled
--------------------------------------------------------------------------

RFC 6455 reserves 4000-4999 for application use, which is the only range a
proxy may mint in without colliding with the protocol. Inside it the gateway
takes the top hundred, because the bottom is crowded with providers: OpenAI
closes 4000 on a bad request shape, ElevenLabs 4300 on an agent queue
timeout, AssemblyAI lives in 3005-3009 and 410. Nothing observed or
documented in any of the four sweeps reaches 4900, so a client that sees one
knows without ambiguity that the GATEWAY ended the session.

The reason carries `llmgw:<code>` where `<code>` is the taxonomy's own
`ERROR_CODES` value. That is deliberate redundancy. The close code is the
class of verdict and is what a switch statement wants; the reason is the
taxonomy row and is what a log line wants, and it is finer -- 4907 is "the
upstream handshake did not complete" and its reason says whether that was a
`connect_timeout`, a `headers_timeout` or an `authentication_failed`. A
client may key on either and neither will surprise it later, because both
vocabularies are closed and both are tested against their sources.

--------------------------------------------------------------------------
The third classification input
--------------------------------------------------------------------------

Phase A classified from an HTTP status and a body. A socket has a third
signal, and it arrives SEPARATELY from the frame that explains it: a close
code. `classify_close` therefore takes the last error frame seen as well,
because on three of the four products the pair means something neither half
means alone -- AssemblyAI's 1008 is a rate limit with one `Error` frame and
an auth failure with another (C30), and Inworld's CLOSE 1000 is a clean end
on its own and a fatal credential failure when a code-7 `error` preceded it
by a millisecond (captures-ws probe 2b).
"""

from __future__ import annotations

from enum import IntEnum
from typing import Any

from llmgw import errors

__all__ = [
    "CLOSE_REASON_PREFIX",
    "CloseCode",
    "CloseVerdict",
    "classify_close",
    "close_code_class",
    "reason_for",
    "verdict_for",
]

CLOSE_REASON_PREFIX = "llmgw:"
"""Every reason the gateway writes starts here. A client can tell a gateway
verdict from a provider's own reason by the prefix alone, without a table --
and the close codes are disjoint anyway, so the two signals agree or one of
them is a bug."""

MAX_REASON_BYTES = 123
"""RFC 6455 6.5.1: a close frame's payload is at most 125 bytes and two of
them are the code. A longer reason is not truncated by the library, it is a
protocol error -- so reasons are built from the closed code vocabulary and
never from a provider's message."""


class CloseCode(IntEnum):
    """The gateway's verdicts. One code per REASON A SESSION CAN END BADLY,
    not one per taxonomy class: several classes map onto 4907 because "the
    handshake did not complete" is one thing to a client and three things to
    a breaker."""

    DRAINING = 4900
    """The process is shutting down. Reconnect elsewhere; nothing is wrong."""

    SESSION_TOTAL = 4901
    """`Budgets.session_total` expired. The session was too long, not too
    slow: this is the clock that exists because every provider caps a socket
    somewhere and finding out from their close code is finding out late."""

    PROVIDER_STALL = 4902
    """The provider stopped producing while a unit was open
    (`Budgets.progress`), or never started one (`Budgets.first_event`)."""

    CLIENT_STALL = 4903
    """The client stopped reading and our outbound buffer stayed full for
    `Budgets.client_stall`. Memory protection, not a provider fault."""

    UPSTREAM_GONE = 4904
    """The upstream socket ended or failed mid-session, after commitment. No
    fallback: a second provider would re-bill and re-synthesise (C24)."""

    FRAME_TOO_LARGE = 4905
    """A frame exceeded `max_frame_bytes` in either direction. Ours, not the
    peer's: the bound is the gateway's own and the blame is too."""

    IDLE = 4906
    """Nothing in either direction for `Budgets.idle`. The only thing that
    reclaims an abandoned Inworld socket -- the provider never will."""

    UPSTREAM_HANDSHAKE = 4907
    """The upstream session could not be established, and the client had
    already been accepted. Under accept-then-relay (Inworld) this is where a
    handshake failure lands, because the 101 went out before the provider had
    said anything at all."""


_VERDICTS: dict[str, CloseCode] = {
    # taxonomy code -> the verdict a client should see. Anything not named
    # here is a handshake-shaped failure and becomes 4907; see `verdict_for`.
    errors.SessionDraining.code: CloseCode.DRAINING,
    errors.TotalDeadlineExceeded.code: CloseCode.SESSION_TOTAL,
    errors.StallTimeout.code: CloseCode.PROVIDER_STALL,
    errors.FirstEventTimeout.code: CloseCode.PROVIDER_STALL,
    errors.ClientTooSlow.code: CloseCode.CLIENT_STALL,
    errors.UpstreamDisconnected.code: CloseCode.UPSTREAM_GONE,
    errors.IncompleteStream.code: CloseCode.UPSTREAM_GONE,
    errors.UpstreamServerError.code: CloseCode.UPSTREAM_GONE,
    errors.FrameTooLarge.code: CloseCode.FRAME_TOO_LARGE,
    errors.SessionIdle.code: CloseCode.IDLE,
}

_DEFAULT_REASONS: dict[CloseCode, str] = {
    CloseCode.DRAINING: errors.SessionDraining.code,
    CloseCode.SESSION_TOTAL: errors.TotalDeadlineExceeded.code,
    CloseCode.PROVIDER_STALL: errors.StallTimeout.code,
    CloseCode.CLIENT_STALL: errors.ClientTooSlow.code,
    CloseCode.UPSTREAM_GONE: errors.UpstreamDisconnected.code,
    CloseCode.FRAME_TOO_LARGE: errors.FrameTooLarge.code,
    CloseCode.IDLE: errors.SessionIdle.code,
    CloseCode.UPSTREAM_HANDSHAKE: errors.HeadersTimeout.code,
}
"""What the reason says when there is no error object to read it off -- a
drain, an idle sweep, a budget. Every value is an `ERROR_CODES` member, which
a unit test pins, so the reason vocabulary cannot drift away from the one the
capture records and the metrics use."""


def verdict_for(err: errors.GatewayError) -> CloseCode:
    """The close code that reports `err` to a client already past the 101."""
    return _VERDICTS.get(err.code, CloseCode.UPSTREAM_HANDSHAKE)


def reason_for(code: CloseCode, err: errors.GatewayError | None = None) -> str:
    """`llmgw:<taxonomy code>`, at most `MAX_REASON_BYTES`.

    The error's own code wins when there is one, because it is the finer
    signal: two `connect_timeout`s and an `authentication_failed` all close
    4907, and only the reason says which happened. Falls back to the code's
    default when the verdict was reached without an exception -- a drain, an
    idle sweep -- which is most of them.
    """
    tail = err.code if err is not None else _DEFAULT_REASONS[code]
    reason = f"{CLOSE_REASON_PREFIX}{tail}"
    return reason[:MAX_REASON_BYTES]


class CloseVerdict:
    """A close the gateway is about to send, as one immutable pair.

    A tiny object rather than a tuple because it travels from the watchdog
    through the relay to the session record and back out as a metric label,
    and three call sites unpacking a two-tuple in different orders is how a
    close code and a reason get swapped.
    """

    __slots__ = ("code", "reason", "error")

    def __init__(self, code: CloseCode, error: errors.GatewayError | None = None) -> None:
        self.code = code
        self.error = error
        self.reason = reason_for(code, error)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"<CloseVerdict {int(self.code)} {self.reason}>"


def close_code_class(code: int | None) -> str:
    """Fold any close code onto `metrics.WS_CLOSE_CLASSES`.

    The raw code is unbounded -- 1000-4999 by the RFC, and every provider
    claims part of the private range -- so it is a capture field and this is
    the metric label. `None` means the socket ended with no close frame at
    all, which the RFC calls 1006 and which is the shape a TCP reset or a
    process kill takes.
    """
    if code is None:
        return "abnormal_1006"
    if code == 1000:
        return "normal_1000"
    if code == 1001:
        return "going_away_1001"
    if code == 1006:
        return "abnormal_1006"
    if code == 1008:
        return "policy_1008"
    if code == 1009:
        return "too_large_1009"
    if code == 1011:
        return "internal_1011"
    if 4900 <= code <= 4999:
        # Ours, and separated from the rest of 4xxx on purpose: "we ended it"
        # and "they ended it" are the two facts an incident has to tell apart.
        return "gateway_49xx"
    if 3000 <= code <= 3999:
        return "provider_3xxx"
    if 4000 <= code <= 4999:
        return "provider_4xxx"
    return "other"


def classify_close(
    code: int | None,
    reason: str,
    *,
    product: str,
    last_error_frame: Any = None,
    expected: bool = False,
) -> errors.GatewayError | None:
    """An upstream close -> a taxonomy class, or None for a clean end.

    `expected` is True when WE asked for the end -- the drain forwarded the
    provider's terminate, or the client closed first and we relayed it. A
    1000 then is the provider agreeing, not a provider failing, and
    classifying it would open a breaker on every clean session.

    `last_error_frame` is the frame the surface classified as ERROR most
    recently, already parsed. It is the disambiguator the HTTP plane never
    needed: the code alone is not enough on three of the four products.

    Hooks for G2-G4 are left where the plan puts them (the `product` switch);
    G1 implements the Inworld rows of the 7.1 table and the provider-agnostic
    ones, and returns `UpstreamDisconnected` for anything it has no row for,
    which is the conservative answer -- retryable, try-next, and blamed on
    the provider whose socket it was.
    """
    if expected and code in (1000, 1001, None):
        return None

    hint = _error_hint(last_error_frame)

    if code == 1000:
        if hint is not None:
            # Inworld's ONLY server-initiated close: a fatal `error` frame and
            # a CLOSE 1000 in the same millisecond (captures-ws probes 2a/2b/
            # 6c). The close looks clean and the session is anything but, so
            # the frame is the verdict and the code is the confirmation.
            return hint
        # A 1000 nobody asked for, mid-session. The provider decided the
        # conversation was over while a unit was open; that is an incomplete
        # stream, exactly as a truncated HTTP body is.
        return errors.IncompleteStream(
            "upstream closed 1000 mid-session with no terminate from us",
            provider=product,
        )
    if code == 1001:
        return errors.UpstreamDisconnected(
            "upstream is going away (1001)", provider=product,
        )
    if code is None or code == 1006:
        return errors.UpstreamDisconnected(
            "upstream socket ended without a close frame (1006)", provider=product,
        )
    if code == 1008:
        if hint is not None:
            return hint
        # No frame to read. AssemblyAI's 1008 is auth-or-rate and the sweep
        # says an absent `Error` frame means auth (C30, voice-assemblyai.md:
        # 59-61); every other provider's 1008 is a policy refusal of the
        # client's own traffic.
        if product.startswith("assemblyai"):
            return errors.AuthenticationFailed(
                "upstream closed 1008 with no Error frame: credential refused",
                provider=product,
            )
        return errors.InvalidRequest(
            f"upstream closed 1008: {reason or 'policy violation'}", provider=product,
        )
    if code == 1009:
        # Our bound should have caught this first. It did not, so the fault
        # is the gateway's and the blame follows the fault.
        return errors.FrameTooLarge(
            "upstream refused a frame as too large (1009)", provider=product,
        )
    if code in (1011, 3005):
        return errors.UpstreamServerError(
            f"upstream closed {code}: {reason or 'internal error'}", provider=product,
        )
    if hint is not None:
        return hint
    return errors.UpstreamDisconnected(
        f"upstream closed {code}: {reason or 'no reason given'}", provider=product,
    )


def _error_hint(frame: Any) -> errors.GatewayError | None:
    """The remembered error frame, if it was already classified into one.

    Surfaces hand `classify_close` whatever their `error_from_frame` last
    produced, so this is an identity check rather than a second parse -- the
    frame was classified once, by the object that knows its dialect, and
    re-reading it here would be a second dialect table to keep in step.
    """
    return frame if isinstance(frame, errors.GatewayError) else None

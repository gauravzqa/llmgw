"""One relayed session: what it holds, what it releases, what it writes.

`Exchange` is a request's mutable state and `Session` is a socket's, but the
list of things a socket holds is longer and the consequences of leaking one
are worse, because a socket holds them for minutes rather than milliseconds:

    a tenant admission permit          (concurrency)
    a tenant SESSION permit            (max_sessions -- new, C23)
    a provider-key permit              (the credential's in-flight cap)
    two breaker tickets                (the target's circuit and the
                                        credential's)
    the process in-flight slot         (max_streams, and what a drain waits on)
    a place in Gateway.ws_sessions     (so a drain can reach it)
    two sockets and two frame buffers

Every one of them is taken in `__aenter__` and released in `__aexit__`, in
the reverse order, on every exit path including the cancellation a shutdown
turns into. There is no `try` in the serving path that can skip it, because
the serving path is inside the context manager rather than beside it.

--------------------------------------------------------------------------
Where the target comes from, and why the 101 names one it may not keep
--------------------------------------------------------------------------

The routing input for Inworld TTS is `create.modelId`, and it arrives in the
first client FRAME -- which cannot be read until the client has been
accepted, because ASGI has no way to receive one before `websocket.accept`.
So the order is forced:

    resolve the workload's plan  ->  connect upstream to its primary
    ->  101 to the client (naming that target)  ->  read `create`
    ->  resolve `create.modelId` within the plan  ->  rewrite and relay

`X-Gw-Served-By` and `X-Gw-Model` on the 101 therefore name the target the
socket was OPENED TOWARD, which is what C23 says they mean here and which
differs from their HTTP meaning ("the target that answered"). The capture
record is the final word: it names the model the frame actually asked for
and the price that was actually applied.

This is sound because of a property of the product rather than of the
gateway: an Inworld TTS plan's targets share a provider row, so they share a
base URL and a credential and the SOCKET is the same whichever of them the
first frame names. What the frame chooses is the wire id and the price, both
of which are decided after it arrives. A `modelId` that resolves onto a
different provider is refused (4907, `llmgw:policy_error`) rather than
silently served by the one we are connected to.

--------------------------------------------------------------------------
The record
--------------------------------------------------------------------------

One `CaptureRecord` per session, `kind="session"`, priced by
`accounting.account_usage` -- the same dot product the HTTP plane uses, so
"characters at the Inworld TTS rate" is one number computed by one function
on both planes (C27). `basis` is exact only when the provider's own meter
arrived: `audioChunk.usage.processedCharactersCount`, summed across flushes.
When it did not, the characters the client asked for are the estimate and
`cost_notes` says which derivation was used, because a bill you cannot
explain is a bill you cannot defend.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from llmgw import errors
from llmgw.accounting import account_usage
from llmgw.capture import CaptureRecord
from llmgw.catalog import Target
from llmgw.clocks import Budgets, Clock
from llmgw.policy import ExecutionPlan
from llmgw.ws.relay import Relay, RelayResult

log = logging.getLogger("llmgw.ws.session")

__all__ = ["Session", "SessionOutcome", "new_session_id"]


def new_session_id() -> str:
    """The `X-Gw-Session-Id` and the record's `request_id`.

    A uuid4 hex, not a counter: the id has to be unique across a FLEET (a
    capture file is aggregated from every machine) and a per-process counter
    collides on the first merge. It is also the join key between a session
    record and the per-response records a Realtime session will write in G3,
    so it must not be reused after a restart."""
    return uuid.uuid4().hex


@dataclass(slots=True)
class SessionOutcome:
    """What the endpoint needs after the relay returns."""

    outcome: errors.Outcome
    code: str
    committed: bool
    close_code: int | None
    close_class: str
    record: CaptureRecord | None


class Session:
    """The holder. Constructed per socket, entered once, never reused."""

    def __init__(
        self,
        gateway: Any,
        surface: Any,
        *,
        tenant: str,
        workload_id: str,
        plan: ExecutionPlan,
        snapshot: Any,
        budgets: Budgets,
        clock: Clock,
        session_id: str | None = None,
    ) -> None:
        self.gateway = gateway
        self.surface = surface
        self.tenant = tenant
        self.workload_id = workload_id
        self.plan = plan
        self.snapshot = snapshot
        """The ONE policy snapshot this session is pinned to, taken at the
        upgrade and never re-fetched (FAILURE-MODES row 10). A socket lives
        for minutes, so the window in which a reload could land mid-session
        is far wider here than on a request -- which makes the pin more
        important, not less: a session routed by one policy and priced by
        another has one `policy_id` field and two policies in it."""
        self.budgets = budgets
        self.clock = clock
        self.id = session_id or new_session_id()

        self.target: Target | None = None
        """The target the socket was opened toward; replaced by the one the
        first frame named once it has been resolved, because that is the one
        the record must price against."""

        self.attempts = 0
        self.started_at = clock.now()
        self.relay: Relay | None = None
        self.upstream_request_id: str | None = None
        self.body_modified = False
        self.cost_notes: list[str] = []

        self._drained = False
        self._drain_deadline = 0.0

    # ------------------------------------------------------------- drain

    def drain(self, *, grace_s: float | None = None) -> None:
        """`Gateway.begin_drain` calls this, synchronously, once. C26.

        Synchronous because the drain loop runs over every open session
        before it awaits anything: a hook that awaited would serialise five
        hundred twenty-second waits inside a hundred-and-thirty-second grace.
        All it does is hand the relay a deadline and wake its watchdog; the
        relay does the polite part (forward the provider's terminate if the
        product has one, wait for open contexts to reach zero) and closes the
        client 4900 either way.

        Safe before the relay exists: a session still in its handshake is
        marked drained and the relay reads the mark the moment it starts, so
        a socket that arrived one millisecond before SIGTERM is not missed.
        """
        self._drained = True
        # PLAN-G 4.3: contexts get until `drain_grace - ws_drain_wait_s`, not
        # `ws_drain_wait_s`. The two differ by an order of magnitude and the
        # short one cuts audio that would have finished: an idle pooled
        # socket has no open contexts and closes 4900 immediately either way,
        # so this window is only ever spent on an utterance actually being
        # synthesised. What `ws_drain_wait_s` reserves is the tail AFTER the
        # client is gone -- time to send the provider its terminate and read
        # the acknowledgement that carries the meter -- which is why it is
        # subtracted from the grace rather than used as the whole budget.
        wait = float(getattr(self.gateway.config, "ws_drain_wait_s", 20.0))
        grace = float(
            grace_s if grace_s is not None
            else getattr(self.gateway.config, "drain_grace_seconds", 130.0)
        )
        self._drain_deadline = self.clock.now() + max(grace - wait, 0.0)
        relay = self.relay
        if relay is not None:
            relay.drain(deadline=self._drain_deadline)

    def attach(self, relay: Relay) -> None:
        """Hand the relay to the session, applying a drain that beat it."""
        self.relay = relay
        if self._drained:
            relay.drain(deadline=self._drain_deadline)

    @property
    def open_contexts(self) -> int:
        return self.relay.open_contexts if self.relay is not None else 0

    # ------------------------------------------------------------ model

    def resolve_first_frame(self, frame: Any) -> tuple[Any, bool]:
        """Read the model out of the first frame and rewrite it. C23/C24.

        Raises `PolicyError` when the frame names a model the catalog does
        not know, or one that lives on a different provider than the socket
        we are already holding -- see the module docstring for why the second
        case cannot be served and must not be silently ignored. A frame that
        names no model at all is relayed untouched: the provider has a
        default and it is not the gateway's job to invent one.
        """
        model = self.surface.model_from_first_frame(frame)
        if model is None:
            return frame, False
        target = self.snapshot.catalog.resolve(
            model, kind=getattr(self.surface, "dialect", None),
        )
        opened = self.target
        if opened is not None and target.provider.id != opened.provider.id:
            raise errors.PolicyError(
                f"model {model!r} routes to provider {target.provider.id!r}, but this "
                f"socket is open to {opened.provider.id!r}; open a socket per provider",
                workload=self.workload_id, model=model,
            )
        self.target = target
        rewritten, changed = self.surface.rewrite_first_frame(
            frame, target.model.api_model,
        )
        self.body_modified = self.body_modified or changed
        return rewritten, changed

    # ----------------------------------------------------------- record

    def build_record(self, result: RelayResult) -> CaptureRecord:
        """The session's one capture record. Never raises (C3, C27).

        `account_usage` is the HTTP plane's pricer, called with the same
        `Usage` object the surface filled in frame by frame, so a character
        costs the same here as it does on `/inworld/tts/v1/voice:stream`.
        """
        usage = result.usage
        notes = list(self.cost_notes)
        if not usage.exact and result.client_characters:
            # The provider sent no usage object at all. Bill what the client
            # asked to synthesise and SAY SO: an estimate labelled exact is
            # worse than an obvious estimate, because it gets billed (C27).
            usage.characters = result.client_characters
            notes.append("characters estimated from relayed send_text text")
        if result.contexts_opened:
            notes.append(f"contexts={result.contexts_opened}")
        if self.target is not None and "inworld-tts-1.5-mini" in getattr(
            self.target.model, "aliases", (),
        ):
            notes.append(
                f"client model id resolved through an alias to "
                f"{self.target.model.id}; price basis is the aliased row's"
            )

        error = result.error
        outcome = _outcome_of(error, committed=result.committed)
        rec = account_usage(
            usage,
            target=self.target,
            catalog=self.gateway.config.catalog,
            outcome=outcome,
            committed=result.committed,
            attempts=max(1, self.attempts),
            workload_id=self.workload_id,
            policy_id=self.plan.policy_id,
            code="none" if error is None else error.code,
            cost_notes=notes,
        )
        now = self.clock.now()
        return CaptureRecord(
            request_id=self.id,
            tenant_id=self.tenant,
            workload_id=self.workload_id,
            provider=rec.provider or "-",
            model=rec.model or "-",
            outcome=rec.outcome.value,
            attempts=rec.attempts,
            tokens=dict(rec.tokens_by_kind),
            cost_usd=rec.cost_usd,
            basis=rec.basis,
            committed=rec.committed,
            kind="session",
            session_id=None,
            first_event_latency=result.first_event_at,
            duration_s=now - self.started_at,
            error_code=None if rec.code == "none" else rec.code,
            recorded_at=now,
            upstream_request_id=self.upstream_request_id,
            units=dict(rec.units_by_kind),
            cost_notes=list(rec.cost_notes),
        )


def _outcome_of(error: errors.GatewayError | None, *, committed: bool) -> errors.Outcome:
    """The session's terminal outcome, by the same rule `decide()` uses.

    COMPLETED with no error. With one, the error's own outcome -- except
    that anything which is not already CANCELED or COMPLETED becomes
    INTERRUPTED once a byte reached the client, which is the commitment
    invariant written once in `errors.decide` and restated here because a
    session has no `ExecutionResult` to run through it.
    """
    if error is None:
        return errors.Outcome.COMPLETED
    return errors.decide(error, committed=committed).outcome

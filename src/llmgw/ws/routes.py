"""The upgrade: the same refusal order as a request, with a different "no".

`PassthroughEndpoint.__call__` refuses in a fixed order and the order is the
contract -- cheapest first, each refusal costing the client less than the
step after it would have. This endpoint reproduces it exactly:

    stream_entered() + llmgw_streams_open        a socket IS a stream (C23)
    draining                 -> 503 draining
    tenant                   -> 401 unauthenticated
    over_capacity()          -> 503 overloaded, Retry-After: 1
    workload                 -> 400 policy_error
    admission.admit()        -> 429   (held for the socket's life)
    admission.enter_session()-> 429   (max_sessions; new, C23)
    limiter.acquire()        -> 429   (the credential's in-flight cap)
    breaker tickets          -> 503   (the circuit already knows)
    upstream connect         -> 502/503/504 by taxonomy

Every one of those is a REAL HTTP RESPONSE, sent with the gateway's existing
error body and `X-Gw-*` headers, through the ASGI `websocket.http.response`
extension. That matters more than it looks: Starlette's default for a
refused upgrade is a close 1008 after the 101, which tells an SDK "the
server accepted you and then changed its mind" and gives a plugin no status
to raise. The LiveKit plugins already handle an HTTP status on the upgrade
(`stt.py:322-326` raises `APIStatusError`), so a 429 with a `Retry-After`
reaches the consumer as a path it has already written code for.

--------------------------------------------------------------------------
After the 101 there is only a close code
--------------------------------------------------------------------------

Once `accept()` has gone out, no failure may be reported as a frame (C25).
The endpoint's post-101 error handling is therefore one line -- the relay
returns a verdict and the verdict is a close -- and everything interesting
happens in `relay.py`. What is left here is the bookkeeping that has to
survive a cancel: the permits, the tickets, the record, the metric pair.

--------------------------------------------------------------------------
The shutdown cut
--------------------------------------------------------------------------

Uvicorn cancels this task three seconds after our own grace runs out, once
per still-open socket, in the same instant. Finding 41 says what that costs
if each one writes to stderr: 4 KB tracebacks hung the S8-B workers
outright, and the 88-byte WARNING that replaced them was still about 450
streams from doing the same. So the cancel is absorbed exactly as
`PassthroughEndpoint` absorbs it -- `uncancel()`, count it in
`ShutdownCuts`, and write NOTHING. The per-socket detail is in the capture
record the session already wrote.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any

from starlette.responses import Response
from starlette.routing import WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from llmgw import errors
from llmgw.ws import client as ws_client
from llmgw.ws.errors import CloseCode, CloseVerdict, close_code_class, verdict_for
from llmgw.ws.relay import TEARDOWN_TIMEOUT, Direction, Relay, close_within
from llmgw.ws.session import Session, new_session_id
from llmgw.ws.surfaces.base import Frame, surface_auth_schemes

log = logging.getLogger("llmgw.ws.routes")

__all__ = ["ClientSide", "UpstreamSide", "WsEndpoint", "build_ws_routes"]

SESSION_ID_HEADER = b"x-gw-session-id"
BODY_MODIFIED_HEADER = b"x-gw-body-modified"


# ==========================================================================
# The two Side adapters
# ==========================================================================


class ClientSide:
    """`relay.Side` over a Starlette `WebSocket`.

    Frames are read at the ASGI message level rather than through
    `receive_text`/`receive_bytes`, because the relay must preserve the
    OPCODE the client chose: a proxy that turned a TEXT frame into a BINARY
    one has edited the message at the protocol layer, and Inworld answers a
    BINARY frame with an error (probe 5) where it would have answered the
    same bytes as TEXT normally.
    """

    __slots__ = ("_ws", "close_code", "close_reason", "_closed")

    def __init__(self, websocket: WebSocket) -> None:
        self._ws = websocket
        self.close_code: int | None = None
        self.close_reason = ""
        self._closed = False

    async def recv(self) -> Frame | None:
        try:
            message = await self._ws.receive()
        except (WebSocketDisconnect, RuntimeError):
            self.close_code = self.close_code or 1006
            return None
        if message["type"] == "websocket.disconnect":
            self.close_code = message.get("code", 1005)
            self.close_reason = message.get("reason", "") or ""
            return None
        text = message.get("text")
        if text is not None:
            return Frame(text.encode("utf-8"), text=True)
        return Frame(bytes(message.get("bytes") or b""), text=False)

    async def send(self, frame: Frame) -> None:
        if frame.text:
            await self._ws.send_text(frame.data.decode("utf-8", "replace"))
        else:
            await self._ws.send_bytes(frame.data)

    async def close(self, code: int, reason: str) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(Exception):
            await self._ws.close(code=code, reason=reason)


class UpstreamSide:
    """`relay.Side` over a `websockets` client connection."""

    __slots__ = ("_conn", "close_code", "close_reason", "_closed")

    def __init__(self, connection: Any) -> None:
        self._conn = connection
        self.close_code: int | None = None
        self.close_reason = ""
        self._closed = False

    async def recv(self) -> Frame | None:
        from websockets.exceptions import ConnectionClosed

        try:
            message = await self._conn.recv()
        except ConnectionClosed as exc:
            rcvd = exc.rcvd
            self.close_code = rcvd.code if rcvd is not None else 1006
            self.close_reason = (rcvd.reason if rcvd is not None else "") or ""
            return None
        return Frame.of(message)

    async def send(self, frame: Frame) -> None:
        await self._conn.send(frame.wire())

    async def close(self, code: int, reason: str) -> None:
        if self._closed:
            return
        self._closed = True
        with suppress(Exception):
            await self._conn.close(code, reason)


# ==========================================================================
# The endpoint
# ==========================================================================



def _scrubs_all(target: Any) -> bool:
    """True when this target's provider row says every error body of its is
    unsafe to show a tenant (`scrub_error_bodies="all"`).

    `Target.provider` is the `ProviderConn` itself, not its id
    (`catalog.py:995-999`), so this reads the row directly rather than
    looking one up. Total, and it fails CLOSED: no target, no row, or a row
    that does not say, and the answer is "scrub". The safe default for "we do
    not know what this provider writes into its errors" is not to forward
    it -- one of them writes our own key fragment there."""
    row = getattr(target, "provider", None)
    if row is None:
        return True
    return getattr(row, "scrub_error_bodies", "all") == "all"

class WsEndpoint:
    """One `WsSurface` on one route. One instance serves both the bare route
    and its `/workloads/{w}` twin, exactly as `PassthroughEndpoint` does --
    a second instance would be a second place for the two forms to drift."""

    def __init__(self, gateway: Any, *, surface: Any, route: str) -> None:
        self._gateway = gateway
        self._surface = surface
        self._route = route
        self._schemes = surface_auth_schemes(surface)
        # Pre-encoded once, as `PassthroughEndpoint` does: ASGI hands header
        # names back as bytes and re-decoding every one per upgrade is work
        # done to make a comparison look nicer. The allowlist is the same
        # one the HTTP plane uses -- `ServerConfig.forward_request_headers`,
        # which already refuses to name a credential or a connection header
        # -- so a header a client may forward to a provider over HTTP is
        # exactly the set it may forward on an upgrade.
        self._forward = frozenset(
            h.lower().encode("latin-1") for h in gateway.config.forward_request_headers
        )

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        """Raw ASGI, like `PassthroughEndpoint`, and for the same reason.

        Starlette's `websocket_session` wrapper would build the `WebSocket`,
        run the endpoint, and close the socket for us on the way out -- a
        close we have already sent with a code and a reason that matter, and
        a second one is a protocol error. Taking the three arguments directly
        is also what makes `websocket.http.response` reachable: the wrapper
        has no notion of refusing an upgrade with a status.
        """
        websocket = WebSocket(scope, receive=receive, send=send)
        gw = self._gateway
        surface_name = self._surface.name
        collectors = gw.collectors
        session_id = new_session_id()

        snapshot = gw.policy.current()
        derived = gw.derived(snapshot)
        requested = _requested_workload(scope)
        state = _Headers(
            policy_id=snapshot.id, catalog_id=derived.catalog_id,
            workload_id=requested or snapshot.default_workload,
            session_id=session_id,
        )

        gw.stream_entered()
        if collectors is not None:
            collectors.stream_open(surface=surface_name)
        try:
            await self._serve(websocket, snapshot, state, session_id, requested)
        except asyncio.CancelledError:
            # ---------------------- SHUTDOWN CUT --------------------------
            # See the module docstring, and app.py's twin of this block. One
            # cancel per open socket in the same instant; the fact is
            # COUNTED and stderr gets nothing.
            if not gw.draining:
                raise
            task = asyncio.current_task()
            if task is not None:
                task.uncancel()
            gw.shutdown_cuts.note(target=state.target, committed=state.committed)
            with suppress(Exception):
                await websocket.close(
                    code=int(CloseCode.DRAINING),
                    reason=CloseVerdict(CloseCode.DRAINING).reason,
                )
            return
        finally:
            gw.stream_exited()
            if collectors is not None:
                collectors.stream_close(surface=surface_name)

    # ------------------------------------------------------------- serve

    async def _serve(
        self, websocket: WebSocket, snapshot: Any, state: _Headers,
        session_id: str, requested: str | None,
    ) -> None:
        gw = self._gateway
        config = gw.config
        surface = self._surface
        scope = websocket.scope

        if gw.draining:
            gw.note_draining_denied()
            await _deny(websocket, state, 503, "draining",
                        "gateway is draining and is not accepting new sessions")
            return
        try:
            state.tenant = tenant = gw.resolve_tenant(scope, schemes=self._schemes)
        except Exception as exc:  # noqa: BLE001 - app.Unauthenticated, by duck type
            await _deny(websocket, state, 401, "unauthenticated", str(exc),
                        extra=[(b"www-authenticate", b"Bearer")])
            return
        if gw.over_capacity():
            gw.note_overloaded_denied()
            await _deny(
                websocket, state, 503, "overloaded",
                f"gateway is at its per-process stream cap ({config.max_streams}); "
                f"retry against another replica",
                extra=[(b"retry-after", b"1")],
            )
            return

        permits: list[Any] = []
        tickets: list[tuple[Any, Any]] = []
        session: Session | None = None
        upstream = None
        verdict_error: errors.GatewayError | None = None
        """How the SESSION ended, for the breaker. Filled in below and read
        in the `finally`, because a ticket taken before the 101 is settled
        long after it and the fact that settles it is only known at the end."""
        try:
            workload_id = gw.resolve_workload(snapshot, requested)
            state.workload_id = workload_id
            workload = snapshot.workloads[workload_id]
            budgets = workload.budgets
            plan = snapshot.plan_for(workload_id, kind=surface.dialect)

            permit = gw.admission.admit(tenant)
            permits.append(permit)
            permits.append(gw.admission.enter_session(tenant))

            session = Session(
                gw, surface, tenant=tenant, workload_id=workload_id, plan=plan,
                snapshot=snapshot, budgets=budgets, clock=gw.clock,
                session_id=session_id,
            )
            gw.ws_sessions.add(session)

            upstream, target, attempts = await self._open_upstream(
                plan, budgets, permits, tickets, state,
                extra_headers=dict(_forwarded(scope, self._forward)),
            )
            session.target = target
            session.attempts = attempts
            state.target = target
            state.attempts = attempts
            telemetry = _parse_telemetry(upstream.response_headers)
            session.upstream_request_id = telemetry
            state.upstream_request_id = telemetry

            await websocket.accept(
                subprotocol=upstream.subprotocol or None, headers=state.headers(),
            )
            state.accepted = True
            verdict_error = await self._relay(
                websocket, upstream, session, budgets, state,
                # Whatever the pre-101 walk did not reach. C24's post-101
                # fallback may use them, once, with the config prefix only.
                remaining=[t for t in plan.targets if t is not target],
            )
        except errors.GatewayError as err:
            verdict_error = err
            if state.accepted:
                # Past the 101: a close code, never a frame (C25). This is
                # the path a post-accept policy error takes -- a `modelId`
                # that routes to another provider, say.
                verdict = CloseVerdict(verdict_for(err), err)
                self._count_close(int(verdict.code), "client")
                with suppress(Exception):
                    await websocket.close(int(verdict.code), verdict.reason)
                if session is not None:
                    self._write_record(session, _empty_result(err))
            else:
                provider_row = gw.config.catalog.providers.get(err.provider or "")
                await _deny_error(
                    websocket, state, err,
                    scrub_all=getattr(provider_row, "scrub_error_bodies", "auth") == "all",
                )
        finally:
            if session is not None:
                gw.ws_sessions.discard(session)
            # Settle the circuits with what the session actually learned, not
            # with a bare release. A relayed session is the only evidence
            # about a provider that this plane ever produces, and the most
            # important verdict it produces -- an Inworld `error` code 7 and
            # a close, meaning the credential is refused -- happens entirely
            # AFTER the 101, where the upstream-connect loop that took the
            # tickets is long gone. Releasing without recording would leave
            # the credential circuit closed over a key that is known bad, and
            # every session after it would pay another handshake to find out.
            #
            # `decide()` supplies the commitment rule: a failure after a byte
            # reached the client keeps the error's health signal but is
            # INTERRUPTED rather than FAILED. A clean end records nothing,
            # which is what `release` means.
            # WHICH circuit hears it is the disposition's `health_key`,
            # verbatim, and the other is released: a stall says nothing about
            # the credential and a refused key says nothing about the model.
            # `Breaker.record()` refuses a mismatched key outright, which is
            # what makes the split mandatory rather than tidy
            # (`executor._settle`, same rule, same reason).
            if verdict_error is not None:
                _attribute(verdict_error, state.target)
            disposition = (
                errors.decide(verdict_error, committed=state.committed)
                if verdict_error is not None else None
            )
            for breaker, ticket in reversed(tickets):
                with suppress(Exception):
                    if disposition is None or ticket.key == disposition.health_key:
                        breaker.record(ticket, disposition)
                    else:
                        breaker.release(ticket)
            for held in reversed(permits):
                with suppress(Exception):
                    held.release()
            if upstream is not None:
                # Bounded for the same reason the relay's own closes are: a
                # provider socket whose send buffer is full does not notice
                # a close until the write does, and an unbounded await here
                # would hold the permits released just below it.
                with suppress(Exception):
                    await asyncio.wait_for(
                        upstream.connection.close(), TEARDOWN_TIMEOUT,
                    )

    # ---------------------------------------------------------- upstream

    async def _open_upstream(
        self, plan: Any, budgets: Any, permits: list[Any],
        tickets: list[tuple[Any, Any]], state: _Headers,
        *, extra_headers: dict[str, str] | None = None,
    ) -> tuple[Any, Any, int]:
        """Walk the plan until one target's upgrade succeeds. Pre-101.

        The executor's attempt loop in miniature, and deliberately not a
        reuse of it: `Executor.execute` is built around a request, a body and
        a pump, none of which exist here. What IS reused is everything that
        makes an attempt an attempt -- the credential cap, both breaker
        circuits, the taxonomy, and `Deadline` -- because a socket that
        skipped them would be a second path to the same providers with none
        of the protections the first path has.

        No frame has been relayed and the client has not been accepted, so a
        failure here is an ordinary HTTP refusal and fallback is free (C24).
        """
        from llmgw.clocks import Deadline
        from llmgw.executor import credential_health_key

        gw = self._gateway
        config = gw.config
        last: errors.GatewayError | None = None
        attempts = 0
        for target in plan.targets:
            attempts += 1
            state.attempts = attempts
            held: list[Any] = []
            taken: list[tuple[Any, Any]] = []
            try:
                held.append(gw.limiter.acquire(
                    target.credential_key, target.provider.max_concurrency,
                ))
                for key in (target.health_key, credential_health_key(target)):
                    breaker = gw.breakers.for_key(key)
                    try:
                        taken.append((breaker, breaker.acquire()))
                    except errors.BreakerOpen:
                        state.breaker = breaker.state.value
                        raise
                deadline = Deadline.start(
                    max(budgets.connect, 1.0) * 2, clock=gw.clock,
                )
                upstream = await ws_client.connect_upstream(
                    target, self._surface.upstream_path,
                    subprotocols=self._surface.subprotocols,
                    extra_headers=extra_headers,
                    budgets=budgets, deadline=deadline,
                    max_size=config.max_frame_bytes,
                )
            except errors.GatewayError as err:
                # Evidence, on both circuits, exactly as the executor records
                # it: a refused upgrade is as good a health signal as a
                # refused POST, and a credential rejected here must open the
                # credential circuit and not the model's.
                disposition = errors.decide(err, committed=False)
                for breaker, ticket in taken:
                    with suppress(Exception):
                        breaker.record(ticket, disposition)
                for one in held:
                    with suppress(Exception):
                        one.release()
                last = err
                if not disposition.try_next:
                    raise
                continue
            permits.extend(held)
            tickets.extend(taken)
            return upstream, target, attempts
        raise last or errors.NoTargetsAvailable(
            "no target accepted a websocket upgrade", workload=state.workload_id,
        )

    # ------------------------------------------------------------- relay

    async def _relay(
        self, websocket: WebSocket, upstream: Any, session: Session,
        budgets: Any, state: _Headers, *, remaining: list[Any] = (),
    ) -> errors.GatewayError | None:
        """Run the session, falling back once per remaining target. C24.

        The loop exists for one case and it is the case the captures made
        real: on Inworld the credential is not checked until the first client
        frame, so "this key is refused" arrives AFTER the client has been
        accepted, and the pre-101 walk that `_open_upstream` performs cannot
        see it. At that instant nothing has reached the client and nothing of
        the client's CONTENT has reached a provider, so the session is still
        recoverable -- and the only thing that may be replayed to the next
        target is the config prefix the relay kept (`create`, capped at 64
        KiB). Content is never replayed, which is why
        `Relay.fallback_eligible` insists on `content_forwarded` being False:
        replaying an utterance bills the tenant twice for one sentence.

        The client socket survives a failed attempt untouched -- it has been
        told nothing, which is the whole point -- so `close_client_on_finish`
        is False while a target remains.
        """
        gw = self._gateway
        config = gw.config
        limits = config.limits_for(self._surface.name)
        collectors = gw.collectors
        surface_name = self._surface.name
        targets = list(remaining)

        def on_bytes(direction: Direction, n: int) -> None:
            if collectors is not None:
                with suppress(Exception):
                    collectors.ws_bytes(
                        surface=surface_name, direction=direction.value, n=n,
                    )

        if collectors is not None:
            with suppress(Exception):
                collectors.ws_session_open(surface=surface_name)
        client = ClientSide(websocket)
        replay: tuple[Frame, ...] = ()
        result = None
        relay = None
        client_close_sent: tuple[int, str] | None = None
        try:
            while True:
                relay = Relay(
                    surface=self._surface,
                    client=client,
                    upstream=UpstreamSide(upstream.connection),
                    budgets=budgets,
                    clock=gw.clock,
                    max_in_bps=limits.max_in_bps,
                    max_out_bps=limits.max_out_bps,
                    buffer_bytes=config.buffer_bytes,
                    max_frame_bytes=config.max_frame_bytes,
                    on_bytes=on_bytes,
                    product=self._surface.product,
                    scrub_errors=_scrubs_all(state.target),
                    rewrite_first=lambda frame: session.resolve_first_frame(frame)[0],
                    # NEVER by the relay: whether this attempt is the last
                    # is not known until its error has been classified, and a
                    # relay that closed the client on an attempt we then fall
                    # back from would have told the client about an attempt
                    # it is not supposed to see. The endpoint closes, once,
                    # below.
                    close_client_on_finish=False,
                    replay=replay,
                )
                session.attach(relay)
                result = await relay.run()
                error = result.error
                if (
                    not targets
                    or error is None
                    or not error.try_next
                    or not relay.fallback_eligible
                ):
                    break
                next_target = targets.pop(0)
                replay = tuple(relay.config_prefix)
                try:
                    upstream = await self._reopen(next_target, budgets, state)
                except errors.GatewayError:
                    # The next target would not take an upgrade either. Keep
                    # the FIRST failure as the verdict: it is the one that
                    # describes what the client's own session hit, and it is
                    # what `send_error`'s HTTP twin would have reported.
                    targets.clear()
                    break
                session.target = next_target
                state.target = next_target
                session.attempts = state.attempts
        finally:
            with suppress(Exception):
                if relay is not None:
                    # Remember what the CLIENT was actually told. A gateway
                    # verdict (4900 drain, 4902 stall, 4903 client-stall,
                    # 4906 idle, 4907) never reaches `result.client_close`,
                    # which only records a close the CLIENT sent or one the
                    # relay sent itself -- so counting that field alone left
                    # `llmgw_ws_close_total{side="client"}` silent for every
                    # verdict we issue, which is precisely the series
                    # FAILURE-MODES rows 33 and 34 tell an operator to watch.
                    client_close_sent = relay.closing()
                    await close_within(client, *client_close_sent)
            if collectors is not None:
                with suppress(Exception):
                    collectors.ws_session_close(surface=surface_name)
        if result is None:  # pragma: no cover - the loop always runs once
            return None
        state.committed = result.committed

        if collectors is not None:
            with suppress(Exception):
                collectors.ws_session_seconds(
                    surface=surface_name, seconds=gw.clock.now() - session.started_at,
                )
                if result.inband_errors:
                    collectors.ws_inband_error(
                        surface=surface_name, fatal=result.error is not None,
                    )
            sent_code = (
                client_close_sent[0] if client_close_sent is not None
                else result.client_close[0]
            )
            self._count_close(sent_code, "client")
            self._count_close(result.upstream_close[0], "upstream")
        self._write_record(session, result)
        return result.error

    async def _reopen(self, target: Any, budgets: Any, state: _Headers) -> Any:
        """One more upstream socket, for a post-101 fallback.

        Deliberately thinner than `_open_upstream`: the permits and tickets
        this session holds were taken for the FIRST target and are released
        together at the end, and taking a second credential permit here would
        double-count one session against the credential's cap. The new
        target's circuits are consulted through the same registry, so an open
        one still refuses.
        """
        from llmgw.clocks import Deadline

        gw = self._gateway
        state.attempts += 1
        deadline = Deadline.start(max(budgets.connect, 1.0) * 2, clock=gw.clock)
        return await ws_client.connect_upstream(
            target, self._surface.upstream_path,
            subprotocols=self._surface.subprotocols,
            budgets=budgets, deadline=deadline,
            max_size=gw.config.max_frame_bytes,
        )

    # ------------------------------------------------------------ record

    def _count_close(self, code: int | None, side: str) -> None:
        collectors = self._gateway.collectors
        if collectors is None or code is None:
            return
        with suppress(Exception):
            collectors.ws_close(
                surface=self._surface.name, side=side,
                code_class=close_code_class(code),
            )

    def _write_record(self, session: Session, result: Any) -> None:
        """The session's terminal record and its exactly-once metric.

        Never raises. A record hook that fails on the cancel path would
        replace a clean truncation with an accounting incident, which is the
        rule `app._record` states and the reason this whole block is inside
        one `except`.
        """
        gw = self._gateway
        try:
            record = session.build_record(result)
            capture = gw.capture
            if capture is not None:
                capture.offer(record)
            collectors = gw.collectors
            if collectors is not None:
                collectors.request(
                    surface=self._surface.name,
                    outcome=record.outcome,
                    code=record.error_code or "none",
                )
                collectors.request_duration(
                    surface=self._surface.name, outcome=record.outcome,
                    seconds=record.duration_s or 0.0,
                )
                if record.committed:
                    collectors.committed(surface=self._surface.name)
                provider = record.provider
                model = record.model
                if provider and model and provider != "-":
                    for unit, n in record.units.items():
                        if n:
                            collectors.units(
                                provider=provider, model=model, unit=unit, n=n,
                            )
                    if record.cost_usd:
                        collectors.cost(
                            provider=provider, model=model,
                            basis=record.basis, usd=record.cost_usd,
                        )
                    if record.first_event_latency is not None:
                        collectors.time_to_first_event(
                            provider=provider, model=model,
                            seconds=record.first_event_latency,
                        )
        except Exception:  # noqa: BLE001 - the hook observes; it does not vote
            log.exception("ws record hook failed for %s", self._route)


# ==========================================================================
# Pre-101 refusals
# ==========================================================================


class _Headers:
    """The `X-Gw-*` set for this upgrade, assembled as facts arrive.

    The socket-plane twin of `Exchange`, and smaller for one reason: the
    fields `Exchange` exists to carry across a status-commitment boundary
    have no analogue here, because a WebSocket has exactly one boundary and
    it is the 101.
    """

    __slots__ = ("policy_id", "catalog_id", "workload_id", "session_id", "tenant",
                 "target", "attempts", "breaker", "upstream_request_id",
                 "accepted", "committed", "body_modified")

    def __init__(self, *, policy_id: str, catalog_id: str, workload_id: str,
                 session_id: str) -> None:
        self.policy_id = policy_id
        self.catalog_id = catalog_id
        self.workload_id = workload_id
        self.session_id = session_id
        self.tenant: str | None = None
        self.target: Any = None
        self.attempts = 0
        self.breaker: str | None = None
        self.upstream_request_id: str | None = None
        self.accepted = False
        self.committed = False
        self.body_modified = True
        """True by default on an accept-then-relay surface, and it is a
        WARRANT rather than a report. The 101 goes out before the first
        client frame exists, so the gateway cannot yet know whether it will
        rewrite `create.modelId` -- but it can know that it WILL if the id
        differs from the target's wire id, which is the only thing the header
        has ever promised a client: that the bytes it sends may not be the
        bytes the provider sees, and that the difference is this one field
        (C23, PLAN-G 4.4)."""

    def headers(self) -> list[tuple[bytes, bytes]]:
        from llmgw.server.app import UPSTREAM_REQUEST_ID_HEADER, gw_headers

        out = gw_headers(
            policy_id=self.policy_id,
            catalog_id=self.catalog_id,
            workload_id=self.workload_id,
            target=self.target,
            attempts=self.attempts,
            tenant=self.tenant,
            breaker=self.breaker,
        )
        out.append((SESSION_ID_HEADER, self.session_id.encode("ascii")))
        if self.body_modified:
            out.append((BODY_MODIFIED_HEADER, b"1"))
        if self.upstream_request_id:
            out.append((
                UPSTREAM_REQUEST_ID_HEADER,
                self.upstream_request_id.encode("latin-1", "replace"),
            ))
        return out


async def _deny(
    websocket: WebSocket, state: _Headers, status: int, code: str, message: str,
    *, extra: list[tuple[bytes, bytes]] | None = None,
) -> None:
    """A pre-101 refusal, as the HTTP response the HTTP plane would send.

    Same body shape, same `X-Gw-*`, same status. A client that understands
    the gateway's 429 on `/v1/chat/completions` understands this one.
    """
    body = json.dumps({"error": {"type": code, "message": message}}).encode("utf-8")
    response = Response(
        content=body, status_code=status, media_type="application/json",
    )
    for name, value in (*state.headers(), *(extra or ())):
        response.raw_headers.append((name, value))
    await _send_denial(websocket, response)


async def _deny_error(
    websocket: WebSocket, state: _Headers, err: errors.GatewayError, *,
    scrub_all: bool,
) -> None:
    """C4 on the upgrade: the provider's own status and body pass through
    once no fallback remains, except where C11 (and B2's widening for
    Inworld) replaces the body with ours."""
    body = err.upstream_body if (err.passthrough and err.upstream_status) else None
    if scrub_all or isinstance(err, errors.AuthenticationFailed):
        body = None
    if not body:
        code = err.code
        message = err.message
        if isinstance(err, errors.AuthenticationFailed):
            code = "upstream_auth"
            message = (
                f"provider rejected the gateway's credential for "
                f"{err.provider or 'upstream'}"
            )
        body = json.dumps({"error": {"type": code, "message": message}}).encode("utf-8")
    response = Response(
        content=body, status_code=err.client_status, media_type="application/json",
    )
    for name, value in state.headers():
        response.raw_headers.append((name, value))
    if err.retry_after is not None:
        response.raw_headers.append(
            (b"retry-after", f"{max(0, int(err.retry_after + 0.999)):d}".encode("ascii")),
        )
    await _send_denial(websocket, response)


async def _send_denial(websocket: WebSocket, response: Response) -> None:
    """`send_denial_response` where the server supports it, a close otherwise.

    The extension is in uvicorn's sansio implementation
    (`websockets_sansio_impl.py`) and `lifecycle.py` pins that implementation
    -- so the fallback is for a future uvicorn that drops it and for test
    harnesses that do not advertise it (PLAN-G R13). Degrading to a close
    1008 loses the status but never hangs the client, which is the right
    order of badness.
    """
    extensions = websocket.scope.get("extensions") or {}
    if "websocket.http.response" in extensions:
        await websocket.send_denial_response(response)
        return
    with suppress(Exception):  # pragma: no cover - only without the extension
        await websocket.close(code=1008, reason="llmgw:refused")


def _requested_workload(scope: Any) -> str | None:
    """The workload named by the path. NOT by a header: the LiveKit Inworld
    TTS plugin sends none we control, and a header a consumer cannot set is
    not a routing input. The path twin exists for callers that can."""
    params = scope.get("path_params") or {}
    value = params.get("workload")
    return str(value) if value else None


def _attribute(err: errors.GatewayError, target: Any) -> None:
    """Name the target on an error raised deep in the relay, so the circuits
    can find it.

    A surface classifies a frame without knowing which target the socket is
    open to -- it sees `{"error":{"code":7}}` and nothing else -- so the
    error it builds carries a provider name and no model and no credential
    id. `GatewayError.health_key()` then answers `("cred", "?")` for a
    credential failure, which matches no ticket, and the credential circuit
    never hears that the key is bad. Filling the fields in here, once, at the
    only place that holds both the error and the target, is cheaper than
    threading the target through every classifier.

    Only ABSENT fields are filled: an error that already named a provider
    knew something this function does not.
    """
    if target is None:
        return
    if not err.provider:
        err.provider = target.provider.id
    if not err.model:
        err.model = target.model.id
    if not err.credential_id:
        err.credential_id = target.credential_key


def _forwarded(scope: Any, allowed: frozenset[bytes]) -> dict[str, str]:
    """`app.forwarded_request_headers`, imported lazily.

    `app` imports `build_ws_routes` from this module at module scope, so this
    module cannot import `app` at module scope. Every such call is behind a
    function: the alternative is a lazy import in `app` instead, which would
    move the cycle rather than break it and would put the ws route table's
    construction behind a runtime import in the one function a startup
    failure has to be legible in.
    """
    from llmgw.server.app import forwarded_request_headers

    return forwarded_request_headers(scope, allowed)


def _parse_telemetry(headers: Any) -> str | None:
    from llmgw.server.app import parse_upstream_telemetry

    return parse_upstream_telemetry(headers).request_id


def _empty_result(err: errors.GatewayError) -> Any:
    from llmgw.ws.relay import RelayResult

    return RelayResult(error=err)


# ==========================================================================
# Mounting
# ==========================================================================


def build_ws_routes(gateway: Any) -> list[WebSocketRoute]:
    """Every registered ws surface, twice: bare and workload-prefixed.

    The twin is registered even for Inworld TTS, whose plugin cannot reach it
    (`urljoin` drops the prefix, tts.py:259). It costs one route and it is
    the only way any OTHER caller of the same provider can name a workload --
    and leaving it out would make the socket plane the one place where
    `/workloads/{w}` is not a universal address.
    """
    from llmgw.server.app import WORKLOAD_ROUTE_PREFIX
    from llmgw.ws import WS_REGISTRY

    routes: list[WebSocketRoute] = []
    for surface in WS_REGISTRY:
        for route in surface.routes:
            endpoint = WsEndpoint(gateway, surface=surface, route=route)
            routes.append(WebSocketRoute(route, endpoint, name=f"{surface.name}_ws"))
            routes.append(WebSocketRoute(
                f"{WORKLOAD_ROUTE_PREFIX}{route}", endpoint,
                name=f"{surface.name}_ws_by_workload",
            ))
    return routes

"""Admission: the layer that says no before anything has been spent.

Layer 3 in the mental model. A request reaches this module having cost the
gateway a header parse and a policy lookup, and this module's job is to refuse
it -- if it is going to be refused -- having spent one dictionary lookup and
an integer compare more. Nothing here opens a socket, allocates a buffer, or
awaits. That is not an optimisation; it is the property that makes overload
protection protect. Shedding load must cost less than serving it, or the
rejection path becomes the overload (CONTRACTS.md C6).

--------------------------------------------------------------------------
Two limits, because two different things run out
--------------------------------------------------------------------------

A request-rate limit bounds arrivals. The thing that fills memory and holds
provider connections is arrivals TIMES duration -- Little's law -- and for an
LLM gateway the duration is the model's choice, not the client's. Sixty
requests a minute at one second each is one stream in flight; sixty a minute
at ninety seconds each is ninety. The RPM dashboard is green in both cases.

So every tenant carries both:

    token bucket      bounds the RATE. Fixed by waiting: the bucket refills.
    concurrency cap   bounds IN-FLIGHT work. Fixed by finishing: a permit is
                      released when the request ends and not before.

They fail differently and the client's remedy differs, which is why they are
two error classes and not one 429: `AdmissionRejected` carries a real
`retry_after`, because waiting that long is guaranteed to help.
`ConcurrencyRejected` carries none, because backing off does nothing until
one of the tenant's own requests completes -- and that is out of the client's
hands.

--------------------------------------------------------------------------
C6 -- denial is free, and the check ORDER is how
--------------------------------------------------------------------------

`admit()` checks the concurrency permit before it touches the token bucket.
A tenant at its concurrency cap is refused with `ConcurrencyRejected` and no
token is consumed. Only a request that has cleared the permit check draws a
token; if the bucket is empty, `AdmissionRejected` is raised and the tenant's
in-flight count is unchanged.

The reason is a double charge. A client at its concurrency limit that is also
debited a rate token for the refusal has paid twice for one denial: it can
neither run the request (no permit) nor run the NEXT one once a permit frees
(no token). Reverse the order and a saturated tenant retrying politely at its
rate limit is bled dry of rate credit for requests that never had a chance.
The concurrency check is also the cheaper of the two -- one integer compare,
no clock read, no float arithmetic -- so it is the right one to fail on first
for the same reason the whole layer sits early in the stack.

--------------------------------------------------------------------------
Lazy refill
--------------------------------------------------------------------------

The bucket refills on `admit()` from the injected clock: `min(burst, tokens +
elapsed * rate)`. There is no background task and no timer, and that is
correct rather than merely convenient: nothing can observe the bucket between
two calls, so a bucket refilled continuously and a bucket refilled at the
instant of the next read are indistinguishable. An idle tenant costs nothing.
A hundred thousand idle tenants cost nothing. A timer per tenant would make
idleness the expensive case, which is backwards.

--------------------------------------------------------------------------
The permit is the mitigation
--------------------------------------------------------------------------

Row 19 of FAILURE-MODES.md: a permit leaked on an unexpected error path.
Capacity ratchets to zero over hours and the gateway stops accepting work for
no visible reason. The defence is not a rule that says "remember to release"
-- rules like that hold until the first 3 a.m. patch -- it is the shape of the
`Permit` object: it is a context manager whose `__exit__` releases on normal
return, on exception, and on `CancelledError`, and a second release raises
`ValueError` instead of decrementing below zero. A negative permit count is
the mirror image of the leak, capacity created from nothing, and it is the
quieter bug of the two because it looks like generosity.

--------------------------------------------------------------------------
No lock, and why that is not a bug
--------------------------------------------------------------------------

A common reference implementation wraps the decision in a
`threading.Lock`. This module does not, because every decision here is one
synchronous function with no `await` inside it, and under asyncio that IS the
lock: no other coroutine can run between the check and the decrement. The
day `admit()` grows an `await` -- to consult a shared store, say -- that
guarantee is gone and the lock comes back. The docstring is here so that day
is recognised when it comes.

--------------------------------------------------------------------------
Honest residual
--------------------------------------------------------------------------

All of this is process-local. A fleet of N instances gives every tenant N
times its cap, and the provider-key cap is N times what the credential can
actually bear. The fix is shared state on the admission path, and it is not
built (FAILURE-MODES.md rows 6 and 7).
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass

from .clocks import Clock
from .errors import AdmissionRejected, ConcurrencyRejected, ProviderKeyExhausted

# Labels. These are the values `metrics.py` accepts for
# `llmgw_admission_denied_total{reason}` and `llmgw_permits_in_use{scope}`;
# `tests/unit/test_admission.py` asserts the two files agree so the
# registration code in P5 cannot be handed a label it will reject.
REASON_TENANT_RATE = "tenant_rate"
REASON_TENANT_CONCURRENCY = "tenant_concurrency"
REASON_PROVIDER_KEY_CONCURRENCY = "provider_key_concurrency"

SCOPE_TENANT = "tenant"
SCOPE_PROVIDER_KEY = "provider_key"
SCOPE_SESSION = "session"
"""The scope of a relayed-WebSocket-session permit (PLAN-G C23). Deliberately
NOT a `metrics.llmgw_permits_in_use{scope}` value: that gauge's label set is
closed at two, and a relayed session is already visible on
`llmgw_ws_sessions_open`. The scope is here so a `Permit`'s repr and the
probe can say what it counts."""

# `tokens` is a float accumulator. The tolerance absorbs representational
# error only -- a refill that lands on 0.9999999999999 after exactly 1/rate
# seconds is one token, and a client told to wait exactly that long and then
# refused would be entitled to call the number a lie.
TOLERANCE = 1e-9


@dataclass(frozen=True, slots=True)
class TenantLimits:
    """One tenant's admission budget.

    Frozen for the same reason `Budgets` is: these arrive from configuration
    and must not change under a request that was admitted against them.
    """

    rate_per_second: float
    """Token bucket refill rate. Sustained requests per second."""

    burst: int
    """Bucket capacity. How far ahead of the sustained rate a tenant may run
    before being asked to wait -- an agent loop firing ten calls in parallel
    is a burst, not a flood."""

    max_concurrency: int
    """In-flight permits. The limit that protects memory and connections."""

    max_sessions: int | None = None
    """Credentials the gateway will mint for this tenant that are alive at
    once (Phase E): a Realtime client secret or an AssemblyAI streaming token
    is a session the tenant may open outside the gateway, and this is the
    only place the gateway can bound how many. A reservation is held for the
    credential's TTL and released by expiry, never by a request ending --
    the gateway does not see the session end. None means no cap."""

    def validate(self) -> TenantLimits:
        if not math.isfinite(self.rate_per_second) or self.rate_per_second <= 0:
            # A zero rate would make `retry_after` infinite -- a denial with
            # no actionable number, which is the thing this module exists to
            # never produce. A tenant with no rate is a tenant with no access,
            # and that is expressed by not configuring it.
            raise ValueError("rate_per_second must be a positive finite number")
        if type(self.burst) is not int or self.burst < 1:
            raise ValueError("burst must be an integer >= 1")
        if type(self.max_concurrency) is not int or self.max_concurrency < 1:
            raise ValueError("max_concurrency must be an integer >= 1")
        if self.max_sessions is not None and (
            type(self.max_sessions) is not int or self.max_sessions < 1
        ):
            raise ValueError("max_sessions must be an integer >= 1, or None for no cap")
        return self


class Permit:
    """Held for the life of one request. Released exactly once, on every exit
    path.

    `with controller.admit(tenant):` is the intended shape. `__exit__` runs
    on normal return, on any exception, and on `asyncio.CancelledError`
    (which is a `BaseException`, and a `with` block does not care). The
    release itself is synchronous and never awaits, so there is no window in
    which a second cancellation can arrive between "we decided to release"
    and "we released".

    A second `release()` is a `ValueError`. It would be easy to make it a
    no-op, and that is exactly the wrong instinct: the caller that releases
    twice usually has a `finally` AND an explicit release, and the day the
    explicit one moves before an early return, the `finally` decrements a
    permit that belongs to some other request. The count goes negative,
    capacity appears from nothing, and the tenant quietly runs above its cap.
    A leak is visible on a gauge. A negative is not.
    """

    __slots__ = ("scope", "key", "_release", "_released")

    def __init__(self, *, scope: str, key: str, release: Callable[[], None]) -> None:
        self.scope = scope
        """`"tenant"` or `"provider_key"` -- the `llmgw_permits_in_use` label."""
        self.key = key
        """The tenant id, or the credential key, this permit counts against."""
        self._release = release
        self._released = False

    @property
    def tenant(self) -> str:
        """The tenant this permit was issued to. For a provider-key permit
        the holder is the credential, and this returns its key."""
        return self.key

    @property
    def released(self) -> bool:
        return self._released

    def release(self) -> None:
        if self._released:
            raise ValueError(
                f"{self.scope} permit for {self.key!r} released twice; the second "
                "release would create capacity from nothing"
            )
        self._released = True
        self._release()

    def __enter__(self) -> Permit:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        self.release()
        # Never suppress. The permit is bookkeeping; the exception is the
        # request's, and it has a taxonomy class waiting for it upstairs.
        return False

    async def __aenter__(self) -> Permit:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return self.__exit__(exc_type, exc, tb)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        state = "released" if self._released else "held"
        return f"<Permit {self.scope}={self.key!r} {state}>"


class _TenantState:
    """Everything the controller knows about one tenant, in one place, so that
    deciding tenant A's admission is a single dictionary lookup that never
    touches tenant B. Isolation is a data-layout property here, not a policy."""

    __slots__ = ("limits", "tokens", "updated", "in_use", "denied", "sessions",
                 "relayed")

    def __init__(self, limits: TenantLimits, now: float) -> None:
        self.limits = limits
        self.tokens = float(limits.burst)
        self.updated = now
        self.in_use = 0
        self.sessions: list[float] = []
        """Expiry instants of minted credentials still alive (Phase E)."""
        self.relayed = 0
        """Relayed WebSocket sessions open right now (PLAN-G C23).

        A COUNT, not a list of expiries, because the two kinds of session end
        differently and the difference is the whole reason they are separate
        fields. A minted credential is used against the provider directly and
        the gateway never sees it end, so the only honest release is the
        clock. A relayed session ends in our own `finally`, so counting it by
        expiry would either hold a slot after the socket closed or free one
        while it was still open. Both live under the SAME `max_sessions` cap,
        because the cap is about how many concurrent provider sessions a
        tenant may have and the provider does not care which door they came
        through."""
        self.denied: dict[str, int] = {
            REASON_TENANT_CONCURRENCY: 0,
            REASON_TENANT_RATE: 0,
        }

    def refill(self, now: float) -> None:
        # `max(0, ...)` guards a clock that moves backwards. SystemClock is
        # monotonic and cannot; a test clock or a future replacement might,
        # and a negative elapsed time would DRAIN the bucket, charging the
        # tenant for time that did not happen.
        self.tokens = self.projected_tokens(now)
        self.updated = max(now, self.updated)

    def projected_tokens(self, now: float) -> float:
        """The bucket as it would read if refilled now, without refilling it.
        For observers. `admit()` is the only writer."""
        elapsed = max(0.0, now - self.updated)
        refilled = self.tokens + elapsed * self.limits.rate_per_second
        return min(float(self.limits.burst), refilled)


class AdmissionController:
    """Per-tenant token bucket plus per-tenant concurrency permit.

    One instance per process. Tenants are configured explicitly with
    `configure()`; an unknown tenant is admitted under `default` if one was
    given and refused otherwise. A gateway that admits unknown tenants under
    no limit has no tenants -- it has one tenant with many names, and the
    first of them to misbehave takes the rest down.
    """

    def __init__(self, *, clock: Clock, default: TenantLimits | None = None) -> None:
        self._clock = clock
        self._default = default.validate() if default is not None else None
        self._tenants: dict[str, _TenantState] = {}
        self._denials: dict[str, int] = {
            REASON_TENANT_CONCURRENCY: 0,
            REASON_TENANT_RATE: 0,
        }

    # ---- configuration -----------------------------------------------------

    def configure(self, tenant: str, limits: TenantLimits) -> None:
        """Set or replace a tenant's limits.

        Replacing limits on a tenant with requests in flight keeps its
        in-flight count -- those permits will be released against this
        tenant, and starting the count at zero would make each release a
        decrement below the truth. The bucket is clamped to the new burst
        and otherwise left alone, so a tenant whose limits were RAISED is not
        handed a full bucket for free and one whose limits were LOWERED is not
        left holding more than its new capacity.
        """
        limits = limits.validate()
        state = self._tenants.get(tenant)
        if state is None:
            self._tenants[tenant] = _TenantState(limits, self._clock.now())
            return
        state.refill(self._clock.now())
        state.limits = limits
        state.tokens = min(state.tokens, float(limits.burst))

    def forget_idle(self, tenant: str) -> bool:
        """Drop a tenant's state if it is holding no permits and its bucket is
        full -- the two conditions under which a fresh state is
        indistinguishable from this one, so evicting it cannot mint tokens or
        lose a permit. Returns whether anything was dropped.

        Tenants admitted under `default` create state on first contact, and
        tenant ids arrive from the client, so without a janitor the table
        grows with every id ever seen. This is the janitor's primitive; the
        schedule that calls it is a P5 concern.
        """
        state = self._tenants.get(tenant)
        if state is None:
            return False
        if state.in_use:
            return False
        if state.projected_tokens(self._clock.now()) < state.limits.burst - TOLERANCE:
            return False
        del self._tenants[tenant]
        return True

    # ---- the decision ------------------------------------------------------

    def admit(self, tenant: str) -> Permit:
        """Admit one request for `tenant`, or raise.

        Check order is the contract (C6). Read the module docstring before
        rearranging it.

            1. unknown tenant, no default   -> AdmissionRejected (no state made)
            2. in_use >= max_concurrency    -> ConcurrencyRejected, bucket untouched
            3. refill; tokens < 1           -> AdmissionRejected with retry_after
            4. take a token, take a permit  -> Permit

        Steps 2 and 3 do not mutate. The first write happens at step 4, so
        there is no partial state to unwind on any denial path and nothing
        for a denial to leak.

        `retry_after` on step 3 is `(1 - tokens) / rate`: the exact time until
        the bucket holds one token, given no competing traffic from the same
        tenant. A denial without a real number produces an immediate retry,
        and an immediate retry against an empty bucket is another denial --
        the client and the gateway then spend the whole refill interval
        confirming to each other that the bucket is empty.
        """
        state = self._tenants.get(tenant)
        if state is None:
            if self._default is None:
                self._denials[REASON_TENANT_RATE] += 1
                raise AdmissionRejected(
                    f"unknown tenant {tenant!r}: no limits configured and no default"
                )
            state = self._tenants[tenant] = _TenantState(self._default, self._clock.now())

        limits = state.limits
        if state.in_use >= limits.max_concurrency:
            # FIRST, and before the clock is read. No refill, no debit: the
            # bucket is exactly as this call found it.
            state.denied[REASON_TENANT_CONCURRENCY] += 1
            self._denials[REASON_TENANT_CONCURRENCY] += 1
            raise ConcurrencyRejected(
                f"tenant {tenant!r} at concurrency cap "
                f"({state.in_use}/{limits.max_concurrency} in flight)"
            )

        state.refill(self._clock.now())
        if state.tokens + TOLERANCE < 1.0:
            retry_after = (1.0 - state.tokens) / limits.rate_per_second
            state.denied[REASON_TENANT_RATE] += 1
            self._denials[REASON_TENANT_RATE] += 1
            raise AdmissionRejected(
                f"tenant {tenant!r} rate budget exhausted; "
                f"next token in {retry_after:.3f}s",
                retry_after=retry_after,
            )

        state.tokens = max(0.0, state.tokens - 1.0)
        state.in_use += 1
        return Permit(scope=SCOPE_TENANT, key=tenant, release=lambda: self._release(tenant))

    def _release(self, tenant: str) -> None:
        state = self._tenants[tenant]
        if state.in_use <= 0:  # pragma: no cover - Permit guards this; belt and braces
            raise ValueError(f"tenant {tenant!r} has no permits in use to release")
        state.in_use -= 1

    # ---- minted sessions (Phase E) --------------------------------------------

    def reserve_session(self, tenant: str, ttl_s: float) -> None:
        """Count one minted credential against the tenant's `max_sessions`.

        A reservation lives for `ttl_s` and is released by the clock, never
        by a call: the session it authorises is opened directly with the
        provider and the gateway never sees it end. Expired reservations are
        pruned on every call, so the list is bounded by the cap. Denial is an
        `AdmissionRejected` with `retry_after` = the earliest expiry, which is
        an honest number (a slot WILL free then) where a bare 429 would not
        be. The rate bucket is the request's own concern (`admit()`), so a
        mint is charged twice: once as a request, once as a session.
        """
        state = self._tenants.get(tenant)
        if state is None:
            if self._default is None:
                self._denials[REASON_TENANT_RATE] += 1
                raise AdmissionRejected(
                    f"unknown tenant {tenant!r}: no limits configured and no default"
                )
            state = self._tenants[tenant] = _TenantState(self._default, self._clock.now())
        cap = state.limits.max_sessions
        if cap is None:
            return
        now = self._clock.now()
        state.sessions = [t for t in state.sessions if t > now]
        live = len(state.sessions) + state.relayed
        if live >= cap:
            # `retry_after` is the earliest MINTED expiry when there is one:
            # a relayed session has no expiry to promise, so a tenant holding
            # only relayed sessions gets a 429 with no number, which is the
            # honest answer ("when one of your sockets closes").
            soonest = min(state.sessions) if state.sessions else None
            state.denied[REASON_TENANT_CONCURRENCY] += 1
            self._denials[REASON_TENANT_CONCURRENCY] += 1
            raise AdmissionRejected(
                f"tenant {tenant!r} at its session cap "
                f"({live}/{cap} sessions alive)",
                retry_after=None if soonest is None else max(0.0, soonest - now),
            )
        state.sessions.append(now + max(0.0, float(ttl_s)))

    def enter_session(self, tenant: str) -> Permit:
        """Count one RELAYED session against the tenant's `max_sessions`.

        The socket-plane twin of `reserve_session` (PLAN-G C23), and the
        reason `max_sessions` is a cap on sessions rather than on mints: a
        tenant that opens two hundred Inworld sockets through the gateway has
        two hundred provider sessions open exactly as surely as a tenant that
        minted two hundred credentials, and a cap that could only see one of
        the two ways would be a cap an operator could not reason about.

        Returns a `Permit`, not None, because a relayed session ends where we
        can see it: the caller holds the permit for the socket's life and the
        `finally` releases it, the same shape `admit()` has. The two permits
        are separate objects on purpose -- one counts requests in flight, one
        counts sessions alive, and a socket is one of each.
        """
        state = self._tenants.get(tenant)
        if state is None:
            if self._default is None:
                self._denials[REASON_TENANT_RATE] += 1
                raise AdmissionRejected(
                    f"unknown tenant {tenant!r}: no limits configured and no default"
                )
            state = self._tenants[tenant] = _TenantState(self._default, self._clock.now())
        cap = state.limits.max_sessions
        if cap is not None:
            now = self._clock.now()
            state.sessions = [t for t in state.sessions if t > now]
            live = len(state.sessions) + state.relayed
            if live >= cap:
                soonest = min(state.sessions) if state.sessions else None
                state.denied[REASON_TENANT_CONCURRENCY] += 1
                self._denials[REASON_TENANT_CONCURRENCY] += 1
                raise AdmissionRejected(
                    f"tenant {tenant!r} at its session cap "
                    f"({live}/{cap} sessions alive)",
                    retry_after=None if soonest is None else max(0.0, soonest - now),
                )
        state.relayed += 1
        return Permit(
            scope=SCOPE_SESSION, key=tenant,
            release=lambda: self._release_session(tenant),
        )

    def _release_session(self, tenant: str) -> None:
        state = self._tenants[tenant]
        if state.relayed <= 0:  # pragma: no cover - Permit guards this
            raise ValueError(f"tenant {tenant!r} has no relayed session to release")
        state.relayed -= 1

    def live_sessions(self, tenant: str) -> int:
        """Sessions counted against `max_sessions`: minted credentials still
        inside their TTL, plus relayed sockets open now. Observation only."""
        state = self._tenants.get(tenant)
        if state is None:
            return 0
        now = self._clock.now()
        return sum(1 for t in state.sessions if t > now) + state.relayed

    # ---- observation -------------------------------------------------------

    def in_use(self, tenant: str) -> int:
        state = self._tenants.get(tenant)
        return 0 if state is None else state.in_use

    def total_in_use(self) -> int:
        """Sum across tenants. The `llmgw_permits_in_use{scope="tenant"}`
        gauge, and the number the chaos tier asserts is zero when idle."""
        return sum(s.in_use for s in self._tenants.values())

    def denials(self) -> dict[str, int]:
        """Denials since construction, keyed by `metrics.DENIAL_REASONS` value.
        P5 registers these against `llmgw_admission_denied_total`. An unknown
        tenant without a default is counted under `tenant_rate`: a tenant
        with no configured budget has a budget of nothing."""
        return dict(self._denials)

    def snapshot(self) -> dict[str, dict[str, object]]:
        """Per tenant: `in_use`, `tokens` (projected to now, not refilled --
        an observer must not be a writer), `limits`, and `denied` by reason.

        Cheap by construction so that asserting "everything returned to zero"
        after a chaos run is a one-liner and gets written."""
        now = self._clock.now()
        return {
            tenant: {
                "in_use": state.in_use,
                "sessions": sum(1 for t in state.sessions if t > now),
                "tokens": state.projected_tokens(now),
                "limits": state.limits,
                "denied": dict(state.denied),
            }
            for tenant, state in self._tenants.items()
        }


class ProviderKeyLimiter:
    """Concurrency cap per upstream CREDENTIAL, shared across every tenant
    that routes through it.

    This is a different axis from the tenant cap and it cannot be folded into
    it. Two tenants each inside their own `max_concurrency` can jointly exceed
    what one provider key is allowed to have in flight -- that is
    FAILURE-MODES.md row 7 -- and no per-tenant number can express "the sum
    across tenants". So there is a second gate, at the attempt loop rather
    than at ingress, keyed by `ProviderConn.key()`.

    Keyed by the credential, not the provider id. `openrouter` and
    `openrouter-toolsafe` are two catalog entries that present one API key to
    one upstream account, and the upstream counts them as one. Keying on the
    conn id would let the two entries jointly run at twice the real cap and
    take the 429s for it (see the `credential_id` docstring in catalog.py).

    `ProviderKeyExhausted.try_next=True` and this is the case it exists for:
    the next target in the plan uses a different key by construction, so the
    request is worth trying there. It is nobody's fault -- `blame=POLICY`,
    `health=NEUTRAL` -- and it must not open a breaker against a provider
    that was never contacted.
    """

    __slots__ = ("_in_use", "_denied")

    def __init__(self) -> None:
        self._in_use: dict[str, int] = {}
        self._denied = 0

    def acquire(self, credential_key: str, cap: int) -> Permit:
        """Take one permit against `credential_key`, or raise
        `ProviderKeyExhausted`. `cap` is `ProviderConn.max_concurrency`,
        passed per call because the catalog owns it and a limiter that caches
        catalog values is a limiter that disagrees with the catalog after a
        policy reload."""
        if type(cap) is not int or cap < 1:
            raise ValueError("provider key cap must be an integer >= 1")
        held = self._in_use.get(credential_key, 0)
        if held >= cap:
            self._denied += 1
            raise ProviderKeyExhausted(
                f"credential {credential_key!r} at concurrency cap ({held}/{cap} in flight)",
                credential_id=credential_key,
            )
        self._in_use[credential_key] = held + 1
        return Permit(
            scope=SCOPE_PROVIDER_KEY,
            key=credential_key,
            release=lambda: self._release(credential_key),
        )

    def _release(self, credential_key: str) -> None:
        held = self._in_use.get(credential_key, 0)
        if held <= 0:  # pragma: no cover - Permit guards this; belt and braces
            raise ValueError(f"credential {credential_key!r} has no permits to release")
        if held == 1:
            # Drop the entry rather than leave a zero. `snapshot()` is then
            # empty when idle, and "is it empty" is the cheapest possible
            # return-to-zero check for the chaos tier.
            del self._in_use[credential_key]
        else:
            self._in_use[credential_key] = held - 1

    def in_use(self, credential_key: str) -> int:
        return self._in_use.get(credential_key, 0)

    def total_in_use(self) -> int:
        """The `llmgw_permits_in_use{scope="provider_key"}` gauge."""
        return sum(self._in_use.values())

    def denials(self) -> dict[str, int]:
        return {REASON_PROVIDER_KEY_CONCURRENCY: self._denied}

    def snapshot(self) -> dict[str, int]:
        """Permits held per credential. Empty when nothing is in flight."""
        return dict(self._in_use)

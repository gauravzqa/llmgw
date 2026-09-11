"""Admission tests.

Every test runs on a `ManualClock`. Nothing sleeps. A rate limiter proven by
waiting for it to refill is proven once, on the author's laptop, and then
marked `skip`.

The named tests -- `test_c6_a_concurrency_denial_costs_no_rate_credit`,
`test_tenant_isolation_under_saturation`, `test_provider_key_concurrency_cap`
-- are the ones CONTRACTS.md C6 and FAILURE-MODES.md rows 6, 7 and 19 point
at. Rename them and update the register.
"""

from __future__ import annotations

import asyncio
import random

import pytest

from llmgw import errors as E
from llmgw import metrics
from llmgw.admission import (
    REASON_PROVIDER_KEY_CONCURRENCY,
    REASON_TENANT_CONCURRENCY,
    REASON_TENANT_RATE,
    SCOPE_PROVIDER_KEY,
    SCOPE_TENANT,
    AdmissionController,
    Permit,
    ProviderKeyLimiter,
    TenantLimits,
)
from llmgw.clocks import ManualClock


class CountingClock(ManualClock):
    """A ManualClock that counts how often `now()` is read.

    Used to prove structural isolation: if tenant B's admission path reads
    the clock the same number of times whether or not tenant A is saturated,
    A's state was not consulted -- there is nothing else on the path that
    would read time."""

    __slots__ = ("reads",)

    def __init__(self, start: float = 0.0) -> None:
        super().__init__(start=start)
        self.reads = 0

    def now(self) -> float:
        self.reads += 1
        return super().now()


def limits(rate: float = 4.0, burst: int = 3, concurrency: int = 2) -> TenantLimits:
    return TenantLimits(rate_per_second=rate, burst=burst, max_concurrency=concurrency)


def make(
    *, default: TenantLimits | None = None, **tenants: TenantLimits
) -> tuple[AdmissionController, ManualClock]:
    # start=0.0 so token arithmetic is exact at the boundaries the tests
    # assert against; a 1000.0 origin puts float noise in the subtraction.
    clock = ManualClock(start=0.0)
    ctl = AdmissionController(clock=clock, default=default)
    for name, lim in tenants.items():
        ctl.configure(name, lim)
    return ctl, clock


def tokens(ctl: AdmissionController, tenant: str) -> float:
    return float(ctl.snapshot()[tenant]["tokens"])  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# Limits
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad",
    [
        dict(rate_per_second=0.0, burst=1, max_concurrency=1),
        dict(rate_per_second=-1.0, burst=1, max_concurrency=1),
        dict(rate_per_second=float("inf"), burst=1, max_concurrency=1),
        dict(rate_per_second=float("nan"), burst=1, max_concurrency=1),
        dict(rate_per_second=1.0, burst=0, max_concurrency=1),
        dict(rate_per_second=1.0, burst=1, max_concurrency=0),
        dict(rate_per_second=1.0, burst=1.5, max_concurrency=1),
    ],
)
def test_limits_reject_values_that_would_make_a_denial_unactionable(bad):
    with pytest.raises(ValueError):
        TenantLimits(**bad).validate()  # type: ignore[arg-type]


def test_sensible_limits_validate():
    assert limits().validate() == limits()


def test_configure_validates_so_a_bad_limit_fails_at_config_time_not_first_request():
    ctl, _ = make()
    with pytest.raises(ValueError):
        ctl.configure("a", TenantLimits(rate_per_second=0.0, burst=1, max_concurrency=1))
    assert "a" not in ctl.snapshot()


# --------------------------------------------------------------------------
# The token bucket
# --------------------------------------------------------------------------


def test_burst_then_refill():
    ctl, clock = make(a=limits(rate=4.0, burst=3, concurrency=100))
    held = [ctl.admit("a") for _ in range(3)]
    assert tokens(ctl, "a") == 0.0

    with pytest.raises(E.AdmissionRejected) as info:
        ctl.admit("a")
    # A real number: exactly the time until one token, given no competition.
    assert info.value.retry_after == pytest.approx(1 / 4.0)
    assert info.value.status == 429
    assert info.value.outcome is E.Outcome.REJECTED
    assert info.value.blame is E.Blame.POLICY
    assert info.value.health is E.Health.NEUTRAL

    clock._now += info.value.retry_after
    held.append(ctl.admit("a"))
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("a")
    for p in held:
        p.release()


def test_retry_after_is_the_time_until_one_token_not_a_guess():
    ctl, clock = make(a=limits(rate=2.0, burst=1, concurrency=100))
    ctl.admit("a").release()
    # Half a token has accrued after 0.25s at 2/s; a whole one needs 0.25s more.
    clock._now += 0.25
    with pytest.raises(E.AdmissionRejected) as info:
        ctl.admit("a")
    assert info.value.retry_after == pytest.approx(0.25)


def test_refill_is_bounded_at_burst_after_a_long_idle():
    ctl, clock = make(a=limits(rate=4.0, burst=3, concurrency=100))
    ctl.admit("a").release()
    assert tokens(ctl, "a") == 2.0
    clock._now += 1_000_000.0
    assert tokens(ctl, "a") == 3.0
    for _ in range(3):
        ctl.admit("a").release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("a")


def test_refill_is_lazy_so_an_idle_tenant_costs_nothing():
    """No timer, no task: advancing the clock by itself changes nothing until
    somebody asks. Snapshot projects; only admit() writes."""
    ctl, clock = make(a=limits(rate=4.0, burst=3, concurrency=100))
    for _ in range(3):
        ctl.admit("a").release()
    clock._now += 10.0
    assert clock.pending_sleepers == 0
    # A projected read does not write: repeated reads agree and admit() then
    # sees the same value.
    assert tokens(ctl, "a") == 3.0 == tokens(ctl, "a")
    ctl.admit("a").release()
    assert tokens(ctl, "a") == 2.0


def test_a_clock_that_moves_backwards_cannot_drain_the_bucket():
    ctl, clock = make(a=limits(rate=4.0, burst=3, concurrency=100))
    ctl.admit("a").release()
    clock._now -= 100.0
    assert tokens(ctl, "a") == 2.0
    ctl.admit("a").release()
    assert tokens(ctl, "a") == 1.0


def test_release_does_not_refund_a_token():
    """Rate credit is spent on admission, not on completion. Refunding on
    release rewards a client that fires and cancels."""
    ctl, _ = make(a=limits(rate=4.0, burst=3, concurrency=100))
    ctl.admit("a").release()
    assert tokens(ctl, "a") == 2.0


# --------------------------------------------------------------------------
# C6 and the check order
# --------------------------------------------------------------------------


def test_c6_a_concurrency_denial_costs_no_rate_credit():
    ctl, _ = make(a=limits(rate=1.0, burst=10, concurrency=2))
    held = [ctl.admit("a"), ctl.admit("a")]
    before = tokens(ctl, "a")
    assert before == 8.0

    for _ in range(50):
        with pytest.raises(E.ConcurrencyRejected) as info:
            ctl.admit("a")
        assert info.value.retry_after is None  # waiting does not help
        assert info.value.outcome is E.Outcome.REJECTED
        assert info.value.blame is E.Blame.POLICY
        assert info.value.health is E.Health.NEUTRAL

    assert tokens(ctl, "a") == before
    assert ctl.in_use("a") == 2
    assert ctl.denials()[REASON_TENANT_CONCURRENCY] == 50
    assert ctl.denials()[REASON_TENANT_RATE] == 0
    for p in held:
        p.release()


def test_concurrency_is_checked_before_the_bucket():
    # Empty bucket, free concurrency -> the bucket is what refuses.
    ctl, _ = make(a=limits(rate=1.0, burst=1, concurrency=5))
    ctl.admit("a").release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("a")

    # Full concurrency, full bucket -> the permit is what refuses, and the
    # bucket is untouched.
    ctl, _ = make(b=limits(rate=1.0, burst=5, concurrency=1))
    p = ctl.admit("b")
    assert tokens(ctl, "b") == 4.0
    with pytest.raises(E.ConcurrencyRejected):
        ctl.admit("b")
    assert tokens(ctl, "b") == 4.0
    p.release()


def test_a_concurrency_denial_does_not_read_the_clock():
    """The cheap check is the one that runs first, and cheap means: no clock,
    no float arithmetic. Proven by counting clock reads."""
    clock = CountingClock()
    ctl = AdmissionController(clock=clock)
    ctl.configure("a", limits(concurrency=1))
    p = ctl.admit("a")
    reads = clock.reads
    with pytest.raises(E.ConcurrencyRejected):
        ctl.admit("a")
    assert clock.reads == reads
    p.release()


def test_a_rate_denial_does_not_take_a_permit():
    ctl, _ = make(a=limits(rate=1.0, burst=1, concurrency=5))
    ctl.admit("a").release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("a")
    assert ctl.in_use("a") == 0


# --------------------------------------------------------------------------
# The permit
# --------------------------------------------------------------------------


def test_permit_releases_on_normal_exit():
    ctl, _ = make(a=limits())
    with ctl.admit("a") as p:
        assert isinstance(p, Permit)
        assert p.tenant == "a"
        assert p.scope == SCOPE_TENANT
        assert ctl.in_use("a") == 1
    assert ctl.in_use("a") == 0
    assert p.released


def test_permit_releases_on_exception_and_does_not_suppress_it():
    ctl, _ = make(a=limits())
    with pytest.raises(E.UpstreamServerError):
        with ctl.admit("a"):
            raise E.UpstreamServerError("boom")
    assert ctl.in_use("a") == 0


async def test_permit_releases_on_cancellation():
    ctl, clock = make(a=limits())
    entered = asyncio.Event()

    async def request() -> None:
        with ctl.admit("a"):
            entered.set()
            await clock.sleep(60.0)

    task = asyncio.create_task(request())
    await entered.wait()
    assert ctl.in_use("a") == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ctl.in_use("a") == 0


async def test_permit_works_as_an_async_context_manager_too():
    ctl, _ = make(a=limits())
    async with ctl.admit("a"):
        assert ctl.in_use("a") == 1
    assert ctl.in_use("a") == 0


def test_double_release_is_a_value_error_and_in_use_never_goes_negative():
    ctl, _ = make(a=limits())
    p = ctl.admit("a")
    p.release()
    assert ctl.in_use("a") == 0
    with pytest.raises(ValueError):
        p.release()
    assert ctl.in_use("a") == 0
    # And the same through the context manager: an explicit release inside
    # the block is the caller bug the ValueError exists to surface.
    with pytest.raises(ValueError):
        with ctl.admit("a") as q:
            q.release()
    assert ctl.in_use("a") == 0


def test_a_permit_released_after_reconfigure_still_counts_down_the_right_tenant():
    ctl, _ = make(a=limits(concurrency=1))
    p = ctl.admit("a")
    ctl.configure("a", limits(concurrency=3, burst=1))
    assert ctl.in_use("a") == 1
    assert tokens(ctl, "a") <= 1.0  # clamped to the new burst, not refilled
    p.release()
    assert ctl.in_use("a") == 0


# --------------------------------------------------------------------------
# Tenants
# --------------------------------------------------------------------------


def test_tenant_isolation_under_saturation():
    clock = CountingClock()
    ctl = AdmissionController(clock=clock)
    ctl.configure("a", limits(rate=1.0, burst=1000, concurrency=10))
    ctl.configure("b", limits(rate=1.0, burst=1000, concurrency=20))

    # Baseline: what tenant B's admission costs when nobody else is around.
    reads_before = clock.reads
    probe = ctl.admit("b")
    baseline_reads = clock.reads - reads_before
    probe.release()

    # Saturate A: cap + 50 opens, 50 refused.
    a_held = [ctl.admit("a") for _ in range(10)]
    a_tokens = tokens(ctl, "a")
    rejected = 0
    for _ in range(50):
        with pytest.raises(E.ConcurrencyRejected):
            ctl.admit("a")
        rejected += 1
    assert rejected == 50
    assert tokens(ctl, "a") == a_tokens, "A's rejections consumed A's tokens"

    # B: all 20 admitted, each at exactly the baseline cost. A's state was
    # not on the path.
    reads_before = clock.reads
    b_held = [ctl.admit("b") for _ in range(20)]
    assert len(b_held) == 20
    assert clock.reads - reads_before == 20 * baseline_reads
    assert ctl.in_use("b") == 20
    assert ctl.in_use("a") == 10

    for p in a_held + b_held:
        p.release()
    assert ctl.total_in_use() == 0


def test_unknown_tenant_without_a_default_is_refused_by_name():
    ctl, _ = make(a=limits())
    with pytest.raises(E.AdmissionRejected, match="ghost"):
        ctl.admit("ghost")
    assert "ghost" not in ctl.snapshot()  # a refusal creates no state
    assert ctl.denials()[REASON_TENANT_RATE] == 1


def test_unknown_tenant_with_a_default_is_admitted_under_it():
    ctl, _ = make(default=limits(rate=1.0, burst=2, concurrency=1))
    p = ctl.admit("ghost")
    with pytest.raises(E.ConcurrencyRejected):
        ctl.admit("ghost")
    p.release()
    ctl.admit("ghost").release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("ghost")
    assert ctl.snapshot()["ghost"]["limits"] == limits(rate=1.0, burst=2, concurrency=1)


def test_two_default_tenants_do_not_share_a_bucket():
    ctl, _ = make(default=limits(rate=1.0, burst=1, concurrency=1))
    ctl.admit("x").release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("x")
    ctl.admit("y").release()


def test_a_configured_tenant_is_not_touched_by_the_default():
    ctl, _ = make(default=limits(burst=100), a=limits(burst=1))
    ctl.admit("a").release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("a")


def test_forget_idle_only_drops_state_that_a_fresh_one_would_equal():
    ctl, clock = make(a=limits(rate=1.0, burst=2, concurrency=1))
    p = ctl.admit("a")
    assert ctl.forget_idle("a") is False  # holding a permit
    p.release()
    assert ctl.forget_idle("a") is False  # bucket not full: forgetting would mint
    clock._now += 1.0
    assert ctl.forget_idle("a") is True
    assert "a" not in ctl.snapshot()
    assert ctl.forget_idle("a") is False
    # Reconfigured, the tenant starts full -- the same as the state dropped.
    ctl.configure("a", limits(rate=1.0, burst=2, concurrency=1))
    assert tokens(ctl, "a") == 2.0


# --------------------------------------------------------------------------
# Provider key cap
# --------------------------------------------------------------------------


def test_provider_key_concurrency_cap():
    """Two tenants, each inside its own concurrency limit, sharing one
    credential. Row 7: the sum can exceed what the key may hold."""
    ctl, _ = make(a=limits(concurrency=3), b=limits(concurrency=3))
    keys = ProviderKeyLimiter()
    cap = 4
    key = "openrouter"  # `openrouter-toolsafe` resolves to the same key()

    held: list[tuple[Permit, Permit]] = []
    for tenant in ("a", "b", "a", "b"):
        tp = ctl.admit(tenant)
        kp = keys.acquire(key, cap)
        held.append((tp, kp))
        assert keys.in_use(key) <= cap

    # Both tenants still have tenant headroom (2 of 3 each). The key does not.
    tp = ctl.admit("a")
    with pytest.raises(E.ProviderKeyExhausted) as info:
        keys.acquire(key, cap)
    assert info.value.try_next is True
    assert info.value.retry_same is False
    assert info.value.health is E.Health.NEUTRAL
    assert info.value.blame is E.Blame.POLICY
    assert info.value.credential_id == key
    assert keys.in_use(key) == cap
    tp.release()

    # A different credential is a different cap: the next target is worth trying.
    other = keys.acquire("anthropic", 1)
    other.release()

    for tp, kp in held:
        kp.release()
        tp.release()
    assert keys.in_use(key) == 0
    assert keys.snapshot() == {}
    assert keys.denials() == {REASON_PROVIDER_KEY_CONCURRENCY: 1}


def test_provider_key_permit_releases_on_exception():
    keys = ProviderKeyLimiter()
    with pytest.raises(RuntimeError):
        with keys.acquire("k", 1) as p:
            assert p.scope == SCOPE_PROVIDER_KEY
            assert p.key == "k"
            raise RuntimeError
    assert keys.total_in_use() == 0
    keys.acquire("k", 1).release()


def test_provider_key_double_release_is_a_value_error():
    keys = ProviderKeyLimiter()
    p = keys.acquire("k", 2)
    p.release()
    with pytest.raises(ValueError):
        p.release()
    assert keys.in_use("k") == 0


def test_provider_key_cap_is_per_call_so_a_catalog_reload_takes_effect():
    keys = ProviderKeyLimiter()
    a = keys.acquire("k", 2)
    b = keys.acquire("k", 2)
    with pytest.raises(E.ProviderKeyExhausted):
        keys.acquire("k", 2)
    c = keys.acquire("k", 3)  # the catalog raised the cap
    with pytest.raises(E.ProviderKeyExhausted):
        keys.acquire("k", 1)  # or lowered it below what is already held
    for p in (a, b, c):
        p.release()


def test_provider_key_cap_must_be_a_positive_integer():
    keys = ProviderKeyLimiter()
    for bad in (0, -1, 1.5):
        with pytest.raises(ValueError):
            keys.acquire("k", bad)  # type: ignore[arg-type]


# --------------------------------------------------------------------------
# Metrics agreement and return-to-zero
# --------------------------------------------------------------------------


def test_denial_reasons_and_scopes_are_labels_metrics_will_accept():
    ctl, _ = make()
    keys = ProviderKeyLimiter()
    reasons = set(ctl.denials()) | set(keys.denials())
    assert reasons == {
        REASON_TENANT_RATE,
        REASON_TENANT_CONCURRENCY,
        REASON_PROVIDER_KEY_CONCURRENCY,
    }
    assert reasons <= set(metrics.DENIAL_REASONS)

    spec = next(s for s in metrics.METRICS if s.name == "llmgw_permits_in_use")
    assert spec.labels == ("scope",)
    assert set(spec.label_values[0]) == {SCOPE_TENANT, SCOPE_PROVIDER_KEY}


def test_snapshot_returns_to_zero_after_every_permit_is_released():
    ctl, _ = make(
        default=limits(concurrency=5), a=limits(concurrency=5), b=limits(concurrency=5)
    )
    keys = ProviderKeyLimiter()
    held: list[Permit] = []
    for tenant in ("a", "b", "c", "a", "b", "c"):
        held.append(ctl.admit(tenant))
        held.append(keys.acquire("k1" if tenant == "a" else "k2", 8))
    assert ctl.total_in_use() == 6
    assert keys.total_in_use() == 6
    assert set(keys.snapshot()) == {"k1", "k2"}

    for p in held:
        p.release()

    assert ctl.total_in_use() == 0
    assert all(entry["in_use"] == 0 for entry in ctl.snapshot().values())
    assert keys.total_in_use() == 0
    assert keys.snapshot() == {}


def test_snapshot_carries_per_tenant_denials_for_the_hot_tenant_dashboard():
    ctl, _ = make(a=limits(rate=1.0, burst=1, concurrency=1), b=limits())
    p = ctl.admit("a")
    with pytest.raises(E.ConcurrencyRejected):
        ctl.admit("a")
    p.release()
    with pytest.raises(E.AdmissionRejected):
        ctl.admit("a")
    snap = ctl.snapshot()
    assert snap["a"]["denied"] == {REASON_TENANT_CONCURRENCY: 1, REASON_TENANT_RATE: 1}
    assert snap["b"]["denied"] == {REASON_TENANT_CONCURRENCY: 0, REASON_TENANT_RATE: 0}


@pytest.mark.parametrize("seed", range(8))
def test_random_admit_release_sequences_keep_every_invariant(seed: int):
    """Property-style. Whatever the interleaving, `in_use` never exceeds the
    cap or drops below zero, tokens stay in [0, burst], a denial never
    changes anything, and once every permit is handed back the count is
    zero everywhere."""
    rng = random.Random(seed)
    tenants = {
        f"t{i}": limits(
            rate=rng.choice([0.5, 1.0, 4.0, 25.0]),
            burst=rng.randint(1, 8),
            concurrency=rng.randint(1, 5),
        )
        for i in range(4)
    }
    ctl, clock = make(**tenants)
    keys = ProviderKeyLimiter()
    cap = 6
    held: list[tuple[Permit, Permit | None]] = []

    def check() -> None:
        snap = ctl.snapshot()
        for name, lim in tenants.items():
            in_use = int(snap[name]["in_use"])  # type: ignore[arg-type]
            toks = float(snap[name]["tokens"])  # type: ignore[arg-type]
            assert 0 <= in_use <= lim.max_concurrency
            assert -TOLERANCE <= toks <= lim.burst + TOLERANCE
        assert 0 <= keys.total_in_use() <= cap

    TOLERANCE = 1e-9
    for _ in range(400):
        op = rng.random()
        if op < 0.45:
            name = rng.choice(list(tenants))
            before = ctl.snapshot()[name]
            try:
                tp = ctl.admit(name)
            except (E.AdmissionRejected, E.ConcurrencyRejected):
                after = ctl.snapshot()[name]
                assert after["in_use"] == before["in_use"]
                assert after["tokens"] == before["tokens"]
            else:
                kp: Permit | None
                try:
                    kp = keys.acquire("shared", cap)
                except E.ProviderKeyExhausted:
                    kp = None
                held.append((tp, kp))
        elif op < 0.8 and held:
            tp, kp = held.pop(rng.randrange(len(held)))
            if kp is not None:
                kp.release()
            tp.release()
        else:
            clock._now += rng.choice([0.0, 0.05, 0.3, 1.0, 7.0])
        check()

    for tp, kp in held:
        if kp is not None:
            kp.release()
        tp.release()
    assert ctl.total_in_use() == 0
    assert all(e["in_use"] == 0 for e in ctl.snapshot().values())
    assert keys.snapshot() == {}

"""Policy tests. Mostly about what a snapshot makes impossible.

Two themes run through the file:

  * a snapshot handed to a request cannot change under it (FAILURE-MODES
    row 10), and
  * everything that could go wrong went wrong at construction, so `plan_for`
    is a lookup that cannot fail on configuration.

Pure unit: no sockets, no clocks that tick, no config files except the one
worked example this repo ships -- see the test that loads it for why that one
earns an exception.
"""

from __future__ import annotations

import dataclasses
import pathlib

import pytest

from llmgw import errors as E
from llmgw.catalog import DEFAULT_CATALOG, Catalog, ModelSpec, ProviderConn
from llmgw.clocks import Budgets
from llmgw.policy import (
    DEFAULT_BUDGETS,
    ExecutionPlan,
    PolicySnapshot,
    PolicyStore,
    Workload,
)


class StubClock:
    """A clock that only tells the time, and moves when a test says so.

    `ManualClock` would do, but it also schedules sleepers and drains an event
    loop, and these are synchronous tests that need exactly one method. A
    seven-line stub is easier to trust than a fixture that runs a scheduler.
    """

    __slots__ = ("_now",)

    def __init__(self, start: float = 1_000.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


# Same wire dialect, different providers -- which is what a real fallback
# pair looks like. An anthropic/deepseek pair reads fine and cannot work:
# one request cannot be sent to two schemas. See _reject_cross_dialect.
INCUMBENT = "deepseek.deepseek-v4-pro"
CANDIDATE = "openrouter.deepseek-v4-flash"

BASIC_TOML = """
default_workload = "chat"

[defaults.budgets]
total = 300.0
first_event = 20.0

[workloads.chat]
incumbent = "deepseek.deepseek-v4-pro"
candidate = "openrouter.deepseek-v4-flash"

[workloads.cheap]
incumbent = "openrouter.mistral-small-2603"
"""


def snapshot(text: str = BASIC_TOML, *, catalog: Catalog | None = None,
             clock: StubClock | None = None) -> PolicySnapshot:
    return PolicySnapshot.from_toml(
        text, catalog=catalog or DEFAULT_CATALOG, clock=clock or StubClock()
    )


# ==========================================================================
# Row 10: immutability and pinning.
# ==========================================================================


def test_row10_a_reload_mid_request_cannot_change_the_snapshot_in_flight():
    """FAILURE-MODES row 10 regression: config reload mid-request.

    The bug this prevents is a request routed by policy v1 and billed by
    policy v2, with one `policy_id` field in the record and two policies
    having taken part -- so the record cannot say which. The mitigation is
    that a request takes ONE snapshot at ingress and the store can only ever
    swap the whole object, never edit it.
    """
    store = PolicyStore(snapshot())
    pinned = store.current()               # ingress: the request takes its copy
    plan_before = pinned.plan_for("chat")

    reloaded = snapshot(BASIC_TOML.replace('total = 300.0', 'total = 42.0'))
    previous = store.replace(reloaded)

    # The in-flight request is untouched: same object, same id, same plan.
    assert previous is pinned
    assert store.current() is reloaded
    assert store.current().id != pinned.id
    assert pinned.plan_for("chat") == plan_before
    assert plan_before.budgets.total == 300.0
    # ...and the new snapshot really is different, so the test is not vacuous.
    assert reloaded.plan_for("chat").budgets.total == 42.0


def test_replace_returns_the_previous_snapshot_so_a_reload_is_loggable():
    """`pol_a -> pol_b` in one line, and a no-op reload is visible as the two
    ids being equal rather than as nothing at all."""
    first = snapshot()
    store = PolicyStore(first)
    same_content = snapshot()
    previous = store.replace(same_content)
    assert previous is first
    assert previous.id == same_content.id  # a reload that changed nothing


RETRY_TOML = BASIC_TOML.replace(
    '[workloads.cheap]',
    '[workloads.chat.retry]\nmax_attempts = 3\nbase_delay = 0.5\nmax_delay = 2.0\n\n'
    '[workloads.cheap]',
)
BAD_RETRY_TOML = RETRY_TOML.replace("max_delay = 2.0", "max_delay = 0.1")


def rejects_inverted_delays(snap: PolicySnapshot) -> None:
    """What the server's `_derive` does, without the server: build the
    `RetryPolicy` each workload's table describes, which is where a
    `max_delay` below `base_delay` is caught -- and which `PolicySnapshot`
    deliberately cannot do, because policy.py does not import retry.py."""
    from llmgw.retry import RetryPolicy

    for wid in snap.workloads:
        table = snap.plan_for(wid).retry
        if table is not None:
            RetryPolicy(**{str(k): v for k, v in table.items()}).validate()


def test_replace_refuses_a_snapshot_the_validator_rejects_and_keeps_serving_the_old():
    """N6. A snapshot that was constructed is one that ROUTES; it is not yet
    one a request can be served from, because the retry table it carries is
    only validated when somebody builds a `RetryPolicy` out of it. Without a
    check at the swap, a reload of `max_delay < base_delay` succeeds and every
    request after it is a 500. With it, the reload is the thing that fails,
    the old snapshot keeps serving, and the failure is a `PolicyError` with
    the real reason chained."""
    good = snapshot(RETRY_TOML)
    store = PolicyStore(good, validate=rejects_inverted_delays)
    assert store.current() is good

    bad = snapshot(BAD_RETRY_TOML)  # constructs fine: policy.py cannot see it
    assert bad.id != good.id
    with pytest.raises(E.PolicyError, match="max_delay=0.1 is below base_delay=0.5"):
        store.replace(bad)

    assert store.current() is good, "a refused reload must leave the old snapshot"
    assert store.current().plan_for("chat").retry["max_delay"] == 2.0

    # And a good reload still works, returning the previous one as before.
    newer = snapshot(RETRY_TOML.replace("max_attempts = 3", "max_attempts = 4"))
    previous = store.replace(newer)
    assert previous is good
    assert store.current() is newer


def test_the_store_runs_its_validator_on_the_snapshot_it_is_born_with():
    """The constructor validates too, so a store cannot start from a snapshot
    its own `replace()` would refuse. And a reload has to be a countable event
    with a `code`: a validator that raises a bare `ValueError` is wrapped as a
    `PolicyError` with the cause chained, while one that already raises
    `PolicyError` is passed through untouched, so the message it composed is
    the message the operator reads."""

    def bare(_snap: PolicySnapshot) -> None:
        raise ValueError("not for serving")

    with pytest.raises(E.PolicyError, match="not for serving") as caught:
        PolicyStore(snapshot(), validate=bare)
    assert isinstance(caught.value.cause, ValueError)

    def already_classified(_snap: PolicySnapshot) -> None:
        raise E.PolicyError("composed upstream", workload="chat")

    with pytest.raises(E.PolicyError) as caught:
        PolicyStore(snapshot(), validate=already_classified)
    assert caught.value.message == "composed upstream"
    assert caught.value.workload == "chat"


def test_a_store_without_a_validator_swaps_unconditionally():
    """The default is the old behaviour: no validator, no refusal. Whoever
    owns the reload path decides what a snapshot must satisfy to serve."""
    store = PolicyStore(snapshot(RETRY_TOML))
    bad = snapshot(BAD_RETRY_TOML)
    store.replace(bad)
    assert store.current() is bad


def test_the_servers_derivation_is_the_validator_the_gateway_installs():
    """The wiring, not just the mechanism: `app._derive` is what refuses the
    reload, so a `[retry]` table the server could not build a policy from is
    caught at `replace()` with the server's own error message."""
    from llmgw.server.app import _derive

    good = snapshot(RETRY_TOML)
    store = PolicyStore(good, validate=_derive)
    with pytest.raises(E.PolicyError, match=r"workload 'chat': \[retry\] is not a valid"):
        store.replace(snapshot(BAD_RETRY_TOML))
    assert store.current() is good


def test_a_snapshot_cannot_be_mutated():
    snap = snapshot()
    with pytest.raises(dataclasses.FrozenInstanceError):
        snap.default_workload = "cheap"     # type: ignore[misc]
    replacement = Workload(id="chat", incumbent=INCUMBENT)
    with pytest.raises(TypeError):
        snap.workloads["chat"] = replacement   # type: ignore[index]


def test_a_workload_and_its_plan_cannot_be_mutated():
    snap = snapshot()
    workload = snap.workloads["chat"]
    plan = snap.plan_for("chat")
    with pytest.raises(dataclasses.FrozenInstanceError):
        workload.incumbent = "fake.echo"    # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.targets = ()                   # type: ignore[misc]
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan.budgets.total = 1.0            # type: ignore[misc]


def test_parsed_retry_config_is_frozen_not_a_live_dict():
    """A frozen dataclass wrapping a mutable dict advertises immutability and
    does not have it. `plan.retry["max_attempts"] = 9` would be row 10 again,
    reaching through the snapshot every pinned request is holding."""
    snap = snapshot("""
default_workload = "chat"
[defaults.retry]
max_attempts = 2
[workloads.chat]
incumbent = "deepseek.deepseek-v4-pro"
""")
    retry = snap.plan_for("chat").retry
    assert retry == {"max_attempts": 2}
    with pytest.raises(TypeError):
        retry["max_attempts"] = 9           # type: ignore[index]


# ==========================================================================
# The id is a content hash.
# ==========================================================================


def test_the_same_config_text_yields_the_same_id():
    """Across processes and restarts, which is the only property that makes a
    policy id usable as a join key over a fleet's capture records."""
    assert snapshot().id == snapshot().id
    assert snapshot().id.startswith("pol_")


def test_a_changed_byte_yields_a_different_id():
    changed = BASIC_TOML.replace("total = 300.0", "total = 300.5")
    assert snapshot(changed).id != snapshot().id


def test_key_order_in_the_toml_does_not_change_the_id():
    """This is what "canonicalised" has to mean. Two humans editing the same
    file must not produce two ids for one policy, or the id stops being a fact
    about the policy and becomes a fact about the editor."""
    reordered = """
default_workload = "chat"

[workloads.cheap]
incumbent = "openrouter.mistral-small-2603"

[workloads.chat]
candidate = "openrouter.deepseek-v4-flash"
incumbent = "deepseek.deepseek-v4-pro"

[defaults.budgets]
first_event = 20.0
total = 300.0
"""
    assert snapshot(reordered).id == snapshot().id


def test_comments_and_whitespace_do_not_change_the_id():
    """Deliberate, not accidental: an id that moves when someone fixes a typo
    in a comment is an id that churns dashboards for no reason. Documented in
    the module docstring so nobody later reads it as a bug."""
    commented = "# a comment nobody's routing depends on\n\n" + BASIC_TOML
    assert snapshot(commented).id == snapshot().id


def test_spelling_out_an_inherited_budget_does_not_change_the_id():
    """The hash is over resolved meaning, not over the file. A config refactor
    that changes nothing about routing must not renumber every record."""
    explicit = BASIC_TOML + """
[workloads.cheap.budgets]
total = 300.0
first_event = 20.0
"""
    assert snapshot(explicit).id == snapshot().id


def test_the_short_id_is_a_prefix_of_the_full_digest():
    """The short form is for a log line. The full digest exists so that a
    collision in the short form is diagnosable rather than deniable."""
    snap = snapshot()
    assert snap.content_digest.startswith(snap.id.removeprefix("pol_"))
    assert len(snap.content_digest) == 64


def test_the_catalog_has_its_own_id_because_prices_are_not_policy():
    """Row 10 is about billing as well as routing, and prices live in the
    catalog. Folding them into the policy id would churn it whenever an
    unrelated model was re-priced; leaving them out entirely would leave the
    record unable to say which price table applied. Two narrow ids."""
    snap = snapshot()
    assert snap.catalog_id.startswith("cat_")
    repriced = DEFAULT_CATALOG.with_overrides(models={
        INCUMBENT: dataclasses.replace(
            DEFAULT_CATALOG.models[INCUMBENT], input_per_m=99.0
        )
    })
    other = snapshot(catalog=repriced)
    assert other.id == snap.id                 # policy document unchanged
    assert other.catalog_id != snap.catalog_id  # price table changed


# ==========================================================================
# plan_for: ordering, overrides, and lookups that cannot fail.
# ==========================================================================


def test_a_candidate_is_tried_first_and_the_incumbent_second():
    plan = snapshot().plan_for("chat")
    assert len(plan) == 2
    assert [t.model.id for t in plan.targets] == [CANDIDATE, INCUMBENT]
    assert plan.primary.model.id == CANDIDATE


def test_an_incumbent_only_workload_gives_exactly_one_target():
    plan = snapshot().plan_for("cheap")
    assert len(plan) == 1
    assert plan.targets[0].model.id == "openrouter.mistral-small-2603"


def test_a_plan_never_holds_a_second_candidate():
    """Two entries maximum, and the schema has no way to ask for a third."""
    for wid in ("chat", "cheap"):
        assert len(snapshot().plan_for(wid)) <= 2


def test_no_workload_named_routes_to_the_default_workload():
    snap = snapshot()
    assert snap.plan_for() == snap.plan_for("chat")
    assert snap.plan_for().workload_id == "chat"


def test_an_explicit_model_overrides_the_whole_plan():
    """Not prepended to it. Prepending would silently fall back to an
    incumbent the caller never asked for and may not be authorised for."""
    plan = snapshot().plan_for("chat", model="fake.echo")
    assert len(plan) == 1
    assert plan.targets[0].model.id == "fake.echo"
    assert plan.workload_id == "chat"          # budgets still come from here
    assert plan.budgets.total == 300.0


def test_an_explicit_model_is_still_validated_against_the_catalog():
    with pytest.raises(E.PolicyError, match="unknown model"):
        snapshot().plan_for("chat", model="not.a.model")


def test_a_plan_carries_the_policy_id_that_produced_it():
    """So the accounting layer never looks the snapshot up a second time --
    a second lookup is a second chance to land on a different policy."""
    snap = snapshot()
    assert snap.plan_for("chat").policy_id == snap.id


def test_an_unknown_workload_is_a_policy_error_naming_the_known_ones():
    with pytest.raises(E.PolicyError) as ei:
        snapshot().plan_for("nope")
    assert "unknown workload" in ei.value.message
    assert "chat" in ei.value.message


def test_policy_errors_are_neutral_and_blame_policy():
    """Our config, not the provider's fault. Counting it against a provider's
    breaker opens a circuit against a provider that was never called."""
    with pytest.raises(E.PolicyError) as ei:
        snapshot().plan_for("nope")
    assert ei.value.health is E.Health.NEUTRAL
    assert ei.value.blame is E.Blame.POLICY
    assert ei.value.outcome is E.Outcome.REJECTED
    assert ei.value.code in E.ERROR_CODES


# ==========================================================================
# Construction-time validation.
# ==========================================================================


def test_a_candidate_identical_to_the_incumbent_is_rejected():
    """A fallback to the same target is a retry wearing a costume: it doubles
    load while looking like redundancy on the dashboard."""
    with pytest.raises(E.PolicyError) as ei:
        Workload(id="w", incumbent=INCUMBENT, candidate=INCUMBENT).validate(
            DEFAULT_CATALOG
        )
    assert "retry" in ei.value.message
    assert "doubles load" in ei.value.message or "not a fallback" in ei.value.message


def test_two_catalog_ids_for_one_upstream_endpoint_are_also_rejected():
    """The version of the mistake that survives review: different catalog
    keys, same provider and same wire model id."""
    alias = dataclasses.replace(DEFAULT_CATALOG.models[CANDIDATE], id="alias.v4-pro")
    catalog = DEFAULT_CATALOG.with_overrides(models={"alias.v4-pro": alias})
    with pytest.raises(E.PolicyError, match="same upstream endpoint"):
        Workload(id="w", incumbent=CANDIDATE, candidate="alias.v4-pro").validate(catalog)


def test_a_budget_that_can_never_fire_is_rejected_at_construction():
    """`first_event > total` is a decorative timeout: `Deadline.slice()` clamps
    it, so it protects nothing while looking like it does."""
    with pytest.raises(E.PolicyError, match="can never fire"):
        Workload(
            id="w", incumbent=INCUMBENT,
            budgets=Budgets(total=5.0, first_event=20.0),
        ).validate(DEFAULT_CATALOG)


def test_a_workload_pointing_at_an_unknown_model_names_the_workload():
    """`unknown model 'deepseek.v4'` without the workload name sends the
    reader to the catalog, which is the wrong file."""
    with pytest.raises(E.PolicyError) as ei:
        Workload(id="summarize", incumbent="deepseek.v4-typo").validate(DEFAULT_CATALOG)
    assert "summarize" in ei.value.message
    assert "incumbent" in ei.value.message
    assert ei.value.model == "deepseek.v4-typo"


def test_a_default_workload_that_does_not_exist_is_rejected():
    with pytest.raises(E.PolicyError, match="default_workload"):
        PolicySnapshot(
            id="pol_x", created_at=0.0,
            workloads={"chat": Workload(id="chat", incumbent=INCUMBENT)},
            catalog=DEFAULT_CATALOG, default_workload="nope",
        )


def test_a_workload_stored_under_a_mismatched_key_is_rejected():
    """The key and the id are two places for one fact, so they are checked
    against each other rather than trusted."""
    with pytest.raises(E.PolicyError, match="does not match"):
        PolicySnapshot(
            id="pol_x", created_at=0.0,
            workloads={"chat": Workload(id="summarize", incumbent=INCUMBENT)},
            catalog=DEFAULT_CATALOG, default_workload="chat",
        )


def test_an_empty_snapshot_is_rejected():
    with pytest.raises(E.PolicyError, match="at least one workload"):
        PolicySnapshot(id="pol_x", created_at=0.0, workloads={},
                       catalog=DEFAULT_CATALOG, default_workload="chat")


def test_plan_for_does_no_validation_because_construction_already_did():
    """The positive statement of the same rule: a constructed snapshot routes.
    Every model in every plan already resolved once, at construction."""
    snap = snapshot()
    for wid in snap.workloads:
        plan = snap.plan_for(wid)
        assert plan.targets
        assert all(t.model.id in snap.catalog.models for t in plan.targets)


# ==========================================================================
# from_toml: malformed input, every time, as a PolicyError.
# ==========================================================================


def test_malformed_toml_is_a_policy_error_not_a_tomllib_error():
    with pytest.raises(E.PolicyError, match="not valid TOML"):
        snapshot("default_workload = [unclosed")


def test_a_missing_incumbent_names_the_workload_and_the_key():
    with pytest.raises(E.PolicyError) as ei:
        snapshot("""
default_workload = "chat"
[workloads.chat]
candidate = "deepseek.deepseek-v4-pro"
""")
    assert "workloads.chat" in ei.value.message
    assert "incumbent" in ei.value.message


def test_an_unknown_model_in_the_toml_names_the_model():
    with pytest.raises(E.PolicyError, match="not in the catalog"):
        snapshot("""
default_workload = "chat"
[workloads.chat]
incumbent = "deepseek.deepseek-v9-ghost"
""")


def test_a_missing_default_workload_is_rejected_rather_than_guessed():
    """"The first workload in the file" would make routing depend on key
    order, which is the one thing a config file must never mean."""
    with pytest.raises(E.PolicyError, match="default_workload"):
        snapshot("""
[workloads.chat]
incumbent = "deepseek.deepseek-v4-pro"
""")


def test_a_default_workload_naming_nothing_is_rejected():
    with pytest.raises(E.PolicyError, match="not a defined workload"):
        snapshot("""
default_workload = "nope"
[workloads.chat]
incumbent = "deepseek.deepseek-v4-pro"
""")


def test_no_workloads_at_all_is_rejected():
    with pytest.raises(E.PolicyError, match="no \\[workloads"):
        snapshot('default_workload = "chat"')


@pytest.mark.parametrize(
    "text, where",
    [
        ('default_workload = "c"\nregion = "eu"\n[workloads.c]\nincumbent = "fake.echo"',
         "top level"),
        ('default_workload = "c"\n[workloads.c]\nincumbent = "fake.echo"\n'
         'candidat = "fake.echo-anthropic"',
         "[workloads.c]"),
        ('default_workload = "c"\n[workloads.c]\nincumbent = "fake.echo"\n'
         '[workloads.c.budgets]\nfirst_evnt = 3.0',
         "budgets"),
        ('default_workload = "c"\n[defaults]\nbudget = 3\n'
         '[workloads.c]\nincumbent = "fake.echo"',
         "[defaults]"),
    ],
)
def test_an_unknown_key_is_an_error_not_a_shrug(text: str, where: str):
    """A parser that ignores what it does not recognise turns `candidat` into
    a silently disabled A/B test reporting 100% incumbent traffic -- which
    looks exactly like a candidate nobody sends traffic to. A typo has to fail
    at reload, where it is one line."""
    with pytest.raises(E.PolicyError) as ei:
        snapshot(text)
    assert "unknown key" in ei.value.message
    assert where in ei.value.message


def test_a_non_numeric_budget_is_rejected_with_the_offending_value():
    with pytest.raises(E.PolicyError, match="number of seconds"):
        snapshot("""
default_workload = "c"
[workloads.c]
incumbent = "fake.echo"
[workloads.c.budgets]
total = "10s"
""")


def test_a_workload_that_is_not_a_table_is_rejected():
    with pytest.raises(E.PolicyError, match="must be a table"):
        snapshot('default_workload = "c"\n[workloads]\nc = "fake.echo"')


# ==========================================================================
# Budget inheritance.
# ==========================================================================


def test_overriding_one_budget_keeps_the_defaults_for_the_rest():
    """The alternative -- a budgets table means "these are ALL my budgets" --
    makes every workload restate five numbers, and five numbers copied five
    times is five numbers that disagree within a quarter."""
    snap = snapshot("""
default_workload = "fast"

[defaults.budgets]
total = 300.0
connect = 2.0
first_event = 20.0
progress = 15.0
client_stall = 30.0

[workloads.fast]
incumbent = "fake.echo"
[workloads.fast.budgets]
first_event = 4.0
""")
    budgets = snap.plan_for("fast").budgets
    assert budgets.first_event == 4.0
    assert budgets.total == 300.0
    assert budgets.connect == 2.0
    assert budgets.progress == 15.0
    assert budgets.client_stall == 30.0


def test_defaults_inherit_from_the_module_defaults_when_unstated():
    snap = snapshot('default_workload = "c"\n[workloads.c]\nincumbent = "fake.echo"')
    assert snap.plan_for("c").budgets == DEFAULT_BUDGETS


def test_retry_is_replaced_wholesale_not_merged():
    """A partially-merged backoff policy is one whose behaviour you cannot
    read off either file."""
    snap = snapshot("""
default_workload = "a"
[defaults.retry]
max_attempts = 3
base_delay = 0.5
[workloads.a]
incumbent = "fake.echo"
[workloads.b]
incumbent = "fake.echo-anthropic"
[workloads.b.retry]
max_attempts = 1
""")
    assert snap.plan_for("a").retry == {"max_attempts": 3, "base_delay": 0.5}
    assert snap.plan_for("b").retry == {"max_attempts": 1}


# ==========================================================================
# single_target: the adoption path.
# ==========================================================================


def test_single_target_round_trips():
    snap = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG,
                                        clock=StubClock())
    plan = snap.plan_for()
    assert len(plan) == 1
    assert plan.targets[0].model.id == "fake.echo"
    assert plan.workload_id == "default"
    assert plan.policy_id == snap.id
    assert plan.budgets == DEFAULT_BUDGETS
    assert plan.retry is None


def test_single_target_takes_budgets_and_validates_them():
    tight = Budgets(total=10.0, first_event=5.0, progress=3.0)
    snap = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG,
                                        budgets=tight, clock=StubClock())
    assert snap.plan_for().budgets is tight
    with pytest.raises(E.PolicyError, match="can never fire"):
        PolicySnapshot.single_target(
            "fake.echo", catalog=DEFAULT_CATALOG,
            budgets=Budgets(total=1.0, first_event=9.0),
        )


def test_single_target_rejects_a_model_that_is_not_in_the_catalog():
    with pytest.raises(E.PolicyError, match="not in the catalog"):
        PolicySnapshot.single_target("nope.nope", catalog=DEFAULT_CATALOG)


def test_single_target_ids_are_content_addressed_too():
    a = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG)
    b = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG)
    c = PolicySnapshot.single_target("fake.echo-anthropic", catalog=DEFAULT_CATALOG)
    assert a.id == b.id != c.id


def test_a_snapshot_works_against_a_catalog_of_pure_fakes():
    """The BYOK / test-catalog path: policy holds no global state, so a
    catalog built entirely from fakes routes through the same code."""
    catalog = Catalog(
        models={"only.model": ModelSpec(id="only.model", provider="p", api_model="m",
                                        input_per_m=1, output_per_m=1,
                                        priced_at="2026-09-09")},
        providers={"p": ProviderConn(id="p", kind="openai")},
    )
    snap = PolicySnapshot.single_target("only.model", catalog=catalog)
    assert str(snap.plan_for().targets[0]) == "p/only.model"


# ==========================================================================
# Age.
# ==========================================================================


def test_age_is_measured_on_a_monotonic_clock_the_caller_supplies():
    clock = StubClock(start=1000.0)
    snap = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG,
                                        clock=clock)
    assert snap.age(clock) == 0.0
    clock.advance(40.0)                       # a 40-minute stream, compressed
    assert snap.age(clock) == 40.0


def test_age_is_reported_and_never_enforced():
    """Row 10's residual risk, left visible rather than closed. Bounding age
    means choosing what to do at the bound, and both choices are bad in
    different directions -- so the number is exposed and the decision is the
    operator's. If this module ever grows a max_age that refuses requests,
    this test is the one that should have to change."""
    clock = StubClock(start=0.0)
    snap = PolicySnapshot.single_target("fake.echo", catalog=DEFAULT_CATALOG,
                                        clock=clock)
    clock.advance(86_400.0)
    assert snap.age(clock) == 86_400.0
    assert len(snap.plan_for()) == 1          # a day old and still serving


# ==========================================================================
# The shipped example.
# ==========================================================================


def test_the_shipped_example_config_loads_and_routes():
    """The one file this suite reads from disk, and it earns the exception:
    an example config that no test parses is an example that rots into a
    documentation bug -- which is worse than no example, because it is the
    thing a new operator copies."""
    text = (pathlib.Path(__file__).parents[2] / "config"
            / "workloads.example.toml").read_text()
    snap = PolicySnapshot.from_toml(text, catalog=DEFAULT_CATALOG,
                                    clock=StubClock())
    assert snap.default_workload == "chat"
    assert len(snap.plan_for()) == 1                          # incumbent only
    assert len(snap.plan_for("summarize")) == 2               # the A/B
    assert snap.plan_for("autocomplete").budgets.first_event == 4.0
    assert snap.plan_for("autocomplete").budgets.connect == 2.0   # inherited
    assert isinstance(snap.plan_for("chat"), ExecutionPlan)

"""The seam between `policy.py` and `retry.py`, which neither module owns.

`policy.py` parses a `[workloads.x.retry]` table into a plain mapping and
deliberately does not import `retry.py`. `retry.py` defines `RetryPolicy` and
deliberately knows nothing about config files. That decoupling was the right
call -- the two were built concurrently and neither should depend on the
other's shape -- but it leaves exactly one unguarded joint:

    RetryPolicy(**dict(plan.retry))

Nothing type-checks that call. A config key that no longer matches a dataclass
field raises `TypeError` on the request path, at the moment a workload is
first used, in production, on the one workload whose retry table nobody
copied from another.

This is not hypothetical. The example config's first draft carried a `jitter`
key that `RetryPolicy` has no field for -- written from the
spec rather than the class, which is precisely how config drift happens on a
team. It was caught by hand. These tests are so it is caught by CI.

The general lesson: when two modules are decoupled on purpose, the seam
between them belongs to a third test that imports both. Decoupling moves the
risk; it does not delete it.
"""

from __future__ import annotations

import dataclasses
import pathlib

import pytest

from llmgw.catalog import DEFAULT_CATALOG
from llmgw.policy import PolicySnapshot
from llmgw.retry import RetryPolicy

EXAMPLE = pathlib.Path(__file__).resolve().parents[2] / "config" / "workloads.example.toml"


@pytest.fixture(scope="module")
def snapshot() -> PolicySnapshot:
    """The shipped example config, parsed. Read from disk on purpose: an
    example nobody parses rots into a documentation bug, and a documentation
    bug in a config file is worse than no example, because it is what a new
    operator copies."""
    return PolicySnapshot.from_toml(EXAMPLE.read_text(), catalog=DEFAULT_CATALOG)


def test_the_example_config_exists_where_the_docs_say_it_does():
    assert EXAMPLE.is_file(), f"{EXAMPLE} is referenced by the README"


def test_every_workloads_retry_table_constructs_a_real_RetryPolicy(snapshot):
    """The joint. One `TypeError` here is one production incident there."""
    for workload_id in snapshot.workloads:
        plan = snapshot.plan_for(workload_id)
        if plan.retry is None:
            continue
        policy = RetryPolicy(**dict(plan.retry))
        assert policy.validate() is policy


def test_no_retry_key_in_the_example_is_unknown_to_RetryPolicy(snapshot):
    """Reported as the offending key name rather than as a bare TypeError,
    because the failure a reader needs to see is *which* key drifted."""
    fields = {f.name for f in dataclasses.fields(RetryPolicy)}
    for workload_id in snapshot.workloads:
        plan = snapshot.plan_for(workload_id)
        unknown = set(dict(plan.retry or {})) - fields
        assert not unknown, (
            f"workload {workload_id!r} configures retry keys {sorted(unknown)} "
            f"which RetryPolicy has no field for. Known fields: {sorted(fields)}"
        )


def test_every_workload_in_the_example_produces_a_usable_plan(snapshot):
    """Cheap, and it makes the example config a tested artifact rather than a
    prose file that happens to be valid TOML."""
    for workload_id in snapshot.workloads:
        plan = snapshot.plan_for(workload_id)
        assert len(plan) >= 1
        assert plan.primary is plan.targets[0]
        plan.budgets.validate()
        assert plan.policy_id == snapshot.id


def test_the_default_workload_is_one_of_the_workloads(snapshot):
    assert snapshot.default_workload in snapshot.workloads


def test_a_retry_mapping_cannot_be_mutated_through_a_plan(snapshot):
    """Row 10 reaching through a pinned snapshot. A frozen dataclass wrapping
    a live dict advertises an immutability it does not have."""
    plan = snapshot.plan_for(snapshot.default_workload)
    if plan.retry is None:
        pytest.skip("default workload configures no retry table")
    with pytest.raises(TypeError):
        plan.retry["max_attempts"] = 99  # type: ignore[index]

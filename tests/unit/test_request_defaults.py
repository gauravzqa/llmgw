"""PLAN-2 B5: per-target request defaults.

merge(model.request_defaults, workload.request_defaults), workload winning,
shallow, frozen. Applying them to a body is `server/app.py`'s job; this file
pins what the plan hands it.
"""

from __future__ import annotations

import pathlib
from types import MappingProxyType

import pytest

from llmgw import errors as E
from llmgw.catalog import DEFAULT_CATALOG, ModelSpec
from llmgw.policy import PolicySnapshot, Workload

TOML = """
default_workload = "plain"

[workloads.plain]
incumbent = "deepseek.deepseek-v4-pro"

[workloads.quiet]
incumbent = "deepseek.deepseek-v4-pro"
candidate = "openrouter.deepseek-v4-flash"
  [workloads.quiet.request_defaults]
  thinking = { type = "disabled" }
  temperature = 0.2
"""


def snap(text: str = TOML) -> PolicySnapshot:
    return PolicySnapshot.from_toml(text, catalog=DEFAULT_CATALOG)


def test_a_workload_without_defaults_hands_the_executor_an_empty_mapping():
    plan = snap().plan_for("plain")
    assert dict(plan.request_defaults) == {}
    assert dict(plan.request_defaults_for(plan.primary)) == {}


def test_workload_defaults_reach_every_target_of_the_plan():
    plan = snap().plan_for("quiet")
    assert len(plan.targets) == 2
    for target in plan.targets:
        merged = plan.request_defaults_for(target)
        assert merged["thinking"] == {"type": "disabled"}
        assert merged["temperature"] == 0.2


def test_the_workload_wins_over_the_model_and_the_merge_is_shallow():
    spec = ModelSpec(id="m.x", provider="deepseek", api_model="x", input_per_m=1.0,
                     output_per_m=1.0, priced_at="2026-09-16",
                     request_defaults={"thinking": {"type": "enabled", "budget": 9},
                                       "top_p": 0.9})
    cat = DEFAULT_CATALOG.with_overrides(models={spec.id: spec})
    s = PolicySnapshot.from_toml(
        'default_workload = "w"\n[workloads.w]\nincumbent = "m.x"\n'
        '[workloads.w.request_defaults]\nthinking = { type = "disabled" }\n',
        catalog=cat,
    )
    plan = s.plan_for("w")
    merged = plan.request_defaults_for(plan.primary)
    # top-level key from the workload replaces the model's whole object
    assert merged["thinking"] == {"type": "disabled"}
    # the model's key the workload did not mention survives
    assert merged["top_p"] == 0.9


def test_defaults_are_frozen_on_the_way_in_and_plain_on_the_way_out():
    plan = snap().plan_for("quiet")
    assert isinstance(plan.request_defaults, MappingProxyType)
    with pytest.raises(TypeError):
        plan.request_defaults["thinking"] = None  # type: ignore[index]
    merged = plan.request_defaults_for(plan.primary)
    assert isinstance(merged, MappingProxyType)
    assert isinstance(merged["thinking"], dict)   # JSON-shaped for the body edit


def test_request_defaults_must_be_a_table():
    with pytest.raises(E.PolicyError, match="table"):
        table = ('  [workloads.quiet.request_defaults]\n'
                 '  thinking = { type = "disabled" }\n  temperature = 0.2\n')
        snap(TOML.replace(table, 'request_defaults = 3\n'))
    with pytest.raises(E.PolicyError, match="request_defaults"):
        Workload(id="w", incumbent="deepseek.deepseek-v4-pro", request_defaults=[1])  # type: ignore[arg-type]


def test_request_defaults_are_part_of_the_policy_id():
    a = snap()
    b = snap(TOML.replace("temperature = 0.2", "temperature = 0.3"))
    assert a.id != b.id


def test_the_shipped_example_disables_thinking_on_the_cheap_candidate():
    text = (pathlib.Path(__file__).resolve().parents[2] / "config"
            / "workloads.example.toml").read_text()
    plan = PolicySnapshot.from_toml(text, catalog=DEFAULT_CATALOG).plan_for("cheap-candidate")
    assert plan.request_defaults_for(plan.primary)["thinking"] == {"type": "disabled"}
    # and the DeepSeek rows themselves carry none: it is a workload decision
    assert dict(DEFAULT_CATALOG.models["deepseek.deepseek-v4-flash"].request_defaults) == {}

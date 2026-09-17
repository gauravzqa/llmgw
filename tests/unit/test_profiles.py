"""PLAN-2 B6: named budget profiles, and the `headers` budget (B4).

Resolution order for a workload's budgets is workload-explicit > profile >
defaults, and `largest_total()` sees profiles nobody uses yet, because the
deploy arithmetic has to hold for every stream a policy can start.
"""

from __future__ import annotations

import pathlib

import pytest

from llmgw import errors as E
from llmgw.catalog import DEFAULT_CATALOG
from llmgw.clocks import Budgets
from llmgw.policy import DEFAULT_BUDGETS, PolicySnapshot

TOML = """
default_workload = "chat"

[defaults.budgets]
total = 300.0
first_event = 20.0
progress = 15.0

[profiles.tts.budgets]
headers = 2.0
first_event = 2.0
progress = 5.0
total = 150.0

[profiles.long_context.budgets]
first_event = 60.0
total = 600.0

[workloads.chat]
incumbent = "anthropic.sonnet-4-6"

[workloads.speech]
incumbent = "anthropic.haiku-4-5"
profile = "tts"

[workloads.speech-patient]
incumbent = "anthropic.haiku-4-5"
profile = "tts"
  [workloads.speech-patient.budgets]
  first_event = 4.0
"""


def snap(text: str = TOML) -> PolicySnapshot:
    return PolicySnapshot.from_toml(text, catalog=DEFAULT_CATALOG)


# -------------------------------------------------------------- resolution


def test_a_profile_starts_from_defaults_and_a_workload_starts_from_its_profile():
    s = snap()
    speech = s.plan_for("speech").budgets
    assert speech.first_event == 2.0            # from the profile
    assert speech.progress == 5.0               # from the profile
    assert speech.headers == 2.0                # from the profile
    assert speech.total == 150.0                # from the profile
    assert speech.connect == DEFAULT_BUDGETS.connect   # profile inherited defaults
    assert speech.client_stall == 30.0
    chat = s.plan_for("chat").budgets
    assert chat.total == 300.0 and chat.first_event == 20.0


def test_workload_explicit_beats_profile_beats_defaults():
    b = snap().plan_for("speech-patient").budgets
    assert b.first_event == 4.0     # workload-explicit
    assert b.progress == 5.0        # profile
    assert b.total == 150.0         # profile
    assert b.connect == 2.0         # defaults


def test_the_profile_name_survives_on_the_workload_for_inspection():
    s = snap()
    assert s.workloads["speech"].profile == "tts"
    assert s.workloads["chat"].profile is None
    assert set(s.profiles) == {"tts", "long_context"}


# -------------------------------------------------------------- validation


def test_an_undefined_profile_is_refused_at_load():
    bad = TOML.replace('profile = "tts"', 'profile = "tts2"', 1)
    with pytest.raises(E.PolicyError, match="tts2"):
        snap(bad)


def test_an_unknown_profile_key_is_refused():
    bad = TOML.replace("[profiles.tts.budgets]",
                       "[profiles.tts]\nretry = 1\n[profiles.tts.budgets]")
    with pytest.raises(E.PolicyError, match="unknown key"):
        snap(bad)


def test_a_profile_that_cannot_fire_is_refused_even_if_unused():
    bad = TOML.replace("first_event = 60.0\ntotal = 600.0",
                       "first_event = 900.0\ntotal = 600.0")
    with pytest.raises(E.PolicyError, match="can never fire"):
        snap(bad)


# ------------------------------------------------------------ largest_total


def test_largest_total_counts_unused_profiles():
    s = snap()
    # workloads: 300 (chat), 150, 150; profiles: 150, 600 (long_context, unused)
    assert s.largest_total() == 600.0


def test_largest_total_is_the_largest_workload_when_no_profile_exceeds_it():
    s = snap(TOML.replace("total = 600.0", "total = 200.0"))
    assert s.largest_total() == 300.0


def test_single_target_snapshot_has_no_profiles_and_its_own_total():
    s = PolicySnapshot.single_target("anthropic.haiku-4-5", catalog=DEFAULT_CATALOG,
                                     budgets=Budgets(total=42.0))
    assert s.profiles == {}
    assert s.largest_total() == 42.0


# ------------------------------------------------------------------ identity


def test_profiles_are_part_of_the_policy_id():
    a = snap()
    b = snap(TOML.replace("total = 600.0", "total = 610.0"))
    assert a.id != b.id, "an unused profile still changes what the policy admits"


def test_a_spelled_out_workload_and_a_profiled_one_share_an_id():
    """The profile NAME is not hashed; the resolved budgets are."""
    via_profile = snap()
    spelled = TOML.replace(
        '[workloads.speech]\nincumbent = "anthropic.haiku-4-5"\nprofile = "tts"',
        '[workloads.speech]\nincumbent = "anthropic.haiku-4-5"\n'
        '  [workloads.speech.budgets]\n  headers = 2.0\n  first_event = 2.0\n'
        '  progress = 5.0\n  total = 150.0',
    )
    assert snap(spelled).id == via_profile.id


# ------------------------------------------------------------- headers budget


def test_budgets_carry_a_headers_phase_separate_from_connect():
    b = Budgets(total=60.0)
    assert b.connect == 2.0
    assert b.headers == 10.0
    assert b.validate() is b


def test_headers_budget_must_be_positive_but_is_clamped_not_refused_by_total():
    with pytest.raises(ValueError, match="headers"):
        Budgets(total=60.0, headers=0.0).validate()
    # A policy written before the field existed with `total = 5` must keep
    # loading: the 10 s default is clamped by `Deadline.slice()`, not refused.
    b = Budgets(total=5.0, first_event=4.0, progress=3.0).validate()
    assert b.headers == 10.0 and b.total == 5.0


def test_headers_is_a_budget_key_in_toml():
    s = snap(TOML.replace("[defaults.budgets]\n", "[defaults.budgets]\nheaders = 7.5\n"))
    assert s.plan_for("chat").budgets.headers == 7.5


# --------------------------------------------------------- shipped example


def test_the_shipped_example_uses_both_features():
    text = (pathlib.Path(__file__).resolve().parents[2] / "config"
            / "workloads.example.toml").read_text()
    s = PolicySnapshot.from_toml(text, catalog=DEFAULT_CATALOG)
    assert set(s.profiles) >= {"tts", "long_context"}
    longform = s.plan_for("longform").budgets
    assert longform.total == 600.0 and longform.first_event == 90.0
    assert s.largest_total() == 600.0

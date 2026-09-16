"""`ServerConfig`: the deploy inequality and the per-process stream cap.

Two knobs P0 added after the load campaign, both checked at startup because
both failed silently for six phases:

* `budgets.total <= drain_grace_seconds`. The shipped defaults were 600 s and
  25 s -- a gateway that admitted ten-minute streams and cut them 25 s into
  every deploy -- and no test noticed because no test held both numbers at
  once. `validated()` now refuses the pair, with an explicit override that
  logs instead.
* `max_streams`. S2 showed the process degrading before it shed; the cap is
  the shed. `0` from the environment means "no cap"; `0` in code is an error.
"""

from __future__ import annotations

import logging

import pytest

from llmgw.clocks import Budgets
from llmgw.server.config import ServerConfig

# --------------------------------------------------------------- defaults


def test_defaults_satisfy_the_deploy_inequality():
    cfg = ServerConfig().validated()
    assert cfg.budgets.total == 120.0
    assert cfg.drain_grace_seconds == 130.0
    assert cfg.budgets.total <= cfg.drain_grace_seconds
    assert cfg.drain_allow_short is False
    assert cfg.max_streams == 150


def test_from_env_defaults_match_the_dataclass_defaults():
    cfg = ServerConfig.from_env({})
    assert cfg.budgets.total == 120.0
    assert cfg.drain_grace_seconds == 130.0
    assert cfg.max_streams == 150


# --------------------------------------------------------------- total vs grace


def _budgets(total: float) -> Budgets:
    return Budgets(total=total, connect=1.0, first_event=1.0, progress=1.0,
                   client_stall=1.0)


def test_total_above_grace_is_refused_with_both_numbers_in_the_message():
    with pytest.raises(ValueError) as info:
        ServerConfig(budgets=_budgets(600.0), drain_grace_seconds=25.0).validated()
    text = str(info.value)
    assert "600" in text and "25" in text
    assert "LLMGW_DRAIN_ALLOW_SHORT" in text


def test_total_equal_to_grace_is_accepted():
    cfg = ServerConfig(budgets=_budgets(30.0), drain_grace_seconds=30.0).validated()
    assert cfg.budgets.total == cfg.drain_grace_seconds


def test_total_below_grace_is_accepted():
    ServerConfig(budgets=_budgets(10.0), drain_grace_seconds=30.0).validated()


def test_allow_short_starts_anyway_and_warns_with_both_numbers(caplog):
    with caplog.at_level(logging.WARNING, logger="llmgw.server.config"):
        cfg = ServerConfig(
            budgets=_budgets(600.0), drain_grace_seconds=25.0, drain_allow_short=True,
        ).validated()
    assert cfg.drain_allow_short is True
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "600" in warnings[0].getMessage() and "25" in warnings[0].getMessage()


def test_allow_short_does_not_warn_when_the_inequality_holds(caplog):
    with caplog.at_level(logging.WARNING, logger="llmgw.server.config"):
        ServerConfig(
            budgets=_budgets(10.0), drain_grace_seconds=30.0, drain_allow_short=True,
        ).validated()
    assert not [r for r in caplog.records if r.name == "llmgw.server.config"]


def test_from_env_refuses_the_old_shipped_pair():
    with pytest.raises(ValueError):
        ServerConfig.from_env({"LLMGW_BUDGET_TOTAL": "600", "LLMGW_DRAIN_GRACE": "25"})


@pytest.mark.parametrize("raw", ["1", "true", "yes", "on"])
def test_from_env_allow_short_override(raw: str):
    cfg = ServerConfig.from_env({
        "LLMGW_BUDGET_TOTAL": "600", "LLMGW_DRAIN_GRACE": "25",
        "LLMGW_DRAIN_ALLOW_SHORT": raw,
    })
    assert cfg.drain_allow_short is True
    assert cfg.budgets.total == 600.0


# --------------------------------------------------------------- max_streams


def test_env_max_streams_parses_a_positive_integer():
    assert ServerConfig.from_env({"LLMGW_MAX_STREAMS": "250"}).max_streams == 250


def test_env_max_streams_zero_means_no_cap():
    assert ServerConfig.from_env({"LLMGW_MAX_STREAMS": "0"}).max_streams is None


def test_env_max_streams_blank_keeps_the_default():
    assert ServerConfig.from_env({"LLMGW_MAX_STREAMS": ""}).max_streams == 150


def test_env_max_streams_garbage_is_loud():
    with pytest.raises(ValueError):
        ServerConfig.from_env({"LLMGW_MAX_STREAMS": "lots"})


def test_code_max_streams_none_is_uncapped():
    assert ServerConfig(max_streams=None).validated().max_streams is None


@pytest.mark.parametrize("bad", [0, -1])
def test_code_max_streams_must_be_positive(bad: int):
    # Zero would refuse every request and look like an outage with no log;
    # in code the spelling for "uncapped" is None, never 0.
    with pytest.raises(ValueError, match="max_streams"):
        ServerConfig(max_streams=bad).validated()

"""The two-turn loop (PLAN-2 A1): the model the provider echoed is accepted
on the next turn.

Observed live on 16 Sep 2026: turn one asks for `openai.gpt-4o-mini`, OpenAI
answers `"model": "gpt-4o-mini-2024-07-18"`, the SDK copies that into turn
two, and the gateway -- knowing only catalog ids -- says `400 unknown model`.
Multi-turn agents do exactly this on every turn.

Same recorder upstream as `test_model_rewrite.py` (imported, so the fixtures
are module-scoped copies with their own ports), with the recorder asked to
echo the served wire id the way a real provider does.
"""

# ruff: noqa: F811 -- fixtures are imported by name and then named again as test
# parameters, which is how pytest finds them and how ruff spells "redefinition".

from __future__ import annotations

import json

import httpx
import pytest
from fakes.upstream import PATHS

from tests.contract.test_model_rewrite import (  # noqa: F401 - fixtures by import
    DIRECT,
    ECHO_FIELD,
    INC_WIRE,
    Recorder,
    Running,
    _clean,
    client,
    gateway,
    post,
    recorder,
    upstream,
)

pytestmark = pytest.mark.contract


def turn(model: str) -> dict:
    return {"model": model, "stream": True, "max_tokens": 64, ECHO_FIELD: True,
            "messages": [{"role": "user", "content": "hi"}]}


def echoed_model(stream: bytes) -> str:
    """The `model` the provider put in its frames -- what an SDK would copy."""
    models = set()
    for line in stream.split(b"\n"):
        if not line.startswith(b"data: ") or line.endswith(b"[DONE]"):
            continue
        models.add(json.loads(line[6:])["model"])
    assert len(models) == 1, models
    return models.pop()


async def test_the_echoed_wire_id_is_accepted_on_the_next_turn(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    """Turn one names the catalog id; the plan falls to the incumbent, whose
    wire id comes back in every frame. Turn two sends that wire id as an SDK
    would. It must route to the same target, not 400."""
    status, headers, stream = await post(
        client, f"{gateway.base_url}/workloads/ab{PATHS['openai']}", turn("rec.candidate"),
    )
    assert status == 200
    assert headers["x-gw-served-by"] == "inc/rec.incumbent"
    seen = echoed_model(stream)
    assert seen == INC_WIRE, "the recorder echoes the wire id it served, as providers do"

    status2, headers2, stream2 = await post(
        client, f"{gateway.base_url}{PATHS['openai']}", turn(seen),
    )
    assert status2 == 200, stream2[:300]
    assert headers2["x-gw-served-by"] == "inc/rec.incumbent"
    assert headers2["x-gw-attempts"] == "1"
    # The explicit-model path resolved the alias to the catalog id, and the
    # upstream still receives its own wire id -- identical to what was sent,
    # so this turn is not marked as modified.
    assert recorder.models[-1] == INC_WIRE
    assert "x-gw-body-modified" not in headers2


async def test_a_bare_catalog_id_and_its_wire_id_reach_the_same_target(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    for model in ("rec.incumbent", INC_WIRE):
        status, headers, _ = await post(
            client, f"{gateway.base_url}{PATHS['openai']}", turn(model),
        )
        assert status == 200
        assert headers["x-gw-served-by"] == "inc/rec.incumbent"
    assert recorder.models == [INC_WIRE, INC_WIRE]


async def test_an_id_no_target_answers_to_is_still_a_400(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    """Aliases widen what resolves; they do not turn unknown strings into a
    default route. Nothing is sent upstream."""
    status, headers, body = await post(
        client, f"{gateway.base_url}{PATHS['openai']}", turn("gpt-99-ultra"),
    )
    assert status == 400
    assert b"unknown model" in body
    assert recorder.models == []


async def test_the_direct_workload_is_unaffected(
    gateway: Running, recorder: Recorder, client: httpx.AsyncClient
):
    """A catalog id that IS its wire id resolves exactly as before."""
    status, headers, _ = await post(
        client, f"{gateway.base_url}/workloads/direct{PATHS['openai']}", turn(DIRECT),
    )
    assert status == 200
    assert headers["x-gw-attempts"] == "1"
    assert recorder.models == [DIRECT]

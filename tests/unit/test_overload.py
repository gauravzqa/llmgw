"""The per-process stream cap, as a property of the endpoint.

`tests/contract/test_overload.py` proves it over sockets with real streams
held open. This file proves the decision itself on the ASGI endpoint with no
`startup()`, the way `test_lifecycle.py` proves the draining shed: the
tracker is bumped by hand to stand in for streams that are in flight, and the
endpoint is invoked once.

What is pinned:

* the (cap+1)th request is refused, 503, `type="overloaded"`, `Retry-After: 1`;
* the cap-th request is NOT refused by the cap (it proceeds to admission and
  the body read, which is where this driver stops it);
* the refusal is counted under the pre-declared `"overloaded"` reason and
  nowhere in the tenant's admission counters (C6);
* the tracker returns to its baseline after a shed request, so a shed can
  never pin a drain;
* `None` is uncapped.
"""

from __future__ import annotations

import json

import pytest

from llmgw.clocks import ManualClock
from llmgw.server.app import ROUTE_TO_UPSTREAM_PATH, Gateway, PassthroughEndpoint
from llmgw.server.config import ServerConfig
from llmgw.surfaces import for_path


def _gateway(max_streams: int | None) -> Gateway:
    return Gateway(ServerConfig(max_streams=max_streams), clock=ManualClock(start=1_000.0))


def _endpoint(gw: Gateway) -> tuple[PassthroughEndpoint, str]:
    path, upstream_path = next(iter(ROUTE_TO_UPSTREAM_PATH.items()))
    surface = for_path(upstream_path)
    assert surface is not None
    return PassthroughEndpoint(gw, surface=surface, route=path), path


class _BodyRead(Exception):
    """Raised by the receive stub: the endpoint got past the cap and asked for
    the body, which is the first thing after admission this driver cannot
    supply. Reaching it IS the assertion that the cap did not fire."""


async def _drive(ep: PassthroughEndpoint, path: str) -> list[dict]:
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    async def receive() -> dict:
        raise _BodyRead

    scope = {
        "type": "http", "method": "POST", "path": path,
        "headers": [], "path_params": {},
    }
    await ep(scope, receive, send)
    return sent


def _response(sent: list[dict]) -> tuple[int, dict[bytes, bytes], dict]:
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = next(m for m in sent if m["type"] == "http.response.body")["body"]
    return start["status"], dict(start["headers"]), json.loads(body)


async def test_the_request_over_the_cap_is_shed_with_retry_after():
    gw = _gateway(max_streams=2)
    ep, path = _endpoint(gw)
    gw.stream_entered()
    gw.stream_entered()  # two in flight; this request will be the third

    status, headers, body = _response(await _drive(ep, path))

    assert status == 503
    assert body["error"]["type"] == "overloaded"
    assert "2" in body["error"]["message"]
    assert headers[b"retry-after"] == b"1"
    assert b"x-gw-tenant" in headers, "refused after the tenant was resolved"


async def test_the_request_at_the_cap_is_not_shed():
    gw = _gateway(max_streams=2)
    ep, path = _endpoint(gw)
    gw.stream_entered()  # one in flight; this request is the second == cap

    with pytest.raises(_BodyRead):
        await _drive(ep, path)
    assert gw.overloaded_denials() == {"overloaded": 0}


async def test_shed_is_counted_under_overloaded_and_costs_no_tenant_credit():
    gw = _gateway(max_streams=1)
    ep, path = _endpoint(gw)
    gw.stream_entered()
    before = dict(gw.admission.denials())

    await _drive(ep, path)

    assert gw.overloaded_denials() == {"overloaded": 1}
    assert gw.draining_denials() == {"draining": 0}
    # C6: the process refused this request for its own sake; the tenant's
    # counters do not move, and no permit was taken.
    assert gw.admission.denials() == before
    assert gw.admission.total_in_use() == 0


async def test_shed_request_leaves_the_tracker_at_its_baseline():
    gw = _gateway(max_streams=1)
    ep, path = _endpoint(gw)
    gw.stream_entered()
    assert gw.inflight == 1

    await _drive(ep, path)

    assert gw.inflight == 1, "the shed request must not pin a drain"


async def test_none_is_uncapped():
    gw = _gateway(max_streams=None)
    ep, path = _endpoint(gw)
    for _ in range(1_000):
        gw.stream_entered()
    assert gw.over_capacity() is False
    with pytest.raises(_BodyRead):
        await _drive(ep, path)


async def test_draining_wins_over_the_cap():
    """Both are 503s; the one that fires first is the one that reads no
    request state at all, and the label says so."""
    gw = _gateway(max_streams=1)
    ep, path = _endpoint(gw)
    gw.stream_entered()
    gw.draining = True

    status, _headers, body = _response(await _drive(ep, path))

    assert status == 503
    assert body["error"]["type"] == "draining"
    assert gw.overloaded_denials() == {"overloaded": 0}


def test_over_capacity_counts_the_current_request():
    """`over_capacity()` is read after `stream_entered()` for the request it
    judges, so N streams may be open under a cap of N and the (N+1)th is the
    one refused."""
    gw = _gateway(max_streams=3)
    for _ in range(3):
        gw.stream_entered()
    assert gw.over_capacity() is False
    gw.stream_entered()
    assert gw.over_capacity() is True
    gw.stream_exited()
    assert gw.over_capacity() is False

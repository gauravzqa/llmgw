"""C11 over real sockets: the fake's 401 body does not reach the client.

`tests/unit/test_auth_scrub.py` pins `send_error`. This file is the same
promise asserted from outside, through a real uvicorn, against the fake's
`401` mode -- whose body quotes `****abcd` exactly the way the real provider
in findings-log #30 did -- with the C4 control right next to it: the fake's
`5xx` body IS forwarded, byte for byte, so the exception is provably the
narrow one.

Both gateways come from `test_fallback.py`'s session cache; both targets are
baked to the same mode so the plan walks candidate then incumbent and the
terminal error is the second target's.
"""

from __future__ import annotations

import httpx
import pytest

from tests.contract.conftest import Fakes
from tests.contract.test_fallback import (  # noqa: F401 - fixtures by import
    UPSTREAM_PATH,
    body,
    client,
    gateways,
    mode,
    policy_file,
)

pytestmark = pytest.mark.contract

SECRET_TAIL = b"abcd"


async def test_a_provider_401_reaches_the_client_as_a_status_without_the_body(
    gateways, fakes: Fakes, client: httpx.AsyncClient  # noqa: F811 - pytest fixtures
):
    gw = gateways(mode("401"), mode("401"))

    # The workload path form: a body that names a model pins ONE target and
    # gets no fallback (see test_fallback's "names no workload" test).
    r = await client.post(gw.url("ab"), json=body(stream=False))

    assert r.status_code == 401
    assert SECRET_TAIL not in r.content
    assert b"Authentication Fails" not in r.content
    assert r.json()["error"]["type"] == "upstream_auth"
    # The walk is visible: two targets tried, both refused the credential.
    assert r.headers["x-gw-attempts"] == "2"
    assert "www-authenticate" not in r.headers
    # Both targets were actually asked -- the scrub is not a short-circuit.
    assert fakes.stats()["by_mode"].get("401") == 2


async def test_a_provider_5xx_body_is_still_forwarded_byte_for_byte(
    gateways, fakes: Fakes, client: httpx.AsyncClient  # noqa: F811 - pytest fixtures
):
    """C4, unchanged. The fake answered directly is the reference bytes."""
    direct = await client.post(
        f"{fakes.openai.base_url}{UPSTREAM_PATH}",
        headers={"X-Fake-Mode": "5xx"}, json=body(stream=False),
    )
    assert direct.status_code >= 500 and direct.content

    gw = gateways(mode("5xx"), mode("5xx"))
    r = await client.post(gw.url("ab"), json=body(stream=False))

    assert r.status_code == direct.status_code
    assert r.content == direct.content
    assert r.headers["x-gw-attempts"] == "2"

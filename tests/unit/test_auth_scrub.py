"""CONTRACTS.md C11: a provider's auth-failure body never reaches the client.

Findings-log #30: DeepSeek's 401 message quotes the tail of the key it
rejected, and C4 passthrough relayed it. Under a shared key that is four
characters of a shared secret handed to whoever sent the request. The fix is
the narrowest possible exception to C4 -- `AuthenticationFailed` keeps the
upstream STATUS and loses the upstream BODY -- and these tests pin both
halves: the auth body is replaced, and a non-auth passthrough body is still
byte-for-byte the provider's.

`send_error` is driven directly with a collecting `send`; the `Exchange` only
needs a snapshot id for the `X-Gw-*` headers, so a stand-in object is enough.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

from llmgw import errors
from llmgw.server.app import Exchange, send_error

SECRET_TAIL = "abcd"
DEEPSEEK_401 = (
    b'{"error":{"message":"Authentication Fails, Your api key: ****'
    + SECRET_TAIL.encode()
    + b' is invalid","type":"authentication_error"}}'
)
SERVER_500 = (
    b'{"error":{"message":"synthetic 500 with a marker: sk-live-abcd",'
    b'"type":"server_error"}}'
)


def _exchange() -> Exchange:
    ex = Exchange(SimpleNamespace(id="policy_test"), catalog_id="cat", workload_id="wl")
    ex.attempts = 1
    ex.tenant = "acme"
    return ex


async def _send_error(err: errors.GatewayError) -> tuple[int, dict[bytes, bytes], bytes]:
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    await send_error(send, err, exchange=_exchange())
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = next(m for m in sent if m["type"] == "http.response.body")["body"]
    return start["status"], dict(start["headers"]), body


async def test_an_upstream_401_body_is_replaced_and_the_status_kept():
    err = errors.from_http_status(401, body=DEEPSEEK_401, provider="deepseek",
                                  model="deepseek-chat")
    assert isinstance(err, errors.AuthenticationFailed)

    status, headers, body = await _send_error(err)

    assert status == 401
    assert SECRET_TAIL.encode() not in body
    assert b"Authentication Fails" not in body
    payload = json.loads(body)
    assert payload["error"]["type"] == "upstream_auth"
    assert "deepseek" in payload["error"]["message"]
    assert headers[b"content-type"] == b"application/json"
    assert headers[b"content-length"] == str(len(body)).encode()
    assert b"x-gw-tenant" in headers
    assert b"www-authenticate" not in headers


async def test_an_upstream_403_is_scrubbed_the_same_way():
    err = errors.from_http_status(403, body=DEEPSEEK_401, provider="openrouter")
    status, _, body = await _send_error(err)
    assert status == 403
    assert SECRET_TAIL.encode() not in body
    assert json.loads(body)["error"]["type"] == "upstream_auth"


async def test_a_non_auth_passthrough_body_is_still_the_providers_bytes():
    """The C4 regression guard: the scrub is for auth failures only."""
    err = errors.from_http_status(500, body=SERVER_500, provider="deepseek")
    assert err.passthrough and not isinstance(err, errors.AuthenticationFailed)

    status, _, body = await _send_error(err)

    assert status == 500
    assert body == SERVER_500


async def test_the_scrub_does_not_change_what_we_record():
    """The taxonomy is untouched: blame, health scope and the raw body on
    the error object are as before, so the breaker and the capture record
    see exactly what they saw. Only the wire changes."""
    err = errors.from_http_status(401, body=DEEPSEEK_401, provider="deepseek")
    assert err.upstream_body == DEEPSEEK_401
    assert err.health_scope is errors.HealthScope.CREDENTIAL
    assert err.try_next is True and err.retry_same is False
    assert err.client_status == 401

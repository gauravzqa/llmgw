"""PLAN-2 B4 over real sockets: `multipart` and `raw` bodies.

No shipped surface declares either kind yet (Phase D's voice surfaces will),
so two test-only surfaces are registered through `build_app(extra_surfaces=)`:
subclasses of the OpenAI chat surface whose upstream `path` is still the
fake's `/v1/chat/completions` and whose `body` is `multipart` or `raw`.

What is proven: the multipart body and its boundary reach the fake intact and
un-edited; the raw body reaches it byte-for-byte with the client's own
content type; a body the gateway cannot route (no model in the first 64 KiB,
or no `?model=`) is a 400 the fake never sees.
"""

from __future__ import annotations

import pytest

from llmgw.clocks import Budgets
from llmgw.server.app import MULTIPART_SCAN_BYTES, build_app
from llmgw.server.config import ServerConfig, fake_catalog
from llmgw.surfaces import OPENAI_CHAT
from tests.contract._phase_a_harness import serve
from tests.contract.conftest import BREAKER_NEVER_TRIPS, Fakes

pytestmark = pytest.mark.contract

FAKE_HEADERS = ("x-fake-mode", "x-fake-events", "x-fake-interval", "x-fake-delay")


# `name` stays `openai_chat`: the metric label vocabulary (`metrics.SURFACES`)
# is closed on purpose and these doubles must not widen it; Phase D adds the
# real voice surface names there. The caps and the dialect hint therefore
# read as chat's, which is fine for bodies of a few hundred KB.


class _MultipartSurface(OPENAI_CHAT.__class__):  # type: ignore[misc,valid-type]
    name = "openai_chat"
    body = "multipart"


class _RawSurface(OPENAI_CHAT.__class__):  # type: ignore[misc,valid-type]
    name = "openai_chat"
    body = "raw"


@pytest.fixture(scope="module")
def gateway(fakes: Fakes):
    config = ServerConfig(
        catalog=fake_catalog(
            openai_url=f"{fakes.openai.base_url}/v1",
            anthropic_url=fakes.anthropic.base_url,
        ),
        fake_upstreams=True,
        forward_request_headers=FAKE_HEADERS,
        breaker=BREAKER_NEVER_TRIPS,
        budgets=Budgets(total=30.0, connect=2.0, first_event=10.0,
                        progress=10.0, client_stall=10.0),
    )
    server = serve(build_app(config, extra_surfaces={
        "/v1/_test/multipart": _MultipartSurface(),
        "/v1/_test/raw": _RawSurface(),
    }))
    try:
        yield server
    finally:
        server.stop()


def _multipart(fields: list[tuple[str, bytes]], boundary: str = "gwB0undary") -> bytes:
    out = []
    for name, value in fields:
        head = f'Content-Disposition: form-data; name="{name}"'
        if name == "file":
            head += '; filename="clip.mp3"\r\nContent-Type: audio/mpeg'
        out.append(f"--{boundary}\r\n{head}\r\n\r\n".encode() + value + b"\r\n")
    return b"".join(out) + f"--{boundary}--\r\n".encode()


async def test_multipart_body_and_boundary_reach_the_fake_intact(
    gateway, fakes: Fakes, client
):
    audio = bytes(range(256)) * 400  # 100 KB of "audio"
    body = _multipart([("model", b"fake.echo"), ("stream", b"false"), ("file", audio)])
    ctype = "multipart/form-data; boundary=gwB0undary"
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/v1/_test/multipart", content=body,
        headers={"content-type": ctype, "x-fake-mode": "multipart-echo"},
    )
    assert r.status_code == 200, r.text[:300]
    echoed = r.json()
    assert echoed["boundary"] == "gwB0undary"
    # The client's content-type must be forwarded verbatim (the boundary is in it).
    assert echoed["content_type"] == ctype
    # The ONE edit a multipart body gets is the model form field, spliced to
    # the target's wire id (18 Sep 2026: OpenAI 404'd on the catalog id that
    # reached it verbatim). `fake.echo` -> `fake-echo` happens to keep the
    # length; boundary, the other fields and the file bytes are untouched.
    assert echoed["values"]["model"] == "fake-echo"
    assert echoed["values"]["stream"] == "false"
    assert echoed["received_bytes"] == len(body)
    assert echoed["fields"] == ["model", "stream", "file"]
    assert echoed["sizes"]["file"] == len(audio)
    assert r.headers["x-gw-body-modified"] == "1"
    assert r.headers["x-gw-served-by"].endswith("fake.echo")
    assert fakes.stats()["total"] == before + 1


async def test_multipart_without_a_routable_model_is_a_400_before_upstream(
    gateway, fakes: Fakes, client
):
    before = fakes.stats()["total"]
    # The model field is AFTER a file larger than the scan window: the
    # documented limitation, and the fail-closed behaviour it implies.
    late = _multipart([
        ("file", b"\xff" * (MULTIPART_SCAN_BYTES + 1024)), ("model", b"fake.echo"),
    ])
    r = await client.post(
        f"{gateway.base_url}/v1/_test/multipart", content=late,
        headers={"content-type": "multipart/form-data; boundary=gwB0undary",
                 "x-fake-mode": "multipart-echo"},
    )
    assert r.status_code == 400
    assert r.json()["error"]["type"] == "invalid_request"
    # And a JSON body on a multipart surface is refused the same way.
    r = await client.post(
        f"{gateway.base_url}/v1/_test/multipart", content=b'{"model":"fake.echo"}',
        headers={"content-type": "application/json", "x-fake-mode": "multipart-echo"},
    )
    assert r.status_code == 400
    assert fakes.stats()["total"] == before


async def test_raw_body_is_forwarded_byte_for_byte_with_its_content_type(
    gateway, fakes: Fakes, client
):
    raw = bytes((i * 31) % 256 for i in range(50_000))
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/v1/_test/raw", params={"model": "fake.echo"}, content=raw,
        headers={"content-type": "audio/pcm", "x-fake-mode": "big-vision"},
    )
    assert r.status_code == 200, r.text[:300]
    assert r.json()["received_bytes"] == len(raw)
    assert "x-gw-body-modified" not in r.headers
    assert fakes.stats()["total"] == before + 1


async def test_raw_body_names_its_model_in_the_query_or_the_header(
    gateway, fakes: Fakes, client
):
    before = fakes.stats()["total"]
    r = await client.post(
        f"{gateway.base_url}/v1/_test/raw", content=b"\x00\x01",
        headers={"content-type": "audio/pcm", "x-fake-mode": "big-vision"},
    )
    assert r.status_code == 400
    assert "X-Gw-Model" in r.json()["error"]["message"]
    assert fakes.stats()["total"] == before
    r = await client.post(
        f"{gateway.base_url}/v1/_test/raw", content=b"\x00\x01",
        headers={"content-type": "audio/pcm", "x-fake-mode": "big-vision",
                 "x-gw-model": "fake.echo"},
    )
    assert r.status_code == 200
    assert fakes.stats()["total"] == before + 1


@pytest.fixture
async def client():
    import httpx

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as c:
        yield c


async def test_multipart_that_already_names_the_wire_id_is_forwarded_untouched(
    gateway, fakes: Fakes, client
):
    body = _multipart([("model", b"fake-echo"), ("file", b"\x01" * 512)])
    ctype = "multipart/form-data; boundary=gwB0undary"
    r = await client.post(
        f"{gateway.base_url}/v1/_test/multipart", content=body,
        headers={"content-type": ctype, "x-fake-mode": "multipart-echo"},
    )
    assert r.status_code == 200, r.text[:300]
    echoed = r.json()
    assert echoed["values"]["model"] == "fake-echo"
    assert echoed["received_bytes"] == len(body)
    assert "x-gw-body-modified" not in r.headers, "a wire id needs no edit"

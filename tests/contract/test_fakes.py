"""The fake upstreams, tested from the outside like any other server.

These tests are not about the gateway. They are about the *instrument*: every
later contract test reads "candidate is `die-mid-stream`, incumbent is `ok`"
and concludes something about the gateway from what the client saw. That
conclusion is only worth anything if `die-mid-stream` really truncates and
`ok` really terminates -- so the instrument gets calibrated first, over real
sockets, from a real client, exactly once.

Everything here runs against session-scoped uvicorn servers on loopback. The
whole file targets well under 20 s, which is why the stall modes are asserted
with a 300 ms client timeout against a 5 s upstream silence instead of by
waiting out the stall. Proving "silent for 300 ms while the upstream intends
5 s" is the same proof and it is sixteen times cheaper.
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest
from fakes import upstream as fake
from fakes import wire

from tests.contract.conftest import Fakes

pytestmark = pytest.mark.contract

# Long enough that a healthy response lands inside it, short enough that a
# stalling one is proven silent without the test paying for the stall.
QUICK = httpx.Timeout(5.0, read=0.3)


def frames(body: bytes) -> list[bytes]:
    """Split an SSE body into frames, dropping the empty tail.

    Deliberately dumb: `body.split(b"\\n\\n")` is not a general SSE parser and
    must not become one. The gateway's parser lives in `src/`; if this helper
    grew a state machine, the contract tier would be testing the fake against
    a second implementation of the thing it is meant to be validating.
    """
    return [f for f in body.split(b"\n\n") if f.strip()]


def data_objects(body: bytes) -> list[dict]:
    """Every JSON `data:` payload, skipping OpenAI's non-JSON `[DONE]`."""
    out = []
    for frame in frames(body):
        for line in frame.split(b"\n"):
            if line.startswith(b"data: "):
                payload = line[len(b"data: ") :]
                if payload != b"[DONE]":
                    out.append(json.loads(payload))
    return out


def reassembled_text(body: bytes, surface: str) -> str:
    """What a correct client ends up showing the user."""
    if surface == "anthropic":
        deltas = (o for o in data_objects(body) if o.get("type") == "content_block_delta")
        return "".join(o["delta"]["text"] for o in deltas)
    return "".join(
        c["delta"].get("content", "")
        for o in data_objects(body)
        for c in o.get("choices", [])
        if "delta" in c
    )


async def read_body(client: httpx.AsyncClient, url: str, **headers: str) -> bytes:
    r = await client.post(url, headers=headers, json={"stream": True})
    r.raise_for_status()
    return r.content


# ------------------------------------------------------------------ ok mode


@pytest.mark.parametrize(
    ("surface", "terminal"),
    [("openai", b"data: [DONE]"), ("anthropic", b"event: message_stop")],
)
async def test_ok_serves_the_canonical_wire_bytes_and_the_surfaces_own_terminal_marker(
    fakes: Fakes, client: httpx.AsyncClient, surface: str, terminal: bytes
):
    """The `ok` body must equal `fakes/wire.py` byte for byte.

    Not "parses to the same events" -- equal. The fakes and the gateway's
    parser tests share `wire`, and that sharing is what stops the contract
    tier from becoming two mocks agreeing with each other. A fake that
    reassembles the canonical frames in its own order has quietly severed
    that link while still looking green.
    """
    body = await read_body(client, fakes.url(surface), **{"X-Fake-Mode": "ok"})

    canonical = wire.joined(
        wire.anthropic_stream() if surface == "anthropic" else wire.openai_stream()
    )
    assert body == canonical
    assert terminal in frames(body)[-1]
    assert reassembled_text(body, surface) == wire.expected_text()


@pytest.mark.parametrize("surface", ["openai", "anthropic"])
async def test_ok_puts_usage_in_the_final_chunk_of_each_surface(
    fakes: Fakes, client: httpx.AsyncClient, surface: str
):
    """Usage arrives in a different place on each surface, which is why cost
    has a per-request basis at all: Anthropic reports input at `message_start`
    and output at `message_delta`, OpenAI reports both in one trailing chunk
    that only exists if the caller asked for it."""
    body = await read_body(client, fakes.url(surface), **{"X-Fake-Mode": "ok"})
    objs = data_objects(body)

    if surface == "anthropic":
        start = next(o for o in objs if o["type"] == "message_start")
        delta = next(o for o in objs if o["type"] == "message_delta")
        assert start["message"]["usage"]["input_tokens"] == wire.ANTHROPIC_INPUT_TOKENS
        assert start["message"]["usage"]["cache_read_input_tokens"] == wire.CACHE_READ_TOKENS
        assert delta["usage"]["output_tokens"] == wire.OUTPUT_TOKENS
    else:
        usage = next(o["usage"] for o in objs if "usage" in o)
        assert usage["prompt_tokens"] == wire.OPENAI_PROMPT_TOKENS
        assert usage["completion_tokens"] == wire.OUTPUT_TOKENS
        assert usage["prompt_tokens_details"]["cached_tokens"] == wire.CACHE_READ_TOKENS


async def test_ok_paces_events_at_the_requested_interval(
    fakes: Fakes, client: httpx.AsyncClient
):
    """A configurable interval is not a nicety: a stream that arrives in one
    write cannot exercise an inter-event stall clock, and a passthrough test
    that measures per-event arrival needs the events to arrive apart."""
    started = time.monotonic()
    body = await read_body(
        client,
        fakes.url("openai"),
        **{"X-Fake-Mode": "ok", "X-Fake-Events": "4", "X-Fake-Interval": "0.05"},
    )
    elapsed = time.monotonic() - started
    assert elapsed >= 4 * 0.05
    assert body.endswith(b"data: [DONE]\n\n")


# ---------------------------------------------------------------- split-frames


@pytest.mark.parametrize("surface", ["openai", "anthropic"])
async def test_split_frames_delivers_bytes_identical_to_ok_in_many_more_writes(
    fakes: Fakes, client: httpx.AsyncClient, surface: str
):
    """The invariance the gateway's parser must have, stated on real sockets.

    Two assertions, and the second is the one that is easy to forget. Equal
    bytes alone would also pass if `split-frames` quietly did one write, so the
    test also asks the upstream how many ASGI writes it made. The client
    genuinely cannot answer that -- httpx re-assembles, the kernel coalesces --
    which is the counters' first appearance as load-bearing rather than
    decorative.
    """
    url = fakes.url(surface)
    plain = await read_body(client, url, **{"X-Fake-Mode": "ok"})
    split = await read_body(
        client, url, **{"X-Fake-Mode": "split-frames", "X-Fake-Seed": "1729"}
    )

    assert split == plain

    writes = fakes.stats()["writes_by_mode"]
    assert writes["ok"] == len(frames(plain))
    assert writes["split-frames"] > 4 * writes["ok"], (
        f"split-frames made {writes['split-frames']} writes for "
        f"{writes['ok']} frames -- it did not really split"
    )


def test_split_frames_always_cuts_inside_a_multibyte_character_and_a_delimiter():
    """Randomness is not allowed to decide whether the hard case is covered.

    Random offsets over a 1 KiB body hit a UTF-8 continuation byte and a
    delimiter pair *most* of the time, and "most of the time" is how you get a
    suite that is green in CI and red during the demo. Both boundaries are
    therefore forced, for every seed.
    """
    payload = wire.joined(fake.ok_frames("anthropic"))
    for seed in range(64):
        writes = fake.split_writes(payload, seed=seed, crlf=False)
        assert b"".join(writes) == payload, f"seed {seed} lost bytes"

        boundaries = set()
        at = 0
        for w in writes[:-1]:
            at += len(w)
            boundaries.add(at)

        mid_char = {i for i in boundaries if 0x80 <= payload[i] < 0xC0}
        mid_delim = {i for i in boundaries if payload[i - 1 : i + 1] == b"\n\n"}
        assert mid_char, f"seed {seed} never split a multi-byte character"
        assert mid_delim, f"seed {seed} never split a frame delimiter"


async def test_split_frames_with_crlf_line_endings_still_reassembles_exactly(
    fakes: Fakes, client: httpx.AsyncClient
):
    """`wire` is LF-only because that is what the providers send, so CRLF is
    opt-in. It exists because the SSE grammar permits CRLF, and a split that
    lands between the CR and the LF of a frame delimiter is precisely the case
    that defeats a scanner matching on the tail of each chunk."""
    url = fakes.url("openai")
    plain = await read_body(client, url, **{"X-Fake-Mode": "ok"})
    crlf = await read_body(
        client, url, **{"X-Fake-Mode": "split-frames", "X-Fake-CRLF": "1"}
    )
    assert crlf == plain.replace(b"\n", b"\r\n")
    assert crlf != plain


# ---------------------------------------------------------------- die-mid-stream


@pytest.mark.parametrize(
    ("surface", "terminal"),
    [("openai", b"[DONE]"), ("anthropic", b"message_stop")],
)
async def test_die_mid_stream_truncates_the_body_with_no_terminal_marker(
    fakes: Fakes, client: httpx.AsyncClient, surface: str, terminal: bytes
):
    """The mechanism, verified rather than assumed.

    The fake raises inside the `StreamingResponse` body generator. Uvicorn
    catches it in `run_asgi`, sees the response has already started, and calls
    `transport.close()` -- it never sends h11's `EndOfMessage`, so the
    chunked encoding's terminating `0\\r\\n\\r\\n` never goes out. httpx reads
    a body that ends before it was told it would and raises
    `RemoteProtocolError`. That exception IS the assertion: a clean short read
    would mean the fake was politely finishing, which is a different mode.

    Limitation, stated where someone will read it: this is a FIN after the
    queued bytes flush, not a TCP RST. A provider whose process dies can give
    the client an ECONNRESET instead, which is a different exception on a
    different code path. ASGI gives no way to send a reset, so the gateway's
    handling of that variant is not covered here.
    """
    received: list[bytes] = []
    with pytest.raises(httpx.RemoteProtocolError):
        async with client.stream(
            "POST",
            fakes.url(surface),
            headers={"X-Fake-Mode": "die-mid-stream", "X-Fake-Events": "3"},
            json={"stream": True},
        ) as r:
            assert r.status_code == 200
            async for chunk in r.aiter_bytes():
                received.append(chunk)

    body = b"".join(received)
    assert terminal not in body

    objs = data_objects(body)
    if surface == "anthropic":
        deltas = [o for o in objs if o["type"] == "content_block_delta"]
        assert [o["type"] for o in objs[:2]] == ["message_start", "content_block_start"]
    else:
        deltas = [o for o in objs if o.get("choices")]
    assert len(deltas) == 3, "the client must see exactly the K events that were sent"


# ---------------------------------------------------------------- the stalls


async def test_stall_before_headers_sends_no_response_at_all(
    fakes: Fakes, client: httpx.AsyncClient
):
    """Nothing comes back -- not even a status line.

    Limitation worth naming: uvicorn has already accepted the TCP connection
    and parsed the request line before our handler runs, so this stalls a
    time-to-first-byte clock, not a connect clock. A real connect stall needs
    a listener that never calls accept().
    """
    started = time.monotonic()
    with pytest.raises(httpx.ReadTimeout):
        await client.post(
            fakes.url("openai"),
            headers={"X-Fake-Mode": "stall-before-headers", "X-Fake-Delay": "5"},
            json={"stream": True},
            timeout=QUICK,
        )
    assert time.monotonic() - started < 2.0, "the client, not the upstream, ended this"


async def test_stall_after_headers_flushes_a_200_and_then_says_nothing(
    fakes: Fakes, client: httpx.AsyncClient
):
    """Headers out, body silent -- the case that splits the commitment rule.

    The gateway has upstream headers but has written nothing to its own
    client, so by CONTRACTS.md #1 it may still fall back. That is the
    contested case, which is exactly why there is a mode for it.
    """
    async with client.stream(
        "POST",
        fakes.url("anthropic"),
        headers={"X-Fake-Mode": "stall-after-headers", "X-Fake-Delay": "5"},
        json={"stream": True},
        timeout=QUICK,
    ) as r:
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        with pytest.raises(httpx.ReadTimeout):
            async for _ in r.aiter_bytes():
                pytest.fail("stall-after-headers must not emit a body")


async def test_stall_mid_stream_delivers_k_events_and_then_goes_quiet(
    fakes: Fakes, client: httpx.AsyncClient
):
    """Two events, then silence: the inter-event stall clock's whole reason to
    exist. A 60 s total deadline with no stall clock holds this socket for 58
    seconds after the upstream stopped saying anything."""
    seen = bytearray()
    async with client.stream(
        "POST",
        fakes.url("openai"),
        headers={
            "X-Fake-Mode": "stall-mid-stream",
            "X-Fake-Events": "2",
            "X-Fake-Delay": "5",
        },
        json={"stream": True},
        timeout=QUICK,
    ) as r:
        assert r.status_code == 200
        with pytest.raises(httpx.ReadTimeout):
            async for chunk in r.aiter_bytes():
                seen += chunk

    assert len(data_objects(bytes(seen))) == 2
    assert b"[DONE]" not in seen


async def test_ping_forever_sends_heartbeats_and_never_a_content_event(
    fakes: Fakes, client: httpx.AsyncClient
):
    """Liveness is not progress.

    Every frame here resets a liveness clock and none of them resets a
    progress clock, which is the distinction `clocks.py` splits into two
    budgets. An upstream stuck in a bad state can heartbeat politely forever;
    a gateway that treats any byte as progress will hold the stream open until
    the total deadline and bill the customer for the privilege.
    """
    for surface in ("openai", "anthropic"):
        seen = bytearray()
        async with client.stream(
            "POST",
            fakes.url(surface),
            headers={"X-Fake-Mode": "ping-forever", "X-Fake-Interval": "0.01"},
            json={"stream": True},
            timeout=QUICK,
        ) as r:
            assert r.status_code == 200
            async for chunk in r.aiter_bytes():
                seen += chunk
                if len(frames(bytes(seen))) >= 6:
                    break

        assert reassembled_text(bytes(seen), surface) == ""
        objs = data_objects(bytes(seen))
        if surface == "anthropic":
            assert objs[0]["type"] == "message_start"
            assert all(o["type"] == "ping" for o in objs[1:])
        else:
            assert all(o["choices"] == [] for o in objs)


# ---------------------------------------------------------------- status codes


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_5xx_serves_the_requested_status_with_a_json_error_body(
    fakes: Fakes, client: httpx.AsyncClient, status: int
):
    """Four distinct statuses, because a gateway that collapses them into
    `is_5xx` cannot tell "the model exploded" (500, retry elsewhere) from
    "the load balancer timed out" (504, the request may still be running)."""
    r = await client.post(
        fakes.url("openai"),
        headers={"X-Fake-Mode": "5xx", "X-Fake-Status": str(status)},
        json={"stream": True},
    )
    assert r.status_code == status
    assert r.json()["error"]["type"] == "server_error"


async def test_429_carries_a_parseable_retry_after_and_ratelimit_headers(
    fakes: Fakes, client: httpx.AsyncClient
):
    """`Retry-After` is a floor, not a suggestion, so it has to be parseable
    as an integer count of seconds -- and the remaining-quota headers have to
    be present, because the per-key limiter reads them rather than guessing."""
    r = await client.post(
        fakes.url("anthropic"),
        headers={"X-Fake-Mode": "429", "X-Fake-Delay": "7"},
        json={"stream": True},
    )
    assert r.status_code == 429
    assert int(r.headers["retry-after"]) == 7
    assert r.headers["x-ratelimit-remaining-requests"] == "0"
    assert r.headers["x-ratelimit-remaining-tokens"] == "0"
    assert r.json()["error"]["type"] == "rate_limit_error"


async def test_529_serves_an_overloaded_error(fakes: Fakes, client: httpx.AsyncClient):
    """529 is Anthropic's, and it is not in any RFC -- which is the point.
    A gateway whose classifier switches on a hardcoded tuple of known statuses
    silently mis-files it as a permanent failure."""
    r = await client.post(
        fakes.url("anthropic"), headers={"X-Fake-Mode": "529"}, json={"stream": True}
    )
    assert r.status_code == 529
    assert r.json()["error"]["type"] == "overloaded_error"


async def test_schema_400_is_an_invalid_request_error(
    fakes: Fakes, client: httpx.AsyncClient
):
    """The class that is `try_next` but not `retry_same`: this candidate has
    made up its mind about the body, but the incumbent may not have."""
    r = await client.post(
        fakes.url("openai"), headers={"X-Fake-Mode": "schema-400"}, json={"stream": True}
    )
    assert r.status_code == 400
    assert r.json()["error"]["type"] == "invalid_request_error"


# ---------------------------------------------------------------- payload shapes


async def test_error_in_stream_delivers_the_error_frame_after_k_good_events(
    fakes: Fakes, client: httpx.AsyncClient
):
    """HTTP said 200; the protocol said otherwise.

    This is the shape that turns an SLO green while users see a broken answer,
    which is why the taxonomy has `interrupted` as a distinct outcome from
    both `completed` and `failed`.
    """
    body = await read_body(
        client,
        fakes.url("anthropic"),
        **{"X-Fake-Mode": "error-in-stream", "X-Fake-Events": "2"},
    )
    assert b"event: error" in body
    assert b"message_stop" not in body
    objs = data_objects(body)
    assert len([o for o in objs if o["type"] == "content_block_delta"]) == 2
    assert objs[-1]["type"] == "error"
    assert objs[-1]["error"]["type"] == "overloaded_error"

    body = await read_body(
        client,
        fakes.url("openai"),
        **{"X-Fake-Mode": "error-in-stream", "X-Fake-Events": "2"},
    )
    assert b"[DONE]" not in body
    objs = data_objects(body)
    assert len(objs) == 3
    assert objs[-1]["error"]["type"] == "server_error"


async def test_huge_event_delivers_one_data_line_of_exactly_the_requested_size(
    fakes: Fakes, client: httpx.AsyncClient
):
    """One frame the parser cannot skip past without buffering all of it.

    The default is 8 MiB and the test uses the default deliberately: the
    number in the docs is the number on the wire. This is the mode that proves
    a gateway's buffers are bounded in *bytes*. Bound them by message count
    and 200 concurrent streams times one buffered 8 MiB frame is an OOM while
    the queue-depth metric reads a comfortable 200.
    """
    body = await read_body(client, fakes.url("openai"), **{"X-Fake-Mode": "huge-event"})
    default_bytes = 8 * 1024 * 1024

    head = body[:default_bytes]
    assert head.startswith(b"data: ")
    assert head.endswith(b"\n\n")
    assert head.count(b"\n") == 2, "the payload must be one unbroken data: line"
    assert body[default_bytes:] == wire.openai_done()

    smaller = await read_body(
        client,
        fakes.url("anthropic"),
        **{"X-Fake-Mode": "huge-event", "X-Fake-Bytes": str(1 << 20)},
    )
    huge = frames(smaller)[2]
    assert len(huge) + len(b"\n\n") == 1 << 20


async def test_huge_event_rejects_a_size_smaller_than_the_frame_envelope(
    fakes: Fakes, client: httpx.AsyncClient
):
    """A 400 up front, never a truncated 200. If the fake failed inside the
    body generator it would look exactly like `die-mid-stream`, and a test
    that was calibrating one mode would silently be exercising another."""
    r = await client.post(
        fakes.url("openai"),
        headers={"X-Fake-Mode": "huge-event", "X-Fake-Bytes": "10"},
        json={"stream": True},
    )
    assert r.status_code == 400
    assert r.json()["error"]["type"] == "fake_upstream_misuse"


async def test_slow_drip_holds_a_stream_open_across_several_intervals(
    fakes: Fakes, client: httpx.AsyncClient
):
    """The long-stream mode, run short. The real scenario is one small event
    every 2 s for 5 minutes; what makes it a test rather than a wait is that
    the interval and the count are both parameters."""
    started = time.monotonic()
    body = await read_body(
        client,
        fakes.url("anthropic"),
        **{"X-Fake-Mode": "slow-drip", "X-Fake-Interval": "0.05", "X-Fake-Events": "6"},
    )
    assert time.monotonic() - started >= 6 * 0.05
    assert b"message_stop" in body
    assert len([o for o in data_objects(body) if o["type"] == "content_block_delta"]) == 6


async def test_an_unknown_mode_is_a_400_that_names_the_header(
    fakes: Fakes, client: httpx.AsyncClient
):
    """A typo must fail loudly. Falling back to `ok` would give a green test
    that proved nothing, which is strictly worse than a red one."""
    r = await client.post(
        fakes.url("openai"), headers={"X-Fake-Mode": "ok-ish"}, json={"stream": True}
    )
    assert r.status_code == 400
    assert "X-Fake-Mode" in r.json()["error"]["message"]
    assert fakes.stats()["total"] == 0, "a rejected request is not an upstream call"


# ---------------------------------------------------------------- the counters


async def test_stats_count_requests_per_mode_and_per_path_and_reset_clears_them(
    fakes: Fakes, client: httpx.AsyncClient
):
    """The counters exist for one assertion the client cannot make.

    "The incumbent was never opened" is a statement about the upstream. A
    client holding a 200 from the candidate cannot distinguish a gateway that
    never tried the incumbent from one that tried it, got a 200 and threw the
    answer away -- and those are a correct gateway and a gateway that
    double-bills every request. Only the upstream knows, so it counts.
    """
    await read_body(client, fakes.url("openai"), **{"X-Fake-Mode": "ok"})
    await read_body(client, fakes.url("openai"), **{"X-Fake-Mode": "ok"})
    await client.post(
        fakes.url("anthropic"), headers={"X-Fake-Mode": "529"}, json={"stream": True}
    )

    s = fakes.stats()
    assert s["total"] == 3
    assert s["by_mode"] == {"ok": 2, "529": 1}
    assert s["by_path"] == {"/v1/chat/completions": 2, "/v1/messages": 1}
    assert s["open_streams"] == 0
    assert s["peak_open_streams"] >= 1
    # Reading the counter must not change it.
    assert fakes.stats()["total"] == 3

    fakes.reset_stats()
    cleared = fakes.stats()
    assert cleared["total"] == 0
    assert cleared["by_mode"] == {}
    assert cleared["peak_open_streams"] == 0


async def test_stats_report_a_stream_as_open_while_it_is_still_in_flight(
    fakes: Fakes, client: httpx.AsyncClient
):
    """`open_streams` is how the chaos tier asserts that no upstream
    connection outlived its request. It has to be observable mid-flight or it
    can only ever report zero."""
    async with client.stream(
        "POST",
        fakes.url("openai"),
        headers={"X-Fake-Mode": "stall-mid-stream", "X-Fake-Events": "1", "X-Fake-Delay": "5"},
        json={"stream": True},
        timeout=QUICK,
    ) as r:
        assert r.status_code == 200
        with pytest.raises(httpx.ReadTimeout):
            async for _ in r.aiter_bytes():
                assert fakes.stats()["open_streams"] == 1

    # The client hung up; Starlette cancels the body generator, whose finally
    # decrements the gauge. Poll rather than sleep a fixed amount: the whole
    # file's budget is spent on assertions, not on waiting.
    for _ in range(100):
        if fakes.stats()["open_streams"] == 0:
            break
        await asyncio.sleep(0.02)
    assert fakes.stats()["open_streams"] == 0


async def test_both_ports_share_one_counter_view(fakes: Fakes, client: httpx.AsyncClient):
    """Process-global on purpose. A fallback test asks one question of the
    whole fleet instead of reconciling two independent views of it."""
    await read_body(client, fakes.url("anthropic"), **{"X-Fake-Mode": "ok"})
    from_openai_port = (await client.get(f"{fakes.openai.base_url}/__stats")).json()
    from_anthropic_port = (await client.get(f"{fakes.anthropic.base_url}/__stats")).json()
    assert from_openai_port == from_anthropic_port
    assert from_openai_port["by_path"] == {"/v1/messages": 1}

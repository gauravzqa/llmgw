"""Walk a real provider stream and print what each layer made of it.

    make trace

This is the smallest thing that exercises every layer, over a real
socket, in the real order:

    fake upstream -> TCP -> httpx -> SSEParser -> Surface -> Usage

It exists because the two test tiers each prove half of that and neither
proves the join. `tests/unit/test_sse.py` and `test_surfaces.py` drive the
parser and the surfaces from `fakes/wire.py` byte literals -- no socket.
`tests/contract/test_fakes.py` drives real sockets but asserts on the raw
bytes the fake emitted -- no parser. Both suites can be green while the
seam between them is broken, so this script walks the seam.

Read the output as five lessons rather than five test cases:

  ok            multi-byte text survives; usage is exact on both halves
  split-frames  identical text under randomized byte splits, ON A SOCKET
  anthropic ok  envelope frames are META; cache_write only exists here
  ping-forever  a stream that is alive and producing nothing -- C7 in one line
  die-mid-stream a truncated stream: exact input, estimated output, C3 in one line
"""

from __future__ import annotations

import asyncio
import sys

import httpx
from fakes.upstream import PATHS, RunningServer, Surface, build_app, serve_in_thread

from llmgw.sse import SSEParser
from llmgw.surfaces import SURFACES
from llmgw.surfaces.base import Usage

_SURFACE_FOR = {"openai": "openai_chat", "anthropic": "anthropic_messages"}


async def trace(key: Surface, server: RunningServer, mode: str,
                headers: dict[str, str] | None = None):
    """Stream one request and fold every frame through its surface."""
    surface = SURFACES[_SURFACE_FOR[key]]
    parser, usage = SSEParser(), Usage()
    text: list[str] = []
    kinds: list[str] = []
    url = f"{server.base_url}{PATHS[key]}"
    request_headers = {"X-Fake-Mode": mode, **(headers or {})}

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            async with client.stream(
                "POST", url,
                json={"model": "fake-echo", "stream": True},
                headers=request_headers,
            ) as response:
                async for chunk in response.aiter_bytes():
                    # feed() takes whatever the network gave us. The chunk
                    # boundaries here are real, not chosen -- which is the
                    # entire point of running this over a socket.
                    for event in parser.feed(chunk):
                        kinds.append(surface.classify(event).value[:4])
                        surface.apply_usage(event, usage)
                        if (delta := surface.text_delta(event)):
                            text.append(delta)
        except httpx.HTTPError as exc:
            # A truncated body is a normal outcome here, not a script bug.
            kinds.append(f"!{type(exc).__name__}")
    for event in parser.close():
        kinds.append(surface.classify(event).value[:4])
    return "".join(text), kinds, usage


SCENARIOS: tuple[tuple[str, Surface, str, dict[str, str] | None], ...] = (
    ("openai    ok          ", "openai", "ok", None),
    ("openai    split-frames", "openai", "split-frames", None),
    ("anthropic ok          ", "anthropic", "ok", None),
    ("anthropic ping-forever", "anthropic", "ping-forever",
     {"X-Fake-Interval": "0.01", "X-Fake-Events": "6"}),
    ("anthropic die-mid     ", "anthropic", "die-mid-stream", {"X-Fake-Events": "3"}),
)


async def main() -> int:
    openai = serve_in_thread(build_app("openai"))
    anthropic = serve_in_thread(build_app("anthropic"))
    servers = {"openai": openai, "anthropic": anthropic}
    try:
        for label, key, mode, headers in SCENARIOS:
            text, kinds, usage = await trace(key, servers[key], mode, headers)
            shown = " ".join(kinds[:14]) + (" ..." if len(kinds) > 14 else "")
            print(f"{label}  text={text!r}")
            print(f"{'':22}  kinds={shown}")
            print(
                f"{'':22}  usage in={usage.input_tokens} cache_r={usage.cache_read_tokens} "
                f"cache_w={usage.cache_write_tokens} out={usage.output_tokens}  "
                f"input_exact={usage.input_exact} output_exact={usage.output_exact} "
                f"billable_exact={usage.exact}"
            )
            print()
    finally:
        openai.stop()
        anthropic.stop()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

"""`python -m llmgw.server`: run the gateway with graceful drain on SIGTERM.

This is the entry the Makefile `run` target points at, and the one a
production unit file would exec. It is deliberately thin -- build the config
from the environment and hand off to `lifecycle.run` -- because every knob,
the drain grace included, is an `LLMGW_*` variable, and a runner that parsed
its own flags would be a second place for the deployment contract to drift.

The importable ASGI app (`llmgw.server.app:app`) is NOT this path: a plain
`uvicorn llmgw.server.app:app` still works and is what the contract and fakes
harness import, but it gets uvicorn's default cut-on-signal behaviour. Drain
on SIGTERM is a property of running through THIS module.
"""

from __future__ import annotations

import logging

from llmgw.server.config import ServerConfig
from llmgw.server.lifecycle import run

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run(ServerConfig.from_env())

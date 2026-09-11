"""Live-provider harness: the only code in this repo that talks to the internet.

Everything under `src/llmgw/` has, until now, only ever been pointed at
`fakes/upstream.py` -- a local, plaintext, zero-latency, perfectly-behaved
liar that we wrote ourselves. That rig proves control flow. It cannot prove
that the catalog's model ids exist, that a provider's 404 body classifies the
way `errors.from_http_status` assumes, or that TLS and DNS fit inside a 2 s
connect budget.

This package is deliberately OUTSIDE `src/llmgw`. Nothing the gateway ships
imports it, so a live harness that reads a secrets file on disk can never
end up on a production import path.

Three entry points, in the order you should run them:

    python -m live.probe    free. lists what each provider actually serves
                            and reconciles it against catalog.MODELS
    python -m live.smoke    spends a few cents. real end-to-end streaming
                            and fallback through build_app()
    pytest tests/live -m live   with LLMGW_LIVE=1; a handful of tiny requests

"""

from __future__ import annotations

__all__ = ["env", "probe", "smoke"]

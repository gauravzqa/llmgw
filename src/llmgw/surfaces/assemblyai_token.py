"""`GET /assemblyai/v3/token`: a temporary streaming-STT token, capped (E2).

AssemblyAI's streaming STT is a WebSocket the gateway does not carry (PLAN-2
G). What it can carry is the credential a browser needs to open that socket
without ever holding the account key: `GET https://streaming.assemblyai.com/
v3/token?expires_in_seconds=N&max_session_duration_seconds=M` answers a
one-time token. The gateway forwards the query with two numbers clamped --
`expires_in_seconds` at 600 (the provider's own maximum) and
`max_session_duration_seconds` at the deployment's drain grace, because a
session longer than the grace is a session a deploy will cut, and the cap
protects the provider bill for a socket nobody is listening to. The mint
counts against `max_sessions` for the session length it authorised.

Auth upstream is the provider row's `raw` scheme (`Authorization: <key>`,
no `Bearer`), on a row whose base is `streaming.assemblyai.com`.
"""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import parse_qsl, urlencode

from llmgw.surfaces.base import BufferedSurface, RequestFacts, as_int

FIXED_MODEL = "assemblyai.streaming"
"""Catalog id the mint routes to: a row on the `assemblyai-streaming`
provider (base `https://streaming.assemblyai.com`). No tokens are ever
billed against it; it exists so the request has one target and one
credential like every other."""

MAX_TOKEN_TTL_S = 600
MIN_SESSION_S = 60
MAX_SESSION_S = 10_800


def capped_query(query: str, *, grace_s: float) -> tuple[str, float]:
    """The forwarded query string with the two numbers clamped, and the
    session length reserved against `max_sessions`. Unknown keys pass."""
    params = dict(parse_qsl(query, keep_blank_values=False))
    expires = as_int(params.get("expires_in_seconds")) or MAX_TOKEN_TTL_S
    expires = max(1, min(expires, MAX_TOKEN_TTL_S))
    asked = as_int(params.get("max_session_duration_seconds"))
    ceiling = int(max(MIN_SESSION_S, min(grace_s, MAX_SESSION_S)))
    session = ceiling if asked is None else max(MIN_SESSION_S, min(asked, ceiling))
    params["expires_in_seconds"] = str(expires)
    params["max_session_duration_seconds"] = str(session)
    return urlencode(params), float(session)


class AssemblyAITokenSurface(BufferedSurface):
    name = "assemblyai_token"
    dialect = "openai"  # the client route carries no dialect; OpenAI-shaped errors
    routes = ("/assemblyai/v3/token",)
    upstream_path = "/v3/token"
    methods = ("GET",)
    forward_query = True
    body = "raw"
    model_key = None
    fixed_model = FIXED_MODEL
    accounts = False
    mint_route = "/assemblyai/v3/token"

    def parse_request(self, body: bytes) -> RequestFacts:
        return RequestFacts(model=FIXED_MODEL, stream=False)

    def prepare_query(self, query: str, *, grace_s: float) -> tuple[str, float]:
        return capped_query(query, grace_s=grace_s)

    def safety_headers(self, tenant: str) -> Mapping[str, str]:
        return {}


ASSEMBLYAI_TOKEN = AssemblyAITokenSurface()

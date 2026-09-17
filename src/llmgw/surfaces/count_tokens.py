"""`POST /anthropic/v1/messages/count_tokens`: a buffered passthrough that
bills nothing (Phase C2).

The provider counts the tokens of a Messages body without running it. The
gateway routes the body to the Anthropic-dialect provider of the model it
names, rewrites `model` to the wire id as it does for a real call, returns
the JSON answer unchanged, and records the request with zero units: a token
count is free at every provider that offers one, and a record that priced
it would be a record inventing a charge. Its own cap in the surface-limits
table (4 MiB) because a count request is a prompt, not a 32 MiB PDF.
"""

from __future__ import annotations

from llmgw.surfaces.base import BufferedSurface


class CountTokensSurface(BufferedSurface):
    name = "count_tokens"
    dialect = "anthropic"
    routes = ("/anthropic/v1/messages/count_tokens",)
    upstream_path = "/v1/messages/count_tokens"
    methods = ("POST",)
    model_key = "model"
    accounts = False


COUNT_TOKENS = CountTokensSurface()

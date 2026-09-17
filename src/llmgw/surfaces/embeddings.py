"""`POST /v1/embeddings`: buffered JSON, input tokens only (Phase C3).

Same routing as chat: the body's `model` is a catalog id (or an alias),
rewritten to the wire id before it leaves. Usage is the response's
`usage.prompt_tokens`, exact when present; there are no output tokens, so the
record bills `input_per_m` alone and `unit="tokens"`. The `float`/`base64`
`encoding_format` and `dimensions` knobs are the client's and pass through
untouched.
"""

from __future__ import annotations

from llmgw.surfaces.base import BufferedSurface


class EmbeddingsSurface(BufferedSurface):
    name = "embeddings"
    dialect = "openai"
    routes = ("/v1/embeddings",)
    upstream_path = "/v1/embeddings"
    methods = ("POST",)
    model_key = "model"
    accounts = True


EMBEDDINGS = EmbeddingsSurface()

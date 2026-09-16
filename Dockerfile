# llmgw -- streaming LLM gateway. The image Fly builds and runs.
#
# Two stages. The builder resolves nothing: it installs exactly what
# `uv.lock` says (`--frozen` fails the build if the lock and pyproject.toml
# disagree), so the dependency set in the container is byte-for-byte the one
# `make venv` gives a developer and the one the contract tests ran against.
# That matters more here than in most services: `server/lifecycle.py` depends
# on uvicorn 0.52 internals for the drain, and an image that resolved a
# different uvicorn would break the drain only in production.
#
# The runtime stage copies the finished virtualenv and the config files and
# nothing else: no uv, no compiler, no tests, no bench, no secrets (see
# .dockerignore -- `.env` never enters the build context at all).

# ── builder ────────────────────────────────────────────────────────────────
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

# Dependencies first, project second, so a source-only change reuses the
# dependency layer. `--no-install-project` installs everything BUT llmgw.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project --no-editable

# Now the package itself, installed (not editable) into the same venv so the
# runtime stage needs the venv and nothing under /app/src.
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# ── runtime ────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# curl is for HEALTHCHECK and for `fly ssh console -C "curl localhost:8080/healthz"`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --uid 10001 --shell /usr/sbin/nologin llmgw

WORKDIR /app

COPY --from=builder --chown=llmgw:llmgw /app/.venv /app/.venv
# Tenants and policy documents ship IN the image; the secrets they refer to
# (tenant tokens, provider keys) arrive as env vars and are named in the
# files by variable name only. See config/tenants.example.toml.
COPY --chown=llmgw:llmgw config/ /app/config/

# Recorded in the image so a shell on the machine can answer "which commit
# is this" (`echo $LLMGW_BUILD_SHA`). Deliberately not read by the app yet.
ARG GIT_SHA=unknown
ENV LLMGW_BUILD_SHA=${GIT_SHA}

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LLMGW_HOST=0.0.0.0 \
    LLMGW_PORT=8080
# Every other knob is an LLMGW_* variable set in fly.toml [env] or by
# `fly secrets`; the deployment contract lives there, not here.
# LLMGW_FAKE_UPSTREAMS defaults to false -- real upstreams in prod.

USER llmgw
EXPOSE 8080

# Fly reads [[http_service.checks]] from fly.toml, so this is for plain
# `docker run` and for anyone reading the image without the toml.
HEALTHCHECK --interval=10s --timeout=2s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

# `python -m llmgw.server` is the lifecycle runner: SIGTERM -> readiness 503
# -> finish in-flight streams (LLMGW_DRAIN_GRACE) -> exit. A bare
# `uvicorn llmgw.server.app:app` would cut every stream on the signal.
CMD ["python", "-m", "llmgw.server"]

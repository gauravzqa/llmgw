# llmgw. Every target is runnable on a clean checkout with `make <target>`.
.PHONY: help venv lock test unit contract chaos live probe smoke scale trace fakes run serve image deploy lint clean

VENV := .venv
PY   := $(VENV)/bin/python
PYTEST := $(VENV)/bin/pytest
GIT_SHA := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)

help:
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

# The venv is built FROM THE LOCK, not resolved afresh: `--frozen` refuses to
# run if uv.lock and pyproject.toml disagree, so a developer's venv, the
# Docker image and the contract tests all see the same dependency set. The
# pip fallback (no uv installed) resolves live and is therefore weaker; it
# exists so a clean machine can still run the tests.
$(VENV): pyproject.toml uv.lock    ## Create the venv from uv.lock (uv, falls back to pip)
	uv sync --frozen --extra dev --python 3.11 2>/dev/null || \
	  (python3 -m venv $(VENV) && $(PY) -m pip install -e '.[dev]')
	@touch $(VENV)

lock:                              ## Re-resolve uv.lock after editing pyproject.toml
	uv lock

venv: $(VENV)                      ## Same as above

test: unit                         ## Default loop: the fast tier only

unit: $(VENV)                      ## Tier 1: no sockets, no sleeps. Target < 3s
	$(PYTEST) tests/unit -q

contract: $(VENV)                  ## Tier 2: real sockets against fake upstreams
	$(PYTEST) tests/contract -q -m contract

chaos: $(VENV)                     ## Tier 3: randomized faults, invariant asserts
	$(PYTEST) tests/chaos -q -m chaos

live: $(VENV)                      ## Tier 4: REAL providers. Costs real money
	LLMGW_LIVE=1 $(PYTEST) tests/live -q -m live

probe: $(VENV)                     ## Reconcile the catalog against live provider APIs (free)
	$(PY) -m live.probe

smoke: $(VENV)                     ## One real request per provider through the gateway
	$(PY) -m live.smoke

scale: $(VENV)                     ## Tier 5: S1-S8 load scenarios, four workers each. About an hour
	for s in S1 S2 S3 S4 S5 S6 S7 S8; do \
	  $(PY) -m bench.load --scenario $$s --gw-workers 4 --fake-workers 4 --repeats 1 || exit 1; \
	done

trace: $(VENV)                     ## Walk one real stream through every layer
	$(PY) -m bench.trace

fakes: $(VENV)                     ## Run the hostile upstreams standalone
	$(PY) -m fakes.upstream --openai-port 8801 --anthropic-port 8802

run: $(VENV)                       ## Run the gateway against the fakes (drains on SIGTERM)
	LLMGW_FAKE_UPSTREAMS=1 LLMGW_PORT=8800 $(PY) -m llmgw.server

serve: $(VENV)                     ## Run the gateway against real providers (drains on SIGTERM)
	LLMGW_PORT=8800 $(PY) -m llmgw.server

image:                             ## Build the container image locally (needs Docker)
	docker build --build-arg GIT_SHA=$(GIT_SHA) -t llmgw:$(GIT_SHA) -t llmgw:latest .

deploy:                            ## Deploy to Fly (see DEPLOY.md for first-time setup)
	fly deploy --build-arg GIT_SHA=$(GIT_SHA)

lint: $(VENV)
	$(VENV)/bin/ruff check src tests fakes bench

clean:
	rm -rf $(VENV) .pytest_cache **/__pycache__

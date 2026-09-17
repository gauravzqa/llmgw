# Deploying llmgw to Fly.io

This repository is the source of truth for the gateway and for its
deployment. Three files carry the deployment contract: `Dockerfile` (what
runs), `fly.toml` (where and with what knobs), and this file (the commands
and the arithmetic behind them). Every knob is an `LLMGW_*` environment
variable, so local (`make run`), the container and Fly all read the same
contract; only the values differ.

The app is **private-only**. It has no public IP and is reached by the sibling
apps over Fly's 6PN network. That single decision is what lets it ship without
TLS termination, without an auth layer in front of `/metrics` and `/probe`,
and without a WAF: nothing off-network can open a socket to it.

## Prerequisites

- `brew install flyctl`, then `fly auth login`. You need to be in the same Fly
  organisation as `layrs-tcg` (the callers must share a 6PN network).
- A credential file OUTSIDE this repository, pointed at by `LLMGW_ENV_FILE`,
  holding the provider keys. `.env.example` lists the names. It is read by
  variable reference below and never printed, copied or uploaded.
- `openssl` for minting tenant tokens.

## 1. Create the app, without deploying and without public IPs

```sh
fly launch --no-deploy --copy-config --no-public-ips --name layrs-llmgw --region sin
```

`--copy-config` keeps the committed `fly.toml` (the drain pairing, the
concurrency limits, the private-only comment block). `--no-public-ips` is the
private-only decision. Then give it a flycast address so the proxy path
(health checks, connection limits, rolling-deploy routing) exists:

```sh
fly ips allocate-v6 --private
fly ips list        # expect one "private" v6 and nothing public
```

Callers now have two addresses:

| address | path | when |
|---|---|---|
| `http://layrs-llmgw.flycast` | Fly proxy → machine | default for services: honours `[http_service.concurrency]` and health checks, routes around a draining machine |
| `http://layrs-llmgw.internal:8080` | direct to a machine | debugging, or a caller that wants the process's own `503 overloaded` rather than the proxy's queueing |

## 2. Secrets, by variable reference

Provider keys come from the credential file; the tenant token is minted here.
Nothing is echoed: the values travel as shell variables into `fly secrets`,
which stores them encrypted and injects them as env vars at boot.

```sh
set -a; . "$LLMGW_ENV_FILE"; set +a

LLMGW_TENANT_LAYRS_TOKEN="$(openssl rand -hex 32)"

fly secrets set --stage \
  ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY" \
  OPENAI_API_KEY="$OPENAI_API_KEY" \
  OPENROUTER_API_KEY="$OPENROUTER_API_KEY" \
  DEEPSEEK_API_KEY="$DEEPSEEK_API_KEY" \
  LLMGW_TENANT_LAYRS_TOKEN="$LLMGW_TENANT_LAYRS_TOKEN"

# Hand the tenant token to the caller the same way (never over chat):
fly secrets set --stage -a layrs-tcg LLMGW_TOKEN="$LLMGW_TENANT_LAYRS_TOKEN"
unset LLMGW_TENANT_LAYRS_TOKEN
```

`--stage` sets the secrets without restarting anything; the deploy in step 3
picks them up. On an app that is already running, drop `--stage` and Fly
performs a rolling restart, which goes through the drain like any deploy.

`config/tenants.toml` names the token by its **variable name**
(`LLMGW_TENANT_LAYRS_TOKEN`), never by value, which is what makes the file
committable and what ships it inside the image. Rotating a token is
`openssl rand`, `fly secrets set` on both apps, done; no rebuild. Only
provider keys named by the catalog and tokens named by the tenants file are
read; anything else set as a secret is ignored.

## 3. Deploy

```sh
make deploy            # = fly deploy --build-arg GIT_SHA=$(git rev-parse --short HEAD)
```

The SHA lands in `$LLMGW_BUILD_SHA` inside the machine. Watch it come up:

```sh
fly status
fly logs               # expect "tenant table loaded" and no "bearer tokens are not checked"
```

If the process refuses to start, `fly logs` shows why before the health check
fails: a `total > drain_grace` pair, a missing tenants file with
`LLMGW_REQUIRE_TENANTS=1`, a token variable named in the file but not set as
a secret. Those are all startup errors by design; nothing in that list can
fail on the request path.

## 4. Verify from inside the network

There is no public URL to curl. Two ways in:

```sh
# On the machine itself
fly ssh console -C "curl -s localhost:8080/healthz"
#   -> {"status":"ok","draining":false}

fly ssh console -C "curl -s localhost:8080/workloads/default/probe"
#   -> tenant_mode, max_streams, inflight, credential_present per target

# From a sibling app, the way a caller will see it
fly ssh console -a layrs-tcg -C "curl -s http://layrs-llmgw.flycast/healthz"
fly ssh console -a layrs-tcg -C "curl -s http://layrs-llmgw.internal:8080/healthz"
```

A first real request, with the tenant token read back from the caller's own
environment rather than typed:

```sh
fly ssh console -a layrs-tcg -C 'sh -c "curl -sN http://layrs-llmgw.flycast/v1/chat/completions \
  -H \"Authorization: Bearer \$LLMGW_TOKEN\" -H \"content-type: application/json\" \
  -d {\"model\":\"anthropic.haiku-4-5\",\"stream\":true,\"max_tokens\":16,\"messages\":[{\"role\":\"user\",\"content\":\"ping\"}]}"'
```

Expect `X-Gw-Tenant: layrs`, `X-Gw-Attempts: 1`, `X-Gw-Served-By`, and a
stream ending in `data: [DONE]`.

Metrics need no setup: `[metrics]` in `fly.toml` points Fly's own Prometheus
at `/metrics`, and the series appear at <https://fly-metrics.net> under the
app. The names to graph are in `src/llmgw/metrics.py`.

## 5. Roll back

```sh
fly releases                          # every deploy, newest first, with its image
fly deploy --image <image-from-the-list>
```

A rollback is a deploy, so it drains like one: the old machine gets SIGTERM,
finishes its streams inside the grace, and exits. Nothing is cut.

## The arithmetic that must stay true

```
LLMGW_BUDGET_TOTAL  <=  LLMGW_DRAIN_GRACE  <  grace + 3 s  <  kill_timeout
       120                   130                 133             140
```

- **total ≤ grace**: a stream is allowed to run for `total`; a deploy waits
  `grace` for it. If the grace were shorter, every deploy would cut the longest
  legitimate streams. `ServerConfig.validated()` refuses to start otherwise
  (override: `LLMGW_DRAIN_ALLOW_SHORT=1`, which logs the cut risk and is for
  benches, not for here).
- **+ 3 s**: after the grace, the process gives uvicorn a fixed 3 s to close
  whatever is left (`lifecycle.UVICORN_SHUTDOWN_TIMEOUT_S`). Without a bound
  uvicorn waits forever, which is the bug the 10 Sep S8 run found.
- **< kill_timeout**: Fly SIGKILLs at `kill_timeout`. If that fired first,
  streams would end by SIGKILL, not by the contract's native ending, and the
  drain would be theatre. Only `fly.toml` can hold this inequality; the process
  cannot see it.

Change one number, change all four, in both files.

## Re-deriving the cap on this VM

`LLMGW_MAX_STREAMS=150` was measured on a 16-core laptop: the largest cap at
which S2 shed before it degraded. A `shared-cpu-1x` is a fraction of one such
core, so 150 is an upper bound, not a setting. To measure it:

1. Deploy a second, throwaway app on the same VM size with
   `LLMGW_FAKE_UPSTREAMS=1` and the fakes running beside it (or point it at a
   fake app on the network).
2. From a machine in the same region, run the S2 shape against it at a few
   caps: `BENCH_GW_MAX_STREAMS=<n> python -m bench.load --scenario S2 ...`
   with the gateway URL overridden to the throwaway app.
3. Pick the largest cap at which the admitted streams' first-event p90 and
   inter-event p99 stay within a few ms of the direct arm and the 5xx mix is
   503 `overloaded` only, no 504. That is the number for `fly.toml`.

Until that is done, expect the real ceiling on `shared-cpu-1x` to be well
under 150 for fast-model streams; the cap is a stream count standing in for an
event rate.

## Deliberately not done here

- **No public IP.** Adding one later is `fly ips allocate-v4` plus putting
  authentication in front of `/metrics` and `/workloads/*/probe`, which today
  rely on the network boundary.
- **No CI.** Deploys are `make deploy` from a checkout whose
  `make test && make contract` are green. A GitHub Actions job that runs those
  on a pull request and `fly deploy --remote-only` on `main` is the obvious
  next step; the Layrs repository has no workflows yet, so this would be the
  first.
- **No capture sink.** `LLMGW_CAPTURE_PATH` is unset, so per-request capture
  records go to the `NullSink`; the machine's disk is ephemeral and a file
  there would vanish on every deploy. A volume or a Supabase sink is a
  separate change. Metrics and logs are unaffected.
- **Process-local limits.** Admission and breaker state live in each machine;
  N machines give a tenant N times its cap. Run one machine until a tenant
  needs a real fleet-wide cap, then add shared state.

## Budgets and caps added in Phase B

- `LLMGW_BUDGET_HEADERS` (10 s in `fly.toml`) bounds the wait for a provider's
  status line; `LLMGW_BUDGET_CONNECT` (2 s default) is TCP+TLS only. Keep
  `headers < first_event`.
- Per-surface caps: `LLMGW_MAX_REQUEST_BYTES__<SURFACE>` and
  `LLMGW_MAX_RESPONSE_BYTES__<SURFACE>` (surface name upper-cased, two
  underscores). Anthropic messages ship at 32 MiB requests because base64
  images and PDFs arrive at that size; chat is 32 MiB too since 18 Sep (the global default). A multipart or raw
  body is buffered up to its surface's cap so a pre-commit retry can resend
  it -- that cap is the per-request memory bound.
- The drain inequality is now checked against the largest total in the policy
  file, not only `LLMGW_BUDGET_TOTAL`: a `[profiles.long_context]` above the
  grace refuses to start unless `LLMGW_DRAIN_ALLOW_SHORT=1`.

## Token minting (Phase E)

`POST /v1/realtime/client_secrets` and `GET /assemblyai/v3/token` hand a
browser or device a short-lived provider credential without it ever holding
the account key. Two knobs, both in `config/tenants.toml`, shipped in the
image: `max_sessions` (credentials alive at once per tenant) and
`[tenants.<id>.realtime]` (the session fields pinned over the client's on
every mint). Every credential's lifetime is capped at `LLMGW_DRAIN_GRACE`;
raise the grace (and `kill_timeout`) knowingly if a tenant needs longer
sessions. The AssemblyAI mint needs a catalog row `assemblyai.streaming` on
the `assemblyai-streaming` provider; without it the route answers 400
`unknown model`.

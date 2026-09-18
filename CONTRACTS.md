# Contracts

Written before the code, enforced by the tests. The point is not that these
are the only defensible choices. It is that an *undefended* choice is the
failure — "what happens if the provider dies after 5 tokens?" has to have an
answer you decided on, not an answer that fell out of the implementation.

Each row names the test that enforces it. A contract with no test is a comment.

---

### C1 — Commitment is the first byte written to the client socket

Not the first byte received from upstream. Not the upstream's response
headers. The first byte *we successfully wrote to the client*.

A failure after upstream HTTP headers but **before** the first client byte
**may still fall back**. We have sent the client nothing, so nothing is
broken and nothing is inconsistent.

This is the contested case and it is stated explicitly for that reason. The
opposite choice — treating upstream headers as commitment — is also
defensible and costs you a fallback you could have had.

**Two boundaries.** At the HTTP layer there is an earlier, weaker line than
C1's:

| Boundary | When | What it locks |
|---|---|---|
| **Status commitment** | `http.response.start` sent | The status code and headers |
| **Content commitment** | first body byte written | Everything. This is C1. |

An earlier revision sent the status as soon as `Upstream.open()` returned, which put the window
"headers arrived, no body followed" *after* status commitment — recoverable by
this contract, unrecoverable in the code. **A later revision closed that gap**, and the two
boundaries now coincide, which has a pleasant second-order effect: the
executor needs one commitment flag rather than two that must agree.

*(A footnote worth keeping, because it is how a contract goes stale: an
earlier draft of this paragraph named `FirstEventTimeout` as the class in that
window. A later verification pass measured it — on the streaming path the
pump's progress clock fires first, so the class the code actually produced was
`StallTimeout`. `FirstEventTimeout` was unreachable there, an artifact of two
modules wrapping the same `await` with two budgets that neither author could
see. A contract naming a class the code cannot produce is a contract nobody
can test.)*

**Decision: the status is held until the upstream's first body byte.**

`http.response.start` is not sent when `Upstream.open()` returns. It is sent
when the upstream yields its first byte of body — the moment we are about to
write to the client anyway. Everything before that point is pre-commitment
and may fall back.

The alternatives, and why this one:

| Send status when | Fallback window | Cost |
|---|---|---|
| `open()` returns (P2) | before upstream headers only | forfeits the most common provider failure |
| **first upstream body byte** | **the full C1 window** | **the upstream's own time-to-first-byte, which we were going to wait for regardless** |
| first CONTENT event | same as above | adds the model's whole thinking time before the client learns the status |

The middle row is nearly free. We cannot write a client byte before the
upstream gives us one, so holding the status until then costs no additional
latency to first *content* — only to first *byte*, and those coincide.

What it does cost: a client sees no status at all while the upstream stalls,
for up to `budgets.first_event`. That is the right trade — under fallback the
client eventually gets a real answer from the incumbent instead of a
truncated 200 from a target that died — but it means `budgets.first_event`
must stay below whatever header timeout the callers and intermediaries in
front of us use. 20s is the default for a reason; raising it is a decision
about someone else's timeout, not just ours.

*Enforced by:* `test_a_candidate_that_dies_after_commitment_is_never_replaced`
and `test_a_candidate_that_answers_500_falls_back_to_the_incumbent` (unit and
contract tiers), `test_commitment_forbids_every_further_attempt` (every error
class), and `test_the_file_never_re_derives_the_commitment_invariant` — a grep
run as a test, banning `if committed` from `executor.py` entirely.

---

### C2 — Post-commitment failures end with the surface's native ending

We never fabricate a success and never invent an error the provider did not
send. What the client sees is what a direct connection to that provider would
have shown it:

| Surface | Ending on post-commitment failure |
|---|---|
| OpenAI chat completions | Body closes without `data: [DONE]` |
| Anthropic messages | Body closes without `message_stop`. We do **not** synthesize `event: error` |
| OpenAI responses | Forward `response.failed` if upstream sent it; otherwise close without `response.completed` |

Rationale: the caller's SDK already knows how to detect its own vendor's
truncated stream. A helpfully synthesized error is a *new* shape their error
handling has never seen, so being helpful here breaks them.

*Enforced by:* `native_ending_per_surface`, `error_in_stream_passthrough`.

---

### C3 — Interrupted usage is recorded as estimated, never as completed

A stream that dies mid-flight has no usage chunk. We record the tokens
counted so far, mark `cost_basis="estimated"`, and set the outcome to
`interrupted`.

Two failures this prevents: billing a customer an exact-looking number that
was inferred, and a 2xx-based SLO reporting a truncated answer as a success.
`llmgw_requests_total{outcome="interrupted"}` exists so that the second one
is visible.

*Enforced by:* `usage_parsed_and_estimated`,
`test_committed_failure_is_interrupted_not_failed`.

---

### C4 — Once no fallback remains, the upstream's status and body pass through unmodified

`X-Gw-Attempts` says how many targets were tried; `X-Gw-Served-By` says which
one answered. We do not improve on a provider's error message.

*Enforced by:* `test_client_status_prefers_the_upstream_status_when_passing_through`,
contract tier passthrough tests.

---

### C5 — Exactly one layer retries, and the caller picks it

`X-Gw-No-Retry: 1` disables our retries entirely so an outer gateway can own
them.

Layered retries multiply. Three layers each retrying three times is 27
requests to a provider that is failing *because* it is overloaded. The answer
to "who retries" is never "everyone, a bit"; it is one named layer, and the
header is how the caller names it.

*Enforced by:* `retry_after_is_floor`,
`test_a_disabled_retry_policy_buys_zero_retries_and_keeps_the_fallback`.

Note the second one: `X-Gw-No-Retry: 1` disables *repetition*, never
*fallback*. Collapsing those two would mean a caller that owns its own
retries silently loses the redundancy its workload was configured for.

---

### C6 — Denial is free

A request rejected by admission consumes no rate credit for a *concurrency*
denial and never opens an upstream connection.

Shedding load must cost less than serving it. A gateway whose rejection path
is expensive stops being overload protection and becomes the overload — and
charging a token to a request you refused means a client at its concurrency
limit also burns its rate budget doing nothing.

*Enforced by:* `tenant_isolation_under_saturation`, `probe_no_upstream_call`.

---

### C7 — Liveness is not progress

Heartbeats (Anthropic `ping`, OpenAI empty-`choices` chunks) reset the
*liveness* clock and never the *progress* clock. A provider stuck in a bad
state can heartbeat politely forever.

The opposite choice, treating any post-commitment frame as proof of life, is
also defensible. This build takes the stricter one.

*Enforced by:* `ping_does_not_reset_progress`,
`test_a_heartbeat_does_not_reset_progress`.

---

### C8 — Cancellation is not evidence about a provider

A client disconnect, a client that stopped reading, and a request cancelled
upstream of us are all `Health.NEUTRAL`. They never count toward a circuit
breaker.

Without this, any client-side incident opens breakers against perfectly
healthy providers — at exactly the moment you need those providers most.

*Enforced by:* `breaker_ignores_client_cancel`,
`test_client_faults_never_blame_the_provider`.

---

### C9 — The process sheds before it saturates

A request that would take the process over `max_streams` is refused at
ingress with 503 `overloaded` and `Retry-After: 1`: after the tenant is
resolved and before its bucket is touched, before the body is read, before
any upstream connection is opened. Streams already open are never affected.
`/healthz`, `/metrics` and `/probe` are outside the cap.

The load campaign's S2 measured the alternative: four processes went from
29 ms to 2.6 s first-event latency at p50 and only then began returning
504s. A proxy whose overload behaviour is "get slow, then fail" has made
every client's timeout its own load shedder, at the worst possible point in
the request. Refusing the (N+1)th stream is cheap and honest; serving it
badly is neither. The 503 says "another replica", not "later": a tenant shed
here is under its own limits and did nothing wrong.

*Enforced by:* `test_the_request_over_the_cap_is_shed_before_any_upstream_work`,
`test_a_slot_freed_by_a_finished_stream_admits_the_next_request`,
`test_shed_is_counted_under_overloaded_and_costs_no_tenant_credit`.

---

### C10 — In production, a bearer token names a tenant or the request is refused

With `LLMGW_REQUIRE_TENANTS=1` the process refuses to start on the
zero-config tenant path: no `LLMGW_TENANTS_FILE`, or a file whose only tenant
is `anonymous`, is a startup error naming the variable to set. Tokens are
resolved at startup from the environment (`token_env`), so the tenants file
carries ids and limits only and a missing secret is a deploy that fails,
never a tenant that quietly cannot authenticate. Once running, an unknown
token is a 401 and is never downgraded to guest access; `X-Gw-Tenant`
carries the id and nothing anywhere carries the token.

The zero-config path stays for `make run` + `curl`. It is loud (a startup
warning, `"tenant_mode": "anonymous"` in `/probe`) but loud is not the same
as refused, and a gateway with two callers on one bucket is FAILURE-MODES
row 6 with a green dashboard.

*Enforced by:* `test_require_tenants_without_a_file_refuses_to_start_naming_both_vars`,
`test_require_tenants_with_only_anonymous_in_the_file_is_refused`,
`test_an_unset_or_empty_token_env_refuses_to_load_and_names_the_variable`,
`test_repr_never_carries_a_token_from_either_source`.

---

### C11 — A provider's auth-failure body never reaches the client

The one exception to C4. When the terminal error is `AuthenticationFailed`
(upstream 401 or 403), the provider rejected the *gateway's* credential; the
client never supplied one and nothing in that body is the client's to read.
At least one provider's 401 quotes the tail of the key it rejected (findings
log #30), which under a shared key is part of a shared secret. So the
upstream status passes through and the body is replaced with the gateway's
own `{"error": {"type": "upstream_auth", ...}}` naming the provider.
Everything else -- classification, credential-scoped breaker, blame, the
capture record -- is exactly as before; only the wire changes. Every other
passthrough class is still forwarded byte for byte.

*Enforced by:* `test_an_upstream_401_body_is_replaced_and_the_status_kept`,
`test_a_non_auth_passthrough_body_is_still_the_providers_bytes`,
`test_a_provider_401_reaches_the_client_as_a_status_without_the_body` and
`test_a_provider_5xx_body_is_still_forwarded_byte_for_byte` (contract).

---

### C12 — The gateway answers in catalog names, and tells you the provider's request id

Two facts a client could not previously get back out of the gateway.

**The model it was served by, in the name the gateway issued.** Every
response with a served target carries `X-Gw-Model: <catalog id>`. On the
buffered (non-streaming) path, when the request body was rewritten to the
provider's wire id (`X-Gw-Body-Modified: 1`), the response body's top-level
`model` is rewritten back to the catalog id. Streaming bodies are never
rewritten: byte-for-byte passthrough holds, and the alias table in `policy`
makes the wire id the provider echoes acceptable on the next turn instead.
Before this, an SDK loop that re-sent the response's `model` got
`400 unknown model` from the gateway (live, 16 Sep 2026).

**The provider's own request id.** `X-Gw-Upstream-Request-Id` carries the
upstream's `x-request-id` / `request-id` / `x-inworld-request-id` on success
and on every error path, including the scrubbed auth path (C11): it is an
identifier, not credential material, and it is the one thing a support
ticket to the provider needs. The provider's rate-limit headers are still
never forwarded; they are read into per-credential gauges instead.

*Enforced by:* `test_x_gw_model_names_the_catalog_id_that_served`,
`test_x_gw_model_follows_the_fallback`,
`test_the_buffered_response_model_is_the_catalog_id_again`,
`test_the_streaming_body_is_not_rewritten`,
`test_a_billing_429_with_no_fallback_passes_through_with_the_upstream_request_id`
(contract); `test_send_error_carries_the_providers_request_id_and_scrubs_auth`,
`test_rewrite_response_model_puts_the_catalog_id_back_on_json_objects_only`
(unit).

---

### C13 — Out of money is not a rate limit, and a queue is not an outage

Two 429-shaped states the classifier now tells apart from a transient limit.

**Billing.** A 429 whose body carries an out-of-money code — OpenAI's
`insufficient_quota`, `credit_balance_exhausted`, the spend- and usage-limit
codes; Anthropic's `details.error_code: enforced_spend_limit_reached` — or
Anthropic's 400 "reached your specified API usage limits", classifies as
`InsufficientCredits`: never retried against the same target, eligible for
the next, NEUTRAL to the breaker, blamed on POLICY. Same disposition as the
402 shape finding 8 fixed, on two more providers.

**Queueing.** A first-event timeout that fires after the provider has shown
liveness (SSE comments, empty-choices frames, `ping`) is
`FirstEventTimeout(queued=True)`: NEUTRAL health, still `try_next`, still not
`retry_same`, and counted on `llmgw_queued_at_provider_total`. A provider
that holds a request in a queue for longer than the client's budget has cost
the client its budget, not lost its health.

*Enforced by:* `test_a_billing_429_falls_back_once_and_never_retries_the_same_target`,
`test_an_impatient_budget_falls_back_and_counts_the_queue_not_an_outage`
(contract); `test_out_of_money_as_a_429_is_insufficient_credits_not_rate_limited`,
`test_a_queued_first_event_timeout_is_neutral_but_still_falls_back` (unit).

### C14 — A body the surface cannot frame is refused before the status is committed

Every surface declares its framing: `sse` (both chat dialects), `jsonl`
(newline-delimited JSON, Inworld text-to-speech) or `raw` (opaque chunks,
binary audio). The pump frames the upstream body with the framer the surface
names; commitment, byte bounds, backpressure, the progress/liveness split and
native endings are unchanged. For `jsonl` the per-frame bound applies per
line; for `raw` the pump's buffer is the only bound.

An SSE surface whose upstream answers a streaming request with a
`content-type` that is not `text/event-stream` is refused as a 502
`unsupported_upstream_framing`, decided on the response headers before the
status is committed to the client: `retry_same=False`, `try_next=True`,
NEUTRAL to the breaker, blamed on the gateway, message naming the content
type. It is therefore a fallback candidate and never a truncated stream.
Before this contract such a body was fed to the SSE parser, produced no
frames, was copied to the client for as long as the first-event budget
allowed, and was recorded as a provider stall with nothing billed (measured
against Inworld on 16 Sep 2026). An absent content type is not evidence and
is let through as before.

*Enforced by:* `test_sse_framer_is_split_invariant`,
`test_jsonl_framer_is_split_invariant`, `test_raw_framer_frames_are_exactly_the_chunks`,
`test_a_jsonl_surface_commits_progresses_and_ends_on_close`,
`test_an_sse_surface_refuses_a_non_sse_content_type_before_any_byte` (unit);
`test_a_misframed_candidate_falls_back_to_the_incumbent_uncut`,
`test_a_misframed_only_target_is_a_502_naming_the_content_type` (contract).


---

### C15 — Units that are not tokens are exact when the provider said so, and priced at a real rate always

The catalog can now price three units (`ModelSpec.unit`): tokens,
characters (TTS, per million at `input_per_m`) and seconds (duration-billed
STT, at `per_minute`); and, inside token usage, the kinds providers price
separately: audio input/output, cached audio input, and Anthropic's 1-hour
cache write. Server-tool calls (`web_search_requests` and friends) are
priced per thousand from `tool_rates`.

The promises. **Exactness is per kind and comes from the provider**: a
count the provider stated is exact, a count the gateway inferred is
`estimated`, and `basis` on the record says which. **No kind is ever priced
at zero for want of a rate**: an audio token on a row with no audio rate is
priced at the text rate, a 1-hour write on a row with no 1-hour rate at the
5-minute rate, and every such fallback is written into `cost_notes` on the
record and the capture line, so a bill that is right by accident can be
told from one that is right on purpose. **Reasoning tokens are never priced
twice**: they are already inside output and are recorded for visibility.
**A tool call with no rate is counted and noted, not priced.**

*Enforced by:* `tests/unit/test_units.py` (every kind, every fallback, the
closed-set pins against `metrics.TOKEN_KINDS` / `metrics.UNITS`).

## C16. Request defaults are a body edit, and say so

A target may carry `request_defaults`; the gateway fills in only top-level
keys the client did not send (one-level merge for dict values, never for
lists, never over a key the client set to anything, `null` included), only on
JSON bodies, and reports the edit under the same `X-Gw-Body-Modified: 1` as
the model rewrite. The capture record lists the keys (`defaulted_keys`). A
multipart or raw body is never edited.

## C17. Caps are per surface, and a body the gateway cannot route is refused

`max_request_bytes` and `max_response_bytes` resolve per `Surface.name`
(`ServerConfig.limits_for`), falling back to the globals. A `multipart` body
must name its `model` in the first 64 KiB of form fields and a `raw` body in
`?model=` or `X-Gw-Model`; otherwise the request is a 400 before any upstream
is contacted. Non-JSON bodies are forwarded byte-for-byte with the client's
own `content-type` and are buffered up to the surface cap so a pre-commit
retry can resend them.

## C18. `/v1/models` is answered from the catalog and never calls upstream

`GET /v1/models` and `GET /anthropic/v1/models` list the catalog ids the
explicit-model path can route on that surface's dialect -- every non-fake
row whose provider speaks the dialect (fakes are listed only on a gateway
pointed at the fakes) -- with each row's wire id and declared aliases
alongside. No upstream connection is opened; the fake's request counter does
not move (`tests/contract/test_text_surfaces.py`). The tenant is resolved
exactly as on the serving path, so the listing is never an unauthenticated
view of the catalog. With a policy document the set is the same, because a
client that names a model gets that model (see `plan_for`).

## C19. A mint never widens the pinned session and never outlives the cap

`POST /v1/realtime/client_secrets` and `GET /assemblyai/v3/token` issue
short-lived provider credentials on the tenant's behalf. Three promises:

* the tenant's pinned fields (`[tenants.<id>.realtime]`: model, voice, tools,
  turn detection, `max_output_tokens`, instructions) are written OVER the
  client's `session`, never under it; the `model` is resolved through the
  policy like any request and sent upstream as the wire id;
* the credential's lifetime is `min(client ask, tenant cap, drain grace)`
  and never above the provider's own maximum (7,200 s OpenAI; 600 s token /
  10,800 s session AssemblyAI); a client that asks for nothing gets the cap,
  not the provider default;
* the mint is charged against the tenant's rate bucket AND its
  `max_sessions`: a reservation held for the credential's TTL and released
  only by the clock, because the session it authorises never transits the
  gateway. The (N+1)th live credential is a 429 whose `Retry-After` is the
  earliest expiry. The upstream request carries `OpenAI-Safety-Identifier:
  <tenant>`; the capture record is written with `kind="mint"`.

The provider's response is returned unchanged.

## C20. A close-ended stream ends by close, and the gateway never fabricates a frame

For surfaces framed `jsonl` or `raw` (Inworld NDJSON, ElevenLabs and OpenAI
binary audio, AssemblyAI's JSON answer) the provider has no terminator: the
connection closing IS the end. The pump treats a clean EOF as TERMINAL for
those framings unless an in-stream error was already seen; SSE keeps C2
exactly (`[DONE]` or the dialect's terminal event, or `IncompleteStream`).

When a close-ended stream is cut by the gateway (budget, drain, client gone)
the client sees the connection close and nothing else: no synthetic trailer,
no padding, no JSON error appended to an audio body. A frame the provider did
not send is a lie in the client's audio buffer.

**Stated cost:** a jsonl or raw stream that a middlebox closes cleanly is
indistinguishable from a complete one, and is billed as complete on whatever
meter had arrived (Inworld and ElevenLabs deliver theirs before the first
audio byte; OpenAI binary TTS is estimated from the bytes forwarded, which is
also what the client received). `terminal_seen` is still recorded, so a
downstream that knows the expected length can tell.

## C21. Character and second usage is exact when the provider reports it, estimated otherwise

`Usage.characters` and `Usage.seconds` carry the provider's own meter when
one exists -- `processedCharactersCount` (Inworld), the `character-cost`
header (ElevenLabs), `usage.seconds` rounded up (OpenAI transcription),
`audio_duration_ms` (AssemblyAI) -- and the record says `exact`. Where no
meter exists (OpenAI binary TTS) the surface's `usage_estimate(facts)` fills
the record from the request and the bytes forwarded and the record says
`estimated`; `cost_notes` names the estimate. A voice request never records
zero with a confident basis: an empty Inworld result (`usage: null`) is an
exact zero because Inworld said so, not because nothing was parsed.

## C22. The Responses surface refuses what it cannot honour and forwards what the provider ended with

`POST /v1/responses` (PLAN-2 Phase F) refuses `background: true` at the
gateway: 400 `invalid_request`, no upstream call, no breaker evidence. A
background response is created and then polled by id, and until polling
routes exist a 200 with `status: "queued"` would hand the client an id the
gateway cannot resolve. For the same reason a body carrying
`previous_response_id` or `conversation` is refused (400, no upstream call)
for a provider whose row says `stateless_responses` -- DeepSeek accepts those
fields with a 200 and silently drops them, which is worse than a refusal --
while a plan with a stateful target further down falls through to it.
Nothing is ever stripped from the body.

On the stream, C2's third row is exact: `response.failed` and `event: error`
frames upstream sent reach the client byte for byte, the record carries the
provider's own classification (`in_stream_error`, or `upstream_overloaded`
when the code says so), and the body then closes. The gateway never
synthesises a `response.completed` (which would report a cut answer as whole)
or a `response.failed` the provider did not send. `response.incomplete` is a
finished turn, not a failure: outcome `completed`, stop reason `length` or
`content_filter`, usage exact.

*Enforced by:* `tests/contract/test_responses.py`
(`background_refused_before_upstream`, `stateless_provider_refuses_state`,
`response_failed_is_forwarded_and_nothing_follows`,
`error_event_is_forwarded_and_nothing_follows`, `incomplete_is_length`),
`tests/unit/test_responses_surface.py`.

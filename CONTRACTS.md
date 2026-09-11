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

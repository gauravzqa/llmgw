# PRELIMINARY scale pass — S1, S2, S3 (loaded machine)

> **PRELIMINARY. DO NOT QUOTE AS CAPACITY.** Single repeat, short window
> (`--warm 15 --measure 60 --repeats 1 --workers 6`), on a machine that was NOT
> quiet (`os.getloadavg()` ~5–7 on 16 cores throughout). A latency/scale bench on
> a contended box OVER-reports everything and, worse, lets the *instrument* (the
> load generator, the fake, and the gateway all running locally) become the
> bottleneck. The full `warm60/measure300/×3` campaign on a quiet box is a
> separate run — command at the bottom.
>
> The only signals worth trusting from this pass are **(a) S1's calibrated
> added-latency floor** and **(b) S3's resource slopes** (RSS/fd/tasks per
> stream). Every tail number here is provisional. S2 is substantively INVALID
> (see §S2) despite its mechanical "CALIBRATED" line.

## Environment

| | |
|---|---|
| Python | 3.11.15 (CPython, arm64) |
| cores | 16 |
| `ulimit -n` (RLIMIT_NOFILE soft) | **1,048,576** (raised shell, not macOS default 256) |
| `os.getloadavg()` at start of S1 / S2 / S3 | (5.6, 5.5, 5.4) / (4.1, 5.0, 5.2) / (7.2, 7.4, 6.4) |
| date | 2026-09-10 |

Because `ulimit -n` is already 1,048,576 here, the classic prediction — "the
256-fd limit breaks first" — **cannot reproduce on this box**; nothing in S1–S3
came near an fd wall (peak fd seen was ~2k in S2, ~1.5k in S3). That prediction is
about a *default* macOS shell (256 fds ≈ breaks at ~120 streams) and is a
deliberate S4 experiment for the quiet-machine run, not something this pass could
observe. Raw per-scenario instrument output: `load-S1-run.md`, `load-S2-run.md`,
`load-S3-run.md`.

---

## S1 — Short non-streaming (400 rps, 1 event) — **CALIBRATED, trustworthy floor**

**Calibration: VALID.** Arm D p99 TTFE 7.23 ms < Arm G p99 TTFE 13.10 ms — the
client path is comfortably faster than the gateway path, so the added-latency
number is the gateway's, not the client's ceiling.

| quantile | Arm D (direct) ms | Arm G (gateway) ms | **added ms** |
|---|---|---|---|
| p50 | 1.87 | 3.12 | **+1.25** |
| p90 | 2.68 | 5.65 | **+2.97** |
| p99 | 7.23 | 13.10 | **+5.87** |
| p99.9 | 14.20 | 41.43 | +27.22 |

- started=30,094 per arm, 0 errors, all 200s. ~405 rps offered, ~404 rps completed.
- Gateway resource at this load: peak 16 concurrent in-flight, peak_tasks 46,
  peak RSS ~47 MB, peak CPU ~50% of one core, fds 22 in / 19 up. Trivial — S1 is
  a request-rate/latency test, not a resource test.

**Saturation flag (real but mild):** Arm D fired **4.1%** of arrivals late (max
6 ms), Arm G **3.3%** late (max 1 ms). So the generator could not perfectly hold
the 400 rps Poisson schedule — the offered load was a few % under target. This is
exactly the instrument-is-a-SUT signal the pre-run prediction called for ("expect the
generator to flag late arrivals before the gateway's p99 doubles"). The lateness
is small enough that the p50/p99 floor is still usable.

**Prediction vs result:** the pre-run prediction said added p50 ≈ **1–2 ms**. Result: **+1.25
ms p50** — dead in the predicted band, and consistent with the overhead bench's
~1 ms single-call floor. The gap is at the tail: added p99 +5.87 ms (and p99.9
+27 ms) is inflated by machine contention + the ~3–4% late arrivals; a quiet box
should pull the p99 delta down toward the 2–5 ms the prediction expected.
**Trustworthy: the +1.25 ms p50 floor. Provisional: the p99/p99.9 tail.**

---

## S2 — Typical streaming (100 rps × 25 s ≈ 2,500 streams) — **SUBSTANTIVELY INVALID**

**Calibration line says "CALIBRATED" — ignore it.** It passed only on the narrow
mechanical test (D p99 TTFE 825 ms < G p99 TTFE 28,521 ms). In substance **both
arms are saturated and the generator/fake are a co-equal bottleneck**, so no
gateway number from S2 is honest:

- **Arm D (pure client→fake, the control) was itself drowning:** 64.8% errors
  (ReadError), total p50 = **107 seconds**, TTFE p50 291 ms. A control arm with
  65% errors and 100-second totals is not a client floor — it is a collapsed
  client+fake. When the control collapses, the "added latency" delta is
  meaningless.
- **Arm G:** 90.3% errors (connect 2,537 / timeout 1,030 / ReadError 763), status
  mix {200: 1,485, 504: 1,956, 429: 381}, TTFE p50 **10.7 s**. The reported added
  first-event of **+10.4 s p50** is an artifact of the whole local fleet melting,
  not gateway overhead.

**Why, and why fewer workers would not fix it:** S2's offered load is 2,500
concurrent 1,000-event streams, driven by 6 generator processes *and* served by
one gateway *and* one fake, all on a box already at loadavg ~5. The gateway core
pegged at **100% CPU** (GIL-bound on SSE parse/re-emit). Lowering `--workers`
reduces generator throughput (more late arrivals, lower offered load) but does
nothing about the 2,500-stream memory/CPU/fd pressure shared across the three
local fleets — so it was not attempted. **The lesson: S2's full 2,500-stream
target simply cannot be driven honestly from this contended single box.** It needs
the quiet machine (ideally generator on separate cores/host).

Resource numbers below are therefore **contaminated** (error-filled, partial
streams) and shown only as a rough shape, not a result:

- peak_streams_open 1,717 (never reached ~2,500 in the window), peak_tasks 5,553
  (~3.2 tasks/stream), peak RSS ~435 MB, CPU 100% (one core), fds 2,034 in /
  1,075 up, marginal RSS ~164 KiB/stream (above the 40–120 band — but inflated by
  half-dead streams, not a real slope).

**Prediction vs result:** prediction was jitter p99 ≈ 1–3 ms, tasks ~10–12k,
I/O-bound low CPU. Result teaches the opposite *about the instrument*: at this
scale on this box the pipeline is CPU/GIL-bound and saturates before any gateway
jitter can be read. **No trustworthy S2 number from this pass.**

---

## S3 — 1k open slow-drip streams (the point of this pass) — resource slopes TRUSTWORTHY, latency N/A

**Calibration: INVALID for latency (by design, not a failure).** Arm D and Arm G
land in *identical* TTFE buckets (p50 530.88 ms = p50 530.88 ms; verdict prints
"D p99 >= G p99 → INVALID"). The reason is benign: TTFE here is dominated by the
fake's **0.5 s slow-drip first-event delay**, which both arms pay identically, so
the gateway's ~1 ms overhead is invisible under a 500 ms floor. S3 is a
**resource** scenario, not a latency one — the latency column is expected to carry
no gateway signal, and it doesn't.

**Resource results (both arms identical schedule; Arm G sampled):**

| metric | value | per-stream | prediction | verdict |
|---|---|---|---|---|
| peak streams_open | **735** | — | ramp toward 1,000 | 735 reached in the 60 s window (10 rps × ~73 s); 1k not reached — report peak |
| RSS | 38.7 MB baseline → **92.3 MB** peak | **60.1 KiB/stream** (regression); 75.9 KiB/stream (endpoint: (94,496−38,672)/735) | **40–120 KiB/stream** | **IN BAND** — headline S3 number |
| fds | 736 inbound / 735 upstream | **≈2 fds/stream** (1 in + 1 up) | ~1k inbound + ~1k upstream | **exactly as predicted** |
| asyncio tasks | peak 3,679 | **≈5.0 tasks/stream** | ~4–5 tasks/stream (httpx/h2 adds its own) | **IN BAND** |
| pump_buffered_bytes | 0 | — | — | no backpressure buildup (slow drip < read speed) |
| breaker / capture drops | 0 / 0 | — | — | clean |
| CPU | peak 75.6% | — | I/O-bound | under one core, as expected |

- **Marginal bytes/stream ≈ 60 KiB (regression) / 76 KiB (endpoint)** — squarely
  in the predicted 40–120 KiB band. This is the number that predicts capacity,
  and it is the most trustworthy result of the whole pass because it is a *slope*
  (contention shifts the intercept, not the slope much).
- **fd split is exactly 1 inbound + 1 upstream per stream** — the cleanest
  confirmation in this pass.
- **Leak check inconclusive (caveat):** the pre-run prediction expected post-run baseline
  tasks to fall to single digits. The reported `baseline_tasks=304` is the *last*
  sample taken while ~300 streams were still mid-drain (100 s streams can't finish
  in a 75 s window), not a post-quiescence reading — so it neither confirms nor
  refutes a leak. A quiet run long enough for all streams to complete is needed to
  read the true baseline.
- **43% ReadError on BOTH arms is a harness artifact, not a gateway fault.**
  S3's streams are 200 events × 0.5 s = **100 s long**, but the window is
  warm 15 + measure 60 and the worker drain waits only `timeout_s`(60)+5 s before
  `client.aclose()` force-closes whatever is still open. Those forced closes
  surface as ReadError, symmetrically on D and G. It is the short window cutting
  its own streams, and it is the direct cause of streams topping out at 735.

**Prediction vs result:** RSS slope predicted 40–120 KiB/stream → got ~60 KiB
(regression) — **prediction held**. fds predicted 1-in/1-up → got exactly that —
**held**. tasks predicted 4–5/stream → got ~5.0 — **held**. The only gap is that
1,000 streams and the clean post-run baseline were not reached, purely because the
window is shorter than one stream's lifetime; the *slope* up to 735 is already
the usable signal.

---

## Summary

| scenario | calibration | trustworthy | provisional / invalid |
|---|---|---|---|
| **S1** | VALID | added p50 **+1.25 ms** (TTFE) | p99 +5.87 ms, p99.9 +27 ms (tail inflated by contention + 3–4% late arrivals) |
| **S2** | "CALIBRATED" mechanically, **INVALID in substance** | nothing | everything — both arms saturated, control arm 65% errors & 107 s totals; pipeline GIL-bound at 100% CPU |
| **S3** | INVALID for latency (drip-dominated, by design) | **RSS ≈60 KiB/stream; fd 1-in/1-up; ≈5 tasks/stream** (up to 735 streams) | leak/baseline inconclusive; 1k not reached; 43% ReadError = harness force-close artifact |

- **loadavg during the runs: ~5–7 on 16 cores throughout** (S3 ran hottest at
  ~7.2). Not quiet. This is why S1's p50 (~1.9 ms D / 3.1 ms G) sits above the
  overhead bench's ~1 ms, and why S2 collapsed.
- **Was the instrument the bottleneck? Yes, at S2 and partially at S1.** S2's
  control arm failing proves the generator+fake could not offer 2,500 streams
  honestly from this box. S1's 3–4% late arrivals show the generator slipping its
  400 rps Poisson schedule. S3's generator kept its (gentle 10 rps) schedule
  fine — late=1 — so S3's resource slopes are not generator-contaminated.

## To get real numbers (quiet machine)

```sh
ulimit -n 1048576
for S in S1 S2 S3 S4 S5 S6 S7 S8; do
  .venv/bin/python -m bench.load --scenario $S \
      --warm 60 --measure 300 --repeats 3 --workers 8
done
# results land in bench/results/load-<S>-run.md
```

Requirements for the defensible run: `os.getloadavg()` well under 1.0 (ideally the
generator on separate cores or a separate host so the control arm can't co-starve
the gateway), and a measure window **longer than one stream's lifetime** for S2
(25 s) and S3 (100 s) so streams_open actually reaches its target (2,500 / 1,000),
the post-run task baseline can be read for a leak check, and the ReadError
force-close artifact disappears.

Source: `bench/load.py`, scenarios in `bench/scenarios/__init__.py`.

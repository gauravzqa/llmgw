"""The property that makes the merged tail number honest.

Averaging percentiles across workers is forbidden. This test asserts the
thing that forbiddance rests on: merging N histograms then taking p99 is NOT
the same as averaging N per-histogram p99s, and the MERGED answer is the one
that tracks the true pooled p99. If these two ever coincided, the whole
multi-worker aggregation in bench/load.py would be arbitrary.

Run: .venv/bin/python -m pytest bench/test_histogram.py -q
 or: .venv/bin/python bench/test_histogram.py
"""

from __future__ import annotations

import math
import random

from bench.load import (
    ArmResult,
    LogHistogram,
    arm_tables,
    calibration_verdict,
    merge_all,
    paired_delta,
)


def _true_pct(values: list[float], q: float) -> float:
    vs = sorted(values)
    if not vs:
        return float("nan")
    rank = q / 100.0 * len(vs)
    i = min(len(vs) - 1, max(0, int(rank)))
    return vs[i]


def test_merge_sums_bucket_counts():
    a = LogHistogram()
    b = LogHistogram()
    for v in (0.001, 0.002, 0.05):
        a.record(v)
    for v in (0.001, 0.3, 0.3):
        b.record(v)
    merged = merge_all([a, b])
    assert merged.total == 6
    # bucket-for-bucket sum
    for i in range(len(merged.counts)):
        assert merged.counts[i] == a.counts[i] + b.counts[i]


def test_merged_p99_differs_from_averaged_p99():
    """The headline property. Workers see VERY different distributions: one is
    fast (most traffic), a few are slow (the tail lives there). Averaging their
    p99s washes the tail out; merging preserves it."""
    rng = random.Random(20260910)
    hists: list[LogHistogram] = []
    pooled: list[float] = []

    # 8 "fast" workers around 2 ms
    for _ in range(8):
        h = LogHistogram()
        for _ in range(5000):
            v = rng.lognormvariate(math.log(0.002), 0.25)
            h.record(v)
            pooled.append(v)
        hists.append(h)
    # 2 "slow" workers around 40 ms -- this is where the true tail is
    for _ in range(2):
        h = LogHistogram()
        for _ in range(5000):
            v = rng.lognormvariate(math.log(0.040), 0.35)
            h.record(v)
            pooled.append(v)
        hists.append(h)

    merged = merge_all(hists)
    merged_p99 = merged.percentile(99)
    avg_of_p99s = sum(h.percentile(99) for h in hists) / len(hists)
    true_p99 = _true_pct(pooled, 99)

    # 1) the two aggregation methods genuinely disagree (not a rounding nit)
    rel_gap = abs(merged_p99 - avg_of_p99s) / true_p99
    assert rel_gap > 0.10, (
        f"merged p99={merged_p99*1e3:.2f} ms vs avg-of-p99s="
        f"{avg_of_p99s*1e3:.2f} ms -- these must differ; averaging hides the tail")

    # 2) the MERGED answer tracks the true pooled p99 within bucket resolution
    rel_err = abs(merged_p99 - true_p99) / true_p99
    assert rel_err < 0.15, (
        f"merged p99={merged_p99*1e3:.2f} ms strayed from true p99="
        f"{true_p99*1e3:.2f} ms (rel {rel_err:.2%})")


def test_percentile_monotonic_and_interpolated():
    h = LogHistogram()
    rng = random.Random(7)
    for _ in range(20000):
        h.record(rng.lognormvariate(math.log(0.01), 0.5))
    p50, p90, p99, p999 = (h.percentile(q) for q in (50, 90, 99, 99.9))
    assert p50 < p90 < p99 <= p999
    # interpolation returns a value strictly inside the observed range
    assert h.min_s <= p50 <= h.max_s


def test_empty_is_nan():
    assert math.isnan(LogHistogram().percentile(99))


# --------------------------------------------------------------------------
# Admitted vs refused: the report must not read a shed as a speed-up
# --------------------------------------------------------------------------
#
# With a per-process cap, a refused request is a ~2 ms 503 and lands in the
# all-requests histograms like any other response. At a 97% shed rate the
# all-requests p50 IS the rejection latency, and the calibration rule (D p99 <
# G p99 -> CALIBRATED) sees Arm G "faster" than the direct arm and flags
# itself INVALID. These tests pin the admitted-only reading.


def _hist(values_s: list[float]) -> LogHistogram:
    h = LogHistogram()
    for v in values_s:
        h.record(v)
    return h


def _arm(arm: str, *, admitted_ttfe_s: list[float], refused: int = 0,
         refused_status: str = "503", transport_refused: int = 0,
         gaps_s: list[float] | None = None) -> ArmResult:
    """A synthetic ArmResult the way run_arm() would merge it: refused
    requests contribute a ~2 ms sample to the all-requests ttfe/total and
    nothing to the admitted twins."""
    ok = len(admitted_ttfe_s)
    reject_s = [0.002] * refused
    status = {"200": ok}
    if refused - transport_refused > 0:
        status[refused_status] = refused - transport_refused
    errs = {"connect": transport_refused} if transport_refused else {}
    gaps = gaps_s or []
    return ArmResult(
        arm=arm, workers=1, rate=1.0, started=ok + refused, ok=ok,
        error=refused, errors_by_kind=errs, status_counts=status,
        late_arrivals=0, max_late_s=0.0, peak_inflight_sum=ok,
        bytes_total=0, gw_headers=None, d_has_x_gw=None,
        ttfe=_hist(admitted_ttfe_s + reject_s),
        total=_hist([v + 1.0 for v in admitted_ttfe_s] + reject_s),
        gaps=_hist(gaps), samples={}, wall_s=1.0, fake_requests=ok,
        cut_midstream=0, refused_before_byte=refused,
        ttfe_adm=_hist(admitted_ttfe_s),
        total_adm=_hist([v + 1.0 for v in admitted_ttfe_s]),
        gaps_adm=_hist(gaps),
    )


def _lognormal(rng: random.Random, median_s: float, n: int) -> list[float]:
    return [rng.lognormvariate(math.log(median_s), 0.2) for _ in range(n)]


def test_admitted_hists_fall_back_to_all_when_nothing_refused():
    """An arm that refused nothing reads identically either way, so an old
    report and a new one agree to the byte."""
    rng = random.Random(1)
    d = _arm("D", admitted_ttfe_s=_lognormal(rng, 0.030, 2000))
    t, tot, gp = d.admitted_hists()
    # populated twins: same population, same numbers
    assert t.quantile_row_ms() == d.ttfe.quantile_row_ms()
    assert tot.quantile_row_ms() == d.total.quantile_row_ms()
    assert d.refused_share == 0.0
    # a result built without the twins (an older worker, or a crash
    # placeholder) falls back to the all-requests histograms themselves
    bare = ArmResult(**{**d.__dict__, "ttfe_adm": LogHistogram(),
                        "total_adm": LogHistogram(), "gaps_adm": LogHistogram()})
    t2, tot2, gp2 = bare.admitted_hists()
    assert t2 is bare.ttfe and tot2 is bare.total and gp2 is bare.gaps
    # and the calibration line carries no cap note
    g = _arm("G", admitted_ttfe_s=_lognormal(rng, 0.031, 2000))
    ok, msg = calibration_verdict(d, g)
    assert ok and "CALIBRATED" in msg and "refused" not in msg


def test_calibration_reads_admitted_only_when_the_arm_mostly_refused():
    """The S4 cap-150 shape: 97% of Arm G's responses are 2 ms rejections.
    All-requests p99 puts G below D (a false INVALID); admitted-only puts G
    at its true ~1 ms above D and the line says why it compared that way."""
    rng = random.Random(2)
    d = _arm("D", admitted_ttfe_s=_lognormal(rng, 1.000, 36000))
    g = _arm("G", admitted_ttfe_s=_lognormal(rng, 1.001, 1200), refused=34907)
    # the false reading, on the all-requests histograms
    assert g.ttfe.percentile(99) < d.ttfe.percentile(99)
    assert g.refused_share > 0.9
    ok, msg = calibration_verdict(d, g)
    assert ok, msg
    assert "CALIBRATED" in msg
    assert "refused by the cap; compared admitted only" in msg
    assert "Arm G 96.7%" in msg, msg
    assert "Arm D" not in msg.split("(")[-1], "D refused nothing; not in the note"


def test_calibration_note_names_both_arms_when_both_shed():
    rng = random.Random(3)
    d = _arm("D", admitted_ttfe_s=_lognormal(rng, 0.030, 500), refused=100,
             transport_refused=100)
    g = _arm("G", admitted_ttfe_s=_lognormal(rng, 0.033, 500), refused=300)
    _, msg = calibration_verdict(d, g)
    assert "Arm D 16.7%" in msg and "Arm G 37.5%" in msg


def test_calibration_note_silent_below_threshold():
    rng = random.Random(4)
    d = _arm("D", admitted_ttfe_s=_lognormal(rng, 0.030, 1000))
    g = _arm("G", admitted_ttfe_s=_lognormal(rng, 0.033, 1000), refused=20)  # 2%
    _, msg = calibration_verdict(d, g)
    assert "refused" not in msg


def test_arm_tables_add_an_admitted_section_only_when_something_was_refused():
    rng = random.Random(5)
    clean = _arm("D", admitted_ttfe_s=_lognormal(rng, 0.030, 500))
    text = arm_tables(clean)
    assert "admitted only" not in text
    assert "refused before a byte" not in text

    capped = _arm("G", admitted_ttfe_s=_lognormal(rng, 0.031, 150), refused=850,
                  transport_refused=50, gaps_s=_lognormal(rng, 0.025, 3000))
    text = arm_tables(capped)
    assert "admitted only (status 200; refused requests excluded):" in text
    # two ttfe rows: all-requests first, admitted second, with n = 1000 vs 150
    rows = [ln for ln in text.splitlines() if ln.startswith("| ttfe")]
    assert len(rows) == 2
    assert "|    1000 |" in rows[0] and "|     150 |" in rows[1]
    # inter-event appears in both tables
    assert sum(ln.startswith("| inter-event") for ln in text.splitlines()) == 2
    assert ("refused before a byte: 850 (85.0% of 1000 answered): "
            "{'503': 800} + 50 transport errors before a byte") in text
    # the plain status line is still there for old-report comparability
    assert "status: {'200': 150, '503': 800}" in text


def test_paired_delta_adds_admitted_table_and_shed_line_when_capped():
    rng = random.Random(6)
    d = _arm("D", admitted_ttfe_s=_lognormal(rng, 0.030, 5000))
    g = _arm("G", admitted_ttfe_s=_lognormal(rng, 0.031, 500), refused=4500)
    text = paired_delta(d, g)
    heads = [ln for ln in text.splitlines()
             if ln.startswith("added first-event latency")]
    assert len(heads) == 2 and heads[1].endswith("admitted only:")
    assert "shed (refused before a byte): Arm D 0.0%, Arm G 90.0%" in text
    # the all-requests table shows the false negative overhead; the admitted
    # one shows the true ~1 ms
    all_p50 = [ln for ln in text.split("admitted only")[0].splitlines()
               if ln.startswith("| p50")][0]
    adm_p50 = [ln for ln in text.split("admitted only")[1].splitlines()
               if ln.startswith("| p50")][0]
    all_added = float(all_p50.split("|")[4])
    adm_added = float(adm_p50.split("|")[4])
    assert all_added < 0 < adm_added
    assert 0.0 < adm_added < 5.0

    # and an uncapped pair renders exactly one table, as before
    g2 = _arm("G", admitted_ttfe_s=_lognormal(rng, 0.031, 5000))
    text2 = paired_delta(d, g2)
    assert text2.count("added first-event latency") == 1
    assert "shed" not in text2


if __name__ == "__main__":
    test_merge_sums_bucket_counts()
    test_merged_p99_differs_from_averaged_p99()
    test_percentile_monotonic_and_interpolated()
    test_empty_is_nan()
    test_admitted_hists_fall_back_to_all_when_nothing_refused()
    test_calibration_reads_admitted_only_when_the_arm_mostly_refused()
    test_calibration_note_names_both_arms_when_both_shed()
    test_calibration_note_silent_below_threshold()
    test_arm_tables_add_an_admitted_section_only_when_something_was_refused()
    test_paired_delta_adds_admitted_table_and_shed_line_when_capped()
    print("all histogram property tests passed")

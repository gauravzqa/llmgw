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

from bench.load import LogHistogram, merge_all


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


if __name__ == "__main__":
    test_merge_sums_bucket_counts()
    test_merged_p99_differs_from_averaged_p99()
    test_percentile_monotonic_and_interpolated()
    test_empty_is_nan()
    print("all histogram property tests passed")

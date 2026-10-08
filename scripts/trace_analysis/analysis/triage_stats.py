"""Stable percentile summaries for Trace Triage dimensions."""

import statistics


def _percentiles(values):
    if not values:
        return {}
    vals = sorted(values)

    def pct(p):
        if len(vals) == 1:
            return vals[0]
        return vals[min(int(len(vals) * p), len(vals) - 1)]

    return {
        "count": len(vals),
        "min": round(vals[0], 3),
        "p50": round(statistics.median(vals), 3),
        "p90": round(pct(0.90), 3),
        "p99": round(pct(0.99), 3),
        "max": round(vals[-1], 3),
    }

"""Deterministic statistical summaries for trace and worker groups."""

from __future__ import annotations

import math


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * quantile
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def _pearson(pairs: list[tuple[float, float]]) -> float | None:
    if len(pairs) < 2:
        return None
    xs = [item[0] for item in pairs]
    ys = [item[1] for item in pairs]
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    denominator = math.sqrt(
        sum((x - mean_x) ** 2 for x in xs) * sum((y - mean_y) ** 2 for y in ys)
    )
    return numerator / denominator if denominator else None


def _metric_summary(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "p50": round(_percentile(values, 0.50), 3),
        "p90": round(_percentile(values, 0.90), 3),
        "p99": round(_percentile(values, 0.99), 3),
        "max": round(max(values, default=0), 3),
    }


def _group_metric(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "p90": round(_percentile(values, 0.90), 3) if values else 0.0,
        "max": round(max(values), 3) if values else 0.0,
    }

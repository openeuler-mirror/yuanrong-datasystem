#!/usr/bin/env python3
"""Analyze URMA_ELAPSED_TOTAL log entries and generate a NUMA/CPU HTML report.

Supported input files are discovered recursively by these default filename patterns:
  - ds_client*log*
  - kvcache.INFO*log*

Both plain-text logs and gzip-compressed logs (*.gz) are supported.
"""

from __future__ import annotations

import argparse
import fnmatch
import gzip
import html
import math
import os
import re
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence

MARKER = "URMA_ELAPSED_TOTAL"
DEFAULT_PATTERNS = ("ds_client*log*", "kvcache.INFO*log*")
CPUID_RE = re.compile(r"\bcpuid\s*:\s*(-?\d+)\b")

# Edit these mappings if the machine topology changes.
NUMA_RANGES: tuple[tuple[str, int, int], ...] = (
    ("NUMA 0", 0, 95),
    ("NUMA 1", 96, 191),
    ("NUMA 2", 192, 287),
    ("NUMA 3", 288, 383),
)

PHYSICAL_CPU_NUMAS: dict[str, tuple[str, ...]] = {
    "Physical CPU 0": ("NUMA 0", "NUMA 1"),
    "Physical CPU 1": ("NUMA 2", "NUMA 3"),
}


@dataclass
class FileStats:
    path: str
    lines: int = 0
    matched: int = 0
    missing_cpuid: int = 0
    invalid_timestamp: int = 0
    out_of_range_cpuid: int = 0
    cpu_counts: Counter[int] = field(default_factory=Counter)
    error: str | None = None


def parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid time '{value}', expected ISO format such as "
            "2026-07-29T16:00:00 or 2026-07-29T16:00:00.123456"
        ) from exc


def normalize_for_compare(value: datetime) -> datetime:
    """Make aware timestamps comparable by dropping tz after UTC conversion.

    Log timestamps in the supplied format are timezone-naive. For a timezone-aware
    CLI value, converting to UTC then dropping tz is less surprising than raising.
    Normally users should pass local naive timestamps matching the logs.
    """
    if value.tzinfo is None:
        return value
    return value.astimezone().replace(tzinfo=None)


def find_numa(cpuid: int) -> str | None:
    for numa, first, last in NUMA_RANGES:
        if first <= cpuid <= last:
            return numa
    return None


def open_log(path: str):
    if path.lower().endswith(".gz"):
        return gzip.open(path, mode="rt", encoding="utf-8", errors="replace")
    return open(path, mode="rt", encoding="utf-8", errors="replace")


def extract_log_time(line: str) -> datetime | None:
    sep = line.find(" | ")
    if sep <= 0:
        return None
    raw = line[:sep].strip()
    try:
        value = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return normalize_for_compare(value)


def analyze_file(path: str, start: datetime | None, end: datetime | None) -> FileStats:
    result = FileStats(path=path)
    try:
        with open_log(path) as stream:
            for line in stream:
                result.lines += 1
                if MARKER not in line:
                    continue

                if start is not None or end is not None:
                    log_time = extract_log_time(line)
                    if log_time is None:
                        result.invalid_timestamp += 1
                        continue
                    if start is not None and log_time < start:
                        continue
                    if end is not None and log_time > end:
                        continue

                match = CPUID_RE.search(line)
                if match is None:
                    result.missing_cpuid += 1
                    continue

                cpuid = int(match.group(1))
                result.matched += 1
                result.cpu_counts[cpuid] += 1
                if find_numa(cpuid) is None:
                    result.out_of_range_cpuid += 1
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        result.error = f"{type(exc).__name__}: {exc}"
    return result


def matches_any(name: str, patterns: Sequence[str]) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def discover_files(inputs: Sequence[str], patterns: Sequence[str]) -> tuple[list[str], list[str]]:
    found: dict[str, None] = {}
    warnings: list[str] = []

    for raw in inputs:
        path = Path(raw).expanduser()
        if not path.exists():
            warnings.append(f"input does not exist: {path}")
            continue

        if path.is_file():
            if matches_any(path.name, patterns):
                found[str(path.resolve())] = None
            else:
                warnings.append(f"file skipped because its name does not match: {path}")
            continue

        def onerror(exc: OSError) -> None:
            warnings.append(f"cannot scan directory: {exc}")

        for root, _, files in os.walk(path, onerror=onerror):
            for name in files:
                if matches_any(name, patterns):
                    candidate = Path(root, name)
                    try:
                        key = str(candidate.resolve())
                    except OSError:
                        key = str(candidate.absolute())
                    found[key] = None

    return sorted(found), warnings


def format_int(value: int) -> str:
    return f"{value:,}"


def format_pct(value: int, total: int) -> str:
    if total <= 0:
        return "0.00%"
    return f"{value * 100.0 / total:.2f}%"


def safe_ratio(high: int, low: int) -> str:
    if high == 0:
        return "0.00"
    if low == 0:
        return "∞"
    return f"{high / low:.2f}"


def bar_chart_svg(title: str, data: Sequence[tuple[str, int]], width: int = 760, height: int = 320) -> str:
    if not data:
        return ""

    margin_left, margin_right, margin_top, margin_bottom = 78, 28, 45, 72
    chart_w = width - margin_left - margin_right
    chart_h = height - margin_top - margin_bottom
    max_value = max((value for _, value in data), default=0)
    scale_max = max(max_value, 1)
    slot = chart_w / max(len(data), 1)
    bar_w = min(86.0, slot * 0.58)

    parts = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        f'<text x="{width / 2:.1f}" y="24" text-anchor="middle" class="chart-title">{html.escape(title)}</text>',
    ]

    # Horizontal grid lines and compact y-axis labels.
    for step in range(5):
        value = scale_max * step / 4
        y = margin_top + chart_h - chart_h * step / 4
        parts.append(f'<line x1="{margin_left}" y1="{y:.1f}" x2="{width - margin_right}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{margin_left - 10}" y="{y + 4:.1f}" text-anchor="end" class="axis-label">{html.escape(compact_number(value))}</text>')

    for index, (label, value) in enumerate(data):
        x = margin_left + index * slot + (slot - bar_w) / 2
        bar_h = chart_h * value / scale_max
        y = margin_top + chart_h - bar_h
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" rx="5" class="bar"/>')
        parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{max(y - 8, margin_top + 12):.1f}" text-anchor="middle" class="value-label">{format_int(value)}</text>')
        parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{margin_top + chart_h + 25:.1f}" text-anchor="middle" class="axis-label">{html.escape(label)}</text>')

    parts.append("</svg>")
    return "".join(parts)


def compact_number(value: float) -> str:
    if value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return str(int(round(value)))


def cpu_heatmap_html(cpu_counts: Counter[int], total: int) -> str:
    max_count = max((cpu_counts.get(cpu, 0) for cpu in range(384)), default=0)
    blocks: list[str] = []
    for numa, first, last in NUMA_RANGES:
        cells: list[str] = []
        for cpu in range(first, last + 1):
            count = cpu_counts.get(cpu, 0)
            level = 0 if max_count == 0 else min(9, int(math.ceil(count * 9 / max_count)))
            cells.append(
                f'<div class="cpu-cell heat-{level}" title="CPU {cpu}: {format_int(count)} requests ({format_pct(count, total)})">'
                f'<span>{cpu}</span><strong>{format_int(count)}</strong></div>'
            )
        blocks.append(
            f'<section class="numa-heat"><h3>{html.escape(numa)} <small>CPU {first}-{last}</small></h3>'
            f'<div class="heat-grid">{"".join(cells)}</div></section>'
        )
    return "".join(blocks)


def build_report(
    output: Path,
    inputs: Sequence[str],
    patterns: Sequence[str],
    files: Sequence[str],
    stats: Sequence[FileStats],
    warnings: Sequence[str],
    start: datetime | None,
    end: datetime | None,
    elapsed_seconds: float,
) -> None:
    cpu_counts: Counter[int] = Counter()
    total_lines = 0
    total_matched = 0
    missing_cpuid = 0
    invalid_timestamp = 0
    out_of_range = 0
    errors: list[tuple[str, str]] = []

    for item in stats:
        total_lines += item.lines
        total_matched += item.matched
        missing_cpuid += item.missing_cpuid
        invalid_timestamp += item.invalid_timestamp
        out_of_range += item.out_of_range_cpuid
        cpu_counts.update(item.cpu_counts)
        if item.error:
            errors.append((item.path, item.error))

    numa_counts: dict[str, int] = {name: 0 for name, _, _ in NUMA_RANGES}
    for cpuid, count in cpu_counts.items():
        numa = find_numa(cpuid)
        if numa is not None:
            numa_counts[numa] += count

    physical_counts: dict[str, int] = {
        physical: sum(numa_counts[numa] for numa in numas)
        for physical, numas in PHYSICAL_CPU_NUMAS.items()
    }

    valid_topology_total = sum(numa_counts.values())
    numa_values = list(numa_counts.values())
    physical_values = list(physical_counts.values())
    numa_ratio = safe_ratio(max(numa_values, default=0), min(numa_values, default=0))
    physical_ratio = safe_ratio(max(physical_values, default=0), min(physical_values, default=0))

    top_cpus = sorted(cpu_counts.items(), key=lambda item: (-item[1], item[0]))[:20]
    top_rows = "".join(
        f"<tr><td>{rank}</td><td>{cpu}</td><td>{html.escape(find_numa(cpu) or 'Out of range')}</td>"
        f"<td>{format_int(count)}</td><td>{format_pct(count, total_matched)}</td></tr>"
        for rank, (cpu, count) in enumerate(top_cpus, 1)
    ) or '<tr><td colspan="5" class="empty">No matching records</td></tr>'

    numa_rows = "".join(
        f"<tr><td>{html.escape(numa)}</td><td>CPU {first}-{last}</td><td>{format_int(numa_counts[numa])}</td>"
        f"<td>{format_pct(numa_counts[numa], valid_topology_total)}</td></tr>"
        for numa, first, last in NUMA_RANGES
    )

    physical_rows = "".join(
        f"<tr><td>{html.escape(physical)}</td><td>{html.escape(', '.join(numas))}</td>"
        f"<td>{format_int(physical_counts[physical])}</td><td>{format_pct(physical_counts[physical], valid_topology_total)}</td></tr>"
        for physical, numas in PHYSICAL_CPU_NUMAS.items()
    )

    issue_items: list[str] = []
    issue_items.extend(f"<li>{html.escape(item)}</li>" for item in warnings)
    issue_items.extend(
        f"<li><code>{html.escape(path)}</code>: {html.escape(error)}</li>" for path, error in errors
    )
    if missing_cpuid:
        issue_items.append(f"<li>{format_int(missing_cpuid)} marker lines had no cpuid field.</li>")
    if invalid_timestamp:
        issue_items.append(f"<li>{format_int(invalid_timestamp)} marker lines had invalid timestamps and were skipped by the time filter.</li>")
    if out_of_range:
        issue_items.append(f"<li>{format_int(out_of_range)} records used a cpuid outside CPU 0-383.</li>")
    issues_html = (
        f'<div class="issues"><h2>Warnings</h2><ul>{"".join(issue_items)}</ul></div>'
        if issue_items
        else ""
    )

    time_range = "All timestamps"
    if start is not None or end is not None:
        time_range = f"{start.isoformat(sep=' ') if start else '-∞'} to {end.isoformat(sep=' ') if end else '+∞'} (inclusive)"

    file_sample = "".join(f"<li><code>{html.escape(path)}</code></li>" for path in files[:50])
    if len(files) > 50:
        file_sample += f"<li>... and {format_int(len(files) - 50)} more files</li>"

    report = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>URMA NUMA Load Report</title>
<style>
:root {{ color-scheme: light; --bg:#f4f7fb; --card:#fff; --text:#172033; --muted:#657089; --line:#dfe5ef; --accent:#4568dc; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif; }}
main {{ max-width:1280px; margin:0 auto; padding:28px; }}
h1 {{ margin:0 0 6px; font-size:30px; }}
h2 {{ margin:0 0 16px; font-size:20px; }}
h3 {{ margin:0 0 10px; font-size:15px; }}
.subtle, small {{ color:var(--muted); font-weight:400; }}
.meta {{ margin:0 0 24px; color:var(--muted); }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; margin-bottom:18px; }}
.card, .panel, .issues {{ background:var(--card); border:1px solid var(--line); border-radius:12px; box-shadow:0 4px 18px rgba(24,39,75,.05); }}
.card {{ padding:17px; }}
.card span {{ color:var(--muted); display:block; }}
.card strong {{ display:block; margin-top:5px; font-size:25px; }}
.grid-2 {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(470px,1fr)); gap:18px; margin:18px 0; }}
.panel {{ padding:20px; overflow:auto; }}
.chart {{ width:100%; min-width:520px; height:auto; }}
.chart-title {{ font-size:17px; font-weight:650; fill:var(--text); }}
.grid {{ stroke:#e5e9f1; stroke-width:1; }}
.bar {{ fill:var(--accent); }}
.axis-label {{ fill:var(--muted); font-size:12px; }}
.value-label {{ fill:var(--text); font-size:12px; font-weight:650; }}
table {{ width:100%; border-collapse:collapse; }}
th, td {{ padding:9px 10px; text-align:left; border-bottom:1px solid var(--line); white-space:nowrap; }}
th {{ color:var(--muted); font-weight:600; }}
.empty {{ text-align:center; color:var(--muted); }}
.numa-heat {{ margin:0 0 22px; }}
.heat-grid {{ display:grid; grid-template-columns:repeat(24,minmax(38px,1fr)); gap:5px; min-width:950px; }}
.cpu-cell {{ height:43px; padding:3px 4px; border-radius:5px; border:1px solid rgba(0,0,0,.08); text-align:center; overflow:hidden; }}
.cpu-cell span {{ display:block; font-size:10px; opacity:.72; }}
.cpu-cell strong {{ display:block; font-size:10px; }}
.heat-0 {{ background:#f1f3f7; }} .heat-1 {{ background:#e8ecff; }} .heat-2 {{ background:#d8e0ff; }}
.heat-3 {{ background:#c6d2ff; }} .heat-4 {{ background:#aec0ff; }} .heat-5 {{ background:#91a9ff; }}
.heat-6 {{ background:#738fff; color:#fff; }} .heat-7 {{ background:#5876e8; color:#fff; }}
.heat-8 {{ background:#405fcb; color:#fff; }} .heat-9 {{ background:#29449f; color:#fff; }}
.issues {{ margin:18px 0; padding:20px; border-color:#e6c873; background:#fffaf0; }}
.issues ul {{ margin:0; padding-left:20px; }}
code {{ overflow-wrap:anywhere; white-space:normal; }}
details {{ margin-top:14px; }}
footer {{ margin-top:22px; color:var(--muted); }}
@media (max-width:700px) {{ main {{ padding:16px; }} .grid-2 {{ grid-template-columns:1fr; }} }}
</style>
</head>
<body><main>
<h1>URMA NUMA Load Report</h1>
<p class="meta">Generated {html.escape(datetime.now().isoformat(sep=' ', timespec='seconds'))} · Time range: {html.escape(time_range)}</p>

<section class="cards">
  <div class="card"><span>Matched requests</span><strong>{format_int(total_matched)}</strong></div>
  <div class="card"><span>Log files</span><strong>{format_int(len(files))}</strong></div>
  <div class="card"><span>Lines scanned</span><strong>{format_int(total_lines)}</strong></div>
  <div class="card"><span>NUMA max/min ratio</span><strong>{numa_ratio}</strong></div>
  <div class="card"><span>Physical CPU max/min ratio</span><strong>{physical_ratio}</strong></div>
  <div class="card"><span>Scan time</span><strong>{elapsed_seconds:.2f}s</strong></div>
</section>

<div class="grid-2">
  <section class="panel">{bar_chart_svg('Requests by NUMA node', list(numa_counts.items()))}</section>
  <section class="panel">{bar_chart_svg('Requests by physical CPU', list(physical_counts.items()))}</section>
</div>

<div class="grid-2">
  <section class="panel"><h2>NUMA distribution</h2><table><thead><tr><th>Node</th><th>CPU range</th><th>Requests</th><th>Share</th></tr></thead><tbody>{numa_rows}</tbody></table></section>
  <section class="panel"><h2>Physical CPU distribution</h2><table><thead><tr><th>Socket</th><th>NUMA nodes</th><th>Requests</th><th>Share</th></tr></thead><tbody>{physical_rows}</tbody></table></section>
</div>

<section class="panel"><h2>Per-CPU heatmap</h2><p class="subtle">Each cell shows CPU ID and request count. Hover for the precise share. Darker means more requests.</p>{cpu_heatmap_html(cpu_counts, total_matched)}</section>

<section class="panel" style="margin-top:18px"><h2>Top 20 CPUs</h2><table><thead><tr><th>Rank</th><th>CPU</th><th>NUMA</th><th>Requests</th><th>Share</th></tr></thead><tbody>{top_rows}</tbody></table></section>

{issues_html}

<section class="panel" style="margin-top:18px">
<h2>Scan configuration</h2>
<table><tbody>
<tr><th>Inputs</th><td>{html.escape(', '.join(inputs))}</td></tr>
<tr><th>Filename patterns</th><td>{html.escape(', '.join(patterns))}</td></tr>
<tr><th>Marker</th><td>{MARKER}</td></tr>
<tr><th>Topology-valid requests</th><td>{format_int(valid_topology_total)}</td></tr>
</tbody></table>
<details><summary>Scanned files (showing at most 50)</summary><ul>{file_sample}</ul></details>
</section>
<footer>Distribution is based on the <code>cpuid</code> field of each matching <code>{MARKER}</code> log line.</footer>
</main></body></html>"""

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8")


def default_jobs() -> int:
    return max(1, min(8, os.cpu_count() or 1))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count URMA_ELAPSED_TOTAL cpuid distribution and generate an HTML NUMA report.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("inputs", nargs="+", help="one or more log directories/files")
    parser.add_argument("-s", "--start", type=parse_datetime, help="inclusive start time in ISO format")
    parser.add_argument("-e", "--end", type=parse_datetime, help="inclusive end time in ISO format")
    parser.add_argument("-o", "--output", default="urma_numa_report.html", help="output HTML path")
    parser.add_argument("-j", "--jobs", type=int, default=default_jobs(), help="parallel file readers")
    parser.add_argument(
        "-p", "--pattern", action="append", dest="patterns",
        help="filename glob; repeat to provide multiple patterns (replaces defaults)",
    )
    parser.add_argument("-q", "--quiet", action="store_true", help="suppress progress output")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    start = normalize_for_compare(args.start) if args.start else None
    end = normalize_for_compare(args.end) if args.end else None
    if start is not None and end is not None and start > end:
        print("error: --start must not be later than --end", file=sys.stderr)
        return 2
    if args.jobs < 1:
        print("error: --jobs must be >= 1", file=sys.stderr)
        return 2

    patterns = tuple(args.patterns or DEFAULT_PATTERNS)
    files, warnings = discover_files(args.inputs, patterns)
    if not files:
        print("error: no matching log files found", file=sys.stderr)
        for warning in warnings:
            print(f"warning: {warning}", file=sys.stderr)
        return 1

    if not args.quiet:
        print(f"Found {format_int(len(files))} log files; workers={args.jobs}")
        print(f"Time range: {start or '-∞'} to {end or '+∞'} (inclusive)")

    started = time.monotonic()
    stats: list[FileStats] = []

    if args.jobs == 1 or len(files) == 1:
        for index, path in enumerate(files, 1):
            stats.append(analyze_file(path, start, end))
            if not args.quiet:
                print(f"\rScanning files: {index}/{len(files)}", end="", flush=True)
    else:
        with ProcessPoolExecutor(max_workers=min(args.jobs, len(files))) as executor:
            futures = {executor.submit(analyze_file, path, start, end): path for path in files}
            for index, future in enumerate(as_completed(futures), 1):
                path = futures[future]
                try:
                    stats.append(future.result())
                except Exception as exc:  # Defensive: keep report generation alive.
                    stats.append(FileStats(path=path, error=f"worker failure: {type(exc).__name__}: {exc}"))
                if not args.quiet:
                    print(f"\rScanning files: {index}/{len(files)}", end="", flush=True)

    if not args.quiet:
        print()

    elapsed = time.monotonic() - started
    output = Path(args.output).expanduser().resolve()
    build_report(
        output=output,
        inputs=args.inputs,
        patterns=patterns,
        files=files,
        stats=stats,
        warnings=warnings,
        start=start,
        end=end,
        elapsed_seconds=elapsed,
    )

    matched = sum(item.matched for item in stats)
    errors = sum(1 for item in stats if item.error)
    print(f"Matched requests: {format_int(matched)}")
    print(f"Report: {output}")
    if errors:
        print(f"Warning: {errors} file(s) could not be fully read; see report", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Parse worker logs and plot URMA srcChipInflight (chip1/chip2) and totalElapsedMs
time series with useNumaAffinity statistics. The output is an interactive HTML
report with range sliders for selecting time windows.

== Log sources (urma_manager.cpp) ==
  - [URMA_ELAPSED_TOTAL]: ... total cost 1.25ms, ... srcChipInflight:{1:5,2:3} ...
  - URMA write useNumaAffinity:1, src:1, dst:2, ..., numa_write_counts:{src1:...,src2:...,dst1:...,dst2:...,src1_src2:...,src2_src1:...}

== Required log directory layout ==
  The input directory must be a worker-log root. Each direct child directory
  contains one worker's logs:

    <root>/                          <- directory passed to -f or listed in --list
    ├── worker-0/                    <- one worker; directory name is worker name
    │   ├── kvcache.INFO.log          current INFO log
    │   ├── kvcache.INFO.<ts>_<pid>.log.gz   optional compressed rotation
    │   └── ...                      optional additional .INFO.* logs
    ├── worker-1/
    │   └── ...
    └── ...

  Requirements:
    1. -f and every --list entry must name the worker-log root, whose direct
       children are worker directories.
    2. A worker directory name is the label in the chart.
    3. INFO log names must contain ".INFO." case-insensitively. Other logs are
       ignored.
    4. Files ending in ".gz" are decompressed automatically. Samples are sorted
       by their parsed timestamps after all INFO logs are read.
    5. In --list mode, output names use each input path to avoid basename
       collisions.
    6. A directory without an INFO log is skipped.

== Supported log lines ==
  1. Custom: 2026-08-15T09:42:00.900857 | I | ... [URMA_ELAPSED_TOTAL]: ...
  2. glog:   I0816 10:23:45.123456 123 file.cpp:1270] ...
  ISO timestamps take precedence. A glog timestamp has no year, so the current
  year is used. Fields are matched in the log body, not by file name or line.

== Arguments ==
  -f, --file DIR    One worker-log root; mutually exclusive with --list.
  --list FILE       Text file with one worker-log root per line; produces one
                    HTML report per root.
  -o, --out OUT     HTML path for -f, or output directory for --list.
  --prefix P1,P2    Optional comma-separated worker-name prefixes to include.
  -j, --jobs N      Optional number of worker scan threads (default: CPU count).
  --downsample N    Aggregate every N samples by mean value and median time.
  -h, --help        Show this help message.

== Examples ==
  # One root -> one interactive HTML report containing all worker panels
  python3 plot_urma_chip_inflight.py -f collected_worker_logs -o urma.html

  # Filter workers by name prefix
  python3 plot_urma_chip_inflight.py -f collected_worker_logs -o urma.html --prefix workerA,workerB

  # One HTML report per root from a list file
  # dirs.txt: one directory per line; blank lines and # comments are skipped.
  #      collected_worker_logs_1
  #      collected_worker_logs_2
  python3 plot_urma_chip_inflight.py --list dirs.txt -o ./reports

  # List mode with filtering and parallel scanning
  python3 plot_urma_chip_inflight.py --list dirs.txt -o ./reports --prefix workerA,workerB -j 8

  # Downsample every five samples
  python3 plot_urma_chip_inflight.py -f collected_worker_logs -o urma.html --downsample 5

== HTML interaction ==
  - Scroll or drag to zoom a chart; use its range slider to select a window.
  - The Inflight & NUMA and Elapsed correlation tabs retain the same worker order.
  - Each elapsed card places totalElapsedMs above total inflight with a shared
    time window and separate y axes, so their units are never mixed.
  - Instantaneous series show max, average, p90, and p99. Percentiles use the
    nearest-rank definition and are calculated from the displayed samples.

== NUMA write-count report ==
  When the sampled write log includes `numa_write_counts`, each worker card
  includes:
    1. source-chip cumulative writes: src1, src2, src1->src2, src2->src1;
    2. destination-chip cumulative writes: dst1, dst2.
  The two source-switch counters record changes between the transmitted source
  chip and the source chip selected by NUMA affinity. They are not destination
  chip traffic counters.

== Notes ==
  1. Exactly one of -f and --list is required.
  2. --prefix matches worker directory names, not root names.
  3. A missing chip value is plotted as zero, so both chip lines have every
     sample timestamp.
  4. Inflight and totalElapsedMs samples use the same --downsample aggregation:
     an arithmetic mean per bucket at the bucket median timestamp. NUMA
     statistics and cumulative write counters are not downsampled.
  5. The generated report loads Plotly from its public CDN.
"""

import os
import re
import sys
import gzip
import argparse
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------- Log parsing ----------------------------

# Custom line prefix: 2026-08-15T09:42:00.900857 | I | file.cpp:1253 | ...
ISO_TS_RE = re.compile(
    r"(?P<year>\d{4})-(?P<mon>\d{2})-(?P<day>\d{2})T"
    r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})(?:\.(?P<us>\d{1,6}))?"
)
# glog line prefix: I0816 10:23:45.123456 123 file.cpp:1270]
GLOG_TS_RE = re.compile(
    r"^(?P<sev>[IWEF])(?P<mon>\d{2})(?P<day>\d{2})\s+"
    r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})(?:\.(?P<us>\d{1,6}))?\s+"
    r"\d+\s+"
)
# srcChipInflight:{1:5,2:3}
INFLIGHT_RE = re.compile(r"srcChipInflight:\{([^}]*)\}")
CHIP_CNT_RE = re.compile(r"(\d+):(-?\d+)")
TOTAL_ELAPSED_MS_RE = re.compile(
    r"\btotal cost\s+(?P<elapsed>-?(?:\d+(?:\.\d*)?|\.\d+))ms\b", re.IGNORECASE
)
# URMA write useNumaAffinity:1, src:1, dst:2,
NUMA_RE = re.compile(
    r"URMA write useNumaAffinity:(?P<numa>\d+),\s*src:(?P<src>\d+),\s*dst:(?P<dst>\d+)"
)
NUMA_WRITE_COUNTS_RE = re.compile(r"numa_write_counts\s*:\s*\{(?P<counts>[^}]*)\}", re.IGNORECASE)
NUMA_WRITE_COUNT_ITEM_RE = re.compile(
    r"(?P<name>(?:src\d+_(?:src|dst)\d+)|src\d+|dst\d+)\s*:\s*(?P<value>\d+)", re.IGNORECASE
)

CURRENT_YEAR = datetime.now().year


def parse_ts(line):
    m = ISO_TS_RE.search(line)
    if m:
        d = m.groupdict()
        try:
            return datetime(int(d["year"]), int(d["mon"]), int(d["day"]),
                            int(d["h"]), int(d["m"]), int(d["s"]),
                            int((d["us"] or "").ljust(6, "0") or 0))
        except ValueError:
            return None
    m = GLOG_TS_RE.match(line)
    if m:
        d = m.groupdict()
        try:
            return datetime(CURRENT_YEAR, int(d["mon"]), int(d["day"]),
                            int(d["h"]), int(d["m"]), int(d["s"]),
                            int((d["us"] or "").ljust(6, "0") or 0))
        except ValueError:
            return None
    return None


def parse_inflight(s):
    return {int(m.group(1)): int(m.group(2)) for m in CHIP_CNT_RE.finditer(s)}


def parse_numa_write_counts(line):
    match = NUMA_WRITE_COUNTS_RE.search(line)
    if match is None:
        return None
    return {item.group("name").lower(): int(item.group("value"))
            for item in NUMA_WRITE_COUNT_ITEM_RE.finditer(match.group("counts"))}


def open_log(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return open(path, "r", encoding="utf-8", errors="replace")


def is_info_log(fname):
    return ".INFO." in fname.upper()


def scan_worker(worker_dir):
    """Return inflight, elapsed, legacy NUMA, and cumulative NUMA write samples."""
    times, chip1, chip2, total_elapsed_ms = [], [], [], []
    write_times, src1, src2, dst1, dst2, src1_src2, src2_src1 = [], [], [], [], [], [], []
    numa = {"dst1_src": {}, "dst2_src": {}, "other_src": {}}
    diagnostics = {"info_files": 0, "snapshot_lines": 0, "parsed_snapshots": 0, "timestamp_failures": 0,
                   "empty_snapshots": 0}
    files = sorted(entry.path for entry in os.scandir(worker_dir)
                   if entry.is_file() and is_info_log(entry.name))
    diagnostics["info_files"] = len(files)
    for path in files:
        try:
            f = open_log(path)
        except OSError:
            continue
        try:
            for line in f:
                if "srcChipInflight" in line and "URMA_ELAPSED_TOTAL" in line:
                    ts = parse_ts(line)
                    if ts is None:
                        continue
                    m = INFLIGHT_RE.search(line)
                    counts = parse_inflight(m.group(1)) if m else {}
                    elapsed_match = TOTAL_ELAPSED_MS_RE.search(line)
                    times.append(ts)
                    chip1.append(counts.get(1, 0))
                    chip2.append(counts.get(2, 0))
                    total_elapsed_ms.append(float(elapsed_match.group("elapsed")) if elapsed_match else 0.0)
                if "numa_write_counts" in line:
                    diagnostics["snapshot_lines"] += 1
                    counts = parse_numa_write_counts(line)
                    ts = parse_ts(line)
                    if counts is None or not counts:
                        diagnostics["empty_snapshots"] += 1
                    elif ts is None:
                        diagnostics["timestamp_failures"] += 1
                    else:
                        write_times.append(ts)
                        src1.append(counts.get("src1", 0))
                        src2.append(counts.get("src2", 0))
                        dst1.append(counts.get("dst1", 0))
                        dst2.append(counts.get("dst2", 0))
                        src1_src2.append(counts.get("src1_src2", 0))
                        src2_src1.append(counts.get("src2_src1", 0))
                        diagnostics["parsed_snapshots"] += 1
                if "URMA write useNumaAffinity" in line:
                    m = NUMA_RE.search(line)
                    if not m:
                        continue
                    src, dst = int(m.group("src")), int(m.group("dst"))
                    key = "dst1_src" if dst == 1 else "dst2_src" if dst == 2 else "other_src"
                    numa[key][src] = numa[key].get(src, 0) + 1
        except (OSError, EOFError, gzip.BadGzipFile) as exc:
            print(f"warning: failed to read log file {path}: {exc}", file=sys.stderr)
        finally:
            f.close()
    samples = sorted(zip(times, chip1, chip2, total_elapsed_ms), key=lambda sample: sample[0])
    if samples:
        times, chip1, chip2, total_elapsed_ms = map(list, zip(*samples))
    write_samples = sorted(zip(write_times, src1, src2, dst1, dst2, src1_src2, src2_src1), key=lambda sample: sample[0])
    if write_samples:
        write_times, src1, src2, dst1, dst2, src1_src2, src2_src1 = map(list, zip(*write_samples))
    return times, chip1, chip2, total_elapsed_ms, numa, (write_times, src1, src2, dst1, dst2, src1_src2, src2_src1), diagnostics


def find_workers(root, prefixes=None):
    """Return direct child worker directories that contain INFO logs."""
    workers = []
    for entry in os.scandir(root):
        if not entry.is_dir():
            continue
        name = entry.name
        if prefixes and not any(name.startswith(prefix) for prefix in prefixes):
            continue
        with os.scandir(entry.path) as children:
            if any(child.is_file() and is_info_log(child.name) for child in children):
                workers.append((name, entry.path))
    workers.sort()
    return workers


def fmt_numa(stats):
    def line(label, d):
        if not d:
            return f"{label}: none"
        parts = [f"src={k}: {v}" for k, v in sorted(d.items())]
        return f"{label}: " + ", ".join(parts) + f"  (total {sum(d.values())})"
    return [line("dst=1 source distribution", stats["dst1_src"]),
            line("dst=2 source distribution", stats["dst2_src"]),
            line("other dst source distribution", stats["other_src"])]


# ---------------------------- HTML rendering ----------------------------

XML_ESC = {ord("&"): "&amp;", ord("<"): "&lt;", ord(">"): "&gt;", ord('"'): "&quot;"}


def esc(s):
    return str(s).translate(XML_ESC)


def poly(points):
    """Return rounded polyline coordinates to reduce SVG size."""
    return " ".join(f"{int(round(x))},{int(round(y))}" for x, y in points)


class SVGCanvas:
    """Worker charts with an independent brush and NUMA summary per panel."""

    def __init__(self, workers_data, out_path, cols=2,
                 fig_w=1000, sub_h=340, margin=50,
                 pad_left=80, pad_right=40, pad_top=30, gap=40, text_h=60):
        self.workers = workers_data
        self.out_path = out_path
        self.cols = cols
        self.fig_w = fig_w
        self.sub_h = sub_h
        self.margin = margin
        self.pad_left = pad_left
        self.pad_right = pad_right
        self.pad_top = pad_top
        self.gap = gap
        self.text_h = text_h
        self.n = len(workers_data)
        self.rows = (self.n + cols - 1) // cols
        self.plot_w = fig_w - pad_left - pad_right
        self.main_h = 170
        self.brush_h = 45
        self.plot_h = self.main_h

    @staticmethod
    def _id(name):
        return re.sub(r"[^A-Za-z0-9_.-]", lambda match: f"_{ord(match.group()):x}_", name)

    def _layout(self):
        """Return [(col_x, row_y, plot_x, plot_y, plot_w, plot_h)]."""
        out = []
        for i in range(self.n):
            r, c = divmod(i, self.cols)
            x = self.pad_left + c * (self.plot_w + self.pad_left + self.pad_right)
            y = self.pad_top + r * (self.sub_h + self.gap)
            out.append((x, y, x, y + self.margin, self.plot_w, self.plot_h))
        return out

    def _y_global(self):
        ymax = 1
        for _, _, c1, c2, _, _, _ in self.workers:
            for arr in (c1, c2):
                if arr:
                    ymax = max(ymax, max(arr))
        return ymax

    def _fmt_axis(self, name, px, py, pw, ph, times, c1, c2, ymax):
        """Format chart axes, grid, legend, and JavaScript-updated elements."""
        t0 = min(times)
        t1 = max(times)
        span = (t1 - t0).total_seconds()
        element_id = self._id(name)
        parts = []
        parts.append(f'<text x="{px}" y="{py - self.margin + 22}" font-family="monospace" '
                     f'font-size="16" font-weight="bold" fill="#222">{esc(name)}</text>')
        parts.append(f'<rect x="{px}" y="{py}" width="{pw}" height="{ph}" '
                     f'fill="#fafafa" stroke="#888" stroke-width="1"/>')
        parts.append(f'<clipPath id="clip-{element_id}"><rect x="{px}" y="{py}" '
                     f'width="{pw}" height="{ph}"/></clipPath>')
        for k in range(1, 4):
            gx = px + pw * k // 4
            parts.append(f'<line x1="{gx}" y1="{py}" x2="{gx}" y2="{py + ph}" '
                         f'stroke="#eee" stroke-width="1" id="grid-{element_id}-{k}"/>')
        for k in range(1, 4):
            gy = py + ph * k // 4
            parts.append(f'<line x1="{px}" y1="{gy}" x2="{px + pw}" y2="{gy}" '
                         f'stroke="#eee" stroke-width="1"/>')
        for k in range(5):
            val = ymax * (4 - k) // 4
            gy = py + ph * k // 4
            parts.append(f'<text x="{px - 6}" y="{gy + 4}" text-anchor="end" '
                         f'font-family="monospace" font-size="10" fill="#666">{val}</text>')
        parts.append(f'<text x="{px - 50}" y="{py + ph // 2}" text-anchor="middle" '
                     f'font-family="monospace" font-size="11" fill="#444" '
                     f'transform="rotate(-90,{px-50},{py+ph//2})">inflight count</text>')
        for k in range(5):
            gx = px + pw * k // 4
            parts.append(f'<line x1="{gx}" y1="{py + ph}" x2="{gx}" y2="{py + ph + 4}" '
                         f'stroke="#888" stroke-width="1" id="xtick-{element_id}-{k}"/>')
            parts.append(f'<text x="{gx}" y="{py + ph + 16}" text-anchor="middle" '
                         f'font-family="monospace" font-size="9" fill="#666" '
                         f'id="xlabel-{element_id}-{k}"></text>')
        parts.append(f'<text x="{px + pw // 2}" y="{py + ph + 32}" text-anchor="middle" '
                     f'font-family="monospace" font-size="11" fill="#444" '
                     f'id="xrange-{element_id}"></text>')
        lx, ly = px + 8, py + 14
        parts.append(f'<line x1="{lx}" y1="{ly}" x2="{lx + 20}" y2="{ly}" '
                     f'stroke="#d62728" stroke-width="1.5"/>')
        parts.append(f'<text x="{lx + 26}" y="{ly + 3}" font-family="monospace" '
                     f'font-size="11" fill="#444">src chip 1</text>')
        parts.append(f'<line x1="{lx + 100}" y1="{ly}" x2="{lx + 120}" y2="{ly}" '
                     f'stroke="#1f77b4" stroke-width="1.5"/>')
        parts.append(f'<text x="{lx + 126}" y="{ly + 3}" font-family="monospace" '
                     f'font-size="11" fill="#444">src chip 2</text>')
        def tx(t):
            return px + int((t - t0).total_seconds() / max(span, 1e-6) * pw)

        def ty(v):
            return py + ph - int(v / ymax * ph)
        pts1 = [(tx(t), ty(v)) for t, v in zip(times, c1)]
        pts2 = [(tx(t), ty(v)) for t, v in zip(times, c2)]
        if pts1:
            parts.append(f'<polyline points="{poly(pts1)}" fill="none" stroke="#d62728" '
                         f'stroke-width="1.2" clip-path="url(#clip-{element_id})" id="line-{element_id}-1"/>')
        else:
            parts.append(f'<polyline points="" fill="none" stroke="#d62728" '
                         f'stroke-width="1.2" clip-path="url(#clip-{element_id})" id="line-{element_id}-1"/>')
        if pts2:
            parts.append(f'<polyline points="{poly(pts2)}" fill="none" stroke="#1f77b4" '
                         f'stroke-width="1.2" clip-path="url(#clip-{element_id})" id="line-{element_id}-2"/>')
        else:
            parts.append(f'<polyline points="" fill="none" stroke="#1f77b4" '
                         f'stroke-width="1.2" clip-path="url(#clip-{element_id})" id="line-{element_id}-2"/>')
        return parts, t0, t1, span

    def _fmt_brush(self, name, bx, by, bw, bh, times, c1, c2, ymax):
        """Format a fixed overview chart with a draggable, resizable brush."""
        parts = []
        element_id = self._id(name)
        t0 = min(times)
        t1 = max(times)
        span = (t1 - t0).total_seconds()
        parts.append(f'<rect x="{bx}" y="{by}" width="{bw}" height="{bh}" '
                     f'fill="#f5f5f5" stroke="#aaa" stroke-width="1"/>')
        def tx(t):
            return bx + int((t - t0).total_seconds() / max(span, 1e-6) * bw)

        def ty(v):
            return by + bh - int(v / ymax * bh)
        p1 = [(tx(t), ty(v)) for t, v in zip(times, c1)]
        p2 = [(tx(t), ty(v)) for t, v in zip(times, c2)]
        if p1:
            parts.append(f'<polyline points="{poly(p1)}" fill="none" stroke="#d62728" '
                         f'stroke-width="1" opacity="0.7"/>')
        if p2:
            parts.append(f'<polyline points="{poly(p2)}" fill="none" stroke="#1f77b4" '
                         f'stroke-width="1" opacity="0.7"/>')
        parts.append(f'<rect x="{bx}" y="{by}" width="{bw}" height="{bh}" '
                     f'fill="rgba(50,120,200,0.25)" stroke="#2a6fb0" stroke-width="1" '
                     f'class="brush-main" data-idx="{element_id}" rx="2"/>')
        parts.append(f'<rect x="{bx - 3}" y="{by - 2}" width="6" height="{bh + 4}" '
                     f'fill="#2a6fb0" class="brush-l" data-idx="{element_id}" rx="1"/>')
        parts.append(f'<rect x="{bx + bw - 3}" y="{by - 2}" width="6" height="{bh + 4}" '
                     f'fill="#2a6fb0" class="brush-r" data-idx="{element_id}" rx="1"/>')
        return parts

    @staticmethod
    def _numa_text(px, py_bottom, lines):
        return [f'<text x="{px}" y="{py_bottom + i * 14}" font-family="monospace" '
                f'font-size="10" fill="#333">{esc(ln)}</text>' for i, ln in enumerate(lines)]

    def render(self):
        if self.n == 0:
            total_w = self.fig_w + self.pad_right
            total_h = self.pad_top + 80
        else:
            total_w = self.pad_left + self.cols * (self.plot_w + self.pad_left + self.pad_right)
            total_h = self.pad_top + self.rows * (self.sub_h + self.gap) + 20

        positions = self._layout()
        ymax = self._y_global()
        body = []
        panel_meta = []
        for i, (name, times, c1, c2, numa_lines, _, _) in enumerate(self.workers):
            pos = positions[i]
            px, py, pw, ph = pos[2], pos[3], pos[4], pos[5]
            axis_parts, t0, t1, span = self._fmt_axis(
                name, px, py, pw, ph, times, c1, c2, ymax)
            body.extend(axis_parts)
            bx = px
            by = py + ph + 40
            bw = pw
            bh = self.brush_h
            body.extend(self._fmt_brush(name, bx, by, bw, bh, times, c1, c2, ymax))
            tx = px
            ty = by + bh + 12
            body.extend(self._numa_text(tx, ty, numa_lines))
            panel_meta.append({
                "idx": i, "name": name, "elementId": self._id(name),
                "mainX": px, "mainY": py, "mainW": pw, "mainH": ph,
                "brushX": bx, "brushY": by, "brushW": bw, "brushH": bh,
                "t0": t0.timestamp(), "t1": t1.timestamp(), "span": span, "ymax": ymax,
                "times": [t.timestamp() for t in times],
                "c1": list(c1), "c2": list(c2),
            })

        svg = ['<?xml version="1.0" encoding="UTF-8"?>']
        svg.append(f'<svg xmlns="http://www.w3.org/2000/svg" '
                   f'width="{total_w}" height="{total_h}" '
                   f'viewBox="0 0 {total_w} {total_h}" font-family="monospace">')
        svg.append('<style>.brush-main{cursor:move}.brush-l,.brush-r{cursor:ew-resize}'
                   'text{user-select:none}</style>')
        svg.append(f'<text x="{self.pad_left}" y="22" font-size="16" font-weight="bold" '
                   f'fill="#000">URMA srcChipInflight (chip1/chip2) time series '
                   f'(drag brush to select range; double-click to reset)</text>')
        svg.append("\n".join(body))
        svg.append(self._zoom_script(panel_meta))
        svg.append("</svg>")

        text = "\n".join(svg)
        if self.out_path.endswith(".svgz") or self.out_path.endswith(".svg.gz"):
            with gzip.open(self.out_path, "wt", encoding="utf-8") as f:
                f.write(text)
        else:
            with open(self.out_path, "w", encoding="utf-8") as f:
                f.write(text)
        return self.out_path

    @staticmethod
    def _zoom_script(panels):
        import json
        panels_json = json.dumps(panels, default=str)
        return f'''<script type="text/ecmascript"><![CDATA[
var panels = {panels_json};
var svgRoot = document.documentElement;
var state = {{}};

panels.forEach(function(p){{
    state[p.idx] = {{x0: p.brushX, x1: p.brushX + p.brushW, mode:null, sx:0}};
}});

function svgPt(cx, cy){{
    var pt = svgRoot.createSVGPoint(); pt.x=cx; pt.y=cy;
    var m = svgRoot.getScreenCTM(); if(!m) return {{x:cx,y:cy}};
    var t = pt.matrixTransform(m.inverse()); return {{x:t.x,y:t.y}};
}}
function panelAt(x, y){{
    for (var i=0;i<panels.length;i++){{var p=panels[i];
        if (x>=p.brushX-3 && x<=p.brushX+p.brushW+3 && y>=p.brushY-3 && y<=p.brushY+p.brushH+3)
            return i;
    }} return -1;
}}
function hitHandle(x, y, p){{
    var s = state[p.idx];
    if (Math.abs(x - s.x0) <= 6) return "l";
    if (Math.abs(x - s.x1) <= 6) return "r";
    return "move";
}}

function winToTime(p, win0, win1){{
    var f0 = (win0 - p.brushX) / p.brushW;
    var f1 = (win1 - p.brushX) / p.brushW;
    return {{t0: p.t0 + f0*p.span, t1: p.t0 + f1*p.span}};
}}

function redrawMain(idx){{
    var p = panels[idx]; var s = state[idx];
    var win0 = s.x0, win1 = s.x1;
    if (win1 - win0 < 1) return;
    var tw = winToTime(p, win0, win1);
    var wt0 = tw.t0, wt1 = tw.t1, wspan = Math.max(1e-6, wt1 - wt0);
    var px = p.mainX, py = p.mainY, pw = p.mainW, ph = p.mainH, ymax = p.ymax;
    var sel1 = [], sel2 = [];
    for (var i=0;i<p.times.length;i++){{
        var t = p.times[i];
        if (t < wt0 - 1e-3 || t > wt1 + 1e-3) continue;
        var gx = px + (t - wt0)/wspan * pw;
        var g1y = py + ph - (p.c1[i]/ymax)*ph;
        var g2y = py + ph - (p.c2[i]/ymax)*ph;
        sel1.push(Math.round(gx)+","+Math.round(g1y));
        sel2.push(Math.round(gx)+","+Math.round(g2y));
    }}
    setLine(idx, 1, sel1);
    setLine(idx, 2, sel2);
    var fmt = (function(){{
        function pad(n){{return (n<10?"0":"")+n;}}
        return function(ts){{
            var d = new Date(ts*1000); return pad(d.getHours())+":"+pad(d.getMinutes())+":"+pad(d.getSeconds());
        }};
    }})();
    for (var k=0;k<5;k++){{
        var t = wt0 + wspan*k/4;
        var gx = px + pw*k/4;
        var tick = document.getElementById("xtick-"+p.elementId+"-"+k);
        var label = document.getElementById("xlabel-"+p.elementId+"-"+k);
        if (tick) tick.setAttribute("x1", Math.round(gx)), tick.setAttribute("x2", Math.round(gx));
        if (label) label.setAttribute("x", Math.round(gx)), label.textContent = fmt(t);
        var grid = document.getElementById("grid-"+p.elementId+"-"+k);
        if (grid) grid.setAttribute("x1", Math.round(gx)), grid.setAttribute("x2", Math.round(gx));
    }}
    var rng = document.getElementById("xrange-"+p.elementId);
    if (rng){{
        var d0 = new Date(wt0*1000), d1 = new Date(wt1*1000);
        function pad2(n){{return (n<10?"0":"")+n;}}
        var m0 = pad2(d0.getMonth()+1), day0 = pad2(d0.getDate()), hm0 = pad2(d0.getHours())+":"+pad2(d0.getMinutes());
        var m1 = pad2(d1.getMonth()+1), day1 = pad2(d1.getDate()), hm1 = pad2(d1.getHours())+":"+pad2(d1.getMinutes());
        rng.textContent = "time ("+m0+"-"+day0+" "+hm0+" ~ "+m1+"-"+day1+" "+hm1+")";
    }}
}}
function setLine(idx, which, pts){{
    var p = panels[idx];
    var el = document.getElementById("line-"+p.elementId+"-"+which);
    if (el) el.setAttribute("points", pts.join(" "));
}}

function setBrushRect(idx, x0, x1){{
    var p = panels[idx]; var s = state[idx];
    s.x0 = Math.max(p.brushX, Math.min(x0, x1));
    s.x1 = Math.min(p.brushX + p.brushW, Math.max(x0, x1));
    var mains = svgRoot.getElementsByClassName("brush-main");
    var ls = svgRoot.getElementsByClassName("brush-l");
    var rs = svgRoot.getElementsByClassName("brush-r");
    for (var i=0;i<mains.length;i++){{ if (mains[i].getAttribute("data-idx")==p.elementId){{
        mains[i].setAttribute("x", s.x0);
        mains[i].setAttribute("width", Math.max(0, s.x1 - s.x0));
    }}}}
    for (var i=0;i<ls.length;i++){{ if (ls[i].getAttribute("data-idx")==p.elementId){{
        ls[i].setAttribute("x", s.x0 - 3);
    }}}}
    for (var i=0;i<rs.length;i++){{ if (rs[i].getAttribute("data-idx")==p.elementId){{
        rs[i].setAttribute("x", s.x1 - 3);
    }}}}
}}

svgRoot.addEventListener("mousedown", function(ev){{
    var pt = svgPt(ev.clientX, ev.clientY);
    var idx = panelAt(pt.x, pt.y);
    if (idx < 0) return;
    var p = panels[idx]; var s = state[idx];
    s.mode = hitHandle(pt.x, pt.y, p);
    s.sx = pt.x; s._ox0 = s.x0; s._ox1 = s.x1;
}});
svgRoot.addEventListener("mousemove", function(ev){{
    if (!panels.some(function(p){{return state[p.idx].mode;}})) return;
    var pt = svgPt(ev.clientX, ev.clientY);
    panels.forEach(function(p){{
        var s = state[p.idx];
        if (!s.mode) return;
        var dx = pt.x - s.sx;
        var bw = p.brushW;
        if (s.mode === "move"){{
            var w = s._ox1 - s._ox0;
            var nx0 = Math.max(p.brushX, Math.min(s._ox0 + dx, p.brushX + bw - w));
            setBrushRect(p.idx, nx0, nx0 + w);
        }} else if (s.mode === "l"){{
            setBrushRect(p.idx, s._ox0 + dx, s._ox1);
        }} else if (s.mode === "r"){{
            setBrushRect(p.idx, s._ox0, s._ox1 + dx);
        }}
        redrawMain(p.idx);
    }});
}});
window.addEventListener("mouseup", function(ev){{
    panels.forEach(function(p){{ state[p.idx].mode = null; }});
}});
svgRoot.addEventListener("dblclick", function(ev){{
    var pt = svgPt(ev.clientX, ev.clientY);
    var idx = panelAt(pt.x, pt.y);
    if (idx < 0) return;
    var p = panels[idx];
    setBrushRect(idx, p.brushX, p.brushX + p.brushW);
    redrawMain(idx);
}});
panels.forEach(function(p){{ redrawMain(p.idx); }});
]]></script>'''


def cumulative_chart_svg(title, times, series, width=920, height=290):
    """Render a static cumulative-count time series chart for one worker."""
    if not times:
        return ""
    left, right, top, bottom = 70, 25, 38, 45
    plot_w, plot_h = width - left - right, height - top - bottom
    t0, t1 = times[0], times[-1]
    span = max((t1 - t0).total_seconds(), 1e-6)
    ymax = max(1, max((max(values) if values else 0 for _, _, values in series), default=0))
    colors = ("#d62728", "#1f77b4", "#2ca02c", "#9467bd")
    parts = [f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}">',
             f'<text x="{width // 2}" y="22" text-anchor="middle" font-size="14" font-weight="bold">{esc(title)}</text>',
             f'<rect x="{left}" y="{top}" width="{plot_w}" height="{plot_h}" fill="#fafafa" stroke="#888"/>']
    for step in range(5):
        value = ymax * step / 4
        y = top + plot_h - plot_h * step / 4
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#e5e5e5"/>')
        parts.append(f'<text x="{left - 7}" y="{y + 4:.1f}" text-anchor="end" font-size="10">{int(value)}</text>')
    for index, (label, color, values) in enumerate(series):
        points = []
        for timestamp, value in zip(times, values):
            x = left + (timestamp - t0).total_seconds() / span * plot_w
            y = top + plot_h - value / ymax * plot_h
            points.append((x, y))
        parts.append(f'<polyline points="{poly(points)}" fill="none" stroke="{color}" stroke-width="1.5"/>')
        legend_x = left + 8 + index * 155
        parts.append(f'<line x1="{legend_x}" y1="{top + 14}" x2="{legend_x + 18}" y2="{top + 14}" stroke="{color}" stroke-width="2"/>')
        final_value = values[-1] if values else 0
        parts.append(f'<text x="{legend_x + 23}" y="{top + 18}" font-size="11">{esc(label)}={final_value}</text>')
    for step in range(5):
        timestamp = t0 + (t1 - t0) * step / 4
        x = left + plot_w * step / 4
        parts.append(f'<text x="{x:.1f}" y="{top + plot_h + 17}" text-anchor="middle" font-size="10">{timestamp:%H:%M:%S}</text>')
    parts.append('</svg>')
    return ''.join(parts)


def write_html_report(out_path, data):
    """Write inflight cards with a right-side NUMA detail drawer per worker."""
    import json
    workers = []
    for name, times, c1, c2, elapsed_ms, numa_lines, write_counts, diagnostics in data:
        write_times, src1, src2, dst1, dst2, src1_src2, src2_src1 = write_counts
        workers.append({
            "name": name,
            "inflight": {"time": [t.isoformat() for t in times], "src1": c1, "src2": c2},
            "inflightMax": {"src1": max(c1, default=0), "src2": max(c2, default=0)},
            "elapsed": {"time": [t.isoformat() for t in times], "totalMs": elapsed_ms},
            "numa": numa_lines,
            "diagnostics": diagnostics,
            "writes": {"time": [t.isoformat() for t in write_times], "src1": src1, "src2": src2,
                       "dst1": dst1, "dst2": dst2, "src1Src2": src1_src2, "src2Src1": src2_src1},
        })
    payload = json.dumps(workers, ensure_ascii=False).replace("</", "<\\/")
    css = '''*{box-sizing:border-box}body{margin:0;background:#f0f2f5;color:#252a34;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif}.header{position:sticky;top:0;z-index:10;padding:22px 20px;text-align:center;color:#fff;background:linear-gradient(135deg,#667eea,#764ba2);box-shadow:0 2px 8px #0003}.header h1{margin:0;font-size:22px}.meta span{display:inline-block;margin:8px 3px 0;padding:3px 10px;border-radius:12px;background:#fff3;font-size:12px}.container{max-width:1500px;margin:auto;padding:20px}.hint,.card{background:#fff;border-radius:10px;box-shadow:0 1px 3px #0002}.hint{padding:10px;text-align:center;color:#666;font-size:12px;margin-bottom:16px}.card{padding:12px;margin-bottom:16px}.card:hover{box-shadow:0 4px 12px #0003}.card-head{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:2px 8px 8px}.card h2{margin:0;font-size:16px}.chart-title{margin:12px 8px 0;font-size:13px;font-weight:600;color:#333}.legend{display:flex;flex-wrap:wrap;gap:12px;margin:7px 8px 1px;font-size:11px;color:#555}.legend span{white-space:nowrap}.legend i{display:inline-block;width:18px;height:2px;margin:0 5px 3px 0;vertical-align:middle}.chart{height:270px}.stats{display:flex;flex-wrap:wrap;gap:14px;border-top:1px solid #eee;padding:8px 4px 2px;margin-top:3px;font:12px monospace;color:#555}.stats b{color:#222}.drawer-button,.close{border:0;border-radius:6px;padding:7px 11px;background:#5d6fd8;color:#fff;cursor:pointer;font-size:12px}.drawer-button:hover,.close:hover{background:#4658bf}.backdrop{position:fixed;inset:0;background:#0005;opacity:0;pointer-events:none;transition:opacity .2s;z-index:20}.drawer{position:fixed;top:0;right:0;width:min(50vw,900px);min-width:520px;height:100vh;overflow:auto;background:#f7f8fc;box-shadow:-4px 0 16px #0003;transform:translateX(100%);transition:transform .25s;z-index:21;padding:18px}.drawer.open{transform:translateX(0)}.backdrop.open{opacity:1;pointer-events:auto}.drawer-head{position:sticky;top:-18px;z-index:1;display:flex;justify-content:space-between;align-items:center;padding:18px 0 12px;background:#f7f8fc}.drawer-title{margin:0;font-size:18px}.drawer-note,.numa{margin:8px 4px;white-space:pre-wrap;font:11px monospace;color:#555}.detail-card{background:#fff;border-radius:10px;padding:10px;margin:12px 0;box-shadow:0 1px 3px #0002}@media(max-width:720px){.drawer{width:100vw;min-width:0}}'''
    css += '.tabs{display:flex;gap:8px;margin-bottom:12px}.tab{border:0;border-radius:7px;padding:9px 14px;background:#dde1ee;color:#35405a;cursor:pointer;font-weight:600}.tab.active{background:#5d6fd8;color:#fff}.tab-page{display:none}.tab-page.active{display:block}'
    script = '''const workers=__PAYLOAD__;let syncing=false;const mainIds=[];const detailIds=[];const elapsedIds=[];
function trace(x,y,name,color){return {x,y,name,mode:"lines",line:{color,width:1.5},hovertemplate:"%{x|%Y-%m-%d %H:%M:%S}<br>"+name+": %{y}<extra></extra>",showlegend:false}}
function legend(items){return '<div class="legend">'+items.map(x=>'<span><i style="background:'+x[1]+'"></i>'+x[0]+'</span>').join('')+'</div>'}
function format(v){return Number.isInteger(v)?String(v):v.toFixed(2)}
function percentile(values,p){if(!values.length)return 0;const sorted=[...values].sort((a,b)=>a-b);return sorted[Math.max(0,Math.ceil(p*sorted.length)-1)]}
function stats(label,values){if(!values.length)return '<span><b>'+label+':</b> no samples</span>';const sum=values.reduce((a,b)=>a+b,0),max=values.reduce((a,b)=>Math.max(a,b),values[0]);return '<span><b>'+label+' max:</b> '+format(max)+'</span><span><b>avg:</b> '+format(sum/values.length)+'</span><span><b>p90:</b> '+format(percentile(values,.90))+'</span><span><b>p99:</b> '+format(percentile(values,.99))+'</span>'}
function layout(yTitle,slider){return {height:270,margin:{l:65,r:25,t:10,b:48},template:"plotly_white",hovermode:"x unified",xaxis:{rangeslider:{visible:slider,thickness:.08,bgcolor:"#f0f0f0"},tickfont:{size:10}},yaxis:{title:yTitle,tickfont:{size:10},gridcolor:"rgba(0,0,0,.05)"},showlegend:false}}
function link(id,ids){document.getElementById(id).on("plotly_relayout",e=>{if(syncing||e["xaxis.range[0]"]===undefined)return;syncing=true;ids.filter(x=>x!==id).forEach(x=>Plotly.relayout(x,{"xaxis.range":[e["xaxis.range[0]"],e["xaxis.range[1]"]]}));setTimeout(()=>syncing=false,50)})}
function openDrawer(i){const w=workers[i],body=document.getElementById("drawer-body"),d=w.diagnostics,src="detail_src",dst="detail_dst";document.getElementById("drawer-title").textContent=w.name+" NUMA write details";detailIds.length=0;const diag='<div class="drawer-note">INFO files: '+d.info_files+'; snapshots found: '+d.snapshot_lines+'; parsed: '+d.parsed_snapshots+'; timestamp failures: '+d.timestamp_failures+'; malformed snapshots: '+d.empty_snapshots+'.</div>';let html=diag;if(w.writes.time.length){const last=w.writes.time.length-1,total=w.writes.src1[last]+w.writes.src2[last],switches=w.writes.src1Src2[last]+w.writes.src2Src1[last],ratio=total?switches*100/total:0;html+='<div class="drawer-note">Source-chip switches: '+switches+' / source total: '+total+' ('+ratio.toFixed(2)+'%).</div><div class="detail-card"><div class="chart-title">Source cumulative writes</div>'+legend([["src1","#d62728"],["src2","#1f77b4"],["src1→src2","#2ca02c"],["src2→src1","#9467bd"]])+'<div id="'+src+'" class="chart"></div></div><div class="detail-card"><div class="chart-title">Destination cumulative writes</div>'+legend([["dst1","#d62728"],["dst2","#1f77b4"]])+'<div id="'+dst+'" class="chart"></div></div><div class="numa">'+w.numa.join("\\n")+'</div>'}else html+='<div class="drawer-note">No usable numa_write_counts sample found for this worker.</div>';body.innerHTML=html;if(w.writes.time.length){Plotly.newPlot(src,[trace(w.writes.time,w.writes.src1,"src1","#d62728"),trace(w.writes.time,w.writes.src2,"src2","#1f77b4"),trace(w.writes.time,w.writes.src1Src2,"src1→src2","#2ca02c"),trace(w.writes.time,w.writes.src2Src1,"src2→src1","#9467bd")],layout("write count",true),{responsive:true});Plotly.newPlot(dst,[trace(w.writes.time,w.writes.dst1,"dst1","#d62728"),trace(w.writes.time,w.writes.dst2,"dst2","#1f77b4")],layout("write count",false),{responsive:true});detailIds.push(src,dst);link(src,detailIds);link(dst,detailIds)}document.getElementById("drawer").classList.add("open");document.getElementById("backdrop").classList.add("open")}
function closeDrawer(){document.getElementById("drawer").classList.remove("open");document.getElementById("backdrop").classList.remove("open")}
function init(){const reports=document.getElementById("reports"),elapsedReports=document.getElementById("elapsed-reports");if(typeof Plotly==="undefined"){reports.textContent="Plotly failed to load; check network access to the configured Plotly CDN.";return}workers.forEach((w,i)=>{const id="inflight_"+i,elapsedId="elapsed_"+i,totalId="total_inflight_"+i,totalInflight=w.inflight.src1.map((v,j)=>v+w.inflight.src2[j]),card=document.createElement("section"),elapsedCard=document.createElement("section");card.className="card";card.innerHTML='<div class="card-head"><h2>'+w.name+'</h2><button class="drawer-button">NUMA write details</button></div><div class="chart-title">Inflight writes by source chip</div>'+legend([["chip 1","#d62728"],["chip 2","#1f77b4"]])+'<div id="'+id+'" class="chart"></div><div class="stats">'+stats("chip 1",w.inflight.src1)+stats("chip 2",w.inflight.src2)+'</div>';elapsedCard.className="card";elapsedCard.innerHTML='<div class="card-head"><h2>'+w.name+'</h2></div><div class="chart-title">URMA total elapsed</div>'+legend([["totalElapsedMs","#ff7f0e"]])+'<div id="'+elapsedId+'" class="chart"></div><div class="stats">'+stats("totalElapsedMs",w.elapsed.totalMs)+'</div><div class="chart-title">Total inflight</div>'+legend([["chip 1 + chip 2","#17becf"]])+'<div id="'+totalId+'" class="chart"></div><div class="stats">'+stats("total inflight",totalInflight)+'</div>';card.querySelector("button").addEventListener("click",()=>openDrawer(i));reports.append(card);elapsedReports.append(elapsedCard);Plotly.newPlot(id,[trace(w.inflight.time,w.inflight.src1,"chip 1","#d62728"),trace(w.inflight.time,w.inflight.src2,"chip 2","#1f77b4")],layout("inflight count",true),{responsive:true});Plotly.newPlot(elapsedId,[trace(w.elapsed.time,w.elapsed.totalMs,"totalElapsedMs","#ff7f0e")],layout("milliseconds",true),{responsive:true});Plotly.newPlot(totalId,[trace(w.inflight.time,totalInflight,"chip 1 + chip 2","#17becf")],layout("inflight count",true),{responsive:true});mainIds.push(id);elapsedIds.push(elapsedId,totalId);link(id,mainIds);link(elapsedId,elapsedIds);link(totalId,elapsedIds)});document.querySelectorAll(".tab").forEach(tab=>tab.addEventListener("click",()=>{document.querySelectorAll(".tab").forEach(x=>x.classList.toggle("active",x===tab));document.querySelectorAll(".tab-page").forEach(x=>x.classList.toggle("active",x.id===tab.dataset.page));window.dispatchEvent(new Event("resize"))}))}
document.getElementById("drawer-close").addEventListener("click",closeDrawer);document.getElementById("backdrop").addEventListener("click",closeDrawer);init();'''.replace("__PAYLOAD__", payload)
    html = '<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>URMA inflight report</title><script src="https://cdn.plot.ly/plotly-3.1.0.min.js"></script><style>' + css + '</style><header class="header"><h1>URMA Inflight and NUMA Write Report</h1><div class="meta"><span>workers: ' + str(len(workers)) + '</span><span>worker order is consistent across tabs</span></div></header><main class="container"><div class="tabs"><button class="tab active" data-page="inflight-page">Inflight &amp; NUMA</button><button class="tab" data-page="elapsed-page">Elapsed correlation</button></div><section id="inflight-page" class="tab-page active"><div class="hint">Inflight writes by source chip. Open details for NUMA write counters.</div><div id="reports"></div></section><section id="elapsed-page" class="tab-page"><div class="hint">For every worker, compare totalElapsedMs with total inflight on the same time window. The y axes remain independent.</div><div id="elapsed-reports"></div></section></main><div id="backdrop" class="backdrop"></div><aside id="drawer" class="drawer"><div class="drawer-head"><h2 id="drawer-title" class="drawer-title">NUMA write details</h2><button id="drawer-close" class="close">Close</button></div><div id="drawer-body"></div></aside><script>' + script + '</script>'
    with open(out_path, "w", encoding="utf-8") as stream:
        stream.write(html)
    return out_path


# ---------------------------- Main flow ----------------------------

def _median_dt(seqs):
    """Return the median of an ascending datetime sequence."""
    m = len(seqs)
    mid = m // 2
    if m % 2 == 1:
        return seqs[mid]
    return seqs[mid - 1] + (seqs[mid] - seqs[mid - 1]) / 2


def downsample_series(times, value_series, n):
    """Aggregate aligned value series by mean value and median timestamp."""
    if n <= 1 or len(times) == 0:
        return times, value_series
    from statistics import mean
    if any(len(values) != len(times) for values in value_series):
        raise ValueError("all value series must be aligned with timestamps")
    sampled_times = []
    sampled_values = [[] for _ in value_series]
    i = 0
    while i < len(times):
        j = min(i + n, len(times))
        seg_t = times[i:j]
        sampled_times.append(_median_dt(seg_t))
        for output, values in zip(sampled_values, value_series):
            output.append(mean(values[i:j]))
        i = j
    return sampled_times, sampled_values


def downsample(times, c1, c2, n):
    """Aggregate the two inflight source-chip series."""
    sampled_times, sampled_values = downsample_series(times, [c1, c2], n)
    return sampled_times, sampled_values[0], sampled_values[1]


def scan_one_worker(name, wdir, ds=1):
    times, c1, c2, elapsed_ms, numa, write_counts, diagnostics = scan_worker(wdir)
    if ds and ds > 1:
        times, (c1, c2, elapsed_ms) = downsample_series(times, [c1, c2, elapsed_ms], ds)
    if not times:
        return None
    return name, times, c1, c2, elapsed_ms, fmt_numa(numa), write_counts, diagnostics


def list_output_basename(root):
    """Return a collision-resistant filename stem for a --list entry."""
    normalized = os.path.normpath(root)
    if normalized == ".":
        normalized = "current"
    normalized = normalized.replace(os.sep, "_")
    normalized = normalized.replace("..", "parent")
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", normalized)
    display_name = normalized.strip("_") or "root"
    digest = hashlib.sha256(root.encode("utf-8")).hexdigest()[:12]
    return f"{display_name}-{digest}"


def main():
    parser = argparse.ArgumentParser(
        description="Plot URMA srcChipInflight time-series HTML reports from worker logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("-f", "--file", metavar="DIR", help="Worker-log root with direct worker subdirectories.")
    src.add_argument("--list", metavar="FILE", help="Text file with one worker-log root per line.")
    parser.add_argument("-o", "--out", default=None,
                        help="HTML path for -f (default: urma_inflight.html), or output directory for --list.")
    parser.add_argument("--prefix", default=None,
                        help="Comma-separated worker-name prefixes to include (for example workerA,workerB).")
    parser.add_argument("-j", "--jobs", type=int, default=None, help="Worker scan threads (default: CPU count).")
    parser.add_argument("--downsample", type=int, default=1, metavar="N",
                        help="Aggregate every N samples; N <= 1 disables downsampling (default: 1).")
    args = parser.parse_args()

    if args.jobs is not None and args.jobs < 1:
        parser.error("--jobs must be at least 1")

    prefixes = None
    if args.prefix:
        prefixes = [p.strip() for p in args.prefix.split(",") if p.strip()]

    # Collect root directories.
    if args.list:
        if not os.path.isfile(args.list):
            print(f"error: list file does not exist: {args.list}", file=sys.stderr)
            sys.exit(1)
        with open(args.list, "r", encoding="utf-8") as f:
            roots = [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    else:
        roots = [args.file]

    out_dir = args.out if args.out and args.out != "-" else "."
    if args.list and out_dir and not args.out:
        out_dir = "."

    multiple = bool(args.list)
    report_paths = []

    for root in roots:
        if not os.path.isdir(root):
            print(f"warning: skipping non-directory: {root}", file=sys.stderr)
            continue
        workers = find_workers(root, prefixes)
        if not workers:
            print(f"warning: no matching worker directories under {root} (prefixes={prefixes})", file=sys.stderr)
            continue

        n_workers = len(workers)
        jobs = args.jobs or min(n_workers, (os.cpu_count() or 4))
        root_name = os.path.basename(os.path.normpath(root))
        # Display root name only when several roots are supplied.
        disp_workers = [(f"{root_name}/{n}" if len(roots) > 1 else n, d) for n, d in workers]

        ds = args.downsample
        ds_tag = f", downsample={ds}" if ds and ds > 1 else ""
        print(f"[{root}] workers={n_workers}, scan_threads={jobs}{ds_tag}")
        data = []
        if jobs <= 1 or n_workers == 1:
            for name, wdir in disp_workers:
                result = scan_one_worker(name, wdir, ds)
                if result is not None:
                    data.append(result)
        else:
            with ThreadPoolExecutor(max_workers=jobs) as ex:
                futs = {ex.submit(scan_one_worker, name, wdir, ds): name
                        for name, wdir in disp_workers}
                for fut in as_completed(futs):
                    result = fut.result()
                    if result is not None:
                        data.append(result)
            order = {name: i for i, (name, _) in enumerate(disp_workers)}
            data.sort(key=lambda d: order[d[0]])

        for name, times, c1, c2, _, _, write_counts, diagnostics in data:
            print(f"  - {name}: URMA_ELAPSED_TOTAL samples={len(times)}, "
                  f"chip1 max={max(c1) if c1 else 0}, chip2 max={max(c2) if c2 else 0}, "
                  f"numa snapshots={diagnostics['parsed_snapshots']}/{diagnostics['snapshot_lines']}")

        if not data:
            print(f"warning: no URMA_ELAPSED_TOTAL samples found under {root}", file=sys.stderr)
            continue

        if multiple:
            os.makedirs(out_dir, exist_ok=True)
            base = list_output_basename(root)
            out_path = os.path.join(out_dir, base + ".html")
        else:
            out_path = args.out or "urma_inflight.html"
            if not out_path.endswith(".html"):
                out_path += ".html"

        path = write_html_report(out_path, data)
        report_paths.append(path)
        print(f"  -> {path}")

    print(f"\ncomplete: generated {len(report_paths)} HTML report(s)")


if __name__ == "__main__":
    main()

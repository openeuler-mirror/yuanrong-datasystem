#!/usr/bin/env python3
"""Render independent write diagnosis from an existing bottleneck model."""
from __future__ import annotations


import argparse
import json
import pathlib

from .analysis.write import RULES, refine, build_model
from .rendering.write import render_model


def render_html(analysis, title, links=()):
    model = build_model(analysis)
    return render_model(model, title, links, analysis.get("metadata", {}).get("raw_input_archives", [])), model


def write_outputs(analysis_json, output, *, title="写入瓶颈分析", links=()):
    if output.parent.resolve() != analysis_json.parent.resolve():
        raise ValueError(
            "Render beside the analysis JSON; use ds_trace_analysis.py package to relocate dependencies"
        )
    refined_output = output.with_name("write.refined.analysis.json")
    paths = [analysis_json, output, refined_output]
    for i, path in enumerate(paths):
        for other in paths[:i]:
            same_file = path.resolve() == other.resolve()
            if not same_file and path.exists() and other.exists():
                same_file = path.samefile(other)
            if same_file:
                raise ValueError("analysis input, HTML output and refined JSON must be distinct files")
    analysis = json.loads(analysis_json.read_text(encoding="utf-8"))
    page, model = render_html(
        analysis, title, [(label, url) for label, url in links if url]
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page, encoding="utf-8")
    refined_output.write_text(
        json.dumps(model, ensure_ascii=False), encoding="utf-8"
    )
    return output.resolve(), refined_output.resolve()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--analysis-json", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--title", default="写入瓶颈分析")
    parser.add_argument("--read-report")
    parser.add_argument("--triage-report")
    parser.add_argument("--numa-report")
    args = parser.parse_args()
    links = [("读取瓶颈", args.read_report), ("Trace Triage", args.triage_report), ("NUMA分析", args.numa_report)]
    try:
        write_outputs(args.analysis_json, args.output, title=args.title, links=links)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()

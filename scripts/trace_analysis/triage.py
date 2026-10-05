#!/usr/bin/env python3
"""Self-verifying DataSystem slow/error trace triage.

The analyzer accepts plain log files, directories, and tar bundles.
It groups lines by trace id, then produces JSON/Markdown summaries across time,
worker, access flow, latency, breakdown, RPC slow, URMA elapsed, and errors.
"""

import argparse
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .resources import asset_path, tool_fingerprint
from .analysis.triage_builder import TraceDimensionSections, TraceDimensionBuilder, _surface_status
from .analysis.triage_accumulator import (
    TraceAccumulator as _TraceAccumulator, DEFAULT_MAX_EVIDENCE_PER_TRACE,
    _add_metric, _classify, _stage, _build_stage_breakdown, _evidence_coverage,
)
from .analysis.triage_stats import _percentiles
from .analysis.triage_flow import (
    _ips_from_text, _flow_stage_rollup, _flow_edge_summary,
    _flow_candidate_edges,
)
from .analysis.triage_ub import (
    _ub_role, _add_lifecycle_metric, _parse_chip_inflight,
)
from .ingest import inventory as _triage_inventory
from .orchestration import store as _triage_store
from .orchestration.contracts import RunOptions, ParseOutputBundle
from .ingest.inventory import (
    DEFAULT_MAX_TAR_MEMBERS,
    DEFAULT_MAX_TAR_MEMBER_BYTES,
    DEFAULT_MAX_TAR_TOTAL_BYTES,
    DERIVED_TRACE_FILE_RE,
    NOISE_OFF_LABEL,
    NOISE_ON_LABEL,

    has_noise_token as _has_noise_token,
    is_noise_off as _is_noise_off,
    is_noise_on as _is_noise_on,
    noise_context_for_path as _noise_context_for_path,
    detect_noise_cohort_mode as _detect_noise_cohort_mode,
    iter_input_leaf_paths as _iter_input_leaf_paths,
    duplicate_input_basenames as _duplicate_input_basenames,
    source_cohort_label as _source_cohort_label,
    slug as _slug,
    preserved_input_name as _preserved_input_name,
    input_identity as _input_identity,
    safe_member_path as _safe_member_path,
    write_inputs_doc as _write_inputs_doc,
)
from .orchestration.store import (
    find_cached_run as _find_cached_run,
    write_json as _write_json,
    read_json as _read_json,
    update_manifest as _update_manifest,
    new_run_dir as _new_run_dir,
)
from .rendering import triage as _triage_rendering
from .ingest import triage as _triage_ingest
from .ingest.triage import (
    TRACE_ID_MAX_SIZE,
    TRACE_ID_CHARS,
    TRACE_ID_FIELD_RE,
    TRACE_ID_EXPLICIT_RE,
    TRACE_ID_PREFIXED_SHORT_UUID_RE,
    TRACE_ID_UUID_RE,
    TS_RE,
    POD_NAME_RE,
    WORKER_POD_NAME_RE,
    IP_RE,
    ACCESS_RE,
    BREAKDOWN_BLOCK_RE,
    BREAKDOWN_ITEM_RE,
    RPC_SLOW_RE,
    RPC_SLOW_FIELD_RE,
    LATENCY_SUMMARY_RE,
    SUMMARY_ITEM_RE,
    URMA_TOTAL_RE,
    URMA_POLL_RE,
    URMA_NOTIFY_RE,
    URMA_THREAD_RE,
    URMA_THREAD_LOOP_GAP_RE,
    URMA_PERF_RE,
    REQUEST_ID_RE,
    SRC_ADDR_RE,
    DST_ADDR_RE,
    DATA_SIZE_RE,
    CPUID_RE,
    STATUS_RE,
    WAIT_OS_RE,
    INFLIGHT_WR_RE,
    FIRST_WRITE_WAKE_SCHED_RE,
    SECOND_WRITE_WAKE_SCHED_RE,
    WRITE_WAKE_SCHED_RE,
    LEGACY_WAKE_SCHED_RE,
    SRC_CHIP_INFLIGHT_RE,
    SLEEP_TARGET_RE,
    TRANSFER_PATH_RE,
    INFLIGHT_REMOTE_GET_RE,
    REMOTE_GET_REQUEST_RE,
    RPC_MAX_CONCURRENCY_ERROR,
    RPC_MAX_CONCURRENCY_RE,
    ERROR_PATTERNS,
    BUILTIN_CUSTOM_METRIC_RULES,
    UbLineContext,
    ParserRules,
    UrmaFieldParser,
    _line_host_ip,
    _ms,
    _first_match,
    _int_match,
)


_LOG = logging.getLogger("ds_trace_triage")


class TraceTriageError(Exception):
    """CLI-facing fatal error."""


def _resolve_run_options(options=None, legacy_args=(), legacy_kwargs=None):
    if isinstance(options, RunOptions):
        if legacy_args or legacy_kwargs:
            raise TypeError("RunOptions cannot be combined with legacy run arguments")
        return options

    names = ("case_name", "scenario", "code_ref", "force", "allow_partial_inputs")
    values = dict(zip(names, ("trace-case", "", "unknown", False, False)))
    positional = (() if options is None else (options,)) + tuple(legacy_args)
    if len(positional) > len(names):
        raise TypeError(f"expected at most {len(names)} legacy run arguments")

    assigned = set()
    for name, value in zip(names, positional):
        values[name] = value
        assigned.add(name)
    for name, value in (legacy_kwargs or {}).items():
        if name not in values:
            raise TypeError(f"unexpected run option: {name}")
        if name in assigned:
            raise TypeError(f"multiple values for run option: {name}")
        values[name] = value
    return RunOptions(**values)


@dataclass
class AnalyzerDeps:
    reader: Any = None
    parser: Any = None
    rules: Any = None


DEFAULT_SITE_HTML_MAX_BYTES = 2 * 1024 * 1024
PUBLISH_HOST_ENV = "DS_TRACE_TRIAGE_PUBLISH_HOST"
PUBLISH_ROOT_ENV = "DS_TRACE_TRIAGE_PUBLISH_ROOT"
PUBLISH_BASE_URL_ENV = "DS_TRACE_TRIAGE_PUBLISH_BASE_URL"
DEFAULT_SITE_PUBLIC_BASE_URL = "https://yche.me/perf"


DEFAULT_RULES = ParserRules()


URMA_FIELDS = UrmaFieldParser()


class TraceInputReader(_triage_inventory.TraceInputReader):
    """Resolve legacy budget settings at the point of each archive read."""

    def __init__(self):
        super().__init__(budget_validator=_check_tar_budget)


class TraceParser(_triage_ingest.TraceParser):
    """Bind legacy module-local parser hooks without sharing mutable rules."""

    def __init__(self, rules=None):
        super().__init__(rules or DEFAULT_RULES, event_extractor=_extract_ub_events,
                         host_ip_parser=_line_host_ip, to_ms=_ms)


def register_error_pattern(pattern):
    """Register a literal error marker for evolved DataSystem log wording."""
    DEFAULT_RULES.register_error_pattern(pattern)


def register_metric_rule(name, pattern, value_group=1, unit_group=None):
    """Register a custom latency metric extracted as ms from a regex match."""
    DEFAULT_RULES.register_metric_rule(name, pattern, value_group=value_group, unit_group=unit_group)


def _iter_input_lines(paths):
    yield from TraceInputReader().iter_lines(paths)


def _iter_file(path):
    yield from TraceInputReader().iter_file(path)


def _worker_from(source, member, line):
    return TraceParser().worker_from(source, member, line)


def _timestamp(line):
    return TraceParser().timestamp(line)


def _input_budget():
    return _triage_inventory.TarBudget(DEFAULT_MAX_TAR_MEMBERS, DEFAULT_MAX_TAR_TOTAL_BYTES,
                                      DEFAULT_MAX_TAR_MEMBER_BYTES)


def _check_tar_budget(path, member, member_count, total_bytes):
    return _input_budget().check(path, member, member_count, total_bytes)


def _extract_ub_events(ctx: UbLineContext):
    return _triage_ingest.extract_ub_events(ctx, field_parser=URMA_FIELDS)


class TraceAccumulator(_TraceAccumulator):
    """Keep the legacy evidence cap configurable at the public module boundary."""

    def __init__(self, paths):
        super().__init__(paths, max_evidence_per_trace=DEFAULT_MAX_EVIDENCE_PER_TRACE)


class TraceAnalyzer:
    """Coordinate input reading, line parsing, trace accumulation, and dimensions."""

    def __init__(self, reader=None, parser=None, rules=None, accumulator_cls=None, dimension_builder=None):
        self.rules = rules or DEFAULT_RULES
        self.reader = reader or TraceInputReader()
        self.parser = parser or TraceParser(self.rules)
        self.accumulator_cls = accumulator_cls or TraceAccumulator
        self.dimension_builder = dimension_builder or TraceDimensionBuilder()
        self.accumulator = None

    def analyze(self, paths, code_ref="unknown", allow_partial_inputs=False):
        self.reader.failures.clear()
        self.accumulator = self.accumulator_cls(paths)
        for source, member, line_no, line in self.reader.iter_lines(paths):
            parsed = self.parser.parse_line(source, member, line_no, line)
            if parsed:
                self.accumulator.ingest(parsed, line)
        if self.reader.failures and not allow_partial_inputs:
            failures = "; ".join(f"{item['path']}:{item['error']}" for item in self.reader.failures[:5])
            raise TraceTriageError(
                f"Failed to read trace input(s): {failures}. "
                "Use --allow-partial-inputs for best-effort analysis."
            )
        snapshot = self.accumulator.finish()
        try:
            return self.dimension_builder.build(
                snapshot, paths, code_ref=code_ref, input_failures=self.reader.failures
            )
        except TypeError as exc:
            message = str(exc)
            if "input_failures" not in message and "unexpected keyword" not in message:
                raise
            return self.dimension_builder.build(snapshot, paths, code_ref=code_ref)


def _analyze_inputs(paths, options=None, deps=None):
    options = options or RunOptions(code_ref="unknown")
    deps = deps or AnalyzerDeps()
    return TraceAnalyzer(reader=deps.reader, parser=deps.parser, rules=deps.rules).analyze(
        paths, code_ref=options.code_ref, allow_partial_inputs=options.allow_partial_inputs
    )


def analyze_inputs(paths, code_ref="unknown", allow_partial_inputs=False):
    return TraceAnalyzer().analyze(paths, code_ref=code_ref, allow_partial_inputs=allow_partial_inputs)


def render_markdown(report):
    return _triage_rendering.render_markdown(report)


def _script_version():
    return tool_fingerprint()[:16]


def _cache_key(inputs, code_ref, case_name, scenario, rules_fingerprint="default"):
    return _triage_store.build_cache_key(inputs, code_ref, case_name, scenario, rules_fingerprint,
                                   identity_provider=_input_identity, version_provider=_script_version)


def _preserve_raw_inputs(inputs, run_dir):
    return _triage_inventory.preserve_raw_inputs(inputs, run_dir, budget=_input_budget())


def _site_publish_config(require_target=False):
    host = os.environ.get(PUBLISH_HOST_ENV, "").strip()
    root = os.environ.get(PUBLISH_ROOT_ENV, "").strip().rstrip("/")
    base_url = os.environ.get(PUBLISH_BASE_URL_ENV, DEFAULT_SITE_PUBLIC_BASE_URL).strip().rstrip("/")
    if require_target and (not host or not root):
        raise TraceTriageError(
            f"Set {PUBLISH_HOST_ENV} and {PUBLISH_ROOT_ENV} before real publish; "
            f"{PUBLISH_BASE_URL_ENV} is optional."
        )
    return {
        "host": host or "<publish-host>",
        "root": root or "<publish-root>/perf",
        "base_url": base_url,
        "is_configured": bool(host and root),
    }


def _write_site_publish_doc(run_dir, manifest):
    run_dir = Path(run_dir)
    filename = f"{run_dir.name}.html"
    publish_config = _site_publish_config()
    remote_path = f"{publish_config['root']}/{filename}"
    url = f"{publish_config['base_url']}/{filename}"
    source_html = run_dir / "report.site.html"
    source_size = source_html.stat().st_size if source_html.exists() else 0
    lines = [
        "# yche.me Publish Checklist",
        "",
        f"- case_name: `{manifest.get('case_name', '')}`",
        f"- scenario: `{manifest.get('scenario', '')}`",
        f"- source_html: `report.site.html`",
        f"- source_size_bytes: `{source_size}`",
        f"- default_publish_limit_bytes: `{DEFAULT_SITE_HTML_MAX_BYTES}`",
        f"- target_host: `{publish_config['host']}`",
        f"- target_path: `{remote_path}`",
        f"- url: `{url}`",
        f"- configured: `{publish_config['is_configured']}`",
        "",
        "## Commands",
        "",
        "```bash",
        f"export {PUBLISH_HOST_ENV}=<publish-host>",
        f"export {PUBLISH_ROOT_ENV}=<publish-root>/perf",
        f"export {PUBLISH_BASE_URL_ENV}={publish_config['base_url']}",
        f"scp report.site.html ${{{PUBLISH_HOST_ENV}}}:${{{PUBLISH_ROOT_ENV}}}/{filename}",
        f"curl -fsSI {url}",
        "```",
        "",
        "## Index Registration",
        "",
        "- Real publish is not complete until the site catalog is updated.",
        "- Add or update one `var P` entry in `<publish-root>/index.html` for "
        f"`perf/{filename}`; keep the edit minimal and preserve existing entries.",
        "- Run an `index.html` JavaScript syntax check after editing, then verify the "
        "new report URL and catalog entry over HTTPS.",
        "",
        "## Verification",
        "",
        "- HTTP status should be 200.",
        (
            "- Open the URL and verify navigation, ECharts, provenance, coverage, "
            "trace filters, downloads, and selected logs."
        ),
        (
            "- Do not publish oversized throw-away pages; pass `--max-site-html-mb` only after "
            "reviewing why the page is large."
        ),
    ]
    (run_dir / "site_publish.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"publish_doc": "site_publish.md", "target_path": remote_path, "url": url}


def _render_html(report, title, site=False, manifest=None):
    return _triage_rendering.render_html(
        report, title, site=site, manifest=manifest, asset_resolver=asset_path
    )


def _build_events(report):
    return _triage_rendering.build_events(report)


def _build_triage(report):
    return _triage_rendering.build_triage(report)


class TraceReportRenderer(_triage_rendering.TraceReportRenderer):
    """Keep legacy function injection at the old module boundary."""

    @staticmethod
    def events(report):
        return _build_events(report)

    @staticmethod
    def triage(report):
        return _build_triage(report)

    @staticmethod
    def markdown(report):
        return render_markdown(report)

    @staticmethod
    def html(report, title, site=False, manifest=None):
        return _render_html(report, title, site=site, manifest=manifest)


class TraceRunStore(_triage_store.TraceRunStore):
    """Retain legacy configuration and publication hooks at the entry boundary."""

    def __init__(self):
        super().__init__(version_provider=_script_version)

    @staticmethod
    def prepare_parse_run(inputs, out_dir, options, rules_fingerprint="default"):
        inventory = _triage_inventory.TraceInputInventory(budget=_input_budget())
        return _triage_store.TraceRunStore(inventory, _script_version).prepare_parse_run(
            inputs, out_dir, options, rules_fingerprint
        )

    @staticmethod
    def write_site_publish_doc(run_dir, manifest):
        return _write_site_publish_doc(run_dir, manifest)


class TraceSitePublisher:
    """Publish site reports with size guard and live marker verification."""

    def __init__(self, store=None, pipeline_factory=None):
        self.store = store or TraceRunStore()
        self.pipeline_factory = pipeline_factory or TraceRunPipeline

    @staticmethod
    def size_guard(source_html, max_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
        source_html = Path(source_html)
        size = source_html.stat().st_size
        return {
            "source_size_bytes": size,
            "max_site_html_bytes": max_bytes,
            "size_status": "ok" if size <= max_bytes else "too_large",
        }

    def publish(self, run_dir, dry_run=True, max_site_html_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
        run_dir = Path(run_dir)
        manifest = self.store.read_json(run_dir / "manifest.json")
        site_target = manifest.get("render_targets", {}).get("site", {})
        if not (run_dir / site_target.get("path", "report.site.html")).exists():
            self.pipeline_factory(store=self.store, site_publisher=self).render_site(run_dir)
            manifest = self.store.read_json(run_dir / "manifest.json")
            site_target = manifest.get("render_targets", {}).get("site", {})
        if not (run_dir / site_target.get("publish_doc", "site_publish.md")).exists():
            publish_doc = self.store.write_site_publish_doc(run_dir, manifest)
            site_target = {**site_target, **publish_doc}
        source_html = run_dir / site_target.get("path", "report.site.html")
        size_guard = self.size_guard(source_html, max_site_html_bytes)
        live_markers = "not-run"
        if dry_run:
            status = "dry-run"
        elif size_guard["size_status"] != "ok":
            status = "blocked-size-limit"
            self._record_publish(run_dir, site_target, status, live_markers, size_guard)
            raise TraceTriageError(
                f"Refuse to publish oversized site HTML: {source_html} is "
                f"{size_guard['source_size_bytes']} bytes > {max_site_html_bytes} bytes. "
                "Review the report or raise --max-site-html-mb intentionally."
            )
        else:
            self._copy_and_verify(source_html, site_target)
            live_markers = "verified"
            status = "copied"
        self._record_publish(run_dir, site_target, status, live_markers, size_guard)
        return site_target.get("url", "")

    @staticmethod
    def _copy_and_verify(source_html, site_target):
        publish_config = _site_publish_config(require_target=True)
        target_path = site_target.get("target_path", "")
        url = site_target.get("url", "")
        if target_path.startswith("<publish-root>"):
            filename = Path(target_path).name
            target_path = f"{publish_config['root']}/{filename}"
        scp_bin = shutil.which("scp") or "/usr/bin/scp"
        curl_bin = shutil.which("curl") or "/usr/bin/curl"
        subprocess.run(
            [scp_bin, str(source_html), f"{publish_config['host']}:{target_path}"],
            check=True,
        )
        subprocess.run([curl_bin, "-fsSI", url], check=True)
        result = subprocess.run(
            [curl_bin, "-fsSL", "-A", "Mozilla/5.0", url],
            check=True,
            capture_output=True,
            text=True,
        )
        for marker in [
            "Trace 分析报告",
            'id="coverage-table"',
            'id="flow-stage-chart"',
            'id="download-report-summary"',
            "/assets/css/site.css",
            "/assets/js/site.js",
        ]:
            if not (marker in result.stdout):
                raise AssertionError(marker)

    def _record_publish(self, run_dir, site_target, status, live_markers, size_guard):
        self.store.update_manifest(run_dir, lambda item: item["render_targets"]["site"].update({
            **site_target,
            "publish": {
                "status": status,
                "catalog_status": "not-registered" if status == "copied" else "not-run",
                "url": site_target.get("url", ""),
                "target_path": site_target.get("target_path", ""),
                "live_markers": live_markers,
                **size_guard,
            },
        }))


class TraceRunPipeline:
    """Manage staged run directories, cache, manifest state, and render targets."""

    def __init__(self, analyzer=None, renderer=None, store=None, site_publisher=None):
        self.analyzer = analyzer or TraceAnalyzer()
        self.renderer = renderer or TraceReportRenderer()
        self.store = store or TraceRunStore()
        self.site_publisher = site_publisher or TraceSitePublisher(self.store)

    def parse(self, inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
        options = _resolve_run_options(options, legacy_args, legacy_kwargs)
        rules_fingerprint = (
            self.analyzer.rules.fingerprint()
            if hasattr(self.analyzer.rules, "fingerprint")
            else "default"
        )
        prepared = self.store.prepare_parse_run(
            inputs, out_dir, options, rules_fingerprint=rules_fingerprint
        )
        if prepared["cached"]:
            return prepared["run_dir"]
        run_dir = prepared["run_dir"]
        report = self.analyzer.analyze(
            inputs,
            code_ref=options.code_ref,
            allow_partial_inputs=options.allow_partial_inputs,
        )
        report["run_scope"] = options.run_id
        report["input_scope"] = prepared["cache_key"] if options.run_id is None else None
        events = self.renderer.events(report)
        bundle = ParseOutputBundle(
            report=report,
            events=events,
            created_at=prepared["created_at"],
            cache_key=prepared["cache_key"],
            identities=prepared["identities"],
        )
        self.store.write_parse_outputs(run_dir, options, bundle)
        return run_dir

    def aggregate(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "parsed_traces.json")
        self.store.write_json(run_dir / "summary.json", report)
        self.store.update_manifest(run_dir, lambda manifest: manifest["stages"].update({
            "aggregate": {"status": "done", "path": "summary.json"}
        }))
        return run_dir / "summary.json"

    def triage(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "summary.json")
        triage = self.renderer.triage(report)
        self.store.write_json(run_dir / "triage.json", triage)
        self.store.write_text(run_dir / "triage.md", self.renderer.markdown(report))
        self.store.update_manifest(run_dir, lambda manifest: manifest["stages"].update({
            "triage": {"status": "done", "path": "triage.json", "markdown": "triage.md"}
        }))
        return run_dir / "triage.json"

    def render_local(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "summary.json")
        manifest = self.store.read_json(run_dir / "manifest.json")
        title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
        self.store.write_text(run_dir / "report.local.html", self.renderer.html(report, title, manifest=manifest))
        self.store.update_manifest(run_dir, lambda item: item["render_targets"].update({
            "local": {"path": "report.local.html", "status": "generated"}
        }))
        return run_dir / "report.local.html"

    def render_site(self, run_dir):
        run_dir = Path(run_dir)
        report = self.store.read_json(run_dir / "summary.json")
        manifest = self.store.read_json(run_dir / "manifest.json")
        title = f"Trace Triage: {manifest.get('case_name', 'trace-case')}"
        self.store.write_text(run_dir / "report.site.html",
                              self.renderer.html(report, title, site=True, manifest=manifest))
        publish_doc = self.store.write_site_publish_doc(run_dir, manifest)
        self.store.update_manifest(run_dir, lambda item: item["render_targets"].update({
            "site": {"path": "report.site.html", "status": "generated", **publish_doc}
        }))
        return run_dir / "report.site.html"

    def run(self, inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
        options = _resolve_run_options(options, legacy_args, legacy_kwargs)
        run_dir = self.parse(inputs, out_dir, options=options)
        if not (run_dir / "summary.json").exists():
            self.aggregate(run_dir)
        if not (run_dir / "triage.json").exists():
            self.triage(run_dir)
        if not (run_dir / "report.local.html").exists():
            self.render_local(run_dir)
        if not (run_dir / "report.site.html").exists():
            self.render_site(run_dir)
        return run_dir


def parse_stage(inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
    return TraceRunPipeline().parse(inputs, out_dir, options, *legacy_args, **legacy_kwargs)


def aggregate_stage(run_dir):
    return TraceRunPipeline().aggregate(run_dir)


def triage_stage(run_dir):
    return TraceRunPipeline().triage(run_dir)


def render_local_stage(run_dir):
    return TraceRunPipeline().render_local(run_dir)


def render_site_stage(run_dir):
    return TraceRunPipeline().render_site(run_dir)


def _publish_size_guard(source_html, max_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
    return TraceSitePublisher().size_guard(source_html, max_bytes)


def publish_site_stage(run_dir, dry_run=True, max_site_html_bytes=DEFAULT_SITE_HTML_MAX_BYTES):
    return TraceSitePublisher().publish(run_dir, dry_run=dry_run, max_site_html_bytes=max_site_html_bytes)


def _verify_html_inline_script(html_path):
    html_path = Path(html_path)
    html = html_path.read_text(encoding="utf-8")
    match = re.search(r"<script>\n  const report = (.*)\n  </script>", html, re.S)
    if not (match):
        raise AssertionError('inline report script not found')
    node = shutil.which("node")
    if not node:
        return "inline-script-present"
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as tmp:
        tmp.write("const report = " + match.group(1))
        tmp_path = Path(tmp.name)
    try:
        subprocess.run([node, "--check", str(tmp_path)], check=True)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass
    return "node-check-passed"


def run_pipeline(inputs, out_dir, options=None, *legacy_args, **legacy_kwargs):
    return TraceRunPipeline().run(inputs, out_dir, options, *legacy_args, **legacy_kwargs)


def _make_self_test_bundle(path):
    trace_id = "019f7b27-56f0-74f0-9a68-5b3742f11e23"
    content = "\n".join([
        (
            f"2026-07-18T19:20:03.100000 | INFO | access_recorder | 192.0.2.10 | 42 | "
            f"{trace_id} | - | 0 | DS_KV_CLIENT_GET | 518923 | 4096"
        ),
        (
            f"2026-07-18T19:20:03.110000 | INFO | client | 192.0.2.10 | 42 | {trace_id} | "
            f"Get done latencySummary:{{client.rpc.get:20298, client.process.get:10}}"
        ),
        (
            f"2026-07-18T19:20:03.130000 | INFO | worker | 192.0.2.10 | 42 | {trace_id} | "
            f"[Get] Done, totalCost: 518.9ms, exceed 3ms: "
            f"{{ ProcessGetObjectRequest: 517 ms, QueryMeta: 0 ms }}"
        ),
        (
            f"2026-07-18T19:20:03.150000 | WARN | worker | 192.0.2.10 | 42 | {trace_id} | "
            f"[ZMQ_RPC_FRAMEWORK_SLOW] e2e_us=8012 client_req_framework_us=100 "
            f"remote_processing_us=7600 client_rsp_framework_us=120 "
            f"server_req_queue_us=20 server_exec_us=7500 server_rsp_queue_us=80 "
            f"network_residual_us=292 method=WorkerOCService.Get"
        ),
        (
            f"2026-07-18T19:20:03.200000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_TOTAL] cost 517.732ms, request id:77, "
            f"src address: 192.0.2.20, target address: 192.0.2.10, "
            f"dataSize:4194304, cpuid:12, status: OK"
        ),
        (
            f"2026-07-18T19:20:03.201000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_POLL_JFC] cost 0.309ms, request id:77"
        ),
        (
            f"2026-07-18T19:20:03.202000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_NOTIFY] cost 0.041ms, request id:77"
        ),
        (
            f"2026-07-18T19:20:03.203000 | WARN | worker | 192.0.2.20 | 42 | {trace_id} | "
            f"[URMA_ELAPSED_THREAD_SHED] cost 12.500ms, request id:77"
        ),
        (
            f"2026-07-18T19:20:03.230000 | ERROR | worker | 192.0.2.10 | 42 | {trace_id} | "
            f"RPC deadline exceeded while waiting WorkerOCService.Get"
        ),
    ])
    with tarfile.open(path, "w:gz") as tar:
        data = content.encode("utf-8")
        info = tarfile.TarInfo("kvchachjpworker-0-worker7/worker.log")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))


def run_self_test():
    with tempfile.TemporaryDirectory(prefix="ds-trace-triage-") as tmp:
        bundle = Path(tmp) / "fixture.tar.gz"
        _make_self_test_bundle(bundle)
        report = analyze_inputs([str(bundle)], code_ref="self-test")
        run_dir = run_pipeline(
            [str(bundle)],
            Path(tmp) / "runs",
            RunOptions(case_name="self-test", scenario="fixture", code_ref="self-test"),
        )
        if not ((run_dir / 'manifest.json').exists()):
            raise AssertionError("(run_dir / 'manifest.json').exists()")
        if not ((run_dir / 'inventory.json').exists()):
            raise AssertionError("(run_dir / 'inventory.json').exists()")
        if not ((run_dir / 'events.jsonl').exists()):
            raise AssertionError("(run_dir / 'events.jsonl').exists()")
        if not ((run_dir / 'summary.json').exists()):
            raise AssertionError("(run_dir / 'summary.json').exists()")
        if not ((run_dir / 'triage.json').exists()):
            raise AssertionError("(run_dir / 'triage.json').exists()")
        if not ((run_dir / 'report.local.html').exists()):
            raise AssertionError("(run_dir / 'report.local.html').exists()")
        if not ((run_dir / 'report.site.html').exists()):
            raise AssertionError("(run_dir / 'report.site.html').exists()")
        inline_status = _verify_html_inline_script(run_dir / "report.local.html")
        if inline_status not in {"inline-script-present", "node-check-passed"}:
            raise AssertionError(f"unexpected inline script status: {inline_status}")
        if not ((run_dir / 'site_publish.md').exists()):
            raise AssertionError("(run_dir / 'site_publish.md').exists()")
        publish_url = publish_site_stage(run_dir, dry_run=True)
        if not (publish_url.startswith('https://yche.me/perf/')):
            raise AssertionError("publish_url.startswith('https://yche.me/perf/')")
        if not ((run_dir / 'raw' / 'inputs' / '01-fixture.tar.gz').exists()):
            raise AssertionError("(run_dir / 'raw' / 'inputs' / '01-fixture.tar.gz').exists()")
        extracted_log = (
            run_dir / "raw" / "extracted" / "01-fixture.tar.gz"
            / "kvchachjpworker-0-worker7" / "worker.log"
        )
        if not extracted_log.exists():
            raise AssertionError(f"missing extracted worker log: {extracted_log}")
        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
        if not (manifest['render_targets']['site']['publish_doc'] == 'site_publish.md'):
            raise AssertionError("manifest['render_targets']['site']['publish_doc'] == 'site_publish.md'")
        if not (manifest['render_targets']['site']['publish']['status'] == 'dry-run'):
            raise AssertionError("manifest['render_targets']['site']['publish']['status'] == 'dry-run'")
        run_report = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
        run_trace = next(iter(run_report["traces"].values()))
        if not (run_trace['stage_breakdown']):
            raise AssertionError("run_trace['stage_breakdown']")
        if not (run_trace['evidence_coverage']['urma'] == 'present'):
            raise AssertionError("run_trace['evidence_coverage']['urma'] == 'present'")
        if not (run_report['dimensions']['coverage']['surfaces']['urma_elapsed']['status'] == 'present'):
            raise AssertionError("urma_elapsed coverage status is not present")
    if not (report['trace_count'] == 1):
        raise AssertionError("report['trace_count'] == 1")
    if not (report['dimensions']['latency_ms']['access']['p50'] == 518.923):
        raise AssertionError("report['dimensions']['latency_ms']['access']['p50'] == 518.923")
    if not (report['dimensions']['breakdown_ms']['ProcessGetObjectRequest']['sum'] == 517.0):
        raise AssertionError("report['dimensions']['breakdown_ms']['ProcessGetObjectRequest']['sum'] == 517.0")
    if not (report['dimensions']['urma_elapsed']['total']['p50'] == 517.732):
        raise AssertionError("report['dimensions']['urma_elapsed']['total']['p50'] == 517.732")
    if not (report['dimensions']['urma_elapsed']['poll_jfc']['p50'] == 0.309):
        raise AssertionError("report['dimensions']['urma_elapsed']['poll_jfc']['p50'] == 0.309")
    if not (report['dimensions']['rpc_slow']['WorkerOCService.Get']['server_exec_us']['p50'] == 7500):
        raise AssertionError("report['dimensions']['rpc_slow']['WorkerOCService.Get']['server_exec_us']['p50'] == 7500")
    if not (report['dimensions']['latency_summary_us']['client.rpc.get']['p50'] == 20298):
        raise AssertionError("report['dimensions']['latency_summary_us']['client.rpc.get']['p50'] == 20298")
    if not (report['dimensions']['errors']['RPC deadline exceeded'] == 1):
        raise AssertionError("report['dimensions']['errors']['RPC deadline exceeded'] == 1")
    if not (report['dimensions']['classifications']['client_deadline_with_urma_wait'] == 1):
        raise AssertionError("report['dimensions']['classifications']['client_deadline_with_urma_wait'] == 1")
    report["self_test"] = True
    return report


def main(argv=None):
    try:
        _LOG.handlers.clear()
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        _LOG.addHandler(handler)
        _LOG.setLevel(logging.INFO)
        _LOG.propagate = False
        argv = list(argv or sys.argv[1:])
        stage_commands = {"parse", "aggregate", "triage", "render-local", "render-site", "publish-site"}
        if argv and argv[0] in ({"run", "verify"} | stage_commands):
            command = argv.pop(0)
            if command == "verify":
                parser = argparse.ArgumentParser(description="Run built-in trace triage verification.")
                parser.add_argument("--output-json", help="Write machine-readable summary JSON.")
                args = parser.parse_args(argv)
                report = run_self_test()
                _LOG.info("verify passed")
                if args.output_json:
                    Path(args.output_json).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                                                      encoding="utf-8")
                return 0
            if command == "parse":
                parser = argparse.ArgumentParser(description="Parse trace inputs into a staged run directory.")
                parser.add_argument("inputs", nargs="+", help="Log files, directories, or tar bundles.")
                parser.add_argument("--out", required=True, help="Output root for timestamped run directories.")
                parser.add_argument("--case", default="trace-case", help="Case name stored in manifest.")
                parser.add_argument("--scenario", default="", help="Scenario description stored in manifest.")
                parser.add_argument(
                    "--code-ref",
                    default="unknown",
                    help="Source ref used for CodeGraph/source validation.",
                )
                parser.add_argument("--force", action="store_true", help="Create a fresh run even when cache matches.")
                parser.add_argument("--allow-partial-inputs", action="store_true",
                                    help=(
                                        "Continue when some inputs cannot be read; "
                                        "failures are recorded in the report."
                                    ))
                args = parser.parse_args(argv)
                _LOG.info(
                    "%s",
                    parse_stage(
                        args.inputs,
                        args.out,
                        RunOptions(
                            case_name=args.case,
                            scenario=args.scenario,
                            code_ref=args.code_ref,
                            force=args.force,
                            allow_partial_inputs=args.allow_partial_inputs,
                        ),
                    ),
                )
                return 0
            if command in ("aggregate", "triage", "render-local", "render-site", "publish-site"):
                parser = argparse.ArgumentParser(description=f"Run trace triage {command} stage.")
                parser.add_argument("run_dir", help="Existing staged run directory.")
                if command == "publish-site":
                    parser.add_argument(
                        "--dry-run",
                        action="store_true",
                        help="Prepare publish metadata without copying.",
                    )
                    parser.add_argument("--max-site-html-mb", type=float, default=2.0,
                                        help="Refuse real yche.me publish when report.site.html exceeds this size.")
                args = parser.parse_args(argv)
                if command == "aggregate":
                    _LOG.info("%s", aggregate_stage(args.run_dir))
                elif command == "triage":
                    _LOG.info("%s", triage_stage(args.run_dir))
                elif command == "render-local":
                    _LOG.info("%s", render_local_stage(args.run_dir))
                elif command == "render-site":
                    _LOG.info("%s", render_site_stage(args.run_dir))
                else:
                    max_bytes = int(args.max_site_html_mb * 1024 * 1024)
                    url = publish_site_stage(args.run_dir, dry_run=args.dry_run, max_site_html_bytes=max_bytes)
                    _LOG.info("%s", f"{'DRY-RUN ' if args.dry_run else ''}{url}")
                return 0
            parser = argparse.ArgumentParser(description="Run staged DataSystem trace triage.")
            parser.add_argument("inputs", nargs="+", help="Log files, directories, or tar bundles.")
            parser.add_argument("--out", required=True, help="Output root for timestamped run directories.")
            parser.add_argument("--case", default="trace-case", help="Case name stored in manifest.")
            parser.add_argument("--scenario", default="", help="Scenario description stored in manifest.")
            parser.add_argument(
                "--code-ref",
                default="unknown",
                help="Source ref used for CodeGraph/source validation.",
            )
            parser.add_argument("--force", action="store_true", help="Create a fresh run even when cache matches.")
            parser.add_argument("--allow-partial-inputs", action="store_true",
                                help="Continue when some inputs cannot be read; failures are recorded in the report.")
            args = parser.parse_args(argv)
            run_dir = run_pipeline(
                args.inputs,
                args.out,
                RunOptions(
                    case_name=args.case,
                    scenario=args.scenario,
                    code_ref=args.code_ref,
                    force=args.force,
                    allow_partial_inputs=args.allow_partial_inputs,
                ),
            )
            _LOG.info("%s", run_dir)
            return 0

        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("inputs", nargs="*", help="Log files, directories, or tar bundles.")
        parser.add_argument("--code-ref", default="unknown", help="Source ref used for CodeGraph/source validation.")
        parser.add_argument("--allow-partial-inputs", action="store_true",
                            help="Continue when some inputs cannot be read; failures are recorded in the report.")
        parser.add_argument("--output-json", help="Write machine-readable summary JSON.")
        parser.add_argument("--output-md", help="Write Markdown summary.")
        parser.add_argument(
            "--self-test",
            action="store_true",
            help="Run the built-in fixture and validate parser behavior.",
        )
        args = parser.parse_args(argv)

        if args.self_test:
            report = run_self_test()
            _LOG.info("self-test passed")
        else:
            if not args.inputs:
                parser.error("inputs are required unless --self-test is used")
            report = analyze_inputs(args.inputs, code_ref=args.code_ref, allow_partial_inputs=args.allow_partial_inputs)

        text = json.dumps(report, ensure_ascii=False, indent=2)
        if args.output_json:
            Path(args.output_json).write_text(text + "\n", encoding="utf-8")
        else:
            _LOG.info("%s", text)
        if args.output_md:
            Path(args.output_md).write_text(render_markdown(report), encoding="utf-8")
        return 0

    except TraceTriageError as exc:
        _LOG.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

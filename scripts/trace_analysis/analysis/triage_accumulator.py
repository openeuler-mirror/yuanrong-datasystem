"""Trace-scoped accumulation and classification of parsed log evidence."""

import json
import re
from collections import Counter, defaultdict

from ..ingest.inventory import (
    detect_noise_cohort_mode as _detect_noise_cohort_mode,
    duplicate_input_basenames as _duplicate_input_basenames,
    source_cohort_label as _source_cohort_label,
)
from ..ingest.triage import (
    ACCESS_RE, BREAKDOWN_BLOCK_RE, BREAKDOWN_ITEM_RE,
    LATENCY_SUMMARY_RE, RPC_MAX_CONCURRENCY_ERROR, RPC_SLOW_FIELD_RE, RPC_SLOW_RE,
    SUMMARY_ITEM_RE, URMA_NOTIFY_RE, URMA_PERF_RE, URMA_POLL_RE,
    URMA_THREAD_LOOP_GAP_RE, URMA_THREAD_RE, URMA_TOTAL_RE, WORKER_POD_NAME_RE, _ms,
)
from .triage_stats import _percentiles


DEFAULT_MAX_EVIDENCE_PER_TRACE = 200


def _add_metric(bucket, key, value):
    item = bucket.setdefault(key, {"count": 0, "sum": 0.0, "max": 0.0})
    item["count"] += 1
    item["sum"] = round(item["sum"] + value, 3)
    item["max"] = round(max(item["max"], value), 3)


def _classify(trace):
    access_for_deadline = trace.get("access_latency_ms_by_role", {}).get("client") or trace["access_latency_ms"]
    max_access = max(access_for_deadline or [0])
    max_urma = max(trace["urma_total_ms"] or [0])
    memory_copy_us = trace["latency_summary_us"].get("client.process.memory_copy", 0)
    set_total_us = trace["latency_summary_us"].get("client.process.set", 0)
    if trace["errors"].get(RPC_MAX_CONCURRENCY_ERROR):
        return "rpc_max_concurrency"
    if trace["errors"] and max_urma >= 50:
        return "client_deadline_with_urma_wait"
    if trace["errors"] and max_access and 18 <= max_access <= 25:
        return "client_deadline_20ms"
    if memory_copy_us >= 1000 and memory_copy_us >= max(set_total_us, 1):
        return "write_memory_copy_dominant"
    if trace["errors"]:
        return "deadline_or_error"
    if max_urma >= 50:
        return "remote_fast_transport_wait"
    if trace["rpc_slow"]:
        return "rpc_slow"
    if trace["access_latency_ms"]:
        return "access_latency_only"
    if trace.get("ub_events") or trace.get("rpc_slow") or trace.get("errors"):
        return "worker_evidence_only"
    return "input_without_latency"


def _stage(stage, duration_ms=None, confidence="missing", source="missing", fields=None):
    row = {"stage": stage, "confidence": confidence, "source": source}
    if duration_ms is not None:
        row["duration_ms"] = round(duration_ms, 3)
    if fields:
        row["fields"] = fields
    return row


def _build_stage_breakdown(trace):
    flows = trace["flows"]
    summary = trace["latency_summary_us"]
    ub_events = trace["ub_events"]
    breakdown = []
    missing = []
    is_write = any(flow in flows for flow in ("DS_KV_CLIENT_SET", "DS_KV_CLIENT_CREATE", "DS_KV_CLIENT_PUBLISH"))

    if is_write:
        create_us = summary.get("client.rpc.create")
        publish_us = summary.get("client.rpc.publish")
        memory_us = summary.get("client.process.memory_copy")
        meta_us = summary.get("worker.rpc.create_meta")
        breakdown.append(_stage("write.client_to_entry_createbuffer", create_us / 1000.0 if create_us else None,
                                "high" if create_us else "missing", "latencySummary client.rpc.create"))
        breakdown.append(_stage("write.client_memory_copy", memory_us / 1000.0 if memory_us else None,
                                "high" if memory_us else "missing", "latencySummary client.process.memory_copy"))
        breakdown.append(_stage("write.client_to_entry_publish", publish_us / 1000.0 if publish_us else None,
                                "high" if publish_us else "missing", "latencySummary client.rpc.publish"))
        breakdown.append(_stage("write.entry_to_meta_publish", meta_us / 1000.0 if meta_us else None,
                                "high" if meta_us else "missing", "latencySummary worker.rpc.create_meta"))
        if not meta_us:
            missing.append({
                "stage": "write.entry_to_meta_publish",
                "expected": ["worker.rpc.create_meta", "MasterOCService.CreateMeta rpc slow"],
                "impact": "cannot split Publish metadata update from entry worker processing",
                "fallback": "mark missing",
            })
        return breakdown, missing

    client_ms = None
    if summary.get("client.rpc.get"):
        client_ms = summary["client.rpc.get"] / 1000.0
    elif trace["access_latency_ms"]:
        client_ms = max(trace["access_latency_ms"])
    remote_costs = [
        event["cost_ms"] for event in ub_events
        if event.get("event_type") == "transfer_path" and event.get("cost_ms") is not None
    ]
    total_costs = [
        event["cost_ms"] for event in ub_events
        if event.get("event_type") == "total" and event.get("cost_ms") is not None
    ]
    qmeta_us = summary.get("worker.rpc.query_meta")
    breakdown.append(_stage("read.client_to_entry_worker", client_ms, "high" if client_ms is not None else "missing",
                            "client access or latencySummary client.rpc.get"))
    breakdown.append(_stage("read.entry_to_meta_worker", qmeta_us / 1000.0 if qmeta_us else None,
                            "high" if qmeta_us else "missing", "latencySummary worker.rpc.query_meta"))
    breakdown.append(_stage("read.entry_to_data_worker", max(remote_costs) if remote_costs else None,
                            "high" if remote_costs else "missing", "Remote get success / worker.rpc.remote_get"))
    breakdown.append(_stage("read.data_worker_ub_write", max(total_costs) if total_costs else None,
                            "high" if total_costs else "missing", "URMA_ELAPSED_TOTAL"))
    if not qmeta_us:
        missing.append({
            "stage": "read.entry_to_meta_worker",
            "expected": ["worker.rpc.query_meta", "QueryMeta rpc slow"],
            "impact": "cannot split meta lookup from entry worker processing",
            "fallback": "mark missing; keep client_to_entry_worker as observed upper bound",
        })
    return breakdown, missing


def _evidence_coverage(trace):
    has_client = bool(trace["flows"]) or any(k.startswith("client.") for k in trace["latency_summary_us"])
    has_entry = any(event["event_type"] in ("transfer_path", "remote_get_start") for event in trace["ub_events"])
    has_data = any(
        event["event_type"] in ("total", "poll_jfc", "notify", "thread_sched")
        for event in trace["ub_events"]
    )
    has_meta = bool(trace["latency_summary_us"].get("worker.rpc.query_meta")
                    or trace["latency_summary_us"].get("worker.rpc.create_meta"))
    return {
        "client": "present" if has_client else "missing",
        "entry_worker": "present" if has_entry else "missing",
        "meta_worker": "present" if has_meta else "missing",
        "data_worker": "present" if has_data else "missing",
        "urma": "present" if has_data else "missing",
        "clock_alignment": "same_host_or_unknown",
    }


class TraceAccumulator:
    """Accumulate parsed log lines into trace-scoped facts and raw dimensions."""

    def __init__(self, paths, max_evidence_per_trace=DEFAULT_MAX_EVIDENCE_PER_TRACE):
        self.max_evidence_per_trace = max_evidence_per_trace
        self.noise_cohort_mode = _detect_noise_cohort_mode(paths)
        self.duplicate_cohort_basenames = _duplicate_input_basenames(paths)
        self.input_paths = tuple(paths)
        self.source_cohort_labels = {}
        self.traces = defaultdict(self._new_trace)
        self.all_ts = []
        self.worker_counts = Counter()
        self.worker_ip_counts = defaultdict(Counter)
        self.flow_counts = Counter()
        self.access_latencies = []
        self.breakdown = {}
        self.rpc_slow = defaultdict(lambda: {"count": 0, "fields_us": defaultdict(list)})
        self.urma = defaultdict(list)
        self.urma_perf = defaultdict(list)
        self.custom_metrics = defaultdict(list)
        self.latency_summary = defaultdict(list)
        self.errors = Counter()
        self.ub_summary = {"transfer_path": Counter(), "edges": defaultdict(lambda: {"count": 0, "latencies": []})}
        self.surface_counts = Counter()

    @staticmethod
    def _new_trace():
        return {
            "lines": 0,
            "workers": Counter(),
            "timestamps": [],
            "flows": Counter(),
            "access_latency_ms": [],
            "access_latency_ms_by_role": defaultdict(list),
            "breakdown_ms": Counter(),
            "rpc_slow": Counter(),
            "rpc_calls": {},
            "rpc_stage_windows": {},
            "query_and_get_calls": {},
            "urma_timeout_events": {},
            "client_processes": set(),
            "rpc_slow_fields_us": defaultdict(list),
            "urma_total_ms": [],
            "urma_poll_jfc_ms": [],
            "urma_notify_ms": [],
            "urma_thread_sched_ms": [],
            "urma_perf": Counter(),
            "custom_metrics_ms": Counter(),
            "ub_events": [],
            "latency_summary_us": Counter(),
            "latency_summary_events": set(),
            "latency_summary_raw": [],
            "errors": Counter(),
            "evidence": [],
            "dropped_evidence": 0,
            "input_sources": Counter(),
            "source_stats": defaultdict(lambda: {
                "errors": Counter(),
                "workers": Counter(),
                "access_latency_ms": [],
                "line_count": 0,
            }),
        }

    def ingest(self, parsed, line):
        trace_id = parsed["trace_id"]
        trace = self.traces[trace_id]
        trace["lines"] += 1
        evidence = parsed["evidence"]
        source_key = (evidence["source"], evidence["member"])
        source_label = self.source_cohort_labels.get(source_key)
        if source_label is None:
            source_label = _source_cohort_label(
                *source_key, self.noise_cohort_mode, self.duplicate_cohort_basenames, self.input_paths
            )
            self.source_cohort_labels[source_key] = source_label
        trace["input_sources"][source_label] += 1
        trace["source_stats"][source_label]["line_count"] += 1
        worker = parsed["worker"]
        trace["workers"][worker] += 1
        trace["source_stats"][source_label]["workers"][worker] += 1
        self.worker_counts[worker] += 1
        host_ip = evidence.get("host_ip")
        if host_ip and WORKER_POD_NAME_RE.fullmatch(worker):
            self.worker_ip_counts[worker][host_ip] += 1
        ts = parsed["timestamp"]
        if ts:
            trace["timestamps"].append(ts.isoformat())
            self.all_ts.append(ts)
        if len(trace["evidence"]) < self.max_evidence_per_trace:
            trace["evidence"].append(evidence)
        else:
            trace["dropped_evidence"] += 1
        self._ingest_ub_events(trace, parsed["ub_events"])
        self._ingest_access(trace, line, source_label)
        self._ingest_breakdown(trace, line)
        self._ingest_rpc_slow(trace, line, parsed)
        self._ingest_query_and_get(trace, line, parsed)
        self._ingest_urma_timeout(trace, line, parsed)
        self._ingest_latency_summary(trace, line, parsed)
        self._ingest_urma_elapsed(trace, line)
        self._ingest_urma_perf(trace, line)
        self._ingest_custom_metrics(trace, parsed["custom_metrics_ms"])
        self._ingest_errors(trace, parsed["errors"], source_label)

    def _ingest_ub_events(self, trace, events):
        for event in events:
            trace["ub_events"].append(event)
            if event.get("transfer_path"):
                self.ub_summary["transfer_path"][event["transfer_path"]] += 1
            if event.get("src_addr") and event.get("target_addr") and event.get("event_type") == "total":
                edge = f"{event['src_addr']} -> {event['target_addr']}"
                self.ub_summary["edges"][edge]["count"] += 1
                if event.get("cost_ms") is not None:
                    self.ub_summary["edges"][edge]["latencies"].append(event["cost_ms"])

    def _ingest_access(self, trace, line, source_label=None):
        access = ACCESS_RE.search(line)
        if not access:
            return
        self.surface_counts["client_access"] += 1
        status, operation, duration_us, _size = access.groups()
        latency_ms = int(duration_us) / 1000.0
        trace["flows"][operation] += 1
        self.flow_counts[operation] += 1
        if status != "0":
            self.errors[f"status={status}"] += 1
            trace["errors"][f"status={status}"] += 1
        trace["access_latency_ms"].append(latency_ms)
        if source_label:
            trace["source_stats"][source_label]["access_latency_ms"].append(latency_ms)
        role = "client" if "CLIENT" in operation else "worker" if "POSIX" in operation else "unknown"
        trace["access_latency_ms_by_role"][role].append(latency_ms)
        self.access_latencies.append(latency_ms)

    def _ingest_breakdown(self, trace, line):
        block = BREAKDOWN_BLOCK_RE.search(line)
        if not block:
            return
        for key, value in BREAKDOWN_ITEM_RE.findall(block.group(1)):
            value_ms = float(value)
            name = " ".join(key.split())
            trace["breakdown_ms"][name] += value_ms
            _add_metric(self.breakdown, name, value_ms)

    def _ingest_rpc_slow(self, trace, line, parsed):
        rpc = RPC_SLOW_RE.search(line)
        if not rpc:
            return
        self.surface_counts["rpc_slow"] += 1
        method = rpc.group(1)
        trace["rpc_slow"][method] += 1
        self.rpc_slow[method]["count"] += 1
        for field, raw_value in RPC_SLOW_FIELD_RE.findall(line):
            val = int(raw_value)
            trace["rpc_slow_fields_us"][field].append(val)
            self.rpc_slow[method]["fields_us"][field].append(val)

        clock_fields = re.findall(r"\b(ClientSend|ClientRecv|ServerRecv|ServerSend)=(-?\d+)", line)
        event = {**self._timing_context(parsed, line), "method": method,
                 "fields_us": {key: int(value) for key, value in re.findall(r"\b(\w+_us)=(-?\d+)", line)},
                 "clocks_ns": {key: int(value) for key, value in clock_fields}}
        for key in ("cntl_failed", "cntl_error_code"):
            match = re.search(rf"\b{key}=(-?\d+)", line)
            event[key] = int(match.group(1)) if match else None
        self._store_timing_event(trace["rpc_calls"], event, parsed)

    @staticmethod
    def _timing_context(parsed, line):
        parts = line.split(" | ")
        process = parts[4].strip().split(":", 1)[0] if len(parts) > 4 else ""
        return {
            "owner": [parsed["evidence"].get("host_ip"), process if process.isdigit() else None],
            "timestamp": parsed["timestamp"].isoformat() if parsed["timestamp"] else None,
        }

    @staticmethod
    def _store_timing_event(events, event, parsed):
        identity = json.dumps(event, sort_keys=True)
        if identity not in events:
            events[identity] = {**event, "source": {key: parsed["evidence"][key]
                               for key in ("source", "member", "line")}}

    def _ingest_urma_timeout(self, trace, line, parsed):
        columns = line.split(" | ")
        if len(columns) < 3 or not re.search(r"\burma_manager\.cpp:\d+", columns[2]):
            return
        match = re.search(r"\[URMA_WAIT_TIMEOUT\] \[urma_request_id:(\d+)\].*?elapsedMs=([\d.]+)", line)
        if not match:
            return
        event = {**self._timing_context(parsed, line), "worker": parsed["worker"],
                 "request_id": match.group(1), "elapsed_ms": float(match.group(2))}
        self._store_timing_event(trace["urma_timeout_events"], event, parsed)

    def _ingest_query_and_get(self, trace, line, parsed):
        context = self._timing_context(parsed, line)
        if re.search(r"\| DS_KV_CLIENT_[A-Z_]+ \|", line) and all(context["owner"]):
            trace["client_processes"].add(tuple(context["owner"]))
        if "QueryAndGet done," not in line:
            return
        phase_fields = re.findall(
            r"\b(preproc|preprocess|localRead|metadata|delivery):\s*([\d.]+)ms", line
        )
        phases = {}
        for key, value in phase_fields:
            phases["preprocess" if key == "preproc" else key] = float(value)
        total = re.search(r"\btotal:\s*([\d.]+)ms", line)
        event = {**context, "phases_ms": phases,
                 "total_ms": float(total.group(1)) if total else None}
        self._store_timing_event(trace["query_and_get_calls"], event, parsed)

    def _ingest_latency_summary(self, trace, line, parsed):
        summary = LATENCY_SUMMARY_RE.search(line)
        if not summary:
            return
        context = self._timing_context(parsed, line)
        if context["timestamp"] and all(context["owner"]):
            if line in trace["latency_summary_events"]:
                return
            trace["latency_summary_events"].add(line)
        self.surface_counts["latency_summary"] += 1
        raw = "latencySummary:{" + summary.group(1) + "}"
        if len(trace["latency_summary_raw"]) < 8:
            trace["latency_summary_raw"].append(raw)
        access = ACCESS_RE.search(line)
        context = self._timing_context(parsed, line) if access else None
        for key, raw_value in SUMMARY_ITEM_RE.findall(summary.group(1)):
            val = int(raw_value)
            trace["latency_summary_us"][key] += val
            self.latency_summary[key].append(val)
            if ".rpc." in key and val >= 0 and access:
                event = {**context, "stage_key": key,
                         "duration_us": val, "operation": access.group(2)}
                self._store_timing_event(trace["rpc_stage_windows"], event, parsed)

    def _ingest_urma_elapsed(self, trace, line):
        # New logs wrap markers and fields in Markdown stars and use cost: / us aliases.
        line = line.replace("*", "")
        loop_gap = URMA_THREAD_LOOP_GAP_RE.search(line)
        if loop_gap:
            self.surface_counts["urma_elapsed"] += 1
            val = float(loop_gap.group(1)) / 1000.0
            trace["urma_thread_sched_ms"].append(val)
            self.urma["thread_sched"].append(val)
        for name, regex in (
            ("total", URMA_TOTAL_RE),
            ("poll_jfc", URMA_POLL_RE),
            ("notify", URMA_NOTIFY_RE),
            ("thread_sched", URMA_THREAD_RE),
        ):
            um = regex.search(line)
            if um:
                self.surface_counts["urma_elapsed"] += 1
                val = _ms(um.group(1), um.group(2) if len(um.groups()) > 1 else "ms")
                trace[f"urma_{name}_ms"].append(val)
                self.urma[name].append(val)

    def _ingest_urma_perf(self, trace, line):
        perf = URMA_PERF_RE.search(line)
        if not perf:
            return
        key, raw_value, unit = perf.groups()
        val = float(raw_value)
        if (unit or "ms").lower() == "us":
            val /= 1000.0
        name = " ".join(key.split())
        trace["urma_perf"][name] += val
        self.urma_perf[name].append(val)

    def _ingest_custom_metrics(self, trace, metrics):
        for name, val in metrics.items():
            trace["custom_metrics_ms"][name] += val
            self.custom_metrics[name].append(val)

    def _ingest_errors(self, trace, patterns, source_label=None):
        for pattern in patterns:
            self.surface_counts["error"] += 1
            self.errors[pattern] += 1
            trace["errors"][pattern] += 1
            if source_label:
                trace["source_stats"][source_label]["errors"][pattern] += 1

    def finish(self):
        trace_rows, classifications, worker_summary = self._build_trace_rows()
        worker_edges = {
            edge: {"count": item["count"], "p99_ms": _percentiles(item["latencies"]).get("p99")}
            for edge, item in self.ub_summary["edges"].items()
        }
        return {
            "trace_rows": trace_rows,
            "classifications": classifications,
            "worker_summary": worker_summary,
            "worker_edges": worker_edges,
            "all_ts": self.all_ts,
            "worker_counts": self.worker_counts,
            "worker_ip_counts": self.worker_ip_counts,
            "flow_counts": self.flow_counts,
            "access_latencies": self.access_latencies,
            "breakdown": self.breakdown,
            "rpc_slow": self.rpc_slow,
            "urma": self.urma,
            "urma_perf": self.urma_perf,
            "custom_metrics": self.custom_metrics,
            "latency_summary": self.latency_summary,
            "errors": self.errors,
            "ub_summary": self.ub_summary,
            "surface_counts": self.surface_counts,
        }

    def _build_trace_rows(self):
        trace_rows = {}
        classifications = Counter()
        worker_roles = defaultdict(set)
        worker_trace_ids = defaultdict(set)
        worker_slow_counts = Counter()
        worker_error_counts = Counter()
        for trace_id, trace in self.traces.items():
            trace["classification"] = _classify(trace)
            classifications[trace["classification"]] += 1
            triage_flags = []
            access_for_deadline = trace.get("access_latency_ms_by_role", {}).get("client") or trace["access_latency_ms"]
            if trace["errors"] and max(trace["urma_total_ms"] or [0]) > max(access_for_deadline or [0]):
                triage_flags.append("late_worker_completion")
            for worker in trace["workers"]:
                worker_trace_ids[worker].add(trace_id)
                if trace["classification"] not in ("unknown", "access_latency_only"):
                    worker_slow_counts[worker] += 1
                if trace["errors"]:
                    worker_error_counts[worker] += sum(trace["errors"].values())
            for event in trace["ub_events"]:
                if event["event_type"] in ("transfer_path", "remote_get_start"):
                    worker_roles[event["worker"]].add("entry_worker")
                if event["event_type"] in ("total", "poll_jfc", "notify", "thread_sched"):
                    worker_roles[event["worker"]].add("data_worker")
            stage_breakdown, missing_evidence = _build_stage_breakdown(trace)
            trace_rows[trace_id] = {
                "classification": trace["classification"],
                "line_count": trace["lines"],
                "workers": dict(trace["workers"]),
                "first_ts": min(trace["timestamps"]) if trace["timestamps"] else None,
                "last_ts": max(trace["timestamps"]) if trace["timestamps"] else None,
                "flows": dict(trace["flows"]),
                "access_latency_ms": _percentiles(trace["access_latency_ms"]),
                "access_latency_ms_by_role": {
                    role: _percentiles(values)
                    for role, values in sorted(trace["access_latency_ms_by_role"].items())
                },
                "breakdown_ms": {k: round(v, 3) for k, v in trace["breakdown_ms"].items()},
                "rpc_slow": dict(trace["rpc_slow"]),
                "rpc_calls": list(trace["rpc_calls"].values()),
                "rpc_stage_windows": list(trace["rpc_stage_windows"].values()),
                "query_and_get_calls": list(trace["query_and_get_calls"].values()),
                "urma_timeout_events": list(trace["urma_timeout_events"].values()),
                "client_processes": [list(owner) for owner in sorted(trace["client_processes"])],
                "rpc_slow_fields_us": {k: _percentiles(v) for k, v in sorted(trace["rpc_slow_fields_us"].items())},
                "urma_elapsed_ms": {
                    "total": _percentiles(trace["urma_total_ms"]),
                    "poll_jfc": _percentiles(trace["urma_poll_jfc_ms"]),
                    "notify": _percentiles(trace["urma_notify_ms"]),
                    "thread_sched": _percentiles(trace["urma_thread_sched_ms"]),
                },
                "urma_perf_ms": {k: round(v, 3) for k, v in trace["urma_perf"].items()},
                "custom_metrics_ms": {k: round(v, 3) for k, v in trace["custom_metrics_ms"].items()},
                "ub_events": trace["ub_events"],
                "latency_summary_us": dict(trace["latency_summary_us"]),
                "latency_summary_raw": trace["latency_summary_raw"],
                "errors": dict(trace["errors"]),
                "input_sources": sorted(trace["input_sources"]),
                "source_stats": {
                    source: {
                        "errors": dict(stats["errors"]),
                        "workers": dict(stats["workers"]),
                        "access_latency_ms": _percentiles(stats["access_latency_ms"]),
                        "line_count": stats["line_count"],
                    }
                    for source, stats in sorted(trace["source_stats"].items())
                },
                "dropped_evidence": trace["dropped_evidence"],
                "triage_flags": triage_flags,
                "stage_breakdown": stage_breakdown,
                "evidence_coverage": _evidence_coverage(trace),
                "missing_evidence": missing_evidence,
                "evidence": trace["evidence"],
            }
        return trace_rows, classifications, self._build_worker_summary(
            worker_roles, worker_trace_ids, worker_slow_counts, worker_error_counts
        )

    def _build_worker_summary(self, worker_roles, worker_trace_ids, worker_slow_counts, worker_error_counts):
        worker_summary = {}
        for worker, item in self.worker_counts.most_common():
            roles = sorted(worker_roles.get(worker) or {"unknown"})
            worker_summary[worker] = {
                "roles": roles,
                "line_count": item,
                "trace_count": len(worker_trace_ids[worker]),
                "slow_trace_count": worker_slow_counts[worker],
                "error_count": worker_error_counts[worker],
                "coverage": {
                    "urma": "present" if "data_worker" in roles else "missing",
                    "remote_get": "present" if "entry_worker" in roles else "missing",
                },
            }
        return worker_summary

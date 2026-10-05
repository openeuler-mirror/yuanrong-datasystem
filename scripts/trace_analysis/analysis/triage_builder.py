"""Assemble the stable triage report schema from accumulated trace facts."""

import re

from .ub_edges import group_operation_edges
from .triage_stats import _percentiles
from .triage_dimensions import (
    _build_cohorts, _build_diagnosis, _build_recommendations,
    _build_source_appendix, _build_time_buckets,
)
from .triage_flow import _build_flow_stages
from .triage_ub import _build_ub_worker_summary, _build_ub_lifecycle_summary


def _surface_status(count):
    return "present" if count else "missing"


class TraceDimensionSections:
    """Build derived report sections from accumulated trace rows."""

    @staticmethod
    def build_coverage(surface_counts):
        return {
            "surfaces": {
                "client_access": {"events": surface_counts["client_access"],
                                  "status": _surface_status(surface_counts["client_access"])},
                "rpc_slow": {"events": surface_counts["rpc_slow"],
                             "status": _surface_status(surface_counts["rpc_slow"])},
                "latency_summary": {"events": surface_counts["latency_summary"],
                                    "status": _surface_status(surface_counts["latency_summary"])},
                "urma_elapsed": {"events": surface_counts["urma_elapsed"],
                                 "status": _surface_status(surface_counts["urma_elapsed"])},
                "error": {"events": surface_counts["error"], "status": _surface_status(surface_counts["error"])},
            }
        }

    @staticmethod
    def build_cohorts(trace_rows):
        return _build_cohorts(trace_rows)

    @staticmethod
    def build_diagnosis(errors, classifications, access_latencies, coverage, cohorts):
        return _build_diagnosis(errors, classifications, access_latencies, coverage, cohorts)

    @staticmethod
    def build_recommendations(classifications, coverage, cohorts, ub_summary):
        return _build_recommendations(classifications, coverage, cohorts, ub_summary)

    @staticmethod
    def build_source_appendix(coverage):
        return _build_source_appendix(coverage)

    @staticmethod
    def build_flow_stages(coverage, flow_counts, ub_summary, trace_rows):
        return _build_flow_stages(coverage, flow_counts, ub_summary, trace_rows)

    @staticmethod
    def build_time_buckets(trace_rows):
        return {"1000ms": _build_time_buckets(trace_rows, 1000),
                "10000ms": _build_time_buckets(trace_rows, 10000)}

    @staticmethod
    def build_ub_worker_summary(trace_rows):
        return _build_ub_worker_summary(trace_rows)

    @staticmethod
    def build_ub_lifecycle_summary(trace_rows):
        return _build_ub_lifecycle_summary(trace_rows)


class TraceDimensionBuilder:
    """Convert accumulated trace facts into the stable report schema."""

    def __init__(self, sections=None):
        self.sections = sections or TraceDimensionSections()

    def build(self, snapshot, paths, code_ref="unknown", input_failures=None):
        trace_rows = snapshot["trace_rows"]
        surface_counts = snapshot["surface_counts"]
        coverage = self.sections.build_coverage(surface_counts)
        cohorts = self.sections.build_cohorts(trace_rows)
        diagnosis = self.sections.build_diagnosis(
            errors=snapshot["errors"],
            classifications=snapshot["classifications"],
            access_latencies=snapshot["access_latencies"],
            coverage=coverage,
            cohorts=cohorts,
        )
        recommendations = self.sections.build_recommendations(
            classifications=snapshot["classifications"],
            coverage=coverage,
            cohorts=cohorts,
            ub_summary=snapshot["ub_summary"],
        )
        time_buckets = self.sections.build_time_buckets(trace_rows)
        ub_worker_summary = self.sections.build_ub_worker_summary(trace_rows)
        ub_lifecycle_summary = self.sections.build_ub_lifecycle_summary(trace_rows)
        flow_stages = self.sections.build_flow_stages(
            coverage, snapshot["flow_counts"], snapshot["ub_summary"], trace_rows
        )
        return {
            "schema_version": 1,
            "code_ref": code_ref,
            "inputs": [str(p) for p in paths],
            "trace_count": len(trace_rows),
            "dimensions": {
                "time": self._build_time_range(snapshot["all_ts"]),
                "time_buckets": time_buckets,
                "workers": {k: {"line_count": v} for k, v in snapshot["worker_counts"].most_common()},
                "worker_summary": snapshot["worker_summary"],
                "worker_ip_mapping": self._build_worker_ip_mapping(snapshot["worker_ip_counts"]),
                "ub_worker_summary": ub_worker_summary,
                "ub_lifecycle_summary": ub_lifecycle_summary,
                "cohorts": cohorts,
                "input_failures": input_failures or [],
                "worker_edges": snapshot["worker_edges"],
                "coverage": coverage,
                "diagnosis": diagnosis,
                "recommendations": recommendations,
                "source_appendix": self.sections.build_source_appendix(coverage),
                "flow_stages": flow_stages,
                "flow": dict(snapshot["flow_counts"]),
                "latency_ms": {"access": _percentiles(snapshot["access_latencies"])},
                "breakdown_ms": snapshot["breakdown"],
                "rpc_slow": self._build_rpc_slow(snapshot["rpc_slow"]),
                "urma_elapsed": {k: _percentiles(v) for k, v in sorted(snapshot["urma"].items())},
                "ub_summary": self._build_ub_summary(snapshot["ub_summary"], trace_rows),
                "urma_perf_ms": {k: _percentiles(v) for k, v in sorted(snapshot["urma_perf"].items())},
                "custom_metrics_ms": {k: _percentiles(v) for k, v in sorted(snapshot["custom_metrics"].items())},
                "latency_summary_us": {k: _percentiles(v) for k, v in sorted(snapshot["latency_summary"].items())},
                "errors": dict(snapshot["errors"]),
                "classifications": dict(snapshot["classifications"]),
            },
            "traces": trace_rows,
        }

    @staticmethod
    def _build_time_range(all_ts):
        return {
            "first_ts": min(all_ts).isoformat() if all_ts else None,
            "last_ts": max(all_ts).isoformat() if all_ts else None,
        }

    @staticmethod
    def _build_rpc_slow(rpc_slow):
        return {
            k: {
                "count": v["count"],
                **{field: _percentiles(vals) for field, vals in sorted(v["fields_us"].items())},
            }
            for k, v in sorted(rpc_slow.items())
        }

    @staticmethod
    def _build_worker_ip_mapping(worker_ip_counts):
        assigned_ips = set()
        rows = []
        worker_candidates = sorted(
            worker_ip_counts.items(),
            key=lambda item: (-sum(item[1].values()), item[0]),
        )
        for worker, ip_counts in worker_candidates:
            candidates = sorted(ip_counts.items(), key=lambda item: (-item[1], item[0]))
            pod_ip = next((ip for ip, _count in candidates if ip not in assigned_ips), None)
            if not pod_ip:
                continue
            assigned_ips.add(pod_ip)
            short_match = re.search(r"(?:^|-)worker(\d+)(?:_|$)", worker, re.I)
            short_name = f"worker {short_match.group(1)}" if short_match else "master"
            rows.append({
                "worker_full_name": worker,
                "worker_short_name": short_name,
                "pod_ip": pod_ip,
            })
        return sorted(rows, key=lambda item: item["worker_full_name"])

    @staticmethod
    def _build_ub_summary(ub_summary, traces):
        return {
            "transfer_path": dict(ub_summary["transfer_path"]),
            "edge_operation_schema_version": 1,
            "edges_by_operation": {
                operation: {
                    edge: {"count": item["count"], "latency_ms": _percentiles(item["latencies"])}
                    for edge, item in sorted(edges.items())
                }
                for operation, edges in group_operation_edges(traces).items()
            },
            "edges": {
                edge: {"count": item["count"], "latency_ms": _percentiles(item["latencies"])}
                for edge, item in sorted(ub_summary["edges"].items())
            },
        }

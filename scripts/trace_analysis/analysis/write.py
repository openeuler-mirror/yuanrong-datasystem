"""Write-specific attribution and budget refinement."""
import copy

from ..evidence.write import RULES, resolve_write_facts, rpc_entries_for_phase
from .budget import _rpc_framework_ms, _valid_write_rpc_fields


def _rpc_phase_evidence(facts, operation):
    unobserved = {"state": "unobserved", "method": None, "e2e_ms": None,
                  "network_ms": None, "queue_ms": None, "framework_ms": None,
                  "source_ref": None}
    candidates = [entry for entry in rpc_entries_for_phase(facts, operation)
                  if _valid_write_rpc_fields(entry["fields"])]
    if not candidates:
        return unobserved
    selected = max(candidates, key=lambda entry: entry["fields"]["e2e"])
    fields = selected["fields"]
    return {"state": "observed", "method": selected["method"],
            "e2e_ms": fields["e2e"] / 1000,
            "network_ms": fields["network_residual"] / 1000,
            "queue_ms": fields["server_req_queue"] / 1000,
            "framework_ms": _rpc_framework_ms(fields),
            "source_ref": dict(selected["source_ref"])}


def refine(row):
    r = copy.deepcopy(row)
    evidence = r["evidence"]
    facts = resolve_write_facts(r)
    r["write_evidence_facts"] = facts
    r["write_rpc_phase_evidence"] = {
        stage: _rpc_phase_evidence(facts, stage.lower())
        for stage in ("Create", "Publish")
    }
    r["observed_parents"] = []
    r["refinement_notes"] = []
    r.setdefault("error_root_causes", [])
    phase_observation = r.setdefault("write_phase_observation", {
        stage: {"state": "unobserved", "parent_ms": None, "source": None}
        for stage in ("Create", "Copy", "Publish")
    })
    phase_observation.setdefault("Copy", {
        "state": "unobserved", "parent_ms": None, "source": None,
    })
    b = r["write_breakdown_ms"]
    for op in ("Create", "Publish"):
        hits = facts["parents"][op]
        for entry in hits:
            r["observed_parents"].append(
                {"stage": op, "ms": entry["ms"], "evidence": evidence[entry["source_ref"]["index"]]}
            )
        if hits and phase_observation[op]["state"] == "unobserved":
            phase_observation[op] = {
                "state": "observed",
                "parent_ms": hits[0]["ms"] if len(hits) == 1 else None,
                "source": "client_worker_rpc" if len(hits) == 1 else "multiple_client_worker_rpc",
            }
        if len(hits) == 1 and not r[op.lower() + "_rpc_ms"]:
            value = hits[0]["ms"]
            if value <= b["未解释残差"] + 0.000001:
                b["未解释残差"] = max(0, b["未解释残差"] - value)
                b[op + " RPC其他"] += value
                r[op.lower() + "_rpc_ms"] = value
                r["refinement_notes"].append(
                    op
                    + "：由同Trace唯一 Client/WorkerRpc costUs 补回父窗口；内部未细分，不算纯网络或业务。"
                )
            else:
                r["refinement_notes"].append(
                    op + "：观测父窗口超过可用残差，保留旁证，不强行裁剪或叠加。"
                )
        elif len(hits) > 1:
            r["refinement_notes"].append(
                op + "：多个调用父窗口，仅列旁证，不推断串并行后求和。"
            )
    r["write_breakdown_ms"] = {k: round(v, 6) for k, v in b.items()}
    r["write_primary_stage"] = max(b, key=b.get)
    if not abs(sum(b.values()) - r["client_ms"]) < 0.025:
        raise ValueError("write stage budget does not close")
    r["client_status_failed"] = r["status"] != 0
    r["issues"] = [entry["name"] for entry in facts["issues"]]
    if (r.get("write_urma_ms") or 0) > 1.5:
        r["issues"].append("URMA传输窗口慢")
    if b.get("RPC网络相关", 0) > 1.5:
        r["issues"].append("RPC网络残差大")
    if not r["issues"]:
        r["issues"] = ["阶段慢/根因待确认"]
    r["client_observers"] = []
    r["worker_observers"] = []
    r["rpc_targets"] = []
    r["worker_events"] = []
    r["operation"] = "写入（接口未观测）"
    r["client_timestamp"] = r["timestamp"]
    client_operations = []
    for observation in facts["identity"]:
        client = observation["client"]
        if client is not None:
            client_operations.append(client)
            r["client_observers"].append(client["host"])
        if observation["worker"] is not None:
            r["worker_observers"].append(observation["worker"])
        event = observation["worker_event"]
        if event is not None and event not in r["worker_events"]:
            r["worker_events"].append(event)
        r["rpc_targets"].extend(observation["targets"])
    if client_operations:
        selected_client = next(
            (client for client in client_operations if client["operation"] in ("SET", "MSET")),
            client_operations[0],
        )
        r["operation"] = selected_client["operation"]
        r["client_timestamp"] = selected_client["timestamp"] or r["client_timestamp"]
    if r["operation"] == "CREATE":
        for stage in ("Copy", "Publish"):
            phase_observation[stage] = {
                "state": "not_applicable", "parent_ms": None, "source": None,
            }
    elif r["operation"] == "PUBLISH":
        for stage in ("Create", "Copy"):
            phase_observation[stage] = {
                "state": "not_applicable", "parent_ms": None, "source": None,
            }
    if r["operation"] in ("SET", "MSET", "PUBLISH"):
        r["wr_applicable"] = True
    elif r["operation"] == "CREATE":
        r["wr_applicable"] = False
    else:
        r["wr_applicable"] = None
    callsite = r.get("write_wr_callsite")
    callsite_source = r.get("write_wr_callsite_source")
    callsite_phases = {
        "buffer.memory_copy_ub": "Copy",
        "buffer.publish_ub": "Publish",
        "ub_transporter.set": "Publish",
    }
    if callsite is not None and callsite not in callsite_phases:
        raise ValueError("write_wr_callsite is invalid")
    if callsite is not None and not callsite_source:
        raise ValueError("write_wr_callsite requires an evidence source")
    if r["wr_applicable"] is False:
        r["wr_phase_attribution"] = {"phase": "not_applicable", "basis": "create_only"}
    elif callsite is not None:
        r["wr_phase_attribution"] = {
            "phase": callsite_phases[callsite],
            "basis": callsite_source,
        }
    else:
        r["wr_phase_attribution"] = {"phase": "unconfirmed", "basis": "wr_callsite_not_observed"}
    for k in ("client_observers", "worker_observers", "rpc_targets"):
        r[k] = sorted(set(r[k]))
    return r



def build_model(analysis):
    return {"schema_version": 1, "write_phase_schema_version": 2,
            "rows": [refine(row) for row in analysis.get("write_traces", [])], "rules": RULES}

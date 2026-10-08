"""Write observations with ordered evidence references; no attribution budgets."""
import hashlib
import math
import re

from .rpc import _rpc_fields

RULES = [
    (
        "SHM分配失败",
        r"fresh_extent_unavailable|Shared memory no space",
        "Create 分配明确失败；检查 arena/extent 可用量、释放与并发申请，不能直接等同整机 OOM。",
    ),
    (
        "E99建连失败",
        r"Cannot assign requested address|cntl_error_code=99\b|\[E99\]",
        "连接建立失败并可能重试；需端口、连接与 TIME_WAIT 指标确认原因，不能仅凭 E99 断言端口耗尽。",
    ),
    (
        "URMA超时",
        r"URMA[_ -]WAIT[_ -]TIMEOUT|Timed out waiting for urma_request_id",
        "URMA 等待超时；缺少完成事件时不能填造完成耗时，需检查 WR/完成队列与目标状态。",
    ),
    (
        "回退失败",
        r"fallback.*(?:reject|not support|fail)|(?:reject|not support).*fallback",
        "回退路径出现失败证据；与触发回退的前置错误分开核对。",
    ),
    (
        "RPC截止超时",
        r"RPC deadline exceeded|RPC timed out|cntl_error_code=1008\b",
        "RPC 未在预算内返回；失败 trailer 不足以区分服务端、网络和客户端观察等待。",
    ),
]

WRITE_CLIENT_FLOWS = frozenset({
    "DS_KV_CLIENT_SET", "DS_KV_CLIENT_MSET", "DS_KV_CLIENT_CREATE", "DS_KV_CLIENT_PUBLISH",
})


def is_write_flow(trace):
    return bool(WRITE_CLIENT_FLOWS.intersection(trace.get("flows", {})))



def _ref(index):
    return {"collection": "evidence", "index": index}


def _stamp(text):
    match = re.search(r"\d{4}-\d\d-\d\dT[\d:.]+", text)
    return match.group() if match else None


def _identity_observation(text, ref):
    fields = text.split(" | ")
    result = {"source_ref": ref, "client": None, "worker": None, "worker_event": None,
              "targets": re.findall(r"(?:Create|Publish)->\[([^\]]+)\]", text)}
    if len(fields) < 9:
        return result
    host, stamp = fields[3].strip(), _stamp(fields[0])
    if "DS_KV_CLIENT_" in text:
        match = re.search(r"\| DS_KV_CLIENT_(\w+) \|", text)
        if match:
            result["client"] = {"operation": match[1], "host": host, "timestamp": stamp}
    if "collected_worker_logs/" in fields[0] or "DS_POSIX_" in text:
        result["worker"] = host
        match = re.search(r"\| (-?\d+) \| DS_POSIX_(CREATE|PUBLISH) \| (\d+) \|", text)
        if match and stamp:
            result["worker_event"] = {"host": host, "timestamp": stamp, "operation": match[2],
                                      "status": int(match[1]), "ms": int(match[3]) / 1000}
    return result


def build_write_facts(trace_id, evidence):
    facts = {"schema_version": 1, "trace_id": trace_id,
             "source_hashes": [hashlib.sha256(text.encode()).hexdigest() for text in evidence],
             "client_summary": None, "rpc_entries": [], "parents": {}, "issues": [], "identity": []}
    parents = {"Create": {}, "Publish": {}}
    issue_refs = {name: [] for name, _, _ in RULES}
    for index, text in enumerate(evidence):
        ref = _ref(index)
        if facts["client_summary"] is None:
            match = re.search(
                r"\| (-?\d+) \| DS_KV_CLIENT_(?:CREATE|MSET|PUBLISH|SET) \| (\d+) \| (\d+) \|", text
            )
            if match:
                facts["client_summary"] = {"status": int(match[1]), "client_ms": int(match[2]) / 1000,
                                           "size_bytes": int(match[3]), "source_ref": ref}
        method, fields = _rpc_fields(text)
        if method:
            facts["rpc_entries"].append({"method": method, "fields": fields, "source_ref": ref})
        parts = text.split(" | ")
        if len(parts) >= 8 and parts[5].strip() == trace_id:
            for operation in parents:
                match = re.search(
                    r"\[Client/WorkerRpc\] " + operation + r" done,.*?costUs:\s*(\d+)", text
                )
                stamp = _stamp(text)
                if match and stamp:
                    key = (stamp, parts[3], parts[4], match[1])
                    parents[operation][key] = {"ms": int(match[1]) / 1000, "source_ref": ref}
        for name, pattern, _ in RULES:
            if re.search(pattern, text, re.I):
                issue_refs[name].append(ref)
        identity = _identity_observation(text, ref)
        if identity["client"] is not None or identity["worker"] is not None or identity["targets"]:
            facts["identity"].append(identity)
    facts["parents"] = {operation: list(hits.values()) for operation, hits in parents.items()}
    facts["issues"] = [{"name": name, "source_refs": refs} for name, refs in issue_refs.items() if refs]
    return facts


def _number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def write_fact_errors(row):
    if "write_evidence_facts" not in row:
        return []
    facts = row["write_evidence_facts"]
    if not isinstance(facts, dict) or type(facts.get("schema_version")) is not int:
        return ["write_evidence_facts: invalid schema"]
    if facts["schema_version"] != 1 or facts.get("trace_id") != row.get("trace_id"):
        return ["write_evidence_facts: unsupported schema or Trace identity"]
    evidence = row.get("evidence", [])
    if not isinstance(evidence, list) or any(not isinstance(text, str) for text in evidence):
        return ["write_evidence_facts: invalid evidence collection"]
    hashes = [hashlib.sha256(text.encode()).hexdigest() for text in evidence]
    if facts.get("source_hashes") != hashes:
        return ["write_evidence_facts: evidence hashes differ"]
    try:
        _validate_entries(facts, len(evidence))
    except (KeyError, TypeError, ValueError, AttributeError, OverflowError) as error:
        return [f"write_evidence_facts: invalid observations ({error})"]
    return []


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _reference(ref, count):
    _require(isinstance(ref, dict), "source reference")
    _require(ref.get("collection") == "evidence", "source collection")
    index = ref.get("index")
    _require(type(index) is int and 0 <= index < count, "source index")


def _validate_entries(facts, count):
    summary = facts["client_summary"]
    if summary is not None:
        _reference(summary["source_ref"], count)
        _require(type(summary["status"]) is int, "client status")
        _require(type(summary["size_bytes"]) is int and summary["size_bytes"] >= 0, "client size")
        _require(_number(summary["client_ms"]), "client duration")
    for name in ("rpc_entries", "issues", "identity"):
        _require(isinstance(facts[name], list), name)
    for entry in facts["rpc_entries"]:
        _reference(entry["source_ref"], count)
        _require(isinstance(entry["method"], str) and bool(entry["method"]), "RPC method")
        _require(isinstance(entry["fields"], dict), "RPC fields")
        _require(all(type(value) is int for value in entry["fields"].values()), "RPC numbers")
    _require(isinstance(facts["parents"], dict), "parents")
    for operation in ("Create", "Publish"):
        _require(isinstance(facts["parents"][operation], list), "parent entries")
        for entry in facts["parents"][operation]:
            _reference(entry["source_ref"], count)
            _require(_number(entry["ms"]), "parent duration")
    for issue in facts["issues"]:
        _require(issue["name"] in {name for name, _, _ in RULES}, "issue name")
        _require(isinstance(issue["source_refs"], list) and bool(issue["source_refs"]), "issue sources")
        for ref in issue["source_refs"]:
            _reference(ref, count)
    for identity in facts["identity"]:
        _validate_identity(identity, count)


def _validate_identity(identity, count):
    _reference(identity["source_ref"], count)
    _require(isinstance(identity["targets"], list), "RPC targets")
    _require(all(isinstance(target, str) for target in identity["targets"]), "RPC target")
    worker = identity["worker"]
    _require(worker is None or isinstance(worker, str), "worker")
    for name in ("client", "worker_event"):
        value = identity[name]
        if value is None:
            continue
        _require(isinstance(value["host"], str) and isinstance(value["operation"], str), name)
        stamp = value["timestamp"]
        _require(stamp is None or isinstance(stamp, str), "timestamp")
        if name == "worker_event":
            _require(type(value["status"]) is int and _number(value["ms"]), "worker event")


def resolve_write_facts(row):
    if "write_evidence_facts" not in row:
        return build_write_facts(row["trace_id"], row.get("evidence", []))
    errors = write_fact_errors(row)
    if errors:
        raise ValueError("; ".join(errors))
    return row["write_evidence_facts"]


def rpc_entries_for_phase(facts, operation):
    if operation not in ("create", "publish"):
        return []
    result = []
    for entry in facts["rpc_entries"]:
        method = entry["method"].rsplit(".", 1)[-1].lower()
        if operation in method and "meta" not in method:
            result.append(entry)
    return result


def rpc_group(facts, operation):
    return [entry["fields"] for entry in rpc_entries_for_phase(facts, operation)]

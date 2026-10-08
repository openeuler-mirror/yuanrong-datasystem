"""Validate persisted observations and their references without reading log text."""

import math


_LIST_FIELDS = ("rpc_entries", "transport_phase_maps", "transport_source_refs", "inline_attempts",
                "query_access_attempts", "query_attempts", "timeout_events", "record_sources")
_DICT_FIELDS = ("durations_ms", "duration_source_refs", "observation_source_refs")
_DURATION_FIELDS = ("local_read", "client_transfer", "provider_pull", "provider_finish")


def _number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def _valid_reference(ref, row):
    if not isinstance(ref, dict) or ref.get("collection") not in ("evidence", "record_sources"):
        return False
    index = ref.get("index")
    values = (row.get("evidence_facts", {}).get("record_sources", [])
              if ref["collection"] == "record_sources" else row.get("evidence", []))
    return type(index) is int and isinstance(values, list) and 0 <= index < len(values)


def _valid_rpc_entry(entry):
    if not isinstance(entry, dict):
        return False
    method = entry.get("method")
    fields = entry.get("fields")
    if not isinstance(method, str) or not method or not isinstance(fields, dict):
        return False
    return all(type(value) is int for value in fields.values())


def _nullable_text(entry, key):
    if key not in entry:
        return False
    value = entry.get(key)
    return value is None or isinstance(value, str)


def _valid_local_processing(facts):
    if "local_processing" not in facts:
        return False
    local = facts.get("local_processing")
    if local is None:
        return True
    return isinstance(local, dict) and all(type(local.get(key)) is int for key in ("remote_objects", "cost_us"))


def _valid_record_source(source):
    if not isinstance(source, dict):
        return False
    index = source.get("evidence_record_index")
    digest = source.get("text_sha256")
    if type(index) is not int or index < 0:
        return False
    return isinstance(digest, str) and len(digest) == 64


def _entry_errors(facts):
    errors = []
    for entry in facts["rpc_entries"]:
        if not _valid_rpc_entry(entry):
            errors.append("invalid RPC entry")
    for phases in facts["transport_phase_maps"]:
        if not isinstance(phases, dict) or any(type(value) is not int for value in phases.values()):
            errors.append("invalid transport phases")
    for name in ("inline_attempts", "query_access_attempts", "query_attempts", "timeout_events"):
        duration_key = "elapsed_ms" if name == "timeout_events" else "total_ms"
        for entry in facts[name]:
            if not isinstance(entry, dict) or not _number(entry.get(duration_key)):
                errors.append(f"invalid {name} duration")
                continue
            for key in ("worker", "timestamp"):
                if not _nullable_text(entry, key):
                    errors.append(f"invalid {name} {key}")
            if name == "timeout_events" and not _nullable_text(entry, "request_id"):
                errors.append("invalid timeout request identity")
    return errors


def evidence_fact_errors(row):
    if "evidence_facts" not in row:
        return []
    facts = row["evidence_facts"]
    if not isinstance(facts, dict) or type(facts.get("schema_version")) is not int or facts["schema_version"] != 1:
        return ["unsupported evidence_facts schema"]
    errors = []
    for name, expected in [(name, list) for name in _LIST_FIELDS] + [(name, dict) for name in _DICT_FIELDS]:
        if not isinstance(facts.get(name), expected):
            errors.append(f"{name} must be {expected.__name__}")
    if errors:
        return [f"evidence_facts: {message}" for message in errors]
    for name in ("worker_query_done_observed", "legacy_pull_src_sentinel"):
        if not isinstance(facts.get(name), bool):
            errors.append(f"{name} must be boolean")
    for name in _DURATION_FIELDS:
        value = facts["durations_ms"].get(name)
        if name not in facts["durations_ms"] or (value is not None and not _number(value)):
            errors.append(f"invalid {name} duration")
        if value is not None and name not in facts["duration_source_refs"]:
            errors.append(f"missing {name} source")
    lock = facts.get("remote_lock_ms")
    if "remote_lock_ms" not in facts or (lock is not None and not _number(lock)):
        errors.append("invalid remote_lock_ms")
    if not _valid_local_processing(facts):
        errors.append("invalid local_processing")
    for name in ("local_processing", "remote_lock_ms", "worker_query_done_observed", "legacy_pull_src_sentinel"):
        observed = facts.get(name) is not None if name in {"local_processing", "remote_lock_ms"} else facts.get(name)
        if observed and name not in facts["observation_source_refs"]:
            errors.append(f"missing {name} source")
    for source in facts["record_sources"]:
        if not _valid_record_source(source):
            errors.append("invalid record source")
    refs = list(facts["transport_source_refs"])
    if len(refs) != len(facts["transport_phase_maps"]):
        errors.append("transport source count mismatch")
    refs.extend(facts["duration_source_refs"].values())
    refs.extend(facts["observation_source_refs"].values())
    for name in ("rpc_entries", "inline_attempts", "query_access_attempts", "query_attempts", "timeout_events"):
        refs.extend(entry.get("source_ref") if isinstance(entry, dict) else None for entry in facts[name])
    if any(not _valid_reference(ref, row) for ref in refs):
        errors.append("invalid source reference")
    errors.extend(_entry_errors(facts))
    return [f"evidence_facts: {message}" for message in errors]

"""Reuse validated per-Run models while keeping presentation outside cache records."""
import json
from pathlib import Path
from time import perf_counter
from threading import Lock
from uuid import uuid4

from .execution_budget import ResourceBudget
from .stage_cache import StageCache, CacheLookup, make_stage_key, _file_hash
from .resources import producer_revision, tool_fingerprint
from .stage_versions import model_version
from .stages import StageResult


class CachedStages:
    def __init__(self, root, run_id, enabled, budget=None, stage_estimates_mb=None, producer_fingerprint=None):
        self.run_root = (root / "runs" / run_id).resolve()
        self.cache = StageCache(root / ".stage-cache")
        self.run_id = run_id
        self.enabled = enabled
        self.diagnostics = {}
        self._diagnostics_lock = Lock()
        self.budget = budget if budget is not None else ResourceBudget(1)
        self.stage_estimates_mb = dict(stage_estimates_mb or {})
        self.producer_fingerprint = producer_fingerprint

    def run(self, stage, inputs, config, produce, validate, restore, *, generation=False, restore_validation=None):
        if restore_validation is not None and not generation:
            raise ValueError("validation receipts require immutable stage generations")
        queued = perf_counter()
        with self.budget.acquire(self.stage_estimates_mb.get(stage)):
            return self._run(stage, inputs, config, produce, validate, restore, queued, generation, restore_validation)

    def _run(self, stage, inputs, config, produce, validate, restore, queued, generation, restore_validation):
        started = perf_counter()
        resolved_inputs = inputs() if callable(inputs) else inputs
        rule_version = model_version(stage)
        key = make_stage_key(resolved_inputs, config, rule_version)
        hit = self.cache.lookup(self.run_id, stage, key) if self.enabled else None
        if hit and hit.status == "hit" and any(
            not path.resolve().is_relative_to(self.run_root) for path in hit.artifacts.values()
        ):
            hit = None
        required = ({"summary_json", "manifest.json", "inventory.json", "parsed_traces.json",
                     "triage.json", "events.jsonl"}
                    if stage == "triage" else {"analysis_json"})
        if hit and hit.status == "hit" and not required.issubset(hit.artifacts):
            hit = CacheLookup("miss", "artifact_contract_invalid")
        if generation and hit and hit.status == "hit":
            if "provenance_json" not in hit.artifacts:
                hit = CacheLookup("miss", "provenance_missing")
        validation_result = None
        if restore_validation is not None and hit and hit.status == "hit":
            if "validation_json" not in hit.artifacts:
                hit = CacheLookup("miss", "validation_receipt_missing")
            else:
                validation_result = self._read_validation_receipt(hit.artifacts["validation_json"], stage, key)
                if validation_result is None:
                    hit = CacheLookup("miss", "validation_receipt_invalid")
        status = "hit" if hit and hit.status == "hit" else "miss"
        reason = hit.reason if hit else "reuse_disabled"
        with self._diagnostics_lock:
            self.diagnostics[stage] = {"status": status, "reason": reason,
                                       "queue_seconds": round(started - queued, 6)}
        try:
            if status == "hit":
                result = restore(hit.artifacts)
            elif generation:
                target = self.run_root / "generations" / stage / uuid4().hex
                target.mkdir(parents=True)
                result = produce(target)
            else:
                result = produce()
            if status == "hit" and restore_validation is not None:
                restore_validation(validation_result)
            else:
                validation_result = validate(result)
            if restore_validation is not None and (not isinstance(validation_result, dict)
                                                   or validation_result.get("valid") is not True):
                raise ValueError("validation receipt requires a successful validation result")
            if status != "hit":
                artifacts = self._models(stage, result)
                if generation:
                    provenance = target / "stage.provenance.json"
                    provenance.write_text(json.dumps({
                        "schema_version": 1, "run_id": self.run_id, "stage": stage,
                        "input_hashes": resolved_inputs, "effective_config": config,
                        "rule_version": rule_version, "cache_key": key,
                        "producer": {"tool_fingerprint": self.producer_fingerprint or tool_fingerprint(),
                                     "revision": producer_revision()},
                    }, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
                    artifacts["provenance_json"] = provenance
                if restore_validation is not None:
                    receipt = target / "stage.validation.json"
                    receipt.write_text(json.dumps({
                        "schema_version": 1, "valid": True, "run_id": self.run_id,
                        "stage": stage, "cache_key": key, "result": validation_result,
                    }, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
                    artifacts["validation_json"] = receipt
                self.cache.record_success(self.run_id, stage, key, artifacts)
            else:
                artifacts = hit.artifacts
            if "provenance_json" in artifacts:
                return StageResult({**result.artifacts, "provenance_json": artifacts["provenance_json"]})
            return result
        except Exception as error:
            with self._diagnostics_lock:
                self.diagnostics[stage].update(status="failed", error=str(error))
            raise RuntimeError(f"Run {self.run_id} stage {stage}: {error}") from error
        finally:
            with self._diagnostics_lock:
                self.diagnostics[stage]["wall_seconds"] = round(perf_counter() - started, 6)
                self.run_root.mkdir(parents=True, exist_ok=True)
                pending = self.run_root / "stage.execution.json.tmp"
                pending.write_text(json.dumps(self.diagnostics, ensure_ascii=False, indent=2), encoding="utf-8")
                pending.replace(self.run_root / "stage.execution.json")

    def _read_validation_receipt(self, path, stage, key):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(receipt, dict):
            return None
        if (type(receipt.get("schema_version")) is not int or receipt["schema_version"] != 1
                or receipt.get("valid") is not True):
            return None
        if (receipt.get("run_id") != self.run_id or receipt.get("stage") != stage
                or receipt.get("cache_key") != key):
            return None
        result = receipt.get("result")
        return result if isinstance(result, dict) and result.get("valid") is True else None

    @staticmethod
    def _models(stage, result):
        if stage != "triage":
            return {"analysis_json": result.artifacts["analysis_json"]}
        directory = result.artifacts["run_dir"]
        files = {"summary_json": result.artifacts["summary_json"]}
        required = ("manifest.json", "inventory.json", "parsed_traces.json", "triage.json", "events.jsonl")
        missing = [name for name in required if not (directory / name).is_file()]
        if missing:
            raise ValueError("required triage artifacts missing: " + ", ".join(missing))
        files.update({name: directory / name for name in required})
        for path in sorted((directory / "raw").rglob("*")):
            if path.is_file():
                files["raw:" + str(path.relative_to(directory))] = path
        return files


def file_inputs(*paths):
    return {str(Path(path)): _file_hash(Path(path)) for path in paths}


def restore_triage(artifacts):
    summary = artifacts["summary_json"]
    return StageResult({"run_dir": summary.parent, "summary_json": summary})


def restore_model(artifacts):
    return StageResult({"analysis_json": artifacts["analysis_json"]})

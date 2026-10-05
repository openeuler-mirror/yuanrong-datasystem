"""Persist Run manifests and analysis artifacts independently of parsing."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from ..ingest.inventory import TraceInputInventory, input_identity, slug
from ..resources import tool_fingerprint
from .contracts import RunOptions


def script_version():
    return tool_fingerprint()[:16]


def build_cache_key(inputs, code_ref, case_name, scenario, rules_fingerprint="default",
                    *, identity_provider=input_identity, version_provider=None):
    version_provider = version_provider or script_version
    identities = [identity_provider(path, index) for index, path in enumerate(inputs, 1)]
    payload = {
        "script_version": version_provider(),
        "rules_fingerprint": rules_fingerprint,
        "code_ref": code_ref,
        "case_name": case_name,
        "scenario": scenario,
        "inputs": sorted(identities, key=lambda item: item["path"]),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest(), identities


def find_cached_run(out_root, cache_key):
    from ..validation import validate_input_inventory

    for manifest_path in sorted(out_root.glob("*/manifest.json")):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(manifest, dict):
            continue
        cache = manifest.get("cache")
        if not isinstance(cache, dict) or cache.get("key") != cache_key:
            continue
        if validate_input_inventory(manifest_path.parent).get("valid") is True:
            return manifest_path.parent
    return None


def write_json(path, value):
    indent = None if Path(path).name in {"parsed_traces.json", "summary.json", "triage.json"} else 2
    path.write_text(json.dumps(value, ensure_ascii=False, indent=indent) + "\n", encoding="utf-8")


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def update_manifest(run_dir, updater):
    manifest_path = Path(run_dir) / "manifest.json"
    manifest = read_json(manifest_path)
    updater(manifest)
    write_json(manifest_path, manifest)
    return manifest


def new_run_dir(out_root, case_name, cache_key):
    now = datetime.now(timezone.utc)
    run_dir = out_root / f"{now.strftime('%Y%m%d-%H%M%S')}-{slug(case_name)}-{cache_key[:8]}"
    suffix = 1
    while run_dir.exists():
        suffix += 1
        run_dir = out_root / f"{now.strftime('%Y%m%d-%H%M%S')}-{slug(case_name)}-{suffix}"
    run_dir.mkdir(parents=True)
    return run_dir, now


class TraceRunStore:
    """Own run-directory cache, manifest, and artifact file operations."""

    def __init__(self, inventory=None, version_provider=None):
        self.inventory = inventory or TraceInputInventory()
        self.version_provider = version_provider or script_version

    def prepare_parse_run(self, inputs, out_dir, options, rules_fingerprint="default"):
        options = options or RunOptions()
        out_root = Path(out_dir)
        out_root.mkdir(parents=True, exist_ok=True)
        cache_key, identities = build_cache_key(
            inputs, options.code_ref, options.case_name, options.scenario, rules_fingerprint,
            identity_provider=self.inventory.identity, version_provider=self.version_provider
        )
        if options.run_id is not None:
            scoped_key = json.dumps([cache_key, options.run_id], ensure_ascii=False).encode("utf-8")
            cache_key = hashlib.sha256(scoped_key).hexdigest()
        if not options.force:
            cached = find_cached_run(out_root, cache_key)
            if cached:
                return {"run_dir": cached, "cached": True}
        run_dir, created_at = new_run_dir(out_root, options.case_name, cache_key)
        self.inventory.preserve(inputs, run_dir)
        return {
            "run_dir": run_dir,
            "created_at": created_at,
            "cache_key": cache_key,
            "identities": identities,
            "cached": False,
        }

    def write_parse_outputs(self, run_dir, options, bundle):
        options = options or RunOptions()
        manifest = {
            "schema_version": 1,
            "case_name": options.case_name,
            "run_id": options.run_id,
            "scenario": options.scenario,
            "analysis_created_at": bundle.created_at.isoformat(),
            "code_ref": options.code_ref,
            "script_version": self.version_provider(),
            "cache": {"key": bundle.cache_key, "status": "created"},
            "trace_time_range": bundle.report["dimensions"]["time"],
            "input_document": "inputs.md",
            "inputs": bundle.identities,
            "stages": {
                "parse": {"status": "done", "path": "parsed_traces.json"},
                "aggregate": {"status": "pending"},
                "triage": {"status": "pending"},
            },
            "render_targets": {
                "local": {"path": "report.local.html", "status": "pending"},
                "site": {"path": "report.site.html", "status": "pending"},
            },
        }
        self.write_json(run_dir / "manifest.json", manifest)
        self.write_json(run_dir / "inventory.json", {
            "schema_version": 1,
            "run_id": options.run_id or options.case_name,
            "input_count": len(bundle.identities),
            "listed_member_count": sum(len(item.get("members", ())) for item in bundle.identities),
            "total_bytes": sum(item.get("size", 0) for item in bundle.identities),
            "inputs": bundle.identities,
        })
        self.inventory.write_document(run_dir, manifest)
        (run_dir / "events.jsonl").write_text(
            "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in bundle.events),
            encoding="utf-8",
        )
        self.write_json(run_dir / "parsed_traces.json", bundle.report)

    @staticmethod
    def read_json(path):
        return read_json(path)

    @staticmethod
    def write_json(path, value):
        write_json(path, value)

    @staticmethod
    def update_manifest(run_dir, updater):
        update_manifest(run_dir, updater)

    @staticmethod
    def write_text(path, text):
        Path(path).write_text(text, encoding="utf-8")

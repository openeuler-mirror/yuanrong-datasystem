"""Typed inputs and outputs for a trace Run."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RunOptions:
    case_name: str = "trace-case"
    scenario: str = ""
    code_ref: str = "unknown"
    force: bool = False
    allow_partial_inputs: bool = False
    run_id: str | None = None


@dataclass
class ParseOutputBundle:
    report: Any
    events: Any
    created_at: Any
    cache_key: str
    identities: Any


@dataclass(frozen=True)
class RunBooleanControls:
    allow_partial_inputs: bool = False
    local_cache: bool | None = None


def validate_run_booleans(config):
    partial = config.get("allow_partial_inputs", False)
    local_cache = config.get("local_cache")
    for name, value, nullable in (("allow_partial_inputs", partial, False),
                                  ("local_cache", local_cache, True)):
        if not isinstance(value, bool) and not (nullable and value is None):
            expected = "boolean or null" if nullable else "boolean"
            raise ValueError(f"Run {config.get('id', '<unnamed>')}: {name} must be {expected}")
    return RunBooleanControls(partial, local_cache)

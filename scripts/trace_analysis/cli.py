"""Unified command dispatcher; each stage owns its existing argument contract."""
import argparse
import importlib
import sys

COMMANDS = {
    "pipeline": "pipeline", "triage": "triage", "read": "bottleneck",
    "write": "write_report", "numa": "numa", "suite": "suite",
    "validate": "validation", "package": "packaging",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=COMMANDS)
    args = parser.parse_args(sys.argv[1:2])
    remaining = sys.argv[2:]
    module = importlib.import_module("." + COMMANDS[args.command], __package__)
    original = sys.argv
    try:
        sys.argv = [original[0], *remaining]
        return module.main()
    finally:
        sys.argv = original

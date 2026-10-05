"""Trace test support shared across domain directories."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
for name in ("support", "ingest", "evidence", "analysis", "pipeline", "rendering", "delivery"):
    sys.path.insert(0, str(ROOT / name))

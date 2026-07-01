import os
from pathlib import Path


def _configure_runtime(pkg_dir: Path) -> None:
    candidate_dirs = [pkg_dir, pkg_dir / "lib"]
    for candidate_dir in candidate_dirs:
        p2p_so = candidate_dir / "libp2p_transfer.so"
        if p2p_so.exists():
            os.environ.setdefault("TRANSFER_ENGINE_P2P_SO_PATH", str(p2p_so))
            break

_PKG_DIR = Path(__file__).resolve().parent
_configure_runtime(_PKG_DIR)

from ._transfer_engine import ErrorCode, Result, TransferEngine


__all__ = ["Result", "ErrorCode", "TransferEngine"]

import ctypes
import os
from pathlib import Path


def _preload_runtime_shared_libs(pkg_dir: Path) -> None:
    candidate_dirs = [pkg_dir, pkg_dir / "lib"]
    for candidate_dir in candidate_dirs:
        p2p_so = candidate_dir / "libp2p_transfer.so"
        if p2p_so.exists():
            os.environ.setdefault("TRANSFER_ENGINE_P2P_SO_PATH", str(p2p_so))
            break

    rtld_global = getattr(ctypes, "RTLD_GLOBAL", 0)
    for base_dir in candidate_dirs:
        if not base_dir.exists():
            continue
        for pattern in ("libglog.so*", "libprotobuf.so*", "libabsl*.so*"):
            for so_path in sorted(base_dir.glob(pattern)):
                try:
                    ctypes.CDLL(str(so_path), mode=rtld_global)
                except OSError:
                    pass


_PKG_DIR = Path(__file__).resolve().parent
_preload_runtime_shared_libs(_PKG_DIR)

from ._transfer_engine import ErrorCode, Result, TransferEngine


__all__ = ["Result", "ErrorCode", "TransferEngine"]

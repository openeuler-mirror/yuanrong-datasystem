import ctypes
import os
from pathlib import Path


def _preload_runtime_shared_libs(pkg_dir: Path) -> None:
    p2p_so = pkg_dir / "libp2ptransfer.so"
    if p2p_so.exists():
        os.environ.setdefault("TRANSFER_ENGINE_P2P_SO_PATH", str(p2p_so))

    rtld_global = getattr(ctypes, "RTLD_GLOBAL", 0)
    for pattern in ("libglog.so*", "libprotobuf.so*", "libabsl*.so*"):
        for so_path in sorted(pkg_dir.glob(pattern)):
            try:
                ctypes.CDLL(str(so_path), mode=rtld_global)
            except OSError:
                pass


_PKG_DIR = Path(__file__).resolve().parent
_preload_runtime_shared_libs(_PKG_DIR)

from ._transfer_engine import Status, StatusCode, TransferEngine


__all__ = ["Status", "StatusCode", "TransferEngine"]

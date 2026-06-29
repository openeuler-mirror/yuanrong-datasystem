import sys
import types

import yr.datasystem  # noqa: F401


for module_name in ("yr.datasystem.lib", "yr.datasystem.lib.libds_client_py"):
    if module_name in sys.modules:
        raise RuntimeError(f"{module_name} was loaded during parent package import")

fake_transfer_engine = types.ModuleType("yr.datasystem._transfer_engine")
fake_transfer_engine.TransferEngine = object()
fake_transfer_engine.Result = object()
fake_transfer_engine.ErrorCode = object()
sys.modules["yr.datasystem._transfer_engine"] = fake_transfer_engine

from yr.datasystem import TransferEngine  # noqa: E402

if TransferEngine is not fake_transfer_engine.TransferEngine:
    raise RuntimeError("TransferEngine was not loaded from the optional transfer-engine module")

for module_name in ("yr.datasystem.lib", "yr.datasystem.lib.libds_client_py"):
    if module_name in sys.modules:
        raise RuntimeError(f"{module_name} was loaded during TransferEngine import")

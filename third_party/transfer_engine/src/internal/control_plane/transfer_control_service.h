#ifndef TRANSFER_ENGINE_INTERNAL_TRANSFER_CONTROL_SERVICE_H
#define TRANSFER_ENGINE_INTERNAL_TRANSFER_CONTROL_SERVICE_H

#include <cstdint>
#include <memory>
#include <string>

#include "internal/control_plane/control_plane.h"

namespace datasystem {

class ConnectionManager;
class RegisteredMemoryTable;
class IDataPlaneBackend;

std::shared_ptr<ITransferControlService> CreateTransferControlService(
    const std::string &localHost, uint16_t localPort, int32_t localDeviceId,
    std::shared_ptr<ConnectionManager> connMgr, std::shared_ptr<RegisteredMemoryTable> registeredMemory,
    std::shared_ptr<IDataPlaneBackend> backend);

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_TRANSFER_CONTROL_SERVICE_H

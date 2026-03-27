#ifndef TRANSFER_ENGINE_INTERNAL_TRANSFER_CONTROL_DISPATCHER_H
#define TRANSFER_ENGINE_INTERNAL_TRANSFER_CONTROL_DISPATCHER_H

#include <memory>
#include <vector>

#include "internal/control_plane/control_plane.h"
#include "internal/control_plane/control_plane_codec.h"
#include "datasystem/transfer_engine/status.h"

namespace datasystem {

Result DispatchControlRequest(const std::shared_ptr<ITransferControlService> &service, RpcMethod method,
                              const std::vector<uint8_t> &reqPayload, RpcMethod *rspMethod,
                              std::vector<uint8_t> *rspPayload);

}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_TRANSFER_CONTROL_DISPATCHER_H

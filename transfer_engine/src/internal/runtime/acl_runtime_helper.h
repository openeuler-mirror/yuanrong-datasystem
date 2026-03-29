#ifndef TRANSFER_ENGINE_INTERNAL_ACL_RUNTIME_HELPER_H
#define TRANSFER_ENGINE_INTERNAL_ACL_RUNTIME_HELPER_H

#include <cstdint>

#include "datasystem/transfer_engine/status.h"

namespace datasystem {
namespace internal {

Result EnsureAclSetDeviceForCurrentThread(int32_t deviceId);

}  // namespace internal
}  // namespace datasystem

#endif  // TRANSFER_ENGINE_INTERNAL_ACL_RUNTIME_HELPER_H

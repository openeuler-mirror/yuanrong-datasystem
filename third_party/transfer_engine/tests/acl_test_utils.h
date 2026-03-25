#ifndef TRANSFER_ENGINE_TESTS_ACL_TEST_UTILS_H
#define TRANSFER_ENGINE_TESTS_ACL_TEST_UTILS_H

#include <cstddef>
#include <cstdint>

#include "datasystem/transfer_engine/status.h"

namespace datasystem {
namespace testutil {

Status EnsureAclInitialized();
Status SetAclDevice(int32_t deviceId);
Status AclMalloc(size_t size, void **devPtr);
Status AclFree(void *devPtr);
Status AclMemcpy(void *dst, size_t dstSize, const void *src, size_t srcSize, int32_t kind);

}  // namespace testutil
}  // namespace datasystem

#endif  // TRANSFER_ENGINE_TESTS_ACL_TEST_UTILS_H

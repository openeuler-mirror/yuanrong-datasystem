#ifndef TRANSFER_ENGINE_TESTS_ACL_TEST_UTILS_H
#define TRANSFER_ENGINE_TESTS_ACL_TEST_UTILS_H

#include <cstddef>
#include <cstdint>

#include "datasystem/transfer_engine/status.h"

namespace datasystem {
namespace testutil {

Result EnsureAclInitialized();
Result SetAclDevice(int32_t deviceId);
Result AclMalloc(size_t size, void **devPtr);
Result AclFree(void *devPtr);
Result AclMemcpy(void *dst, size_t dstSize, const void *src, size_t srcSize, int32_t kind);

}  // namespace testutil
}  // namespace datasystem

#endif  // TRANSFER_ENGINE_TESTS_ACL_TEST_UTILS_H

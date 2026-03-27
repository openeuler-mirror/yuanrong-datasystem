#include "internal/runtime/acl_runtime_helper.h"

#include <cstdlib>
#include <dlfcn.h>

#include <string>

#include <glog/logging.h>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace internal {
namespace {

using FnAclrtSetDevice = int (*)(int32_t);

struct AclRuntimeLoader {
    void *handle = nullptr;
    FnAclrtSetDevice setDevice = nullptr;
    std::string initError;

    AclRuntimeLoader()
    {
        handle = dlopen("libascendcl.so", RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            initError = std::string("dlopen libascendcl.so failed: ") + dlerror();
            return;
        }
        setDevice = reinterpret_cast<FnAclrtSetDevice>(dlsym(handle, "aclrtSetDevice"));
        const char *symErr = dlerror();
        if (setDevice == nullptr) {
            initError = std::string("dlsym aclrtSetDevice failed: ") +
                        (symErr == nullptr ? "unknown error" : symErr);
        }
    }

    ~AclRuntimeLoader()
    {
        if (handle != nullptr) {
            dlclose(handle);
        }
    }
};

AclRuntimeLoader &GetAclRuntimeLoader()
{
    static AclRuntimeLoader loader;
    return loader;
}

}  // namespace

Result EnsureAclSetDeviceForCurrentThread(int32_t deviceId)
{
    auto &loader = GetAclRuntimeLoader();
    if (!loader.initError.empty()) {
        return TE_MAKE_STATUS(ErrorCode::kNotReady, loader.initError);
    }

    const char *visible = std::getenv("ASCEND_RT_VISIBLE_DEVICES");
    LOG(INFO) << "EnsureAclSetDeviceForCurrentThread"
              << ", device_id=" << deviceId
              << ", ASCEND_RT_VISIBLE_DEVICES=" << (visible == nullptr ? "<unset>" : visible);

    const int rc = loader.setDevice(deviceId);
    if (rc != 0) {
        return TE_MAKE_STATUS(ErrorCode::kRuntimeError,
                              "aclrtSetDevice failed, device_id=" + std::to_string(deviceId) +
                                  ", rc=" + std::to_string(rc) +
                                  ", hint: please call aclInit(nullptr) before using transfer_engine.");
    }
    return Result::OK();
}

}  // namespace internal
}  // namespace datasystem

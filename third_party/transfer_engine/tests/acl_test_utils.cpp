#include "acl_test_utils.h"

#include <dlfcn.h>

#include <string>

#include "datasystem/transfer_engine/status_helper.h"

namespace datasystem {
namespace testutil {
namespace {

using FnAclInit = int (*)(const char *);
using FnAclSetDevice = int (*)(int32_t);
using FnAclMalloc = int (*)(void **, size_t, int32_t);
using FnAclFree = int (*)(void *);
using FnAclMemcpy = int (*)(void *, size_t, const void *, size_t, int32_t);

struct AclApi {
    void *handle = nullptr;
    FnAclInit aclInit = nullptr;
    FnAclSetDevice aclSetDevice = nullptr;
    FnAclMalloc aclMalloc = nullptr;
    FnAclFree aclFree = nullptr;
    FnAclMemcpy aclMemcpy = nullptr;
    std::string initError;

    AclApi()
    {
        handle = dlopen("libascendcl.so", RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            initError = std::string("dlopen libascendcl.so failed: ") + dlerror();
            return;
        }
        aclInit = reinterpret_cast<FnAclInit>(dlsym(handle, "aclInit"));
        aclSetDevice = reinterpret_cast<FnAclSetDevice>(dlsym(handle, "aclrtSetDevice"));
        aclMalloc = reinterpret_cast<FnAclMalloc>(dlsym(handle, "aclrtMalloc"));
        aclFree = reinterpret_cast<FnAclFree>(dlsym(handle, "aclrtFree"));
        aclMemcpy = reinterpret_cast<FnAclMemcpy>(dlsym(handle, "aclrtMemcpy"));

        if (aclInit == nullptr || aclSetDevice == nullptr || aclMalloc == nullptr || aclFree == nullptr ||
            aclMemcpy == nullptr) {
            const char *err = dlerror();
            initError = std::string("dlsym acl symbol failed: ") + (err == nullptr ? "unknown error" : err);
        }
    }

    ~AclApi()
    {
        if (handle != nullptr) {
            dlclose(handle);
        }
    }
};

AclApi &GetAclApi()
{
    static AclApi api;
    return api;
}

}  // namespace

Status EnsureAclInitialized()
{
    auto &api = GetAclApi();
    if (!api.initError.empty()) {
        return TE_MAKE_STATUS(StatusCode::kNotReady, api.initError);
    }
    const int rc = api.aclInit(nullptr);
    // 0: success, 100002: already initialized.
    if (rc != 0 && rc != 100002) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "aclInit failed, rc=" + std::to_string(rc));
    }
    return Status::OK();
}

Status SetAclDevice(int32_t deviceId)
{
    auto &api = GetAclApi();
    if (!api.initError.empty()) {
        return TE_MAKE_STATUS(StatusCode::kNotReady, api.initError);
    }
    const int rc = api.aclSetDevice(deviceId);
    if (rc != 0) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError,
                              "aclrtSetDevice failed, device_id=" + std::to_string(deviceId) +
                                  ", rc=" + std::to_string(rc));
    }
    return Status::OK();
}

Status AclMalloc(size_t size, void **devPtr)
{
    TE_CHECK_PTR_OR_RETURN(devPtr);
    auto &api = GetAclApi();
    if (!api.initError.empty()) {
        return TE_MAKE_STATUS(StatusCode::kNotReady, api.initError);
    }
    const int rc = api.aclMalloc(devPtr, size, 0);
    if (rc != 0) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "aclrtMalloc failed, rc=" + std::to_string(rc));
    }
    return Status::OK();
}

Status AclFree(void *devPtr)
{
    if (devPtr == nullptr) {
        return Status::OK();
    }
    auto &api = GetAclApi();
    if (!api.initError.empty()) {
        return TE_MAKE_STATUS(StatusCode::kNotReady, api.initError);
    }
    const int rc = api.aclFree(devPtr);
    if (rc != 0) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "aclrtFree failed, rc=" + std::to_string(rc));
    }
    return Status::OK();
}

Status AclMemcpy(void *dst, size_t dstSize, const void *src, size_t srcSize, int32_t kind)
{
    auto &api = GetAclApi();
    if (!api.initError.empty()) {
        return TE_MAKE_STATUS(StatusCode::kNotReady, api.initError);
    }
    const int rc = api.aclMemcpy(dst, dstSize, src, srcSize, kind);
    if (rc != 0) {
        return TE_MAKE_STATUS(StatusCode::kRuntimeError, "aclrtMemcpy failed, rc=" + std::to_string(rc));
    }
    return Status::OK();
}

}  // namespace testutil
}  // namespace datasystem

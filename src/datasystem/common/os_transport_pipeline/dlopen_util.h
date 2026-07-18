/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: shared loader for libcudart.so and direct os_transport wrappers
 */

#ifndef OS_XPRT_PIPLN_DLOPEN_UTIL
#define OS_XPRT_PIPLN_DLOPEN_UTIL

#include <cstdint>
#ifndef PIPLN_USE_MOCK
#include <cuda_runtime.h>
#endif
#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_opcode.h>

#include "datasystem/common/util/dlutils.h"
#include "os-transport/os_transport.h"

namespace OsXprtPipln {

struct LibLoaderBase {
    LibLoaderBase(const std::string &libName);
    virtual void Load();
    virtual void UnLoad();

    bool TryLoad(const std::string &path);
    void AddPath(const std::string &path);
    bool Inited()
    {
        return handle_ != nullptr;
    }

    std::vector<std::string> libpaths_;
    void *handle_ = nullptr;
    std::string libName_;
};

#define CALL_DYN_FUNC_BASE(type, ret, funcName, ...)                 \
    {                                                                \
        auto &func = OsXprtPipln::type::Instance()->funcName##Func_; \
        if (func)                                                    \
            ret = static_cast<decltype(ret)>(func(__VA_ARGS__));     \
        else                                                         \
            ret = static_cast<decltype(ret)>(-1);                    \
    }

#define DO_LOAD_DYNLIB(type) type::Instance()->Load();
#define DO_UNLOAD_DYNLIB(type) type::Instance()->UnLoad();
#define IS_VALID_DYNFUNC(type, func) (type::Instance()->func##Func_ != nullptr)

#define DO_LOAD_OS_TRANSPORT() static_cast<void>(0)
#define DO_UNLOAD_OS_TRANSPORT() static_cast<void>(0)
#define CALL_OS_XPRT_FUNC(ret, funcName, ...)                                     \
    do {                                                                          \
        ret = static_cast<decltype(ret)>(OS_XPRT_DIRECT_##funcName(__VA_ARGS__)); \
    } while (0)

#define OS_XPRT_DIRECT_DoLogReg os_transport_log_reg
#define OS_XPRT_DIRECT_DoInit os_transport_init
#define OS_XPRT_DIRECT_DoRecv os_transport_recv
#define OS_XPRT_DIRECT_DoSend os_transport_send
#define OS_XPRT_DIRECT_DoDestroy os_transport_destroy
#define OS_XPRT_DIRECT_DoWaitTimeout wait_and_free_sync_timeout
#define OS_XPRT_DIRECT_DoNotify os_transport_wake_up_task
#define OS_XPRT_DIRECT_DoCancel os_transport_cancel_tasks

#ifndef PIPLN_USE_MOCK
struct CudaRTLibLoader : public LibLoaderBase {
    CudaRTLibLoader() : LibLoaderBase("libcudart.so")
    {
    }
    void Load() override;
    void UnLoad() override;
    static CudaRTLibLoader *Instance();

    // cuda toolkit
    REG_METHOD(cudaStreamCreateWithFlags, cudaError_t, cudaStream_t *, unsigned int);
    REG_METHOD(cudaEventCreate, cudaError_t, cudaEvent_t *, unsigned int);
    REG_METHOD(cudaMemcpyAsync, cudaError_t, void *, const void *, size_t, cudaMemcpyKind, cudaStream_t);
    REG_METHOD(cudaEventRecord, cudaError_t, cudaEvent_t, cudaStream_t);
    REG_METHOD(cudaStreamDestroy, cudaError_t, cudaStream_t);
    REG_METHOD(cudaEventSynchronize, cudaError_t, cudaEvent_t);
    REG_METHOD(cudaEventDestroy, cudaError_t, cudaEvent_t);
    REG_METHOD(cudaGetErrorString, const char *, cudaError_t);
    REG_METHOD(cudaHostRegister, cudaError_t, void *, size_t, unsigned int);
    REG_METHOD(cudaHostUnregister, cudaError_t, void *);
};
#else
typedef int cudaError_t;
struct CudaRTLibLoader {
    void Load(){};
    void UnLoad(){};
    static CudaRTLibLoader *Instance()
    {
        static struct CudaRTLibLoader dummy;
        return &dummy;
    }
};
#endif
#define CALL_CUDA_RT_FUNC(ret, funcName, ...) CALL_DYN_FUNC_BASE(CudaRTLibLoader, ret, funcName, __VA_ARGS__);
}  // namespace OsXprtPipln
#endif

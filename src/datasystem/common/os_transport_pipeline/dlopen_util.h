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
 * Description: share lib loaders for libcudart.so and os_transport.so
 */

#ifndef OS_XPRT_PIPLN_DLOPEN_UTIL
#define OS_XPRT_PIPLN_DLOPEN_UTIL

#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_opcode.h>

#include "datasystem/common/util/dlutils.h"
#include "os-transport/os_transport.h"

namespace OsXprtPipln {

struct LibLoaderBase {
    LibLoaderBase(const std::string &libName) : libName_(libName){};
    virtual void Load();
    virtual void UnLoad();
    bool Inited()
    {
        return handle_ != nullptr;
    }

    void *handle_ = nullptr;
    std::string libName_;
};

#define CALL_DYN_FUNC_BASE(type, ret, funcName, ...)                   \
    {                                                                  \
        auto &func = OsXprtPipln::type::Instance()->funcName##Func_; \
        if (func)                                                      \
            ret = static_cast<decltype(ret)>(func(__VA_ARGS__));       \
        else                                                           \
            ret = static_cast<decltype(ret)>(-1);                      \
    }

#define DO_LOAD_DYNLIB(type) type::Instance()->Load();
#define DO_UNLOAD_DYNLIB(type) type::Instance()->UnLoad();
#define IS_VALID_DYNFUNC(type, func) (type::Instance()->func##Func_ != nullptr)

struct OsTransportLibLoader : public LibLoaderBase {
    OsTransportLibLoader() : LibLoaderBase("libos_transport.so")
    {
    }
    void Load() override;
    void UnLoad() override;
    static OsTransportLibLoader *Instance();

    // os transport
    REG_METHOD(DoInit, uint32_t, urma_context_t *urma_ctx, os_transport_cfg_t *ost_cfg, void **handle);
    REG_METHOD(DoRecv, uint32_t, void *handle, ost_buffer_info_t *host_src, ost_device_info_t *device_dst, uint32_t len,
               uint32_t client_key, task_sync_t **ret_sync_handle);
    REG_METHOD(DoSend, uint32_t, void *handle, urma_jetty_info_t *jetty_info, ost_buffer_info_t *local_src,
               ost_buffer_info_t *remote_dst, uint32_t len, uint32_t server_key, uint32_t client_key,
               task_sync_t **ret_sync_handle);
    REG_METHOD(DoDestroy, uint32_t, void *handle);
    REG_METHOD(DoWait, uint32_t, void *handle, task_sync_t *sync_handle);
    REG_METHOD(DoNotify, int, void *handle, void *cr_t);
};

#define CALL_OS_XPRT_FUNC(ret, funcName, ...) CALL_DYN_FUNC_BASE(OsTransportLibLoader, ret, funcName, __VA_ARGS__);

struct CudaRTLibLoader : public LibLoaderBase {
    CudaRTLibLoader() : LibLoaderBase("libcudart.so")
    {
    }
    void Load() override;
    void UnLoad() override;
    static CudaRTLibLoader *Instance();

    // cuda toolkit
    REG_METHOD(cudaSetDevice, cudaError_t, int);
    REG_METHOD(cudaIpcOpenMemHandle, cudaError_t, void **, cudaIpcMemHandle_t, unsigned int);
    REG_METHOD(cudaStreamCreateWithFlags, cudaError_t, cudaStream_t *, unsigned int);
    REG_METHOD(cudaEventCreate, cudaError_t, cudaEvent_t *, unsigned int);
    REG_METHOD(cudaMemcpyAsync, cudaError_t, void *, const void *, size_t, cudaMemcpyKind, cudaStream_t);
    REG_METHOD(cudaEventRecord, cudaError_t, cudaEvent_t, cudaStream_t);
    REG_METHOD(cudaStreamWaitEvent, cudaError_t, cudaStream_t, cudaEvent_t, unsigned int);
    REG_METHOD(cudaEventDestroy, cudaError_t, cudaEvent_t);
    REG_METHOD(cudaIpcCloseMemHandle, cudaError_t, void *);
    REG_METHOD(cudaIpcGetMemHandle, cudaError_t, cudaIpcMemHandle_t *, void *);
    REG_METHOD(cudaGetErrorString, const char *, cudaError_t);
};

#define CALL_CUDA_RT_FUNC(ret, funcName, ...) CALL_DYN_FUNC_BASE(CudaRTLibLoader, ret, funcName, __VA_ARGS__);

}  // namespace OsXprtPipln
#endif
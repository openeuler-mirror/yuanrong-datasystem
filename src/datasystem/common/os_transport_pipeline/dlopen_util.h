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
 * Description: Direct os_transport wrappers.
 */

#ifndef OS_XPRT_PIPLN_DLOPEN_UTIL
#define OS_XPRT_PIPLN_DLOPEN_UTIL

#include <cstdint>
#ifndef PIPLN_USE_MOCK
#include <cuda_runtime.h>
#endif

#include "datasystem/common/util/cuda_host_memory.h"
#include "os-transport/os_transport.h"

namespace OsXprtPipln {

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
template <typename T>
T LoadCudaRuntimeFunc(const char *name)
{
    return reinterpret_cast<T>(reinterpret_cast<intptr_t>(datasystem::GetCudaRuntimeSymbol(name)));
}

#define CALL_CUDA_RT_FUNC(ret, funcName, ...)                              \
    do {                                                                   \
        using CudaFunc = decltype(&funcName);                              \
        static CudaFunc cudaFunc = LoadCudaRuntimeFunc<CudaFunc>(#funcName); \
        ret = cudaFunc == nullptr ? static_cast<decltype(ret)>(-1)         \
                                  : static_cast<decltype(ret)>(cudaFunc(__VA_ARGS__)); \
    } while (0)
#endif

}  // namespace OsXprtPipln
#endif

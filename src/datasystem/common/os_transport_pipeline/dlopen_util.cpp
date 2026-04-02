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

#include "datasystem/common/os_transport_pipeline/dlopen_util.h"

namespace OsXprtPipln {

using namespace datasystem;

#define DLSYM_FUNC_OBJ_DESTROY(funcName) funcName##Func_ = nullptr;

#define DLSYM_FUNC_OBJ_LINK(funcName, originName, pluginHandle) \
    Instance()->funcName##Func_ = DlsymWithCast<funcName##FunPtr>(pluginHandle, #originName);

void LibLoaderBase::Load()
{
    handle_ = dlopen(libName_.c_str(), RTLD_LAZY | RTLD_GLOBAL);
    if (!handle_) {
        LOG(ERROR) << "dlopen " << libName_ << " failed for: libos_transport.so -> " << GetDlErrorMsg();
        return;
    }

    char origin[PATH_MAX] = {};
    const bool hasOrigin = (dlinfo(handle_, RTLD_DI_ORIGIN, origin) == 0 && origin[0] != '\0');
    LOG(INFO) << "dlopen " << libName_ << " succeeded for: libos_transport.so"
              << (hasOrigin ? std::string(", origin path: ") + origin : "");
}

void LibLoaderBase::UnLoad()
{
    if (!handle_)
        return;
    dlclose(handle_);
    handle_ = nullptr;
}

OsTransportLibLoader *OsTransportLibLoader::Instance()
{
    static OsTransportLibLoader inst;
    return &inst;
}

void OsTransportLibLoader::Load()
{
    LibLoaderBase::Load();
    DLSYM_FUNC_OBJ_LINK(DoInit, os_transport_init, handle_);
    DLSYM_FUNC_OBJ_LINK(DoRecv, os_transport_recv, handle_);
    DLSYM_FUNC_OBJ_LINK(DoSend, os_transport_send, handle_);
    DLSYM_FUNC_OBJ_LINK(DoDestroy, os_transport_destroy, handle_);
    DLSYM_FUNC_OBJ_LINK(DoWait, wait_and_free_sync, handle_);
    DLSYM_FUNC_OBJ_LINK(DoNotify, os_transport_wake_up_task, handle_);
}

void OsTransportLibLoader::UnLoad()
{
    LibLoaderBase::UnLoad();
    DLSYM_FUNC_OBJ_DESTROY(DoInit);
    DLSYM_FUNC_OBJ_DESTROY(DoRecv);
    DLSYM_FUNC_OBJ_DESTROY(DoSend);
    DLSYM_FUNC_OBJ_DESTROY(DoDestroy);
    DLSYM_FUNC_OBJ_DESTROY(DoWait);
    DLSYM_FUNC_OBJ_DESTROY(DoNotify);
}

CudaRTLibLoader *CudaRTLibLoader::Instance()
{
    static CudaRTLibLoader inst;
    return &inst;
}

void CudaRTLibLoader::Load()
{
    LibLoaderBase::Load();
    DLSYM_FUNC_OBJ(cudaSetDevice, handle_);
    DLSYM_FUNC_OBJ(cudaIpcOpenMemHandle, handle_);
    DLSYM_FUNC_OBJ(cudaStreamCreateWithFlags, handle_);
    DLSYM_FUNC_OBJ(cudaEventCreate, handle_);
    DLSYM_FUNC_OBJ(cudaMemcpyAsync, handle_);
    DLSYM_FUNC_OBJ(cudaEventRecord, handle_);
    DLSYM_FUNC_OBJ(cudaStreamWaitEvent, handle_);
    DLSYM_FUNC_OBJ(cudaEventDestroy, handle_);
    DLSYM_FUNC_OBJ(cudaIpcCloseMemHandle, handle_);
    DLSYM_FUNC_OBJ(cudaIpcGetMemHandle, handle_);
    DLSYM_FUNC_OBJ(cudaGetErrorString, handle_);
}

void CudaRTLibLoader::UnLoad()
{
    LibLoaderBase::UnLoad();
    DLSYM_FUNC_OBJ_DESTROY(cudaSetDevice);
    DLSYM_FUNC_OBJ_DESTROY(cudaIpcOpenMemHandle);
    DLSYM_FUNC_OBJ_DESTROY(cudaStreamCreateWithFlags);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventCreate);
    DLSYM_FUNC_OBJ_DESTROY(cudaMemcpyAsync);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventRecord);
    DLSYM_FUNC_OBJ_DESTROY(cudaStreamWaitEvent);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventDestroy);
    DLSYM_FUNC_OBJ_DESTROY(cudaIpcCloseMemHandle);
    DLSYM_FUNC_OBJ_DESTROY(cudaIpcGetMemHandle);
    DLSYM_FUNC_OBJ_DESTROY(cudaGetErrorString);
}

}  // namespace OsXprtPipln
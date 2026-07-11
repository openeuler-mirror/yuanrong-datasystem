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
 * Description: shared loader for libcudart.so
 */

#include "datasystem/common/os_transport_pipeline/dlopen_util.h"

#include <cstdio>
#include <mutex>

static constexpr int MAX_POPEN_LINE_LENGTH = 256;

namespace OsXprtPipln {

using namespace datasystem;

#define DLSYM_FUNC_OBJ_DESTROY(funcName) funcName##Func_ = nullptr;

#define DLSYM_FUNC_OBJ_LINK(funcName, originName, pluginHandle) \
    Instance()->funcName##Func_ = DlsymWithCast<funcName##FunPtr>(pluginHandle, #originName);

static std::vector<std::string> g_defaultLibPaths;
static std::mutex g_loadDefaultPathMutex;
static void FindDSInstallPath()
{
    std::string dsPath = "";
    FILE *f = popen("pip show openyuanrong-datasystem", "r");
    char buf[MAX_POPEN_LINE_LENGTH];
    const char *whitespace = " \t\n\r\f\v";
    while (fgets(buf, MAX_POPEN_LINE_LENGTH, f)) {
        std::string line = buf;
        static std::string prefix = "Location: ";
        size_t pos = line.find(prefix);
        if (pos != std::string::npos) {
            pos += prefix.size();
            size_t end = line.find_last_not_of(whitespace);
            g_defaultLibPaths.push_back(line.substr(pos, end - pos + 1) + "/yr/datasystem/lib/");
        }
    }
    pclose(f);
    return;
}

void LibLoaderBase::AddPath(const std::string &path)
{
    if (path.empty())
        libpaths_.push_back(path);  // means search default path first
    libpaths_.push_back(path + "/");
}

LibLoaderBase::LibLoaderBase(const std::string &libName) : libName_(libName)
{
    if (g_defaultLibPaths.empty()) {
        std::lock_guard<std::mutex> l(g_loadDefaultPathMutex);
        if (g_defaultLibPaths.empty()) {
            FindDSInstallPath();
            g_defaultLibPaths.push_back("");  // system path
        }
    }
}

bool LibLoaderBase::TryLoad(const std::string &path)
{
    std::string filePath = path + libName_;
    handle_ = dlopen(filePath.c_str(), RTLD_LAZY | RTLD_GLOBAL);
    if (!handle_) {
        LOG(WARNING) << "try dlopen " << libName_ << " from " << path << " failed -> " << GetDlErrorMsg();
        return false;
    }
    return true;
}

void LibLoaderBase::Load()
{
    std::vector<std::string> searchPaths;
    searchPaths.insert(searchPaths.end(), libpaths_.begin(), libpaths_.end());
    searchPaths.insert(searchPaths.end(), g_defaultLibPaths.begin(), g_defaultLibPaths.end());

    for (auto &path : searchPaths) {
        if (TryLoad(path))
            break;
    }
    if (!handle_)
        return;

    char origin[PATH_MAX] = {};
    const bool hasOrigin = (dlinfo(handle_, RTLD_DI_ORIGIN, origin) == 0 && origin[0] != '\0');
    LOG(INFO) << "dlopen " << libName_ << " succeeded for: " << libName_
              << (hasOrigin ? std::string(", origin path: ") + origin : "");
}

void LibLoaderBase::UnLoad()
{
    if (!handle_)
        return;
    dlclose(handle_);
    handle_ = nullptr;
}


#ifndef PIPLN_USE_MOCK
CudaRTLibLoader *CudaRTLibLoader::Instance()
{
    static CudaRTLibLoader inst;
    return &inst;
}
void CudaRTLibLoader::Load()
{
    LibLoaderBase::Load();
    DLSYM_FUNC_OBJ(cudaSetDevice, handle_);
    DLSYM_FUNC_OBJ(cudaDeviceReset, handle_);
    DLSYM_FUNC_OBJ(cudaIpcOpenMemHandle, handle_);
    DLSYM_FUNC_OBJ(cudaStreamCreateWithFlags, handle_);
    DLSYM_FUNC_OBJ(cudaEventCreate, handle_);
    DLSYM_FUNC_OBJ(cudaStreamDestroy, handle_);
    DLSYM_FUNC_OBJ(cudaMemcpyAsync, handle_);
    DLSYM_FUNC_OBJ(cudaEventRecord, handle_);
    DLSYM_FUNC_OBJ(cudaEventSynchronize, handle_);
    DLSYM_FUNC_OBJ(cudaEventDestroy, handle_);
    DLSYM_FUNC_OBJ(cudaIpcCloseMemHandle, handle_);
    DLSYM_FUNC_OBJ(cudaIpcGetMemHandle, handle_);
    DLSYM_FUNC_OBJ(cudaGetErrorString, handle_);
    DLSYM_FUNC_OBJ(cudaHostRegister, handle_);
    DLSYM_FUNC_OBJ(cudaHostUnregister, handle_);
}

void CudaRTLibLoader::UnLoad()
{
    LibLoaderBase::UnLoad();
    DLSYM_FUNC_OBJ_DESTROY(cudaSetDevice);
    DLSYM_FUNC_OBJ_DESTROY(cudaDeviceReset);
    DLSYM_FUNC_OBJ_DESTROY(cudaIpcOpenMemHandle);
    DLSYM_FUNC_OBJ_DESTROY(cudaStreamCreateWithFlags);
    DLSYM_FUNC_OBJ_DESTROY(cudaStreamDestroy);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventCreate);
    DLSYM_FUNC_OBJ_DESTROY(cudaMemcpyAsync);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventRecord);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventSynchronize);
    DLSYM_FUNC_OBJ_DESTROY(cudaEventDestroy);
    DLSYM_FUNC_OBJ_DESTROY(cudaIpcCloseMemHandle);
    DLSYM_FUNC_OBJ_DESTROY(cudaIpcGetMemHandle);
    DLSYM_FUNC_OBJ_DESTROY(cudaGetErrorString);
    DLSYM_FUNC_OBJ_DESTROY(cudaHostRegister);
    DLSYM_FUNC_OBJ_DESTROY(cudaHostUnregister);
}
#endif
}  // namespace OsXprtPipln

/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/common/rdma/npu/hixl_plugin_loader.h"

#include <cstddef>
#include <dlfcn.h>
#include <iomanip>
#include <link.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/log/log.h"
#include "hixl_plugin_sha256.h"
#include "datasystem/common/util/dlutils.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {

constexpr const char *HIXL_PLUGIN_LIBRARY = "libds_hixl_plugin.so";
constexpr int64_t HIXL_PLUGIN_MAX_SIZE = 20 * 1024 * 1024;
constexpr int HASH_FIELD_WIDTH = 2;

std::string ParentDirectory(const std::string &path)
{
    const size_t separator = path.find_last_of('/');
    return separator == std::string::npos ? "." : path.substr(0, separator);
}

}  // namespace

HixlPluginLoader &HixlPluginLoader::Instance()
{
    static HixlPluginLoader loader;
    return loader;
}

HixlPluginLoader::HixlPluginLoader() : expectedSha256_(HIXL_PLUGIN_SHA256)
{
}

#ifdef WITH_TESTS
HixlPluginLoader::HixlPluginLoader(std::string pluginPath, std::string expectedSha256)
    : pluginPathOverride_(std::move(pluginPath)), expectedSha256_(std::move(expectedSha256))
{
}
#endif

Status HixlPluginLoader::GetApi(const DsHixlApi *&api)
{
    api = nullptr;
    std::call_once(loadOnce_, [this]() { loadStatus_ = LoadPlugin(); });
    RETURN_IF_NOT_OK(loadStatus_);
    api = api_;
    return Status::OK();
}

Status HixlPluginLoader::LoadPlugin()
{
    std::string pluginPath = pluginPathOverride_;
    if (pluginPath.empty()) {
        Dl_info dlInfo{};
        if (dladdr(reinterpret_cast<void *>(&HixlPluginLoader::Instance), &dlInfo) == 0
            || dlInfo.dli_fname == nullptr) {
            RETURN_STATUS_LOG_ERROR(K_NOT_SUPPORTED,
                                    FormatString("Load HIXL plugin failed, dladdr error: %s", GetDlErrorMsg()));
        }
        pluginPath = ParentDirectory(dlInfo.dli_fname) + "/" + HIXL_PLUGIN_LIBRARY;
    }
    RETURN_IF_NOT_OK(VerifySha256(pluginPath));

    void *candidateHandle = dlopen(pluginPath.c_str(), RTLD_NOW | RTLD_LOCAL);
    if (candidateHandle == nullptr) {
        RETURN_STATUS_LOG_ERROR(
            K_NOT_SUPPORTED,
            FormatString("Load HIXL plugin %s failed, dlopen error: %s", HIXL_PLUGIN_LIBRARY, GetDlErrorMsg()));
    }
    bool publishHandle = false;
    Raii closeRejectedPlugin([&candidateHandle, &publishHandle]() {
        if (!publishHandle) {
            dlclose(candidateHandle);
        }
    });

    auto getApi = DlsymWithCast<DsHixlGetApiFunc>(candidateHandle, "DsHixlGetApi");
    CHECK_FAIL_RETURN_STATUS(getApi != nullptr, K_NOT_SUPPORTED, "HIXL plugin entry DsHixlGetApi is unavailable");

    const DsHixlApi *api = nullptr;
    DsHixlResult result = getApi(DS_HIXL_ABI_VERSION_1, &api);
    CHECK_FAIL_RETURN_STATUS(result == DS_HIXL_OK, K_NOT_SUPPORTED,
                             FormatString("HIXL plugin rejected ABI version %u, result %d",
                                          DS_HIXL_ABI_VERSION_1, result));
    RETURN_IF_NOT_OK(ValidateApi(api));
    pluginHandle_ = candidateHandle;
    api_ = api;
    publishHandle = true;
    LOG(INFO) << "Loaded HIXL plugin " << HIXL_PLUGIN_LIBRARY << " with ABI version " << api_->abiVersion;
    return Status::OK();
}

Status HixlPluginLoader::VerifySha256(const std::string &pluginPath) const
{
    const int64_t fileSize = FileSize(pluginPath, false);
    CHECK_FAIL_RETURN_STATUS(fileSize >= 0, K_NOT_SUPPORTED,
                             FormatString("HIXL plugin locate failed: %s is missing", HIXL_PLUGIN_LIBRARY));
    CHECK_FAIL_RETURN_STATUS(fileSize <= HIXL_PLUGIN_MAX_SIZE, K_NOT_AUTHORIZED,
                             FormatString("HIXL plugin size %lld exceeds limit %lld", fileSize,
                                          HIXL_PLUGIN_MAX_SIZE));

    std::string fileContent;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadFileToString(pluginPath, fileContent), "Failed to read HIXL plugin");
    CHECK_FAIL_RETURN_STATUS(fileContent.size() <= static_cast<size_t>(HIXL_PLUGIN_MAX_SIZE), K_NOT_AUTHORIZED,
                             "HIXL plugin grew beyond the size limit while being verified");
    std::unique_ptr<unsigned char[]> hashData;
    unsigned int hashSize = 0;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.HashSHA256(fileContent.data(), fileContent.size(), hashData, hashSize));

    std::stringstream hash;
    for (unsigned int i = 0; i < hashSize; ++i) {
        hash << std::hex << std::setw(HASH_FIELD_WIDTH) << std::setfill('0') << static_cast<int>(hashData[i]);
    }
    const auto actualSha256 = hash.str();
    CHECK_FAIL_RETURN_STATUS(
        actualSha256 == expectedSha256_, K_NOT_AUTHORIZED,
        FormatString("HIXL plugin verify failed: integrity check rejected %s; expected sha256 %s, actual sha256 %s, "
                     "file size %lld, read size %zu. Ensure the plugin and core library come from the same build",
                     pluginPath, expectedSha256_, actualSha256, fileSize, fileContent.size()));
    return Status::OK();
}

Status HixlPluginLoader::ValidateApi(const DsHixlApi *api)
{
    constexpr size_t requiredSize =
        offsetof(DsHixlApi, transfer_sync) + sizeof(((DsHixlApi *)nullptr)->transfer_sync);
    CHECK_FAIL_RETURN_STATUS(api != nullptr, K_NOT_SUPPORTED, "HIXL plugin returned a null API table");
    CHECK_FAIL_RETURN_STATUS(api->abiVersion == DS_HIXL_ABI_VERSION_1, K_NOT_SUPPORTED,
                             FormatString("HIXL plugin ABI mismatch, expected %u, actual %u",
                                          DS_HIXL_ABI_VERSION_1, api->abiVersion));
    CHECK_FAIL_RETURN_STATUS(api->structSize >= requiredSize, K_NOT_SUPPORTED,
                             FormatString("HIXL plugin API table is too short, expected at least %zu, actual %u",
                                          requiredSize, api->structSize));
    CHECK_FAIL_RETURN_STATUS(api->create_engine != nullptr && api->finalize_engine != nullptr
                                 && api->destroy_engine != nullptr && api->initialize_engine != nullptr
                                 && api->connect_engine != nullptr && api->disconnect_engine != nullptr
                                 && api->register_memory != nullptr && api->deregister_memory != nullptr
                                 && api->transfer_sync != nullptr,
                             K_NOT_SUPPORTED, "HIXL plugin API table contains a null required function");
    return Status::OK();
}

}  // namespace datasystem

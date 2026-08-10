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

#ifndef DATASYSTEM_COMMON_RDMA_NPU_HIXL_PLUGIN_LOADER_H
#define DATASYSTEM_COMMON_RDMA_NPU_HIXL_PLUGIN_LOADER_H

#include <mutex>
#include <string>

#include "datasystem/common/rdma/npu/hixl_plugin_api.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class HixlPluginLoader {
public:
    static HixlPluginLoader &Instance();

    HixlPluginLoader(const HixlPluginLoader &) = delete;
    HixlPluginLoader(HixlPluginLoader &&) = delete;
    HixlPluginLoader &operator=(const HixlPluginLoader &) = delete;
    HixlPluginLoader &operator=(HixlPluginLoader &&) = delete;
    ~HixlPluginLoader() = default;

    Status GetApi(const DsHixlApi *&api);

#ifdef WITH_TESTS
    HixlPluginLoader(std::string pluginPath, std::string expectedSha256);
#endif

private:
    HixlPluginLoader();

    Status LoadPlugin();
    Status VerifySha256(const std::string &pluginPath) const;
    static Status ValidateApi(const DsHixlApi *api);

    std::once_flag loadOnce_;
    void *pluginHandle_ = nullptr;
    const DsHixlApi *api_ = nullptr;
    Status loadStatus_;
    std::string pluginPathOverride_;
    std::string expectedSha256_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_NPU_HIXL_PLUGIN_LOADER_H

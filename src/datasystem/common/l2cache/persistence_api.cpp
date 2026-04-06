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
 * Description: Persistence API factory and shared helpers.
 */

#include "datasystem/common/l2cache/persistence_api.h"

#include <curl/curl.h>

#include "datasystem/common/l2cache/aggregated_persistence_api.h"
#include "datasystem/common/l2cache/l2cache_client.h"
#include "datasystem/common/l2cache/object_persistence_api.h"
#include "datasystem/common/l2cache/slot_client/slot_client.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_string(l2_cache_type);
DS_DECLARE_string(sfs_path);
DS_DECLARE_string(distributed_disk_path);

namespace datasystem {
std::unique_ptr<PersistenceApi> PersistenceApi::Create()
{
    if (FLAGS_l2_cache_type == "distributed_disk") {
        return std::make_unique<AggregatedPersistenceApi>(std::make_unique<SlotClient>(FLAGS_distributed_disk_path));
    }
    return std::make_unique<ObjectPersistenceApi>();
}

std::shared_ptr<PersistenceApi> PersistenceApi::CreateShared()
{
    auto api = Create();
    return std::shared_ptr<PersistenceApi>(api.release());
}

Status PersistenceApi::UrlEncode(const std::string &objectPath, std::string &encodePath)
{
    std::ostringstream uniqSlash;
    CURL *curl = curl_easy_init();
    if (curl == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to init curl, encode the object key failed.");
    }
    Raii raii([&curl] { curl_easy_cleanup(curl); });
    char *urlEncode = curl_easy_escape(curl, objectPath.c_str(), objectPath.size());
    if (urlEncode == nullptr) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Failed to curl_easy_escape, encode the object key failed.");
    }

    Raii raiiEncode([&urlEncode] { curl_free(urlEncode); });
    std::string path(urlEncode);
    for (size_t i = 0; i < path.size(); i++) {
        if (path.at(i) == '%') {
            uniqSlash << L2CACHE_PERCENT_SIGN_ENCODE;
        } else {
            uniqSlash << path.at(i);
        }
    }
    encodePath = uniqSlash.str();
    return Status::OK();
}
}  // namespace datasystem

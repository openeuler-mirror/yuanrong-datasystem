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

/** Description: KVClient initialization from kvtest configuration. */

#pragma once

#include "common/config.h"

#include <datasystem/kv_client.h>

/**
 * @brief Initialize a KVClient with kvtest's optional process-level client configuration.
 * @param[in] cfg Parsed kvtest configuration.
 * @param[in,out] client KVClient to initialize.
 * @return Initialization status.
 */
inline datasystem::Status InitKvtestClient(const Config &cfg, datasystem::KVClient &client)
{
    datasystem::KVClientConfig::Builder builder;
    if (cfg.urmaSendLaneCountPerPeer.has_value()) {
        builder.UrmaSendLaneCountPerPeer(cfg.urmaSendLaneCountPerPeer.value());
    }
    datasystem::KVClientConfig clientConfig;
    auto status = builder.Build(clientConfig);
    if (!status.IsOk()) {
        return status;
    }
    return client.Init(clientConfig);
}

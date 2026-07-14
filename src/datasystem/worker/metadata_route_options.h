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

/**
 * Description: Worker-owned metadata routing configuration.
 */
#ifndef DATASYSTEM_WORKER_METADATA_ROUTE_OPTIONS_H
#define DATASYSTEM_WORKER_METADATA_ROUTE_OPTIONS_H

#include "datasystem/common/util/net_util.h"

namespace datasystem::worker {

/**
 * @brief Business routing facts that do not belong to cluster-topology placement.
 */
struct MetadataRouteOptions {
    MetadataRouteOptions() : masterAddress("", -1)
    {
    }

    bool requireAvailableTarget{ false };
    bool centralizedMode{ false };
    HostPort masterAddress;
};

}  // namespace datasystem::worker

#endif  // DATASYSTEM_WORKER_METADATA_ROUTE_OPTIONS_H

// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DATASYSTEM_COORDINATOR_DISCOVERY_H
#define DATASYSTEM_COORDINATOR_DISCOVERY_H

#include <string>
#include <vector>

#include "datasystem/utils/status.h"

namespace datasystem {

class __attribute((visibility("default"))) ICoordinatorDiscovery {
public:
    virtual ~ICoordinatorDiscovery() = default;

    /**
     * @brief Get Coordinator candidate addresses.
     * @param[out] serviceList Coordinator addresses in "host:port" format. An implementation may return an empty list
     * when no candidate is currently available; the consumer decides whether an empty result is valid.
     * @return Status of the discovery operation. An empty result may still return OK.
     */
    virtual Status GetCoordinators(std::vector<std::string> &serviceList) = 0;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COORDINATOR_DISCOVERY_H

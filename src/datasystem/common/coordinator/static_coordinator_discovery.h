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

#ifndef DATASYSTEM_COMMON_COORDINATOR_STATIC_COORDINATOR_DISCOVERY_H
#define DATASYSTEM_COMMON_COORDINATOR_STATIC_COORDINATOR_DISCOVERY_H

#include <string>
#include <vector>

#include "datasystem/utils/coordinator_discovery.h"

namespace datasystem {

class StaticCoordinatorDiscovery final : public ICoordinatorDiscovery {
public:
    explicit StaticCoordinatorDiscovery(std::string serviceAddress);
    ~StaticCoordinatorDiscovery() override = default;

    Status GetCoordinators(std::vector<std::string> &serviceList) override;

private:
    const std::string serviceAddress_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_COORDINATOR_STATIC_COORDINATOR_DISCOVERY_H

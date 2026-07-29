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

#include "datasystem/common/coordinator/static_coordinator_discovery.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {

std::vector<std::string> ParseCoordinatorAddresses(const std::string &serviceAddress)
{
    std::vector<std::string> addresses;
    size_t begin = 0;
    while (begin <= serviceAddress.size()) {
        const auto separator = serviceAddress.find(',', begin);
        const auto end = separator == std::string::npos ? serviceAddress.size() : separator;
        auto address = Trim(serviceAddress.substr(begin, end - begin));
        if (!address.empty()) {
            addresses.emplace_back(std::move(address));
        }
        if (separator == std::string::npos) {
            break;
        }
        begin = separator + 1;
    }
    std::sort(addresses.begin(), addresses.end());
    addresses.erase(std::unique(addresses.begin(), addresses.end()), addresses.end());
    return addresses;
}
}  // namespace

StaticCoordinatorDiscovery::StaticCoordinatorDiscovery(const std::string &serviceAddress)
    : addresses_(ParseCoordinatorAddresses(serviceAddress))
{
}

Status StaticCoordinatorDiscovery::GetCoordinators(std::vector<std::string> &serviceList)
{
    serviceList = addresses_;
    return Status::OK();
}

Status StaticCoordinatorDiscovery::GetCoordinators(std::chrono::steady_clock::time_point deadline,
                                                   std::vector<std::string> &serviceList)
{
    CHECK_FAIL_RETURN_STATUS(std::chrono::steady_clock::now() < deadline, K_RPC_DEADLINE_EXCEEDED,
                             "Coordinator discovery deadline exceeded");
    return GetCoordinators(serviceList);
}

size_t StaticCoordinatorDiscovery::GetCount() const
{
    return addresses_.size();
}

}  // namespace datasystem

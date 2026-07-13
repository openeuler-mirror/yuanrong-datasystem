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
 * Description: BrokenFilter - self-contained filter with its own brokenMap_.
 * Marks workers as broken (5s TTL) on RPC failures. LookupOwner skips broken workers.
 * Worker recovery relies on TTL natural expiry — no external cleanup needed.
 */
#ifndef DATASYSTEM_CLIENT_ROUTING_BROKEN_FILTER_H
#define DATASYSTEM_CLIENT_ROUTING_BROKEN_FILTER_H

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>

#include "datasystem/client/routing/i_worker_filter.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {

class BrokenFilter : public IWorkerFilter {
public:
    BrokenFilter();
    ~BrokenFilter() override = default;

    bool IsAvailable(const HostPort &addr) const override;
    void OnWorkerStateChange(const HostPort &addr, StatusCode status) override;

private:
    using BrokenMap = std::unordered_map<std::string, std::chrono::steady_clock::time_point>;
    std::shared_ptr<const BrokenMap> brokenMap_;
    static constexpr std::chrono::seconds BROKEN_TTL{ 5 };
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_ROUTING_BROKEN_FILTER_H

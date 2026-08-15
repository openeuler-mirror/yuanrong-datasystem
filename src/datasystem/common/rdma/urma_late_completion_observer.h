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

#ifndef DATASYSTEM_COMMON_RDMA_URMA_LATE_COMPLETION_OBSERVER_H
#define DATASYSTEM_COMMON_RDMA_URMA_LATE_COMPLETION_OBSERVER_H

#include <cstdint>
#include <memory>
#include <string>

namespace datasystem {
struct UrmaLateCompletion {
    uint64_t requestId = 0;
    int cqeStatus = 0;
    std::string remoteAddress;
    std::string remoteInstanceId;
};

class UrmaLateCompletionObserver {
public:
    virtual ~UrmaLateCompletionObserver() = default;
    virtual void OnLateUrmaCompletion(const UrmaLateCompletion &completion, uint64_t ownerToken,
                                      uint64_t peerToken) noexcept = 0;
};

struct UrmaLateCompletionContext {
    std::weak_ptr<UrmaLateCompletionObserver> observer;
    uint64_t ownerToken = 0;
    uint64_t peerToken = 0;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RDMA_URMA_LATE_COMPLETION_OBSERVER_H

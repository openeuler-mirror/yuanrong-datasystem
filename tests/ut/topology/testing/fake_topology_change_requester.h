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
 * Description: Topology change requester fake for module tests.
 */
#ifndef TESTS_UT_TOPOLOGY_TESTING_FAKE_TOPOLOGY_CHANGE_REQUESTER_H
#define TESTS_UT_TOPOLOGY_TESTING_FAKE_TOPOLOGY_CHANGE_REQUESTER_H

#include <mutex>
#include <vector>

#include "datasystem/topology/runtime/topology_change_handler.h"

namespace datasystem {
namespace topology {

struct SubmittedScaleInRequest {
    ScaleInRequest request;
    Revision observedRevision{ 0 };
};

class FakeTopologyChangeRequester final : public ITopologyChangeRequester {
public:
    explicit FakeTopologyChangeRequester(size_t capacity);
    ~FakeTopologyChangeRequester() override = default;
    FakeTopologyChangeRequester(const FakeTopologyChangeRequester &) = delete;
    FakeTopologyChangeRequester &operator=(const FakeTopologyChangeRequester &) = delete;
    FakeTopologyChangeRequester(FakeTopologyChangeRequester &&) = delete;
    FakeTopologyChangeRequester &operator=(FakeTopologyChangeRequester &&) = delete;

    /**
     * @brief Set whether the fake request channel is available.
     * @param[in] available True when SubmitScaleInRequest may accept requests.
     */
    void SetAvailable(bool available);

    /**
     * @brief Return submitted requests as a stable copy.
     * @return Submitted scale-in requests.
     */
    std::vector<SubmittedScaleInRequest> SubmittedRequests() const;

    Status SubmitScaleInRequest(const ScaleInRequest &request, Revision observedMembershipRevision) override;

private:
    mutable std::mutex mutex_;
    size_t capacity_{ 0 };
    bool available_{ true };
    std::vector<SubmittedScaleInRequest> requests_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // TESTS_UT_TOPOLOGY_TESTING_FAKE_TOPOLOGY_CHANGE_REQUESTER_H

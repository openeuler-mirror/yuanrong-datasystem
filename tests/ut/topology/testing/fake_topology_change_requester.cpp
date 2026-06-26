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
#include "tests/ut/topology/testing/fake_topology_change_requester.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace topology {

FakeTopologyChangeRequester::FakeTopologyChangeRequester(size_t capacity) : capacity_(capacity)
{
}

void FakeTopologyChangeRequester::SetAvailable(bool available)
{
    std::lock_guard<std::mutex> lock(mutex_);
    available_ = available;
}

std::vector<SubmittedScaleInRequest> FakeTopologyChangeRequester::SubmittedRequests() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return requests_;
}

Status FakeTopologyChangeRequester::SubmitScaleInRequest(const ScaleInRequest &request,
                                                         Revision observedMembershipRevision)
{
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK_FAIL_RETURN_STATUS(available_, K_NOT_READY, "fake topology change requester is unavailable");
    CHECK_FAIL_RETURN_STATUS(!request.workerId.empty(), K_INVALID, "fake scale-in worker id is empty");
    CHECK_FAIL_RETURN_STATUS(
        request.reason == ScaleInReason::ORDERLY_SHUTDOWN || request.reason == ScaleInReason::MANUAL_DRAIN, K_INVALID,
        "fake scale-in reason is invalid");
    CHECK_FAIL_RETURN_STATUS(requests_.size() < capacity_, K_WRITE_BACK_QUEUE_FULL,
                             "fake topology change queue is full");
    requests_.push_back({ request, observedMembershipRevision });
    return Status::OK();
}

}  // namespace topology
}  // namespace datasystem

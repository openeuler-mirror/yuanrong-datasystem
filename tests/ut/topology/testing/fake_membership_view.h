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
 * Description: Membership view fake for module tests.
 */
#ifndef TESTS_UT_TOPOLOGY_TESTING_FAKE_MEMBERSHIP_VIEW_H
#define TESTS_UT_TOPOLOGY_TESTING_FAKE_MEMBERSHIP_VIEW_H

#include <memory>
#include <mutex>

#include "datasystem/topology/membership/membership_types.h"

namespace datasystem {
namespace topology {

class FakeMembershipView final : public IMembershipView {
public:
    FakeMembershipView() = default;
    ~FakeMembershipView() override = default;
    FakeMembershipView(const FakeMembershipView &) = delete;
    FakeMembershipView &operator=(const FakeMembershipView &) = delete;
    FakeMembershipView(FakeMembershipView &&) = delete;
    FakeMembershipView &operator=(FakeMembershipView &&) = delete;

    /**
     * @brief Seed one immutable membership snapshot.
     * @param[in] snapshot Snapshot exposed through IMembershipView.
     * @return K_OK on success.
     */
    Status SeedSnapshot(const MembershipSnapshot &snapshot);

    Status GetSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const override;
    Status GetRecord(const TopologyNodeId &nodeId, MembershipRecord &record) const override;
    Status GetReadyEndpoint(const TopologyNodeId &nodeId, TopologyEndpoint &endpoint) const override;
    Status ListReadyMembers(std::vector<MembershipRecord> &members) const override;

private:
    mutable std::mutex mutex_;
    std::shared_ptr<const MembershipSnapshot> snapshot_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // TESTS_UT_TOPOLOGY_TESTING_FAKE_MEMBERSHIP_VIEW_H

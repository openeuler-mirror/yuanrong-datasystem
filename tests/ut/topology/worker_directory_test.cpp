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
 * Description: Cluster membership worker directory view tests.
 */
#include "datasystem/topology/membership/cluster_membership.h"

#include <string>
#include <utility>

#include "gtest/gtest.h"

#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

class DirectoryRegistry final : public IClusterRegistry {
public:
    Status ListWorkers(MembershipSnapshot &snapshot) override
    {
        ++listCount;
        snapshot = snapshot_;
        return Status::OK();
    }

    Status HandleWorkerEvent(const CoordinationEvent &event, WorkerWatchEvent &typed) override
    {
        (void)event;
        (void)typed;
        return Status(K_RUNTIME_ERROR, "watch is unused");
    }

    void SetSnapshot(MembershipSnapshot snapshot)
    {
        snapshot_ = std::move(snapshot);
    }

    uint64_t listCount{ 0 };

private:
    MembershipSnapshot snapshot_;
};

WorkerRecord MakeRecord(const std::string &workerId, WorkerServiceState state)
{
    WorkerRecord record;
    record.workerId = workerId;
    record.endpoint = { "127.0.0.1", workerId == "127.0.0.1:7001" ? 7001 : 7002 };
    record.serviceState = state;
    record.capability.hostId = "host-a";
    record.capability.compatibilityVersion = "v1";
    record.modRevision = 1;
    return record;
}

TEST(WorkerDirectoryTest, SnapshotUnavailableReturnsNotReady)
{
    DirectoryRegistry registry;
    ClusterMembership membership(registry, "127.0.0.1:7001");
    IWorkerDirectory &directory = membership;

    std::shared_ptr<const MembershipSnapshot> snapshot;
    EXPECT_EQ(directory.GetSnapshot(snapshot).GetCode(), K_NOT_READY);
    WorkerRecord record;
    EXPECT_EQ(directory.GetWorkerRecord("127.0.0.1:7001", record).GetCode(), K_NOT_READY);
    WorkerEndpoint endpoint;
    EXPECT_EQ(directory.GetReadyEndpoint("127.0.0.1:7001", endpoint).GetCode(), K_NOT_READY);
    std::vector<WorkerRecord> workers;
    EXPECT_EQ(directory.ListReadyWorkers(workers).GetCode(), K_NOT_READY);
    EXPECT_EQ(registry.listCount, 0ul);
}

TEST(WorkerDirectoryTest, ReadsOnlyImmutableSnapshot)
{
    MembershipSnapshot snapshot;
    snapshot.revision = 10;
    snapshot.workers.emplace("127.0.0.1:7001", MakeRecord("127.0.0.1:7001", WorkerServiceState::READY));
    snapshot.workers.emplace("127.0.0.1:7002", MakeRecord("127.0.0.1:7002", WorkerServiceState::START));

    DirectoryRegistry registry;
    registry.SetSnapshot(std::move(snapshot));
    ClusterMembership membership(registry, "127.0.0.1:7001");
    DS_ASSERT_OK(membership.Rebuild());
    IWorkerDirectory &directory = membership;

    WorkerRecord record;
    DS_ASSERT_OK(directory.GetWorkerRecord("127.0.0.1:7001", record));
    EXPECT_EQ(record.endpoint.ToString(), "127.0.0.1:7001");
    EXPECT_EQ(record.serviceState, WorkerServiceState::READY);
    EXPECT_EQ(directory.GetWorkerRecord("127.0.0.1:7003", record).GetCode(), K_NOT_FOUND);

    WorkerEndpoint endpoint;
    DS_ASSERT_OK(directory.GetReadyEndpoint("127.0.0.1:7001", endpoint));
    EXPECT_EQ(endpoint.ToString(), "127.0.0.1:7001");
    EXPECT_EQ(directory.GetReadyEndpoint("127.0.0.1:7002", endpoint).GetCode(), K_NOT_FOUND);

    std::vector<WorkerRecord> workers;
    DS_ASSERT_OK(directory.ListReadyWorkers(workers));
    ASSERT_EQ(workers.size(), 1ul);
    EXPECT_EQ(workers[0].workerId, "127.0.0.1:7001");
    EXPECT_EQ(registry.listCount, 1ul);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem

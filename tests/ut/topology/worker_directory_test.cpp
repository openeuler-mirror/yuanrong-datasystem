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
 * Description: Worker directory tests.
 */
#include "datasystem/topology/membership/worker_directory.h"

#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "datasystem/common/util/status_helper.h"
#include "tests/ut/common.h"

namespace datasystem {
namespace topology {
namespace {

class FakeSnapshotProvider final : public IMembershipSnapshotProvider {
public:
    Status GetSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const override
    {
        ++callCount;
        snapshot = snapshot_;
        CHECK_FAIL_RETURN_STATUS(snapshot != nullptr, K_NOT_READY, "snapshot is not ready");
        return Status::OK();
    }

    void SetSnapshot(std::shared_ptr<const MembershipSnapshot> snapshot)
    {
        snapshot_ = std::move(snapshot);
    }

    mutable uint64_t callCount{ 0 };

private:
    std::shared_ptr<const MembershipSnapshot> snapshot_;
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
    FakeSnapshotProvider provider;
    WorkerDirectory directory(provider);

    std::shared_ptr<const MembershipSnapshot> snapshot;
    EXPECT_EQ(directory.GetSnapshot(snapshot).GetCode(), K_NOT_READY);
    WorkerRecord record;
    EXPECT_EQ(directory.GetWorkerRecord("127.0.0.1:7001", record).GetCode(), K_NOT_READY);
    WorkerEndpoint endpoint;
    EXPECT_EQ(directory.GetReadyEndpoint("127.0.0.1:7001", endpoint).GetCode(), K_NOT_READY);
    std::vector<WorkerRecord> workers;
    EXPECT_EQ(directory.ListReadyWorkers(workers).GetCode(), K_NOT_READY);
    EXPECT_EQ(provider.callCount, 4ul);
}

TEST(WorkerDirectoryTest, ReadsOnlyImmutableSnapshot)
{
    auto snapshot = std::make_shared<MembershipSnapshot>();
    snapshot->revision = 10;
    snapshot->workers.emplace("127.0.0.1:7001", MakeRecord("127.0.0.1:7001", WorkerServiceState::READY));
    snapshot->workers.emplace("127.0.0.1:7002", MakeRecord("127.0.0.1:7002", WorkerServiceState::START));

    FakeSnapshotProvider provider;
    provider.SetSnapshot(snapshot);
    WorkerDirectory directory(provider);

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
    EXPECT_GE(provider.callCount, 4ul);
}

}  // namespace
}  // namespace topology
}  // namespace datasystem

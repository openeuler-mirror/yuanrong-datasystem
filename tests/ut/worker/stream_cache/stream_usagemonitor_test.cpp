/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Usage Monitor test
 */

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <utility>
#include "ut/common.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/usage_monitor.h"

using namespace datasystem::worker::stream_cache;
namespace datasystem {
namespace worker {
namespace stream_cache {
class ClientWorkerSCServiceImplMock : public ClientWorkerSCServiceImpl {
public:
    ClientWorkerSCServiceImplMock() : ClientWorkerSCServiceImpl(HostPort(), HostPort(), nullptr, nullptr, nullptr){};
    Status SendBlockProducerReq(const std::string &streamName, const std::string &remoteWorkerAddr)
    {
        std::string id = streamName + remoteWorkerAddr;
        blockedProducers.insert(id);
        return Status::OK();
    }
    Status SendUnBlockProducerReq(const std::string &streamName, const std::string &remoteWorkerAddr)
    {
        std::string id = streamName + remoteWorkerAddr;
        blockedProducers.erase(id);
        return Status::OK();
    }
    std::multiset<std::string> blockedProducers;
};
}  // namespace stream_cache
}  // namespace worker
namespace ut {
static constexpr uint64_t DEFAULT_TOTAL_SIZE = 10;
class UsageMonitorTest : public CommonTest {
public:
    UsageMonitorTest()
    {
    }
    ~UsageMonitorTest() override = default;

    void SetUp() override
    {
        FLAGS_v = 2;  // vlog is 2.
        cliWorkerMockPtr_ = std::make_shared<ClientWorkerSCServiceImplMock>();
        impl_ = cliWorkerMockPtr_.get();
        usageMonitor_ = std::make_unique<UsageMonitor>(impl_, DEFAULT_TOTAL_SIZE);
        DS_ASSERT_OK(usageMonitor_->Init());
        CommonTest::SetUp();
    }
    void TearDown() override
    {
        if (usageMonitor_) {
            usageMonitor_->Stop();
            usageMonitor_.reset();
        }
        CommonTest::TearDown();
    }
    std::shared_ptr<ClientWorkerSCServiceImplMock> cliWorkerMockPtr_;
    ClientWorkerSCServiceImpl *impl_;
    std::unique_ptr<UsageMonitor> usageMonitor_;
};

TEST_F(UsageMonitorTest, TestNormalCase)
{
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr(RandomData().GetRandomString(8));
    // Dummy Reserve with size of 0
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 0));
    // Should not be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr, 1);
    usageMonitor_->DecUsage(streamName, remoteWorkerAddr, 1);
    ASSERT_FALSE(usageMonitor_->CheckOverUsed().IsError());

    // 80% of memory is used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr, 4);
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr, 5);
    ASSERT_TRUE(usageMonitor_->CheckOverUsed(0.8).IsError());
    ASSERT_FALSE(usageMonitor_->CheckOverUsed(1).IsError());

    // Should be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr, 11);
    ASSERT_TRUE(usageMonitor_->CheckOverUsed().IsError());
}

TEST_F(UsageMonitorTest, TestNormalCaseUsagePerStream)
{
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr2(RandomData().GetRandomString(8));
    // Dummy Reserve with size of 0
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 0));
    // Should not be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 1);
    usageMonitor_->DecUsage(streamName, remoteWorkerAddr1, 1);
    ASSERT_FALSE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, 1, 0).IsError());

    // 80% of memory is used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 4);
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr2, 5);
    const double threshold = 0.8;
    ASSERT_TRUE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, threshold, 0).IsError());
    ASSERT_FALSE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, 1, 0).IsError());

    // Should be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 11);
    ASSERT_TRUE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, 1, 0).IsError());
}

TEST_F(UsageMonitorTest, TestBGThreadLogic)
{
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr(RandomData().GetRandomString(8));
    // Dummy Reserve with size of 0
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 0));
    // Not over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr, 1);
    std::shared_ptr<UsageItem> usageItem;
    usageMonitor_->GetMostUsed(usageItem);
    ASSERT_FALSE(usageMonitor_->CheckOverUsed().IsError());
    sleep(1);
    // usage not blocked by background thread
    ASSERT_FALSE(usageItem->usageBlocked);

    // Should be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr, 11);
    ASSERT_TRUE(usageMonitor_->CheckOverUsed().IsError());
    sleep(1);
    // usage blocked by background thread
    ASSERT_TRUE(usageItem->usageBlocked);

    // Next largest Item
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 1);
    std::shared_ptr<UsageItem> usageItem1;
    usageMonitor_->GetMostUsed(usageItem1);
    sleep(1);
    // usage blocked by background thread
    ASSERT_TRUE(usageItem1->usageBlocked);

    usageMonitor_->DecUsage(streamName, remoteWorkerAddr, 12);
    sleep(1);
    // Background thread unblocks all items
    ASSERT_FALSE(usageItem->usageBlocked);
    ASSERT_FALSE(usageItem1->usageBlocked);
}

TEST_F(UsageMonitorTest, TestGetMostUsed)
{
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));
    // Dummy Reserve with size of 0
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 0));
    ASSERT_FALSE(usageMonitor_->CheckOverUsed().IsError());
    // Should be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 1);

    const std::string remoteWorkerAddr2(RandomData().GetRandomString(8));
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr2, 10);

    const std::string remoteWorkerAddr3(RandomData().GetRandomString(8));
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr3, 1);

    // Background thread blocked the most used producer
    std::shared_ptr<UsageItem> usageItem;
    usageMonitor_->GetMostUsed(usageItem);
    ASSERT_EQ(usageItem->streamName, streamName);
    ASSERT_EQ(usageItem->remoteWorkerAddr, remoteWorkerAddr2);
    ASSERT_EQ(usageItem->usage, DEFAULT_TOTAL_SIZE);
}

TEST_F(UsageMonitorTest, DISABLED_TestGetMostUsedBlocked)
{
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));
    // Dummy Reserve with size of 0
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 0));
    ASSERT_FALSE(usageMonitor_->CheckOverUsed().IsError());
    // Should be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 1);

    const std::string remoteWorkerAddr2(RandomData().GetRandomString(8));
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr2, 10);
    std::shared_ptr<UsageItem> usageItem;
    usageMonitor_->GetMostUsed(usageItem);
    usageItem->usageBlocked = true;

    const std::string remoteWorkerAddr3(RandomData().GetRandomString(8));
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr3, 2);

    // As previous most used is blocked we should get next most used
    std::shared_ptr<UsageItem> usageItem1;
    usageMonitor_->GetMostUsed(usageItem1);
    ASSERT_EQ(usageItem1->streamName, streamName);
    ASSERT_EQ(usageItem1->remoteWorkerAddr, remoteWorkerAddr3);
    ASSERT_EQ(usageItem1->usage, (uint64_t)2);
}

TEST_F(UsageMonitorTest, TestBasicReserve1)
{
    const int moreThanHalf = 6;
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string streamName2(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));

    // First test that memory cannot be reserved if memory are already reserved
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, moreThanHalf));
    DS_ASSERT_NOT_OK(usageMonitor_->ReserveMemory(streamName2, moreThanHalf));
    usageMonitor_->UndoReserveMemory(streamName);
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName2, moreThanHalf));
    usageMonitor_->UndoReserveMemory(streamName2);

    // Then test that memory can be reserved even if not enough memory is available upfront
    // As long as the total reserved memory is still in bound, it is allowed to reserve
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 1));
    DS_ASSERT_OK(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, 1, moreThanHalf));
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName2, moreThanHalf));

    // Also test that one cannot use other's reserved memory
    // Stream1 reserved 1, Stream2 reserved 6, so stream1 can at most use 4, while stream2 can use at most 9
    DS_ASSERT_NOT_OK(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, 1, moreThanHalf));
    DS_ASSERT_NOT_OK(
        usageMonitor_->CheckNIncOverUsedForStream(streamName2, remoteWorkerAddr1, 0, 1, DEFAULT_TOTAL_SIZE));
    DS_ASSERT_OK(
        usageMonitor_->CheckNIncOverUsedForStream(streamName2, remoteWorkerAddr1, 0, 1, DEFAULT_TOTAL_SIZE - 1));
}

TEST_F(UsageMonitorTest, TestBasicReserve2)
{
    const int moreThanHalf = 6;
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string streamName2(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));

    // Test that reserved memory adjustment cannot go over the limit
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, moreThanHalf));
    DS_ASSERT_NOT_OK(usageMonitor_->ReserveMemory(streamName, DEFAULT_TOTAL_SIZE + 1));

    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName2, 1));
    DS_ASSERT_NOT_OK(usageMonitor_->ReserveMemory(streamName, DEFAULT_TOTAL_SIZE));
}

TEST_F(UsageMonitorTest, TestLowerBound)
{
    const std::string streamName(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr1(RandomData().GetRandomString(8));
    const std::string remoteWorkerAddr2(RandomData().GetRandomString(8));
    // Dummy Reserve with size of 0
    DS_ASSERT_OK(usageMonitor_->ReserveMemory(streamName, 0));

    // Should not be over used as lower bound is 1 though the ratio is 0.01
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 1);
    const double lowerTh = 0.01;
    ASSERT_TRUE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, lowerTh, 0).IsError());
    ASSERT_FALSE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 1, lowerTh, 0).IsError());
    LOG_IF_ERROR(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 0, lowerTh, 0), "OOM");
    // 80% of memory is used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 4);
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr2, 5);
    const double higherTh = 0.8;
    ASSERT_TRUE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 1, higherTh, 0).IsError());
    ASSERT_FALSE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 1, 1, 0).IsError());

    // Should be over used
    usageMonitor_->IncUsage(streamName, remoteWorkerAddr1, 11);
    ASSERT_TRUE(usageMonitor_->CheckNIncOverUsedForStream(streamName, remoteWorkerAddr1, 1, 1, 0).IsError());
}
}  // namespace ut
}  // namespace datasystem

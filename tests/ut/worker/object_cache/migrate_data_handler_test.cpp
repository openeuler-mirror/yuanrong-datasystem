/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Test interface to HashRingHealthCheck
 */
#include <cstdint>
#include <cstring>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "ut/common.h"
#include "../../../common/binmock/binmock.h"
#include "datasystem/common/rdma/fast_transport_manager_wrapper.h"
#include "datasystem/common/rpc/mem_view.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/log/log.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/limiter/data_limiter.h"
#include "datasystem/worker/object_cache/data_migrator/handler/async_resource_releaser.h"
#include "datasystem/worker/object_cache/data_migrator/handler/migrate_data_handler.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/scale_down_node_selector.h"
#include "datasystem/worker/object_cache/data_migrator/strategy/spill_node_selector.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "eviction_manager_common.h"

using namespace datasystem::object_cache;
using namespace ::testing;

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);
DS_DECLARE_uint32(data_migrate_rate_limit_mb);
namespace datasystem {
namespace ut {

class ScaleDownNodeSelectorTest : public CommonTest {};

TEST_F(ScaleDownNodeSelectorTest, TestCheckCondition)
{
    HostPort address;
    ScaleDownNodeSelector migrateStrategy(nullptr, address);
    MigrateDataRspPb rsp;

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 0, 60, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), true);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 0, 30, 1)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), true);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 0, 15, 2)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), true);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 1, 15, 3)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), false);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 0, 60, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::DISK), true);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 0, 30, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), false);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 0, 10, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), false);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 1, 60, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), false);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 1, 30, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), false);

    datasystem::inject::Set("ScaleDownNodeSelector.CheckCondition", "call(0, 1, 10, 0)");
    ASSERT_EQ(migrateStrategy.CheckCondition(rsp, CacheType::MEMORY), false);
}

class DataLimiterTest : public CommonTest {
};

TEST_F(DataLimiterTest, TestLimiterBasicFunction)
{
    LOG(INFO) << "Test migrate data limiter basic function";
    uint64_t rate = 40 * 1024ul * 1024ul;
    uint64_t requireSize = 80 * 1024ul * 1024ul;
    DataLimiter limiter(rate);
    Timer timer;
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(950));
    timer.Reset();
    limiter.WaitAllow(requireSize / 2);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(475));
    timer.Reset();
    limiter.WaitAllow(requireSize / 4);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(240));
    timer.Reset();
    limiter.WaitAllow(requireSize / 8);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(120));
}

TEST_F(DataLimiterTest, TestLimiterVerySmallSize)
{
    LOG(INFO) << "Test migrate data limiter very small sizes";
    uint64_t rate = 40 * 1024ul * 1024ul;
    uint64_t requireSize = 1024ul;
    DataLimiter limiter(rate);
    Timer timer;
    for (size_t i = 0; i < 80 * 1024ul; ++i) {
        limiter.WaitAllow(requireSize);
    }
    ASSERT_GE(timer.ElapsedMilliSecond(), double(950));
}

TEST_F(DataLimiterTest, TestBoundaryCase)
{
    LOG(INFO) << "Test migrate data limiter boundary case";
    uint64_t rate = UINT64_MAX / 2;
    uint64_t requireSize = UINT64_MAX;
    DataLimiter limiter(rate);
    Timer timer;
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(950));
    timer.Reset();
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(1950));
}

TEST_F(DataLimiterTest, TestRequestLargeSize)
{
    LOG(INFO) << "Test migrate data limiter boundary case";
    uint64_t rate = 100;
    uint64_t requireSize = 500;
    DataLimiter limiter(rate, rate);
    Timer timer;
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(3900));
    timer.Reset();
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(4900));
}

TEST_F(DataLimiterTest, TestMaxLimitSize)
{
    LOG(INFO) << "Test migrate data limiter boundary case";
    uint64_t rate = 100;
    uint64_t requireSize = 500;
    DataLimiter limiter(rate, rate);
    datasystem::inject::Set("migrate.limiter.elapsed.longtime", "1*call()");
    Timer timer;
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(0));
    timer.Reset();
    limiter.WaitAllow(requireSize);
    ASSERT_GE(timer.ElapsedMilliSecond(), double(4900));
}

class MigrateDataHandlerTest : public CommonTest, public EvictionManagerCommon {
public:
    virtual void SetUp()
    {
        CommonTest::SetUp();
        Init();
        const uint64_t memSize = 1024ul * 1024ul * 1024ul;
        DS_ASSERT_OK(memory::Allocator::Instance()->Init(memSize));
        FLAGS_spill_directory = "./spill" + GetStringUuid();
        FLAGS_spill_size_limit = memSize;
        DS_ASSERT_OK(WorkerOcSpill::Instance()->Init());
        LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
    }

    void TearDown() override
    {
        RELEASE_STUBS
    }

    virtual void Init()
    {
        hostPort_ = HostPort("127.0.0.1", 18481);
        remoteApi_ = std::make_shared<WorkerRemoteWorkerOCApi>(hostPort_, nullptr);
        objectTable_ = std::make_shared<ObjectTable>();
        strategy_ = std::make_shared<ScaleDownNodeSelector>(nullptr, hostPort_);
    }

    void CreateObjects(const std::string &prefix, uint64_t dataSize, uint32_t count,
                       std::vector<ImmutableString> &objectKeys, bool locked = false)
    {
        for (uint32_t i = 0; i < count; ++i) {
            std::string objectKey = prefix + std::to_string(i);
            DS_ASSERT_OK(CreateObject(objectKey, dataSize));
            objectKeys.emplace_back(objectKey);
        }
        if (!locked || dataSize <= SHM_THRESHOLD) {
            return;
        }

        for (const auto &objectKey : objectKeys) {
            std::shared_ptr<SafeObjType> entry;
            DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
            auto shm = entry->Get()->GetShmUnit();
            auto lockFrame = reinterpret_cast<uint32_t *>(shm->GetPointer());
            auto tmpLock = std::make_shared<object_cache::ShmLock>(lockFrame, entry->Get()->GetMetadataSize(), 0);
            DS_ASSERT_OK(tmpLock->Init());
            ASSERT_TRUE(tmpLock->WLatch(1));
        }
    }

    void SpillObject(std::vector<ImmutableString> &objectKeys)
    {
        for (const auto &objectKey : objectKeys) {
            std::shared_ptr<SafeObjType> entry;
            DS_ASSERT_OK(objectTable_->Get(objectKey, entry));
            auto shm = entry->Get()->GetShmUnit();
            DS_ASSERT_OK(WorkerOcSpill::Instance()->Spill(
                objectKey, static_cast<uint8_t *>(shm->GetPointer()) + entry->Get()->GetMetadataSize(),
                entry->Get()->GetDataSize(), false));
            entry->Get()->stateInfo.SetSpillState(true);
        }
    }

    bool CompareData(const std::string &objectKey, const MemView &memView)
    {
        std::shared_ptr<SafeObjType> entry;
        Status rc = objectTable_->Get(objectKey, entry);
        if (rc.IsError()) {
            LOG(ERROR) << "Object not found: " << objectKey;
            return false;
        }
        rc = entry->RLock();
        if (rc.IsError()) {
            LOG(ERROR) << "Object Lock failed: " << objectKey;
            return false;
        }

        Raii raii([&entry]() { entry->RUnlock(); });

        return memcmp(static_cast<uint8_t *>((*entry)->GetShmUnit()->GetPointer()) + (*entry)->GetMetadataSize(),
                      memView.Data(), memView.Size())
               == 0;
    }

    Status MockMigrateSmallData1(MigrateDataReqPb &req, const std::vector<MemView> &payloads, MigrateDataRspPb &rsp);
    Status MockMigrateSmallData2(MigrateDataReqPb &req, const std::vector<MemView> &payloads, MigrateDataRspPb &rsp);

protected:
    HostPort hostPort_;

    MigrateType type_ = MigrateType::SCALE_DOWN;

    std::shared_ptr<WorkerRemoteWorkerOCApi> remoteApi_;

    std::shared_ptr<SelectionStrategy> strategy_;
};

TEST_F(MigrateDataHandlerTest, TestMigrateDataMeetsNoSpaceError)
{
    LOG(INFO) << "Test migrate data meets no space error";
    constexpr uint64_t minSize = 1024ul * 1024ul - 1;
    MigrateDataRspPb fakeRsp;
    fakeRsp.set_remain_bytes(minSize);
    fakeRsp.set_limit_rate(FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul);
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(1)
        .WillRepeatedly(DoAll(SetArgReferee<2>(fakeRsp), Return(Status::OK())));

    MigrateDataHandler handler1(type_, "127.0.0.1:18888", {}, objectTable_, remoteApi_, strategy_);
    auto result1 = handler1.MigrateDataToRemote();
    DS_ASSERT_OK(result1.status);

    DS_ASSERT_OK(CreateObject("xxx", 1));
    MigrateDataHandler handler2(type_, "127.0.0.1:18888", { "xxx" }, objectTable_, remoteApi_, strategy_);
    auto result2 = handler2.MigrateDataToRemote();
    EXPECT_EQ(result2.status.GetCode(), StatusCode::K_NO_SPACE);
}

Status MigrateDataHandlerTest::MockMigrateSmallData1(MigrateDataReqPb &req, const std::vector<MemView> &payloads,
                                                     MigrateDataRspPb &rsp)
{
    EXPECT_EQ(req.type(), type_);
    constexpr uint64_t remainBytes = 1024ul * 1024ul * 1024ul;
    rsp.set_remain_bytes(remainBytes);
    constexpr double availableRatio = 85.0;
    rsp.set_available_ratio(availableRatio);
    uint64_t limitRate = FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul;
    rsp.set_limit_rate(limitRate);
    // Input parameters check.
    EXPECT_EQ(static_cast<uint64_t>(req.objects().size()), payloads.size())
        << "object size: " << req.objects().size() << ", payload size: " << payloads.size();
    EXPECT_LE(req.objects().size(), 300);
    auto objectInfos = req.objects();
    for (const auto &info : objectInfos) {
        const auto objectKey = info.object_key();
        EXPECT_EQ(info.part_index().size(), 1);
        const auto &memView = payloads[info.part_index(0)];
        EXPECT_EQ(memView.Size(), info.data_size());
        if (objectTable_->Contains(objectKey)) {
            EXPECT_TRUE(CompareData(objectKey, memView));
        }
        rsp.add_success_ids(objectKey);
    }
    return Status::OK();
}

TEST_F(MigrateDataHandlerTest, TestMigrateSmallMemoryObjects)
{
    LOG(INFO) << "Test migrate small memory objects.";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(5)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData1));

    constexpr uint64_t size = 100;
    constexpr uint64_t count = 1024;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);

    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count);
    ASSERT_TRUE(result.skipIds.empty());
    ASSERT_TRUE(result.failedIds.empty());
}

TEST_F(MigrateDataHandlerTest, TestMigrateNoExistObjects)
{
    LOG(INFO) << "Test migrate no exist objects(being deleted).";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(5)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData1));

    constexpr uint64_t size = 100;
    constexpr uint64_t count = 1024;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);
    for (size_t i = 0; i < count; ++i) {
        objectKeys.emplace_back("No_Exist_Object_" + std::to_string(i));
    }
    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count);
    ASSERT_EQ(result.skipIds.size(), count);
    ASSERT_TRUE(result.failedIds.empty());
}

TEST_F(MigrateDataHandlerTest, TestMigrateObjectsButLockFail)
{
    LOG(INFO) << "Test migrate objects but lock fail.";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(5)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData1));

    // Create normal objects.
    constexpr uint64_t size1 = 100;
    constexpr uint64_t succCount = 1024;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size1, succCount, objectKeys);

    // Create locked objects.
    constexpr uint64_t size2 = 600 * 1024ul;
    constexpr uint64_t failCount = 10;
    std::vector<ImmutableString> failObjectKeys;
    CreateObjects("Locked", size2, failCount, failObjectKeys, true);

    (void)objectKeys.insert(objectKeys.end(), failObjectKeys.begin(), failObjectKeys.end());
    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), succCount);
    ASSERT_TRUE(result.skipIds.empty());
    ASSERT_EQ(result.failedIds.size(), failCount);
}

Status MigrateDataHandlerTest::MockMigrateSmallData2(MigrateDataReqPb &req, const std::vector<MemView> &payloads,
                                                     MigrateDataRspPb &rsp)
{
    EXPECT_EQ(req.type(), type_);
    static uint64_t remainBytes = 1024ul * 1024ul + 90'000ul - 1;
    remainBytes -= req.objects_size() * 100ul;
    rsp.set_remain_bytes(remainBytes);
    constexpr double availableRatio = 85.0;
    rsp.set_available_ratio(availableRatio);
    uint64_t limitRate = FLAGS_data_migrate_rate_limit_mb * 1024ul * 1024ul;
    rsp.set_limit_rate(limitRate);
    // Parameters check.
    EXPECT_EQ(static_cast<uint64_t>(req.objects().size()), payloads.size())
        << "object size: " << req.objects().size() << ", payload size: " << payloads.size();
    EXPECT_LE(req.objects().size(), 300);
    auto objectInfos = req.objects();
    for (const auto &info : objectInfos) {
        const auto objectKey = info.object_key();
        EXPECT_EQ(info.part_index().size(), 1);
        const auto &memView = payloads[info.part_index(0)];
        EXPECT_EQ(memView.Size(), info.data_size());
        if (objectTable_->Contains(objectKey)) {
            EXPECT_TRUE(CompareData(objectKey, memView));
        }
        rsp.add_success_ids(objectKey);
    }
    return Status::OK();
}

TEST_F(MigrateDataHandlerTest, TestMigrateDataMeetsNoSpaceError1)
{
    LOG(INFO) << "Test migrate objects and only some objects succeeded(meets OOM)";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(4)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData2));

    // Create normal objects.
    constexpr uint64_t size = 100;
    constexpr uint64_t count = 1024;
    constexpr uint64_t failCount = count - 900ul;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);

    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();

    ASSERT_EQ(result.status.GetCode(), StatusCode::K_NO_SPACE);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count - failCount);
    ASSERT_TRUE(result.skipIds.empty());
    ASSERT_EQ(result.failedIds.size(), failCount);
}

TEST_F(MigrateDataHandlerTest, TestMigrateSpilledObjects)
{
    LOG(INFO) << "Test migrate objects that exist in spill dir.";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(5)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData1));

    constexpr uint64_t smallSize = 1024ul;
    constexpr uint64_t smallCount = 1024ul;
    std::vector<ImmutableString> smallObjects;
    CreateObjects("League_of_Legends", smallSize, smallCount, smallObjects);

    constexpr uint64_t bigSize = 1024ul * 1024ul;
    constexpr uint64_t bigCount = 20ul;
    std::vector<ImmutableString> bigObjects;
    CreateObjects("Spilled_Obejct", bigSize, bigCount, bigObjects);
    SpillObject(bigObjects);

    std::vector<ImmutableString> objectKeys(smallObjects);
    objectKeys.insert(objectKeys.end(), bigObjects.begin(), bigObjects.end());
    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();

    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), smallCount + bigCount);
    ASSERT_TRUE(result.skipIds.empty());
    ASSERT_TRUE(result.failedIds.empty());
}

TEST_F(MigrateDataHandlerTest, TestMigrateBigObjectsWithLimitedRate)
{
    LOG(INFO) << "Test migrate objects with limited rate.";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(6)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData1));

    FLAGS_data_migrate_rate_limit_mb = 1;
    constexpr uint64_t smallSize = 2 * 1024ul;
    constexpr uint64_t smallCount = 1024ul;
    std::vector<ImmutableString> smallObjects;
    CreateObjects("Small_Object", smallSize, smallCount, smallObjects);

    constexpr uint64_t bigSize = 2 * 1024ul * 1024ul;
    constexpr uint64_t bigCount = 2ul;
    std::vector<ImmutableString> bigObjects;
    CreateObjects("BigObject", bigSize, bigCount, bigObjects);

    // Rate limit is 1MB/s, 6MB data would cost 6s.
    std::vector<ImmutableString> objectKeys(smallObjects);
    objectKeys.insert(objectKeys.end(), bigObjects.begin(), bigObjects.end());
    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);

    Timer timer;
    auto result = handler.MigrateDataToRemote();
    auto elapsed = timer.ElapsedMilliSecond();

    ASSERT_GT(elapsed, 4900.0);
    ASSERT_LE(elapsed, 5100.0);
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), smallCount + bigCount);
    ASSERT_TRUE(result.skipIds.empty());
    ASSERT_TRUE(result.failedIds.empty());
}

TEST_F(MigrateDataHandlerTest, TestPayloadData)
{
    std::vector<RpcMessage> data;
    auto payloadData = std::make_unique<PayloadData>("111", 0, std::move(data), 2, datasystem::CacheType::MEMORY);
    const size_t payloadDataSize = 2;
    ASSERT_EQ(payloadData->Size(), payloadDataSize);
    std::vector<MemView> memViews = payloadData->GetMemViews();
    ASSERT_EQ(memViews.size(), 0);
}

class MigrateDataHandlerSpillTest : public MigrateDataHandlerTest {
public:
    void SetUp() override
    {
        Init();
        const uint64_t memSize = 1024UL * 1024UL * 1024UL;
        DS_ASSERT_OK(memory::Allocator::Instance()->Init(memSize));
    }

    void Init() override
    {
        hostPort_ = HostPort("127.0.0.1", 18481);
        remoteApi_ = std::make_shared<WorkerRemoteWorkerOCApi>(hostPort_, nullptr);
        objectTable_ = std::make_shared<ObjectTable>();
        strategy_ = std::make_shared<SpillNodeSelector>(nullptr, hostPort_);
        type_ = MigrateType::SPILL;
        AsyncResourceReleaser::Instance().Init(objectTable_);

        BINEXPECT_CALL(&object_cache::NodeSelector::GetAvailableMemory, (_))
            .WillRepeatedly(Return(1024UL * 1024UL * 1024UL));
    }
};

TEST_F(MigrateDataHandlerSpillTest, TestMigrateObjectsByTCP)
{
    LOG(INFO) << "Test migrate objects for spill type.";
    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateData, (_, _, _))
        .Times(5)
        .WillRepeatedly(Invoke(this, &MigrateDataHandlerTest::MockMigrateSmallData1));
    constexpr uint64_t size = 100;
    constexpr uint64_t count = 1024;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);

    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count);
    ASSERT_TRUE(result.skipIds.empty());
    ASSERT_TRUE(result.failedIds.empty());
}

TEST_F(MigrateDataHandlerSpillTest, DISABLED_TestMigrateObjectsByFastTransport)
{
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::IsFastTransportEnabled, ()).WillRepeatedly(Return(true));

    // Create objects and mark them spilled, but keep shm unit so fast transport can read from memory.
    constexpr uint64_t size = 100;
    constexpr uint64_t count = 3;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);

    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateDataDirect, (_, _))
        .Times(1)
        .WillOnce(Invoke([](MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp) {
            rsp.set_remain_bytes(1024ul * 1024ul * 1024ul);
            (void)req;
            return Status::OK();
        }));

    BINEXPECT_CALL(&AsyncResourceReleaser::AddTask, (_, _)).Times(0);

    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count);
    ASSERT_TRUE(result.failedIds.empty());
}

TEST_F(MigrateDataHandlerSpillTest, DISABLED_TestReleaseFailEnqueueTask)
{
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::IsFastTransportEnabled, ()).WillRepeatedly(Return(true));

    constexpr uint64_t size = 100;
    constexpr uint64_t count = 3;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);

    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateDataDirect, (_, _))
        .Times(1)
        .WillOnce(Invoke([](MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp) {
            rsp.set_remain_bytes(1024UL * 1024UL * 1024UL);
            (void)req;
            return Status::OK();
        }));

    // Make Release() fail before MigrateDataToRemote() returns (sync path),
    // then switch to success and let async queue clean up.
    BINEXPECT_CALL(&AsyncResourceReleaser::Release, (_, _))
        .Times(AtLeast(static_cast<int>(count)))
        .WillRepeatedly(Return(Status(K_TRY_AGAIN, "release failed")));
    DS_ASSERT_OK(inject::Set("AsyncResourceReleaser.WorkerThread.delay", "1*call(300)"));

    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();
    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count);
    ASSERT_TRUE(result.failedIds.empty());

    // After migrate returns, release stubs so async retries can erase objects.
    RELEASE_STUBS
    std::this_thread::sleep_for(std::chrono::milliseconds(600)); // sleep 600 ms to let async queue process.
    for (const auto &key : objectKeys) {
        DS_ASSERT_NOT_OK(objectTable_->Contains(key));
    }
}

TEST_F(MigrateDataHandlerSpillTest, DISABLED_TestFastTransportRetryOnRpcError)
{
    BINEXPECT_CALL(&datasystem::IsUrmaEnabled, ()).WillRepeatedly(Return(true));
    BINEXPECT_CALL(&datasystem::IsFastTransportEnabled, ()).WillRepeatedly(Return(true));

    constexpr uint64_t size = 100;
    constexpr uint64_t count = 3;
    std::vector<ImmutableString> objectKeys;
    CreateObjects("League_of_Legends", size, count, objectKeys);

    BINEXPECT_CALL(&WorkerRemoteWorkerOCApi::MigrateDataDirect, (_, _))
        .WillOnce(Return(Status(StatusCode::K_RPC_UNAVAILABLE, "rpc error")))
        .WillOnce(Return(Status(StatusCode::K_RPC_UNAVAILABLE, "rpc error")))
        .WillOnce(Return(Status::OK()));

    BINEXPECT_CALL(&AsyncResourceReleaser::AddTask, (_, _)).Times(0);

    MigrateDataHandler handler(type_, "127.0.0.1:18888", objectKeys, objectTable_, remoteApi_, strategy_);
    auto result = handler.MigrateDataToRemote();

    DS_ASSERT_OK(result.status);
    ASSERT_EQ(result.address, hostPort_.ToString());
    ASSERT_EQ(result.successIds.size(), count);
    ASSERT_TRUE(result.failedIds.empty());
}

}  // namespace ut
}  // namespace datasystem
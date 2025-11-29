/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Test EvictionManager.
 */
#include <fcntl.h>
#include <vector>

#include "securec.h"

#include "bench_helper.h"
#include "common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/lock.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/queue/queue.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/object/buffer.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "eviction_manager_common.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace datasystem::master;

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);
DS_DECLARE_string(master_address);
DS_DECLARE_string(etcd_address);

namespace datasystem {
namespace ut {

class EvictionManagerTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(maxMemorySize);
        akSkManager_ = std::make_shared<AkSkManager>(0);
    }
    std::shared_ptr<AkSkManager> akSkManager_;
};

TEST_F(EvictionManagerTest, TestAllocator)
{
    ASSERT_EQ(GetMaxMemorySize(), maxMemorySize);
    ASSERT_EQ(GetAllocatedSize(), size_t(0));

    uint64_t dataSize = 1024 * 1024;
    auto metaSize = GetMetaSize(dataSize);
    for (int i = 1; i <= 10; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(CreateObject(objectKey, dataSize));
        DS_EXPECT_NOT_OK(CreateObject(objectKey, dataSize));
        ASSERT_EQ(GetAllocatedSize(), i * (dataSize + metaSize));
    }

    for (int i = 10; i >= 1; i--) {
        std::string objectKey = "key_" + std::to_string(i);
        DS_EXPECT_OK(DeleteObject(objectKey));
        DS_EXPECT_NOT_OK(DeleteObject(objectKey));
        ASSERT_EQ(GetAllocatedSize(), (i - 1) * (dataSize + metaSize));
    }
}

TEST_F(EvictionManagerTest, TestEvictionList)
{
    object_cache::EvictionList evictionList;
    ASSERT_EQ(evictionList.Size(), size_t(0));
    for (int i = 1; i <= 4; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        uint8_t counter = i;
        evictionList.Add(objectKey, counter);
    }
    ASSERT_EQ(evictionList.Size(), size_t(4));

    for (int i = 1; i <= 4; i++) {
        std::string objectKey = "key_" + std::to_string(i);
        EvictionList::Node node;
        DS_EXPECT_OK(evictionList.GetObjectInfo(objectKey, node));
        ASSERT_EQ(node.curCounter, i);
        ASSERT_EQ(node.maxCounter, i);
    }

    EvictionList::Node node;
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_1" && node.curCounter == 1 && node.maxCounter == 1);

    (void)evictionList.Erase("key_1");
    ASSERT_EQ(evictionList.Size(), size_t(3));
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_1", node));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_2" && node.curCounter == 2 && node.maxCounter == 2);

    (void)evictionList.Erase("key_3");
    ASSERT_EQ(evictionList.Size(), size_t(2));
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_3", node));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_2" && node.curCounter == 2 && node.maxCounter == 2);

    (void)evictionList.Erase("key_2");
    ASSERT_EQ(evictionList.Size(), size_t(1));
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_2", node));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_4" && node.curCounter == 4 && node.maxCounter == 4);

    (void)evictionList.Erase("key_4");
    DS_EXPECT_NOT_OK(evictionList.GetObjectInfo("key_4", node));
    ASSERT_EQ(evictionList.Size(), size_t(0));

    // Add again
    evictionList.Add("key_4", 4);
    ASSERT_EQ(evictionList.Size(), size_t(1));
    DS_EXPECT_OK(evictionList.GetOldestObjectInfo(node));
    ASSERT_TRUE(node.objectKey == "key_4" && node.curCounter == 4 && node.maxCounter == 4);
}

TEST_F(EvictionManagerTest, TestEvictionManagerInit)
{
    std::shared_ptr<ObjectTable> &objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, HostPort("127.0.0.1", 31501),
                                                          HostPort("127.0.0.1", 31500));
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));
    std::vector<EvictionList::Node> objsInList;
    EvictionList::Node oldest;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(0));
}

TEST_F(EvictionManagerTest, TestEvictionManagerAddErase)
{
    std::shared_ptr<ObjectTable> &objectTable = GetObjectTable();
    object_cache::WorkerOcEvictionManager evictionManager(objectTable, HostPort("127.0.0.1", 31501),
                                                          HostPort("127.0.0.1", 31500));
    auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
    DS_EXPECT_OK(evictionManager.Init(globalRefTable, akSkManager_));

    uint64_t dataSize = 10 * 1024 * 1024;
    std::string id1 = "id1";
    DS_EXPECT_OK(CreateObject(id1, dataSize));
    std::shared_ptr<SafeObjType> entry1;
    DS_EXPECT_OK(objectTable_->Get(id1, entry1));
    evictionManager.Add(id1);

    std::string id2 = "id2";
    DS_EXPECT_OK(CreateObject(id2, dataSize, WriteMode::WRITE_THROUGH_L2_CACHE));
    std::shared_ptr<SafeObjType> entry2;
    DS_EXPECT_OK(objectTable_->Get(id2, entry2));
    evictionManager.Add(id2);

    std::string id3 = "id3";
    std::vector<std::string> objectKeys = { id3 };
    std::vector<std::string> failIncIds;
    std::vector<std::string> firstIncIds;
    globalRefTable->GIncreaseRef(ClientKey::Intern("client-id"), objectKeys, failIncIds, firstIncIds);
    DS_EXPECT_OK(CreateObject(id3, dataSize));
    std::shared_ptr<SafeObjType> entry3;
    DS_EXPECT_OK(objectTable_->Get(id3, entry3));
    evictionManager.Add(id3);

    std::unordered_map<std::string, std::shared_ptr<SafeObjType>> objsInTable;
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInTable.size(), size_t(3));
    ASSERT_EQ((*objsInTable[id1])->GetDataSize(), dataSize);
    ASSERT_EQ((*objsInTable[id2])->GetDataSize(), dataSize);
    ASSERT_EQ((*objsInTable[id3])->GetDataSize(), dataSize);

    std::vector<EvictionList::Node> objsInList;
    EvictionList::Node oldest;
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(3));
    ASSERT_TRUE(objsInList[0].objectKey == id1 && objsInList[0].curCounter == 1);
    ASSERT_TRUE(objsInList[1].objectKey == id2 && objsInList[1].curCounter == 1);
    ASSERT_TRUE(objsInList[2].objectKey == id3 && objsInList[2].curCounter == 2);
    ASSERT_TRUE(oldest.objectKey == id1 && oldest.curCounter == 1);

    objsInList.clear();
    evictionManager.Erase(id1);
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(2));
    ASSERT_TRUE(objsInList[0].objectKey == id2 && objsInList[0].curCounter == 1);
    ASSERT_TRUE(objsInList[1].objectKey == id3 && objsInList[1].curCounter == 2);

    objsInList.clear();
    evictionManager.Erase(id2);
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(1));
    ASSERT_TRUE(objsInList[0].objectKey == id3 && objsInList[0].curCounter == 2);

    objsInList.clear();
    evictionManager.Erase(id3);
    DS_EXPECT_OK(evictionManager.GetAllObjectsInfo(objsInList, oldest));
    ASSERT_EQ(objsInList.size(), size_t(0));

    DS_EXPECT_OK(DeleteObject(id1));
    DS_EXPECT_OK(DeleteObject(id2));
    DS_EXPECT_OK(DeleteObject(id3));
    objsInTable.clear();
    GetAllObjsFromObjectTable(objsInTable);
    ASSERT_EQ(objsInTable.size(), size_t(0));
}

class ScEvictionObjectTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        LOG(INFO) << "Init ScEvictionObjectTest";
    }

    void InitTest()
    {
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();
        akSkManager_ = std::make_shared<AkSkManager>(0);

        allocator->Init(maxSize_, 0, false, true, 5000, ocPercent_, scPercent_);  // decay is 5000 ms.
        std::shared_ptr<ObjectTable> &objectTable = GetObjectTable();
        evictionManager_ = std::make_shared<object_cache::WorkerOcEvictionManager>(
            objectTable, HostPort("127.0.0.1", 32131),  // worker port is 32131,
            HostPort("127.0.0.1", 52319));              // master port is 52319;
        auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable, akSkManager_));
        scAllocateManager_ = std::make_shared<worker::stream_cache::WorkerSCAllocateMemory>(evictionManager_);
    }

    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<object_cache::WorkerOcEvictionManager> evictionManager_;
    std::shared_ptr<worker::stream_cache::WorkerSCAllocateMemory> scAllocateManager_;
    uint64_t maxSize_ = 0;
    int scPercent_ = 0;
    int ocPercent_ = 0;
    int limit = 100 * 1024 * 1024;  // spill limit size is 100 * 1024 * 1024;
};

TEST_F(ScEvictionObjectTest, DISABLED_TestEvictSc50Oc50)
{
    maxSize_ = 50 * 1024 * 1024;                 // shared memory size 50 * 1024 * 1024
    scPercent_ = 70;                             // sc shared memory max size is 70 / 100 * maxSize_
    ocPercent_ = 100;                            // oc shared memory max size is 50 / 100 * maxSize_
    constexpr size_t limit = 100 * 1024 * 1024;  //
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestEvictSc50Oc50";
    InitTest();
    auto streamSize = 1 * 1024 * 1024;  // stream page size is 1 * 1024 * 1024;
    for (int i = 0; i < 30; i++) {      // object num is 30
        auto prefix = "test_for_evict_";
        auto objectSize = 1 * 1024 * 1024;
        DS_ASSERT_OK(CreateObject(prefix + std::to_string(i), objectSize));
        evictionManager_->Add(prefix + std::to_string(i));
    }
    auto unit = std::make_shared<ShmUnit>();
    for (int i = 0; i < 30; i++) {  // stream num is 30
        DS_ASSERT_OK(scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer" + std::to_string(i),
                                                                 streamSize, true, *unit, true));
    }
}

TEST_F(ScEvictionObjectTest, TestEvictScSizeMax)
{
    maxSize_ = 50 * 1024 * 1024;  // shared memory size 50 * 1024 * 1024
    scPercent_ = 50;              // sc shared memory max size is 50% * maxSize_
    ocPercent_ = 100;             // oc shared memory max size is 100% * maxSize_
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestEvictScSizeMax";
    InitTest();
    auto size = 27 * 1024 * 1024;  // stream page size is 27 * 1024 * 1024;
    auto unit = std::make_shared<ShmUnit>();
    auto status = unit->AllocateMemory("", size, true, ServiceType::STREAM);
    ASSERT_EQ(status.GetCode(), StatusCode::K_OUT_OF_MEMORY) << status.GetMsg();
    status = scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer", size, true, *unit, true);
    ASSERT_TRUE(status.GetMsg().find("Stream cache memory size overflow, maxStreamSize") != std::string::npos);
}

TEST_F(ScEvictionObjectTest, TestScNotEvictObject)
{
    maxSize_ = 50 * 1024 * 1024;  // shared memory size 50 * 1024 * 1024
    scPercent_ = 100;             // sc shared memory max size is 100% * maxSize_
    ocPercent_ = 100;             // oc shared memory max size is 100% * maxSize_
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestScNotEvictObject";
    auto streamSize = 2 * 1024 * 1024;  // stream page size is 2 * 1024 * 1024;
    InitTest();
    auto unit = std::make_shared<ShmUnit>();
    for (int i = 0; i < 9; i++) {  // stream page num is 9
        DS_ASSERT_OK(scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer" + std::to_string(i),
                                                                 streamSize, true, *unit, true));
    }
}

TEST_F(ScEvictionObjectTest, TestEvictObject)
{
    LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
    maxSize_ = 10 * 1024 * 1024;                 // shared memory size 10 * 1024 * 1024
    scPercent_ = 100;                            // sc shared memory max size is 100% * maxSize_
    ocPercent_ = 50;                             // oc shared memory max size is 50% * maxSize_
    constexpr size_t limit = 100 * 1024 * 1024;  // spill limit size is 100 * 1024 * 1024;
    FLAGS_spill_size_limit = limit;
    FLAGS_spill_directory = "./spill_TestEvictObject";
    auto streamSize = 8 * 1024 * 1024;
    InitTest();
    auto unit = std::make_shared<ShmUnit>();
    DS_ASSERT_OK(scAllocateManager_->AllocateMemoryForStream(DEFAULT_TENANT_ID, "qwer", streamSize, true, *unit, true));
    const int kNumObjectsToCreate = 10;
    for (int i = 0; i < kNumObjectsToCreate; i++) {
        auto prefix = "test_for_evict_";
        auto objectSize = 500 * 1024;
        DS_ASSERT_OK(CreateObject(prefix + std::to_string(i), objectSize, WriteMode::NONE_L2_CACHE, true, false,
                                  DataFormat::BINARY, true, evictionManager_));
        evictionManager_->Add(prefix + std::to_string(i));
    }
}

class EvictionManagerBenchTest : public CommonTest, public BenchHelper {};

TEST_F(EvictionManagerBenchTest, BenchThread1)
{
    const int logLevel = 2;
    FLAGS_minloglevel = logLevel;
    EvictionList list;
    const int threadCnt = 1;
    PerfTwoAction(
        threadCnt, GenUniqueString, [&list](const std::string &key) { list.Add(key, Q1); },
        [&list](const std::string &key) { list.Erase(key); });
}

TEST_F(EvictionManagerBenchTest, BenchThread4)
{
    const int logLevel = 2;
    FLAGS_minloglevel = logLevel;
    EvictionList list;
    const int threadCnt = 4;
    PerfTwoAction(
        threadCnt, GenUniqueString, [&list](const std::string &key) { list.Add(key, Q1); },
        [&list](const std::string &key) { list.Erase(key); });
}

TEST_F(EvictionManagerBenchTest, BenchThread8)
{
    const int logLevel = 2;
    FLAGS_minloglevel = logLevel;
    EvictionList list;
    const int threadCnt = 8;
    PerfTwoAction(
        threadCnt, GenUniqueString, [&list](const std::string &key) { list.Add(key, Q1); },
        [&list](const std::string &key) { list.Erase(key); });
}
}  // namespace ut
}  // namespace datasystem

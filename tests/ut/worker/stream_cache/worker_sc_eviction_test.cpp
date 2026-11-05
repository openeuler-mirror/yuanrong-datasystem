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

#include "ut/common.h"
#include "ut/worker/object_cache/eviction_manager_common.h"
#include "ut/worker/object_cache/test_metadata_route.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"

using namespace datasystem::object_cache;
using namespace datasystem::worker;
using namespace datasystem::master;

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_size_limit);

namespace datasystem {
namespace ut {

class ScEvictionObjectTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp() override
    {
        LOG(INFO) << "Init ScEvictionObjectTest";
    }

    void TearDown() override
    {
        scAllocateManager_.reset();
        evictionManager_.reset();
        objectTable_.reset();
        WorkerOcSpill::Instance()->ResetForTest();
        if (allocator != nullptr) {
            allocator->ResetForTest();
            allocator = nullptr;
        }
        CommonTest::TearDown();
    }

    void InitTest()
    {
        scAllocateManager_.reset();
        evictionManager_.reset();
        objectTable_.reset();
        WorkerOcSpill::Instance()->ResetForTest();
        if (allocator != nullptr) {
            allocator->ResetForTest();
        }
        objectTable_ = std::make_shared<ObjectTable>();
        allocator = datasystem::memory::Allocator::Instance();

        allocator->Init(maxSize_, false, true, 5000, ocPercent_, scPercent_);  // decay is 5000 ms.
        std::shared_ptr<ObjectTable> &objectTable = GetObjectTable();
        evictionManager_ = std::make_shared<object_cache::WorkerOcEvictionManager>(
            objectTable, HostPort("127.0.0.1", 32131),  // worker port is 32131,
            HostPort("127.0.0.1", 52319), GetTestMetadataRoute());  // master port is 52319;
        auto globalRefTable = std::make_shared<ObjectGlobalRefTable<ClientKey>>();
        DS_ASSERT_OK(evictionManager_->Init(globalRefTable));
        scAllocateManager_ = std::make_shared<worker::stream_cache::WorkerSCAllocateMemory>(evictionManager_);
    }

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

}  // namespace ut
}  // namespace datasystem

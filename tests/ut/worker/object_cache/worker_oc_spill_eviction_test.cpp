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
 * Description: Test SpillFileManager and eviction manager.
 */

#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/worker_oc_spill.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "common.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/log/log.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "eviction_manager_common.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_uint64(spill_file_open_limit);
DS_DECLARE_uint64(spill_file_max_size_mb);
DS_DECLARE_uint64(spill_size_limit);

using namespace datasystem::object_cache;

namespace datasystem {
namespace ut {

class SpillEvictionTest : public CommonTest, public EvictionManagerCommon {
public:
    void SetUp()
    {
        CommonTest::SetUp();

        const uint64_t memSize = 1024ul * 1024ul * 1024ul;
        allocator = datasystem::memory::Allocator::Instance();
        allocator->Init(memSize);

        FLAGS_spill_directory = "./spill" + GetStringUuid();
        FLAGS_spill_size_limit = maxSize_;
        FLAGS_v = 1;

        objectTable_ = std::make_shared<ObjectTable>();
        gRefTable_ = std::make_shared<ObjectGlobalRefTable>();
        akSkManager_ = std::make_shared<AkSkManager>(0);
        DS_ASSERT_OK(akSkManager_->SetClientAkSk(accessKey_, secretKey_));
        evictionManager_ = std::make_shared<WorkerOcEvictionManager>(objectTable_, workerAddr_, workerAddr_);
        DS_ASSERT_OK(evictionManager_->Init(gRefTable_, akSkManager_));

        handler_ = WorkerOcSpill::Instance();
        LOG_IF_ERROR(inject::Set("worker.Spill.Sync", "return()"), "set inject point failed");
    }

    void CreateObjects(const std::string &prefix, uint64_t dataSize, uint32_t count,
                       WriteMode mode = WriteMode::NONE_L2_CACHE)
    {
        for (uint32_t i = 0; i < count; ++i) {
            std::string objectKey = prefix + GetModeName(mode) + std::to_string(i);
            DS_ASSERT_OK(CreateObject(objectKey, dataSize, mode));
        }
    }

    void SpillEvictMock(const std::string &objectKey, const void *buffer, uint64_t size, bool retry = false,
                        bool evictable = true)
    {
        evictionManager_->TryEvictSpilledObjects(size);
        if (retry) {
            Status rc;
            const size_t maxRetryCount = 5;
            for (size_t i = 0; i < maxRetryCount; ++i) {
                rc = WorkerOcSpill::Instance()->Spill(objectKey, buffer, size, evictable);
                if (rc.GetCode() == StatusCode::K_NO_SPACE) {
                    constexpr int sleepTimes = 50'000;
                    usleep(sleepTimes);
                } else {
                    break;
                }
            }
            DS_ASSERT_OK(rc);
        } else {
            DS_ASSERT_OK(WorkerOcSpill::Instance()->Spill(objectKey, buffer, size, evictable));
        }
    }

protected:
    std::string GetModeName(WriteMode mode)
    {
        switch (mode) {
            case WriteMode::NONE_L2_CACHE:
                return "none_l2_cache";
            case WriteMode::WRITE_BACK_L2_CACHE:
                return "write_back_l2_cache";
            case WriteMode::WRITE_THROUGH_L2_CACHE:
                return "write_through_l2_cache";
            case WriteMode::NONE_L2_CACHE_EVICT:
                return "none_l2_cache_evict";
            default:
                return "UNKONOWN";
        }
    }

    std::shared_ptr<WorkerOcEvictionManager> evictionManager_;
    HostPort workerAddr_{ "127.0.0.1", 18481 };
    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
    WorkerOcSpill *handler_;

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ObjectGlobalRefTable> gRefTable_;
};

TEST_F(SpillEvictionTest, DISABLED_TestSpillEvictableObject)
{
    LOG(INFO) << "Test spill evictable objects and not meets K_NO_SPACE error.";
    std::string prefix = "Obj";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = 100;

    CreateObjects(prefix, size, count, WriteMode::WRITE_THROUGH_L2_CACHE);

    for (size_t i = 0; i < count; ++i) {
        std::string objKey = prefix + GetModeName(WriteMode::WRITE_THROUGH_L2_CACHE) + std::to_string(i);
        SpillEvictMock(objKey, spillData.data(), spillData.size());
    }
}

TEST_F(SpillEvictionTest, DISABLED_TestSpillEvictableObjectConcurrently)
{
    LOG(INFO) << "Test spill evictable objects concurrently and not meets K_NO_SPACE error.";
    std::string prefix = "Obj";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = 20;
    size_t threadNum = 5;

    CreateObjects(prefix, size, count * threadNum, WriteMode::WRITE_THROUGH_L2_CACHE);

    std::vector<std::thread> threads(threadNum);

    for (size_t i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, count, i, &prefix, &spillData]() {
            for (size_t k = i * count; k < (i + 1) * count; ++k) {
                std::string objKey = prefix + GetModeName(WriteMode::WRITE_THROUGH_L2_CACHE) + std::to_string(k);
                SpillEvictMock(objKey, spillData.data(), spillData.size(), true);
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(SpillEvictionTest, DISABLED_TestSpillEvictWriteBackObject)
{
    LOG(INFO) << "Test spill write back objects evict.";
    std::string prefix1 = "Vector";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = maxSize_ / size - 1;

    CreateObjects(prefix1, size, count, WriteMode::WRITE_BACK_L2_CACHE);

    for (size_t i = 0; i < count; ++i) {
        std::string objKey = prefix1 + GetModeName(WriteMode::WRITE_BACK_L2_CACHE) + std::to_string(i);
        SpillEvictMock(objKey, spillData.data(), spillData.size());
    }

    // Write back objects has not been written to l2 cache, it can not be evict now, so we now have no space.
    DS_ASSERT_NOT_OK(handler_->Spill("randomrandom", spillData.data(), spillData.size()));

    // Set write back done flags and spill the none l2 cache objects to evict all write back objects.
    for (auto &entry : *objectTable_) {
        entry.second->Get()->stateInfo.SetWriteBackDone(true);
    }

    std::string prefix2 = "Camille";
    CreateObjects(prefix2, size, count, WriteMode::NONE_L2_CACHE);
    for (size_t i = 0; i < count; ++i) {
        std::string objKey = prefix2 + GetModeName(WriteMode::NONE_L2_CACHE) + std::to_string(i);
        SpillEvictMock(objKey, spillData.data(), spillData.size(), true, false);
    }
}

TEST_F(SpillEvictionTest, DISABLED_TestEvictWaterMark)
{
    LOG(INFO) << "Test spill evict water mark.";
    std::string prefix = "Vector";
    const uint64_t size = 4 * 1024ul * 1024ul;
    std::vector<uint8_t> spillData(size, 0);
    size_t count = maxSize_ / size - 1;

    CreateObjects(prefix, size, count, WriteMode::WRITE_THROUGH_L2_CACHE);
    for (size_t i = 0; i < count; ++i) {
        std::string objectKey = prefix + GetModeName(WriteMode::WRITE_THROUGH_L2_CACHE) + std::to_string(i);
        DS_ASSERT_OK(WorkerOcSpill::Instance()->Spill(objectKey, spillData.data(), spillData.size(), true));
    }
    ASSERT_TRUE(WorkerOcSpill::Instance()->IsSpaceExceedHWM());

    std::string newPrefix = "new_added_object";
    CreateObjects(newPrefix, size, 1, WriteMode::NONE_L2_CACHE);
    SpillEvictMock(newPrefix, spillData.data(), spillData.size(), true);

    // Wait for compact process complete.
    const int sleepUs = 10'000;
    usleep(sleepUs);
    ASSERT_FALSE(WorkerOcSpill::Instance()->IsSpaceExceedLWM());
}
}  // namespace ut
}  // namespace datasystem

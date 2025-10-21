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
 * Description: shm threshold test.
 */
#include "gtest/gtest.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/shared_memory/allocator.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
class ShmThresholdTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = totalWorker;
        opts.numEtcd = 1;
        opts.enableSpill = true;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -oc_shm_threshold_percentage=80";
        FLAGS_v = 1;
    }
 
protected:
    std::string spillDir;
    int totalWorker = 2;
    std::string objKey0 = "objKey0";
    std::string objKey1 = "objKey1";
    size_t shmSz = 20 * 1000ul * 1000ul;
    CreateParam createParam = { .writeMode = WriteMode::NONE_L2_CACHE,
                                .consistencyType = ConsistencyType::CAUSAL };
};
 
TEST_F(ShmThresholdTest, DISABLED_LEVEL1_AllocationFailedForThreshold)
{
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    RandomData randomData;
    auto bytes1 = randomData.RandomBytes(shmSz);
    auto bytes2 = randomData.RandomBytes(shmSz * 3);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Allocator.MemoryAllocatedToStream", "call(50000000)"));
    // Object cache can use at max 100MB x 80% = 80MB. However, 50MB is used already.
    // Therefore, shared memroy available to OC is 50MB. Can't create object with size = 60MB.
    std::vector<std::string> failedIds;
    client1->GIncreaseRef({ objKey0 }, failedIds);
    DS_ASSERT_OK(client1->Put(objKey0, bytes1.data(), bytes1.size(), createParam));  // obj0: 20MB.)
    client1->GIncreaseRef({ objKey1 }, failedIds);
    Status rc = client1->Put(objKey1, bytes2.data(), bytes2.size(), createParam);  // obj0: 60MB.
 
    DS_ASSERT_NOT_OK(rc);
    ASSERT_EQ(rc.GetCode(), K_OUT_OF_MEMORY);
}
 
TEST_F(ShmThresholdTest, EvictionWithShmThreshold)
{
    std::shared_ptr<ObjectClient> client1, client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    RandomData randomData;
    auto bytes1 = randomData.RandomBytes(shmSz * 2);
    auto bytes2 = randomData.RandomBytes(shmSz);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.Allocator.MemoryAllocatedToStream", "call(40000000)"));
    // Object cache can use at max 100MB x 80% = 80MB. However, 40MB is used already.
    // Therefore, shared memroy available to OC is 60MB. Eviction starts when shm reaches 60MB x 80% WaterMark = 48MB.
    std::vector<std::string> failedIds;
    client1->GIncreaseRef({ objKey0 }, failedIds);
    client1->Put(objKey0, bytes1.data(), bytes1.size(), createParam);  // obj0: 40MB.
 
    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> objectKeys;
    objectKeys.push_back(objKey0);
    objectKeys.push_back(objKey1);
    DS_ASSERT_OK(client1->Get({ objKey0 }, 0, buffers));
    DS_ASSERT_NOT_OK(client1->Get({ objKey1 }, 0, buffers));
    buffers.clear();
 
    DS_ASSERT_OK(client1->GIncreaseRef({ objKey1 }, failedIds));
    // eviction should happen at this stage.
    DS_ASSERT_OK(client1->Put(objKey1, bytes2.data(), bytes2.size(), createParam));  // obj0: 20MB.
}
}  // namespace st
}  // namespace datasystem
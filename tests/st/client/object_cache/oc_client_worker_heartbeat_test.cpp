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
 * Description:
 */
#include <gtest/gtest.h>

#include "oc_client_common.h"

#include "datasystem/client/mmap_manager.h"
#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace st {
namespace {
constexpr int WORKER_NUM = 3;
constexpr int64_t SHM_SIZE = 600 * 1024;
}  // namespace
class OCClientWorkerHeartbeatTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.workerGflagParams = "-node_timeout_s=1 -heartbeat_interval_ms=500";
        opts.disableRocksDB = false;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }
};

TEST_F(OCClientWorkerHeartbeatTest, DISABLED_CheckHealthFile)
{
    // checkout health probe.
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
}

TEST_F(OCClientWorkerHeartbeatTest, LEVEL2_OneClientOneWorkerRestartTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    InitTestClient(0, cliLocal);
    std::string obj1Id = NewObjectKey();
    std::shared_ptr<Buffer> data;
    std::vector<std::string> failedObjKeys;
    DS_ASSERT_OK(cliLocal->GIncreaseRef({ obj1Id }, failedObjKeys));
    DS_ASSERT_OK(cliLocal->Create(obj1Id, SHM_SIZE, CreateParam{}, data));
    DS_ASSERT_OK(data->Seal());

    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne({ 0, 1, 2 }));

    std::vector<std::string> objectKeys;
    objectKeys.push_back(obj1Id);
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->GDecreaseRef({ obj1Id }, failedObjKeys));
    DS_ASSERT_OK(cliLocal->GIncreaseRef({ obj1Id }, failedObjKeys));
    DS_ASSERT_OK(cliLocal->Create(obj1Id, SHM_SIZE, CreateParam{}, data));
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(SHM_SIZE, data.get()->GetSize());
    DS_ASSERT_OK(data->Seal());
    DS_ASSERT_OK(cliLocal->Get(objectKeys, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));
    DS_ASSERT_OK(cliLocal->GDecreaseRef({ obj1Id }, failedObjKeys));
}

TEST_F(OCClientWorkerHeartbeatTest, LEVEL1_MultiClientOneWorkerRestartTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    std::shared_ptr<ObjectClient> cliLocal2;
    InitTestClient(0, cliLocal);
    InitTestClient(0, cliLocal2);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 65, 66, 67, 68, 69 };
    std::vector<std::string> failedObjKeys;
    DS_ASSERT_OK(cliLocal->GIncreaseRef({ obj1Id }, failedObjKeys));
    DS_ASSERT_OK(cliLocal2->GIncreaseRef({ obj2Id }, failedObjKeys));
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliLocal2, obj2Id, data2);

    // Shutdown worker
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // The interval is 0.5s, so the max duration for discovering the worker status is 0.5x2.

    // Restart worker
    cluster_->StartWorkers();
    sleep(1);  // The interval is 0.5s, so the max duration for discovering the worker status is 0.5x2.

    DS_ASSERT_OK(cliLocal->GDecreaseRef({ obj1Id }, failedObjKeys));
    DS_ASSERT_OK(cliLocal2->GDecreaseRef({ obj2Id }, failedObjKeys));
    // Create object
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliLocal2, obj2Id, data2);

    ThreadPool getPool(2);
    std::vector<std::string> getObj1 = { obj2Id };
    auto fut1 = getPool.Submit([getObj1, &cliLocal, obj2Id]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal->Get(getObj1, 0, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*(dataList[0]), std::string{ 65, 66, 67, 68, 69 });
    });

    std::vector<std::string> getObj2 = { obj1Id };
    auto fut2 = getPool.Submit([getObj2, &cliLocal2, obj1Id]() {
        std::vector<Optional<Buffer>> dataList;
        DS_ASSERT_OK(cliLocal2->Get(getObj2, 0, dataList));
        ASSERT_TRUE(NotExistsNone(dataList));
        ASSERT_NE(dataList[0]->ImmutableData(), nullptr);
        AssertBufferEqual(*(dataList[0]), std::string{ 65, 66, 67, 68, 69, 70 });
    });
    fut1.get();
    fut2.get();
}

TEST_F(OCClientWorkerHeartbeatTest, LEVEL2_MultiClientMultiWorkerRestartTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    std::shared_ptr<ObjectClient> cliLocal2;
    InitTestClient(0, cliLocal);
    InitTestClient(1, cliLocal2);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 65, 66, 67, 68, 69 };
    std::vector<std::string> failedObjKeys;
    DS_ASSERT_OK(cliLocal->GIncreaseRef({ obj1Id }, failedObjKeys));
    DS_ASSERT_OK(cliLocal2->GIncreaseRef({ obj2Id }, failedObjKeys));
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliLocal2, obj2Id, data2);

    std::vector<std::string> objectKeys;
    objectKeys.push_back(NewObjectKey());
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_NOT_OK(cliLocal->Get(objectKeys, 500, dataList));

    cluster_->ShutdownNode(WORKER, 1);
    sleep(1);  // The interval is 0.5s, so the max duration for discovering the worker status is 0.5x2.

    // Get shutdown worker1 object
    objectKeys.clear();
    objectKeys.push_back(obj2Id);
    std::vector<Optional<Buffer>> dataList2;
    DS_ASSERT_NOT_OK(cliLocal->Get(objectKeys, 2, dataList2));

    // Restart worker1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 1));
    sleep(1);  // The interval is 0.5s, so the max duration for discovering the worker status is 0.5x2.
    DS_ASSERT_OK(cliLocal2->GDecreaseRef({ obj2Id }, failedObjKeys));
    // Create same object success
    CreateAndSealObject(cliLocal2, obj2Id, data2);

    // Get worker0 object
    objectKeys.clear();
    objectKeys.push_back(obj1Id);
    std::vector<Optional<Buffer>> dataList3;
    DS_ASSERT_OK(cliLocal2->Get(objectKeys, 200, dataList3));
    ASSERT_TRUE(NotExistsNone(dataList3));
}

TEST_F(OCClientWorkerHeartbeatTest, OneClientShutdownTest)
{
    std::shared_ptr<ObjectClient> cliLocal;
    std::shared_ptr<ObjectClient> cliLocal2;
    std::shared_ptr<ObjectClient> cliLocal3;
    InitTestClient(0, cliLocal);
    InitTestClient(0, cliLocal2);
    InitTestClient(1, cliLocal3);

    std::string obj1Id = NewObjectKey();
    std::string obj2Id = NewObjectKey();
    std::vector<uint8_t> data1 = { 65, 66, 67, 68, 69, 70 };
    std::vector<uint8_t> data2 = { 65, 66, 67, 68, 69, 70 };
    CreateAndSealObject(cliLocal, obj1Id, data1);
    CreateAndSealObject(cliLocal, obj2Id, data2);

    std::vector<std::string> objectKeys;
    objectKeys.push_back(obj1Id);
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(cliLocal->Get(objectKeys, 0, dataList));
    ASSERT_TRUE(NotExistsNone(dataList));

    std::vector<Optional<Buffer>> dataList2;
    DS_ASSERT_OK(cliLocal2->Get(objectKeys, 0, dataList2));
    ASSERT_TRUE(NotExistsNone(dataList2));

    std::vector<std::string> objectKeys2;
    objectKeys2.push_back(obj2Id);
    std::vector<Optional<Buffer>> dataList3;
    DS_ASSERT_OK(cliLocal3->Get(objectKeys2, 0, dataList3));
    ASSERT_TRUE(NotExistsNone(dataList3));
}
}  // namespace st
}  // namespace datasystem

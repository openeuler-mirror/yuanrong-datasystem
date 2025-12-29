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

#include <gtest/gtest.h>
#include <unistd.h>

#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/protos/ut_object.stub.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {
class WorkerDfxTest : public OCClientCommon {
public:
    void TearDown() override
    {
        objClient0_ = nullptr;
        objClient1_ = nullptr;
        objClient2_ = nullptr;
        ExternalClusterTest::TearDown();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -sc_stream_socket_num=0 -node_timeout_s=1 "
            "-heartbeat_interval_ms=500";
        opts.numWorkers = 3;
        opts.masterIdx = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    void InitTestClients(int32_t timeout = 60000)
    {
        InitTestClient(0, objClient0_, timeout);
        InitTestClient(0, objClient1_, timeout);
        InitTestClient(1, objClient2_, timeout);
    }

protected:
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient1_;
    std::shared_ptr<ObjectClient> objClient2_;
};

TEST_F(WorkerDfxTest, LEVEL1_TestWorkerCrashAndOperateBuffer)
{
    LOG(INFO) << "Test worker crash and operate the shm buffer.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string objKey = "Warriors";
    CreateParam param;
    size_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objKey, objSize, param, buffer));

    // Then we crash the worker and try to operate the shm pointer.
    cluster_->ShutdownNodes(WORKER);
    sleep(2);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    std::vector<uint8_t> data;
    data.resize(objSize);
    std::vector<std::string> failedObjectKeys;
    // Make sure the op below would be failed.
    DS_ASSERT_NOT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_NOT_OK(buffer->Publish());
    DS_ASSERT_NOT_OK(client1->GIncreaseRef({ objKey }, failedObjectKeys));
    DS_ASSERT_NOT_OK(buffer->Seal());
    DS_ASSERT_NOT_OK(buffer->WLatch());
    DS_ASSERT_NOT_OK(buffer->RLatch());
    DS_ASSERT_NOT_OK(buffer->UnWLatch());
    DS_ASSERT_NOT_OK(buffer->UnRLatch());
    DS_ASSERT_NOT_OK(client1->GDecreaseRef({ objKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestWorkerRestartAndOperateShmBuffer)
{
    LOG(INFO) << "Test worker restart and operate the not shm buffer.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::string objKey1 = "Warriors1";
    std::string objKey2 = "Warriors2";
    CreateParam param;
    size_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    std::vector<Optional<Buffer>> buffers;
    std::vector<uint8_t> data;
    data.resize(objSize);

    DS_ASSERT_OK(client1->Create(objKey1, objSize, param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Seal());
    DS_ASSERT_OK(client1->Get({ objKey1 }, 0, buffers));

    // Then we crash the worker and try to operate the shm pointer.
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    sleep(2);

    // Make sure the op below would be failed.
    DS_ASSERT_OK(client1->Create(objKey2, objSize, param, buffer));
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Seal());
    DS_ASSERT_OK(client1->Get({ objKey2 }, 0, buffers));
    buffer.reset();
}

TEST_F(WorkerDfxTest, TestWorkerRestartAndOperateNoShmBuffer)
{
    LOG(INFO) << "Test worker restart and operate the not shm buffer.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1);
    std::string objKey = "Warriors";
    CreateParam param;
    size_t objSize = 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objKey, objSize, param, buffer));

    // Then we crash the worker and try to operate the shm pointer.
    cluster_->ShutdownNodes(WORKER);
    sleep(1);  // The heartbeat interval is 0.5s, and the maximum number of worker disconnections is 1s.
    DS_ASSERT_OK(cluster_->StartWorkers());
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    std::vector<uint8_t> data;
    data.resize(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy(data.data(), data.size()));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffer->Seal());
}

TEST_F(WorkerDfxTest, TestWorkerCrashRecovery)
{
    LOG(INFO) << "Test worker crash recovery and decrease the global reference.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    size_t objNum = 5;
    std::vector<std::string> objectKeys;
    for (size_t i = 0; i < objNum; ++i) {
        std::string objKey = "Warriors" + std::to_string(i);
        CreateAndSealObject(client1, objKey, data);
        objectKeys.emplace_back(objKey);
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Shutdown worker and then restart to verify the metadata recovery.
    cluster_->ShutdownNodes(WORKER);
    DS_ASSERT_OK(cluster_->StartWorkers());
    const int workerNum = 3;
    for (int i = 0; i < workerNum; i++) {
        DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
    }
    sleep(2);

    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(WorkerDfxTest, LEVEL1_TestClientExitAndCreateTheSameObject)
{
    LOG(INFO) << "Test client exit and other client create the objects with same IDs";
    std::shared_ptr<ObjectClient> client1;
    int timeoutMs = 3000;
    InitTestClient(0, client1, timeoutMs);
    size_t size = 4 * 1024ul * 1024ul;
    std::string namePrefix = "ToyotaLee";
    size_t cnt = 0;

    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers;
    Status rc;
    do {
        std::string id = namePrefix + std::to_string(cnt);
        CreateParam param;
        std::shared_ptr<Buffer> buffer;
        rc = client1->Create(id, size, param, buffer);
        if (rc.IsOk()) {
            cnt++;
            buffers.emplace_back(std::move(buffer));
            objKeys.emplace_back(id);
        }
    } while (rc.IsOk());

    // client 0 exit, its metadata would be clear in worker.
    buffers.clear();
    client1.reset();

    sleep(1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client2);

    for (const auto &id : objKeys) {
        CreateParam param;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client2->Create(id, size, param, buffer));
        buffers.emplace_back(std::move(buffer));
    }
}

TEST_F(WorkerDfxTest, TestGdecreaseWorkerClearFailed)
{
    LOG(INFO) << "Test worker clear object failed and decrease the global reference.";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::string data = "123456";
    std::string objectKey = "object1";
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client1->Create(objectKey, data.size(), CreateParam{}, buffer));
    DS_ASSERT_OK(buffer->Publish());
    std::vector<Optional<Buffer>> buffers;
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    failedObjectKeys.clear();
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.clear_object_failure", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_NOT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(1));
    failedObjectKeys.clear();
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_NOT_OK(client2->Get({ objectKey }, 0, buffers));
}

TEST_F(WorkerDfxTest, DISABLED_TestWorkerNotFirstGincreaseAndMasterCrash)
{
    std::shared_ptr<ObjectClient> client2;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client2);
    InitTestClient(0, client1);
    std::string objectKey = NewObjectKey();
    std::string data = "123456";
    int dataSize = data.size();
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    std::shared_ptr<Buffer> buffer1;
    DS_ASSERT_OK(client2->Create(objectKey, dataSize, param, buffer1));
    DS_ASSERT_OK(buffer1->MemoryCopy(data.data(), dataSize));
    DS_ASSERT_OK(buffer1->Publish());
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client2->GIncreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    cluster_->ShutdownNode(WORKER, 1);
    sleep(10);  // Wait 10 seconds for worker shutdown finished
    failObjects.clear();
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    cluster_->StartNode(WORKER, 1, "");
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    failObjects.clear();
    ASSERT_EQ(client1->QueryGlobalRefNum(objectKey), 2);
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
    failObjects.clear();
    DS_ASSERT_OK(client2->GDecreaseRef({ objectKey }, failObjects));
    ASSERT_TRUE(failObjects.empty());
}

TEST_F(WorkerDfxTest, LEVEL1_TestWorkerCrashRecoveryAndClientExist)
{
    LOG(INFO) << "Test worker crash recovery and client exit before worker recovery.";
    std::string objKey = "KingBob";
    {
        std::shared_ptr<ObjectClient> client1;
        auto timeoutMs = 3000;
        InitTestClient(0, client1, timeoutMs);
        CreateParam param;
        size_t objSize = 500 * 1024ul;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client1->Create(objKey, objSize, param, buffer));
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client1->GIncreaseRef({ objKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(buffer->Seal());

        // Shutdown worker and exit client.
        cluster_->ShutdownNode(WORKER, 0);
    }
    // When worker restart, it need to notify master to decrease the global
    // reference in objectClient0_, and if buffer is not keep buffer, it will
    // be notify to deleted.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    std::shared_ptr<ObjectClient> client2;
    InitTestClient(1, client2);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client2->Get({ objKey }, 1'000, buffers));
    LOG(INFO) << "Test worker crash recovery and client exit before worker recovery end.";
}

TEST_F(WorkerDfxTest, TestWorkerPublishSealTimeout)
{
    LOG(INFO) << "Test worker process seal timeout.";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.publish_failure", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(buffer->Publish());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.seal_failure", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(buffer->Seal());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient2_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));
}

TEST_F(WorkerDfxTest, TestMasterCreateMetaFailure)
{
    LOG(INFO) << "Test master process create meta failure.";
    int32_t timeoutMs = 3000;
    InitTestClients(timeoutMs);
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "master.create_meta_failure", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(3000)");
    DS_ASSERT_NOT_OK(buffer->Seal());
    DS_ASSERT_OK(buffer->Seal());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient2_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_BUF_EQ(*buffers2[0], data);
}

TEST_F(WorkerDfxTest, TestAutoUnWLatch)
{
    LOG(INFO) << "Test latch auto unlock when client crash";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), 1ul);
    DS_ASSERT_OK(buffer->WLatch());
    DS_ASSERT_OK(datasystem::inject::Set("buffer.release", "call()"));
    objClient0_.reset();
    buffer.reset();
    DS_ASSERT_OK(buffers2[0]->WLatch());
}

TEST_F(WorkerDfxTest, TestAutoUnRLatch)
{
    LOG(INFO) << "Test latch auto unlock when client crash";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), 1ul);
    DS_ASSERT_OK(buffer->RLatch());
    DS_ASSERT_OK(datasystem::inject::Set("buffer.release", "call()"));
    objClient0_.reset();
    buffer.reset();
    DS_ASSERT_OK(buffers2[0]->WLatch());
}

TEST_F(WorkerDfxTest, TestPublishFailNotVisiable)
{
    LOG(INFO) << "Test shm buffer publish fail and not visible";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));

    cluster_->ShutdownNode(WORKER, 1);

    DS_ASSERT_NOT_OK(buffer->Publish());
    DS_ASSERT_NOT_OK(buffers2[0]->RLatch());

    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(buffers2[0]->RLatch());
}

TEST_F(WorkerDfxTest, TestClientExitAsyncNotifyToMaster)
{
    // Fix DTS2022082215708
    LOG(INFO) << "Test client exit and async notify to master";
    InitTestClients();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.gdecrease", "2*return(K_RPC_UNAVAILABLE)"));

    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::vector<std::shared_ptr<Buffer>> buffers;
    for (size_t i = 0; i < 10; ++i) {
        std::string objKey = "Warriors" + std::to_string(1);
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(objClient0_->GIncreaseRef({ objKey }, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        buffers.emplace_back(std::move(buffer));
    }
    buffers.clear();
    objClient0_.reset();
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient1_->Create("Warriors", objSize, param, buffer));
}

TEST_F(WorkerDfxTest, TestMasterCacheInvalidFailure)
{
    LOG(INFO) << "Test master process cache invalid failure.";
    // Sort worker addresses.
    std::map<std::string, size_t> workerAddrMap;
    size_t needWorkerNum = 2;
    for (size_t i = 0; i < needWorkerNum; ++i) {
        HostPort addr;
        cluster_->GetWorkerAddr(i, addr);
        workerAddrMap.emplace(addr.ToString(), i);
    }
    // Create client
    std::vector<std::shared_ptr<ObjectClient>> clientVec;
    int32_t timeoutMs = 1000;
    for (const auto &it : workerAddrMap) {
        std::shared_ptr<ObjectClient> client;
        InitTestClient(it.second, client, timeoutMs);
        clientVec.push_back(client);
    }
    // worker1 create/publish object, worker1 get object
    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(clientVec[0]->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));

    // worker1 publish failed
    std::string data2 = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "master.cache_invalid_failed", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(1000)");
    DS_ASSERT_NOT_OK(buffer->Publish());

    // worker1 get old data
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(clientVec[0]->Get({ objKey }, 5000, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_EQ(buffers3.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers3[0]).ImmutableData()), (*buffers3[0]).GetSize()));

    // worker1 publish success
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker2 get new data
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));
}

TEST_F(WorkerDfxTest, TestPublishFailReGet)
{
    LOG(INFO) << "Test shm buffer publish fail and not visible";
    // Sort worker addresses.
    std::map<std::string, size_t> workerAddrMap;
    size_t needWorkerNum = 2;
    for (size_t i = 0; i < needWorkerNum; ++i) {
        HostPort addr;
        cluster_->GetWorkerAddr(i, addr);
        workerAddrMap.emplace(addr.ToString(), i);
    }
    // Create client
    std::vector<std::shared_ptr<ObjectClient>> clientVec;
    int32_t timeoutMs = 1000;
    for (const auto &it : workerAddrMap) {
        std::shared_ptr<ObjectClient> client;
        InitTestClient(it.second, client, timeoutMs);
        clientVec.push_back(client);
    }

    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 1024ul;
    std::shared_ptr<Buffer> buffer;
    // worker2 create
    DS_ASSERT_OK(clientVec[1]->Create(objKey, objSize, param, buffer));
    std::string data = GenRandomString(objSize);
    // Make sure the op below would be failed.
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data.c_str(), data.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker1 get object
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(clientVec[0]->Get({ objKey }, 5000, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_EQ(buffers2.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers2[0]).ImmutableData()), (*buffers2[0]).GetSize()));

    // worker2 publish failed
    std::string data2 = GenRandomString(objSize);
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.CreateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerMasterOCApi.UpdateMeta.timeoutMs", "call(20)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "master.cache_invalid_failed", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    datasystem::inject::Set("rpc_util.retry_on_error_after_func", "1*sleep(1000)");
    DS_ASSERT_NOT_OK(buffer->Publish());

    // worker2 retry get old data
    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));

    // worker2 retry publish success
    DS_ASSERT_OK(buffer->MemoryCopy((void *)data2.c_str(), data2.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker1 get new data
    std::vector<Optional<Buffer>> buffers5;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers5));
    ASSERT_TRUE(NotExistsNone(buffers5));
    ASSERT_EQ(buffers5.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers5[0]).ImmutableData()), (*buffers5[0]).GetSize()));

    // worker2 retry get new data
    std::vector<Optional<Buffer>> buffers6;
    DS_ASSERT_OK(clientVec[1]->Get({ objKey }, 5000, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_EQ(buffers4.size(), size_t(1));
    ASSERT_EQ(data2,
              std::string(reinterpret_cast<const char *>((*buffers4[0]).ImmutableData()), (*buffers4[0]).GetSize()));
}

TEST_F(WorkerDfxTest, TestGIncreaseRetry)
{
    LOG(INFO) << "Test GIncreaseRef retry";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.GIncrease_ref_failure", "1*return(K_TRY_AGAIN)"));
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestWorkerToMasterGIncreaseRetry)
{
    LOG(INFO) << "Test worker to master, GIncreaseRef retry";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.GIncrease_ref_failure", "1*return(K_TRY_AGAIN)"));
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestGIncreaseRetryIdempotence)
{
    LOG(INFO) << "Test GIncreaseRef idempotence";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.GIncrease_ref_Idempotence", "1*return(K_TRY_AGAIN)"));
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestGDecreaseClientRetry)
{
    LOG(INFO) << "Test GDecreaseRef client retry";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.gdecrease", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, TestGDecreaseRetryIdempotence)
{
    LOG(INFO) << "Test GDecreaseRef retry Idempotence";
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client1);
    std::string objectKey = NewObjectKey();
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.GDecrease_Ref_Idempotence", "1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failedObjectKeys));
}

TEST_F(WorkerDfxTest, DISABLED_TestChangePrimaryCopy)
{
    std::shared_ptr<ObjectClient> client1, client2, client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(2, client3);
    std::string objectKey = "obj1";
    std::vector<std::string> failObjects;
    DS_ASSERT_OK(client1->GIncreaseRef({ objectKey }, failObjects));
    int64_t dataSize = 10;
    std::string value1 = GenRandomString(dataSize);
    CreateParam param = { .consistencyType = ConsistencyType::CAUSAL };
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(value1.data()), value1.size(), param));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client3->Get({ objectKey }, 1'000, buffers));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(2000)"));
    sleep(10);
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    sleep(5);
    DS_ASSERT_OK(client2->Get({ objectKey }, 1'000, buffers));
    DS_ASSERT_OK(client1->GDecreaseRef({ objectKey }, failObjects));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
}

class MasterDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = "-client_reconnect_wait_s=1 -v=1";
        opts.numWorkers = 3;
        opts.masterIdx = masterIdx_;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.disableRocksDB = false;
    }

    void InitTestClients()
    {
        InitTestClient(0, objClient0_);
        InitTestClient(0, objClient01_);
        InitTestClient(1, objClient1_);
    }

    void TearDown() override
    {
        objClient0_ = nullptr;
        objClient01_ = nullptr;
        objClient1_ = nullptr;
        ExternalClusterTest::TearDown();
    }

protected:
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient01_;
    std::shared_ptr<ObjectClient> objClient1_;
    const int masterIdx_ = 2;
};

TEST_F(MasterDfxTest, TestMasterRecoveryAndDecreaseGRef)
{
    LOG(INFO) << "Test master crash recovery and decrease the global reference.";
    InitTestClients();
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    size_t objNum = 5;
    std::vector<std::string> objectKeys;
    for (size_t i = 0; i < objNum; ++i) {
        std::string objKey = "Warriors_" + std::to_string(i);
        CreateAndSealObject(objClient0_, objKey, data);
        objectKeys.emplace_back(objKey);
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Shutdown master and then restart to verify the metadata recovery.
    cluster_->ShutdownNode(WORKER, 2);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    sleep(2);

    DS_ASSERT_OK(objClient0_->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(objClient1_->Get({ "Warriors_0" }, 1'000, buffers));
}

TEST_F(MasterDfxTest, LEVEL1_TestMasterRecoveryAndClientExit)
{
    LOG(INFO) << "Test master crash and client exit before recovery";
    InitTestClients();
    std::string objKey = "Stuart";
    CreateParam param;
    size_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef({ objKey }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    DS_ASSERT_OK(buffer->Seal());

    // Shutdown master and exit client.
    cluster_->ShutdownNode(WORKER, 2);
    buffer.reset();
    objClient0_.reset();
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));

    // wait for async queue send message to master.
    sleep(2);
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(objClient1_->Get({ objKey }, 1'000, buffers));
}

TEST_F(MasterDfxTest, LEVEL1_TestMasterRecoveryAndClientExit2)
{
    LOG(INFO) << "Test master crash and client exit before recovery";
    InitTestClients();
    std::string objKey = "Stuart";
    CreateParam param;
    size_t objSize = 600 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));

    // Shutdown master and exit client.
    cluster_->ShutdownNode(WORKER, 2);
    objClient0_.reset();
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));

    // wait for async queue send message to master.
    sleep(1);

    DS_ASSERT_OK(objClient01_->Create(objKey, objSize, param, buffer));
}

TEST_F(MasterDfxTest, LEVEL1_TestMasterCrashAndGet)
{
    LOG(INFO) << "Test master crash and get";
    InitTestClients();
    std::string objKey = "Stuart";
    CreateParam param{ .consistencyType = ConsistencyType::CAUSAL };
    size_t objSize = 600 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());

    std::vector<Optional<Buffer>> getBuffers;
    std::string randData = GenRandomString(objSize);
    DS_ASSERT_OK(objClient1_->Get({ objKey }, 1'000, getBuffers));
    ASSERT_TRUE(NotExistsNone(getBuffers));
    ASSERT_EQ(getBuffers.size(), size_t(1));
    DS_ASSERT_OK(getBuffers[0]->MemoryCopy((void *)randData.data(), randData.size()));
    DS_ASSERT_OK(getBuffers[0]->Publish());

    // Shutdown master.
    cluster_->ShutdownNode(WORKER, 2);
    cluster_->SetInjectAction(WORKER, 0, "worker.query_meta", "1*return(K_RPC_UNAVAILABLE)");

    std::vector<Optional<Buffer>> getBuffers0;
    DS_ASSERT_NOT_OK(objClient0_->Get({ objKey }, 1'000, getBuffers0));

    // Restart master and wait for worker reconnect.
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 2));
    sleep(2);

    getBuffers0.clear();
    DS_ASSERT_OK(objClient0_->Get({ objKey }, 1'000, getBuffers0));
    ASSERT_TRUE(NotExistsNone(getBuffers0));
    ASSERT_EQ(getBuffers0.size(), size_t(1));
    std::string realData((const char *)getBuffers0[0]->ImmutableData(), getBuffers0[0]->GetSize());
    ASSERT_EQ(realData, randData);

    // worker 0 publish again.
    randData = GenRandomString(objSize);
    DS_ASSERT_OK(buffer->MemoryCopy((void *)randData.data(), randData.size()));
    DS_ASSERT_OK(buffer->Publish());

    // worker 1 get again and check.
    getBuffers0.clear();
    DS_ASSERT_OK(objClient0_->Get({ objKey }, 1'000, getBuffers0));
    ASSERT_TRUE(NotExistsNone(getBuffers0));
    ASSERT_EQ(getBuffers0.size(), size_t(1));
    std::string realData1((const char *)getBuffers0[0]->ImmutableData(), getBuffers0[0]->GetSize());
    ASSERT_EQ(realData1, randData);
}

class MasterWorkerDisconnectDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.masterGflagParams = "-node_timeout_s=1 -node_dead_timeout_s=2 -v=3 -heartbeat_interval_ms=500";
        opts.workerGflagParams = "-node_timeout_s=30 -heartbeat_interval_ms=500 -v=3";
        opts.numMasters = 1;
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
    }

    void InitObjectClients()
    {
        InitTestClient(0, objClient0_);
        InitTestClient(1, objClient1_);
    }

    void TearDown() override
    {
        objClient0_ = nullptr;
        objClient1_ = nullptr;
        ClusterTest::TearDown();
    }

protected:
    std::shared_ptr<ObjectClient> objClient0_;
    std::shared_ptr<ObjectClient> objClient1_;
};

TEST_F(MasterWorkerDisconnectDfxTest, TestWorkerTimeoutAndPushMeta)
{
    LOG(INFO) << "Test worker recovery timeout and master clean its metadata then worker push metadata to master";
    InitObjectClients();
    size_t num = 10;
    std::vector<uint8_t> data;
    data.resize(500 * 1024ul);
    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers(num);
    for (size_t i = 0; i < num; ++i) {
        std::string objKey = "BigEmperor" + std::to_string(i);
        objKeys.emplace_back(objKey);
        CreateParam param;
        DS_ASSERT_OK(objClient0_->Create(objKey, data.size(), param, buffers[i]));
        DS_ASSERT_OK(buffers[i]->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffers[i]->Publish());
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(objClient0_->GIncreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Let hearbeat timeout and master will clear its metadata, then when worker reconnect,
    // master will ask us to send the metadata to master.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(2000)"));
    sleep(4);

    for (size_t i = 0; i < num; ++i) {
        DS_ASSERT_OK(buffers[i]->Seal());
    }

    std::vector<Optional<Buffer>> getBuffers;
    DS_ASSERT_OK(objClient1_->Get(objKeys, 60'000, getBuffers));
    ASSERT_TRUE(NotExistsNone(getBuffers));
    DS_ASSERT_OK(objClient0_->GDecreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(MasterWorkerDisconnectDfxTest, LEVEL2_TestMasterDeadAndWorkerReturnMasterTimeout)
{
    InitObjectClients();
    std::string objKey = NewObjectKey();
    std::string data = "Unicorn Gundam";
    std::vector<std::string> failedObjs;
    DS_ASSERT_OK(objClient1_->GIncreaseRef({ objKey }, failedObjs));
    DS_ASSERT_OK(objClient1_->Put(objKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                  CreateParam{}));
    cluster_->ShutdownNode(ClusterNodeType::WORKER, 0);
    DS_ASSERT_NOT_OK(objClient1_->GDecreaseRef({ objKey }, failedObjs));
    failedObjs.clear();
    // Wait worker notify master timeout.
    std::this_thread::sleep_for(std::chrono::seconds(30));
    // Worker return K_MASTER_TIMOUT to client and don't retry.
    LOG(INFO) << "master timeout";
    for (size_t i = 0; i < 10; i++) {
        Timer timer;
        Status rc = objClient1_->GDecreaseRef({ objKey }, failedObjs);
        EXPECT_NE(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_LE(timer.ElapsedMilliSecondAndReset(), 1000);
        failedObjs.clear();
        rc = objClient1_->GIncreaseRef({ NewObjectKey() }, failedObjs);
        EXPECT_NE(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_LE(timer.ElapsedMilliSecondAndReset(), 1000);
        failedObjs.clear();
        std::vector<Optional<Buffer>> buffers;
        rc = objClient1_->Get({ objKey }, 0, buffers);
        EXPECT_NE(rc.GetCode(), K_RPC_UNAVAILABLE);
        ASSERT_LE(timer.ElapsedMilliSecondAndReset(), 1000);
    }
    LOG(INFO) << "test case finished";
    cluster_->StartNode(ClusterNodeType::MASTER, 0, {});
    cluster_->WaitNodeReady(ClusterNodeType::MASTER, 0);
}

TEST_F(WorkerDfxTest, TestRemoteGetRpcUnavailableRetry)
{
    FLAGS_v = 1;
    LOG(INFO) << "Test remote get from a busy worker. Local worker should retry";
    InitTestClients();
    std::string objKey = "Warriors";
    CreateParam param;
    param.consistencyType = ConsistencyType::CAUSAL;
    uint64_t objSize = 500 * 1024ul;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(objClient0_->Create(objKey, objSize, param, buffer));
    DS_ASSERT_OK(buffer->Publish());
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure", "1*return(K_RPC_UNAVAILABLE)"));

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(objClient2_->Get({ objKey }, 5000, buffers2));
}

class WorkerReconciliationDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.waitWorkerReady = false;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -client_reconnect_wait_s=5 -node_timeout_s=5";
        opts.disableRocksDB = false;
    }

    void TearDown() override
    {
        for (auto &client : clients_) {
            client.reset();
        }
        utSvcStub0_.reset();
        utSvcStub1_.reset();
        aksk_.reset();
        db_.reset();
        ExternalClusterTest::TearDown();
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitEtcd();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < WORKER_NUM; ++i) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        GetWorkerUuids();
        InitializeTest();
    }

    void InitEtcd()
    {
        std::pair<HostPort, HostPort> addrs;
        DS_ASSERT_OK(cluster_->GetEtcdAddrs(0, addrs));
        etcdAddress_ = addrs.first;
        db_ = std::make_shared<EtcdStore>(etcdAddress_.ToString());
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(ETCD_RING_PREFIX);
            (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
        }
    }

    void GetWorkerUuids()
    {
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ring.ParseFromString(value);
        for (auto worker : ring.workers()) {
            HostPort workerAddr;
            DS_ASSERT_OK(workerAddr.ParseString(worker.first));
            uuidMap_.emplace(std::move(workerAddr), worker.second.worker_uuid());
        }
    }

    void InitializeTest()
    {
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr0_));
        DS_ASSERT_OK(cluster_->GetWorkerAddr(1, workerAddr1_));
        InitObjectClients();
        InitApis();
    }

    void InitObjectClients()
    {
        clients_.resize(CLIENT_NUM);
        int idx = 0;
        InitTestClient(0, clients_[idx++]);
        InitTestClient(0, clients_[idx++]);
        InitTestClient(1, clients_[idx++]);
        InitTestClient(1, clients_[idx++]);
    }

    void InitApis()
    {
        HostPort fakeLocalAddr;
        fakeLocalAddr.ParseString("127.0.0.1:30000");
        aksk_ = std::make_shared<AkSkManager>();
        DS_ASSERT_OK(aksk_->SetClientAkSk(ak_, sk_));
        RpcCredential cred;
        RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred);
        auto channel = std::make_shared<RpcChannel>(workerAddr0_, cred);
        utSvcStub0_ = std::make_unique<UtOCService_Stub>(channel);
        channel = std::make_shared<RpcChannel>(workerAddr1_, cred);
        utSvcStub1_ = std::make_unique<UtOCService_Stub>(channel);
    }

    void PutObjGIncreaseRef()
    {
        size_t sz = 1024;
        std::string data = RandomData().GetRandomString(sz);
        std::vector<std::string> failedIds;
        std::vector<std::string> objKeys(CLIENT_NUM);
        // obj0 by client0, primary copy on node0, meta on node0
        objKeys[0] = GetStringUuid() + ";" + uuidMap_[workerAddr0_];
        // obj1 by client1, primary copy on node0, meta on node1
        objKeys[1] = GetStringUuid() + ";" + uuidMap_[workerAddr1_];
        // obj2 by client2, primary copy on node1, meta on node1
        objKeys[2] = GetStringUuid() + ";" + uuidMap_[workerAddr1_];
        // obj3 by client3, primary copy on node1, meta on node0
        objKeys[3] = GetStringUuid() + ";" + uuidMap_[workerAddr0_];
        for (size_t i = 0; i < CLIENT_NUM; ++i) {
            DS_ASSERT_OK(
                clients_[i]->Put(objKeys[i], reinterpret_cast<uint8_t *>(&data.front()), sz, CreateParam(), {}));
            DS_ASSERT_OK(clients_[i]->GIncreaseRef({ objKeys[i] }, failedIds));
            ASSERT_TRUE(failedIds.empty());
        }

        GRefTableReqPb req;
        GRefTableRspPb rsp;
        DS_ASSERT_OK(aksk_->GenerateSignature(req));
        // worker0's table should have two clients and two objects
        DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
        // master0's table should have two workers and two objects
        rsp.clear_single_client_gref();
        DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
        // worker1's table should have two clients and two objects
        rsp.clear_single_client_gref();
        DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
        // master1's table should have two workers and two objects
        rsp.clear_single_client_gref();
        DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
        ASSERT_EQ(rsp.single_client_gref().size(), 2);
    }

protected:
    std::vector<std::shared_ptr<ObjectClient>> clients_;
    std::unique_ptr<UtOCService_Stub> utSvcStub0_;
    std::unique_ptr<UtOCService_Stub> utSvcStub1_;
    std::shared_ptr<AkSkManager> aksk_;
    std::shared_ptr<EtcdStore> db_;
    HostPort workerAddr0_;
    HostPort workerAddr1_;
    HostPort etcdAddress_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::string ak_ = "QTWAOYTTINDUT2QVKYUC";
    std::string sk_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    const size_t CLIENT_NUM = 4;
    const size_t WORKER_NUM = 2;
    RpcAuthKeys authKeys_;
};

TEST_F(WorkerReconciliationDfxTest, LEVEL2_ClientExitDuringWorkerRestart1)
{
    PutObjGIncreaseRef();

    // shutdown node1 and client2
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[2].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart2)
{
    PutObjGIncreaseRef();

    // shutdown node1 and client3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart3)
{
    PutObjGIncreaseRef();

    // shutdown node1 and client2 and client3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[2].reset();
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have no client and no object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    // master1's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart4)
{
    PutObjGIncreaseRef();
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    for (auto &client : clients_) {
        client.reset();
    }
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    // shutdown all nodes and clients
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));

    // restart node0 first and then node1
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // all masters and workers should have no gref
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_ClientExitDuringWorkerRestart5)
{
    // similar to LEVEL1_ClientExitDuringWorkerRestart4 but restart nodes in different order
    PutObjGIncreaseRef();
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    for (size_t i = 0; i < CLIENT_NUM; ++i) {
        clients_[i].reset();
    }
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    // shutdown all nodes and clients
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));

    // restart node1 first and then node0
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // all masters and workers should have no gref
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 0);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL1_GiveUpReconciliation)
{
    PutObjGIncreaseRef();

    // shutdown node1 and client3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);

    cluster_->SetInjectAction(WORKER, 0, "EtcdClusterManager.IfNeedTriggerReconciliation.noreconciliation",
                              "return(K_OK)");

    // restart node1
    DS_ASSERT_OK(cluster_->StartNode(
        WORKER, 1, " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(1000)"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL2_ClientExitDuringWorkerNetworkIssue)
{
    PutObjGIncreaseRef();

    // node1 suffers from network issue and client3 exits
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(10000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.gdecrease", "1*return(K_RPC_UNAVAILABLE)"));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    clients_[3].reset();

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    // master0 and worker0 should be good
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);

    // node1 recovers
    int waitTimeSec = 30;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, LEVEL2_ClientExitDuringWorkerNetworkIssueAndRestart)
{
    PutObjGIncreaseRef();

    // shutdown worker 0
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // node1 suffers from network issue and client3 exits
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(6000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.gdecrease", "1*return(K_RPC_UNAVAILABLE)"));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    clients_[3].reset();

    // node1 recovers
    int waitTimeSec = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    // restart worker0
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    // wait for reconciliation between restarted master and all workers
    waitTimeSec = 10;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    GRefTableReqPb req;
    GRefTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));

    // worker1 should reconcile with master0 and master1
    // worker0's table should have two clients and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
    // master0's table should have one worker and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub0_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // worker1's table should have one client and one object
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetWorkerGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 1);
    // master1's table should have two workers and two objects
    rsp.clear_single_client_gref();
    DS_ASSERT_OK(utSvcStub1_->GetMasterGRefTable(req, rsp));
    ASSERT_EQ(rsp.single_client_gref().size(), 2);
}

TEST_F(WorkerReconciliationDfxTest, DISABLED_LEVEL1_RestartAgainNoExtraReconciliation)
{
    PutObjGIncreaseRef();
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.PreShutDown.skip", "return(K_OK)");
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    inject::Set("ObjectClientImpl.ProcessWorkerLost", "call()");
    // restart the whole cluster
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0,
                                     " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call("
                                     "5000);worker.PreShutDown.skip:return(K_OK)"));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-inject_actions=worker.PreShutDown.skip:return(K_OK)"));

    // cluster node table should not contain "restart" tag
    auto waitSwitch = [](uint64_t num) {
        auto sec5s = 5;
        Timer t;
        do {
            if (inject::GetExecuteCount("ObjectClientImpl.ProcessWorkerLost") == num) {
                inject::Clear("ObjectClientImpl.ProcessWorkerLost");
                return;
            }
            auto ms100 = 100;
            std::this_thread::sleep_for(std::chrono::milliseconds(ms100));
        } while (t.ElapsedSecond() < sec5s);
        LOG(ERROR) << "wait switch timeout";
    };
    auto isAllNodeReady = [this](std::unique_ptr<UtOCService_Stub> &stub) -> Status {
        auto request = [this](std::unique_ptr<UtOCService_Stub> &stub) -> Status {
            CmNodeTableReqPb req;
            CmNodeTableRspPb rsp;
            RETURN_IF_NOT_OK(aksk_->GenerateSignature(req));
            stub->GetCmNodeTable(req, rsp);
            auto nodeSize = 2;
            CHECK_FAIL_RETURN_STATUS(rsp.cm_node_table_size() == nodeSize, K_RUNTIME_ERROR,
                                     "node size not equal");  // The number of worker is 2
            CHECK_FAIL_RETURN_STATUS(rsp.cm_node_table().at(0).addition_event_type() == "ready", K_RUNTIME_ERROR,
                                     "node 0 is not ready");
            CHECK_FAIL_RETURN_STATUS(rsp.cm_node_table().at(1).addition_event_type() == "ready", K_RUNTIME_ERROR,
                                     "node 0 is not ready");
            return Status::OK();
        };
        int i = 1, repeatTime = 100;
        Status res;
        while (true) {
            res = request(stub);
            if (res.IsOk()) {
                LOG(INFO) << "st case isAllNodeReady func try times:" << i;
                return res;
            }
            if (i > repeatTime) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            i++;
        }
        return res;
    };
    auto waitCallNum4 = 4;
    waitSwitch(waitCallNum4);
    DS_ASSERT_OK(isAllNodeReady(utSvcStub0_));
    DS_ASSERT_OK(isAllNodeReady(utSvcStub1_));
    inject::Set("ObjectClientImpl.ProcessWorkerLost", "call()");
    // restart node 1
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, "-inject_actions=worker.PreShutDown.skip:return(K_OK)"));
    auto waitCallNum2 = 2;
    waitSwitch(waitCallNum2);
    DS_ASSERT_OK(isAllNodeReady(utSvcStub0_));
    DS_ASSERT_OK(isAllNodeReady(utSvcStub1_));
}

TEST_F(WorkerReconciliationDfxTest, LEVEL2_RecoverAgainNoExtraReconciliation)
{
    PutObjGIncreaseRef();

    // the whole cluster recovers from network issue
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(7000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(7000)"));
    int waitTimeSec = 15;
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    // cluster node table should not contain "recover" tag
    CmNodeTableReqPb req;
    CmNodeTableRspPb rsp;
    DS_ASSERT_OK(aksk_->GenerateSignature(req));
    utSvcStub0_->GetCmNodeTable(req, rsp);
    ASSERT_EQ(rsp.cm_node_table_size(), 2);  // The number of worker is 2
    ASSERT_EQ(rsp.cm_node_table().at(0).addition_event_type(), "ready");
    ASSERT_EQ(rsp.cm_node_table().at(1).addition_event_type(), "ready");

    rsp.clear_cm_node_table();
    utSvcStub1_->GetCmNodeTable(req, rsp);
    ASSERT_EQ(rsp.cm_node_table_size(), 2);  // The number of worker is 2
    ASSERT_EQ(rsp.cm_node_table().at(0).addition_event_type(), "ready");
    ASSERT_EQ(rsp.cm_node_table().at(1).addition_event_type(), "ready");

    // disconnect node 1 again
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "heartbeat.sleep", "1*sleep(7000)"));
    std::this_thread::sleep_for(std::chrono::seconds(waitTimeSec));

    rsp.clear_cm_node_table();
    utSvcStub0_->GetCmNodeTable(req, rsp);
    ASSERT_EQ(rsp.cm_node_table_size(), 2);  // The number of worker is 2
    ASSERT_EQ(rsp.cm_node_table().at(0).addition_event_type(), "ready");
    ASSERT_EQ(rsp.cm_node_table().at(1).addition_event_type(), "ready");

    rsp.clear_cm_node_table();
    utSvcStub1_->GetCmNodeTable(req, rsp);
    ASSERT_EQ(rsp.cm_node_table_size(), 2);  // The number of worker is 2
    ASSERT_EQ(rsp.cm_node_table().at(0).addition_event_type(), "ready");
    ASSERT_EQ(rsp.cm_node_table().at(1).addition_event_type(), "ready");
}

class StandbyWorkerDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t workerNum = 3;
        opts.numWorkers = workerNum;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -v=1 -add_node_wait_time_s=3 -auto_del_dead_node=true -node_timeout_s=1"
            " -node_dead_timeout_s=3 -heartbeat_interval_ms=500";
        opts.waitWorkerReady = false;
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.interval", "call(500)");
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
        datasystem::inject::Set("ClientWorkerCommonApi.SendHeartbeat.timeoutMs", "call(500)");
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
        client2_.reset();
        ExternalClusterTest::TearDown();
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes)
    {
        for (auto i : indexes) {
            DS_ASSERT_OK(externalCluster_->StartWorker(i, HostPort()));
        }
        for (auto i : indexes) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
    }

    void InitClients()
    {
        uint32_t idx = 0;
        InitClient(idx++, client_, true);
        InitClient(idx++, client1_, true);
        InitClient(idx++, client2_, true);
    }

    void InitClient(int workerIndex, std::shared_ptr<ObjectClient> &client, bool enableCrossNodeConnection)
    {
        InitTestClient(workerIndex, client, [enableCrossNodeConnection](ConnectOptions &opts) {
            opts.enableCrossNodeConnection = enableCrossNodeConnection;
            opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
            opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        });
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::shared_ptr<ObjectClient> client_;
    std::shared_ptr<ObjectClient> client1_;
    std::shared_ptr<ObjectClient> client2_;
};

TEST_F(StandbyWorkerDfxTest, DISABLED_TestUseStandbyWorker)
{
    LOG(INFO) << "Test worker fault and use standby worker.";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    // Fault the worker and try to use standby worker.
    cluster_->ShutdownNode(WORKER, 0);
    sleep(5);

    std::string objKey = "obj1";
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    // Client will use non-shm because current worker is not local worker.
    DS_ASSERT_OK(
        client_->Put(objKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client_->Get({ objKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);

    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client2_->Get({ objKey }, 0, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_BUF_EQ((*buffers2[0]), data);

    // Restore the local worker
    StartWorkerAndWaitReady({ 0 });
    sleep(5);  // sleep more than add_node_wait_time_s
    std::string objKey2 = "obj2";
    std::string data2(objSize, 'b');
    // Client will use shm because local worker is restored.
    DS_ASSERT_OK(client_->Put(objKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data2.data())), data2.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client_->Get({ objKey2 }, 0, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_BUF_EQ((*buffers3[0]), data2);

    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client_->Get({ objKey }, 0, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_BUF_EQ((*buffers4[0]), data);
}

TEST_F(StandbyWorkerDfxTest, DISABLED_LEVEL1_TestStandbyWorkerFault)
{
    LOG(INFO) << "Test standby worker fault and use standby2 worker.";
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();
    // Fault the worker and try to use standby worker.
    cluster_->ShutdownNode(WORKER, 0);
    sleep(5);

    std::string objKey = "obj1";
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    DS_ASSERT_OK(
        client_->Put(objKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client_->Get({ objKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);

    // Fault the standby worker and try to use standby2 worker.
    cluster_->ShutdownNode(WORKER, 1);
    sleep(5);

    std::string objKey2 = "obj2";
    std::string data2(objSize, 'b');
    DS_ASSERT_OK(client_->Put(objKey2, reinterpret_cast<uint8_t *>(const_cast<char *>(data2.data())), data2.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers2;
    DS_ASSERT_OK(client_->Get({ objKey2 }, 0, buffers2));
    ASSERT_TRUE(NotExistsNone(buffers2));
    ASSERT_BUF_EQ((*buffers2[0]), data2);

    std::vector<Optional<Buffer>> buffers3;
    DS_ASSERT_OK(client1_->Get({ objKey2 }, 0, buffers3));
    ASSERT_TRUE(NotExistsNone(buffers3));
    ASSERT_BUF_EQ((*buffers3[0]), data2);

    std::vector<Optional<Buffer>> buffers4;
    DS_ASSERT_OK(client2_->Get({ objKey2 }, 0, buffers4));
    ASSERT_TRUE(NotExistsNone(buffers4));
    ASSERT_BUF_EQ((*buffers4[0]), data2);
}

TEST_F(StandbyWorkerDfxTest, LEVEL1_TestEnableCrossNodeConnectionParam)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    // Not supported standby node.
    InitTestClient(0, client0, [](ConnectOptions &opts) {
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    // Supported standby node.
    InitTestClient(0, client1, [](ConnectOptions &opts) {
        opts.enableCrossNodeConnection = true;
        opts.accessKey = "QTWAOYTTINDUT2QVKYUC";
        opts.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    });
    cluster_->ShutdownNode(WORKER, 0);
    sleep(5);
    std::string objectKey = "obj1";
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    DS_ASSERT_NOT_OK(client0->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                                  CreateParam{}));
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);
}

class WorkerWithoutReconciliationTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams =
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=1 -enable_reconciliation=false "
            "-heartbeat_interval_ms=500";
        auto numWorkers = 2;
        opts.numWorkers = numWorkers;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        opts.disableRocksDB = false;
    }
};

TEST_F(WorkerWithoutReconciliationTest, LEVEL2_WithoutReconciliationTest)
{
    std::shared_ptr<ObjectClient> client0;
    InitTestClient(0, client0);
    std::string objectKey = GetStringUuid();
    size_t objSize = 500 * 1024ul;
    std::string data(objSize, 'a');
    std::vector<std::string> failedObjects;

    DS_ASSERT_OK(client0->GIncreaseRef({ objectKey }, failedObjects));
    DS_ASSERT_OK(client0->Put(objectKey, reinterpret_cast<uint8_t *>(const_cast<char *>(data.data())), data.size(),
                              CreateParam{}));
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client0->Get({ objectKey }, 0, buffers1));
    ASSERT_TRUE(NotExistsNone(buffers1));
    ASSERT_BUF_EQ((*buffers1[0]), data);

    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(ClusterNodeType::WORKER, 0, {}));
    DS_ASSERT_OK(cluster_->WaitNodeReady(ClusterNodeType::WORKER, 0));
    const size_t sleepTime = 3;
    std::this_thread::sleep_for(std::chrono::seconds(sleepTime));

    buffers1.clear();
    Status rc = client0->Get({ objectKey }, 0, buffers1);
    LOG(INFO) << "The status of get the object after node0(master and worker) restart: " << rc.ToString();
    ASSERT_TRUE(rc.GetCode() == StatusCode::K_RUNTIME_ERROR);
}

class WorkerPushMetaTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.workerGflagParams = FormatString(
            "-client_reconnect_wait_s=1 -ipc_through_shared_memory=true -node_timeout_s=2 -node_dead_timeout_s=%d "
            "-heartbeat_interval_ms=1000 -auto_del_dead_node=false",
            nodeDeadTimeout_);
        auto numWorkers = 2;
        opts.numWorkers = numWorkers;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
    }

protected:
    int nodeDeadTimeout_ = 3;
};

TEST_F(WorkerPushMetaTest, TestWorkerTimeoutAndPushMeta)
{
    std::shared_ptr<ObjectClient> client0;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client0);
    InitTestClient(1, client1);

    size_t num = 10;
    std::vector<uint8_t> data;
    size_t dataSize = 500 * 1024ul;
    data.resize(dataSize);
    std::vector<std::string> objKeys;
    std::vector<std::shared_ptr<Buffer>> buffers(num);
    for (size_t i = 0; i < num; ++i) {
        std::string objKey = "BigEmperor" + std::to_string(i);
        objKeys.emplace_back(objKey);
        CreateParam param;
        DS_ASSERT_OK(client0->Create(objKey, data.size(), param, buffers[i]));
        DS_ASSERT_OK(buffers[i]->MemoryCopy(data.data(), data.size()));
        DS_ASSERT_OK(buffers[i]->Publish());
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client0->GIncreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));

    // Let hearbeat timeout and master will clear its metadata, then when worker reconnect,
    // master will ask us to send the metadata to master.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RunKeepAliveTask", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.KeepAlive.send", "pause()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdStore.LaunchKeepAliveThreads.loopQuickly", "call(0)"));

    std::this_thread::sleep_for(std::chrono::seconds(nodeDeadTimeout_ + 1));
    cluster_->ClearInjectAction(WORKER, 0, "worker.RunKeepAliveTask");
    cluster_->ClearInjectAction(WORKER, 0, "worker.KeepAlive.send");
    std::this_thread::sleep_for(std::chrono::seconds(nodeDeadTimeout_));

    for (size_t i = 0; i < num; ++i) {
        DS_ASSERT_OK(buffers[i]->Seal());
    }

    std::vector<Optional<Buffer>> getBuffers;
    const int timeoutMs = 30'000;
    DS_ASSERT_OK(client1->Get(objKeys, timeoutMs, getBuffers));
    ASSERT_TRUE(NotExistsNone(getBuffers));
    DS_ASSERT_OK(client0->GDecreaseRef(objKeys, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

class WorkerKillTest : public OCClientCommon {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerNum = 3;
        opts.numWorkers = workerNum;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.waitWorkerReady = false;
        const std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, uint32_t masterIdx = 0,
                                 uint32_t maxWaitTimeSec = 20)
    {
        HostPort master;
        ASSERT_LE(masterIdx, workerAddress_.size());
        DS_ASSERT_OK(master.ParseString(workerAddress_[masterIdx]));
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, master).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::string> workerAddress_;
};

TEST_F(WorkerKillTest, LEVEL1_GetAfterGDecrease)
{
    const int workerIdx2 = 2;
    StartWorkerAndWaitReady({ 0, 1, workerIdx2 }, 1);
    const int waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    const int connectTimeout = 10000;
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client, connectTimeout);
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(1, client1, connectTimeout);
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(workerIdx2, client2, connectTimeout);

    std::string objectKey = NewObjectKey();
    size_t size = 600 * 1024;
    std::string data = GenRandomString(size);
    std::shared_ptr<Buffer> buffer;
    CreateParam param{ .consistencyType = ConsistencyType::PRAM };
    DS_ASSERT_OK(client1->Create(objectKey, size, param, buffer));
    buffer->MemoryCopy((void *)data.data(), size);
    buffer->Publish();

    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers));

    DS_ASSERT_OK(externalCluster_->KillWorker(workerIdx2));

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_NOT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));

    StartWorkerAndWaitReady({ workerIdx2 }, 1);

    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    failedObjectKeys.clear();
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));

    DS_ASSERT_NOT_OK(client->Get({ objectKey }, 0, buffers));
}

TEST_F(WorkerKillTest, DISABLED_ClearPrimaryAfterWorkerRestart)
{
    const int workerIdx2 = 2;
    StartWorkerAndWaitReady({ 0, 1, workerIdx2 });
    const int waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    std::shared_ptr<ObjectClient> client1, client2;
    const int timeout = 5'000;
    InitTestClient(1, client1, timeout);
    InitTestClient(workerIdx2, client2, timeout);
    std::string objectKey = "obj1";
    int64_t dataSize = 10;
    std::string value1 = GenRandomString(dataSize);
    CreateParam param = {};
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(value1.data()), value1.size(), param));
    sleep(1);
    DS_ASSERT_OK(externalCluster_->KillWorker(1));

    ThreadPool threadPool(1);
    auto fut = threadPool.Submit([&]() {
        std::vector<Optional<Buffer>> buffers;
        ASSERT_EQ(client2->Get({ objectKey }, timeout, buffers).GetCode(), StatusCode::K_RUNTIME_ERROR);
    });

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.RemoveMetaByWorker.delay", "sleep(2000)"));
    StartWorkerAndWaitReady({ 1 });

    fut.get();
}

class ClientDiskCacheDfxTest : public OCClientCommon {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            opts.workerSpecifyGflagParams[i] += FormatString(" -shared_disk_directory=%s -shared_disk_size_mb=32", dir);
        }
    }
};

TEST_F(ClientDiskCacheDfxTest, ClientCrash)
{
    std::vector<std::shared_ptr<ObjectClient>> clients;
    int num = 3;
    clients.resize(num);
    for (int i = 0; i < num; i++) {
        InitTestClient(0, clients[i]);
    }

    std::string val = GenRandomString(1000 * 1024);  // 1000 KB
    CreateParam param;
    param.cacheType = CacheType::DISK;
    for (int i = 0; i < num; i++) {
        int objectsNum = 10;
        for (int j = 0; j < objectsNum; j++) {
            std::string objectKey = GenRandomString();
            std::vector<std::string> failedObjectKeys;
            DS_ASSERT_OK(clients[i]->GIncreaseRef({ objectKey }, failedObjectKeys));
            DS_ASSERT_OK(clients[i]->Put(objectKey, reinterpret_cast<const uint8_t *>(val.data()), val.size(), param));
        }
    }

    clients[0].reset();
    clients[num - 1].reset();

    int objectsNum = 20;
    for (int j = 0; j < objectsNum; j++) {
        std::string objectKey = GenRandomString();
        DS_ASSERT_OK(clients[1]->Put(objectKey, reinterpret_cast<const uint8_t *>(val.data()), val.size(), param));
    }
}
}  // namespace st
}  // namespace datasystem

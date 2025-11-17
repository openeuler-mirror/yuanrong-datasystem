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
 * Description: Object client multi replica tests.
 */
 
#include <cstddef>
#include <map>
#include <string>
#include <thread>
#include <vector>
 
#include <gtest/gtest.h>
#include <unistd.h>
 
#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "common_distributed_ext.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/common/log/log.h"
#include "datasystem/object_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
 
DS_DECLARE_string(etcd_address);
namespace datasystem {
namespace st {

class ObjectClientReplicaTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            " -v=1 -node_timeout_s=3 -enable_meta_replica=true -inject_actions=test.start.notWait:call(1)";
        opts.waitWorkerReady = false;
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
        ResetClients();
        ExternalClusterTest::TearDown();
    }
    
    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void StartWorkerAndWaitReady(std::initializer_list<int> indexes,
                                 const std::unordered_map<int, std::string> &workerFlags = {}, int maxWaitTimeSec = 20)
    {
        for (auto i : indexes) {
            std::string flags;
            auto iter = workerFlags.find(i);
            if (iter != workerFlags.end()) {
                flags = " " + iter->second;
            }
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort(), flags).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
        for (auto i : indexes) {
            // When the scale-in scenario is tested, the scale-in failure may not be determined correctly.
            // Therefore, the scale-in failure is directly exited.
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "Hashring.Scaletask.Fail", "abort()"));
        }
        InitWorkersInfoMap(indexes);
    }
 
    void StartWorkerAndWaitReady(std::initializer_list<int> indexes, const std::string &flags, int maxWaitTimeSec = 20)
    {
        std::unordered_map<int, std::string> workerFlags;
        for (auto i : indexes) {
            workerFlags.emplace(i, flags);
        }
        StartWorkerAndWaitReady(indexes, workerFlags, maxWaitTimeSec);
    }

    void RestartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int signal = SIGTERM)
    {
        for (auto i : indexes) {
            if (signal == SIGTERM) {
                ASSERT_TRUE(cluster_->ShutdownNode(WORKER, i).IsOk()) << i;
            } else {
                ASSERT_TRUE(externalCluster_->KillWorker(i).IsOk()) << i;
            }
        }

        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->StartNode(WORKER, i, "").IsOk()) << i;
        }
        int maxWaitTimeSec = 40;  // need reconliation
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }
 
    void InitClients(int count)
    {
        for (int i = 0; i < count; i++) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(i, client);
            clients_.emplace_back(std::move(client));
        }
    }
 
    void InitClients(std::vector<int> indexs)
    {
        for (auto index : indexs) {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(index, client);
            clients_.emplace_back(std::move(client));
        }
    }

    void ResetClients()
    {
        clients_.clear();
    }
 
    std::vector<std::vector<std::string>> RandomGroup(std::vector<std::string> keys, int count)
    {
        if (count == 0) {
            return {};
        }
        std::vector<std::vector<std::string>> groupKeys;
        std::mt19937 gen(std::random_device{}());
        std::shuffle(std::begin(keys), std::end(keys), gen);
        size_t groupCount = keys.size() / count;
        if (keys.size() % count != 0) {
            groupCount++;
        }
        groupKeys.resize(groupCount);
        for (size_t i = 0; i < keys.size(); i++) {
            const auto &key = keys[i];
            groupKeys[i / count].emplace_back(key);
        }
        return groupKeys;
    }
 
    void GenerateKeyValues(std::vector<std::string> &keys, std::vector<std::string> &values, int num, int valSize)
    {
        auto keySize = 20;
        for (int i = 0; i < num; i++) {
            keys.emplace_back(randomData_.GetRandomString(keySize));
            values.emplace_back(randomData_.GetRandomString(valSize));
        }
    }
 
protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::vector<std::shared_ptr<ObjectClient>> clients_;
    std::unordered_map<uint32_t, std::string> workerUuids_;
    std::unique_ptr<EtcdStore> etcd_;
    const int SHM_SIZE = 500 * 1024;
    const int objNum = 50;  // objNum is 50
    int idx0 = 0;
    int idx1 = 1;
    int idx2 = 2;
};
 
TEST_F(ObjectClientReplicaTest, LEVEL1_TestObjectPutGetHash)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    std::vector<std::string> ids, vals;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = NewObjectKey();
        ids.emplace_back(objectKey);
        std::string data = GenRandomString(10);
        vals.emplace_back(data);
    }
 
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(clients_[idx0]->GIncreaseRef(ids, failedObjectKeys));
    for (int i = 0; i < objNum; i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()),
                                         vals[i].size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(clients_[idx0]->Get({ ids[i] }, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[idx0], vals[i]);
    }
    DS_ASSERT_OK(clients_[idx0]->GDecreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(clients_[idx0]->Get(ids, 0, buffers));
}
 
TEST_F(ObjectClientReplicaTest, TestObjectPutGetHashRemote)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    std::vector<std::string> ids, vals;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = NewObjectKey();
        ids.emplace_back(objectKey);
        std::string data = GenRandomString(10);
        vals.emplace_back(data);
    }
 
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(clients_[idx0]->GIncreaseRef(ids, failedObjectKeys));
    for (int i = 0; i < objNum; i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()),
                                         vals[i].size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(clients_[idx2]->Get({ ids[i] }, 0, buffers));  // index 2
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[idx0], vals[i]);
    }
    DS_ASSERT_OK(clients_[idx0]->GDecreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(clients_[idx1]->Get(ids, 0, buffers));
}
 
TEST_F(ObjectClientReplicaTest, TestObjectPutGetWorkerId)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    std::vector<std::string> ids, vals;
    int keyNum = 30;
    for (int i = 0; i < keyNum; i++) {
        for (size_t i = 0; i < clients_.size(); i++) {
            std::string objectKey;
            DS_ASSERT_OK(clients_[i]->GenerateObjectKey("", objectKey));
            ids.emplace_back(objectKey);
            std::string data = GenRandomString(10);
            vals.emplace_back(data);
        }
    }
 
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(clients_[idx0]->GIncreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    for (size_t i = 0; i < ids.size(); i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()),
                                         vals[i].size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(clients_[idx0]->Get({ ids[i] }, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[idx0], vals[i]);
    }
    failedObjectKeys.clear();
    DS_ASSERT_OK(clients_[idx0]->GDecreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(clients_[idx0]->Get(ids, 0, buffers));
}
 
TEST_F(ObjectClientReplicaTest, TestObjectPutGetWorkerIdRemote)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    std::vector<std::string> ids, vals;
    for (int i = 0; i < objNum; i++) {
        for (size_t i = 0; i < clients_.size(); i++) {
            std::string objectKey;
            DS_ASSERT_OK(clients_[i]->GenerateObjectKey("", objectKey));
            ids.emplace_back(objectKey);
            std::string data = GenRandomString(10);
            vals.emplace_back(data);
        }
    }
 
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(clients_[idx1]->GIncreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    for (int i = 0; i < objNum; i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()),
                                         vals[i].size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(clients_[idx1]->Get({ ids[i] }, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[idx0], vals[i]);
    }
    failedObjectKeys.clear();
    DS_ASSERT_OK(clients_[idx1]->GDecreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(clients_[idx2]->Get(ids, 0, buffers));
}
 
TEST_F(ObjectClientReplicaTest, TestObjectPutGetWorkerIdAndHash)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    sleep(1);  // wait for worker update replicas finish.
    InitClients({ 0, 1, 2 });
    std::vector<std::string> ids, vals;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = NewObjectKey();
        ids.emplace_back(objectKey);
        std::string data = GenRandomString(10);
        vals.emplace_back(data);
    }
    int num = 10;  // a batch of key is 10;
    for (int i = 0; i < num; i++) {
        for (size_t i = 0; i < clients_.size(); i++) {
            std::string objectKey;
            DS_ASSERT_OK(clients_[i]->GenerateObjectKey("", objectKey));
            ids.emplace_back(objectKey);
            std::string data = GenRandomString(10);
            vals.emplace_back(data);
        }
    }
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(clients_[idx0]->GIncreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    for (size_t i = 0; i < ids.size(); i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()),
                                         vals[i].size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers;
        DS_ASSERT_OK(clients_[idx0]->Get({ ids[i] }, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[idx0], vals[i]);
    }
    failedObjectKeys.clear();
    DS_ASSERT_OK(clients_[idx0]->GDecreaseRef(ids, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(clients_[idx0]->Get(ids, 0, buffers));
}
 
TEST_F(ObjectClientReplicaTest, TestBuffer)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    std::string objectKey = NewObjectKey();
    std::shared_ptr<Buffer> buffer;
    std::shared_ptr<ObjectClient> client;
    std::shared_ptr<ObjectClient> client1;
    InitTestClient(0, client);
    InitTestClient(1, client1);
    size_t size = 500 * 1024;
    std::string data = GenRandomString(size);
 
    DS_ASSERT_OK(client->Create(objectKey, size, CreateParam{}, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(size, buffer.get()->GetSize());
    std::vector<std::string> objectKeys{ objectKey };
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef(objectKeys, failedObjectKeys));
    buffer->WLatch();
    buffer->MemoryCopy(data.data(), size);
    buffer->Publish();
    buffer->UnWLatch();
 
    std::vector<Optional<Buffer>> buffers;
    std::vector<Optional<Buffer>> buffer1;
    DS_ASSERT_OK(client->Get(objectKeys, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    ASSERT_EQ(buffers.size(), objectKeys.size());
    ASSERT_EQ(buffers[idx0]->GetSize(), size);
    buffers[idx0]->RLatch();
    AssertBufferEqual(*buffers[idx0], data);
    buffers[idx0]->UnRLatch();
 
    DS_ASSERT_OK(client1->Get(objectKeys, 0, buffer1));
    ASSERT_TRUE(NotExistsNone(buffer1));
    DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
    ASSERT_EQ(buffer1.size(), objectKeys.size());
    ASSERT_EQ(buffer1[idx0]->GetSize(), size);
    buffer1[idx0]->RLatch();
    AssertBufferEqual(*buffer1[idx0], data);
    buffer1[idx0]->UnRLatch();
    DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
 
    DS_ASSERT_OK(client->GDecreaseRef(objectKeys, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());
}
 
class ObjUpdateToReplicaTest : public ObjectClientReplicaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 ";
        opts.waitWorkerReady = false;
        opts.disableRocksDB = false;
    }
 
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
};
 
class ObjReplicaScaleUpTest : public ObjectClientReplicaTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 5;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=1 -node_timeout_s=3 -enable_meta_replica=true";
        opts.waitWorkerReady = false;
        opts.addNodeTime = 1;
    }
 
    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
};
 
TEST_F(ObjReplicaScaleUpTest, DISABLED_LEVEL1_TestScaleUpRedirect)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    for (int i = 0; i < 3; i++) {  // worker num is 3
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MigrateByRanges.Delay", "1*sleep(6000)"));
    }
    std::shared_ptr<ObjectClient> client;
    std::vector<std::string> ids, vals;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = NewObjectKey();
        ids.emplace_back(objectKey);
        std::string data = GenRandomString(10);
        vals.emplace_back(data);
    }
    int keyNum = 20;
    for (int i = 0; i < keyNum; i++) {
        for (size_t i = 0; i < clients_.size(); i++) {
            std::string objectKey;
            DS_ASSERT_OK(clients_[i]->GenerateObjectKey("", objectKey));
            ids.emplace_back(objectKey);
            std::string data = GenRandomString(10);
            vals.emplace_back(data);
        }
    }
    for (size_t i = 0; i < ids.size(); i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()), vals[i].size(), CreateParam{}));
    }
    StartWorkerAndWaitReady({ 3 }, FormatString(" -v=2"));
    InitTestClient(3, client);  // worker idx 3
    std::thread thread1([&ids, vals, &client] {
        std::this_thread::sleep_for(std::chrono::seconds(2));  // wait time 2 s
        for (size_t i = 0; i < ids.size(); i++) {
            std::vector<Optional<Buffer>> buffers;
            DS_ASSERT_OK(client->Get({ ids[i] }, 0, buffers));
            std::vector<std::string> failedIds;
            DS_ASSERT_OK(client->GIncreaseRef({ ids[i] }, failedIds));
            ASSERT_TRUE(NotExistsNone(buffers));
            AssertBufferEqual(*buffers[0], vals[i]);
            failedIds.clear();
            DS_ASSERT_OK(client->GDecreaseRef({ ids[i] }, failedIds));
            buffers.clear();
            DS_ASSERT_NOT_OK(client->Get({ ids[i] }, 0, buffers));
        }
    });
    thread1.join();
    WaitReplicaInCurrentNode(3);  // worker num 3
}
 
TEST_F(ObjReplicaScaleUpTest, DISABLED_LEVEL1_TestScaleUpRedirectRemoteClient)
{
    StartWorkerAndWaitReady({ 0, 1, 2 }, FormatString(" -v=2"));
    InitClients({ 0, 1, 2 });
    for (int i = 0; i < 3; i++) {  // worker idx 3
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MigrateByRanges.Delay", "1*sleep(6000)"));
    }
    std::shared_ptr<ObjectClient> client;
    std::vector<std::string> ids, vals;
    for (int i = 0; i < objNum; i++) {
        std::string objectKey = NewObjectKey();
        ids.emplace_back(objectKey);
        std::string data = GenRandomString(10);
        vals.emplace_back(data);
    }
    int keyNum = 20;
    for (int i = 0; i < keyNum; i++) {
        for (size_t i = 0; i < clients_.size(); i++) {
            std::string objectKey;
            DS_ASSERT_OK(clients_[i]->GenerateObjectKey("", objectKey));
            ids.emplace_back(objectKey);
            std::string data = GenRandomString(10);
            vals.emplace_back(data);
        }
    }
    for (size_t i = 0; i < ids.size(); i++) {
        DS_ASSERT_OK(clients_[idx0]->Put(ids[i], (uint8_t *)(vals[i].c_str()), vals[i].size(), CreateParam{}));
    }
    StartWorkerAndWaitReady({ 3 }, FormatString(" -v=2"));
    InitTestClient(3, client);  // worker idx 3
    std::thread thread1([&ids, vals, &client] {
        std::this_thread::sleep_for(std::chrono::seconds(2));  // wait time 2 s
        for (size_t i = 0; i < ids.size(); i++) {
            std::vector<Optional<Buffer>> buffers;
            DS_ASSERT_OK(client->Get({ ids[i] }, 0, buffers));
            std::vector<std::string> failedIds;
            DS_ASSERT_OK(client->GIncreaseRef({ ids[i] }, failedIds));
            ASSERT_TRUE(NotExistsNone(buffers));
            AssertBufferEqual(*buffers[0], vals[i]);
            failedIds.clear();
            DS_ASSERT_OK(client->GDecreaseRef({ ids[i] }, failedIds));
            buffers.clear();
            DS_ASSERT_NOT_OK(client->Get({ ids[i] }, 0, buffers));
        }
    });
    thread1.join();
    WaitReplicaInCurrentNode(3);  // worker num 3
}
}  // namespace st
}  // namespace datasystem
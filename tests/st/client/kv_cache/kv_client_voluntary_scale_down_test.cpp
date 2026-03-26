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
 * Description: state cache cross AZ test
 */

#include "cluster/external_cluster.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include <unistd.h>
#include "common.h"
#include "datasystem/utils/status.h"
#include "client/object_cache/oc_client_common.h"
#include "client/kv_cache/kv_client_scale_common.h"

#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

const std::string HOST_IP_PREFIX = "127.0.0.1";
constexpr size_t DEFAULT_WORKER_NUM = 4;
class KVClientVoluntaryScaleDownTest : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=2";

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void InitTestEtcdInstance()
    {
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        FLAGS_etcd_address = etcdAddress;
        LOG(INFO) << "The etcd address is:" << FLAGS_etcd_address << std::endl;
        db_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((db_ != nullptr) && (db_->Init().IsOk())) {
            db_->DropTable(ETCD_RING_PREFIX);
            // We don't check rc here. If table to drop does not exist, it's fine.
            (void)db_->CreateTable(ETCD_RING_PREFIX, ETCD_RING_PREFIX);
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX);
            (void)db_->CreateTable(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX);
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
        InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
        InitTestKVClient(2, client2_, 2000);  // Init client2 to worker 2 with 2000ms timeout
        InitTestKVClient(3, client3_, 2000);  // Init client2 to worker 3 with 2000ms timeout
        GetHashOnWorker();
        GetWorkerUuids();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        client3_.reset();
        db_.reset();
        ExternalClusterTest::TearDown();
    }

    void GetHashOnWorker(size_t workerNum = 4)
    {
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        for (size_t i = 0; i < workerNum; ++i) {
            auto tokens = ring.workers().at(workerAddress_[i]).hash_tokens();
            workerHashValue_.emplace_back(*tokens.begin() - 1);
        }
        ASSERT_EQ(workerHashValue_.size(), workerNum);
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

    void SetWorkerHashInjection(std::vector<uint32_t> injectNode = std::vector<uint32_t>{})
    {
        if (injectNode.size() == 0) {
            for (size_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
                DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
            }
            return;
        }

        for (auto i : injectNode) {
            DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
        }
    }

    void UnsetWorkerHashInjection(std::vector<uint32_t> injectNode = std::vector<uint32_t>{})
    {
        if (injectNode.size() == 0) {
            for (size_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
                DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "MurmurHash3"));
            }
            return;
        }

        for (auto i : injectNode) {
            DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "MurmurHash3"));
        }
    }

    void SetNormalObject(std::shared_ptr<KVClient> client, int workerIdx, std::vector<std::string> &objectKey,
                         std::vector<std::string> &data, WriteMode mode = WriteMode::NONE_L2_CACHE, int diffnum = 0)
    {
        for (uint32_t i = 0; i < objectKey.size(); ++i) {
            auto num = diffnum + i;
            objectKey[i] = "a_key_hash_to_" + std::to_string(workerHashValue_[workerIdx] - num);
            data[i] = randomData_.GetRandomString(10);  // Generate the data of length 10
            SetParam param{ .writeMode = mode };
            DS_ASSERT_OK(client->Set(objectKey[i], data[i], param));
        }
    }

    void SetUuidObject(std::shared_ptr<KVClient> client, int workerIdx, std::vector<std::string> &objectKey,
                       std::vector<std::string> &data, WriteMode mode = WriteMode::NONE_L2_CACHE)
    {
        HostPort workerHost;
        workerHost.ParseString(workerAddress_[workerIdx]);
        for (uint32_t i = 0; i < objectKey.size(); ++i) {
            objectKey[i] = NewObjectKey() + std::to_string(i) + ";" + uuidMap_[workerHost];
            data[i] = randomData_.GetRandomString(10);  // Generate the data of length 10
            SetParam param{ .writeMode = mode };
            DS_ASSERT_OK(client->Set(objectKey[i], data[i], param));
        }
    }

    void AssertWorkerNum(int num)
    {
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        ASSERT_EQ(ring.workers_size(), num);
    }

    void VoluntaryScaleDownInject(int workerIdx, bool resetClient = true)
    {
        if (resetClient) {
            const int w2Idx = 2;
            const int w3Idx = 3;
            if (workerIdx == 0) {
                client0_.reset();
            } else if (workerIdx == 1) {
                client1_.reset();
            } else if (workerIdx == w2Idx) {
                client2_.reset();
            } else if (workerIdx == w3Idx) {
                client3_.reset();
            }
        }
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker"
                        + std::to_string(workerIdx) + "/log/worker-status";
        std::ofstream ofs(checkFilePath);
        if (!ofs.is_open()) {
            LOG(ERROR) << "Can not open worker status file in " << checkFilePath
                       << ", voluntary scale in will not start, errno: " << errno;
        } else {
            ofs << "voluntary scale in\n";
        }
        ofs.close();
        kill(cluster_->GetWorkerPid(workerIdx), SIGTERM);
    }

protected:
    std::unique_ptr<EtcdStore> db_;
    std::vector<std::string> workerAddress_;
    std::vector<std::string> workerHost_;
    std::vector<uint32_t> workerHashValue_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::shared_ptr<KVClient> client0_, client1_, client2_, client3_;
};

TEST_F(KVClientVoluntaryScaleDownTest, TestTtlAndMigrateData)
{
    auto key = client0_->GenerateKey();
    ASSERT_TRUE(!key.empty());
    auto val = "aaaaaa";
    SetParam param;
    param.ttlSecond = 3; // ttl is 3 s
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "NotifyDeleteAndClearMeta", "sleep(3000)"));
    DS_ASSERT_OK(client1_->Set(key, val, param));
    sleep(3); // wait for ttl 3 s
    client1_.reset();
    VoluntaryScaleDownInject(1);
    sleep(5); // wait for voluntary 5 s
    AssertWorkerNum(3);  // The number of worker is 3
    std::string value;
    DS_ASSERT_NOT_OK(client0_->Get(key, value));
    DS_ASSERT_NOT_OK(client2_->Get(key, value));
    DS_ASSERT_NOT_OK(client3_->Get(key, value));
}

TEST_F(KVClientVoluntaryScaleDownTest, NormalObjectVoluntaryScaleDown)
{
    SetWorkerHashInjection();
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data);

    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(3);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, TestGiveUpLocation)
{
    int objectCnt = 50;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client0_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
    }
    HostPort worker0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0));
    VoluntaryScaleDownInject(0);  // index 0
    VoluntaryScaleDownInject(1);  // index 1
    VoluntaryScaleDownInject(2);  // worker index 2
    WaitAllNodesJoinIntoHashRing(1);
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client3_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client3_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }
    std::stringstream table;
    table << ETCD_LOCATION_TABLE_PREFIX << ETCD_HASH_SUFFIX;
    std::vector<std::pair<std::string, std::string>> outMetas;
    DS_ASSERT_NOT_OK(db_->RangeSearch(table.str(), std::to_string(0), std::to_string(UINT32_MAX), outMetas));
}

TEST_F(KVClientVoluntaryScaleDownTest, UuidObjectVoluntaryScaleDown)
{
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetUuidObject(client3_, 0, objectKey, data);

    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client1_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(3);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, NormalObjectConsecutiveVoluntaryScaleDown)
{
    SetWorkerHashInjection();
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data);

    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(2);  // The number of worker is 2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, StartKeyWithWorkerUuid)
{
    auto key = client0_->GenerateKey();
    std::string data = "aaaaaaaaaa";
    DS_ASSERT_OK(client2_->Set(key, data));
    for (size_t i = 1; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MigrateByRanges.Delay", "sleep(3000)"));
    }
    VoluntaryScaleDownInject(0);
    InitTestKVClient(1, client1_, 3000);  // Init client1 to worker 1 with 3000ms timeout
    bool stop = false;
    std::thread t1([&stop, &key, &data, this] {
        while (!stop) {
            ReadParam readParam{ .key = key, .offset = 0, .size = data.size() - 1 };
            std::vector<ReadParam> params = { readParam };
            std::vector<Optional<ReadOnlyBuffer>> buffers;
            DS_ASSERT_OK(client1_->Read(params, buffers));
            Optional<ReadOnlyBuffer> buffer = buffers.back();
            std::string getValue(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize());
            ASSERT_EQ(data.substr(0, data.size() - 1), getValue);
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // wait for 100 ms
        }
    });
    WaitAllNodesJoinIntoHashRing(3);  // The number of worker is 3
    AssertWorkerNum(3);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    WaitAllNodesJoinIntoHashRing(DEFAULT_WORKER_NUM);
    stop = true;
    t1.join();
}

TEST_F(KVClientVoluntaryScaleDownTest, SetKeyWithWorkerUuid)
{
    InitTestKVClient(0, client0_, 3000);  // Init client1 to worker 1 with 3000ms timeout
    auto key = client0_->GenerateKey();
    std::string data = "aaaaaaaaaa";
    DS_ASSERT_OK(client2_->Set(key, data));
    for (size_t i = 1; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "MigrateByRanges.Delay", "sleep(5000)"));
    }
    VoluntaryScaleDownInject(0);
    InitTestKVClient(1, client1_, 3000);  // Init client1 to worker 1 with 3000ms timeout
    WaitAllNodesJoinIntoHashRing(3);  // The number of worker is 3
    AssertWorkerNum(3);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client0_, 3000);  // Init client1 to worker 1 with 3000ms timeout
    bool stop = false;
    std::thread t2([&stop, &key, &data, this] {
        while (!stop) {
            auto key = client0_->GenerateKey();
            std::string data = "aaaaaaaaaa";
            DS_ASSERT_OK(client2_->Set(key, data));
        }
    });
    WaitAllNodesJoinIntoHashRing(DEFAULT_WORKER_NUM);
    stop = true;
    t2.join();
}

TEST_F(KVClientVoluntaryScaleDownTest, LEVEL2_TestNotRemoveFailedWorkerWhenRestart)
{
    SetWorkerHashInjection();
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "sleep(6000)"));
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 1, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished
    kill(cluster_->GetWorkerPid(0), SIGKILL);
    for (size_t i = 1; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "ProcessWorkerRestart", "return(K_OK)"));
    }
    sleep(1);
    std::string checkFilePath = FLAGS_log_dir.c_str();
    std::string client = "client";
    checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker" + std::to_string(0)
                    + "/log/worker-status";
    Remove(checkFilePath);
    DS_ASSERT_OK(cluster_->StartNode(
        WORKER, 0, " -inject_actions=GetHashRingWorkerNum:sleep(5000);Reconciliation.before:sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "SyncNodeTableWithHashRing", "return()"));
    sleep(2);  // wait 2 s for worker shutdown
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client0_, 2000);  // client rpc timeout is 2000 ms
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Set(objectKey[i], "val", param));
        DS_ASSERT_OK(client0_->Get(objectKey[i], getValue));
        ASSERT_EQ("val", getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
    }
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    DS_ASSERT_NOT_OK(db_->RangeSearch(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                      std::to_string(0), std::to_string(UINT64_MAX), outKeyValues));
    ASSERT_EQ(outKeyValues.size(), 0);
    client0_.reset();
    kill(cluster_->GetWorkerPid(0), SIGKILL);
}

TEST_F(KVClientVoluntaryScaleDownTest, LEVEL2_TestWorkersTimeout)
{
    SetWorkerHashInjection();
    int objectCnt = 10;
    std::pair<HostPort, HostPort> addrs;
    DS_ASSERT_OK(cluster_->GetEtcdAddrs(0, addrs));
    auto etcdAddress = addrs.first.ToString();
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 1, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client0_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
    }
    client0_.reset();
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(0));
    sleep(1);
    for (int i = 0; i < objectCnt; ++i) {
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
    }
    client1_.reset();
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(1));
    DS_ASSERT_OK(
        cluster_->StartNode(WORKER, 0,
                            " -client_reconnect_wait_s=1 "
                            "-inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(3000)"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    sleep(5);  // wait worker0 recover for 5s
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, " -client_reconnect_wait_s=1 -inject_actions=MurmurHash3:return()"));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    bool success = false;
    Timer timer;
    while (!success && timer.ElapsedMilliSecond() < 20000) {  // timeout is 20000 ms.
        std::vector<std::pair<std::string, std::string>> outKeyValues;
        auto status = (db_->RangeSearch(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                        std::to_string(0), std::to_string(UINT64_MAX), outKeyValues));
        if (status.GetCode() == K_NOT_FOUND) {
            success = true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    DS_ASSERT_NOT_OK(db_->RangeSearch(std::string(ETCD_ASYNC_WORKER_OP_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                      std::to_string(0), std::to_string(UINT64_MAX), outKeyValues));
    ASSERT_EQ(outKeyValues.size(), 0);
    client0_.reset();
}

TEST_F(KVClientVoluntaryScaleDownTest, DISABLED_TestWorkerGetWhenRedirect)
{
    SetWorkerHashInjection();
    std::shared_ptr<KVClient> client, client1;
    InitTestKVClient(0, client);
    InitTestKVClient(1, client1);
    auto key = client->Set("aaaaaaaaa");
    client.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.QueryMeta,wait", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay.left", "1*sleep(2000)"));
    VoluntaryScaleDownInject(0);
    std::string val;
    DS_ASSERT_OK(client1->Get(key, val));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "OCMetadataManager.QueryMeta,wait"));
}

TEST_F(KVClientVoluntaryScaleDownTest, UuidObjectConsecutiveVoluntaryScaleDown)
{
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetUuidObject(client3_, 0, objectKey, data);

    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(2);  // The number of worker is 2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, NormalObjectReverseConsecutiveVoluntaryScaleDown)
{
    SetWorkerHashInjection();
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data);

    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(2);  // The number of worker is 2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, UuidObjectReverseConsecutiveVoluntaryScaleDown)
{
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetUuidObject(client3_, 0, objectKey, data);

    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(2);  // The number of worker is 2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, LEVEL1_NormalObjectSetGetDelAndVoluntaryScaleDownConcurrently)
{
    int objectCnt = 1000;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);

    std::thread t1([&]() {
        for (uint32_t i = 0; i < objectKey.size(); ++i) {
            objectKey[i] = randomData_.GetRandomString(20) + std::to_string(i);  // Generate the data of length 20
            data[i] = randomData_.GetRandomString(10);                           // Generate the data of length 10
            SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
            DS_ASSERT_OK(client3_->Set(objectKey[i], data[i], param));
        }
    });
    VoluntaryScaleDownInject(0);
    t1.join();

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
    }

    std::thread t2([&]() {
        for (int i = 0; i < objectCnt; ++i) {
            std::string getValue;
            DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
            ASSERT_EQ(data[i], getValue);
        }
    });
    VoluntaryScaleDownInject(1);
    t2.join();

    std::thread t3([&]() {
        for (int i = 0; i < objectCnt; ++i) {
            DS_ASSERT_OK(client3_->Del(objectKey[i]));
        }
    });
    VoluntaryScaleDownInject(2);  // Voluntary scale down the worker 2
    t3.join();

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }
    sleep(3);  // Wait 3 seconds for voluntary scale down finished
    AssertWorkerNum(1);
}

TEST_F(KVClientVoluntaryScaleDownTest, LEVEL1_UuidObjectSetGetDelAndVoluntaryScaleDownConcurrently)
{
    int objectCnt = 1000;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);

    std::thread t1([&]() {
        HostPort workerHost;
        workerHost.ParseString(workerAddress_[0]);
        for (uint32_t i = 0; i < objectKey.size(); ++i) {
            objectKey[i] = NewObjectKey() + std::to_string(i) + ";" + uuidMap_[workerHost];
            data[i] = randomData_.GetRandomString(10);  // Generate the data of length 10
            SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
            DS_ASSERT_OK(client3_->Set(objectKey[i], data[i], param));
        }
    });
    VoluntaryScaleDownInject(0);
    t1.join();

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
    }

    std::thread t2([&]() {
        for (int i = 0; i < objectCnt; ++i) {
            std::string getValue;
            DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
            ASSERT_EQ(data[i], getValue);
        }
    });
    VoluntaryScaleDownInject(1);
    t2.join();

    std::thread t3([&]() {
        for (int i = 0; i < objectCnt; ++i) {
            DS_ASSERT_OK(client3_->Del(objectKey[i]));
        }
    });
    VoluntaryScaleDownInject(2);  // Voluntary scale down the worker 2
    t3.join();
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(1);
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, VoluntaryWorkersOneByOne)
{
    int objectCnt = 50;
    int objectCnt1 = 10;
    for (size_t i = 1; i < DEFAULT_WORKER_NUM; i++) {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "InspectAndProcessPeriodically.skip", "return()"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "OCMetadataManager.ReplacePrimary", "1*sleep(5000)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "worker.migrate_service.return", "1*return(K_NOT_READY)"));
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "VoluntaryScaledown.MigrateData.Delay", "sleep(3000)"));
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt1);
    std::vector<std::string> data(objectCnt);
    std::vector<std::string> data1(objectCnt1);
    SetNormalObject(client0_, 0, objectKey, data, WriteMode::NONE_L2_CACHE);
    SetUuidObject(client0_, 0, objectKey1, data1, WriteMode::NONE_L2_CACHE);
    VoluntaryScaleDownInject(0);
    sleep(2); // wait 2s for voluntary worker 1
    VoluntaryScaleDownInject(1);
    sleep(10);            // Wait 10 seconds for voluntary scale down finished
    AssertWorkerNum(2);  // The number of worker is 2
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
    }
}

TEST_F(KVClientVoluntaryScaleDownTest, RemoteGetSuccessWhenScaleDown)
{
    int objectCnt = 50;
    int objectCnt1 = 10;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 3, "CreateMultiCopyMeta.skip", "60*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 3, "CreateCopyMeta.skip", "60*return()"));
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt1);
    std::vector<std::string> data(objectCnt);
    std::vector<std::string> data1(objectCnt1);
    SetNormalObject(client0_, 0, objectKey, data, WriteMode::NONE_L2_CACHE);
    SetUuidObject(client0_, 0, objectKey1, data1, WriteMode::NONE_L2_CACHE);
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client3_->Get(objectKey[i], getValue));
    }
    for (int i = 0; i < objectCnt1; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client3_->Get(objectKey1[i], getValue));
    }
    VoluntaryScaleDownInject(0);
    sleep(2); // wait 2s for voluntary worker 1
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        auto rc = client2_->Get(objectKey[i], getValue);
        DS_ASSERT_OK(rc);
    }
    for (int i = 0; i < objectCnt1; ++i) {
        std::string getValue;
        auto rc = client2_->Get(objectKey1[i], getValue);
        DS_ASSERT_OK(rc);
    }
}

TEST_F(KVClientVoluntaryScaleDownTest, DISABLED_MasterAsyncTaskRecover)
{
    int objectCnt = 15;
    int objectCnt1 = 5;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt1);
    std::vector<std::string> data(objectCnt);
    std::vector<std::string> data1(objectCnt1);
    SetNormalObject(client3_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetUuidObject(client3_, 0, objectKey1, data1, WriteMode::WRITE_THROUGH_L2_CACHE);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "redis.Luadel.failed", "return(K_RPC_DEADLINE_EXCEEDED)"));
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
    }
    for (int i = 0; i < objectCnt1; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey1[i], getValue));
        ASSERT_EQ(data1[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey1[i]));
    }
    auto cmd = "etcdctl --endpoints " + FLAGS_etcd_address + " get --prefix /";
    int expectNum = 0;
    auto i = system(cmd.c_str());
    ASSERT_EQ(i, expectNum);
    std::stringstream table1, table2;
    table1 << ETCD_GLOBAL_CACHE_TABLE_PREFIX << ETCD_HASH_SUFFIX;
    table2 << ETCD_GLOBAL_CACHE_TABLE_PREFIX << ETCD_WORKER_SUFFIX;
    uint32_t begin = 0, end = UINT32_MAX;
    std::string keyBegin = master::Hash2Str(begin);
    std::string keyEnd = master::Hash2Str(end);
    std::vector<std::pair<std::string, std::string>> metas, metas1;
    DS_ASSERT_OK(db_->RangeSearch(table1.str(), keyBegin, keyEnd, metas));
    ASSERT_FALSE(metas.empty());
    DS_ASSERT_OK(db_->RangeSearch(table2.str(), keyBegin, keyEnd, metas1));
    ASSERT_FALSE(metas1.empty());
    VoluntaryScaleDownInject(0);
    sleep(8);            // Wait 8 seconds for voluntary scale down finished
    AssertWorkerNum(3);  // The number of worker is 3
    i = system(cmd.c_str());
    ASSERT_EQ(i, expectNum);
    std::string valueGet;
    metas1.clear();
    metas.clear();
    ASSERT_EQ(db_->RangeSearch(table1.str(), keyBegin, keyEnd, metas).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(db_->RangeSearch(table2.str(), keyBegin, keyEnd, metas1).GetCode(), K_NOT_FOUND);
    ASSERT_TRUE(metas1.empty());
}

TEST_F(KVClientVoluntaryScaleDownTest, ExpireObjWhenScaleDown)
{
    SetWorkerHashInjection();
    std::shared_ptr<KVClient> client, client1, client2;
    InitTestKVClient(0, client);
    InitTestKVClient(1, client1);
    InitTestKVClient(2, client2);  // start worker node 2
    std::string key1, key2;
    std::vector<std::string> keys, failedKeys;
    std::string value = "the sun, the moon and you";
    size_t objNum = 10;
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key1_" + std::to_string(i);
        DS_ASSERT_OK(client->Set(key1, value));
        keys.emplace_back(key1);
        key2 = "key2_" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key2, value));
        keys.emplace_back(key2);
    }
    client.reset();
    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    uint64_t ttlTimeout = 10;
    DS_ASSERT_OK(client2->Expire(keys, ttlTimeout, failedKeys));
    EXPECT_TRUE(failedKeys.empty());
}

class KVClientVoluntaryScaleDownTestWithEnableCrossNode : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=1 -client_dead_timeout_s=15";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
    }
};

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode, LEVEL1_TestVoluntarySwitchWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    VoluntaryScaleDownInject(0, false);
    sleep(12);           // Wait 12 seconds for voluntary scale down finished
    AssertWorkerNum(3);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    t1.join();
    uint32_t count = 2000;
    for (size_t i = 0; i < count; ++i) {
        auto key = client0_->Set("xxx");
        ASSERT_FALSE(key.empty());
        std::string getVal;
        DS_ASSERT_OK(client0_->Get(key, getVal));
    }
}

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode, LEVEL2_TestVoluntaryTwoWorkersAndSwitchWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    VoluntaryScaleDownInject(0, false);
    VoluntaryScaleDownInject(1, false);
    sleep(12);           // Wait 12 seconds for voluntary scale down finished
    AssertWorkerNum(2);  // The number of worker is 2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    t1.join();
    uint32_t count = 500;
    for (size_t i = 0; i < count; ++i) {
        auto key = client0_->Set("xxx");
        ASSERT_FALSE(key.empty());
        std::string getVal;
        DS_ASSERT_OK(client0_->Get(key, getVal));
    }
}

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode, LEVEL2_TestVoluntaryThreeWorkersAndSwitchWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    std::thread t1([this]() {
        uint32_t count = 4000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });
    HostPort addr1;
    HostPort addr2;
    HostPort addr3;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, addr1));  // Get worker1 address
    DS_ASSERT_OK(cluster_->GetWorkerAddr(2, addr2));  // Get worker2 address
    DS_ASSERT_OK(cluster_->GetWorkerAddr(3, addr3));  // Get worker3 address

    datasystem::inject::Set("client.standby_worker", "1*call(" + addr1.ToString() + ")");
    VoluntaryScaleDownInject(0, false);
    sleep(5);  // sleep 5s to wait for worker0 shutdown done.
    datasystem::inject::Set("client.standby_worker", "1*call(" + addr2.ToString() + ")");
    VoluntaryScaleDownInject(1, false);  // scale down worker1
    sleep(5);                            // sleep 5s to wait for worker1 shutdown done.
    datasystem::inject::Set("client.standby_worker", "1*call(" + addr3.ToString() + ")");
    VoluntaryScaleDownInject(2, false);  // scale down worker2
    sleep(10);                           // sleep 10s to wait for task done.
    t1.join();
}

class KVClientVoluntaryScaleDownTestWithEnableCrossNode1 : public KVClientVoluntaryScaleDownTestWithEnableCrossNode {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownTestWithEnableCrossNode::SetClusterSetupOptions(opts);
        opts.numWorkers = workerNum_;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    }

private:
    const uint32_t workerNum_ = 2;
};

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode1, LEVEL2_TestVoluntarySwitchWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    VoluntaryScaleDownInject(0, false);
    sleep(12);           // Wait 12 seconds for voluntary scale down finished
    AssertWorkerNum(1);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    sleep(10);  // sleep 10s to wait for scale down done.
    t1.join();
    VoluntaryScaleDownInject(1, false);
    uint32_t count = 500;
    for (size_t i = 0; i < count; ++i) {
        auto key = client0_->Set("xxx");
        ASSERT_FALSE(key.empty());
        std::string getVal;
        DS_ASSERT_OK(client0_->Get(key, getVal));
    }
    sleep(10);  // sleep 10s to wait for scale down done.
}

TEST_F(KVClientVoluntaryScaleDownTest, VoluntaryDownWorker1WriteThroughL2CacheNoCopy)
{
    int objectCnt = 5;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetUuidObject(client0_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    VoluntaryScaleDownInject(0);  // worker index is 0
    sleep(10);  // Wait 10 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
    }
}

TEST_F(KVClientVoluntaryScaleDownTest, VoluntaryDownWorker1WriteBackL2CacheNoCopy)
{
    int objectCnt = 5;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetUuidObject(client0_, 0, objectKey, data, WriteMode::WRITE_BACK_L2_CACHE);
    VoluntaryScaleDownInject(0);  // worker index is 0
    sleep(10);  // Wait 10 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
    }
}

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode1, DISABLED_LEVEL1_TestCrossNodeConnectWorkerRestart)
{
    InitTestKVClient(0, client0_, 2000, true);        // Init client0 to worker 0 with 2000ms timeout
    VoluntaryScaleDownInject(0, false);               // scale down worker0
    sleep(10);                                        // Wait 10 seconds for voluntary scale down finished
    AssertWorkerNum(1);                               // The number of worker is 3
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));  // Restart worker1
    DS_ASSERT_OK(cluster_->StartNode(
        WORKER, 1,
        " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(1)"));  // Restart worker1
    sleep(10);  // sleep 10s to wait for scale up done.
    std::thread t1([this]() {
        uint32_t count = 1000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });
    VoluntaryScaleDownInject(1, false);  // scale down worker1
    sleep(12);                           // Wait 12 seconds for voluntary scale down finished
    t1.join();
}

class KVClientVoluntaryScaleDownTestWithEnableCrossNode2 : public KVClientVoluntaryScaleDownTestWithEnableCrossNode {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownTestWithEnableCrossNode::SetClusterSetupOptions(opts);
        opts.numWorkers = workerNum_;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    }

private:
    const uint32_t workerNum_ = 3;
};

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode2, LEVEL2_TestVoluntarySwitchOtherWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    VoluntaryScaleDownInject(0, false);
    sleep(12);  // Wait 12 seconds for voluntary scale down finished
    VoluntaryScaleDownInject(1, false);
    sleep(10);           // sleep 10s to wait for scale down done.
    AssertWorkerNum(1);  // The number of worker is 3
    t1.join();

    uint32_t count = 1000;
    for (size_t i = 0; i < count; ++i) {
        auto key = client0_->Set("xxx");
        ASSERT_FALSE(key.empty());
        std::string getVal;
        DS_ASSERT_OK(client0_->Get(key, getVal));
    }
    sleep(10);  // sleep 10s to wait for scale down done.
}

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode2, DISABLED_LEVEL1_TestVoluntarySwitchErrorWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    HostPort addr1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, addr1));  // Get worker1 address

    datasystem::inject::Set("client.standby_worker", "1*call(" + addr1.ToString() + ")");
    VoluntaryScaleDownInject(0, false);
    VoluntaryScaleDownInject(1, false);
    sleep(12);  // sleep 12s to wait for scale down done.

    t1.join();
}

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode2, DISABLED_LEVEL1_TestVoluntarySwitchunhealthyWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    HostPort addr1;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, addr1));  // Get worker1 address
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HashRing.HealthProbe", "1*call()"));
    datasystem::inject::Set("client.standby_worker", "1*call(" + addr1.ToString() + ")");
    VoluntaryScaleDownInject(0, false);
    sleep(12);  // sleep 12s to wait for scale down done.
    t1.join();
}

class KVClientVoluntaryScaleDownTestWithEnableCrossNode3 : public KVClientVoluntaryScaleDownTestWithEnableCrossNode {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownTestWithEnableCrossNode::SetClusterSetupOptions(opts);
        opts.numWorkers = workerNum_;
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    }

private:
    const uint32_t workerNum_ = 1;
};

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode3, LEVEL1_TestVoluntarySwitchOtherWorker)
{
    InitTestKVClient(0, client0_, 2000, true);  // Init client0 to worker 0 with 2000ms timeout
    datasystem::inject::Set("ListenWorker.CheckHeartbeat.heartbeat_interval_ms", "call(500)");
    std::thread t1([this]() {
        uint32_t count = 2000;
        for (size_t i = 0; i < count; ++i) {
            auto key = client0_->Set("xxx");
            ASSERT_FALSE(key.empty());
            std::string getVal;
            DS_ASSERT_OK(client0_->Get(key, getVal));
        }
    });

    VoluntaryScaleDownInject(0, false);
    sleep(12);  // Wait 12 seconds for voluntary scale down finished
    t1.join();
}

TEST_F(KVClientVoluntaryScaleDownTestWithEnableCrossNode3, TestHealthCheck)
{
    InitTestKVClient(0, client0_, 2000, false);  // Init client0 to worker 0 with 2000ms timeout
    DS_ASSERT_OK(client0_->HealthCheck());
    VoluntaryScaleDownInject(0, false);
    sleep(2);  // sleep 2s to wait for scale down begin.
    int count = 10;
    for (auto i = 0; i < count; ++i) {
        ASSERT_EQ(client0_->HealthCheck().GetCode(), StatusCode::K_SCALE_DOWN);
    }
}

class KVClientVoluntaryScaleDownDfxTest : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=2 -node_timeout_s=5 -auto_del_dead_node=true";
        opts.skipWorkerPreShutdown = false;

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void AssertAllNodesJoinIntoHashRing(int num)
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        ASSERT_EQ(ring.workers_size(), num);
        for (auto &worker : ring.workers()) {
            ASSERT_TRUE(worker.second.state() == WorkerPb::ACTIVE) << ring.ShortDebugString();
        }
        ASSERT_TRUE(ring.add_node_info_size() == 0);
        ASSERT_TRUE(ring.del_node_info_size() == 0);
    }
};

TEST_F(KVClientVoluntaryScaleDownDfxTest, LEVEL2_VoluntaryWorkerScaleDown)
{
    int objectCnt = 100;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetUuidObject(client3_, 0, objectKey1, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(10)"));
    VoluntaryScaleDownInject(0);
    sleep(2); // wait 2 s for voluntary sacle down.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AutoCreate", "return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "EtcdKeepAlive.SendKeepAliveMessage", "return(K_RUNTIME_ERROR)"));
    sleep(10);  // Wait 10 seconds for worker scale down
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        DS_ASSERT_OK(client2_->Get(objectKey1[i], getValue));
    }
    AssertAllNodesJoinIntoHashRing(3);  // The number of worker is 3
}

TEST_F(KVClientVoluntaryScaleDownTest, LEVEL2_MultipleKvAndConsecutiveVoluntaryScaleDown)
{
    int objectCnt = 1000;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    for (uint32_t i = 0; i < objectKey.size(); ++i) {
        objectKey[i] = randomData_.GetRandomString(10) + std::to_string(i);  // Generate the object of length 10
        data[i] = randomData_.GetRandomString(10);                           // Generate the data of length 10
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        DS_ASSERT_OK(client0_->Set(objectKey[i], data[i], param));
    }

    std::thread t([&]() {
        for (int i = 0; i < objectCnt; ++i) {
            std::string getValue;
            DS_ASSERT_OK(client3_->Get(objectKey[i], getValue));
            ASSERT_EQ(data[i], getValue);
        }
    });
    VoluntaryScaleDownInject(1);  // Voluntary scale down the worker 1
    VoluntaryScaleDownInject(2);  // Voluntary scale down the worker 2
    sleep(3);                     // Wait 3 seconds for voluntary scale down finished
    t.join();

    for (int i = 0; i < objectCnt; ++i) {
        DS_ASSERT_OK(client3_->Del(objectKey[i]));
    }

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_NOT_OK(client0_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(2);  // The number of worker is 2
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, ""));
}

TEST_F(KVClientVoluntaryScaleDownTest, DISABLED_VoluntaryScaleDownDuringEtcdFailure)
{
    SetWorkerHashInjection();
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data);

    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    VoluntaryScaleDownInject(0);
    DS_ASSERT_OK(externalCluster_->StartEtcdCluster());
    sleep(6);  // Wait 6 seconds for voluntary scale down finished

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
        ASSERT_EQ(data[i], getValue);
        DS_ASSERT_OK(client2_->Del(objectKey[i]));
        DS_ASSERT_NOT_OK(client3_->Get(objectKey[i], getValue));
    }

    AssertWorkerNum(3);  // The number of worker is 3
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
}

class KVClientVoluntaryScaleDownWorkerDfxTest : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            "-shared_memory_size_mb=5120 -v=2 -node_timeout_s=3 "
            "-auto_del_dead_node=true -node_dead_timeout_s=5 ";

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
    }

    void StartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes, int maxWaitTimeSec = 120)
    {
        for (auto i : indexes) {
            ASSERT_TRUE(externalCluster_->StartWorker(i, HostPort()).IsOk()) << i;
        }
        for (auto i : indexes) {
            ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, i, maxWaitTimeSec).IsOk()) << i;
        }
    }

    void AssertAllNodesJoinIntoHashRing(int num)
    {
        if (!db_) {
            InitTestEtcdInstance();
        }
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        ASSERT_EQ(ring.workers_size(), num);
        for (auto &worker : ring.workers()) {
            ASSERT_TRUE(worker.second.state() == WorkerPb::ACTIVE) << ring.ShortDebugString();
        }
        ASSERT_TRUE(ring.add_node_info_size() == 0);
        ASSERT_TRUE(ring.del_node_info_size() == 0);
    }
};

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, DISABLED_TestVoluntaryFailed)
{
    StartWorkerAndWaitReady({ 0, 1, 2, 3 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "skip.SubmitScaleUpTask", "return()"));
    for (int i = 1; i <= 3; i++) { // worker num 3
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "ProcessWorkerIsLocal", "call()"));
    }
    VoluntaryScaleDownInject(0);  // index 0
    bool success = false;
    while (!success) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // wait 500 ms
        auto status = db_->CAS(
            ETCD_RING_PREFIX, "",
            [this, &success](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                HostPort addr;
                RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(0, addr));
                HashRingPb ring;
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ring.ParseFromString(oldValue), K_RUNTIME_ERROR,
                                                     "Failed to parse HashRingPb from string");
                auto iter = ring.workers().find(addr.ToString());
                if (iter != ring.workers().end() && iter->second.state() == WorkerPb::LEAVING) {
                    ring.clear_add_node_info();
                } else {
                    return Status::OK();
                }
                ring.mutable_workers()->at(addr.ToString()).clear_hash_tokens();
                newValue = std::make_unique<std::string>(ring.SerializeAsString());
                LOG(INFO) << ring.DebugString() << "----------------";
                success = true;
                return Status::OK();
            });
        DS_ASSERT_OK(status);
    }
    DS_ASSERT_OK(cluster_->KillWorker(0));
    sleep(8); // wait 8s for success.
    AssertWorkerNum(3);  // The number of worker is 3
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, TaskRetry)
{
    // init 2 nodes
    StartWorkerAndWaitReady({ 0, 1 });
    // simulate an etcd put failure to test if the scale-up task can retry
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.finishaddnodeinfo", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.HashRingHealthCheck", "1*call(3000)"));
    VoluntaryScaleDownInject(0);

    sleep(10);           // wait for the task retry for 10 seconds
    AssertWorkerNum(1);  // The number of worker is 1
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL2_VoluntaryScaleWorkerScaleDownScaleup)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "MigrateByRanges.Delay", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "MigrateByRanges.Delay", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "SubmitScaleDownTask.skip", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "SubmitScaleDownTask.skip", "sleep(5000)"));
    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    kill(cluster_->GetWorkerPid(2), SIGTERM);  // worker index 2
    std::thread t2([this] {
        kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 1  // Shutdown the worker 1
        kill(cluster_->GetWorkerPid(1), SIGTERM);  // Shutdown the worker 2
    });
    sleep(6);  // wait for node timeout 6 s
    StartWorkerAndWaitReady({ 3 });
    t2.join();
    sleep(20);                          // wait for the task retry for 20 seconds
    AssertAllNodesJoinIntoHashRing(1);  // The number of worker is 2
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL2_VoluntaryScaleDownScaleupGet)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
    InitTestKVClient(2, client2_, 2000);  // Init client2 to worker 2 with 2000ms timeout
    std::string value = GenRandomString(140);
    int keyNum = 2000;
    SetParam param{.writeMode = WriteMode::NONE_L2_CACHE};
    std::list<std::string> listStr;
    std::list<std::string>::iterator it;
    std::string valueGet;
    // w1 set
    for (int i = 0; i < keyNum; i++) {
        auto key = client1_->Set(value, param);
        listStr.push_back(key);
    }
    // w1 scale down
    VoluntaryScaleDownInject(1);
    // w0 get
    for (it = listStr.begin(); it != listStr.end(); it++) {
        DS_ASSERT_OK(client0_->Get(*it, valueGet));
        ASSERT_EQ(value, valueGet);
    }
    // w2 scale down
    VoluntaryScaleDownInject(2);
    // w0 get
    for (it = listStr.begin(); it != listStr.end(); it++) {
        DS_ASSERT_OK(client0_->Get(*it, valueGet));
        ASSERT_EQ(value, valueGet);
    }
    // scale up w3
    StartWorkerAndWaitReady({ 3 });
    sleep(2);
    InitTestKVClient(3, client3_, 2000);
    // w3 get
    for (it = listStr.begin(); it != listStr.end(); it++) {
        LOG(INFO) << "Start validate w3 get";
        DS_ASSERT_OK(client3_->Get(*it, valueGet));
        ASSERT_EQ(value, valueGet);
    }
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL1_VoluntaryScaleDownLocationNotFound)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client0_);        // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_, 1000);  // Init client1 to worker 1 with 1000ms timeout
    std::vector<std::string> keys;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "AsyncUpdateLocationAddTask.failed",  // index is 1
                                           "return(K_RUNTIME_ERROR)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.remove_location",  // index is 1
                                           "return(K_RPC_DEADLINE_EXCEEDED)"));
    for (int i = 0; i < 10; i++) {  // obj num is 10
        auto key = "a_key_for_test_" + std::to_string(i);
        keys.emplace_back(key);
        DS_ASSERT_OK(client0_->Set(key, "value"));
        std::string val;
        DS_ASSERT_NOT_OK(client1_->Get(key, val));
    }
    client0_.reset();
    VoluntaryScaleDownInject(0);
    kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 0
    sleep(10);                                 // wait for 10s for worker scale down.
    AssertAllNodesJoinIntoHashRing(1);         // The number of worker is 1
    for (const auto &key : keys) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(key, val));
    }
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL1_VoluntaryScaleWorkerScaleup)
{
    StartWorkerAndWaitReady({ 0, 1 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "sleep(5000)"));
    InitTestKVClient(0, client0_);  // Init client0 to worker 0 with 2000ms timeout
    int objectCnt = 20;             // obj num is 20
    for (int i = 0; i < objectCnt; i++) {
        auto key = "a_key_for_test_" + std::to_string(i);
        DS_ASSERT_OK(client0_->Set(key, "value"));
    }
    client0_.reset();
    VoluntaryScaleDownInject(0);
    std::thread t2([this] {
        kill(cluster_->GetWorkerPid(0), SIGTERM);  // worker index 1  // Shutdown the worker 1
    });
    sleep(1);  // wait for 1 s.
    StartWorkerAndWaitReady({ 3 });
    t2.join();
    sleep(10);                          // wait for the task retry for 10 seconds
    AssertAllNodesJoinIntoHashRing(2);  // The number of worker is 2
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL2_VoluntaryWorkerScaleDown)
{
    StartWorkerAndWaitReady({ 0, 1 });
    StartWorkerAndWaitReady({ 2, 3 });
    if (!db_) {
        InitTestEtcdInstance();
    }
    int timeoutS = 10;
    bool success = false;
    Timer timer;
    while (timer.ElapsedSecond() < timeoutS && !success) {
        success = true;
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        if (ring.workers().size() == 4) {  // worker size is 4
            for (auto &worker : ring.workers()) {
                if (worker.second.state() != WorkerPb::ACTIVE) {
                    success = false;
                }
            }
        } else {
            success = false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));  // interval is 300 ms
    }
    ASSERT_TRUE(success);
    InitTestKVClient(0, client0_);      // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_);      // Init client1 to worker 1 with 2000ms timeout
    InitTestKVClient(2, client2_);      // Init client2 to worker 2 with 2000ms timeout
    InitTestKVClient(3, client3_);      // Init client2 to worker 3 with 2000ms timeout
    GetHashOnWorker();
    GetWorkerUuids();
    SetWorkerHashInjection();
    int objectCnt = 2;
    int diffNum = 30;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt);
    std::vector<std::string> objectKey2(objectCnt);
    std::vector<std::string> objectKey3(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetUuidObject(client3_, 0, objectKey1, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetNormalObject(client3_, 0, objectKey2, data, WriteMode::NONE_L2_CACHE, diffNum);
    SetUuidObject(client3_, 0, objectKey3, data);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(5)"));
    client0_.reset();
    client2_.reset();
    VoluntaryScaleDownInject(0);
    sleep(2);  // Wait 2 seconds for voluntary scale down finished
    client2_.reset();
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(2));  // Shutdown the worker 2
    timeoutS = 50; // wait time is 50 s
    timer.Reset();
    success = false;
    while (timer.ElapsedSecond() < timeoutS && !success) {
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        if (ring.workers().size() == 2) {  // worker size is 2
            success = true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));  // interval is 2 s
    }
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
        DS_ASSERT_OK(client1_->Get(objectKey2[i], getValue));
        DS_ASSERT_OK(client1_->Get(objectKey1[i], getValue));
        DS_ASSERT_OK(client1_->Get(objectKey3[i], getValue));
    }
    AssertAllNodesJoinIntoHashRing(2);  // The number of worker is 2
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL1_TestVolunDownWorkersMasterLeaving)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    GetHashOnWorker(3);             // worker num is 3
    InitTestKVClient(0, client0_);  // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_);  // Init client1 to worker 1 with 2000ms timeout
    InitTestKVClient(2, client2_);  // Init client1 to worker 1 with 2000ms timeout
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client2_, 2, objectKey, data);  // worker index is 2
    std::vector<std::string> objectKey1(objectCnt);
    std::vector<std::string> data1(objectCnt);
    SetNormalObject(client2_, 2, objectKey1, data1);  // worker index is 2
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "sleep(5000)"));
    for (const auto &id : objectKey) {
        std::string getValue;
        DS_ASSERT_OK(client0_->Get(id, getValue));
    }
    VoluntaryScaleDownInject(0);
    sleep(2);                       // wait for 2s
    for (int i = 0; i <= 2; i++) {  // worker num is 2
        DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "EtcdClusterManager.checkConnection",
                                               "return(K_NOT_FOUND)"));
    }
    StartWorkerAndWaitReady({ 3 });
    sleep(2);  // wait for 2s
    VoluntaryScaleDownInject(2);
    for (int i = 0; i <= 2; i++) {  // worker num is 2
        DS_ASSERT_OK(cluster_->ClearInjectAction(ClusterNodeType::WORKER, i, "EtcdClusterManager.checkConnection"));
    }
    sleep(10);  // wait for 10s

    AssertWorkerNum(2);  // The number of worker is 2
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL2_TestScaleDownWorkersUuidMetaData)
{
    StartWorkerAndWaitReady({ 0, 1 });
    StartWorkerAndWaitReady({ 2, 3 });
    sleep(10);                          // Wait 10 seconds for worker ready.
    AssertAllNodesJoinIntoHashRing(4);  // The number of worker is 4
    InitTestKVClient(0, client0_);      // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_);      // Init client1 to worker 1 with 2000ms timeout
    InitTestKVClient(2, client2_);      // Init client2 to worker 2 with 2000ms timeout
    InitTestKVClient(3, client3_);      // Init client2 to worker 3 with 2000ms timeout
    GetHashOnWorker();
    GetWorkerUuids();
    int objectCnt = 5;
    std::vector<std::string> objectKey1(objectCnt);
    std::vector<std::string> objectKey2(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetUuidObject(client0_, 0, objectKey1, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetUuidObject(client1_, 0, objectKey2, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    VoluntaryScaleDownInject(0);
    sleep(2);  // Wait 2 seconds for voluntary scale down finished
    client2_.reset();
    client3_.reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));  // Shutdown the worker 2
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 3));  // Shutdown the worker 3
    sleep(10);                                        // Wait 10 seconds for voluntary scale down finished
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey1[i], getValue));
    }
    AssertAllNodesJoinIntoHashRing(1);  // The number of worker is 1
}

TEST_F(KVClientVoluntaryScaleDownWorkerDfxTest, LEVEL1_VoluntaryWorkerScaleDownTest)
{
    StartWorkerAndWaitReady({ 0, 1, 2, 3 });
    if (!db_) {
        InitTestEtcdInstance();
    }
    int timeoutS = 10;
    bool success = false;
    Timer timer;
    while (timer.ElapsedSecond() < timeoutS && !success) {
        success = true;
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        if (ring.workers().size() == 4) {  // worker size is 4
            for (auto &worker : ring.workers()) {
                if (worker.second.state() != WorkerPb::ACTIVE) {
                    success = false;
                }
            }
        } else {
            success = false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(300));  // interval is 300 ms
    }
    ASSERT_TRUE(success);
    InitTestKVClient(0, client0_);      // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_);      // Init client1 to worker 1 with 2000ms timeout
    InitTestKVClient(2, client2_);      // Init client2 to worker 2 with 2000ms timeout
    InitTestKVClient(3, client3_);      // Init client2 to worker 3 with 2000ms timeout
    GetHashOnWorker();
    GetWorkerUuids();
    SetWorkerHashInjection();
    int objectCnt = 20;
    int diffNum = 30;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt);
    std::vector<std::string> objectKey2(objectCnt);
    std::vector<std::string> objectKey3(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client3_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetUuidObject(client3_, 0, objectKey1, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetNormalObject(client3_, 0, objectKey2, data, WriteMode::NONE_L2_CACHE, diffNum);
    SetUuidObject(client3_, 0, objectKey3, data);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(5)"));
    client0_.reset();
    VoluntaryScaleDownInject(0);
    client1_.reset();
    sleep(2);  // Wait 2 seconds for voluntary scale down finished
    DS_ASSERT_OK(cluster_->QuicklyShutdownWorker(1));
    timeoutS = 50; // wait time is 50 s
    timer.Reset();
    success = false;
    while (timer.ElapsedSecond() < timeoutS && !success) {
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ASSERT_TRUE(ring.ParseFromString(value));
        if (ring.workers().size() == 2) {  // worker size is 2
            success = true;
        }
        std::this_thread::sleep_for(std::chrono::seconds(2));  // interval is 2 s
    }

    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(objectKey[i], getValue));
        DS_ASSERT_OK(client2_->Get(objectKey2[i], getValue));
        DS_ASSERT_OK(client2_->Get(objectKey1[i], getValue));
        DS_ASSERT_OK(client2_->Get(objectKey3[i], getValue));
    }
    AssertAllNodesJoinIntoHashRing(2);  // The number of worker is 2
}

class STCVoluntaryScaleDownWorkerFaileDfxTest : public KVClientVoluntaryScaleDownWorkerDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownWorkerDfxTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams =
            "-shared_memory_size_mb=100 -v=2 -node_timeout_s=60 "
            "-auto_del_dead_node=true -node_dead_timeout_s=600 -rolling_update_timeout_s=1 "
            "-inject_actions=worker.HashRingHealthCheck:call(100)";
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }
};

TEST_F(STCVoluntaryScaleDownWorkerFaileDfxTest, DISABLED_LEVEL1_TestScaleDownFailedAndRestart)
{
    LOG(INFO) << "Test worker scale down failed and restart";
    StartWorkerAndWaitReady({ 0, 1 });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.after_finish_add_node_info", "abort()"));
    VoluntaryScaleDownInject(0);
    sleep(5);  // sleep 5s to wait for scale down.
    ASSERT_TRUE(cluster_
                    ->StartNode(WORKER, 0,
                                " -inject_actions=Reconciliation.before:return(K_OK);WorkerOCServiceImpl."
                                "GiveUpReconciliation.setHealthFile:call(1000) -client_reconnect_wait_s=1 ")
                    .IsOk());
    ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, 0, 20).IsOk());  // wait 20s to restart.
    InitTestKVClient(0, client0_);
    auto key = client0_->GenerateKey();
    ASSERT_FALSE(key.empty());
    DS_ASSERT_OK(cluster_->KillWorker(1));
    DS_ASSERT_OK(cluster_->KillWorker(0));
}

class STCVoluntaryScaleDownWorkerFaileDfxTest2 : public KVClientVoluntaryScaleDownWorkerDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownWorkerDfxTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams =
            "-shared_memory_size_mb=100 -v=2 -node_timeout_s=2 "
            "-auto_del_dead_node=true -node_dead_timeout_s=3 ";
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }
};

TEST_F(STCVoluntaryScaleDownWorkerFaileDfxTest2, TestRedirctDuringScaleDownFailedAndRestart)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client0_);
    auto keyWithW0Id = client0_->GenerateKey();
    client0_.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.after_finish_add_node_info", "call()"));
    VoluntaryScaleDownInject(0);
    WaitAllNodesJoinIntoHashRing(1);
    InitTestKVClient(1, client1_);
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 1, "HashRingTaskExecutor.SubmitOneScaleUpTask.PreMarkAddNodeInfoFinished", "sleep(3000)"));
    DS_ASSERT_OK(externalCluster_->StartWorker(0, HostPort(), ""));
    // Wait for worker0 to write add_node_info so that worker1 can trigger the sacling up migration task.
    WaitAddNodeInfoInHashRing();
    // The metadata has now been migrated, but hash ring hasn't been updated yet.
    // It's expected that SET requests will be routed to the node where the metadata has been migrated, so that GET can
    // query meta successfully.
    DS_ASSERT_OK(client1_->Set(keyWithW0Id, "val"));
    WaitAllNodesJoinIntoHashRing(2);  // wait for 2 workers to join into hash ring.
    ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, 0, 20).IsOk());  // wait 20s to restart.
    InitTestKVClient(0, client0_);
    std::string valToGet;
    DS_ASSERT_OK(client0_->Get(keyWithW0Id, valToGet));
    ASSERT_EQ(valToGet, "val");
}

// Test the scenario where records in update_worker_map are deleted by scheduled tasks during metadata migration.
TEST_F(STCVoluntaryScaleDownWorkerFaileDfxTest2, TestRedirctDuringScaleDownFailedAndRestart2)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client0_);
    auto keyWithW0Id = client0_->GenerateKey();
    client0_.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.after_finish_add_node_info", "call()"));
    VoluntaryScaleDownInject(0);
    WaitAllNodesJoinIntoHashRing(1);
    InitTestKVClient(1, client1_);
    DS_ASSERT_OK(client1_->Set(keyWithW0Id, "val"));
    // To prevent worker0 from receiving the hash ring indicating that the scale up is complete, otherwise the
    // redirection cannot be triggered.
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 1, "HashRingTaskExecutor.SubmitOneScaleUpTask.PreMarkAddNodeInfoFinished", "sleep(5000)"));
    DS_ASSERT_OK(externalCluster_->StartWorker(0, HostPort(), ""));
    // Wait for worker0 to write add_node_info so that worker1 can trigger the sacling up migration task.
    WaitAddNodeInfoInHashRing();
    // The background task in worker1 will delete the workerId of worker0 in update_worker_map in advance.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "HashRingTools.WorkerUuidRemovable", "return()"));
    // After receiving the migration from worker1, refuse to update the hash ring to ensure that a redirection will be
    // triggered.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "HashRing.UpdateRing.sleep", "return()"));
    ASSERT_TRUE(cluster_->WaitNodeReady(WORKER, 0, 20).IsOk());  // wait 20s to restart.
    InitTestKVClient(0, client0_);
    std::string valToGet;
    DS_ASSERT_OK(client0_->Get(keyWithW0Id, valToGet));
    ASSERT_EQ(valToGet, "val");
}

class STCVoluntaryScaleDownWorkerDfxTest : public KVClientVoluntaryScaleDownWorkerDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            "-shared_memory_size_mb=5120 -v=2 -node_timeout_s=3 "
            "-auto_del_dead_node=true -node_dead_timeout_s=5 ";

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void GetAllHashOnWorker(size_t idx, std::vector<uint32_t> &hashes)
    {
        if (idx >= DEFAULT_WORKER_NUM) {
            return;
        }
        std::string value;
        db_->Get(ETCD_RING_PREFIX, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        auto tokens = ring.workers().at(workerAddress_[idx]).hash_tokens();
        for (const auto &token : tokens) {
            hashes.emplace_back(token);
        }
    }
};

TEST_F(STCVoluntaryScaleDownWorkerDfxTest, DISABLED_LEVEL1_TestWorkerRestartTimeout)
{
    StartWorkerAndWaitReady({ 0, 1 });
    sleep(10);                          // Wait 10 seconds for worker ready.
    AssertAllNodesJoinIntoHashRing(2);  // The number of worker is 2
    InitTestKVClient(0, client0_);      // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(1, client1_);      // Init client1 to worker 1 with 2000ms timeout
    GetHashOnWorker(2);
    GetWorkerUuids();
    int objectCnt = 5;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> objectKey1(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client0_, 0, objectKey, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    SetUuidObject(client0_, 0, objectKey1, data, WriteMode::WRITE_THROUGH_L2_CACHE);
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    sleep(4);  // Wait 4 seconds for worker shutdown
    VoluntaryScaleDownInject(0);
    sleep(4);  // Wait 4 seconds for voluntary scale down finished
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    sleep(10);                          // Wait 10 seconds for worker ready.
    AssertAllNodesJoinIntoHashRing(1);  // The number of worker is 1
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(objectKey[i], getValue));
        DS_ASSERT_OK(client1_->Get(objectKey1[i], getValue));
    }
}

TEST_F(STCVoluntaryScaleDownWorkerDfxTest, DestFaultDuringScaleDown)
{
    STCScaleTest::StartWorkerAndWaitReady(
        { 0, 1, 2 },
        " -inject_actions=test.start.notWait:call(0);hashring.RemoveToken:call(" + workerAddress_[1] + ",2)");
    WaitAllNodesJoinIntoHashRing(3, 20);  // Wait for 3 workers to join the hash ring within 20s.
    std::vector<uint32_t> hashesOnWorker0;
    GetAllHashOnWorker(0, hashesOnWorker0);
    SetWorkerHashInjection({ 0, 1, 2 });

    InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
    InitTestKVClient(2, client2_, 2000);  // Init client2 to worker 2 with 2000ms timeout
    int objectCnt = 100;
    std::vector<std::string> keys;
    size_t p = 0;
    for (int i = 0; i < objectCnt; i++) {
        auto hash = --hashesOnWorker0[p++ % hashesOnWorker0.size()];
        keys.emplace_back("a_key_hash_to_" + std::to_string(hash));
        DS_ASSERT_OK(client0_->Set(keys[i], GenRandomString()));
    }
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "hashring.regenerate.sleep", "1*sleep(10000)"));
    VoluntaryScaleDownInject(0);
    sleep(4);  // Wait 4 seconds for voluntary scale down finished
    for (int i = 0; i < objectCnt; ++i) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(keys[i], getValue));
    }
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
}

class STCVoluntaryStartScaleDownWorkerDfxTest : public KVClientVoluntaryScaleDownWorkerDfxTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = 20;  // add node wait time is 20 s
        opts.workerGflagParams =
            "-v=2 -node_timeout_s=3 "
            "-auto_del_dead_node=true -node_dead_timeout_s=5 ";

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }
};

TEST_F(STCVoluntaryStartScaleDownWorkerDfxTest, NodeStartAfterVolunDownKilled)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    WaitAllNodesJoinIntoHashRing(3, 20);  // Wait for 3 workers to join the hash ring within 20s.
    VoluntaryScaleDownInject(0);
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    StartWorkerAndWaitReady({ 0 });
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    WaitAllNodesJoinIntoHashRing(2, 20);  // Wait for w0 to exit the hash ring within 20s.
}

const static int SCALE_ADD_TIME = 10;
class KVCacheClientClusterState : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_ADD_TIME;
        opts.skipWorkerPreShutdown = false;
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=2 -node_timeout_s=2 -auto_del_dead_node=false";
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }
};

TEST_F(KVCacheClientClusterState, DISABLED_TestWorkerReady)
{
    std::string value;
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    DS_ASSERT_OK(db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value));
    auto pos = value.find(";");
    auto state = value.substr(pos + 1);
    ASSERT_EQ(state, "ready");
}

TEST_F(KVCacheClientClusterState, DISABLED_TestWorkerRecovery)
{
    std::string value;
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
    auto pos = value.find(";");
    auto state = value.substr(pos + 1);
    ASSERT_EQ(state, "ready");
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(4000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "recover.toReady.delay", "1*sleep(2000)"));
    std::function<Status()> fun = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        pos = value.find(";");
        state = value.substr(pos + 1);
        if (state == "recover") {
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    std::function<Status()> fun1 = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        pos = value.find(";");
        state = value.substr(pos + 1);
        if (state == "ready") {
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    int waitTime = 20;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun, waitTime, K_OK));
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
}

TEST_F(KVCacheClientClusterState, DISABLED_TestWorkerRecoveryWhenRestart)
{
    client0_.reset();
    client1_.reset();
    client2_.reset();
    client3_.reset();
    std::string value;
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
    auto pos = value.find(";");
    auto state = value.substr(pos + 1);
    ASSERT_EQ(state, "ready");
    DS_ASSERT_OK(externalCluster_->KillWorker(2)); // index is 2
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(4000)"));
    std::function<Status()> fun = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        pos = value.find(";");
        state = value.substr(pos + 1);
        if (state == "recover") {
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    std::function<Status()> fun1 = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        pos = value.find(";");
        state = value.substr(pos + 1);
        if (state == "ready") {
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    int waitTime = 20;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun, waitTime, K_OK));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 2, " -client_reconnect_wait_s=1")); // index is 2
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
}

TEST_F(KVCacheClientClusterState, DISABLED_TestWorkerLeavingTimeout)
{
    std::string value;
    for (int i = 0; i < 20; i++) {  // Generate 20 objects
        auto key = "a_key_for_test_" + std::to_string(i);
        DS_ASSERT_OK(client0_->Set(key, "vvvvvvvvvv"));
    }
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "heartbeat.sleep", "1*sleep(4000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "recover.toexiting.delay", "1*sleep(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(60)"));
    VoluntaryScaleDownInject(0);
    std::function<Status()> fun1 = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        auto pos = value.find(";");
        auto state = value.substr(pos + 1);
        if (state == "exiting") {
            return Status::OK();
        }
        LOG(INFO) << "-----------" << state;
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    std::function<Status()> fun = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        auto pos = value.find(";");
        auto state = value.substr(pos + 1);
        if (state == "recover") {
            return Status::OK();
        }
        LOG(INFO) << "-----------" << state;
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    int waitTime = 20;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
    auto key = "test";
    auto val = "qqqqqqq";
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun, waitTime, K_OK));
    DS_ASSERT_NOT_OK(client0_->Set(key, val));
}

TEST_F(KVCacheClientClusterState, DISABLED_TestWorkerLeaving)
{
    std::string value;
    for (int i = 0; i < 20; i++) {  // Generate 20 objects
        auto key = "a_key_for_test_" + std::to_string(i);
        DS_ASSERT_OK(client0_->Set(key, "vvvvvvvvvv"));
    }
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    VoluntaryScaleDownInject(0);
    std::function<Status()> fun1 = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        auto pos = value.find(";");
        auto state = value.substr(pos + 1);
        if (state == "exiting") {
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    int waitTime = 10;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
    auto key = "test";
    auto val = "qqqqqqq";
    DS_ASSERT_NOT_OK(client0_->Set(key, val));
}

TEST_F(KVCacheClientClusterState, LEVEL1_TestEtcdRestartWorkerReady)
{
    inject::Set("ObjClient.ShutDown", "return(K_OK)");
    auto thread = Thread([this]() {
        client0_.reset();
        client1_.reset();
        client2_.reset();
        client3_.reset();
    });
    std::string value;
    std::string nodeState = "recover";
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    std::function<Status()> fun1 = [&]() {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        auto pos = value.find(";");
        auto state = value.substr(pos + 1);
        if (state == nodeState) {
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.PreShutDown.skip", "return(K_OK)");
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "HandleNodeRemoveEvent.delay", "return(K_OK)"));
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "etcdrecover.worker.delaytoready", "sleep(2000)"));
    }
    DS_ASSERT_OK(cluster_->ShutdownNodes(ETCD));
    DS_ASSERT_OK(cluster_->StartEtcdCluster());
    int waitTime = 20;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
    nodeState = "ready";
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
    thread.join();
}

class KVCacheClientClusterStateTest : public KVCacheClientClusterState {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_ADD_TIME;
        opts.workerGflagParams =
            " -v=2 -node_timeout_s=2 -node_dead_timeout_s=5 -auto_del_dead_node=true";
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }
};

TEST_F(KVCacheClientClusterStateTest, LEVEL1_WorkerLeaving)
{
    std::string value;
    for (int i = 0; i < 20; i++) {  // Generate 20 objects
        auto key = "a_key_for_test_" + std::to_string(i);
        DS_ASSERT_OK(client0_->Set(key, "vvvvvvvvvv"));
    }
    DS_ASSERT_OK(db_->CreateTable(ETCD_CLUSTER_TABLE, "/" + std::string(ETCD_CLUSTER_TABLE)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(5)"));
    VoluntaryScaleDownInject(0);
    std::function<Status()> fun1 = [&] {
        db_->Get(ETCD_CLUSTER_TABLE, workerAddress_.front(), value);
        auto pos = value.find(";");
        auto state = value.substr(pos + 1);
        if (state == "exiting") {
            return Status::OK();
        }
        LOG(INFO) << "-----------" << state;
        return Status(K_RUNTIME_ERROR, "not equal");
    };
    int waitTime = 20;
    DS_ASSERT_OK(cluster_->WaitForExpectedResult(fun1, waitTime, K_OK));
    ConnectOptions connectOptions;
    uint32_t timeoutMs = 5'000;
    InitConnectOpt(0, connectOptions, timeoutMs, false);  // init worker0, timeout is 5000ms
    client0_ = std::make_shared<KVClient>(connectOptions);
    ASSERT_EQ(client0_->Init().GetCode(), StatusCode::K_NOT_READY);
}

class VoluntaryScaleDownBySwitch : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams =
            "-shared_memory_size_mb=5120 -enable_lossless_data_exit_mode=true -check_async_queue_empty_time_s=3";
    }
};

TEST_F(VoluntaryScaleDownBySwitch, ClusterShutdown)
{
    StartWorkerAndWaitReady({ 0, 1, 2 });
    InitClients();

    kill(cluster_->GetWorkerPid(0), SIGTERM);
    kill(cluster_->GetWorkerPid(1), SIGTERM);
    using namespace std::chrono_literals;
    std::string val = GenRandomString();
    std::vector<std::string> keys;
    uint32_t objCount = 100;
    for (uint32_t i = 0; i < objCount; i++) {
        DS_ASSERT_OK(client_->Set("0_" + std::to_string(i), val));
        keys.emplace_back("0_" + std::to_string(i));
        DS_ASSERT_OK(client1_->Set("1_" + std::to_string(i), val));
        keys.emplace_back("1_" + std::to_string(i));
        DS_ASSERT_OK(client2_->Set("2_" + std::to_string(i), val));
        keys.emplace_back("2_" + std::to_string(i));
    }
    for (uint32_t i = 0; i < objCount; i++) {
        std::string key;
        (void)client_->GenerateKey("0_" + std::to_string(i), key);
        DS_ASSERT_OK(client_->Set(key, val));
        keys.emplace_back(key);
        (void)client1_->GenerateKey("1_" + std::to_string(i), key);
        DS_ASSERT_OK(client1_->Set(key, val));
        keys.emplace_back(key);
        (void)client2_->GenerateKey("2_" + std::to_string(i), key);
        DS_ASSERT_OK(client2_->Set(key, val));
        keys.emplace_back(key);
    }
    client_.reset();
    client1_.reset();

    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 and w1 to exit the hash ring within 20s.

    std::string outVal;
    for (auto &key : keys) {
        DS_ASSERT_OK(client2_->Get(key, outVal));
        ASSERT_EQ(val, outVal);
    }
}

// Test point: worker can exit if HashState::FAIL
TEST_F(VoluntaryScaleDownBySwitch, ShutDownHashRingFailNode)
{
    // int client
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    // remove w0 from ring so w0 should exit
    std::string value;
    DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
    HashRingPb ring;
    ASSERT_TRUE(ring.ParseFromString(value));
    HostPort w0;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, w0));
    ring.mutable_workers()->erase(w0.ToString());
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));

    // test if w0 exits with client connected
    // skip the error of non-normally exit in TearDown
    (void)cluster_->ShutdownNode(WORKER, 0);
}

TEST_F(VoluntaryScaleDownBySwitch, LEVEL2_AllNodesScaleDownAndUp)
{
    const size_t workerNum = 2;
    StartWorkerAndWaitReady({ 0, 1 });
    for (size_t i = 0; i < workerNum; i++) {
        kill(cluster_->GetWorkerPid(i), SIGTERM);
    }
    sleep(15);  // Wait 15s for all nodes exit
    StartWorkerAndWaitReady({ 0, 1 });
    WaitAllNodesJoinIntoHashRing(workerNum, 20);  //  Wait for all workers to join the hash ring within 20s.
}

TEST_F(VoluntaryScaleDownBySwitch, TriggerDataMigrateAfterClientExit)
{
    StartWorkerAndWaitReady({ 0, 1 });
    InitTestKVClient(0, client_);
    std::string key = GenRandomString();
    std::string val = GenRandomString();
    DS_ASSERT_OK(client_->Set(key, val));

    kill(cluster_->GetWorkerPid(0), SIGTERM);

    sleep(5);  // Wait 5s for w0 migrate meta finish
    client_.reset();
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
}

TEST_F(VoluntaryScaleDownBySwitch, LEVEL2_TestDestNodeScaleDownWhenVoluntaryScaleDown)
{
    auto wait = [this](int workerIndex, const std::string &name) {
        uint64_t executCount = 0;
        int intervalMs = 100;
        int maxTimeout = 30;
        Timer timer;
        while (executCount == 0) {
            ASSERT_TRUE(timer.ElapsedSecond() < maxTimeout) << "time out for " << name;
            DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIndex, name, executCount));
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
        }
    };
    // start worker0 ~ worker2
    // worker0 and worker1 voluntary scale down and worker2 scale down.
    std::string gflags = "-node_dead_timeout_s=5 -node_timeout_s=3";
    StartWorkerAndWaitReady({ 0, 1, 2 }, gflags);  // start 3 workers

    InitTestKVClient(0, client_);
    std::vector<std::string> keys;
    uint32_t objCount = 100;
    for (uint32_t i = 0; i < objCount; i++) {
        std::string key = GetStringUuid();
        DS_ASSERT_OK(client_->Set(key, "value"));
    }
    client_.reset();

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.BeforeShutdown", "pause()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.BeforeShutdown", "pause()"));
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    kill(cluster_->GetWorkerPid(1), SIGTERM);
    kill(cluster_->GetWorkerPid(2), SIGKILL);  // force kill worker 2
    // wait worker0 pend shutdown but not delete etcd cluster key.
    wait(0, "worker.BeforeShutdown");
    const int startWorkerIdx = 3;
    StartWorkerAndWaitReady({ startWorkerIdx }, gflags);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "worker.BeforeShutdown"));
    sleep(2);  // wait 2 sec for worker3 process delete event for worker0

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, startWorkerIdx, "worker.VoluntaryScaleDown", "call()"));
    kill(cluster_->GetWorkerPid(startWorkerIdx), SIGTERM);
    // wait worker3 in VoluntaryScaleDown
    wait(startWorkerIdx, "worker.VoluntaryScaleDown");
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "worker.BeforeShutdown"));
    // wait all node shutdown
}

TEST_F(VoluntaryScaleDownBySwitch, TestVoluntaryScaleDownNodeNotExistsInClusterTable)
{
    // The hashring exists pending leaving node.
    HashRingPb ring;
    WorkerPb workerPb;
    workerPb.set_worker_uuid("5d941f6a-dd77-43ee-b3cf-40a1cf77489e");
    workerPb.set_state(WorkerPb::LEAVING);
    workerPb.set_need_scale_down(true);
    std::vector<uint32_t> tokens = { 703003027, 1320750813, 2986027838, 1537794891 };
    for (auto token : tokens) {
        workerPb.mutable_hash_tokens()->Add(token);
    }
    ring.mutable_workers()->insert({ "127.0.0.1:9999", workerPb });
    ring.set_cluster_has_init(true);
    DS_ASSERT_OK(db_->Put(ETCD_RING_PREFIX, "", ring.SerializeAsString()));

    int timeoutSec = 20;
    std::string gflags =
        "-node_dead_timeout_s=5 -node_timeout_s=3 "
        "-inject_actions=EtcdClusterManager.CheckWaitNodeTableComplete.waitTime:call(2)";
    StartWorkerAndWaitReady({ 0, 1 }, timeoutSec, false, gflags);
    WaitAllNodesJoinIntoHashRing(2);  // worker count 2
}

class ConcurrentVoluntaryScaleDown : public KVClientVoluntaryScaleDownTest {};

TEST_F(ConcurrentVoluntaryScaleDown, ContinuousScaleDown)
{
    SetWorkerHashInjection();
    std::vector<std::shared_ptr<KVClient>> clients = { client0_, client1_, client2_, client3_ };
    std::vector<std::string> allKeys;
    std::vector<std::string> allValues;
    for (size_t i = 0; i < clients.size(); i++) {
        int cnt = 100;
        std::vector<std::string> keys(cnt);
        std::vector<std::string> values(cnt);
        SetNormalObject(clients[i], i, keys, values);
        allKeys.insert(allKeys.end(), keys.begin(), keys.end());
        allValues.insert(allValues.end(), values.begin(), values.end());
        SetUuidObject(clients[i], i, keys, values);
        allKeys.insert(allKeys.end(), keys.begin(), keys.end());
        allValues.insert(allValues.end(), values.begin(), values.end());
    }
    clients.clear();

    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    WaitAllNodesJoinIntoHashRing(2, 20);  // Wait for w0 and w1 to exit the hash ring within 20s.

    for (size_t i = 0; i < allKeys.size(); i++) {
        std::string getValue;
        DS_ASSERT_OK(client2_->Get(allKeys[i], getValue));
        ASSERT_EQ(allValues[i], getValue);
    }

    const int w2Idx = 2;
    VoluntaryScaleDownInject(w2Idx);
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w2 to exit the hash ring within 20s.
    for (size_t i = 0; i < allKeys.size(); i++) {
        std::string getValue;
        DS_ASSERT_OK(client3_->Get(allKeys[i], getValue));
        ASSERT_EQ(allValues[i], getValue);
        DS_ASSERT_OK(client3_->Del(allKeys[i]));
    }

    AssertWorkerNum(1);  // The number of worker is 1
}

TEST_F(ConcurrentVoluntaryScaleDown, SetGetDelDuringScaleDown)
{
    SetWorkerHashInjection();
    std::vector<std::shared_ptr<KVClient>> clients = { client0_, client1_, client2_, client3_ };
    std::vector<std::string> allObjectKeys;
    std::vector<std::string> allValues;
    for (size_t i = 0; i < clients.size(); i++) {
        int cnt = 100;
        std::vector<std::string> keys(cnt);
        std::vector<std::string> values(cnt);
        SetNormalObject(clients[i], i, keys, values);
        allObjectKeys.insert(allObjectKeys.end(), keys.begin(), keys.end());
        allValues.insert(allValues.end(), values.begin(), values.end());
        SetUuidObject(clients[i], i, keys, values);
        allObjectKeys.insert(allObjectKeys.end(), keys.begin(), keys.end());
        allValues.insert(allValues.end(), values.begin(), values.end());
    }
    clients.clear();

    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);

    std::thread t1([this] {
        int cnt = 500;
        std::vector<std::string> keys(cnt);
        std::vector<std::string> values(cnt);
        const int w2Idx = 2;
        SetNormalObject(client2_, w2Idx, keys, values, WriteMode::NONE_L2_CACHE, cnt);
        SetUuidObject(client2_, w2Idx, keys, values);
    });
    std::thread t2([&] {
        for (size_t i = 0; i < allObjectKeys.size(); i++) {
            std::string getValue;
            DS_ASSERT_OK(client3_->Get(allObjectKeys[i], getValue));
            ASSERT_EQ(allValues[i], getValue) << allObjectKeys[i];
            DS_ASSERT_OK(client3_->Del(allObjectKeys[i]));
        }
    });

    WaitAllNodesJoinIntoHashRing(2, 20);  // Wait for w0 and w1 to exit the hash ring within 20s.
    t1.join();
    t2.join();
}

TEST_F(ConcurrentVoluntaryScaleDown, TestTwoWorkerScaledownConcurrently)
{
    LOG(INFO) << "Test 2 worker scale down concurrently";
    std::string key = "MarckYao";
    std::string val = "xxx";
    std::string getVal;
    DS_ASSERT_OK(client0_->Set(key, val));
    DS_ASSERT_OK(client1_->Get(key, getVal));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "process.change.primary.copy", "sleep(1000)"));
    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    WaitAllNodesJoinIntoHashRing(2, 20);  // Wait for w0 and w1 to exit the hash ring within 20s.

    getVal.clear();
    DS_ASSERT_OK(client2_->Get(key, getVal));
    ASSERT_EQ(val, getVal);
}

class ConcurrentVoluntaryScaleDownDfx : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=2 -node_timeout_s=3 -node_dead_timeout_s=5";

        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }
};

TEST_F(ConcurrentVoluntaryScaleDownDfx, SrcFaultDuringScaleDown)
{
    SetWorkerHashInjection();
    int cnt = 100;
    std::vector<std::string> keys0(cnt);
    std::vector<std::string> keys0WithUuid(cnt);
    std::vector<std::string> keys1(cnt);
    std::vector<std::string> keys1WithUuid(cnt);
    std::vector<std::string> values(cnt);
    SetNormalObject(client2_, 0, keys0, values);
    SetUuidObject(client2_, 0, keys0WithUuid, values);
    SetNormalObject(client2_, 1, keys1, values);
    SetUuidObject(client2_, 1, keys1WithUuid, values);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "1*sleep(5000)"));
    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(1);
    sleep(3);  // wait 3s for w0 and w1 migrate meta
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    WaitAllNodesJoinIntoHashRing(2, 20);  // Wait for w0 and w1 to exit the hash ring within 20s.
    for (int i = 0; i < cnt; i++) {
        std::string getValue;
        DS_ASSERT_OK(client3_->Get(keys0[i], getValue));
        DS_ASSERT_OK(client3_->Get(keys0WithUuid[i], getValue));
        DS_ASSERT_OK(client3_->Get(keys1[i], getValue));
        DS_ASSERT_OK(client3_->Get(keys1WithUuid[i], getValue));
    }
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    WaitAllNodesJoinIntoHashRing(DEFAULT_WORKER_NUM, 20);  // Wait for all workers to join the hash ring within 20s.
}

TEST_F(ConcurrentVoluntaryScaleDownDfx, LEVEL1_TestStandbyNodeNotExist)
{
    WaitAllNodesJoinIntoHashRing(DEFAULT_WORKER_NUM);
    client2_.reset();
    DS_ASSERT_OK(db_->CAS(
        ETCD_RING_PREFIX, "",
        [this](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
            HashRingPb oldRing;
            if (!oldRing.ParseFromString(oldValue)) {
                LOG(WARNING) << "Failed to parse HashRingPb from string. give up and wait for next time if needed.";
                return Status::OK();
            }
            auto range = (*oldRing.mutable_del_node_info())[workerAddress_[2]].mutable_changed_ranges()->Add();
            range->set_workerid("127.0.0.1:222222");
            range->set_from(111111111);  // token is 111111111
            range->set_end(2222222222);  // token is 2222222222

            newValue = std::make_unique<std::string>(oldRing.SerializeAsString());
            return Status::OK();
        }));
    WaitAllNodesJoinIntoHashRing(3); // worker num is 3
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));
}

TEST_F(ConcurrentVoluntaryScaleDownDfx, LEVEL1_DestFaultDuringScaleDown)
{
    SetWorkerHashInjection();
    int cnt = 100;
    std::vector<std::string> keys0(cnt);
    std::vector<std::string> keys0WithUuid(cnt);
    std::vector<std::string> keys1(cnt);
    std::vector<std::string> keys1WithUuid(cnt);
    std::vector<std::string> values(cnt);
    SetNormalObject(client0_, 0, keys0, values);
    SetUuidObject(client0_, 0, keys0WithUuid, values);
    const int w2Idx = 2;
    SetNormalObject(client2_, w2Idx, keys1, values);
    SetUuidObject(client2_, w2Idx, keys1WithUuid, values);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "1*sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, w2Idx, "VoluntaryScaledown.MigrateData.Delay", "1*sleep(5000)"));
    DS_ASSERT_OK(externalCluster_->KillWorker(1));
    VoluntaryScaleDownInject(0);
    VoluntaryScaleDownInject(w2Idx);
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0, w1 and w2 to exit the hash ring within 20s.
    for (int i = 0; i < cnt; i++) {
        std::string getValue;
        DS_ASSERT_OK(client3_->Get(keys0[i], getValue));
        DS_ASSERT_OK(client3_->Get(keys0WithUuid[i], getValue));
        DS_ASSERT_OK(client3_->Get(keys1[i], getValue));
        DS_ASSERT_OK(client3_->Get(keys1WithUuid[i], getValue));
    }
}

class VoluntaryScaleDownUpgrade : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerNum_;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = "-shared_memory_size_mb=10 -v=2";

        for (size_t i = 0; i < workerNum_; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    std::string GetWorkerUuid(size_t idx)
    {
        if (idx >= workerNum_) {
            return "";
        }
        uuidMap_.clear();
        GetWorkerUuids();
        HostPort workerHost;
        workerHost.ParseString(workerAddress_[idx]);
        return uuidMap_[workerHost];
    }

    void GetUuidAddrMap()
    {
        uuid2DestAddr_.clear();
        addr2Uuid_.clear();
        std::string value;
        DS_ASSERT_OK(db_->Get(ETCD_RING_PREFIX, "", value));
        HashRingPb ring;
        ring.ParseFromString(value);
        for (auto &it : ring.key_with_worker_id_meta_map()) {
            uuid2DestAddr_[it.first] = it.second;
        }
        for (auto &it : ring.update_worker_map()) {
            addr2Uuid_[it.first] = it.second.worker_uuid();
        }
    }

protected:
    const size_t workerNum_ = 2;
    std::unordered_map<std::string, std::string> uuid2DestAddr_;
    std::unordered_map<std::string, std::string> addr2Uuid_;
};

TEST_F(VoluntaryScaleDownUpgrade, ReuseUuid)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    SetWorkerHashInjection({ 0, 1 });
    auto worker0Uuid = GetWorkerUuid(0);
    InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
    int cnt = 10;
    std::vector<std::string> keys(cnt);
    std::vector<std::string> values(cnt);
    SetUuidObject(client1_, 0, keys, values);
    VoluntaryScaleDownInject(0);
    sleep(10);                            // wait 10s for w0 scale down finish
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    for (const auto &key : keys) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(key, val));
    }
    GetUuidAddrMap();
    ASSERT_TRUE(uuid2DestAddr_.find(worker0Uuid) != uuid2DestAddr_.end());
    ASSERT_TRUE(addr2Uuid_.find(workerAddress_[0]) != addr2Uuid_.end());

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0 }));
    WaitAllNodesJoinIntoHashRing(2, 20);       // Wait for w0 to join the hash ring within 20s.
    ASSERT_EQ(worker0Uuid, GetWorkerUuid(0));  // reuse worker0 uuid
    GetUuidAddrMap();
    ASSERT_TRUE(uuid2DestAddr_.empty());
    ASSERT_TRUE(addr2Uuid_.empty());

    InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
    for (const auto &key : keys) {
        std::string val;
        DS_ASSERT_OK(client0_->Get(key, val));
    }
}

TEST_F(VoluntaryScaleDownUpgrade, UuidCleanUp)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }, " -rolling_update_timeout_s=5"));
    SetWorkerHashInjection({ 0, 1 });
    auto worker0Uuid = GetWorkerUuid(0);
    InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
    int cnt = 10;
    std::vector<std::string> keys(cnt);
    std::vector<std::string> values(cnt);
    SetUuidObject(client1_, 0, keys, values);
    VoluntaryScaleDownInject(0);
    sleep(10);                            // sleep 10 to wait w0 scale down finish
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    for (const auto &key : keys) {
        std::string val;
        DS_ASSERT_OK(client1_->Get(key, val));
    }
    GetUuidAddrMap();
    ASSERT_TRUE(uuid2DestAddr_.find(worker0Uuid) != uuid2DestAddr_.end());
    ASSERT_TRUE(addr2Uuid_.empty());  // worker0Uuid clean up by w1

    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0 }));
    WaitAllNodesJoinIntoHashRing(2, 20);       // Wait for w0 to join the hash ring within 20s.
    ASSERT_NE(worker0Uuid, GetWorkerUuid(0));  // worker0 generate new uuid
    GetUuidAddrMap();
    ASSERT_TRUE(uuid2DestAddr_.find(worker0Uuid) != uuid2DestAddr_.end());

    InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
    for (const auto &key : keys) {
        std::string val;
        DS_ASSERT_OK(client0_->Get(key, val));
    }
}

TEST_F(VoluntaryScaleDownUpgrade, LEVEL1_RestartRestoreScaleDown)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady(
        { 0, 1 },
        " -inject_actions=test.start.notWait:call(0);"
        "EtcdClusterManager.IfNeedTriggerReconciliation.noreconciliation:return(K_OK)"));
    SetWorkerHashInjection({ 0, 1 });
    GetWorkerUuids();
    GetHashOnWorker(workerNum_);
    int cnt = 10;
    std::vector<std::string> keys0(cnt);
    std::vector<std::string> keys0WithUuid(cnt);
    std::vector<std::string> values(cnt);
    InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
    SetNormalObject(client1_, 0, keys0, values);
    SetUuidObject(client1_, 0, keys0WithUuid, values);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "1*sleep(5000)"));
    VoluntaryScaleDownInject(0);
    sleep(5);  // wait 5s for w0 migrate meta
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
    sleep(2);  // wait 2s for w0 exit
    // Restart w0, w0 should restore scale down task.
    DS_ASSERT_OK(externalCluster_->StartWorker(0, HostPort()));
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    for (int i = 0; i < cnt; i++) {
        std::string getValue;
        DS_ASSERT_OK(client1_->Get(keys0[i], getValue));
        DS_ASSERT_OK(client1_->Get(keys0WithUuid[i], getValue));
    }
}

TEST_F(VoluntaryScaleDownUpgrade, DISABLED_AddNodeDuringScaleDown)
{
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 0, 1 }));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ScaleUpTask.NotRunVoluntaryDownTask", "1*sleep(100000)"));
    VoluntaryScaleDownInject(0);
    sleep(1);
    DS_ASSERT_OK(externalCluster_->StartWorkerAndWaitReady({ 2 }));
    DS_ASSERT_OK(externalCluster_->KillWorker(0));
}

class VoluntaryScaleDownAsyncL2DataQueueTest : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 2;  // worker num is 2
        opts.numOBS = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=2 -enable_lossless_data_exit_mode=true ";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
        InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
        GetHashOnWorker(cluster_->GetWorkerNum());
        GetWorkerUuids();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
};

TEST_F(VoluntaryScaleDownAsyncL2DataQueueTest, TestAsyncDataQueueMigrate)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.save", "return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AsyncSendManager.CheckHealth", "return()"));
    uint32_t objCount = 100;
    std::unordered_set<std::string> keys;
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    std::string val = "test";
    for (uint32_t i = 0; i < objCount; i++) {
        auto key = client0_->GenerateKey("");
        ASSERT_FALSE(key.empty());
        keys.emplace(std::move(key));
    }
    for (const auto &key : keys) {
        // generate meta table metadata
        DS_ASSERT_OK(client0_->Set(key, val, param));
    }

    // scale down w0, and the async queue would migrate to w1.
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    client0_.reset();

    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    HostPort w0Addr;
    cluster_->GetWorkerAddr(0, w0Addr);
    WaitNodeVoluntaryScaleDownDone(w0Addr.ToString(), 20);  // wait 20s.

    for (const auto &key : keys) {
        // generate meta table metadata
        std::string getVal;
        DS_ASSERT_OK(client1_->Get(key, getVal));
        ASSERT_EQ(getVal, val);
    }
    client1_.reset();
}

TEST_F(VoluntaryScaleDownAsyncL2DataQueueTest, TestAsyncDataQueueMigrateAfterGet)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.save", "return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "AsyncSendManager.CheckHealth", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "persistence.service.del", "return(K_OK)"));
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    std::string key = "key111";
    std::string val = "val";
    DS_ASSERT_OK(client0_->Set(key, val, param));
    std::string getVal;
    DS_ASSERT_OK(client1_->Get(key, getVal));

    // scale down w0, and the async queue would migrate to w1.
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    client0_.reset();

    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    HostPort w0Addr;
    cluster_->GetWorkerAddr(0, w0Addr);
    WaitNodeVoluntaryScaleDownDone(w0Addr.ToString(), 20);  // wait 20s.

    DS_ASSERT_OK(client1_->Del(key));
    sleep(2);  // sleep 2s to wait for async global cache delete.

    EtcdStore store(cluster_->GetEtcdAddrs());
    DS_ASSERT_OK(store.Init());
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX));
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    ASSERT_EQ(store
                  .RangeSearch(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_HASH_SUFFIX, HashToStr(0),
                               HashToStr(UINT32_MAX), outKeyValues)
                  .GetCode(),
              StatusCode::K_NOT_FOUND);
}

class VoluntaryScaleDownAsyncL2MetaQueueTest : public KVClientVoluntaryScaleDownTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientVoluntaryScaleDownTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 2;  // worker num is 2
        opts.numOBS = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=5120 -v=2 -enable_lossless_data_exit_mode=true ";
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < cluster_->GetWorkerNum(); i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
        InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
        GetHashOnWorker(cluster_->GetWorkerNum());
        GetWorkerUuids();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }
};

TEST_F(VoluntaryScaleDownAsyncL2MetaQueueTest, TestMigrateAsyncToEtcdMetadata)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "meta_async_queue.poll", "return()"));
    uint32_t objCount = 100;
    std::unordered_set<std::string> keys;
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    std::string val = "test";
    for (uint32_t i = 0; i < objCount; i++) {
        auto key = client0_->GenerateKey("");
        ASSERT_FALSE(key.empty());
        keys.emplace(std::move(key));
    }
    for (const auto &key : keys) {
        // generate meta table metadata
        DS_ASSERT_OK(client1_->Set(key, val, param));
    }
    for (const auto &key : keys) {
        // generate location metadata
        DS_ASSERT_OK(client0_->Get(key, val));
    }

    // scale down w0, and the async queue would migrate to w1.
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    client0_.reset();
    client1_.reset();
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    HostPort w0Addr;
    cluster_->GetWorkerAddr(0, w0Addr);
    WaitNodeVoluntaryScaleDownDone(w0Addr.ToString(), 20);  // wait 20s.

    EtcdStore store(cluster_->GetEtcdAddrs());
    DS_ASSERT_OK(store.Init());
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    DS_ASSERT_OK(store.RangeSearch(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, HashToStr(0),
                                   HashToStr(UINT32_MAX), outKeyValues));
    ASSERT_EQ(outKeyValues.size(), objCount);
    for (const auto &pair : outKeyValues) {
        auto pos = pair.first.find_last_of("/");
        ASSERT_TRUE(pos != std::string::npos);
        auto key = pair.first.substr(pos + 1);
        ASSERT_TRUE(keys.find(key) != keys.end());
    }

    outKeyValues.clear();
    ASSERT_EQ(store
                  .RangeSearch(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, HashToStr(0),
                               HashToStr(UINT32_MAX), outKeyValues)
                  .GetCode(),
              K_NOT_FOUND);
}

TEST_F(VoluntaryScaleDownAsyncL2MetaQueueTest, TestMigrateAsyncToEtcdMetadataWithRPCError)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "meta_async_queue.poll", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.streamSendData", "2*return()"));

    uint32_t objCount = 100;
    std::unordered_set<std::string> keys;
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    std::string val = "test";
    for (uint32_t i = 0; i < objCount; i++) {
        auto key = client0_->GenerateKey("");
        ASSERT_FALSE(key.empty());
        keys.emplace(std::move(key));
    }
    for (const auto &key : keys) {
        // generate meta table metadata
        DS_ASSERT_OK(client1_->Set(key, val, param));
    }
    for (const auto &key : keys) {
        // generate location metadata
        DS_ASSERT_OK(client0_->Get(key, val));
    }

    // scale down w0, and the async queue would migrate to w1.
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    client0_.reset();
    client1_.reset();
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    HostPort w0Addr;
    cluster_->GetWorkerAddr(0, w0Addr);
    WaitNodeVoluntaryScaleDownDone(w0Addr.ToString(), 20);  // wait 20s.

    EtcdStore store(cluster_->GetEtcdAddrs());
    DS_ASSERT_OK(store.Init());
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    DS_ASSERT_OK(store.RangeSearch(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, HashToStr(0),
                                   HashToStr(UINT32_MAX), outKeyValues));
    ASSERT_EQ(outKeyValues.size(), objCount);
    for (const auto &pair : outKeyValues) {
        auto pos = pair.first.find_last_of("/");
        ASSERT_TRUE(pos != std::string::npos);
        auto key = pair.first.substr(pos + 1);
        ASSERT_TRUE(keys.find(key) != keys.end());
    }

    outKeyValues.clear();
    ASSERT_EQ(store
                  .RangeSearch(std::string(ETCD_LOCATION_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, HashToStr(0),
                               HashToStr(UINT32_MAX), outKeyValues)
                  .GetCode(),
              K_NOT_FOUND);
}

TEST_F(VoluntaryScaleDownAsyncL2MetaQueueTest, LEVEL2_TestMigrateAsyncDelTypeToEtcdMetadata)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "meta_async_queue.poll", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "global_cache_delete.delete_objects", "return()"));
    uint32_t objCount = 100;
    std::unordered_set<std::string> keys;
    SetParam param;
    param.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    std::string val = "test";
    for (uint32_t i = 0; i < objCount; i++) {
        auto key = client0_->GenerateKey("");
        ASSERT_FALSE(key.empty());
        keys.emplace(std::move(key));
    }
    // Set and del the keys, then it will leave global cache delete etcd key need to migrate to w1.
    for (const auto &key : keys) {
        // generate location metadata
        DS_ASSERT_OK(client0_->Set(key, val, param));
    }
    sleep(2);  // sleep 2s wait for ready
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client0_->Del(std::vector<std::string>(keys.begin(), keys.end()), failedKeys));

    // scale down w0, and the async queue would migrate to w1.
    kill(cluster_->GetWorkerPid(0), SIGTERM);
    client0_.reset();
    client1_.reset();
    WaitAllNodesJoinIntoHashRing(1, 20);  // Wait for w0 to exit the hash ring within 20s.
    HostPort w0Addr;
    cluster_->GetWorkerAddr(0, w0Addr);
    WaitNodeVoluntaryScaleDownDone(w0Addr.ToString(), 20);  // wait 20s.
    sleep(2);                                               // sleep 2s wait for global cache delete.
    EtcdStore store(cluster_->GetEtcdAddrs());
    DS_ASSERT_OK(store.Init());
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    DS_ASSERT_OK(store.CreateTable(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX,
                                   std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX));
    std::vector<std::pair<std::string, std::string>> outKeyValues;
    ASSERT_EQ(store
                  .RangeSearch(std::string(ETCD_META_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, HashToStr(0),
                               HashToStr(UINT32_MAX), outKeyValues)
                  .GetCode(),
              K_NOT_FOUND);
    int count = 10;
    int curr = 0;
    StatusCode code;
    do {
        code = store
                   .RangeSearch(std::string(ETCD_GLOBAL_CACHE_TABLE_PREFIX) + ETCD_WORKER_SUFFIX, HashToStr(0),
                                HashToStr(UINT32_MAX), outKeyValues)
                   .GetCode();
        curr++;
    } while (curr < count && code == StatusCode::K_OK);
    ASSERT_EQ(code, K_NOT_FOUND);
}

class VoluntaryScaleDownRedirectTest : public VoluntaryScaleDownAsyncL2MetaQueueTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numWorkers = 4; // worker num is 4
        opts.addNodeTime = 0;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = FormatString(
            " -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d -client_reconnect_wait_s=1", timeoutS_, deadTimeoutS_);
        opts.waitWorkerReady = false;
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        GetHashOnWorker();
        GetWorkerUuids();
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    void TearDown() override
    {
        client0_.reset();
        client1_.reset();
        db_.reset();
        ExternalClusterTest::TearDown();
    }

protected:
    int timeoutS_ = 3;
    int deadTimeoutS_ = 5;
};

TEST_F(VoluntaryScaleDownRedirectTest, DISABLED_ScaleUpDelayRedirect)
{
    Timer timer;
    InitTestKVClient(0, client_);
    auto key = client_->Set("oooooo");
    client1_.reset();
    client_.reset();
    DS_ASSERT_OK(externalCluster_->QuicklyShutdownWorker(0));
    WaitAllNodesJoinIntoHashRing(3); // worker num is 3
    auto t1 = timer.ElapsedSecond();
    HashRingPb ring;
    std::string hashRingStr;
    auto trueRingTable = ETCD_RING_PREFIX;
    DS_ASSERT_OK(db_->Get(trueRingTable, "", hashRingStr));
    ASSERT_TRUE(ring.ParseFromString(hashRingStr));
    auto standbyWorker = ring.key_with_worker_id_meta_map().begin()->second;
    std::vector<size_t> aliveWorkerIndexs;
    size_t voluntaryDownIndex = 0;
    for (size_t i = 1; i < workerAddress_.size(); i++) {
        if (workerAddress_[i] == standbyWorker) {
            voluntaryDownIndex = i;
        } else {
            aliveWorkerIndexs.emplace_back(i);
        }
    }
    auto t2 = timer.ElapsedSecond();
    VoluntaryScaleDownInject(voluntaryDownIndex);
    InitTestKVClient(voluntaryDownIndex, client_, 3000); // timeout is 3000 ms.
    auto key1 = client_->Set("oooooo");
    client_.reset();
    std::ostringstream cmd;
    cmd << "call(" << workerAddress_[aliveWorkerIndexs[1]] << ")";
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, aliveWorkerIndexs[1], "HashRing.UpdateRing.sleep", "sleep(5000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, voluntaryDownIndex, "GetStandbyWorkerExceptNoLock", cmd.str()));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, voluntaryDownIndex));
    WaitAllNodesJoinIntoHashRing(2); // worker num is 2
    auto t3 = timer.ElapsedSecond();
    InitTestKVClient(aliveWorkerIndexs[0], client_, 3000); // timeout is 3000 ms.
    DS_ASSERT_OK(client_->Set(key, "pppppppppppppp"));
    std::string getValue;
    DS_ASSERT_OK(client_->Get(key, getValue));
    client_.reset();
    auto t4 = timer.ElapsedSecond();
    LOG(INFO) << "--------" << t1 << "----------" << t2 << "-----------" << t3 << "----------" << t4;
}

class KVCacheClientWorkerNotExitTest : public KVCacheClientClusterState {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = 2; // num worker is 2
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_ADD_TIME;
        opts.workerGflagParams =
            " -v=2 -node_timeout_s=2 -node_dead_timeout_s=5 -auto_del_dead_node=true";
        for (size_t i = 0; i < 2; i++) { // num worker is 2
            opts.workerConfigs.emplace_back(HOST_IP_PREFIX + std::to_string(i), GetFreePort());
            workerHost_.emplace_back(HOST_IP_PREFIX + std::to_string(i));
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < 2; i++) { // num worker is 2
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
        InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
    }

    bool IsProcessAlive(pid_t pid)
    {
        int status;
        auto ret = waitpid(pid, &status, WNOHANG);

        bool alive;
        if (ret == -1) {
            alive = false;
        } else if (ret == 0) {
            alive = true;
        } else {
            alive = false;
        }
        return alive;
    }
};

TEST_F(KVCacheClientWorkerNotExitTest, WorkerNotExit)
{
    std::string value;
    for (int i = 0; i < 20; i++) {  // Generate 20 objects
        auto key = "a_key_for_test_" + std::to_string(i);
        DS_ASSERT_OK(client0_->Set(key, "vvvvvvvvvv"));
    }
    client0_.reset();
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "BatchMigrateMetadata.delay", "call(5)"));
    std::thread t1([this] {
        sleep(2); // delay 2 s voluntary scale down w1
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "InspectAndProcessPeriodically.skip", "return()"));
        VoluntaryScaleDownInject(1);
    });
    VoluntaryScaleDownInject(0);
    t1.join();
    while (true) {
        if (!IsProcessAlive(cluster_->GetWorkerPid(0)) && !IsProcessAlive(cluster_->GetWorkerPid(1))) {
            break;
        }
        sleep(1);
    }
}

TEST_F(KVClientVoluntaryScaleDownTest, CreateClientWithServiceDiscoveryDuringScaleDown)
{
    // write data into worker0.
    int objectCnt = 10;
    std::vector<std::string> objectKey(objectCnt);
    std::vector<std::string> data(objectCnt);
    SetNormalObject(client0_, 0, objectKey, data);

    // Thread 1: scale down to have only worker3.
    std::thread scaleDownThread([this] {
        VoluntaryScaleDownInject(0);
        VoluntaryScaleDownInject(1);
        VoluntaryScaleDownInject(2);
    });

    // Thread 2: create client via Service Discovery and do some set/get.
    std::thread clientCreateThread([this, &objectKey, &objectCnt, &data] {
        sleep(1);

        // initialize ServiceDiscovery and client.
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()) {
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        ServiceDiscoveryOptions opts;
        opts.etcdAddress = etcdAddress;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(opts);
        DS_ASSERT_OK(serviceDiscovery->Init());
        ConnectOptions connectOptions;
        connectOptions.connectTimeoutMs = 3000;
        connectOptions.requestTimeoutMs = 0;
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        connectOptions.serviceDiscovery = serviceDiscovery;
        std::shared_ptr<KVClient> newClient = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(newClient->Init());

        // do set/get with new client.
        std::string newKey = "new_key_" + GetStringUuid();
        std::string newVal = "new_val";
        DS_ASSERT_OK(newClient->Set(newKey, newVal));
        for (int i = 0; i < objectCnt; ++i) {
            std::string getValue;
            DS_ASSERT_OK(newClient->Get(objectKey[i], getValue));
            ASSERT_EQ(data[i], getValue);
        }
        std::string readBack;
        DS_ASSERT_OK(newClient->Get(newKey, readBack));
        ASSERT_EQ(newVal, readBack);
    });

    scaleDownThread.join();
    clientCreateThread.join();

    // wait for 3 seconds and validate activate worker count.
    sleep(3);
    AssertWorkerNum(1);
}
}  // namespace st
}  // namespace datasystem

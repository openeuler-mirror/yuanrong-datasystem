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
#include <gtest/gtest.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "client/object_cache/oc_client_common.h"
#include "client/kv_cache/kv_client_scale_common.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace st {

constexpr size_t DEFAULT_WORKER_NUM = 4;
const size_t MASTER_NUM = 2;
const std::string AZ1 = "AZ1";
const std::string AZ2 = "AZ2";

class KVClientCrossAZTest : virtual public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        // worker 0, 2 use the same master address, which is worker 0 address.
        // worker 1, 3 use the same master address, which is worker 1 address.
        opts.crossAZMap = { { 0, 0 }, { 1, 1 }, { 2, 0 }, { 3, 1 } };
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.numMasters = MASTER_NUM;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        std::string OBSGflag = FormatString(
            "-other_cluster_names=AZ1,AZ2,AZ3 "
            "-v=2 "
            "-cross_cluster_get_meta_from_worker=%s -inject_actions=TryGetObjectFromRemote.NoRetry:call() ",
            crossAzGetMetaFromWorker_) + appendCmd_;

        opts.workerGflagParams = OBSGflag;
        for (size_t i = 0; i < workerNum_; i++) {
            std::string param = "-cluster_name=";
            if (i % MASTER_NUM == 0) {
                param.append(AZ1);
            } else {
                param.append(AZ2);
            }
            opts.workerSpecifyGflagParams[i] += param;
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
        etcdStore_ = std::make_unique<EtcdStore>(etcdAddress);
        if ((etcdStore_ != nullptr) && (etcdStore_->Init().IsOk())) {
            std::string az1RingStr = "/" + AZ1 + std::string(ETCD_RING_PREFIX);
            std::string az2RingStr = "/" + AZ2 + std::string(ETCD_RING_PREFIX);
            etcdStore_->DropTable(az1RingStr);
            etcdStore_->DropTable(az2RingStr);
            (void)etcdStore_->CreateTable(az1RingStr, az1RingStr);
            (void)etcdStore_->CreateTable(az2RingStr, az2RingStr);
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
    }

    void TearDown() override
    {
        etcdStore_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitConnectOpt(uint32_t workerIndex, ConnectOptions &connectOptions, const std::string &tenantId,
                        int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.tenantId = tenantId;
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    }

    void InitTestKVClientWithTenantId(uint32_t workerIndex, std::shared_ptr<KVClient> &client,
                                         const std::string &tenantId)
    {
        ConnectOptions connectOptions;
        InitConnectOpt(workerIndex, connectOptions, tenantId);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void BasicTestKey(bool withWorkerId, size_t keyNum, size_t dataSize, const std::string &tenantId = "")
    {
        std::shared_ptr<KVClient> client0;
        std::shared_ptr<KVClient> client1;
        InitTestKVClientWithTenantId(0, client0, tenantId);
        InitTestKVClientWithTenantId(1, client1, tenantId);
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };

        std::map<std::string, std::string> dataMap;

        for (size_t i = 0; i < keyNum; i++) {
            std::string key;
            std::string value = "value" + std::to_string(i) + randomData_.GetRandomString(dataSize);
            if (withWorkerId) {
                key = client0->Set(value, param);
                ASSERT_NE(key, "");
            } else {
                key = "key" + std::to_string(i);
                DS_ASSERT_OK(client0->Set(key, value, param));
            }
            LOG(INFO) << "generate key: " << key;
            dataMap[key] = value;
        }

        for (const auto &pair : dataMap) {
            std::string key = pair.first;
            std::string value = pair.second;
            std::string OutValueCrossAZ;
            // AZ2 can get from AZ1
            DS_ASSERT_OK(client1->Get(key, OutValueCrossAZ));
            ASSERT_EQ(OutValueCrossAZ, value);

            std::string newValue = "newvalue:" + key;
            DS_ASSERT_OK(client0->Set(key, newValue, param));
            // AZ2 can get new value from AZ1
            DS_ASSERT_OK(client1->Get(key, OutValueCrossAZ));
            ASSERT_EQ(OutValueCrossAZ, newValue);
            DS_ASSERT_OK(client1->Del(key));
            DS_ASSERT_NOT_OK(client0->Get(key, OutValueCrossAZ));
            // AZ2 get value fail from AZ1 after the object is delete
            DS_ASSERT_OK(client0->Del(key));
            DS_ASSERT_NOT_OK(client1->Get(key, OutValueCrossAZ));
        }
    }

protected:
    ExternalCluster *externalCluster_ = nullptr;
    std::unique_ptr<EtcdStore> etcdStore_;
    const int32_t timeoutMs_ = 10'000;
    std::string crossAzGetMetaFromWorker_ = "true";
    std::string appendCmd_ = "";
    size_t workerNum_ = DEFAULT_WORKER_NUM;
};

TEST_F(KVClientCrossAZTest, LEVEL2_DISABLED_MasterOnOtherAzOfflineWorkerKey)
{
    std::shared_ptr<KVClient> az2client1;
    InitTestKVClient(1, az2client1, timeoutMs_);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };

    std::string workerKey;
    (void)az2client1->GenerateKey("worker_key", workerKey);
    std::string valVer0 = RandomData().GetRandomString(10 * 1024 * 1024);
    DS_ASSERT_OK(az2client1->Set(workerKey, valVer0, param));

    az2client1.reset();
    ASSERT_TRUE(cluster_->QuicklyShutdownWorker(1));

    std::shared_ptr<KVClient> az1client0;
    InitTestKVClient(0, az1client0, timeoutMs_);
    std::string valVer1 = "value_version1";
    DS_ASSERT_NOT_OK(az1client0->Set(workerKey, valVer1, param));  // unique master crash, can not modify
    DS_ASSERT_OK(az1client0->Del(workerKey));  // unique master crash, del is ok. (del from l2cache and mark an async
                                               // request to master)
    sleep(2);  // 2s, wait for async global delete

    // the previous del literally remove the data, so that get-op return K_NOT_FOUND on az1
    std::string outVal;
    DS_ASSERT_NOT_OK(az1client0->Get(workerKey, outVal));
    // the previous del literally remove the data, so that get-op return K_NOT_FOUND on az2
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->StartWorkerAndWaitReady({1}));
    sleep(2);  // 2s, wait for async notify
    InitTestKVClient(1, az2client1, timeoutMs_);
    ASSERT_EQ(az2client1->Get(workerKey, outVal).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(KVClientCrossAZTest, LEVEL2_DISABLED_MasterOnOtherAzOfflineHashKey)
{
    const int AZ2_WORKER1 = 1;
    const int AZ2_WORKER3 = 3;
    std::shared_ptr<KVClient> az2client1;
    InitTestKVClient(AZ2_WORKER1, az2client1, timeoutMs_);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };

    std::string hashKey1 = "hash_key1";
    std::string hashKey2 = "hash_key2";
    std::string valVer0 = RandomData().GetRandomString(10 * 1024 * 1024);
    DS_ASSERT_OK(az2client1->Set(hashKey1, valVer0, param));
    DS_ASSERT_OK(az2client1->Set(hashKey2, valVer0, param));

    az2client1.reset();
    ASSERT_TRUE(cluster_->QuicklyShutdownWorker(AZ2_WORKER1));
    ASSERT_TRUE(cluster_->QuicklyShutdownWorker(AZ2_WORKER3));

    std::shared_ptr<KVClient> az1client0;
    InitTestKVClient(0, az1client0, timeoutMs_);
    std::string valVer1 = "value_version1";

    // hashkey1: non-unique master crash, modify ok(generate an async request to master), del ok as key on current az.
    DS_ASSERT_OK(az1client0->Set(hashKey1, valVer1, param));
    DS_ASSERT_OK(az1client0->Del(hashKey1));
    // hashkey2: non-unique master crash, del ok as key on other az(generate an async request to master)
    DS_ASSERT_OK(az1client0->Del(hashKey2));
    sleep(2);  // 2s, wait for async global delete

    // the previous del literally remove the data, so that get-op return K_NOT_FOUND on az1
    std::string outVal;
    ASSERT_EQ(az1client0->Get(hashKey1, outVal).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(az1client0->Get(hashKey2, outVal).GetCode(), StatusCode::K_NOT_FOUND);

    // the previous del literally remove the data, so that get-op return K_NOT_FOUND on az2
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->StartWorkerAndWaitReady({AZ2_WORKER1, AZ2_WORKER3}));
    std::shared_ptr<KVClient> az2client3;
    InitTestKVClient(AZ2_WORKER1, az2client1, timeoutMs_);
    InitTestKVClient(AZ2_WORKER3, az2client3, timeoutMs_);
    sleep(2);  // 2s, wait for async notify
    ASSERT_EQ(az2client1->Get(hashKey1, outVal).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(az2client1->Get(hashKey2, outVal).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(az2client3->Get(hashKey1, outVal).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(az2client3->Get(hashKey2, outVal).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(KVClientCrossAZTest, DISABLED_ShmMemWithoutWorkerId)
{
    BasicTestKey(false, 10, 10 * 1024 * 1024);
    BasicTestKey(false, 10, 10);
}

TEST_F(KVClientCrossAZTest, DISABLED_ShmMemWithWorkerId)
{
    BasicTestKey(true, 10, 10 * 1024 * 1024);
    BasicTestKey(true, 10, 10);
}

TEST_F(KVClientCrossAZTest, EXCLUSIVE_ParrelGetTheSameKey)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };

    std::string key = "simple_key";
    std::string value = RandomData().GetRandomString(10 * 1024 * 1024);

    client0->Set(key, value, param);
    std::string OutValueCrossAZ;
    DS_ASSERT_OK(client0->Get(key, OutValueCrossAZ));
    size_t threadNum = 30;
    std::unique_ptr<ThreadPool> pool = std::make_unique<ThreadPool>(threadNum);
    for (size_t i = 0; i < threadNum; i++) {
        pool->Execute([this, &key, &value]() {
            std::shared_ptr<KVClient> client;
            InitTestKVClient(1, client);
            std::string valueGet;
            DS_ASSERT_OK(client->Get(key, valueGet));
            ASSERT_EQ(value, valueGet);
        });
    }
}

TEST_F(KVClientCrossAZTest, DISABLED_LEVEL1_TestErrorMsgWhenEtcdFailed)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);
    DS_ASSERT_OK(externalCluster_->ShutdownEtcds());
    std::string valToGet;
    auto status = client0->Get("not_exist", valToGet);
    ASSERT_EQ(status.GetCode(), K_NOT_FOUND);
}

TEST_F(KVClientCrossAZTest, TestQuerySize)
{
    std::shared_ptr<KVClient> az2client1;
    InitTestKVClient(1, az2client1, timeoutMs_);
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };

    std::string workerKey;
    DS_ASSERT_OK(az2client1->GenerateKey("worker_key", workerKey));
    std::string valVer0 = RandomData().GetRandomString(10 * 1024 * 1024);
    DS_ASSERT_OK(az2client1->Set(workerKey, valVer0, param));

    std::shared_ptr<KVClient> az1client0;
    InitTestKVClient(0, az1client0, timeoutMs_);
    std::vector<uint64_t> outSizes;
    ASSERT_EQ(az1client0->QuerySize({ workerKey }, outSizes), Status::OK());
    ASSERT_EQ(outSizes.size(), 1);
    ASSERT_EQ(outSizes[0], valVer0.size());
}

TEST_F(KVClientCrossAZTest, LEVEL2_TestQuerySizeOnEtcd)
{
    std::shared_ptr<KVClient> az2client1;
    InitTestKVClient(1, az2client1, timeoutMs_);
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };

    std::string workerKey = az2client1->GenerateKey("worker_key");
    std::string valVer0 = RandomData().GetRandomString(10 * 1024 * 1024);
    DS_ASSERT_OK(az2client1->Set(workerKey, valVer0, param));
    ASSERT_TRUE(cluster_->ShutdownNode(WORKER, 1));
    int64_t waitShutDownTime = 2;
    sleep(waitShutDownTime);
    auto externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
    DS_ASSERT_OK(externalCluster->StartWorkerAndWaitReady({ 1 }));
    std::shared_ptr<KVClient> az1client0;
    InitTestKVClient(0, az1client0, timeoutMs_);
    std::vector<uint64_t> outSizes;
    ASSERT_EQ(az1client0->QuerySize({ workerKey }, outSizes), Status::OK());
    ASSERT_EQ(outSizes.size(), 1);
    ASSERT_EQ(outSizes[0], valVer0.size());
}
class KVClientCannotCrossAzGetMetaFromWorkerTest : public KVClientCrossAZTest, public KVClientScaleCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientCrossAZTest::crossAzGetMetaFromWorker_ = "false";
        KVClientCrossAZTest::appendCmd_ =
            FormatString("-node_timeout_s=%d -node_dead_timeout_s=%d", nodeTimeOutS_, nodeDeadTimeOutS_);
        KVClientCrossAZTest::SetClusterSetupOptions(opts);
    }

private:
    const int nodeTimeOutS_ = 3;
    const int nodeDeadTimeOutS_ = 5;
};

TEST_F(KVClientCannotCrossAzGetMetaFromWorkerTest, LEVEL1_TestL2CacheLoss)
{
    std::shared_ptr<KVClient> az2client1;
    InitTestKVClient(1, az2client1, timeoutMs_);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    auto key = az2client1->Set("val", param);
    ASSERT_TRUE(!key.empty());
    // meta: w1/etcd; data: w1/obs
    // remove data in obs
    LOG(INFO) << externalCluster_->GetExternalOptions().rootDir;
    DS_ASSERT_OK(RemoveAll(externalCluster_->GetExternalOptions().rootDir + "/OBS/"));
    // meta: w1/etcd; data: w1
    std::shared_ptr<KVClient> az1client1;
    int timeoutMs = 3'000;
    // This client will get data that does not exist, so the timeout of rpc needs to be shortened.
    InitTestKVClient(0, az1client1, timeoutMs);
    std::string valToGet;
    // n the case of Flag_cross_cluster_get_meta_from_worker=false
    // because we cannot update the metadata on the master of
    // other az, the data will not be cached locally
    DS_ASSERT_OK(az1client1->Get(key, valToGet));
    // meta: w1/etcd; data: w1
    az2client1.reset();
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, 1));
    // meta: w3/etcd; data:
    int waitTimeoutS = 10;
    KVClientScaleCommon::InitTestEtcdInstance({"AZ1", "AZ2"});
    WaitAllNodesJoinIntoHashRing(1, waitTimeoutS, "AZ2");
    DS_ASSERT_NOT_OK(az1client1->Get(key, valToGet));
}

class AZTestWithTanent : public KVClientCrossAZTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        // worker 0, 2 use the same master address, which is worker 0 address.
        // worker 1, 3 use the same master address, which is worker 1 address.
        opts.crossAZMap = { { 0, 0 }, { 1, 1 }, { 2, 0 }, { 3, 1 } };
        opts.numEtcd = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.numMasters = MASTER_NUM;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        std::string obsGflag =
            "-other_cluster_names=AZ1,AZ2,AZ3 "
            "-system_access_key=datasystem_ak "
            "-system_secret_key=datasystem_ak "
            "-authorization_enable=true "
            "-v=2 "
            "-cross_cluster_get_meta_from_worker=true ";

        opts.workerGflagParams = obsGflag;
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            std::string param = "-cluster_name=";
            if (i % MASTER_NUM == 0) {
                param.append(AZ1);
            } else {
                param.append(AZ2);
            }
            opts.workerSpecifyGflagParams[i] = param;
        }
    }
};

TEST_F(AZTestWithTanent, DISABLED_LEVEL1_BasicTestKeyWithTenantId)
{
    BasicTestKey(false, 5, 10 * 1024 * 1024, "teantId1");
    BasicTestKey(true, 5, 10 * 1024 * 1024, "teantId1");
    BasicTestKey(false, 5, 10, "teantId2");
    BasicTestKey(true, 5, 10, "teantId2");
}

const std::string HOST_IP = "127.0.0.1";
constexpr int SCALE_RESTART_ADD_TIME = 2;
constexpr int EACH_AZ_WORKER_NUM = 2;
class KVClientCrossAzReadTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = DEFAULT_WORKER_NUM;
        opts.enableDistributedMaster = "true";
        opts.addNodeTime = SCALE_RESTART_ADD_TIME;
        std::string obsGflag =
            "-shared_memory_size_mb=5120 -node_timeout_s=3 -node_dead_timeout_s=8 -auto_del_dead_node=false "
            "-other_cluster_names=AZ1,AZ2 -v=1 -log_monitor=true";
        FLAGS_v = 1;
        opts.workerGflagParams = obsGflag;
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());

            std::string param = "-cluster_name=";
            if (i < EACH_AZ_WORKER_NUM) {
                param.append(AZ1);
            } else {
                param.append(AZ2);
            }
            opts.workerSpecifyGflagParams[i] = param;
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
            std::string az1RingStr = "/" + AZ1 + std::string(ETCD_RING_PREFIX);
            std::string az2RingStr = "/" + AZ2 + std::string(ETCD_RING_PREFIX);
            std::string az1ClusterStr = "/" + AZ1 + "/" + std::string(ETCD_CLUSTER_TABLE);
            db_->DropTable(az1RingStr);
            db_->DropTable(az2RingStr);
            db_->DropTable(az1ClusterStr);
            (void)db_->CreateTable(az1RingStr, az1RingStr);
            (void)db_->CreateTable(az2RingStr, az2RingStr);
            (void)db_->CreateTable(az1ClusterStr, az1ClusterStr);
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        InitTestEtcdInstance();
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < DEFAULT_WORKER_NUM; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "persistence.service.save", "100*return(K_OK)"));
            DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, i, "persistence.service.del", "100*return(K_OK)"));
        }
        InitTestKVClient(0, client0_, 2000);  // Init client0 to worker 0 with 2000ms timeout
        InitTestKVClient(1, client1_, 2000);  // Init client1 to worker 1 with 2000ms timeout
        InitTestKVClient(2, client2_, 2000);  // Init client2 to worker 2 with 2000ms timeout
        InitTestKVClient(3, client3_, 2000);  // Init client3 to worker 3 with 2000ms timeout
    }

    void GetHashOnWorker()
    {
        std::string az1RingStr = "/" + AZ1 + std::string(ETCD_RING_PREFIX);
        std::string az2RingStr = "/" + AZ2 + std::string(ETCD_RING_PREFIX);
        std::vector<std::string> azName{ az1RingStr, az2RingStr };
        for (uint32_t i = 0; i < azName.size(); ++i) {
            std::string value;
            db_->Get(azName[i], "", value);
            HashRingPb ring;
            ring.ParseFromString(value);
            auto tokens = ring.workers().at(workerAddress_[i << 1]).hash_tokens();
            workerHashValue_.emplace_back(*tokens.begin() - 1);
            tokens = ring.workers().at(workerAddress_[(i << 1) | 1]).hash_tokens();
            workerHashValue_.emplace_back(*tokens.begin() - 1);
        }
        ASSERT_EQ(workerHashValue_.size(), DEFAULT_WORKER_NUM);
    }

    void GetWorkerUuids()
    {
        std::string az1RingStr = "/" + AZ1 + std::string(ETCD_RING_PREFIX);
        std::string az2RingStr = "/" + AZ2 + std::string(ETCD_RING_PREFIX);
        std::vector<std::string> azName{ az1RingStr, az2RingStr };
        std::string value;
        for (auto &str : azName) {
            DS_ASSERT_OK(db_->Get(str, "", value));
            HashRingPb ring;
            ring.ParseFromString(value);
            for (auto worker : ring.workers()) {
                HostPort workerAddr;
                DS_ASSERT_OK(workerAddr.ParseString(worker.first));
                uuidMap_.emplace(std::move(workerAddr), worker.second.worker_uuid());
            }
        }
        ASSERT_EQ(uuidMap_.size(), DEFAULT_WORKER_NUM);
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath = FLAGS_log_dir;
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

    int GetWorkerNum(std::string azName)
    {
        std::string ringStr = "/" + azName + std::string(ETCD_RING_PREFIX);
        std::string value;
        db_->Get(ringStr, "", value);
        HashRingPb ring;
        ring.ParseFromString(value);
        return ring.workers_size();
    }

    void StartWorker(int start, int end)
    {
        for (int i = start; i <= end; ++i) {
            DS_ASSERT_OK(cluster_->StartNode(WORKER, i, ""));
        }
        for (int i = start; i <= end; ++i) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
    }

    void SetWorkerHashInjection()
    {
        for (uint32_t i = 0; i < DEFAULT_WORKER_NUM; ++i) {
            DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, i, "MurmurHash3", "return()"));
        }
    }

    void ClearGetInjection(int start, int end)
    {
        for (int i = start; i <= end; ++i) {
            DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, i, "persistence.service.get"));
        }
    }

    void SuccessToSetObject(std::string &objectKey1, std::string &objectKey2, std::string &data1, std::string &data2)
    {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        DS_ASSERT_OK(client2_->Set(objectKey1, data1, param));
        DS_ASSERT_OK(client2_->Set(objectKey2, data2, param));
        std::string getValue1, getValue2;
        DS_ASSERT_OK(client0_->Get(objectKey1, getValue1));
        DS_ASSERT_OK(client1_->Get(objectKey2, getValue2));
        ASSERT_EQ(data1, getValue1);
        ASSERT_EQ(data2, getValue2);
    }

    void SuccessToSetObjectAndHaveCopy(std::string &objectKey1, std::string &objectKey2, std::string &data1,
                                       std::string &data2)
    {
        SuccessToSetObject(objectKey1, objectKey2, data1, data2);
        std::string getValue1, getValue2;
        DS_ASSERT_OK(client3_->Get(objectKey1, getValue1));
        DS_ASSERT_OK(client3_->Get(objectKey2, getValue2));
        ASSERT_EQ(data1, getValue1);
        ASSERT_EQ(data2, getValue2);
    }

    void BeforeTimeOutGetFromL2Cache(std::string &objectKey1, std::string &objectKey2, std::string &data1,
                                     std::string &data2)
    {
        std::string getValue1, getValue2;
        DS_ASSERT_NOT_OK(client0_->Get(objectKey2, getValue1));
        DS_ASSERT_NOT_OK(client1_->Get(objectKey1, getValue2));
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data1)));
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, 1, "persistence.service.get", FormatString("return(%s)", data2)));
        DS_ASSERT_OK(client0_->Get(objectKey1, getValue1));
        DS_ASSERT_OK(client1_->Get(objectKey2, getValue2));
        ASSERT_EQ(data1, getValue1);
        ASSERT_EQ(data2, getValue2);
        ClearGetInjection(0, 1);
    }

    void GetObjectAfterTimeout(std::string &objectKey1, std::string &objectKey2, std::string &data1, std::string &data2)
    {
        std::string getValue1, getValue2;
        DS_ASSERT_OK(client0_->Get(objectKey2, getValue2));
        DS_ASSERT_NOT_OK(client1_->Get(objectKey1, getValue1));
        DS_ASSERT_OK(
            cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data1)));
        DS_ASSERT_OK(client0_->Get(objectKey1, getValue1));
        ASSERT_EQ(data1, getValue1);
        ASSERT_EQ(data2, getValue2);
        ClearGetInjection(0, 0);
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

protected:
    std::unique_ptr<EtcdStore> db_;
    std::vector<std::string> workerAddress_;
    std::vector<uint32_t> workerHashValue_;
    std::unordered_map<HostPort, std::string> uuidMap_;
    std::shared_ptr<KVClient> client0_, client1_, client2_, client3_;
};

TEST_F(KVClientCrossAzReadTest, LEVEL1_DISABLED_VoluntaryScaleDownEvent)
{
    std::string value;
    HostPort workerAddress;
    std::string objectKey1 = "object1";
    std::string getValue1;
    cluster_->GetWorkerAddr(0, workerAddress);

    VoluntaryScaleDownInject(0);
    client0_.reset();

    // wait for the worker process to hear the signal.
    sleep(3);  // Wait 3 seconds for voluntary scale down finished
    DS_ASSERT_OK(db_->Get("/" + AZ1 + "/" + std::string(ETCD_CLUSTER_TABLE),
                          workerAddress.Host() + ":" + std::to_string(workerAddress.Port()), value));
    auto pos = value.find(";");
    ASSERT_EQ(value.substr(pos + 1, value.length() - 1), "exiting");
    // wait 5 seconds for the worker to shutdown.
    sleep(5);
    ConnectOptions connectOptions;
    int32_t timeoutMs = 5'000; // 5s, faster identification of errors
    InitConnectOpt(0, connectOptions, timeoutMs);
    client0_ = std::make_shared<KVClient>(connectOptions);
    DS_ASSERT_NOT_OK(client0_->Init());
}

TEST_F(KVClientCrossAzReadTest, LEVEL1_DISABLED_NormalObjectReadCrossAzAndRpcUnavailable)
{
    GetHashOnWorker();
    std::string objectKey1 = "a_key_hash_to_" + std::to_string(workerHashValue_[2]);
    std::string objectKey2 = "a_key_hash_to_" + std::to_string(workerHashValue_[3]);
    std::string data1 = randomData_.GetRandomString(10);
    std::string data2 = randomData_.GetRandomString(10);

    // 1. Set two object with different meta data position
    SetWorkerHashInjection();
    SuccessToSetObject(objectKey1, objectKey2, data1, data2);

    // 2. Construct RPC unavailable, client can only get data when obs exist data
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    // Scale down AZ1 worker1 node, let get meta from worker0 firstly
    sleep(10);
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 0, "worker_oc_service_get_impl.pull_object_data_from_remote_worker.before_get_from_remote",
        "return(K_RPC_UNAVAILABLE)"));
    std::string getValue1, getValue2;
    DS_ASSERT_NOT_OK(client0_->Get(objectKey1, getValue1));
    DS_ASSERT_NOT_OK(client0_->Get(objectKey2, getValue2));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data2)));
    sleep(3);
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data1)));
    DS_ASSERT_OK(client0_->Get(objectKey1, getValue2));
    ASSERT_EQ(data2, getValue1);
    ASSERT_EQ(data1, getValue2);
}

TEST_F(KVClientCrossAzReadTest, DISABLED_ObjectWithUuidReadCrossAzAndRpcUnavailable)
{
    GetWorkerUuids();
    HostPort workerHost;
    workerHost.ParseString(workerAddress_[2]);
    std::string objectKey1 = NewObjectKey() + ";" + uuidMap_[workerHost];
    workerHost.ParseString(workerAddress_[3]);
    std::string objectKey2 = NewObjectKey() + ";" + uuidMap_[workerHost];
    std::string data1 = randomData_.GetRandomString(10);
    std::string data2 = randomData_.GetRandomString(10);
    sleep(1);

    // 1. Set two object with different meta data position
    SuccessToSetObject(objectKey1, objectKey2, data1, data2);

    // 2. Construct RPC unavailable, client can only get data when obs exist data
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 0, "worker_oc_service_get_impl.pull_object_data_from_remote_worker.before_get_from_remote",
        "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(
        WORKER, 1, "worker_oc_service_get_impl.pull_object_data_from_remote_worker.before_get_from_remote",
        "return(K_RPC_UNAVAILABLE)"));
    std::string getValue1, getValue2;
    DS_ASSERT_NOT_OK(client0_->Get(objectKey1, getValue1));
    DS_ASSERT_NOT_OK(client1_->Get(objectKey2, getValue2));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data2)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "persistence.service.get", FormatString("return(%s)", data1)));
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey1, getValue2));
    ASSERT_EQ(data2, getValue1);
    ASSERT_EQ(data1, getValue2);
}

TEST_F(KVClientCrossAzReadTest, DISABLED_NormalObjectReadCrossAzAndWorkerTimeout)
{
    GetHashOnWorker();
    std::string objectKey1 = "a_key_hash_to_" + std::to_string(workerHashValue_[2]);
    std::string objectKey2 = "a_key_hash_to_" + std::to_string(workerHashValue_[3]);
    std::string data1 = randomData_.GetRandomString(10);
    std::string data2 = randomData_.GetRandomString(10);

    // 1. Set two object with different meta data position
    sleep(1);
    SetWorkerHashInjection();
    SuccessToSetObject(objectKey1, objectKey2, data1, data2);

    // 2. Shutdown worker, after timeout client can only get data from obs
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    sleep(4);
    std::string getValue1, getValue2;
    DS_ASSERT_NOT_OK(client0_->Get(objectKey1, getValue1));
    DS_ASSERT_NOT_OK(client1_->Get(objectKey2, getValue2));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data2)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "persistence.service.get", FormatString("return(%s)", data1)));
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey1, getValue2));
    ASSERT_EQ(data2, getValue1);
    ASSERT_EQ(data1, getValue2);
    getValue1.clear(), getValue2.clear();

    // 3. client3 set data, and meta data on shutdown worker can't set success, and after success to set the another
    // object, it can be getted from another AZ
    ClearGetInjection(0, 1);
    std::string data3 = randomData_.GetRandomString(10);
    std::string data4 = randomData_.GetRandomString(10);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_NOT_OK(client3_->Set(objectKey1, data3, param));
    DS_ASSERT_OK(client3_->Set(objectKey2, data4, param));
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    ASSERT_EQ(data4, getValue1);

    StartWorker(2, 2);
}

TEST_F(KVClientCrossAzReadTest, DISABLED_ObjectWithUuidReadCrossAzAndWorkerTimeout)
{
    GetWorkerUuids();
    HostPort workerHost;
    workerHost.ParseString(workerAddress_[2]);
    std::string objectKey1 = NewObjectKey() + "objectKey1;" + uuidMap_[workerHost];
    workerHost.ParseString(workerAddress_[3]);
    std::string objectKey2 = NewObjectKey() + "objectKey2;" + uuidMap_[workerHost];
    std::string data1 = randomData_.GetRandomString(10);
    std::string data2 = randomData_.GetRandomString(10);

    // 1. Set two object with different meta data position
    SuccessToSetObject(objectKey1, objectKey2, data1, data2);

    // 2. Shutdown worker, after timeout client can only get data from obs
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    sleep(4);
    std::string getValue1, getValue2;
    DS_ASSERT_NOT_OK(client0_->Get(objectKey1, getValue1));
    DS_ASSERT_NOT_OK(client1_->Get(objectKey2, getValue2));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.get", FormatString("return(%s)", data2)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "persistence.service.get", FormatString("return(%s)", data1)));
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey1, getValue2));
    ASSERT_EQ(data2, getValue1);
    ASSERT_EQ(data1, getValue2);
    getValue1.clear(), getValue2.clear();

    // 3. client3 set data, and meta data on shutdown worker can't set success, and after success to set the another
    // object, it can be getted from another AZ
    ClearGetInjection(0, 1);
    std::string data3 = randomData_.GetRandomString(10);
    std::string data4 = randomData_.GetRandomString(10);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_NOT_OK(client3_->Set(objectKey1, data3, param));
    DS_ASSERT_OK(client3_->Set(objectKey2, data4, param));
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    ASSERT_EQ(data4, getValue1);

    StartWorker(2, 2);
}

TEST_F(KVClientCrossAzReadTest, DISABLED_NormalObjectReadCrossAzScaleDownAndUp)
{
    GetHashOnWorker();
    std::string objectKey1 = "a_key_hash_to_" + std::to_string(workerHashValue_[2]);
    std::string objectKey2 = "a_key_hash_to_" + std::to_string(workerHashValue_[3]);
    std::string data1 = randomData_.GetRandomString(10);
    std::string data2 = randomData_.GetRandomString(10);

    // 1. Set two object with different meta data position, and have a copy object in worker3
    SetWorkerHashInjection();
    SuccessToSetObjectAndHaveCopy(objectKey1, objectKey2, data1, data2);

    // 2. Shutdown worker, before timeout only can get data from L2 cache
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    BeforeTimeOutGetFromL2Cache(objectKey1, objectKey2, data1, data2);

    // 3. After timeout before scale down, meta in the other node can get data normally, meta in the shutdown node can
    // only get data from L2 cache
    sleep(3);
    ClearGetInjection(0, 1);
    GetObjectAfterTimeout(objectKey1, objectKey2, data1, data2);

    // 4. After node scale down, the situation is the same
    sleep(6);
    GetObjectAfterTimeout(objectKey1, objectKey2, data1, data2);

    // 5. Set a value, other AZ get get new value
    std::string data3 = randomData_.GetRandomString(10);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client3_->Set(objectKey2, data3, param));
    std::string getValue1, getValue2;
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey2, getValue2));
    ASSERT_EQ(data3, getValue1);
    ASSERT_EQ(data3, getValue2);
    getValue1.clear(), getValue2.clear();

    // 6. After node start and scale up, object can be getted normally
    StartWorker(2, 2);
    sleep(3);
    ClearGetInjection(0, 0);
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey1, getValue2));
    ASSERT_EQ(data1, getValue2);
    ASSERT_EQ(data3, getValue1);

    // 7. After delete the object, it can't be getted again
    cluster_->SetInjectAction(WORKER, 2, "persistence.service.del", "return(K_OK)");
    cluster_->SetInjectAction(WORKER, 3, "persistence.service.del", "return(K_OK)");
    DS_ASSERT_OK(client2_->Del(objectKey1));
    DS_ASSERT_OK(client3_->Del(objectKey2));
    DS_ASSERT_NOT_OK(client0_->Get(objectKey1, getValue1));
    DS_ASSERT_NOT_OK(client1_->Get(objectKey2, getValue2));
}

TEST_F(KVClientCrossAzReadTest, DISABLED_ObjectWithUuidReadCrossAzScaleDownAndUp)
{
    GetWorkerUuids();
    HostPort workerHost;
    workerHost.ParseString(workerAddress_[2]);
    std::string objectKey1 = NewObjectKey() + ";" + uuidMap_[workerHost];
    workerHost.ParseString(workerAddress_[3]);
    std::string objectKey2 = NewObjectKey() + ";" + uuidMap_[workerHost];
    std::string data1 = randomData_.GetRandomString(10);
    std::string data2 = randomData_.GetRandomString(10);

    // 1. Set two object with different meta data position, and have a copy object in worker3
    SuccessToSetObjectAndHaveCopy(objectKey1, objectKey2, data1, data2);

    // 2. Shutdown worker, before timeout only can get data from L2 cache
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 2));
    BeforeTimeOutGetFromL2Cache(objectKey1, objectKey2, data1, data2);

    // 3. After timeout before scale down, meta in the other node can get data normally, meta in the shutdown node can
    // only get data from L2 cache
    sleep(3);
    GetObjectAfterTimeout(objectKey1, objectKey2, data1, data2);

    // 4. After node scale down, the situation is the same
    sleep(6);
    GetObjectAfterTimeout(objectKey1, objectKey2, data1, data2);

    // 5. Set a value, other AZ get get new value
    std::string data3 = randomData_.GetRandomString(10);
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client3_->Set(objectKey2, data3, param));
    std::string getValue1, getValue2;
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey2, getValue2));
    ASSERT_EQ(data3, getValue1);
    ASSERT_EQ(data3, getValue2);
    getValue1.clear(), getValue2.clear();

    // 6. After node start and scale up, object can be getted normally
    StartWorker(2, 2);
    sleep(3);
    ClearGetInjection(0, 0);
    DS_ASSERT_OK(client0_->Get(objectKey2, getValue1));
    DS_ASSERT_OK(client1_->Get(objectKey1, getValue2));
    ASSERT_EQ(data1, getValue2);
    ASSERT_EQ(data3, getValue1);

    // 7. After delete the object, it can't be getted again
    cluster_->SetInjectAction(WORKER, 2, "persistence.service.del", "return(K_OK)");
    cluster_->SetInjectAction(WORKER, 3, "persistence.service.del", "return(K_OK)");
    DS_ASSERT_OK(client2_->Del(objectKey1));
    DS_ASSERT_OK(client3_->Del(objectKey2));
    DS_ASSERT_NOT_OK(client0_->Get(objectKey1, getValue1));
    DS_ASSERT_NOT_OK(client1_->Get(objectKey2, getValue2));
}

TEST_F(KVClientCrossAzReadTest, DISABLED_VoluntaryScaleDownChangePrimaryAddrss)
{
    GetHashOnWorker();
    std::string objectKey = "a_key_hash_to_" + std::to_string(workerHashValue_[2]);
    std::string data = randomData_.GetRandomString(10);

    // 1. Set two object with different meta data position, and have a copy object in worker3
    SetWorkerHashInjection();
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_OK(client2_->Set(objectKey, data, param));
    std::string getValue1, getValue2, getValue3;
    DS_ASSERT_OK(client3_->Get(objectKey, getValue1));
    ASSERT_EQ(data, getValue1);
    client2_.reset();
    VoluntaryScaleDownInject(2);  // Voluntary scale down the worker 2

    sleep(10);  // Wait 10 seconds for worker scale down

    ASSERT_EQ(GetWorkerNum(AZ2), 1);
    DS_ASSERT_OK(client0_->Get(objectKey, getValue2));
    DS_ASSERT_OK(client1_->Get(objectKey, getValue3));
    ASSERT_EQ(data, getValue2);
    ASSERT_EQ(data, getValue3);
}

class KVClientCrossAzGetMetaAndData : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        std::string gflag =
            " -v=2 -shared_memory_size_mb=5120 -node_timeout_s=3 -node_dead_timeout_s=8 -auto_del_dead_node=true "
            "-other_cluster_names=AZ1,AZ2,AZ3,AZ4 -cross_cluster_get_meta_from_worker=true -v=2";

        opts.workerGflagParams = gflag;
        std::vector<std::string> otherAzNames = { "AZ1", "AZ2", "AZ3", "AZ4" };
        for (size_t i = 0; i < workerNum_; i++) {
            opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
            std::string param = "-cluster_name=";
            param.append(otherAzNames[i]);
            opts.workerSpecifyGflagParams[i] = param;
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        clients_.resize(workerNum_);
        int32_t timeoutMs = 2000;
        for (size_t i = 0; i < workerNum_; i++) {
            InitTestKVClient(i, clients_[i], timeoutMs);
        }
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    const size_t workerNum_ = 4;
    std::vector<std::string> workerAddress_;
    std::vector<std::shared_ptr<KVClient>> clients_;
};

TEST_F(KVClientCrossAzGetMetaAndData, LEVEL1_ObjectCrossAzSetGetDel)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string data1 = randomData_.GetRandomString(20 * 1024 * 1024);
    std::string data2 = randomData_.GetRandomString(20 * 1024 * 1024);
    std::string getValue1;

    // case1: az1 generate key, az1 set, az2 get, az1 del
    std::string objectKey0;
    (void)clients_[0]->GenerateKey("key_cross_az", objectKey0);
    DS_ASSERT_OK(clients_[0]->Set(objectKey0, data1, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    // second get from local copy, it will faster than first get
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[0]->Del(objectKey0));
    ASSERT_EQ(clients_[1]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);

    // case2: az1 generate key, az1 set, az2 get, az2 del
    DS_ASSERT_OK(clients_[0]->Set(objectKey0, data1, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[1]->Del(objectKey0));
    ASSERT_EQ(clients_[0]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);

    // case3: az1 generate key, az1 set, az2 get, az3 del
    DS_ASSERT_OK(clients_[0]->Set(objectKey0, data1, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[2]->Del(objectKey0));
    ASSERT_EQ(clients_[0]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(clients_[1]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);

    // case4: cross az set: az1 generate key, az1 set val1, az2 set val2, az2 get, az1 get, az3 del
    std::string objectKey1;
    (void)clients_[0]->GenerateKey("key_cross_az_set1", objectKey1);
    DS_ASSERT_OK(clients_[0]->Set(objectKey1, data1, param));
    DS_ASSERT_OK(clients_[1]->Set(objectKey1, data2, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey1, getValue1));
    ASSERT_EQ(data2, getValue1);
    // The state cache guarantees causal consistency by default, so no sleep is required after modifying data.
    DS_ASSERT_OK(clients_[0]->Get(objectKey1, getValue1));
    ASSERT_EQ(data2, getValue1);
    DS_ASSERT_OK(clients_[2]->Set(objectKey1, data1, param));
    // The state cache guarantees causal consistency by default, so no sleep is required after modifying data.
    DS_ASSERT_OK(clients_[0]->Get(objectKey1, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[3]->Del(objectKey1));
    ASSERT_EQ(clients_[0]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(clients_[1]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);
    int client2Index = 2;
    ASSERT_EQ(clients_[client2Index]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);

    // case5: cross az set:  az1 generate key, az2 set val2, az2 get, az1 get, az1 set, az1 get, az3 del
    std::string objectKey2;
    (void)clients_[0]->GenerateKey("key_cross_az_set2", objectKey2);
    DS_ASSERT_OK(clients_[1]->Set(objectKey2, data2, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey2, getValue1));
    ASSERT_EQ(data2, getValue1);
    DS_ASSERT_OK(clients_[0]->Get(objectKey2, getValue1));
    ASSERT_EQ(data2, getValue1);
    DS_ASSERT_OK(clients_[0]->Set(objectKey2, data1, param));
    DS_ASSERT_OK(clients_[0]->Get(objectKey2, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[3]->Get(objectKey2, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[2]->Del(objectKey2));
    ASSERT_EQ(clients_[0]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(clients_[1]->Get(objectKey0, getValue1).GetCode(), K_NOT_FOUND);
}

TEST_F(KVClientCrossAzGetMetaAndData, ObjectCrossAzSetGetDel2)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string getValue1;
    std::string objectKey0;
    (void)clients_[0]->GenerateKey("key0", objectKey0);
    DS_ASSERT_OK(clients_[0]->Set(objectKey0, "data1", param));
    DS_ASSERT_OK(clients_[1]->Set(objectKey0, "data2", param));
    DS_ASSERT_OK(clients_[2]->Get(objectKey0, getValue1));
    ASSERT_EQ("data2", getValue1);
}

class KVClientCrossAzNoNeedMeta : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        std::string gflag =
            " -v=2"
            " -shared_memory_size_mb=5120"
            " -node_timeout_s=3"
            " -node_dead_timeout_s=8"
            " -auto_del_dead_node=true"
            " -other_cluster_names=AZ1,AZ2"
            " -cross_cluster_get_meta_from_worker=true"
            " -oc_io_from_l2cache_need_metadata=false"
            " -v=2";

        opts.workerGflagParams = gflag;
        opts.workerSpecifyGflagParams[0] += " -cluster_name=AZ1 ";
        opts.workerSpecifyGflagParams[1] += " -cluster_name=AZ2 ";
        std::vector<std::string> otherAzNames = { "AZ1", "AZ2" };
        for (size_t i = 0; i < workerNum_; i++) {
            opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
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
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        clients_.resize(workerNum_);
        int32_t timeoutMs = 2000;
        for (size_t i = 0; i < workerNum_; i++) {
            InitTestKVClient(i, clients_[i], timeoutMs);
        }
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    const size_t workerNum_ = 2;
    std::vector<std::string> workerAddress_;
    std::vector<std::shared_ptr<KVClient>> clients_;
};

TEST_F(KVClientCrossAzNoNeedMeta, LEVEL2_TestDeleteVersionBetweenPutAndDel)
{
    std::string key = "key";
    SetParam param{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    DS_ASSERT_OK(clients_[0]->Set(key, "VERSION1_AZ1", param));
    clients_[0].reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    DS_ASSERT_OK(clients_[1]->Set(key, "VERSION2_AZ2", param));
    clients_[1].reset();
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));

    InitTestKVClient(0, clients_[0]);
    std::string val;
    DS_ASSERT_OK(clients_[0]->Get(key, val));
    ASSERT_EQ(val, "VERSION2_AZ2");
}

class KVClientCrossAzGetMetaAndDataTwoWorkerPerAz : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        opts.workerGflagParams = gflag_;
        for (size_t i = 0; i < workerNum_; i++) {
            opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
            workerAddress_.emplace_back(opts.workerConfigs.back().ToString());
            std::string param = "-cluster_name=";
            param.append(otherAzNames_[i % otherAzNames_.size()]);
            opts.workerSpecifyGflagParams[i] = param;
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        DS_ASSERT_OK(cluster_->StartWorkers());
        for (size_t i = 0; i < workerNum_; i++) {
            DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, i));
        }
        clients_.resize(workerNum_);
        int32_t timeoutMs = 2000;
        for (size_t i = 0; i < workerNum_; i++) {
            InitTestKVClient(i, clients_[i], timeoutMs);
        }
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

protected:
    const size_t workerNum_ = 8;
    std::vector<std::string> workerAddress_;
    std::vector<std::shared_ptr<KVClient>> clients_;
    const std::vector<std::string> otherAzNames_ = { "AZ1", "AZ2", "AZ3", "AZ4" };
    std::string gflag_ =
        " -v=2 -shared_memory_size_mb=5120 -node_timeout_s=3 -node_dead_timeout_s=8 -auto_del_dead_node=true "
        "-other_cluster_names=AZ1,AZ2,AZ3,AZ4 -cross_cluster_get_meta_from_worker=true";
};

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, LEVEL2_TestParallelCrossAzSet)
{
    int loopTime = 300;
    auto setFunc = [&loopTime, this](size_t thrdNum) {
        auto val = "val" + std::to_string(thrdNum);
        for (int i = 0; i < loopTime; i++) {
            (void)clients_[thrdNum]->Set("key", val);
        }
    };
    std::vector<std::thread> thrds;
    for (size_t i = 0; i < otherAzNames_.size(); i++) {
        thrds.emplace_back(setFunc, i);
    }
    thrds.emplace_back([this]() {
        std::string valToGet;
        (void)clients_[1]->Get("key1", valToGet);
        int client2Index = 2;
        (void)clients_[client2Index]->Get("key1", valToGet);
    });

    for (auto &thrd : thrds) {
        thrd.join();
    }
    clients_.clear();
}

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, DISABLED_LEVEL1_ObjectCrossAzSetGetDelDfx1)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string data1 = randomData_.GetRandomString(20);
    std::string getValue1;

    // az1 generate key, az2 set, meta fail, az1 other worker get, az2 get
    std::string objectKey0;
    (void)clients_[0]->GenerateKey("key_cross_az_dfx1", objectKey0);
    // cross az set
    DS_ASSERT_OK(clients_[1]->Set(objectKey0, data1, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[2]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    // meta crash, az1 get, az1 other worker get
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    sleep(1);
    // Before scaling down, if there is a local copy, we can still get it successfully.
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    DS_ASSERT_OK(clients_[2]->Get(objectKey0, getValue1));
    DS_ASSERT_NOT_OK(clients_[4]->Get(objectKey0, getValue1));
    sleep(5);
    // After scaling is triggered, if the local copy is not stored in L2 cache, it will be deleted directly.
    DS_ASSERT_NOT_OK(clients_[1]->Get(objectKey0, getValue1));
    DS_ASSERT_NOT_OK(clients_[2]->Get(objectKey0, getValue1));
    DS_ASSERT_OK(clients_[1]->Del(objectKey0));
}

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, LEVEL2_ObjectCrossAzGetDelDfx2)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string data1 = randomData_.GetRandomString(20);
    std::string getValue1;

    // az3 generate key, az1 set, other az get
    std::string objectKey0;
    (void)clients_[0]->GenerateKey("key_cross_az_dfx2", objectKey0);
    // cross az get
    DS_ASSERT_OK(clients_[0]->Set(objectKey0, data1, param));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[2]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[3]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[4]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    // meta crash, az1 get, az0 get
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    sleep(1);
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    DS_ASSERT_OK(clients_[4]->Get(objectKey0, getValue1));
    sleep(5);
    DS_ASSERT_NOT_OK(clients_[1]->Get(objectKey0, getValue1));
    DS_ASSERT_NOT_OK(clients_[4]->Get(objectKey0, getValue1));
    DS_ASSERT_OK(clients_[1]->Del(objectKey0));
}

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, DISABLED_LEVEL1_ObjectCrossAzGetDelDfx3)
{
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string data1 = randomData_.GetRandomString(20 * 1024 * 1024);
    std::string getValue1;
    // az0 generate key, az0 set, other az get
    std::string objectKey0;
    (void)clients_[0]->GenerateKey("key_cross_az_dfx3", objectKey0);
    DS_ASSERT_OK(clients_[4]->Set(objectKey0, data1, param));
    // primary copy crash, master exist, query from master and load data from obs
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 4));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    // meta node crash, query from etcd and load data from obs
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[1]->Del(objectKey0));
}

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, DISABLED_ObjectCrossAzGetDelDfx4)
{
    SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    std::string data1 = randomData_.GetRandomString(10 * 1024 * 1024);
    std::string getValue1;
    // az0 generate key, az0 set, other az1 get
    std::string objectKey0 = clients_[0]->Set("data_on_w00", param);
    ASSERT_NE(objectKey0, "");
    DS_ASSERT_OK(clients_[4]->Set(objectKey0, data1, param));
    // w4 primary copy restart, az1 query from etcd and remote get from w4, w4 will load data from cloud storage.
    // The state cache guarantees causal consistency by default, so no sleep is required after modifying data.
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 4));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 4, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 4));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 4, "persistence.service.get", FormatString("1*return(%s)", data1)));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "persistence.service.get", FormatString("2*return(%s)", data1)));
    DS_ASSERT_OK(clients_[1]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[0]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
    DS_ASSERT_OK(clients_[4]->Get(objectKey0, getValue1));
    ASSERT_EQ(data1, getValue1);
}

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, TestHashKeyCrossAzSetGet)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key = "key";
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string valToGet;
    int client2Index = 2, client5Index = 5;
    // az1 set, az2 modify, az2 remote get, az3 remote get
    DS_ASSERT_OK(clients_[0]->Set(key, val1, param));
    DS_ASSERT_OK(clients_[1]->Set(key, val2, param));
    DS_ASSERT_OK(clients_[client5Index]->Get(key, valToGet));
    ASSERT_EQ(val2, valToGet);
    DS_ASSERT_OK(clients_[client2Index]->Get(key, valToGet));
    ASSERT_EQ(val2, valToGet);
}

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAz, TestHashKeyCrossAzDel)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key = "key";
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string valToGet;
    int client2Index = 2, client5Index = 5;
    // az1 set, az2 modify, az2 remote get, az3 remote get
    DS_ASSERT_OK(clients_[0]->Set(key, val1, param));
    DS_ASSERT_OK(clients_[1]->Set(key, val2, param));
    DS_ASSERT_OK(clients_[client5Index]->Get(key, valToGet));
    ASSERT_EQ(val2, valToGet);
    DS_ASSERT_OK(clients_[client2Index]->Get(key, valToGet));
    ASSERT_EQ(val2, valToGet);
    DS_ASSERT_OK(clients_[0]->Del(key));
    sleep(1);
    ASSERT_EQ(clients_[client5Index]->Get(key, valToGet).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(clients_[client2Index]->Get(key, valToGet).GetCode(), K_NOT_FOUND);
}

class KVClientCrossAzGetMetaAndDataTwoWorkerPerAzP1 : public KVClientCrossAzGetMetaAndDataTwoWorkerPerAz {
public:
    KVClientCrossAzGetMetaAndDataTwoWorkerPerAzP1()
    {
        gflag_ += " -async_delete=true";
    }
};

TEST_F(KVClientCrossAzGetMetaAndDataTwoWorkerPerAzP1, TestHashKeyCrossAzDel)
{
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key = "key";
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string valToGet;
    int client2Index = 2, client5Index = 5;
    // az1 set, az2 modify, az2 remote get, az3 remote get
    DS_ASSERT_OK(clients_[0]->Set(key, val1, param));
    DS_ASSERT_OK(clients_[1]->Set(key, val2, param));
    DS_ASSERT_OK(clients_[client5Index]->Get(key, valToGet));
    ASSERT_EQ(val2, valToGet);
    DS_ASSERT_OK(clients_[client2Index]->Get(key, valToGet));
    ASSERT_EQ(val2, valToGet);
    DS_ASSERT_OK(clients_[0]->Del(key));
    // wait async delete success
    sleep(1);
    ASSERT_EQ(clients_[client5Index]->Get(key, valToGet).GetCode(), K_NOT_FOUND);
    ASSERT_EQ(clients_[client2Index]->Get(key, valToGet).GetCode(), K_NOT_FOUND);
}

class TestTheImpactOnCluster2WhenScalingInCluster1 : public STCScaleTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.waitWorkerReady = false;
        opts.numEtcd = 1;
        opts.numWorkers = workerNum_;
        opts.enableDistributedMaster = "true";
        opts.numOBS = 1;
        opts.disableRocksDB = false;
        std::string gflag =
            " -v=1 -shared_memory_size_mb=512 -node_timeout_s=3 -node_dead_timeout_s=8 -auto_del_dead_node=true "
            "-other_cluster_names=AZ1,AZ2 -cross_cluster_get_meta_from_worker=true ";

        opts.workerGflagParams = gflag;
        for (size_t i = 0; i < workerNum_; i++) {
            auto azName = azNames_[i % azNames_.size()];
            std::string param = "-cluster_name=" + azName;
            opts.workerSpecifyGflagParams[i] = param;
        }
    }

    void SetUp() override
    {
        CommonTest::SetUp();
        DS_ASSERT_OK(Init());
        ASSERT_TRUE(cluster_ != nullptr);
        DS_ASSERT_OK(cluster_->StartEtcdCluster());
        DS_ASSERT_OK(cluster_->StartOBS());
        externalCluster_ = dynamic_cast<ExternalCluster *>(cluster_.get());
        InitTestEtcdInstance(azNames_);
        clients_.resize(workerNum_);
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
        std::string checkFilePath = FLAGS_log_dir.c_str();
        std::string client = "client";
        checkFilePath = checkFilePath.substr(0, checkFilePath.length() - client.length()) + "/worker" +
                        std::to_string(workerIdx) + "/log/worker-status";
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

    void InitClients(std::initializer_list<int> workerIndex)
    {
        for (auto i : workerIndex) {
            InitTestKVClient(i, clients_[i]);
        }
    }

    void GeneratePredictableHashKey(std::vector<size_t> destMasterIndexs, std::string &predictableHashKey)
    {
        std::string failureSign = "FAILED";
        predictableHashKey = failureSign;
        ASSERT_EQ(destMasterIndexs.size(), azNames_.size());
        std::unordered_set<std::string> destMasterAddrs;
        for (auto destMasterIndex : destMasterIndexs) {
            HostPort destMasterHostPort;
            DS_ASSERT_OK(cluster_->GetWorkerAddr(destMasterIndex, destMasterHostPort));
            destMasterAddrs.insert(destMasterHostPort.ToString());
        }

        // <azName, <hashToken, workerAddr>>
        std::unordered_map<std::string, std::map<HashPosition, std::string>> currTokenMap;
        for (const auto &azName : azNames_) {
            std::string value;
            auto ringPrefix = "/" + azName + ETCD_RING_PREFIX;
            DS_ASSERT_OK(db_->Get(ringPrefix, "", value));
            HashRingPb ring;
            ASSERT_TRUE(ring.ParseFromString(value));
            std::map<HashPosition, std::string> currTokenMapInThisAz;
            for (const auto &kv : ring.workers()) {
                ASSERT_EQ(kv.second.state(), WorkerPb::ACTIVE);
                const auto &workerId = kv.first;
                for (auto token : kv.second.hash_tokens()) {
                    currTokenMapInThisAz.insert({ token, workerId });
                }
            }
            currTokenMap.insert({ azName, std::move(currTokenMapInThisAz) });
        }

        int loopNum = 1000;
        for (int i = 0; i < loopNum; i++) {
            std::string temPredictableHashKey = GenRandomString(10);
            auto keyHash = MurmurHash3_32(temPredictableHashKey);
            bool available = true;
            for (const auto &kv : currTokenMap) {
                const auto &currTokenMapInSpecificAz = kv.second;
                auto iter = currTokenMapInSpecificAz.upper_bound(keyHash);
                auto masterAddr =
                    iter == currTokenMapInSpecificAz.end() ? currTokenMapInSpecificAz.begin()->second : iter->second;
                if (destMasterAddrs.find(masterAddr) == destMasterAddrs.end()) {
                    available = false;
                    break;
                }
            }
            if (available) {
                predictableHashKey = std::move(temPredictableHashKey);
                break;
            }
        }
        ASSERT_NE(failureSign, predictableHashKey);
    }

    void TestScaleUp1(bool testReplica)
    {
        std::string injectStr = testReplica ? " -enable_meta_replica=true" : "";
        StartWorkerAndWaitReady({ 0, 2 }, injectStr);
        StartWorkerAndWaitReady({ 1, 3 }, injectStr);
        InitClients({ 0, 1, 2, 3 });

        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        std::string key1;
        (void)clients_[1]->GenerateKey("key", key1);
        std::string val1 = "val1";
        std::string val2 = "val2";
        DS_ASSERT_OK(clients_[0]->Set(key1, val1, param));
        std::string valToGet;
        int client2Index = 2, client3Index = 3;
        DS_ASSERT_OK(clients_[client2Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        DS_ASSERT_OK(clients_[client3Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        // real time broadcast: master[w1]; primary copy[w0]; locations[w0, w2, w3]
        // scale up w5/w7, meta carrying workerId will not be migrated.
        StartWorkerAndWaitReady({ 5, 7 });
        DS_ASSERT_OK(clients_[1]->Set(key1, val2, param));
        // real time broadcast: master[w1]; primary copy[w1]; locations[w1]
        // Each node that stores old data should receive a cache invalid notification, and we will not obtain the old
        // data.
        DS_ASSERT_OK(clients_[client2Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val2);
        DS_ASSERT_OK(clients_[client3Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val2);
    }

    void TestScaleDown1(bool testReplica)
    {
        std::string injectStr = testReplica ? " -enable_meta_replica=true" : "";
        StartWorkerAndWaitReady({ 0, 2, 4 }, injectStr);
        StartWorkerAndWaitReady(
            { 5 },
            " -inject_actions=HashRing.InitRing.CustomWorkerId:call(01234567-0123-0123-0123-"
            "0123456789ab)"
                + injectStr);  // After w5 scale down, w3 will take over the meta with workerId on it.
        StartWorkerAndWaitReady({ 3 },
                                " -inject_actions=HashRing.InitRing.CustomWorkerId:call(11234567-0123-0123-0123-"
                                "0123456789ab)"
                                    + injectStr);
        StartWorkerAndWaitReady({ 1 },
                                " -inject_actions=HashRing.InitRing.CustomWorkerId:call(21234567-0123-0123-0123-"
                                "0123456789ab)"
                                    + injectStr);
        InitClients({ 0, 1, 2, 3, 4, 5 });

        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        int client5Index = 5, client2Index = 2, worker5Index = 5, client3Index = 3;
        std::string key1;
        (void)clients_[client5Index]->GenerateKey("key", key1);
        std::string val1 = "val1";
        std::string val2 = "val2";
        DS_ASSERT_OK(clients_[0]->Set(key1, val1, param));
        std::string valToGet;
        DS_ASSERT_OK(clients_[client2Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        DS_ASSERT_OK(clients_[1]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        // real time broadcast: master[w5]; primary copy[w0]; locations[w0, w1, w2]
        // scale down w5, w3 will take over the meta with workerId in w5.
        clients_[client5Index].reset();
        DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, worker5Index));
        int remainingNodesInAz1Num = 2, checkTimeoutS = 60;
        WaitAllNodesJoinIntoHashRing(remainingNodesInAz1Num, checkTimeoutS, "AZ2");
        // real time broadcast: master[w3]; primary copy[w3]; locations[w1, w2];
        // Note: Now we have a primary copy node, but there is no data on it. Be careful!!
        DS_ASSERT_OK(clients_[client2Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        // real time broadcast: master[w3]; primary copy[w3]; locations[w1, w2];
        DS_ASSERT_OK(clients_[1]->Set(key1, val2, param));
        DS_ASSERT_OK(clients_[client2Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val2);
        DS_ASSERT_OK(clients_[client3Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val2);
    }

    void TestVoluntaryScaleDown1(bool testReplica)
    {
        std::string injectStr = testReplica ? " -enable_meta_replica=true" : "";
        StartWorkerAndWaitReady({ 0, 2, 4 }, injectStr);
        StartWorkerAndWaitReady({ 1, 3, 5 }, injectStr);
        InitClients({ 0, 1, 2, 3, 4, 5 });

        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        std::string key1;
        (void)clients_[0]->GenerateKey("key", key1);
        std::string val1 = "val1";
        std::string val2 = "val2";
        int client5Index = 5, client2Index = 2, worker5Index = 5, client3Index = 3;
        DS_ASSERT_OK(clients_[client5Index]->Set(key1, val1, param));
        std::string valToGet;
        // real time broadcast: master[w0]; primary copy[w5]; locations[w5]
        // Voluntary scale down w5, w5 will push its data to w0(master)
        clients_[5].reset();
        VoluntaryScaleDownInject(worker5Index);
        int remainingNodesInAz1Num = 2, checkTimeoutS = 60;
        WaitAllNodesJoinIntoHashRing(remainingNodesInAz1Num, checkTimeoutS, "AZ2");
        // real time broadcast: master[w0]; primary copy[w0]; locations[w0];
        DS_ASSERT_OK(clients_[0]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        DS_ASSERT_OK(clients_[client3Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val1);
        // real time broadcast: master[w0]; primary copy[w0]; locations[w0, w3];
        DS_ASSERT_OK(clients_[client2Index]->Set(key1, val2, param));
        // real time broadcast: master[w0]; primary copy[w2]; locations[w2];
        DS_ASSERT_OK(clients_[0]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val2);
        DS_ASSERT_OK(clients_[client3Index]->Get(key1, valToGet));
        ASSERT_EQ(valToGet, val2);
    }

    void TestCluster1CrashWhenCluster2Restart(bool testReplica)
    {
        std::string injectStr = testReplica ? " -enable_meta_replica=true -oc_io_from_l2cache_need_metadata=false" : "";
        StartWorkerAndWaitReady({ 0, 2 }, injectStr);
        StartWorkerAndWaitReady({ 1, 3 }, injectStr);
        InitClients({ 0, 1, 2, 3 });
        int loopNum = 10;
        int clientNum = 4;
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        std::string val = "val";
        std::vector<std::string> keys;
        for (int i = 0; i < loopNum; i++) {
            int clientIndex1 = i % clientNum;
            int clientIndex2 = clientIndex1 == clientNum - 1 ? 0 : clientIndex1 + 1;
            std::string key;
            (void)clients_[clientIndex1]->GenerateKey("key" + std::to_string(i), key);
            ASSERT_NE(key, "");
            DS_ASSERT_OK(clients_[clientIndex2]->Set(key, val, param));
            keys.emplace_back(std::move(key));
        }
        std::for_each(clients_.begin(), clients_.end(), [](std::shared_ptr<KVClient> &item) { item.reset(); });
        DS_ASSERT_OK(externalCluster_->ShutdownNodes(WORKER));
        StartWorkerAndWaitReady(
            { 1, 3 }, " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(5000)" + injectStr);
        InitClients({ 1 });
        std::vector<std::string> valsToGet;
        DS_ASSERT_OK(clients_[1]->Get(keys, valsToGet));
        ASSERT_EQ(valsToGet.size(), keys.size());
        for (const auto &valToGet : valsToGet) {
            ASSERT_EQ(valToGet, val);
        }
    }

    void TestCluster2NotStartWhenCluster1Restart(bool testReplica)
    {
        std::string injectStr = testReplica ? " -enable_meta_replica=true" : "";
        StartWorkerAndWaitReady({ 0, 2 }, injectStr);
        DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
        StartWorkerAndWaitReady(
            { 0 }, " -inject_actions=WorkerOCServiceImpl.GiveUpReconciliation.setHealthFile:call(5000) " + injectStr);
    }

protected:
    const size_t workerNum_ = 10;
    std::vector<std::shared_ptr<KVClient>> clients_;
    const std::vector<std::string> azNames_ = { "AZ1", "AZ2" };
    const int hashTokenNumForInject_ = 40'000;
};

// Test basic scale up
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, TestScaleUp1)
{
    TestScaleUp1(false);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, TestScaleUp1WithReplica)
{
    TestScaleUp1(true);
}

// Test basic scale down
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestScaleDown1)
{
    TestScaleDown1(false);
}

// Test basic scale down
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, DISABLED_TestScaleDown1WithReplica)
{
    TestScaleDown1(true);
}

// Test migration of data across clusters
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, TestVoluntaryScaleDown1)
{
    TestVoluntaryScaleDown1(false);
}

// Test migration of data across clusters
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestVoluntaryScaleDown1WithReplica)
{
    TestVoluntaryScaleDown1(true);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestCluster1CrashWhenCluster2Restart)
{
    TestCluster1CrashWhenCluster2Restart(false);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, DISABLED_TestCluster1CrashWhenCluster2RestartWithReplica)
{
    TestCluster1CrashWhenCluster2Restart(true);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, DISABLED_TestAsyncNotifyRemoveMetaBase)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5});

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({0, 3}, predictableHashKey);
    // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string valToGet;
    int client5Index = 5, worker3Index = 3;
    // meta: w3; data: w1
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, worker3Index));
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val2, param));
    StartWorkerAndWaitReady({ 3 });
    int waitTimeS = 2;  // wait async notification success.
    sleep(waitTimeS);
    DS_ASSERT_OK(clients_[client5Index]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val2);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestAsyncNotifyRemoveMetaAfterOtherAzUpdate)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5});

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({ 0, 3 }, predictableHashKey);  // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string val3 = "val3";
    std::string valToGet;
    int worker3Index = 3;
    // meta: w3; data: w1
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, worker3Index));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess",
                                           "return(K_OK)"));
    // w0 will try to notify w3 to delete data, but w3 is disconnected at this time, and w0 will add the notification
    // task to the asynchronous queue. And the injection causes w0 to not actually notify w3 to remove meta.
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val2, param));
    StartWorkerAndWaitReady({ 3 });
    // Because the metadata in az1 has not been deleted, the set will go through the update process this time, and there
    // is no need to notify other az to delete the meta.
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val3, param));
    // After the injection is cleared, a notification to delete metadata on w0 will be sent to w3. At this time, there
    // is a newer version on w3, and w3 refuses to delete it. After w0 receives the reply, it will delete its own
    // metadata.
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess"));
    int waitTimeS = 2;  // wait async notification success.
    sleep(waitTimeS);
    // The client can also get the latest data from az0.
    DS_ASSERT_OK(clients_[0]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val3);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, DISABLED_LEVEL1_TestAsyncNotifyRemoveMetaAfterLocalAzUpdate)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5 });

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({ 0, 3 }, predictableHashKey);  // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string val3 = "val3";
    std::string val4 = "val4";
    std::string valToGet;
    int worker3Index = 3;
    // meta: w3; data: w1
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, worker3Index));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess",
                                           "return(K_OK)"));
    // w0 will try to notify w3 to delete data, but w3 is disconnected at this time, and w0 will add the notification
    // task to the asynchronous queue. And the injection causes w0 to not actually notify w3 to remove meta.
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val2, param));
    StartWorkerAndWaitReady({ 3 });
    // Because the metadata in az1 has not been deleted, the set will go through the update process this time, and there
    // is no need to notify other az to delete the meta.
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val3, param));
    // Because the metadata in az0 has not been deleted, the set will go through the update process this time, and there
    // is no need to notify other az to delete the meta. And the uncompleted async notifications of remove meta on w0
    // will be updated.
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val4, param));
    // After the injection is cleared, a notification to delete metadata on w0 will be sent to w3. At this time, the
    // version in the notification is newer, and w3 delete meta on itself.
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess"));
    int waitTimeS = 2;  // wait async notification success.
    sleep(waitTimeS);
    // The client can also get the latest data from az1.
    DS_ASSERT_OK(clients_[1]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);
}

/*
az0: V2, az1: V3
az0 -> az1: remove meta
az0: update meta to V4
az1 -> az0: You should remove meta
az0: start process the response from az0
az0 -> az1: Now you should remove meta
*/
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestParallelRemoveMetaAndUpdateMeta1)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5 });

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({ 0, 3 }, predictableHashKey);  // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string val3 = "val3";
    std::string val4 = "val4";
    std::string valToGet;
    int worker3Index = 3;
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->QuicklyShutdownWorker(worker3Index));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess",
                                           "return(K_OK)"));
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val2, param));
    StartWorkerAndWaitReady({ 3 }, " -client_reconnect_wait_s=1");
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val3, param));
    int processWaitTimeSec = 5, s2Ms = 1000, worker3Idx = 3;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, worker3Idx,
                                           "OCMetadataManager.ProcessRemoveMetaNotifyFromOtherAz.ReturnSlowly",
                                           FormatString("1*sleep(%d)", processWaitTimeSec * s2Ms)));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess"));
    int waitTimeS = 2;  // async notification will send,  but not return.
    sleep(waitTimeS);
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val4, param));
    sleep(processWaitTimeSec);  // make sure async notification finish.
    DS_ASSERT_OK(clients_[1]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);
    DS_ASSERT_OK(clients_[0]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);
}

/*
az0: V2, az1: V3
az0 -> az1: remove meta
az1 -> az0: You should remove meta
az0: update meta to V4
az0: start process the response from az0
az0 -> az1: Now you should remove meta
*/
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestParallelRemoveMetaAndUpdateMeta2)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5 });

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({ 0, 3 }, predictableHashKey);  // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string val3 = "val3";
    std::string val4 = "val4";
    std::string valToGet;
    int worker3Index = 3;
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->QuicklyShutdownWorker(worker3Index));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess",
                                           "return(K_OK)"));
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val2, param));
    StartWorkerAndWaitReady({ 3 }, " -client_reconnect_wait_s=1");
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val3, param));
    int processWaitTimeSec = 5, s2Ms = 1000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.NotifyMasterRemoveMeta.ProcessSlowly",
                                           FormatString("1*sleep(%d)", processWaitTimeSec * s2Ms)));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess"));
    int waitTimeS = 2;  // async notification will return, but not process
    sleep(waitTimeS);
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val4, param));
    sleep(processWaitTimeSec);  // make sure async notification finish.
    DS_ASSERT_OK(clients_[1]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);
    DS_ASSERT_OK(clients_[0]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);
}

// This test case is very similar to "TestParallelRemoveMetaAndUpdateMeta2", both simulating that the client is slow in
// processing the response, but the timing of injecting the fault is different
TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, DISABLED_LEVEL1_TestParallelRemoveMetaAndUpdateMeta3)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5 });

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({ 0, 3 }, predictableHashKey);  // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string val2 = "val2";
    std::string val3 = "val3";
    std::string val4 = "val4";
    std::string valToGet;
    int worker3Index = 3;
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->QuicklyShutdownWorker(worker3Index));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess",
                                           "return(K_OK)"));
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val2, param));
    StartWorkerAndWaitReady({ 3 }, " -client_reconnect_wait_s=1");
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val3, param));
    int processWaitTimeSec = 5, s2Ms = 1000;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.NotifyDeleteAndClearMeta.ProcessSlowly",
                                           FormatString("1*sleep(%d)", processWaitTimeSec * s2Ms)));
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 0, "OCNotifyWorkerManager.ProcessAsyncNotifyOpImpl.SkipProcess"));
    int waitTimeS = 2;
    sleep(waitTimeS);  // async notification will return, but not process
    DS_ASSERT_OK(clients_[0]->Set(predictableHashKey, val4, param));
    sleep(processWaitTimeSec + 1);  // make sure async notification finish.
    DS_ASSERT_OK(clients_[1]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);

    DS_ASSERT_OK(clients_[0]->Get(predictableHashKey, valToGet));
    ASSERT_EQ(valToGet, val4);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestSyncDelAsyncNotifyDeleteAllCopyMetaBase)
{
    StartWorkerAndWaitReady({ 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)",
                                                   hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d)", hashTokenNumForInject_));
    InitClients({ 0, 1, 5});

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({0, 3}, predictableHashKey);
    // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string valToGet;
    int client5Index = 5, worker3Index = 3;
    // meta: w3; data: w1
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, worker3Index));
    DS_ASSERT_OK(clients_[0]->Del(predictableHashKey));
    StartWorkerAndWaitReady({ 3 });
    int waitTimeS = 2;  // wait async notification success.
    sleep(waitTimeS);
    ASSERT_EQ(clients_[client5Index]->Get(predictableHashKey, valToGet).GetCode(), K_NOT_FOUND);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, LEVEL2_TestAsyncDelAsyncNotifyDeleteAllCopyMetaBase)
{
    StartWorkerAndWaitReady(
        { 0, 2 }, FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d) -async_delete=true",
                               hashTokenNumForInject_));
    StartWorkerAndWaitReady(
        { 1, 3, 5 },
        FormatString(" -inject_actions=HashRing.Init.ChangeDefaultHashTokenNum:call(%d) -async_delete=true",
                     hashTokenNumForInject_));
    InitClients({ 0, 1, 5});

    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string predictableHashKey;
    GeneratePredictableHashKey({0, 3}, predictableHashKey);
    // w0/w3 will manage the metadata of "key".
    std::string val1 = "val1";
    std::string valToGet;
    int client5Index = 5, worker3Index = 3;
    // meta: w3; data: w1
    DS_ASSERT_OK(clients_[1]->Set(predictableHashKey, val1, param));
    DS_ASSERT_OK(externalCluster_->ShutdownNode(WORKER, worker3Index));
    DS_ASSERT_OK(clients_[0]->Del(predictableHashKey));
    StartWorkerAndWaitReady({ 3 }, " -async_delete=true");
    int waitTimeS = 2;  // wait async notification success.
    sleep(waitTimeS);
    ASSERT_EQ(clients_[client5Index]->Get(predictableHashKey, valToGet).GetCode(), K_NOT_FOUND);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, TestReplicaUpgrade)
{
    StartWorkerAndWaitReady({ 0, 2, 6 }, " -enable_meta_replica=true");
    StartWorkerAndWaitReady({ 1, 3, 5 });
    InitClients({ 0 });
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key = "key";
    std::string val1 = "val1";
    std::string valToGet;
    DS_ASSERT_OK(clients_[0]->Set(key, val1, param));
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1, DISABLED_LEVEL1_TestCluster2NotStartWhenCluster1Restart)
{
    TestCluster2NotStartWhenCluster1Restart(false);
}

TEST_F(TestTheImpactOnCluster2WhenScalingInCluster1,
    DISABLED_LEVEL1_TestCluster2NotStartWhenCluster1RestartWithReplica)

{
    TestCluster2NotStartWhenCluster1Restart(true);
}

class SCDisckCacheCrossAzTest : public KVClientCrossAZTest {
public:
    void SetUp() override
    {
        workerNum_ = 3; // workerNum is 3
        KVClientCrossAZTest::SetUp();
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVClientCrossAZTest::SetClusterSetupOptions(opts);
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            opts.workerSpecifyGflagParams[i] += FormatString(" -shared_disk_directory=%s -shared_disk_size_mb=32", dir);
        }
    }
};

TEST_F(SCDisckCacheCrossAzTest, GetDiskObjectCrossAz)
{
    const int AZ1_WORKER0 = 0;
    const int AZ2_WORKER1 = 1;
    const int AZ1_WORKER2 = 2;
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(AZ1_WORKER0, client0);
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    param.cacheType = CacheType::DISK;
    std::string key = client0->Set("value0", param);

    std::shared_ptr<KVClient> client2;
    InitTestKVClient(AZ1_WORKER2, client2);
    DS_ASSERT_OK(client2->Set(key, "value1", param));
    ASSERT_TRUE(cluster_->ShutdownNode(WORKER, AZ1_WORKER2));

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(AZ2_WORKER1, client1);
    std::string val;
    DS_ASSERT_OK(client1->Get(key, val));
    ASSERT_EQ(val, "value1");
}
}  // namespace st
}  // namespace datasystem

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
 * Description: State client evict tests.
 */

#include "datasystem/kv_client.h"

#include <unistd.h>
#include <atomic>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/rpc/rpc_stub_cache_mgr.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
namespace {
const std::string HOST_IP = "127.0.0.1";
}  // namespace

class KVCacheClientEvictTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.enableSpill = true;
        opts.workerGflagParams =
            "-shared_memory_size_mb=8 -log_monitor=true  -v=1 -spill_size_limit=" + std::to_string(maxSize_);
        opts.numEtcd = 1;
        opts.numWorkers = 1;
        opts.numOBS = 1;
        opts.enableDistributedMaster = "false";
        opts.injectActions = "worker.Spill.Sync:return()";
        for (size_t i = 0; i < opts.numWorkers; i++) {
            std::string dir = GetTestCaseDataDir() + "/worker" + std::to_string(i) + "/shared_disk";
            opts.workerSpecifyGflagParams[i] = FormatString("-shared_disk_directory=%s -shared_disk_size_mb=8", dir);
        }
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitTestKVClientWithTenant(0, client_);
    }

    void InitConnectOptW(uint32_t workerIndex, ConnectOptions &connectOptions, int32_t timeoutMs = 60000)
    {
        HostPort workerAddress;
        ASSERT_TRUE(workerIndex < cluster_->GetWorkerNum());
        DS_ASSERT_OK(cluster_->GetWorkerAddr(workerIndex, workerAddress));
        connectOptions = { .host = workerAddress.Host(), .port = workerAddress.Port(), .connectTimeoutMs = timeoutMs };
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    }

    void InitTestKVClientWithTenant(uint32_t workerIndex, std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions;
        InitConnectOptW(workerIndex, connectOptions);
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

protected:
    std::shared_ptr<KVClient> client_;

    uint64_t maxSize_ = 64 * 1024ul * 1024ul;
};

TEST_F(KVCacheClientEvictTest, TestNoneL2CacheEvictTypeBasicFunction)
{
    LOG(INFO) << "Test None L2 cache evictable objects basic function";
    size_t count = 100;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    std::vector<SetParam> params{ { .writeMode = WriteMode::NONE_L2_CACHE_EVICT },
                                  { .writeMode = WriteMode::NONE_L2_CACHE_EVICT,
                                    .ttlSecond = 0,
                                    .existence = ExistenceOpt::NONE,
                                    .cacheType = CacheType::DISK } };
    for (auto &param : params) {
        for (size_t i = 0; i < count; ++i) {
            auto key = client_->Set(data, param);
            ASSERT_FALSE(key.empty());
            keys.emplace_back(std::move(key));
        }

        std::string getKey = keys.back();
        std::string val;
        DS_ASSERT_OK(client_->Get(getKey, val));
        ASSERT_FALSE(val.empty());
    }
}

TEST_F(KVCacheClientEvictTest, TestTimeoutEvict1)
{
    std::shared_ptr<KVClient> client;
    ConnectOptions connectOptions;
    const int wokerIndex = 0;
    const int timeout = 10000;
    InitConnectOptW(wokerIndex, connectOptions, timeout);
    client = std::make_shared<KVClient>(connectOptions);
    DS_ASSERT_OK(client->Init());

    // shared_memory_size_mb = 8mb, object_size = 3mb
    size_t count = 3;
    size_t dataSize = 3 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };

    for (size_t i = 0; i < count; ++i) {
        std::string key;
        (void)client->GenerateKey("", key);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerApi.Create.MockTimeout", "2*return(K_INVALID)"));

    for (size_t i = 0; i < count; ++i) {
        DS_ASSERT_NOT_OK(client->Set(keys[i], data, param));
    }

    std::string getKey = keys.back();
    std::string val;
    DS_ASSERT_NOT_OK(client->Get(getKey, val));
    ASSERT_TRUE(val.empty());
}

TEST_F(KVCacheClientEvictTest, TestTimeoutEvict2)
{
    // shared_memory_size_mb = 8mb, object_size = 3mb
    size_t count = 3;
    size_t dataSize = 3 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };

    for (size_t i = 0; i < count; ++i) {
        std::string key;
        (void)client_->GenerateKey("", key);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    const int wokerIndex = 0;
    const int firstKeyIndex = 0;
    const int secondKeyIndex = 1;
    const int thridKeyIndex = 2;
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, wokerIndex, "WorkerOcServiceCreateImpl.Create.timeoutMs", "2*call()"));
    DS_ASSERT_OK(client_->Set(keys[firstKeyIndex], data, param));
    DS_ASSERT_OK(client_->Set(keys[secondKeyIndex], data, param));
    DS_ASSERT_OK(client_->Set(keys[thridKeyIndex], data, param));

    std::string getKey = keys.back();
    std::string val;
    DS_ASSERT_OK(client_->Get(getKey, val));
    ASSERT_FALSE(val.empty());
}

TEST_F(KVCacheClientEvictTest, TestNoneL2CacheEvictTypeConcurrently)
{
    LOG(INFO) << "Test None L2 cache evictable objects concurrently";
    size_t threadNum = 4;
    size_t count = 25;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::vector<std::thread> threads(threadNum);
    std::string data(dataSize, '0');

    for (size_t i = 0; i < threadNum * count; ++i) {
        std::string key;
        (void)client_->GenerateKey("", key);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    for (size_t i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, count, &data, &keys, i]() {
            SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
            for (size_t k = i * count; k < (i + 1) * count; ++k) {
                DS_ASSERT_OK(client_->Set(keys[k], data, param));
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(KVCacheClientEvictTest, TestWriteL2CacheTypeEvict)
{
    LOG(INFO) << "Test write back/through type evict";
    size_t count = 30;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::string data(dataSize, '0');

    for (size_t i = 0; i < count; ++i) {
        SetParam param;
        param.writeMode = i < count / 2 ? WriteMode::WRITE_BACK_L2_CACHE : WriteMode::WRITE_THROUGH_L2_CACHE;
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    for (size_t i = 0; i < count; ++i) {
        std::string val;
        DS_ASSERT_OK(client_->Get(keys[i], val));
        ASSERT_EQ(val, data);
    }
}

TEST_F(KVCacheClientEvictTest, TestWriteL2CacheTypeEvictAndPublishAgain)
{
    LOG(INFO) << "Test write through type evict and publish again";
    size_t count = 20;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::string data(dataSize, '0');

    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string newData(dataSize, '1');
    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        DS_ASSERT_OK(client_->Set(keys[i], newData, param));
    }

    for (size_t i = 0; i < count; ++i) {
        std::string val;
        DS_ASSERT_OK(client_->Get(keys[i], val));
        ASSERT_EQ(val, newData);
    }
}

TEST_F(KVCacheClientEvictTest, TestSpillDirIsFullAndEvict)
{
    LOG(INFO) << "Test write through type evict and publish again";
    size_t count = 15;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::string data(dataSize, '0');
    std::string keyPrefix = "Marck";

    // 1. Fill up the spill dir and memory space reserved for only one object.
    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
        DS_ASSERT_OK(client_->Set(keyPrefix + std::to_string(i), data, param));
    }

    // 2. Set evictable object, the elder object would be evict.
    std::string keyPrefix1 = "MarckMarck";
    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
        DS_ASSERT_OK(client_->Set(keyPrefix1 + std::to_string(i), data, param));
    }

    // 3. Get last evictable object.
    std::string getVal;
    DS_ASSERT_OK(client_->Get(keyPrefix1 + std::to_string(count - 1), getVal));

    // 4. Get NONE_L2_CACHE objects.
    for (size_t i = 0; i < count; ++i) {
        DS_ASSERT_OK(client_->Get(keyPrefix + std::to_string(i), getVal));
    }

    // 5. Get last evictable object, it has been evict.
    ASSERT_EQ(client_->Get(keyPrefix1 + std::to_string(count - 1), getVal).GetCode(), StatusCode::K_NOT_FOUND);
}

class KVCacheClientEvict2WorkerTest : public KVCacheClientEvictTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVCacheClientEvictTest::SetClusterSetupOptions(opts);
        opts.numWorkers = 2;
    }

    void SetUp() override
    {
        KVCacheClientEvictTest::SetUp();
        InitTestKVClientWithTenant(1, client1_);
    }

protected:
    std::shared_ptr<KVClient> client1_;
};

TEST_F(KVCacheClientEvict2WorkerTest, TestRemoteGetEvictObject)
{
    LOG(INFO) << "Test remote get evicted objects";
    size_t count = 20;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::string data(dataSize, '0');

    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    for (size_t i = 0; i < count; ++i) {
        std::string getVal;
        DS_ASSERT_OK(client1_->Get(keys[i], getVal));
        ASSERT_EQ(getVal, data);
    }
}

TEST_F(KVCacheClientEvict2WorkerTest, LEVEL1_TestEvictPrimaryCopyChange)
{
    LOG(INFO) << "Test evict objects primary copy change";
    size_t count = 10;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::string data(dataSize, '0');

    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    for (size_t i = 0; i < count; ++i) {
        SetParam param{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
        auto key = client1_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    for (size_t i = 0; i < count; ++i) {
        std::string getVal;
        DS_ASSERT_OK(client_->Get(keys[i], getVal));
        ASSERT_EQ(getVal, data);
    }
}

TEST_F(KVCacheClientEvict2WorkerTest, TestLastLocalCopyEvictRemovesMeta)
{
    std::string key = client_->GenerateKey("last_local_copy_evict");
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    DS_ASSERT_OK(client_->Set(key, "value", param));
    HostPort worker0Addr, worker1Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Addr));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Addr));
    auto akSkManager = std::make_shared<AkSkManager>();
    DS_ASSERT_OK(akSkManager->SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
    DS_ASSERT_OK(RpcStubCacheMgr::Instance().Init(100));
    worker::WorkerRemoteMasterOCApi masterApi(worker0Addr, worker1Addr, akSkManager);
    DS_ASSERT_OK(masterApi.Init());
    master::CreateCopyMetaReqPb copyReq;
    master::CreateCopyMetaRspPb copyRsp;
    copyReq.set_object_key(key);
    copyReq.set_address(worker1Addr.ToString());
    DS_ASSERT_OK(masterApi.CreateCopyMeta(copyReq, copyRsp));
    auto removeLocation = [&](const HostPort &addr) {
        master::RemoveMetaReqPb req;
        master::RemoveMetaRspPb rsp;
        req.add_ids(key);
        req.set_address(addr.ToString());
        req.set_cause(master::RemoveMetaReqPb::EVICTION);
        req.set_version(UINT64_MAX);
        DS_ASSERT_OK(masterApi.RemoveMeta(req, rsp));
    };
    removeLocation(worker0Addr);
    removeLocation(worker1Addr);
    master::QueryMetaReqPb queryReq;
    master::QueryMetaRspPb queryRsp;
    std::vector<RpcMessage> payloads;
    queryReq.add_ids(key);
    queryReq.set_address(worker1Addr.ToString());
    DS_ASSERT_OK(masterApi.QueryMeta(queryReq, 0, queryRsp, payloads));
    ASSERT_EQ(queryRsp.not_exist_ids_size(), 1);
}

TEST_F(KVCacheClientEvict2WorkerTest, LEVEL2_TestEvictTypeGetAfterWorkerRestart)
{
    LOG(INFO) << "Test evict objects get after worker restart";
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::string data(dataSize, '0');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    auto key = client_->Set(data, param);
    ASSERT_FALSE(key.empty());

    // Shutdown data node let data loss.
    DS_ASSERT_OK(cluster_->ShutdownNode(ClusterNodeType::WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    std::string getVal;
    // Get.
    ASSERT_EQ(client1_->Get(key, getVal).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(client_->Get(key, getVal).GetCode(), StatusCode::K_NOT_FOUND);

    // Get with subscribe.
    std::thread t1([this, &key, &data, &param]() { DS_ASSERT_OK(client1_->Set(key, data, param)); });
    uint64_t timeoutMs = 5'000;
    DS_ASSERT_OK(client1_->Get(key, getVal, timeoutMs));
    ASSERT_EQ(getVal, data);
    getVal.clear();
    DS_ASSERT_OK(client_->Get(key, getVal, timeoutMs));
    ASSERT_EQ(getVal, data);

    t1.join();
}

class KVCacheClientEvictWithoutSpillTest : public KVCacheClientEvictTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVCacheClientEvictTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams = "-shared_memory_size_mb=24 -log_monitor=true -v=1";
    }
};

TEST_F(KVCacheClientEvictWithoutSpillTest, TestNoneL2CacheEvictTypeBasicFunction)
{
    LOG(INFO) << "Test None L2 cache evictable objects basic function";
    size_t count = 100;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;

    std::string data(dataSize, '0');
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    for (size_t i = 0; i < count; ++i) {
        auto key = client_->Set(data, param);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    std::string getKey = keys.back();
    std::string val;
    DS_ASSERT_OK(client_->Get(getKey, val));
    ASSERT_FALSE(val.empty());
}

TEST_F(KVCacheClientEvictWithoutSpillTest, TestNoneL2CacheEvictTypeConcurrently)
{
    LOG(INFO) << "Test None L2 cache evictable objects concurrently";
    size_t threadNum = 4;
    size_t count = 25;
    size_t dataSize = 4 * 1024ul * 1024ul;
    std::vector<std::string> keys;
    std::vector<std::thread> threads(threadNum);
    std::string data(dataSize, '0');

    for (size_t i = 0; i < threadNum * count; ++i) {
        std::string key;
        (void)client_->GenerateKey("", key);
        ASSERT_FALSE(key.empty());
        keys.emplace_back(std::move(key));
    }

    for (size_t i = 0; i < threadNum; ++i) {
        threads[i] = std::thread([this, count, &data, &keys, i]() {
            SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
            for (size_t k = i * count; k < (i + 1) * count; ++k) {
                DS_ASSERT_OK(client_->Set(keys[k], data, param));
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}
}  // namespace st
}  // namespace datasystem

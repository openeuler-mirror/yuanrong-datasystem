/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
#include "datasystem/common/ak_sk/ak_sk_manager.h"

#include <unistd.h>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <fstream>
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
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

DS_DECLARE_string(etcd_address);
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
        opts.injectActions = "worker.Spill.Sync:return()";
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
                                    .existence = ExistenceOpt::NONE } };
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
    std::string key;
    DS_ASSERT_OK(FindKeyRoutedToWorker(0, "last_local_copy_evict_", key));
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    DS_ASSERT_OK(client_->Set(key, "value", param));
    HostPort worker0Addr, worker1Addr;
    DS_ASSERT_OK(cluster_->GetWorkerAddr(0, worker0Addr));
    DS_ASSERT_OK(cluster_->GetWorkerAddr(1, worker1Addr));
    auto akSkManager = std::make_shared<AkSkManager>();
    DS_ASSERT_OK(akSkManager->SetClientAkSk("QTWAOYTTINDUT2QVKYUC", "MFyfvK41ba2giqM7**********KGpownRZlmVmHc"));
    DS_ASSERT_OK(RpcStubCacheMgr::Instance().Init(100, HostPort(), akSkManager));
    worker::WorkerRemoteMasterOCApi masterApi(worker0Addr, worker1Addr);
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

namespace {
bool PollGetNotFoundUntil(KVClient &client, const std::string &key, int deadlineMs)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(deadlineMs);
    while (std::chrono::steady_clock::now() < deadline) {
        std::string val;
        if (client.Get(key, val).GetCode() == StatusCode::K_NOT_FOUND) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return false;
}

bool PollWorkerLogContains(const std::string &logPath, const std::string &needle, int deadlineMs)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(deadlineMs);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream input(logPath);
        if (input.is_open()) {
            std::string line;
            while (std::getline(input, line)) {
                if (line.find(needle) != std::string::npos) {
                    return true;
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return false;
}
}

// Two workers without spill: evictable primaries go through the END_LIFE path instead of spilling,
// and the DELETE path can be forced deterministically for the get/re-publish race tests.
class KVCacheClientEvictGetSetRaceTest : public KVCacheClientEvictTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        KVCacheClientEvictTest::SetClusterSetupOptions(opts);
        opts.enableSpill = false;
        opts.numWorkers = 2;
        // Flush INFO logs quickly so log-based window assertions observe guard lines in time.
        opts.workerGflagParams += " -logbufsecs=1";
    }

    void SetUp() override
    {
        KVCacheClientEvictTest::SetUp();
        InitTestKVClientWithTenant(1, client1_);
    }

protected:
    std::shared_ptr<KVClient> client1_;
};

TEST_F(KVCacheClientEvictGetSetRaceTest, TestEvictDeletePathGetNotFoundThenRepublish)
{
    LOG(INFO) << "Test evict DELETE path: republish advances the version and the stale RemoveMeta(EVICTION) is skipped";
    const size_t dataSize = 4 * 1024ul * 1024ul;
    const size_t padSize = 1024ul * 1024ul;
    std::string targetKey;
    DS_ASSERT_OK(FindKeyRoutedToWorker(0, "evict_delete_get_set_race_", targetKey));
    const std::string newVal(dataSize, 'b');

    // Hold only the eviction's own RemoveMeta send path, so the 3s residual-metadata window is
    // deterministic: Del-triggered NORMAL RemoveMeta requests take a different code path and never
    // consume this hold.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0,
        "WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.beforeSend", "1*sleep(3000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "evictAction.setDelete", "1*call()"));
    Raii clearInjects([&]() {
        (void)cluster_->ClearInjectAction(WORKER, 0,
            "WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.beforeSend");
        (void)cluster_->ClearInjectAction(WORKER, 0, "evictAction.setDelete");
    });

    SetParam evictParam{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    DS_ASSERT_OK(client_->Set(targetKey, std::string(dataSize, 'a'), evictParam));
    std::vector<std::string> paddings;
    DS_ASSERT_OK(FindKeysRoutedToWorker(0, "protected_del_", 5, paddings));
    for (const auto &padKey : paddings) {
        DS_ASSERT_OK(client_->Set(padKey, std::string(padSize, 'p'),
                                  SetParam{ .writeMode = WriteMode::NONE_L2_CACHE }));
    }

    for (const auto &padKey : paddings) {
        DS_ASSERT_OK(client_->Del(padKey));
    }

    // Re-publish while the stale RemoveMeta(EVICTION) is held at the master: the create overwrites the
    // residual metadata with a newer version, and the stale eviction cleanup must be skipped by the
    // version guard instead of erasing the new object's metadata.
    DS_ASSERT_OK(client_->Set(targetKey, newVal, evictParam));

    std::this_thread::sleep_for(std::chrono::milliseconds(3'500));
    // The version guard rejecting the stale eviction ack is the observable proof the residual-meta
    // window was really constructed; without it the case would pass even if the eviction cleanup had
    // simply won the race.
    const std::string worker0Log =
        FormatString("%s/worker0/log/datasystem_worker.INFO.log", cluster_->GetRootDir());
    ASSERT_TRUE(PollWorkerLogContains(worker0Log, "Remove meta version", 5'000))
        << "version guard line not found, log: " << worker0Log;
    std::string val;
    DS_ASSERT_OK(client_->Get(targetKey, val));
    ASSERT_EQ(val, newVal);
    DS_ASSERT_OK(client1_->Get(targetKey, val));
    ASSERT_EQ(val, newVal);
}

TEST_F(KVCacheClientEvictGetSetRaceTest, TestEvictDeletePathSetNxDuringResidualMetaWindow)
{
    LOG(INFO) << "Test evict DELETE path: SetNx is idempotent inside the residual meta window and real after cleanup";
    const size_t dataSize = 4 * 1024ul * 1024ul;
    const size_t padSize = 1024ul * 1024ul;
    const std::string targetKey = "evict_delete_setnx_race";

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0,
        "WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.beforeSend", "1*sleep(3000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "evictAction.setDelete", "1*call()"));
    Raii clearInjects([&]() {
        (void)cluster_->ClearInjectAction(WORKER, 0,
            "WorkerOcEvictionManager.RemoveMetaFromMasterForEviction.beforeSend");
        (void)cluster_->ClearInjectAction(WORKER, 0, "evictAction.setDelete");
    });

    SetParam evictParam{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    DS_ASSERT_OK(client_->Set(targetKey, std::string(dataSize, 'a'), evictParam));
    std::vector<std::string> paddings;
    for (int i = 0; i < 5; i++) {
        std::string padKey = "protected_setnx_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(padKey, std::string(padSize, 'p'),
                                  SetParam{ .writeMode = WriteMode::NONE_L2_CACHE }));
        paddings.push_back(std::move(padKey));
    }

    for (const auto &padKey : paddings) {
        DS_ASSERT_OK(client_->Del(padKey));
    }

    // Accepted known window: eviction deleted the local copy while the RemoveMeta(EVICTION) cleanup
    // is still in flight (bounded by the eviction batch delay, ~10ms, plus rpc/retries). Inside the
    // window SetNx still answers idempotent-success even though this replica's data is gone. In the
    // production DELETE path every eviction candidate keeps a data backstop (other replicas for
    // non-primary copies, L2 for write-through/write-back-done objects), so "key exists" is the
    // semantically right answer there; objects without any backstop take the END_LIFE path, whose
    // window is closed by this change. Closing this window as well would need a delete-intent
    // notification protocol, which was evaluated and rejected as disproportionate. The K_NOT_FOUND
    // assertion below documents the tradeoff explicitly instead of presenting it as an
    // idempotent-success guarantee.
    SetParam nxParam{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT, .ttlSecond = 0, .existence = ExistenceOpt::NX };
    DS_ASSERT_OK(client_->Set(targetKey, std::string(dataSize, 'x'), nxParam));
    std::string val;
    ASSERT_EQ(client1_->Get(targetKey, val).GetCode(), StatusCode::K_NOT_FOUND);

    // After the stale RemoveMeta(EVICTION) cleans the metadata, SetNx really creates.
    std::this_thread::sleep_for(std::chrono::milliseconds(3'500));
    DS_ASSERT_OK(client_->Set(targetKey, std::string(dataSize, 'x'), nxParam));
    DS_ASSERT_OK(client_->Get(targetKey, val));
    ASSERT_EQ(val, std::string(dataSize, 'x'));
    DS_ASSERT_OK(client1_->Get(targetKey, val));
    ASSERT_EQ(val, std::string(dataSize, 'x'));
}

TEST_F(KVCacheClientEvictGetSetRaceTest, TestEvictEndLifeGetNotFoundThenRepublish)
{
    LOG(INFO) << "Test evict END_LIFE path: get fast-fails K_NOT_FOUND, immediate republish survives the async cleanup";
    const size_t dataSize = 4 * 1024ul * 1024ul;
    const size_t padSize = 1024ul * 1024ul;
    const std::string targetKey = "evict_endlife_get_set_race";
    const std::string newVal(dataSize, 'b');

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.DeleteObjectWithTryLock.before", "1*sleep(1500)"));
    Raii clearInjects([&]() {
        (void)cluster_->ClearInjectAction(WORKER, 0, "worker.DeleteObjectWithTryLock.before");
    });

    SetParam evictParam{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    DS_ASSERT_OK(client_->Set(targetKey, std::string(dataSize, 'a'), evictParam));
    std::vector<std::string> paddings;
    for (int i = 0; i < 5; i++) {
        std::string padKey = "protected_endlife_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(padKey, std::string(padSize, 'p'),
                                  SetParam{ .writeMode = WriteMode::NONE_L2_CACHE }));
        paddings.push_back(std::move(padKey));
    }

    ASSERT_TRUE(PollGetNotFoundUntil(*client1_, targetKey, 10'000));

    for (const auto &padKey : paddings) {
        DS_ASSERT_OK(client_->Del(padKey));
    }

    DS_ASSERT_OK(client_->Set(targetKey, newVal, evictParam));

    std::string val;
    DS_ASSERT_OK(client_->Get(targetKey, val));
    ASSERT_EQ(val, newVal);
    DS_ASSERT_OK(client1_->Get(targetKey, val));
    ASSERT_EQ(val, newVal);
}
}  // namespace st
}  // namespace datasystem

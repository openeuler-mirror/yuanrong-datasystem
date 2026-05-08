/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: This is used to test the ObjectClient class when URMA is enabled.
 */
#include <gtest/gtest.h>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/immutable_string/immutable_string_pool.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/kv_client.h"
#include "oc_client_common.h"
#include "securec.h"
#include "zmq_curve_test_common.h"

namespace datasystem {
namespace st {
namespace {
const char *HOST_IP = "127.0.0.1";
constexpr int WORKER_NUM = 3;
const int K_2 = 2, K_5 = 5, K_10 = 10, K_100 = 100;
constexpr int64_t SHM_SIZE = 500 * 1024;
constexpr int ABNORMAL_EXIT_CODE = -2;
const char *WARMUP_PREPARE_INJECT = "WorkerOCServiceImpl.PrepareUrmaWarmupObject.done";
const char *WARMUP_REMOTE_GET_INJECT = "WorkerOcServiceGetImpl.WarmupGetObjectFromRemoteWorker.begin";
const char *QUERY_META_INJECT = "worker.before_query_meta";
}  // namespace
class UrmaObjectClientTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        // Enable worker->worker batch get by default.
        opts.vLogLevel = 2;
        opts.workerGflagParams =
            " -shared_memory_size_mb=5120 -payload_nocopy_threshold=1000000 -enable_worker_worker_batch_get=true"
            " -batch_get_threshold_mb=20";
#ifdef USE_URMA
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_urma=true ";
#else
        opts.workerGflagParams += " -arena_per_tenant=1 -enable_urma=false ";
#endif
#ifdef URMA_OVER_UB
        opts.workerGflagParams += " -urma_mode=UB -ipc_through_shared_memory=false ";
#endif
    }

    void SetUp() override
    {
        ImmutableStringPool::Instance().Init();
        intern::StringPool::InitAll();
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    static pid_t ForkForTest(std::function<void()> func)
    {
        pid_t child = fork();
        if (child == 0) {
            // avoid zmq problem when fork.
            std::thread thread(func);
            thread.join();
            exit(0);
        }
        return child;
    }

    int WaitForChildFork(pid_t pid)
    {
        if (pid == 0) {
            return 0;
        }
        int statLoc;
        if (waitpid(pid, &statLoc, 0) < 0) {
            LOG(ERROR) << FormatString("waitpid: %d error!", pid);
            return -1;
        }
        if (WIFEXITED(statLoc)) {
            const int exitStatus = WEXITSTATUS(statLoc);
            if (exitStatus != 0) {
                LOG(ERROR) << FormatString("Non-zero exit status %d from test!", exitStatus);
            }
            return exitStatus;
        } else {
            LOG(ERROR) << FormatString("Non-normal exit %d from child!, status: %d", pid, statLoc);
            return ABNORMAL_EXIT_CODE;
        }
    }

    void RestartWorkerAndWaitReady(std::initializer_list<uint32_t> indexes)
    {
        auto *externalCluster = dynamic_cast<ExternalCluster *>(cluster_.get());
        ASSERT_NE(externalCluster, nullptr);
        DS_ASSERT_OK(externalCluster->RestartWorkerAndWaitReadyOneByOne(indexes));
    }

    void WaitForWorkerInjectExecuteCount(uint32_t workerIdx, const std::string &name, uint64_t expectedCount,
                                         uint64_t timeoutMs = 5000)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
        uint64_t executeCount = 0;
        while (std::chrono::steady_clock::now() < deadline) {
            DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIdx, name, executeCount));
            if (executeCount >= expectedCount) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, workerIdx, name, executeCount));
        ASSERT_GE(executeCount, expectedCount) << name;
    }
};

class UrmaConnectionWarmupTest : public UrmaObjectClientTest {
protected:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.injectActions += opts.injectActions.empty() ? "" : ";";
        opts.injectActions += std::string(WARMUP_PREPARE_INJECT) + ":call();" + WARMUP_REMOTE_GET_INJECT + ":call();"
                              + QUERY_META_INJECT + ":call()";
    }
};

class UrmaConnectionWarmupWithoutBatchGetTest : public UrmaConnectionWarmupTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaConnectionWarmupTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_worker_worker_batch_get=false";
    }
};

class UrmaConnectionWarmupFailureTest : public UrmaObjectClientTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.injectActions += opts.injectActions.empty() ? "" : ";";
        opts.injectActions += std::string(WARMUP_PREPARE_INJECT) + ":call();" + WARMUP_REMOTE_GET_INJECT
                              + ":return(K_RPC_UNAVAILABLE)";
    }
};

class UrmaObjectClientAuthorizationTest : public UrmaObjectClientTest {
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -authorization_enable=true";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
    }

protected:
    std::string tenantId1_ = "tenant1";
    std::string tenantId2_ = "tenant2";
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(UrmaConnectionWarmupTest, StartupAndRestartTriggerWarmup)
{
#ifndef USE_URMA
    GTEST_SKIP() << "URMA warmup ST requires USE_URMA.";
#endif
    for (uint32_t i = 0; i < WORKER_NUM; ++i) {
        WaitForWorkerInjectExecuteCount(i, WARMUP_PREPARE_INJECT, 1);
        WaitForWorkerInjectExecuteCount(i, WARMUP_REMOTE_GET_INJECT, 1, 60'000);
        uint64_t queryMetaCount = 0;
        DS_ASSERT_OK(cluster_->GetInjectActionExecuteCount(WORKER, i, QUERY_META_INJECT, queryMetaCount));
        ASSERT_EQ(queryMetaCount, 0ul);
    }
    RestartWorkerAndWaitReady({ 0 });
    WaitForWorkerInjectExecuteCount(0, WARMUP_PREPARE_INJECT, 1);
    WaitForWorkerInjectExecuteCount(0, WARMUP_REMOTE_GET_INJECT, 1, 60'000);
}

TEST_F(UrmaConnectionWarmupFailureTest, RemoteWarmupFailureDoesNotBlockReady)
{
#ifndef USE_URMA
    GTEST_SKIP() << "URMA warmup ST requires USE_URMA.";
#endif
    for (uint32_t i = 0; i < WORKER_NUM; ++i) {
        WaitForWorkerInjectExecuteCount(i, WARMUP_PREPARE_INJECT, 1);
        WaitForWorkerInjectExecuteCount(i, WARMUP_REMOTE_GET_INJECT, 1, 60'000);
    }
}

TEST_F(UrmaConnectionWarmupWithoutBatchGetTest, RemoteWarmupRunsWhenBatchGetDisabled)
{
#ifndef USE_URMA
    GTEST_SKIP() << "URMA warmup ST requires USE_URMA.";
#endif
    for (uint32_t i = 0; i < WORKER_NUM; ++i) {
        WaitForWorkerInjectExecuteCount(i, WARMUP_PREPARE_INJECT, 1);
        WaitForWorkerInjectExecuteCount(i, WARMUP_REMOTE_GET_INJECT, 1, 60'000);
    }
}

TEST_F(UrmaObjectClientTest, UrmaPutGetDeleteShmTest)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), SHM_SIZE, CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
    ASSERT_TRUE(NotExistsNone(buffers));
    AssertBufferEqual(*buffers[0], data);
    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(UrmaObjectClientTest, TestParallelGetSameObject)
{
    std::shared_ptr<ObjectClient> client;
    InitTestClient(0, client);
    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->GIncreaseRef({ objectKey }, failedObjectKeys));
    DS_ASSERT_OK(client->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), SHM_SIZE, CreateParam{}));
    std::vector<std::future<void>> futs;
    const int numThreads = 100;
    ThreadPool threadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
        futs.emplace_back(threadPool.Submit([this, objectKey, data]() {
            std::shared_ptr<ObjectClient> client;
            InitTestClient(0, client);
            std::vector<Optional<Buffer>> buffers;
            const int numIterations = 10;
            for (int j = 0; j < numIterations; j++) {
                buffers.clear();
                DS_ASSERT_OK(client->Get({ objectKey }, 0, buffers));
                ASSERT_TRUE(NotExistsNone(buffers));
                AssertBufferEqual(*buffers[0], data);
            }
        }));
    }
    for (auto &fut : futs) {
        fut.get();
    }

    DS_ASSERT_OK(client->GDecreaseRef({ objectKey }, failedObjectKeys));
    ASSERT_EQ(failedObjectKeys.size(), size_t(0));
}

TEST_F(UrmaObjectClientTest, TestRepeatedSetOOM)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    const int numPuts = 1000;
    const int half = numPuts / 2;
    // Total 8GB which is larger than the 5GB URMA arena size.
    const int sizePerPut = 8 * 1024 * 1024;
    for (int i = 0; i < numPuts; i++) {
        std::string objectKey = NewObjectKey();
        std::string data = GenRandomString(sizePerPut);
        SetParam param;
        param.ttlSecond = 5;
        DS_ASSERT_OK(client->Set(objectKey, data, param));
        if (i == half) {
            // Sleep for 5 seconds to make the first half of the objects expire,
            // so then the second half of the puts can succeed without OOM.
            sleep(5);
        }
    }
}

TEST_F(UrmaObjectClientTest, TestRepeatedForkGet)
{
    const int sizePerPut = 8 * 1024 * 1024;
    const int numObjects = 10;
    std::vector<std::string> objectKeys;
    for (int i = 0; i < numObjects; i++) {
        objectKeys.emplace_back(NewObjectKey());
    }
    std::string data = GenRandomString(sizePerPut);

    auto child = ForkForTest([this, objectKeys, data]() {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(0, client);
        for (const auto &objectKey : objectKeys) {
            DS_ASSERT_OK(client->Set(objectKey, data));
        }
    });
    DS_ASSERT_TRUE(WaitForChildFork(child), 0);
    const int numSubprocesses = 5;
    for (int i = 0; i < numSubprocesses; i++) {
        child = ForkForTest([this, objectKeys, data]() {
            for (int j = 0; j < numObjects; j++) {
                std::shared_ptr<KVClient> client;
                InitTestKVClient(0, client);
                std::vector<Optional<Buffer>> buffers;
                DS_ASSERT_OK(client->Get({ objectKeys[j] }, buffers));
                ASSERT_TRUE(NotExistsNone(buffers));
                AssertBufferEqual(*buffers[0], data);
            }
        });
        DS_ASSERT_TRUE(WaitForChildFork(child), 0);
    }
}

// bus error happen in aarch64
TEST_F(UrmaObjectClientTest, TestBatchRemoteGet1)
{
    // Test that the batch get path in urma case is working as expected.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 100;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back("values_" + std::to_string(i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(UrmaObjectClientTest, TestBatchRemoteGet2)
{
    // Test specifically batch get for 8KB * 1024, so it needs multiple batches when allocating in URMA case.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<StringView> values;
    std::vector<std::string> valuesForVer;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        valuesForVer.emplace_back(GenRandomString(objectSize));
        values.emplace_back(valuesForVer.back());
    }

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client2->MSet(keys, values, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(valuesForVer[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(valuesForVer[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(UrmaObjectClientTest, TestBatchRemoteGet3)
{
    // Test that with big objects (>= 1M), the logic batches all the other small objects,
    // and allocates memory separate for the big objects.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    const uint64_t bigSize = 1024 * 1024;
    keys.emplace_back("big_data1");
    values.emplace_back(GenRandomString(bigSize));
    const int bigIndex = 300;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back(GenRandomString(objectSize));
        if (i == bigIndex) {
            keys.emplace_back("big_data2");
            values.emplace_back(GenRandomString(bigSize));
        }
    }
    keys.emplace_back("big_data3");
    values.emplace_back(GenRandomString(bigSize));

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(UrmaObjectClientAuthorizationTest, TestBatchRemoteGet4)
{
    // Test that with tenant authorization enabled,
    // the logic still batches the allocation, and the tenant id is selected correctly.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId2_); });
    InitTestKVClient(1, client2, [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId2_); });

    const int numKV = 1024;
    const uint64_t objectSize = 8 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back(GenRandomString(objectSize));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(UrmaObjectClientTest, TestBatchRemoteGetErrorCode1)
{
    // Test the error handling in urma batch get logic.
    const int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string key3 = "key3";
    std::string key4 = "key4";
    const int64_t size1 = (int64_t)1024;
    std::string value1 = GenPartRandomString(size1);
    std::string value4 = GenPartRandomString(size1);
    std::string valueGet;
    std::vector<std::string> valsGet;
    ASSERT_EQ(client1->Set(key1, value1), Status::OK());
    ASSERT_EQ(client1->Set(key4, value4), Status::OK());

    // TestCase1: single-key scenario, key successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase2: single-key scenario, key failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);

    // TestCase3: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key2, key4 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase4: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2, key3 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(UrmaObjectClientTest, TestBatchRemoteGetErrorCode2)
{
    // Test the error handling in urma batch get logic.
    // In this case, the worker->worker get is really batched, and error code is injected.
    const int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string key3 = "key3";
    std::string key4 = "key4";
    const int64_t size1 = (int64_t)1024;
    std::string value0 = GenPartRandomString(size1);
    std::string value1 = GenPartRandomString(size1);
    std::string value2 = GenPartRandomString(size1);
    std::string value3 = GenPartRandomString(size1);
    std::string value4 = GenPartRandomString(size1);
    std::string valueGet;
    std::vector<std::string> valsGet;
    ASSERT_EQ(client1->Set(key0, value0), Status::OK());
    ASSERT_EQ(client1->Set(key1, value1), Status::OK());
    ASSERT_EQ(client1->Set(key2, value2), Status::OK());
    ASSERT_EQ(client1->Set(key3, value3), Status::OK());
    ASSERT_EQ(client1->Set(key4, value4), Status::OK());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.batch_get_failure_for_keys", "call()"));

    // TestCase0: single-key scenario, key fails with OOM
    ASSERT_EQ(client0->Get({ key0 }, valsGet).GetCode(), StatusCode::K_OUT_OF_MEMORY);

    // TestCase0.1: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1, key0 }, valsGet).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(client0->Get({ key0, key1 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase1: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1, key2 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase2: Same as case 1, but the order changes.
    ASSERT_EQ(client0->Get({ key2, key4 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase3: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2, key3 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);

    // TestCase4: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key3, key2 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(UrmaObjectClientTest, TestBatchGetSplitPayload)
{
    std::shared_ptr<ObjectClient> client1, client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const uint64_t KB = 1024;
    const uint64_t MB_10 = K_10 * KB * KB;
    std::string data(MB_10, 'a');
    std::vector<std::string> objectKeys;
    for (int i = 0; i < K_100; i++) {
        const std::string objectKey = NewObjectKey();
        DS_ASSERT_OK(
            client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        objectKeys.push_back(objectKey);
    }

    std::vector<Optional<Buffer>> buffer;
    // Remote Get
    DS_ASSERT_OK(client2->Get(objectKeys, 0, buffer));
    ASSERT_EQ(buffer[0]->GetSize(), MB_10);
    ASSERT_EQ(memcmp(data.data(), buffer[0]->MutableData(), MB_10), 0);
    // Local Get
    buffer.clear();
    DS_ASSERT_OK(client2->Get(objectKeys, 0, buffer));
    ASSERT_EQ(buffer[0]->GetSize(), MB_10);
    ASSERT_EQ(memcmp(data.data(), buffer[0]->MutableData(), MB_10), 0);
}

TEST_F(UrmaObjectClientTest, TestDeadLock)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.before_CreateMetadataToMaster", "1*return(K_WORKER_TIMEOUT)"));
    const size_t dataSize = 1024UL * 1024UL;
    std::string key = "key";
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(client->Set(key, value));

    std::string value2;
    DS_ASSERT_OK(client->Get(key, value2));

    ASSERT_EQ(value, value2);
}

TEST_F(UrmaObjectClientTest, TestMutableData)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    auto func = [&client](size_t dataSize) {
        std::string key = "key";
        std::string value(dataSize, 'a');
        DS_ASSERT_OK(client->Set(key, value));
        SetParam param;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(key, dataSize, param, buffer));
        auto ptr = buffer->MutableData();
        ASSERT_NE(ptr, nullptr);
        ASSERT_EQ(memset_s(ptr, dataSize, 'a', dataSize), EOK);
        DS_ASSERT_OK(buffer->Publish());

        std::string value2;
        DS_ASSERT_OK(client->Get(key, value2));

        ASSERT_EQ(value, value2);
    };
    const size_t smallSize = 1024UL;
    const size_t bigSize = 1024UL * 1024UL;
    func(smallSize);
    func(bigSize);
}

TEST_F(UrmaObjectClientTest, TestMemcopyThenMutableData)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    auto func = [&client](size_t dataSize) {
        std::string key = "key";
        std::string value(dataSize, 'a');
        DS_ASSERT_OK(client->Set(key, value));
        SetParam param;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(key, dataSize, param, buffer));
        std::string temp(dataSize, 'b');
        DS_ASSERT_OK(buffer->MemoryCopy(temp.data(), temp.size()));
        auto ptr = buffer->MutableData();
        ASSERT_NE(ptr, nullptr);
        ASSERT_EQ(memset_s(ptr, dataSize, 'a', dataSize), EOK);
        DS_ASSERT_OK(buffer->Publish());

        std::string value2;
        DS_ASSERT_OK(client->Get(key, value2));

        ASSERT_EQ(value, value2);
    };
    const size_t smallSize = 1024UL;
    const size_t bigSize = 1024UL * 1024UL;
    func(smallSize);
    func(bigSize);
}

class UrmaObjectClientDisableDataReplicationTest : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_data_replication=false ";
    }
};

TEST_F(UrmaObjectClientDisableDataReplicationTest, TestBatchRemoteGet)
{
    // Test that the batch get path in urma case is working as expected.
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 100;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back("values_" + std::to_string(i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Del(keys[i]));
    }

    DS_ASSERT_NOT_OK(client1->Get(keys, valuesGet));
}

TEST_F(UrmaObjectClientDisableDataReplicationTest, TestMultiLocalGet)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const int numKV = 100;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back("values_" + std::to_string(i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client->Get(keys, valuesGet));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    DS_ASSERT_OK(client->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());
}

#ifdef USE_URMA
TEST_F(UrmaObjectClientTest, InvalidFastTransportMemSizeCheck)
{
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions, 5000);
    connectOptions.fastTransportMemSize = 2ULL * 1024ULL * 1024ULL * 1024ULL + 1;
    KVClient client(connectOptions);

    auto status = client.Init();
    ASSERT_EQ(status.GetCode(), StatusCode::K_INVALID);
}

TEST_F(UrmaObjectClientTest, UrmaRemoteGetSmall)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
    LOG(INFO) << "Test case UrmaRemoteGetSmall success";
}

TEST_F(UrmaObjectClientTest, UrmaGetObjMetaInfoTimeoutReturnsError)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    constexpr int requestTimeoutMs = 50;
    constexpr int connectTimeoutMs = 1000;
    InitTestClient(0, client1, connectTimeoutMs, requestTimeoutMs);
    InitTestClient(1, client2, connectTimeoutMs, requestTimeoutMs);

    const std::string objectKey = NewObjectKey();
    const std::string data(1024, 'a');
    DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.GetObjMetaInfo", "1*sleep(200)"));

    std::vector<Optional<Buffer>> buffers;
    Status status = client2->Get({ objectKey }, 0, buffers);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_RPC_UNAVAILABLE) << status.ToString();
}

TEST_F(UrmaObjectClientTest, UrmaRemoteGetBig)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    const uint64_t KB = 1024;
    const uint64_t MB = KB * KB;
    const uint64_t GB = KB * KB * KB;
    const std::vector<uint64_t> sizes{ 10 * MB, 100 * MB, 800 * MB, 2 * GB + 1 * KB };
    for (const auto &size : sizes) {
        LOG(INFO) << "Data size of the iteration: " << size;
        const std::string objectKey = NewObjectKey();
        const std::string data = GenRandomString(size);
        DS_ASSERT_OK(client2->Set(objectKey, data));

        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(client1->Get({ objectKey }, buffers1));

        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(data, std::string(reinterpret_cast<const char *>(buffers1[0]->ImmutableData()), size));
        DS_ASSERT_OK(client2->Del(objectKey));
    }
}

TEST_F(UrmaObjectClientTest, UrmaPutAndRemoteGetTest)
{
    uint64_t size = 1024 * 1024;
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string data = GenRandomString(size);
    size_t test_cnt = 100;
    for (size_t i = 0; i < test_cnt; i++) {
        std::string objectKey = NewObjectKey();
        std::vector<std::string> objectKeys{ objectKey };
        std::vector<std::string> failedObjectKeys;
        DS_ASSERT_OK(client1->GIncreaseRef(objectKeys, failedObjectKeys));
        DS_ASSERT_OK(client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), size, CreateParam{}));
        std::vector<Optional<Buffer>> buffers;

        DS_ASSERT_OK(client2->GIncreaseRef(objectKeys, failedObjectKeys));
        DS_ASSERT_OK(client2->Get(objectKeys, 0, buffers));
        ASSERT_TRUE(NotExistsNone(buffers));
        AssertBufferEqual(*buffers[0], data);
        DS_ASSERT_OK(client2->GDecreaseRef(objectKeys, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
        DS_ASSERT_OK(client1->GDecreaseRef(objectKeys, failedObjectKeys));
        ASSERT_TRUE(failedObjectKeys.empty());
    }
}

TEST_F(UrmaObjectClientTest, UrmaRemoteGetTwoSmallParallel)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();
    const int K_2 = 2;
    ThreadPool threadPool(K_2);
    auto fut = threadPool.Submit([client1, client2, objectKey1]() {
        const int64_t size = 1024;
        std::string data(size, 'a');
        DS_ASSERT_OK(
            client2->Put(objectKey1, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(client1->Get({ objectKey1 }, 0, buffers1));

        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    });
    auto fut2 = threadPool.Submit([client1, client2, objectKey2]() {
        const int64_t size = 1024;
        std::string data(size, 'b');
        DS_ASSERT_OK(
            client2->Put(objectKey2, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        DS_ASSERT_OK(client1->Get({ objectKey2 }, 0, buffers1));

        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    });
    fut.get();
    fut2.get();
    LOG(INFO) << "Test case UrmaRemoteGetTwoSmallParallel success";
}

TEST_F(UrmaObjectClientTest, UrmaRemoteGetSizeChanged)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize",
                                           "1*call(1023)"));
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));

    ASSERT_EQ(buffers1[0]->GetSize(), size);
    ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    LOG(INFO) << "Test case UrmaRemoteGetSmall success";
}

TEST_F(UrmaObjectClientTest, UrmaRemoteBatchGetSizeChanged)
{
    // Test that with batch get, a batch of failure due to size change can be retried automatically.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize",
                                           "10*call(1023)"));
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 100;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back("values_" + std::to_string(i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client1->Get(keys, valuesGet));
    DS_ASSERT_OK(client1->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }
}

TEST_F(UrmaObjectClientTest, UrmaRemoteGetSizeChangedInvalid)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "WorkerOcServiceGetImpl.PrepareGetRequestHelper.changeSize",
                                           "1*call(1023)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "WorkerWorkerOCServiceImpl.GetObjectRemoteImpl.changeDataSize",
                                           "1*call(0)"));
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffers1));

    LOG(INFO) << "Test case UrmaRemoteGetSizeChangedInvalid success";
}

class UrmaObjectClientTestEventMode : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -urma_event_mode=true";
    }
};

TEST_F(UrmaObjectClientTestEventMode, DISABLED_UrmaRemoteGetSmallWithError)
{
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "UrmaManager.CheckCompletionRecordStatus", "call(0, 9)"));

    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);

    std::string objectKey = NewObjectKey();
    std::string data = GenRandomString(SHM_SIZE);

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), SHM_SIZE, CreateParam{}));
    std::vector<Optional<Buffer>> buffers;
    DS_ASSERT_NOT_OK(client1->Get({ objectKey }, 0, buffers));
}

TEST_F(UrmaObjectClientTestEventMode, DISABLED_UrmaRemoteGetSmall)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
    LOG(INFO) << "Test case UrmaRemoteGetSmall success";
}

TEST_F(UrmaObjectClientTestEventMode, DISABLED_UrmaRemoteGetBig)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const uint64_t KB = 1024;
    const uint64_t GB = KB * KB * KB;
    const uint64_t size = 2 * GB + 1 * KB;
    const std::string objectKey = NewObjectKey();
    const std::string data = GenRandomString(size);

    DS_ASSERT_OK(client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));

    ASSERT_EQ(buffers1[0]->GetSize(), size);
    ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
}

TEST_F(UrmaObjectClientTest, UrmaParallelWrite)
{
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 1024;
    const int64_t num_objects = 100;
    std::vector<std::string> objectKeys;
    std::string objectKey = NewObjectKey();
    std::string data(size, 'a');
    for (int i = 0; i < num_objects; i++) {
        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        objectKeys.push_back(objectKey);
    }

    std::vector<std::string> batch;
    std::vector<Optional<Buffer>> buf;
    const int batchSize = 10;
    auto total_time = std::chrono::high_resolution_clock::duration::zero();
    for (int i = 0; i < num_objects; i++) {
        batch.push_back(objectKeys[i]);
        if (((i + 1) % batchSize) == 0) {
            auto start_time = std::chrono::high_resolution_clock::now();
            DS_ASSERT_OK(client1->Get(batch, 0, buf));
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = end_time - start_time;
            total_time += duration;
            for (size_t j = 0; j < batch.size(); ++j) {
                CHECK_EQ(data, std::string(reinterpret_cast<const char *>(buf[j]->ImmutableData()), buf[j]->GetSize()));
            }
        }
    }
    auto total_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(total_time);
    LOG(INFO) << "Average Time per Get req: " << total_time_ms.count() / batchSize << " ms";
}

TEST_F(UrmaObjectClientTest, UrmaGetMultipleWorkers)
{
    std::shared_ptr<ObjectClient> client1, client2, client3;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    InitTestClient(K_2, client3);
    const int64_t size = 1024;
    const int64_t num_objects = 5;
    std::string data(size, 'a');
    std::vector<std::string> objectIds;
    for (int i = 0; i < num_objects; i++) {
        std::string objectId = NewObjectKey();
        std::string objectId1 = NewObjectKey();
        DS_ASSERT_OK(
            client2->Put(objectId, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        DS_ASSERT_OK(
            client3->Put(objectId1, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));
        objectIds.push_back(objectId);
        objectIds.push_back(objectId1);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    std::vector<Optional<Buffer>> buffers1;
    DS_ASSERT_OK(client1->Get({ objectIds }, 0, buffers1));
    for (uint64_t i = 0; i < buffers1.size(); i++) {
        ASSERT_EQ(buffers1[i]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[i]->MutableData(), size), 0);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    LOG(INFO) << "Time take " << duration.count() << " ms";
    LOG(INFO) << "Testcase UrmaGetMultipleWorkers Finished";
}

class UrmaClientWorkerDisableUDS : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams +=
            " -urma_mode=UB -ipc_through_shared_memory=true --enable_data_replication=false "
            "-inject_actions=worker.bind_unix_path:return(K_OK) ";
    }
};

TEST_F(UrmaClientWorkerDisableUDS, CreateBufferOnly)
{
    FLAGS_v = 1;
    const int ttlSecond = 120;
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    const int numPuts = 1000;
    const int testSize = 8 * 1024 * 1024;
    for (int i = 0; i < numPuts; i++) {
        std::string key = NewObjectKey();
        SetParam param;
        param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        param.ttlSecond = ttlSecond;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(key, testSize, param, buffer));
    }
}

TEST_F(UrmaClientWorkerDisableUDS, CreateBufferAndPublish)
{
    FLAGS_v = 1;
    const int ttlSecond = 120;
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    const int numPuts = 1000;
    const int testSize = 8 * 1024 * 1024;
    for (int i = 0; i < numPuts; i++) {
        std::string key = NewObjectKey();
        SetParam param;
        param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        param.ttlSecond = ttlSecond;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(key, testSize, param, buffer));
        DS_ASSERT_OK(buffer->Publish());
    }
}

TEST_F(UrmaClientWorkerDisableUDS, CreateBufferAndMutableData)
{
    FLAGS_v = 1;
    const int ttlSecond = 120;
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    const int numPuts = 1000;
    const int testSize = 8 * 1024 * 1024;
    for (int i = 0; i < numPuts; i++) {
        std::string key = NewObjectKey();
        SetParam param;
        param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        param.ttlSecond = ttlSecond;
        std::shared_ptr<Buffer> buffer;
        DS_ASSERT_OK(client->Create(key, testSize, param, buffer));
        (void)buffer->MutableData();
        (void)buffer->GetSize();
    }
}

TEST_F(UrmaClientWorkerDisableUDS, PublishRpcFaildAndTryAgain)
{
    FLAGS_v = 1;
    std::shared_ptr<KVClient> client;
    const int timeoutMs = 1'000;
    InitTestKVClient(0, client, timeoutMs);
    const int testSize = 1024 * 1024;
    std::string key = NewObjectKey();
    std::string value(testSize, 'a');
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "ShmUnit.FreeMemory", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.after_publish", "1*return(K_TRY_AGAIN)"));
    DS_ASSERT_OK(client->Set(key, value));
    std::string getValue;
    DS_ASSERT_OK(client->Get(key, getValue));
    ASSERT_EQ(getValue, value);
}

TEST_F(UrmaClientWorkerDisableUDS, UrmaConnectionFailedBase)
{
    FLAGS_v = 1;
    std::shared_ptr<KVClient> client;
    const int timeoutMs = 1'000;

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "urma.import_jetty", "1*return(K_URMA_ERROR)"));
    DS_ASSERT_OK(inject::Set("client.set.urma_write_ok", "call()"));
    DS_ASSERT_OK(inject::Set("client.urma_handshake_retry", "pause()"));
    InitTestKVClient(0, client, timeoutMs);
    const int testSize = 1024 * 1024;
    std::string key = NewObjectKey();
    std::string value(testSize, 'a');
    std::shared_ptr<Buffer> buffer;
    DS_ASSERT_OK(client->Set(key, value));
    std::string getValue;
    DS_ASSERT_OK(client->Get(key, getValue));
    ASSERT_EQ(getValue, value);

    ASSERT_EQ(inject::GetExecuteCount("client.set.urma_write_ok"), 0);
    DS_ASSERT_OK(inject::Clear("client.urma_handshake_retry"));
    sleep(1);

    std::string value2(testSize, 'a');
    std::string getValue2;
    DS_ASSERT_OK(client->Set(key, value2));
    DS_ASSERT_OK(client->Get(key, getValue2));
    ASSERT_EQ(getValue2, value2);
    ASSERT_EQ(inject::GetExecuteCount("client.set.urma_write_ok"), 1);
}

class UrmaTestWorkerDisconnect : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.numWorkers -= 1;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams +=
            " -urma_mode=UB -ipc_through_shared_memory=true --enable_data_replication=false"
            " --enable_transport_fallback=false"
            " -inject_actions=worker.bind_unix_path:return(K_OK) ";
    }
};

TEST_F(UrmaTestWorkerDisconnect, StopWorkerForUbDisconnect)
{
    FLAGS_v = 1;
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int numKV = 32;
    const int dataSize = 8 * 1024 * 1024;
    std::string value(dataSize, 'a');
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back(value);
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(keys[i], getValue));
        ASSERT_EQ(value, getValue);
    }

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "urma.ModifyJettyToError", "call()"));
    kill(cluster_->GetWorkerPid(1), SIGTERM);
    WaitForWorkerInjectExecuteCount(0, "urma.ModifyJettyToError", 1);
    LOG(INFO) << "Success";
}

class UrmaObjectClientTestMismatch : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = WORKER_NUM;
        opts.numEtcd = 1;
        opts.enableDistributedMaster = "false";
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        opts.workerConfigs.emplace_back(HOST_IP, GetFreePort());
        for (const auto &addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        // Enable worker->worker batch get by default.
        opts.vLogLevel = 2;
        opts.workerGflagParams =
            " -shared_memory_size_mb=5120 -payload_nocopy_threshold=1000000 -enable_worker_worker_batch_get=true "
            "-arena_per_tenant=1";
        // Specify mismatching enable_urma setting for the workers.
        opts.workerSpecifyGflagParams[0] += " -enable_urma=true ";
        opts.workerSpecifyGflagParams[1] += " -enable_urma=false ";
    }
};

TEST_F(UrmaObjectClientTestMismatch, UrmaRemoteGetDirection1)
{
    // Test that with mismatching enable_urma setting, remote get can still go through.
    // Worker1 with enable_urma=true tries to remote get from worker2 with enable_urma=false,
    // worker1 will handshake to exchange jfr, pre-allocate memory and prepare urma info,
    // but the data will be sent back at payload, and all are fine.
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 10 * 1024 * 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client2->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client1->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
}

TEST_F(UrmaObjectClientTestMismatch, UrmaRemoteGetDirection2)
{
    // Test that with mismatching enable_urma setting, remote get can still go through.
    // Worker2 with enable_urma=false tries to remote get from worker1 with enable_urma=true,
    // worker2 will not prepare anything urma related, so urma_info will be empty,
    // so then worker1 will fallback to non-urma logic, and all are fine.
    std::shared_ptr<ObjectClient> client1;
    std::shared_ptr<ObjectClient> client2;
    InitTestClient(0, client1);
    InitTestClient(1, client2);
    const int64_t size = 10 * 1024 * 1024;
    const int64_t num_objects = 20;
    for (int i = 0; i < num_objects; i++) {
        std::string objectKey = NewObjectKey();
        std::string data(size, 'a');

        DS_ASSERT_OK(
            client1->Put(objectKey, reinterpret_cast<const uint8_t *>(data.data()), data.size(), CreateParam{}));

        std::vector<Optional<Buffer>> buffers1;
        auto start_time = std::chrono::high_resolution_clock::now();
        DS_ASSERT_OK(client2->Get({ objectKey }, 0, buffers1));
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        LOG(INFO) << "Time take " << duration.count() << " ms";
        ASSERT_EQ(buffers1[0]->GetSize(), size);
        ASSERT_EQ(memcmp(data.data(), buffers1[0]->MutableData(), size), 0);
    }
    LOG(INFO) << "Test case UrmaRemoteGetSmall success";
}

class UrmaCqeErrorTest : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
    }
};

TEST_F(UrmaCqeErrorTest, RemoteWorkerPollCqeErrorBaseCase)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const size_t dataSize = 1024 * 512UL;
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(client1->Set("key1", value));
    DS_ASSERT_OK(client1->Set("key2", value));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.CheckCompletionRecordStatus", "1*call(0, 9)"));
    std::string getValue;
    DS_ASSERT_OK(client2->Get("key1", getValue));
    ASSERT_EQ(value, getValue);
    DS_ASSERT_OK(client2->Get("key2", getValue));
    ASSERT_EQ(value, getValue);
}

TEST_F(UrmaCqeErrorTest, RemoteWorkerGetCqeError)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    InitTestKVClient(2, client3);

    const int keyCount = 200;
    const size_t dataSize = 1024 * 512UL;
    std::string value(dataSize, 'a');
    for (int i = 0; i < keyCount; i++) {
        std::string key = "key-" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, value));
    }

    std::string getValue;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.VerifyExclusiveJetty", "1000*call()"));
    DS_ASSERT_OK(client2->Get("key-1", getValue));
    ASSERT_EQ(value, getValue);
    DS_ASSERT_OK(client3->Get("key-100", getValue));
    ASSERT_EQ(value, getValue);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.CheckCompletionRecordStatus",
                                           "100*call(0, 0)->1*call(0, 9)"));
    std::vector<std::thread> threads;
    threads.emplace_back([&client2, &value] {
        for (int i = 0; i < keyCount / 2; i++) {
            std::string key = "key-" + std::to_string(i);
            std::string getValue;
            DS_ASSERT_OK(client2->Get(key, getValue));
            ASSERT_EQ(value, getValue);
        }
    });

    threads.emplace_back([&client3, &value] {
        for (int i = keyCount / 2; i < keyCount; i++) {
            std::string key = "key-" + std::to_string(i);
            std::string getValue;
            DS_ASSERT_OK(client3->Get(key, getValue));
            ASSERT_EQ(value, getValue);
        }
    });

    for (auto &thread : threads) {
        thread.join();
    }
}

TEST_F(UrmaCqeErrorTest, RemoteWorkerMultiGetCqeError)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    std::shared_ptr<KVClient> client3;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    InitTestKVClient(2, client3);

    const int keyCount = 500;
    const size_t dataSize = 1024 * 512UL;
    std::string value(dataSize, 'a');
    for (int i = 0; i < keyCount; i++) {
        std::string key = "key-" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, value));
    }

    std::string getValue;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.VerifyExclusiveJetty", "1000*call()"));
    DS_ASSERT_OK(client2->Get("key-1", getValue));
    ASSERT_EQ(value, getValue);
    DS_ASSERT_OK(client3->Get("key-100", getValue));
    ASSERT_EQ(value, getValue);

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "UrmaManager.CheckCompletionRecordStatus", "10*call(0, 0)->1*call(0, 9)"));
    std::vector<std::thread> threads;
    const int batchSize = 10;
    threads.emplace_back([&client2, &value, batchSize] {
        std::vector<std::string> keys;
        for (int i = 0; i < keyCount / 2; i++) {
            std::string key = "key-" + std::to_string(i);
            keys.emplace_back(std::move(key));
            if (keys.size() == batchSize) {
                std::vector<std::string> getValues;
                DS_ASSERT_OK(client2->Get(keys, getValues));
                for (const auto &val : getValues) {
                    ASSERT_EQ(value, val);
                }
                keys.clear();
            }
        }
    });

    threads.emplace_back([&client3, &value, batchSize] {
        std::vector<std::string> keys;
        for (int i = keyCount / 2; i < keyCount; i++) {
            std::string key = "key-" + std::to_string(i);
            keys.emplace_back(std::move(key));
            if (keys.size() == batchSize) {
                std::vector<std::string> getValues;
                DS_ASSERT_OK(client3->Get(keys, getValues));
                for (const auto &val : getValues) {
                    ASSERT_EQ(value, val);
                }
                keys.clear();
            }
        }
    });

    for (auto &thread : threads) {
        thread.join();
    }
}

TEST_F(UrmaCqeErrorTest, ClientToWorkerSetBaseCase)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const size_t dataSize = 1024 * 512UL;
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(inject::Set("UrmaManager.CheckCompletionRecordStatus", "1*call(0, 9)"));
    DS_ASSERT_OK(client->Set("key1", value));
    DS_ASSERT_OK(client->Set("key2", value));
    std::string getValue;
    DS_ASSERT_OK(client->Get("key1", getValue));
    ASSERT_EQ(value, getValue);
    DS_ASSERT_OK(client->Get("key2", getValue));
    ASSERT_EQ(value, getValue);
}

TEST_F(UrmaCqeErrorTest, ClientToWorkerSetRejectsFallbackPayloadAtOneMb)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const size_t dataSize = UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes;
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(inject::Set("UrmaManager.CheckCompletionRecordStatus", "1*call(0, 9)"));
    Status status = client->Set("key-one-mb", value);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR);
    ASSERT_NE(status.GetMsg().find("fallback tcp failed"), std::string::npos);
}

TEST_F(UrmaCqeErrorTest, ClientToWorkerSetRejectsWhenPendingWouldExceedTenMb)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    constexpr size_t dataSize = 950 * 1024UL;
    constexpr int concurrentReqNum = 12;
    std::vector<Status> statuses(concurrentReqNum, Status::OK());
    std::vector<std::thread> threads;
    std::promise<void> startSignal;
    auto startFuture = startSignal.get_future().share();

    DS_ASSERT_OK(inject::Set("UrmaManager.CheckCompletionRecordStatus", "100*call(0, 9)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.after_publish", "100*sleep(2000)"));

    for (int i = 0; i < concurrentReqNum; ++i) {
        threads.emplace_back([&, i]() {
            startFuture.wait();
            statuses[i] = client->Set("key-pending-limit-" + std::to_string(i), std::string(dataSize, 'a'));
        });
    }
    startSignal.set_value();

    for (auto &thread : threads) {
        thread.join();
    }

    for (const auto &status : statuses) {
        ASSERT_TRUE(status.IsOk());
    }
}

TEST_F(UrmaCqeErrorTest, WorkerToClientGetBaseCase)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const size_t dataSize = 1024 * 512UL;
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(client->Set("key1", value));
    DS_ASSERT_OK(client->Set("key2", value));
    std::string getValue;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.CheckCompletionRecordStatus", "1*call(0, 9)"));
    DS_ASSERT_OK(client->Get("key1", getValue));
    ASSERT_EQ(value, getValue);
    DS_ASSERT_OK(client->Get("key2", getValue));
    ASSERT_EQ(value, getValue);
}

TEST_F(UrmaCqeErrorTest, WorkerToClientGetRejectsFallbackPayloadAtOneMb)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    const size_t dataSize = UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes;
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(client->Set("key-get-one-mb", value));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.CheckCompletionRecordStatus", "1*call(0, 9)"));

    std::string getValue;
    Status status = client->Get("key-get-one-mb", getValue);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR);
    ASSERT_NE(status.GetMsg().find("fallback tcp failed"), std::string::npos);
}

class UrmaAsyncEventTest : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_transport_fallback=false ";
    }
};

TEST_F(UrmaAsyncEventTest, RemoteWorkerGetJfsAsyncEvent)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    const int keyCount = 100;
    const size_t dataSize = 1024 * 512UL;
    std::string value(dataSize, 'a');
    for (int i = 0; i < keyCount; i++) {
        DS_ASSERT_OK(client1->Set("key-" + std::to_string(i), value));
    }

    std::string getValue;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "UrmaManager.VerifyExclusiveJetty", "1000*call()"));
    DS_ASSERT_OK(client2->Get("key-0", getValue));
    ASSERT_EQ(value, getValue);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "UrmaManager.HandleJettyErrAsyncEvent", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "UrmaManager.InjectAsyncEvent",
                                           "2*return(" + std::to_string(static_cast<int>(URMA_EVENT_JETTY_ERR)) + ")"));
    WaitForWorkerInjectExecuteCount(1, "UrmaManager.HandleJettyErrAsyncEvent", 1);

    for (int i = 0; i < keyCount; i++) {
        DS_ASSERT_OK(client2->Get("key-" + std::to_string(i), getValue));
        ASSERT_EQ(value, getValue);
    }
}

class UrmaNumaAffinityTest : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams +=
            " -enable_transport_fallback=false -enable_ub_numa_affinity=true "
            "-shared_memory_distribution_policy=interleave_affinity_numa";
        opts.numWorkers -= 1;
    }
};

TEST_F(UrmaNumaAffinityTest, WorkerToWorker)
{
    FLAGS_v = 1;
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    DS_ASSERT_OK(inject::Set("UrmaManager.UrmaWriteNumaAffinity", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWriteNumaAffinity", "call()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "UrmaManager.UrmaWriteNumaAffinity", "call()"));

    const int numKV = 32;
    const int dataSize = 8 * 1024 * 1024;
    std::string value(dataSize, 'a');
    std::vector<std::string> keys;
    std::vector<std::string> values;
    std::vector<std::pair<std::string, std::string>> kvPairs;
    for (int i = 0; i < numKV; i++) {
        keys.emplace_back("keys_" + std::to_string(i));
        values.emplace_back(value);
    }

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client2->Set(keys[i], values[i]));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        std::string getValue;
        DS_ASSERT_OK(client1->Get(keys[i], getValue));
        ASSERT_EQ(value, getValue);
    }

    uint64_t executeCountClient = inject::GetExecuteCount("UrmaManager.UrmaWriteNumaAffinity");
    ASSERT_EQ(executeCountClient, numKV);

    uint64_t executeCountWorker1;
    DS_ASSERT_OK(
        cluster_->GetInjectActionExecuteCount(WORKER, 0, "UrmaManager.UrmaWriteNumaAffinity", executeCountWorker1));
    ASSERT_EQ(executeCountWorker1, numKV);

    uint64_t executeCountWorker2;
    DS_ASSERT_OK(
        cluster_->GetInjectActionExecuteCount(WORKER, 1, "UrmaManager.UrmaWriteNumaAffinity", executeCountWorker2));
    ASSERT_EQ(executeCountWorker2, numKV);
}

class UrmaFallbackTest : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -client_dead_timeout_s=1 ";
    }
};

TEST_F(UrmaFallbackTest, ClientAndWorkerErrorFallback)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    DS_ASSERT_OK(inject::Set("UrmaManager.UrmaWriteError", "return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWriteError", "return()"));

    std::string key = "UrmaKey";
    std::string value = "UrmaValue";
    DS_ASSERT_OK(client->Set(key, value));

    std::vector<std::string> getValues;
    DS_ASSERT_OK(client->Get({ key }, getValues));
    ASSERT_EQ(getValues.size(), 1);
    ASSERT_EQ(value, getValues[0]);
}

TEST_F(UrmaFallbackTest, WorkerWorkerSignleWriteFallback)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWriteError", "1*return()"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWaitError", "1*return()"));

    std::string key1 = "UrmaKeyWrite";
    std::string key2 = "UrmaKeyWait";
    std::string value1 = "UrmaValue1";
    std::string value2 = "UrmaValue2";
    DS_ASSERT_OK(client1->Set(key1, value1));
    DS_ASSERT_OK(client1->Set(key2, value2));

    std::vector<std::string> getValues;
    DS_ASSERT_OK(client2->Get({ key1 }, getValues));
    ASSERT_EQ(getValues.size(), 1);
    ASSERT_EQ(value1, getValues[0]);

    getValues.clear();
    DS_ASSERT_OK(client2->Get({ key2 }, getValues));
    ASSERT_EQ(getValues.size(), 1);
    ASSERT_EQ(value2, getValues[0]);
}

TEST_F(UrmaFallbackTest, WorkerWorkerRejectsFallbackPayloadAtOneMb)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWriteError", "1*return()"));

    const size_t dataSize = UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes;
    std::string key = "worker-worker-one-mb";
    std::string value(dataSize, 'a');
    DS_ASSERT_OK(client1->Set(key, value));

    std::string getValue;
    Status status = client2->Get(key, getValue);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR);
    ASSERT_NE(status.GetMsg().find("fallback tcp failed"), std::string::npos);
}

TEST_F(UrmaFallbackTest, WorkerWorkerBatchWriteFallback)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.GatherWriteError", "return()"));

    const int batchNum = 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (size_t i = 0; i < batchNum; ++i) {
        std::string key = "KeyWrite_" + std::to_string(i);
        std::string value = "WriteValue" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, value));
        keys.emplace_back(key);
        values.emplace_back(value);
    }

    std::vector<std::string> valuesGet;
    DS_ASSERT_OK(client2->Get(keys, valuesGet));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
    }
}

TEST_F(UrmaFallbackTest, WorkerWorkerBatchGetWaitFallback)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWaitError", "return()"));

    const int batchNum = 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (size_t i = 0; i < batchNum; ++i) {
        std::string key = "KeyWait_" + std::to_string(i);
        std::string value = "WaitValue" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, value));
        keys.emplace_back(key);
        values.emplace_back(value);
    }

    std::vector<std::string> valuesGet;
    DS_ASSERT_OK(client2->Get(keys, valuesGet));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
    }
}

TEST_F(UrmaFallbackTest, UrmaHandshakeTimeoutReturnEarlyAndContinueInBackground)
{
    FLAGS_v = 1;
    std::shared_ptr<KVClient> client;
    const int timeoutMs = 2'000;
    const int firstHandshakeSleepMs = 3'000;
    const int initReturnLowerBoundMs = 1'500;
    const int initReturnUpperBoundMs = 3'500;

    DS_ASSERT_OK(inject::Set("client.urma_first_handshake_delay", "sleep(3000)"));

    auto start = std::chrono::steady_clock::now();
    InitTestKVClient(0, client, timeoutMs);
    auto initElapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    ASSERT_GT(initElapsedMs, initReturnLowerBoundMs);
    ASSERT_LT(initElapsedMs, initReturnUpperBoundMs);

    const int testSize = 1024 * 1024;
    std::string key1 = NewObjectKey();
    std::string value1(testSize, 'a');
    std::string getValue1;
    DS_ASSERT_OK(client->Set(key1, value1));
    DS_ASSERT_OK(client->Get(key1, getValue1));
    ASSERT_EQ(getValue1, value1);

    usleep((firstHandshakeSleepMs + timeoutMs) * 1000);

    std::string key2 = NewObjectKey();
    std::string value2(testSize, 'b');
    std::string getValue2;
    DS_ASSERT_OK(client->Set(key2, value2));
    DS_ASSERT_OK(client->Get(key2, getValue2));
    ASSERT_EQ(getValue2, value2);

    DS_ASSERT_OK(inject::Clear("client.urma_first_handshake_delay"));
}

class UrmaDisableFallbackTest : public UrmaObjectClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        UrmaObjectClientTest::SetClusterSetupOptions(opts);
        opts.workerGflagParams += " -enable_transport_fallback=false ";
    }
};

TEST_F(UrmaDisableFallbackTest, TestUrmaRemoteGetFailed)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWriteError", "return()"));

    std::string key1 = "UrmaKeyWrite";
    std::string key2 = "UrmaKeyWait";
    std::string value1 = "UrmaValue1";
    std::string value2 = "UrmaValue2";
    DS_ASSERT_OK(client1->Set(key1, value1));
    DS_ASSERT_OK(client1->Set(key2, value2));

    std::vector<std::string> getValues;
    DS_ASSERT_NOT_OK(client2->Get({ key1, key2 }, getValues));
}

TEST_F(UrmaDisableFallbackTest, TestUrmaRemoteGetWaitTimeoutReturnsUrmaWaitTimeout)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "UrmaManager.UrmaWaitError", "return()"));

    std::string key = "UrmaKeyWaitTimeout";
    std::string value = "UrmaValueWaitTimeout";
    DS_ASSERT_OK(client1->Set(key, value));

    std::vector<std::string> getValues;
    Status status = client2->Get({ key }, getValues);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_WAIT_TIMEOUT) << status.ToString();
}

TEST_F(UrmaDisableFallbackTest, UrmaRemoteGetReconnectAfterWorkerRestart)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    std::string firstKey = "urma-reconnect-before-restart";
    std::string firstValue = GenRandomString(256 * 1024);
    DS_ASSERT_OK(client2->Set(firstKey, firstValue));

    std::string getValue;
    DS_ASSERT_OK(client1->Get(firstKey, getValue));
    ASSERT_EQ(firstValue, getValue);

    RestartWorkerAndWaitReady({ 1 });

    std::shared_ptr<KVClient> restartedClient2;
    InitTestKVClient(1, restartedClient2);
    std::string secondKey = "urma-reconnect-after-restart";
    std::string secondValue = GenRandomString(256 * 1024);
    DS_ASSERT_OK(restartedClient2->Set(secondKey, secondValue));

    DS_ASSERT_OK(client1->Get(secondKey, getValue));
    ASSERT_EQ(secondValue, getValue);
}

#endif
}  // namespace st
}  // namespace datasystem

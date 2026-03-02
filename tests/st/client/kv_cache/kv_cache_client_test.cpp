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
#include <algorithm>
#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "client/object_cache/oc_client_common.h"
#include "cluster/base_cluster.h"
#include "common.h"
#include "common_distributed_ext.h"
#include "datasystem/client/object_cache/client_worker_api.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"

#define RETRY_IF_OUT_MEMORY(rc_, statement_, maxRetryCnt_)                      \
    do {                                                                        \
        int currCnt_ = 0;                                                       \
        do {                                                                    \
            rc_ = (statement_);                                                 \
            if (rc_.IsOk() || rc_.GetCode() != K_OUT_OF_MEMORY) {               \
                break;                                                          \
            }                                                                   \
            currCnt_++;                                                         \
            usleep(100'000);                                                    \
            LOG(INFO) << "OOM happen, retry " << currCnt_ << " times";          \
        } while (rc_.GetCode() == K_OUT_OF_MEMORY && currCnt_ <= maxRetryCnt_); \
    } while (0)

DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
constexpr int WAIT_ASYNC_NOTIFY_WORKER = 300;
constexpr int64_t NON_SHM_SIZE = 50 * 1024;
constexpr int64_t SHM_SIZE = 500 * 1024;
class KVCacheClientTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true -max_client_num=2000";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
        InitClients();
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
        client2_.reset();
        client3_.reset();
        client4_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitClients()
    {
        InitTestKVClient(0, client_);
        InitTestKVClient(0, client1_);
        InitTestKVClient(0, client2_);
        InitTestKVClient(0, client3_);
        InitTestKVClient(0, client4_);
    }

    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
    std::shared_ptr<KVClient> client3_;
    std::shared_ptr<KVClient> client4_;
};

TEST_F(KVCacheClientTest, TestKVCacheClientInitByEnvSuccess)
{
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    int replace = 1;
    (void)setenv("DATASYSTEM_HOST", connectOptions.host.c_str(), replace);
    (void)setenv("DATASYSTEM_PORT", std::to_string(connectOptions.port).c_str(), replace);
    (void)setenv("DATASYSTEM_CONNECT_TIME_MS", std::to_string(connectOptions.connectTimeoutMs).c_str(), replace);
    (void)setenv("DATASYSTEM_TOKEN", connectOptions.token.GetData(), replace);
    (void)setenv("DATASYSTEM_CLIENT_PUBLIC_KEY", connectOptions.clientPublicKey.c_str(), replace);
    (void)setenv("DATASYSTEM_CLIENT_PRIVATE_KEY", connectOptions.clientPrivateKey.GetData(), replace);
    (void)setenv("DATASYSTEM_SERVER_PUBLIC_KEY", connectOptions.serverPublicKey.c_str(), replace);
    (void)setenv("DATASYSTEM_ACCESS_KEY", connectOptions.accessKey.c_str(), replace);
    (void)setenv("DATASYSTEM_SECRET_KEY", connectOptions.secretKey.GetData(), replace);
    std::shared_ptr<KVClient> client = std::make_shared<KVClient>();
    DS_ASSERT_OK(client->Init());
}

TEST_F(KVCacheClientTest, TestKVCacheClientInitByEnvFailedWithNotEnoughParam)
{
    int replace = 1;
    (void)setenv("DATASYSTEM_HOST", "127.0.0.1", replace);
    std::shared_ptr<KVClient> client = std::make_shared<KVClient>();
    DS_ASSERT_NOT_OK(client->Init());
}

TEST_F(KVCacheClientTest, TestKVCacheClientInitByEnvFailedWithNotEnoughParam2)
{
    std::shared_ptr<KVClient> client = std::make_shared<KVClient>();
    DS_ASSERT_NOT_OK(client->Init());
}

TEST_F(KVCacheClientTest, TestSetWriteMode)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    Optional<ReadOnlyBuffer> buffer;
    ASSERT_EQ(client->Get(key, buffer), Status::OK());
    ASSERT_EQ(value, std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()));
    ASSERT_EQ(client->Del(key), Status::OK());
    std::string key1 = "key1";
    std::string value1 = "value1";
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    ASSERT_EQ(client->Set(key1, value1, param1), Status::OK());
    std::string valueGet1;
    ASSERT_EQ(client->Get(key1, valueGet1), Status::OK());
    ASSERT_EQ(value1, std::string(valueGet1.data(), valueGet1.size()));
    Optional<ReadOnlyBuffer> buffer1;
    ASSERT_EQ(client->Get(key1, buffer1), Status::OK());
    ASSERT_EQ(value1, std::string(reinterpret_cast<const char *>(buffer1->ImmutableData()), buffer1->GetSize()));
    ASSERT_EQ(client->Del(key1), Status::OK());
}

TEST_F(KVCacheClientTest, TestFirstStubConnectFailed)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "ZmqSockConnHelper.StubConnect", "1*return(K_RPC_DEADLINE_EXCEEDED)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "ZmqBaseStubConn.WaitForConnect", "sleep(3000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "RpcStubCacheMgr.EnableOcWorkerWorkerDirectPort", "return()"));
    std::string key = "key";
    std::string value = GenRandomString(1024 * 1024);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client1->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
}

TEST_F(KVCacheClientTest, TestRemoteGetStatus)
{
    std::shared_ptr<KVClient> client;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client);
    InitTestKVClient(1, client1);
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "worker.GetObjectFromAnywhere", "return(K_NOT_FOUND_IN_L2CACHE)"));

    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    std::string key = client1->GenerateKey();
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string valueGet, val1;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    auto status = client1->Get(key, val1);
    ASSERT_EQ(status.GetCode(), K_NOT_FOUND);
    ASSERT_TRUE(status.GetMsg().find("Cannot get object from worker and l2 cache") != std::string::npos);
}

TEST_F(KVCacheClientTest, TestMsetAndMGet)
{
    std::shared_ptr<KVClient> client;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client);
    InitTestKVClient(1, client1);
    std::vector<std::string> keys;
    std::vector<StringView> values;
    size_t bigSize = 1024UL;
    size_t batchSize = 10'000UL;
    std::vector<std::string> vals;
    for (size_t i = 0; i < batchSize; ++i) {
        keys.emplace_back("key" + std::to_string(i));
        vals.emplace_back(GenRandomString(bigSize));
        values.emplace_back(vals.back());
    }
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client->MSet(keys, values, failedKeys, MSetParam()));
    std::vector<std::string> getVals;
    DS_ASSERT_OK(client1->Get(keys, getVals));
    for (size_t i = 0; i < batchSize; ++i) {
        ASSERT_EQ(vals[i], getVals[i]);
    }
}

TEST_F(KVCacheClientTest, TestSetAndExistConcurrently)
{
    std::shared_ptr<KVClient> client;
    std::shared_ptr<KVClient> client1;
    int32_t timeoutMs = 1000;
    InitTestKVClient(0, client, timeoutMs);
    InitTestKVClient(1, client1, timeoutMs);

    // worker.PublishObjectWithLock.begin
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.PublishObjectWithLock.begin", "sleep(100)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "OCMetadataManager.QueryMeta,wait", "sleep(500)"));

    std::string key = client->GenerateKey();
    std::string value = "value";
    std::thread t1([client, &key, &value]() { DS_ASSERT_OK(client->Set(key, StringView(value), SetParam{})); });
    std::thread t2([client, &key]() {
        std::vector<bool> exists;
        DS_ASSERT_OK(client->Exist({ key }, exists));
    });
    t1.join();
    t2.join();

    std::string getVal;
    DS_ASSERT_OK(client1->Get(key, getVal));
    ASSERT_EQ(value, getVal);
}

TEST_F(KVCacheClientTest, SubscribeTimeoutTest)
{
    std::shared_ptr<KVClient> client1, client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    auto key = client1->GenerateKey();
    std::thread t1([&] () {
        std::string val;
        DS_ASSERT_OK(client1->Get(key, val, 20000));  // timeout is 20000 ms
    });
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.remote_get_failed", "1*sleep(15000)"));
    DS_ASSERT_OK(client2->Set(key, "aaaaaa"));
    t1.join();
}

TEST_F(KVCacheClientTest, TestSingleKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key1";
    std::string value = "value1";
    ASSERT_EQ(client->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    Optional<ReadOnlyBuffer> buffer;
    ASSERT_EQ(client->Get(key, buffer), Status::OK());
    ASSERT_EQ(value, std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()));

    ASSERT_EQ(client->Del(key), Status::OK());
    ASSERT_EQ(client->Get(key, valueGet, 5).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(client->Get(key, buffer, 5).GetCode(), StatusCode::K_NOT_FOUND);

    ASSERT_EQ(client->Del(key), Status::OK());
    ASSERT_EQ(client->Del("key2"), Status::OK());
}

TEST_F(KVCacheClientTest, TestGetFuncSingleKeyErrorCode)
{
    int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::string key = "key1";
    int64_t size1 = (int64_t)1024 * 1024 * 2;
    std::string value = GenPartRandomString(size1);
    std::string valueGet;
    ASSERT_EQ(client0->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(client0->Set(key, value), Status::OK());

    // TestCase1: single-key scenario, if KeyNotFound occurs during worker processing, the SDK returns
    // K_RUNTIME_ERROR.
    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 1, "worker.before_GetObjectFromRemoteWorkerAndDump", "return(K_NOT_FOUND)"));
    DS_ASSERT_OK(inject::Set("Get.RetryOnError.retry_on_error_after_func", "1*sleep(1000)"));
    ASSERT_EQ(client1->Get(key, valueGet).GetCode(), StatusCode::K_RUNTIME_ERROR);
    DS_ASSERT_OK(cluster_->ClearInjectAction(WORKER, 1, "worker.before_GetObjectFromRemoteWorkerAndDump"));

    // TestCase2: If the worker response returns an error code, the SDK returns the same
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.after_add_remote_get_objects", "1*return(K_NOT_FOUND)"));
    DS_ASSERT_OK(inject::Set("Get.RetryOnError.retry_on_error_after_func", "1*sleep(1000)"));
    ASSERT_EQ(client1->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);

    // TestCase3: single-key scenario, K_RPC_UNAVAILABLE Occurs When a Client Accesses a Worker.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.before_query_meta", "1*sleep(2000)"));
    ASSERT_EQ(client1->Get(key, valueGet).GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    const int kWaitForSleepCompletion = 3;
    sleep(kWaitForSleepCompletion);
}

TEST_F(KVCacheClientTest, TestGetFuncMultiKeyErrorCode)
{
    int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string key3 = "key3";
    int64_t size1 = (int64_t)1024;
    std::string value = GenPartRandomString(size1);
    std::string valueGet;
    std::vector<std::string> valsGet;
    ASSERT_EQ(client0->Set(key1, value), Status::OK());

    // TestCase1: multi-key scenario, Some keys are successful, and the SDK returns a success code.
    ASSERT_EQ(client0->Get({ key1, key2 }, valsGet).GetCode(), StatusCode::K_OK);

    // TestCase2: multi-key scenario, All keys are failed and response is ok, the SDK returns K_NOT_FOUND.
    ASSERT_EQ(client0->Get({ key2, key3 }, valsGet).GetCode(), StatusCode::K_NOT_FOUND);

    // TestCase3: multi-key scenario, All keys are failed and response has error code, the SDK returns the failed code.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.before_query_meta", "return(K_IO_ERROR)"));
    ASSERT_EQ(client1->Get({ key2, key3 }, valsGet).GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(KVCacheClientTest, TestSetFail)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    auto key = client_->GenerateKey();
    SetParam param;
    param.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "PutToEtcdStore.failed", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_NOT_OK(client->Set(key, "aaaaaaa", param));
    std::string val;
    ASSERT_EQ(client->Get({ key }, val).GetCode(), K_NOT_FOUND);
}

TEST_F(KVCacheClientTest, TestReadOnlyBufferFunction)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key = "key";
    std::string value = "value";
    ASSERT_EQ(client->Set(key, value), Status::OK());
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client->Get({ key }, buffers, 0));
    ASSERT_EQ(buffers.size(), 1ul);
    ASSERT_TRUE(buffers[0]);
    ASSERT_EQ((uint64_t)buffers[0]->GetSize(), value.size());
    DS_ASSERT_OK(buffers[0]->RLatch());
    DS_ASSERT_OK(buffers[0]->UnRLatch());
    ASSERT_NE(buffers[0]->ImmutableData(), nullptr);

    auto other = std::move(buffers[0]);
    ASSERT_TRUE(other);
    ASSERT_EQ((uint64_t)other->GetSize(), value.size());
    DS_ASSERT_OK(other->RLatch());
    DS_ASSERT_OK(other->UnRLatch());
    ASSERT_NE(other->ImmutableData(), nullptr);
}

TEST_F(KVCacheClientTest, TestSetAndGetSubscribeTimeout)
{
    std::shared_ptr<KVClient> client0;
    InitTestKVClient(0, client0);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.RemoveSubscribeCache.deadlock", "1*sleep(2000)"));

    std::string key = "QuQu_Paul";
    std::string val;
    DS_ASSERT_NOT_OK(client0->Get(key, val, 1'000));
    val = "58";
    DS_ASSERT_OK(client0->Set(key, val));
}

TEST_F(KVCacheClientTest, DISABLED_TestSpecialKeyVal)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    ASSERT_EQ(client->Set("", "value").GetCode(), StatusCode::K_INVALID);
    std::string value;
    ASSERT_EQ(client->Get("", value).GetCode(), StatusCode::K_INVALID);
    Optional<ReadOnlyBuffer> buffer;
    ASSERT_EQ(client->Get("", buffer).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(client->Del("").GetCode(), StatusCode::K_INVALID);

    ASSERT_EQ(client->Set("key1", "").GetCode(), StatusCode::K_INVALID);

    std::vector<std::string> vals;
    ASSERT_EQ(client->Get({ "key2", "" }, vals).GetCode(), StatusCode::K_INVALID);
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    ASSERT_EQ(client->Get({ "key2", "" }, buffers).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(client->Get({}, vals).GetCode(), StatusCode::K_INVALID);
    ASSERT_EQ(client->Get({}, buffers).GetCode(), StatusCode::K_INVALID);
    std::vector<std::string> failedKeys;
    ASSERT_EQ(client->Del({}, failedKeys).GetCode(), StatusCode::K_INVALID);

    ASSERT_EQ(client->Set("key2", "v2").GetCode(), StatusCode::K_OK);
    ASSERT_EQ(client->Set("key3", "v3").GetCode(), StatusCode::K_OK);

    vals.clear();
    ASSERT_EQ(client->Get({ "key2", "key3", "key3" }, vals).GetCode(), StatusCode::K_OK);
    ASSERT_TRUE(NotExistsNone(vals));
    ASSERT_EQ(std::string(vals[0].data(), vals[0].size()), "v2");
    ASSERT_EQ(std::string(vals[1].data(), vals[1].size()), "v3");

    buffers.clear();
    ASSERT_EQ(client->Get({ "key2", "key3", "key3" }, buffers).GetCode(), StatusCode::K_OK);
    ASSERT_EQ("v2", std::string(reinterpret_cast<const char *>(buffers[0]->ImmutableData()), buffers[0]->GetSize()));
    ASSERT_EQ("v3", std::string(reinterpret_cast<const char *>(buffers[1]->ImmutableData()), buffers[0]->GetSize()));

    failedKeys.clear();
    LOG(INFO) << "Start to delete.";
    ASSERT_EQ(client->Del({ "key2", "key3", "key3" }, failedKeys), Status::OK());
    ASSERT_EQ(failedKeys.size(), 0ul);
}

TEST_F(KVCacheClientTest, TestSetValWithoutKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    SetParam param = { .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    cluster_->SetInjectAction(WORKER, 0, "worker.save_to_redis_failure", "1*return(K_UNKNOWN_ERROR)");
    std::string value = "value";
    for (int i = 0; i < 2; i++) {
        auto key = client->Set(value, param);
        if (i == 1) {
            ASSERT_NE(key, "");
        }
        std::vector<std::string> valuesGet;
        std::vector<Optional<ReadOnlyBuffer>> buffers;
        if (!key.empty()) {
            DS_ASSERT_OK(client->Get({ key }, valuesGet));
            DS_ASSERT_OK(client->Get({ key }, buffers));
            std::vector<std::string> failedKeys;
            DS_ASSERT_OK(client->Del({ key }, failedKeys));
        }
    }
}

TEST_F(KVCacheClientTest, TestMultiKey)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::vector<std::string> keys = { "keys_1", "keys_2", "keys_3" };
    std::vector<std::string> values = { "values_1", "values_2", "values_3" };

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client->Set(keys[i], values[i]));
    }

    std::vector<std::string> valuesGet;
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client->Get(keys, valuesGet));
    DS_ASSERT_OK(client->Get(keys, buffers));
    ASSERT_TRUE(NotExistsNone(valuesGet));
    ASSERT_EQ(keys.size(), valuesGet.size());
    ASSERT_EQ(keys.size(), buffers.size());

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client->Del(keys, failedKeys));
    ASSERT_EQ(0ul, failedKeys.size());

    valuesGet.clear();
    buffers.clear();
    DS_ASSERT_NOT_OK(client->Get(keys, valuesGet));
    DS_ASSERT_NOT_OK(client->Get(keys, buffers));

    failedKeys.clear();
    std::vector<std::string> notExistsKeys = { "notexists_1", "notexists_2" };
    DS_ASSERT_OK(client->Del(notExistsKeys, failedKeys));
    ASSERT_EQ(failedKeys.size(), 0ul);
}

TEST_F(KVCacheClientTest, TestMultiKeyPartiallyGetDel)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::vector<std::string> keys = { "keys_1", "keys_2", "keys_3" };
    std::vector<std::string> values = { "values_1", "values_2", "values_3" };

    for (size_t i = 0; i < keys.size(); i++) {
        DS_ASSERT_OK(client->Set(keys[i], values[i]));
    }

    std::vector<std::string> keysMore = keys;
    keysMore.emplace_back("keys_4");
    std::vector<std::string> valuesGet;
    DS_ASSERT_OK(client->Get(keysMore, valuesGet));
    ASSERT_TRUE(ExistsNone(valuesGet));
    ASSERT_EQ(keysMore.size(), valuesGet.size());
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client->Get(keysMore, buffers));
    ASSERT_EQ(keysMore.size(), buffers.size());
    ASSERT_EQ(values[0],
              std::string(reinterpret_cast<const char *>(buffers[0]->ImmutableData()), buffers[0]->GetSize()));
    ASSERT_EQ(values[1],
              std::string(reinterpret_cast<const char *>(buffers[1]->ImmutableData()), buffers[1]->GetSize()));
    ASSERT_EQ(values[2],
              std::string(reinterpret_cast<const char *>(buffers[2]->ImmutableData()), buffers[2]->GetSize()));
    ASSERT_FALSE(buffers[3]);

    for (size_t i = 0; i < keys.size(); i++) {
        ASSERT_EQ(values[i], std::string(valuesGet[i].data(), valuesGet[i].size()));
        ASSERT_EQ(values[i],
                  std::string(reinterpret_cast<const char *>(buffers[i]->ImmutableData()), buffers[i]->GetSize()));
    }

    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client->Del({ "keys_1", "keys_2", "not_exists" }, failedKeys));
    ASSERT_EQ(failedKeys.size(), 0ul);
}

TEST_F(KVCacheClientTest, TestAllowModify)
{
    FLAGS_v = 1;
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::string key = "key1";
    std::string value = "value1";
    DS_ASSERT_OK(client->Set(key, "hi"));
    DS_ASSERT_OK(client->Set(key, value));
    std::string valueGet;
    DS_ASSERT_OK(client->Get(key, valueGet));
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    Optional<ReadOnlyBuffer> buffer;
    DS_ASSERT_OK(client->Get(key, buffer));
    ASSERT_EQ(value, std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()));
}

TEST_F(KVCacheClientTest, TestGetInOtherWorker)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    std::string key = "key1";
    std::string value = "value1";
    DS_ASSERT_OK(client1->Set(key, value));
    std::string valueGet;
    DS_ASSERT_OK(client2->Get(key, valueGet));
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    Optional<ReadOnlyBuffer> buffer;
    DS_ASSERT_OK(client2->Get(key, buffer));
    ASSERT_EQ(value, std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()));

    std::vector<std::string> vals;
    DS_ASSERT_OK(client2->Get({ "key1", "key2" }, vals));
    ASSERT_EQ(std::string(vals[0].data(), vals[0].size()), "value1");
    ASSERT_EQ(vals[1], "");

    std::vector<Optional<ReadOnlyBuffer>> buffers;
    DS_ASSERT_OK(client2->Get({ "key1", "key2" }, buffers));
    ASSERT_EQ(std::string(reinterpret_cast<const char *>(buffers[0]->ImmutableData()), buffers[0]->GetSize()),
              "value1");
    ASSERT_FALSE(buffers[1]);
}

TEST_F(KVCacheClientTest, TestGetInOtherWorker2)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    std::string key = "key1";
    std::string value = "value1";
    DS_ASSERT_OK(client1->Set(key, value));

    std::vector<std::string> vals;
    DS_ASSERT_OK(client2->Get({ "key1", "key2" }, vals));
    ASSERT_EQ(vals[1], "");
    ASSERT_EQ(vals[0], "value1");
}

TEST_F(KVCacheClientTest, TestSetInOtherWorker)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    std::string key = "key1";
    std::string value = "value1";
    DS_ASSERT_OK(client1->Set(key, "hi"));
    DS_ASSERT_OK(client2->Set(key, value));
    std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_ASYNC_NOTIFY_WORKER));
    std::string valueGet;
    DS_ASSERT_OK(client1->Get(key, valueGet));
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    DS_ASSERT_OK(client2->Get(key, valueGet));
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
}

TEST_F(KVCacheClientTest, TestCacheInvaliation)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    DS_ASSERT_OK(client1->Set("key1", "val1"));
    std::string valueGet;
    DS_ASSERT_OK(client2->Get("key1", valueGet));
    ASSERT_EQ(std::string(valueGet.data(), valueGet.size()), "val1");

    DS_ASSERT_OK(client1->Set("key1", "val2"));
    std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_ASYNC_NOTIFY_WORKER));
    DS_ASSERT_OK(client2->Get("key1", valueGet));
    ASSERT_EQ(std::string(valueGet.data(), valueGet.size()), "val2");
}

TEST_F(KVCacheClientTest, TestDeleteInOtherWorker)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    // delete from other worker
    {
        std::string key = "key1";
        std::string value = "value1";
        DS_ASSERT_OK(client1->Set(key, value));
        DS_ASSERT_OK(client2->Del(key));
        std::string valueGet;
        ASSERT_EQ(client1->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);
        ASSERT_EQ(client2->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);

        Optional<ReadOnlyBuffer> buffer;
        ASSERT_EQ(client1->Get(key, buffer).GetCode(), StatusCode::K_NOT_FOUND);
        ASSERT_EQ(client2->Get(key, buffer).GetCode(), StatusCode::K_NOT_FOUND);
    }
}

TEST_F(KVCacheClientTest, TestDeleteInOtherWorkerAfterGet)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    {
        std::string key = "key2";
        std::string value = "value2";
        DS_ASSERT_OK(client1->Set(key, value));
        std::string valueGet;
        Optional<ReadOnlyBuffer> buffer;
        DS_ASSERT_OK(client1->Get(key, valueGet));
        ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
        DS_ASSERT_OK(client1->Get(key, buffer));
        ASSERT_EQ(value, std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()));

        DS_ASSERT_OK(client2->Get(key, valueGet));
        ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
        DS_ASSERT_OK(client2->Get(key, buffer));
        ASSERT_EQ(value, std::string(reinterpret_cast<const char *>(buffer->ImmutableData()), buffer->GetSize()));
        DS_ASSERT_OK(client2->Del(key));
        ASSERT_EQ(client1->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);
        ASSERT_EQ(client2->Get(key, valueGet).GetCode(), StatusCode::K_NOT_FOUND);
        ASSERT_EQ(client1->Get(key, buffer).GetCode(), StatusCode::K_NOT_FOUND);
        ASSERT_EQ(client2->Get(key, buffer).GetCode(), StatusCode::K_NOT_FOUND);
    }
}

TEST_F(KVCacheClientTest, ReserveGetAndLockConcurrentTest)
{
    auto task = [&]() {
        std::shared_ptr<KVClient> client;
        InitTestKVClient(0, client);
        client->Set("key", "value");
        std::string value;
        client->Get("key", value);
        client->Del("key");
    };
    int threadNum = 20;
    ThreadPool threadPool(threadNum);
    std::vector<std::future<void>> futures;
    for (int i = 0; i < threadNum; i++) {
        futures.emplace_back(threadPool.Submit(task));
    }
    for (auto &f : futures) {
        f.get();
    }
}

TEST_F(KVCacheClientTest, LEVEL1_TestWorkerRestartAndPutGet)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string key = "League_of_Legends";
    std::string data = randomData_.GetRandomString(513 * 1024ul);
    DS_ASSERT_OK(client1->Set(key, data));

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));

    std::string data1 = randomData_.GetRandomString(513 * 1024ul);
    DS_ASSERT_OK(client2->Set(key, data1));
    std::string getValue;
    DS_ASSERT_OK(client1->Get(key, getValue));
    ASSERT_EQ(data1, getValue);
}

TEST_F(KVCacheClientTest, DeleteTryAgain)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    DS_ASSERT_OK(client1->Set(objectKey1, data));
    DS_ASSERT_OK(client2->Set(objectKey2, data));

    std::vector<std::string> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, dataList));
    ASSERT_TRUE(dataList.size() == 2 && !dataList[0].empty() && !dataList[1].empty());

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.DeleteObjectWithTryLock.before",
                                           "1*return(K_RUNTIME_ERROR)"));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->Del({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_TRUE(!failedObjectKeys.empty());
    std::vector<std::string> failedObjectKeys2;
    DS_ASSERT_OK(client1->Del(failedObjectKeys, failedObjectKeys2));
    ASSERT_TRUE(failedObjectKeys2.empty());

    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2 }, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0].empty() && dataList[1].empty());
    DS_ASSERT_NOT_OK(client1->Get({ objectKey1, objectKey2 }, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0].empty() && dataList[1].empty());
}

TEST_F(KVCacheClientTest, DeleteDeadLock)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey1 = NewObjectKey();
    std::string objectKey2 = NewObjectKey();

    uint64_t size = 1024;
    std::string data = GenRandomString(size);

    DS_ASSERT_OK(client1->Set(objectKey1, data));
    DS_ASSERT_OK(client2->Set(objectKey2, data));

    std::vector<std::string> dataList;
    DS_ASSERT_OK(client2->Get({ objectKey1, objectKey2 }, dataList));
    ASSERT_TRUE(dataList.size() == 2 && !dataList[0].empty() && !dataList[1].empty());

    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "worker.DeleteObjectWithTryLock.before",
                                           "1*return(K_WORKER_DEADLOCK)"));
    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client1->Del({ objectKey1, objectKey2 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    DS_ASSERT_NOT_OK(client2->Get({ objectKey1, objectKey2 }, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0].empty() && dataList[1].empty());
    DS_ASSERT_NOT_OK(client1->Get({ objectKey1, objectKey2 }, dataList));
    ASSERT_TRUE(dataList.size() == 2 && dataList[0].empty() && dataList[1].empty());
}

TEST_F(KVCacheClientTest, GetTimeoutNotAddShmUnit)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::string objectKey1 = NewObjectKey();

    uint64_t size = 20 * 1024 * 1024;
#ifdef USE_URMA
    // in URMA mode, we exactly support 80% of maxSize (including shmCircularQueue)
    size = 19 * 1024 * 1024;
#endif
    std::string data = GenRandomString(size);

    DS_ASSERT_OK(client->Set(objectKey1, data));

    DS_ASSERT_OK(inject::Set("ClientWorkerApi.Get.retryTimeout", "1*call(3000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.Get.asyncGetStart", "1*call(2000)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "worker.Get.beforeReturn", "1*sleep(4000)"));
    std::vector<std::string> dataList;
    ASSERT_EQ(client->Get({ objectKey1 }, dataList).GetCode(), K_RPC_UNAVAILABLE);

    std::vector<std::string> failedObjectKeys;
    DS_ASSERT_OK(client->Del({ objectKey1 }, failedObjectKeys));
    ASSERT_TRUE(failedObjectKeys.empty());

    std::string objectKey2 = NewObjectKey();
    DS_ASSERT_OK(client->Set(objectKey2, data));
}

class KVCacheClientNoRedirectTest : public KVCacheClientTest {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 2; // worker num is 2
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams =
            "-shared_memory_size_mb=25 -v=1 -log_monitor=true -max_client_num=2000 --enable_redirect=false";
    }
};

TEST_F(KVCacheClientNoRedirectTest, ConcurrentDeleteAndRemoteGet)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey = NewObjectKey();

    uint64_t size = 128;
    std::string data = GenRandomString(size);
    for (int i = 0; i < 100; i++) { // obj num is 100
        DS_ASSERT_OK(client1->Set(objectKey, data));
        std::string val;
        DS_ASSERT_OK(client2->Get(objectKey, val));
    }
}

TEST_F(KVCacheClientTest, DISABLED_ConcurrentDeleteAndRemoteGet)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey = NewObjectKey();

    uint64_t size = 128;
    std::string data = GenRandomString(size);

    ThreadPool pool(3);

    pool.Execute([&client1, &objectKey, data] {
        for (int i = 0; i < 1000; i++) {
            DS_ASSERT_OK(client1->Set(objectKey, data));
        }
    });

    pool.Execute([&client1, &objectKey, data] {
        for (int i = 0; i < 1000; i++) {
            DS_ASSERT_OK(client1->Del(objectKey));
        }
    });

    pool.Execute([&client2, &objectKey, data] {
        for (int i = 0; i < 1000; i++) {
            std::string val;
            (void)client2->Get(objectKey, val);
        }
    });
}

TEST_F(KVCacheClientTest, TestGenerateKey)
{
    LOG(INFO) << "Start to TestGenerateKey from SC client";
    std::shared_ptr<KVClient> client = std::make_shared<KVClient>();
    std::string key;
    (void)client->GenerateKey("", key);
    ASSERT_EQ(key, "");

    InitTestKVClient(0, client);
    (void)client->GenerateKey("", key);
    ASSERT_TRUE(key.size() != 0);
    ASSERT_TRUE(key.find(";") != std::string::npos);
}

TEST_F(KVCacheClientTest, LEVEL1_TestRemoteGetFromSelfAddressScenarios)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey = NewObjectKey();
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "MasterWorkerOCServiceImpl.DeleteNotification.retry", "sleep(3000)"));

    std::thread t1([&client1, &objectKey, data, &param]() {
        sleep(1);
        DS_ASSERT_OK(client1->Set(objectKey, data, param));
        std::vector<std::string> dataList;
        sleep(1);
        Timer timer;
        uint64_t subTimeoutMs = 8000;
        EXPECT_EQ(client1->Get({ objectKey }, dataList, subTimeoutMs).GetCode(), StatusCode::K_NOT_FOUND);
        uint64_t elapsedMilliSeconds = timer.ElapsedMilliSecond();
        EXPECT_LE(elapsedMilliSeconds, 10000ul);
        EXPECT_GE(elapsedMilliSeconds, 7000ul);
    });

    std::thread t2([&client2, &objectKey, data]() {
        std::vector<std::string> dataList;
        uint64_t subTimeoutMs = 5'000;
        DS_ASSERT_OK(client2->Get({ objectKey }, dataList, subTimeoutMs));
        DS_ASSERT_OK(client2->Del(objectKey));
    });

    t1.join();
    t2.join();
}

TEST_F(KVCacheClientTest, TestRemoteGetFromSelfAddressScenarios1)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey = NewObjectKey();
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE, .ttlSecond = 0, .existence = ExistenceOpt::NX };

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "MasterWorkerOCServiceImpl.DeleteNotification.retry", "sleep(3000)"));

    std::thread t1([&client1, &objectKey, data, &param]() {
        sleep(1);
        DS_ASSERT_OK(client1->Set(objectKey, data, param));
        std::vector<std::string> dataList;
        sleep(1);
        Timer timer;
        uint64_t subTimeoutMs = 10'000;
        DS_ASSERT_OK(client1->Get({ objectKey }, dataList, subTimeoutMs));
        ASSERT_EQ(data, dataList[0]);
    });

    std::thread t2([&client2, &objectKey, data, &param]() {
        std::vector<std::string> dataList;
        uint64_t subTimeoutMs = 5'000;
        DS_ASSERT_OK(client2->Get({ objectKey }, dataList, subTimeoutMs));
        DS_ASSERT_OK(client2->Del(objectKey));
        DS_ASSERT_OK(client2->Set(objectKey, data, param));
    });

    t1.join();
    t2.join();
}

TEST_F(KVCacheClientTest, DISABLED_FixResidualLocationProblem)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0);
    InitTestKVClient(1, client1);

    std::string key = "key1";
    std::string value = "value1";
    DS_ASSERT_OK(client1->Set(key, value));

    std::string valueGet;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.after_query_meta", "1*sleep(10000)"));
    auto t1 = std::thread([&]() { DS_ASSERT_NOT_OK(client0->Get(key, valueGet)); });

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    t1.join();

    Status ret = client0->Get(key, valueGet);
    std::string str = ret.ToString();
    ASSERT_TRUE(str.find("The pointer [impl_->shmUnit] is null") == std::string::npos);
    ASSERT_TRUE(str.find("Fail to get object") != std::string::npos);
}

TEST_F(KVCacheClientTest, TestQueryMetaRetry)
{
    constexpr int timeoutMs = 10000;  // 10s
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, timeoutMs);
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "MasterOCServiceImpl.QueryMeta.busy", "1*sleep(10000)"));

    std::string key = "key1";
    std::string value = "value1";
    ASSERT_EQ(client->Set(key, value), Status::OK());

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1, timeoutMs);
    std::string valueGet;
    auto rc = client1->Get(key, valueGet);
    // ZMQ should be successful, because of the dispatch mode, the test point for uRPC is retry times.
    if (rc.IsError()) {
        std::string errMsg = rc.ToString();
        std::string checkStr = "RPC unavailable * 2";
        ASSERT_TRUE(errMsg.find(checkStr) != std::string::npos);
    }
}

TEST_F(KVCacheClientTest, SetRetryWhenOOM)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    std::string key = "key";
    int shmSize = 22 * 1024 * 1024;
#ifdef USE_URMA
    // in URMA mode, we exactly support 80% of maxSize(including shmCircularQueue)
    shmSize = 19 * 1024 * 1024;
#endif
    std::string data = GenRandomString(shmSize);
    DS_ASSERT_OK(client1->Set(key, data));

    std::thread t1([&client1, &key]() {
        const int waitTime = 10;
        sleep(waitTime);
        client1->Del(key);
    });

    const int num = 10;
    const int noShmSize = 499 * 1024;
    std::string data2 = GenRandomString(noShmSize);
    for (int i = 0; i < num; i++) {
        std::string key = "key" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key, data2));
    }

    t1.join();
}

TEST_F(KVCacheClientTest, TestDelOneKeyMeetsLockErrorThenSetFail)
{
    std::string key = "key";
    std::string val = "value";

    DS_ASSERT_OK(client_->Set(key, val));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "safe_table.get_and_lock", "6*return(K_NOT_FOUND)"));
    DS_ASSERT_OK(client_->Del(key));

    DS_ASSERT_OK(client_->Set(key, val));
}

TEST_F(KVCacheClientTest, TestGetKeyAlwaysMeetsCacheInvalid)
{
    std::string key = "key";
    std::string val = "value";

    DS_ASSERT_OK(client_->Set(key, val));

    int32_t timeoutMs = 5'000;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1, timeoutMs);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.worker_worker_remote_get_failure",
                                           "return(K_WORKER_PULL_OBJECT_NOT_FOUND)"));

    std::string getVal;
    ASSERT_EQ(client1->Get(key, getVal, timeoutMs).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(KVCacheClientTest, TestSetGetDelTheSameKeyConcurrently)
{
    std::string key = "key";
    std::string val = "value";

    std::shared_ptr<KVClient> client0;
    int32_t sleepMs = 3000;
    InitTestKVClient(0, client0, sleepMs);
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);

    DS_ASSERT_OK(client1->Set(key, val));

    // 1. w0 remote get failed and residual location in master.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.remote_get_failed", "return(K_RPC_UNAVAILABLE)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.remove_location", "return(K_RPC_UNAVAILABLE)"));
    std::string getVal;
    DS_ASSERT_NOT_OK(client0->Get(key, getVal));

    // 2. Get the key and let it wait 1200ms after add in remote get object list and before lock.
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.after_add_remote_get_objects", "1*sleep(2000)"));
    std::thread t1([&key, &val, &client0]() {
        std::string getVal;
        DS_ASSERT_OK(client0->Get(key, getVal));
        ASSERT_EQ(val, getVal);
    });

    // 3. Del the key from w1, and it would notify w0, and found that w0 is in remote get progress,
    //    just set the need delete flag and go back.
    std::thread t2([&key, client1]() {
        const uint64_t usecs = 1'000'000;
        usleep(usecs);
        DS_ASSERT_OK(client1->Del(key));
    });

    // 4. Set again in w0, it would cause segfault now!
    std::thread t3([&key, &val, &client0]() {
        const uint64_t usecs = 1'500'000;
        usleep(usecs);
        DS_ASSERT_OK(client0->Set(key, val));
    });

    t1.join();
    t2.join();
    t3.join();
}

TEST_F(KVCacheClientTest, TestDelKeyMeetsRPCDeadLineExceeded)
{
    std::string key = "key";
    std::string val = "value";

    DS_ASSERT_OK(client_->Set(key, val));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.DeleteAllCopyMeta", "1*return(K_RPC_DEADLINE_EXCEEDED)"));

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);
    DS_ASSERT_OK(client1->Del(key));
}

TEST_F(KVCacheClientTest, TestPRAMSetAndDelTheSameKeyConcurrently)
{
    std::string key = "key";
    std::string val = "value";

    DS_ASSERT_OK(client_->Set(key, val));

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "master.send_cache_invalid.before_remove_location", "1*sleep(2000)"));

    // PRAM set would async update the cache invalid to w0.
    DS_ASSERT_OK(client1->Set(key, val));
    // Delete the metadata.
    DS_ASSERT_OK(client1->Del(key));

    // w0 Set again.
    DS_ASSERT_OK(client_->Set(key, val));

    int sleepSecs = 5;
    sleep(sleepSecs);

    // w1 delete the key, but now the location has been deleted by async notify thread.
    DS_ASSERT_OK(client1->Del(key));

    DS_ASSERT_OK(client_->Set(key, val));
}

TEST_F(KVCacheClientTest, TestPRAMSetAndDelTheSameKeyConcurrently2)
{
    std::string key = "key";
    std::string val = "value";

    DS_ASSERT_OK(client_->Set(key, val));

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "master.send_cache_invalid.before_remove_location", "1*sleep(2000)"));

    // PRAM set would async update the cache invalid to w0.
    DS_ASSERT_OK(client1->Set(key, val));

    int sleepSecs = 1;
    sleep(sleepSecs);

    // w0 Set again, and the cache invalid notification response would arrive after set return.
    DS_ASSERT_OK(client_->Set(key, val));

    int sleepSecs1 = 3;
    sleep(sleepSecs1);

    // w1 set and change primary copy.
    DS_ASSERT_OK(client1->Set(key, val));

    // Delete the metadata.
    DS_ASSERT_OK(client1->Del(key));

    DS_ASSERT_OK(client_->Set(key, val));
}

TEST_F(KVCacheClientTest, TestWorkerNotSupportShmQueue)
{
    auto func = [this](int index) {
        LOG(INFO) << "TestWorkerNotSupportShmQueue:" << index;
        std::shared_ptr<KVClient> client;
        InitTestKVClient(index, client);

        DS_ASSERT_OK(client->Init());
        std::string data(1024 * 1024, 'a');
        auto key = client->Set(data);
        ASSERT_TRUE(!key.empty());
        std::string value;
        DS_ASSERT_OK(client->Get(key, value));
        ASSERT_EQ(data, value);
        DS_ASSERT_OK(client->Del(key));
    };
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.RegisterClient.end", "call(0)"));
    func(0);
    func(1);
}

TEST_F(KVCacheClientTest, TestPRAMSetAndDelTheSameKeyConcurrently3)
{
    std::string key = "key";
    std::string val = "value";

    DS_ASSERT_OK(client_->Set(key, val));

    std::shared_ptr<KVClient> client1;
    InitTestKVClient(1, client1);

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.send_cache_invalid.before_notify", "1*sleep(2000)"));
    // PRAM set would async update the cache invalid to w0.
    DS_ASSERT_OK(client1->Set(key, val));

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.UpdateMeta", "1*sleep(1500)"));

    // Now w0 Set again and the UpdateNotification arrived at w0 at the same time.
    DS_ASSERT_OK(client_->Set(key, val));

    int sleepSecs = 5;
    sleep(sleepSecs);

    // Delete the key but it loss the w0 location.
    DS_ASSERT_OK(client1->Del(key));
    // Error happen.
    DS_ASSERT_OK(client_->Set(key, val));
}

TEST_F(KVCacheClientTest, TestCreateMetaFailed)
{
    std::shared_ptr<KVClient> client;
    int32_t timeout = 1000;
    InitTestKVClient(0, client, timeout);

    DS_ASSERT_OK(
        cluster_->SetInjectAction(WORKER, 0, "worker.before_CreateMetadataToMaster", "return(K_RPC_UNAVAILABLE)"));
    Status status = client->Set("key", "value");
    LOG(INFO) << "status code: " << status.GetCode();
    ASSERT_TRUE(status.GetCode() == K_RPC_UNAVAILABLE);
}

TEST_F(KVCacheClientTest, TestCreateAndSetBufferSuccess)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(1, client);
    std::string key = "key";
    std::shared_ptr<Buffer> buffer;
    uint32_t ttl = 3;

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.ttlSecond = ttl;
    DS_ASSERT_OK(client->Create(key, NON_SHM_SIZE, param, buffer));
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(NON_SHM_SIZE, buffer->GetSize());
    DS_ASSERT_OK(client->Set(buffer));
    Optional<Buffer> getBuffer;
    ASSERT_EQ(client->Get(key, getBuffer), Status::OK());
    ASSERT_EQ(NON_SHM_SIZE, getBuffer->GetSize());

    auto waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    DS_ASSERT_NOT_OK(client->Get(key, getBuffer));
}

TEST_F(KVCacheClientTest, TestMCreateAndMSetBufferSuccess)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(1, client);
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::vector<std::string> keys{ key1, key2 };
    std::vector<uint64_t> sizes{ NON_SHM_SIZE, SHM_SIZE };
    std::vector<std::shared_ptr<Buffer>> buffers;
    uint32_t ttl = 3;

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    param.ttlSecond = ttl;
    DS_ASSERT_OK(client->MCreate(keys, sizes, param, buffers));
    ASSERT_EQ(buffers.size(), keys.size());
    ASSERT_EQ(NON_SHM_SIZE, buffers[0]->GetSize());
    ASSERT_EQ(SHM_SIZE, buffers[1]->GetSize());
    DS_ASSERT_OK(client->MSet(buffers));
    std::vector<Optional<Buffer>> getBuffers;
    ASSERT_EQ(client->Get(keys, getBuffers), Status::OK());
    ASSERT_EQ(getBuffers.size(), keys.size());

    ASSERT_EQ(NON_SHM_SIZE, getBuffers[0]->GetSize());
    ASSERT_EQ(SHM_SIZE, getBuffers[1]->GetSize());

    auto waitTime = 5;
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));
    DS_ASSERT_NOT_OK(client->Get(keys, getBuffers));
}

TEST_F(KVCacheClientTest, TestMSetEmptyInputFailed)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(1, client);
    std::vector<std::shared_ptr<Buffer>> emptyBuffers;
    Status rc = client->MSet(emptyBuffers);
    ASSERT_EQ(rc.GetCode(), K_INVALID);
}

TEST_F(KVCacheClientTest, TestMCreateKeysSizesMismatchError)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(1, client);
    std::vector<std::string> keys{"key1", "key2", "key3"};
    std::vector<uint64_t> sizes{NON_SHM_SIZE, SHM_SIZE};
    std::vector<std::shared_ptr<Buffer>> buffers;

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    Status rc = client->MCreate(keys, sizes, param, buffers);
    ASSERT_EQ(rc.GetCode(), K_INVALID);
}

TEST_F(KVCacheClientTest, TestShmAndNonShmBufferMixedSetSuccess)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(1, client);
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::vector<std::string> keys{ key1, key2 };
    std::shared_ptr<Buffer> nonShmBufffer;
    std::shared_ptr<Buffer> shmBufffer;

    std::vector<uint64_t> sizes{ NON_SHM_SIZE, SHM_SIZE };

    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    DS_ASSERT_OK(client->Create(key1, NON_SHM_SIZE, param, nonShmBufffer));
    ASSERT_NE(nonShmBufffer, nullptr);
    ASSERT_EQ(NON_SHM_SIZE, nonShmBufffer->GetSize());

    DS_ASSERT_OK(client->Create(key2, SHM_SIZE, param, shmBufffer));
    ASSERT_NE(shmBufffer, nullptr);
    ASSERT_EQ(SHM_SIZE, shmBufffer->GetSize());

    std::vector<std::shared_ptr<Buffer>> buffers{nonShmBufffer, shmBufffer};
    DS_ASSERT_OK(client->MSet(buffers));
    std::vector<Optional<Buffer>> getBuffers;
    ASSERT_EQ(client->Get(keys, getBuffers), Status::OK());
    ASSERT_EQ(getBuffers.size(), keys.size());

    ASSERT_EQ(NON_SHM_SIZE, getBuffers[0]->GetSize());
    ASSERT_EQ(SHM_SIZE, getBuffers[1]->GetSize());
}

TEST_F(KVCacheClientTest, ClientConnect)
{
    FLAGS_v = 1;
    std::vector<std::thread> threads;
    int threadCount = 2;
    std::atomic_bool flag = { false };
    DS_ASSERT_OK(inject::Set("client.RecvPageFd", "call(3)"));
    DS_ASSERT_OK(inject::Set("client.CloseSocketFd", "call(5)"));
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([&flag, this] {
            ConnectOptions connectOptions;
            int timeout = 5000;
            InitConnectOpt(0, connectOptions, timeout);
            int testCount = 300;
            for (int i = 0; i < testCount; i++) {
                if (flag) {
                    break;
                }
                std::shared_ptr<KVClient> client;
                client = std::make_shared<KVClient>(connectOptions);
                Status rc = client->Init();
                if (rc.IsError()) {
                    flag = true;
                    LOG(ERROR) << "failed:" << rc;
                }
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    ASSERT_FALSE(flag);
}

TEST_F(KVCacheClientTest, LEVEL1_TestUnableToGetOldFd)
{
    std::shared_ptr<KVClient> client0, client1;
    int timeoutMs = 5000;
    InitTestKVClient(0, client0, timeoutMs);
    InitTestKVClient(1, client1, timeoutMs);

    const size_t valSize = 500 * 1024 + 1;
    std::string value = GenRandomString(valSize);

    // return before receiving fd.
    auto key = client1->Set(value);
    ASSERT_NE(key, "");
    std::string valToGet;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_GetClientFd", "1*sleep(3000)"));
    DS_ASSERT_OK(datasystem::inject::Set("ClientWorkerCommonApi.GetClientFd.preReceive", "1*return(K_RUNTIME_ERROR)"));
    DS_ASSERT_NOT_OK(client0->Get(key, valToGet));

    // will not get old fd.
    key = client1->Set(value);
    ASSERT_NE(key, "");
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_GetClientFd", "1*return(K_OK)"));
    DS_ASSERT_NOT_OK(client0->Get(key, valToGet));

    // Fd cache is ok.
    int loopTime = 100;
    for (int i = 0; i < loopTime; i++) {
        auto key = client1->Set(value);
        ASSERT_NE(key, "");
        std::string valToGet;
        DS_ASSERT_OK(client0->Get(key, valToGet));
        DS_ASSERT_OK(client0->Del(key));
    }
}

class KVCacheClientDisconnectTest : public KVCacheClientTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -max_client_num=2000";
    }
};

TEST_F(KVCacheClientDisconnectTest, TestConcurrentDisconnect)
{
    const int threadCount = 10;
    const int clientCountPerThread = 30;
    std::vector<std::thread> threads;
    for (int i = 0; i < threadCount; i++) {
        threads.emplace_back([this] {
            std::vector<std::shared_ptr<KVClient>> clients;
            while (clients.size() < clientCountPerThread) {
                std::shared_ptr<KVClient> client;
                InitTestKVClient(0, client);
                clients.emplace_back(std::move(client));
                InitTestKVClient(0, client);
                clients.emplace_back(std::move(client));
                auto it = clients.begin();
                clients.erase(it);
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
}

class EXCLUSIVE_KVCacheBigClusterTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = num_;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
        InitClients();
    }

    void TearDown() override
    {
        clients_.clear();
        ExternalClusterTest::TearDown();
    }

    void InitClients()
    {
        clients_.resize(num_);
        for (uint64_t i = 0; i < clients_.size(); ++i) {
            InitTestKVClient(i, clients_[i]);
        }
    }

    void KvTest(uint64_t index)
    {
        std::string key = "HelloWorldThisIsDataSystem";
        std::string val = "test0mutual0exclusion0set0data0";
        SetParam param;
        param.existence = ExistenceOpt::NX;
        size_t loop = 20;
        for (size_t i = 0; i < loop; ++i) {
            Status status = clients_[index]->Set(key, val, param);
            ASSERT_TRUE(status.IsOk() || status.GetCode() == StatusCode::K_OC_KEY_ALREADY_EXIST) << status.ToString();
            if (status.GetCode() == StatusCode::K_OC_KEY_ALREADY_EXIST) {
                std::string getVal;
                Timer timer;
                Status status = clients_[index]->Get(key, getVal, 20'000);
                uint64_t elasped = timer.ElapsedMilliSecond();
                ASSERT_TRUE(status.IsOk() || status.GetCode() == StatusCode::K_NOT_FOUND) << status.ToString();
                if (status.GetCode() == StatusCode::K_NOT_FOUND) {
                    ASSERT_GE(elasped, 18'000ul);
                } else {
                    ASSERT_EQ(getVal.size(), val.size());
                }

                DS_ASSERT_OK(clients_[index]->Del(key));
            }
        }
    }

    void SetGet()
    {
        std::string key = "HelloWorldThisIsDataSystem";
        std::string val = "test0mutual0exclusion0set0data0";
        size_t loop = 50;
        for (size_t i = 0; i < loop; ++i) {
            DS_ASSERT_OK(clients_[0]->Set(key, val));
            DS_ASSERT_OK(clients_[1]->Set(key, val));
            std::string getVal1;
            std::string getVal2;
            DS_ASSERT_OK(clients_[0]->Get(key, getVal1));
            DS_ASSERT_OK(clients_[1]->Get(key, getVal2));
            ASSERT_EQ(getVal1, val);
            ASSERT_EQ(getVal2, val);
        }
    }

    std::vector<std::shared_ptr<KVClient>> clients_;

    uint64_t num_ = 2;
};

TEST_F(EXCLUSIVE_KVCacheBigClusterTest, DISABLED_TestKVSetGetDelConcurrency)
{
    LOG(INFO) << "YuanRong disgusting testcase";
    uint64_t multi = 10;
    std::vector<std::thread> threads(num_ * multi);
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([this, i]() { KvTest(i % num_); });
    }
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(EXCLUSIVE_KVCacheBigClusterTest, LEVEL2_TestKVSetGetConcurrency)
{
    LOG(INFO) << "YuanRong disgusting testcase";
    uint64_t multi = 5;
    std::vector<std::thread> threads(num_ * multi);
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([this]() { SetGet(); });
    }
    for (auto &t : threads) {
        t.join();
    }
}

TEST_F(EXCLUSIVE_KVCacheBigClusterTest, LEVEL2_TestKVDelManyKeysConcurrency)
{
    std::string keyPrefix = "Attack_on_Titan";
    std::string val = "Love_And_Peace";
    std::vector<std::string> keys;
    uint64_t loop = 10;
    for (size_t i = 0; i < loop; ++i) {
        std::string key = keyPrefix + std::to_string(i);
        DS_ASSERT_OK(clients_[0]->Set(key, val));
        keys.emplace_back(key);
    }

    for (size_t i = 1; i < clients_.size(); ++i) {
        std::string getVal;
        DS_ASSERT_OK(clients_[i]->Get(keyPrefix + std::to_string(0), getVal));
    }

    uint64_t multi = 5;
    std::vector<std::thread> threads(num_ * multi);
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([this, i, &keys]() {
            std::vector<std::string> failedKeys;
            DS_ASSERT_OK(clients_[i % num_]->Del(keys, failedKeys));
        });
    }
    for (auto &t : threads) {
        t.join();
    }

    for (size_t i = 0; i < num_; ++i) {
        std::vector<std::string> getVals;
        ASSERT_EQ(clients_[i]->Get(keys, getVals).GetCode(), StatusCode::K_NOT_FOUND);
    }
}

class KVCacheClientTestWithAsyncDelete : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 2;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = "-shared_memory_size_mb=25 -async_delete=true -v=1";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }
};

TEST_F(KVCacheClientTestWithAsyncDelete, TestDel)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);

    std::string key = "key";
    std::string value = "value";
    auto times = 10;
    for (auto i = 0; i < times; i++) {
        DS_ASSERT_OK(client1->Set(key, value));
        std::string valueGet;
        DS_ASSERT_OK(client1->Get(key, valueGet));
        EXPECT_EQ(value, valueGet);
        DS_ASSERT_OK(client2->Get(key, valueGet));
        EXPECT_EQ(value, valueGet);

        ASSERT_EQ(client1->Del(key), Status::OK());
        std::this_thread::sleep_for(std::chrono::milliseconds(WAIT_ASYNC_NOTIFY_WORKER));
        DS_ASSERT_NOT_OK(client1->Get(key, valueGet));
        DS_ASSERT_NOT_OK(client2->Get(key, valueGet));
    }
}

TEST_F(KVCacheClientTestWithAsyncDelete, LEVEL1_AsyncDelWithDuplicatedObjectKey)
{
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);

    std::string key = "key";
    std::string value = "value";
    std::string newValue = "newValue";

    DS_ASSERT_OK(client1->Set(key, value));
    std::string valueGet;
    DS_ASSERT_OK(client1->Get(key, valueGet));
    EXPECT_EQ(value, valueGet);

    // ExpiredObjectManger getExpiredOjbect but don't notify to delete.
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "master.ExpiredObjectManager.AsyncDelete", "1*call(10)"));
    DS_ASSERT_OK(
        cluster_->SetInjectAction(ClusterNodeType::WORKER, 1, "master.ExpiredObjectManager.AsyncDelete", "1*call(10)"));
    ASSERT_EQ(client1->Del(key), Status::OK());
    std::this_thread::sleep_for(std::chrono::seconds(3));
    // Set will be success after async delete finish.
    DS_EXPECT_OK(client1->Set(key, newValue));
    std::this_thread::sleep_for(std::chrono::seconds(10));
    // After new set, objectKey will be removed from expiredObjectManager.
    DS_EXPECT_OK(client1->Get(key, valueGet));
    EXPECT_EQ(newValue, valueGet);
    std::this_thread::sleep_for(std::chrono::seconds(5));
    DS_EXPECT_OK(client1->Get(key, valueGet));
    EXPECT_EQ(newValue, valueGet);
}

class KVCacheClientWriteModeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        InitClients();
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitClients()
    {
        InitTestKVClient(0, client_);
    }

    std::shared_ptr<KVClient> client_;
};

TEST_F(KVCacheClientWriteModeTest, TestSetWriteModeButNoneL2CacheType)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::string key1 = "key1";
    std::string value1 = "value1";
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    DS_ASSERT_NOT_OK(client->Set(key1, value1, param1));

    std::string key2 = "key2";
    std::string value2 = "value2";
    SetParam param2{ .writeMode = WriteMode::WRITE_BACK_L2_CACHE };
    DS_ASSERT_NOT_OK(client->Set(key2, value2, param2));
}

class KVClientShutdownTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=100 -v=2 -log_monitor=true";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
    }

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }
};

TEST_F(KVClientShutdownTest, TestAsynInitAndShutdown)
{
    ConnectOptions connectOptions;
    InitConnectOpt(0, connectOptions);
    auto client = std::make_shared<KVClient>(connectOptions);
    DS_ASSERT_NOT_OK(client->ShutDown());
    DS_ASSERT_OK(client->Init());

    int threadNum = 2, loopsNum = 1000;
    ThreadPool threadPool(threadNum);
    threadPool.Execute([client, loopsNum]() {
        for (int i = 0; i < loopsNum; i++) {
            DS_ASSERT_OK(client->Init());
        }
    });

    threadPool.Execute([client, loopsNum]() {
        for (int i = 0; i < loopsNum; i++) {
            DS_ASSERT_OK(client->ShutDown());
        }
    });
}

TEST_F(KVClientShutdownTest, TestMulInvokeShutdownWithNullPtr)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));

    ASSERT_EQ(client->ShutDown().GetCode(), K_RPC_UNAVAILABLE);
    ASSERT_EQ(client->ShutDown().GetCode(), K_RPC_UNAVAILABLE);
    client.reset();
}

TEST_F(KVClientShutdownTest, TestAsynMSetTxAndShutdown)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);

    std::vector<std::string> keys;
    std::vector<StringView> values;
    size_t maxElementSize = 2;
    int bigValSize = 600 * 1024;
    std::vector<std::string> vals;

    std::string valueGet;
    MSetParam param;
    param.existence = ExistenceOpt::NX;
    param.writeMode = WriteMode::NONE_L2_CACHE;
    for (size_t i = 0; i < maxElementSize; ++i) {
        vals.emplace_back(randomData_.GetRandomString(bigValSize));
    }

    for (size_t i = 0; i < maxElementSize; ++i) {
        auto key = randomData_.GetRandomString(20);
        keys.emplace_back(key);
        values.emplace_back(vals[i]);
    }
    int threadNum = 50;
    auto ThreadSetGet = [&] {
        (void)client->MSetTx(keys, values, param);
        DS_ASSERT_OK(client->ShutDown());
    };

    std::vector<std::thread> clientThreads(threadNum);
    for (int i = 0; i < threadNum; ++i) {
        clientThreads[i] = std::thread(ThreadSetGet);
    }
    for (auto &t : clientThreads) {
        t.join();
    }

    InitTestKVClient(0, client);
    std::vector<std::string> failedIds;
    DS_ASSERT_OK(client->Del(keys, failedIds));
    DS_ASSERT_OK(client->MSetTx(keys, values, param));
    DS_ASSERT_OK(client->ShutDown());
}

class KVClientDfxTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "true";
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=100 -v=2 -log_monitor=true -node_timeout_s=2";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
    }
};

TEST_F(KVClientDfxTest, LEVEL2_TestWorkerRestartAddresEmpty)
{
    std::shared_ptr<KVClient> client1;
    std::shared_ptr<KVClient> client2;
    InitTestKVClient(0, client1);
    InitTestKVClient(1, client2);
    std::string objectKey = NewObjectKey();
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    // test
    SetParam param;
    param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client2->Set(key1, "qqqqqqqqq", param));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 1));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 1, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 1));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    std::string val;
    Timer timer;
    DS_ASSERT_NOT_OK(client1->Get(key1, val));
    ASSERT_TRUE(timer.ElapsedMilliSecond() < 3000);  // not wait for 10s, time is less than 3000ms
}

class KVClientQuerySizeTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const uint32_t numWorkers = 2;
        opts.numEtcd = 1;
        opts.numWorkers = numWorkers;
        opts.workerGflagParams = " -shared_memory_size_mb=100 -authorization_enable=true ";
        opts.systemAccessKey = accessKey_;
        opts.systemSecretKey = secretKey_;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
        InitClients();
    }

    void TearDown() override
    {
        client_.reset();
        client1_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitClients()
    {
        InitTestKVClient(0, client_,
                         [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId_); });
        InitTestKVClient(0, client1_,
                         [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1_); });
        InitTestKVClient(1, client2_,
                         [&](ConnectOptions &opts) { opts.SetAkSkAuth(accessKey_, secretKey_, tenantId2_); });
    }

    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
    std::string tenantId_ = "client0";
    std::string tenantId1_ = "client1";
    std::string tenantId2_ = "client0";

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(KVClientQuerySizeTest, TestQueryExistKeys)
{
    std::string key = "key0";
    std::string value = "value0";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    std::vector<uint64_t> outSizes;
    ASSERT_EQ(client_->QuerySize({ key }, outSizes), Status::OK());
    int keyCount = 1;
    ASSERT_EQ(outSizes.size(), keyCount);
    ASSERT_EQ(outSizes[0], value.length());
}

TEST_F(KVClientQuerySizeTest, TestQueryPartNotExistKeys)
{
    std::string key = "key0";
    std::string value = "value0";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    std::vector<uint64_t> outSizes;
    std::string key1 = "key1";
    ASSERT_EQ(client_->QuerySize({ key, key1 }, outSizes), Status::OK());
    ASSERT_EQ(outSizes[0], value.length());
    ASSERT_EQ(outSizes[1], 0);
}

TEST_F(KVClientQuerySizeTest, TestQueryNotExistKeys)
{
    std::string key = "key0";
    std::vector<uint64_t> outSizes;
    std::string key1 = "key1";
    ASSERT_EQ(client_->QuerySize({ key, key1 }, outSizes).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(outSizes[0], 0);
    ASSERT_EQ(outSizes[1], 0);
}

TEST_F(KVClientQuerySizeTest, TestTenantQueryKeys)
{
    std::string key = "key0";
    std::string value = "value0";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    std::vector<uint64_t> outSizes;
    std::string key1 = "key1";
    ASSERT_EQ(client1_->QuerySize({ key, key1 }, outSizes).GetCode(), StatusCode::K_NOT_FOUND);
    ASSERT_EQ(outSizes[0], 0);
    ASSERT_EQ(outSizes[1], 0);
}

TEST_F(KVClientQuerySizeTest, TestDifferentWorker)
{
    std::string key = "key0";
    std::string value = "value0";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    std::vector<uint64_t> outSizes;
    std::string key1 = "key1";
    ASSERT_EQ(client2_->QuerySize({ key, key1 }, outSizes), Status::OK());
    ASSERT_EQ(outSizes[0], value.size());
    ASSERT_EQ(outSizes[1], 0);
}

TEST_F(KVClientQuerySizeTest, TestRetry)
{
    std::string key = "key0";
    std::string value = "value0";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    std::vector<uint64_t> outSizes;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.before_query_meta", "return(K_RPC_UNAVAILABLE)"));
    ASSERT_EQ(client_->QuerySize({ key }, outSizes), Status::OK());
    int keyCount = 1;
    ASSERT_EQ(outSizes.size(), keyCount);
    ASSERT_EQ(outSizes[0], value.length());
}

TEST_F(KVClientQuerySizeTest, TestRPCError)
{
    std::string key = "key0";
    std::string value = "value0";
    std::shared_ptr<KVClient> client;
    ConnectOptions connectOptions;
    int timeout = 1000;
    InitConnectOpt(0, connectOptions, timeout);
    client = std::make_shared<KVClient>(connectOptions);
    ASSERT_EQ(client->Init(), Status::OK());
    ASSERT_EQ(client->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    std::vector<uint64_t> outSizes;
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.before_query_meta", "1*sleep(2000)"));
    ASSERT_EQ(client->QuerySize({ key }, outSizes).GetCode(), StatusCode::K_RPC_UNAVAILABLE);
    int keyCount = 1;
    ASSERT_EQ(outSizes.capacity(), keyCount);
}

class KVClientWriteRocksdbTest : public OCClientCommon, public CommonDistributedExt {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        const int workerCount = 2;
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerCount;
        opts.enableDistributedMaster = "true";
        opts.workerGflagParams = " -v=2 -log_monitor=true ";
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

    void TearDown() override
    {
        ExternalClusterTest::TearDown();
    }

    BaseCluster *GetCluster() override
    {
        return cluster_.get();
    }

    void VoluntaryScaleDownInject(int workerIdx)
    {
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

protected:
    ExternalCluster *externalCluster_ = nullptr;
};

TEST_F(KVClientWriteRocksdbTest, TestNoneModeNoneL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=none");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client1);
    std::string val;
    ASSERT_EQ(client1->Get(key1, val).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(KVClientWriteRocksdbTest, TestNoneModeL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=none");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    SetParam param1;
    param1.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    DS_ASSERT_OK(client1->Set(key1, data, param1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.before_sub_async_send_etcd_req", "1*return(K_OK)"));
    std::string key2;
    (void)client1->GenerateKey("", key2);
    SetParam param2;
    param2.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    DS_ASSERT_OK(client1->Set(key2, data, param2));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client1);
    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
    ASSERT_EQ(client1->Get(key2, val).GetCode(), StatusCode::K_NOT_FOUND);
}

TEST_F(KVClientWriteRocksdbTest, TestNoneModeVoluntaryScaleDown)
{
    StartWorkerAndWaitReady({ 0, 1 },
                            "-node_timeout_s=5 -node_dead_timeout_s=8 -enable_lossless_data_exit_mode=true "
                            "-rocksdb_write_mode=none");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));

    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
}

TEST_F(KVClientWriteRocksdbTest, TestNoneModeScaleUp)
{
    StartWorkerAndWaitReady({ 0 }, "-rocksdb_write_mode=none");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));
    StartWorkerAndWaitReady({ 1 });
    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
}

TEST_F(KVClientWriteRocksdbTest, TestSyncModeNoneL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=sync");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client1);
    std::string val;
    ASSERT_EQ(client1->Get(key1, val).GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(KVClientWriteRocksdbTest, TestSyncModeL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=sync");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    SetParam param1;
    param1.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    DS_ASSERT_OK(client1->Set(key1, data, param1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.before_sub_async_send_etcd_req", "1*return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.save", "return(K_OK)"));
    std::string key2;
    (void)client1->GenerateKey("", key2);
    SetParam param2;
    param2.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    DS_ASSERT_OK(client1->Set(key2, data, param2));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client1);
    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
    ASSERT_EQ(client1->Get(key2, val).GetCode(), StatusCode::K_RUNTIME_ERROR);
    ASSERT_EQ(val, data);
}

TEST_F(KVClientWriteRocksdbTest, TestSyncModeVoluntaryScaleDown)
{
    StartWorkerAndWaitReady({ 0, 1 },
                            "-node_timeout_s=5 -node_dead_timeout_s=8 -enable_lossless_data_exit_mode=true "
                            "-rocksdb_write_mode=sync");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));

    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
}

TEST_F(KVClientWriteRocksdbTest, TestSyncModeScaleUp)
{
    StartWorkerAndWaitReady({ 0 }, "-rocksdb_write_mode=sync");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));
    StartWorkerAndWaitReady({ 1 });
    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
}

TEST_F(KVClientWriteRocksdbTest, TestASyncModeNoneL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=async");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client1);
    std::string val;
    ASSERT_EQ(client1->Get(key1, val).GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(KVClientWriteRocksdbTest, TestASyncModeL2Cache)
{
    StartWorkerAndWaitReady({ 0, 1 }, "-rocksdb_write_mode=async");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    SetParam param1;
    param1.writeMode = WriteMode::WRITE_THROUGH_L2_CACHE;
    DS_ASSERT_OK(client1->Set(key1, data, param1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "master.before_sub_async_send_etcd_req", "1*return(K_OK)"));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "persistence.service.save", "return(K_OK)"));
    std::string key2;
    (void)client1->GenerateKey("", key2);
    SetParam param2;
    param2.writeMode = WriteMode::WRITE_BACK_L2_CACHE;
    DS_ASSERT_OK(client1->Set(key2, data, param2));
    DS_ASSERT_OK(cluster_->ShutdownNode(WORKER, 0));
    DS_ASSERT_OK(cluster_->StartNode(WORKER, 0, ""));
    DS_ASSERT_OK(cluster_->WaitNodeReady(WORKER, 0));
    InitTestKVClient(0, client1);
    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
    ASSERT_EQ(client1->Get(key2, val).GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(KVClientWriteRocksdbTest, TestASyncModeVoluntaryScaleDown)
{
    StartWorkerAndWaitReady({ 0, 1 },
                            "-node_timeout_s=5 -node_dead_timeout_s=8 -enable_lossless_data_exit_mode=true "
                            "-rocksdb_write_mode=async");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));

    VoluntaryScaleDownInject(0);
    sleep(3);  // Wait 3 seconds for voluntary scale down finished

    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
}

TEST_F(KVClientWriteRocksdbTest, TestASyncModeScaleUp)
{
    StartWorkerAndWaitReady({ 0 }, "-rocksdb_write_mode=async");
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client1);
    uint64_t size = 128;
    std::string data = GenRandomString(size);
    std::string key1;
    (void)client1->GenerateKey("", key1);
    DS_ASSERT_OK(client1->Set(key1, data));
    StartWorkerAndWaitReady({ 1 });
    std::string val;
    DS_ASSERT_OK(client1->Get(key1, val));
    ASSERT_EQ(val, data);
}

class KVCacheClientServiceDiscoveryTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 2;
        opts.enableDistributedMaster = "false";
        opts.numEtcd = 1;
        opts.workerGflagParams = "-shared_memory_size_mb=25 -v=1 -log_monitor=true -max_client_num=2000";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        FLAGS_log_monitor = true;
        InitClients();
    }

    void TearDown() override
    {
        client_.reset();
        ExternalClusterTest::TearDown();
    }

    void InitTestKVClient(std::shared_ptr<KVClient> &client)
    {
        // initialize serviceDiscovery.
        std::string etcdAddress;
        for (size_t i = 0; i < cluster_->GetEtcdNum(); ++i) {
            std::pair<HostPort, HostPort> addrs;
            cluster_->GetEtcdAddrs(i, addrs);
            if (!etcdAddress.empty()){
                etcdAddress += ",";
            }
            etcdAddress += addrs.first.ToString();
        }
        ServiceDiscoveryOptions opts;
        opts.etcdAddress = etcdAddress;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(opts);
        DS_ASSERT_OK(serviceDiscovery->Init());

        ConnectOptions connectOptions{
            .connectTimeoutMs = 60000,
            .requestTimeoutMs = 0,
            .accessKey = "QTWAOYTTINDUT2QVKYUC",
            .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc",
            .serviceDiscovery = serviceDiscovery
        };
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void InitClients()
    {
        InitTestKVClient(client_);
    }

    std::shared_ptr<KVClient> client_;
};

TEST_F(KVCacheClientServiceDiscoveryTest, TestSimpleSetGet)
{
    std::string key = "aaaa";
    std::string value = "sssss";
    ASSERT_EQ(client_->Set(key, value), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client_->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
}
}  // namespace st
}  // namespace datasystem

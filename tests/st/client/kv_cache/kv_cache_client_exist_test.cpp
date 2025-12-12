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
 * Description: State client exist tests.
 */
#include <unistd.h>
#include <memory>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <tbb/concurrent_hash_map.h>
#include "gmock/gmock.h"

#include "client/object_cache/oc_client_common.h"
#include "common.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/kv_client.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"

DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
constexpr int WAIT_ASYNC_NOTIFY_WORKER = 300;
class KVCacheClientExistTest : public OCClientCommon {
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

TEST_F(KVCacheClientExistTest, TestSpecialKeys)
{
    std::vector<bool> exists;
    ASSERT_EQ(client_->Exist({ "" }, exists).GetCode(), StatusCode::K_INVALID);
}

TEST_F(KVCacheClientExistTest, TestEmptyKeys)
{
    std::vector<bool> exists;
    ASSERT_EQ(client_->Exist({}, exists).GetCode(), StatusCode::K_INVALID);
}

TEST_F(KVCacheClientExistTest, TestNotExistKeys)
{
    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::vector<bool> exists;
    ASSERT_EQ(client_->Exist({ key0, key1, key2 }, exists).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(exists[0], false);
    ASSERT_EQ(exists[1], false);
    ASSERT_EQ(exists[2], false); // check object 2
}

TEST_F(KVCacheClientExistTest, TestPartNotExistKeys)
{
    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string value0 = "value0";
    ASSERT_EQ(client_->Set(key0, value0), Status::OK());

    std::vector<bool> exists;
    ASSERT_EQ(client_->Exist({ key0, key1, key2 }, exists).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(exists[0], true);
    ASSERT_EQ(exists[1], false);
    ASSERT_EQ(exists[2], false); // check object 2
}

TEST_F(KVCacheClientExistTest, TestExistKeys)
{
    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string value0 = "value0";
    std::string value1 = "value1";
    std::string value2 = "value2";
    ASSERT_EQ(client_->Set(key0, value0), Status::OK());
    ASSERT_EQ(client_->Set(key1, value1), Status::OK());
    ASSERT_EQ(client_->Set(key2, value2), Status::OK());

    std::vector<bool> exists;
    ASSERT_EQ(client_->Exist({ key0, key1, key2 }, exists).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(exists[0], true);
    ASSERT_EQ(exists[1], true);
    ASSERT_EQ(exists[2], true); // check object 2
}

TEST_F(KVCacheClientExistTest, TestRetryExistKeys)
{
    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string value0 = "value0";
    ASSERT_EQ(client_->Set(key0, value0), Status::OK());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.before_query_meta", "return(K_RPC_UNAVAILABLE)"));

    std::vector<bool> exists;
    ASSERT_EQ(client_->Exist({ key0, key1, key2 }, exists).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(exists[0], true);
    ASSERT_EQ(exists[1], false);
    ASSERT_EQ(exists[2], false); // check object 2
}

TEST_F(KVCacheClientExistTest, TestDifferentWorker)
{
    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string value0 = "value0";
    std::string value1 = "value1";
    ASSERT_EQ(client_->Set(key0, value0), Status::OK());
    ASSERT_EQ(client2_->Set(key1, value1), Status::OK());

    std::vector<bool> exists;
    ASSERT_EQ(client2_->Exist({ key0, key1, key2 }, exists).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(exists[0], true);
    ASSERT_EQ(exists[1], true);
    ASSERT_EQ(exists[2], false); // check object 2
}

TEST_F(KVCacheClientExistTest, TestTenantExistKeys)
{
    std::string key0 = "key0";
    std::string key1 = "key1";
    std::string key2 = "key2";
    std::string value0 = "value0";
    std::string value1 = "value1";
    ASSERT_EQ(client_->Set(key0, value0), Status::OK());
    ASSERT_EQ(client1_->Set(key1, value1), Status::OK());

    std::vector<bool> exists;
    ASSERT_EQ(client1_->Exist({ key0, key1, key2 }, exists).GetCode(), StatusCode::K_OK);
    ASSERT_EQ(exists[0], false);
    ASSERT_EQ(exists[1], true);
    ASSERT_EQ(exists[2], false); // check object 2
}

TEST_F(KVCacheClientExistTest, TestExistence)
{
    int32_t timeout = 1000;
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, timeout);
    InitTestKVClient(1, client1, timeout);

    std::vector<std::string> keys;
    const size_t numOfKeys = 100;
    for (size_t i = 0; i < numOfKeys; ++i) {
        keys.emplace_back(GetStringUuid());
    }

    std::vector<bool> exists;
    DS_ASSERT_OK(client0->Exist(keys, exists));
    EXPECT_THAT(exists, testing::Each(false));

    std::vector<std::string> values;
    for (size_t i = 0; i < numOfKeys; ++i) {
        values.emplace_back("value_" + std::to_string(i));
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        DS_ASSERT_OK(client0->Set(keys[i], values[i]));
    }

    DS_ASSERT_OK(client0->Exist(keys, exists));
    EXPECT_THAT(exists, testing::Each(true));
    DS_ASSERT_OK(client1->Exist(keys, exists));
    EXPECT_THAT(exists, testing::Each(true));

    auto idxToDelete = randomData_.GetRandomIndex(keys.size());
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1->Del({keys[idxToDelete]}, failedKeys));
    ASSERT_TRUE(failedKeys.empty());

    DS_ASSERT_OK(client0->Exist(keys, exists));
    EXPECT_THAT(exists, testing::Contains(true));
    ASSERT_FALSE(exists[idxToDelete]);

    DS_ASSERT_OK(client0->Del(keys, failedKeys));
    DS_ASSERT_OK(client0->Exist(keys, exists));
    EXPECT_THAT(exists, testing::Each(false));
}

TEST_F(KVCacheClientExistTest, ConnectTimeout)
{
    auto connectTimeoutMs = 4000;
    auto requestTimeoutMs = 7000;
    auto op = ConnectOptions{
        .host = "127.0.0.1",
        .port = 11212,
        .connectTimeoutMs = connectTimeoutMs,
        .requestTimeoutMs = requestTimeoutMs,
        .clientPublicKey = "",
        .clientPrivateKey = "",
        .serverPublicKey = "",
        .accessKey = "QTWAOYTTINDUT2QVKYUC",
        .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc",
    };
    KVClient client(op);
    Timer timer;
    client.Init();
    auto ms1000 = 1000;
    auto t = timer.ElapsedSecond() - connectTimeoutMs / ms1000;
    DS_ASSERT_TRUE((abs(t) <= 1), true);
    HostPort addr;
    cluster_->GetWorkerAddr(0, addr);
    op = ConnectOptions{
        .host = addr.Host(),
        .port = addr.Port(),
        .connectTimeoutMs = connectTimeoutMs,
        .requestTimeoutMs = requestTimeoutMs,
        .clientPublicKey = "",
        .clientPrivateKey = "",
        .serverPublicKey = "",
        .accessKey = "QTWAOYTTINDUT2QVKYUC",
        .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc",
    };
    cluster_->SetInjectAction(ClusterNodeType::WORKER, 0, "Exist.Sleep", "sleep(8000)");
    client = KVClient(op);
    DS_ASSERT_OK(client.Init());
    Timer timer2;
    std::vector<bool> ex;
    client.Exist({ "failedKeys" }, ex);
    t = timer2.ElapsedSecond() - requestTimeoutMs / ms1000;
    DS_ASSERT_TRUE((abs(t) <= 1), true);
}
}  // namespace st
}  // namespace datasystem

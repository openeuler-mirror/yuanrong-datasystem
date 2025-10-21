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
 * Description: State client expire tests.
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
#include "datasystem/kv_cache/read_only_buffer.h"
#include "datasystem/kv_cache/kv_client.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"

DS_DECLARE_bool(log_monitor);

namespace datasystem {
namespace st {
const int32_t OPT_TIMEOUT_MS = 1000;
class KVCacheClientExpireTest : public OCClientCommon {
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
        InitTestKVClient(0, client_, [&](ConnectOptions &opts) {
            opts.connectTimeoutMs = OPT_TIMEOUT_MS;
            opts.SetAkSkAuth(accessKey_, secretKey_, tenantId_);
        });
        InitTestKVClient(0, client1_, [&](ConnectOptions &opts) {
            opts.connectTimeoutMs = OPT_TIMEOUT_MS;
            opts.SetAkSkAuth(accessKey_, secretKey_, tenantId1_);
        });
        InitTestKVClient(1, client2_, [&](ConnectOptions &opts) {
            opts.connectTimeoutMs = OPT_TIMEOUT_MS;
            opts.SetAkSkAuth(accessKey_, secretKey_, tenantId_);
        });
    }

    std::shared_ptr<KVClient> client_;
    std::shared_ptr<KVClient> client1_;
    std::shared_ptr<KVClient> client2_;
    std::string tenantId_ = "client0";
    std::string tenantId1_ = "client1";

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(KVCacheClientExpireTest, TestExpireKey)
{
    std::string key1, key2;
    std::vector<std::string> keys, failedKeys;
    std::string value = "emotional damage";
    size_t objNum = 20;
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key1, value));
        keys.emplace_back(key1);
    }
    (void)client_->GenerateKey("", key2);
    DS_ASSERT_OK(client_->Set(key2, value));
    keys.emplace_back(key2);

    uint64_t ttlSecond = 1;
    DS_ASSERT_OK(client_->Expire(keys, ttlSecond, failedKeys));
    std::this_thread::sleep_for(std::chrono::milliseconds(2500)); // sleep 2500 millsec
    std::vector<std::string> getValue;
    auto rc = client_->Get(keys, getValue);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
}

TEST_F(KVCacheClientExpireTest, ExpireInvalidKey)
{
    std::string key1 = "";
    std::string key2 = "dasfa$$,,aca./?|*";
    std::vector<std::string> keys, failedKeys;
    Status rc;
    uint64_t ttlTimeout = 10;
    rc = client_->Expire(keys, ttlTimeout, failedKeys);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    rc = client_->Expire({ key1 }, ttlTimeout, failedKeys);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    rc = client_->Expire({ key2 }, ttlTimeout, failedKeys);
    EXPECT_EQ(rc.GetCode(), K_INVALID);
    size_t objNum = 20;
    failedKeys.clear();
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key1" + std::to_string(i);
        keys.emplace_back(key1);
    }
    rc = client_->Expire(keys, ttlTimeout, failedKeys);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
}

TEST_F(KVCacheClientExpireTest, ExpireHalfFailed)
{
    std::string key1, key2;
    std::vector<std::string> keys, failedKeys;
    std::string value = "Have a good day";
    size_t objNum = 20;
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key1_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key1, value));
        keys.emplace_back(key1);
        key2 = "key2_" + std::to_string(i);
        keys.emplace_back(key2);
    }
    uint64_t ttlTimeout = 10;
    DS_ASSERT_OK(client_->Expire(keys, ttlTimeout, failedKeys));
    EXPECT_EQ(failedKeys.size(), objNum);
}

TEST_F(KVCacheClientExpireTest, ExpireObjRetry)
{
    std::string key1;
    std::vector<std::string> keys, failedKeys;
    std::string value = "value";
    size_t objNum = 10;
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key1, value));
        keys.emplace_back(key1);
    }
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 1, "worker.expire_failed)", "return(K_RPC_UNAVAILABLE)"));

    uint64_t ttlTimeout = 2;
    DS_ASSERT_OK(client_->Expire(keys, ttlTimeout, failedKeys));
    ASSERT_TRUE(failedKeys.empty());
}

TEST_F(KVCacheClientExpireTest, ExpireObjWithDiffTenant)
{
    std::string key = "key";
    std::string value1 = "Good morning";
    std::string value2 = "Good night";
    DS_ASSERT_OK(client1_->Set(key, value1));
    DS_ASSERT_OK(client2_->Set(key, value2));

    uint64_t ttlTimeout = 2;
    std::vector<std::string> failedKeys;
    DS_ASSERT_OK(client1_->Expire({key}, ttlTimeout, failedKeys));
    sleep(ttlTimeout * 2); // wait 2 * ttl to del meta
    std::string getValue;
    auto rc = client1_->Get(key, getValue);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
    DS_ASSERT_OK(client2_->Get(key, getValue));
    EXPECT_EQ(getValue, value2);
}

TEST_F(KVCacheClientExpireTest, ExpireObjWhenScaleDown)
{
    std::string key1, key2;
    std::vector<std::string> keys, failedKeys;
    std::string value = "the sun, the moon and you";
    
    size_t objNum = 20;
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key1_" + std::to_string(i);
        DS_ASSERT_OK(client_->Set(key1, value));
        keys.emplace_back(key1);
        key2 = "key2_" + std::to_string(i);
        DS_ASSERT_OK(client2_->Set(key2, value));
        keys.emplace_back(key2);
    }
    
    uint64_t ttlTimeout = 10;
    DS_ASSERT_OK(client_->Expire(keys, ttlTimeout, failedKeys));
}

class KVClientExpireCrossAZ : public KVCacheClientExpireTest {
void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numEtcd = 1;
        opts.numOBS = 1;
        opts.numWorkers = workerNum_;
        opts.addNodeTime = 3; // add node time is 3 sec
        opts.workerGflagParams = FormatString(
            " -v=1 -node_timeout_s=%d -node_dead_timeout_s=%d -other_az_names=AZ1,AZ2 "
            "-cross_az_get_meta_from_worker=true -cross_az_get_data_from_worker=true",
            timeoutS_, deadTimeoutS_);
        for (size_t i = 0; i < workerNum_; i++) {
            std::string param = "-etcd_table_prefix=" + azNames_[i % azNames_.size()];
            opts.workerSpecifyGflagParams[i] += param;
        }
    }

private:
    std::vector<std::string> azNames_ = {"AZ1", "AZ2"};
    size_t workerNum_ = 2;
    int timeoutS_ = 20;
    int deadTimeoutS_ = 60;
};

TEST_F(KVClientExpireCrossAZ, ExpireObjCrossAZ)
{
    std::shared_ptr<KVClient> client0;
    std::shared_ptr<KVClient> client1;
    InitTestKVClient(0, client0, OPT_TIMEOUT_MS);
    InitTestKVClient(1, client1, OPT_TIMEOUT_MS);
    std::string key1, key2;
    std::vector<std::string> keys, failedKeys;
    std::string value = "I love three things in the world";
    size_t objNum = 20;
    for (size_t i = 0; i < objNum; i++) {
        key1 = "key1_" + std::to_string(i);
        DS_ASSERT_OK(client0->Set(key1, value));
        keys.emplace_back(key1);
        key2 = "key2_" + std::to_string(i);
        DS_ASSERT_OK(client1->Set(key2, value));
        keys.emplace_back(key2);
    }
    (void)client0->GenerateKey("", key1);
    (void)client1->GenerateKey("", key2);
    DS_ASSERT_OK(client0->Set(key1, value));
    DS_ASSERT_OK(client1->Set(key2, value));
    keys.emplace_back(key1);
    keys.emplace_back(key2);
    uint64_t ttlSecond = 1;
    DS_ASSERT_OK(client0->Expire(keys, ttlSecond, failedKeys));
    ASSERT_TRUE(failedKeys.empty());
    std::this_thread::sleep_for(std::chrono::milliseconds(2500)); // sleep 2500 millsec
    std::string getValue;
    auto rc = client0->Get(key2, getValue);
    EXPECT_EQ(rc.GetCode(), K_NOT_FOUND);
}

}
}
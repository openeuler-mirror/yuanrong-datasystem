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
 * Description:
 */
#include <gtest/gtest.h>
#include <algorithm>

#include "common.h"
#include "datasystem/context/context.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
class KVClientTenantAuthTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -authorization_enable=true ";
        opts.numEtcd = 1;
    }

    void InitTestKVClient(std::shared_ptr<KVClient> &client)
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(token1,tenant1)"));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr_.Host();
        connectOptions.port = workerAddr_.Port();
        connectOptions.connectTimeoutMs = 60000;  // 60000 ms is timeout
        connectOptions.token = "token1";
        connectOptions.requestTimeoutMs = 0;
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr_));
    }

protected:
    HostPort workerAddr_;
};

TEST_F(KVClientTenantAuthTest, TestUpdateToken)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(client);
    std::string key = "key";
    std::string key1 = "key1";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string token = "qqqqqq";
    SensitiveValue token1(token);
    DS_ASSERT_OK(client->UpdateToken(token1));
    ASSERT_NE(client->Set(key1, value, param), Status::OK());
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.auth", "return(qqqqqq,tenant1)"));
    ASSERT_EQ(client->Set(key1, value, param), Status::OK());
}

TEST_F(KVClientTenantAuthTest, TestSetWriteMode)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(client);

    std::string key = "key";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string valueGet;
    ASSERT_EQ(client->Get(key, valueGet), Status::OK());
    ASSERT_EQ(value, std::string(valueGet.data(), valueGet.size()));
    ASSERT_EQ(client->Del(key), Status::OK());

    std::string key1 = "key1";
    std::string value1 = "value1";
    SetParam param1{ .writeMode = WriteMode::WRITE_THROUGH_L2_CACHE };
    ASSERT_EQ(client->Set(key1, value1, param1), Status::OK());
    std::string valueGet1;
    ASSERT_EQ(client->Get(key1, valueGet1), Status::OK());
    ASSERT_EQ(value1, std::string(valueGet1.data(), valueGet1.size()));
    ASSERT_EQ(client->Del(key1), Status::OK());
}

class KVClientNotEnableAuthTest : public KVClientTenantAuthTest {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.workerGflagParams = " -authorization_enable=false ";
        opts.systemAccessKey = "";
        opts.tenantAccessKey = "";
    }
};

TEST_F(KVClientNotEnableAuthTest, TestInitClientWithTenantId)
{
    std::shared_ptr<KVClient> client1;
    ConnectOptions connectOptions1{ .host = workerAddr_.Host(),
                                    .port = workerAddr_.Port(),
                                    .connectTimeoutMs = 60 * 1000 };
    connectOptions1.tenantId = "tenant1";
    client1 = std::make_shared<KVClient>(connectOptions1);
    DS_ASSERT_OK(client1->Init());
    auto key = "key";
    auto val = "anyVal";
    DS_ASSERT_OK(client1->Set(key, val));

    std::shared_ptr<KVClient> client2;
    ConnectOptions connectOptions2{ .host = workerAddr_.Host(),
                                    .port = workerAddr_.Port(),
                                    .connectTimeoutMs = 60 * 1000 };
    connectOptions2.tenantId = "tenant2";
    client2 = std::make_shared<KVClient>(connectOptions2);
    DS_ASSERT_OK(client2->Init());
    std::string valToGet;
    auto status = client2->Get(key, valToGet);
    ASSERT_EQ(status.GetCode(), K_NOT_FOUND);
}

class KVClientTenantIdsAuthTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams = "";
        opts.numEtcd = 1;
    }

    void KVClientTest(std::shared_ptr<KVClient> &client)
    {
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
        valueGet.clear();
        DS_ASSERT_NOT_OK(client->Get(key, valueGet));
    }

    void TestMSet(std::shared_ptr<KVClient> &client)
    {
        MSetParam param;
        std::vector<std::string> keys;
        std::vector<StringView> values;
        param.existence = ExistenceOpt::NX;
        size_t maxElementSize = 8;
        int bigValSize = 1000 * 1024l;
        std::vector<std::string> vals;
        int val = 300;
        int randSet = 2;
        for (size_t i = 0; i < maxElementSize; ++i) {
            if (i % randSet == 0) {
                vals.emplace_back(randomData_.GetRandomString(bigValSize));
            } else {
                vals.emplace_back(randomData_.GetRandomString(val));
            }
        }
        for (size_t i = 0; i < maxElementSize; ++i) {
            auto key = "a_key_for_mset_" + std::to_string(i);
            keys.emplace_back(key);
            values.emplace_back(vals[i]);
        }
        DS_ASSERT_OK(client->MSetTx(keys, values, param));
        int i = 0;
        for (const auto &key : keys) {
            std::string val;
            DS_ASSERT_OK(client->Get(key, val));
            ASSERT_EQ(val, values[i].data());
            DS_ASSERT_OK(client->Del(key));
            val.clear();
            DS_ASSERT_NOT_OK(client->Get(key, val));
            i++;
        }
    }

    void InitAkSkTestKVClient(std::shared_ptr<KVClient> &client)
    {
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr_.Host();
        connectOptions.port = workerAddr_.Port();
        connectOptions.connectTimeoutMs = 60 * 1000;  // 60s
        connectOptions.requestTimeoutMs = 0;
        connectOptions.token = "";
        connectOptions.clientPublicKey = "";
        connectOptions.clientPrivateKey = "";
        connectOptions.serverPublicKey = "";
        connectOptions.accessKey = "QTWAOYTTINDUT2QVKYUC";
        connectOptions.secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr_));
    }

protected:
    HostPort workerAddr_;
};

TEST_F(KVClientTenantIdsAuthTest, TestKVClient)
{
    std::shared_ptr<KVClient> client;
    InitAkSkTestKVClient(client);

    std::thread thread1([&client, this] {
        Context::SetTenantId("tenant2");
        KVClientTest(client);
        TestMSet(client);
    });

    std::thread thread2([&client, this] {
        Context::SetTenantId("tenantId1");
        KVClientTest(client);
        TestMSet(client);
    });
    thread1.join();
    thread2.join();
}
}  // namespace st
}  // namespace datasystem

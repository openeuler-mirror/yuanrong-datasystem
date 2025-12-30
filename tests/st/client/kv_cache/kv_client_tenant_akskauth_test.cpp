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
 * Description: Update aksk test.
 */

#include <gtest/gtest.h>
#include <algorithm>
#include <functional>

#include "common.h"
#include "datasystem/context/context.h"
#include "client/object_cache/oc_client_common.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace st {
class KVClientTenantAkSkAuthTest : public OCClientCommon {
public:
    std::vector<std::string> workerAddress_;

    void SetClusterSetupOptions(ExternalClusterOptions &opts)
    {
        opts.numOBS = 1;
        opts.numWorkers = 1;
        opts.numEtcd = 1;
        opts.iamKit = "yuanrong_iam";
        std::string hostIp = "127.0.0.1";
        opts.workerConfigs.emplace_back(hostIp, GetFreePort());
        for (auto addr : opts.workerConfigs) {
            workerAddress_.emplace_back(addr.ToString());
        }
        opts.enableDistributedMaster = "false";
        opts.workerGflagParams =
            " -authorization_enable=true "
            "-yuanrong_iam_url=https://www.example.com";
        opts.numEtcd = 1;
    }

    void InitAkSkTestKVClient(std::shared_ptr<KVClient> &client)
    {
        DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.akauth", "return(accesskey1,secretkey1,tenant1)"));
        ConnectOptions connectOptions;
        connectOptions.host = workerAddr_.Host();
        connectOptions.port = workerAddr_.Port();
        connectOptions.connectTimeoutMs = 60000;  // 60000 ms is timeout
        connectOptions.accessKey = "accesskey1";
        connectOptions.secretKey = "secretkey1";
        connectOptions.tenantId = "tenant1";
        client = std::make_shared<KVClient>(connectOptions);
        DS_ASSERT_OK(client->Init());
    }

    void SetUp()
    {
        ExternalClusterTest::SetUp();
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr_));
    }

protected:
    HostPort workerAddr_;
};

TEST_F(KVClientTenantAkSkAuthTest, LEVEL2_TestUpdateAksk)
{
    std::shared_ptr<KVClient> client;
    InitAkSkTestKVClient(client);

    std::string key = "key";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());

    std::string accessKey2 = "newaccesskey";
    std::string secretKey_ = "newsecretkey";
    SensitiveValue secretKey2(secretKey_);
    DS_ASSERT_OK(client->UpdateAkSk(accessKey2, secretKey2));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.akauth", "return(accesskey1,secretkey1,tenant1)"));
    ASSERT_NE(client->Set(key, value, param), Status::OK());

    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "worker.akauth", "return(newaccesskey,newsecretkey,tenant1)"));
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
}

TEST_F(KVClientTenantAkSkAuthTest, TestTokenExperied)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    std::string key = "key";
    std::string key1 = "key1";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    ASSERT_EQ(client->Set(key, value, param), Status::OK());
    std::string token = "qqqqqq";
    SensitiveValue token1(token);
    DS_ASSERT_OK(client->UpdateToken(token1));
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "SendRequestAndRetry.SendToken", "return()"));
    auto status = client->Set(key1, value, param);
    ASSERT_EQ(status.GetCode(), K_NOT_AUTHORIZED);
    ASSERT_TRUE(status.GetMsg().find("token expried") != std::string::npos);
}

TEST_F(KVClientTenantAkSkAuthTest, TestAkSkExperied)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client);
    DS_ASSERT_OK(client->UpdateAkSk("accesskey1", "secretkey1"));
    std::string key = "key";
    std::string key1 = "key1";
    std::string value = "value";
    SetParam param{ .writeMode = WriteMode::NONE_L2_CACHE };
    DS_ASSERT_OK(cluster_->SetInjectAction(WORKER, 0, "SendRequestAndRetry.SendAkSk", "return()"));
    auto status = client->Set(key1, value, param);
    ASSERT_EQ(status.GetCode(), K_NOT_AUTHORIZED);
    ASSERT_TRUE(status.GetMsg().find("AkSk expired") != std::string::npos);
}
}  // namespace st
}  // namespace datasystem

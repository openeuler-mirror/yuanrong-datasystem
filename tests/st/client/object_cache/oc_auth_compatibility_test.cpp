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

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "oc_client_common.h"

namespace datasystem {
namespace st {
namespace {
constexpr char TEST_ACCESS_KEY[] = "test-only-rpc-access";
constexpr char TEST_SECRET_KEY[] = "test-only-rpc-secret";
}

class OcRpcAuthIntegrationTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &options) override
    {
        options.numWorkers = 2;
        options.numEtcd = 1;
        options.systemAccessKey = TEST_ACCESS_KEY;
        options.systemSecretKey = TEST_SECRET_KEY;
        options.workerGflagParams = " -shared_memory_size_mb=64";
    }
};

TEST_F(OcRpcAuthIntegrationTest, CrossWorkerRequestsUseCurrentCredentialsAndRejectInvalidRotation)
{
    const auto credentials = [](ConnectOptions &options) {
        options.accessKey = TEST_ACCESS_KEY;
        options.secretKey = SensitiveValue(TEST_SECRET_KEY);
        options.connectTimeoutMs = 5000;
        options.requestTimeoutMs = 2000;
        options.enableLocalCache = true;
    };
    std::shared_ptr<KVClient> writer;
    std::shared_ptr<KVClient> reader;
    InitTestKVClient(0, writer, credentials);
    InitTestKVClient(1, reader, credentials);
    ASSERT_NE(writer, nullptr);
    ASSERT_NE(reader, nullptr);
    const std::string value(8192, 'x');
    DS_ASSERT_OK(writer->Set("rpc-auth-before-rotation", value));
    std::string received;
    DS_ASSERT_OK(reader->Get("rpc-auth-before-rotation", received));
    EXPECT_EQ(received, value);
    DS_ASSERT_OK(writer->UpdateAkSk(TEST_ACCESS_KEY, SensitiveValue("test-only-invalid-secret")));
    EXPECT_EQ(writer->Set("rpc-auth-rejected", value).GetCode(), K_NOT_AUTHORIZED);
    DS_ASSERT_OK(writer->UpdateAkSk(TEST_ACCESS_KEY, SensitiveValue(TEST_SECRET_KEY)));
    DS_ASSERT_OK(writer->Set("rpc-auth-after-rotation", value));
    DS_ASSERT_OK(reader->Get("rpc-auth-after-rotation", received));
    EXPECT_EQ(received, value);
}

class OcRpcAuthOffIntegrationTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &options) override
    {
        options.numWorkers = 1;
        options.numEtcd = 1;
        options.systemAccessKey.clear();
        options.systemSecretKey.clear();
        options.workerGflagParams = " -shared_memory_size_mb=64";
    }
};

TEST_F(OcRpcAuthOffIntegrationTest, OrdinaryClientKeepsWorkingAfterFirstEnableIsRejected)
{
    std::shared_ptr<KVClient> client;
    InitTestKVClient(0, client, [](ConnectOptions &options) {
        options.connectTimeoutMs = 5000;
        options.requestTimeoutMs = 2000;
        options.enableLocalCache = true;
    });
    ASSERT_NE(client, nullptr);
    DS_ASSERT_OK(client->Set("rpc-auth-off", "ordinary"));
    EXPECT_EQ(client->UpdateAkSk(TEST_ACCESS_KEY, SensitiveValue(TEST_SECRET_KEY)).GetCode(), K_NOT_SUPPORTED);
    std::string value;
    DS_ASSERT_OK(client->Get("rpc-auth-off", value));
    EXPECT_EQ(value, "ordinary");
}
}  // namespace st
}  // namespace datasystem

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
#include <gtest/gtest.h>
#include <memory>

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/client/object_cache/client_worker_api.h"
#include "oc_client_common.h"
#include "zmq_curve_test_common.h"

DS_DECLARE_string(encrypt_kit);

namespace datasystem {
namespace st {
class OCClientPlaintextZmqCurveTest : public OCClientCommon, public ZmqCurveTestCommon {
public:
    const std::string &GetCurveKeyDir() override
    {
        return plaintextCurveDir_;
    }

    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = workerCount;
        opts.numEtcd = 1;

        // use default configurations for all the other zmq curve gflags settings
        const std::string zmqConfig =
            "-enable_curve_zmq=true -encrypt_kit=plaintext -curve_key_dir=" + GetCurveKeyDir();
        opts.workerGflagParams = zmqConfig;
        FLAGS_v = 1;
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        AuthServerProcesses();
    }

    Status AuthServerProcesses()
    {
        FLAGS_encrypt_kit = "plaintext";
        RETURN_IF_NOT_OK(ClientPreLoadKey(validClientName, authKeys_));
        RpcCredential cred;
        RETURN_IF_NOT_OK(RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred));
        ServerProcess *serverProcess = nullptr;
        for (uint8_t i = 0; i < workerCount; i++) {
            cluster_->GetProcess(WORKER, i, serverProcess);
            serverProcess->SetRpcSession(cred);
        }
        return Status::OK();
    }

    Status InitTestClient(std::shared_ptr<ObjectClient> &client, const RpcAuthKeys &authKeys = {}, uint32_t index = 0)
    {
        HostPort workerAddress;
        RETURN_IF_NOT_OK(cluster_->GetWorkerAddr(index, workerAddress));
        const char *serverPublicKey;
        authKeys.GetServerKey(WORKER_SERVER_NAME, serverPublicKey);
        ConnectOptions connectOpts{ .host = workerAddress.Host(),
                                    .port = workerAddress.Port(),
                                    .connectTimeoutMs = 1 * 1000,
                                    .requestTimeoutMs = 0,
                                    .clientPublicKey = authKeys.GetClientPublicKey(),
                                    .clientPrivateKey = authKeys.GetClientPrivateKey(),
                                    .serverPublicKey = serverPublicKey,
                                    .accessKey = "QTWAOYTTINDUT2QVKYUC",
                                    .secretKey = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc" };
        client = std::make_shared<ObjectClient>(connectOpts);
        RETURN_IF_NOT_OK(client->Init());
        return Status::OK();
    }

    std::string plaintextCurveDir_ = "data/client_plaintext_zmq_curve_test";
    const uint32_t workerCount = 2;
};

TEST_F(OCClientPlaintextZmqCurveTest, TestValidClientCurve)
{
    LOG(INFO) << "Start to test plaintext zmq curve for valid client.";
    std::shared_ptr<ObjectClient> client;
    RpcAuthKeys authKeys = {};
    DS_ASSERT_OK(ClientPreLoadKey(validClientName, authKeys));
    DS_ASSERT_OK(InitTestClient(client, authKeys));

    std::string objectId = NewObjectKey();
    std::shared_ptr<Buffer> data4CreatOption;
    CreateParam param;
    DS_ASSERT_OK(client->Create(objectId, 20, param, data4CreatOption)); // obj size 20
    DS_ASSERT_OK(data4CreatOption->Seal());
    ASSERT_NE(data4CreatOption, nullptr);
    std::vector<std::string> objectIds;
    objectIds.push_back(objectId);
    std::vector<Optional<Buffer>> dataList;
    DS_ASSERT_OK(client->Get(objectIds, 5, dataList)); // obj size 5
    ASSERT_TRUE(NotExistsNone(dataList));
    ASSERT_EQ(20, dataList[0]->GetSize()); // obj size 20
}

TEST_F(OCClientPlaintextZmqCurveTest, TestInValidClientCurve)
{
    LOG(INFO) << "Start to test plaintext zmq curve for invalid client.";
    std::shared_ptr<ObjectClient> client;
    RpcAuthKeys authKeys = {};
    DS_ASSERT_OK(ClientPreLoadKey(invalidClientName, authKeys));
    DS_ASSERT_NOT_OK(InitTestClient(client, authKeys));
}
}  // namespace st
}  // namespace datasystem

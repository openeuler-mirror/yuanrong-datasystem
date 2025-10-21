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
 * Description: update object data test
 */

#include <gtest/gtest.h>
#include <unistd.h>
#include <memory>

#include "common.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_auth_keys.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/ut_object.stub.rpc.pb.h"
#include "datasystem/utils/status.h"
#include "oc_client_common.h"

namespace datasystem {
namespace st {

const std::string WORKER_SERVER_NAME = "worker";

class OcAuthCompatibilityTest : public OCClientCommon {
public:
    void SetClusterSetupOptions(ExternalClusterOptions &opts) override
    {
        opts.numWorkers = 1;  // worker num is 1
        opts.numEtcd = 1;     // only one etcd
        opts.numOcThreadNum = 1;
        opts.numRpcThreads = 1;
        opts.workerGflagParams =
            " -shared_memory_size_mb=64 -v=2 -log_monitor=true -rpc_thread_num=1 -oc_thread_num=1 "
            "-inject_actions=client_manager.GetClientInfo:return()";
    }

    void SetUp() override
    {
        ExternalClusterTest::SetUp();
        akSkManager_ = std::make_shared<AkSkManager>();
        DS_ASSERT_OK(akSkManager_->SetClientAkSk(accessKey_, secretKey_));

        HostPort workerAddr0;
        DS_ASSERT_OK(cluster_->GetWorkerAddr(0, workerAddr0));
        RpcCredential cred;
        RpcAuthKeyManager::CreateClientCredentials(authKeys_, WORKER_SERVER_NAME, cred);
        auto channel = std::make_shared<RpcChannel>(workerAddr0, cred);
        stub_ = std::make_unique<UtOCService_Stub>(channel);
    }

protected:
    std::shared_ptr<UtOCService_Stub> stub_;
    std::shared_ptr<AkSkManager> akSkManager_;
    RpcAuthKeys authKeys_;

    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(OcAuthCompatibilityTest, TestUnaryAuthCompatibility)
{
    TestReqPbV2 req;
    TestRspPbV1 rsp;
    int num = 3;
    req.set_filed1(num);
    req.set_field2("xxx");
    req.set_field3(num);
    for (int i = 0; i < num; ++i) {
        req.add_filed4(std::to_string(i));
    }
    req.set_filed5(num);  // filed5 not exist in TestReqPbV1.
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));

    uint32_t authType = 1;
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));

    authType = 2;  // set auth type as 2
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));
}

TEST_F(OcAuthCompatibilityTest, TestStreamAuthCompatibility)
{
    TestReqPbV2 req;
    TestRspPbV1 rsp;
    int num = 3;
    req.set_filed1(num);
    req.set_field2("xxx");
    req.set_field3(num);
    for (int i = 0; i < num; ++i) {
        req.add_filed4(std::to_string(i));
    }
    req.set_filed5(num);  // filed5 not exist in TestReqPbV1.
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    std::unique_ptr<datasystem::ClientWriterReader<TestReqPbV2, TestRspPbV1>> stream;
    DS_ASSERT_OK(stub_->TestStreamCompatibilityV2(&stream));
    DS_ASSERT_OK(stream->Write(req));
    DS_ASSERT_OK(stream->Read(rsp));
    DS_ASSERT_OK(stream->Finish());

    uint32_t authType = 1;
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    std::unique_ptr<datasystem::ClientWriterReader<TestReqPbV2, TestRspPbV1>> stream1;
    DS_ASSERT_OK(stub_->TestStreamCompatibilityV2(&stream1));
    DS_ASSERT_OK(stream1->Write(req));
    DS_ASSERT_OK(stream1->Read(rsp));
    DS_ASSERT_OK(stream1->Finish());

    authType = 2;  // set auth type as 2
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    std::unique_ptr<datasystem::ClientWriterReader<TestReqPbV2, TestRspPbV1>> stream2;
    DS_ASSERT_OK(stub_->TestStreamCompatibilityV2(&stream2));
    DS_ASSERT_OK(stream2->Write(req));
    DS_ASSERT_OK(stream2->Read(rsp));
    DS_ASSERT_OK(stream2->Finish());
}

TEST_F(OcAuthCompatibilityTest, TestStreamAuthCompatibility1)
{
    TestReqPbV2 req;
    TestRspPbV1 rsp;
    int num = 3;
    req.set_filed1(num);
    req.set_field2("xxx");
    req.set_field3(num);
    for (int i = 0; i < num; ++i) {
        req.add_filed4(std::to_string(i));
    }
    req.set_filed5(num);  // filed5 not exist in TestReqPbV1.
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    std::unique_ptr<datasystem::ClientWriter<TestReqPbV2>> stream;
    DS_ASSERT_OK(stub_->TestStreamCompatibility1V2(&stream));
    DS_ASSERT_OK(stream->Write(req));
    DS_ASSERT_OK(stream->Read(rsp));
    DS_ASSERT_OK(stream->Finish());

    uint32_t authType = 1;
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    std::unique_ptr<datasystem::ClientWriter<TestReqPbV2>> stream1;
    DS_ASSERT_OK(stub_->TestStreamCompatibility1V2(&stream1));
    DS_ASSERT_OK(stream1->Write(req));
    DS_ASSERT_OK(stream1->Read(rsp));
    DS_ASSERT_OK(stream1->Finish());

    authType = 2;  // set auth type as 2
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    std::unique_ptr<datasystem::ClientWriter<TestReqPbV2>> stream2;
    DS_ASSERT_OK(stub_->TestStreamCompatibility1V2(&stream2));
    DS_ASSERT_OK(stream2->Write(req));
    DS_ASSERT_OK(stream2->Read(rsp));
    DS_ASSERT_OK(stream2->Finish());
}

TEST_F(OcAuthCompatibilityTest, TestUnaryAuthCompatibilityWithOldVersion)
{
    TestReqPbV2 req;
    TestRspPbV1 rsp;
    int num = 3;
    req.set_filed1(num);
    req.set_field2("xxx");
    req.set_field3(num);
    for (int i = 0; i < num; ++i) {
        req.add_filed4(std::to_string(i));
    }
    req.set_filed5(num);  // filed5 not exist in TestReqPbV1.
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    ASSERT_EQ(stub_->TestUnaryCompatibilityV2(req, rsp).GetCode(), StatusCode::K_NOT_AUTHORIZED);

    req.clear_filed5();
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));

    uint32_t authType = 1;
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));

    authType = 2;  // set auth type as 2
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));
}

TEST_F(OcAuthCompatibilityTest, TestStreamAuthWithOldVersionCompatibility)
{
    TestReqPbV2 req;
    TestRspPbV1 rsp;
    int num = 3;
    req.set_filed1(num);
    req.set_field2("xxx");
    req.set_field3(num);
    for (int i = 0; i < num; ++i) {
        req.add_filed4(std::to_string(i));
    }
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    std::unique_ptr<datasystem::ClientWriterReader<TestReqPbV2, TestRspPbV1>> stream;
    DS_ASSERT_OK(stub_->TestStreamCompatibilityV2(&stream));
    DS_ASSERT_OK(stream->Write(req));
    DS_ASSERT_OK(stream->Read(rsp));
    DS_ASSERT_OK(stream->Finish());

    uint32_t authType = 1;
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    std::unique_ptr<datasystem::ClientWriterReader<TestReqPbV2, TestRspPbV1>> stream1;
    DS_ASSERT_OK(stub_->TestStreamCompatibilityV2(&stream1));
    DS_ASSERT_OK(stream1->Write(req));
    DS_ASSERT_OK(stream1->Read(rsp));
    DS_ASSERT_OK(stream1->Finish());

    authType = 2;  // set auth type as 2
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    std::unique_ptr<datasystem::ClientWriterReader<TestReqPbV2, TestRspPbV1>> stream2;
    DS_ASSERT_OK(stub_->TestStreamCompatibilityV2(&stream2));
    DS_ASSERT_OK(stream2->Write(req));
    DS_ASSERT_OK(stream2->Read(rsp));
    DS_ASSERT_OK(stream2->Finish());
}

TEST_F(OcAuthCompatibilityTest, TestStreamAuthWithOldVersionCompatibility1)
{
    TestReqPbV2 req;
    TestRspPbV1 rsp;
    int num = 3;
    req.set_filed1(num);
    req.set_field2("xxx");
    req.set_field3(num);
    for (int i = 0; i < num; ++i) {
        req.add_filed4(std::to_string(i));
    }
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    std::unique_ptr<datasystem::ClientWriter<TestReqPbV2>> stream;
    DS_ASSERT_OK(stub_->TestStreamCompatibility1V2(&stream));
    DS_ASSERT_OK(stream->Write(req));
    DS_ASSERT_OK(stream->Read(rsp));
    DS_ASSERT_OK(stream->Finish());

    uint32_t authType = 1;
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    std::unique_ptr<datasystem::ClientWriter<TestReqPbV2>> stream1;
    DS_ASSERT_OK(stub_->TestStreamCompatibility1V2(&stream1));
    DS_ASSERT_OK(stream1->Write(req));
    DS_ASSERT_OK(stream1->Read(rsp));
    DS_ASSERT_OK(stream1->Finish());

    authType = 2;  // set auth type as 2
    req.set_auth_req(authType);
    DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
    // old version will not carry these message.
    g_ReqAk.clear();
    g_ReqSignature.clear();
    g_ReqTimestamp = 0;
    std::unique_ptr<datasystem::ClientWriter<TestReqPbV2>> stream2;
    DS_ASSERT_OK(stub_->TestStreamCompatibility1V2(&stream2));
    DS_ASSERT_OK(stream2->Write(req));
    DS_ASSERT_OK(stream2->Read(rsp));
    DS_ASSERT_OK(stream2->Finish());
}

TEST_F(OcAuthCompatibilityTest, TestUnaryAuthMixOldAndNewVersion)
{
    auto requestToServer = [this](bool newVersion, int num, bool enableAuth) {
        TestReqPbV2 req;
        TestRspPbV1 rsp;
        req.set_filed1(num);
        req.set_field2("xxx");
        req.set_field3(num);
        req.set_auth_req(enableAuth);
        for (int i = 0; i < num; ++i) {
            req.add_filed4(std::to_string(i));
        }
        if (newVersion) {
            req.set_filed5(num);  // filed5 not exist in TestReqPbV1.
        }
        DS_ASSERT_OK(akSkManager_->GenerateSignature(req));
        if (!newVersion) {
            // old version will not carry these message.
            g_ReqAk.clear();
            g_ReqSignature.clear();
            g_ReqTimestamp = 0;
        }
        DS_ASSERT_OK(stub_->TestUnaryCompatibilityV2(req, rsp));
    };

    int num = 25;
    const int div = 2;
    const int div1 = 3;
    for (int i = 0; i < num; ++i) {
        requestToServer(i % div == 0, i, i % div1 == 0);
        requestToServer(i % div == 0, i, i % div1 == 0);
    }
}
}  // namespace st
}  // namespace datasystem
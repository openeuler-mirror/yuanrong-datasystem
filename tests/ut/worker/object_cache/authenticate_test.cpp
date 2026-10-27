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
 * Description: Test SpillRequestHandler and SpillFileManager.
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <atomic>
#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <string>

#include "ut/common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/iam/yuanrong_iam.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/protos/ut_object.pb.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/common/rpc/brpc_auth.h"
#include "datasystem/protos/share_memory.auth.pb.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/common/util/raii.h"
#include "../../../common/binmock/binmock.h"

DS_DECLARE_string(spill_directory);
DS_DECLARE_string(yuanrong_iam_url);

using namespace ::testing;

namespace datasystem {
namespace ut {

class AuthenticateTest : public CommonTest {
public:
    void SetUp() override
    {
        FLAGS_v = 1;
        ASSERT_TRUE(TimerQueue::GetInstance()->Initialize());
        datasystem::inject::Set("CurlHttpClient.Send.ReleaseCurlHandle", "100*call");
    }

    Status RegisterReqCheck(std::shared_ptr<AkSkManager> akSkManager, const std::string &akSkTenantId,
                            const SensitiveValue &token, std::string &tenantId)
    {
        tenantId.clear();
        proof_ = RpcRequestProof{};
        RegisterClientReqPb req;
        req.set_client_id("clientid");
        if (!token.Empty()) {
            req.set_token(token.GetData(), token.GetSize());
        }

        if (!akSkTenantId.empty()) {
            req.set_tenant_id(akSkTenantId);
            RETURN_IF_NOT_OK(GenerateProof(akSkManager, req));
        }
        return VerifySignature(akSkManager, req, tenantId);
    }

    template <typename Req>
    Status GenerateProof(std::shared_ptr<AkSkManager> manager, const Req &req)
    {
        proof_ = RpcRequestProof{};
        AkSkData key;
        RETURN_IF_NOT_OK(manager->SnapshotClientKey(key));
        if (key.NotComplete()) {
            return Status::OK();
        }
        proof_.captured = true;
        proof_.timestampUs = GetSystemClockTimeStampUs();
        proof_.accessKey = key.accessKey;
        butil::IOBuf bytes;
        bytes.append(req.SerializeAsString());
        RETURN_IF_NOT_OK(ComputeRpcRequestDigest(proof_.timestampUs, bytes, proof_.digest));
        return Signature::GetSignatureFromDigest(key, proof_.digest, proof_.signature);
    }

    template <typename Req>
    Status VerifySignature(std::shared_ptr<AkSkManager> akSkManager, Req &req, std::string &tenantId)
    {
        const bool tenantEnabled = TenantAuthManager::Instance()->AuthEnabled();
        req.clear_signature();
        req.clear_access_key();
        if (!(tenantEnabled && !req.token().empty()) && !FLAGS_skip_authenticate) {
            if (tenantEnabled && !proof_.signature.empty() && akSkManager->IsTenantAk(proof_.accessKey)) {
                std::string ignored;
                RETURN_IF_NOT_OK(TenantAuthManager::Instance()->TenantAkAuth(proof_.accessKey, "", ignored));
            }
            if (akSkManager->SystemAuthEnabled() || (tenantEnabled && !proof_.signature.empty())) {
                RETURN_IF_NOT_OK(VerifyRpcRequest(*akSkManager, &proof_));
                req.set_access_key(proof_.accessKey);
                req.set_signature(proof_.signature);
            }
        }
        return worker::AuthenticateRequest(akSkManager, req, req.tenant_id(), tenantId);
    }

protected:
    RpcRequestProof proof_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(AuthenticateTest, TestEnableAll)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey_, secretKey_);
    akSkManager->SetServerAkSk(AkSkType::TENANT, accessKey_, secretKey_);
    DS_ASSERT_OK(inject::Set("worker.auth", "return(token1,tenant1)"));
    TenantAuthManager::Instance()->Init(true);
    std::string akSkTenantId = "test_tenant";
    std::string token = "token1";
    std::string tenantId;
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
    ASSERT_EQ(tenantId, "tenant1");
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, akSkTenantId, "", tenantId));
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, "", token, tenantId));
    ASSERT_EQ(tenantId, "tenant1");
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, "", "", tenantId));
}

TEST_F(AuthenticateTest, RpcEntryKeepsBodyCredentialsUntrustedAndPreservesHeartbeatException)
{
    const bool skip = FLAGS_skip_authenticate;
    const auto iamUrl = FLAGS_yuanrong_iam_url;
    Raii restore([skip, iamUrl] {
        FLAGS_skip_authenticate = skip;
        FLAGS_yuanrong_iam_url = iamUrl;
        EXPECT_TRUE(TenantAuthManager::Instance()->Init(false).IsOk());
    });
    FLAGS_skip_authenticate = false;
    FLAGS_yuanrong_iam_url = "http://127.0.0.1:1";
    auto manager = std::make_shared<AkSkManager>();
    ASSERT_TRUE(TenantAuthManager::Instance()->Init(true, manager).IsOk());
    BrpcAuthService::Authenticate startup;
    const auto *direct = QueryAndGetReqPb::descriptor()->file()->FindServiceByName("WorkerOCService");
    ASSERT_TRUE(worker::CreateRpcAuthenticator(*direct, manager, startup).IsOk());
    QueryAndGetReqPb unsignedDirect;
    brpc::Controller unsignedCall;
    BrpcAuthService::Authenticate unsignedTenant;
    RegisterClientReqPb unsignedRegistration;
    const auto *workerService = RegisterClientReqPb::descriptor()->file()->FindServiceByName("WorkerService");
    ASSERT_TRUE(worker::CreateRpcAuthenticator(*workerService, manager, unsignedTenant).IsOk());
    EXPECT_FALSE(unsignedTenant(*workerService->FindMethodByName("RegisterClient"), unsignedCall,
                               unsignedRegistration).IsOk());
    EXPECT_TRUE(startup(*direct->FindMethodByName("QueryAndGet"), unsignedCall, unsignedDirect).IsOk());
    GetHashRingReqPb unsignedRing;
    EXPECT_TRUE(startup(*direct->FindMethodByName("GetHashRing"), unsignedCall, unsignedRing).IsOk());
    EXPECT_EQ(manager->VerifySignatureFromDigest("", 0, "", AkSkDigest{}).GetCode(), K_NOT_AUTHORIZED);
    ASSERT_TRUE(manager->SetServerAkSk(AkSkType::TENANT, "test-only-loaded-access",
                                       SensitiveValue("test-only-loaded-secret")).IsOk());
    EXPECT_EQ(startup(*direct->FindMethodByName("QueryAndGet"), unsignedCall, unsignedDirect).GetCode(),
              K_NOT_AUTHORIZED);
    EXPECT_EQ(startup(*direct->FindMethodByName("GetHashRing"), unsignedCall, unsignedRing).GetCode(),
              K_NOT_AUTHORIZED);
    EXPECT_FALSE(unsignedTenant(*workerService->FindMethodByName("RegisterClient"), unsignedCall,
                               unsignedRegistration).IsOk());
    ASSERT_TRUE(manager->SetServerAkSk(AkSkType::SYSTEM, "test-only-access",
                                       SensitiveValue("test-only-secret")).IsOk());
    ASSERT_TRUE(TenantAuthManager::Instance()->Init(true, manager).IsOk());
    RegisterClientReqPb request;
    request.set_access_key("test-only-access");
    request.set_signature("test-only-forged-body");
    std::string tenant;
    BrpcAuthService::Authenticate authenticate;
    const auto *service = RegisterClientReqPb::descriptor()->file()->FindServiceByName("WorkerService");
    ASSERT_TRUE(worker::CreateRpcAuthenticator(*service, manager, authenticate).IsOk());
    brpc::Controller controller;
    EXPECT_EQ(authenticate(*service->FindMethodByName("RegisterClient"), controller, request).GetCode(),
              K_NOT_AUTHORIZED);
    EXPECT_TRUE(request.access_key().empty());
    EXPECT_TRUE(request.signature().empty());
    HeartbeatReqPb heartbeat;
    heartbeat.set_client_id("test-only-missing-client");
    EXPECT_TRUE(authenticate(*service->FindMethodByName("Heartbeat"), controller, heartbeat).IsOk());
    EXPECT_EQ(worker::AuthenticateRequest(manager, heartbeat, "", tenant).GetCode(), K_NOT_AUTHORIZED);
    ASSERT_TRUE(TenantAuthManager::Instance()->Init(false).IsOk());
    EXPECT_EQ(worker::AuthenticateRequest(manager, heartbeat, "", tenant).GetCode(), K_NOT_AUTHORIZED);

    class TenantService final : public WorkerServiceAuth {
    public:
        explicit TenantService(std::shared_ptr<AkSkManager> manager) : manager_(std::move(manager)) {}
        void GetClientFd(google::protobuf::RpcController *controller, const GetClientFdReqPb *request,
                         GetClientFdRspPb *, google::protobuf::Closure *done) override
        {
            brpc::ClosureGuard complete(done);
            const auto &req = *request;
            std::string effective = req.tenant_id();
            if (effective.empty()) {
                bool exists = false;
                effective = worker::ClientManager::Instance().GetAuthTenantIdByClientId(
                    ClientKey::Intern(req.client_id()), exists);
            }
            std::string tenant;
            const auto status = worker::AuthenticateRequest(manager_, req, effective, tenant);
            if (status.IsError()) {
                controller->SetFailed(status.ToString());
            } else {
                ++admitted;
            }
        }
        std::atomic<unsigned> admitted{0};
    private:
        std::shared_ptr<AkSkManager> manager_;
    } native(manager);
    Raii releaseStubs([] { RELEASE_STUBS });
    BINEXPECT_CALL(&worker::ClientManager::GetAuthTenantIdByClientId, (_, _))
        .WillRepeatedly([](const ClientKey &, bool &exists) {
            exists = true;
            return std::string("test-only-session-tenant");
        });
    BINEXPECT_CALL(&TenantAuthManager::TenantAkAuth, (_, _, _))
        .WillRepeatedly([](const std::string &, const std::string &requested, std::string &tenant) {
            EXPECT_TRUE(requested == "test-only-session-tenant" || requested == "test-only-other-tenant");
            tenant = "test-only-session-tenant";
            return Status::OK();
        });
    BINEXPECT_CALL(&TenantAuthManager::TenantTokenAuth, (_, _))
        .WillRepeatedly([](const SensitiveValue &, std::string &tenant) {
            tenant = "test-only-session-tenant";
            return Status::OK();
        });
    ASSERT_TRUE(manager->SetServerAkSk(AkSkType::TENANT, "test-only-tenant-access",
                                       SensitiveValue("test-only-tenant-secret")).IsOk());
    ASSERT_TRUE(TenantAuthManager::Instance()->Init(true, manager).IsOk());
    ASSERT_TRUE(RegisterRpcAuthCapture().IsOk());
    BrpcAuthCapture capture;
    std::unique_ptr<BrpcAuthService> entry;
    ASSERT_TRUE(BrpcAuthService::Create(native, *WorkerServiceAuth::descriptor(), authenticate, entry).IsOk());
    brpc::Server server;
    ASSERT_EQ(server.AddService(entry.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    brpc::ServerOptions options;
    options.session_local_data_factory = &capture;
    ASSERT_EQ(server.Start("127.0.0.1:0", &options), 0);
    std::shared_ptr<const RpcMethodMap> methods;
    ASSERT_TRUE(RpcMethodMap::Create({{service, WorkerServiceAuth::descriptor()}}, methods).IsOk());
    auto provider = std::make_shared<Signature>("test-only-tenant-access", SensitiveValue("test-only-tenant-secret"));
    BrpcChannelConfig config;
    config.endpoint = butil::endpoint2str(server.listen_address()).c_str();
    config.timeout_ms = 1000;
    config.max_retry = 0;
    std::unique_ptr<brpc::Channel> channel;
    ASSERT_TRUE(CreateBrpcAuthChannel(config, {provider, methods}, channel).IsOk());
    GetClientFdReqPb req;
    req.set_client_id("test-only-session");
    GetClientFdRspPb rsp;
    for (unsigned i = 0; i < 4; ++i) {
        brpc::Controller call;
        if (i == 1) {
            req.set_tenant_id("test-only-other-tenant");
        } else if (i == 2) {
            req.clear_tenant_id();
            ASSERT_TRUE(provider->SetClientAkSk("test-only-tenant-access",
                                                 SensitiveValue("test-only-invalid-secret")).IsOk());
        } else if (i == 3) {
            constexpr size_t tokenSize = 256;
            req.set_token(std::string(tokenSize, 't'));
        }
        channel->CallMethod(service->FindMethodByName("GetClientFd"), &call, &req, &rsp, nullptr);
        EXPECT_EQ(call.Failed(), i == 1 || i == 2);
    }
    EXPECT_EQ(native.admitted.load(), 2);
    server.Stop(0);
    server.Join();
}

TEST_F(AuthenticateTest, TestEnableAKSK)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey_, secretKey_);
    akSkManager->SetServerAkSk(AkSkType::TENANT, accessKey_, secretKey_);
    DS_ASSERT_OK(inject::Set("worker.auth", "return(token1,tenant1)"));
    TenantAuthManager::Instance()->Init(false);

    std::string akSkTenantId = "test_tenant";
    std::string token = "token1";
    std::string tenantId;

    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, "", tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, "", token, tenantId));
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, "", "", tenantId));
}

TEST_F(AuthenticateTest, TestSystemAKSK)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey_, secretKey_);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, accessKey_, secretKey_);
    DS_ASSERT_OK(inject::Set("worker.auth", "return(token1,tenant1)"));
    TenantAuthManager::Instance()->Init(true);

    std::string akSkTenantId = "test_tenant";
    std::string token = "token1";
    std::string tenantId;

    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
    ASSERT_EQ(tenantId, "tenant1");
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, "", tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, "", token, tenantId));
    ASSERT_EQ(tenantId, "tenant1");
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, "", "", tenantId));
}

TEST_F(AuthenticateTest, TestDisableAll)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey_, secretKey_);

    DS_ASSERT_OK(inject::Set("worker.auth", "return(token1,tenant1)"));
    TenantAuthManager::Instance()->Init(false);

    std::string akSkTenantId = "test_tenant";
    std::string token = "token1";
    std::string tenantId;

    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, "", tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, "", token, tenantId));
    ASSERT_EQ(tenantId, "");
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, "", "", tenantId));
    ASSERT_EQ(tenantId, "");
}

TEST_F(AuthenticateTest, TestProtobufFilterSensitiveInfo)
{
    PublishReqPb req;
    req.set_client_id("clientId");
    req.set_token("1111111111111");
    req.set_signature("1111111111111");
    std::string filterReq = LogHelper::IgnoreSensitive(req);
    auto reqPos = filterReq.find("token: \"***\"");
    ASSERT_TRUE(reqPos != std::string::npos);
    reqPos = filterReq.find("signature: \"***\"");
    ASSERT_TRUE(reqPos != std::string::npos);

    auto originalReq = req.ShortDebugString().find("token: \"1111111111111\"");
    ASSERT_TRUE(originalReq != std::string::npos);

    PublishRspPb resp;
    std::string filterResp = LogHelper::IgnoreSensitive(resp);
    auto respPos = filterResp.find("**");
    ASSERT_TRUE(respPos == std::string::npos);
}

TEST_F(AuthenticateTest, TestEnableInYuanrong)
{
    const std::string accessKey = "accesskey";
    const std::string secretKey = "secretkey";
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey, secretKey);
    akSkManager->SetServerAkSk(AkSkType::TENANT, accessKey_, secretKey_);
    DS_ASSERT_OK(inject::Set("worker.akauth", "return(accesskey,secretkey,test_tenant)"));
    TenantAuthManager::Instance()->Init(true, akSkManager);
    std::string akSkTenantId = "test_tenant";
    std::string tenantId;
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, "tenant1", "", tenantId));
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, "", tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, "", "", tenantId));
}

TEST_F(AuthenticateTest, TestSystemRoleAuth)
{
    Raii releaseStubs([] { RELEASE_STUBS });
    BINEXPECT_CALL(&CurlHttpClient::SendRequest, (_, _, _))
        .WillOnce([](const std::shared_ptr<HttpRequest> &req, std::shared_ptr<HttpResponse> &rsp, CURL *curlHandle) {
            (void)req;
            (void)curlHandle;
            rsp->SetStatus(HTTP_OK);
            std::string rspStr = R"({
                "accessKey": "test-only-system-role-access",
                "dataKey": "dataKey",
                "expiredTimeSpan": "0",
                "expiredTimeStamp": "0",
                "role": "system",
                "secretKey": "test-only-system-role-secret",
                "tenantID": "faas"
            })";
            rsp->GetBody()->write(rspStr.c_str(), rspStr.length());
            return Status::OK();
        });
    FLAGS_yuanrong_iam_url = "http://iam-server.default.svc.cluster.local:31218";
    const std::string accessKey = "test-only-system-role-access";
    const std::string secretKey = "test-only-system-role-secret";
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey, secretKey);
    akSkManager->SetServerAkSk(AkSkType::TENANT, accessKey_, secretKey_);
    TenantAuthManager::Instance()->Init(true, akSkManager);
    std::string reqTenantId = "tenant1";
    std::string tenantId;
    // The system role does not verify tenantId.
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, reqTenantId, "", tenantId));
    ASSERT_EQ(tenantId, reqTenantId);
    // Hit clientAkTable_
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, reqTenantId, "", tenantId));
    ASSERT_EQ(tenantId, reqTenantId);
}

TEST_F(AuthenticateTest, TestOnlySystemAKSK)
{
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey_, secretKey_);
    akSkManager->SetServerAkSk(AkSkType::SYSTEM, accessKey_, secretKey_);
    DS_ASSERT_OK(inject::Set("worker.auth", "return(token1,tenant1)"));
    TenantAuthManager::Instance()->Init(false);

    std::string akSkTenantId = "test_tenant";
    std::string token = "token1";
    std::string tenantId;

    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, "", token, tenantId));
    DS_ASSERT_OK(RegisterReqCheck(akSkManager, akSkTenantId, "", tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
    akSkManager->SetClientAkSk("accesskey", "secretKey");
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
    akSkManager->SetClientAkSk("", "");
    DS_ASSERT_NOT_OK(RegisterReqCheck(akSkManager, akSkTenantId, token, tenantId));
}

TEST_F(AuthenticateTest, TestAKSKCompatibility)
{
    const std::string accessKey = "test-only-compat-access";
    const std::string secretKey = "test-only-compat-secret";
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(accessKey, secretKey);
    akSkManager->SetServerAkSk(AkSkType::TENANT, accessKey_, secretKey_);
    DS_ASSERT_OK(inject::Set("worker.akauth", "return(test-only-compat-access,test-only-compat-secret,test_tenant)"));
    TenantAuthManager::Instance()->Init(true, akSkManager);
    std::string akSkTenantId = "test_tenant";

    TestReqPbV2 req;
    req.set_filed1(1); // set filed1 as 1.
    req.set_field2("xxx");
    req.set_field3(2);  // set filed3 as 2.
    req.set_tenant_id(akSkTenantId);
    int count = 3;
    for (int i = 0; i < count; ++i) {
        req.mutable_filed4()->Add(std::to_string(i));
    }
    req.set_filed5(1);
    DS_ASSERT_OK(GenerateProof(akSkManager, req));

    TestReqPbV1 req1;
    ASSERT_TRUE(req1.ParseFromString(req.SerializeAsString()));
    std::string tenantId;
    DS_ASSERT_OK(VerifySignature(akSkManager, req1, tenantId));
    ASSERT_EQ(tenantId, akSkTenantId);
}
}  // namespace ut
}  // namespace datasystem

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
#include <string>

#include "common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/protos/ut_object.pb.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/authenticate.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/common/util/raii.h"
#include "../../../common/binmock/binmock.h"

DS_DECLARE_string(spill_directory);

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
        RegisterClientReqPb req;
        req.set_client_id("clientid");
        if (!token.Empty()) {
            req.set_token(token.GetData(), token.GetSize());
        }

        if (!akSkTenantId.empty()) {
            req.set_tenant_id(akSkTenantId);
            akSkManager->GenerateSignature(req);
        }
        auto serializedStr = req.SerializeAsString();
        RETURN_IF_NOT_OK(g_SerializedMessage.CopyBuffer(serializedStr.c_str(), serializedStr.size()));

        return worker::AuthenticateRequest(akSkManager, req, req.tenant_id(), tenantId);
    }

    template <typename Req>
    Status GenerateSignature(std::shared_ptr<AkSkManager> akSkManager, Req &req)
    {
        RETURN_IF_NOT_OK(akSkManager->GenerateSignature(req));
        auto serializedStr = req.SerializeAsString();
        return g_SerializedMessage.CopyBuffer(serializedStr.c_str(), serializedStr.size());
    }

    template <typename Req>
    Status VerifySignature(std::shared_ptr<AkSkManager> akSkManager, Req &req, std::string &tenantId)
    {
        return worker::AuthenticateRequest(akSkManager, req, req.tenant_id(), tenantId);
    }

protected:
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
}  // namespace ut
}  // namespace datasystem
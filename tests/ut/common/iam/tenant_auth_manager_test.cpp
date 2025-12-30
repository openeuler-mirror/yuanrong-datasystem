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
 * Description: TenantAuth manager test.
 */
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>
#include <google/protobuf/repeated_field.h>

#include "common.h"
#include "datasystem/common/httpclient/http_client.h"
#include "datasystem/common/iam/iam.h"
#include "datasystem/common/iam/yuanrong_iam.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

DS_DECLARE_string(iam_kit);
DS_DECLARE_string(yuanrong_iam_url);
DS_DECLARE_string(yuanrong_iam_ca);
DS_DECLARE_string(yuanrong_iam_cert);
DS_DECLARE_string(yuanrong_iam_key);

namespace datasystem {
namespace ut {
constexpr int HTTP_STATUS_OK = 200;
const std::string TENANT_ID = "tenantId";
const std::string EXPIRED_TIME = "86400";
const std::string TOKEN =
    "eyJraWQiOiI2VnFua3NIemg2WnNVT0NMbDVNMjdPeUxCNG9xOUMxbCIsInR5cCI6IkpXVCIsImFsZyI6IkhTMjU2In0."
    "eyJzdWIiOiIxMDAwMDUxMjkwMTQwNTkzNDA4IiwiZG4iOjIsImNsaWVudF90eXBlIjoxLCJleHAiOjE2NzA1NzQ1NTEsImlhdCI6MTY3MDQwMTc1MX"
    "0.JFZcvhPc1JrG8dSge2YYxTalaV4zwkqJwkUgazyU28Y";
const std::string ACCESSKEY = "QTWAOYTTINDUT2QVKYUC";
const std::string SECRETKEY = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
const std::string IAM_ROLE = "normal";

void SendSuccess(std::shared_ptr<HttpResponse> &response)
{
    response->AddHeader(RESPONSE_EXPIRED_TIME, EXPIRED_TIME);
    response->AddHeader(RESPONSE_TENANTID, TENANT_ID);
    response->SetStatus(HTTP_STATUS_OK);
}

void SendAkSuccess(std::shared_ptr<HttpResponse> &response)
{
    nlohmann::json body;
    body[RESPBODY_EXPIRED_TIME] = EXPIRED_TIME;
    body[RESPBODY_TENANTID] = TENANT_ID;
    body[RESPBODY_SECRETKEY] = SECRETKEY;
    body[RESPBODY_ROLE] = IAM_ROLE;
    std::string json_str = body.dump(4);
    std::shared_ptr<std::iostream> body_stream = response->GetBody();
    body_stream->seekp(std::ios::beg);
    *body_stream << json_str;
    response->SetBody(body_stream);
    response->SetStatus(HTTP_STATUS_OK);
}

void SendFailed(std::shared_ptr<HttpResponse> &response)
{
    response->AddHeader(RESPONSE_EXPIRED_TIME, EXPIRED_TIME);
    response->SetStatus(HTTP_STATUS_OK);
}

void SendFailed1(std::shared_ptr<HttpResponse> &response)
{
    response->AddHeader(RESPONSE_TENANTID, TENANT_ID);
    response->SetStatus(HTTP_STATUS_OK);
}
class MockYuanRongIAMCurlClient : public CurlHttpClient {
public:
    MockYuanRongIAMCurlClient() = default;
    ~MockYuanRongIAMCurlClient() = default;
    Status TslSend(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response,
                   const TslParam &param)
    {
        (void)request;
        (void)response;
        (void)param;
        CHECK_FAIL_RETURN_STATUS(sendFunc_ != nullptr, K_RUNTIME_ERROR, "func is nullptr");
        sendFunc_(response);
        return Status::OK();
    }

    Status Send(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response)
    {
        (void)request;
        (void)response;
        CHECK_FAIL_RETURN_STATUS(sendFunc_ != nullptr, K_RUNTIME_ERROR, "func is nullptr");
        sendFunc_(response);
        return Status::OK();
    }

    void SendFunc(std::function<void(std::shared_ptr<HttpResponse> &response)> sendFunc)
    {
        sendFunc_ = sendFunc;
    }

protected:
    std::function<void(std::shared_ptr<HttpResponse> &response)> sendFunc_ = nullptr;
};

class MockYuanRongIAM : public YuanRongIAM {
public:
    MockYuanRongIAM(std::shared_ptr<AkSkManager> akSkManager = nullptr) : YuanRongIAM(std::move(akSkManager))
    {
    }

    ~MockYuanRongIAM() = default;

    void ResetCurlClient(std::shared_ptr<MockYuanRongIAMCurlClient> cleint)
    {
        httpClient_ = cleint;
    }
};

class TenantAuthManagerTest : public CommonTest {
public:
    void SetUp() override
    {
        FLAGS_v = 1;
        ASSERT_TRUE(TimerQueue::GetInstance()->Initialize());
    }
};

class MockTenantAuthManager : public TenantAuthManager {
public:
    static MockTenantAuthManager *Instance()
    {
        static MockTenantAuthManager tenantAuthManager;
        return &tenantAuthManager;
    }

    ~MockTenantAuthManager() = default;

    void ResetIAM(std::shared_ptr<MockYuanRongIAM> iam)
    {
        iamAuth_ = iam;
    }
};

TEST_F(TenantAuthManagerTest, YuanIamServerWithToken)
{
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(TOKEN.c_str(), TOKEN.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(TOKEN.c_str(), TOKEN.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(TOKEN.c_str(), TOKEN.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true));
    auto mockIam = std::make_shared<MockYuanRongIAM>();
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    SensitiveValue token(TOKEN);
    std::string tenantId;
    errno = EOK;
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->TenantTokenAuth(token, tenantId));
    ASSERT_EQ(tenantId, TENANT_ID);
}

TEST_F(TenantAuthManagerTest, YuanIamServerWithAk)
{
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(ACCESSKEY, SECRETKEY);
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(ACCESSKEY.c_str(), ACCESSKEY.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(ACCESSKEY.c_str(), ACCESSKEY.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(ACCESSKEY.c_str(), ACCESSKEY.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true, akSkManager));
    auto mockIam = std::make_shared<MockYuanRongIAM>(akSkManager);
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendAkSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    std::string accesskey(ACCESSKEY);
    std::string tenantId;
    errno = EOK;
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->TenantAkAuth(accesskey, TENANT_ID, tenantId));
    ASSERT_EQ(tenantId, TENANT_ID);
}

TEST_F(TenantAuthManagerTest, YuanIamServerWithoutTls)
{
    errno = EOK;
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "";
    FLAGS_yuanrong_iam_cert = "";
    FLAGS_yuanrong_iam_key = "";
    FLAGS_iam_kit = "yuanrong_iam";
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true));
    auto mockIam = std::make_shared<MockYuanRongIAM>();
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    SensitiveValue token(TOKEN);
    std::string tenantId;
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->TenantTokenAuth(token, tenantId));
    ASSERT_EQ(tenantId, TENANT_ID);
}

TEST_F(TenantAuthManagerTest, YuanIamServerWithoutTls1)
{
    errno = EOK;
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "";
    FLAGS_yuanrong_iam_cert = "";
    FLAGS_yuanrong_iam_key = "";
    FLAGS_iam_kit = "yuanrong_iam";
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(ACCESSKEY, SECRETKEY);
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true, akSkManager));
    auto mockIam = std::make_shared<MockYuanRongIAM>(akSkManager);
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendAkSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    std::string accesskey(ACCESSKEY);
    std::string tenantId;
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->TenantAkAuth(accesskey, TENANT_ID, tenantId));
    ASSERT_EQ(tenantId, TENANT_ID);
}

TEST_F(TenantAuthManagerTest, YuanIamServerFailed)
{
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(TOKEN.c_str(), TOKEN.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(TOKEN.c_str(), TOKEN.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(TOKEN.c_str(), TOKEN.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true));
    auto mockIam = std::make_shared<MockYuanRongIAM>();
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendFailed);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    SensitiveValue token(TOKEN);
    std::string tenantId;
    DS_ASSERT_NOT_OK(MockTenantAuthManager::Instance()->TenantTokenAuth(token, tenantId));
}

TEST_F(TenantAuthManagerTest, YuanIamServerFailed1)
{
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(TOKEN.c_str(), TOKEN.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(TOKEN.c_str(), TOKEN.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(TOKEN.c_str(), TOKEN.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true));
    auto mockIam = std::make_shared<MockYuanRongIAM>();
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendFailed1);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    SensitiveValue token(TOKEN);
    std::string tenantId;
    DS_ASSERT_NOT_OK(MockTenantAuthManager::Instance()->TenantTokenAuth(token, tenantId));
}

TEST_F(TenantAuthManagerTest, YuanIamServerRetry)
{
    datasystem::inject::Set("yuanrongIam.auth.failed", "3*call()");
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(TOKEN.c_str(), TOKEN.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(TOKEN.c_str(), TOKEN.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(TOKEN.c_str(), TOKEN.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true));
    auto mockIam = std::make_shared<MockYuanRongIAM>();
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    SensitiveValue token(TOKEN);
    std::string tenantId;
    errno = EOK;
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->TenantTokenAuth(token, tenantId));
}

TEST_F(TenantAuthManagerTest, YuanIamServerWithAkRetry)
{
    datasystem::inject::Set("yuanrongIam.auth.failed", "3*call()");
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    auto akSkManager = std::make_shared<AkSkManager>();
    akSkManager->SetClientAkSk(ACCESSKEY, SECRETKEY);
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(ACCESSKEY.c_str(), ACCESSKEY.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(ACCESSKEY.c_str(), ACCESSKEY.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(ACCESSKEY.c_str(), ACCESSKEY.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true, akSkManager));
    auto mockIam = std::make_shared<MockYuanRongIAM>(akSkManager);
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendAkSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    std::string accesskey(ACCESSKEY);
    std::string tenantId;
    errno = EOK;
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->TenantAkAuth(accesskey, TENANT_ID, tenantId));
    ASSERT_EQ(tenantId, TENANT_ID);
}

TEST_F(TenantAuthManagerTest, YuanIamServerRetryFailed)
{
    datasystem::inject::Set("yuanrongIam.auth.failed", "10*call()");
    FLAGS_yuanrong_iam_url = "http://wqeqweqwewq.com";
    FLAGS_yuanrong_iam_ca = "./ca";
    FLAGS_yuanrong_iam_cert = "./cert.crt";
    FLAGS_yuanrong_iam_key = "./key";
    FLAGS_iam_kit = "yuanrong_iam";
    std::ofstream ofs(FLAGS_yuanrong_iam_ca);
    ofs.write(TOKEN.c_str(), TOKEN.size());
    ofs.close();
    std::ofstream ofs1(FLAGS_yuanrong_iam_cert);
    ofs1.write(TOKEN.c_str(), TOKEN.size());
    ofs1.close();
    std::ofstream ofs2(FLAGS_yuanrong_iam_key);
    ofs2.write(TOKEN.c_str(), TOKEN.size());
    ofs2.close();
    DS_ASSERT_OK(MockTenantAuthManager::Instance()->Init(true));
    auto mockIam = std::make_shared<MockYuanRongIAM>();
    auto client = std::make_shared<MockYuanRongIAMCurlClient>();
    client->SendFunc(SendSuccess);
    mockIam->ResetCurlClient(client);
    MockTenantAuthManager::Instance()->ResetIAM(mockIam);
    SensitiveValue token(TOKEN);
    std::string tenantId;
    errno = EOK;
    DS_ASSERT_NOT_OK(MockTenantAuthManager::Instance()->TenantTokenAuth(token, tenantId));
}
}  // namespace ut
}  // namespace datasystem
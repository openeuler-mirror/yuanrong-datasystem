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
 * Description: OBS client test.
 */

#include <fstream>

#include "ut/common.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/l2cache/obs_client/obs_client.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/common/util/random_data.h"

#include <eSDKOBS.h>

DS_DECLARE_string(encrypt_kit);
DS_DECLARE_bool(enable_cloud_service_token_rotation);
DS_DECLARE_string(log_dir);

const int DEFAULT_TTL_SECOND = 86400;
const std::string DEFAULT_PROJECT_ID = "mock_projectID";
const std::string DEFAULT_REGION_ID = "mock_regionID";
const std::string DEFAULT_IDENTITY_PROVIDER = "mock_provider";
const std::string DEFAULT_IAM_HOST_NAME = "iam.cn-north-7.mycloud.com";

namespace datasystem {
namespace ut {
class ObsClientTest : public CommonTest {
};

class ObsClientTokenRotationTest : public CommonTest {
public:
    void SetUp() override;

    static void InitializeCsmsToken(int initTimes = 1);

    void TestTokenRotation(size_t checkOriginTokenNum, bool wantFirstUpdateFailed);

    struct RotationTokenConf {
        std::string projectID;
        std::string identityProvider;
        std::string iamHostName;
        bool enableTokenByAgency{};
        int tokenTTLSeconds{};
        std::string tokenAgencyName;
        std::string tokenAgencyDomain;
        std::string regionID;
    };

protected:
    RandomData randData_;
    const std::string obsEndpoint_ = "ddl.test.com:19000";
    const std::string bucket_ = "test";
    const std::string tokenRotationConfig_ = "TOKEN_ROTATION_CONFIG";
    RotationTokenConf rotationConf_;
    std::string rotationConfigStr_;
    size_t timeout_ = 60000;
    std::unique_ptr<ObsClient> client_;
};

void ObsClientTokenRotationTest::SetUp()
{
    FLAGS_enable_cloud_service_token_rotation = true;
    rotationConfigStr_ = FormatString(
        R"({"projectID": "%s", "regionID": "%s", "identityProvider": "%s", "iamHostName": "%s", "tokenTTLSeconds": %d,
            "enableTokenByAgency": false, "tokenAgencyName": "", "tokenAgencyDomain": ""})",
        DEFAULT_PROJECT_ID, DEFAULT_REGION_ID, DEFAULT_IDENTITY_PROVIDER, DEFAULT_IAM_HOST_NAME, DEFAULT_TTL_SECOND);
    client_ = std::make_unique<ObsClient>(obsEndpoint_, bucket_);
    LOG(INFO) << "Origin rotationConfigStr_: " << rotationConfigStr_;
}

void ObsClientTokenRotationTest::InitializeCsmsToken(int initTimes)
{
    std::string timeString = std::to_string(initTimes);
    std::string csmsTokenPath = FLAGS_log_dir + "/csms-token";
    std::ofstream ofs(csmsTokenPath);
    ASSERT_TRUE(ofs.is_open());
    ofs << "csms-token test.";
    ofs.close();
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.ReadCSMSToken.readTestOidcToken",
                                         FormatString("%s*call(%s)", timeString, csmsTokenPath)));
}

void ObsClientTokenRotationTest::TestTokenRotation(size_t checkOriginTokenNum, bool wantFirstUpdateFailed)
{
    int initTimes = 5;
    InitializeCsmsToken(initTimes);
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.TokenRotationInit.FirstInitCredentialInfo",
                                         "1*return(J76L6Z7FS87RMKBXYZTV,J8knSPHNhJGKvkknkrpoTEk4S99sHu85cpEpN6sA)"));
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), rotationConfigStr_.c_str(), 1), 0);
    const size_t TEST_INTERVAL_SEC = 2;
    const size_t TEST_RETRY_WAIT_SEC = 2;
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.StartTokenRotation.SetRotationInterval",
                                         FormatString("1*call(%d,%d)", TEST_INTERVAL_SEC, TEST_RETRY_WAIT_SEC)));
    DS_ASSERT_OK(datasystem::inject::Set(
        "ObsClient.CheckValidRotationToken.VerifyOriginCredential",
        FormatString("%d*return(J76L6Z7FS87RMKBXYZTV,J8knSPHNhJGKvkknkrpoTEk4S99sHu85cpEpN6sA)", checkOriginTokenNum)));
    DS_ASSERT_OK(client_->Init());
    DS_ASSERT_OK(client_->CheckValidRotationToken());
    if (wantFirstUpdateFailed) {
        std::this_thread::sleep_for(std::chrono::seconds(TEST_INTERVAL_SEC + TEST_RETRY_WAIT_SEC + 1));
        DS_ASSERT_OK(client_->CheckValidRotationToken());
    }
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.TokenRotationInit.UpdateInitCredentialInfo",
                                         "1*return(G0MU4KFBBEM3FQZYXKAE,B58XMLxVvjpy8qDxgQwYMzsL4VMtp9rzqt4TYkkF)"));
    int waitCount = 3;
    do {
        std::this_thread::sleep_for(std::chrono::seconds(TEST_RETRY_WAIT_SEC));
        waitCount--;
    } while (client_->CheckValidRotationToken().IsOk() && waitCount > 0);
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.CheckValidRotationToken.VerifyUpdateCredential",
                                         "1*return(G0MU4KFBBEM3FQZYXKAE,B58XMLxVvjpy8qDxgQwYMzsL4VMtp9rzqt4TYkkF)"));
    DS_ASSERT_OK(client_->CheckValidRotationToken());
}

TEST_F(ObsClientTokenRotationTest, DISABLED_TestObsClientInitSuccess)
{
    InitializeCsmsToken();
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.TokenRotationInit.FirstInitCredentialInfo",
                                         "1*return(J76L6Z7FS87RMKBXYZTV,J8knSPHNhJGKvkknkrpoTEk4S99sHu85cpEpN6sA)"));
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), rotationConfigStr_.c_str(), 1), 0);
    DS_ASSERT_OK(client_->Init());
}

TEST_F(ObsClientTokenRotationTest, DISABLED_TestObsClientReadCsmsTokenError)
{
    std::string csmsTokenPath = FLAGS_log_dir + "/csms-token";
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.ReadCSMSToken.readTestOidcToken",
                                         FormatString("1*call(%s)", csmsTokenPath)));
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.TokenRotationInit.FirstInitCredentialInfo",
                                         "1*return(J76L6Z7FS87RMKBXYZTV,J8knSPHNhJGKvkknkrpoTEk4S99sHu85cpEpN6sA)"));
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), rotationConfigStr_.c_str(), 1), 0);
    DS_ASSERT_NOT_OK(client_->Init());
}

TEST_F(ObsClientTokenRotationTest, DISABLED_TestObsClientReadConfigEnvError)
{
    int initTimes = 5;
    InitializeCsmsToken(initTimes);
    DS_ASSERT_OK(datasystem::inject::Set("ObsClient.TokenRotationInit.FirstInitCredentialInfo",
                                         "5*return(J76L6Z7FS87RMKBXYZTV,J8knSPHNhJGKvkknkrpoTEk4S99sHu85cpEpN6sA)"));

    const size_t QUOTE_LENGTH = 2;
    std::string errorProjectType = rotationConfigStr_;
    errorProjectType.replace(errorProjectType.find(DEFAULT_PROJECT_ID) - 1, DEFAULT_PROJECT_ID.length() + QUOTE_LENGTH,
                             "256");
    LOG(INFO) << "errorProjectType string: " << errorProjectType;
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), errorProjectType.c_str(), 1), 0);
    DS_ASSERT_NOT_OK(client_->Init());

    const size_t MAX_STRING_LEN = 128;
    std::string exceedMaxLenProjectId(MAX_STRING_LEN + 1, 'a');
    exceedMaxLenProjectId = "\"" + exceedMaxLenProjectId + "\"";
    std::string exceedLimitConfig = rotationConfigStr_;
    exceedLimitConfig.replace(exceedLimitConfig.find(DEFAULT_PROJECT_ID) - 1,
                              DEFAULT_PROJECT_ID.length() + QUOTE_LENGTH, exceedMaxLenProjectId);
    LOG(INFO) << "exceedLimitConfig string: " << exceedLimitConfig;
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), exceedLimitConfig.c_str(), 1), 0);
    DS_ASSERT_NOT_OK(client_->Init());

    std::string ttlString = std::to_string(DEFAULT_TTL_SECOND);
    std::string errorTtlType = rotationConfigStr_;
    errorTtlType.replace(errorTtlType.find(ttlString), ttlString.length(), "\"86400\"");
    LOG(INFO) << "errorTtlType string: " << errorTtlType;
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), errorTtlType.c_str(), 1), 0);
    DS_ASSERT_NOT_OK(client_->Init());

    const size_t BELOW_MIN_SEC = 86401;
    std::string belowTtlString = std::to_string(BELOW_MIN_SEC);
    std::string belowTtlLimit = rotationConfigStr_;
    belowTtlLimit.replace(belowTtlLimit.find(ttlString), ttlString.length(), belowTtlString);
    LOG(INFO) << "belowTtlLimit string: " << belowTtlLimit;
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), belowTtlLimit.c_str(), 1), 0);
    DS_ASSERT_NOT_OK(client_->Init());

    std::string falseString = "false";
    std::string errorBooleanType = rotationConfigStr_;
    errorBooleanType.replace(errorBooleanType.find(falseString), falseString.length(), "\"false\"");
    LOG(INFO) << "errorBooleanType string: " << errorBooleanType;
    ASSERT_EQ(setenv(tokenRotationConfig_.c_str(), errorBooleanType.c_str(), 1), 0);
    DS_ASSERT_NOT_OK(client_->Init());
}

TEST_F(ObsClientTokenRotationTest, DISABLED_TestObsClientSuccessUpdateToken)
{
    size_t checkOriginTokenNum = 2;
    bool wantFirstUpdateFailed = false;
    TestTokenRotation(checkOriginTokenNum, wantFirstUpdateFailed);
}

TEST_F(ObsClientTokenRotationTest, DISABLED_TestObsClientFirstFailedAndFinalSuccessUpdateToken)
{
    size_t checkOriginTokenNum = 3;
    bool wantFirstUpdateFailed = true;
    TestTokenRotation(checkOriginTokenNum, wantFirstUpdateFailed);
}
}  // namespace ut
}  // namespace datasystem
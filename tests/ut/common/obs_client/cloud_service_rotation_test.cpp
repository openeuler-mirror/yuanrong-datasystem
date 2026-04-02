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
 * Description: Cloud service rotation test.
 */

#include "ut/common.h"

#include <memory>

#include "datasystem/common/l2cache/obs_client/cloud_service_rotation.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util//format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/inject/inject_point.h"

const int DEFAULT_TTL_SECOND = 86400;
const std::string DEFAULT_PROJECT_ID = "mock_projectID";
const std::string DEFAULT_REGION_ID = "mock_regionID";
const std::string DEFAULT_IDENTITY_PROVIDER = "mock_provider";
const std::string DEFAULT_IAM_HOST_NAME = "iam.cn-north-7.mycloud.com";

namespace datasystem {
namespace ut {
class CloudServiceRotationTest : public CommonTest {
public:
    void SetUp() override;

protected:
    CredentialInfo credentialInfo_;
    CCMSTokenConf ccmsTokenConf_;
    std::string rotationConfigStr_;
    std::unique_ptr<CCMSRotationAccessToken> ccmsRotationTokenRequest_;
};

void CloudServiceRotationTest::SetUp()
{
    ccmsTokenConf_.projectID = DEFAULT_PROJECT_ID;
    ccmsTokenConf_.regionID = DEFAULT_REGION_ID;
    ccmsTokenConf_.identityProvider = DEFAULT_IDENTITY_PROVIDER;
    ccmsTokenConf_.iamHostName = DEFAULT_IAM_HOST_NAME;
    ccmsTokenConf_.tokenTTLSeconds = DEFAULT_TTL_SECOND;
    ccmsTokenConf_.enableTokenByAgency = false;
    ccmsRotationTokenRequest_ = std::make_unique<CCMSRotationAccessToken>(ccmsTokenConf_);
}

TEST_F(CloudServiceRotationTest, TestInvalidRotationRequestOption)
{
    DS_ASSERT_OK(datasystem::inject::Set("CCMSRotationAccessToken.UpdateAccessToken.ErrorAction", "1*call()"));
    DS_ASSERT_NOT_OK(ccmsRotationTokenRequest_->UpdateAccessToken(credentialInfo_));
}

TEST_F(CloudServiceRotationTest, TestInvalidJsonString)
{
    DS_ASSERT_OK(
        datasystem::inject::Set("CCMSRotationRequest.BuildCCMSCommonRequest.ErrorJsonString", "1*return(ErrorString)"));
    DS_ASSERT_NOT_OK(ccmsRotationTokenRequest_->UpdateAccessToken(credentialInfo_));
}
}  // namespace ut
}  // namespace datasystem
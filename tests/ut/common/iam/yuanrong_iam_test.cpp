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
 * Description: yuanrong iam test.
 */

#include <memory>

#include "ut/common.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/httpclient/http_message.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/iam/yuanrong_iam.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"

namespace datasystem {
namespace ut {

class YuanRongIAMTestHelper : public YuanRongIAM {
public:
    using YuanRongIAM::SetAuthorization;

    YuanRongIAMTestHelper(std::shared_ptr<AkSkManager> akSkManager) : YuanRongIAM(std::move(akSkManager))
    {
    }
};

class YuanRongIAMTest : public CommonTest {
public:
    void SetUp() override
    {
        Logging::GetInstance()->Start("ds_llt", true, 1);
        akSkManager_ = std::make_shared<AkSkManager>();
        akSkManager_->SetClientAkSk(accessKey_, secretKey_);
        handler_ = std::make_shared<YuanRongIAMTestHelper>(akSkManager_);
    };

protected:
    std::shared_ptr<YuanRongIAMTestHelper> handler_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
};

TEST_F(YuanRongIAMTest, AuthorizationTest)
{
    datasystem::inject::Set("YuanRongIAM.Authorization.SetTimestamp", "call(20241228T125807Z)");
    auto req = std::make_shared<HttpRequest>();
    req->SetMethod(HttpMethod::GET);
    req->SetUrl("https://www.example.com/v1/77b6a44cba5143ab91d13ab9a8ff44fd/vpcs/");
    req->AddQueryParam("limit", "2");
    req->AddQueryParam("marker", "13551d6b-755d-4757-b956-536f674975c0");
    req->AddHeader("Host", "service.region.example.com");
    req->AddHeader("Content-Type", "application/json");
    DS_ASSERT_OK(handler_->SetAuthorization(req));
    std::string expectAuth =
        "HmacSha256 "
        "timestamp=20241228T125807Z,ak=QTWAOYTTINDUT2QVKYUC,signature="
        "cd8843cef767c77cf1b51e33f4e6e82f24c2bc1c097e16766380db10e7938871";
    EXPECT_EQ(req->GetHeader("X-Signature"), expectAuth);
}
}  // namespace ut
}  // namespace datasystem
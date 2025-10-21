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
 * Description: AK/SK test.
 */
#include <gtest/gtest.h>
#include <memory>
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/aes/aes_impl.h"
#include "common.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
namespace ut {
class AkSkManagerHelper : public AkSkManager {
public:
    using datasystem::AkSkManager::ConstructAesAndDecrypt;
};

class AkSkTest : public CommonTest {
public:
    void SetUp() override
    {
        akSkManager_ = std::make_shared<AkSkManagerHelper>();
    }

    void VerifyAkSk(const std::string &clientAk, const std::string &clientSk, const std::string &serverAk,
                    const std::string &serverSk, AkSkType serverType, bool expectOk)
    {
        GetClientFdReqPb req;
        AkSkManager akSkManager;
        akSkManager.GenerateSignature(req);
        akSkManager.SetClientAkSk(clientAk, clientSk);
        akSkManager.SetServerAkSk(serverType, serverAk, serverSk);
        std::string clientId = "client1";
        req.set_client_id(clientId);
        akSkManager.GenerateSignature(req);
        auto serializedStr = req.SerializeAsString();
        DS_ASSERT_OK(g_SerializedMessage.CopyBuffer(serializedStr.c_str(), serializedStr.size()));
        if (expectOk) {
            EXPECT_EQ(akSkManager.VerifySignatureAndTimestamp(req), Status::OK());
        } else {
            EXPECT_NE(akSkManager.VerifySignatureAndTimestamp(req), Status::OK());
        }
    }

    Status EncodeEncryptedData(std::string &cipherText)
    {
        auto strs = Split(cipherText, ":");
        const auto splitLen = 3;
        if (strs.size() != splitLen) {
            LOG(ERROR) << "Failed to encode cipherText, invalid strs size:" << strs.size();
            RETURN_STATUS(K_INVALID, "Invalid cipherText format");
        }
        auto byteContent = strs[splitLen - 1];
        auto hexContent = AesImpl::EncodeToHexString(strs[splitLen - 1]);
        cipherText = strs[0] + ":" + hexContent + strs[1];
        return Status::OK();
    }

protected:
    std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
    std::string errorKey_ = "axfasdfasdfacxvzfasdfasdfasdfasdfasdfasdf";
    std::string dataKey_ = "MFyfvK41ba2giqMUiUKGpownRZlmVmHc";
    std::shared_ptr<AkSkManagerHelper> akSkManager_;
};

TEST_F(AkSkTest, SimpleAkSk)
{
    VerifyAkSk(accessKey_, secretKey_, accessKey_, secretKey_, AkSkType::SYSTEM, true);
    VerifyAkSk(accessKey_, accessKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(secretKey_, secretKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(errorKey_, accessKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(errorKey_, secretKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
    VerifyAkSk(accessKey_, errorKey_, accessKey_, secretKey_, AkSkType::SYSTEM, false);
}

TEST_F(AkSkTest, AkSkTimeout)
{
    GetClientFdReqPb req;
    AkSkManager akSkManager(1);
    DS_ASSERT_OK(akSkManager.GenerateSignature(req));
    DS_ASSERT_OK(akSkManager.SetClientAkSk(accessKey_, secretKey_));
    DS_ASSERT_OK(akSkManager.SetServerAkSk(AkSkType::SYSTEM, "", secretKey_,  dataKey_));
    DS_ASSERT_OK(akSkManager.SetServerAkSk(AkSkType::SYSTEM, accessKey_, secretKey_,  dataKey_));
    std::string clientId = "client1";
    req.set_client_id(clientId);
    DS_ASSERT_OK(akSkManager.GenerateSignature(req));
    sleep(2);
    EXPECT_NE(akSkManager.VerifySignatureAndTimestamp(req), Status::OK());
}

TEST_F(AkSkTest, AkSkCompatibility)
{
    DS_ASSERT_OK(datasystem::inject::Set("AkSk.SetTimestamp", "call(123456)"));

    GetClientFdReqPb req;
    req.set_client_id("clientId");
    AkSkManager akSkManager;
    DS_ASSERT_OK(akSkManager.GenerateSignature(req));
    DS_ASSERT_OK(akSkManager.SetClientAkSk(accessKey_, secretKey_));
    DS_ASSERT_OK(akSkManager.SetServerAkSk(AkSkType::SYSTEM, "", secretKey_,  dataKey_));
    DS_ASSERT_OK(akSkManager.SetServerAkSk(AkSkType::SYSTEM, accessKey_, secretKey_,  dataKey_));
    std::string clientId = "client1";
    req.set_client_id(clientId);
    DS_ASSERT_OK(akSkManager.GenerateSignature(req));

    char expectSignRaw[] = "0539b3418b6ce67ae741dca0edf21ed40b76cd075965cef55c5959c181b70f2a\0";
    std::string expectSign(expectSignRaw, sizeof(expectSignRaw) - 1);
    ASSERT_EQ(req.signature(), expectSign);
}

TEST_F(AkSkTest, TestAesDecryptData)
{
    // construct encrypted secret key from iam, format: iv:cipherText+tag
    std::string secretKey = "123456";
    std::string hexDataKey = "1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF1234567890ABCDEF";
    auto bytePerHex = 2;
    const auto decodeDkSize = hexDataKey.size() / bytePerHex;
    auto decodeDk = std::make_unique<char[]>(decodeDkSize + 1);
    DS_ASSERT_OK(
        AesImpl::DecodeToString(hexDataKey.c_str(), hexDataKey.size(), decodeDk.get(), decodeDkSize + 1, true));
    auto aes = std::make_unique<AesImpl>(SensitiveValue(decodeDk.get(), decodeDkSize), AesImpl::Algorithm::AES_256_GCM);
    std::string cipherText;
    DS_ASSERT_OK(aes->Encrypt(SensitiveValue(secretKey), cipherText));
    DS_ASSERT_OK(EncodeEncryptedData(cipherText));

    datasystem::inject::Set("SecretManager.RootKeyActive", "return()");
    SensitiveValue encryptData(cipherText);
    DS_ASSERT_OK(akSkManager_->SetClientAkSk("ak", "sk", hexDataKey));
    DS_ASSERT_OK(akSkManager_->ConstructAesAndDecrypt(encryptData));
    ASSERT_EQ(encryptData.GetData(), secretKey);
}
}  // namespace ut
}  // namespace datasystem
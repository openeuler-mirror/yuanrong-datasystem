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
 * Description: Encrypt and decrypt test.
 */
#include "datasystem/common/flags/flags.h"
 
#include <fstream>
#include <memory>
#include <string>

#include <gtest/gtest.h>
 
#include "common.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/protos/share_memory.pb.h"
#include "datasystem/utils/sensitive_value.h"
 
DS_DECLARE_string(encrypt_kit);
 
namespace datasystem {
namespace st {
class EncryptDecryptTest : public CommonTest {
public:
    const std::string accessKey_ = "QTWAOYTTINDUT2QVKYUC";
    const std::string secretKey_ = "MFyfvK41ba2giqM7**********KGpownRZlmVmHc";
 
    void SetUp() override
    {
        FLAGS_encrypt_kit = "plaintext";
    }
 
protected:
    const std::string keyComponentDir = "data/encrypt_test/key_component_dir";
};
 
TEST_F(EncryptDecryptTest, TestNoneEncrypt)
{
    FLAGS_encrypt_kit = "plaintext";
    Status res;
    DS_ASSERT_OK(LoadAllSecretKeys());
    res = SecretManager::Instance()->LoadSecretKeys();
    ASSERT_EQ(res.GetCode(), K_INVALID);
 
    res = SecretManager::Instance()->GenerateRootKey();
    ASSERT_EQ(res.GetCode(), K_INVALID);
 
    std::unique_ptr<char[]> plainText;
    int size;
    res = SecretManager::Instance()->Decrypt("xx", plainText, size);
    ASSERT_EQ(res.GetCode(), K_OK);
    std::string decryptTextStr(plainText.get(), size);
    ASSERT_EQ(decryptTextStr, "xx");
 
    std::string cipher;
    res = SecretManager::Instance()->Encrypt("xx", cipher);
    ASSERT_EQ(res.GetCode(), K_OK);
    ASSERT_EQ("xx", cipher);
 
    res = SecretManager::Instance()->DestroyKeyFactor();
    ASSERT_EQ(res.GetCode(), K_INVALID);
 
    res = SecretManager::Instance()->DestroyRootKey();
    ASSERT_EQ(res.GetCode(), K_INVALID);
 
    std::string s1, s2, s3, salt;
    res = SecretManager::Instance()->GenerateAllKeyComponent(s1, s2, s3, salt);
    ASSERT_EQ(res.GetCode(), K_INVALID);
 
    ASSERT_TRUE(SecretManager::Instance()->CheckPreCondition());
}
}  // namespace st
}  // namespace datasystem
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
 * Description: A very, very boring logging test.
 */

#include "datasystem/utils/aes.h"
#include <cstddef>
#include <string>

#include "common.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
namespace ut {
class AesTest : public CommonTest {};

TEST_F(AesTest, TestAes128)
{
    std::string keyStr(16, 'a');
    SensitiveValue key(keyStr);
    Aes aes(key, Aes::Algorithm::AES_128_GCM);
    SensitiveValue plainText("test encrypt");
    std::string cipherText;
    DS_EXPECT_OK(aes.Encrypt(plainText, cipherText));
    SensitiveValue plainText2;
    DS_EXPECT_OK(aes.Decrypt(cipherText, plainText2));
    EXPECT_EQ(plainText, plainText2);
}

TEST_F(AesTest, TestAes192)
{
    std::string keyStr(24, 'a');
    SensitiveValue key(keyStr);
    Aes aes(key, Aes::Algorithm::AES_192_GCM);
    SensitiveValue plainText("test encrypt");
    std::string cipherText;
    DS_EXPECT_OK(aes.Encrypt(plainText, cipherText));
    SensitiveValue plainText2;
    DS_EXPECT_OK(aes.Decrypt(cipherText, plainText2));
    EXPECT_EQ(plainText, plainText2);
}

TEST_F(AesTest, TestAes256)
{
    std::string keyStr(32, 'a');
    SensitiveValue key(keyStr);
    Aes aes(key, Aes::Algorithm::AES_256_GCM);
    SensitiveValue plainText("test encrypt");
    std::string cipherText;
    DS_EXPECT_OK(aes.Encrypt(plainText, cipherText));
    SensitiveValue plainText2;
    DS_EXPECT_OK(aes.Decrypt(cipherText, plainText2));
    EXPECT_EQ(plainText, plainText2);
}

TEST_F(AesTest, TestBigData)
{
    size_t dataSize = 10 * 1024 * 1024;
    std::string data(dataSize, 'a');
    std::string keyStr(32, 'a');
    SensitiveValue key(keyStr);
    Aes aes(key, Aes::Algorithm::AES_256_GCM);
    SensitiveValue plainText(data);
    std::string cipherText;
    DS_EXPECT_OK(aes.Encrypt(plainText, cipherText));
    SensitiveValue plainText2;
    DS_EXPECT_OK(aes.Decrypt(cipherText, plainText2));
    EXPECT_EQ(plainText, plainText2);
}

TEST_F(AesTest, TestSpecialSymbols)
{
    std::string data = "Various symbols: !@#$%^&*()-_=+[]{};:,.<>?/\\|\"'";
    std::string keyStr(32, 'a');
    SensitiveValue key(keyStr);
    Aes aes(key, Aes::Algorithm::AES_256_GCM);
    SensitiveValue plainText(data);
    std::string cipherText;
    DS_EXPECT_OK(aes.Encrypt(plainText, cipherText));
    SensitiveValue plainText2;
    DS_EXPECT_OK(aes.Decrypt(cipherText, plainText2));
    EXPECT_EQ(plainText, plainText2);
}

}  // namespace ut
}  // namespace datasystem
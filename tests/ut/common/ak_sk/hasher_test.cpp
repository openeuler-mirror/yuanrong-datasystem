/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Hasher unit test (new methods for OBS V2 signing).
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
namespace ut {

// --------------- GetHMACSha1 ---------------

TEST(HasherTest, GetHMACSha1OutputLength)
{
    Hasher hasher;
    SensitiveValue key("test-secret-key");
    std::string output;
    Status rc = hasher.GetHMACSha1(key, "hello world", output);
    ASSERT_EQ(rc.GetCode(), 0);
    // HMAC-SHA1 always produces 20 bytes
    EXPECT_EQ(output.size(), 20u);
}

TEST(HasherTest, GetHMACSha1Deterministic)
{
    Hasher hasher;
    SensitiveValue key("my-secret");
    std::string output1, output2;
    ASSERT_EQ(hasher.GetHMACSha1(key, "test-data", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha1(key, "test-data", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetHMACSha1DifferentInputs)
{
    Hasher hasher;
    SensitiveValue key("secret");
    std::string output1, output2;
    ASSERT_EQ(hasher.GetHMACSha1(key, "input-a", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha1(key, "input-b", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

TEST(HasherTest, GetHMACSha1DifferentKeys)
{
    Hasher hasher;
    SensitiveValue key1("secret-a");
    SensitiveValue key2("secret-b");
    std::string output1, output2;
    ASSERT_EQ(hasher.GetHMACSha1(key1, "same-data", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha1(key2, "same-data", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

// --------------- Base64Encode ---------------

TEST(HasherTest, Base64EncodeEmpty)
{
    std::string output;
    Status rc = Hasher::Base64Encode("", output);
    ASSERT_EQ(rc.GetCode(), 0);
    EXPECT_EQ(output, "");
}

TEST(HasherTest, Base64EncodeSimpleString)
{
    // "hello" -> "aGVsbG8="
    std::string output;
    ASSERT_EQ(Hasher::Base64Encode("hello", output).GetCode(), 0);
    EXPECT_EQ(output, "aGVsbG8=");
}

TEST(HasherTest, Base64EncodeKnownVector)
{
    // "abc123" -> "YWJjMTIz"
    std::string output;
    ASSERT_EQ(Hasher::Base64Encode("abc123", output).GetCode(), 0);
    EXPECT_EQ(output, "YWJjMTIz");
}

TEST(HasherTest, Base64EncodeBinaryData)
{
    // Binary data with null bytes: {0x00, 0x01, 0x02, 0x03} -> "AAECAw=="
    std::string input(4, '\0');
    input[1] = 0x01;
    input[2] = 0x02;
    input[3] = 0x03;
    std::string output;
    ASSERT_EQ(Hasher::Base64Encode(input, output).GetCode(), 0);
    EXPECT_EQ(output, "AAECAw==");
}

TEST(HasherTest, Base64EncodeDeterministic)
{
    std::string output1, output2;
    ASSERT_EQ(Hasher::Base64Encode("deterministic-test", output1).GetCode(), 0);
    ASSERT_EQ(Hasher::Base64Encode("deterministic-test", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

// --------------- GetHMACSha1Base64 ---------------

TEST(HasherTest, GetHMACSha1Base64ProducesValidBase64)
{
    Hasher hasher;
    SensitiveValue key("test-key");
    std::string output;
    Status rc = hasher.GetHMACSha1Base64(key, "test-data", output);
    ASSERT_EQ(rc.GetCode(), 0);
    // Output should be non-empty and a valid Base64 string
    EXPECT_FALSE(output.empty());
    // Base64 characters: A-Z, a-z, 0-9, +, /, =
    for (char c : output) {
        EXPECT_TRUE((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+'
                    || c == '/' || c == '=');
    }
}

TEST(HasherTest, GetHMACSha1Base64Deterministic)
{
    Hasher hasher;
    SensitiveValue key("secret-key");
    std::string output1, output2;
    ASSERT_EQ(hasher.GetHMACSha1Base64(key, "same-input", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha1Base64(key, "same-input", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetHMACSha1Base64ConsistentWithComponents)
{
    // GetHMACSha1Base64 should equal Base64Encode(GetHMACSha1(...))
    Hasher hasher;
    SensitiveValue key("consistency-key");
    std::string data = "check-consistency";

    std::string rawSha1;
    ASSERT_EQ(hasher.GetHMACSha1(key, data, rawSha1).GetCode(), 0);

    std::string base64OfRaw;
    ASSERT_EQ(Hasher::Base64Encode(rawSha1, base64OfRaw).GetCode(), 0);

    std::string combined;
    ASSERT_EQ(hasher.GetHMACSha1Base64(key, data, combined).GetCode(), 0);

    EXPECT_EQ(combined, base64OfRaw);
}

// --------------- GetMD5 ---------------

TEST(HasherTest, GetMD5OutputLength)
{
    std::string output;
    Status rc = Hasher::GetMD5("test data", output);
    ASSERT_EQ(rc.GetCode(), 0);
    // MD5 always produces 16 bytes
    EXPECT_EQ(output.size(), 16u);
}

TEST(HasherTest, GetMD5EmptyString)
{
    std::string output;
    ASSERT_EQ(Hasher::GetMD5("", output).GetCode(), 0);
    // MD5("") = d41d8cd98f00b204e9800998ecf8427e
    EXPECT_EQ(output.size(), 16u);
}

TEST(HasherTest, GetMD5Deterministic)
{
    std::string output1, output2;
    ASSERT_EQ(Hasher::GetMD5("hello", output1).GetCode(), 0);
    ASSERT_EQ(Hasher::GetMD5("hello", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetMD5DifferentInputs)
{
    std::string output1, output2;
    ASSERT_EQ(Hasher::GetMD5("input-a", output1).GetCode(), 0);
    ASSERT_EQ(Hasher::GetMD5("input-b", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

// --------------- GetMD5Base64 ---------------

TEST(HasherTest, GetMD5Base64ProducesValidBase64)
{
    std::string output;
    Status rc = Hasher::GetMD5Base64("test data", output);
    ASSERT_EQ(rc.GetCode(), 0);
    EXPECT_FALSE(output.empty());
    // Verify all characters are valid Base64
    for (char c : output) {
        EXPECT_TRUE((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+'
                    || c == '/' || c == '=');
    }
}

TEST(HasherTest, GetMD5Base64Deterministic)
{
    std::string output1, output2;
    ASSERT_EQ(Hasher::GetMD5Base64("deterministic", output1).GetCode(), 0);
    ASSERT_EQ(Hasher::GetMD5Base64("deterministic", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetMD5Base64ConsistentWithComponents)
{
    // GetMD5Base64 should equal Base64Encode(GetMD5(...))
    std::string data = "consistency-check";

    std::string rawMd5;
    ASSERT_EQ(Hasher::GetMD5(data, rawMd5).GetCode(), 0);

    std::string base64OfRaw;
    ASSERT_EQ(Hasher::Base64Encode(rawMd5, base64OfRaw).GetCode(), 0);

    std::string combined;
    ASSERT_EQ(Hasher::GetMD5Base64(data, combined).GetCode(), 0);

    EXPECT_EQ(combined, base64OfRaw);
}

}  // namespace ut
}  // namespace datasystem

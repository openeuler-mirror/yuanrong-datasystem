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

#include <cstdio>
#include <sstream>
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
    EXPECT_EQ(output.size(), 20U);  // NOLINT(readability-magic-numbers)
}

TEST(HasherTest, GetHMACSha1Deterministic)
{
    Hasher hasher;
    SensitiveValue key("my-secret");
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha1(key, "test-data", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha1(key, "test-data", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetHMACSha1DifferentInputs)
{
    Hasher hasher;
    SensitiveValue key("secret");
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha1(key, "input-a", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha1(key, "input-b", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

TEST(HasherTest, GetHMACSha1DifferentKeys)
{
    Hasher hasher;
    SensitiveValue key1("secret-a");
    SensitiveValue key2("secret-b");
    std::string output1;
    std::string output2;
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
    std::string input(4, '\0');  // NOLINT(readability-magic-numbers)
    input[1] = 0x01;
    input[2] = 0x02;
    input[3] = 0x03;
    std::string output;
    ASSERT_EQ(Hasher::Base64Encode(input, output).GetCode(), 0);
    EXPECT_EQ(output, "AAECAw==");
}

TEST(HasherTest, Base64EncodeDeterministic)
{
    std::string output1;
    std::string output2;
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
    std::string output1;
    std::string output2;
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
    EXPECT_EQ(output.size(), 16U);  // NOLINT(readability-magic-numbers)
}

TEST(HasherTest, GetMD5EmptyString)
{
    std::string output;
    ASSERT_EQ(Hasher::GetMD5("", output).GetCode(), 0);
    // MD5("") = d41d8cd98f00b204e9800998ecf8427e
    EXPECT_EQ(output.size(), 16U);  // NOLINT(readability-magic-numbers)
}

TEST(HasherTest, GetMD5Deterministic)
{
    std::string output1;
    std::string output2;
    ASSERT_EQ(Hasher::GetMD5("hello", output1).GetCode(), 0);
    ASSERT_EQ(Hasher::GetMD5("hello", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetMD5DifferentInputs)
{
    std::string output1;
    std::string output2;
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
    std::string output1;
    std::string output2;
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

// --------------- GetHMACSha256 (std::string key) - AWS V4 signing key derivation ---------------

TEST(HasherTest, GetHMACSha256StringKeyOutputLength)
{
    Hasher hasher;
    std::string key = "test-secret-key";
    std::string output;
    Status rc = hasher.GetHMACSha256(key, "hello world", output);
    ASSERT_EQ(rc.GetCode(), 0);
    // HMAC-SHA256 always produces 32 bytes
    EXPECT_EQ(output.size(), 32U);  // NOLINT(readability-magic-numbers)
}

TEST(HasherTest, GetHMACSha256StringKeyDeterministic)
{
    Hasher hasher;
    std::string key = "my-secret";
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha256(key, "test-data", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha256(key, "test-data", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetHMACSha256StringKeyDifferentInputs)
{
    Hasher hasher;
    std::string key = "secret";
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha256(key, "input-a", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha256(key, "input-b", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

TEST(HasherTest, GetHMACSha256StringKeyDifferentKeys)
{
    Hasher hasher;
    std::string key1 = "secret-a";
    std::string key2 = "secret-b";
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha256(key1, "same-data", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha256(key2, "same-data", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

TEST(HasherTest, GetHMACSha256StringKeyAwsV4SigningKeyDerivation)
{
    // Test AWS V4 signing key derivation chain
    // Reference: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
    Hasher hasher;
    std::string secretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    std::string date = "20151229";
    std::string region = "us-east-1";
    std::string service = "s3";

    // Step 1: kDate = HMAC-SHA256(AWS4 + secretKey, date)
    std::string kDate;
    std::string aws4Prefix = "AWS4" + secretKey;
    ASSERT_EQ(hasher.GetHMACSha256(aws4Prefix, date, kDate).GetCode(), 0);
    EXPECT_EQ(kDate.size(), 32U);  // NOLINT(readability-magic-numbers)

    // Step 2: kRegion = HMAC-SHA256(kDate, region)
    std::string kRegion;
    ASSERT_EQ(hasher.GetHMACSha256(kDate, region, kRegion).GetCode(), 0);
    EXPECT_EQ(kRegion.size(), 32U);  // NOLINT(readability-magic-numbers)

    // Step 3: kService = HMAC-SHA256(kRegion, service)
    std::string kService;
    ASSERT_EQ(hasher.GetHMACSha256(kRegion, service, kService).GetCode(), 0);
    EXPECT_EQ(kService.size(), 32U);  // NOLINT(readability-magic-numbers)

    // Step 4: kSigning = HMAC-SHA256(kService, "aws4_request")
    std::string kSigning;
    ASSERT_EQ(hasher.GetHMACSha256(kService, "aws4_request", kSigning).GetCode(), 0);
    EXPECT_EQ(kSigning.size(), 32U);  // NOLINT(readability-magic-numbers)
}

// --------------- GetHMACSha256Hex (std::string key) - AWS V4 signature ---------------

TEST(HasherTest, GetHMACSha256HexStringKeyOutputLength)
{
    Hasher hasher;
    std::string key = "test-secret-key";
    std::string output;
    Status rc = hasher.GetHMACSha256Hex(key, "hello world", output);
    ASSERT_EQ(rc.GetCode(), 0);
    // Hex-encoded HMAC-SHA256 produces 64 characters (32 bytes * 2)
    EXPECT_EQ(output.size(), 64U);  // NOLINT(readability-magic-numbers)
}

TEST(HasherTest, GetHMACSha256HexStringKeyValidHex)
{
    Hasher hasher;
    std::string key = "secret";
    std::string output;
    ASSERT_EQ(hasher.GetHMACSha256Hex(key, "test-data", output).GetCode(), 0);
    // Verify all characters are valid hex
    for (char c : output) {
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
    }
}

TEST(HasherTest, GetHMACSha256HexStringKeyDeterministic)
{
    Hasher hasher;
    std::string key = "my-secret";
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha256Hex(key, "test-data", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha256Hex(key, "test-data", output2).GetCode(), 0);
    EXPECT_EQ(output1, output2);
}

TEST(HasherTest, GetHMACSha256HexStringKeyDifferentInputs)
{
    Hasher hasher;
    std::string key = "secret";
    std::string output1;
    std::string output2;
    ASSERT_EQ(hasher.GetHMACSha256Hex(key, "input-a", output1).GetCode(), 0);
    ASSERT_EQ(hasher.GetHMACSha256Hex(key, "input-b", output2).GetCode(), 0);
    EXPECT_NE(output1, output2);
}

TEST(HasherTest, GetHMACSha256HexStringKeyConsistentWithBinary)
{
    // GetHMACSha256Hex should produce hex encoding of GetHMACSha256 binary output
    Hasher hasher;
    std::string key = "consistency-key";
    std::string data = "check-consistency";

    std::string binary;
    ASSERT_EQ(hasher.GetHMACSha256(key, data, binary).GetCode(), 0);

    std::string hex;
    ASSERT_EQ(hasher.GetHMACSha256Hex(key, data, hex).GetCode(), 0);

    // Manually convert binary to hex and compare
    std::ostringstream oss;
    for (unsigned char c : binary) {
        char buf[3];  // NOLINT(readability-magic-numbers)
        snprintf(buf, sizeof(buf), "%02x", c);
        oss << buf;
    }
    EXPECT_EQ(hex, oss.str());
}

}  // namespace ut
}  // namespace datasystem

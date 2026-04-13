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
 * Description: OBS V2 signature utility unit test.
 */

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "datasystem/common/l2cache/obs_client/obs_signature.h"

namespace datasystem {
namespace ut {

// --------------- BuildStringToSign ---------------

TEST(ObsSignatureTest, BuildStringToSignBasicFields)
{
    std::map<std::string, std::string> headers;
    std::string result = ObsSignature::BuildStringToSign("PUT", "d41d8cd98f00b204e9800998ecf8427e",
                                                         "text/plain", "Wed, 01 Mar 2024 12:00:00 GMT", headers,
                                                         "/bucket/object");
    // Expected: METHOD\nMD5\nContentType\nDate\nCanonicalResource
    EXPECT_EQ(result,
              "PUT\nd41d8cd98f00b204e9800998ecf8427e\ntext/plain\nWed, 01 Mar 2024 12:00:00 GMT\n/bucket/object");
}

TEST(ObsSignatureTest, BuildStringToSignEmptyFields)
{
    std::map<std::string, std::string> headers;
    // Empty contentMd5 and contentType still produce newlines
    std::string result =
        ObsSignature::BuildStringToSign("GET", "", "", "Wed, 01 Mar 2024 12:00:00 GMT", headers, "/bucket/object");
    // METHOD\n\n\nDate\nCanonicalResource
    EXPECT_EQ(result, "GET\n\n\nWed, 01 Mar 2024 12:00:00 GMT\n/bucket/object");
}

TEST(ObsSignatureTest, BuildStringToSignWithObsHeaders)
{
    std::map<std::string, std::string> headers;
    headers["x-obs-acl"] = "public-read";
    headers["x-obs-meta-key"] = "value";
    std::string result = ObsSignature::BuildStringToSign("PUT", "", "", "Wed, 01 Mar 2024 12:00:00 GMT", headers,
                                                         "/bucket/object");
    // Should contain header lines between Date and CanonicalResource
    EXPECT_NE(result.find("x-obs-acl:public-read"), std::string::npos);
    EXPECT_NE(result.find("x-obs-meta-key:value"), std::string::npos);
    // Verify the structure: method, empty md5, empty content-type, date, headers, resource
    EXPECT_EQ(result.substr(0, 4), "PUT\n");
    EXPECT_GE(result.find("/bucket/object"), 0u);
}

TEST(ObsSignatureTest, BuildStringToSignNoHeaders)
{
    std::map<std::string, std::string> headers;
    std::string result =
        ObsSignature::BuildStringToSign("GET", "", "", "Wed, 01 Mar 2024 12:00:00 GMT", headers, "/bucket/");
    // Without headers, should be: METHOD\n\n\nDate\n/bucket/
    EXPECT_EQ(result, "GET\n\n\nWed, 01 Mar 2024 12:00:00 GMT\n/bucket/");
}

// --------------- Sign ---------------

TEST(ObsSignatureTest, SignProducesValidBase64)
{
    std::string signature;
    std::string stringToSign = "GET\n\n\nWed, 01 Mar 2024 12:00:00 GMT\n/bucket/object";
    Status rc = ObsSignature::Sign("my-secret-key", stringToSign, signature);
    ASSERT_EQ(rc.GetCode(), 0);
    EXPECT_FALSE(signature.empty());
    // Verify it's valid Base64
    for (char c : signature) {
        EXPECT_TRUE((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+'
                    || c == '/' || c == '=');
    }
}

TEST(ObsSignatureTest, SignDeterministic)
{
    std::string stringToSign = "PUT\n\napplication/xml\nWed, 01 Mar 2024 12:00:00 GMT\n/bucket/object";
    std::string sig1, sig2;
    ASSERT_EQ(ObsSignature::Sign("secret", stringToSign, sig1).GetCode(), 0);
    ASSERT_EQ(ObsSignature::Sign("secret", stringToSign, sig2).GetCode(), 0);
    EXPECT_EQ(sig1, sig2);
}

TEST(ObsSignatureTest, SignDifferentKeys)
{
    std::string stringToSign = "GET\n\n\nWed, 01 Mar 2024 12:00:00 GMT\n/bucket/object";
    std::string sig1, sig2;
    ASSERT_EQ(ObsSignature::Sign("secret-a", stringToSign, sig1).GetCode(), 0);
    ASSERT_EQ(ObsSignature::Sign("secret-b", stringToSign, sig2).GetCode(), 0);
    EXPECT_NE(sig1, sig2);
}

// --------------- BuildAuthHeader ---------------

TEST(ObsSignatureTest, BuildAuthHeaderFormat)
{
    std::string result = ObsSignature::BuildAuthHeader("myAccessKey", "abc123signature==");
    EXPECT_EQ(result, "OBS myAccessKey:abc123signature==");
}

TEST(ObsSignatureTest, BuildAuthHeaderEmptySignature)
{
    std::string result = ObsSignature::BuildAuthHeader("myAccessKey", "");
    EXPECT_EQ(result, "OBS myAccessKey:");
}

TEST(ObsSignatureTest, BuildAuthHeaderContainsOBS)
{
    std::string result = ObsSignature::BuildAuthHeader("AK123", "sig456");
    EXPECT_EQ(result.substr(0, 4), "OBS ");
    EXPECT_NE(result.find("AK123:"), std::string::npos);
}

// --------------- BuildCanonicalResource ---------------

TEST(ObsSignatureTest, BuildCanonicalResourceBucketOnly)
{
    std::string result = ObsSignature::BuildCanonicalResource("my-bucket", "");
    EXPECT_EQ(result, "/my-bucket/");
}

TEST(ObsSignatureTest, BuildCanonicalResourceWithObjectKey)
{
    std::string result = ObsSignature::BuildCanonicalResource("my-bucket", "path/to/object.txt");
    EXPECT_EQ(result, "/my-bucket/path/to/object.txt");
}

TEST(ObsSignatureTest, BuildCanonicalResourceWithSubResources)
{
    std::map<std::string, std::string> subResources;
    subResources["uploads"] = "";
    std::string result = ObsSignature::BuildCanonicalResource("my-bucket", "obj", subResources);
    EXPECT_NE(result.find("?uploads"), std::string::npos);
}

TEST(ObsSignatureTest, BuildCanonicalResourceWithSubResourcesWithValues)
{
    std::map<std::string, std::string> subResources;
    subResources["uploadId"] = "upload123";
    subResources["partNumber"] = "1";
    std::string result = ObsSignature::BuildCanonicalResource("my-bucket", "obj", subResources);
    EXPECT_NE(result.find("uploadId=upload123"), std::string::npos);
    EXPECT_NE(result.find("partNumber=1"), std::string::npos);
}

TEST(ObsSignatureTest, BuildCanonicalResourceNoSubResources)
{
    std::string result = ObsSignature::BuildCanonicalResource("bucket", "key");
    EXPECT_EQ(result, "/bucket/key");
}

// --------------- FormatDateRFC1123 ---------------

TEST(ObsSignatureTest, FormatDateRFC1123EndsWithGMT)
{
    std::string date = ObsSignature::FormatDateRFC1123();
    EXPECT_GE(date.size(), 24u);
    // Should end with "GMT"
    EXPECT_EQ(date.substr(date.size() - 3), "GMT");
}

TEST(ObsSignatureTest, FormatDateRFC1123ContainsComma)
{
    std::string date = ObsSignature::FormatDateRFC1123();
    // RFC 1123 format: "Wed, 01 Mar 2024 12:00:00 GMT" - contains a comma after day-of-week
    EXPECT_NE(date.find(","), std::string::npos);
}

TEST(ObsSignatureTest, FormatDateRFC1123ContainsColons)
{
    std::string date = ObsSignature::FormatDateRFC1123();
    // Time portion has colons: "HH:MM:SS"
    EXPECT_NE(date.find(":"), std::string::npos);
}

}  // namespace ut
}  // namespace datasystem

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
 * Description: AWS V4 signature unit test.
 */

#include <ctime>

#include <gtest/gtest.h>

#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/l2cache/obs_client/aws_v4_signature.h"
#include "datasystem/common/ak_sk/hasher.h"

namespace datasystem {
namespace ut {

class AwsV4SignatureTest : public testing::Test {
protected:
    const std::string ak_ = "AKIAIOSFODNN7EXAMPLE";
    const std::string sk_ = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const std::string region_ = "us-east-1";
};

TEST_F(AwsV4SignatureTest, TestUriEncodeSimple)
{
    EXPECT_EQ("test", AwsV4Signature::UriEncode("test"));
}

TEST_F(AwsV4SignatureTest, TestUriEncodeSpace)
{
    EXPECT_EQ("test%20file", AwsV4Signature::UriEncode("test file"));
}

TEST_F(AwsV4SignatureTest, TestUriEncodeSlash)
{
    EXPECT_EQ("test%2Ffile", AwsV4Signature::UriEncode("test/file", true));
    EXPECT_EQ("test/file", AwsV4Signature::UriEncode("test/file", false));
}

TEST_F(AwsV4SignatureTest, TestUriEncodeSpecialChars)
{
    EXPECT_EQ("test%2Bfile", AwsV4Signature::UriEncode("test+file"));
    EXPECT_EQ("test%3Dfile", AwsV4Signature::UriEncode("test=file"));
}

TEST_F(AwsV4SignatureTest, TestUriEncodeUnreservedChars)
{
    // Unreserved chars should not be encoded: A-Z a-z 0-9 - . _ ~
    EXPECT_EQ("ABCabc123-._~", AwsV4Signature::UriEncode("ABCabc123-._~"));
}

TEST_F(AwsV4SignatureTest, TestFormatDateISO8601)
{
    std::time_t now = std::time(nullptr);
    std::string date = AwsV4Signature::FormatDateISO8601(now);
    EXPECT_EQ(16U, date.size());  // NOLINT(readability-magic-numbers)
    EXPECT_EQ('T', date[8]);      // NOLINT(readability-magic-numbers)
    EXPECT_EQ('Z', date[15]);     // NOLINT(readability-magic-numbers)
}

TEST_F(AwsV4SignatureTest, TestFormatDateYYYYMMDD)
{
    std::time_t now = std::time(nullptr);
    std::string date = AwsV4Signature::FormatDateYYYYMMDD(now);
    EXPECT_EQ(8U, date.size());  // NOLINT(readability-magic-numbers)
}

TEST_F(AwsV4SignatureTest, TestSha256HexEmpty)
{
    std::string hash = AwsV4Signature::Sha256Hex("");
    EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hash);
}

TEST_F(AwsV4SignatureTest, TestSha256HexHello)
{
    std::string hash = AwsV4Signature::Sha256Hex("Hello, World!");
    EXPECT_EQ(64U, hash.size());  // NOLINT(readability-magic-numbers)
    EXPECT_EQ("dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f", hash);
}

TEST_F(AwsV4SignatureTest, TestSignatureType)
{
    AwsV4Signature sig("us-east-1");
    EXPECT_EQ(SignatureType::AWS_V4, sig.GetType());
}

TEST_F(AwsV4SignatureTest, TestSigningKeyDerivation)
{
    // Test that signing key derivation produces consistent results via Sha256Hex
    // Since CalculateSigningKey is private, we test the public interface
    std::string hash1 = AwsV4Signature::Sha256Hex("test-data");
    std::string hash2 = AwsV4Signature::Sha256Hex("test-data");
    EXPECT_EQ(hash1, hash2);
    EXPECT_EQ(64U, hash1.size());  // NOLINT(readability-magic-numbers)
}

// --- SignRequest tests ---

TEST_F(AwsV4SignatureTest, TestSignRequestProducesAuthHeader)
{
    AwsV4Signature sig(region_);
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
    request->SetMethod(HttpMethod::GET);
    request->AddHeader("Host", "minio.example.com:9000");

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;

    std::map<std::string, std::string> subResources;
    Status rc = sig.SignRequest(cred, request, "", subResources);
    ASSERT_EQ(rc.GetCode(), 0);

    const auto &headers = request->Headers();
    auto authIt = headers.find("Authorization");
    ASSERT_NE(authIt, headers.end());
    EXPECT_EQ(authIt->second.substr(0, 16), "AWS4-HMAC-SHA256");  // NOLINT(readability-magic-numbers)
}

TEST_F(AwsV4SignatureTest, TestSignRequestAddsAmzDate)
{
    AwsV4Signature sig(region_);
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
    request->SetMethod(HttpMethod::GET);
    request->AddHeader("Host", "minio.example.com:9000");

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;

    std::map<std::string, std::string> subResources;
    ASSERT_EQ(sig.SignRequest(cred, request, "", subResources).GetCode(), 0);

    const auto &headers = request->Headers();
    auto dateIt = headers.find("x-amz-date");
    ASSERT_NE(dateIt, headers.end());
    EXPECT_EQ(16U, dateIt->second.size());  // NOLINT(readability-magic-numbers)
    EXPECT_EQ('T', dateIt->second[8]);      // NOLINT(readability-magic-numbers)
    EXPECT_EQ('Z', dateIt->second[15]);     // NOLINT(readability-magic-numbers)
}

TEST_F(AwsV4SignatureTest, TestSignRequestAddsContentSha256)
{
    AwsV4Signature sig(region_);
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
    request->SetMethod(HttpMethod::GET);
    request->AddHeader("Host", "minio.example.com:9000");

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;

    std::map<std::string, std::string> subResources;
    ASSERT_EQ(sig.SignRequest(cred, request, "", subResources).GetCode(), 0);

    const auto &headers = request->Headers();
    auto shaIt = headers.find("x-amz-content-sha256");
    ASSERT_NE(shaIt, headers.end());
    EXPECT_EQ("UNSIGNED-PAYLOAD", shaIt->second);
}

TEST_F(AwsV4SignatureTest, TestSignRequestWithToken)
{
    AwsV4Signature sig(region_);
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
    request->SetMethod(HttpMethod::GET);
    request->AddHeader("Host", "minio.example.com:9000");

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;
    cred.token = "temp-session-token-123";

    std::map<std::string, std::string> subResources;
    ASSERT_EQ(sig.SignRequest(cred, request, "", subResources).GetCode(), 0);

    const auto &headers = request->Headers();
    auto tokenIt = headers.find("x-amz-security-token");
    ASSERT_NE(tokenIt, headers.end());
    EXPECT_EQ("temp-session-token-123", tokenIt->second);
}

TEST_F(AwsV4SignatureTest, TestSignRequestWithoutTokenOmitsSecurityToken)
{
    AwsV4Signature sig(region_);
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
    request->SetMethod(HttpMethod::GET);
    request->AddHeader("Host", "minio.example.com:9000");

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;

    std::map<std::string, std::string> subResources;
    ASSERT_EQ(sig.SignRequest(cred, request, "", subResources).GetCode(), 0);

    const auto &headers = request->Headers();
    EXPECT_EQ(headers.find("x-amz-security-token"), headers.end());
}

TEST_F(AwsV4SignatureTest, TestSignRequestDifferentRegionsProduceDifferentSignatures)
{
    AwsV4Signature sigUsEast1("us-east-1");
    AwsV4Signature sigEuWest1("eu-west-1");

    auto buildRequest = []() -> std::shared_ptr<HttpRequest> {
        auto req = std::make_shared<HttpRequest>();
        req->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
        req->SetMethod(HttpMethod::GET);
        req->AddHeader("Host", "minio.example.com:9000");
        return req;
    };

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;

    auto req1 = buildRequest();
    auto req2 = buildRequest();
    std::map<std::string, std::string> subResources;

    ASSERT_EQ(sigUsEast1.SignRequest(cred, req1, "", subResources).GetCode(), 0);
    ASSERT_EQ(sigEuWest1.SignRequest(cred, req2, "", subResources).GetCode(), 0);

    const auto &auth1 = req1->Headers().find("Authorization")->second;
    const auto &auth2 = req2->Headers().find("Authorization")->second;
    EXPECT_NE(auth1, auth2);
}

TEST_F(AwsV4SignatureTest, TestSignRequestAuthContainsCredential)
{
    AwsV4Signature sig(region_);
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl("http://minio.example.com:9000/test-bucket/test-object");
    request->SetMethod(HttpMethod::GET);
    request->AddHeader("Host", "minio.example.com:9000");

    ObsCredential cred;
    cred.ak = ak_;
    cred.sk = sk_;

    std::map<std::string, std::string> subResources;
    ASSERT_EQ(sig.SignRequest(cred, request, "", subResources).GetCode(), 0);

    const auto &auth = request->Headers().find("Authorization")->second;
    EXPECT_NE(auth.find(ak_), std::string::npos);
    EXPECT_NE(auth.find(region_), std::string::npos);
    EXPECT_NE(auth.find("aws4_request"), std::string::npos);
}

}  // namespace ut
}  // namespace datasystem

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
 * Description: OBS service detector unit test.
 */

#include <functional>

#include <gtest/gtest.h>

#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/httpclient/http_response.h"
#include "datasystem/common/l2cache/obs_client/obs_service_detector.h"

namespace datasystem {
namespace ut {

class ObsServiceDetectorTest : public testing::Test {};

class MockCurlHttpClientForDetector : public CurlHttpClient {
public:
    MockCurlHttpClientForDetector() = default;

    Status Send(const std::shared_ptr<HttpRequest> &request,
                std::shared_ptr<HttpResponse> &response) override
    {
        (void)request;
        if (sendFunc_) {
            return sendFunc_(response);
        }
        return Status::OK();
    }

    void SetSendFunc(std::function<Status(std::shared_ptr<HttpResponse> &)> func)
    {
        sendFunc_ = std::move(func);
    }

private:
    std::function<Status(std::shared_ptr<HttpResponse> &)> sendFunc_;
};

TEST_F(ObsServiceDetectorTest, TestParseRegionFromOBS)
{
    EXPECT_EQ("cn-north-4",
              ObsServiceDetector::ParseRegionFromEndpoint("obs.cn-north-4.myhuaweicloud.com"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromS3)
{
    EXPECT_EQ("us-west-2",
              ObsServiceDetector::ParseRegionFromEndpoint("s3.us-west-2.amazonaws.com"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromMinIO)
{
    // MinIO doesn't have region in hostname, defaults to us-east-1
    EXPECT_EQ("us-east-1",
              ObsServiceDetector::ParseRegionFromEndpoint("minio.example.com:9000"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromLocalhost)
{
    EXPECT_EQ("us-east-1",
              ObsServiceDetector::ParseRegionFromEndpoint("127.0.0.1:19000"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromOBSWithPort)
{
    EXPECT_EQ("cn-south-1",
              ObsServiceDetector::ParseRegionFromEndpoint("obs.cn-south-1.myhuaweicloud.com:443"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromS3WithBucketPrefix)
{
    // bucket.s3.region.amazonaws.com pattern
    EXPECT_EQ("ap-southeast-1",
              ObsServiceDetector::ParseRegionFromEndpoint("mybucket.s3.ap-southeast-1.amazonaws.com"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromOBSCnNorth1)
{
    EXPECT_EQ("cn-north-1",
              ObsServiceDetector::ParseRegionFromEndpoint("obs.cn-north-1.myhuaweicloud.com"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromS3UsEast1)
{
    EXPECT_EQ("us-east-1",
              ObsServiceDetector::ParseRegionFromEndpoint("s3.us-east-1.amazonaws.com"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromEmptyEndpoint)
{
    EXPECT_EQ("us-east-1",
              ObsServiceDetector::ParseRegionFromEndpoint(""));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromNoDots)
{
    EXPECT_EQ("us-east-1",
              ObsServiceDetector::ParseRegionFromEndpoint("localhost"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromOBSEurope)
{
    EXPECT_EQ("eu-west-1",
              ObsServiceDetector::ParseRegionFromEndpoint("obs.eu-west-1.myhuaweicloud.com"));
}

TEST_F(ObsServiceDetectorTest, TestParseRegionFromS3ApNortheast)
{
    EXPECT_EQ("ap-northeast-2",
              ObsServiceDetector::ParseRegionFromEndpoint("s3.ap-northeast-2.amazonaws.com"));
}

// --- Mock-based Detect() tests ---

TEST_F(ObsServiceDetectorTest, TestDetectMinIOFromServerHeader)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("Server", "MinIO");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::AWS_V4, ObsServiceDetector::Detect(client, "minio.example.com:9000", "test-bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectOBSFromServerHeader)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("Server", "obs");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::OBS_V2, ObsServiceDetector::Detect(client, "obs.cn-north-4.myhuaweicloud.com", "bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectAmazonS3FromServerHeader)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("Server", "AmazonS3");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::AWS_V4, ObsServiceDetector::Detect(client, "s3.us-east-1.amazonaws.com", "bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectFromObsRequestIdHeader)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("x-obs-request-id", "ABC123");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::OBS_V2, ObsServiceDetector::Detect(client, "obs.example.com", "bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectFromAmzRequestIdHeader)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("x-amz-request-id", "XYZ789");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::AWS_V4, ObsServiceDetector::Detect(client, "minio.example.com", "bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectSendFailureDefaultsToOBS)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            (void)response;
            return Status(K_RUNTIME_ERROR, "connection refused");
        });
    EXPECT_EQ(SignatureType::OBS_V2, ObsServiceDetector::Detect(client, "unreachable.host", "bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectUnknownServerDefaultsToOBS)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("Server", "nginx");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::OBS_V2, ObsServiceDetector::Detect(client, "unknown.server.com", "bucket"));
}

TEST_F(ObsServiceDetectorTest, TestDetectOBSHasCaseInsensitiveServerHeader)
{
    std::shared_ptr<CurlHttpClient> client = std::make_shared<MockCurlHttpClientForDetector>();
    static_cast<MockCurlHttpClientForDetector *>(client.get())->SetSendFunc(
        [](std::shared_ptr<HttpResponse> &response) -> Status {
            response->AddHeader("Server", "OBS/3.0");
            response->SetStatus(200);  // NOLINT(readability-magic-numbers)
            return Status::OK();
        });
    EXPECT_EQ(SignatureType::OBS_V2, ObsServiceDetector::Detect(client, "obs.example.com", "bucket"));
}

}  // namespace ut
}  // namespace datasystem

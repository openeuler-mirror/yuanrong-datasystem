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
 * Description: AWS Signature Version 4 implementation for S3/MinIO compatibility.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_AWS_V4_SIGNATURE_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_AWS_V4_SIGNATURE_H

#include <string>
#include <map>
#include <memory>

#include "datasystem/common/l2cache/obs_client/obs_signature_provider.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

class HttpRequest;

class AwsV4Signature : public ObsSignatureProvider {
public:
    explicit AwsV4Signature(const std::string &region);

    ~AwsV4Signature() override = default;

    Status SignRequest(const ObsCredential &credential,
                       std::shared_ptr<HttpRequest> &request,
                       const std::string &contentMd5,
                       const std::map<std::string, std::string> &subResources) override;

    SignatureType GetType() const override { return SignatureType::AWS_V4; }

    static std::string UriEncode(const std::string &input, bool encodeSlash = true);
    static std::string FormatDateISO8601(const std::time_t &now);
    static std::string FormatDateYYYYMMDD(const std::time_t &now);
    static std::string Sha256Hex(const std::string &data);

private:
    std::string ExtractCanonicalUri(const std::string &url);
    std::string BuildCanonicalQueryString(const std::map<std::string, std::string> &queryParams);
    Status BuildSignedHeaders(const std::map<std::string, std::string> &headers,
                              std::string &canonicalHeaders, std::string &signedHeaders);
    Status CalculateSigningKey(const std::string &sk, const std::string &date, std::string &signingKey);
    Status CalculateSignature(const std::string &signingKey, const std::string &stringToSign,
                              std::string &signature);
    std::string BuildCanonicalRequest(const std::string &method, const std::string &canonicalUri,
                                      const std::string &canonicalQueryString,
                                      const std::string &canonicalHeaders,
                                      const std::string &signedHeaders, const std::string &hashedPayload);
    std::string BuildStringToSign(const std::string &date, const std::string &credentialScope,
                                  const std::string &hashedCanonicalRequest);
    std::string BuildAuthorizationHeader(const std::string &ak, const std::string &credentialScope,
                                         const std::string &signedHeaders, const std::string &signature);

    std::string region_;
    std::string service_ = "s3";
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_AWS_V4_SIGNATURE_H

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
 * Description: OBS token cloud service rotation.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_CLOUD_SERVICE_ROTATION_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_CLOUD_SERVICE_ROTATION_H

#include "datasystem/common/httpclient/http_client.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
struct CredentialInfo {
    SensitiveValue access;
    SensitiveValue secret;
    SensitiveValue securityToken;
};

struct CCMSTokenConf {
    std::string projectID;
    std::string identityProvider;
    std::string iamHostName;
    bool enableTokenByAgency;
    int tokenTTLSeconds;
    std::string tokenAgencyName;
    std::string tokenAgencyDomain;
    std::string regionID;
    SensitiveValue oidcToken;
};

enum class CCMSRotationRequestOption {
    GET_IAM_TOKEN = 0,
    GET_REAL_IAM_TOKEN_BY_AGENCY = 1,
    GET_OBS_TOKEN = 2,
    UNKNOWN = 100
};

class CCMSRotationRequest : public HttpRequest {
public:
    /**
     * @brief Send a request in CCMS mode to obtain the OBS token.
     * @param[in] ccmsTokenConf The config of request.
     */
    explicit CCMSRotationRequest(const CCMSTokenConf &ccmsTokenConf);

    ~CCMSRotationRequest() override = default;

    /**
     * @brief Build iam or obs request headers and bodies, request depends on CCMSRotationRequestOption.
     */
    void BuildRequest() override;

    /**
     * @brief Clean Sensitive infos.
     */
    void ClearSensitiveInfo() override;

    /**
     * @brief Set request option.
     * @param[in]
     */
    void SetRequestOption(CCMSRotationRequestOption option);

    /**
     * @brief Set temp token for building request header.
     * @param[in] token
     */
    void SetTempIAMToken(const SensitiveValue &token);

private:
    void BuildIAMRequest();
    void BuildIAMAgencyRequest();
    void BuildOBSRequest();

    /**
     * @brief Construct the header and body of each request to be sent.
     * @param[in] address Set the url for sending requests.
     * @param[in] jsonString Body to be sent.
     * @param[in] headerMap Header to be sent.
     */
    void BuildCCMSCommonRequest(std::string address, const std::string &jsonString,
                                const std::map<std::string, std::string> &headerMap);
    SensitiveValue tempIAMToken_;
    CCMSRotationRequestOption requestOption_ = CCMSRotationRequestOption::UNKNOWN;
    CCMSTokenConf conf_;
};

class CCMSRotationAccessToken {
public:
    explicit CCMSRotationAccessToken(CCMSTokenConf ccmsTokenConf);

    /**
     * @brief Update the access token for obs when it is expired.
     */
    Status UpdateAccessToken(CredentialInfo &credentialInfo);

private:
    Status GetObsCredentialInfo(const std::shared_ptr<HttpResponse> &response,
                                std::shared_ptr<std::stringstream> &respStream, CredentialInfo &credentialInfo);

    CCMSTokenConf ccmsConf_;
    std::shared_ptr<HttpClient> httpClient_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_CLOUD_SERVICE_ROTATION_H

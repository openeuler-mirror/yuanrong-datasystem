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

#include "datasystem/common/l2cache/obs_client/cloud_service_rotation.h"

#include <sstream>
#include <utility>

#include <nlohmann/json.hpp>

#include "datasystem/common/log/log.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/inject/inject_point.h"

const std::string HEADER_SUBJECT_TOKEN = "X-Subject-Token";

const std::string CREDENTIAL = "credential";
const std::string ACCESS_KEY = "access";
const std::string SECRET_KEY = "secret";
const std::string TEMP_TOKEN = "securitytoken";

namespace datasystem {
CCMSRotationRequest::CCMSRotationRequest(const CCMSTokenConf &ccmsTokenConf) : conf_(ccmsTokenConf)
{
}

void CCMSRotationRequest::BuildRequest()
{
    this->SetMethod(HttpMethod::POST);
    switch (requestOption_) {
        case CCMSRotationRequestOption::GET_IAM_TOKEN:
            BuildIAMRequest();
            break;
        case CCMSRotationRequestOption::GET_REAL_IAM_TOKEN_BY_AGENCY:
            BuildIAMAgencyRequest();
            break;
        case CCMSRotationRequestOption::GET_OBS_TOKEN:
            BuildOBSRequest();
            break;
        case CCMSRotationRequestOption::UNKNOWN:
            LOG(ERROR) << "Unknown operation";
            break;
        default:
            LOG(ERROR) << "CCMS Rotation is not supported this request, build request failed.";
    }
}

void CCMSRotationRequest::ClearSensitiveInfo()
{
    if (!conf_.oidcToken.Empty()) {
        conf_.oidcToken.Clear();
    }
    if (!tempIAMToken_.Empty()) {
        tempIAMToken_.Clear();
    }

    auto it = headers_.find("X-Auth-Token");
    if (it != headers_.end()) {
        ClearStr(it->second);
    } else {
        std::stringstream *ptr = dynamic_cast<std::stringstream *>(this->GetBody().get());
        ClearStream(ptr);
    }
}

void CCMSRotationRequest::BuildIAMRequest()
{
    std::string address = FormatString("https://%s/v3.0/OS-AUTH/id-token/tokens", conf_.iamHostName);
    std::string jsonString =
        FormatString(R"({"auth":{"id_token":{"id":"%s"},"scope":{"project":{"id":"%s","name":"%s"}}}})",
                     conf_.oidcToken.GetData(), conf_.projectID, conf_.regionID);
    std::map<std::string, std::string> headerMap;
    (void)headerMap.insert({ "X-Idp-Id", conf_.identityProvider });
    BuildCCMSCommonRequest(address, jsonString, headerMap);
    ClearStr(jsonString);
}

void CCMSRotationRequest::BuildIAMAgencyRequest()
{
    std::string address = FormatString("https://%s/v3/auth/tokens", conf_.iamHostName);
    std::string jsonString = FormatString(
        R"({"auth":{"identity":{"methods":["assume_role"],"assume_role":{"agency_name":"%s","domain_name":"%s"}},
            "scope":{"domain":{"name": "%s"}}}})",
        conf_.tokenAgencyName, conf_.tokenAgencyDomain, conf_.tokenAgencyDomain);
    std::map<std::string, std::string> headerMap;
    (void)headerMap.insert({ "X-Auth-Token", tempIAMToken_.GetData() });
    BuildCCMSCommonRequest(address, jsonString, headerMap);
}

void CCMSRotationRequest::BuildOBSRequest()
{
    std::string address = FormatString("https://%s/v3.0/OS-CREDENTIAL/securitytokens", conf_.iamHostName);
    std::string jsonString = FormatString(
        R"({"auth":{"identity":{"methods":["token"],"token":{"duration_seconds":%d}}}})", conf_.tokenTTLSeconds);
    std::map<std::string, std::string> headerMap;
    (void)headerMap.insert({ "X-Auth-Token", tempIAMToken_.GetData() });
    BuildCCMSCommonRequest(address, jsonString, headerMap);
}

void CCMSRotationRequest::BuildCCMSCommonRequest(std::string address, const std::string &jsonString,
                                                 const std::map<std::string, std::string> &headerMap)
{
    SetUrl(std::move(address));
    this->RemoveAllHeader();
    this->AddHeader("Content-Type", "application/json");
    for (auto &headerContent : headerMap) {
        this->AddHeader(headerContent.first, headerContent.second);
    }
    nlohmann::json body;
    INJECT_POINT("CCMSRotationRequest.BuildCCMSCommonRequest.ErrorJsonString", [&body](const std::string &errString) {
        try {
            body = nlohmann::json::parse(errString);
        } catch (const nlohmann::json::exception &e) {
            LOG(ERROR) << FormatString("Json string parse error, Build request failed. ") << e.what();
            return;
        }
    });
    try {
        body = nlohmann::json::parse(jsonString);
    } catch (const nlohmann::json::exception &e) {
        LOG(ERROR) << FormatString("Json string parse error, Build request failed.") << e.what();
        return;
    }
    auto bodyStream = std::make_shared<std::stringstream>();
    *bodyStream << body;
    this->SetBody(bodyStream);
}

void CCMSRotationRequest::SetRequestOption(CCMSRotationRequestOption option)
{
    requestOption_ = option;
}

void CCMSRotationRequest::SetTempIAMToken(const SensitiveValue &token)
{
    tempIAMToken_ = token;
}

CCMSRotationAccessToken::CCMSRotationAccessToken(CCMSTokenConf ccmsTokenConf) : ccmsConf_(std::move(ccmsTokenConf))
{
    bool httpVerifyCert = GetBoolFromEnv("DATASYSTEM_HTTP_VERIFY_CERT", false);
    httpClient_ = std::make_shared<CurlHttpClient>(httpVerifyCert);
}

Status CCMSRotationAccessToken::UpdateAccessToken(CredentialInfo &credentialInfo)
{
    auto accessTokenReq = std::make_shared<CCMSRotationRequest>(ccmsConf_);
    auto response = std::make_shared<HttpResponse>();
    auto respStream = std::make_shared<std::stringstream>();
    response->SetBody(respStream);
    accessTokenReq->SetRequestOption(CCMSRotationRequestOption::GET_IAM_TOKEN);
    INJECT_POINT("CCMSRotationAccessToken.UpdateAccessToken.ErrorAction",
                 [&accessTokenReq]() {
                     int errorActionCode = 10;
                     accessTokenReq->SetRequestOption(CCMSRotationRequestOption(errorActionCode));
                     return Status::OK();
                 });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(httpClient_->Send(accessTokenReq, response), "Get temp iam token error.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        response->IsSuccess(), StatusCode::K_RUNTIME_ERROR,
        FormatString("Failed to get iam temp token, status code is %d", response->GetStatus()));

    SensitiveValue tempIAMToken = response->GetHeader(HEADER_SUBJECT_TOKEN);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!tempIAMToken.Empty(), StatusCode::K_RUNTIME_ERROR,
                                         "Error for get empty temp iam token.");
    accessTokenReq->SetTempIAMToken(tempIAMToken);

    if (ccmsConf_.enableTokenByAgency) {
        accessTokenReq->SetRequestOption(CCMSRotationRequestOption::GET_REAL_IAM_TOKEN_BY_AGENCY);
        respStream = std::make_shared<std::stringstream>();
        response->SetBody(respStream);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(httpClient_->Send(accessTokenReq, response), "Get real iam token error.");
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            response->IsSuccess(), StatusCode::K_RUNTIME_ERROR,
            FormatString("Failed to get iam real token by agency, status code is %d", response->GetStatus()));

        SensitiveValue realIAMToken = response->GetHeader(HEADER_SUBJECT_TOKEN);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!realIAMToken.Empty(), StatusCode::K_RUNTIME_ERROR,
                                             "Error for get empty temp iam token.");
        accessTokenReq->SetTempIAMToken(realIAMToken);
    }

    accessTokenReq->SetRequestOption(CCMSRotationRequestOption::GET_OBS_TOKEN);
    respStream = std::make_shared<std::stringstream>();
    response->SetBody(respStream);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(httpClient_->Send(accessTokenReq, response), "Get temp obs token error.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        response->IsSuccess(), StatusCode::K_RUNTIME_ERROR,
        FormatString("Failed to temp obs token, status code is %d", response->GetStatus()));
    return GetObsCredentialInfo(response, respStream, credentialInfo);
}

Status CCMSRotationAccessToken::GetObsCredentialInfo(const std::shared_ptr<HttpResponse> &response,
                                                     std::shared_ptr<std::stringstream> &respStream,
                                                     CredentialInfo &credentialInfo)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(response != nullptr, StatusCode::K_RUNTIME_ERROR, "response is nullptr");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(respStream != nullptr, StatusCode::K_RUNTIME_ERROR, "response stream is null");
    nlohmann::json jsonRes;
    try {
        jsonRes = nlohmann::json::parse(respStream->str());
    } catch (nlohmann::json::exception &e) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Failed to parse response to json and status code is %d. Exception: %s",
                                             response->GetStatus(), e.what()));
    }
    CHECK_FAIL_RETURN_STATUS(jsonRes.is_object(), StatusCode::K_RUNTIME_ERROR, "The response is not a json object.");
    CHECK_FAIL_RETURN_STATUS(jsonRes.contains(CREDENTIAL) && jsonRes[CREDENTIAL].is_object(),
                             StatusCode::K_RUNTIME_ERROR, "The response obs credential is not a json object.");
    auto accessPtr = jsonRes[CREDENTIAL][ACCESS_KEY].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(accessPtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find access.");
    credentialInfo.access = *accessPtr;
    std::fill(accessPtr->begin(), accessPtr->end(), 0);

    auto secretPtr = jsonRes[CREDENTIAL][SECRET_KEY].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(secretPtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find secret.");
    credentialInfo.secret = *secretPtr;
    std::fill(secretPtr->begin(), secretPtr->end(), 0);

    auto tokenPtr = jsonRes[CREDENTIAL][TEMP_TOKEN].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(tokenPtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find obs temp token.");
    credentialInfo.securityToken = *tokenPtr;
    std::fill(tokenPtr->begin(), tokenPtr->end(), 0);
    return Status::OK();
}
}  // namespace datasystem
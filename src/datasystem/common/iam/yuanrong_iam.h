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
 * Description: This interface is used for IAM authentication.
 */
#ifndef DATASYSTEM_COMMON_IAM_YUANRONG_IAM_H
#define DATASYSTEM_COMMON_IAM_YUANRONG_IAM_H

#include <curl/curl.h>

#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/iam/iam.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
constexpr int HTTP_OK = 200;
constexpr int HTTP_BAD_REQUEST_METHOD_NOT_ALLOWED = 405;
constexpr int HTTP_BAD_REQUEST = 400;
constexpr int HTTP_BAD_REQUEST_FORBIDDEN = 403;
constexpr int HTTP_BAD_REQUEST_INTERNAL_SERVER_ERROR = 500;
const std::string REQUEST_AUTHKEY = "X-Auth";
const std::string RESPONSE_TENANTID = "X-Tenant-ID";
const std::string RESPONSE_EXPIRED_TIME = "X-Expired-Time-Span";
const std::string RESPBODY_TENANTID = "tenantID";
const std::string RESPBODY_EXPIRED_TIME = "expiredTimeSpan";
const std::string RESPBODY_SECRETKEY = "secretKey";
const std::string RESPBODY_ROLE = "role";
class YuanRongVerifyTenantAuthReq : public HttpRequest {
public:
    /**
     * @brief worker verify client token request
     * @param token token for verify tenant token auth.
     */
    YuanRongVerifyTenantAuthReq(const SensitiveValue &token);

    /**
     * @brief worker verify client aksk request
     * @param accessKey accessKey for verify tenant aksk auth.
     */
    YuanRongVerifyTenantAuthReq(const std::string &accessKey);

    ~YuanRongVerifyTenantAuthReq() = default;

    void ClearSensitiveInfo() override;
};

class YuanRongIAM : public IAM {
public:
    YuanRongIAM(std::shared_ptr<AkSkManager> akSkManager = nullptr) : akSkManager_(std::move(akSkManager))
    {
    }

    ~YuanRongIAM() = default;

    Status Init(bool &authEnable) override;

    /**
     * @brief Authenticating the token using IAM service, and get a tenant id from IAM service.
     * @param[in] token Token to be authenticated.
     * @param[out] tenantId ID of the tenant corresponding to the token.
     * @param[out] expireSec Expire second for token.
     * @return Status of the call.
     */
    Status VerifyTenantToken(const SensitiveValue &token, std::string &tenantId, uint64_t &expireSec) override;

    /**
     * @brief Authenticating the AKSK using IAM service, and get a tenant id wiht Serect key from IAM service.
     * @param[in] acceessKey Access key to be authenticated.
     * @param[out] serectKey Secret key corresponding to the Access key.
     * @param[out] tenantId ID of the tenant corresponding to the AKSK.
     * @param[out] expireSec expireSec time for AKSK.
     * @param[out] isSystemRole The tenant is system or not.
     * @return Status of the call.
     */
    Status VerifyTenantAkSk(const std::string &accessKey, SensitiveValue &secretKey, std::string &tenantId,
                            uint64_t &expireSec, bool &isSystemRole) override;

    /**
     * @brief DecryptTlsKey
     * @param[in] keyPath FilePath of auth file
     * @param[out] value file context of auth file
     * @return Status of the call
     */
    static Status DecryptTLSKey(const std::string &keyPath, SensitiveValue &value, bool isEncrypted);

    /**
     * @brief Send requests to the server through client and retry when the request fails.
     * @param[in] setAuthHeader Set the Authorization header.
     * @param[in] request Message of http request.
     * @param[in] response Message of http response.
     * @param[out] rc Status of the call.
     * @return Status of the call.
     */
    Status SendRequestAndRetry(std::function<void(std::shared_ptr<YuanRongVerifyTenantAuthReq> &)> setAuthHeader,
                               std::shared_ptr<YuanRongVerifyTenantAuthReq> &request,
                               std::shared_ptr<HttpResponse> &response);

    /**
     * @brief Get AKSK Authciaction info from response.
     * @param[in] response Message of http response.
     * @param[in] respStream response stream.
     * @param[out] secretKey Secret key corresponding to the Access key.
     * @param[out] tenantId ID of the tenant corresponding to the AKSK.
     * @param[out] expireSec expireSec time for AKSK.
     * @param[out] isSystemRole The tenant is system or not.
     * @return Status of the call.
     */
    Status GetAkSkAuthInfo(const std::shared_ptr<HttpResponse> &response,
                           std::shared_ptr<std::stringstream> &respStream, SensitiveValue &secretKey,
                           std::string &tenantId, uint64_t &expireSec, bool &isSystemRole);

protected:
    /**
     * @brief Set the Authorization.
     * @param[in] request The http request.
     * @return Status of the call.
     */
    Status SetAuthorization(const std::shared_ptr<HttpRequest> &request);

    std::shared_ptr<CurlHttpClient> httpClient_;

private:
    /**
     * @brief Get the Tsl Config object
     * @return Status of the call
     */
    Status GetTslConfig();

    SensitiveValue cert_;
    SensitiveValue ca_;
    SensitiveValue key_;
    bool httpAuth_ = true;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_IAM_IAM_CLIENT_API_H
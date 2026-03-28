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
#include "datasystem/common/iam/yuanrong_iam.h"

#include <cerrno>
#include <cstddef>
#include <ctime>
#include <fstream>
#include <sstream>

#include "datasystem/common/ak_sk/hasher.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/httpclient/http_message.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

#include <securec.h>
#include <nlohmann/json.hpp>

DS_DEFINE_string(yuanrong_iam_url, "", "yuanrong_iam auth server url, ex: http(s)://xxx.xxx");
DS_DEFINE_string(yuanrong_iam_ca, "",
                 "File path of trusted CA/ca bundle file for yuanrong iam, optional. Use standard characters only.");
DS_DEFINE_string(yuanrong_iam_cert, "",
                 "File path of client certificate file for yuanrong iam, optional. Use standard characters only.");
DS_DEFINE_string(yuanrong_iam_key, "",
                 "File path of client private key, optional for yuanrong iam. Use standard characters only");
DS_DEFINE_string(yuanrong_iam_passphrase, "",
                 "File path of client pwd, optional for yuanrong iam. Use standard characters only");
DS_DECLARE_string(encrypt_kit);

namespace datasystem {
YuanRongVerifyTenantAuthReq::YuanRongVerifyTenantAuthReq(const SensitiveValue &token)
{
    SetUrl(FLAGS_yuanrong_iam_url + "/iam-server/v1/token/auth");
    this->SetMethod(HttpMethod::GET);
    this->AddHeader(REQUEST_AUTHKEY, token.GetData());
}

YuanRongVerifyTenantAuthReq::YuanRongVerifyTenantAuthReq(const std::string &accessKey)
{
    SetUrl(FLAGS_yuanrong_iam_url + "/iam-server/v1/credential/require");
    this->SetMethod(HttpMethod::GET);
    this->AddHeader(REQUEST_AUTHKEY, accessKey);
}

void YuanRongVerifyTenantAuthReq::ClearSensitiveInfo()
{
    size_t size = headers_[REQUEST_AUTHKEY].size();
    headers_[REQUEST_AUTHKEY].assign(size, '0');
}

std::string GetErrNameFromStatus(int i)
{
    if (i == HTTP_BAD_REQUEST) {
        return "invalid param in request";
    } else if (i == HTTP_BAD_REQUEST_FORBIDDEN) {
        return "Authentication error";
    } else if (i == HTTP_BAD_REQUEST_METHOD_NOT_ALLOWED) {
        return "http request method error";
    } else if (i == HTTP_BAD_REQUEST_INTERNAL_SERVER_ERROR) {
        return "yuanrong iam server not enable";
    } else if (i == HTTP_OK) {
        return "OK";
    } else {
        return "unknown error";
    }
}

Status YuanRongIAM::GetTslConfig()
{
    TlsConfig config;
    config.passPhrasePath = FLAGS_yuanrong_iam_passphrase;
    config.caPath = FLAGS_yuanrong_iam_ca;
    config.keyPath = FLAGS_yuanrong_iam_key;
    config.certPath = FLAGS_yuanrong_iam_cert;
    TlsInfo info;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SecretManager::Instance()->GetPemTlsInfo(config, info),
                                     "yuanrong iam get tls info failed");
    // read client cert
    ca_ = info.ca;
    cert_ = info.cert;
    key_ = info.key;
    return Status::OK();
}

Status YuanRongIAM::Init(bool &authEnable)
{
    if (authEnable) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsHttpUrl(FLAGS_yuanrong_iam_url), StatusCode::K_INVALID,
                                             "yuanrong_iam_url is invalid");
        if (FLAGS_yuanrong_iam_ca.empty() && FLAGS_yuanrong_iam_cert.empty() && FLAGS_yuanrong_iam_key.empty()) {
            httpAuth_ = false;
        };
        if (httpAuth_) {
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsNotEmpty(FLAGS_yuanrong_iam_ca), StatusCode::K_INVALID,
                                                 "yuanrong_iam_ca gflag can't be empty.");
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsNotEmpty(FLAGS_yuanrong_iam_cert), StatusCode::K_INVALID,
                                                 "yuanrong_iam_cert gflag can't be empty.");
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsNotEmpty(FLAGS_yuanrong_iam_key), StatusCode::K_INVALID,
                                                 "yuanrong_iam_key gflag can't be empty.");
            GetTslConfig();
        }
    }
    LOG(INFO) << "connect with yuanrong iam tls auth is : " << httpAuth_ << " encrypt kit is " << FLAGS_encrypt_kit;
    httpClient_ = std::make_shared<CurlHttpClient>(httpAuth_);
    return Status::OK();
}

Status YuanRongIAM::SetAuthorization(const std::shared_ptr<HttpRequest> &request)
{
    CHECK_FAIL_RETURN_STATUS(akSkManager_ != nullptr, StatusCode::K_INVALID, "AK/SK manager has not been initialized.");
    std::string canonicalRequest;
    RETURN_IF_NOT_OK(request->GetCanonicalRequest(true, canonicalRequest));
    std::string hashVal;
    Hasher hasher;
    RETURN_IF_NOT_OK(hasher.GetSha256Hex(canonicalRequest, hashVal));
    auto timestamp = GetUtcTimestamp();
    INJECT_POINT("YuanRongIAM.Authorization.SetTimestamp", [&timestamp](std::string ts) {
        timestamp = ts;
        return Status::OK();
    });
    std::string stringToSign = timestamp + " " + hashVal;
    std::string signature;
    RETURN_IF_NOT_OK(akSkManager_->GenerateSignature(stringToSign, signature));
    std::stringstream ss;
    ss << "HmacSha256 timestamp=" << timestamp << ",ak=" << akSkManager_->GetAccessKey() << ",signature=" << signature;
    request->AddHeader("X-Signed-Header", request->GetSignedHeaders());
    request->AddHeader("X-Signature", ss.str());
    return Status::OK();
}

Status YuanRongIAM::DecryptTLSKey(const std::string &keyPath, SensitiveValue &text, bool isEncrypted)
{
    char keyRealPath[PATH_MAX + 1] = { 0 };
    if (realpath(keyPath.c_str(), keyRealPath) == nullptr) {
        RETURN_STATUS_LOG_ERROR(
            K_INVALID, FormatString("Error: Invalid file path, Errno = %s, file path: %s", StrErr(errno), keyPath));
    }
    std::ifstream ifs(keyRealPath);
    if (!ifs.is_open()) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Error: Cannot open yuanrong iam key file.");
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    std::string ciphertext = buffer.str();
    if (isEncrypted) {
        int textSize;
        std::unique_ptr<char[]> tmpText;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SecretManager::Instance()->Decrypt(ciphertext, tmpText, textSize),
                                         "TLSKey decrypt failed.");
        text = SensitiveValue(std::move(tmpText), textSize);
        return Status::OK();
    }
    text = SensitiveValue(ciphertext);
    return Status::OK();
}

Status YuanRongIAM::GetAkSkAuthInfo(const std::shared_ptr<HttpResponse> &response,
                                    std::shared_ptr<std::stringstream> &respStream, SensitiveValue &secretKey,
                                    std::string &tenantId, uint64_t &expireSec, bool &isSystemRole)
{
    nlohmann::json jsonRes;
    try {
        jsonRes = nlohmann::json::parse(respStream->str());
    } catch (nlohmann::json::exception &e) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Failed to parse response to json and status code is %d. Exception: %s",
                                             response->GetStatus(), e.what()));
    }
    CHECK_FAIL_RETURN_STATUS(jsonRes.is_object(), StatusCode::K_RUNTIME_ERROR, "The response is not a json object.");
    auto secretPtr = jsonRes[RESPBODY_SECRETKEY].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(secretPtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find secretKey.");
    secretKey = SensitiveValue(*secretPtr);
    std::fill(secretPtr->begin(), secretPtr->end(), 0);

    auto tenantIdPtr = jsonRes[RESPBODY_TENANTID].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(tenantIdPtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find tenantId.");
    tenantId = *tenantIdPtr;

    auto expireTimePtr = jsonRes[RESPBODY_EXPIRED_TIME].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(expireTimePtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find expireTime.");
    std::string expiredString = *expireTimePtr;
    int32_t expiredTime;
    CHECK_FAIL_RETURN_STATUS(
        Uri::StrToInt32(expiredString.c_str(), expiredTime), K_INVALID,
        FormatString("AkSk expired time convert to long failed, expiredTime string: %s", expiredString));
    CHECK_FAIL_RETURN_STATUS(expiredTime >= 0, K_NOT_AUTHORIZED, "AkSk expired");
    expireSec = static_cast<uint64_t>(expiredTime);
    auto rolePtr = jsonRes[RESPBODY_ROLE].get_ptr<std::string *>();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rolePtr != nullptr, StatusCode::K_RUNTIME_ERROR,
                                         "The response cannot find role.");
    isSystemRole = (*rolePtr == "system");
    LOG(INFO) << FormatString("success verify the client AkSk, expire in %d seconds.", expireSec);
    return Status::OK();
}

namespace {
bool WorkerAuthInjection(const SensitiveValue &token, std::string &tenantId, uint64_t &expireSec)
{
    // for release version, avoid the compile warning of "unused parameter"
    (void)token;
    (void)tenantId;
    (void)expireSec;
    INJECT_POINT("worker.auth", [&token, &tenantId, &expireSec](const std::string &key, const std::string &val) {
        if (!token.Empty() && key == token.GetData()) {
            const uint64_t expireTime = 30 * 60;
            tenantId = val;
            LOG(INFO) << "12222";
            expireSec = expireTime;
            return true;
        }
        return false;
    });
    return false;
}

bool WorkerAkskInjection(const std::string &accessKey, SensitiveValue &secretKey, std::string &tenantId,
                         uint64_t &expireSec)
{
    (void)accessKey;
    (void)secretKey;
    (void)tenantId;
    (void)expireSec;
    INJECT_POINT("worker.akauth", [&accessKey, &secretKey, &tenantId, &expireSec](
                                      const std::string &key, const std::string &val1, const std::string &val2) {
        if (!accessKey.empty() && key == accessKey) {
            const uint64_t expireTime = 30 * 60;
            secretKey = SensitiveValue(val1);
            tenantId = val2;
            expireSec = expireTime;
            return true;
        }
        return false;
    });
    return false;
}
}  // namespace

Status YuanRongIAM::SendRequestAndRetry(
    std::function<void(std::shared_ptr<YuanRongVerifyTenantAuthReq> &)> setAuthHeader,
    std::shared_ptr<YuanRongVerifyTenantAuthReq> &request, std::shared_ptr<HttpResponse> &response)
{
    INJECT_POINT("SendRequestAndRetry.SendToken", [&response] {
        response->AddHeader(RESPONSE_EXPIRED_TIME, "-1");
        response->AddHeader(RESPONSE_TENANTID, "tenaant1");
        response->SetStatus(HTTP_OK);
        return Status::OK();
    });
    INJECT_POINT("SendRequestAndRetry.SendAkSk", [&response] {
        nlohmann::json body;
        body[RESPBODY_EXPIRED_TIME] = "-1";
        body[RESPBODY_TENANTID] = "tenant1";
        body[RESPBODY_SECRETKEY] = "secretkey1";
        body[RESPBODY_ROLE] = "normal";
        std::string json_str = body.dump(4);
        std::shared_ptr<std::iostream> body_stream = response->GetBody();
        body_stream->seekp(std::ios::beg);
        *body_stream << json_str;
        response->SetBody(body_stream);
        response->SetStatus(HTTP_OK);
        return Status::OK();
    });
    Timer timer;
    Status rc;
    std::vector<int64_t> retryIntervalSecMs = { 100, 500, 1500, 3000 };
    int64_t retryTimeMs = 15000;
    size_t retryTime = 0;
    int maxRetryMs = std::min(retryTimeMs, reqTimeoutDuration.CalcRemainingTime());
    while (timer.ElapsedMilliSecond() < maxRetryMs) {
        setAuthHeader(request);
        const int64_t requestTimeoutMs = 5000;
        request->SetConnectTimeoutMs(requestTimeoutMs);
        request->SetRequestTimeoutMs(requestTimeoutMs);
        if (httpAuth_) {
            TslParam param = { .ca_ = ca_, .cert_ = cert_, .key_ = key_ };
            rc = httpClient_->TslSend(request, response, param);
        } else {
            rc = httpClient_->Send(request, response);
        }
        INJECT_POINT("yuanrongIam.auth.failed", [&rc]() {
            rc = Status(K_RUNTIME_ERROR, "auth failed");
            return Status::OK();
        });
        if (rc.IsOk() && response->GetStatus() != HTTP_BAD_REQUEST_INTERNAL_SERVER_ERROR) {
            break;
        }
        LOG(ERROR) << "auth to iam-server failed, rc: " << rc.ToString() << " response code: " << response->GetStatus();
        int64_t retryInterval = retryTime < retryIntervalSecMs.size()
                                    ? retryIntervalSecMs[retryTime]
                                    : retryIntervalSecMs[retryIntervalSecMs.size() - 1];
        std::this_thread::sleep_for(std::chrono::milliseconds(retryInterval));
        retryTime++;
    }
    return rc;
}

Status YuanRongIAM::VerifyTenantToken(const SensitiveValue &token, std::string &tenantId, uint64_t &expireSec)
{
    if (WorkerAuthInjection(token, tenantId, expireSec)) {
        return Status::OK();
    }
    auto response = std::make_shared<HttpResponse>();
    auto respStream = std::make_shared<std::stringstream>();
    response->SetBody(respStream);
    auto request = std::make_shared<YuanRongVerifyTenantAuthReq>(token);
    RETURN_IF_NOT_OK_APPEND_MSG(SendRequestAndRetry(
                                    [&token](std::shared_ptr<YuanRongVerifyTenantAuthReq> &request) {
                                        request->AddHeader(REQUEST_AUTHKEY, token.GetData());
                                    },
                                    request, response),
                                " when verify client token.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        response->GetStatus() == HTTP_OK, StatusCode::K_RUNTIME_ERROR,
        FormatString("Failed to verify client token and status is %d. error message: %s", response->GetStatus(),
                     GetErrNameFromStatus(response->GetStatus())));
    tenantId = response->GetHeader(RESPONSE_TENANTID);
    std::string expiredString = response->GetHeader(RESPONSE_EXPIRED_TIME);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!tenantId.empty(), K_INVALID, "response tenantId is empty");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!expiredString.empty(), K_INVALID, "response expiredString is empty");
    int32_t expiredTime;
    CHECK_FAIL_RETURN_STATUS(
        Uri::StrToInt32(expiredString.c_str(), expiredTime), K_INVALID,
        FormatString("token expired time convert to long failed, expiredTime string: %s", expiredString));
    CHECK_FAIL_RETURN_STATUS(expiredTime > 0, K_NOT_AUTHORIZED, "token expried");
    expireSec = static_cast<uint64_t>(expiredTime);
    LOG(INFO) << FormatString("success verify the client token, expire in %d seconds.", expireSec);
    return Status::OK();
}

Status YuanRongIAM::VerifyTenantAkSk(const std::string &accessKey, SensitiveValue &secretKey, std::string &tenantId,
                                     uint64_t &expireSec, bool &isSystemRole)
{
    if (WorkerAkskInjection(accessKey, secretKey, tenantId, expireSec)) {
        return Status::OK();
    }
    auto response = std::make_shared<HttpResponse>();
    auto respStream = std::make_shared<std::stringstream>();
    response->SetBody(respStream);
    auto request = std::make_shared<YuanRongVerifyTenantAuthReq>(accessKey);
    RETURN_IF_NOT_OK_APPEND_MSG(SetAuthorization(request), " set authorization header failed.");
    RETURN_IF_NOT_OK_APPEND_MSG(SendRequestAndRetry(
                                    [&accessKey, &tenantId](std::shared_ptr<YuanRongVerifyTenantAuthReq> &request) {
                                        request->AddHeader(REQUEST_AUTHKEY, accessKey);
                                        request->AddHeader(RESPONSE_TENANTID, tenantId);
                                    },
                                    request, response),
                                " when verify client AkSk.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        response->GetStatus() == HTTP_OK, StatusCode::K_RUNTIME_ERROR,
        FormatString("Failed to verify client AkSk and status is %d. error message: %s", response->GetStatus(),
                     GetErrNameFromStatus(response->GetStatus())));
    return GetAkSkAuthInfo(response, respStream, secretKey, tenantId, expireSec, isSystemRole);
}
}  // namespace datasystem
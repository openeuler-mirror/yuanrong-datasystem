/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Interface to OBS via HTTP REST API.
 */

#include "datasystem/common/l2cache/obs_client/obs_client.h"

#include <chrono>
#include <fstream>
#include <limits>
#include <string>

#include <nlohmann/json.hpp>
#include <securec.h>

#include "datasystem/common/httpclient/curl_http_client.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/res_metric_collector.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/l2cache/get_object_info_list_resp.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/status.h"

DS_DEFINE_string(obs_access_key, "", "The access key for obs AK/SK authentication.");
DS_DEFINE_string(obs_secret_key, "", "The secret key for obs AK/SK authentication.");
DS_DEFINE_string(obs_endpoint, "", "The endpoint to connect to for obs");
DS_DEFINE_string(obs_bucket, "", "The single obs bucket to use");
DS_DEFINE_bool(enable_cloud_service_token_rotation, false,
               "Enable the OBS client to access OBS using a temporary token. After the token expires, obtain a new "
               "token and connect to OBS again.");
DS_DEFINE_bool(obs_https_enabled, false,
               "Whether to enable the https in obs. false: use HTTP (default), true: use HTTPS");
DS_DECLARE_string(encrypt_kit);

const std::string CSMS_TOKEN_PATH = "/var/run/secrets/tokens/csms-token";
const std::string TOKEN_ROTATION_CONFIG = "TOKEN_ROTATION_CONFIG";
const size_t MAX_OBS_TOKEN_TIMEOUT_SEC = 86400;  // 24 hours.
const size_t MIN_OBS_TOKEN_TIMEOUT_SEC = 900;    // 15 minutes.
const size_t ROTATION_DEFAULT_INTERVAL = 43200;  // 12 hours.
const size_t CONFIG_VALID_STRING_LEN = 128;
const int64_t OBS_DEFAULT_TIMEOUT_MS = 30000;    // 30 seconds default timeout for OBS requests.

namespace datasystem {
inline bool ValidateConfigString(const std::string &key, const std::string &value)
{
    if (value.size() > CONFIG_VALID_STRING_LEN || !Validator::ValidateEligibleChar(key.c_str(), value)) {
        LOG(ERROR) << FormatString("The value %s of %s is invalid.", value, key);
        return false;
    }
    return true;
}

inline bool ValidateConfigInt(const std::string &key, size_t value)
{
    if (value < MIN_OBS_TOKEN_TIMEOUT_SEC || value > MAX_OBS_TOKEN_TIMEOUT_SEC) {
        LOG(ERROR) << FormatString("The value %zu of %s is invalid.", value, key);
        return false;
    }
    return true;
}

inline Status VerifyJsonConfig(const nlohmann::json &jsonConfig, CCMSTokenConf &ccmsConfig)
{
    for (const auto &kv : jsonConfig.items()) {
        if (kv.key() == "projectID") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "string"
                                         && ValidateConfigString(kv.key(), kv.value().get<std::string>()),
                                     K_INVALID, "Failed to verify projectId.");
            ccmsConfig.projectID = kv.value().get<std::string>();
        } else if (kv.key() == "identityProvider") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "string"
                                         && ValidateConfigString(kv.key(), kv.value().get<std::string>()),
                                     K_INVALID, "Failed to verify identityProvider.");
            ccmsConfig.identityProvider = kv.value().get<std::string>();
        } else if (kv.key() == "iamHostName") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "string"
                                         && ValidateConfigString(kv.key(), kv.value().get<std::string>()),
                                     K_INVALID, "Failed to verify iamHostName.");
            ccmsConfig.iamHostName = kv.value().get<std::string>();
        } else if (kv.key() == "enableTokenByAgency") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "boolean", K_INVALID,
                                     "Failed to verify enableTokenByAgency.");
            ccmsConfig.enableTokenByAgency = kv.value().get<bool>();
        } else if (kv.key() == "tokenTTLSeconds") {
            CHECK_FAIL_RETURN_STATUS(
                std::string(kv.value().type_name()) == "number" && ValidateConfigInt(kv.key(), kv.value().get<int>()),
                K_INVALID, "Failed to verify tokenTTLSeconds.");
            ccmsConfig.tokenTTLSeconds = kv.value().get<int>();
        } else if (kv.key() == "tokenAgencyName") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "string"
                                         && ValidateConfigString(kv.key(), kv.value().get<std::string>()),
                                     K_INVALID, "Failed to verify tokenAgencyName.");
            ccmsConfig.tokenAgencyName = kv.value().get<std::string>();
        } else if (kv.key() == "tokenAgencyDomain") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "string"
                                         && ValidateConfigString(kv.key(), kv.value().get<std::string>()),
                                     K_INVALID, "Failed to verify tokenAgencyDomain.");
            ccmsConfig.tokenAgencyDomain = kv.value().get<std::string>();
        } else if (kv.key() == "regionID") {
            CHECK_FAIL_RETURN_STATUS(std::string(kv.value().type_name()) == "string"
                                         && ValidateConfigString(kv.key(), kv.value().get<std::string>()),
                                     K_INVALID, "Failed to verify regionID.");
            ccmsConfig.regionID = kv.value().get<std::string>();
        }
    }
    return Status::OK();
}

ObsClient::~ObsClient()
{
    if (isTokenRotationStarting_.load()) {
        isTokenRotationStarting_.store(false);
        rotationCv_.notify_all();
        if (rotationThread_.joinable()) {
            rotationThread_.join();
        }
    }
}

Status ObsClient::Init()
{
    if (initialized_.load()) {
        return Status::OK();
    }
    httpClient_ = std::make_shared<CurlHttpClient>(false);
    Status status;
    if (FLAGS_enable_cloud_service_token_rotation) {
        status = ObsClientInitByToken();
    } else {
        status = ObsClientInitByAkSk();
    }
    CHECK_FAIL_RETURN_STATUS(status.IsOk(), K_RUNTIME_ERROR, "OBS client init failed. " + status.ToString());
    initialized_.store(true);
    LOG(INFO) << "ObsClient is initialized.";
    return Status::OK();
}

Status ObsClient::ObsClientInitByAkSk()
{
    CHECK_FAIL_RETURN_STATUS(!bucketName_.empty(), K_INVALID, "OBS bucket name is empty");
    CHECK_FAIL_RETURN_STATUS(!endPoint_.empty(), K_INVALID, "OBS endpoint is empty");
    CHECK_FAIL_RETURN_STATUS(!FLAGS_obs_access_key.empty(), K_INVALID, "FLAGS_obs_access_key is empty");
    CHECK_FAIL_RETURN_STATUS(!FLAGS_obs_secret_key.empty(), K_INVALID, "FLAGS_obs_secret_key is empty");
    if (!credentialManager_.Init()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "ObsCredentialManager initialization failed.");
    }
    threadPool_ = std::make_unique<ThreadPool>(NUM_THREAD, 0, "ObsClient");
    return Status::OK();
}

Status ObsClient::ObsClientInitByToken()
{
    CHECK_FAIL_RETURN_STATUS(!bucketName_.empty(), K_INVALID, "OBS bucket name is empty");
    CHECK_FAIL_RETURN_STATUS(!endPoint_.empty(), K_INVALID, "OBS endpoint is empty");
    rotationIntervalSec_ = ROTATION_DEFAULT_INTERVAL;  // default is 12 hours.
    RETURN_IF_NOT_OK(TokenRotationInit());
    Raii cleanRaii([this] {
        obsTempCredentialInfo_.securityToken.Clear();
        obsTempCredentialInfo_.access.Clear();
        obsTempCredentialInfo_.secret.Clear();
    });
    if (!credentialManager_.Init()) {
        RETURN_STATUS(K_RUNTIME_ERROR, "ObsCredentialManager initialization failed.");
    }
    threadPool_ = std::make_unique<ThreadPool>(NUM_THREAD, 0, "ObsClient");
    isTokenRotationStarting_.store(true);
    rotationThread_ = Thread(&ObsClient::StartTokenRotation, this);
    return Status::OK();
}

Status ObsClient::TokenRotationInit()
{
    SensitiveValue csmsToken;
    RETURN_IF_NOT_OK(ReadCSMSToken(csmsToken));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!csmsToken.Empty(), K_INVALID, "Csms token is empty.");
    CCMSTokenConf ccmsConf;
    ccmsConf.oidcToken = std::move(csmsToken);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadConfigFromEnv(ccmsConf), "Can not read ccms config.");
    if (ccmsConf.tokenTTLSeconds > 0) {
        size_t halfInterval = 2;
        rotationIntervalSec_ = ccmsConf.tokenTTLSeconds / halfInterval;
    }

    auto ccmsRotationAccessToken = std::make_unique<CCMSRotationAccessToken>(ccmsConf);
    INJECT_POINT("ObsClient.TokenRotationInit.FirstInitCredentialInfo",
                 [this](const std::string &ak, const std::string &sk) {
                     obsTempCredentialInfo_.access = ak;
                     obsTempCredentialInfo_.secret = sk;
                     obsTempCredentialInfo_.securityToken = "ORIGIN_MOCK_TOKEN";
                     return Status::OK();
                 });
    INJECT_POINT("ObsClient.TokenRotationInit.UpdateInitCredentialInfo",
                 [this](const std::string &ak, const std::string &sk) {
                     obsTempCredentialInfo_.access = ak;
                     obsTempCredentialInfo_.secret = sk;
                     obsTempCredentialInfo_.securityToken = "UPDATE_MOCK_TOKEN";
                     return Status::OK();
                 });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ccmsRotationAccessToken->UpdateAccessToken(obsTempCredentialInfo_),
                                     "Access obs temp credential info failed.");
    return Status::OK();
}

Status ObsClient::ReadCSMSToken(SensitiveValue &csmsToken)
{
    errno = 0;
    std::string filePath = CSMS_TOKEN_PATH;
    std::ifstream csmsIfs(filePath);
    Raii closeRaii([&csmsIfs] { csmsIfs.close(); });
    // Inject for test
    INJECT_POINT("ObsClient.ReadCSMSToken.readTestOidcToken", [&csmsIfs, &filePath](const std::string &oidcPath) {
        csmsIfs.close();
        filePath = oidcPath;
        csmsIfs.open(filePath);
        return Status::OK();
    });
    if (!csmsIfs.is_open()) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Error: Cannot open csms token file, errno = %d", errno));
    }
    const uintmax_t maxFileSize = 10 * 1024 * 1024;  // 10MB
    auto fileSize = FileSize(filePath);
    CHECK_FAIL_RETURN_STATUS(fileSize >= 0, K_RUNTIME_ERROR, "Get file size failed");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(static_cast<uintmax_t>(fileSize) <= maxFileSize, K_RUNTIME_ERROR,
                                         "Csms file exceed max file size.");
    std::stringstream buffer;
    buffer << csmsIfs.rdbuf();
    csmsToken = buffer.str();
    return Status::OK();
}

Status ObsClient::ReadConfigFromEnv(CCMSTokenConf &ccmsConfig)
{
    std::string rotationConfig = GetStringFromEnv(TOKEN_ROTATION_CONFIG.c_str(), "");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!rotationConfig.empty(), K_RUNTIME_ERROR, "Can not read ccms config from env");
    nlohmann::json jsonConfig;
    try {
        jsonConfig = nlohmann::json::parse(rotationConfig);
    } catch (nlohmann::json::exception &e) {
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR,
                                FormatString("Failed to parse ccms json config. Exception: %s", e.what()));
    }
    CHECK_FAIL_RETURN_STATUS(jsonConfig.is_object(), StatusCode::K_RUNTIME_ERROR,
                             "The jsonConfig is not a json object.");
    return VerifyJsonConfig(jsonConfig, ccmsConfig);
}

Status ObsClient::UpdateTempObsToken()
{
    Raii cleanRaii([this] {
        obsTempCredentialInfo_.securityToken.Clear();
        obsTempCredentialInfo_.access.Clear();
        obsTempCredentialInfo_.secret.Clear();
    });
    RETURN_IF_NOT_OK(TokenRotationInit());
    return credentialManager_.UpdateCredentialInfo();
}

Status ObsClient::Upload(const std::string &objectPath, int64_t timeoutMs, const std::shared_ptr<std::iostream> &body,
                         uint64_t asyncElapse)
{
    (void)asyncElapse;                                                 // not used in ObsClient
    static const size_t multipartUploadThreshold = 100 * 1024 * 1024;  // 100MB
    size_t sz = static_cast<size_t>(GetSize(body.get()));
    Timer timer(timeoutMs);
    if (sz <= multipartUploadThreshold) {
        RETURN_IF_NOT_OK(StreamingUpload(body, sz, objectPath, timer));
    } else {
        static const size_t partitionSize = 10 * 1024 * 1024;  // 10MB
        RETURN_IF_NOT_OK(MultiPartUpload(body, sz, objectPath, partitionSize, timer));
    }
    return Status::OK();
}

Status ObsClient::Download(const std::string &objectPath, int64_t timeoutMs,
                           std::shared_ptr<std::stringstream> &content)
{
    Timer timer(timeoutMs);
    return GetObject(objectPath, content, timer);
}

Status ObsClient::Delete(const std::vector<std::string> &objectPaths, uint64_t asyncElapse)
{
    (void)asyncElapse;
    // Can delete 1000 objects at most each time.
    static const size_t limit = 1000;
    size_t numBatch = objectPaths.size() / limit;
    numBatch += (objectPaths.size() % limit == 0u ? 0u : 1u);
    std::vector<std::pair<size_t, size_t>> failedRanges;
    Status rc;
    size_t i = 1;
    for (; i < numBatch; ++i) {
        rc = BatchDeleteObjects(objectPaths, limit * (i - 1), limit * i);
        if (rc.IsError()) {
            failedRanges.emplace_back(limit * (i - 1), limit * i);
        }
    }
    rc = BatchDeleteObjects(objectPaths, limit * (i - 1), objectPaths.size());
    if (rc.IsError()) {
        failedRanges.emplace_back(limit * (i - 1), objectPaths.size());
    }
    if (!failedRanges.empty()) {
        std::stringstream ss;
        ss << "Failed to delete objects:\n";
        for (auto p : failedRanges) {
            for (size_t j = p.first; j < p.second; ++j) {
                ss << objectPaths[j] << "\n";
            }
        }
        ss << "\n" << rc.GetMsg();
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, ss.str());
    }
    return Status::OK();
}

Status ObsClient::List(const std::string &objectPrefix, int64_t timeoutMs, bool listIncompleteVersions,
                       std::shared_ptr<GetObjectInfoListResp> &listResp)
{
    (void)listIncompleteVersions;
    Timer timer(timeoutMs);
    return ListObjects(objectPrefix, timer, listResp);
}

// ========================
// HTTP REST API operations
// ========================

std::shared_ptr<HttpRequest> ObsClient::BuildRequest(HttpMethod method, const std::string &objectKey,
                                                     int64_t timeoutMs,
                                                     const std::map<std::string, std::string> &subResources)
{
    std::string scheme = FLAGS_obs_https_enabled ? "https://" : "http://";
    std::string host;
    std::string path;
    if (FLAGS_obs_https_enabled) {
        // Virtual-hosted-style URL: https://bucket.endpoint/objectKey
        host = bucketName_ + "." + endPoint_;
        path = objectKey.empty() ? "/" : "/" + objectKey;
    } else {
        // Path-style URL: http://endpoint/bucket/objectKey
        host = endPoint_;
        path = "/" + bucketName_ + "/" + objectKey;
    }
    std::string url = scheme + host + path;
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl(std::move(url));
    request->SetMethod(std::move(method));
    request->SetRequestTimeoutMs(timeoutMs);
    request->SetConnectTimeoutMs(timeoutMs);
    for (const auto &param : subResources) {
        request->AddQueryParam(param.first, param.second);
    }
    request->ConcatenateQueryParams();
    return request;
}

Status ObsClient::SignRequest(const ObsCredential &credential, std::shared_ptr<HttpRequest> &request,
                              const std::string &contentMd5,
                              const std::map<std::string, std::string> &subResources)
{
    std::string method = HttpRequest::HttpMethodStr(request->GetMethod());
    std::string date = ObsSignature::FormatDateRFC1123();
    request->AddHeader("Date", date);

    std::string contentType = request->GetHeader("Content-Type");

    // Collect x-obs-* headers for signing (sorted map)
    std::map<std::string, std::string> obsHeaders;
    for (const auto &hdr : request->Headers()) {
        if (hdr.first.find("x-obs-") == 0) {
            obsHeaders[hdr.first] = hdr.second;
        }
    }

    // Add token header if using token rotation
    if (!credential.token.empty()) {
        request->AddHeader("x-obs-security-token", credential.token);
        obsHeaders["x-obs-security-token"] = credential.token;
    }

    // Extract objectKey from URL for canonical resource
    std::string objectKey;
    const auto &url = request->GetUrl();
    std::string scheme = FLAGS_obs_https_enabled ? "https://" : "http://";
    std::string prefix;
    if (FLAGS_obs_https_enabled) {
        prefix = scheme + bucketName_ + "." + endPoint_ + "/";
    } else {
        prefix = scheme + endPoint_ + "/" + bucketName_ + "/";
    }
    if (url.size() > prefix.size()) {
        objectKey = url.substr(prefix.size());
        auto qpos = objectKey.find('?');
        if (qpos != std::string::npos) {
            objectKey = objectKey.substr(0, qpos);
        }
    }

    std::string canonicalResource = ObsSignature::BuildCanonicalResource(bucketName_, objectKey, subResources);

    std::string stringToSign = ObsSignature::BuildStringToSign(method, contentMd5, contentType, date, obsHeaders,
                                                               canonicalResource);
    std::string signature;
    RETURN_IF_NOT_OK(ObsSignature::Sign(credential.sk, stringToSign, signature));
    request->AddHeader("Authorization", ObsSignature::BuildAuthHeader(credential.ak, signature));
    return Status::OK();
}

Status ObsClient::StreamingUpload(const std::shared_ptr<std::iostream> &body, size_t size, const std::string &objPath,
                                  Timer &timer)
{
    LOG(INFO) << FormatString("Streaming upload starts. Object path: %s.", objPath);
    ObsCredential credential = credentialManager_.GetCredential();
    int64_t remainingTime = timer.GetRemainingTimeMs();
    auto request = BuildRequest(HttpMethod::PUT, objPath, remainingTime);
    request->AddHeader("Content-Type", "application/octet-stream");
    request->SetBody(body);

    RETURN_IF_NOT_OK(SignRequest(credential, request, "", {}));

    std::shared_ptr<HttpResponse> response;
    int httpStatus = 0;
    INJECT_POINT("ObsClient.StreamingUpload.ObsUploadFailed", [&httpStatus]() {
        httpStatus = 500;
        return Status::OK();
    });
    INJECT_POINT("ObsClient.StreamingUpload.ObsNoSuchKey", [&httpStatus]() {
        httpStatus = 404;
        return Status::OK();
    });
    if (httpStatus == 0) {
        RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, remainingTime, response));
        httpStatus = response->GetStatus();
    }
    successRateVec_.BlockingEmplaceBackCode(httpStatus);
    if (httpStatus >= 200 && httpStatus < 300) {
        LOG(INFO) << FormatString("Putting object to OBS is done. Object path: %s", objPath);
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR,
                            FormatString("Failed to put object: %s, buffer size: %zu, http status: %d", objPath, size,
                                         httpStatus));
}

Status ObsClient::InitMultiPartUpload(const std::string &objPath, Timer &timer, std::string &uploadId)
{
    ObsCredential credential = credentialManager_.GetCredential();
    int64_t remaining = timer.GetRemainingTimeMs();
    std::map<std::string, std::string> subResources = { {"uploads", ""} };
    auto request = BuildRequest(HttpMethod::POST, objPath, remaining, subResources);
    request->AddHeader("Content-Type", "application/octet-stream");

    RETURN_IF_NOT_OK(SignRequest(credential, request, "", subResources));

    std::shared_ptr<HttpResponse> response;
    RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, remaining, response));
    int httpStatus = response->GetStatus();
    CHECK_FAIL_RETURN_STATUS(httpStatus >= 200 && httpStatus < 300, K_RUNTIME_ERROR,
                             FormatString("Initiate multipart upload failed. Object: %s, http status: %d", objPath,
                                          httpStatus));

    // Parse UploadId from response body
    std::string respBody;
    auto &respStream = response->GetBody();
    if (respStream != nullptr) {
        std::ostringstream oss;
        oss << respStream->rdbuf();
        respBody = oss.str();
    }
    uploadId = ObsXmlUtil::ParseInitiateMultipartResponse(respBody);
    CHECK_FAIL_RETURN_STATUS(!uploadId.empty(), K_RUNTIME_ERROR,
                             FormatString("Failed to parse UploadId from response. Object: %s, body: %s", objPath,
                                          respBody));
    return Status::OK();
}

Status ObsClient::MultiPartUpload(const std::shared_ptr<std::iostream> &body, size_t size, const std::string &objPath,
                                  size_t partitionSize, Timer &timer)
{
    LOG(INFO) << FormatString("Multipart upload starts. Object path: %s.", objPath);

    // Initialize multipart upload
    std::string uploadId;
    RETURN_IF_NOT_OK(InitMultiPartUpload(objPath, timer, uploadId));

    if (size / partitionSize > std::numeric_limits<int>::max()) {
        RETURN_STATUS(StatusCode::K_INVALID, "size too large: " + std::to_string(size));
    }
    // Concurrent upload
    MultiPartUploadBuffer multiUploadBuf;
    multiUploadBuf.buffer = body;
    multiUploadBuf.noStatus = 1;
    multiUploadBuf.partSize = partitionSize;
    multiUploadBuf.partNum =
        static_cast<int>((size % partitionSize == 0) ? (size / partitionSize) : (size / partitionSize + 1));
    std::vector<OnePartUploadBuffer> parts(multiUploadBuf.partNum, OnePartUploadBuffer());
    RETURN_IF_NOT_OK(SubmitUploadThreads(multiUploadBuf, objPath, size, uploadId, parts));

    // Complete upload
    ObsCredential credential = credentialManager_.GetCredential();
    int64_t remaining = timer.GetRemainingTimeMs();
    std::map<std::string, std::string> subResources = { {"uploadId", uploadId} };
    auto request = BuildRequest(HttpMethod::POST, objPath, remaining, subResources);
    request->AddHeader("Content-Type", "application/xml");

    // Build complete multipart XML body
    std::vector<std::pair<int, std::string>> partInfos;
    for (int i = 0; i < multiUploadBuf.partNum; ++i) {
        partInfos.emplace_back(static_cast<int>(parts[i].partNum), parts[i].eTag);
    }
    std::string xmlBody = ObsXmlUtil::BuildCompleteMultipartXml(partInfos);
    auto xmlStream = std::make_shared<std::stringstream>(xmlBody);
    request->SetBody(xmlStream);

    // Calculate Content-MD5 for the XML body
    std::string md5Base64;
    RETURN_IF_NOT_OK(Hasher::GetMD5Base64(xmlBody, md5Base64));
    request->AddHeader("Content-MD5", md5Base64);

    RETURN_IF_NOT_OK(SignRequest(credential, request, md5Base64, subResources));

    std::shared_ptr<HttpResponse> response;
    RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, remaining, response));
    int httpStatus = response->GetStatus();
    successRateVec_.BlockingEmplaceBackCode(httpStatus);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        httpStatus >= 200 && httpStatus < 300, K_RUNTIME_ERROR,
        FormatString("Complete multipart upload failed. Object: %s, buffer size: %zu, http status: %d", objPath, size,
                     httpStatus));
    LOG(INFO) << FormatString("Uploading object to OBS is done. Object path: %s", objPath);

    return Status::OK();
}

Status ObsClient::SubmitUploadThreads(const MultiPartUploadBuffer &multiPartBuffer, const std::string &objPath,
                                      size_t bufferSize, const std::string &uploadId,
                                      std::vector<OnePartUploadBuffer> &parts)
{
    std::vector<std::future<Status>> results;
    // Use lock to ensure the upload tasks of the same object are submitted to thread pool together
    std::unique_lock<std::mutex> lk(multiPartUploadMx_);

    auto job = [this, &objPath, &uploadId, &parts, &multiPartBuffer, bufferSize](int i) {
        auto startTime = std::chrono::steady_clock::now();
        int64_t limit = static_cast<int64_t>(parts[i].partSize) * 2;  // generous timeout
        Status rc = Status::OK();
        int64_t elapsed = 0;
        do {
            rc = OnePartUpload(parts[i]);
            if (rc.GetCode() != K_RUNTIME_ERROR) {
                return rc;
            }
            elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startTime)
                    .count();
        } while (elapsed < limit);
        return rc;
    };

    for (int i = 0; i < multiPartBuffer.partNum; ++i) {
        OnePartUploadBuffer &part = parts[i];
        part.buffer = multiPartBuffer.buffer;
        part.partNum = i + 1;
        part.uploadId = uploadId;
        part.key = objPath;
        if (i != multiPartBuffer.partNum - 1) {
            part.partSize = multiPartBuffer.partSize;
        } else {
            part.partSize = bufferSize - multiPartBuffer.partSize * i;
        }
        part.offset = multiPartBuffer.partSize * i;
        part.mx = &multiPartBuffer.mx;

        results.emplace_back(threadPool_->Submit(job, i));
    }
    lk.unlock();

    for (int i = 0; i < multiPartBuffer.partNum; ++i) {
        results[i].wait();
    }
    for (int i = 0; i < multiPartBuffer.partNum; ++i) {
        Status rc = results[i].get();
        if (rc.IsError()) {
            // Abort the multipart upload on failure
            (void)AbortMultipartUpload(objPath, uploadId);
            RETURN_STATUS(K_RUNTIME_ERROR,
                          FormatString("Failed to Upload object: %s, err msg: %s", objPath, rc.GetMsg()));
        }
    }

    return Status::OK();
}

Status ObsClient::OnePartUpload(OnePartUploadBuffer &onePartUploadBuffer)
{
    bool setPoint = false;
    INJECT_POINT("ObsClient.OnePartUpload.sleepReturnFailure", [&setPoint](int sleepTimeMs) {
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        setPoint = true;
        return Status::OK();
    });
    if (setPoint) {
        RETURN_STATUS(K_RUNTIME_ERROR, "Runtime error for test.");
    }

    ObsCredential credential = credentialManager_.GetCredential();
    std::map<std::string, std::string> subResources = { {"partNumber", std::to_string(onePartUploadBuffer.partNum)},
                                                        {"uploadId", onePartUploadBuffer.uploadId} };
    int64_t timeoutMs = std::max(OBS_DEFAULT_TIMEOUT_MS,
                                  static_cast<int64_t>(onePartUploadBuffer.partSize) * 2);
    auto request = BuildRequest(HttpMethod::PUT, onePartUploadBuffer.key, timeoutMs, subResources);
    request->AddHeader("Content-Type", "application/octet-stream");

    // Read the part data from the shared buffer
    std::string partData;
    partData.resize(onePartUploadBuffer.partSize);
    {
        std::unique_lock<std::mutex> lk(*onePartUploadBuffer.mx);
        onePartUploadBuffer.buffer->seekg(onePartUploadBuffer.offset);
        onePartUploadBuffer.buffer->read(&partData[0], onePartUploadBuffer.partSize);
        auto bytesRead = onePartUploadBuffer.buffer->gcount();
        partData.resize(bytesRead);
    }
    auto partStream = std::make_shared<std::stringstream>(partData);
    request->SetBody(partStream);

    RETURN_IF_NOT_OK(SignRequest(credential, request, "", subResources));

    std::shared_ptr<HttpResponse> response;
    RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, timeoutMs, response));
    int httpStatus = response->GetStatus();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        httpStatus >= 200 && httpStatus < 300, K_RUNTIME_ERROR,
        FormatString("Upload one part failed. Object path: %s, upload ID: %s, part size: %zu, part number: %zu, "
                     "http status: %d",
                     onePartUploadBuffer.key, onePartUploadBuffer.uploadId, onePartUploadBuffer.partSize,
                     onePartUploadBuffer.partNum, httpStatus));

    // Extract ETag from response header
    const auto &headers = response->Headers();
    auto etagIt = headers.find("ETag");
    if (etagIt != headers.end()) {
        onePartUploadBuffer.eTag = etagIt->second;
    } else {
        etagIt = headers.find("etag");
        if (etagIt != headers.end()) {
            onePartUploadBuffer.eTag = etagIt->second;
        }
    }

    LOG(INFO) << FormatString("Uploading one part is done. Object path: %s, part number: %zu",
                              onePartUploadBuffer.key, onePartUploadBuffer.partNum);

    return Status::OK();
}

Status ObsClient::AbortMultipartUpload(const std::string &objectPath, const std::string &uploadId)
{
    ObsCredential credential = credentialManager_.GetCredential();
    std::map<std::string, std::string> subResources = { {"uploadId", uploadId} };
    auto request = BuildRequest(HttpMethod::DELETE, objectPath, OBS_DEFAULT_TIMEOUT_MS, subResources);

    RETURN_IF_NOT_OK(SignRequest(credential, request, "", subResources));

    std::shared_ptr<HttpResponse> response;
    Status rc = SendObsRequest(httpClient_, request, OBS_DEFAULT_TIMEOUT_MS, response);
    if (rc.IsError()) {
        LOG(WARNING) << FormatString("Abort multipart upload failed. Object: %s, uploadId: %s, status: %s",
                                     objectPath, uploadId, rc.ToString());
    }
    return Status::OK();
}

Status ObsClient::GetObject(const std::string &objPath, std::shared_ptr<std::stringstream> &buf, Timer &timer)
{
    LOG(INFO) << FormatString("GetObject starts. Object path: %s", objPath);
    ObsCredential credential = credentialManager_.GetCredential();
    int64_t remaining = timer.GetRemainingTimeMs();
    auto request = BuildRequest(HttpMethod::GET, objPath, remaining);
    request->AddHeader("Content-Type", "application/octet-stream");

    RETURN_IF_NOT_OK(SignRequest(credential, request, "", {}));

    std::shared_ptr<HttpResponse> response;
    RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, remaining, response));
    int httpStatus = response->GetStatus();
    successRateVec_.BlockingEmplaceBackCode(httpStatus);
    if (httpStatus == 404) {
        RETURN_STATUS_LOG_ERROR(K_NOT_FOUND, FormatString("Failed to get object: %s, http status: %d", objPath,
                                                           httpStatus));
    }
    if (httpStatus < 200 || httpStatus >= 300) {
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Failed to get object: %s, http status: %d", objPath,
                                                               httpStatus));
    }
    // Copy response body to output buffer
    auto &respBody = response->GetBody();
    buf = std::make_shared<std::stringstream>();
    if (respBody != nullptr) {
        *buf << respBody->rdbuf();
    }
    LOG(INFO) << FormatString("Getting object is done. Object path: %s.", objPath);

    return Status::OK();
}

Status ObsClient::SendListObjectsRequest(const std::string &objectPrefix, const std::string &marker,
                                         uint16_t maxKeys, int64_t timeoutMs, const ObsCredential &credential,
                                         std::string &respBody)
{
    std::string queryString = "?prefix=" + objectPrefix + "&max-keys=" + std::to_string(maxKeys);
    if (!marker.empty()) {
        queryString += "&marker=" + marker;
    }
    std::string scheme = FLAGS_obs_https_enabled ? "https://" : "http://";
    std::string url;
    if (FLAGS_obs_https_enabled) {
        url = scheme + bucketName_ + "." + endPoint_ + "/" + queryString;
    } else {
        url = scheme + endPoint_ + "/" + bucketName_ + "/" + queryString;
    }
    auto request = std::make_shared<HttpRequest>();
    request->SetUrl(std::move(url));
    request->SetMethod(HttpMethod::GET);
    request->SetRequestTimeoutMs(timeoutMs);
    request->SetConnectTimeoutMs(timeoutMs);
    request->AddHeader("Content-Type", "application/xml");

    std::map<std::string, std::string> subResources;
    RETURN_IF_NOT_OK(SignRequest(credential, request, "", subResources));

    std::shared_ptr<HttpResponse> response;
    RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, timeoutMs, response));
    int httpStatus = response->GetStatus();
    successRateVec_.BlockingEmplaceBackCode(httpStatus);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(httpStatus >= 200 && httpStatus < 300, K_RUNTIME_ERROR,
                                         FormatString("Failed to list objects. Prefix: %s, http status: %d",
                                                      objectPrefix, httpStatus));

    auto &respStream = response->GetBody();
    if (respStream != nullptr) {
        std::ostringstream oss;
        oss << respStream->rdbuf();
        respBody = oss.str();
    }
    return Status::OK();
}

Status ObsClient::ListObjects(const std::string &objectPrefix, Timer &timer,
                              std::shared_ptr<GetObjectInfoListResp> &listResp)
{
    LOG(INFO) << FormatString("ListObjects starts. Prefix: %s", objectPrefix);
    CHECK_FAIL_RETURN_STATUS(listResp != nullptr, K_INVALID, "Must provide GetObjectInfoListResp");
    ObsCredential credential = credentialManager_.GetCredential();
    int64_t remaining = timer.GetRemainingTimeMs();
    std::string nextMarker;
    static const uint16_t maxNumObj = 1000;
    int keyCount = 0;
    do {
        std::string respBody;
        RETURN_IF_NOT_OK(SendListObjectsRequest(objectPrefix, nextMarker, maxNumObj, remaining, credential, respBody));

        std::vector<std::string> keys;
        std::vector<uint64_t> sizes;
        bool isTruncated = false;
        std::string parsedNextMarker;
        RETURN_IF_NOT_OK(ObsXmlUtil::ParseListObjectsResponse(respBody, keys, sizes, isTruncated, parsedNextMarker));

        ListObjectData listObjData;
        listObjData.isTruncated = isTruncated ? 1 : 0;
        listObjData.nextMarker = parsedNextMarker;
        listObjData.keyCount = static_cast<int>(keys.size());
        for (size_t idx = 0; idx < keys.size(); ++idx) {
            listObjData.objects.emplace_back(keys[idx], 0, "", sizes[idx], "", "", "", "");
        }
        keyCount = listObjData.keyCount;
        if (!parsedNextMarker.empty()) {
            nextMarker = parsedNextMarker;
        }
        listResp->FillInListObjectData(listObjData);
    } while (keyCount == maxNumObj);
    LOG(INFO) << FormatString("Listing objects is done. Prefix: %s.", objectPrefix);
    return Status::OK();
}

Status ObsClient::BatchDeleteObjects(const std::vector<std::string> &objects, size_t beg, size_t end)
{
    LOG(INFO) << FormatString("BatchDeleteObjects starts. Begin: %zu, end: %zu.", beg, end);
    if (beg == end) {
        return Status::OK();
    }
    // Can delete 1000 objects at most each time.
    static const size_t limit = 1000;
    CHECK_FAIL_RETURN_STATUS(beg < end, K_INVALID, "The end index of the range should be greater than the begin.");
    CHECK_FAIL_RETURN_STATUS(end <= objects.size(), K_INVALID, "The end of the range is out of the vector.");
    CHECK_FAIL_RETURN_STATUS(end - beg <= limit, K_INVALID, "Batch size is at most 1000.");
    ObsCredential credential = credentialManager_.GetCredential();

    std::vector<std::string> keysToDelete(objects.begin() + beg, objects.begin() + end);
    std::string xmlBody = ObsXmlUtil::BuildBatchDeleteXml(keysToDelete);
    auto bodyStream = std::make_shared<std::stringstream>(xmlBody);

    std::map<std::string, std::string> subResources = { {"delete", ""} };
    auto request = BuildRequest(HttpMethod::POST, "", OBS_DEFAULT_TIMEOUT_MS, subResources);
    request->AddHeader("Content-Type", "application/xml");
    request->SetBody(bodyStream);

    // Calculate Content-MD5
    std::string md5Base64;
    RETURN_IF_NOT_OK(Hasher::GetMD5Base64(xmlBody, md5Base64));
    request->AddHeader("Content-MD5", md5Base64);

    RETURN_IF_NOT_OK(SignRequest(credential, request, md5Base64, subResources));

    std::shared_ptr<HttpResponse> response;
    RETURN_IF_NOT_OK(SendObsRequest(httpClient_, request, OBS_DEFAULT_TIMEOUT_MS, response));
    int httpStatus = response->GetStatus();
    successRateVec_.BlockingEmplaceBackCode(httpStatus);
    if (httpStatus < 200 || httpStatus >= 300) {
        std::stringstream ss;
        ss << "Failed to delete objects:\n";
        for (size_t i = beg; i < end; ++i) {
            ss << objects[i] << "\n";
        }
        ss << "number of objects: " << (end - beg) << ", http status: " << httpStatus;

        // Try to parse error response for more details
        std::string respBody;
        auto &respStream = response->GetBody();
        if (respStream != nullptr) {
            std::ostringstream oss;
            oss << respStream->rdbuf();
            respBody = oss.str();
        }
        std::string errCode;
        std::string errMsg;
        ObsXmlUtil::ParseErrorResponse(respBody, errCode, errMsg);
        if (!errCode.empty()) {
            ss << ", error: " << errCode << " - " << errMsg;
        }
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, ss.str());
    }
    LOG(INFO) << FormatString("Deleting objects is done. Begin: %zu, end: %zu.", beg, end);

    return Status::OK();
}

void ObsClient::StartTokenRotation()
{
    size_t waitIntervalSec = 600;  // 10 minutes.
    INJECT_POINT("ObsClient.StartTokenRotation.SetRotationInterval",
                 [this, &waitIntervalSec](size_t rotationInterval, size_t errorWaitSec) {
                     rotationIntervalSec_ = rotationInterval;
                     waitIntervalSec = errorWaitSec;
                 });
    LOG(INFO) << "Enable to start token rotation. Rotation interval is " << rotationIntervalSec_ << " seconds.";
    while (isTokenRotationStarting_.load()) {
        std::chrono::system_clock::time_point timeLater =
            std::chrono::system_clock::now() + std::chrono::seconds(rotationIntervalSec_);
        std::unique_lock<std::mutex> lock(rotationMutex_);
        rotationCv_.wait_until(lock, timeLater, [this]() { return !isTokenRotationStarting_.load(); });
        if (!isTokenRotationStarting_.load()) {
            break;
        }
        Status rc;
        int retryTimes = 0;
        while (isTokenRotationStarting_.load()) {
            LOG(INFO) << "Start to update obs temp token.";
            rc = UpdateTempObsToken();
            retryTimes++;
            if (rc.IsOk()) {
                break;
            }
            LOG(WARNING) << "Update obs temp token failed, error msg: " + rc.ToString();
            timeLater = std::chrono::system_clock::now() + std::chrono::seconds(waitIntervalSec);
            rotationCv_.wait_until(lock, timeLater, [this]() { return !isTokenRotationStarting_.load(); });
            if (!isTokenRotationStarting_.load()) {
                break;
            }
        }
        if (rc.IsError()) {
            LOG(ERROR) << "Failed to update the obs token, the obs may fail to be connected. " + rc.ToString();
        }
    };
}

Status ObsClient::CheckValidRotationToken()
{
    INJECT_POINT("ObsClient.CheckValidRotationToken.VerifyOriginCredential",
                 [this](const std::string &validAk, const std::string &validSk) {
                     return credentialManager_.VerifyEncryptedCredential(validAk, validSk, "ORIGIN_MOCK_TOKEN");
                 });
    INJECT_POINT("ObsClient.CheckValidRotationToken.VerifyUpdateCredential",
                 [this](const std::string &validAk, const std::string &validSk) {
                     return credentialManager_.VerifyEncryptedCredential(validAk, validSk, "UPDATE_MOCK_TOKEN");
                 });
    return Status::OK();
}

// ============================================
// ObsCredentialManager implementation
// ============================================

Status ObsClient::ObsCredentialManager::VerifyEncryptedCredential(const std::string &encryptedAk,
                                                                  const std::string &encryptedSk,
                                                                  const std::string &encryptedToken)
{
    std::shared_lock<std::shared_timed_mutex> optionLk(optionMutex_);
    CHECK_FAIL_RETURN_STATUS(encryptedInfos_.access.GetData() == encryptedAk, K_INVALID, "Check encrypted ak failed.");
    CHECK_FAIL_RETURN_STATUS(encryptedInfos_.secret.GetData() == encryptedSk, K_INVALID, "Check encrypted sk failed.");
    CHECK_FAIL_RETURN_STATUS(encryptedInfos_.securityToken.GetData() == encryptedToken, K_INVALID,
                             "Check encrypted token failed.");
    return Status::OK();
}

Status ObsClient::ObsCredentialManager::Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText,
                                                int &outSize)
{
    outSize = 0;
    auto rc = SecretManager::Instance()->Decrypt(cipher, plainText, outSize);
    if (rc.IsError()) {
        ClearUniqueChar(plainText, outSize);
        return rc;
    }
    return Status::OK();
}

Status ObsClient::ObsCredentialManager::DecryptAKSK()
{
    std::unique_ptr<char[]> plainTextOfAK;
    std::unique_ptr<char[]> plainTextOfSK;
    int textLenOfAK = 0;
    int textLenOfSK = 0;
    Raii raii([&plainTextOfAK, &textLenOfAK, &plainTextOfSK, &textLenOfSK]() mutable {
        ClearUniqueChar(plainTextOfAK, textLenOfAK);
        ClearUniqueChar(plainTextOfSK, textLenOfSK);
    });
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Decrypt(FLAGS_obs_access_key, plainTextOfAK, textLenOfAK),
                                     "ObsAK decrypt failed.");
    accessKey_ = std::make_unique<char[]>(textLenOfAK + 1);
    accessKeyLen_ = textLenOfAK;
    auto rc = strcpy_s(accessKey_.get(), textLenOfAK + 1, plainTextOfAK.get());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == EOK, K_RUNTIME_ERROR,
                                         "strcpy_s access key in ObsCredentialManager failed: " + std::to_string(rc));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Decrypt(FLAGS_obs_secret_key, plainTextOfSK, textLenOfSK),
                                     "ObsSK decrypt failed.");
    secretKey_ = std::make_unique<char[]>(textLenOfSK + 1);
    secretKeyLen_ = textLenOfSK;
    rc = strcpy_s(secretKey_.get(), textLenOfSK + 1, plainTextOfSK.get());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == EOK, K_RUNTIME_ERROR,
                                         "strcpy_s secret key in ObsCredentialManager failed: " + std::to_string(rc));
    return Status::OK();
}

bool ObsClient::ObsCredentialManager::Init()
{
    endPoint_ = std::make_unique<char[]>(client_->endPoint_.size() + 1);
    int rc = strcpy_s(endPoint_.get(), client_->endPoint_.size() + 1, client_->endPoint_.c_str());
    if (rc != EOK) {
        LOG(ERROR) << "strcpy_s endpoint in ObsCredentialManager failed: " << rc;
        return false;
    }

    bucketName_ = std::make_unique<char[]>(client_->bucketName_.size() + 1);
    rc = strcpy_s(bucketName_.get(), client_->bucketName_.size() + 1, client_->bucketName_.c_str());
    if (rc != EOK) {
        LOG(ERROR) << "strcpy_s bucket name in ObsCredentialManager failed: " << rc;
        return false;
    }

    if (FLAGS_enable_cloud_service_token_rotation) {
        auto status = UpdateCredentialInfo();
        return status.IsOk();
    }

    if (FLAGS_encrypt_kit != ENCRYPT_KIT_PLAINTEXT) {
        auto status = DecryptAKSK();
        if (status.IsError()) {
            LOG(ERROR) << "DecryptAKSK failed: " << status.ToString();
            return false;
        };
    } else {
        accessKey_ = std::make_unique<char[]>(FLAGS_obs_access_key.size() + 1);
        accessKeyLen_ = static_cast<int>(FLAGS_obs_access_key.size());
        int status = strcpy_s(accessKey_.get(), FLAGS_obs_access_key.size() + 1, FLAGS_obs_access_key.c_str());
        if (status != EOK) {
            LOG(ERROR) << "strcpy_s access key in ObsCredentialManager failed: " << status;
            return false;
        }

        secretKey_ = std::make_unique<char[]>(FLAGS_obs_secret_key.size() + 1);
        secretKeyLen_ = static_cast<int>(FLAGS_obs_secret_key.size());
        status = strcpy_s(secretKey_.get(), FLAGS_obs_secret_key.size() + 1, FLAGS_obs_secret_key.c_str());
        if (status != EOK) {
            LOG(ERROR) << "strcpy_s secret key in ObsCredentialManager failed: " << status;
            return false;
        }
    }

    return true;
}

ObsCredential ObsClient::ObsCredentialManager::GetCredential()
{
    ObsCredential cred;
    if (FLAGS_enable_cloud_service_token_rotation) {
        // Decrypt temp credentials
        ObsTempCredential tempCredential;
        std::shared_lock<std::shared_timed_mutex> optionLk(optionMutex_);
        if (!encryptedInfos_.access.Empty() && !encryptedInfos_.secret.Empty()
            && !encryptedInfos_.securityToken.Empty()) {
            auto rc1 =
                DecryptOneInfo(encryptedInfos_.access, tempCredential.tempAccessKey, tempCredential.accessKeyLen);
            auto rc2 =
                DecryptOneInfo(encryptedInfos_.secret, tempCredential.tempSecretKey, tempCredential.secretKeyLen);
            auto rc3 = DecryptOneInfo(encryptedInfos_.securityToken, tempCredential.tempToken, tempCredential.tokenLen);
            if (rc1.IsOk() && rc2.IsOk() && rc3.IsOk()) {
                cred.ak = std::string(tempCredential.tempAccessKey.get(), tempCredential.accessKeyLen);
                cred.sk = std::string(tempCredential.tempSecretKey.get(), tempCredential.secretKeyLen);
                cred.token = std::string(tempCredential.tempToken.get(), tempCredential.tokenLen);
            } else {
                LOG(ERROR) << "Decrypt temp ak, sk or tokens failed.";
            }
        } else {
            LOG(ERROR) << "Can not found temp ak, sk or tokens.";
        }
    } else {
        // AK/SK mode
        if (accessKey_ != nullptr) {
            cred.ak = std::string(accessKey_.get());
        }
        if (secretKey_ != nullptr) {
            cred.sk = std::string(secretKey_.get());
        }
    }
    return cred;
}

Status ObsClient::ObsCredentialManager::DecryptOneInfo(const SensitiveValue &info,
                                                       std::unique_ptr<char[]> &textInfo, int &textLen)
{
    int tempSize;
    auto status = Decrypt(info.GetData(), textInfo, tempSize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(status.IsOk(), K_RUNTIME_ERROR,
                                         "Decrypt temp credential in use failed, msg is: " + status.ToString());
    textLen = tempSize;
    return Status::OK();
}

bool ObsClient::ObsCredentialManager::IsCredentialInitialized() const
{
    return !client_->obsTempCredentialInfo_.access.Empty() && !client_->obsTempCredentialInfo_.secret.Empty()
           && !client_->obsTempCredentialInfo_.securityToken.Empty();
}

Status ObsClient::ObsCredentialManager::UpdateCredentialInfo()
{
    std::lock_guard<std::shared_timed_mutex> optionLk(optionMutex_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(IsCredentialInitialized(), K_INVALID, "OBS temp credential is empty.");
    std::string accessCipher;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        SecretManager::Instance()->Encrypt(client_->obsTempCredentialInfo_.access.GetData(), accessCipher),
        "Encrypt obs temp credential failed.");
    encryptedInfos_.access = accessCipher;

    std::string secretCipher;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        SecretManager::Instance()->Encrypt(client_->obsTempCredentialInfo_.secret.GetData(), secretCipher),
        "Encrypt obs temp credential failed.");
    encryptedInfos_.secret = secretCipher;

    std::string tempTokenCipher;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
        SecretManager::Instance()->Encrypt(client_->obsTempCredentialInfo_.securityToken.GetData(), tempTokenCipher),
        "Encrypt obs temp credential failed.");
    encryptedInfos_.securityToken = tempTokenCipher;
    return Status::OK();
}
}  // namespace datasystem

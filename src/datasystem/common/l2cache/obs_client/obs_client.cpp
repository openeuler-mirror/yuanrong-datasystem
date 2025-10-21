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
 * Description: Interface to OBS SDK.
 */

#include "datasystem/common/l2cache/obs_client/obs_client.h"

#include <chrono>
#include <fstream>
#include <limits>
#include <string>

#include <eSDKOBS.h>
#include <nlohmann/json.hpp>
#include <securec.h>

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
    if (initialized_.load()) {
        obs_deinitialize();
        initialized_.store(false);
    }
}

Status ObsClient::Init()
{
    if (initialized_.load()) {
        return Status::OK();
    }
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
    obs_status ret = OBS_STATUS_BUTT;
    ret = obs_initialize(OBS_INIT_ALL);
    CHECK_FAIL_RETURN_STATUS(ret == OBS_STATUS_OK, K_RUNTIME_ERROR,
                             FormatString("obs_initialize failed with status: %s.", obs_get_status_name(ret)));
    if (!optGenerator_.Init()) {
        obs_deinitialize();
        RETURN_STATUS(K_RUNTIME_ERROR, "OptionGenerator initialization failed.");
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
    obs_status ret = OBS_STATUS_BUTT;
    ret = obs_initialize(OBS_INIT_ALL);
    CHECK_FAIL_RETURN_STATUS(ret == OBS_STATUS_OK, K_RUNTIME_ERROR,
                             FormatString("obs_initialize failed with status: %s.", obs_get_status_name(ret)));
    if (!optGenerator_.Init()) {
        obs_deinitialize();
        RETURN_STATUS(K_RUNTIME_ERROR, "OptionGenerator initialization failed.");
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
    return optGenerator_.UpdateCredentialInfo();
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

Status ObsClient::StreamingUpload(const std::shared_ptr<std::iostream> &body, size_t size, const std::string &objPath,
                                  Timer &timer)
{
    LOG(INFO) << FormatString("Streaming upload starts. Object path: %s.", objPath);
    ObsTempCredential tempCredential;
    obs_options opts;
    optGenerator_.GenerateObsOption(opts, tempCredential);

    obs_put_properties properties;
    init_put_properties(&properties);
    ObsPutBuffer obsBuf;
    obsBuf.buffer = body;
    obsBuf.bufferSize = size;
    obs_put_object_handler handler = { { &PutRspPropertiesCallback, &PutRspCompleteCallback },
                                       &PutObjectDataCallback,
                                       nullptr };
    int64_t remainingTime = timer.GetRemainingTimeMs();
    opts.request_options.max_connected_time = remainingTime;
    opts.request_options.connect_time = remainingTime;
    put_object(&opts, const_cast<char *>(objPath.c_str()), obsBuf.bufferSize, &properties, nullptr, &handler, &obsBuf);
    INJECT_POINT("ObsClient.StreamingUpload.ObsUploadFailed", [&obsBuf]() {
        obsBuf.status = OBS_STATUS_InternalError;
        return Status::OK();
    });
    INJECT_POINT("ObsClient.StreamingUpload.ObsNoSuchKey", [&obsBuf]() {
        obsBuf.status = OBS_STATUS_NoSuchKey;
        return Status::OK();
    });
    successRateVec_.BlockingEmplaceBackCode(static_cast<int>(obsBuf.status));
    if (OBS_STATUS_OK == obsBuf.status) {
        LOG(INFO) << FormatString("Putting object to OBS is done. Object path: %s", objPath);
        return Status::OK();
    }
    RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Failed to put object: %s, buffer size: %zu, status: %s",
                                                          objPath, size, obs_get_status_name(obsBuf.status)));
}

Status ObsClient::InitMultiPartUpload(const std::string &objPath, obs_options &opts, obs_put_properties &putProperties,
                                      Timer &timer, const size_t uploadIdSize, char *uploadId)
{
    obs_response_handler initiateMultiPartUploadHandler = { &MultiUploadRspPropertiesCallback,
                                                            &MultiUploadRspCompleteCallback };

    int64_t remaining = timer.GetRemainingTimeMs();
    opts.request_options.connect_time = remaining;
    opts.request_options.max_connected_time = remaining;
    ObsRsp initRsp;
    initiate_multi_part_upload(&opts, const_cast<char *>(objPath.c_str()), uploadIdSize, uploadId, &putProperties,
                               nullptr, &initiateMultiPartUploadHandler, &initRsp);
    CHECK_FAIL_RETURN_STATUS(
        initRsp.status == OBS_STATUS_OK, K_RUNTIME_ERROR,
        FormatString("Initiate multipart upload failed. obs status: %s", obs_get_status_name(initRsp.status)));

    return Status::OK();
}

Status ObsClient::MultiPartUpload(const std::shared_ptr<std::iostream> &body, size_t size, const std::string &objPath,
                                  size_t partitionSize, Timer &timer)
{
    LOG(INFO) << FormatString("Multipart upload starts. Object path: %s.", objPath);
    ObsTempCredential tempCredential;
    obs_options opts;
    optGenerator_.GenerateObsOption(opts, tempCredential);

    // Initialize multipart upload
    const size_t uploadIdSize = 255;
    char uploadId[uploadIdSize + 1] = { 0 };
    obs_put_properties putProperties;
    (void)memset_s((void *)&putProperties, sizeof(obs_put_properties), 0, sizeof(obs_put_properties));
    init_put_properties(&putProperties);
    RETURN_IF_NOT_OK(InitMultiPartUpload(objPath, opts, putProperties, timer, uploadIdSize, uploadId));

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
    int64_t remaining = timer.GetRemainingTimeMs();
    opts.request_options.connect_time = remaining;
    opts.request_options.max_connected_time = remaining;
    std::vector<obs_options> optVec(multiUploadBuf.partNum, opts);
    RETURN_IF_NOT_OK(SubmitUploadThreads(multiUploadBuf, objPath, size, uploadId, optVec, parts));

    // Complete upload
    obs_complete_multi_part_upload_handler completeMultiHandler = {
        { &MultiUploadRspPropertiesCallback, &MultiUploadRspCompleteCallback }, &MultiUploadCompleteCallback
    };
    ObsRsp completeRsp;
    std::vector<obs_complete_upload_Info> uploadInfo(multiUploadBuf.partNum, obs_complete_upload_Info());
    for (int i = 0; i < multiUploadBuf.partNum; ++i) {
        uploadInfo[i].etag = parts[i].eTag;
        uploadInfo[i].part_number = parts[i].partNum;
    }
    remaining = timer.GetRemainingTimeMs();
    opts.request_options.connect_time = remaining;
    opts.request_options.max_connected_time = remaining;
    complete_multi_part_upload(&opts, const_cast<char *>(objPath.c_str()), uploadId, multiUploadBuf.partNum,
                               &uploadInfo[0], &putProperties, &completeMultiHandler, &completeRsp);
    successRateVec_.BlockingEmplaceBackCode(static_cast<int>(completeRsp.status));
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        completeRsp.status == OBS_STATUS_OK, K_RUNTIME_ERROR,
        FormatString("Complete multipart upload failed. Object: %s, buffer size: %zu, obs status: %s", objPath, size,
                     obs_get_status_name(completeRsp.status)));
    LOG(INFO) << FormatString("Uploading object to OBS is done. Object path: %s", objPath);

    return Status::OK();
}

Status ObsClient::SubmitUploadThreads(const MultiPartUploadBuffer &multiPartBuffer, const std::string &objPath,
                                      size_t bufferSize, char *uploadId, std::vector<obs_options> &options,
                                      std::vector<OnePartUploadBuffer> &parts)
{
    std::vector<std::future<Status>> results;
    // Use lock to ensure the upload tasks of the same object are submitted to thread pool together
    std::unique_lock<std::mutex> lk(multiPartUploadMx_);

    auto job = [this, &options, &parts](int i) {
        auto startTime = std::chrono::steady_clock::now();
        int64_t elapsed = 0;
        int64_t limit = options[i].request_options.max_connected_time;
        Status rc = Status::OK();
        do {
            options[i].request_options.max_connected_time = limit - elapsed;
            options[i].request_options.connect_time = limit - elapsed;
            rc = OnePartUpload(parts[i]);
            if (rc.GetCode() != K_RUNTIME_ERROR) {
                return rc;
            }
            elapsed =
                std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - startTime)
                    .count();
        } while (elapsed < limit);
        return rc;
    };

    for (int i = 0; i < multiPartBuffer.partNum; ++i) {
        OnePartUploadBuffer &part = parts[i];
        part.buffer = multiPartBuffer.buffer;
        part.partNum = i + 1;
        part.uploadId = uploadId;
        part.key = const_cast<char *>(objPath.c_str());
        part.option = &options[i];
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
            RETURN_IF_NOT_OK(Delete({ objPath }));
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
    obs_upload_part_info partInfo;
    partInfo.part_number = onePartUploadBuffer.partNum;
    partInfo.upload_id = onePartUploadBuffer.uploadId;
    obs_upload_handler handler = { { &OneUploadRspPropertiesCallback, &OneUploadRspCompleteCallback },
                                   &OneUploadDataCallback,
                                   nullptr };
    upload_part(onePartUploadBuffer.option, onePartUploadBuffer.key, &partInfo, onePartUploadBuffer.partSize, nullptr,
                nullptr, &handler, &onePartUploadBuffer);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        onePartUploadBuffer.status == OBS_STATUS_OK, K_RUNTIME_ERROR,
        FormatString("Upload one part failed. Object path: %s, upload ID: %s, buffer size: %zu, part number: %zu, "
                     "obs err status: %s",
                     onePartUploadBuffer.key, partInfo.upload_id, onePartUploadBuffer.partSize, partInfo.part_number,
                     obs_get_status_name(onePartUploadBuffer.status)));
    LOG(INFO) << FormatString("Uploading one part is done. Object path: %s, part number: %zu", onePartUploadBuffer.key,
                              partInfo.part_number);

    return Status::OK();
}

Status ObsClient::GetObject(const std::string &objPath, std::shared_ptr<std::stringstream> &buf, Timer &timer)
{
    LOG(INFO) << FormatString("GetObject starts. Object path: %s", objPath);
    ObsTempCredential tempCredential;
    obs_options opts;
    optGenerator_.GenerateObsOption(opts, tempCredential);

    obs_get_conditions conditions;
    (void)memset_s(&conditions, sizeof(obs_get_conditions), 0, sizeof(obs_get_conditions));
    init_get_properties(&conditions);

    obs_get_object_handler getObjHandler = { { &GetObjRspPropertiesCallback, &GetObjResponseCompleteCallback },
                                             &GetObjDataCallback };

    obs_object_info objInfo = { const_cast<char *>(objPath.c_str()), nullptr };
    GetObjectBuffer getObjBuf;
    getObjBuf.buffer = std::make_shared<std::stringstream>();
    int64_t remaining = timer.GetRemainingTimeMs();
    opts.request_options.connect_time = remaining;
    opts.request_options.max_connected_time = remaining;
    get_object(&opts, &objInfo, &conditions, nullptr, &getObjHandler, &getObjBuf);
    successRateVec_.BlockingEmplaceBackCode(static_cast<int>(getObjBuf.status));
    if (getObjBuf.status != OBS_STATUS_OK) {
        auto errCode = getObjBuf.status == OBS_STATUS_NoSuchKey ? K_NOT_FOUND : K_RUNTIME_ERROR;
        RETURN_STATUS_LOG_ERROR(errCode, FormatString("Failed to get object: %s, obs status: %s", objPath,
                                                      obs_get_status_name(getObjBuf.status)));
    }
    buf = std::move(getObjBuf.buffer);
    LOG(INFO) << FormatString("Getting object is done. Object path: %s.", objPath);

    return Status::OK();
}

Status ObsClient::ListObjects(const std::string &objectPrefix, Timer &timer,
                              std::shared_ptr<GetObjectInfoListResp> &listResp)
{
    LOG(INFO) << FormatString("ListObjects starts. Prefix: %s", objectPrefix);
    CHECK_FAIL_RETURN_STATUS(listResp != nullptr, K_INVALID, "Must provide GetObjectInfoListResp");
    ObsTempCredential tempCredential;
    obs_options opts;
    optGenerator_.GenerateObsOption(opts, tempCredential);

    obs_list_objects_handler handler = { { &ListObjRspPropertiesCallback, &ListObjRspCompleteCallback },
                                         &ListObjectsCallback };
    uint64_t remaining = timer.GetRemainingTimeMs();
    opts.request_options.connect_time = remaining;
    opts.request_options.max_connected_time = remaining;
    std::string nextMarker;
    static const uint16_t maxNumObj = 1000;
    int keyCount = 0;
    do {
        ListObjectData listObjData;
        if (!listResp->NextMarker().empty()) {
            nextMarker = listResp->NextMarker();
        }
        list_bucket_objects(&opts, objectPrefix.c_str(), nextMarker.c_str(), nullptr, maxNumObj, &handler,
                            &listObjData);
        successRateVec_.BlockingEmplaceBackCode(static_cast<int>(listObjData.status));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(listObjData.status == OBS_STATUS_OK, K_RUNTIME_ERROR,
                                             FormatString("Failed to list objects. Prefix: %s, obs status: %s",
                                                          objectPrefix, obs_get_status_name(listObjData.status)));
        keyCount = listObjData.keyCount;
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
    ObsTempCredential tempCredential;
    obs_options opts;
    optGenerator_.GenerateObsOption(opts, tempCredential);

    auto objectInfos = std::make_unique<obs_object_info[]>(end - beg);
    for (size_t i = beg; i < end; ++i) {
        objectInfos[i - beg].key = const_cast<char *>(objects[i].c_str());
        objectInfos[i - beg].version_id = nullptr;
    }

    obs_delete_object_info delInfo;
    delInfo.keys_number = end - beg;

    ObsRsp delRsp;
    obs_delete_object_handler handler = { { &BatchDelObjRspPropertiesCallback, &BatchDelObjResponseCompleteCallback },
                                          &BatchDelObjectsDataCallback };
    batch_delete_objects(&opts, objectInfos.get(), &delInfo, nullptr, &handler, &delRsp);
    successRateVec_.BlockingEmplaceBackCode(static_cast<int>(delRsp.status));
    if (delRsp.status != OBS_STATUS_OK) {
        std::stringstream ss;
        ss << "Failed to delete objects:\n";
        for (size_t i = beg; i < end; ++i) {
            ss << objects[i] << "\n";
        }
        ss << "number of objects: " << delInfo.keys_number << ", obs status: " << obs_get_status_name(delRsp.status);
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, ss.str());
    }
    LOG(INFO) << FormatString("Deleting objects is done. Begin: %zu, end: %zu.", beg, end);

    return Status::OK();
}

void ObsClient::CreateBucketRspCompleteCallback(obs_status status, const obs_error_details *errorDetails,
                                                void *callbackData)
{
    (void)errorDetails;
    if (callbackData != nullptr) {
        obs_status *ret = (obs_status *)callbackData;
        *ret = status;
    }
}

obs_status ObsClient::PutRspPropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    (void)properties;
    (void)callbackData;
    return OBS_STATUS_OK;
}

void ObsClient::PutRspCompleteCallback(obs_status status, const obs_error_details *errorDetails, void *callbackData)
{
    (void)errorDetails;
    if (callbackData != nullptr) {
        auto data = (ObsPutBuffer *)callbackData;
        data->status = status;
    }
}

int ObsClient::PutObjectDataCallback(int bufferSize, char *buffer, void *callbackData)
{
    int toRead = 0;
    if (callbackData != nullptr) {
        auto data = (ObsPutBuffer *)callbackData;
        if (data->bufferSize <= 0) {
            return 0;
        }
        toRead = std::min(data->bufferSize, (size_t)bufferSize);
        data->buffer->read(buffer, toRead);
        toRead = static_cast<int>(data->buffer->gcount());
        data->bufferSize -= toRead;
        data->offset += toRead;
    }
    return toRead;
}

obs_status ObsClient::MultiUploadRspPropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    (void)properties;
    (void)callbackData;
    return OBS_STATUS_OK;
}

void ObsClient::MultiUploadRspCompleteCallback(obs_status status, const obs_error_details *errorDetails,
                                               void *callbackData)
{
    (void)errorDetails;
    if (callbackData != nullptr) {
        ObsRsp *data = (ObsRsp *)callbackData;
        data->status = status;
    }
}

obs_status ObsClient::MultiUploadCompleteCallback(const char *location, const char *bucket, const char *key,
                                                  const char *eTag, void *callbackData)
{
    (void)callbackData;
    VLOG(1) << FormatString("location = %s \nbucket = %s \nkey = %s \neTag = %s \n", location, bucket, key, eTag);
    return OBS_STATUS_OK;
}

obs_status ObsClient::OneUploadRspPropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    if (properties == nullptr || callbackData == nullptr) {
        return OBS_STATUS_AbortedByCallback;
    }
    OnePartUploadBuffer *data = (OnePartUploadBuffer *)callbackData;
    if (properties->etag != nullptr) {
        errno_t re = strcpy_s(data->eTag, sizeof(data->eTag), properties->etag);
        if (re != EOK) {
            return OBS_STATUS_AbortedByCallback;
        }
    }
    return OBS_STATUS_OK;
}

void ObsClient::OneUploadRspCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData)
{
    (void)error;
    if (callbackData == nullptr) {
        return;
    }
    OnePartUploadBuffer *data = (OnePartUploadBuffer *)callbackData;
    data->status = status;
}

int ObsClient::OneUploadDataCallback(int bufferSize, char *buffer, void *callbackData)
{
    size_t toRead = 0;
    if (callbackData != nullptr) {
        OnePartUploadBuffer *data = (OnePartUploadBuffer *)callbackData;
        if (data->partSize > 0) {
            toRead = std::min(data->partSize, (size_t)bufferSize);
            std::unique_lock<std::mutex> lk(*data->mx);
            data->buffer->seekg(data->offset);  // reset the iostream after it was used by other threads
            data->buffer->read(buffer, toRead);
            toRead = static_cast<size_t>(data->buffer->gcount());
        }
        data->partSize -= toRead;
        data->offset += toRead;
    }
    return toRead;
}

obs_status ObsClient::GetObjRspPropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    (void)properties;
    (void)callbackData;
    return OBS_STATUS_OK;
}

void ObsClient::GetObjResponseCompleteCallback(obs_status status, const obs_error_details *errorDetails,
                                               void *callbackData)
{
    (void)errorDetails;
    if (callbackData != nullptr) {
        auto data = (GetObjectBuffer *)callbackData;
        data->status = status;
    }
}

obs_status ObsClient::GetObjDataCallback(int bufferSize, const char *buffer, void *callbackData)
{
    if (callbackData == nullptr) {
        LOG(ERROR) << "callbackData is nullptr.";
        return OBS_STATUS_AbortedByCallback;
    }
    if (buffer == nullptr) {
        LOG(ERROR) << "buffer is nullptr.";
        return OBS_STATUS_AbortedByCallback;
    }
    if (bufferSize <= 0) {
        LOG(ERROR) << "bufferSize is invalid.";
        return OBS_STATUS_AbortedByCallback;
    }
    GetObjectBuffer *data = (GetObjectBuffer *)callbackData;
    if (data->buffer == nullptr) {
        LOG(ERROR) << "data->buffer is nullptr.";
        return OBS_STATUS_AbortedByCallback;
    }
    data->buffer->write(buffer, bufferSize);
    if (data->buffer->bad()) {
        LOG(ERROR) << "Failed to get object data from obs.";
        return OBS_STATUS_AbortedByCallback;
    }
    data->size += static_cast<size_t>(bufferSize);
    return OBS_STATUS_OK;
}

obs_status ObsClient::ListObjRspPropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    (void)properties;
    (void)callbackData;
    return OBS_STATUS_OK;
}

void ObsClient::ListObjRspCompleteCallback(obs_status status, const obs_error_details *errorDetails, void *callbackData)
{
    (void)errorDetails;
    if (callbackData != nullptr) {
        auto data = (ListObjectData *)callbackData;
        data->status = status;
    }
}

obs_status ObsClient::ListObjectsCallback(int isTruncated, const char *nextMarker, int contentsCount,
                                          const obs_list_objects_content *contents, int commonPrefixesCount,
                                          const char **commonPrefixes, void *callbackData)
{
    (void)commonPrefixes;
    (void)commonPrefixesCount;

    if (callbackData == nullptr) {
        return OBS_STATUS_AbortedByCallback;
    }
    auto *data = (ListObjectData *)callbackData;

    data->isTruncated = isTruncated;

    if ((nextMarker == nullptr || nextMarker[0] == 0) && contentsCount != 0) {
        nextMarker = contents[contentsCount - 1].key;
    }

    if (nextMarker != nullptr) {
        data->nextMarker = nextMarker;
    }

    auto &callbackObjects = data->objects;
    callbackObjects.reserve(contentsCount);
    for (int i = 0; i < contentsCount; ++i) {
        const obs_list_objects_content &content = contents[i];
        callbackObjects.emplace_back(content.key, content.last_modified, content.etag, content.size, content.owner_id,
                                     content.owner_display_name, content.storage_class, content.type);
    }
    data->keyCount += contentsCount;

    return OBS_STATUS_OK;
}

obs_status ObsClient::BatchDelObjRspPropertiesCallback(const obs_response_properties *properties, void *callbackData)
{
    (void)properties;
    (void)callbackData;
    return OBS_STATUS_OK;
}

void ObsClient::BatchDelObjResponseCompleteCallback(obs_status status, const obs_error_details *errorDetails,
                                                    void *callbackData)
{
    (void)errorDetails;
    if (callbackData != nullptr) {
        auto data = (ObsRsp *)callbackData;
        data->status = status;
    }
}

obs_status ObsClient::BatchDelObjectsDataCallback(int contentsCount, obs_delete_objects *delObjs, void *callbackData)
{
    (void)contentsCount;
    (void)delObjs;
    (void)callbackData;
    return OBS_STATUS_OK;
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
                     return optGenerator_.VerifyEncryptedCredential(validAk, validSk, "ORIGIN_MOCK_TOKEN");
                 });
    INJECT_POINT("ObsClient.CheckValidRotationToken.VerifyUpdateCredential",
                 [this](const std::string &validAk, const std::string &validSk) {
                     return optGenerator_.VerifyEncryptedCredential(validAk, validSk, "UPDATE_MOCK_TOKEN");
                 });
    return Status::OK();
}

Status ObsClient::OptionGenerator::VerifyEncryptedCredential(const std::string &encryptedAk,
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

Status ObsClient::OptionGenerator::Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize)
{
    outSize = 0;
    auto rc = SecretManager::Instance()->Decrypt(cipher, plainText, outSize);
    if (rc.IsError()) {
        ClearUniqueChar(plainText, outSize);
        return rc;
    }
    return Status::OK();
}

Status ObsClient::OptionGenerator::DecryptAKSK()
{
    std::unique_ptr<char[]> plainTextOfAK, plainTextOfSK;
    int textLenOfAK = 0, textLenOfSK = 0;
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
                                         "strcpy_s access key in OptionGenerator failed: " + std::to_string(rc));
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Decrypt(FLAGS_obs_secret_key, plainTextOfSK, textLenOfSK),
                                     "ObsSK decrypt failed.");
    secretKey_ = std::make_unique<char[]>(textLenOfSK + 1);
    secretKeyLen_ = textLenOfSK;
    rc = strcpy_s(secretKey_.get(), textLenOfSK + 1, plainTextOfSK.get());
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == EOK, K_RUNTIME_ERROR,
                                         "strcpy_s secret key in OptionGenerator failed: " + std::to_string(rc));
    return Status::OK();
}

bool ObsClient::OptionGenerator::Init()
{
    endPoint_ = std::make_unique<char[]>(client_->endPoint_.size() + 1);
    int rc = strcpy_s(endPoint_.get(), client_->endPoint_.size() + 1, client_->endPoint_.c_str());
    if (rc != EOK) {
        LOG(ERROR) << "strcpy_s endpoint in OptionGenerator failed: " << rc;
        return false;
    }

    bucketName_ = std::make_unique<char[]>(client_->bucketName_.size() + 1);
    rc = strcpy_s(bucketName_.get(), client_->bucketName_.size() + 1, client_->bucketName_.c_str());
    if (rc != EOK) {
        LOG(ERROR) << "strcpy_s bucket name in OptionGenerator failed: " << rc;
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
            LOG(ERROR) << "strcpy_s access key in OptionGenerator failed: " << status;
            return false;
        }

        secretKey_ = std::make_unique<char[]>(FLAGS_obs_secret_key.size() + 1);
        secretKeyLen_ = static_cast<int>(FLAGS_obs_secret_key.size());
        status = strcpy_s(secretKey_.get(), FLAGS_obs_secret_key.size() + 1, FLAGS_obs_secret_key.c_str());
        if (status != EOK) {
            LOG(ERROR) << "strcpy_s secret key in OptionGenerator failed: " << status;
            return false;
        }
    }

    return true;
}

void ObsClient::OptionGenerator::GenerateObsOption(obs_options &opts, ObsTempCredential &tempCredential)
{
    init_obs_options(&opts);
    if (FLAGS_enable_cloud_service_token_rotation) {
        GenerateTokenRotationObsOption(opts, tempCredential);
    } else {
        GenerateCommonObsOption(opts);
    }
}

void ObsClient::OptionGenerator::GenerateCommonObsOption(obs_options &opts)
{
    opts.bucket_options.host_name = endPoint_.get();
    opts.bucket_options.bucket_name = bucketName_.get();
    opts.bucket_options.access_key = accessKey_.get();
    opts.bucket_options.secret_access_key = secretKey_.get();
    opts.bucket_options.uri_style = obs_uri_style::OBS_URI_STYLE_PATH;
    opts.bucket_options.protocol = OBS_PROTOCOL_HTTP;
    if (FLAGS_obs_https_enabled) {
        opts.bucket_options.protocol = OBS_PROTOCOL_HTTPS;
    }
}

void ObsClient::OptionGenerator::GenerateTokenRotationObsOption(obs_options &opts, ObsTempCredential &tempCredential)
{
    std::shared_lock<std::shared_timed_mutex> optionLk(optionMutex_);
    if (encryptedInfos_.access.Empty() || encryptedInfos_.secret.Empty() || encryptedInfos_.securityToken.Empty()) {
        LOG(ERROR) << "Can not found temp ak, sk or tokens.";
        return;
    }
    auto rc1 = DecryptOneInfo(encryptedInfos_.access, tempCredential.tempAccessKey, tempCredential.accessKeyLen);
    auto rc2 = DecryptOneInfo(encryptedInfos_.secret, tempCredential.tempSecretKey, tempCredential.secretKeyLen);
    auto rc3 = DecryptOneInfo(encryptedInfos_.securityToken, tempCredential.tempToken, tempCredential.tokenLen);
    if (rc1.IsError() || rc2.IsError() || rc3.IsError()) {
        LOG(ERROR) << "Decrypt temp ak, sk or tokens failed.";
        return;
    }
    opts.bucket_options.host_name = endPoint_.get();
    opts.bucket_options.bucket_name = bucketName_.get();
    opts.bucket_options.uri_style = obs_uri_style::OBS_URI_STYLE_PATH;
    opts.bucket_options.protocol = OBS_PROTOCOL_HTTP;
    if (FLAGS_obs_https_enabled) {
        opts.bucket_options.protocol = OBS_PROTOCOL_HTTPS;
    }
    opts.bucket_options.access_key = tempCredential.tempAccessKey.get();
    opts.bucket_options.secret_access_key = tempCredential.tempSecretKey.get();
    opts.bucket_options.token = tempCredential.tempToken.get();
}

Status ObsClient::OptionGenerator::DecryptOneInfo(const SensitiveValue &info, std::unique_ptr<char[]> &textInfo,
                                                  int &textLen)
{
    int tempSize;
    auto status = Decrypt(info.GetData(), textInfo, tempSize);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(status.IsOk(), K_RUNTIME_ERROR,
                                         "Decrypt temp credential in use failed, msg is: " + status.ToString());
    textLen = tempSize;
    return Status::OK();
}

bool ObsClient::OptionGenerator::IsCredentialInitialized() const
{
    return !client_->obsTempCredentialInfo_.access.Empty() && !client_->obsTempCredentialInfo_.secret.Empty()
           && !client_->obsTempCredentialInfo_.securityToken.Empty();
}

Status ObsClient::OptionGenerator::UpdateCredentialInfo()
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
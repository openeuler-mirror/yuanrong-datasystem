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
 * Description: Monitor logger, flush current node's res information and operation time cost
 * information, there is two Bounded-buffer to support async write log messages.
 */
#include "datasystem/common/log/access_recorder.h"

#include <cstddef>
#include <sstream>
#include <string>
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_dir);

namespace datasystem {

AccessRecorder::AccessRecorder(AccessRecorderKey key) : beg_(clock::now()), isRecord_(false)
{
#define ACCESS_RECORDER_KEY_DEF(keyEnum, keyType) #keyEnum,
    static const char *keyNames[] = {
#include "datasystem/common/log/access_point.def"
    };
#undef ACCESS_RECORDER_KEY_DEF

#define ACCESS_RECORDER_KEY_DEF(keyEnum, keyType) AccessKeyType::keyType,
    static AccessKeyType types[] = {
#include "datasystem/common/log/access_point.def"
    };
#undef ACCESS_RECORDER_KEY_DEF

    handleName_ = keyNames[static_cast<size_t>(key)];
    handleType_ = types[static_cast<size_t>(key)];
}

AccessRecorder::AccessRecorder(std::string handleName)
    : beg_(clock::now()), handleName_(std::move(handleName)), handleType_(AccessKeyType::REQUEST_OUT), isRecord_(false)
{
}

void AccessRecorder::Record(StatusCode code, const std::string &dataSize, const RequestParam &reqParam,
                            const std::string &respMsg, uint64_t asyncElapseTime)
{
    Record(static_cast<int>(code), dataSize, reqParam, respMsg, asyncElapseTime);
}

void AccessRecorder::Record(StatusCode code, const StreamRequestParam &reqParam, const StreamResponseParam &respParam)
{
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - beg_).count();
    if (FLAGS_log_monitor && Logging::AccessRecorderManagerInstance() != nullptr) {
        Logging::AccessRecorderManagerInstance()->LogPerformance(handleName_, handleType_, elapsed, code, "0",
                                                                 reqParam.ToString(), respParam.ToString());
    }
    isRecord_ = true;
}

void AccessRecorder::Record(int code, const std::string &dataSize, const RequestParam &reqParam,
                            const std::string &respMsg, uint64_t asyncElapseTime)
{
    uint64_t elapsed = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - beg_).count();
    if (FLAGS_log_monitor && Logging::AccessRecorderManagerInstance() != nullptr) {
        Logging::AccessRecorderManagerInstance()->LogPerformance(handleName_, handleType_, elapsed, code, dataSize,
                                                                 reqParam.ToString(), respMsg, asyncElapseTime);
    }
    isRecord_ = true;
}

AccessRecorder::~AccessRecorder()
{
    if (!isRecord_) {
        LOG(WARNING) << handleName_ << " Not call AccessRecorder::Record()";
    }
}

Status AccessRecorderManager::Init(bool isClient, bool isEmbeddedClient)
{
    isClient_ = isClient;
    if (FLAGS_log_monitor) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ResetWriteLogger(isEmbeddedClient), "AccessRecorder init failed");
    }
    return Status::OK();
}

Status AccessRecorderManager::ResetWriteLogger(bool isEmbeddedClient)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Logging::CreateLogDir(), K_NOT_READY, "Log file creation failed");

    std::vector<std::pair<AccessKeyType, std::string>> typeList;
    if (isClient_) {
        static auto pid = getpid();
        std::string clientAccessLogName = CLIENT_ACCESS_LOG_NAME + "_" + std::to_string(pid);

        // Allow overriding client log filename via environment variable
        std::string accessLogName = GetStringFromEnv(ACCESS_LOG_NAME_ENV.c_str(), "");
        if (Logging::ValidateLogName(accessLogName)) {
            clientAccessLogName = std::move(accessLogName);
        }

        typeList = { std::make_pair(AccessKeyType::CLIENT, FLAGS_log_dir + "/" + clientAccessLogName + ".log") };
    } else {
        typeList = { std::make_pair(AccessKeyType::ACCESS, FLAGS_log_dir + "/" + ACCESS_LOG_NAME + ".log"),
                     std::make_pair(AccessKeyType::REQUEST_OUT, FLAGS_log_dir + "/" + REQUEST_OUT_LOG_NAME + ".log") };
        if (isEmbeddedClient) {
            typeList.emplace_back(AccessKeyType::CLIENT, FLAGS_log_dir + "/" + CLIENT_ACCESS_LOG_NAME + ".log");
        }
    }

    for (const auto &kv : typeList) {
        auto exporter = std::make_unique<HardDiskExporter>();
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(exporter->Init(kv.second), "Exporter init failed.");
        exporterMap_.emplace(kv.first, std::move(exporter));
    }

    return Status::OK();
}

Status AccessRecorderManager::SubmitWriteMessage()
{
    if (exporterMap_.empty()) {
        const std::string errMsg = "AccessRecorder is not init.";
        if (FLAGS_log_monitor) {
            LOG(ERROR) << errMsg;
        }
        RETURN_STATUS(K_NOT_READY, errMsg);
    }

    for (auto &iter : exporterMap_) {
        iter.second->SubmitWriteMessage();
    }
    return Status::OK();
}

Status AccessRecorderManager::LogPerformance(const std::string &handleName, AccessKeyType type, int64_t microsecond,
                                             int code, const std::string &dataSize, const std::string &reqMsg,
                                             const std::string &respMsg, uint64_t asyncElapseTime)
{
    if (exporterMap_.empty()) {
        const std::string errMsg = "AccessRecorder is not init.";
        if (FLAGS_log_monitor) {
            LOG(ERROR) << errMsg;
        }
        RETURN_STATUS(K_NOT_READY, errMsg);
    }

    if (type == AccessKeyType::REQUEST_OUT && isClient_) {
        return Status::OK();
    }

    std::ostringstream logStream;
    logStream << code << " | " << handleName << " | " << microsecond;
    logStream << " | " << dataSize << " | " << reqMsg << " | " << respMsg;
    if (type == AccessKeyType::REQUEST_OUT) {
        logStream << " | " << asyncElapseTime;
    }
    auto it = exporterMap_.find(type);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(it != exporterMap_.end(), K_RUNTIME_ERROR,
                                         FormatString("Access type: %zu not found!", int(type)));
    static Uri uri(__FILE__);
    it->second->Send(logStream.str(), uri, __LINE__);
    return Status::OK();
}

std::string objectKeysToString(const std::vector<std::string> &keys)
{
    std::string ret = "[";
    uint32_t len = 0;
    uint32_t count = 0;
    for (auto key : keys) {
        if (count >= LOG_TOTAL_KEYS_SIZE_LIMIT) {
            ret.append("***").append(",").append("total:").append(std::to_string(keys.size())).append("]");
            return ret;
        }
        if (len > LOG_OBJECT_KEY_SIZE_LIMIT - key.length()) {
            ret.append("total:").append(std::to_string(keys.size())).append("]");
            return ret;
        }
        count += 1;
        len = len + key.length() > LOG_OBJECT_KEY_SIZE_LIMIT ? LOG_OBJECT_KEY_SIZE_LIMIT : key.length();
        ret.append(key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT)).append(",");
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("]");
    return ret;
}

std::string objectKeysToString(const char **cKey, size_t keyLen)
{
    std::string ret = "[";
    uint32_t len = 0;
    for (size_t i = 0; i < keyLen; i++) {
        if (len > LOG_TOTAL_KEYS_SIZE_LIMIT - strlen(cKey[i])) {
            ret.append("total:").append(std::to_string(keyLen)).append("]");
            return ret;
        }
        len = len + strlen(cKey[i]) > LOG_OBJECT_KEY_SIZE_LIMIT ? LOG_OBJECT_KEY_SIZE_LIMIT : strlen(cKey[i]);
        std::string s(cKey[i]);
        ret.append(s.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT)).append(",");
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("]");
    return ret;
}

std::string RequestParam::ToString() const
{
    std::string ret = "{";
    if (!outReq.empty()) {
        ret.append(outReq).append("}");
        return ret;
    }
    if (!objectKey.empty()) {
        ret.append("Object_key:").append(objectKey).append(",");
    }
    if (!nestedKey.empty()) {
        ret.append("Nested_keys:").append(nestedKey).append(",");
    }
    if (!keep.empty()) {
        ret.append("keep:").append(keep).append(",");
    }
    if (!writeMode.empty()) {
        ret.append("Write_mode:").append(writeMode).append(",");
    }
    if (!consistencyType.empty()) {
        ret.append("consistency_type:").append(consistencyType).append(",");
    }
    if (!isSeal.empty()) {
        ret.append("is_seal:").append(isSeal).append(",");
    }
    if (!isRetry.empty()) {
        ret.append("is_retry:").append(isRetry).append(",");
    }
    if (!ttlSecond.empty()) {
        ret.append("ttl_second:").append(ttlSecond).append(",");
    }
    if (!subTimeout.empty()) {
        ret.append("sub_timeout:").append(subTimeout).append(",");
    }
    if (!timeout.empty()) {
        ret.append("timeout:").append(timeout).append(",");
    }
    if (!cacheType.empty()) {
        ret.append("cacheType:").append(cacheType).append(",");
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("}");
    return ret;
}

std::string StreamRequestParam::ToString() const
{
    std::stringstream ss;
    ss << "{stream_name:" << streamName;
    if (!producerId.empty()) {
        ss << ",producer_id:" << producerId;
    }
    if (!consumerId.empty()) {
        ss << ",consumer_id:" << consumerId;
    }
    if (!clientId.empty()) {
        ss << ",client_id:" << clientId;
    }
    if (delayFlushTime) {
        ss << ",delay_flush_time:" << delayFlushTime.value();
    }
    if (pageSize) {
        ss << ",page_size:" << pageSize.value();
    }
    if (maxStreamSize) {
        ss << ",max_stream_size:" << maxStreamSize.value();
    }
    if (autoCleanup) {
        ss << ",auto_cleanup:" << autoCleanup.value();
    }
    if (retainForNumConsumers) {
        ss << ",retain_for_num_consumers:" << retainForNumConsumers.value();
    }
    if (encryptStream) {
        ss << ",encrypt_stream:" << encryptStream.value();
    }
    if (!subscriptionName.empty()) {
        ss << ",subscription_name:" << subscriptionName;
    }
    if (autoAck) {
        ss << ",auto_ack:" << autoAck.value();
    }
    if (streamMode) {
        ss << ",stream_mode:" << streamMode.value();
    }
    ss << "}";
    return ss.str();
}

std::string StreamResponseParam::ToString() const
{
    std::stringstream ss;
    ss << "{msg:" << (msg.empty() ? "OK" : msg.c_str());
    if (count) {
        ss << ",count:" << count.value();
    }
    if (senderProducerNo) {
        ss << ",sender_producer_no:" << senderProducerNo.value();
    }
    if (enableDataVerification) {
        ss << ",enable_data_verification:" << enableDataVerification.value();
    }
    if (streamNo) {
        ss << ",stream_no:" << streamNo.value();
    }
    if (enableSharedPage) {
        ss << ",enable_shared_page:" << enableSharedPage.value();
    }
    if (sharedPageSize) {
        ss << ",shared_page_size:" << sharedPageSize.value();
    }
    ss << "}";
    return ss.str();
}
}  // namespace datasystem

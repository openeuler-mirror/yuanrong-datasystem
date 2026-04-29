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
#ifndef DATASYSTEM_COMMON_LOG_ACCESS_RECORDER_H
#define DATASYSTEM_COMMON_LOG_ACCESS_RECORDER_H

#include <chrono>
#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

namespace datasystem {
constexpr uint64_t LOG_OBJECT_KEY_SIZE_LIMIT = 255;
constexpr uint64_t LOG_TOTAL_KEYS_SIZE_LIMIT = 1024;
constexpr uint64_t LOG_OBJECT_KEYS_COUNT_LIMIT = 10;
#define ACCESS_RECORDER_KEY_DEF(keyEnum, keyType) keyEnum,
enum class AccessRecorderKey : size_t {
#include "datasystem/common/log/access_point.def"
};
#undef ACCESS_RECORDER_KEY_DEF

enum class AccessKeyType { CLIENT, ACCESS, REQUEST_OUT };

struct RequestParam {
    std::string ToString() const;
    std::string objectKey;
    std::string nestedKey;
    std::string keep;
    std::string writeMode;
    std::string consistencyType;
    std::string isSeal;
    std::string isRetry;
    std::string ttlSecond;
    std::string existence;
    std::string subTimeout;
    std::string timeout;
    std::string outReq;
    std::string cacheType;
    std::string remoteClientId;
    std::string transportType;
};

struct StreamRequestParam {
    std::string ToString() const;
    std::string streamName;
    std::string producerId;
    std::string consumerId;
    std::string clientId;
    Optional<int64_t> delayFlushTime;
    Optional<int64_t> pageSize;
    Optional<uint64_t> maxStreamSize;
    Optional<bool> autoCleanup;
    Optional<uint64_t> retainForNumConsumers;
    Optional<bool> encryptStream;
    std::string subscriptionName;
    Optional<bool> autoAck;
    Optional<uint64_t> reserveSize;
    Optional<int32_t> streamMode;
};

struct StreamResponseParam {
    std::string ToString() const;
    std::string msg;
    Optional<uint64_t> count;
    Optional<uint64_t> senderProducerNo;
    Optional<bool> enableDataVerification;
    Optional<uint64_t> streamNo;
    Optional<uint64_t> sharedPageSize;
    Optional<bool> enableSharedPage;
};

class AccessRecorder {
public:
    /**
     * @brief Construct the AccessRecorder.
     */
    explicit AccessRecorder(AccessRecorderKey key);

    explicit AccessRecorder(std::string keyName);

    ~AccessRecorder();

    /**
     * @brief Get performance information and write this message.
     * @param[in] code The status code of the operation.
     * @param[in] dataSize Data size of the operation.
     * @param[in] reqParam Parameters of the request.
     * @param[in] respMsg Error message of the response.
     * @param[in] asyncElapseTime the time of this request being in the async queue.
     */
    void Record(StatusCode code, const std::string &dataSize = "", const RequestParam &reqParam = {},
                const std::string &respMsg = "", uint64_t asyncElapseTime = 0);

    /**
     * @brief Get performance information and write this message.
     * @param[in] code The status code of the operation.
     * @param[in] dataSize Data size of the operation.
     * @param[in] reqParam Parameters of the request.
     * @param[in] respMsg Error message of the response.
     * @param[in] asyncElapseTime the time of this request being in the async queue.
     */
    void Record(int code, const std::string &dataSize = "", const RequestParam &reqParam = {},
                const std::string &respMsg = "", uint64_t asyncElapseTime = 0);

    /**
     * @brief Get performance information and write this message.
     * @param[in] code The status code of the operation.
     * @param[in] reqParam Parameters of the request.
     * @param[in] respMsg Error message of the response.
     */
    void Record(StatusCode code, const StreamRequestParam &reqParam, const StreamResponseParam &respParam);

private:
    using clock = std::chrono::steady_clock;
    std::chrono::time_point<clock> beg_;
    std::string handleName_;
    AccessKeyType handleType_;
    bool isRecord_ = false;
};

struct AccessRecorderStreamWrap {
    AccessRecorder recorder;
    StreamRequestParam reqParam;
    StreamResponseParam rspParam;
    Status rc;

    AccessRecorderStreamWrap(AccessRecorderKey key) : recorder(key)
    {
    }

    ~AccessRecorderStreamWrap()
    {
        rspParam.msg = rc.GetMsg();
        try {
            recorder.Record(rc.GetCode(), reqParam, rspParam);
        } catch (const std::invalid_argument &ex) {
            LOG(ERROR) << ex.what();
        }
    }

    void SetStatus(Status status)
    {
        if (status.IsError()) {
            rc = status;
        }
    }
};

class AccessRecorderManager {
public:
    AccessRecorderManager(const AccessRecorderManager &other) = delete;

    AccessRecorderManager(AccessRecorderManager &&other) = delete;

    AccessRecorderManager &operator=(const AccessRecorderManager &) = delete;

    AccessRecorderManager &operator=(AccessRecorderManager &&) = delete;

    AccessRecorderManager() = default;

    ~AccessRecorderManager() = default;

    /**
     * @brief Init time cost logger.
     * @param[in] isClient Whether is client.
     * @param[in] isEmbeddedClient Whether is embedded client.
     * @return Status of the call.
     */
    Status Init(bool isClient, bool isEmbeddedClient);

    /**
     * @brief Write the log message to log cache buffer.
     * @param[in] handleName The name of the operation.
     * @param[in] type The type of the operation.
     * @param[in] code The status code of the operation.
     * @param[in] microsecond The time of this operation cost.
     * @param[in] dataSize Data size of the operation.
     * @param[in] reqMsg The request message.
     * @param[in] respMsg Error message of the response.
     * @param[in] asyncElapseTime the time of this request being in the async queue.
     * @return Status of the call.
     */
    Status LogPerformance(const std::string &handleName, AccessKeyType type, int64_t microsecond, int code = 0,
                          const std::string &dataSize = "", const std::string &reqMsg = "",
                          const std::string &respMsg = "", uint64_t asyncElapseTime = 0);

    /**
     * @brief Reset the logger.
     * @return Status of the call.
     */
    Status ResetWriteLogger(bool isEmbeddedClient);

    /**
     * @brief Write message and submit to flush pool directly.
     * @return Status of the call.
     */
    Status SubmitWriteMessage();

private:
    std::unordered_map<AccessKeyType, std::unique_ptr<HardDiskExporter>> exporterMap_;
    bool isClient_ = false;
};

std::string objectKeysToString(const std::vector<std::string> &keys);

// This function is not available in the opensource_master branch.
std::string objectKeysToString(const char **cKey, size_t keyLen);

template <typename T>
std::string ObjectKeysToAbbrStr(T &&keys)
{
    if (keys.empty()) {
        return "+count:0";
    }
    return FormatString("%s,count:%s", keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT), keys.size());
}
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_LOG_ACCESS_RECORDER_H

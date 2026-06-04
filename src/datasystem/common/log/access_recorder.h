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
#include <cstdint>
#include <functional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include <google/protobuf/repeated_field.h>

#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"

namespace datasystem {

class HardDiskExporter;
class ObjectAccessRecorder;
class StreamAccessRecorder;
class RequestOutRecorder;
constexpr uint64_t LOG_OBJECT_KEY_SIZE_LIMIT = 255;
constexpr uint64_t LOG_TOTAL_KEYS_SIZE_LIMIT = 1024;
constexpr uint64_t LOG_OBJECT_KEYS_COUNT_LIMIT = 10;
#define ACCESS_RECORDER_KEY_DEF(keyEnum, keyType) keyEnum,
enum class AccessRecorderKey : size_t {
#include "datasystem/common/log/access_point.def"
};
#undef ACCESS_RECORDER_KEY_DEF

enum class AccessKeyType { CLIENT, ACCESS, REQUEST_OUT };

/**
 * @brief Transport kind recorded in client access logs.
 */
enum class AccessTransportKind : uint8_t {
    SHM = 0,
    UB,
    TCP,
};

/**
 * @brief Thread-local tracker for the actual client-to-worker transport of the current request.
 */
class AccessTransportTracker {
public:
    AccessTransportTracker() = delete;
    ~AccessTransportTracker() = delete;

    static void Reset();
    static void Record(AccessTransportKind kind);
    static std::string ToString();

private:
    static thread_local AccessTransportKind current_;
};

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

struct CArrayRef {
    const char *const *keys = nullptr;
    const size_t *lens = nullptr;
    uint64_t count = 0;
};

struct FieldEntry {
    enum Kind : uint8_t { NONE, NUMERIC, TEXT_VIEW, TEXT_OWNED, TRACKED_TRANSPORT, PROVIDER };
    Kind kind = NONE;
    int64_t numericValue = 0;
    std::string_view textView;
    std::string textOwned;
    std::function<std::string()> provider;

    void SetNumeric(int64_t v)
    {
        kind = NUMERIC;
        numericValue = v;
    }

    void SetView(std::string_view v)
    {
        kind = TEXT_VIEW;
        textView = v;
    }

    void SetOwned(std::string v)
    {
        kind = TEXT_OWNED;
        textOwned = std::move(v);
    }

    void SetTrackedTransport()
    {
        kind = TRACKED_TRANSPORT;
    }

    void SetProvider(std::function<std::string()> p)
    {
        kind = PROVIDER;
        provider = std::move(p);
    }

    void Fill(std::string &out) const;
};

struct ObjectKeyState {
    enum Kind : uint8_t {
        NONE,
        VIEW,
        OWNED,
        VECTOR_REF,
        VECTOR_SUMMARY_REF,
        PROTO_REF,
        C_ARRAY_REF,
        PROVIDER
    };

    Kind kind = NONE;
    std::string_view view;
    std::string owned;
    const std::vector<std::string> *vectorRef = nullptr;
    const google::protobuf::RepeatedPtrField<std::string> *protoRef = nullptr;
    CArrayRef cArray;
    std::function<std::string()> provider;

    void FillObjectKey(RequestParam &req) const;
    void FillNestedKey(RequestParam &req) const;
};

struct DataSizeState {
    uint64_t value = 0;
    std::function<uint64_t()> provider;

    uint64_t Resolve() const
    {
        return provider ? provider() : value;
    }
};

struct ObjectAccessState {
    ObjectKeyState objectKey;
    ObjectKeyState nestedKey;
    FieldEntry writeMode;
    FieldEntry consistencyType;
    FieldEntry remoteClientId;
    FieldEntry transportType;
    FieldEntry keep;
    FieldEntry isSeal;
    FieldEntry isRetry;
    FieldEntry ttlSecond;
    FieldEntry existence;
    FieldEntry subTimeout;
    FieldEntry timeout;
    FieldEntry cacheType;
    int code = static_cast<int>(StatusCode::K_OK);
    std::string respMsg;
    DataSizeState dataSize;
    uint64_t asyncElapsedUs = 0;
};

struct StreamAccessState {
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
    int code = static_cast<int>(StatusCode::K_OK);
    std::string respMsg;
    Optional<uint64_t> count;
    Optional<uint64_t> senderProducerNo;
    Optional<bool> enableDataVerification;
    Optional<uint64_t> streamNo;
    Optional<uint64_t> sharedPageSize;
    Optional<bool> enableSharedPage;
};

struct RequestOutState {
    enum OutReqKind : uint8_t { NONE, VIEW, OWNED, PROVIDER };
    OutReqKind outReqKind = NONE;
    std::string_view outReqView;
    std::string outReqOwned;
    std::function<std::string()> outReqProvider;
    uint64_t dataSize = 0;
    uint64_t asyncElapsedUs = 0;
    int code = static_cast<int>(StatusCode::K_OK);
    std::string respMsg;
};

class AccessRecorder {
public:
    explicit AccessRecorder(AccessRecorderKey key);
    ~AccessRecorder();

    AccessRecorder(const AccessRecorder &) = delete;
    AccessRecorder &operator=(const AccessRecorder &) = delete;

    AccessRecorder(AccessRecorder &&other) noexcept;
    AccessRecorder &operator=(AccessRecorder &&other) noexcept;

    static ObjectAccessRecorder Object(AccessRecorderKey key);
    static StreamAccessRecorder Stream(AccessRecorderKey key);
    static RequestOutRecorder RequestOut(AccessRecorderKey key);

private:
    friend class ObjectAccessRecorder;
    friend class StreamAccessRecorder;
    friend class RequestOutRecorder;

    using clock = std::chrono::steady_clock;

    void WritePerformance(int code, uint64_t dataSize, uint64_t elapsed, const RequestParam &reqParam,
                          const std::string &respMsg, uint64_t asyncElapseTime);

    bool ShouldRecordInternal() const
    {
        return shouldRecord_;
    }

    uint64_t ElapsedUs() const;

    void MarkRecorded()
    {
        isRecord_ = true;
    }

    void WriteObject(int code, uint64_t dataSize, uint64_t elapsedUs, const RequestParam &req,
                     const std::string &respMsg, uint64_t asyncElapsedUs);

    void WriteStream(int code, uint64_t elapsedUs, const StreamRequestParam &req, const StreamResponseParam &rsp);

    std::chrono::time_point<clock> beg_;
    std::string handleName_;
    AccessKeyType handleType_;
    bool isRecord_ = false;
    bool shouldRecord_ = false;
};

class ObjectAccessRecorder {
public:
    ObjectAccessRecorder(ObjectAccessRecorder &&) noexcept = default;
    ObjectAccessRecorder &operator=(ObjectAccessRecorder &&) noexcept = default;
    ObjectAccessRecorder(const ObjectAccessRecorder &) = delete;
    ObjectAccessRecorder &operator=(const ObjectAccessRecorder &) = delete;
    ~ObjectAccessRecorder() = default;

    ObjectAccessRecorder &ObjectKeyRef(std::string_view key);
    ObjectAccessRecorder &ObjectKeyOwned(std::string key);
    template <typename Provider>
    ObjectAccessRecorder &ObjectKeyProvider(Provider &&provider)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        state_.objectKey.kind = ObjectKeyState::PROVIDER;
        state_.objectKey.provider = std::forward<Provider>(provider);
        return *this;
    }
    ObjectAccessRecorder &ObjectKeysRef(const std::vector<std::string> &keys);
    ObjectAccessRecorder &ObjectKeysRef(const google::protobuf::RepeatedPtrField<std::string> &keys);
    ObjectAccessRecorder &ObjectKeysRef(const char *const *keys, const size_t *keyLens, uint64_t keyCount);
    ObjectAccessRecorder &ObjectKeysSummaryRef(const std::vector<std::string> &keys);
    ObjectAccessRecorder &NestedKeysRef(const std::vector<std::string> &keys);
    ObjectAccessRecorder &NestedKeysRef(const char *const *keys, const size_t *keyLens, uint64_t keyCount);
    template <typename Provider>
    ObjectAccessRecorder &NestedKeyProvider(Provider &&provider)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        state_.nestedKey.kind = ObjectKeyState::PROVIDER;
        state_.nestedKey.provider = std::forward<Provider>(provider);
        return *this;
    }

    ObjectAccessRecorder &Keep(bool keep);
    ObjectAccessRecorder &WriteMode(int writeMode);
    ObjectAccessRecorder &WriteModeText(std::string_view writeMode);
    template <typename Provider>
    ObjectAccessRecorder &WriteModeProvider(Provider &&p)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        state_.writeMode.SetProvider([p = std::forward<Provider>(p)]() {
            return std::to_string(p());
        });
        return *this;
    }
    ObjectAccessRecorder &ConsistencyType(int consistencyType);
    ObjectAccessRecorder &ConsistencyTypeText(std::string_view consistencyType);
    ObjectAccessRecorder &IsSeal(bool isSeal);
    ObjectAccessRecorder &IsRetry(bool isRetry);
    ObjectAccessRecorder &TtlSecond(uint32_t ttlSecond);
    template <typename Provider>
    ObjectAccessRecorder &TtlSecondProvider(Provider &&p)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        state_.ttlSecond.SetProvider([p = std::forward<Provider>(p)]() {
            return std::to_string(p());
        });
        return *this;
    }
    ObjectAccessRecorder &Existence(int existence);
    ObjectAccessRecorder &SubTimeoutMs(uint64_t timeoutMs);
    ObjectAccessRecorder &TimeoutMs(uint64_t timeoutMs);
    ObjectAccessRecorder &CacheType(int cacheType);
    ObjectAccessRecorder &RemoteClientId(std::string_view remoteClientId);
    ObjectAccessRecorder &TransportType(std::string_view transportType);
    ObjectAccessRecorder &TransportTypeOwned(std::string transportType);
    ObjectAccessRecorder &TrackedTransportType();

    ObjectAccessRecorder &DataSize(uint64_t dataSize);
    template <typename SizeProvider>
    ObjectAccessRecorder &DataSizeProvider(SizeProvider &&provider)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        state_.dataSize.provider = std::forward<SizeProvider>(provider);
        return *this;
    }

    ObjectAccessRecorder &Result(const Status &rc);
    ObjectAccessRecorder &Result(StatusCode code, std::string_view msg = {});
    ObjectAccessRecorder &Result(int code, std::string_view msg = {});

    ObjectAccessRecorder &AsyncElapsedUs(uint64_t asyncElapsedUs);

    void Record();

private:
    friend class AccessRecorder;
    explicit ObjectAccessRecorder(AccessRecorderKey key);
    AccessRecorder core_;
    ObjectAccessState state_;
};

class StreamAccessRecorder {
public:
    StreamAccessRecorder(StreamAccessRecorder &&) noexcept = default;
    StreamAccessRecorder &operator=(StreamAccessRecorder &&) noexcept = default;
    StreamAccessRecorder(const StreamAccessRecorder &) = delete;
    StreamAccessRecorder &operator=(const StreamAccessRecorder &) = delete;
    ~StreamAccessRecorder() = default;

    StreamAccessRecorder &StreamName(std::string_view streamName);
    StreamAccessRecorder &ProducerId(std::string_view producerId);
    StreamAccessRecorder &ConsumerId(std::string_view consumerId);
    StreamAccessRecorder &ClientId(std::string_view clientId);
    StreamAccessRecorder &DelayFlushTime(int64_t value);
    StreamAccessRecorder &PageSize(int64_t value);
    StreamAccessRecorder &MaxStreamSize(uint64_t value);
    StreamAccessRecorder &AutoCleanup(bool value);
    StreamAccessRecorder &RetainForNumConsumers(uint64_t value);
    StreamAccessRecorder &EncryptStream(bool value);
    StreamAccessRecorder &SubscriptionName(std::string_view subscriptionName);
    StreamAccessRecorder &AutoAck(bool value);
    StreamAccessRecorder &ReserveSize(uint64_t value);
    StreamAccessRecorder &StreamMode(int32_t value);

    StreamAccessRecorder &Count(uint64_t value);
    StreamAccessRecorder &SenderProducerNo(uint64_t value);
    StreamAccessRecorder &EnableDataVerification(bool value);
    StreamAccessRecorder &StreamNo(uint64_t value);
    StreamAccessRecorder &SharedPageSize(uint64_t value);
    StreamAccessRecorder &EnableSharedPage(bool value);

    StreamAccessRecorder &Result(const Status &rc);
    StreamAccessRecorder &Result(StatusCode code, std::string_view msg = {});
    StreamAccessRecorder &Result(int code, std::string_view msg = {});

    void Record();

private:
    friend class AccessRecorder;
    explicit StreamAccessRecorder(AccessRecorderKey key);
    AccessRecorder core_;
    StreamAccessState state_;
};

class RequestOutRecorder {
public:
    RequestOutRecorder(RequestOutRecorder &&) noexcept = default;
    RequestOutRecorder &operator=(RequestOutRecorder &&) noexcept = default;
    RequestOutRecorder(const RequestOutRecorder &) = delete;
    RequestOutRecorder &operator=(const RequestOutRecorder &) = delete;
    ~RequestOutRecorder() = default;

    RequestOutRecorder &OutReq(std::string_view req);
    RequestOutRecorder &OutReqOwned(std::string req);
    template <typename Proto>
    RequestOutRecorder &OutReqKeyIfPresent(const Proto &req)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        if constexpr (HasMemberFunc_key<Proto>::value) {
            state_.outReqKind = RequestOutState::VIEW;
            state_.outReqView = req.key();
        }
        return *this;
    }
    template <typename Provider>
    RequestOutRecorder &OutReqProvider(Provider &&provider)
    {
        if (!core_.ShouldRecordInternal()) {
            return *this;
        }
        state_.outReqKind = RequestOutState::PROVIDER;
        state_.outReqProvider = std::forward<Provider>(provider);
        return *this;
    }
    RequestOutRecorder &DataSize(uint64_t dataSize);
    RequestOutRecorder &AsyncElapsedUs(uint64_t asyncElapsedUs);
    RequestOutRecorder &Result(int code, std::string_view msg = {});
    RequestOutRecorder &Result(StatusCode code, std::string_view msg = {});

    void Record();

private:
    friend class AccessRecorder;
    explicit RequestOutRecorder(AccessRecorderKey key);
    template <typename T, typename = void>
    struct HasMemberFunc_key : std::false_type {};
    template <typename T>
    struct HasMemberFunc_key<T, std::void_t<decltype(std::declval<T>().key())>> : std::true_type {};
    AccessRecorder core_;
    RequestOutState state_;
};

class AccessRecorderManager {
public:
    AccessRecorderManager(const AccessRecorderManager &other) = delete;

    AccessRecorderManager(AccessRecorderManager &&other) = delete;

    AccessRecorderManager &operator=(const AccessRecorderManager &) = delete;

    AccessRecorderManager &operator=(AccessRecorderManager &&) = delete;

    AccessRecorderManager() = default;

    ~AccessRecorderManager();

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

AccessKeyType GetAccessKeyType(AccessRecorderKey key);

bool IsCurrentRequestLogSampled(AccessKeyType type);

std::string FormatAccessReqMsg(AccessKeyType type, const std::string &reqMsg);

std::string objectKeysToString(const std::vector<std::string> &keys);

std::string objectKeysToString(const char **cKey, size_t keyLen);

std::string objectKeysToString(const char *const *cKey, const size_t *cKeyLens, size_t keyLen);

std::string objectKeysToString(const google::protobuf::RepeatedPtrField<std::string> &keys);

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
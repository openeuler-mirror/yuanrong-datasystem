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

#include <algorithm>
#include <cstddef>
#include <sstream>
#include <string>
#include <utility>

#include <unistd.h>

#include "datasystem/common/constants.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/log_sampler.h"
#include "datasystem/common/log/logging.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/hard_disk_exporter/hard_disk_exporter.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/request_context.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

DS_DECLARE_bool(log_monitor);
DS_DECLARE_string(log_dir);

namespace datasystem {
namespace {
constexpr char LOG_SAMPLED_MARK[] = "logSampled:true";
constexpr size_t EMPTY_REQ_MSG_SIZE = 2;

#define ACCESS_RECORDER_KEY_DEF(keyEnum, keyType) #keyEnum,
static const char *g_keyNames[] = {
#include "datasystem/common/log/access_point.def"
};
#undef ACCESS_RECORDER_KEY_DEF

#define ACCESS_RECORDER_KEY_DEF(keyEnum, keyType) AccessKeyType::keyType,
static AccessKeyType gKeyTypes[] = {
#include "datasystem/common/log/access_point.def"
};
#undef ACCESS_RECORDER_KEY_DEF
}  // namespace

bool IsCurrentRequestLogSampled(AccessKeyType type)
{
    if (type == AccessKeyType::REQUEST_OUT || !Trace::Instance().IsRequestLogTrace()) {
        return false;
    }
    if (LogSampler::Instance().IsSamplerEnabledFast()) {
        return LogSampler::Instance().IsCurrentRequestSampledIn();
    }
    return true;
}

std::string FormatAccessReqMsg(AccessKeyType type, const std::string &reqMsg)
{
    std::ostringstream logStream;
    if (!IsCurrentRequestLogSampled(type)) {
        return reqMsg;
    }
    if (reqMsg.size() >= EMPTY_REQ_MSG_SIZE && reqMsg.front() == '{' && reqMsg.back() == '}') {
        logStream.write(reqMsg.data(), reqMsg.size() - 1);
        if (reqMsg.size() > EMPTY_REQ_MSG_SIZE) {
            logStream << ",";
        }
        logStream << LOG_SAMPLED_MARK << "}";
        return logStream.str();
    }
    logStream << "{";
    if (!reqMsg.empty()) {
        logStream << reqMsg << ",";
    }
    logStream << LOG_SAMPLED_MARK << "}";
    return logStream.str();
}

AccessKeyType GetAccessKeyType(AccessRecorderKey key)
{
    return gKeyTypes[static_cast<size_t>(key)];
}

AccessRecorder::AccessRecorder(AccessRecorderKey key)
    : handleType_(gKeyTypes[static_cast<size_t>(key)]), isRecord_(false)
{
    if (key == AccessRecorderKey::DS_ETCD_UNKNOWN) {
        shouldRecord_ = false;
        isRecord_ = true;
    } else {
        shouldRecord_ = LogSampler::Instance().ShouldRecordAccess(key);
        Trace::Instance().SetAccessShouldRecord(shouldRecord_);
        if (shouldRecord_) {
            handleName_ = g_keyNames[static_cast<size_t>(key)];
            beg_ = clock::now();
        } else {
            isRecord_ = true;
        }
    }
}

void AccessRecorder::WritePerformance(int code, uint64_t dataSize, uint64_t elapsed,
                                      const RequestParam &reqParam, const std::string &respMsg,
                                      uint64_t asyncElapseTime)
{
    if (FLAGS_log_monitor && Logging::AccessRecorderManagerInstance() != nullptr) {
        Logging::AccessRecorderManagerInstance()->LogPerformance(handleName_, handleType_, elapsed, code,
                                                                 std::to_string(dataSize), reqParam.ToString(),
                                                                 respMsg, asyncElapseTime);
    }
}

AccessRecorder::~AccessRecorder()
{
    if (!isRecord_) {
        LOG(WARNING) << handleName_ << " Not call AccessRecorder::Record()";
    }
}

AccessRecorder::AccessRecorder(AccessRecorder &&other) noexcept
    : beg_(other.beg_),
      handleName_(std::move(other.handleName_)),
      handleType_(other.handleType_),
      isRecord_(other.isRecord_),
      shouldRecord_(other.shouldRecord_)
{
    other.isRecord_ = true;
    other.shouldRecord_ = false;
}

AccessRecorder &AccessRecorder::operator=(AccessRecorder &&other) noexcept
{
    if (this == &other) {
        return *this;
    }
    beg_ = other.beg_;
    handleName_ = std::move(other.handleName_);
    handleType_ = other.handleType_;
    isRecord_ = other.isRecord_;
    shouldRecord_ = other.shouldRecord_;
    other.isRecord_ = true;
    other.shouldRecord_ = false;
    return *this;
}

uint64_t AccessRecorder::ElapsedUs() const
{
    return std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - beg_).count();
}

void AccessRecorder::WriteObject(int code, uint64_t dataSize, uint64_t elapsedUs, const RequestParam &req,
                                 const std::string &respMsg, uint64_t asyncElapsedUs)
{
    WritePerformance(code, dataSize, elapsedUs, req, respMsg, asyncElapsedUs);
}

void AccessRecorder::WriteStream(int code, uint64_t elapsedUs, const StreamRequestParam &req,
                                 const StreamResponseParam &rsp)
{
    if (FLAGS_log_monitor && Logging::AccessRecorderManagerInstance() != nullptr) {
        Logging::AccessRecorderManagerInstance()->LogPerformance(handleName_, handleType_, elapsedUs, code, "0",
                                                                 req.ToString(), rsp.ToString());
    }
}

ObjectAccessRecorder AccessRecorder::Object(AccessRecorderKey key)
{
    return ObjectAccessRecorder(key);
}

StreamAccessRecorder AccessRecorder::Stream(AccessRecorderKey key)
{
    return StreamAccessRecorder(key);
}

RequestOutRecorder AccessRecorder::RequestOut(AccessRecorderKey key)
{
    return RequestOutRecorder(key);
}

void FieldEntry::Fill(std::string &out) const
{
    switch (kind) {
        case NUMERIC:
            out = std::to_string(numericValue);
            break;
        case TEXT_VIEW:
            out = textView.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
            break;
        case TEXT_OWNED:
            out = textOwned.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
            break;
        case TRACKED_TRANSPORT:
            out = AccessTransportTracker::ToString();
            break;
        case PROVIDER:
            if (provider) {
                out = provider();
            }
            break;
        default:
            break;
    }
}

void ObjectKeyState::FillObjectKey(RequestParam &req) const
{
    switch (kind) {
        case VIEW:
            req.objectKey = view.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
            break;
        case OWNED:
            req.objectKey = owned.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
            break;
        case VECTOR_REF:
            req.objectKey = objectKeysToString(*vectorRef);
            break;
        case VECTOR_SUMMARY_REF:
            req.objectKey = ObjectKeysToAbbrStr(*vectorRef);
            break;
        case PROTO_REF: {
            std::vector<std::string> keys;
            keys.reserve(protoRef->size());
            for (const auto &k : *protoRef) {
                keys.emplace_back(k);
            }
            req.objectKey = objectKeysToString(keys);
            break;
        }
        case C_ARRAY_REF:
            req.objectKey = objectKeysToString(cArray.keys, cArray.lens, cArray.count);
            break;
        case PROVIDER:
            req.objectKey = provider();
            break;
        default:
            break;
    }
}

void ObjectKeyState::FillNestedKey(RequestParam &req) const
{
    switch (kind) {
        case VIEW:
            req.nestedKey = view.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
            break;
        case OWNED:
            req.nestedKey = owned.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT);
            break;
        case VECTOR_REF:
            req.nestedKey = objectKeysToString(*vectorRef);
            break;
        case C_ARRAY_REF:
            req.nestedKey = objectKeysToString(cArray.keys, cArray.lens, cArray.count);
            break;
        case PROVIDER:
            req.nestedKey = provider();
            break;
        default:
            break;
    }
}

ObjectAccessRecorder::ObjectAccessRecorder(AccessRecorderKey key) : core_(key), state_() {}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeyRef(std::string_view key)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::VIEW;
    state_.objectKey.view = key;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeyOwned(const std::string &key)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::OWNED;
    state_.objectKey.owned = key;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeyOwned(std::string &&key)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::OWNED;
    state_.objectKey.owned = std::move(key);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeysRef(const std::vector<std::string> &keys)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::VECTOR_REF;
    state_.objectKey.vectorRef = &keys;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeysRef(const google::protobuf::RepeatedPtrField<std::string> &keys)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::PROTO_REF;
    state_.objectKey.protoRef = &keys;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeysRef(const char *const *keys, const size_t *keyLens,
                                                          uint64_t keyCount)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::C_ARRAY_REF;
    state_.objectKey.cArray.keys = keys;
    state_.objectKey.cArray.lens = keyLens;
    state_.objectKey.cArray.count = keyCount;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ObjectKeysSummaryRef(const std::vector<std::string> &keys)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.objectKey.kind = ObjectKeyState::VECTOR_SUMMARY_REF;
    state_.objectKey.vectorRef = &keys;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::NestedKeysRef(const std::vector<std::string> &keys)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.nestedKey.kind = ObjectKeyState::VECTOR_REF;
    state_.nestedKey.vectorRef = &keys;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::NestedKeysRef(const char *const *keys, const size_t *keyLens,
                                                          uint64_t keyCount)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.nestedKey.kind = ObjectKeyState::C_ARRAY_REF;
    state_.nestedKey.cArray.keys = keys;
    state_.nestedKey.cArray.lens = keyLens;
    state_.nestedKey.cArray.count = keyCount;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::Keep(bool keep)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.keep.SetNumeric(keep ? 1 : 0);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::WriteMode(int writeMode)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.writeMode.SetNumeric(writeMode);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::WriteModeText(std::string_view writeMode)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.writeMode.SetView(writeMode);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ConsistencyType(int consistencyType)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.consistencyType.SetNumeric(consistencyType);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::ConsistencyTypeText(std::string_view consistencyType)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.consistencyType.SetView(consistencyType);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::IsSeal(bool isSeal)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.isSeal.SetNumeric(isSeal ? 1 : 0);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::IsRetry(bool isRetry)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.isRetry.SetNumeric(isRetry ? 1 : 0);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::TtlSecond(uint32_t ttlSecond)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.ttlSecond.SetNumeric(ttlSecond);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::Existence(int existence)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.existence.SetNumeric(existence);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::SubTimeoutMs(uint64_t timeoutMs)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.subTimeout.SetNumeric(timeoutMs);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::TimeoutMs(uint64_t timeoutMs)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.timeout.SetNumeric(timeoutMs);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::CacheType(int cacheType)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.cacheType.SetNumeric(cacheType);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::RemoteClientId(std::string_view remoteClientId)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.remoteClientId.SetView(remoteClientId);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::TransportType(std::string_view transportType)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.transportType.SetView(transportType);
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::TransportTypeOwned(std::string transportType)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.transportType.SetOwned(std::move(transportType));
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::TrackedTransportType()
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.transportType.SetTrackedTransport();
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::DataSize(uint64_t dataSize)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.dataSize.value = dataSize;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::Result(const Status &rc)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = rc.GetCode();
    state_.respMsg = rc.GetMsg();
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::Result(StatusCode code, std::string_view msg)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = static_cast<int>(code);
    state_.respMsg = msg;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::Result(int code, std::string_view msg)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = code;
    state_.respMsg = msg;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::AsyncElapsedUs(uint64_t asyncElapsedUs)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.asyncElapsedUs = asyncElapsedUs;
    return *this;
}

ObjectAccessRecorder &ObjectAccessRecorder::LatencySummary(std::string summary)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.latencySummary = std::move(summary);
    return *this;
}

void ObjectAccessRecorder::Record()
{
    if (!core_.ShouldRecordInternal()) {
        core_.MarkRecorded();
        return;
    }
    if (state_.latencySummary.empty()) {
        state_.latencySummary = Trace::Instance().ConsumeLatencySummary();
    }
    uint64_t elapsed = core_.ElapsedUs();
    RequestParam req;
    state_.objectKey.FillObjectKey(req);
    state_.nestedKey.FillNestedKey(req);
    state_.writeMode.Fill(req.writeMode);
    state_.consistencyType.Fill(req.consistencyType);
    state_.remoteClientId.Fill(req.remoteClientId);
    state_.transportType.Fill(req.transportType);
    state_.keep.Fill(req.keep);
    state_.isSeal.Fill(req.isSeal);
    state_.isRetry.Fill(req.isRetry);
    state_.ttlSecond.Fill(req.ttlSecond);
    state_.existence.Fill(req.existence);
    state_.subTimeout.Fill(req.subTimeout);
    state_.timeout.Fill(req.timeout);
    state_.cacheType.Fill(req.cacheType);
    req.latencySummary = state_.latencySummary;
    core_.WriteObject(state_.code, state_.dataSize.Resolve(), elapsed, req, state_.respMsg, state_.asyncElapsedUs);
    core_.MarkRecorded();
}

StreamAccessRecorder::StreamAccessRecorder(AccessRecorderKey key) : core_(key), state_() {}

StreamAccessRecorder &StreamAccessRecorder::StreamName(std::string_view streamName)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.streamName = streamName;
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::ProducerId(std::string_view producerId)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.producerId = producerId;
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::ConsumerId(std::string_view consumerId)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.consumerId = consumerId;
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::ClientId(std::string_view clientId)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.clientId = clientId;
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::DelayFlushTime(int64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.delayFlushTime = Optional<int64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::PageSize(int64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.pageSize = Optional<int64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::MaxStreamSize(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.maxStreamSize = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::AutoCleanup(bool value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.autoCleanup = Optional<bool>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::RetainForNumConsumers(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.retainForNumConsumers = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::EncryptStream(bool value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.encryptStream = Optional<bool>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::SubscriptionName(std::string_view subscriptionName)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.subscriptionName = subscriptionName;
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::AutoAck(bool value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.autoAck = Optional<bool>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::ReserveSize(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.reserveSize = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::StreamMode(int32_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.streamMode = Optional<int32_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::Count(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.count = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::SenderProducerNo(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.senderProducerNo = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::EnableDataVerification(bool value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.enableDataVerification = Optional<bool>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::StreamNo(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.streamNo = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::SharedPageSize(uint64_t value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.sharedPageSize = Optional<uint64_t>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::EnableSharedPage(bool value)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.enableSharedPage = Optional<bool>(value);
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::Result(const Status &rc)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = rc.GetCode();
    state_.respMsg = rc.GetMsg();
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::Result(StatusCode code, std::string_view msg)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = static_cast<int>(code);
    state_.respMsg = msg;
    return *this;
}

StreamAccessRecorder &StreamAccessRecorder::Result(int code, std::string_view msg)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = code;
    state_.respMsg = msg;
    return *this;
}

void StreamAccessRecorder::Record()
{
    if (!core_.ShouldRecordInternal()) {
        core_.MarkRecorded();
        return;
    }
    uint64_t elapsed = core_.ElapsedUs();
    StreamRequestParam req;
    req.streamName = state_.streamName;
    req.producerId = state_.producerId;
    req.consumerId = state_.consumerId;
    req.clientId = state_.clientId;
    req.delayFlushTime = state_.delayFlushTime;
    req.pageSize = state_.pageSize;
    req.maxStreamSize = state_.maxStreamSize;
    req.autoCleanup = state_.autoCleanup;
    req.retainForNumConsumers = state_.retainForNumConsumers;
    req.encryptStream = state_.encryptStream;
    req.subscriptionName = state_.subscriptionName;
    req.autoAck = state_.autoAck;
    req.reserveSize = state_.reserveSize;
    req.streamMode = state_.streamMode;
    StreamResponseParam rsp;
    rsp.msg = state_.respMsg.empty() ? "OK" : state_.respMsg;
    rsp.count = state_.count;
    rsp.senderProducerNo = state_.senderProducerNo;
    rsp.enableDataVerification = state_.enableDataVerification;
    rsp.streamNo = state_.streamNo;
    rsp.sharedPageSize = state_.sharedPageSize;
    rsp.enableSharedPage = state_.enableSharedPage;
    core_.WriteStream(state_.code, elapsed, req, rsp);
    core_.MarkRecorded();
}

RequestOutRecorder::RequestOutRecorder(AccessRecorderKey key) : core_(key), state_() {}

RequestOutRecorder &RequestOutRecorder::OutReq(std::string_view req)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.outReqKind = RequestOutState::VIEW;
    state_.outReqView = req;
    return *this;
}

RequestOutRecorder &RequestOutRecorder::OutReqOwned(std::string req)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.outReqKind = RequestOutState::OWNED;
    state_.outReqOwned = std::move(req);
    return *this;
}

RequestOutRecorder &RequestOutRecorder::DataSize(uint64_t dataSize)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.dataSize = dataSize;
    return *this;
}

RequestOutRecorder &RequestOutRecorder::AsyncElapsedUs(uint64_t asyncElapsedUs)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.asyncElapsedUs = asyncElapsedUs;
    return *this;
}

RequestOutRecorder &RequestOutRecorder::Result(int code, std::string_view msg)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = code;
    state_.respMsg = msg;
    return *this;
}

RequestOutRecorder &RequestOutRecorder::Result(StatusCode code, std::string_view msg)
{
    if (!core_.ShouldRecordInternal()) {
        return *this;
    }
    state_.code = static_cast<int>(code);
    state_.respMsg = msg;
    return *this;
}

void RequestOutRecorder::Record()
{
    if (!core_.ShouldRecordInternal()) {
        core_.MarkRecorded();
        return;
    }
    uint64_t elapsed = core_.ElapsedUs();
    RequestParam req;
    switch (state_.outReqKind) {
        case RequestOutState::VIEW:
            req.outReq = state_.outReqView;
            break;
        case RequestOutState::OWNED:
            req.outReq = state_.outReqOwned;
            break;
        case RequestOutState::PROVIDER:
            req.outReq = state_.outReqProvider();
            break;
        default:
            break;
    }
    core_.WriteObject(state_.code, state_.dataSize, elapsed, req, state_.respMsg, state_.asyncElapsedUs);
    core_.MarkRecorded();
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
        std::string clientAccessLogName = Logging::GetClientLogName(CLIENT_ACCESS_LOG_NAME, getpid());

        // Allow overriding client access log filename via config or environment variable.
        std::string accessLogName = Logging::GetClientAccessLogName();
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

AccessRecorderManager::~AccessRecorderManager() = default;

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
    logStream << " | " << dataSize << " | " << FormatAccessReqMsg(type, reqMsg);
    logStream << " | " << respMsg;
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
    for (const auto &key : keys) {
        if (count >= LOG_TOTAL_KEYS_SIZE_LIMIT) {
            ret.append("***").append(",").append("total:").append(std::to_string(keys.size())).append("]");
            return ret;
        }
        if (len > LOG_OBJECT_KEY_SIZE_LIMIT - key.length()) {
            ret.append("total:").append(std::to_string(keys.size())).append("]");
            return ret;
        }
        count += 1;
        len = std::min(len + static_cast<uint32_t>(key.length()), static_cast<uint32_t>(LOG_OBJECT_KEY_SIZE_LIMIT));
        ret.append(key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT)).append(",");
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("]");
    return ret;
}

std::string objectKeysToString(const google::protobuf::RepeatedPtrField<std::string> &keys)
{
    std::string ret = "[";
    uint32_t len = 0;
    uint32_t count = 0;
    for (const auto &key : keys) {
        if (count >= LOG_TOTAL_KEYS_SIZE_LIMIT) {
            ret.append("***").append(",").append("total:").append(std::to_string(keys.size())).append("]");
            return ret;
        }
        if (len > LOG_OBJECT_KEY_SIZE_LIMIT - key.length()) {
            ret.append("total:").append(std::to_string(keys.size())).append("]");
            return ret;
        }
        count += 1;
        len = std::min(len + static_cast<uint32_t>(key.length()),
                       static_cast<uint32_t>(LOG_OBJECT_KEY_SIZE_LIMIT));
        ret.append(key.substr(0, LOG_OBJECT_KEY_SIZE_LIMIT)).append(",");
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("]");
    return ret;
}

std::string objectKeysToString(const char *const *cKey, size_t keyLen)
{
    std::string ret = "[";
    uint32_t totalDisplayLen = 0;
    for (size_t i = 0; i < keyLen; i++) {
        if (cKey[i] == nullptr) {
            continue;
        }
        size_t declaredLen = strlen(cKey[i]);
        if (declaredLen == 0) {
            continue;
        }
        size_t boundedLen = std::min(declaredLen, static_cast<size_t>(LOG_TOTAL_KEYS_SIZE_LIMIT));
        size_t displayLen = std::min(boundedLen, static_cast<size_t>(LOG_OBJECT_KEY_SIZE_LIMIT));
        if (totalDisplayLen > LOG_TOTAL_KEYS_SIZE_LIMIT - displayLen) {
            ret.append("total:").append(std::to_string(keyLen)).append("]");
            return ret;
        }
        totalDisplayLen += displayLen;
        ret.append(cKey[i], displayLen).append(",");
    }
    if (ret.length() > 1) {
        ret.pop_back();
    }
    ret.append("]");
    return ret;
}

std::string objectKeysToString(const char *const *cKey, const size_t *cKeyLens, size_t keyLen)
{
    std::string ret = "[";
    uint32_t totalDisplayLen = 0;
    for (size_t i = 0; i < keyLen; i++) {
        if (cKey[i] == nullptr) {
            continue;
        }
        size_t declaredLen = (cKeyLens != nullptr) ? cKeyLens[i] : strlen(cKey[i]);
        if (declaredLen == 0) {
            continue;
        }
        size_t boundedLen = std::min(declaredLen, static_cast<size_t>(LOG_TOTAL_KEYS_SIZE_LIMIT));
        size_t displayLen = std::min(boundedLen, static_cast<size_t>(LOG_OBJECT_KEY_SIZE_LIMIT));
        if (totalDisplayLen > LOG_TOTAL_KEYS_SIZE_LIMIT - displayLen) {
            ret.append("total:").append(std::to_string(keyLen)).append("]");
            return ret;
        }
        totalDisplayLen += displayLen;
        ret.append(cKey[i], displayLen).append(",");
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
    if (!transportType.empty()) {
        ret.append("transportType:").append(transportType).append(",");
    }
    if (!latencySummary.empty()) {
        ret.append("latencySummary:").append(latencySummary).append(",");
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

const char* AccessTransportTracker::KindToName(AccessTransportKind kind)
{
    switch (kind) {
        case AccessTransportKind::UB:
            return "UB";
        case AccessTransportKind::TCP:
            return "TCP";
        case AccessTransportKind::SHM:
        default:
            return "SHM";
    }
}

void AccessTransportTracker::Reset()
{
    GetRequestContext()->accessTransportKind = AccessTransportKind::SHM;
}

void AccessTransportTracker::Record(AccessTransportKind kind)
{
    GetRequestContext()->accessTransportKind = kind;
}

std::string AccessTransportTracker::ToString()
{
    return KindToName(GetRequestContext()->accessTransportKind);
}
}  // namespace datasystem

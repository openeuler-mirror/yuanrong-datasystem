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

#include "datasystem/common/log/trace.h"

#include <algorithm>
#include <cstring>

#include <securec.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/log/spdlog/log_rate_limiter.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace {
uint64_t FNV1aHash(const std::string &s)
{
    if (s.empty()) {
        return 0;
    }
    uint64_t h = 14695981039346656037ULL;
    for (char c : s) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ULL;
    }
    return h;
}

void EnsureRequestSampleDecision(Trace &trace)
{
    if (!trace.IsRequestLogTrace() || trace.GetCachedHash() == 0) {
        return;
    }
    bool admitted = false;
    (void)LogRateLimiter::Instance().GetOrCreateRequestDecision(trace.GetCachedHash(), admitted);
}
}  // namespace

const int Trace::TRACEID_MAX_SIZE;
const int Trace::TRACEID_PREFIX_SIZE;
const int Trace::SHORT_UUID_SIZE;

Trace &Trace::Instance()
{
    // thread_local object, which is used for multi-thread traceID isolation.
    static thread_local Trace instance;
    return instance;
}

void TraceGuard::Reset()
{
    switch (type_) {
        case TraceGuardType::CLEAR_TRACE_ID:
            Trace::Instance().Invalidate();
            break;
        case TraceGuardType::CLEAR_SUB_TRACE_ID:
            Trace::Instance().InvalidateSubTraceID();
            break;
        case TraceGuardType::RESTORE_REQUEST_LOG_CONTEXT:
            Trace::Instance().SetRequestLogTrace(requestLogTrace_);
            Trace::Instance().SetRequestSampleDecision(requestSampleDecisionValid_, requestSampleDecisionAdmitted_);
            break;
        case TraceGuardType::INVALID:
            break;
        default:
            LOG(WARNING) << "Unsupport type: " << static_cast<int>(type_);
    }
    type_ = TraceGuardType::INVALID;
}

TraceGuard &TraceGuard::operator=(TraceGuard &&other) noexcept
{
    if (this != &other) {
        Reset();
        type_ = other.type_;
        requestLogTrace_ = other.requestLogTrace_;
        requestSampleDecisionValid_ = other.requestSampleDecisionValid_;
        requestSampleDecisionAdmitted_ = other.requestSampleDecisionAdmitted_;
        other.type_ = TraceGuardType::INVALID;
    }
    return *this;
}

TraceGuard::~TraceGuard()
{
    Reset();
}

TraceGuard Trace::SetTraceUUID()
{
    if (traceID_[0] != '\0') {
        return TraceGuard(TraceGuardType::INVALID);
    }
    SetRequestLogTrace(false);
    SetRequestSampleDecision(false, false);
    std::string uuid = GetStringUuid();
    auto prefixLen = std::min<size_t>(strlen(prefix_), TRACEID_PREFIX_SIZE);
    int ret = EOK;
    auto dest = traceID_;
    auto src = uuid.c_str();
    if (prefixLen > 0) {
        ret = memcpy_s(traceID_, TRACEID_MAX_SIZE, prefix_, prefixLen);
        if (ret != EOK) {
            LOG(ERROR) << "copy prefix to trace id failed: " << ret;
        }
        traceID_[prefixLen] = ';';
        dest += prefixLen + 1;
        if (uuid.length() > SHORT_UUID_SIZE) {
            src += uuid.length() - SHORT_UUID_SIZE;
        }
    }
    size_t destMax = prefixLen > 0 ? TRACEID_MAX_SIZE - prefixLen : TRACEID_MAX_SIZE + 1;
    ret = strcpy_s(dest, destMax, src);
    if (ret != EOK) {
        LOG(ERROR) << "copy uuid to trace id failed: " << ret;
    }
    cachedHash_ = FNV1aHash(traceID_);
    return TraceGuard(TraceGuardType::CLEAR_TRACE_ID);
}

TraceGuard Trace::SetRequestTraceUUID()
{
    if (traceID_[0] != '\0') {
        bool oldRequestLogTrace = requestLogTrace_;
        TraceGuard traceGuard(TraceGuardType::RESTORE_REQUEST_LOG_CONTEXT, requestLogTrace_,
                              requestSampleDecisionValid_, requestSampleDecisionAdmitted_);
        SetRequestLogTrace(true);
        if (!oldRequestLogTrace) {
            SetRequestSampleDecision(false, false);
        }
        EnsureRequestSampleDecision(*this);
        return traceGuard;
    }
    TraceGuard traceGuard = SetTraceUUID();
    SetRequestLogTrace(true);
    EnsureRequestSampleDecision(*this);
    return traceGuard;
}

void Trace::SetPrefix(const std::string &prefix)
{
    auto copySize = std::min<size_t>(TRACEID_PREFIX_SIZE, prefix.length());
    int ret = strncpy_s(prefix_, TRACEID_PREFIX_SIZE + 1, prefix.c_str(), copySize);
    if (ret != EOK) {
        LOG(ERROR) << "strncpy_s failed: " << ret;
    }
}

TraceGuard Trace::SetTraceNewID(const std::string &traceID, bool keep)
{
    SetRequestLogTrace(false);
    SetRequestSampleDecision(false, false);
    auto copySize = traceID.size();
    if (traceID.size() > TRACEID_MAX_SIZE) {
        LOG(WARNING) << FormatString("The traceID length %zu exceeds the maximum length %d.", traceID.size(),
                                     TRACEID_MAX_SIZE);
        copySize = TRACEID_MAX_SIZE;
    }
    int ret = strncpy_s(traceID_, TRACEID_MAX_SIZE + 1, traceID.c_str(), copySize);
    if (ret != EOK) {
        LOG(ERROR) << "Error number of strcpy_s: " << ret;
    }
    cachedHash_ = FNV1aHash(traceID_);
    return TraceGuard(keep ? TraceGuardType::INVALID : TraceGuardType::CLEAR_TRACE_ID);
}

TraceGuard Trace::SetTraceContext(const TraceContext &context, bool keep)
{
    bool oldRequestLogTrace = requestLogTrace_;
    bool oldRequestSampleDecisionValid = requestSampleDecisionValid_;
    bool oldRequestSampleDecisionAdmitted = requestSampleDecisionAdmitted_;
    TraceGuard traceGuard = SetTraceNewID(context.traceID, keep);
    SetRequestLogTrace(context.requestLogTrace);
    SetRequestSampleDecision(context.requestSampleDecisionValid, context.requestSampleDecisionAdmitted);
    EnsureRequestSampleDecision(*this);
    if (keep) {
        return TraceGuard(TraceGuardType::RESTORE_REQUEST_LOG_CONTEXT, oldRequestLogTrace,
                          oldRequestSampleDecisionValid, oldRequestSampleDecisionAdmitted);
    }
    return traceGuard;
}

TraceContext Trace::GetContext() const
{
    TraceContext context;
    context.traceID = traceID_;
    context.requestLogTrace = requestLogTrace_;
    context.requestSampleDecisionValid = requestSampleDecisionValid_;
    context.requestSampleDecisionAdmitted = requestSampleDecisionAdmitted_;
    return context;
}

std::string Trace::GetTraceID()
{
    return traceID_;
}

void Trace::Invalidate()
{
    traceID_[0] = '\0';
    cachedHash_ = 0;
    SetRequestLogTrace(false);
    SetRequestSampleDecision(false, false);
}

TraceGuard Trace::SetSubTraceID(const std::string &subTraceID)
{
    subPosition_ = strlen(traceID_);
    auto copySize = subTraceID.size();
    if (subTraceID.size() > static_cast<size_t>(TRACEID_MAX_SIZE) - subPosition_) {
        LOG(WARNING) << FormatString("The traceID length %zu exceeds the maximum length %d.",
            subTraceID.size() + subPosition_,
            TRACEID_MAX_SIZE);
        copySize = TRACEID_MAX_SIZE - subPosition_;
    }
    int ret = strncpy_s(traceID_ + subPosition_, TRACEID_MAX_SIZE - subPosition_ + 1, subTraceID.c_str(), copySize);
    if (ret != EOK) {
        LOG(ERROR) << "Error number of strcpy_s: " << ret;
    }
    cachedHash_ = FNV1aHash(traceID_);
    return TraceGuard(TraceGuardType::CLEAR_SUB_TRACE_ID);
}

void Trace::InvalidateSubTraceID()
{
    if (subPosition_ >= 0) {
        traceID_[subPosition_ > TRACEID_MAX_SIZE ? TRACEID_MAX_SIZE : subPosition_] = '\0';
    }
    subPosition_ = -1;
    cachedHash_ = FNV1aHash(traceID_);
}
}  // namespace datasystem

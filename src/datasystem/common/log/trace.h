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
 * Description: Trace class, which stores, obtains, and clears trace ID.TraceGuard class is used as the return value of
 * the SetTraceUUID method of the Trace class, which is responsible for clearing TraceID during destructor.
 */
#ifndef DATASYSTEM_COMMON_LOG_TRACE_H
#define DATASYSTEM_COMMON_LOG_TRACE_H

#include <cstdint>
#include <string>

namespace datasystem {
constexpr size_t SHORT_TRACEID_SIZE = 16;

class TraceGuard;

struct TraceContext {
    std::string traceID;
    bool requestLogTrace = false;
    bool requestSampleDecisionValid = false;
    bool requestSampleDecisionAdmitted = false;
};

class Trace {
public:
    Trace(const Trace &other) = delete;

    Trace(Trace &&other) = delete;

    Trace &operator=(const Trace &) = delete;

    Trace &operator=(Trace &&) = delete;

    ~Trace() = default;

    /**
     * @brief Singleton mode, obtaining instance.
     * @return Instance of Trace.
     */
    static Trace &Instance();

    /**
     * @brief Set traceID to thread_local.(The traceID is the automatically generated UUID)
     * @note This method is used to set traceID for external interfaces to simplify code compilation.
     * @return TraceGuard object. The TraceID is cleared when the object is destructed.
     */
    TraceGuard SetTraceUUID();

    /**
     * @brief Set a new trace ID and mark it as a user request log trace.
     * @return TraceGuard object. The TraceID is cleared when the object is destructed.
     */
    TraceGuard SetRequestTraceUUID();

    /**
     * @brief Set prefix for trace id.
     * @param[in] prefix The prefix for trace id.
     */
    void SetPrefix(const std::string &prefix);

    /**
     * @brief Set traceID to thread_local.
     * @note This method is used in the cross-thread scenario. A child thread obtains the TraceID of the parent thread
     * and sets the TraceID to the thread_local of the child thread.
     * @param[in] traceID The traceID.
     * @param[in] keep Indicates that the trace ID is not cleared when the scope is out.
     * @return TraceGuard object. The TraceID is cleared when the object is destructed.
     */
    TraceGuard SetTraceNewID(const std::string &traceID, bool keep = false);

    /**
     * @brief Restore a trace context captured from another thread.
     * @param[in] context Trace context snapshot.
     * @param[in] keep Indicates that the trace context is not cleared when the scope is out.
     * @return TraceGuard object. The TraceID is cleared when the object is destructed.
     */
    TraceGuard SetTraceContext(const TraceContext &context, bool keep = false);

    /**
     * @brief Capture the current trace context for cross-thread propagation.
     * @return Current trace context snapshot.
     */
    TraceContext GetContext() const;

    /**
     * @brief Obtains the trace ID stored in thread_local.
     * @return The traceID.
     */
    std::string GetTraceID();

    /**
     * @brief Get the cached FNV-1a hash of the current trace ID.
     * @return 64-bit hash value, 0 if trace ID is empty.
     */
    uint64_t GetCachedHash() const
    {
        return cachedHash_;
    }

    /**
     * @brief Get raw pointer to internal trace ID buffer (zero-copy).
     * @return Pointer to null-terminated trace ID string.
     */
    const char *GetTraceIDPtr() const
    {
        return traceID_;
    }

    /**
     * @brief Set request log sampling decision bound to current trace context.
     * @param[in] valid Whether decision is present.
     * @param[in] admitted Whether request trace is admitted when valid=true.
     */
    void SetRequestSampleDecision(bool valid, bool admitted)
    {
        requestSampleDecisionValid_ = valid;
        requestSampleDecisionAdmitted_ = admitted;
    }

    /**
     * @brief Get request log sampling decision for current trace context.
     * @param[out] admitted Admitted result when returns true.
     * @return true if decision exists, otherwise false.
     */
    bool GetRequestSampleDecision(bool &admitted) const
    {
        if (!requestSampleDecisionValid_) {
            return false;
        }
        admitted = requestSampleDecisionAdmitted_;
        return true;
    }

    /**
     * @brief Whether current trace belongs to request-log sampling scope.
     * @return true if current trace should participate in request sampling.
     */
    bool IsRequestLogTrace() const
    {
        return requestLogTrace_;
    }

    /**
     * @brief Set whether current trace belongs to request-log sampling scope.
     * @param[in] enabled true to enable request-log sampling for current trace.
     */
    void SetRequestLogTrace(bool enabled = true)
    {
        requestLogTrace_ = enabled;
    }

    /**
     * @brief Clear the trace ID stored in thread_local.
     */
    void Invalidate();

    /**
     * @brief Clear the sub trace ID stored in thread_local.
     */
    void InvalidateSubTraceID();

    /**
     * @brief Set the sub trace ID stored in thread_local.
     */
    TraceGuard SetSubTraceID(const std::string &subTraceID);

    static const int TRACEID_PREFIX_SIZE = 36;
    static const int SHORT_UUID_SIZE = 12;
    static const int TRACEID_MAX_SIZE = TRACEID_PREFIX_SIZE + SHORT_UUID_SIZE + 1;

private:
    Trace() = default;

    // The caller set prefix by Context::SetTraceId
    char prefix_[TRACEID_PREFIX_SIZE + 1] = { 0 };

    // Do not use the heap memory to avoid core dump caused by the sequence of singleton destructors.
    // declaring character array (+1 for null terminator)
    char traceID_[TRACEID_MAX_SIZE + 1] = { 0 };

    int16_t subPosition_ = -1;

    uint64_t cachedHash_ = 0;
    bool requestLogTrace_ = false;
    bool requestSampleDecisionValid_ = false;
    bool requestSampleDecisionAdmitted_ = false;
};

enum class TraceGuardType : int {
    INVALID = -1,
    CLEAR_TRACE_ID = 0,
    CLEAR_SUB_TRACE_ID,
    RESTORE_REQUEST_LOG_CONTEXT
};

class TraceGuard {
public:
    explicit TraceGuard(TraceGuardType type) : type_(type)
    {
    }

    TraceGuard(TraceGuardType type, bool requestLogTrace, bool requestSampleDecisionValid,
               bool requestSampleDecisionAdmitted)
        : type_(type),
          requestLogTrace_(requestLogTrace),
          requestSampleDecisionValid_(requestSampleDecisionValid),
          requestSampleDecisionAdmitted_(requestSampleDecisionAdmitted)
    {
    }

    TraceGuard(TraceGuard &&other) noexcept
        : type_(other.type_),
          requestLogTrace_(other.requestLogTrace_),
          requestSampleDecisionValid_(other.requestSampleDecisionValid_),
          requestSampleDecisionAdmitted_(other.requestSampleDecisionAdmitted_)
    {
        other.type_ = TraceGuardType::INVALID;
    }

    TraceGuard(const TraceGuard &other) = delete;

    TraceGuard &operator=(const TraceGuard &) = delete;

    TraceGuard &operator=(TraceGuard &&other) noexcept;

    ~TraceGuard();

private:
    void Reset();

    TraceGuardType type_;
    bool requestLogTrace_ = false;
    bool requestSampleDecisionValid_ = false;
    bool requestSampleDecisionAdmitted_ = false;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_LOG_TRACE_H

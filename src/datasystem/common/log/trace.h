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

#include <array>
#include <cstdint>
#include <string>

#include "datasystem/common/log/latency_phase_types.h"

namespace datasystem {

constexpr size_t SHORT_TRACEID_SIZE = 16;
constexpr uint16_t LATENCY_TICK_MAX_NUM = 16;
// FormatLatencySummary is bounded by LATENCY_SUMMARY_PHASE_MAX. Keep Trace heap-free so its thread-local fallback
// remains safe when process-static SDK clients are destroyed during process teardown.
constexpr size_t LATENCY_SUMMARY_MAX_SIZE = 2048;

enum class LatencyTickKey : uint16_t {
    UNKNOWN = 0,

    CLIENT_GET_START,
    CLIENT_GET_RPC_START,
    CLIENT_GET_RPC_END,
    CLIENT_GET_END,

    CLIENT_SET_START,
    CLIENT_CREATE_RPC_START,
    CLIENT_CREATE_RPC_END,
    CLIENT_MEMORY_COPY_START,
    CLIENT_MEMORY_COPY_END,
    CLIENT_UB_TRANSFER_START,
    CLIENT_UB_TRANSFER_END,
    CLIENT_PUBLISH_RPC_START,
    CLIENT_PUBLISH_RPC_END,
    CLIENT_SET_END,

    CLIENT_CREATE_START,
    CLIENT_CREATE_END,

    CLIENT_EXIST_START,
    CLIENT_EXIST_RPC_START,
    CLIENT_EXIST_RPC_END,
    CLIENT_EXIST_END,

    WORKER_GET_START,
    WORKER_QUERYMETA_START,
    WORKER_QUERYMETA_END,
    WORKER_REMOTEGET_START,
    WORKER_REMOTEGET_END,
    WORKER_URMA_START,
    WORKER_URMA_END,
    WORKER_L2CACHE_READ_START,
    WORKER_L2CACHE_READ_END,
    WORKER_GET_END,

    WORKER_CREATE_START,
    WORKER_CREATE_END,

    WORKER_PUBLISH_START,
    WORKER_CREATE_META_RPC_START,
    WORKER_CREATE_META_RPC_END,
    WORKER_UPDATE_META_RPC_START,
    WORKER_UPDATE_META_RPC_END,
    WORKER_PUBLISH_END,

    WORKER_EXIST_START,
    WORKER_EXIST_QUERYMETA_START,
    WORKER_EXIST_QUERYMETA_END,
    WORKER_EXIST_END,

    META_QUERYMETA_START,
    META_QUERYMETA_END,
    META_CREATE_META_START,
    META_CREATE_META_END,
    META_UPDATE_META_START,
    META_UPDATE_META_END,

    DATA_REMOTEGET_START,
    DATA_REMOTEGET_END,

    CLIENT_DIRECT_ROUTE_START,
    CLIENT_DIRECT_ROUTE_END,
    CLIENT_DIRECT_QUERY_AND_GET_START,
    CLIENT_DIRECT_QUERY_AND_GET_END,
    CLIENT_DIRECT_GET_DATA_START,
    CLIENT_DIRECT_GET_DATA_END,
    CLIENT_DIRECT_MATERIALIZE_START,
    CLIENT_DIRECT_MATERIALIZE_END,
};

struct LatencyTick {
    LatencyTickKey key = LatencyTickKey::UNKNOWN;
    uint64_t tick = 0;
};

class TraceGuard;

// Forward declaration for brpc M:N support.
// Defined in request_context.cpp. Returns per-bthread Trace from RequestContext
// when a handler is active, or nullptr to fall back to thread_local.
class Trace;
Trace* GetBthreadTrace();

struct TraceContext {
    std::string traceID;
    bool requestLogTrace = false;
    bool requestSampleDecisionValid = false;
    bool requestSampleDecisionAdmitted = false;
    std::array<LatencyTick, LATENCY_TICK_MAX_NUM> latencyTicks{};
    uint16_t latencyTickCount = 0;
    uint16_t latencyTickDroppedCount = 0;
    DownstreamPhaseResult downstreamPhases{};
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
     * Under brpc M:N, returns the RequestContext's per-bthread Trace when a
     * ScopedRequestContext is active on the current bthread. Otherwise
     * (background std::thread, client SDK threads, async thread pools, ZMQ
     * pthread handlers, or any context without an active ScopedRequestContext),
     * GetBthreadTrace() returns nullptr and this falls back to a per-pthread
     * thread_local Trace, which is safe because such contexts each own their
     * pthread or are cooperative-yield-free within a single LOG/Trace call.
     *
     * Implementation note: the body lives in trace.cpp (out-of-line). When the
     * body was inlined in this header, each DSO that includes trace.h got its
     * own `static thread_local Trace instance;`, so the client SDK's
     * libds_client_py.so (where the pybind Set/Get lambdas call
     * SetRequestTraceUUID) and libdatasystem.so (where the access-log and
     * RPC-send paths call GetTraceID) observed DIFFERENT thread_local
     * instances. Symptom: SDK access log / INFO log traceID column was empty
     * even though SetRequestTraceUUID minted a UUID, because the mint wrote to
     * the pybind-DSO's instance while the log prefix read the
     * libdatasystem-DSO's (empty) instance. Defining Instance() out-of-line
     * in trace.cpp forces a single thread_local instance shared across all
     * DSOs that link libdatasystem.so, restoring cross-DSO traceID visibility.
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

    void SetAccessShouldRecord(bool shouldRecord)
    {
        accessShouldRecord_ = shouldRecord;
    }

    bool GetAccessShouldRecord() const
    {
        return accessShouldRecord_;
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

    /**
     * @brief Append a latency tick with current steady_clock timestamp.
     * @param[in] key The tick key identifying the measurement point.
     * Overflow ticks beyond LATENCY_TICK_MAX_NUM are silently dropped
     * and counted via GetLatencyTickDroppedCount().
     */
    void AddLatencyTick(LatencyTickKey key);

    /**
     * @brief Reset all latency ticks (count and dropped count to zero).
     */
    void ClearLatencyTicks();

    /**
     * @brief Get number of valid latency ticks currently stored.
     * @return Count of ticks in [0, LATENCY_TICK_MAX_NUM].
     */
    uint16_t GetLatencyTickCount() const;

    /**
     * @brief Get pointer to the latency tick array.
     * Only indices [0, GetLatencyTickCount()) contain valid entries.
     * @return Pointer to internal tick array.
     */
    const LatencyTick *GetLatencyTicks() const;

    /**
     * @brief Get number of ticks dropped due to array overflow.
     * @return Dropped tick count.
     */
    uint16_t GetLatencyTickDroppedCount() const;

    void SetLatencySummary(std::string summary);
    std::string GetLatencySummary() const;
    void ClearLatencySummary();
    std::string ConsumeLatencySummary();

    void AddDownstreamPhase(LatencySummaryPhase phase, uint64_t durationUs);
    void AddDownstreamTickDroppedCount(uint16_t count);
    void AddDownstreamPhaseDroppedCount(uint16_t count);
    const DownstreamPhaseResult &GetDownstreamPhases() const;
    void ClearDownstreamPhases();

    static const int TRACEID_PREFIX_SIZE = 36;
    static const int SHORT_UUID_SIZE = 12;
    static const int TRACEID_MAX_SIZE = TRACEID_PREFIX_SIZE + SHORT_UUID_SIZE + 1;

private:
    Trace() = default;
    friend struct RequestContext;  // Allows per-bthread Trace in RequestContext for brpc M:N isolation

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
    bool accessShouldRecord_ = true;
    LatencyTick latencyTicks_[LATENCY_TICK_MAX_NUM] = {};
    uint16_t latencyTickCount_ = 0;
    uint16_t latencyTickDroppedCount_ = 0;
    std::array<char, LATENCY_SUMMARY_MAX_SIZE + 1> latencySummary_{};
    size_t latencySummarySize_ = 0;
    DownstreamPhaseResult downstreamPhases_{};
};

/**
 * @brief Add a latency tick when tracing is enabled for the request.
 * @param[in] traceEnabled Whether latency trace collection is enabled.
 * @param[in] key The tick key identifying the measurement point.
 */
inline void AddLatencyTickIfEnabled(bool traceEnabled, LatencyTickKey key)
{
    if (traceEnabled) {
        Trace::Instance().AddLatencyTick(key);
    }
}

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

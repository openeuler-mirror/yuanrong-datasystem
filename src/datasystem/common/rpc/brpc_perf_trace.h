/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: brpc RPC framework latency tracing.
 */
#ifndef DATASYSTEM_COMMON_RPC_BRPC_PERF_TRACE_H
#define DATASYSTEM_COMMON_RPC_BRPC_PERF_TRACE_H

#include <atomic>
#include <array>
#include <chrono>
#include <cstring>
#include <string>

#include <butil/iobuf.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/format.h"

namespace datasystem {

inline constexpr uint64_t BRPC_NS_PER_US = 1000ULL;
inline constexpr uint64_t BRPC_RPC_FRAMEWORK_SLOW_LOG_THRESHOLD_NS = 1ULL * 1000ULL * 1000ULL;
inline constexpr std::array<char, 8> BRPC_TRACE_TRAILER_MAGIC = { 'B', 'R', 'P', 'C', 'T', 'R', 'C', '1' };
inline constexpr uint32_t BRPC_TRACE_TRAILER_VERSION = 1U;

struct BrpcServerTraceSummary {
    uint64_t serverRecvTs { 0 };
    uint64_t serverExecStartTs { 0 };
    uint64_t serverExecEndTs { 0 };
    uint64_t serverSendTs { 0 };
};

inline uint64_t BrpcTraceNowNs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

inline uint64_t BrpcNsToUs(uint64_t ns)
{
    return ns / BRPC_NS_PER_US;
}

inline void RecordBrpcLatencyMetric(metrics::KvMetricId id, uint64_t deltaNs)
{
    metrics::GetHistogram(static_cast<uint16_t>(id)).Observe(BrpcNsToUs(deltaNs));
}

class BrpcPerfTrace {
public:
    BrpcPerfTrace(std::string traceId, std::string methodName)
        : traceId_(std::move(traceId)), methodName_(std::move(methodName))
    {
    }

    ~BrpcPerfTrace() = default;

    void MarkClientStart(uint64_t ts = BrpcTraceNowNs()) { clientStartTs_.store(ts, std::memory_order_relaxed); }
    void MarkClientSend(uint64_t ts = BrpcTraceNowNs()) { clientSendTs_.store(ts, std::memory_order_relaxed); }
    void MarkClientRecv(uint64_t ts = BrpcTraceNowNs()) { clientRecvTs_.store(ts, std::memory_order_relaxed); }
    void MarkClientEnd(uint64_t ts = BrpcTraceNowNs()) { clientEndTs_.store(ts, std::memory_order_relaxed); }
    void MarkServerRecv(uint64_t ts = BrpcTraceNowNs()) { serverRecvTs_.store(ts, std::memory_order_relaxed); }
    void MarkServerExecStart(uint64_t ts = BrpcTraceNowNs())
    {
        serverExecStartTs_.store(ts, std::memory_order_relaxed);
    }
    void MarkServerExecEnd(uint64_t ts = BrpcTraceNowNs()) { serverExecEndTs_.store(ts, std::memory_order_relaxed); }
    void MarkServerSend(uint64_t ts = BrpcTraceNowNs()) { serverSendTs_.store(ts, std::memory_order_relaxed); }
    void MergeServerTraceSummary(const BrpcServerTraceSummary &summary)
    {
        MarkServerRecv(summary.serverRecvTs);
        MarkServerExecStart(summary.serverExecStartTs);
        MarkServerExecEnd(summary.serverExecEndTs);
        MarkServerSend(summary.serverSendTs);
    }

    const std::string &TraceId() const { return traceId_; }
    const std::string &MethodName() const { return methodName_; }
    uint64_t ClientStartTs() const { return clientStartTs_.load(std::memory_order_relaxed); }
    uint64_t ClientSendTs() const { return clientSendTs_.load(std::memory_order_relaxed); }
    uint64_t ClientRecvTs() const { return clientRecvTs_.load(std::memory_order_relaxed); }
    uint64_t ClientEndTs() const { return clientEndTs_.load(std::memory_order_relaxed); }
    uint64_t ServerRecvTs() const { return serverRecvTs_.load(std::memory_order_relaxed); }
    uint64_t ServerExecStartTs() const { return serverExecStartTs_.load(std::memory_order_relaxed); }
    uint64_t ServerExecEndTs() const { return serverExecEndTs_.load(std::memory_order_relaxed); }
    uint64_t ServerSendTs() const { return serverSendTs_.load(std::memory_order_relaxed); }

private:
    std::string traceId_;
    std::string methodName_;
    std::atomic<uint64_t> clientStartTs_ { 0 };
    std::atomic<uint64_t> clientSendTs_ { 0 };
    std::atomic<uint64_t> clientRecvTs_ { 0 };
    std::atomic<uint64_t> clientEndTs_ { 0 };
    std::atomic<uint64_t> serverRecvTs_ { 0 };
    std::atomic<uint64_t> serverExecStartTs_ { 0 };
    std::atomic<uint64_t> serverExecEndTs_ { 0 };
    std::atomic<uint64_t> serverSendTs_ { 0 };
};

inline void AppendUint32ToIOBuf(butil::IOBuf &buf, uint32_t value)
{
    buf.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

inline void AppendUint64ToIOBuf(butil::IOBuf &buf, uint64_t value)
{
    buf.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

inline bool ReadUint32FromBuffer(const char *data, size_t dataSize, size_t &offset, uint32_t &value)
{
    if (offset + sizeof(value) > dataSize) {
        return false;
    }
    std::memcpy(&value, data + offset, sizeof(value));
    offset += sizeof(value);
    return true;
}

inline bool ReadUint64FromBuffer(const char *data, size_t dataSize, size_t &offset, uint64_t &value)
{
    if (offset + sizeof(value) > dataSize) {
        return false;
    }
    std::memcpy(&value, data + offset, sizeof(value));
    offset += sizeof(value);
    return true;
}

inline bool HasCompleteServerTrace(const BrpcPerfTrace &trace)
{
    return trace.ServerRecvTs() != 0 && trace.ServerExecStartTs() != 0 && trace.ServerExecEndTs() != 0
           && trace.ServerSendTs() != 0;
}

inline void AppendBrpcServerTraceTrailer(const BrpcPerfTrace &trace, butil::IOBuf &attachment)
{
    if (!HasCompleteServerTrace(trace)) {
        return;
    }
    butil::IOBuf payload;
    AppendUint32ToIOBuf(payload, BRPC_TRACE_TRAILER_VERSION);
    AppendUint64ToIOBuf(payload, trace.ServerRecvTs());
    AppendUint64ToIOBuf(payload, trace.ServerExecStartTs());
    AppendUint64ToIOBuf(payload, trace.ServerExecEndTs());
    AppendUint64ToIOBuf(payload, trace.ServerSendTs());
    const uint32_t payloadSize = static_cast<uint32_t>(payload.size());
    attachment.append(payload);
    AppendUint32ToIOBuf(attachment, payloadSize);
    attachment.append(BRPC_TRACE_TRAILER_MAGIC.data(), BRPC_TRACE_TRAILER_MAGIC.size());
}

inline bool ConsumeBrpcServerTraceTrailer(butil::IOBuf &attachment, BrpcServerTraceSummary &summary)
{
    constexpr size_t expectedPayloadSize = sizeof(uint32_t) + 4 * sizeof(uint64_t);
    const size_t footerSize = sizeof(uint32_t) + BRPC_TRACE_TRAILER_MAGIC.size();
    if (attachment.size() < footerSize) {
        return false;
    }
    std::array<char, footerSize> footer {};
    const size_t footerOffset = attachment.size() - footerSize;
    if (attachment.copy_to(footer.data(), footer.size(), footerOffset) != footer.size()) {
        return false;
    }
    if (std::memcmp(footer.data() + sizeof(uint32_t), BRPC_TRACE_TRAILER_MAGIC.data(),
                    BRPC_TRACE_TRAILER_MAGIC.size())
        != 0) {
        return false;
    }
    uint32_t payloadSize = 0;
    std::memcpy(&payloadSize, footer.data(), sizeof(payloadSize));
    if (payloadSize > footerOffset) {
        return false;
    }
    const size_t payloadOffset = footerOffset - payloadSize;
    if (payloadSize != expectedPayloadSize) {
        return false;
    }
    std::array<char, expectedPayloadSize> payload {};
    if (attachment.copy_to(payload.data(), payloadSize, payloadOffset) != payloadSize) {
        return false;
    }
    size_t offset = 0;
    uint32_t version = 0;
    BrpcServerTraceSummary parsed;
    if (!ReadUint32FromBuffer(payload.data(), payloadSize, offset, version) || version != BRPC_TRACE_TRAILER_VERSION
        || !ReadUint64FromBuffer(payload.data(), payloadSize, offset, parsed.serverRecvTs)
        || !ReadUint64FromBuffer(payload.data(), payloadSize, offset, parsed.serverExecStartTs)
        || !ReadUint64FromBuffer(payload.data(), payloadSize, offset, parsed.serverExecEndTs)
        || !ReadUint64FromBuffer(payload.data(), payloadSize, offset, parsed.serverSendTs) || offset != payloadSize) {
        return false;
    }
    attachment.pop_back(payloadSize + footerSize);
    summary = parsed;
    return true;
}

inline bool MergeBrpcServerTraceTrailer(butil::IOBuf &attachment, BrpcPerfTrace &trace)
{
    BrpcServerTraceSummary summary;
    if (!ConsumeBrpcServerTraceTrailer(attachment, summary)) {
        return false;
    }
    trace.MergeServerTraceSummary(summary);
    return true;
}

inline uint64_t PositiveDelta(uint64_t endTs, uint64_t startTs)
{
    return endTs > startTs ? endTs - startTs : 0;
}

inline uint64_t TraceDelta(uint64_t endTs, uint64_t startTs)
{
    return startTs != 0 && endTs != 0 ? PositiveDelta(endTs, startTs) : 0;
}

inline bool ShouldRecordBrpcTraceOnDestroy(const BrpcPerfTrace &trace)
{
    return trace.ClientStartTs() != 0;
}

inline void RecordBrpcRpcTrace(const BrpcPerfTrace &trace)
{
    const uint64_t e2eNs = TraceDelta(trace.ClientEndTs(), trace.ClientStartTs());
    const uint64_t clientReqFrameworkNs = TraceDelta(trace.ClientSendTs(), trace.ClientStartTs());
    const uint64_t remoteProcessingNs = TraceDelta(trace.ClientRecvTs(), trace.ClientSendTs());
    const uint64_t clientRspFrameworkNs = TraceDelta(trace.ClientEndTs(), trace.ClientRecvTs());
    const uint64_t serverReqQueueNs = TraceDelta(trace.ServerExecStartTs(), trace.ServerRecvTs());
    const uint64_t serverExecNs = TraceDelta(trace.ServerExecEndTs(), trace.ServerExecStartTs());
    const uint64_t serverRspQueueNs = TraceDelta(trace.ServerSendTs(), trace.ServerExecEndTs());
    const uint64_t serverProcessingNs = TraceDelta(trace.ServerSendTs(), trace.ServerRecvTs());
    uint64_t networkResidualNs = 0;
    if (remoteProcessingNs > 0 && serverProcessingNs > 0) {
        networkResidualNs = remoteProcessingNs > serverProcessingNs ? remoteProcessingNs - serverProcessingNs : 0;
    }

    if (clientReqFrameworkNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_CLIENT_REQ_FRAMEWORK_LATENCY, clientReqFrameworkNs);
    }
    if (remoteProcessingNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_REMOTE_PROCESSING_LATENCY, remoteProcessingNs);
    }
    if (clientRspFrameworkNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_CLIENT_RSP_FRAMEWORK_LATENCY, clientRspFrameworkNs);
    }
    if (serverReqQueueNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_SERVER_REQ_QUEUE_LATENCY, serverReqQueueNs);
    }
    if (serverExecNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_SERVER_EXEC_LATENCY, serverExecNs);
    }
    if (serverRspQueueNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_SERVER_RSP_QUEUE_LATENCY, serverRspQueueNs);
    }
    if (e2eNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_RPC_E2E_LATENCY, e2eNs);
    }
    if (networkResidualNs > 0) {
        RecordBrpcLatencyMetric(metrics::KvMetricId::BRPC_RPC_NETWORK_RESIDUAL_LATENCY, networkResidualNs);
    }
    const uint64_t frameworkNs = e2eNs > serverExecNs ? e2eNs - serverExecNs : 0;
    const int vlogLevel =
        (frameworkNs > BRPC_RPC_FRAMEWORK_SLOW_LOG_THRESHOLD_NS || FLAGS_enable_perf_trace_log) ? 0 : 1;
    VLOG(vlogLevel) << "[BRPC_RPC_FRAMEWORK_SLOW] trace_id=" << trace.TraceId()
                    << " method=" << trace.MethodName()
                    << " framework_us=" << BrpcNsToUs(frameworkNs)
                    << " e2e_us=" << BrpcNsToUs(e2eNs)
                    << " client_req_framework_us=" << BrpcNsToUs(clientReqFrameworkNs)
                    << " remote_processing_us=" << BrpcNsToUs(remoteProcessingNs)
                    << " server_req_queue_us=" << BrpcNsToUs(serverReqQueueNs)
                    << " server_exec_us=" << BrpcNsToUs(serverExecNs)
                    << " network_residual_us=" << BrpcNsToUs(networkResidualNs);
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_BRPC_PERF_TRACE_H

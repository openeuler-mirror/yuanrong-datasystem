/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Zmq static configured parameters.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_CONSTANTS_H
#define DATASYSTEM_COMMON_RPC_ZMQ_CONSTANTS_H

#include "datasystem/common/rpc/rpc_constants.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/common/metrics/kv_metrics.h"

namespace datasystem {
static constexpr int64_t ZMQ_END_SEQNO = -1;             // Mark the end of stream.
static constexpr int64_t ZMQ_INVALID_PAYLOAD_INX = -1;   // Initial value of payload index.
static constexpr int64_t ZMQ_EMBEDDED_PAYLOAD_INX = -2;  // Payload is sent inline.
static constexpr int64_t ZMQ_OFFLINE_PAYLOAD_INX = -3;   // Payload is sent offline but not parked.
static constexpr int ZMQ_CONTEXT_IO_THREADS = 1;         // ZMQ context thread.
static constexpr int ZMQ_CONTEXT_MAX_SOCKETS = 2048;     // ZMQ context max sockets.
static constexpr char WORKER_PREFIX[] = "WORKER-ID-";    // Eye-catcher for ZMQ socket
static constexpr int STUB_INTERNAL_TIMEOUT = 1200;       // For ZmqStub::GetStreamPeer
static constexpr int ZMQ_CURVE_KEY_SIZE = 32;            // Size of binary curve keys
static constexpr int ZMQ_ENCODE_KEY_SIZE_NUL_TERM = 41;  // Size of z85-encoded curve keys, with null terminator
static constexpr int ZMQ_MAX_EPOLL = 64;                 // epoll max events
static constexpr int ZMQ_SHORT_UUID = 18;                // length of uuid
static constexpr uint32_t K_LIVENESS = 120;              // Peer liveness check before a new zmq socket is created
#define ZMQ_NO_FILE_FD RPC_NO_FILE_FD
#define ZMQ_EIGHT RPC_EIGHT
#define ZMQ_SOCKET_BACKLOG RPC_SOCKET_BACKLOG

// ==================== RPC Tracing Ticks ====================
inline constexpr const char* TICK_CLIENT_ENQUEUE = "CLIENT_ENQUEUE";
inline constexpr const char* TICK_CLIENT_TO_STUB = "CLIENT_TO_STUB";
inline constexpr const char* TICK_CLIENT_RECV = "CLIENT_RECV";
inline constexpr const char* TICK_SERVER_RECV = "SERVER_RECV";
inline constexpr const char* TICK_SERVER_DEQUEUE = "SERVER_DEQUEUE";
inline constexpr const char* TICK_SERVER_EXEC_END = "SERVER_EXEC_END";
inline constexpr const char* TICK_SERVER_SEND = "SERVER_SEND";
// Synthetic duration ticks stored in MetaPb (values are durations, not epoch wall clocks).
inline constexpr const char* TICK_SERVER_EXEC_NS = "SERVER_EXEC_NS";
inline constexpr const char* TICK_SERVER_RPC_WINDOW_NS = "SERVER_RPC_WINDOW_NS";
// ==================== RPC Tracing Helpers (always enabled) ====================
inline constexpr uint64_t kNsPerUs = 1000ULL;

// Convert nanoseconds to microseconds for metrics reporting
inline uint64_t NsToUs(uint64_t ns)
{
    return ns / kNsPerUs;
}

// Record a latency histogram metric, converting ns -> us before Observe
inline void RecordLatencyMetric(metrics::KvMetricId id, uint64_t deltaNs)
{
    metrics::GetHistogram(static_cast<uint16_t>(id)).Observe(NsToUs(deltaNs));
}

inline uint64_t GetTimeSinceEpoch()
{
    return std::chrono::high_resolution_clock::now().time_since_epoch().count();
}

inline uint64_t RecordTick(MetaPb& meta, const char* tickName)
{
    auto ts = GetTimeSinceEpoch();
    TickPb tick;
    tick.set_ts(ts);
    tick.set_tick_name(tickName);
    meta.mutable_ticks()->Add(std::move(tick));
    return ts;
}

inline bool MetaHasNamedTick(const MetaPb &meta, const char *tickName)
{
    for (int i = 0; i < meta.ticks_size(); ++i) {
        if (meta.ticks(i).tick_name() == tickName) {
            return true;
        }
    }
    return false;
}

static inline void RecordServerLatencyMetrics(MetaPb &meta)
{
    uint64_t serverRecvTs = 0;
    uint64_t serverDequeuTs = 0;
    uint64_t serverExecEndTs = 0;
    uint64_t serverSendTs = 0;
    for (int i = 0; i < meta.ticks_size(); i++) {
        const auto &tick = meta.ticks(i);
        if (tick.tick_name() == TICK_SERVER_RECV) {
            serverRecvTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_DEQUEUE) {
            serverDequeuTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_EXEC_END) {
            serverExecEndTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_SEND) {
            serverSendTs = tick.ts();
        }
    }

    if (serverDequeuTs > serverRecvTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_SERVER_QUEUE_WAIT_LATENCY, serverDequeuTs - serverRecvTs);
    }

    if (serverExecEndTs > serverDequeuTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_SERVER_EXEC_LATENCY, serverExecEndTs - serverDequeuTs);
    }

    // Require EXEC_END wall tick; omitting it (e.g. early Write return before payload append) previously made
    // (serverSendTs - 0) look like astronomical "latency".
    if (serverExecEndTs > 0 && serverSendTs > serverExecEndTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_SERVER_REPLY_LATENCY, serverSendTs - serverExecEndTs);
    }

    uint64_t serverExecNs = (serverExecEndTs > serverRecvTs) ? (serverExecEndTs - serverRecvTs) : 0;
    TickPb execTick;
    execTick.set_ts(serverExecNs);
    execTick.set_tick_name(TICK_SERVER_EXEC_NS);
    meta.mutable_ticks()->Add(std::move(execTick));

    uint64_t serverWindowNs =
        (serverSendTs > serverRecvTs) ? static_cast<uint64_t>(serverSendTs - serverRecvTs) : 0;
    TickPb windowTick;
    windowTick.set_ts(serverWindowNs);
    windowTick.set_tick_name(TICK_SERVER_RPC_WINDOW_NS);
    meta.mutable_ticks()->Add(std::move(windowTick));
}

// Caller e2e uses one clock only: CLIENT_RECV - CLIENT_ENQUEUE.
// Server RPC framework time is SERVER_SEND - SERVER_RECV measured entirely on the server host and embedded as duration
// tick SERVER_RPC_WINDOW_NS (still valid if CLIENT_* wall timestamps differ from SERVER_* clocks).
// Approximate remainder on the caller timeline (milliseconds-style algebra, mixed bases):
//   zmq_rpc_network_latency = e2e - (CLIENT_TO_STUB - CLIENT_ENQUEUE) - SERVER_RPC_WINDOW_NS,
// clipped at zero. SERVER_RPC_WINDOW_NS stores server-side SEND-RECV duration.
// CLIENT_* deltas use client wall clock; NTP skew/noise dominates this residual — not literal one-way RTT.
// Prefer e2e + queuing splits for triaging.

static inline void RecordRpcLatencyMetrics(MetaPb &meta)
{
    uint64_t clientEnqueueTs = 0;
    uint64_t clientToStubTs = 0;
    uint64_t clientRecvTs = 0;
    uint64_t serverRecvTs = 0;
    uint64_t serverSendTs = 0;
    uint64_t serverRpcWindowNs = 0;

    for (int i = 0; i < meta.ticks_size(); i++) {
        const auto &tick = meta.ticks(i);
        const std::string &name = tick.tick_name();
        if (name == TICK_CLIENT_ENQUEUE) {
            clientEnqueueTs = tick.ts();
        } else if (name == TICK_CLIENT_TO_STUB) {
            clientToStubTs = tick.ts();
        } else if (name == TICK_CLIENT_RECV) {
            clientRecvTs = tick.ts();
        } else if (name == TICK_SERVER_RECV) {
            serverRecvTs = tick.ts();
        } else if (name == TICK_SERVER_SEND) {
            serverSendTs = tick.ts();
        } else if (name == TICK_SERVER_RPC_WINDOW_NS) {
            serverRpcWindowNs = tick.ts();
        }
    }

    if (serverRpcWindowNs == 0U && serverSendTs > serverRecvTs) {
        serverRpcWindowNs = serverSendTs - serverRecvTs;
    }

    uint64_t e2eNs = (clientRecvTs > clientEnqueueTs) ? (clientRecvTs - clientEnqueueTs) : 0;

    uint64_t clientFrameworkNs =
        (clientToStubTs > clientEnqueueTs) ? (clientToStubTs - clientEnqueueTs) : 0U;

    uint64_t networkResidualNs = 0U;
    if (e2eNs > 0U && serverRpcWindowNs > 0U && clientFrameworkNs < e2eNs) {
        uint64_t afterQueuingOnCallerClock = e2eNs - clientFrameworkNs;
        networkResidualNs = (afterQueuingOnCallerClock > serverRpcWindowNs)
            ? (afterQueuingOnCallerClock - serverRpcWindowNs)
            : 0U;
    }

    if (clientToStubTs > clientEnqueueTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_CLIENT_QUEUING_LATENCY, clientToStubTs - clientEnqueueTs);
    }
    if (e2eNs > 0U) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_RPC_E2E_LATENCY, e2eNs);
    }
    if (networkResidualNs > 0U) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_RPC_NETWORK_LATENCY, networkResidualNs);
    }
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_CONSTANTS_H

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
inline constexpr const char *TICK_CLIENT_START = "CLIENT_START";
inline constexpr const char *TICK_CLIENT_SEND = "CLIENT_SEND";
inline constexpr const char *TICK_CLIENT_RECV = "CLIENT_RECV";
inline constexpr const char *TICK_CLIENT_END = "CLIENT_END";

inline constexpr const char *TICK_SERVER_RECV = "SERVER_RECV";
inline constexpr const char *TICK_SERVER_EXEC_START = "SERVER_EXEC_START";
inline constexpr const char *TICK_SERVER_EXEC_END = "SERVER_EXEC_END";
inline constexpr const char *TICK_SERVER_SEND = "SERVER_SEND";
// ==================== RPC Tracing Helpers (always enabled) ====================
inline constexpr uint64_t kNsPerUs = 1000ULL;
inline constexpr uint64_t kRpcFrameworkSlowLogThresholdNs = 3ULL * 1000ULL * 1000ULL;
inline constexpr uint64_t kRpcCheckLatencyThresholdNs = 2ULL * 1000ULL * 1000ULL;

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

inline void CheckRpcLatencyAfterClientSend(const MetaPb &meta)
{
    uint64_t clientStartTs = 0;
    uint64_t clientSendTs = 0;
    for (int i = 0; i < meta.latency_ticks_size(); i++) {
        const auto &tick = meta.latency_ticks(i);
        if (tick.tick_name() == TICK_CLIENT_START) {
            clientStartTs = tick.ts();
        } else if (tick.tick_name() == TICK_CLIENT_SEND) {
            clientSendTs = tick.ts();
        }
    }
    uint64_t reqFrameworkNs = (clientSendTs > clientStartTs) ? (clientSendTs - clientStartTs) : 0;
    if (reqFrameworkNs > kRpcCheckLatencyThresholdNs) {
        LOG(INFO) << "[ZMQ_RPC_FRAMEWORK_SLOW] phase=CLIENT_SEND trace_id=" << meta.trace_id()
                  << " client_req_framework_us=" << NsToUs(reqFrameworkNs);
    }
}

inline void CheckRpcLatencyAfterServerExecStart(const MetaPb &meta)
{
    uint64_t serverRecvTs = 0;
    uint64_t serverExecStartTs = 0;
    for (int i = 0; i < meta.latency_ticks_size(); i++) {
        const auto &tick = meta.latency_ticks(i);
        if (tick.tick_name() == TICK_SERVER_RECV) {
            serverRecvTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_EXEC_START) {
            serverExecStartTs = tick.ts();
        }
    }
    uint64_t reqQueueNs = (serverExecStartTs > serverRecvTs) ? (serverExecStartTs - serverRecvTs) : 0;
    if (reqQueueNs > kRpcCheckLatencyThresholdNs) {
        LOG(INFO) << "[ZMQ_RPC_FRAMEWORK_SLOW] phase=SERVER_EXEC_START trace_id=" << meta.trace_id()
                  << " server_req_queue_us=" << NsToUs(reqQueueNs);
    }
}

inline void CheckRpcLatencyAfterServerSend(const MetaPb &meta)
{
    uint64_t serverExecEndTs = 0;
    uint64_t serverSendTs = 0;
    for (int i = 0; i < meta.latency_ticks_size(); i++) {
        const auto &tick = meta.latency_ticks(i);
        if (tick.tick_name() == TICK_SERVER_EXEC_END) {
            serverExecEndTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_SEND) {
            serverSendTs = tick.ts();
        }
    }
    uint64_t rspQueueNs = (serverSendTs > serverExecEndTs) ? (serverSendTs - serverExecEndTs) : 0;
    if (rspQueueNs > kRpcCheckLatencyThresholdNs) {
        LOG(INFO) << "[ZMQ_RPC_FRAMEWORK_SLOW] phase=SERVER_SEND trace_id=" << meta.trace_id()
                  << " server_rsp_queue_us=" << NsToUs(rspQueueNs);
    }
}

inline void CheckRpcLatencyAfterClientRecv(const MetaPb &meta)
{
    uint64_t serverRecvTs = 0;
    uint64_t serverExecStartTs = 0;
    uint64_t serverExecEndTs = 0;
    uint64_t serverSendTs = 0;
    for (int i = 0; i < meta.latency_ticks_size(); i++) {
        const auto &tick = meta.latency_ticks(i);
        if (tick.tick_name() == TICK_SERVER_RECV) {
            serverRecvTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_EXEC_START) {
            serverExecStartTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_EXEC_END) {
            serverExecEndTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_SEND) {
            serverSendTs = tick.ts();
        }
    }
    uint64_t serverReqQueueNs = (serverExecStartTs > serverRecvTs) ? (serverExecStartTs - serverRecvTs) : 0;
    uint64_t serverRspQueueNs = (serverSendTs > serverExecEndTs) ? (serverSendTs - serverExecEndTs) : 0;
    if (serverReqQueueNs > kRpcCheckLatencyThresholdNs || serverRspQueueNs > kRpcCheckLatencyThresholdNs) {
        LOG(INFO) << "[ZMQ_RPC_FRAMEWORK_SLOW] phase=CLIENT_RECV trace_id=" << meta.trace_id()
                  << " server_req_queue_us=" << NsToUs(serverReqQueueNs)
                  << " server_rsp_queue_us=" << NsToUs(serverRspQueueNs);
    }
}

inline uint64_t RecordTick(MetaPb &meta, const char *tickName)
{
    auto ts = GetTimeSinceEpoch();
    TickPb tick;
    tick.set_ts(ts);
    tick.set_tick_name(tickName);
    meta.mutable_latency_ticks()->Add(std::move(tick));
    // Pointer comparison is safe: all tick name constants are inline constexpr with unique addresses under C++17 ODR.
    // The only risk is a future caller passing a raw string literal instead of the named constant, which would
    // silently skip the check.
    if (tickName == TICK_CLIENT_SEND) {
        CheckRpcLatencyAfterClientSend(meta);
    } else if (tickName == TICK_SERVER_EXEC_START) {
        CheckRpcLatencyAfterServerExecStart(meta);
    } else if (tickName == TICK_SERVER_SEND) {
        CheckRpcLatencyAfterServerSend(meta);
    } else if (tickName == TICK_CLIENT_RECV) {
        CheckRpcLatencyAfterClientRecv(meta);
    }
    return ts;
}

inline bool MetaHasNamedLatencyTick(const MetaPb &meta, const char *tickName)
{
    for (int i = 0; i < meta.latency_ticks_size(); ++i) {
        if (meta.latency_ticks(i).tick_name() == tickName) {
            return true;
        }
    }
    return false;
}

static inline void RecordServerLatencyMetrics(MetaPb &meta)
{
    uint64_t serverRecvTs = 0;
    uint64_t serverExecStartTs = 0;
    uint64_t serverExecEndTs = 0;
    uint64_t serverSendTs = 0;
    for (int i = 0; i < meta.latency_ticks_size(); i++) {
        const auto &tick = meta.latency_ticks(i);
        if (tick.tick_name() == TICK_SERVER_RECV) {
            serverRecvTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_EXEC_START) {
            serverExecStartTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_EXEC_END) {
            serverExecEndTs = tick.ts();
        } else if (tick.tick_name() == TICK_SERVER_SEND) {
            serverSendTs = tick.ts();
        }
    }

    if (serverRecvTs > 0 && serverExecStartTs > serverRecvTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_SERVER_REQ_QUEUING_LATENCY, serverExecStartTs - serverRecvTs);
    }

    if (serverExecStartTs > 0 && serverExecEndTs > serverExecStartTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_SERVER_EXEC_LATENCY, serverExecEndTs - serverExecStartTs);
    }

    if (serverExecEndTs > 0 && serverSendTs > serverExecEndTs) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_SERVER_RSP_QUEUING_LATENCY, serverSendTs - serverExecEndTs);
    }
}

// RPC latency metrics use MetaPb.latency_ticks, not MetaPb.ticks. The latter is reserved for GetLapTime.
// CLIENT_* deltas use the client host clock and SERVER_* deltas use the server host clock; only durations are compared.
// The residual network estimate is approximate, not a literal one-way RTT.

static inline void RecordRpcLatencyMetrics(MetaPb &meta)
{
    uint64_t clientStartTs = 0;
    uint64_t clientSendTs = 0;
    uint64_t clientRecvTs = 0;
    uint64_t clientEndTs = 0;
    uint64_t serverRecvTs = 0;
    uint64_t serverExecStartTs = 0;
    uint64_t serverExecEndTs = 0;
    uint64_t serverSendTs = 0;

    for (int i = 0; i < meta.latency_ticks_size(); i++) {
        const auto &tick = meta.latency_ticks(i);
        const std::string &name = tick.tick_name();
        if (name == TICK_CLIENT_START) {
            clientStartTs = tick.ts();
        } else if (name == TICK_CLIENT_SEND) {
            clientSendTs = tick.ts();
        } else if (name == TICK_CLIENT_RECV) {
            clientRecvTs = tick.ts();
        } else if (name == TICK_CLIENT_END) {
            clientEndTs = tick.ts();
        } else if (name == TICK_SERVER_RECV) {
            serverRecvTs = tick.ts();
        } else if (name == TICK_SERVER_EXEC_START) {
            serverExecStartTs = tick.ts();
        } else if (name == TICK_SERVER_EXEC_END) {
            serverExecEndTs = tick.ts();
        } else if (name == TICK_SERVER_SEND) {
            serverSendTs = tick.ts();
        }
    }

    uint64_t e2eNs = (clientEndTs > clientStartTs) ? (clientEndTs - clientStartTs) : 0;

    uint64_t clientReqFrameworkNs = (clientSendTs > clientStartTs) ? (clientSendTs - clientStartTs) : 0U;
    uint64_t clientRspFrameworkNs = (clientEndTs > clientRecvTs) ? (clientEndTs - clientRecvTs) : 0U;

    // Client-observed remote processing time includes network transfer plus server-side work.
    uint64_t remoteProcessingNs = (clientRecvTs > clientSendTs) ? (clientRecvTs - clientSendTs) : 0U;
    // Server-side processing time includes server framework overhead and application execution.
    uint64_t serverProcessingNs = (serverSendTs > serverRecvTs) ? (serverSendTs - serverRecvTs) : 0U;
    uint64_t serverReqQueueNs = (serverExecStartTs > serverRecvTs) ? (serverExecStartTs - serverRecvTs) : 0U;
    uint64_t serverExecNs = (serverExecEndTs > serverExecStartTs) ? (serverExecEndTs - serverExecStartTs) : 0U;
    uint64_t serverRspQueueNs = (serverSendTs > serverExecEndTs) ? (serverSendTs - serverExecEndTs) : 0U;

    uint64_t networkResidualNs = 0U;
    if (remoteProcessingNs > 0 && serverProcessingNs > 0) {
        networkResidualNs = (remoteProcessingNs > serverProcessingNs) ? (remoteProcessingNs - serverProcessingNs) : 0U;
    }

    if (clientReqFrameworkNs > 0U) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_CLIENT_REQ_QUEUING_LATENCY, clientReqFrameworkNs);
    }
    if (clientRspFrameworkNs > 0U) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_CLIENT_RSP_QUEUING_LATENCY, clientRspFrameworkNs);
    }
    if (e2eNs > 0U) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_RPC_E2E_LATENCY, e2eNs);
    }
    if (networkResidualNs > 0U) {
        RecordLatencyMetric(metrics::KvMetricId::ZMQ_RPC_NETWORK_LATENCY, networkResidualNs);
    }
    uint64_t frameworkNs = (e2eNs > serverExecNs) ? (e2eNs - serverExecNs) : 0U;
    const int vlogLevel = frameworkNs > kRpcFrameworkSlowLogThresholdNs ? 0 : 1;
    VLOG(vlogLevel) << "[ZMQ_RPC_FRAMEWORK_SLOW] trace_id=" << meta.trace_id()
                    << " framework_us=" << NsToUs(frameworkNs) << " e2e_us=" << NsToUs(e2eNs)
                    << " client_req_framework_us=" << NsToUs(clientReqFrameworkNs)
                    << " remote_processing_us=" << NsToUs(remoteProcessingNs)
                    << " client_rsp_framework_us=" << NsToUs(clientRspFrameworkNs)
                    << " server_req_queue_us=" << NsToUs(serverReqQueueNs) << " server_exec_us=" << NsToUs(serverExecNs)
                    << " server_rsp_queue_us=" << NsToUs(serverRspQueueNs)
                    << " network_residual_us=" << NsToUs(networkResidualNs);
}
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_CONSTANTS_H

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

#include <chrono>
#include "datasystem/protos/meta_zmq.pb.h"

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
inline constexpr const char* TICK_CLIENT_SEND = "CLIENT_SEND";
inline constexpr const char* TICK_CLIENT_RECV = "CLIENT_RECV";
inline constexpr const char* TICK_SERVER_RECV = "SERVER_RECV";
inline constexpr const char* TICK_SERVER_DEQUEUE = "SERVER_DEQUEUE";
inline constexpr const char* TICK_SERVER_EXEC_END = "SERVER_EXEC_END";
inline constexpr const char* TICK_SERVER_SEND = "SERVER_SEND";

// ==================== RPC Tracing Helpers (always enabled) ====================
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

inline uint64_t GetTotalTicksTime(const MetaPb& meta)
{
    auto n = meta.ticks_size();
    if (n > 1) {
        return meta.ticks(n - 1).ts() - meta.ticks(0).ts();
    }
    return 0;
}

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_CONSTANTS_H

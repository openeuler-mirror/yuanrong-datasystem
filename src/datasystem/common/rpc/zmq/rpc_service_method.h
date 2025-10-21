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
 * Description: Service Method class.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_SVC_METHOD_H
#define DATASYSTEM_COMMON_RPC_ZMQ_SVC_METHOD_H

namespace datasystem {
// Internal methods have negative method indexes.
static constexpr int ZMQ_SOCKPATH_METHOD = -1;           // Sock path method.
static constexpr int ZMQ_HEARTBEAT_METHOD = -2;          // Heartbeat method.
static constexpr int ZMQ_STREAM_WORKER_METHOD = -3;      // Get a dedicated worker for streaming.
static constexpr int ZMQ_PAYLOAD_TICK_METHOD = -4;       // Performance measurement.
static constexpr int ZMQ_TCP_DIRECT_METHOD = -5;         // Get the direct tcp/ip hostport
static constexpr int ZMQ_PAYLOAD_GET_METHOD = -6;        // Direct payload get
static constexpr int ZMQ_PAYLOAD_HANDSHAKE_METHOD = -7;  // Payload handshake method.
static constexpr int ZMQ_PAYLOAD_PUT_METHOD = -8;        // Parallel payload put method.
#ifdef USE_URMA
static constexpr int ZMQ_EXCHANGE_JFR_METHOD = -9;       // Exchange jfr (urma only).
#endif

/**
 * @brief An abstract class to describe a method.
 */
class RpcServiceMethod {
public:
    RpcServiceMethod() = default;
    virtual ~RpcServiceMethod() = default;
    virtual bool HasPayloadSendOption() const
    {
        return false;
    }
    virtual bool HasPayloadRecvOption() const
    {
        return false;
    }
    virtual bool ClientStreaming() const
    {
        return false;
    }
    virtual bool ServerStreaming() const
    {
        return false;
    }
    virtual bool HasUnarySocketOption() const
    {
        return false;
    }
    virtual std::string MethodName() const = 0;
    virtual int32_t MethodIndex() const = 0;

    virtual void Init()
    {
    }

private:
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_SVC_METHOD_H

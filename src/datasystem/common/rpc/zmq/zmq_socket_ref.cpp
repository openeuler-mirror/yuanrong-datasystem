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

#include "datasystem/common/rpc/zmq/zmq_socket_ref.h"

#include <limits>
#include <unordered_map>

#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
const std::unordered_map<uint64_t, std::string> ZMQ_OPTION_MAPPINGS{
    { ZMQ_IMMEDIATE, "ZMQ_IMMEDIATE" },
    { ZMQ_LINGER, "ZMQ_LINGER" },
    { ZMQ_SNDTIMEO, "ZMQ_SNDTIMEO" },
    { ZMQ_RCVTIMEO, "ZMQ_RCVTIMEO" },
    { ZMQ_SNDHWM, "ZMQ_SNDHWM" },
    { ZMQ_RCVHWM, "ZMQ_RCVHWM" },
    { ZMQ_CURVE_SERVER, "ZMQ_CURVE_SERVER" },
    { ZMQ_BACKLOG, "ZMQ_BACKLOG" },
    { ZMQ_FD, "ZMQ_FD" },
    { ZMQ_EVENTS, "ZMQ_EVENTS" },
    { ZMQ_PROBE_ROUTER, "ZMQ_PROBE_ROUTER" },
    { ZMQ_ROUTING_ID, "ZMQ_ROUTING_ID" },
    { ZMQ_LAST_ENDPOINT, "ZMQ_LAST_ENDPOINT" },
    { ZMQ_CURVE_PUBLICKEY, "ZMQ_CURVE_PUBLICKEY" },
    { ZMQ_CURVE_SECRETKEY, "ZMQ_CURVE_SECRETKEY" },
    { ZMQ_CURVE_SERVERKEY, "ZMQ_CURVE_SERVERKEY" }
};

Status ZmqSocketRef::ZmqErrnoToStatus(int rc, const std::string &msg, StatusCode defaultRc)
{
    if (rc == EINTR) {
        RETURN_STATUS(K_INTERRUPTED, FormatString("%s. Operation was interrupted", msg));
    }
    if (rc == EAGAIN) {
        RETURN_STATUS(
            K_TRY_AGAIN,
            FormatString("%s. Non-blocking operation mode was requested and no message is available at the moment",
                         msg));
    }
    RETURN_STATUS_LOG_ERROR(defaultRc, FormatString("%s: %s", msg, zmq_strerror(rc)));
}

static std::string GetOptionName(int option)
{
    auto iter = ZMQ_OPTION_MAPPINGS.find(option);
    if (iter != ZMQ_OPTION_MAPPINGS.end()) {
        return iter->second;
    }
    return "UNKNOWN";
}

Status ZmqSocketRef::SetOption(int option, const void *val, size_t len)
{
    int rc = zmq_setsockopt(sock_, option, val, len);
    if (rc == -1) {
        std::string errMsg = FormatString("ZMQ set option %s (%d) unsuccessful", GetOptionName(option), option);
        return ZmqErrnoToStatus(errno, errMsg);
    }
    return Status::OK();
}

Status ZmqSocketRef::GetOption(int option, void *val, size_t *len) const
{
    RETURN_RUNTIME_ERROR_IF_NULL(val);
    RETURN_RUNTIME_ERROR_IF_NULL(len);
    RETURN_RUNTIME_ERROR_IF_NULL(sock_);
    int rc = zmq_getsockopt(sock_, option, val, len);
    if (rc == -1) {
        std::string errMsg = FormatString("ZMQ get option %s (%d) unsuccessful", GetOptionName(option), option);
        return ZmqErrnoToStatus(errno, errMsg);
    }
    return Status::OK();
}

Status ZmqSocketRef::Bind(const std::string &endPoint)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null reference pointer");
    int rc;
    if (RpcChannel::IsTcpipEndPointIPv6(endPoint)) {
        int optV6 = 1;
        VLOG(RPC_LOG_LEVEL) << "ZmqSocketRef Bind is setting the ipv6 socket option for endpoint: " << endPoint;
        rc = zmq_setsockopt(sock_, ZMQ_IPV6, &optV6, sizeof(optV6));
        if (rc == -1) {
            return ZmqErrnoToStatus(errno, FormatString("ZMQ Bind failed to set IPv6 for endPoint: %s", endPoint));
        }
    }
    rc = zmq_bind(sock_, endPoint.data());
    if (rc == -1) {
        return ZmqErrnoToStatus(errno, FormatString("ZMQ bind to %s unsuccessful", endPoint));
    }
    return Status::OK();
}

Status ZmqSocketRef::Connect(const std::string &endPoint, bool isIPv6)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null reference pointer");
    int rc;
    // Check the endpoint name for IPv6 format
    if (isIPv6) {
        int optV6 = 1;
        VLOG(RPC_LOG_LEVEL) << "ZmqSocketRef Connect is setting the ipv6 socket option for endpoint: " << endPoint;
        rc = zmq_setsockopt(sock_, ZMQ_IPV6, &optV6, sizeof(optV6));
        if (rc == -1) {
            return ZmqErrnoToStatus(errno, FormatString("ZMQ connect failed to set IPv6 for endPoint: %s", endPoint));
        }
    }
    
    rc = zmq_connect(sock_, endPoint.data());
    if (rc == -1) {
        return ZmqErrnoToStatus(errno, FormatString("ZMQ connect to %s unsuccessful", endPoint));
    }
    return Status::OK();
}

void ZmqSocketRef::Close()
{
    if (sock_ != nullptr) {
        RETRY_ON_EINTR(zmq_close(sock_));
        sock_ = nullptr;
    }
}

Status ZmqSocketRef::RecvMsg(ZmqMessage &msg, ZmqRecvFlags flags)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null reference pointer");
    int rc = zmq_msg_recv(msg.GetHandle(), sock_, static_cast<int>(flags));
    if (rc == -1) {
        return ZmqErrnoToStatus(errno, "ZMQ recv msg unsuccessful", K_RPC_UNAVAILABLE);
    }
    static const auto maxInt = std::numeric_limits<int>::max();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == maxInt || static_cast<size_t>(rc) == msg.Size(), K_RUNTIME_ERROR,
                                         FormatString("Expect both values are equal. msg(%d), rc(%d)", msg.Size(), rc));
    return Status::OK();
}

Status ZmqSocketRef::SendMsg(ZmqMessage &msg, ZmqSendFlags flags)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null reference pointer");
    // Get the message size before it is sent. Once sent, the message will be nullified.
    const auto msgSize = msg.Size();
    int rc = zmq_msg_send(msg.GetHandle(), sock_, static_cast<int>(flags));
    if (rc == -1) {
        return ZmqErrnoToStatus(errno, "ZMQ send msg unsuccessful", K_RPC_CANCELLED);
    }
    static const auto maxInt = std::numeric_limits<int>::max();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == maxInt || static_cast<size_t>(rc) == msgSize, K_RUNTIME_ERROR,
                                         FormatString("Expect to send out %d bytes but only got %d", msgSize, rc));
    return Status::OK();
}
}  // namespace datasystem

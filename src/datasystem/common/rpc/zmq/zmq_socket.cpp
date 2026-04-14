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
 * Description: Zmq Socket.
 */
#include "datasystem/common/rpc/zmq/zmq_socket.h"

#include <chrono>

#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_stub.h"

namespace datasystem {
ZmqSocket::ZmqSocket(const std::shared_ptr<ZmqContext> &ctx, ZmqSocketType type)
    : ctx_(ctx), sock_(ctx->CreateZmqSocket(type))
{
}

ZmqSocket::~ZmqSocket()
{
    Close();
}

Status ZmqSocket::SetClientCredential(const RpcCredential &cred)
{
    if (cred.GetAuthMechanism() == RPC_AUTH_TYPE::CURVE) {
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqCurvePublicKey, cred.cred_.curve_.curvePublicKey_));
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqCurveSecretKey, cred.cred_.curve_.curveSecretKey_));
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqCurveServerKey, cred.cred_.curve_.curveServerKey_));
    }
    return Status::OK();
}

Status ZmqSocket::SetServerCredential(const RpcCredential &cred)
{
    if (cred.GetAuthMechanism() == RPC_AUTH_TYPE::CURVE) {
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqCurveServer, true));
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqCurveSecretKey, cred.cred_.curve_.curveSecretKey_));
    }
    return Status::OK();
}

Status ZmqSocket::UpdateOptions(const RpcOptions &opt)
{
    // Only refresh those we are allowed to change.
    RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqSndtimeo, opt.timeout_));
    RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqRcvtimeo, opt.timeout_));
    if (opt.hwm_ != RPC_HWM) {
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqSndhwm, opt.hwm_));
        RETURN_IF_NOT_OK(sock_.Set(sockopt::ZmqRcvhwm, opt.hwm_));
    }
    return Status::OK();
}

Status ZmqSocket::ZmqRecvMsg(ZmqMessage &msg, ZmqRecvFlags flags)
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_RECV_MSG);
    bool blocking = (flags == ZmqRecvFlags::NONE);
    auto startTick = std::chrono::steady_clock::now();
    Status status = sock_.RecvMsg(msg, flags);
    auto endTick = std::chrono::steady_clock::now();
    // Adjust the return code for blocking mode
    if (status.GetCode() == K_TRY_AGAIN && blocking) {
        int64_t ms = std::chrono::duration_cast<std::chrono::seconds>(endTick - startTick).count();
        RETURN_STATUS(K_RPC_UNAVAILABLE,
                      FormatString("Waited for %d seconds. Didn't receive any response from server", ms));
    }
    return status;
}

Status ZmqSocket::ZmqSendMsg(ZmqMessage &msg, ZmqSendFlags flags)
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_SEND_MSG);
    return sock_.SendMsg(msg, flags);
}

Status ZmqSocket::Bind(const std::string &endPoint)
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_BIND);
    return sock_.Bind(endPoint);
}

void ZmqSocket::Close()
{
    if (!sock_.IsValid()) {
        return;
    }
    auto ctx = ctx_.lock();
    // If the context is gone (or closed), the socket should be considered invalid.
    if (ctx == nullptr || ctx->GetHandle() == nullptr) {
        sock_ = ZmqSocketRef();
        return;
    }
    bool sockFound = ctx->CloseSocket(sock_.GetHandle());
    if (!sockFound) {
        LOG(WARNING) << "Zmq close socket: socket not found";
    }
    // Once closed. It is no longer valid.
    sock_ = ZmqSocketRef();
}

Status ZmqSocket::Connect(const RpcChannel &channel)
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_CONNECT);
    return sock_.Connect(channel.GetZmqEndPoint(), channel.IsIPv6());
}

Status ZmqSocket::GetAllFrames(ZmqMsgFrames &queue, ZmqRecvFlags flags)
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_GET_ALL_MSG);
    bool more = false;
    do {
        ZmqMessage msg;
        RETURN_IF_NOT_OK(ZmqRecvMsg(msg, flags));
        point.Record();
        more = msg.More();
        queue.push_back(std::move(msg));
    } while (more);
    return Status::OK();
}

Status ZmqSocket::SendAllFrames(ZmqMsgFrames &frames, ZmqSendFlags flags)
{
    bool more = !frames.empty();
    while (more) {
        ZmqMessage msg = std::move(frames.front());
        frames.pop_front();
        more = !frames.empty();
        auto flg =
            static_cast<ZmqSendFlags>(static_cast<unsigned int>(flags) | static_cast<int>(ZmqSendFlags::SNDMORE));
        RETURN_IF_NOT_OK(ZmqSendMsg(msg, more ? flg : flags));
    }
    return Status::OK();
}
}  // namespace datasystem

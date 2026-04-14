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
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_SOCKET_H
#define DATASYSTEM_COMMON_RPC_ZMQ_SOCKET_H

#include <utility>

#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rpc/zmq/zmq_common.h"
#include "datasystem/common/rpc/zmq/zmq_constants.h"
#include "datasystem/common/rpc/zmq/zmq_context.h"
#include "datasystem/common/rpc/zmq/zmq_socket_ref.h"
#include "datasystem/common/rpc/rpc_options.h"

namespace datasystem {
/**
 * @brief A wrapper for ZMQ socket.
 * @note All APIs will not throw but return Status object.
 */
class ZmqSocket {
public:
    /**
     * @brief Constructor of ZmqSocket.
     * @note The socket created will take default values from RpcOptions.
     * @param[in] ctx ZMQ context.
     * @param[in] type ZMQ socket type (router, dealer, etc).
     * @param[in] opt ZMQ options.
     */
    ZmqSocket(const std::shared_ptr<ZmqContext>& ctx, ZmqSocketType type);

    /**
     * @brief Destructor of ZmqSocket.
     */
    virtual ~ZmqSocket();

    ZmqSocket(const ZmqSocket &other) = delete;
    ZmqSocket &operator=(const ZmqSocket &other) = delete;

    ZmqSocket(ZmqSocket &&other) noexcept : ctx_(std::move(other.ctx_)), sock_(std::move(other.sock_))
    {
    }

    ZmqSocket &operator=(ZmqSocket &&other) noexcept
    {
        if (this != &other) {
            // Release the currently owned socket before taking over a new one.
            Close();
            ctx_ = std::move(other.ctx_);
            sock_ = std::move(other.sock_);
        }
        return *this;
    }

    /**
     * @brief Used mainly in calling c version of zmq api.
     * @return socket reference inside pointer.
     */
    explicit operator void *() noexcept
    {
        return sock_.GetHandle();
    }
    explicit operator void const *() const noexcept
    {
        return sock_.GetHandle();
    }

    /**
     * @brief Bind to end point.
     * @param[in] endPoint Endpoint to bind.
     * @return Status of call.
     */
    Status Bind(const std::string &endPoint);

    /**
     * @brief Close the socket.
     */
    void Close();

    /**
     * @brief Get attributes of a socket.
     * @param[in] args Variadic template Arguments.
     * @return Attributes of socket.
     */
    template <typename A, typename... Ts>
    auto Get(A attr, Ts... args) const
    {
        return sock_.Get(attr, args...);
    }

    /**
     * @brief Set attributes of a socket.
     * @param[in] attr Attribute tag.
     * @param[in] val Attribute value.
     */
    template <typename A, typename V>
    Status Set(A attr, V val)
    {
        return sock_.Set(attr, val);
    }

    /**
     * @brief Connect to a channel.
     * @param[in] channel RpcChannel object.
     * @return Status of call.
     */
    Status Connect(const RpcChannel &channel);

    bool IsValid() const
    {
        return sock_.IsValid();
    }

    /**
     * @brief A wrapper to read from a ZMQ socket.
     * @param[out] msg ZMQ message.
     * @param[in] flags ZMQ recv flag.
     * @return Status of call.
     */
    Status ZmqRecvMsg(ZmqMessage &msg, ZmqRecvFlags flags = ZmqRecvFlags::NONE);
    /**
     * @brief A wrapper to receive into a protobuf.
     * @tparam T Pb Type.
     * @param[out] pb Protobuf.
     * @param[in] flags ZMQ receive flags.
     * @return Status of call.
     */
    template <typename T>
    Status ZmqRecvProtobuf(T &pb, ZmqRecvFlags flags = ZmqRecvFlags::NONE)
    {
        PerfPoint point(PerfKey::ZMQ_SOCKET_RECV_PB);
        ZmqMessage msg;
        PerfPoint point2(PerfKey::ZMQ_SOCKET_RECV_PB_MSG);
        RETURN_IF_NOT_OK(ZmqRecvMsg(msg, flags));
        point2.Record();
        return ParseFromZmqMessage(msg, pb);
    }

    /**
     * @brief A wrapper to call ZMQ send.
     * @param[in] msg ZMQ message.
     * @param[in] flags ZMQ send flag.
     * @return Status object.
     */
    Status ZmqSendMsg(ZmqMessage &msg, ZmqSendFlags flags = ZmqSendFlags::NONE);

    /**
     * @brief A wrapper to send protobuf.
     * @param[in] pb protobuf.
     * @param[in] flags ZMQ send flags.
     * @return Status object.
     */
    template <typename T>
    Status ZmqSendProtobuf(const T &pb, ZmqSendFlags flags = ZmqSendFlags::NONE)
    {
        PerfPoint point(PerfKey::ZMQ_SOCKET_SEND_PB);
        ZmqMessage msg;
        RETURN_IF_NOT_OK(SerializeToZmqMessage<T>(pb, &msg));
        return ZmqSendMsg(msg, flags);
    }

    /**
     * @brief A wrapper to send a Status object.
     * @param[in] rc Return code.
     * @param[in] flags ZMQ send flags.
     * @return Status object.
     */
    Status ZmqSendStatus(const Status &rc, ZmqSendFlags flags = ZmqSendFlags::NONE)
    {
        PerfPoint point(PerfKey::ZMQ_SOCKET_SEND_STATUS);
        ZmqMessage msg = StatusToZmqMessage(rc);
        return ZmqSendMsg(msg, flags);
    }

    /**
     * @brief A wrapper to receive a Status object.
     * @param[in] flags ZMQ send flags.
     * @return Status object.
     */
    Status ZmqRecvStatus(ZmqRecvFlags flags = ZmqRecvFlags::NONE)
    {
        PerfPoint point(PerfKey::ZMQ_SOCKET_RECV_STATUS);
        ZmqMessage errMsg;
        PerfPoint point2(PerfKey::ZMQ_SOCKET_RECV_STATUS_MSG);
        RETURN_IF_NOT_OK(ZmqRecvMsg(errMsg, flags));
        point2.Record();
        return ZmqMessageToStatus(errMsg);
    }

    /**
     * @brief A wrapper to send a string.
     * @param[in] str Send string.
     * @param[in] flags ZMQ send flags.
     * @return Status object.
     */
    Status ZmqSendString(const std::string &str, ZmqSendFlags flags = ZmqSendFlags::NONE)
    {
        PerfPoint point(PerfKey::ZMQ_SOCKET_SEND_STRING);
        ZmqMessage msg;
        RETURN_IF_NOT_OK(msg.CopyBuffer(str.data(), str.size()));
        return ZmqSendMsg(msg, flags);
    }

    /**
     * @brief Allow external callers to change some sock properties.
     * @param[in] opt Zmq options.
     */
    Status UpdateOptions(const RpcOptions &opt);

    /**
     * @brief Pop all the frames and save them in a queue.
     * @param[in] queue Zmq frames.
     * @param[in] flags ZMQ send flags.
     * @return Status object.
     */
    Status GetAllFrames(ZmqMsgFrames &queue, ZmqRecvFlags flags = ZmqRecvFlags::DONTWAIT);

    /**
     * @brief Send all the messages in the vector.
     * @param[in] frames Zmq frames.
     * @param[in] flags ZMQ send flags.
     * @return Status object.
     */
    Status SendAllFrames(ZmqMsgFrames &frames, ZmqSendFlags flags = ZmqSendFlags::NONE);

    std::string GetWorkerId() const
    {
        return sock_.Get(sockopt::ZmqRoutingId);
    }

    auto GetEventFd() const
    {
        return sock_.Get(sockopt::ZmqEventFd, ZMQ_NO_FILE_FD);
    }

    auto GetEvents() const
    {
        return sock_.Get(sockopt::ZmqEvents, 0);
    }

    /**
     * @brief Set up a client ZMQ socket curve credential
     * @param[in] cred Credential
     */
    Status SetClientCredential(const RpcCredential &cred);
    /**
     * @brief Set up a server ZMQ socket curve credential
     * @param[in] cred Credential
     */
    Status SetServerCredential(const RpcCredential &cred);

private:
    std::weak_ptr<ZmqContext> ctx_;
    ZmqSocketRef sock_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_SOCKET_H

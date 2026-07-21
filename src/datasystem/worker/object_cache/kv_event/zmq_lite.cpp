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
 * Description: Minimal reusable ZMQ helpers.
 */
#include "datasystem/worker/object_cache/kv_event/zmq_lite.h"

#include <cerrno>
#include <cstring>
#include <utility>

#include <zmq.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {

namespace {
constexpr int K_MAX_ZMQ_CLOSE_RETRY = 16;

Status ZmqCallStatus(int rc, const std::string &message, StatusCode defaultCode = K_RUNTIME_ERROR)
{
    if (rc == 0) {
        return Status::OK();
    }
    return ZmqLiteSocket::ZmqErrnoToStatus(errno, message, defaultCode);
}
}  // namespace

struct ZmqLiteMessage::Impl {
    Impl()
    {
        (void)zmq_msg_init(&msg);
    }

    zmq_msg_t msg{};
};

ZmqLiteMessage::ZmqLiteMessage() : impl_(std::make_unique<Impl>())
{
}

ZmqLiteMessage::~ZmqLiteMessage()
{
    (void)Close();
}

ZmqLiteMessage::ZmqLiteMessage(ZmqLiteMessage &&other) noexcept : impl_(std::move(other.impl_))
{
    other.impl_ = std::make_unique<Impl>();
}

ZmqLiteMessage &ZmqLiteMessage::operator=(ZmqLiteMessage &&other) noexcept
{
    if (this != &other) {
        impl_.swap(other.impl_);
    }
    return *this;
}

const void *ZmqLiteMessage::Data() const
{
    return zmq_msg_data(&impl_->msg);
}

void *ZmqLiteMessage::Data()
{
    return zmq_msg_data(&impl_->msg);
}

size_t ZmqLiteMessage::Size() const
{
    return zmq_msg_size(&impl_->msg);
}

bool ZmqLiteMessage::More() const
{
    return zmq_msg_more(&impl_->msg) > 0;
}

bool ZmqLiteMessage::Empty() const
{
    return Size() == 0;
}

void *ZmqLiteMessage::GetHandle()
{
    return &impl_->msg;
}

Status ZmqLiteMessage::Close()
{
    int rc = zmq_msg_close(&impl_->msg);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to close ZMQ message: %s", zmq_strerror(errno)));
    return Status::OK();
}

Status ZmqLiteMessage::CopyBuffer(const void *data, size_t size)
{
    RETURN_IF_NOT_OK(Close());
    int rc = zmq_msg_init_size(&impl_->msg, size);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        rc == 0, K_RUNTIME_ERROR,
        FormatString("Unable to create ZMQ message of size %zu: %s", size, zmq_strerror(errno)));
    if (size == 0) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(data != nullptr, K_INVALID, "Null ZMQ message source buffer");
    std::memcpy(zmq_msg_data(&impl_->msg), data, size);
    return Status::OK();
}

ZmqLiteSocket::ZmqLiteSocket(void *sock, std::shared_ptr<ZmqLiteContextState> contextState)
    : sock_(sock), contextState_(std::move(contextState))
{
}

ZmqLiteSocket::~ZmqLiteSocket()
{
    Close();
}

ZmqLiteSocket::ZmqLiteSocket(ZmqLiteSocket &&other) noexcept
    : sock_(other.sock_), contextState_(std::move(other.contextState_))
{
    other.sock_ = nullptr;
}

ZmqLiteSocket &ZmqLiteSocket::operator=(ZmqLiteSocket &&other) noexcept
{
    if (&other != this) {
        Close();
        sock_ = other.sock_;
        contextState_ = std::move(other.contextState_);
        other.sock_ = nullptr;
    }
    return *this;
}

bool ZmqLiteSocket::IsValid() const
{
    return sock_ != nullptr;
}

void *ZmqLiteSocket::GetHandle() const
{
    return sock_;
}

void ZmqLiteSocket::Close()
{
    if (sock_ == nullptr) {
        return;
    }
    void *sock = sock_;
    sock_ = nullptr;
    auto contextState = std::move(contextState_);
    if (contextState != nullptr) {
        std::lock_guard<std::mutex> lock(contextState->mutex);
        auto iter = contextState->sockets.find(reinterpret_cast<intptr_t>(sock));
        if (iter != contextState->sockets.end()) {
            CloseHandle(sock);
            contextState->sockets.erase(iter);
        }
    } else {
        CloseHandle(sock);
    }
}

void ZmqLiteSocket::CloseHandle(void *sock)
{
    int rc = -1;
    for (int retry = 0; retry < K_MAX_ZMQ_CLOSE_RETRY; ++retry) {
        rc = zmq_close(sock);
        if (rc == 0 || errno != EINTR) {
            break;
        }
    }
    if (rc != 0) {
        LOG(WARNING) << FormatString("Unable to close ZMQ socket: %s", zmq_strerror(errno));
    }
}

Status ZmqLiteSocket::ZmqErrnoToStatus(int err, const std::string &message, StatusCode defaultCode)
{
    if (err == EINTR) {
        RETURN_STATUS(K_INTERRUPTED, FormatString("%s. Operation was interrupted", message));
    }
    if (err == EAGAIN) {
        RETURN_STATUS(K_TRY_AGAIN, FormatString("%s. No message is available at the moment", message));
    }
    RETURN_STATUS(defaultCode, FormatString("%s: %s", message, zmq_strerror(err)));
}

Status ZmqLiteSocket::SetOption(int option, const void *value, size_t len)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null ZMQ socket");
    int rc = zmq_setsockopt(sock_, option, value, len);
    return ZmqCallStatus(rc, FormatString("ZMQ set option %d failed", option));
}

Status ZmqLiteSocket::SetLinger(int value)
{
    return SetOption(ZMQ_LINGER, &value, sizeof(value));
}

Status ZmqLiteSocket::SetRcvtimeo(int value)
{
    return SetOption(ZMQ_RCVTIMEO, &value, sizeof(value));
}

Status ZmqLiteSocket::SubscribeAll()
{
    return SetOption(ZMQ_SUBSCRIBE, "", 0);
}

Status ZmqLiteSocket::Bind(const std::string &endpoint)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null ZMQ socket");
    int rc = zmq_bind(sock_, endpoint.data());
    return ZmqCallStatus(rc, FormatString("ZMQ bind to %s failed", endpoint));
}

Status ZmqLiteSocket::Connect(const std::string &endpoint)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null ZMQ socket");
    int rc = zmq_connect(sock_, endpoint.data());
    return ZmqCallStatus(rc, FormatString("ZMQ connect to %s failed", endpoint));
}

Status ZmqLiteSocket::RecvMsg(ZmqLiteMessage &msg, ZmqLiteRecvFlags flags)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null ZMQ socket");
    int recvFlags = flags == ZmqLiteRecvFlags::DONTWAIT ? ZMQ_DONTWAIT : 0;
    int rc = zmq_msg_recv(static_cast<zmq_msg_t *>(msg.GetHandle()), sock_, recvFlags);
    if (rc == -1) {
        return ZmqCallStatus(rc, "ZMQ recv message failed", K_RPC_UNAVAILABLE);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(static_cast<size_t>(rc) == msg.Size(), K_RUNTIME_ERROR,
                                         FormatString("ZMQ recv size mismatch, rc: %d, msg size: %zu", rc, msg.Size()));
    return Status::OK();
}

Status ZmqLiteSocket::SendMsg(ZmqLiteMessage &msg, ZmqLiteSendFlags flags)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sock_ != nullptr, K_INVALID, "Null ZMQ socket");
    auto msgSize = msg.Size();
    int sendFlags = flags == ZmqLiteSendFlags::SNDMORE ? ZMQ_SNDMORE : 0;
    int rc = zmq_msg_send(static_cast<zmq_msg_t *>(msg.GetHandle()), sock_, sendFlags);
    if (rc == -1) {
        return ZmqCallStatus(rc, "ZMQ send message failed", K_RPC_CANCELLED);
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(static_cast<size_t>(rc) == msgSize, K_RUNTIME_ERROR,
                                         FormatString("ZMQ send size mismatch, rc: %d, msg size: %zu", rc, msgSize));
    return Status::OK();
}

ZmqLiteContext::ZmqLiteContext(int ioThreads, int maxSockets)
    : ioThreads_(ioThreads), maxSockets_(maxSockets), state_(std::make_shared<ZmqLiteContextState>())
{
}

ZmqLiteContext::~ZmqLiteContext()
{
    Close(false);
}

Status ZmqLiteContext::Init()
{
    std::lock_guard<std::mutex> lock(state_->mutex);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!state_->closed && ctx_ == nullptr, K_INVALID,
                                         "ZMQ context is already initialized or closed");
    ctx_ = zmq_ctx_new();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ctx_ != nullptr, K_RUNTIME_ERROR,
                                         FormatString("Unable to create ZMQ context: %s", zmq_strerror(errno)));
    int rc = zmq_ctx_set(ctx_, ZMQ_IO_THREADS, ioThreads_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to set ZMQ_IO_THREADS: %s", zmq_strerror(errno)));
    rc = zmq_ctx_set(ctx_, ZMQ_MAX_SOCKETS, maxSockets_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rc == 0, K_RUNTIME_ERROR,
                                         FormatString("Unable to set ZMQ_MAX_SOCKETS: %s", zmq_strerror(errno)));
    return Status::OK();
}

void *ZmqLiteContext::GetHandle() const
{
    std::lock_guard<std::mutex> lock(state_->mutex);
    return ctx_;
}

ZmqLiteSocket ZmqLiteContext::CreateZmqSocket(ZmqLiteSocketType type)
{
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->closed || ctx_ == nullptr) {
        return ZmqLiteSocket();
    }
    int socketType = type == ZmqLiteSocketType::PUB ? ZMQ_PUB : ZMQ_SUB;
    void *sock = zmq_socket(ctx_, socketType);
    if (sock == nullptr) {
        LOG(ERROR) << FormatString("Unable to create ZMQ socket of type %d: %s", socketType, zmq_strerror(errno));
        return ZmqLiteSocket();
    }
    state_->sockets.insert(reinterpret_cast<intptr_t>(sock));
    return ZmqLiteSocket(sock, state_);
}

void ZmqLiteContext::Close(bool logging)
{
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->closed) {
        return;
    }
    state_->closed = true;
    if (!state_->sockets.empty() && logging) {
        LOG(WARNING) << FormatString("ZMQ lite context shutdown. %zu socket(s) not closed", state_->sockets.size());
    }
    for (auto socket : state_->sockets) {
        ZmqLiteSocket::CloseHandle(reinterpret_cast<void *>(socket));
    }
    state_->sockets.clear();
    if (ctx_ != nullptr) {
        (void)zmq_ctx_term(ctx_);
        ctx_ = nullptr;
    }
}

#ifdef WITH_TESTS
size_t ZmqLiteContext::GetOpenSocketCount() const
{
    std::lock_guard<std::mutex> lock(state_->mutex);
    return state_->sockets.size();
}
#endif

}  // namespace datasystem

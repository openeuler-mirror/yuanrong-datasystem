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
 * Description: Raw socket communication.
 */
#include "datasystem/common/rpc/unix_sock_fd.h"

#include <netinet/tcp.h>
#include <poll.h>
#include <google/protobuf/descriptor.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/util/fd_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/protos/utils.pb.h"

constexpr int RECV_RETRY_COUNT = 10;

namespace datasystem {
#define RW_RETRY_ON_EINTR(nbytes, statement)                      \
    do {                                                          \
        int cnt_ = 0;                                             \
        do {                                                      \
            (nbytes) = (statement);                               \
            cnt_++;                                               \
        } while ((nbytes) == -1 && errno == EINTR && cnt_ <= 10); \
    } while (0)

Status UnixSockFd::ErrnoToStatus(int err, int fd)
{
    if (err == EAGAIN || err == EWOULDBLOCK || err == EINTR || err == EINPROGRESS) {
        RETURN_STATUS(K_TRY_AGAIN, FormatString("Socket receive error. err %s", StrErr(err)));
    }
    if (err == ECONNRESET || err == EPIPE) {
        RETURN_STATUS(StatusCode::K_RPC_UNAVAILABLE, FormatString("Connect reset. fd %d. err %s", fd, StrErr(err)));
    }
    RETURN_STATUS(K_RUNTIME_ERROR, FormatString("Socket receive error. err %s", StrErr(err)));
}

Status UnixSockFd::Poll(short event, int timeout) const
{
    pollfd item = { .fd = fd_, .events = event, .revents = 0 };
    auto n = poll(&item, 1, timeout);
    if (n == 0) {
        RETURN_STATUS(K_TRY_AGAIN, FormatString("fd %d not ready to send/receive", fd_));
    }
    if (n < 0) {
        return ErrnoToStatus(errno, fd_);
    }
    return Status::OK();
}

Status UnixSockFd::Recv(void *data, size_t size, bool blocking) const
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_RECV);
    Status rc;
    auto target = static_cast<ssize_t>(size);
    auto sizeRemain = target;
    int retryCount = 0;
    while (sizeRemain > 0) {
        ssize_t bytesReceived;
        RW_RETRY_ON_EINTR(bytesReceived, recv(fd_, data, size, 0));
        if (bytesReceived == -1) {
            rc = ErrnoToStatus(errno, fd_);
            if (rc.GetCode() != K_TRY_AGAIN) {
                VLOG(RPC_LOG_LEVEL) << "recv failed with rc: " << rc.ToString();
            }
            RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
            // If we have received some bytes, continue to read the rest.
            CHECK_FAIL_RETURN_STATUS(blocking || ((sizeRemain != target) && (retryCount++ < RECV_RETRY_COUNT)),
                                     K_TRY_AGAIN, "Nothing to read");
            // If we get here, it means either blocking is true or we have read at least one byte
            continue;
        }
        if (bytesReceived == 0) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "bytesReceived is 0");
        }
        data = static_cast<char *>(data) + bytesReceived;
        size -= bytesReceived;
        sizeRemain -= bytesReceived;
    }
    point.Record();
    return Status::OK();
}

Status UnixSockFd::Send(MemView &buf) const
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_SEND);
    Status rc;
    auto sizeRemain = static_cast<ssize_t>(buf.Size());
    while (sizeRemain > 0) {
        ssize_t bytesSend;
        RW_RETRY_ON_EINTR(bytesSend, send(fd_, buf.Data(), buf.Size(), MSG_NOSIGNAL));
        if (bytesSend == -1) {
            rc = ErrnoToStatus(errno, fd_);
            if (rc.GetCode() == K_TRY_AGAIN) {
                // Getting EWOULDBLOCK or EAGAIN on a non-blocking socket.
                // Wait and poll before we try again
                rc = Poll(POLLOUT, RPC_POLL_TIME);
            }
            if (rc.IsOk() || rc.GetCode() == K_TRY_AGAIN) {
                // If ready to send again (or we waited for 100ms), try again
                continue;
            }
            VLOG(RPC_KEY_LOG_LEVEL) << "send failed with rc: " << rc.ToString();
            return rc;
        }
        buf += bytesSend;
        sizeRemain -= bytesSend;
    }
    point.Record();
    return Status::OK();
}

Status UnixSockFd::Recv32(uint32_t &out, bool blocking) const
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_RECV_32);
    uint32_t val;
    char arr[sizeof(uint32_t)];
    RETURN_IF_NOT_OK(Recv(arr, sizeof(arr), blocking));
    google::protobuf::io::ArrayInputStream osWrapper(arr, sizeof(arr), sizeof(arr));
    google::protobuf::io::CodedInputStream input(&osWrapper);
    CHECK_FAIL_RETURN_STATUS(input.ReadLittleEndian32(&val), K_RUNTIME_ERROR, "Google read error");
    out = val;
    point.Record();
    return Status::OK();
}

Status UnixSockFd::Send32(uint32_t val)
{
    {
        google::protobuf::io::ArrayOutputStream osWrapper(workArea_, waSz_);
        google::protobuf::io::CodedOutputStream output(&osWrapper);
        output.WriteLittleEndian32(val);
    }
    MemView ptr(workArea_, sizeof(val));
    return Send(ptr);
}

Status UnixSockFd::SendStatus(const Status &rc)
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_SEND_STATUS);
    ErrorInfoPb err;
    err.set_error_code(rc.GetCode());
    err.set_error_msg(rc.GetMsg());
    return SendProtobuf<ErrorInfoPb>(err);
}

Status UnixSockFd::RecvStatus()
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_RECV_STATUS);
    ErrorInfoPb err;
    RETURN_IF_NOT_OK(RecvProtobuf<ErrorInfoPb>(err));
    Status rc(static_cast<StatusCode>(err.error_code()), err.error_msg());
    point.Record();
    return rc;
}

Status UnixSockFd::CreateUnixSocket()
{
    fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
    CHECK_FAIL_RETURN_STATUS(fd_ != RPC_NO_FILE_FD, K_RUNTIME_ERROR,
                             FormatString("Socket create failed: errno = %s", StrErr(errno)));
    VLOG(RPC_LOG_LEVEL) << FormatString("create uds fd %d", fd_);
    return Status::OK();
}

Status UnixSockFd::CreateTcpIpSocket()
{
    fd_ = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, IPPROTO_TCP);
    CHECK_FAIL_RETURN_STATUS(fd_ != RPC_NO_FILE_FD, K_RUNTIME_ERROR,
                             FormatString("Socket create failed: errno = %d", errno));
    VLOG(RPC_LOG_LEVEL) << FormatString("Create tcp socket fd %d", fd_);
    // For tcp/ip, turn on address/port reuse
    int opt = 1;
    auto err = setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set reuse error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::SetNoDelay() const
{
    int opt = 1;
    // Turn off Nagel algorithm
    auto err = setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR,
                             FormatString("Socket set tcp nodelay error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::KeepAlive() const
{
    int opt = 1;
    auto err = setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(opt));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR,
                             FormatString("Socket set tcp keep alive error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::SetNonBlocking() const
{
    auto flags = fcntl(fd_, F_GETFL, 0);
    CHECK_FAIL_RETURN_STATUS(flags != -1, K_RUNTIME_ERROR, FormatString("Socket get fcntl error: errno = %d", errno));
    auto err = fcntl(fd_, F_SETFL, flags | O_NONBLOCK);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set fcntl error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::SetBlocking() const
{
    auto flags = fcntl(fd_, F_GETFL, 0);
    CHECK_FAIL_RETURN_STATUS(flags != -1, K_RUNTIME_ERROR, FormatString("Socket get fcntl error: errno = %d", errno));
    auto err = fcntl(fd_, F_SETFL, flags & ~O_NONBLOCK);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set fcntl error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::SetTimeout(int64_t timeout) const
{
    auto s = timeout / ONE_THOUSAND;
    auto us = (timeout % ONE_THOUSAND) * ONE_THOUSAND;
    struct timeval t {
        .tv_sec = s, .tv_usec = us
    };
    auto err = setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &t, sizeof(t));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set timeout error: errno = %d", errno));
    err = setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &t, sizeof(t));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set timeout error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::SetBufSize(int sz) const
{
    auto err = setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &sz, sizeof(sz));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set buf size error: errno = %d", errno));
    err = setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &sz, sizeof(sz));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set buf size error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::SetUpSockPath(const std::string &path, struct sockaddr_un &addr)
{
    size_t maxlen = sizeof(addr.sun_path);
    if (path.length() > maxlen) {
        std::stringstream ss;
        ss << "The domain socket is : " << path << ", and its len(" << path.length()
           << ") is greater than linux max socket len(" << maxlen << ")";
        RETURN_STATUS_LOG_ERROR(StatusCode::K_RUNTIME_ERROR, ss.str());
    }
    int ret = memset_s(&addr, sizeof(sockaddr_un), '\0', sizeof(sockaddr_un));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Set sock addr failed, the memset_s return: %d", ret));
    addr.sun_family = AF_UNIX;
    ret = memcpy_s(addr.sun_path, maxlen, path.data(), path.length());
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy sock addr path failed, the memcpy_sp return: %d", ret));
    return Status::OK();
}

Status UnixSockFd::SetUpTcpIpAddr(const std::string &tcpEndPt, struct sockaddr_in &addr)
{
    addr.sin_family = AF_INET;
    // Parse tcpEndPt which is in the form of "x.y.z.w:port"
    auto pos = tcpEndPt.find_last_of(':');
    CHECK_FAIL_RETURN_STATUS(pos != std::string::npos, K_INVALID, FormatString("Invalid address %s", tcpEndPt));
    std::string address = tcpEndPt.substr(0, pos);
    std::string port = tcpEndPt.substr(pos + 1);
    // If we bind to random port, pass 0 to below.
    if (port == "*") {
        port = "0";
    }
    try {
        addr.sin_port = htons(static_cast<short>(std::stoi(port)));
    } catch (const std::exception &e) {
        return Status(StatusCode::K_RUNTIME_ERROR, e.what());
    }
    auto err = inet_pton(AF_INET, address.c_str(), &addr.sin_addr);
    CHECK_FAIL_RETURN_STATUS(err == 1, K_RUNTIME_ERROR, FormatString("Invalid ip address %s", address));
    return Status::OK();
}

Status UnixSockFd::Bind(struct sockaddr_un &addr, mode_t perm) const
{
    auto err = bind(fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(sockaddr_un));
    if (err < 0) {
        std::stringstream oss;
        oss << "Bind to " << std::string(addr.sun_path, sizeof(addr.sun_path)) << " fail: " << std::to_string(errno);
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, oss.str());
    }
    RETURN_IF_NOT_OK(ChangeFileMod(addr.sun_path, perm));
    err = listen(fd_, RPC_SOCKET_BACKLOG);
    if (err < 0) {
        std::stringstream oss;
        oss << "Listen to " << std::string(addr.sun_path, sizeof(addr.sun_path)) << " fail: " << std::to_string(errno);
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, oss.str());
    }
    VLOG(RPC_KEY_LOG_LEVEL) << "Unix socket successfully created " << addr.sun_path << ". fd = " << fd_;
    return Status::OK();
}

Status UnixSockFd::Bind(struct sockaddr_in &addr) const
{
    auto err = bind(fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(sockaddr_in));
    if (err < 0) {
        std::stringstream oss;
        oss << FormatString("Bind failed. Errno = %d", errno);
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, oss.str());
    }
    err = listen(fd_, RPC_SOCKET_BACKLOG);
    if (err < 0) {
        std::stringstream oss;
        oss << FormatString("Listen failed. Errno = %d", errno);
        RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, oss.str());
    }
    HostPort out;
    RETURN_IF_NOT_OK(GetBindingHostPort(out));
    VLOG(RPC_KEY_LOG_LEVEL) << FormatString("Tcp/Ip socket successfully created %s. fd = %d", out.ToString(), fd_);
    return Status::OK();
}

Status UnixSockFd::Bind(const std::string &ZmqEndPt, mode_t perm, std::string &bindStr)
{
    const std::string tcpTransport = "tcp://";
    const std::string udsTransport = "ipc://";
    std::string tcpHostPort = ParseEndPt(ZmqEndPt, tcpTransport);
    std::string udsPath = ParseEndPt(ZmqEndPt, udsTransport);
    if (!tcpHostPort.empty()) {
        sockaddr_in addr{};
        RETURN_IF_NOT_OK(UnixSockFd::SetUpTcpIpAddr(tcpHostPort, addr));
        if (fd_ == RPC_NO_FILE_FD) {
            RETURN_IF_NOT_OK(CreateTcpIpSocket());
        }
        RETURN_IF_NOT_OK(Bind(addr));
        // We may do a random port binding. Figure out the binding port.
        HostPort val;
        RETURN_IF_NOT_OK(GetBindingHostPort(val));
        bindStr = val.ToString();
    } else if (!udsPath.empty()) {
        sockaddr_un addr{};
        RETURN_IF_NOT_OK(UnixSockFd::SetUpSockPath(udsPath, addr));
        if (fd_ == RPC_NO_FILE_FD) {
            RETURN_IF_NOT_OK(CreateUnixSocket());
        }
        RETURN_IF_NOT_OK(Bind(addr, perm));
        bindStr = udsPath;
    } else {
        RETURN_STATUS(K_INVALID, FormatString("Invalid end point %s", ZmqEndPt));
    }
    return Status::OK();
}

Status UnixSockFd::Connect(struct sockaddr_un &addr) const
{
    auto err = connect(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(struct sockaddr_un));
    if (err < 0) {
        std::stringstream oss;
        oss << FormatString("Socket (%d) can not connect to %s with unix domain socket: errno = %d", fd_,
                            std::string(addr.sun_path), errno);
        const int interval = 100;
        VLOG_EVERY_N(RPC_KEY_LOG_LEVEL, interval) << oss.str();
        return { K_RPC_UNAVAILABLE, oss.str() };
    }
    return Status::OK();
}

Status UnixSockFd::Connect(struct sockaddr_in &addr) const
{
    auto err = connect(fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(struct sockaddr_in));
    if (err < 0) {
        std::stringstream oss;
        std::string address(INET_ADDRSTRLEN, 0);
        bool success =
            (inet_ntop(AF_INET, &addr.sin_addr, const_cast<char *>(address.c_str()), INET_ADDRSTRLEN) != nullptr);
        if (success) {
            address.resize(strlen(address.c_str()));
            oss << FormatString("Socket (%d) connect to %s:%d failed: errno = %d", fd_, address, ntohs(addr.sin_port),
                                errno);
            const int interval = 100;
            VLOG_EVERY_N(RPC_KEY_LOG_LEVEL, interval) << oss.str();
            RETURN_STATUS(K_RPC_UNAVAILABLE, oss.str());
        } else {
            oss << FormatString("Invalid network address. Errno %d", errno);
            RETURN_STATUS(K_RUNTIME_ERROR, oss.str());
        }
    }
    RETURN_IF_NOT_OK(SetNoDelay());
    RETURN_IF_NOT_OK(KeepAlive());
    return Status::OK();
}

Status UnixSockFd::Connect(const std::string &ZmqEndPt)
{
    std::string tcpHostPort;
    std::string udsPath;
    if (!(tcpHostPort = ParseEndPt(ZmqEndPt, "tcp://")).empty()) {
        sockaddr_in addr{};
        RETURN_IF_NOT_OK(UnixSockFd::SetUpTcpIpAddr(tcpHostPort, addr));
        if (fd_ == RPC_NO_FILE_FD) {
            RETURN_IF_NOT_OK(CreateTcpIpSocket());
        }
        RETURN_IF_NOT_OK(Connect(addr));
    } else if (!(udsPath = ParseEndPt(ZmqEndPt, "ipc://")).empty()) {
        sockaddr_un addr{};
        RETURN_IF_NOT_OK(UnixSockFd::SetUpSockPath(udsPath, addr));
        if (fd_ == RPC_NO_FILE_FD) {
            RETURN_IF_NOT_OK(CreateUnixSocket());
        }
        RETURN_IF_NOT_OK(Connect(addr));
    } else {
        RETURN_STATUS(K_INVALID, FormatString("Invalid end point %s", ZmqEndPt));
    }
    return Status::OK();
}

Status UnixSockFd::GetBindingHostPort(HostPort &out) const
{
    sockaddr_in addr{};
    socklen_t len = sizeof(sockaddr_in);
    auto err = getsockname(fd_, reinterpret_cast<sockaddr *>(&addr), &len);
    CHECK_FAIL_RETURN_STATUS(err == 0, K_RUNTIME_ERROR, FormatString("getsockname failed with errno %d", errno));
    std::string address(INET_ADDRSTRLEN, 0);
    bool success =
        (inet_ntop(AF_INET, &addr.sin_addr, const_cast<char *>(address.c_str()), INET_ADDRSTRLEN) != nullptr);
    CHECK_FAIL_RETURN_STATUS(success, K_RUNTIME_ERROR, FormatString("Invalid network address. Errno %d", errno));
    auto port = ntohs(addr.sin_port);
    address.resize(strlen(address.c_str()));
    out = HostPort(address, port);
    return Status::OK();
}

}  // namespace datasystem

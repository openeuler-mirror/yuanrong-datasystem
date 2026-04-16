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

#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <google/protobuf/descriptor.h>
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/util/fd_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/protos/utils.pb.h"

constexpr int RECV_RETRY_COUNT = 10;

// in case of compilation on systems without IPPROTO_SCMTCP
#ifndef IPPROTO_SCMTCP
#define IPPROTO_SCMTCP 518
#endif

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
        RETURN_STATUS(StatusCode::K_RPC_UNAVAILABLE,
                      FormatString("[TCP_CONNECT_RESET] Connect reset. fd %d. err %s", fd, StrErr(err)));
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
    if (timeoutEnabled_) {
        CHECK_FAIL_RETURN_STATUS(blocking, K_RUNTIME_ERROR,
                                 "Receive with timeout is only supported for blocking receive!");
        return RecvWithTimeout(data, size);
    } else {
        return RecvNoTimeout(data, size, blocking);
    }
}

Status UnixSockFd::RecvNoTimeout(void *data, size_t size, bool blocking) const
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
    if (timeoutEnabled_) {
        return SendWithTimeout(buf);
    } else {
        return SendNoTimeout(buf);
    }
}

Status UnixSockFd::SendNoTimeout(MemView &buf) const
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
    // Set both the send and recv timeout for this socket
    RETURN_IF_NOT_OK(SetTimeout(TimeoutType::SendTimeout, timeout));
    RETURN_IF_NOT_OK(SetTimeout(TimeoutType::RecvTimeout, timeout));
    return Status::OK();
}

Status UnixSockFd::SetTimeoutEnforced(int64_t timeout)
{
    RETURN_IF_NOT_OK(SetTimeout(timeout));
    timeoutEnabled_ = true;
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

Status UnixSockFd::GetAddrInfo(const std::string &tcpIpAddr, struct addrinfo **servInfo)
{
    std::string serverAddr;
    std::string serverPort;
    struct addrinfo hints;

    // Split/extract the address and the port
    RpcChannel::ParseTcpipEndpoint(tcpIpAddr, serverAddr, serverPort);

    int ret = memset_s(&hints, sizeof(hints), '\0', sizeof(hints));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Set sock hints failed, the memset_s return: %d", ret));

    hints.ai_family = AF_UNSPEC;  // supports both ipv4 and ipv6
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    int err = getaddrinfo(serverAddr.c_str(), serverPort.c_str(), &hints, servInfo);
    CHECK_FAIL_RETURN_STATUS(err == 0, K_RUNTIME_ERROR,
                             FormatString("getaddrinfo failed: %s:%s", serverAddr, serverPort));
    return Status::OK();
}

Status UnixSockFd::BindUds(struct sockaddr_un &addr, mode_t perm) const
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

Status UnixSockFd::BindTcp(struct addrinfo *servInfo, HostPort &outHostPort)
{
    struct addrinfo *currServInfo;
    std::string errMsg;

    CHECK_FAIL_RETURN_STATUS(servInfo != nullptr, K_RUNTIME_ERROR, "Bind called with null servInfo");

    // Each loop pass runs socket create, bind, listen and then fetches the address.
    // If any of these fails, close the socket and reloop to the next address in the servInfo.
    // If the end of the loop is reached, then it is unsuccessful and report the error.
    for (currServInfo = servInfo; currServInfo != NULL; currServInfo = currServInfo->ai_next) {
        auto protocol = isScmTcp_ ? IPPROTO_SCMTCP : currServInfo->ai_protocol;
        VLOG(1) << "Attempting to create socket for bind family: " << currServInfo->ai_family
                << ", socktype: " << currServInfo->ai_socktype << ", with protocol: " << protocol;
        fd_ = socket(currServInfo->ai_family, currServInfo->ai_socktype, protocol);
        if (fd_ == RPC_NO_FILE_FD) {
            errMsg = FormatString("Socket create failed: errno = %d", errno);
            VLOG(RPC_LOG_LEVEL) << errMsg;
            continue;
        }

        int reuse = 1;
        auto err = setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        if (err < 0) {
            // non-fatal error
            errMsg = FormatString("setsockopt(SO_REUSEADDR) failed: errno = %d", errno);
            VLOG(RPC_LOG_LEVEL) << errMsg;
        }

        err = bind(fd_, currServInfo->ai_addr, currServInfo->ai_addrlen);
        if (err < 0) {
            errMsg = FormatString("Socket bind failed: errno = %d", errno);
            VLOG(RPC_LOG_LEVEL) << errMsg;
            Close();
            continue;
        }

        err = listen(fd_, RPC_SOCKET_BACKLOG);
        if (err < 0) {
            errMsg = FormatString("Socket listen failed: errno = %d", errno);
            VLOG(RPC_LOG_LEVEL) << errMsg;
            Close();
            continue;
        }

        // All is good. Fetch the bound host port info and break out of the serv info loop
        RETURN_IF_NOT_OK(GetSocketHostPort(currServInfo, outHostPort));
        break;
    }

    freeaddrinfo(servInfo);

    // If end of the servInfo list was reached and no success, then fail with error
    CHECK_FAIL_RETURN_STATUS(currServInfo != nullptr, K_RUNTIME_ERROR,
                             FormatString("Tcpip Bind failed. Last Error: %s", errMsg));

    VLOG(RPC_KEY_LOG_LEVEL) << FormatString("BindTcp socket for %s success. fd = %d", outHostPort.ToString(), fd_);

    return Status::OK();
}

Status UnixSockFd::Bind(const std::string &zmqEndPt, mode_t perm, std::string &bindStr)
{
    const std::string tcpTransport = "tcp://";
    const std::string udsTransport = "ipc://";
    std::string tcpHostPort = ParseEndPt(zmqEndPt, tcpTransport);
    std::string udsPath = ParseEndPt(zmqEndPt, udsTransport);

    CHECK_FAIL_RETURN_STATUS(fd_ == RPC_NO_FILE_FD, K_RUNTIME_ERROR,
                             FormatString("Bind attempted against a socket that is not closed."));

    if (!tcpHostPort.empty()) {
        struct addrinfo *servInfo;
        RETURN_IF_NOT_OK(GetAddrInfo(tcpHostPort, &servInfo));
        HostPort boundHostPort;
        RETURN_IF_NOT_OK(BindTcp(servInfo, boundHostPort));
        bindStr = boundHostPort.ToString();
    } else if (!udsPath.empty()) {
        sockaddr_un addr{};
        RETURN_IF_NOT_OK(UnixSockFd::SetUpSockPath(udsPath, addr));
        if (fd_ == RPC_NO_FILE_FD) {
            RETURN_IF_NOT_OK(CreateUnixSocket());
        }
        RETURN_IF_NOT_OK(BindUds(addr, perm));
        bindStr = udsPath;
    } else {
        RETURN_STATUS(K_INVALID, FormatString("Invalid end point %s", zmqEndPt));
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
        return { K_RPC_UNAVAILABLE, std::string("[UDS_CONNECT_FAILED] ") + oss.str() };
    }
    return Status::OK();
}

Status UnixSockFd::ConnectTcp(struct addrinfo *servInfo)
{
    struct addrinfo *currServInfo;
    std::string errMsg;
    CHECK_FAIL_RETURN_STATUS(servInfo != nullptr, K_RUNTIME_ERROR, "Connect called with null servInfo");

    // Each loop pass runs socket create, then connect.
    // If any of these fails, close the socket and reloop to the next address in the servInfo.
    // If the end of the loop is reached, then it is unsuccessful and report the error.
    for (currServInfo = servInfo; currServInfo != NULL; currServInfo = currServInfo->ai_next) {
        auto protocol = isScmTcp_ ? IPPROTO_SCMTCP : currServInfo->ai_protocol;
        VLOG(1) << "Attempting to create socket for connect family: " << currServInfo->ai_family
                << ", socktype: " << currServInfo->ai_socktype << ", with protocol: " << protocol;
        fd_ = socket(currServInfo->ai_family, currServInfo->ai_socktype, protocol);
        if (fd_ == RPC_NO_FILE_FD) {
            errMsg = FormatString("Socket create failed: errno = %d", errno);
            VLOG(RPC_LOG_LEVEL) << errMsg;
            continue;
        }

        auto err = connect(fd_, currServInfo->ai_addr, currServInfo->ai_addrlen);
        if (err < 0) {
            errMsg = FormatString("Socket connect failed: errno = %d", errno);
            VLOG(RPC_LOG_LEVEL) << errMsg;
            Close();
            continue;
        }

        // All is good, break out of the serv info loop
        break;
    }

    freeaddrinfo(servInfo);

    // If end of the servInfo list was reached and no success, then fail with error
    CHECK_FAIL_RETURN_STATUS(currServInfo != nullptr, K_RPC_UNAVAILABLE,
                             FormatString("[TCP_CONNECT_FAILED] Tcpip Connect failed. Last Error: %s", errMsg));

    RETURN_IF_NOT_OK(SetNoDelay());
    RETURN_IF_NOT_OK(KeepAlive());
    VLOG(RPC_LOG_LEVEL) << FormatString("Tcpip socket %d connected successfully.", fd_);

    return Status::OK();
}

Status UnixSockFd::Connect(const std::string &ZmqEndPt)
{
    std::string tcpHostPort;
    std::string udsPath;
    if (!(tcpHostPort = ParseEndPt(ZmqEndPt, "tcp://")).empty()) {
        struct addrinfo *servInfo;
        RETURN_IF_NOT_OK(GetAddrInfo(tcpHostPort, &servInfo));
        RETURN_IF_NOT_OK(ConnectTcp(servInfo));
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

Status UnixSockFd::Accept(UnixSockFd &outSockFd)
{
    int newFd = accept(fd_, nullptr, nullptr);
    if (newFd <= 0) {
        Status rc = UnixSockFd::ErrnoToStatus(errno, fd_);
        if (rc.IsError() && rc.GetCode() != K_TRY_AGAIN) {
            VLOG(RPC_LOG_LEVEL) << FormatString("Spawn uds connection with listener fd %d failed with status %s", fd_,
                                                rc.ToString());
        }
        return rc;
    }

    outSockFd = UnixSockFd(newFd);
    return Status::OK();
}

Status UnixSockFd::GetSocketHostPort(struct addrinfo *servInfo, datasystem::HostPort &outHostPort)
{
    sockaddr *addr;
    socklen_t len;
    sockaddr_in addrV4;
    sockaddr_in6 addrV6;

    if (servInfo->ai_family == AF_INET) {
        len = sizeof(sockaddr_in);
        addr = reinterpret_cast<sockaddr *>(&addrV4);
    } else {
        len = sizeof(sockaddr_in6);
        addr = reinterpret_cast<sockaddr *>(&addrV6);
    }

    auto err = getsockname(fd_, addr, &len);
    CHECK_FAIL_RETURN_STATUS(err == 0, K_RUNTIME_ERROR, FormatString("getsockname failed with errno %d", errno));

    char s[INET6_ADDRSTRLEN];
    bool success = (inet_ntop(servInfo->ai_family, GetInAddr(addr), s, sizeof(s)) != nullptr);
    CHECK_FAIL_RETURN_STATUS(success, K_RUNTIME_ERROR, FormatString("Invalid network address. Errno %d", errno));

    auto port = ntohs(GetInPort(addr));
    outHostPort = HostPort(s, port);
    return Status::OK();
}

Status UnixSockFd::SetTimeout(TimeoutType timeoutType, int64_t timeoutMs) const
{
    auto s = timeoutMs / ONE_THOUSAND;
    auto us = (timeoutMs % ONE_THOUSAND) * ONE_THOUSAND;
    struct timeval t{ .tv_sec = s, .tv_usec = us };

    // Note: std::to_underlying() is available in more recent C++ versions. static_cast for now.
    auto err = setsockopt(fd_, SOL_SOCKET, static_cast<int>(timeoutType), &t, sizeof(t));
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket set timeout error: errno = %d", errno));
    return Status::OK();
}

Status UnixSockFd::GetTimeout(TimeoutType timeoutType, int64_t &timeoutMs) const
{
    socklen_t tLen = sizeof(struct timeval);
    struct timeval t;

    // Note: std::to_underlying() is available in more recent C++ versions. static_cast for now.
    auto err = getsockopt(fd_, SOL_SOCKET, static_cast<int>(timeoutType), &t, &tLen);
    CHECK_FAIL_RETURN_STATUS(err != -1, K_RUNTIME_ERROR, FormatString("Socket get timeout error: errno = %d", errno));

    // convert to ms for output
    timeoutMs = (t.tv_sec * ONE_THOUSAND) + (t.tv_usec / ONE_THOUSAND);
    return Status::OK();
}

Status UnixSockFd::RecvWithTimeout(void *data, size_t size) const
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_RECV);
    int64_t startingTimeoutMs = 0;
    int64_t timeRemainingMs = 0;
    auto sizeRemain = static_cast<ssize_t>(size);

    // Fetch the timeout from the fd. The timeout within the socket may continuously be adjusted as calls are made
    // (and time has been consumed).
    RETURN_IF_NOT_OK(GetTimeout(TimeoutType::RecvTimeout, startingTimeoutMs));

    // Create a timer with the given amount of time remaining
    Timer timer(startingTimeoutMs);

    while (sizeRemain > 0) {
        ssize_t bytesReceived;
        int err;
        bytesReceived = recv(fd_, data, size, 0);
        err = errno;

        timeRemainingMs = timer.GetRemainingTimeMs();
        // Regardless of success or fail of the call. If we ran out of time then return to the caller with error.
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(timeRemainingMs > 0, K_RPC_DEADLINE_EXCEEDED, "Socket recv timeout");

        if (bytesReceived == -1) {
            Status rc = ErrnoToStatus(err, fd_);
            if (rc.GetCode() != K_TRY_AGAIN) {
                VLOG(RPC_LOG_LEVEL) << "recv failed with rc: " << rc.ToString();
            }
            RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
        } else if (bytesReceived == 0) {
            RETURN_STATUS(StatusCode::K_RPC_CANCELLED, "bytesReceived is 0");
        } else {
            // Record the received data so far
            data = static_cast<char *>(data) + bytesReceived;
            size -= bytesReceived;
            sizeRemain -= bytesReceived;
        }

        // Assign the timeout for the next receive call so that it has less time allowed than before, then reloop.
        RETURN_IF_NOT_OK(SetTimeout(TimeoutType::RecvTimeout, timeRemainingMs));
    }
    point.Record();
    // The recv timeout was set already naturally after the last recv. Update the send timeout to be the same.
    RETURN_IF_NOT_OK(SetTimeout(TimeoutType::SendTimeout, timeRemainingMs));
    return Status::OK();
}

Status UnixSockFd::SendWithTimeout(MemView &buf) const
{
    PerfPoint point(PerfKey::ZMQ_SOCKET_FD_SEND);
    Status rc;
    int64_t startingTimeoutMs = 0;
    int64_t timeRemainingMs = 0;

    // Fetch the timeout from the fd. The timeout within the socket may continuously be adjusted as calls are made
    // (and time has been consumed).
    RETURN_IF_NOT_OK(GetTimeout(TimeoutType::SendTimeout, startingTimeoutMs));

    // Create a timer with the given amount of time remaining
    Timer timer(startingTimeoutMs);

    auto sizeRemain = static_cast<ssize_t>(buf.Size());
    while (sizeRemain > 0) {
        ssize_t bytesSend;
        int err;
        bytesSend = send(fd_, buf.Data(), buf.Size(), MSG_NOSIGNAL);
        err = errno;

        timeRemainingMs = timer.GetRemainingTimeMs();
        // Regardless of success or fail of the call. If we ran out of time then return to the caller with error.
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(timeRemainingMs > 0, K_RPC_DEADLINE_EXCEEDED, "Socket send timeout");

        if (bytesSend == -1) {
            rc = ErrnoToStatus(err, fd_);
            if (rc.GetCode() != K_TRY_AGAIN) {
                VLOG(RPC_LOG_LEVEL) << "send failed with rc: " << rc.ToString();
            }
            RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
        } else {
            buf += bytesSend;
            sizeRemain -= bytesSend;
        }

        // Assign the timeout for the next send call so that it has less time allowed than before, then reloop
        RETURN_IF_NOT_OK(SetTimeout(TimeoutType::SendTimeout, timeRemainingMs));
    }
    point.Record();

    // The send timeout was set already naturally after the last send. Update the recv timeout to be the same.
    RETURN_IF_NOT_OK(SetTimeout(TimeoutType::RecvTimeout, timeRemainingMs));
    return Status::OK();
}

void *UnixSockFd::GetInAddr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return &((reinterpret_cast<struct sockaddr_in *>(sa))->sin_addr);
    }
    return &((reinterpret_cast<struct sockaddr_in6 *>(sa))->sin6_addr);
}

in_port_t UnixSockFd::GetInPort(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET) {
        return ((reinterpret_cast<struct sockaddr_in *>(sa))->sin_port);
    }
    return ((reinterpret_cast<struct sockaddr_in6 *>(sa))->sin6_port);
}
}  // namespace datasystem

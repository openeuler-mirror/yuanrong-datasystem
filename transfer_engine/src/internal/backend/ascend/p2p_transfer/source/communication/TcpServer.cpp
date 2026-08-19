/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#include "communication/TcpServer.h"
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include "securec.h"

constexpr int LISTEN_BACKLOG = 3;

namespace {

std::string SocketErrorString(int err)
{
    char errBuf[256] = {0};
#if defined(__GLIBC__) && defined(_GNU_SOURCE)
    return strerror_r(err, errBuf, sizeof(errBuf));
#else
    if (strerror_r(err, errBuf, sizeof(errBuf)) != 0) {
        return "unknown error";
    }
    return errBuf;
#endif
}

std::string NormalizeBindIp(const std::string &ip)
{
    if (ip.empty()) {
        return "";
    }
    return ip;
}

Status SetTcpSocketOptions(int fd, int family)
{
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) == -1) {
        return Status::Error(ErrorCode::SOCKET_ERROR,
                             "Failed to set socket option TCP_NODELAY: " + SocketErrorString(errno));
    }
    if (family == AF_INET6) {
        (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &optval, sizeof(optval));
    }
    return Status::Success();
}

struct ListenCandidate {
    int fd = -1;
    sockaddr_storage address {};
    socklen_t addressLen = 0;
    int family = AF_UNSPEC;
};

Status TryCreateListenCandidate(const addrinfo &info, ListenCandidate &candidate, int &lastErrno, bool &retryable)
{
    retryable = false;
    int fd = socket(info.ai_family, info.ai_socktype, info.ai_protocol);
    if (fd < 0) {
        lastErrno = errno;
        retryable = true;
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to create socket");
    }

    Status optionStatus = SetTcpSocketOptions(fd, info.ai_family);
    if (!optionStatus.IsSuccess()) {
        close(fd);
        return optionStatus;
    }

    if (bind(fd, info.ai_addr, info.ai_addrlen) < 0) {
        lastErrno = errno;
        retryable = true;
        close(fd);
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to bind socket");
    }
    if (listen(fd, LISTEN_BACKLOG) < 0) {
        lastErrno = errno;
        retryable = true;
        close(fd);
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to listen on socket");
    }

    errno_t err = memcpy_s(&candidate.address, sizeof(candidate.address), info.ai_addr, info.ai_addrlen);
    if (err != EOK) {
        close(fd);
        return Status::Error(ErrorCode::INTERNAL_ERROR, "Failed to copy server address");
    }
    candidate.fd = fd;
    candidate.addressLen = static_cast<socklen_t>(info.ai_addrlen);
    candidate.family = info.ai_family;
    return Status::Success();
}

}  // namespace

TCPServer::TCPServer(const std::string &interfaceIp, uint32_t acceptTimeOut)
    : serverFd(-1),
      server_port(0),
      client_fd(-1),
      interface_ip(interfaceIp),
      addressLen(0),
      addressFamily(AF_UNSPEC),
      initialized(false),
      acceptTimeOut(acceptTimeOut)
{
    memset_s(&address, sizeof(address), 0, sizeof(address));
}

TCPServer::~TCPServer()
{
    if (initialized) {
        initialized = false;
        Close();
    }
}

Status TCPServer::Listen(uint16_t port)
{
    const std::string bindIp = NormalizeBindIp(interface_ip);
    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    struct addrinfo *result = nullptr;
    const std::string portStr = std::to_string(port);
    const char *node = bindIp.empty() ? nullptr : bindIp.c_str();
    int rc = getaddrinfo(node, portStr.c_str(), &hints, &result);
    if (rc != 0) {
        return Status::Error(ErrorCode::INVALID_INPUT,
                             "Failed to resolve bind address " + bindIp + ": " + gai_strerror(rc));
    }

    int lastErrno = 0;
    ListenCandidate candidate;
    for (auto *it = result; it != nullptr; it = it->ai_next) {
        bool retryable = false;
        Status candidateStatus = TryCreateListenCandidate(*it, candidate, lastErrno, retryable);
        if (candidateStatus.IsSuccess()) {
            errno_t err = memcpy_s(&address, sizeof(address), &candidate.address, sizeof(candidate.address));
            if (err != EOK) {
                close(candidate.fd);
                freeaddrinfo(result);
                return Status::Error(ErrorCode::INTERNAL_ERROR, "Failed to copy server address");
            }
            addressLen = candidate.addressLen;
            addressFamily = candidate.family;
            serverFd = candidate.fd;
            break;
        }
        if (!retryable) {
            freeaddrinfo(result);
            return candidateStatus;
        }
    }
    freeaddrinfo(result);

    if (serverFd < 0) {
        errno = lastErrno;
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to bind or listen on socket");
    }

    server_port = port;
    initialized = true;

    return Status::Success();
}

Status TCPServer::ListenFirstAvailable(uint16_t startPort, uint16_t endPort)
{
    uint16_t tryPort = startPort;
    Status listenStatus;
    do {
        listenStatus = this->Listen(tryPort++);
    } while (!listenStatus.IsSuccess() && tryPort <= endPort && errno == EADDRINUSE);

    return listenStatus;
}

uint16_t TCPServer::GetPort()
{
    return server_port;
}

Status TCPServer::Accept()
{
    // Set up connect timeout
    if (acceptTimeOut > 0) {
        struct timeval tv;
        tv.tv_sec = acceptTimeOut;
        tv.tv_usec = 0;

        if (setsockopt(serverFd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&tv, sizeof tv) < 0) {
            return Status::Error(ErrorCode::SOCKET_ERROR,
                                 "Failed to set socket option SO_RCVTIMEO: " + SocketErrorString(errno));
        }
    }

    struct sockaddr_storage clientAddress {};
    socklen_t addrlen = sizeof(clientAddress);
    client_fd = accept(serverFd, reinterpret_cast<struct sockaddr *>(&clientAddress), &addrlen);
    if (client_fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return Status::Error(ErrorCode::TCP_ERROR, "TCPServer accept timed out");
            ;
        } else {
            return Status::Error(ErrorCode::TCP_ERROR, "TCPServer accept failed");
            ;
        }
    }

    return Status::Success();
}

std::string TCPServer::GetIp()
{
    return interface_ip;
}

int TCPServer::GetIpFamily()
{
    return addressFamily;
}

Status TCPServer::Disconnect()
{
    if (client_fd != -1) {
        if (close(client_fd) == -1) {
            return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to close client fd " + SocketErrorString(errno));
        }
        client_fd = -1;
    }

    return Status::Success();
}

// Implementation of Read method from TcpCommunicator interface
int TCPServer::Read(unsigned char *buffer, size_t bufferSize)
{
    return recv(client_fd, buffer, bufferSize, 0);
}

// Implementation of Write method from TcpCommunicator interface
int TCPServer::Write(const unsigned char *buffer, size_t bufferSize)
{
    return send(client_fd, buffer, bufferSize, 0);
}

Status TCPServer::Close()
{
    this->Disconnect();

    if ((serverFd) != -1) {
        if (close(serverFd) == -1) {
            return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to close server fd " + SocketErrorString(errno));
        }
        (serverFd) = -1;
    }

    return Status::Success();
}

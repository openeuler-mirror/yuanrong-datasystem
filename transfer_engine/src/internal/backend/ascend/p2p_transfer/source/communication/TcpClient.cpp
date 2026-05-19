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
#include "communication/TcpClient.h"
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string>
#include "securec.h"

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

Status SetTcpSocketOptions(int fd, int family)
{
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) == -1) {
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to set TCP_NODELAY: " + SocketErrorString(errno));
    }
    if (family == AF_INET6) {
        (void)setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &optval, sizeof(optval));
    }
    return Status::Success();
}

}  // namespace

TCPClient::TCPClient(const std::string &serverAddress, uint16_t port, uint32_t connectTimeOut)
    : serverFd(-1),
      port(port),
      server_address(serverAddress),
      addressLen(0),
      initialized(false),
      connectTimeOut(connectTimeOut)
{
    memset_s(&address, sizeof(address), 0, sizeof(address));
}

TCPClient::~TCPClient()
{
    if (initialized) {
        initialized = false;
        Close();
    }
}

Status TCPClient::Init()
{
    if (initialized) {
        return Status::Error(ErrorCode::REPEAT_INITIALIZE, "Client already initialized");
    }

    struct addrinfo hints {};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *result = nullptr;
    const std::string portStr = std::to_string(port);
    int rc = getaddrinfo(server_address.c_str(), portStr.c_str(), &hints, &result);
    if (rc != 0) {
        return Status::Error(ErrorCode::INVALID_INPUT,
                             "Failed to resolve server address " + server_address + ": " + gai_strerror(rc));
    }

    for (auto *it = result; it != nullptr; it = it->ai_next) {
        int fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) {
            continue;
        }
        Status optionStatus = SetTcpSocketOptions(fd, it->ai_family);
        if (!optionStatus.IsSuccess()) {
            close(fd);
            freeaddrinfo(result);
            return optionStatus;
        }
        errno_t err = memcpy_s(&address, sizeof(address), it->ai_addr, it->ai_addrlen);
        if (err != EOK) {
            close(fd);
            freeaddrinfo(result);
            return Status::Error(ErrorCode::INTERNAL_ERROR, "Failed to copy server address");
        }
        addressLen = static_cast<socklen_t>(it->ai_addrlen);
        serverFd = fd;
        break;
    }
    freeaddrinfo(result);

    if (serverFd < 0) {
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to create socket " + SocketErrorString(errno));
    }

    initialized = true;
    return Status::Success();
}

std::string TCPClient::GetServerIp()
{
    return server_address;
}

Status TCPClient::Connect()
{
    if (!initialized) {
        return Status::Error(ErrorCode::NOT_INITIALIZED, "Client not yet initialized");
    }

    // Set up connect timeout
    if (connectTimeOut > 0) {
        struct timeval tv;
        tv.tv_sec = connectTimeOut;
        tv.tv_usec = 0;

        if (setsockopt(serverFd, SOL_SOCKET, SO_SNDTIMEO, (const char *)&tv, sizeof tv) < 0) {
            return Status::Error(ErrorCode::SOCKET_ERROR,
                                 "Failed to set socket option SO_SNDTIMEO: " + SocketErrorString(errno));
        }
    }

    if (connect(serverFd, reinterpret_cast<struct sockaddr *>(&address), addressLen) < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return Status::Error(ErrorCode::SOCKET_ERROR, "TCPClient connection timed out");
        } else {
            return Status::Error(ErrorCode::SOCKET_ERROR, "TCPClient connect failed");
        }
    }

    return Status::Success();
}

int TCPClient::Read(unsigned char *buffer, size_t bufferSize)
{
    return recv(serverFd, buffer, bufferSize, 0);
}

int TCPClient::Write(const unsigned char *buffer, size_t bufferSize)
{
    return send(serverFd, buffer, bufferSize, 0);
}

Status TCPClient::Disconnect()
{
    if (serverFd != -1) {
        if (close(serverFd) == -1) {
            return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to close server fd " + SocketErrorString(errno));
        }
        serverFd = -1;
    }

    return Status::Success();
}

Status TCPClient::Close()
{
    CHECK_STATUS(this->Disconnect());
    return Status::Success();
}


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
#include "securec.h"
#include <netinet/tcp.h>

constexpr int LISTEN_BACKLOG = 3;

TCPServer::TCPServer(const std::string &interfaceIp, uint32_t acceptTimeOut)
    : interface_ip(interfaceIp), serverFd(-1), client_fd(-1), acceptTimeOut(acceptTimeOut)
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
    // Create server socket
    serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd == 0) {
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to create socket");
    }

    int optval = 1;  // Value for TCP_NODELAY
    if (setsockopt(serverFd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) == -1) {
        return Status::Error(ErrorCode::SOCKET_ERROR,
                             std::string("Failed to set socket option TCP_NODELAY: ") + strerror(errno));
    }

    address.sin_family = AF_INET;
    if (interface_ip.empty() || interface_ip == "0.0.0.0") {
        address.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, interface_ip.c_str(), &address.sin_addr) <= 0) {
            return Status::Error(ErrorCode::INVALID_INPUT,
                                 "Failed to convert IP address " + interface_ip + " to binary");
        }
    }

    address.sin_port = htons(port);

    // Bind the socket to the port
    if (bind(serverFd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        close(serverFd);
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to bind socket to port");
    }

    // Start listening for connections
    if (listen(serverFd, LISTEN_BACKLOG) < 0) {
        close(serverFd);
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to listen on socket");
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
                                 std::string("Failed to set socket option SO_RCVTIMEO: ") + strerror(errno));
        }
    }

    socklen_t addrlen = sizeof(address);
    client_fd = accept(serverFd, (struct sockaddr *)&address, &addrlen);
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

Status TCPServer::Disconnect()
{
    if (client_fd != -1) {
        if (close(client_fd) == -1) {
            return Status::Error(ErrorCode::SOCKET_ERROR, std::string("Failed to close client fd ") + strerror(errno));
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
        if (close(client_fd) == -1) {
            return Status::Error(ErrorCode::SOCKET_ERROR, std::string("Failed to close server fd ") + strerror(errno));
        }
        (serverFd) = -1;
    }

    return Status::Success();
}
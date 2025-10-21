/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "communication/TcpClient.h"
#include "securec.h"
#include <netinet/tcp.h>

TCPClient::TCPClient(const std::string &serverAddress, uint16_t port, uint32_t connectTimeOut)
    : serverFd(-1), port(port), server_address(serverAddress), connectTimeOut(connectTimeOut), initialized(false)
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

    serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) {
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to create socket " + std::string(strerror(errno)));
    }

    int optval = 1;  // Value for TCP_NODELAY
    if (setsockopt(serverFd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) == -1) {
        close(serverFd);
        return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to set TCP_NODELAY: " + std::string(strerror(errno)));
    }

    address.sin_family = AF_INET;
    address.sin_port = htons(port);

    // Convert IP address from string to binary form
    if (inet_pton(AF_INET, server_address.c_str(), &address.sin_addr) <= 0) {
        close(serverFd);
        return Status::Error(ErrorCode::INVALID_INPUT, "Failed to convert IP address " + server_address + " to binary");
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
                                 "Failed to set socket option SO_SNDTIMEO: " + std::string(strerror(errno)));
        }
    }

    if (connect(serverFd, (struct sockaddr *)&address, sizeof(address)) < 0) {
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
            return Status::Error(ErrorCode::SOCKET_ERROR, "Failed to close server fd " + std::string(strerror(errno)));
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
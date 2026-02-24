/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_TCP_CLIENT_H
#define P2P_TCP_CLIENT_H

#include <unistd.h>
#include "TcpCommunicator.h"
#include "Serializer.h"
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "TCPObjectMixin.h"

// TCPClient supports communication of char* with a server
class TCPClient : public TcpCommunicator {
public:
    TCPClient(const std::string &serverAddress, uint16_t port, uint32_t connectTimeOut);
    ~TCPClient();

    // Initialize server (socket, etc.)
    Status Init();

    // Get IP of the server the client is connected to
    std::string GetServerIp();

    // Connect client to server
    Status Connect(std::function<int()> *p2pCallback = nullptr);

    // Disconnect client
    Status Disconnect();

    // Read buffer over TCP
    int Read(unsigned char *buffer, size_t bufferSize) override;

    // Write buffer over TCP
    int Write(const unsigned char *buffer, size_t bufferSize) override;

    // Close client fd
    Status Close();

private:
    int serverFd;
    uint16_t port;
    std::string server_address;
    struct sockaddr_in address;
    bool initialized;
    uint32_t connectTimeOut;  // Seconds
};

// TCPObjectClient supports communication of arbitrary objects with a server
class TCPObjectClient : public TCPObjectMixin<TCPClient> {
public:
    using TCPObjectMixin::TCPObjectMixin;
};

#endif  // P2P_TCP_CLIENT_H
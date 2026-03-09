/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_TCP_SERVER_H
#define P2P_TCP_SERVER_H

#include <unistd.h>
#include <chrono>
#include "TcpCommunicator.h"
#include "Serializer.h"
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "TCPObjectMixin.h"

#define DEFAULT_BASE_PORT 56800
#define DEFAULT_END_PORT 56900
#define MAX_MSG_SIZE 1024

// TCPServer supports communication of char* with a client
class TCPServer : public TcpCommunicator {
public:
    TCPServer(const std::string &interfaceIp, uint32_t acceptTimeOut);
    ~TCPServer();

    // Listen for connections on the given port
    Status Listen(uint16_t port);

    // Listen for connections on the first available port in the given range
    Status ListenFirstAvailable(uint16_t startPort, uint16_t endPort);

    // Returns the port the server is listening on
    uint16_t GetPort();

    // Accept connection
    Status Accept(std::function<int()> *p2pCallback = nullptr);

    // Get IP the server is listening on
    std::string GetIp();

    // Disconnect server
    Status Disconnect();

    // Read buffer over TCP
    int Read(unsigned char *buffer, size_t bufferSize) override;

    // Write buffer over TCP
    int Write(const unsigned char *buffer, size_t bufferSize) override;

    // Close server fd
    Status Close();

    // Read pings from TCPClient
    void HeartbeatService();

private:
    int serverFd;
    int server_port;
    int client_fd;
    std::string interface_ip;
    struct sockaddr_in address;
    bool initialized;
    uint32_t acceptTimeOut;  // Seconds
    std::chrono::steady_clock::time_point lastPingTime;
};

// TCPObjectServer supports communication of arbitrary objects with a server
class TCPObjectServer : public TCPObjectMixin<TCPServer> {
public:
    using TCPObjectMixin::TCPObjectMixin;
};

#endif  // P2P_TCP_SERVER_H
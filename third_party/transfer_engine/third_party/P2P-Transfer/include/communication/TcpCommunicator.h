/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_TCP_COMMUNICATOR_H
#define P2P_TCP_COMMUNICATOR_H

#include <iostream>
#include <string>
#include <cstring>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

class TcpCommunicator {
public:
    virtual ~TcpCommunicator() {}

    // Read data from the socket
    virtual int Read(unsigned char *buffer, size_t bufferSize) = 0;

    // Write data to the socket
    virtual int Write(const unsigned char *buffer, size_t bufferSize) = 0;
};

#endif  // P2P_TCP_COMMUNICATOR_H
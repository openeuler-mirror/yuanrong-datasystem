/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_COMMUNICATOR_H
#define P2P_COMMUNICATOR_H

#include <stdint.h>
#include "communication/TcpClient.h"
#include "communication/TcpServer.h"
#include "tools/env.h"
#include "tools/tools.h"
#include "p2p.h"
#include "communicator/CommChannel.h"

enum P2PCommChannelType { P2P_COMM_HCCS, P2P_COMM_RDMA };

enum P2PCommRole { P2P_COMM_RECEIVER, P2P_COMM_SENDER };

Status p2pKindToCommRole(P2pKind kind, P2PCommRole &role);

struct P2PCommArgs {
    int32_t deviceId;
    P2pLink linkPref;
    P2PCommRole role;
    // Buffers allocated for communication
    uint32_t nRecvBuffs;      // Amount of buffs
    uint32_t blockSizeBytes;  // Buff size
    // Buffer transmission granularity (transmitted and pipelined in chunkSizeBytes chunks)
    uint32_t chunkSizeBytes;
};

#define ROOTHANDLE_IP_ADDRESS_BUFFER_LEN 64
#define ROOTHANDLE_INDENTIFIER_MAX_LENGTH 64
#define COMMUNICATOR_TCP_TIMEOUT_S 30

// P2P Communicator identification information, allowing a client to connect to a communicator
struct P2PRootHandle {
    char ip[ROOTHANDLE_IP_ADDRESS_BUFFER_LEN];
    uint16_t listenPort;
    char identifier[ROOTHANDLE_INDENTIFIER_MAX_LENGTH];
};

class P2PCommunicator {
public:
    P2PCommunicator(bool isRoot);

    P2PCommunicator(const P2PCommunicator &) = delete;
    P2PCommunicator &operator=(const P2PCommunicator &) = delete;

    Status StartRoot();
    Status StartClient(P2PRootHandle &rootHandle);
    Status GetRootHandle(P2PRootHandle &rootHandle);
    Status GetChannelType(P2PCommChannelType &channelType);
    Status EstablishConnection(P2PCommArgs &args);
    Status Receive(void **dstPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream);
    Status Send(void **srcPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream);

private:
    Status CreateServer();
    Status AcceptClient();
    Status CreateClient(std::string ip, uint16_t port);
    Status ConnectServer();
    P2PCommChannelType DetermineChannelType(const P2PCommArgs &args);

    bool isRoot;
    bool established;
    P2PCommChannelType channel;
    P2PCommRole role;
    std::string identifier;
    std::string clientIp;
    std::string serverIp;

    std::unique_ptr<TCPObjectClient> client;
    std::unique_ptr<TCPObjectServer> server;
    std::unique_ptr<ReceiveChannel> receiver;
    std::unique_ptr<SendChannel> sender;
};

#endif  // P2P_COMMUNICATOR_H
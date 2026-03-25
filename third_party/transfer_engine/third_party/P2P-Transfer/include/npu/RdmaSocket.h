/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RDMA_SOCKET_H
#define P2P_RDMA_SOCKET_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include <arpa/inet.h>
#include <chrono>
#include <vector>
#include "external/ra.h"

enum RdmaSocketStatus {
    SOCKET_INITIALIZED = 0,
    SOCKET_LISTENING,
    SOCKET_CONNECTING,
    SOCKET_WHITELISTED,
    SOCKET_CONNECTED,
    SOCKET_UNINITIALIZED
};

class RdmaSocket {
public:
    RdmaSocket(uint32_t phyId, union hccp_ip_addr ipv4Addr, enum socket_role socketRole)
        : status(RdmaSocketStatus::SOCKET_UNINITIALIZED), phyId(phyId), socketRole(socketRole)
    {
        roceDevInfo.phy_id = phyId;
        roceDevInfo.family = AF_INET;
        roceDevInfo.local_ip = ipv4Addr;
    }

    ~RdmaSocket();

    RdmaSocket(const RdmaSocket &) = delete;
    RdmaSocket &operator=(const RdmaSocket &) = delete;

    Status init();
    Status listenFirstAvailable(unsigned int startPort, unsigned int endPort);
    Status getListenPort(unsigned int &listenport);
    Status connect(union hccp_ip_addr remoteIp, unsigned int remotePort, std::string tag);
    Status getSocketStatus(RdmaSocketStatus *socketStatus);
    Status waitReady(uint32_t timeOutMs);
    Status getFdHandle(void **fdHandle);
    Status addWhitelist(union hccp_ip_addr remoteIp, std::string tag);
    Status removeWhitelist(union hccp_ip_addr remoteIp);

    Status close();

private:
    RdmaSocketStatus status;
    uint32_t phyId;
    enum socket_role socketRole;
    unsigned int listenPort;

    struct rdev roceDevInfo {};
    void *roceSocketHandle;
    struct socket_listen_info_t roceConn {};
    std::vector<socket_wlist_info_t> whiteList;

    std::string tag;
    union hccp_ip_addr remoteIp;
    unsigned int remotePort;

    socket_info_t socketInfo{};
};

#endif  // P2P_RDMA_SOCKET_H
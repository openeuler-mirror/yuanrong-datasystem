/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include "npu/RdmaSocket.h"
#include "npu/RaWrapper.h"
#include <cstdio>
#include <thread>
#include <unistd.h>

RdmaSocket::~RdmaSocket()
{
  if (status == RdmaSocketStatus::SOCKET_LISTENING) {
    STATUS_ERROR(RaSocketListenStop(&roceConn, 1));
  }

    if (status >= RdmaSocketStatus::SOCKET_LISTENING) {
        for (int i = 0; i < whiteList.size(); i++) {
            RaSocketWhiteListDel(roceSocketHandle, &whiteList[i], 1);
        }
    }

    if (status >= RdmaSocketStatus::SOCKET_INITIALIZED) {
        RaRdevDeinit(rdmaHandle, EVENTID);
    }

    if (status >= RdmaSocketStatus::SOCKET_INITIALIZED) {
        ra_socket_deinit(roceSocketHandle);
    }
}

Status RdmaSocket::init()
{
    if (status != RdmaSocketStatus::SOCKET_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma agent is already initialized");
    }

    ACL_CHECK_STATUS(ra_socket_init(NETWORK_OFFLINE, roceDevInfo, &roceSocketHandle));
    status = RdmaSocketStatus::SOCKET_INITIALIZED;

    rdev_init_info roceInitInfo = {DEFAULT_INIT_RDMA_CONFIG};
    roceInitInfo.mode = NETWORK_OFFLINE;
    roceInitInfo.notify_type = EVENTID;

    CHECK_STATUS(RaRdevInitV2(roceInitInfo, roceDevInfo, &rdmaHandle));
    status = RdmaSocketStatus::RDEV_INITIALIZED;

    return Status::Success();
}

Status RdmaSocket::listenFirstAvailable(unsigned int startPort, unsigned int endPort)
{
    if (status != RdmaSocketStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma rdev is not initialized");
    }
    
    unsigned int tryPort = startPort;
    roceConn.socket_handle = roceSocketHandle;
    roceConn.port = tryPort;

    Status res;
    while (true) {
        res = RaSocketListenStart(&roceConn, 1);
        if (res.IsSuccess()) {
            break;
        } else if (res.Code() == ErrorCode::ERR_SOCK_EADDRINUSE) {
            tryPort++;
            roceConn.port = tryPort;
        } else {
            return res;
        }
    }
    
    this->listenPort = tryPort;

    status = RdmaSocketStatus::SOCKET_LISTENING;
    return Status::Success();
}

Status RdmaSocket::getListenPort(unsigned int &listenPort)
{
    if (status == RdmaSocketStatus::SOCKET_UNINITIALIZED || status == RdmaSocketStatus::RDEV_INITIALIZED
        || status == RdmaSocketStatus::SOCKET_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "listen port cannot be obtained in current state");
    }

    listenPort = this->listenPort;

    return Status::Success();
}

Status RdmaSocket::connect(union hccp_ip_addr remoteIp,
                           unsigned int remotePort,
                           std::string tag)
{
    if (socketRole != socket_role::CLIENT) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "connect only supported for client socket");
    }

    if (status != RdmaSocketStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "socket must be initialized to connect");
    }

    struct socket_connect_info_t connectInfo {};
    connectInfo.socket_handle = roceSocketHandle;
    connectInfo.remote_ip = remoteIp;
    connectInfo.port = remotePort;
    snprintf(connectInfo.tag, sizeof(connectInfo.tag), "%s", tag.c_str());
    CHECK_STATUS(RaSocketBatchConnect(&connectInfo, 1));
    status = RdmaSocketStatus::SOCKET_CONNECTING;

    socketInfo.remote_ip = remoteIp;
    socketInfo.socket_handle = roceSocketHandle;
    snprintf(socketInfo.tag, sizeof(socketInfo.tag), "%s", tag.c_str());

    this->remoteIp = remoteIp;
    this->remotePort = remotePort;
    this->tag = tag;

    return Status::Success();
}

Status RdmaSocket::getSocketStatus(RdmaSocketStatus* socketStatus)
{
    if (status == RdmaSocketStatus::SOCKET_CONNECTING || status == RdmaSocketStatus::SOCKET_WHITELISTED) {
        uint32_t numConnected = 0;

        CHECK_STATUS(RaGetSockets(socketRole, &socketInfo, 1, &numConnected));
        if (numConnected == 1) {
            status = RdmaSocketStatus::SOCKET_CONNECTED;
        }
    }

    *socketStatus = status;
    return Status::Success();
}

Status RdmaSocket::waitReady(uint32_t timeOutMs)
{
    if (status == RdmaSocketStatus::SOCKET_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "socket has not been initialized yet");
    }

    auto startTime = std::chrono::steady_clock::now();
    auto timeOutDuration = std::chrono::milliseconds(timeOutMs);

    RdmaSocketStatus socketStatus = RdmaSocketStatus::SOCKET_UNINITIALIZED;
    while (socketStatus != RdmaSocketStatus::SOCKET_CONNECTED) {
        auto currentTime = std::chrono::steady_clock::now();
        if (timeOutMs > 0 && currentTime - startTime >= timeOutDuration) {
            return Status::Error(ErrorCode::TIMEOUT, "Timeout waiting for socket to connect.");
        }

        CHECK_STATUS(this->getSocketStatus(&socketStatus));
    }

    return Status::Success();
}

Status RdmaSocket::getRdmaHandle(void** rdmaHandle)
{
    if (status < RdmaSocketStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "rdev is not yet initialized");
    }

    *rdmaHandle = this->rdmaHandle;

    return Status::Success();
}

Status RdmaSocket::getFdHandle(void** fdHandle)
{
    if (status != RdmaSocketStatus::SOCKET_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "socket is not yet connected");
    }

    *fdHandle = socketInfo.fd_handle;

    return Status::Success();
}

Status RdmaSocket::close()
{
    if (status == RdmaSocketStatus::SOCKET_UNINITIALIZED || status < RdmaSocketStatus::SOCKET_CONNECTING) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "socket connection has not been created");
    }

    struct socket_close_info_t closeInfo {};
    closeInfo.socket_handle = socketInfo.socket_handle;
    closeInfo.fd_handle = socketInfo.fd_handle;

    CHECK_STATUS(RaSocketBatchClose(&closeInfo, 1));
    return Status::Success();
}

Status RdmaSocket::addWhitelist(union hccp_ip_addr remoteIp, std::string tag)
{
    if (socketRole != socket_role::SERVER) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "whitelist only supported for server socket");
    }

    if (status != RdmaSocketStatus::SOCKET_LISTENING) {
        return Status::Error(ErrorCode::NOT_SUPPORTED,
                             "socket is not listening");
    }

    socket_wlist_info_t whiteListEntry = {};
    const unsigned int kConnectionLimit = 8;
    const unsigned int kNumEntries = 1;

    whiteListEntry.remote_ip = remoteIp;

    snprintf(whiteListEntry.tag, sizeof(whiteListEntry.tag), "%s", tag.c_str());

    whiteListEntry.conn_limit = kConnectionLimit;
    CHECK_STATUS(RaSocketWhiteListAdd(roceSocketHandle, &whiteListEntry, kNumEntries));
    whiteList.push_back(whiteListEntry);

    socketInfo.remote_ip = remoteIp;
    socketInfo.socket_handle = roceSocketHandle;
    
    snprintf(socketInfo.tag, sizeof(socketInfo.tag), "%s", tag.c_str());

    status = RdmaSocketStatus::SOCKET_WHITELISTED;

    return Status::Success();
}

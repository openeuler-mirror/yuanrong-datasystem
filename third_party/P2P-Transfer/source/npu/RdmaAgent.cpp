/*
* Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/RdmaAgent.h"
#include "npu/RaWrapper.h"

RdmaAgent::~RdmaAgent()
{
    if (status == RdmaAgentStatus::RA_INITIALIZED) {
        RaDeinit(&initConfig);
    }
}

Status RdmaAgent::init()
{
    if (status == RdmaAgentStatus::RA_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma agent is already initialized");
    }

    CHECK_STATUS(RaInit(&initConfig));
    status = RdmaAgentStatus::RA_INITIALIZED;
    return Status::Success();
}

Status RdmaAgent::getDeviceIpv4(union hccp_ip_addr *ipv4Addr)
{
    if (status != RdmaAgentStatus::RA_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma agent is not initialized");
    }

    unsigned int num = 0;
    struct ra_get_ifattr ifAttr {};
    ifAttr.phy_id = initConfig.phy_id;
    ifAttr.nic_position = initConfig.nic_position;
    ifAttr.is_all = false;
    CHECK_STATUS(RaGetIfNum(&ifAttr, &num));

    struct interface_info interface_infos[num];
    CHECK_STATUS(RaGetIfaddrs(&ifAttr, interface_infos, &num));
    for (int i = 0; i < num; i++) {
        if (interface_infos[i].family == AF_INET) {
            ipv4Addr->addr = interface_infos[i].ifaddr.ip.addr;
            return Status::Success();
        }
    }

    return Status::Error(ErrorCode::NOT_FOUND, "IPv4 device IP not found");
}
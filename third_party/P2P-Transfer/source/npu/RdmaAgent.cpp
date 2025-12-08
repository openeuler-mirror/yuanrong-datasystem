/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/RdmaAgent.h"
#include "npu/RaWrapper.h"
#include <string>
#include "hccl/hccl_network_pub.h"
#include "hccl/adapter_hccp_common.h"
#include "runtime/dev.h"

std::shared_ptr<RdmaAgent> RdmaAgent::instances[MAX_LOCAL_DEVICES];
std::mutex RdmaAgent::instanceMutex;

Status RdmaAgent::GetInstance(uint32_t deviceId, std::shared_ptr<RdmaAgent> &outAgent)
{
    std::lock_guard<std::mutex> lock(instanceMutex);

    if (deviceId >= MAX_LOCAL_DEVICES) {
        return Status::Error(ErrorCode::OUT_OF_RANGE, "DeviceId " + std::to_string(deviceId) + " out of range.");
    }

    if (!instances[deviceId]) {
        uint32_t phyId;
        ACL_CHECK_STATUS(rtGetDevicePhyIdByIndex(deviceId, &phyId));
        instances[deviceId] = std::make_shared<RdmaAgent>(deviceId, phyId);
        CHECK_STATUS(instances[deviceId]->init());
    }

    outAgent = instances[deviceId];
    return Status::Success();
}

RdmaAgent::RdmaAgent(uint32_t devId, uint32_t phyId)
    : status(RdmaAgentStatus::RA_UNINITIALIZED), devId(devId), phyId(phyId)
{
    nicDeployment = NICDeployment::NIC_DEPLOYMENT_DEVICE;
}

RdmaAgent::~RdmaAgent()
{
    if (status == RdmaAgentStatus::RA_INITIALIZED) {
        HcclNetDeInit(nicDeployment, phyId, devId, false);
    }
}

Status RdmaAgent::init()
{
    if (status == RdmaAgentStatus::RA_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma agent is already initialized");
    }

    ACL_CHECK_STATUS(HcclNetInit(nicDeployment, phyId, devId, false));

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
    ifAttr.phy_id = phyId;
    ifAttr.nic_position = static_cast<int>(nicDeployment);
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
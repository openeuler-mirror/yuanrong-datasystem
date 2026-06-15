/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/RdmaAgent.h"
#include "npu/RaWrapper.h"
#include <string>
#include "external/hccl_network_pub.h"
#include "runtime/dev.h"
#include "external/adapter_rts_common.h"
#include "tools/env.h"

std::shared_ptr<RdmaAgent> RdmaAgent::instances[MAX_LOCAL_DEVICES];
std::mutex RdmaAgent::instanceMutex;

Status RdmaAgent::GetInstance(uint32_t deviceId, std::shared_ptr<RdmaAgent> &outAgent)
{
    std::lock_guard<std::mutex> lock(instanceMutex);

    // Prefer runtime logic ID and only normalize when the runtime rejects it.
    uint32_t logicDeviceId = deviceId;
    uint32_t phyId = 0;
    HcclResult phyResult = hrtGetDevicePhyIdByIndex(logicDeviceId, phyId, true);

    if (phyResult != HCCL_SUCCESS) {
        if (!TryMapVisibleToLogicDeviceId(deviceId, logicDeviceId)) {
            ACL_CHECK_STATUS(rtGetDeviceIndexByPhyId(deviceId, &logicDeviceId));
        }
        ACL_CHECK_STATUS(hrtGetDevicePhyIdByIndex(logicDeviceId, phyId, true));
    }

    if (logicDeviceId >= MAX_LOCAL_DEVICES) {
        return Status::Error(ErrorCode::OUT_OF_RANGE,
                             "LogicDeviceId " + std::to_string(logicDeviceId) + " out of range.");
    }

    if (!instances[logicDeviceId]) {
        instances[logicDeviceId] = std::make_shared<RdmaAgent>(logicDeviceId, phyId);
        CHECK_STATUS(instances[logicDeviceId]->init());
    }

    outAgent = instances[logicDeviceId];
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
    struct ra_get_ifattr ifAttr{};
    ifAttr.phy_id = phyId;
    ifAttr.nic_position = static_cast<int>(nicDeployment);
    ifAttr.is_all = false;
    CHECK_STATUS(RaGetIfNumWrapper(&ifAttr, &num));

    struct interface_info interface_infos[num];
    CHECK_STATUS(RaGetIfaddrsWrapper(&ifAttr, interface_infos, &num));
    for (int i = 0; i < num; i++) {
        if (interface_infos[i].family == AF_INET) {
            ipv4Addr->addr = interface_infos[i].ifaddr.ip.addr;
            return Status::Success();
        }
    }

    return Status::Error(ErrorCode::NOT_FOUND, "IPv4 device IP not found");
}

uint32_t RdmaAgent::getPhyId() const
{
    return phyId;
}

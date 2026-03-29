/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/RdmaAgent.h"
#include "npu/RaWrapper.h"
#include <string>
#include "tools/env.h"
#include "tools/logging.h"
#include "external/hccl_network_pub.h"
#include "runtime/dev.h"
#include "external/adapter_rts_common.h"

std::shared_ptr<RdmaAgent> RdmaAgent::instances[MAX_LOCAL_DEVICES];
std::mutex RdmaAgent::instanceMutex;

Status RdmaAgent::GetInstance(uint32_t deviceId, std::shared_ptr<RdmaAgent> &outAgent)
{
    std::lock_guard<std::mutex> lock(instanceMutex);

    uint32_t logicDeviceId = deviceId;
    bool mappedByEnv = TryMapVisibleToLogicDeviceId(deviceId, logicDeviceId);
    if (mappedByEnv) {
        p2p::LogInfo(std::string("RdmaAgent::GetInstance pre-map by env, input_device_id=") +
                     std::to_string(deviceId) + ", logic_device_id=" + std::to_string(logicDeviceId));
    }
    uint32_t phyId = 0;
    HcclResult phyRc = hrtGetDevicePhyIdByIndex(logicDeviceId, phyId, true);
    p2p::LogInfo(std::string("RdmaAgent::GetInstance phy query #1, input_device_id=") +
                 std::to_string(deviceId) + ", logic_device_id=" + std::to_string(logicDeviceId) +
                 ", rc=" + std::to_string(static_cast<int>(phyRc)));
    if (phyRc != HCCL_SUCCESS) {
        // Runtime may expose visible/physical id here (e.g. ASCEND_RT_VISIBLE_DEVICES),
        // normalize to a runtime logic index before querying physical id again.
        if (!mappedByEnv) {
            ACL_CHECK_STATUS(rtGetDeviceIndexByPhyId(deviceId, &logicDeviceId));
            p2p::LogInfo(std::string("RdmaAgent::GetInstance fallback rtGetDeviceIndexByPhyId success, input=") +
                         std::to_string(deviceId) + ", logic_device_id=" + std::to_string(logicDeviceId));
        } else {
            p2p::LogInfo(std::string("RdmaAgent::GetInstance fallback env map success, input=") +
                         std::to_string(deviceId) + ", logic_device_id=" + std::to_string(logicDeviceId));
        }
        ACL_CHECK_STATUS(hrtGetDevicePhyIdByIndex(logicDeviceId, phyId, true));
        p2p::LogInfo(std::string("RdmaAgent::GetInstance phy query #2 success, logic_device_id=") +
                     std::to_string(logicDeviceId) + ", phy_id=" + std::to_string(phyId));
    } else {
        p2p::LogInfo(std::string("RdmaAgent::GetInstance phy query #1 success, phy_id=") + std::to_string(phyId));
    }

    if (logicDeviceId >= MAX_LOCAL_DEVICES) {
        return Status::Error(ErrorCode::OUT_OF_RANGE, "LogicDeviceId " + std::to_string(logicDeviceId) + " out of range.");
    }

    if (!instances[logicDeviceId]) {
        // Cache and initialize by normalized logic id to avoid duplicate HcclNetInit
        // when callers pass physical ids in other paths.
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

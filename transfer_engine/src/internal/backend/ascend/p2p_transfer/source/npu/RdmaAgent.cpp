/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/RdmaAgent.h"
#include "npu/RaWrapper.h"
#include <cstdlib>
#include <string>
#include <vector>
#include "tools/env.h"
#include "tools/logging.h"
#include "tools/tools.h"
#include "external/hccl_network_pub.h"
#include "runtime/dev.h"
#include "external/adapter_rts_common.h"

std::shared_ptr<RdmaAgent> RdmaAgent::instances[MAX_LOCAL_DEVICES];
std::mutex RdmaAgent::instanceMutex;

namespace {

Status ParseRoceAddressFamily(int &family)
{
    family = AF_UNSPEC;
    const char *envValue = std::getenv(ROCE_ADDR_FAMILY_ENV);
    if (envValue == nullptr || envValue[0] == '\0' || std::string(envValue) == "auto") {
        return Status::Success();
    }
    if (std::string(envValue) == "ipv4" || std::string(envValue) == "IPv4") {
        family = AF_INET;
        return Status::Success();
    }
    if (std::string(envValue) == "ipv6" || std::string(envValue) == "IPv6") {
        family = AF_INET6;
        return Status::Success();
    }
    return Status::Error(ErrorCode::INVALID_ENV, std::string("Invalid ") + ROCE_ADDR_FAMILY_ENV + " value.");
}

bool FamilyMatches(int actualFamily, int preferredFamily)
{
    return preferredFamily == AF_UNSPEC || actualFamily == preferredFamily;
}

Status CopyDeviceIp(const interface_info &info, P2PIpAddress *ipAddr)
{
    ipAddr->family = info.family;
    ipAddr->addr = info.ifaddr.ip;
    return Status::Success();
}

template <typename T>
Status ValidateOutputAddress(const T *address, const char *name)
{
    if (address == nullptr) {
        return Status::Error(ErrorCode::INVALID_INPUT, std::string(name) + " should not be null");
    }
    return Status::Success();
}

}  // namespace

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

Status RdmaAgent::getDeviceIp(P2PIpAddress *ipAddr)
{
    if (status != RdmaAgentStatus::RA_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma agent is not initialized");
    }
    CHECK_STATUS(ValidateOutputAddress(ipAddr, "ipAddr"));

    unsigned int num = 0;
    struct ra_get_ifattr ifAttr{};
    ifAttr.phy_id = phyId;
    ifAttr.nic_position = static_cast<int>(nicDeployment);
    ifAttr.is_all = false;
    CHECK_STATUS(RaGetIfNumWrapper(&ifAttr, &num));
    if (num == 0) {
        return Status::Error(ErrorCode::NOT_FOUND, "NPU network interface not found");
    }

    std::vector<interface_info> interfaceInfos(num);
    CHECK_STATUS(RaGetIfaddrsWrapper(&ifAttr, interfaceInfos.data(), &num));

    int preferredFamily = AF_UNSPEC;
    CHECK_STATUS(ParseRoceAddressFamily(preferredFamily));

    if (preferredFamily == AF_INET || preferredFamily == AF_INET6) {
        for (unsigned int i = 0; i < num; i++) {
            if (interfaceInfos[i].family == preferredFamily) {
                return CopyDeviceIp(interfaceInfos[i], ipAddr);
            }
        }
        return Status::Error(ErrorCode::NOT_FOUND,
                             (preferredFamily == AF_INET6) ? "IPv6 device IP not found" : "IPv4 device IP not found");
    }

    for (unsigned int i = 0; i < num; i++) {
        if (interfaceInfos[i].family == AF_INET && FamilyMatches(interfaceInfos[i].family, preferredFamily)) {
            return CopyDeviceIp(interfaceInfos[i], ipAddr);
        }
    }
    for (unsigned int i = 0; i < num; i++) {
        if (interfaceInfos[i].family == AF_INET6 && FamilyMatches(interfaceInfos[i].family, preferredFamily)) {
            return CopyDeviceIp(interfaceInfos[i], ipAddr);
        }
    }

    return Status::Error(ErrorCode::NOT_FOUND, "device IP not found");
}

Status RdmaAgent::getDeviceIpv4(union hccp_ip_addr *ipv4Addr)
{
    CHECK_STATUS(ValidateOutputAddress(ipv4Addr, "ipv4Addr"));
    if (status != RdmaAgentStatus::RA_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma agent is not initialized");
    }

    p2p::LogInfo(std::string("RdmaAgent::getDeviceIpv4 begin, logic_device_id=") + std::to_string(devId) +
                 ", phy_id=" + std::to_string(phyId) +
                 ", nic_deployment=" + std::to_string(static_cast<int>(nicDeployment)));
    unsigned int num = 0;
    struct ra_get_ifattr ifAttr{};
    ifAttr.phy_id = phyId;
    ifAttr.nic_position = static_cast<int>(nicDeployment);
    ifAttr.is_all = false;
    Status ifNumRc = RaGetIfNumWrapper(&ifAttr, &num);
    if (!ifNumRc.IsSuccess()) {
        p2p::LogError(std::string("RdmaAgent::getDeviceIpv4 query interface count failed, logic_device_id=") +
                      std::to_string(devId) + ", phy_id=" + std::to_string(phyId) +
                      ", reason=" + ifNumRc.ToString());
        return ifNumRc;
    }
    if (num == 0) {
        std::string msg = "NPU network interface not found, logic_device_id=" + std::to_string(devId) +
                          ", phy_id=" + std::to_string(phyId) +
                          ", hint: verify the NPU device is exposed to the container and its RoCE network is "
                          "configured";
        p2p::LogError(std::string("RdmaAgent::getDeviceIpv4 failed, reason=") + msg);
        return Status::Error(ErrorCode::NOT_FOUND, msg);
    }

    struct interface_info interface_infos[num];
    Status ifAddrRc = RaGetIfaddrsWrapper(&ifAttr, interface_infos, &num);
    if (!ifAddrRc.IsSuccess()) {
        p2p::LogError(std::string("RdmaAgent::getDeviceIpv4 query interface addresses failed, logic_device_id=") +
                      std::to_string(devId) + ", phy_id=" + std::to_string(phyId) +
                      ", interface_count=" + std::to_string(num) + ", reason=" + ifAddrRc.ToString());
        return ifAddrRc;
    }
    for (int i = 0; i < num; i++) {
        if (interface_infos[i].family == AF_INET) {
            ipv4Addr->addr = interface_infos[i].ifaddr.ip.addr;
            p2p::LogInfo(std::string("RdmaAgent::getDeviceIpv4 success, logic_device_id=") +
                         std::to_string(devId) + ", phy_id=" + std::to_string(phyId) +
                         ", npu_ip=" + in_addr_to_string(ipv4Addr->addr));
            return Status::Success();
        }
    }

    std::string msg = "NPU IPv4 address not found, logic_device_id=" + std::to_string(devId) +
                      ", phy_id=" + std::to_string(phyId) + ", interface_count=" + std::to_string(num) +
                      ", hint: configure a valid NPU RoCE IPv4 address on this device";
    p2p::LogError(std::string("RdmaAgent::getDeviceIpv4 failed, reason=") + msg);
    return Status::Error(ErrorCode::NOT_FOUND, msg);
}

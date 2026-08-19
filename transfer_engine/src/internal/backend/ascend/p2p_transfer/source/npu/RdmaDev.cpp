/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "npu/RdmaDev.h"
#include "npu/RdmaAgent.h"
#include "npu/RaWrapper.h"
#include <arpa/inet.h>
#include <cstring>
#include <string>

namespace {

bool IsValidIpFamily(int family)
{
    return family == AF_INET || family == AF_INET6;
}

P2PIpAddress NormalizeSegmentIp(const P2PSegmentHandle &segmentHandle)
{
    P2PIpAddress ip;
    // An unrecognized tag comes from the padding bytes of the legacy IPv4-only wire layout.
    ip.family = segmentHandle.ipFamilyTag == P2P_SEGMENT_IPV6_FAMILY_TAG ? AF_INET6 : AF_INET;
    ip.addr = segmentHandle.ipAddr;
    return ip;
}

RdmaDev::RemoteRegionKey MakeRemoteRegionKey(uintptr_t ddrAddr, const P2PIpAddress &ip)
{
    RdmaDev::RemoteRegionKey key {};
    key.family = ip.family;
    key.ddrAddr = ddrAddr;
    if (ip.family == AF_INET6) {
        std::memcpy(key.addr.data(), &ip.addr.addr6, sizeof(ip.addr.addr6));
    } else {
        key.family = AF_INET;
        std::memcpy(key.addr.data(), &ip.addr.addr, sizeof(ip.addr.addr));
    }
    return key;
}

bool SameIpAddress(const P2PIpAddress &left, const P2PIpAddress &right)
{
    if (left.family != right.family) {
        return false;
    }
    if (left.family == AF_INET6) {
        return std::memcmp(&left.addr.addr6, &right.addr.addr6, sizeof(left.addr.addr6)) == 0;
    }
    return left.addr.addr.s_addr == right.addr.addr.s_addr;
}

}  // namespace

std::shared_ptr<RdmaDev> RdmaDev::instances[MAX_LOCAL_DEVICES];
std::mutex RdmaDev::instanceMutex;

// For now, we just distinguish devices by deviceId. Only 1 device/IP per device
Status RdmaDev::GetInstance(uint32_t deviceId, std::shared_ptr<RdmaDev> &outDev)
{
    std::lock_guard<std::mutex> lock(instanceMutex);

    if (deviceId >= MAX_LOCAL_DEVICES) {
        return Status::Error(ErrorCode::OUT_OF_RANGE, "DeviceId " + std::to_string(deviceId) + " out of range.");
    }

    if (!instances[deviceId]) {
        std::shared_ptr<RdmaAgent> agent;
        CHECK_STATUS(RdmaAgent::GetInstance(deviceId, agent));

        P2PIpAddress localIp;
        CHECK_STATUS(agent->getDeviceIp(&localIp));

        instances[deviceId] = std::make_shared<RdmaDev>(deviceId, localIp);
        CHECK_STATUS(instances[deviceId]->init());
    }

    outDev = instances[deviceId];
    return Status::Success();
}

RdmaDev::RdmaDev(uint32_t phyId, P2PIpAddress localIp)
    : status(RdmaDevStatus::RDEV_UNINITIALIZED), phyId(phyId), localIp(localIp)
{
    roceDevInfo.phy_id = phyId;
    roceDevInfo.family = localIp.family;
    roceDevInfo.local_ip = localIp.addr;
}

RdmaDev::~RdmaDev()
{
    if (status == RdmaDevStatus::RDEV_INITIALIZED) {
        RaRdevDeinitWrapper(rdmaHandle, EVENTID, initialized);
    }
}

Status RdmaDev::init()
{
    if (status == RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is already initialized");
    }

    rdev_init_info roceInitInfo = { DEFAULT_INIT_RDMA_CONFIG };
    roceInitInfo.mode = NETWORK_OFFLINE;
    roceInitInfo.notify_type = EVENTID;

    CHECK_STATUS(RaRdevInitV2Wrapper(roceInitInfo, roceDevInfo, &rdmaHandle, initialized));
    status = RdmaDevStatus::RDEV_INITIALIZED;

    return Status::Success();
}

Status RdmaDev::getRdmaHandle(void **rdmaHandle)
{
    if (status < RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }

    *rdmaHandle = this->rdmaHandle;

    return Status::Success();
}

Status RdmaDev::getIp(P2PIpAddress *ipAddr)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }
    if (ipAddr == nullptr) {
        return Status::Error(ErrorCode::INVALID_INPUT, "ipAddr should not be null");
    }
    *ipAddr = localIp;
    return Status::Success();
}

Status RdmaDev::getIpv4(union hccp_ip_addr *retIpv4Addr)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }
    if (retIpv4Addr == nullptr) {
        return Status::Error(ErrorCode::INVALID_INPUT, "ipv4Addr should not be null");
    }
    if (localIp.family != AF_INET) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev local IP is not IPv4");
    }
    retIpv4Addr->addr = localIp.addr.addr;
    return Status::Success();
}

Status RdmaDev::registerGlobalMemoryRegion(void *ddrPtr, void *devPtr, uint64_t size, int access)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }
    // Later check if not nullptr
    {
        std::lock_guard<std::mutex> lock(mrMut);
        auto it = registeredMrs.find(ddrPtr);
        if (it != registeredMrs.end()) {
            return Status::Error(ErrorCode::NOT_SUPPORTED, "address already registered to rdma dev");
        }

        void *mrHandle;
        struct mr_info mrInfo {};
        mrInfo.addr = devPtr;
        mrInfo.size = size;
        mrInfo.access = access;
        CHECK_STATUS(RaGlobalMrRegWrapper(rdmaHandle, &mrInfo, &mrHandle));
        registeredMrs[ddrPtr] = { size, devPtr, mrHandle, ddrPtr, mrInfo.rkey };
    }

    return Status::Success();
}

Status RdmaDev::getSegmentHandle(void *addr, struct P2PSegmentHandle &segmentHandle)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }

    {
        std::lock_guard<std::mutex> lock(mrMut);
        auto it = registeredMrs.find(addr);
        if (it == registeredMrs.end()) {
            return Status::Error(ErrorCode::NOT_SUPPORTED, "address not registered to rdma dev");
        }

        P2PLocalSegment localSegment = it->second;
        segmentHandle.size = localSegment.size;
        segmentHandle.devPtr = localSegment.devPtr;
        segmentHandle.ddrPtr = localSegment.ddrPtr;
        segmentHandle.rKey = localSegment.rKey;
        segmentHandle.ipAddr = localIp.addr;
        segmentHandle.ipFamilyTag = localIp.family == AF_INET6 ? P2P_SEGMENT_IPV6_FAMILY_TAG
                                                               : P2P_SEGMENT_IPV4_FAMILY_TAG;
    }

    return Status::Success();
}

Status RdmaDev::unRegisterGlobalMemoryRegion(void *addr)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }

    {
        std::lock_guard<std::mutex> lock(mrMut);
        auto it = registeredMrs.find(addr);
        if (it == registeredMrs.end()) {
            return Status::Error(ErrorCode::NOT_SUPPORTED, "address not registered to rdma dev");
        }

        void *mrHandle = it->second.mrHandle;

        CHECK_STATUS(RaGlobalMrDeRegWrapper(rdmaHandle, mrHandle));
    }

    return Status::Success();
}

Status p2pSegmentPermissionsToFlag(P2pSegmentPermissions permissions, int &flag)
{
    switch (permissions) {
        case P2P_SEGMENT_READ_WRITE:
            flag = RA_ACCESS_REMOTE_WRITE | RA_ACCESS_LOCAL_WRITE | RA_ACCESS_REMOTE_READ;
            break;
        case P2P_SEGMENT_READ_ONLY:
            flag = RA_ACCESS_REMOTE_READ;
            break;
        case P2P_SEGMENT_WRITE_ONLY:
            flag = RA_ACCESS_REMOTE_WRITE | RA_ACCESS_LOCAL_WRITE;
            break;
        default:
            return Status::Error(ErrorCode::NOT_SUPPORTED, "p2pKind unknown");
    }

    return Status::Success();
}

Status RdmaDev::addRemoteSegment(struct P2PSegmentHandle segmentHandle)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }

    P2PIpAddress segmentIp = NormalizeSegmentIp(segmentHandle);
    RemoteRegionKey key = MakeRemoteRegionKey(reinterpret_cast<uintptr_t>(segmentHandle.ddrPtr), segmentIp);
    {
        std::lock_guard<std::mutex> lock(remoteMrMut);
        auto it = remoteMrs.find(key);
        if (it != remoteMrs.end()) {
            return Status::Error(ErrorCode::NOT_SUPPORTED, "address already registered remote mr to rdma dev");
        }

        remoteMrs[key] = segmentHandle;
    }

    return Status::Success();
}

Status RdmaDev::getRemoteSegment(void *ddrPtr, P2PIpAddress remoteIp, struct P2PSegmentHandle &segmentHandle)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }
    if (!IsValidIpFamily(remoteIp.family)) {
        return Status::Error(ErrorCode::INVALID_INPUT, "remote host segment lookup got invalid address family");
    }

    uintptr_t targetAddr = reinterpret_cast<uintptr_t>(ddrPtr);
    RemoteRegionKey key = MakeRemoteRegionKey(targetAddr, remoteIp);

    {
        std::lock_guard<std::mutex> lock(remoteMrMut);

        // Find first region with key greater than than our target key.
        auto it = remoteMrs.upper_bound(key);
        // No region can contain the address.
        if (it == remoteMrs.begin()) {
            std::cerr << "tried to get ddrPtr " << ddrPtr << std::endl;
            return Status::Error(ErrorCode::NOT_SUPPORTED, "address not registered to rdma dev");
        }

        --it;

        const P2PSegmentHandle &candidate = it->second;
        P2PIpAddress candidateIp = NormalizeSegmentIp(candidate);
        uintptr_t candidateStart = reinterpret_cast<uintptr_t>(candidate.ddrPtr);
        const bool containsTarget = targetAddr >= candidateStart && targetAddr - candidateStart < candidate.size;
        if (!SameIpAddress(candidateIp, remoteIp) || !containsTarget) {
            std::cerr << "tried to get ddrPtr 2 " << ddrPtr << std::endl;
            std::cerr << "candidate family " << candidateIp.family << "!= remote family " << remoteIp.family
                      << std::endl;
            std::cerr << "target address " << targetAddr << ", candidate start " << candidateStart
                      << ", candidate size " << candidate.size << std::endl;

            return Status::Error(ErrorCode::NOT_SUPPORTED, "address not registered to rdma dev");
        }

        segmentHandle = candidate;
    }

    return Status::Success();
}

Status RdmaDev::getRemoteSegment(void *ddrPtr, union hccp_ip_addr ipv4Addr, struct P2PSegmentHandle &segmentHandle)
{
    P2PIpAddress remoteIp;
    remoteIp.family = AF_INET;
    remoteIp.addr = ipv4Addr;
    return getRemoteSegment(ddrPtr, remoteIp, segmentHandle);
}

// Add mutex for registering mr etc. if necessary

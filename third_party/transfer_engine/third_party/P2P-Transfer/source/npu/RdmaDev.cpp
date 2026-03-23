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
#include <string>
#include <arpa/inet.h>

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

        union hccp_ip_addr ipv4Addr;
        CHECK_STATUS(agent->getDeviceIpv4(&ipv4Addr));

        instances[deviceId] = std::make_shared<RdmaDev>(deviceId, ipv4Addr);
        CHECK_STATUS(instances[deviceId]->init());
    }

    outDev = instances[deviceId];
    return Status::Success();
}

RdmaDev::RdmaDev(uint32_t phyId, union hccp_ip_addr ipv4Addr)
    : status(RdmaDevStatus::RDEV_UNINITIALIZED), phyId(phyId), ipv4Addr(ipv4Addr)
{
    roceDevInfo.phy_id = phyId;
    roceDevInfo.family = AF_INET;
    roceDevInfo.local_ip = ipv4Addr;
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

Status RdmaDev::getIpv4(union hccp_ip_addr *retIpv4Addr)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }
    retIpv4Addr->addr = ipv4Addr.addr;
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
        segmentHandle.ipAddr = ipv4Addr;
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

    // Later check if not nullptr
    RemoteRegionKey key = { segmentHandle.ipAddr.addr.s_addr, segmentHandle.ddrPtr };
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

Status RdmaDev::getRemoteSegment(void *ddrPtr, union hccp_ip_addr ipv4Addr, struct P2PSegmentHandle &segmentHandle)
{
    if (status != RdmaDevStatus::RDEV_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma dev is not yet initialized");
    }

    RemoteRegionKey key = { ipv4Addr.addr.s_addr, ddrPtr };

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

        // Verify region found is the correct region
        if (candidate.ipAddr.addr.s_addr != ipv4Addr.addr.s_addr || ddrPtr < candidate.ddrPtr
            || ddrPtr >= (char *)candidate.ddrPtr + candidate.size) {
            std::cerr << "tried to get ddrPtr 2 " << ddrPtr << std::endl;
            std::cerr << "candidate.ipAddr.addr.s_addr " << candidate.ipAddr.addr.s_addr << "!= ipv4Addr.addr.s_addr "
                      << ipv4Addr.addr.s_addr << std::endl;
            std::cerr << "ddrPtr " << ddrPtr << "< candidate.ddrPtr " << candidate.ddrPtr << std::endl;
            std::cerr << "ddrPtr " << ddrPtr << ">= (char*)candidate.ddrPtr + candidate.size) "
                      << (char *)candidate.ddrPtr + candidate.size << std::endl;

            return Status::Error(ErrorCode::NOT_SUPPORTED, "address not registered to rdma dev");
        }

        segmentHandle = candidate;
    }

    return Status::Success();
}

// Add mutex for registering mr etc. if necessary
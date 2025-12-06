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
#ifndef P2P_RA_DEV_H
#define P2P_RA_DEV_H
#include <stddef.h>
#include <array>
#include <cstring>
#include <memory>
#include <mutex>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include <arpa/inet.h>
#include "external/ra.h"
#include "npu/RdmaAgent.h"
#include "tools/common.h"
#include <unordered_map>
#include <map>
#include "p2p.h"

enum RdmaDevStatus { RDEV_INITIALIZED, RDEV_UNINITIALIZED };

struct P2PSegmentHandle {
    uint64_t size;
    void *devPtr;
    void *ddrPtr;
    uint32_t rKey;
    union hccp_ip_addr ipAddr;  // uint32_t
};

struct P2PLocalSegment {
    uint64_t size;
    void *devPtr;
    void *mrHandle;
    void *ddrPtr;
    uint32_t rKey;
};

Status p2pSegmentPermissionsToFlag(P2pSegmentPermissions permissions, int &flag);

constexpr uint32_t DEFAULT_INIT_RDMA_CONFIG = 0;

class RdmaDev {
public:
    //                                ipV4Addr, ddrPtr
    using RemoteRegionKey = std::pair<uint32_t, void *>;
    static std::shared_ptr<RdmaDev> instances[MAX_LOCAL_DEVICES];
    static std::mutex instanceMutex;

    static Status GetInstance(uint32_t deviceId, std::shared_ptr<RdmaDev> &outDev);

    static void cleanup()
    {
        std::lock_guard<std::mutex> lock(instanceMutex);
        for (int i = 0; i < MAX_LOCAL_DEVICES; ++i) {
            instances[i].reset();
        }
    }

    explicit RdmaDev(uint32_t phyId, union hccp_ip_addr ipv4Addr);
    ~RdmaDev();

    RdmaDev(const RdmaDev &) = delete;
    RdmaDev &operator=(const RdmaDev &) = delete;

    Status init();
    Status getRdmaHandle(void **rdmaHandle);
    Status getIpv4(union hccp_ip_addr *ipv4Addr);
    Status registerGlobalMemoryRegion(void *ddrPtr, void *devPtr, uint64_t size, int access);
    Status unRegisterGlobalMemoryRegion(void *addr);
    Status getSegmentHandle(void *addr, struct P2PSegmentHandle &segmentHandle);
    Status addRemoteSegment(struct P2PSegmentHandle segmentHandle);
    Status getRemoteSegment(void *ddrPtr, union hccp_ip_addr ipv4Addr, struct P2PSegmentHandle &segmentHandle);

private:
    RdmaDevStatus status;
    uint32_t phyId;
    union hccp_ip_addr ipv4Addr;
    // Maps address to mr_handle

    std::mutex mrMut;
    std::unordered_map<void *, P2PLocalSegment> registeredMrs;
    std::mutex remoteMrMut;
    std::map<RemoteRegionKey, P2PSegmentHandle> remoteMrs;

    void *rdmaHandle;
    struct rdev roceDevInfo {};
};

#endif  // P2P_RA_DEV_H
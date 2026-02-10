/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RA_H
#define P2P_RA_H
#include <stddef.h>
#include <array>
#include <cstring>
#include <memory>
#include <mutex>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include <arpa/inet.h>
#include "tools/common.h"
#include "version/hccl_version.h"

#if HCCL_VERSION_NUM >= ((8 * 10000000) + (5 * 100000))
enum class NICDeployment {
    NIC_DEPLOYMENT_HOST = 0,
    NIC_DEPLOYMENT_DEVICE,
    NIC_DEPLOYMENT_RESERVED
};
#else
#include "hccl/hccl_common.h"
#endif

constexpr uint32_t DEFAULT_HDC_TYPE = 6;

enum RdmaAgentStatus { RA_INITIALIZED, RA_UNINITIALIZED };

class RdmaAgent {
public:
    static std::shared_ptr<RdmaAgent> instances[MAX_LOCAL_DEVICES];
    static std::mutex instanceMutex;

    static Status GetInstance(uint32_t deviceId, std::shared_ptr<RdmaAgent> &outAgent);

    static void cleanup()
    {
        std::lock_guard<std::mutex> lock(instanceMutex);
        for (int i = 0; i < MAX_LOCAL_DEVICES; ++i) {
            instances[i].reset();
        }
    }

    explicit RdmaAgent(uint32_t devId, uint32_t phyId);
    ~RdmaAgent();

    RdmaAgent(const RdmaAgent &) = delete;
    RdmaAgent &operator=(const RdmaAgent &) = delete;

    Status init();
    Status getDeviceIpv4(union hccp_ip_addr *addr);

private:
    RdmaAgentStatus status;
    uint32_t phyId;
    uint32_t devId;
    NICDeployment nicDeployment;
};

#endif  // P2P_RA_H
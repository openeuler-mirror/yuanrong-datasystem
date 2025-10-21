/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_RA_H
#define P2P_RA_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"
#include <arpa/inet.h>
#include "external/ra.h"

constexpr uint32_t DEFAULT_HDC_TYPE = 6;

enum RdmaAgentStatus { RA_INITIALIZED, RA_UNINITIALIZED };

class RdmaAgent {
public:
    RdmaAgent(uint32_t phyId) : status(RdmaAgentStatus::RA_UNINITIALIZED), phyId(phyId)
    {
        initConfig.phy_id = phyId;
        initConfig.nic_position = static_cast<uint32_t>(NICDeployment::NIC_DEPLOYMENT_DEVICE);
        initConfig.hdc_type = DEFAULT_HDC_TYPE;
    }

    ~RdmaAgent();

    RdmaAgent(const RdmaAgent &) = delete;
    RdmaAgent &operator=(const RdmaAgent &) = delete;

    Status init();
    Status getDeviceIpv4(union hccp_ip_addr *addr);

private:
    RdmaAgentStatus status;
    uint32_t phyId;

    struct ra_init_config initConfig {};
};

#endif  // P2P_RA_H
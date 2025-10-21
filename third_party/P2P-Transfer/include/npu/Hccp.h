/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_HCCP_H
#define P2P_HCCP_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "../tools/Status.h"
#include "../tools/npu-error.h"
#include "acl/acl.h"

enum HccpStatus {
    HCCP_STARTED,
    HCCP_UNINITIALIZED
};

class Hccp {
public:
    Hccp(int32_t devId) : status(HccpStatus::HCCP_UNINITIALIZED), devId(devId)
    {
    }
    ~Hccp();

    Hccp(const Hccp &) = delete;
    Hccp &operator=(const Hccp &) = delete;

    Status start();
    Status stop();

private:
    HccpStatus status;
    int32_t devId;
};

#endif  // P2P_HCCP_H
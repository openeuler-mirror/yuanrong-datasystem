/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
#include "version/hccl_version.h"

#if HCCL_VERSION_NUM >= ((8 * 10000000) + (5 * 100000))
#ifndef HCCL_NETWORK_PUB_H
#define HCCL_NETWORK_PUB_H
#include "hccl/hccl_types.h"

typedef signed int s32;
typedef unsigned int u32;

using HcclNetDevCtx = void *;

struct HcclNetDevInfo {
    s32 devicePhyId;
    s32 deviceLogicalId;
    u32 superDeviceId;
    u32 rsvd;
};

enum class NicType {
    VNIC_TYPE = 0,
    DEVICE_NIC_TYPE,
    HOST_NIC_TYPE
};

HcclResult HcclNetInit(NICDeployment nicDeployment, s32 devicePhyId, s32 deviceLogicId,
    bool enableWhiteListFlag, bool hasBackup = false);
HcclResult HcclNetDeInit(NICDeployment nicDeployment, s32 devicePhyId, s32 deviceLogicId,
    bool hasBackup = false);

#endif  // HCCL_NETWORK_PUB_H

#else

#include "hccl/hccl_network_pub.h"

#endif  // HCCL_VERSION_NUM
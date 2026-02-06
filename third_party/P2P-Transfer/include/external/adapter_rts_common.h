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
#ifndef ADAPTER_RTS_COMMON_H
#define ADAPTER_RTS_COMMON_H
#ifdef __cplusplus
extern "C" {
#endif
typedef signed int s32;
typedef unsigned int u32;

HcclResult hrtGetDevicePhyIdByIndex(u32 deviceLogicId, u32 &devicePhyId, bool isFresh = false);
HcclResult hrtGetDeviceRefresh(s32 *deviceLogicId);

#ifdef __cplusplus
}
#endif
#endif
#else

#include "hccl/adapter_rts_common.h"

#endif  // ADAPTER_RTS_COMMON_H

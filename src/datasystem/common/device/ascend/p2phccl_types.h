/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

/**
 * Description: The AscendCL device context plugin.
 */

#ifndef DATASYSTEM_COMMON_DEVICE_P2PHCCL_TYPES_H
#define DATASYSTEM_COMMON_DEVICE_P2PHCCL_TYPES_H

#include <stdint.h>

namespace datasystem {
#ifdef __cplusplus
extern "C" {
#endif

const uint32_t P2P_ROOT_INFO_BYTES = 4108;  // 4108: root info length

/**
 * @brief handle to HCCL communicator
 */
typedef void *P2PComm;

typedef enum P2pKind {
    P2P_RECEIVER,
    P2P_SENDER,
    P2P_BIDIRECTIONAL  // Currently unsuported
} P2pKind;

typedef enum P2pLink {
    P2P_LINK_HCCS,
    P2P_LINK_ROCE,
} P2pLink;

}  // namespace datasystem
#ifdef __cplusplus
};
#endif
#endif

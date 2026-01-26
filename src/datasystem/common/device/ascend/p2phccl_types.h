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
#include <unordered_map>
#include "datasystem/common/device/device_manager_base.h"
#include "datasystem/common/device/ascend/cann_types.h"

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
    P2P_LINK_AUTO,  // Uses the host IP (environment variable / detected) to determine whether 2 NPUs are on the same
                    // machine
} P2pLink;

typedef enum P2pSegmentPermissions {
    P2P_SEGMENT_READ_WRITE = 0,
    P2P_SEGMETN_READ_ONLY,
    P2P_SEGMENT_WRITE_ONLY,
} P2pSegmentPermissions;

const uint32_t P2P_SEGMENT_INFO_BYTES = 48;
/**
 * @brief P2P Segment Info
 */
typedef struct P2pSegmentInfoDef {
    char internal[P2P_SEGMENT_INFO_BYTES];
} P2pSegmentInfo;

/**
 * @brief P2P Scatter Entry (NPU specific, uses HcclDataType)
 */
typedef struct P2pScatterEntry {
    void *ddrBuf;
    void **dstBufs;
    uint64_t *counts;
    HcclDataType dataType;
    uint32_t numEl;
} P2pScatterEntry;

#ifdef __cplusplus
};
#endif
}  // namespace datasystem

// ==================== Type Conversion Utilities ====================

namespace datasystem {

/**
 * @brief Convert P2pKindBase to P2pKind using static map
 * @param[in] kind Source P2pKindBase value
 * @param[out] result Reference to store the converted P2pKind
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if kind not in map
 */
inline Status ToP2pKind(P2pKindBase kind, P2pKind &result)
{
    static const std::unordered_map<int, P2pKind> mapping = {
        {static_cast<int>(P2pKindBase::RECEIVER), P2P_RECEIVER},
        {static_cast<int>(P2pKindBase::SENDER), P2P_SENDER},
        {static_cast<int>(P2pKindBase::BIDIRECTIONAL), P2P_BIDIRECTIONAL},
    };
    
    auto it = mapping.find(static_cast<int>(kind));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "P2pKindBase not supported for P2pKind conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert P2pLinkBase to P2pLink using static map
 * @param[in] link Source P2pLinkBase value
 * @param[out] result Reference to store the converted P2pLink
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if link not in map
 */
inline Status ToP2pLink(P2pLinkBase link, P2pLink &result)
{
    static const std::unordered_map<int, P2pLink> mapping = {
        {static_cast<int>(P2pLinkBase::HCCS), P2P_LINK_HCCS},
        {static_cast<int>(P2pLinkBase::ROCE), P2P_LINK_ROCE},
        {static_cast<int>(P2pLinkBase::AUTO), P2P_LINK_AUTO},
    };
    
    auto it = mapping.find(static_cast<int>(link));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "P2pLinkBase not supported for P2pLink conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert P2pSegmentPermBase to P2pSegmentPermissions using static map
 * @param[in] perm Source P2pSegmentPermBase value
 * @param[out] result Reference to store the converted P2pSegmentPermissions
 * @return Status::OK() if conversion succeeded, K_NOT_SUPPORTED if perm not in map
 */
inline Status ToP2pSegmentPermissions(P2pSegmentPermBase perm, P2pSegmentPermissions &result)
{
    static const std::unordered_map<int, P2pSegmentPermissions> mapping = {
        {static_cast<int>(P2pSegmentPermBase::READ_WRITE), P2P_SEGMENT_READ_WRITE},
        {static_cast<int>(P2pSegmentPermBase::READ_ONLY), P2P_SEGMETN_READ_ONLY},
        {static_cast<int>(P2pSegmentPermBase::WRITE_ONLY), P2P_SEGMENT_WRITE_ONLY},
    };
    
    auto it = mapping.find(static_cast<int>(perm));
    if (it == mapping.end()) {
        return Status(StatusCode::K_NOT_SUPPORTED,
            "P2pSegmentPermBase not supported for P2pSegmentPermissions conversion");
    }
    result = it->second;
    return Status::OK();
}

/**
 * @brief Convert P2pSegmentBase* to P2pSegmentInfo*
 * @note Both structures have compatible memory layout
 * @param[in] segment Source P2pSegmentBase pointer
 * @param[out] result Reference to store the converted P2pSegmentInfo pointer
 * @return Status::OK() if conversion succeeded, K_INVALID if segment is null
 */
inline Status ToP2pSegmentInfo(P2pSegmentBase *segment, P2pSegmentInfo *&result)
{
    if (sizeof(P2pSegmentBase) != sizeof(P2pSegmentInfo)) {
        return Status(StatusCode::K_INVALID, "P2pSegmentBase size must the same as P2pSegmentInfo");
    }
    result = reinterpret_cast<P2pSegmentInfo*>(segment);
    return Status::OK();
}

/**
 * @brief Convert const P2pSegmentBase* to const P2pSegmentInfo*
 * @note Both structures have compatible memory layout
 * @param[in] segment Source const P2pSegmentBase pointer
 * @param[out] result Reference to store the converted const P2pSegmentInfo pointer
 * @return Status::OK() if conversion succeeded, K_INVALID if segment is null
 */
inline Status ToP2pSegmentInfo(const P2pSegmentBase *segment, const P2pSegmentInfo *&result)
{
    if (sizeof(P2pSegmentBase) != sizeof(P2pSegmentInfo)) {
        return Status(StatusCode::K_INVALID, "P2pSegmentBase size must the same as P2pSegmentInfo");
    }
    result = reinterpret_cast<const P2pSegmentInfo*>(segment);
    return Status::OK();
}

/**
 * @brief Convert P2pSegmentBase to P2pSegmentInfo
 * @param[in] segment Source P2pSegmentBase value
 * @param[out] result Reference to store the converted P2pSegmentInfo value
 * @return Status::OK() if conversion succeeded
 */
inline Status ToP2pSegmentInfo(const P2pSegmentBase &segment, P2pSegmentInfo &result)
{
    if (sizeof(P2pSegmentBase) != sizeof(P2pSegmentInfo)) {
        return Status(StatusCode::K_INVALID, "P2pSegmentBase size must the same as P2pSegmentInfo");
    }
    std::copy(
        reinterpret_cast<const uint8_t*>(&segment),
        reinterpret_cast<const uint8_t*>(&segment) + sizeof(segment),
        reinterpret_cast<uint8_t*>(&result)
    );
    return Status::OK();
}

/**
 * @brief Convert P2pScatterBase to P2pScatterEntry (requires HcclDataType conversion)
 */
inline Status ToP2pScatterEntry(const P2pScatterBase &scatter, P2pScatterEntry &entry)
{
    entry.ddrBuf = scatter.srcBuf;
    entry.dstBufs = scatter.dstBufs;
    entry.counts = scatter.counts;
    entry.numEl = scatter.numEntries;
    return ToHcclDataType(scatter.dataType, entry.dataType);
}

}  // namespace datasystem

#endif
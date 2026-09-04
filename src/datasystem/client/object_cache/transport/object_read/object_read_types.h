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

/** Description: Defines routed object-read requests and owned results. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_OBJECT_READ_TYPES_H
#define DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_OBJECT_READ_TYPES_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "datasystem/client/object_cache/transport/data_plane/i_data_transporter.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
inline constexpr char STALE_TRANSPORT_SNAPSHOT_MESSAGE[] =
    "Worker endpoint is absent from latest transport snapshot";
inline constexpr char WORKER_DRAINING_FOR_SCALE_IN_MESSAGE[] = "Worker is draining for ScaleIn";

inline bool IsTransportSnapshotStaleLocation(const Status &status)
{
    return status.GetCode() == K_NOT_READY
           && status.GetMsg().find(STALE_TRANSPORT_SNAPSHOT_MESSAGE) != std::string::npos;
}

inline bool IsWorkerDrainingForScaleIn(const Status &status)
{
    return status.GetCode() == K_NOT_READY
           && status.GetMsg().find(WORKER_DRAINING_FOR_SCALE_IN_MESSAGE) != std::string::npos;
}

inline int64_t SelectLocationRefreshBackoffMs(bool draining, uint8_t retryCount, int64_t currentBackoffMs)
{
    constexpr int64_t immediateRetryBackoffMs = 0;
    return !draining && retryCount == 0 ? immediateRetryBackoffMs : currentBackoffMs;
}

/** @brief Merge two transport kinds by enum priority (SHM < UB < TCP), mirroring access-log semantics. */
inline AccessTransportKind MergeTransportKind(AccessTransportKind lhs, AccessTransportKind rhs)
{
    return static_cast<AccessTransportKind>(std::max<uint8_t>(static_cast<uint8_t>(lhs),
                                                              static_cast<uint8_t>(rhs)));
}

/** @brief One object and metadata owner supplied by the routing layer. */
struct ObjectReadItem {
    size_t requestIndex = 0;
    std::string objectKey;
    HostPort metaOwner;
};

/** @brief Logical object-read request accepted by TransportLayer. */
struct ObjectReadRequest {
    std::vector<ObjectReadItem> items;
    std::shared_ptr<const TransportReadContext> context;
    /** @brief Whether this request records client latency phases. */
    bool traceEnabled = false;
};

/** @brief Data result and independent status for one input item. */
struct ObjectReadItemResult {
    size_t requestIndex = 0;
    std::string objectKey;
    Status status = Status(K_NOT_READY, "Object data is not read");
    DataGetResult data;
    /** @brief Highest medium actually attempted for this item, including admission-denied sources. */
    AccessTransportKind attemptedKind = AccessTransportKind::SHM;
};

/** @brief Object read results retained in request order. */
struct ObjectReadResult {
    void Clear()
    {
        items.clear();
        actualKind = AccessTransportKind::SHM;
    }

    std::vector<ObjectReadItemResult> items;
    AccessTransportKind actualKind = AccessTransportKind::SHM;
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_OBJECT_READ_OBJECT_READ_TYPES_H

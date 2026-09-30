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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_GET_HASH_RING_RESPONSE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_GET_HASH_RING_RESPONSE_H

#include <cstdint>
#include <string>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem::object_cache {

/**
 * @brief Build the versioned GetHashRing response from one immutable topology snapshot.
 * @param[in] snapshot Current topology snapshot.
 * @param[in] requestedVersion SDK-side cached topology version; zero requests a full response.
 * @param[in] masterAddress Current master address.
 * @param[out] rsp GetHashRing response. All existing fields are cleared before the response is populated.
 * @return K_NOT_READY until membership host IDs are known; otherwise K_OK or the topology conversion error.
 */
Status BuildGetHashRingResponse(const cluster::TopologySnapshot &snapshot, uint64_t requestedVersion,
                                const std::string &masterAddress, GetHashRingRspPb &rsp,
                                const std::string &requestedHostIdsDigest = "");

}  // namespace datasystem::object_cache

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_GET_HASH_RING_RESPONSE_H

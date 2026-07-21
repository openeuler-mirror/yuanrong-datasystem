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
#include <functional>
#include <string>

#include <google/protobuf/map.h>

#include "datasystem/cluster/model/topology_snapshot.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem::object_cache {

using RoutingHostIdMap = google::protobuf::Map<std::string, std::string>;
using RoutingHostIdLoader = std::function<Status(RoutingHostIdMap &)>;

/**
 * @brief Build the versioned GetHashRing response from one immutable topology snapshot.
 * @param[in] snapshot Current topology snapshot.
 * @param[in] requestedVersion SDK-side cached topology version; zero requests a full response.
 * @param[in] masterAddress Current master address.
 * @param[in] loadHostIds Loads worker address to host ID mappings only for a full response.
 * @param[out] rsp GetHashRing response. All existing fields are cleared before the response is populated.
 * @return K_OK or the topology/host ID conversion error.
 */
Status BuildGetHashRingResponse(const cluster::TopologySnapshot &snapshot, uint64_t requestedVersion,
                                const std::string &masterAddress, const RoutingHostIdLoader &loadHostIds,
                                GetHashRingRspPb &rsp);

}  // namespace datasystem::object_cache

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_GET_HASH_RING_RESPONSE_H

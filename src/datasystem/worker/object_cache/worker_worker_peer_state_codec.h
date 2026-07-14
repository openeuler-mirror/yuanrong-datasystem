/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Convert worker topology peer state to and from worker RPC protobufs.
 */
#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_PEER_STATE_CODEC_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_PEER_STATE_CODEC_H

#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/cluster/runtime/control_backend_state.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

/**
 * @brief Encode fresh identity-bound control-backend evidence into the existing peer RPC response.
 * @param[in] observation Fresh complete local observation.
 * @param[out] rsp Response unchanged when validation fails.
 * @return K_OK, K_INVALID for malformed evidence, or K_NOT_READY for stale/unknown evidence.
 */
Status FillGetClusterStateRspPbFromControlBackendObservation(const cluster::ControlBackendObservation &observation,
                                                             GetClusterStateRspPb &rsp);

/**
 * @brief Decode and validate identity-bound control-backend evidence from the existing peer RPC response.
 * @param[in] peerAddress Canonical address of the peer that returned the response.
 * @param[in] rsp Peer response to validate.
 * @param[out] observation Observation unchanged when validation fails.
 * @return K_OK, K_INVALID for malformed evidence, or K_NOT_READY when the peer is not ready.
 */
Status FillControlBackendObservationFromGetClusterStateRspPb(const std::string &peerAddress,
                                                             const GetClusterStateRspPb &rsp,
                                                             cluster::ControlBackendObservation &observation);
}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_WORKER_PEER_STATE_CODEC_H

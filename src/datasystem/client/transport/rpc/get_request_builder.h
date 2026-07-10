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

/** Description: Defines common Get request construction and response status handling for data transporters. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_GET_REQUEST_BUILDER_H
#define DATASYSTEM_CLIENT_TRANSPORT_GET_REQUEST_BUILDER_H

#include <cstddef>
#include <vector>

#include "datasystem/client/transport/data_plane/i_data_transporter.h"

namespace datasystem {
namespace client {

/**
 * @brief Validate the logical fields shared by TCP and UB Get requests.
 * @param[in] input Logical Get request.
 * @return K_OK when the request is valid; K_INVALID otherwise.
 */
Status ValidateGetRequest(const TransportGetRequest &input);

/**
 * @brief Build common Get fields; authentication and signatures are intentionally deferred until invocation.
 * @param[in] input Logical Get request.
 * @param[in] requestIndices Indices into input.objectKeys; empty means all objects.
 * @param[out] request Get request without authentication fields.
 * @return K_OK on success; the error code otherwise.
 */
Status BuildGetRequest(const TransportGetRequest &input, const std::vector<size_t> &requestIndices,
                       GetReqPb &request);

/**
 * @brief Convert a worker Get response to the transport-level status used by the existing object-client Get flow.
 * @param[in] response Worker Get response.
 * @param[in] kind Transport used for this response.
 * @return A retry/rebuild error when required; K_OK when business status remains in response.last_rc().
 */
Status GetTransportResponseStatus(const GetRspPb &response, AccessTransportKind kind);

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_GET_REQUEST_BUILDER_H

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

/** Description: Defines Exist request construction for data transport. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_EXIST_REQUEST_BUILDER_H
#define DATASYSTEM_CLIENT_TRANSPORT_EXIST_REQUEST_BUILDER_H

#include "datasystem/client/transport/data_plane/i_data_transporter.h"

namespace datasystem {
namespace client {

/**
 * @brief Validate the logical fields of an Exist request.
 * @param[in] input Logical Exist request.
 * @return K_OK when the request is valid; K_INVALID otherwise.
 */
Status ValidateExistRequest(const TransportExistRequest &input);

/**
 * @brief Build Exist fields; authentication and signatures are intentionally deferred until invocation.
 * @param[in] input Logical Exist request.
 * @param[out] request Exist request without authentication fields.
 * @return K_OK on success; the error code otherwise.
 */
Status BuildExistRequest(const TransportExistRequest &input, ExistReqPb &request);

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_EXIST_REQUEST_BUILDER_H

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

/** Description: Defines the data-plane connection lifecycle interface. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_I_DATA_PLANE_CONNECTION_H
#define DATASYSTEM_CLIENT_TRANSPORT_I_DATA_PLANE_CONNECTION_H

#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {
class IDataPlaneConnection {
public:
    virtual ~IDataPlaneConnection() = default;

    /**
     * @brief Establish the data-plane connection to the worker.
     * @param[in] workerAddr Target worker address.
     * @return K_OK on success; K_URMA_CONNECT_FAILED or K_RPC_UNAVAILABLE on failure.
     */
    virtual Status Establish(const HostPort &workerAddr) = 0;

    /** @brief Whether the connection is usable right now. */
    virtual bool IsAlive() const = 0;

    /** @return The transport kind (SHM / UB / TCP). */
    virtual AccessTransportKind Kind() const = 0;

    /** @brief Tear down the QP / fd / channel. */
    virtual void Teardown() = 0;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_I_DATA_PLANE_CONNECTION_H

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

/** Description: Defines endpoint data-plane execution and scoped connection rebuild. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_DATA_PLANE_EXECUTOR_H
#define DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_DATA_PLANE_EXECUTOR_H

#include <functional>
#include <memory>

#include "datasystem/client/transport/data_plane/data_plane_manager.h"
#include "datasystem/client/transport/transport_advisor.h"

namespace datasystem {
namespace client {
class DataPlaneExecutor {
public:
    using Operation = std::function<Status(IDataTransporter &)>;

    DataPlaneExecutor(std::shared_ptr<DataPlaneManager> manager, std::shared_ptr<TransportAdvisor> advisor);

    ~DataPlaneExecutor() = default;

    /**
     * @brief Execute one endpoint operation and rebuild the affected connection once when required.
     * @param[in] workerAddr Target data-worker address.
     * @param[in] operation Operation invoked on the endpoint-scoped transporter.
     * @return K_OK on success; the error code otherwise.
     */
    Status Execute(const HostPort &workerAddr, const Operation &operation);

private:
    std::shared_ptr<DataPlaneManager> manager_;
    std::shared_ptr<TransportAdvisor> advisor_;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_DATA_PLANE_DATA_PLANE_EXECUTOR_H

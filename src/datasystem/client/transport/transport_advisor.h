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

/** Description: Defines the client transport selection policy. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H
#define DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H

#include "datasystem/client/transport/transport_kind.h"
#include "datasystem/common/util/net_util.h"

namespace datasystem {
namespace client {
class TransportAdvisor {
public:
    TransportAdvisor() = default;
    virtual ~TransportAdvisor() = default;

    /**
     * @brief Suggest a transport hint for the target worker.
     * @param[in] workerAddr Target worker address.
     * @return The suggested TransportHint.
     */
    virtual TransportHint GetTransportHint(const HostPort &workerAddr) const;
};
}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_TRANSPORT_ADVISOR_H

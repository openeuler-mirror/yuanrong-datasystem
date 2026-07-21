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
 * Description: Primitive coordination backend event model.
 */
#include "datasystem/cluster/coordination_backend/coordination_backend.h"

#include <sstream>

namespace datasystem::cluster {

std::string CoordinationEvent::ToString() const
{
    std::stringstream stream;
    const char *typeName = "UNSPECIFIED";
    if (type == CoordinationEventType::PUT) {
        typeName = "PUT";
    } else if (type == CoordinationEventType::DELETE) {
        typeName = "DELETE";
    } else if (type == CoordinationEventType::RESET) {
        typeName = "RESET";
    }
    stream << "type: " << typeName << ", key: " << key << ", version: " << version << ", revision: " << revision;
    return stream.str();
}

void ICoordinationBackend::SetMembershipReadyHandler(const MembershipReadyHandler &handler)
{
    (void)handler;
}

bool ICoordinationBackend::OwnsWatchIdentity(const std::string &coordinatorId, int64_t watchId) const
{
    (void)coordinatorId;
    (void)watchId;
    return false;
}

bool ICoordinationBackend::IsWatchRegistrationInProgress() const
{
    return false;
}

void ICoordinationBackend::InvalidateWatches()
{
}

void ICoordinationBackend::HandleWatchEvent(const std::string &coordinatorId, int64_t watchId,
                                            CoordinationEvent &&event)
{
    (void)coordinatorId;
    (void)watchId;
    (void)event;
}

}  // namespace datasystem::cluster

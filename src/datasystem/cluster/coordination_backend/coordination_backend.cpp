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
    const char *typeName = type == CoordinationEventType::PUT ? "PUT" : "DELETE";
    stream << "type: " << typeName << ", key: " << key << ", version: " << version << ", revision: " << revision;
    return stream.str();
}

}  // namespace datasystem::cluster

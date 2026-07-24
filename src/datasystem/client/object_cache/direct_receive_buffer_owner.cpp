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

#include "datasystem/client/object_cache/direct_receive_buffer_owner.h"

#if defined(BUILD_PIPLN_H2D) && defined(USE_URMA)
#include <utility>

namespace datasystem {

DirectReceiveBufferOwner::DirectReceiveBufferOwner(std::shared_ptr<UrmaManager::BufferHandle> receiveHandle,
                                                   std::shared_ptr<UrmaManager::BufferHandle> fallbackHandle)
    : receiveHandle_(std::move(receiveHandle)), fallbackHandle_(std::move(fallbackHandle))
{
}

}  // namespace datasystem
#endif

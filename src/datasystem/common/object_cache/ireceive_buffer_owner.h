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

/** Description: Defines ownership hooks for transport-backed receive buffers. */
#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_IRECEIVE_BUFFER_OWNER_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_IRECEIVE_BUFFER_OWNER_H

#include "datasystem/utils/status.h"

namespace datasystem {
namespace client {

/**
 * @brief Owns transport-specific state backing a zero-copy receive buffer.
 *
 * Release is explicit so Buffer::Release can retire worker-side references before
 * dropping the final shared_ptr. Implementations must make Release idempotent.
 */
class IReceiveBufferOwner {
public:
    virtual ~IReceiveBufferOwner() = default;

    virtual void Release()
    {
    }

    virtual bool ManagesWorkerReference() const
    {
        return false;
    }

    virtual Status CheckAlive() const
    {
        return Status::OK();
    }
};

}  // namespace client
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_OBJECT_CACHE_IRECEIVE_BUFFER_OWNER_H

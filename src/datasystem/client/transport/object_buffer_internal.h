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

/** Description: Provides strongly typed transport access to ObjectBuffer state. */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_OBJECT_BUFFER_INTERNAL_H
#define DATASYSTEM_CLIENT_TRANSPORT_OBJECT_BUFFER_INTERNAL_H

#include <memory>
#include <utility>

#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/object/object_buffer.h"

namespace datasystem {

class ObjectBufferInternal {
public:
    static Status Create(std::shared_ptr<ObjectBufferInfo> info, std::shared_ptr<ObjectBuffer> &out)
    {
        std::shared_ptr<void> state = std::move(info);
        return ObjectBuffer::Create(std::move(state), out);
    }

    static const ObjectBufferInfo &GetInfo(const ObjectBuffer &buffer)
    {
        return *static_cast<const ObjectBufferInfo *>(buffer.state_.get());
    }

    static ObjectBufferInfo &GetMutableInfo(ObjectBuffer &buffer)
    {
        return *static_cast<ObjectBufferInfo *>(buffer.state_.get());
    }
};

}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_OBJECT_BUFFER_INTERNAL_H

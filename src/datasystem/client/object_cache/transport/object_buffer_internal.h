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

    // Take the ObjectBufferInfo out of an ObjectBuffer's erased state_, transferring sole
    // ownership to the caller. Used by the two-step Create path to hand the routed info
    // (populated by ShmTransporter::Create: workerAddr/shmId/pointer/mmapEntry/
    // sessionLockId/receiveBufferOwner) to a legacy Buffer via Buffer::CreateBuffer.
    // After this the ObjectBuffer's state_ is empty; its destructor is null-safe
    // (it guards state_ != nullptr) so it will not free the payload.
    static std::shared_ptr<ObjectBufferInfo> ExtractInfo(std::shared_ptr<ObjectBuffer> &buf)
    {
        if (buf == nullptr) {
            return nullptr;
        }
        // std::static_pointer_cast has no rvalue overload before C++20, so passing std::move(state_)
        // would still COPY and leave the source ObjectBuffer owning the info. Its destructor would then
        // free a malloc'd payload (TCP-fallback, ownsLocalMemory_=true) that the legacy Buffer now owns,
        // nulling the Buffer's pointer. Explicitly release state_ to transfer ownership cleanly.
        auto info = std::static_pointer_cast<ObjectBufferInfo>(buf->state_);
        buf->state_.reset();
        return info;
    }

    // Clear the ObjectBuffer's claim on the payload pointer so its destructor will not free it.
    // Used by the two-step Publish path: a transient ObjectBuffer is reconstructed from an
    // ObjectBufferInfo that a legacy Buffer already owns; without this, ObjectBuffer::Init
    // would set ownsLocalMemory_ for the TCP-fallback shape and the transient's destructor would
    // free (and null) the shared pointer prematurely. No-op for SHM
    // buffers, whose ownsLocalMemory_ is never set.
    static void DisownLocalMemory(ObjectBuffer &buffer)
    {
        buffer.ownsLocalMemory_ = false;
    }
};

}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_TRANSPORT_OBJECT_BUFFER_INTERNAL_H

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

/** Description: Defines ObjectBuffer — a lightweight, transport-layer-owned object data carrier (Buffer v2). */
#ifndef DATASYSTEM_OBJECT_OBJECT_BUFFER_H
#define DATASYSTEM_OBJECT_OBJECT_BUFFER_H

#include <cstdint>
#include <memory>

#include "datasystem/utils/status.h"

namespace datasystem {
class ObjectBuffer {
public:
    ~ObjectBuffer();
    ObjectBuffer(ObjectBuffer &&) noexcept;
    ObjectBuffer &operator=(ObjectBuffer &&) noexcept;
    ObjectBuffer(const ObjectBuffer &) = delete;
    ObjectBuffer &operator=(const ObjectBuffer &) = delete;

    /// \brief Create and initialize an ObjectBuffer from opaque transport state.
    /// \param[in] state State whose dynamic object type must be ObjectBufferInfo.
    /// \param[out] out Initialized ObjectBuffer.
    /// \return K_OK on success; the error code otherwise.
    static Status Create(std::shared_ptr<void> state, std::shared_ptr<ObjectBuffer> &out);

    /// \brief Write user data to the buffer. Pure memcpy; no eager URMA.
    /// \param[in] data Source data pointer.
    /// \param[in] length Number of bytes to copy.
    /// \return Status of the result.
    Status MemoryCopy(const void *data, uint64_t length);

    /// \brief Get a mutable pointer to the data payload.
    void *MutableData();
    /// \brief Get an immutable pointer to the data payload.
    const void *ImmutableData() const;
    /// \brief Get the data capacity in bytes.
    int64_t GetSize() const;

    /// \brief Acquire exclusive write lock on the buffer.
    Status WLatch(uint64_t timeoutSec = 60);
    /// \brief Acquire shared read lock on the buffer.
    Status RLatch(uint64_t timeoutSec = 60);
    /// \brief Release the shared read lock.
    Status UnRLatch();
    /// \brief Release the exclusive write lock.
    Status UnWLatch();

protected:
    explicit ObjectBuffer(std::shared_ptr<void> state);

private:
    friend class ObjectBufferInternal;

    Status Init();               // Lock-type decision (ref: buffer.cpp:53-101, without CheckDeprecated)
    Status MallocBufferHelper(); // Plain malloc (ref: buffer.cpp:103-114)

    std::shared_ptr<void> state_;
    std::shared_ptr<void> latch_;
    bool isShm_ = false;
};

}  // namespace datasystem

#endif  // DATASYSTEM_OBJECT_OBJECT_BUFFER_H

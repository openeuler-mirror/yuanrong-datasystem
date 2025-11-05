/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

/**
 * Description: This file is used to read data in the server.
 */

#ifndef DATASYSTEM_KV_READ_ONLY_BUFFER_H
#define DATASYSTEM_KV_READ_ONLY_BUFFER_H

#include <memory>
#include <vector>

#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class KVClient;
class __attribute ((visibility ("default"))) ReadOnlyBuffer {
public:
    ReadOnlyBuffer() = default;

    ~ReadOnlyBuffer() = default;

    /**
     * @brief Get the data size of the buffer.
     * @return The data size of the buffer.
     */
    int64_t GetSize() const;

    /**
     * @brief Get a immutable data pointer.
     * @return A const void * to the data.
     */
    const void *ImmutableData();

    /**
     * @brief A Read lock is executed on the memory to protect the memory from concurrent writes (allow concurrent
     * reads).
     * @param[in] timeout Try-lock timeout, default value is 60 seconds.
     * @return Status of the result.
     */
    Status RLatch(uint64_t timeout = 60 /* default is 60s */);

    /**
     * @brief Unlock the read latch on memory.
     * @return Status of the result.
     */
    Status UnRLatch();

private:
    friend KVClient;

    explicit ReadOnlyBuffer(std::shared_ptr<Buffer> &buffer)
    {
        buffer_ = buffer;
    }

    std::shared_ptr<Buffer> buffer_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_KV_CACHE_READ_ONLY_BUFFER_H
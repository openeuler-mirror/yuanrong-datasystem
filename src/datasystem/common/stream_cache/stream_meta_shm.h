/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Record meta for a stream.
 */
#ifndef DATASYSTEM_COMMON_STREAM_CACHE_STREAM_META_SHM_H
#define DATASYSTEM_COMMON_STREAM_CACHE_STREAM_META_SHM_H

#include <cstdint>
#include <memory>
#include "datasystem/client/mmap/immap_table_entry.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class StreamMetaShm {
public:
    StreamMetaShm(std::string streamName, void *shmPtr, size_t shmSz, uint64_t maxStreamSize)
        : streamName_(std::move(streamName)),
          shmPtr_(reinterpret_cast<uint8_t *>((shmPtr))),
          shmSz_(shmSz),
          maxStreamSize_(maxStreamSize)
    {
    }

    Status Init(std::shared_ptr<client::IMmapTableEntry> mmapTableEntry = nullptr);

    /**
     * @brief Try to increase the usage of shared memory in this node for this stream.
     * @param[in] size The size to be increased.
     * @return Status of the call.
     */
    Status TryIncUsage(uint64_t size);

    /**
     * @brief Try to decrease the usage of shared memory in this node for this stream.
     * @param[in] size The size to be increased.
     * @return Status of the call.
     */
    Status TryDecUsage(uint64_t size);

private:
    const std::string streamName_;
    uint8_t *shmPtr_;
    const size_t shmSz_;
    uint64_t *usage_{ nullptr };
    std::shared_ptr<client::IMmapTableEntry> mmapTableEntry_;  // for client.
    uint64_t maxStreamSize_ = 0;
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_STREAM_CACHE_STREAM_META_SHM_H

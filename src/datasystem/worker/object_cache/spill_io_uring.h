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

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SPILL_IO_URING_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SPILL_IO_URING_H

#include <cstddef>
#include <cstdint>

#include <linux/io_uring.h>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

class SpillIoUring {
public:
    struct Completion {
        void *context = nullptr;
        int32_t result = 0;
    };

    SpillIoUring() = default;
    ~SpillIoUring();
    SpillIoUring(const SpillIoUring &) = delete;
    SpillIoUring &operator=(const SpillIoUring &) = delete;

    Status Init(uint32_t depth);
    void Shutdown();
    bool IsInitialized() const;
    uint32_t Depth() const;
    uint32_t PendingSubmissions() const;

    Status PrepareWrite(int fd, const void *buffer, uint32_t size, uint64_t offset, void *context);
    Status Submit();
    Status WaitCompletion(Completion &completion, int timeoutMs);

    static uint32_t WriteChunkSize(uint64_t remaining, uint32_t alignment);

private:
    Status MapRings(const io_uring_params &params);
    void BindRingOffsets(const io_uring_params &params);
    Status PrepareSqe(io_uring_sqe *&sqe);

    int ringFd_ = -1;
    uint32_t depth_ = 0;
    void *sqRing_ = nullptr;
    void *cqRing_ = nullptr;
    io_uring_sqe *sqes_ = nullptr;
    size_t sqRingSize_ = 0;
    size_t cqRingSize_ = 0;
    size_t sqesSize_ = 0;

    uint32_t *sqHead_ = nullptr;
    uint32_t *sqTail_ = nullptr;
    uint32_t *sqMask_ = nullptr;
    uint32_t *sqEntries_ = nullptr;
    uint32_t *sqArray_ = nullptr;
    uint32_t *cqHead_ = nullptr;
    uint32_t *cqTail_ = nullptr;
    uint32_t *cqMask_ = nullptr;
    io_uring_cqe *cqes_ = nullptr;

    uint32_t preparedHead_ = 0;
    uint32_t preparedTail_ = 0;
};

}  // namespace object_cache
}  // namespace datasystem
#endif

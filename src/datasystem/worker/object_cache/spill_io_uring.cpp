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

#include "datasystem/worker/object_cache/spill_io_uring.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <poll.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <linux/io_uring.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace object_cache {
namespace {
template <typename T>
T *RingOffset(uint8_t *base, uint32_t offset)
{
    return reinterpret_cast<T *>(base + offset);
}
}  // namespace

SpillIoUring::~SpillIoUring()
{
    Shutdown();
}

Status SpillIoUring::Init(uint32_t depth)
{
    INJECT_POINT("worker.Spill.IoUringInitError");
    CHECK_FAIL_RETURN_STATUS(ringFd_ < 0, K_RUNTIME_ERROR, "spill io_uring is already initialized");
    io_uring_params params{};
    ringFd_ = static_cast<int>(syscall(__NR_io_uring_setup, depth, &params));
    if (ringFd_ < 0) {
        return Status(K_RUNTIME_ERROR, FormatString("io_uring_setup(depth=%u) failed: %s", depth, StrErr(errno)));
    }

    Status rc = MapRings(params);
    if (rc.IsError()) {
        Shutdown();
        return rc;
    }
    BindRingOffsets(params);
    preparedHead_ = 0;
    preparedTail_ = 0;
    depth_ = params.sq_entries;
    return Status::OK();
}

Status SpillIoUring::MapRings(const io_uring_params &params)
{
    sqRingSize_ = params.sq_off.array + params.sq_entries * sizeof(uint32_t);
    cqRingSize_ = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0) {
        sqRingSize_ = std::max(sqRingSize_, cqRingSize_);
        cqRingSize_ = sqRingSize_;
    }
    sqRing_ = mmap(nullptr, sqRingSize_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_,
                   IORING_OFF_SQ_RING);
    if (sqRing_ == MAP_FAILED) {
        sqRing_ = nullptr;
        return Status(K_RUNTIME_ERROR, FormatString("mmap io_uring SQ failed: %s", StrErr(errno)));
    }
    if ((params.features & IORING_FEAT_SINGLE_MMAP) != 0) {
        cqRing_ = sqRing_;
    } else {
        cqRing_ = mmap(nullptr, cqRingSize_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_,
                       IORING_OFF_CQ_RING);
        if (cqRing_ == MAP_FAILED) {
            cqRing_ = nullptr;
            return Status(K_RUNTIME_ERROR, FormatString("mmap io_uring CQ failed: %s", StrErr(errno)));
        }
    }
    sqesSize_ = params.sq_entries * sizeof(io_uring_sqe);
    sqes_ = static_cast<io_uring_sqe *>(
        mmap(nullptr, sqesSize_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, ringFd_, IORING_OFF_SQES));
    if (sqes_ == MAP_FAILED) {
        sqes_ = nullptr;
        return Status(K_RUNTIME_ERROR, FormatString("mmap io_uring SQEs failed: %s", StrErr(errno)));
    }
    return Status::OK();
}

void SpillIoUring::BindRingOffsets(const io_uring_params &params)
{
    auto *sqRing = static_cast<uint8_t *>(sqRing_);
    auto *cqRing = static_cast<uint8_t *>(cqRing_);
    sqHead_ = RingOffset<uint32_t>(sqRing, params.sq_off.head);
    sqTail_ = RingOffset<uint32_t>(sqRing, params.sq_off.tail);
    sqMask_ = RingOffset<uint32_t>(sqRing, params.sq_off.ring_mask);
    sqEntries_ = RingOffset<uint32_t>(sqRing, params.sq_off.ring_entries);
    sqArray_ = RingOffset<uint32_t>(sqRing, params.sq_off.array);
    cqHead_ = RingOffset<uint32_t>(cqRing, params.cq_off.head);
    cqTail_ = RingOffset<uint32_t>(cqRing, params.cq_off.tail);
    cqMask_ = RingOffset<uint32_t>(cqRing, params.cq_off.ring_mask);
    cqes_ = RingOffset<io_uring_cqe>(cqRing, params.cq_off.cqes);
}

void SpillIoUring::Shutdown()
{
    if (sqes_ != nullptr) {
        (void)munmap(sqes_, sqesSize_);
    }
    if (cqRing_ != nullptr && cqRing_ != sqRing_) {
        (void)munmap(cqRing_, cqRingSize_);
    }
    if (sqRing_ != nullptr) {
        (void)munmap(sqRing_, sqRingSize_);
    }
    if (ringFd_ >= 0) {
        (void)close(ringFd_);
    }
    ringFd_ = -1;
    depth_ = 0;
    sqRing_ = nullptr;
    cqRing_ = nullptr;
    sqes_ = nullptr;
    sqHead_ = nullptr;
    sqTail_ = nullptr;
    sqMask_ = nullptr;
    sqEntries_ = nullptr;
    sqArray_ = nullptr;
    cqHead_ = nullptr;
    cqTail_ = nullptr;
    cqMask_ = nullptr;
    cqes_ = nullptr;
    preparedHead_ = 0;
    preparedTail_ = 0;
}

bool SpillIoUring::IsInitialized() const
{
    return ringFd_ >= 0;
}

uint32_t SpillIoUring::Depth() const
{
    return depth_;
}

uint32_t SpillIoUring::PendingSubmissions() const
{
    return preparedTail_ - preparedHead_;
}

Status SpillIoUring::PrepareSqe(io_uring_sqe *&sqe)
{
    CHECK_FAIL_RETURN_STATUS(IsInitialized(), K_RUNTIME_ERROR, "spill io_uring is not initialized");
    const uint32_t kernelHead = __atomic_load_n(sqHead_, __ATOMIC_ACQUIRE);
    CHECK_FAIL_RETURN_STATUS(preparedTail_ - kernelHead < *sqEntries_, K_TRY_AGAIN, "spill io_uring SQ is full");
    sqe = &sqes_[preparedTail_ & *sqMask_];
    std::memset(sqe, 0, sizeof(*sqe));
    ++preparedTail_;
    return Status::OK();
}

Status SpillIoUring::PrepareWrite(int fd, const void *buffer, uint32_t size, uint64_t offset, void *context)
{
    INJECT_POINT("worker.Spill.IoUringWriteError");
    io_uring_sqe *sqe = nullptr;
    RETURN_IF_NOT_OK(PrepareSqe(sqe));
    sqe->opcode = IORING_OP_WRITE;
    sqe->fd = fd;
    sqe->off = offset;
    sqe->addr = reinterpret_cast<uint64_t>(buffer);
    sqe->len = size;
    sqe->user_data = reinterpret_cast<uint64_t>(context);
    return Status::OK();
}

Status SpillIoUring::Submit()
{
    INJECT_POINT("worker.Spill.IoUringSubmitError");
    uint32_t remaining = preparedTail_ - preparedHead_;
    if (remaining == 0) {
        return Status::OK();
    }
    uint32_t kernelTail = __atomic_load_n(sqTail_, __ATOMIC_RELAXED);
    for (uint32_t index = preparedHead_; index != preparedTail_; ++index) {
        sqArray_[kernelTail++ & *sqMask_] = index & *sqMask_;
    }
    __atomic_store_n(sqTail_, kernelTail, __ATOMIC_RELEASE);
    preparedHead_ = preparedTail_;
    while (remaining > 0) {
        int submitted = static_cast<int>(syscall(__NR_io_uring_enter, ringFd_, remaining, 0, 0, nullptr, 0));
        if (submitted < 0 && errno == EINTR) {
            continue;
        }
        if (submitted < 0) {
            return Status(K_RUNTIME_ERROR, FormatString("io_uring_enter submit failed: %s", StrErr(errno)));
        }
        CHECK_FAIL_RETURN_STATUS(submitted > 0, K_RUNTIME_ERROR, "io_uring_enter submitted zero SQEs");
        remaining -= static_cast<uint32_t>(submitted);
    }
    return Status::OK();
}

uint32_t SpillIoUring::WriteChunkSize(uint64_t remaining, uint32_t alignment)
{
    if (remaining == 0 || alignment == 0) {
        return 0;
    }
    const auto maxResult = static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
    const uint64_t maxAlignedResult = maxResult - maxResult % alignment;
    return static_cast<uint32_t>(std::min(remaining, maxAlignedResult));
}

Status SpillIoUring::WaitCompletion(Completion &completion, int timeoutMs)
{
    INJECT_POINT("worker.Spill.IoUringWaitCompletionError");
    INJECT_POINT("worker.Spill.IoUringWaitCompletionTimeout");
    CHECK_FAIL_RETURN_STATUS(IsInitialized(), K_RUNTIME_ERROR, "spill io_uring is not initialized");
    while (__atomic_load_n(cqHead_, __ATOMIC_RELAXED) == __atomic_load_n(cqTail_, __ATOMIC_ACQUIRE)) {
        pollfd ringPoll{};
        ringPoll.fd = ringFd_;
        ringPoll.events = POLLIN;
        int rc = poll(&ringPoll, 1, timeoutMs);
        if (rc < 0 && errno == EINTR) {
            continue;
        }
        if (rc < 0) {
            return Status(K_RUNTIME_ERROR, FormatString("poll spill io_uring failed: %s", StrErr(errno)));
        }
        CHECK_FAIL_RETURN_STATUS(rc > 0, K_TRY_AGAIN, "Timed out waiting for spill io_uring completion");
        CHECK_FAIL_RETURN_STATUS((ringPoll.revents & (POLLERR | POLLHUP | POLLNVAL)) == 0, K_RUNTIME_ERROR,
                                 FormatString("spill io_uring poll failed, revents=%d", ringPoll.revents));
    }
    uint32_t head = __atomic_load_n(cqHead_, __ATOMIC_RELAXED);
    const io_uring_cqe &cqe = cqes_[head & *cqMask_];
    completion.context = reinterpret_cast<void *>(cqe.user_data);
    completion.result = cqe.res;
    INJECT_POINT_NO_RETURN("worker.Spill.IoUringCompletionResult",
                           [&completion](int64_t result) { completion.result = static_cast<int32_t>(result); });
    __atomic_store_n(cqHead_, head + 1, __ATOMIC_RELEASE);
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem

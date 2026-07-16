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

/**
 * Description: RAII wrapper for the shm fd-passing socket fd. Owns the fd: closes it on destruction
 * (RETRY_ON_EINTR), is move-only (no double-close / dangling copies), and supports release() to
 * transfer ownership. See shm-and-heartbeat design review fix #2.
 */
#ifndef DATASYSTEM_CLIENT_TRANSPORT_SHM_FD_H
#define DATASYSTEM_CLIENT_TRANSPORT_SHM_FD_H

#include <unistd.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace client {
constexpr int INVALID_SHM_FD = -1;  // Equivalent to INVALID_SOCKET_FD (-1); duplicated here to keep
                                    // shm_entry.h free of the heavier client_worker_common_api.h.

/**
 * @brief Move-only RAII owner of a shm fd-passing socket fd. Closing is idempotent: once released
 * or moved-from, the fd is INVALID_SHM_FD and destruction is a no-op.
 */
class ShmFd {
public:
    ShmFd() = default;
    explicit ShmFd(int fd) : fd_(fd) {}
    ~ShmFd()
    {
        Reset();
    }

    ShmFd(const ShmFd &) = delete;
    ShmFd &operator=(const ShmFd &) = delete;

    ShmFd(ShmFd &&other) noexcept : fd_(other.fd_)
    {
        other.fd_ = INVALID_SHM_FD;
    }
    ShmFd &operator=(ShmFd &&other) noexcept
    {
        if (this != &other) {
            Reset();
            fd_ = other.fd_;
            other.fd_ = INVALID_SHM_FD;
        }
        return *this;
    }

    int Get() const
    {
        return fd_;
    }

    bool IsValid() const
    {
        return fd_ != INVALID_SHM_FD;
    }

    /**
     * @brief Relinquish ownership of the fd without closing it. The caller becomes responsible for
     * the fd's lifetime.
     */
    int Release()
    {
        int fd = fd_;
        fd_ = INVALID_SHM_FD;
        return fd;
    }

    /**
     * @brief Close the current fd (if any) and adopt a new one.
     */
    void Reset(int fd = INVALID_SHM_FD)
    {
        if (fd_ != INVALID_SHM_FD) {
            RETRY_ON_EINTR(close(fd_));
        }
        fd_ = fd;
    }

private:
    int fd_ = INVALID_SHM_FD;
};
}  // namespace client
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_TRANSPORT_SHM_FD_H

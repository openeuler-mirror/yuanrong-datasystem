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
 * Description: Limit URMA fallback TCP payloads to avoid congesting the shared RPC channel.
 */

#ifndef DATASYSTEM_COMMON_OBJECT_CACHE_URMA_FALLBACK_TCP_LIMITER_H
#define DATASYSTEM_COMMON_OBJECT_CACHE_URMA_FALLBACK_TCP_LIMITER_H

#include <atomic>
#include <cstdint>
#include <string>

#include "datasystem/utils/status.h"

namespace datasystem {

class UrmaFallbackTcpLimiter {
public:
    UrmaFallbackTcpLimiter() = delete;
    ~UrmaFallbackTcpLimiter() = delete;

    static constexpr uint64_t kMaxPendingBytes = 10 * 1024 * 1024;
    static constexpr uint64_t kMaxSinglePayloadBytes = 1024 * 1024;

    class Ticket {
    public:
        Ticket() = default;
        Ticket(const Ticket &) = delete;
        Ticket &operator=(const Ticket &) = delete;
        Ticket(Ticket &&other) noexcept;
        Ticket &operator=(Ticket &&other) noexcept;
        ~Ticket();

        bool IsActive() const;

    private:
        friend class UrmaFallbackTcpLimiter;

        Ticket(std::atomic<uint64_t> *pendingBytes, uint64_t bytes) noexcept;
        void Release();

        std::atomic<uint64_t> *pendingBytes_{ nullptr };
        uint64_t bytes_{ 0 };
    };

    static Status TryAcquireProcessScope(uint64_t bytes, const Status &transportStatus, const std::string &direction,
                                         Ticket &ticket, bool checkSinglePayloadLimit = true);

    static Status TryAcquire(std::atomic<uint64_t> &pendingBytes, uint64_t bytes, const Status &transportStatus,
                             const std::string &direction, Ticket &ticket, bool checkSinglePayloadLimit = true);

private:
    static Status BuildRejectStatus(const Status &transportStatus, const std::string &reason);
    // Worker-side fallback TCP payloads share one RPC data channel per process, so this limiter is process scoped.
    static std::atomic<uint64_t> processPendingBytes_;
};
}  // namespace datasystem

#endif

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

#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"

namespace datasystem {

std::atomic<uint64_t> UrmaFallbackTcpLimiter::processPendingBytes_{ 0 };

UrmaFallbackTcpLimiter::Ticket::Ticket(std::atomic<uint64_t> *pendingBytes, uint64_t bytes) noexcept
    : pendingBytes_(pendingBytes), bytes_(bytes)
{
}

UrmaFallbackTcpLimiter::Ticket::Ticket(Ticket &&other) noexcept
    : pendingBytes_(other.pendingBytes_), bytes_(other.bytes_)
{
    other.pendingBytes_ = nullptr;
    other.bytes_ = 0;
}

UrmaFallbackTcpLimiter::Ticket &UrmaFallbackTcpLimiter::Ticket::operator=(Ticket &&other) noexcept
{
    if (this != &other) {
        Release();
        pendingBytes_ = other.pendingBytes_;
        bytes_ = other.bytes_;
        other.pendingBytes_ = nullptr;
        other.bytes_ = 0;
    }
    return *this;
}

UrmaFallbackTcpLimiter::Ticket::~Ticket()
{
    Release();
}

bool UrmaFallbackTcpLimiter::Ticket::IsActive() const
{
    return pendingBytes_ != nullptr && bytes_ > 0;
}

void UrmaFallbackTcpLimiter::Ticket::Release()
{
    if (pendingBytes_ != nullptr && bytes_ > 0) {
        (void)pendingBytes_->fetch_sub(bytes_, std::memory_order_acq_rel);
        pendingBytes_ = nullptr;
        bytes_ = 0;
    }
}

Status UrmaFallbackTcpLimiter::TryAcquireProcessScope(uint64_t bytes, const Status &transportStatus,
                                                      const std::string &direction, Ticket &ticket,
                                                      bool checkSinglePayloadLimit)
{
    return TryAcquire(processPendingBytes_, bytes, transportStatus, direction, ticket, checkSinglePayloadLimit);
}

Status UrmaFallbackTcpLimiter::TryAcquire(std::atomic<uint64_t> &pendingBytes, uint64_t bytes,
                                          const Status &transportStatus, const std::string &direction, Ticket &ticket,
                                          bool checkSinglePayloadLimit)
{
    if (bytes == 0) {
        return Status::OK();
    }
    if (checkSinglePayloadLimit && bytes >= kMaxSinglePayloadBytes) {
        auto rc = BuildRejectStatus(
            transportStatus,
            FormatString("%s payload %llu bytes is not smaller than the limit %llu bytes", direction, bytes,
                         kMaxSinglePayloadBytes));
        LOG(WARNING) << rc.ToString();
        return rc;
    }

    uint64_t current = pendingBytes.load(std::memory_order_relaxed);
    while (true) {
        if (current > kMaxPendingBytes || bytes > kMaxPendingBytes - current) {
            auto rc = BuildRejectStatus(
                transportStatus,
                FormatString("%s pending %llu bytes plus payload %llu bytes exceeds the limit %llu bytes", direction,
                             current, bytes, kMaxPendingBytes));
            LOG(WARNING) << rc.ToString();
            return rc;
        }
        if (pendingBytes.compare_exchange_weak(current, current + bytes, std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
            ticket = Ticket(&pendingBytes, bytes);
            return Status::OK();
        }
    }
}

Status UrmaFallbackTcpLimiter::BuildRejectStatus(const Status &transportStatus, const std::string &reason)
{
    if (transportStatus.IsOk()) {
        LOG(WARNING) << "URMA fallback TCP limiter received OK transport status, use default URMA error instead.";
    }
    const auto code = transportStatus.IsOk() ? StatusCode::K_URMA_ERROR : transportStatus.GetCode();
    const auto message = transportStatus.GetMsg().empty() ? "URMA transport failed" : transportStatus.GetMsg();
    return Status(code, FormatString("%s, fallback tcp failed: %s", message, reason));
}
}  // namespace datasystem

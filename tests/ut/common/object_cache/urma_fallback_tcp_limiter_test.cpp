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

#include <atomic>
#include <string>

#include <gtest/gtest.h>

#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"

namespace datasystem {
namespace ut {

TEST(UrmaFallbackTcpLimiterTest, RejectsPayloadNotSmallerThanOneMb)
{
    std::atomic<uint64_t> pendingBytes{ 0 };
    UrmaFallbackTcpLimiter::Ticket ticket;
    Status status = UrmaFallbackTcpLimiter::TryAcquire(
        pendingBytes, UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes, Status(K_URMA_ERROR, "urma write failed"),
        "worker->client", ticket);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR);
    ASSERT_NE(status.GetMsg().find("fallback tcp failed"), std::string::npos);
    ASSERT_EQ(pendingBytes.load(), 0UL);
}

TEST(UrmaFallbackTcpLimiterTest, RejectsWhenPendingPlusCurrentExceedsTenMb)
{
    std::atomic<uint64_t> pendingBytes{ UrmaFallbackTcpLimiter::kMaxPendingBytes - 256 };
    UrmaFallbackTcpLimiter::Ticket ticket;
    Status status =
        UrmaFallbackTcpLimiter::TryAcquire(pendingBytes, 512, Status(K_URMA_ERROR, "urma write failed"),
                                           "worker->worker", ticket, false);
    ASSERT_TRUE(status.IsError());
    ASSERT_EQ(status.GetCode(), StatusCode::K_URMA_ERROR);
    ASSERT_NE(status.GetMsg().find("fallback tcp failed"), std::string::npos);
    ASSERT_EQ(pendingBytes.load(), UrmaFallbackTcpLimiter::kMaxPendingBytes - 256);
}

TEST(UrmaFallbackTcpLimiterTest, ReleasesPendingBytesWhenTicketDestroyed)
{
    std::atomic<uint64_t> pendingBytes{ 0 };
    {
        UrmaFallbackTcpLimiter::Ticket ticket;
        Status status =
            UrmaFallbackTcpLimiter::TryAcquire(pendingBytes, 512 * 1024UL, Status(K_URMA_ERROR, "urma write failed"),
                                               "worker->client", ticket);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        ASSERT_EQ(pendingBytes.load(), 512 * 1024UL);
        ASSERT_TRUE(ticket.IsActive());
    }
    ASSERT_EQ(pendingBytes.load(), 0UL);
}

TEST(UrmaFallbackTcpLimiterTest, ReleasesProcessPendingBytesWhenProcessScopeTicketDestroyed)
{
    {
        UrmaFallbackTcpLimiter::Ticket ticket;
        Status status = UrmaFallbackTcpLimiter::TryAcquireProcessScope(
            512 * 1024UL, Status(K_URMA_ERROR, "urma write failed"), "worker->worker", ticket);
        ASSERT_TRUE(status.IsOk()) << status.ToString();
        ASSERT_TRUE(ticket.IsActive());
    }
    UrmaFallbackTcpLimiter::Ticket ticket;
    Status status = UrmaFallbackTcpLimiter::TryAcquireProcessScope(
        UrmaFallbackTcpLimiter::kMaxPendingBytes - 1, Status(K_URMA_ERROR, "urma write failed"), "worker->worker",
        ticket, false);
    ASSERT_TRUE(status.IsOk()) << status.ToString();
    ASSERT_TRUE(ticket.IsActive());
}
}  // namespace ut
}  // namespace datasystem

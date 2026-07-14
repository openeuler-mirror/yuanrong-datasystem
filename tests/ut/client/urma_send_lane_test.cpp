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

#include <cstdint>
#include <memory>

#include <gtest/gtest.h>

#include "datasystem/common/rdma/urma_send_lane.h"

namespace datasystem {
namespace {

std::shared_ptr<UrmaJetty> MakeOpaqueJetty()
{
    // The pool only compares UrmaJetty pointer identity in these tests. Use an aliasing shared_ptr so the test can stay
    // independent of the heavy UrmaJetty/URMA resource construction path.
    auto owner = std::make_shared<uint64_t>();
    return std::shared_ptr<UrmaJetty>(owner, reinterpret_cast<UrmaJetty *>(owner.get()));
}

TEST(SendJettyPoolTest, PopReleaseAndExhaustionUpdateStats)
{
    // Covers the normal acquire/exhaust/release lifecycle and verifies that observable pool stats stay consistent with
    // the idle index stack.
    SendJettyPool pool;
    auto first = MakeOpaqueJetty();
    auto second = MakeOpaqueJetty();
    pool.Add(first);
    pool.Add(second);

    auto stats = pool.GetStats();
    EXPECT_EQ(stats.poolSize, 2ul);
    EXPECT_EQ(stats.idleCount, 2ul);
    EXPECT_EQ(stats.inUseCount, 0ul);

    std::shared_ptr<UrmaJetty> acquired;
    ASSERT_TRUE(pool.PopIdle(acquired));
    EXPECT_EQ(acquired.get(), second.get());
    stats = pool.GetStats();
    EXPECT_EQ(stats.idleCount, 1ul);
    EXPECT_EQ(stats.inUseCount, 1ul);

    ASSERT_TRUE(pool.PopIdle(acquired));
    EXPECT_EQ(acquired.get(), first.get());
    EXPECT_FALSE(pool.PopIdle(acquired));
    stats = pool.GetStats();
    EXPECT_EQ(stats.idleCount, 0ul);
    EXPECT_EQ(stats.inUseCount, 2ul);

    EXPECT_TRUE(pool.Release(first));
    stats = pool.GetStats();
    EXPECT_EQ(stats.idleCount, 1ul);
    EXPECT_EQ(stats.inUseCount, 1ul);
}

TEST(SendJettyPoolTest, DuplicateReleaseDoesNotDuplicateIdleIndex)
{
    // A lane may be released through duplicated/error cleanup paths. Releasing the same jetty twice must not create two
    // idle entries for one physical lane.
    SendJettyPool pool;
    auto jetty = MakeOpaqueJetty();
    pool.Add(jetty);

    std::shared_ptr<UrmaJetty> acquired;
    ASSERT_TRUE(pool.PopIdle(acquired));
    ASSERT_FALSE(pool.PopIdle(acquired));

    EXPECT_TRUE(pool.Release(jetty));
    EXPECT_TRUE(pool.Release(jetty));
    auto stats = pool.GetStats();
    EXPECT_EQ(stats.poolSize, 1ul);
    EXPECT_EQ(stats.idleCount, 1ul);
    EXPECT_EQ(stats.inUseCount, 0ul);

    ASSERT_TRUE(pool.PopIdle(acquired));
    EXPECT_EQ(acquired.get(), jetty.get());
    EXPECT_FALSE(pool.PopIdle(acquired));
}

TEST(SendJettyPoolTest, RemoveFixesSwappedIdleIndex)
{
    // Remove compacts the backing vector by moving the last jetty into the removed slot. Any idle index pointing to the
    // old last slot must be rewritten, otherwise PopIdle can return a stale/removed entry or silently lose capacity.
    SendJettyPool pool;
    auto first = MakeOpaqueJetty();
    auto second = MakeOpaqueJetty();
    auto third = MakeOpaqueJetty();
    pool.Add(first);
    pool.Add(second);
    pool.Add(third);

    EXPECT_TRUE(pool.Remove(first));
    auto stats = pool.GetStats();
    EXPECT_EQ(stats.poolSize, 2ul);
    EXPECT_EQ(stats.idleCount, 2ul);

    std::shared_ptr<UrmaJetty> acquired;
    ASSERT_TRUE(pool.PopIdle(acquired));
    EXPECT_NE(acquired.get(), first.get());
    ASSERT_TRUE(pool.PopIdle(acquired));
    EXPECT_NE(acquired.get(), first.get());
    EXPECT_FALSE(pool.PopIdle(acquired));
}

TEST(SendJettyPoolTest, RemoveInUseJettyKeepsOtherIdleJettys)
{
    // Retiring an in-use lane should remove that lane only; unrelated idle lanes must remain acquirable so one faulty
    // request does not shrink the healthy part of the pool.
    SendJettyPool pool;
    auto first = MakeOpaqueJetty();
    auto second = MakeOpaqueJetty();
    pool.Add(first);
    pool.Add(second);

    std::shared_ptr<UrmaJetty> acquired;
    ASSERT_TRUE(pool.PopIdle(acquired));
    ASSERT_EQ(acquired.get(), second.get());

    EXPECT_TRUE(pool.Remove(second));
    auto stats = pool.GetStats();
    EXPECT_EQ(stats.poolSize, 1ul);
    EXPECT_EQ(stats.idleCount, 1ul);
    EXPECT_EQ(stats.inUseCount, 0ul);

    ASSERT_TRUE(pool.PopIdle(acquired));
    EXPECT_EQ(acquired.get(), first.get());
    EXPECT_FALSE(pool.PopIdle(acquired));
}

TEST(SendLaneLeaseTest, ReleaseWaitsForSealAndLastEvent)
{
    // A request-level lease can have multiple chunk events. The lane is returned only after the request is sealed and
    // the last outstanding event completes, preventing later chunks from racing with an early release.
    auto jetty = MakeOpaqueJetty();
    UrmaSendLaneLease lease(jetty);
    lease.AddEvent();
    lease.AddEvent();

    EXPECT_EQ(lease.MarkEventReleased(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.GetPendingEventCount(), 1u);
    EXPECT_EQ(lease.Seal(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_FALSE(lease.IsSettled());

    EXPECT_EQ(lease.MarkEventReleased(), UrmaSendLaneLease::SettleAction::RELEASE);
    EXPECT_TRUE(lease.IsSettled());
    EXPECT_EQ(lease.Seal(), UrmaSendLaneLease::SettleAction::NONE);
}

TEST(SendLaneLeaseTest, CompletionBeforeSealDoesNotReleaseEarly)
{
    // Completion can arrive before the producer has submitted all chunks. Such early completions must decrement pending
    // work but cannot settle the lane until Seal marks the request submission boundary.
    auto jetty = MakeOpaqueJetty();
    UrmaSendLaneLease lease(jetty);
    lease.AddEvent();

    EXPECT_EQ(lease.MarkEventReleased(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.GetPendingEventCount(), 0u);
    EXPECT_FALSE(lease.IsSettled());

    lease.AddEvent();
    EXPECT_EQ(lease.Seal(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.MarkEventReleased(), UrmaSendLaneLease::SettleAction::RELEASE);
}

TEST(SendLaneLeaseTest, RetireWinsAndIsIdempotent)
{
    // Any chunk-level failure should retire the shared lane when the request drains. Later duplicate completions must be
    // no-ops so cleanup is applied exactly once.
    auto jetty = MakeOpaqueJetty();
    UrmaSendLaneLease lease(jetty);
    lease.AddEvent();
    lease.AddEvent();

    EXPECT_EQ(lease.MarkEventRetired(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.Seal(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.MarkEventReleased(), UrmaSendLaneLease::SettleAction::RETIRE);
    EXPECT_EQ(lease.MarkEventRetired(), UrmaSendLaneLease::SettleAction::NONE);
}

TEST(SendLaneLeaseTest, RequestRetireDoesNotConsumePreviouslySubmittedEvents)
{
    // A later chunk can fail before it has an event, while earlier chunks are still in flight. Marking that failure
    // must preserve the earlier event count and convert the final settlement to RETIRE rather than RELEASE.
    auto jetty = MakeOpaqueJetty();
    UrmaSendLaneLease lease(jetty);
    lease.AddEvent();

    EXPECT_EQ(lease.RequestRetire(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.GetPendingEventCount(), 1u);
    EXPECT_EQ(lease.Seal(), UrmaSendLaneLease::SettleAction::NONE);
    EXPECT_EQ(lease.MarkEventReleased(), UrmaSendLaneLease::SettleAction::RETIRE);
    EXPECT_TRUE(lease.IsSettled());
}

}  // namespace
}  // namespace datasystem

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

// Per-peer in-flight jetty concurrency cap UT. These tests touch only the in-flight
// counter/condvar (no URMA hardware), so they run with real or mock URMA builds.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "datasystem/common/rdma/urma_manager.h"
#include "datasystem/common/rdma/urma_resource.h"
#include "datasystem/common/util/raii.h"

DS_DECLARE_uint32(urma_send_jetty_lane_pool_size);
DS_DECLARE_uint32(urma_send_lane_count_per_peer);

namespace datasystem {
class UrmaConnectionTestAccess {
public:
    UrmaConnectionTestAccess() = delete;
    ~UrmaConnectionTestAccess() = delete;
    static constexpr uint32_t MaxInflight = 8;
    static constexpr auto MaxRetired = UrmaConnection::MAX_RETIRED_JETTIES;

    static uint32_t ConfiguredMaxInflight(const UrmaConnection &connection)
    {
        return connection.maxInflightJetties_;
    }

    static uint32_t Inflight(const UrmaConnection &connection)
    {
        std::lock_guard<bthread::Mutex> lock(connection.peerState_->mutex);
        return connection.peerState_->inflight;
    }

    static void ExpireCooldown(UrmaConnection &connection)
    {
        std::lock_guard<bthread::Mutex> lock(connection.peerState_->mutex);
        connection.peerState_->retryAfter = std::chrono::steady_clock::time_point::min();
    }

    static std::shared_ptr<UrmaConnection> Find(const std::string &key)
    {
        TbbUrmaConnectionMap::const_accessor accessor;
        auto &map = UrmaManager::Instance().urmaConnectionMap_;
        return map.find(accessor, key) ? accessor->second : nullptr;
    }

    static void Put(const std::string &key, std::shared_ptr<UrmaConnection> connection)
    {
        TbbUrmaConnectionMap::accessor accessor;
        UrmaManager::Instance().urmaConnectionMap_.insert(accessor, key);
        accessor->second = std::move(connection);
    }

    static UrmaResource &Resource()
    {
        return *UrmaManager::Instance().urmaResource_;
    }

    static bool HoldRead(const std::string &key, TbbUrmaConnectionMap::const_accessor &accessor)
    {
        return UrmaManager::Instance().urmaConnectionMap_.find(accessor, key);
    }
};

namespace {
using datasystem::HostPort;

// FIFO pool test helper: aliasing shared_ptr so the test stays independent of UrmaJetty construction.
std::shared_ptr<UrmaJetty> MakeOpaqueJetty()
{
    auto owner = std::make_shared<uint64_t>();
    return std::shared_ptr<UrmaJetty>(owner, reinterpret_cast<UrmaJetty *>(owner.get()));
}

TEST(SendJettyPoolFifoTest, PopTakesHeadReleaseReturnsTail)
{
    // FIFO vs LIFO divergence: pop head, release it back; under LIFO the just-released Jetty would
    // be re-popped immediately (stack top), under FIFO it goes to the tail and the next head is
    // popped first. This rotation is the issue #93 LIFO-amplification fix.
    SendJettyPool pool;
    auto jA = MakeOpaqueJetty();
    auto jB = MakeOpaqueJetty();
    auto jC = MakeOpaqueJetty();
    pool.Add(jA);  // queue: [A]
    pool.Add(jB);  // queue: [A, B]
    pool.Add(jC);  // queue: [A, B, C]

    std::shared_ptr<UrmaJetty> got;
    // FIFO pops head: A, B, C.
    ASSERT_TRUE(pool.PopIdle(got));
    std::printf("[EVIDENCE-FIFO] pop1=%p (expect head jA=%p)\n", (void *)got.get(), (void *)jA.get());
    EXPECT_EQ(got.get(), jA.get()) << "FIFO pops head (A) first";
    ASSERT_TRUE(pool.PopIdle(got));
    std::printf("[EVIDENCE-FIFO] pop2=%p (expect jB=%p)\n", (void *)got.get(), (void *)jB.get());
    EXPECT_EQ(got.get(), jB.get());

    // Release jA back. Under FIFO it goes to the tail: queue is now [C, A]. Under LIFO it would
    // be the stack top: the next pop would return jA again.
    pool.Release(jA);
    ASSERT_TRUE(pool.PopIdle(got));
    std::printf("[EVIDENCE-FIFO] after release(jA) pop=%p (expect jC=%p, NOT jA — released went to tail)\n",
                (void *)got.get(), (void *)jC.get());
    EXPECT_EQ(got.get(), jC.get()) << "released Jetty went to tail; head (C) popped, not re-popping jA";
    ASSERT_TRUE(pool.PopIdle(got));
    std::printf("[EVIDENCE-FIFO] next pop=%p (expect jA=%p, finally the tail)\n", (void *)got.get(), (void *)jA.get());
    EXPECT_EQ(got.get(), jA.get());
    EXPECT_FALSE(pool.PopIdle(got)) << "pool exhausted";
}

// Construct a UrmaConnection without an imported target Jetty. AcquireInflightSlot only touches
// the in-flight counter/condvar, not targetJetty_, so a null target is sufficient for slot tests.
std::shared_ptr<UrmaConnection> MakeConnection(const std::string &instanceId = "peer-X")
{
    UrmaJfrInfo info;
    info.uniqueInstanceId = instanceId;
    return std::make_shared<UrmaConnection>(nullptr, info);
}

constexpr int64_t LONG_BUDGET_US = 60 * 1000 * 1000LL;  // 60s, well above any test wait
constexpr int64_t TINY_BUDGET_US = 1000LL;             // 1ms, force fast timeout

TEST(UrmaConnectionInflightTest, AcquireRespectsPerPeerCap)
{
    // A peer may hold at most MAX_INFLIGHT_JETTIES concurrent slots; the next acquire blocks.
    auto conn = MakeConnection();
    for (uint32_t i = 0; i < UrmaConnectionTestAccess::MaxInflight; ++i) {
        ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk()) << "slot " << i << " should succeed";
    }
    std::printf("[EVIDENCE] peer-X held %u slots (=MAX_INFLIGHT_JETTIES=%u) after %u successful acquires\n",
                UrmaConnectionTestAccess::Inflight(*conn), UrmaConnectionTestAccess::MaxInflight,
                UrmaConnectionTestAccess::MaxInflight);
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*conn), UrmaConnectionTestAccess::MaxInflight);
    // 9th acquire with a tiny budget times out instead of exceeding the cap.
    auto rc = conn->AcquireInflightSlot(TINY_BUDGET_US);
    std::printf("[EVIDENCE] 9th acquire rc=%d (%s), count still %u (cap not exceeded)\n",
                static_cast<int>(rc.GetCode()), rc.GetMsg().c_str(), UrmaConnectionTestAccess::Inflight(*conn));
    EXPECT_EQ(rc.GetCode(), StatusCode::K_URMA_TRY_AGAIN);
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*conn), UrmaConnectionTestAccess::MaxInflight);
}

TEST(UrmaConnectionInflightTest, ConfiguredCapIsClampedByPoolSize)
{
    constexpr uint32_t CONFIGURED_LANE_COUNT = 4;
    constexpr uint32_t POOL_LANE_COUNT = 2;
    const auto savedLaneCount = FLAGS_urma_send_lane_count_per_peer;
    const auto savedPoolSize = FLAGS_urma_send_jetty_lane_pool_size;
    Raii restoreFlags([savedLaneCount, savedPoolSize] {
        FLAGS_urma_send_lane_count_per_peer = savedLaneCount;
        FLAGS_urma_send_jetty_lane_pool_size = savedPoolSize;
    });
    FLAGS_urma_send_lane_count_per_peer = CONFIGURED_LANE_COUNT;
    FLAGS_urma_send_jetty_lane_pool_size = POOL_LANE_COUNT;

    auto conn = MakeConnection();
    EXPECT_EQ(UrmaConnectionTestAccess::ConfiguredMaxInflight(*conn), POOL_LANE_COUNT);
    for (uint32_t i = 0; i < POOL_LANE_COUNT; ++i) {
        ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    }
    EXPECT_EQ(conn->AcquireInflightSlot(TINY_BUDGET_US).GetCode(), StatusCode::K_URMA_TRY_AGAIN);
}

TEST(UrmaConnectionInflightTest, ConfiguredCapIsAppliedAtConnectionCreation)
{
    constexpr uint32_t INITIAL_LANE_COUNT = 2;
    constexpr uint32_t UPDATED_LANE_COUNT = 4;
    const auto savedLaneCount = FLAGS_urma_send_lane_count_per_peer;
    const auto savedPoolSize = FLAGS_urma_send_jetty_lane_pool_size;
    Raii restoreFlags([savedLaneCount, savedPoolSize] {
        FLAGS_urma_send_lane_count_per_peer = savedLaneCount;
        FLAGS_urma_send_jetty_lane_pool_size = savedPoolSize;
    });
    FLAGS_urma_send_jetty_lane_pool_size = UPDATED_LANE_COUNT;
    FLAGS_urma_send_lane_count_per_peer = INITIAL_LANE_COUNT;

    auto initial = MakeConnection();
    FLAGS_urma_send_lane_count_per_peer = UPDATED_LANE_COUNT;
    auto updated = MakeConnection();

    EXPECT_EQ(UrmaConnectionTestAccess::ConfiguredMaxInflight(*initial), INITIAL_LANE_COUNT);
    EXPECT_EQ(UrmaConnectionTestAccess::ConfiguredMaxInflight(*updated), UPDATED_LANE_COUNT);
}

TEST(UrmaConnectionInflightTest, ReleaseUnblocksWaiter)
{
    // After a release, a previously-blocked waiter proceeds and the count stays bounded.
    auto conn = MakeConnection();
    for (uint32_t i = 0; i < UrmaConnectionTestAccess::MaxInflight; ++i) {
        ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    }
    std::atomic<bool> acquired{ false };
    std::thread waiter([&] {
        auto rc = conn->AcquireInflightSlot(LONG_BUDGET_US);
        if (rc.IsOk()) {
            acquired.store(true);
        }
        std::printf("[EVIDENCE] waiter woke, rc=%d, count=%u\n",
                    static_cast<int>(rc.GetCode()), UrmaConnectionTestAccess::Inflight(*conn));
    });
    // Give the waiter time to park on the condvar.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::printf("[EVIDENCE] before release: waiter acquired=%d, count=%u (waiter blocked)\n",
                acquired.load() ? 1 : 0, UrmaConnectionTestAccess::Inflight(*conn));
    EXPECT_FALSE(acquired.load()) << "waiter must block while cap is saturated";
    conn->ReleaseInflightSlot();
    waiter.join();
    EXPECT_TRUE(acquired.load()) << "waiter woken after release";
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*conn), UrmaConnectionTestAccess::MaxInflight);
    // Cleanup so the test does not leak a slot into the next case.
    conn->ReleaseInflightSlot();
}

TEST(UrmaConnectionInflightTest, PeersAreIsolated)
{
    // Peer X saturating its cap does not block peer Y.
    auto connX = MakeConnection("peer-X");
    auto connY = MakeConnection("peer-Y");
    for (uint32_t i = 0; i < UrmaConnectionTestAccess::MaxInflight; ++i) {
        ASSERT_TRUE(connX->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    }
    std::printf("[EVIDENCE] peer-X saturated at %u; peer-Y count=%u before its acquire\n",
                UrmaConnectionTestAccess::Inflight(*connX), UrmaConnectionTestAccess::Inflight(*connY));
    // Y can still acquire its own slots while X is saturated.
    ASSERT_TRUE(connY->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    std::printf("[EVIDENCE] peer-Y acquired 1 slot (count=%u) while peer-X still at %u (isolated)\n",
                UrmaConnectionTestAccess::Inflight(*connY), UrmaConnectionTestAccess::Inflight(*connX));
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*connY), 1u);
    // X is still blocked at its cap.
    EXPECT_EQ(connX->AcquireInflightSlot(TINY_BUDGET_US).GetCode(), StatusCode::K_URMA_TRY_AGAIN);
    connY->ReleaseInflightSlot();
    while (UrmaConnectionTestAccess::Inflight(*connX) > 0) {
        connX->ReleaseInflightSlot();
    }
}

TEST(UrmaConnectionInflightTest, UnbalancedReleaseDoesNotUnderflow)
{
    // Regression for the review finding: fetch_sub-based release checked the guard AFTER the
    // irreversible decrement, so a double release wrapped the counter to ~4 billion and froze
    // the peer's cap forever. The lock-based release must keep the counter at 0 and the peer
    // must remain usable afterwards.
    auto conn = MakeConnection();
    ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    conn->ReleaseInflightSlot();
    conn->ReleaseInflightSlot();  // unbalanced double release
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*conn), 0u) << "counter must not wrap below zero";
    // Peer not frozen: a subsequent acquire still succeeds.
    ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*conn), 1u);
    conn->ReleaseInflightSlot();
}

TEST(UrmaConnectionInflightTest, BadPeerRotationIsCircuitBroken)
{
    // Issue #93 fatal-CQE form, regression for the review finding: a bad peer whose writes
    // retire their jetties must NOT rotate through the whole pool. After MAX_RETIRED_JETTIES
    // retired jetties the peer is circuit-broken (K_URMA_TRY_AGAIN on AcquireInflightSlot),
    // while a good peer on the same pool is unaffected. Recovery = new connection object.
    if (std::getenv("DS_URMA_DEV_NAME") == nullptr) {
        GTEST_SKIP() << "rotation test needs an initialized jetty pool (set DS_URMA_DEV_NAME)";
    }
    auto &manager = UrmaManager::Instance();
    ASSERT_TRUE(manager.Init(HostPort("127.0.0.1", 0)).IsOk());
    auto &resource = UrmaConnectionTestAccess::Resource();
    const auto st0 = resource.GetSendJettyPoolStats();
    ASSERT_GT(st0.poolSize, 0u) << "pool must be initialized";

    auto bad = MakeConnection("bad-rotation");
    int retired = 0;
    for (int i = 0; i < 1000; i++) {  // far above pool size
        std::shared_ptr<UrmaJetty> jetty;
        if (!bad->AcquireInflightSlot(LONG_BUDGET_US).IsOk()) {
            break;
        }
        if (!resource.AcquireJetty(jetty).IsOk()) {
            bad->ReleaseInflightSlot();
            break;
        }
        // Full ApplyActiveSendLaneAction(RETIRE) sequence.
        ASSERT_TRUE(resource.RetireJetty(jetty).IsOk());
        bad->OnJettyRetired();
        bad->ReleaseInflightSlot();
        retired++;
    }
    std::printf("[EVIDENCE] bad peer retired %d jetties then circuit-broken "
                "(MAX_RETIRED_JETTIES=%u, pool was %zu)\n",
                retired, UrmaConnectionTestAccess::MaxRetired, st0.poolSize);
    EXPECT_EQ(retired, UrmaConnectionTestAccess::MaxRetired) << "damage must be bounded, not the whole pool";
    EXPECT_TRUE(bad->IsCircuitBroken());
    // Circuit-broken peer: further acquisition is refused outright.
    auto rc = bad->AcquireInflightSlot(LONG_BUDGET_US);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_URMA_TRY_AGAIN);

    // A good peer is still served: the pool survived X's rampage.
    auto good = MakeConnection("good-after-rampage");
    ASSERT_TRUE(good->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    std::shared_ptr<UrmaJetty> goodJetty;
    ASSERT_TRUE(resource.AcquireJetty(goodJetty).IsOk());
    resource.ReleaseJetty(goodJetty);
    good->ReleaseInflightSlot();

    // Recovery: a rebuilt connection starts with a zeroed counter.
    auto rebuilt = MakeConnection("bad-rotation");
    EXPECT_FALSE(rebuilt->IsCircuitBroken());
    ASSERT_TRUE(rebuilt->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    rebuilt->ReleaseInflightSlot();
}

class ScopedConnectionEntry {
public:
    explicit ScopedConnectionEntry(std::string key) : key_(std::move(key)) {}
    ~ScopedConnectionEntry() { (void)UrmaManager::Instance().RemoveRemoteDevice(key_); }
    const std::string &Key() const { return key_; }

private:
    std::string key_;
};

void TripBreaker(const std::shared_ptr<UrmaConnection> &connection)
{
    for (uint32_t i = 0; i < UrmaConnectionTestAccess::MaxRetired; ++i) {
        connection->OnJettyRetired();
    }
}

TEST(UrmaConnectionInflightTest, BrokenEntryRetainsBudgetUntilCooledDownReplacement)
{
    ScopedConnectionEntry entry("breaker-recovery");
    auto old = MakeConnection();
    TripBreaker(old);
    UrmaConnectionTestAccess::Put(entry.Key(), old);
    auto &manager = UrmaManager::Instance();
    EXPECT_EQ(manager.CheckUrmaConnectionStable(entry.Key(), "peer-X").GetCode(), K_URMA_TRY_AGAIN);
    EXPECT_EQ(UrmaConnectionTestAccess::Find(entry.Key()), old);
    UrmaConnectionTestAccess::ExpireCooldown(*old);
    EXPECT_EQ(manager.CheckUrmaConnectionStable(entry.Key(), "peer-X").GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(UrmaConnectionTestAccess::Find(entry.Key()), old);
    auto rebuilt = MakeConnection();
    ASSERT_TRUE(rebuilt->PrepareReplacement(*old).IsOk());
    UrmaConnectionTestAccess::Put(entry.Key(), rebuilt);
    EXPECT_TRUE(manager.CheckUrmaConnectionStable(entry.Key(), "peer-X").IsOk());
    EXPECT_EQ(old->GetUrmaJfrInfo().uniqueInstanceId, "peer-X");
    // Cooldown already expired and the entry was replaced: the stale connection asks for a rebuild
    // (NEED_CONNECT) so data-plane owners replace it instead of retrying on the retired object.
    EXPECT_EQ(old->AcquireInflightSlot(TINY_BUDGET_US).GetCode(), K_URMA_NEED_CONNECT);
}

TEST(UrmaConnectionInflightTest, RepeatedFailedGenerationsOnlyAdmitOneProbeAfterCooldown)
{
    ScopedConnectionEntry entry("persistent-failure-generations");
    auto connection = MakeConnection();
    auto healthy = MakeConnection("healthy");
    TripBreaker(connection);
    UrmaConnectionTestAccess::Put(entry.Key(), connection);
    constexpr uint32_t generations = 3;
    for (uint32_t generation = 0; generation < generations; ++generation) {
        auto next = MakeConnection();
        EXPECT_EQ(next->PrepareReplacement(*connection).GetCode(), K_URMA_TRY_AGAIN);
        UrmaConnectionTestAccess::ExpireCooldown(*connection);
        ASSERT_TRUE(next->PrepareReplacement(*connection).IsOk());
        UrmaConnectionTestAccess::Put(entry.Key(), next);
        ASSERT_TRUE(next->AcquireInflightSlot(TINY_BUDGET_US).IsOk());
        EXPECT_EQ(next->AcquireInflightSlot(TINY_BUDGET_US).GetCode(), K_URMA_TRY_AGAIN);
        ASSERT_TRUE(healthy->AcquireInflightSlot(TINY_BUDGET_US).IsOk());
        healthy->ReleaseInflightSlot();
        next->OnJettyRetired();
        next->ReleaseInflightSlot();
        EXPECT_TRUE(next->IsCircuitBroken());
        EXPECT_FALSE(next->CanReconnect());
        connection = std::move(next);
    }
}

TEST(UrmaConnectionInflightTest, OnlyCurrentSuccessfulProbeResetsBudget)
{
    auto old = MakeConnection();
    TripBreaker(old);
    UrmaConnectionTestAccess::ExpireCooldown(*old);
    auto next = MakeConnection();
    ASSERT_TRUE(next->PrepareReplacement(*old).IsOk());
    ASSERT_TRUE(next->AcquireInflightSlot(TINY_BUDGET_US).IsOk());
    old->OnTransferFinished(true);
    EXPECT_EQ(next->AcquireInflightSlot(TINY_BUDGET_US).GetCode(), K_URMA_TRY_AGAIN);
    next->OnTransferFinished(true);
    next->ReleaseInflightSlot();
    for (uint32_t i = 0; i < UrmaConnectionTestAccess::MaxInflight; ++i) {
        ASSERT_TRUE(next->AcquireInflightSlot(TINY_BUDGET_US).IsOk());
    }
    for (uint32_t i = 0; i < UrmaConnectionTestAccess::MaxInflight; ++i) {
        next->ReleaseInflightSlot();
    }
    next->OnJettyRetired();
    EXPECT_FALSE(next->IsCircuitBroken());
}

TEST(UrmaConnectionInflightTest, FailedProbeAndChangedInstance)
{
    auto old = MakeConnection();
    TripBreaker(old);
    UrmaConnectionTestAccess::ExpireCooldown(*old);
    auto next = MakeConnection();
    ASSERT_TRUE(next->PrepareReplacement(*old).IsOk());
    ASSERT_TRUE(next->AcquireInflightSlot(TINY_BUDGET_US).IsOk());
    next->OnTransferFinished(false);
    next->ReleaseInflightSlot();
    EXPECT_TRUE(next->IsCircuitBroken());
    EXPECT_FALSE(next->CanReconnect());
    auto restarted = MakeConnection("peer-new-instance");
    ASSERT_TRUE(restarted->PrepareReplacement(*next).IsOk());
    EXPECT_FALSE(restarted->IsCircuitBroken());
    ASSERT_TRUE(restarted->AcquireInflightSlot(TINY_BUDGET_US).IsOk());
    restarted->ReleaseInflightSlot();
}

TEST(UrmaConnectionInflightTest, StableCheckAndRemovalDoNotWaitForHeldConnection)
{
    ScopedConnectionEntry entry("held-connection");
    auto held = MakeConnection();
    TripBreaker(held);
    UrmaConnectionTestAccess::Put(entry.Key(), held);
    std::thread remover([&] { EXPECT_TRUE(UrmaManager::Instance().RemoveRemoteDevice(entry.Key()).IsOk()); });
    remover.join();
    EXPECT_EQ(UrmaConnectionTestAccess::Find(entry.Key()), nullptr);
    EXPECT_EQ(held->GetUrmaJfrInfo().uniqueInstanceId, "peer-X");
    EXPECT_TRUE(held->IsCircuitBroken());
}

TEST(UrmaConnectionInflightTest, ProbeOutcomeRequiresSuccessfulCompletedWr)
{
    auto jetty = MakeOpaqueJetty();
    UrmaSendLaneLease empty(jetty);
    empty.Seal();
    EXPECT_FALSE(empty.CompletedSuccessfully());
    UrmaSendLaneLease cancelled(jetty);
    cancelled.AddWr();
    cancelled.CancelWr();
    cancelled.Seal();
    EXPECT_FALSE(cancelled.CompletedSuccessfully());
    UrmaSendLaneLease failed(jetty);
    failed.AddWr();
    failed.CompleteWr(false);
    failed.Seal();
    EXPECT_FALSE(failed.CompletedSuccessfully());
    UrmaSendLaneLease success(jetty);
    success.AddWr();
    success.CompleteWr(true);
    success.Seal();
    EXPECT_TRUE(success.CompletedSuccessfully());
}

TEST(UrmaConnectionInflightTest, StabilityCheckDoesNotAcquireWriterWhileReaderIsHeld)
{
    ScopedConnectionEntry entry("stable-check-read-lock");
    auto connection = MakeConnection();
    TripBreaker(connection);
    UrmaConnectionTestAccess::ExpireCooldown(*connection);
    UrmaConnectionTestAccess::Put(entry.Key(), connection);
    TbbUrmaConnectionMap::const_accessor heldRead;
    ASSERT_TRUE(UrmaConnectionTestAccess::HoldRead(entry.Key(), heldRead));
    auto checked = std::async(std::launch::async, [&] {
        return UrmaManager::Instance().CheckUrmaConnectionStable(entry.Key(), "peer-X");
    });
    constexpr auto waitBudget = std::chrono::seconds(1);
    const auto waitResult = checked.wait_for(waitBudget);
    heldRead.release();
    EXPECT_EQ(waitResult, std::future_status::ready);
    EXPECT_EQ(checked.get().GetCode(), K_URMA_NEED_CONNECT);
}

TEST(UrmaConnectionInflightTest, SharedLaneOwnsConnectionAfterMapRemoval)
{
    ScopedConnectionEntry entry("lane-connection-lifetime");
    auto connection = MakeConnection();
    std::weak_ptr<UrmaConnection> weak = connection;
    UrmaConnectionTestAccess::Put(entry.Key(), connection);
    auto lane = std::make_shared<UrmaSendLaneLease>(MakeOpaqueJetty(), 0, connection);
    connection.reset();
    ASSERT_TRUE(UrmaManager::Instance().RemoveRemoteDevice(entry.Key()).IsOk());
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(lane->GetConnection()->GetUrmaJfrInfo().uniqueInstanceId, "peer-X");
    lane.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(UrmaConnectionInflightTest, NegativeDeadlineRejectedImmediately)
{
    // A non-positive remaining budget must not block; it returns deadline-exceeded at once.
    auto conn = MakeConnection();
    auto rc = conn->AcquireInflightSlot(0);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(UrmaConnectionTestAccess::Inflight(*conn), 0u);
}

// Fallback key resolution in the worker-entry precheck: a handshake registered before the client id was known
// lives under the peer address, so the precheck is given that address as a fallback for the client-id key.
TEST(UrmaConnectionFallbackTest, ResolvesMissingClientIdKeyThroughPeerAddress)
{
    ScopedConnectionEntry entry("fallback-resolves");
    UrmaConnectionTestAccess::Put(entry.Key(), MakeConnection());
    auto &manager = UrmaManager::Instance();

    // Without the fallback the client-id key is a miss — the K_URMA_NEED_CONNECT the read path heals over TCP.
    EXPECT_EQ(manager.CheckUrmaConnectionStable("client-id-unknown", "peer-X").GetCode(), K_URMA_NEED_CONNECT);
    // With the peer address the same request is admitted instead of rejected.
    EXPECT_TRUE(manager.CheckUrmaConnectionStable("client-id-unknown", "peer-X", entry.Key()).IsOk());
}

TEST(UrmaConnectionFallbackTest, FallbackThatCannotDifferIsNotConsulted)
{
    ScopedConnectionEntry entry("fallback-inert");
    UrmaConnectionTestAccess::Put(entry.Key(), MakeConnection());
    auto &manager = UrmaManager::Instance();

    // An empty fallback, and one equal to the request key, both short-circuit on the guard. That is what makes
    // it safe for the caller to pass "" when the key is already the peer address.
    EXPECT_EQ(manager.CheckUrmaConnectionStable("client-id-unknown", "peer-X", "").GetCode(), K_URMA_NEED_CONNECT);
    EXPECT_EQ(manager.CheckUrmaConnectionStable("client-id-unknown", "peer-X", "client-id-unknown").GetCode(),
              K_URMA_NEED_CONNECT);
}

TEST(UrmaConnectionFallbackTest, StaleInstanceIdStaysVisibleWhenRequestKeyHasConnection)
{
    ScopedConnectionEntry entry("fallback-stale");
    UrmaConnectionTestAccess::Put(entry.Key(), MakeConnection("peer-new"));
    auto &manager = UrmaManager::Instance();

    // The fallback only covers a missing connection: a stale instance id under the request key is a real
    // inconsistency and must stay visible rather than being masked by an address-keyed hit.
    EXPECT_EQ(manager.CheckUrmaConnectionStable(entry.Key(), "peer-old", "peer-address").GetCode(),
              K_URMA_NEED_CONNECT);
}

TEST(UrmaConnectionFallbackTest, ConnectionReachedThroughFallbackNeverReportsTryAgain)
{
    ScopedConnectionEntry entry("fallback-broken");
    auto connection = MakeConnection();
    TripBreaker(connection);
    UrmaConnectionTestAccess::Put(entry.Key(), connection);
    auto &manager = UrmaManager::Instance();

    // Primary key, circuit-broken, reconnect cooldown not elapsed: unchanged behaviour.
    EXPECT_EQ(manager.CheckUrmaConnectionStable(entry.Key(), "peer-X").GetCode(), K_URMA_TRY_AGAIN);
    // The same broken connection reached through the fallback address must not report K_URMA_TRY_AGAIN: that
    // code is outside both the worker-side remap and the read path's K_URMA_NEED_CONNECT self-heal, so it
    // would hard-fail a read that the no-fallback path turns into a recoverable "needs connect".
    EXPECT_EQ(manager.CheckUrmaConnectionStable("client-id-unknown", "peer-X", entry.Key()).GetCode(),
              K_URMA_NEED_CONNECT);
}

}  // namespace
}  // namespace datasystem

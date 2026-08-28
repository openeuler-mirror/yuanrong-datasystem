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

// Per-peer in-flight jetty concurrency cap UT. Validates the blast-radius guarantee of issue #93:
// a peer can occupy at most MAX_INFLIGHT_JETTIES of the pool concurrently, so a bad peer cannot
// drain the whole pool. These tests touch only the in-flight counter/condvar (no URMA hardware),
// so they build and run under both BUILD_WITH_URMA and BUILD_WITH_URMA_MOCK.

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#define private public
#include "datasystem/common/rdma/urma_manager.h"
#include "datasystem/common/rdma/urma_resource.h"
#undef private

namespace datasystem {
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
    for (uint32_t i = 0; i < UrmaConnection::MAX_INFLIGHT_JETTIES; ++i) {
        ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk()) << "slot " << i << " should succeed";
    }
    std::printf("[EVIDENCE] peer-X held %u slots (=MAX_INFLIGHT_JETTIES=%u) after %u successful acquires\n",
                conn->inflightJettyCount_, UrmaConnection::MAX_INFLIGHT_JETTIES,
                UrmaConnection::MAX_INFLIGHT_JETTIES);
    EXPECT_EQ(conn->inflightJettyCount_, UrmaConnection::MAX_INFLIGHT_JETTIES);
    // 9th acquire with a tiny budget times out instead of exceeding the cap.
    auto rc = conn->AcquireInflightSlot(TINY_BUDGET_US);
    std::printf("[EVIDENCE] 9th acquire rc=%d (%s), count still %u (cap not exceeded)\n",
                static_cast<int>(rc.GetCode()), rc.GetMsg().c_str(), conn->inflightJettyCount_);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_URMA_TRY_AGAIN);
    EXPECT_EQ(conn->inflightJettyCount_, UrmaConnection::MAX_INFLIGHT_JETTIES);
}

TEST(UrmaConnectionInflightTest, ReleaseUnblocksWaiter)
{
    // After a release, a previously-blocked waiter proceeds and the count stays bounded.
    auto conn = MakeConnection();
    for (uint32_t i = 0; i < UrmaConnection::MAX_INFLIGHT_JETTIES; ++i) {
        ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    }
    std::atomic<bool> acquired{ false };
    std::thread waiter([&] {
        auto rc = conn->AcquireInflightSlot(LONG_BUDGET_US);
        if (rc.IsOk()) {
            acquired.store(true);
        }
        std::printf("[EVIDENCE] waiter woke, rc=%d, count=%u\n",
                    static_cast<int>(rc.GetCode()), conn->inflightJettyCount_);
    });
    // Give the waiter time to park on the condvar.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::printf("[EVIDENCE] before release: waiter acquired=%d, count=%u (waiter blocked)\n",
                acquired.load() ? 1 : 0, conn->inflightJettyCount_);
    EXPECT_FALSE(acquired.load()) << "waiter must block while cap is saturated";
    conn->ReleaseInflightSlot();
    waiter.join();
    EXPECT_TRUE(acquired.load()) << "waiter woken after release";
    EXPECT_EQ(conn->inflightJettyCount_, UrmaConnection::MAX_INFLIGHT_JETTIES);
    // Cleanup so the test does not leak a slot into the next case.
    conn->ReleaseInflightSlot();
}

TEST(UrmaConnectionInflightTest, PeersAreIsolated)
{
    // Peer X saturating its cap does not block peer Y.
    auto connX = MakeConnection("peer-X");
    auto connY = MakeConnection("peer-Y");
    for (uint32_t i = 0; i < UrmaConnection::MAX_INFLIGHT_JETTIES; ++i) {
        ASSERT_TRUE(connX->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    }
    std::printf("[EVIDENCE] peer-X saturated at %u; peer-Y count=%u before its acquire\n",
                connX->inflightJettyCount_, connY->inflightJettyCount_);
    // Y can still acquire its own slots while X is saturated.
    ASSERT_TRUE(connY->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    std::printf("[EVIDENCE] peer-Y acquired 1 slot (count=%u) while peer-X still at %u (isolated)\n",
                connY->inflightJettyCount_, connX->inflightJettyCount_);
    EXPECT_EQ(connY->inflightJettyCount_, 1u);
    // X is still blocked at its cap.
    EXPECT_EQ(connX->AcquireInflightSlot(TINY_BUDGET_US).GetCode(), StatusCode::K_URMA_TRY_AGAIN);
    connY->ReleaseInflightSlot();
    while (connX->inflightJettyCount_ > 0) {
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
    EXPECT_EQ(conn->inflightJettyCount_, 0u) << "counter must not wrap below zero";
    // Peer not frozen: a subsequent acquire still succeeds.
    ASSERT_TRUE(conn->AcquireInflightSlot(LONG_BUDGET_US).IsOk());
    EXPECT_EQ(conn->inflightJettyCount_, 1u);
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
    auto &resource = *manager.urmaResource_;
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
                retired, UrmaConnection::MAX_RETIRED_JETTIES, st0.poolSize);
    EXPECT_EQ(retired, UrmaConnection::MAX_RETIRED_JETTIES) << "damage must be bounded, not the whole pool";
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

TEST(UrmaConnectionInflightTest, NegativeDeadlineRejectedImmediately)
{
    // A non-positive remaining budget must not block; it returns deadline-exceeded at once.
    auto conn = MakeConnection();
    auto rc = conn->AcquireInflightSlot(0);
    EXPECT_EQ(rc.GetCode(), StatusCode::K_RPC_DEADLINE_EXCEEDED);
    EXPECT_EQ(conn->inflightJettyCount_, 0u);
}

}  // namespace
}  // namespace datasystem

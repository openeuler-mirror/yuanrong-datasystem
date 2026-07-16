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

/** Description: ShmConnectionPool unit tests — covers review fixes #1/#2/#6/#7/#9:
 *  move-only ShmEntry (ShmFd RAII), lock-free-of-handshake GetOrCreateShm, pointer return,
 *  failed build leaves no residue, ReleaseShm/ReleaseAll idempotent resource release. */

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/client/transport/shm_connection_pool.h"
#include "datasystem/utils/status.h"

namespace {
// Stub connector: hands out a real pipe read-end as the fake socketFd so ~ShmFd's close() is
// well-defined, and counts builds so the "first builder wins" invariant is checkable.
struct StubConnector {
    std::atomic<int> buildCount{ 0 };
    datasystem::Status operator()(const datasystem::HostPort &workerAddr, datasystem::client::ShmEntry &entry)
    {
        int pipefds[2];
        if (pipe(pipefds) != 0) {
            return { datasystem::StatusCode::K_RUNTIME_ERROR, "pipe failed" };
        }
        close(pipefds[1]);
        entry.workerAddr = workerAddr;
        entry.socketFd = datasystem::client::ShmFd(pipefds[0]);
        entry.shmId = "shm-" + workerAddr.ToString();
        entry.workerStartId = "epoch-1";
        buildCount.fetch_add(1);
        return datasystem::Status::OK();
    }
};
}  // namespace

using datasystem::client::ShmConnectionPool;
using datasystem::client::ShmEntry;

TEST(ShmConnectionPoolTest, GetOrCreateBuildsOnceThenReuses)
{
    StubConnector stub;
    ShmConnectionPool pool("host-sdk", [&stub](const datasystem::HostPort &a, ShmEntry &e) { return stub(a, e); });
    datasystem::HostPort w;
    ASSERT_TRUE(w.ParseString("127.0.0.1:1234").IsOk());
    const ShmEntry *e1 = pool.GetOrCreateShm(w);
    ASSERT_NE(e1, nullptr);
    const ShmEntry *e2 = pool.GetOrCreateShm(w);
    ASSERT_NE(e2, nullptr);
    EXPECT_EQ(e1->socketFd.Get(), e2->socketFd.Get()) << "second call must reuse, not rebuild";
    EXPECT_EQ(stub.buildCount.load(), 1) << "connector built exactly once";
    EXPECT_EQ(pool.Size(), 1u);
}

TEST(ShmConnectionPoolTest, ConcurrentGetOrCreateBuildsOnce)
{
    StubConnector stub;
    ShmConnectionPool pool("host-sdk", [&stub](const datasystem::HostPort &a, ShmEntry &e) { return stub(a, e); });
    datasystem::HostPort w;
    ASSERT_TRUE(w.ParseString("127.0.0.1:5555").IsOk());
    constexpr int N = 16;
    std::vector<std::thread> ts;
    std::atomic<int> oks{ 0 };
    for (int i = 0; i < N; i++) {
        ts.emplace_back([&] {
            if (pool.GetOrCreateShm(w) != nullptr) {
                oks++;
            }
        });
    }
    for (auto &t : ts) {
        t.join();
    }
    // With lock-free-of-handshake GetOrCreateShm (review fix #6) every concurrent caller that misses
    // the fast path runs the connector; the winner's entry is installed and losers discard theirs
    // (~ShmFd closes the duplicate fd). So buildCount may be > 1 — only the invariants that matter
    // are checked: all callers get a usable entry (oks==N) and the pool ends with exactly one entry.
    EXPECT_EQ(oks.load(), N);
    EXPECT_EQ(pool.Size(), 1u);
    EXPECT_GE(stub.buildCount.load(), 1);
}

TEST(ShmConnectionPoolTest, DistinctWorkersGetDistinctEntries)
{
    StubConnector stub;
    ShmConnectionPool pool("host-sdk", [&stub](const datasystem::HostPort &a, ShmEntry &e) { return stub(a, e); });
    datasystem::HostPort wa, wb;
    ASSERT_TRUE(wa.ParseString("127.0.0.1:1").IsOk());
    ASSERT_TRUE(wb.ParseString("127.0.0.1:2").IsOk());
    const ShmEntry *a = pool.GetOrCreateShm(wa);
    const ShmEntry *b = pool.GetOrCreateShm(wb);
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_NE(a->socketFd.Get(), b->socketFd.Get()) << "different workers get independent fds";
    EXPECT_EQ(pool.Size(), 2u);
}

TEST(ShmConnectionPoolTest, FailedBuildLeavesNoEntry)
{
    ShmConnectionPool pool("host-sdk", [](const datasystem::HostPort &, ShmEntry &) {
        return datasystem::Status(datasystem::StatusCode::K_RUNTIME_ERROR, "fail");
    });
    datasystem::HostPort w;
    ASSERT_TRUE(w.ParseString("127.0.0.1:9").IsOk());
    EXPECT_EQ(pool.GetOrCreateShm(w), nullptr);
    EXPECT_EQ(pool.Size(), 0u) << "no half-built entry left after failure (review fix #1)";
}

TEST(ShmConnectionPoolTest, ReleaseShmIdempotentAndClears)
{
    StubConnector stub;
    ShmConnectionPool pool("host-sdk", [&stub](const datasystem::HostPort &a, ShmEntry &e) { return stub(a, e); });
    datasystem::HostPort w;
    ASSERT_TRUE(w.ParseString("127.0.0.1:77").IsOk());
    ASSERT_NE(pool.GetOrCreateShm(w), nullptr);
    EXPECT_EQ(pool.Size(), 1u);
    pool.ReleaseShm(w);
    EXPECT_EQ(pool.Size(), 0u);
    pool.ReleaseShm(w); // idempotent, no crash
    EXPECT_EQ(pool.Size(), 0u);
}

TEST(ShmConnectionPoolTest, ReleaseAllClearsEverything)
{
    StubConnector stub;
    ShmConnectionPool pool("host-sdk", [&stub](const datasystem::HostPort &a, ShmEntry &e) { return stub(a, e); });
    datasystem::HostPort w;
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(w.ParseString("127.0.0.1:" + std::to_string(1000 + i)).IsOk());
        ASSERT_NE(pool.GetOrCreateShm(w), nullptr);
    }
    EXPECT_EQ(pool.Size(), 5u);
    pool.ReleaseAll();
    EXPECT_EQ(pool.Size(), 0u);
}

TEST(ShmConnectionPoolTest, GetShmEntryAfterReleaseIsNull)
{
    StubConnector stub;
    ShmConnectionPool pool("host-sdk", [&stub](const datasystem::HostPort &a, ShmEntry &e) { return stub(a, e); });
    datasystem::HostPort w;
    ASSERT_TRUE(w.ParseString("127.0.0.1:88").IsOk());
    ASSERT_NE(pool.GetOrCreateShm(w), nullptr);
    ASSERT_NE(pool.GetShmEntry(w), nullptr);
    pool.ReleaseShm(w);
    EXPECT_EQ(pool.GetShmEntry(w), nullptr) << "after release, lookup returns nullptr";
}

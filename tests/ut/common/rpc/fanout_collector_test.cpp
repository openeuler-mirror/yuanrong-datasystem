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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "datasystem/common/rpc/fanout_collector.h"

namespace datasystem {
namespace {
HostPort Peer(int port)
{
    return HostPort("127.0.0.1", port);
}

Status TryAgain()
{
    return Status(K_TRY_AGAIN, "not ready");
}

Status RuntimeError(const std::string &msg)
{
    return Status(K_RUNTIME_ERROR, msg);
}

const FanoutPeerResult *FindResult(const std::vector<FanoutPeerResult> &results, int64_t tagId)
{
    for (const auto &result : results) {
        if (result.tagId == tagId) {
            return &result;
        }
    }
    return nullptr;
}
}  // namespace

TEST(FanoutCollectorTest, AddPeerRejectsInvalidAndDuplicates)
{
    FanoutCollector collector;
    EXPECT_EQ(collector.AddPeer(HostPort(), 1).GetCode(), K_INVALID);
    EXPECT_EQ(collector.AddPeer(Peer(1), 0).GetCode(), K_INVALID);

    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    EXPECT_EQ(collector.AddPeer(Peer(1), 2).GetCode(), K_INVALID);
    EXPECT_EQ(collector.AddPeer(Peer(2), 1).GetCode(), K_INVALID);
    EXPECT_EQ(collector.Size(), 1ul);
    EXPECT_EQ(collector.PendingCount(), 1ul);
    EXPECT_EQ(collector.OutstandingCount(), 1ul);
}

TEST(FanoutCollectorTest, PollOnceTracksPartialReadyAndFirstError)
{
    FanoutCollector collector;
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    ASSERT_TRUE(collector.AddPeer(Peer(2), 2).IsOk());
    ASSERT_TRUE(collector.AddPeer(Peer(3), 3).IsOk());
    int tag2Reads = 0;

    auto readFn = [&tag2Reads](int64_t tagId) {
        if (tagId == 1) {
            return Status::OK();
        }
        if (tagId == 2) {
            ++tag2Reads;
            return tag2Reads == 1 ? TryAgain() : Status::OK();
        }
        return RuntimeError("worker failed");
    };

    ASSERT_TRUE(collector.PollOnce(readFn).IsOk());
    EXPECT_FALSE(collector.Done());
    EXPECT_EQ(collector.PendingCount(), 1ul);
    EXPECT_EQ(collector.AggregateStatus().GetCode(), K_TRY_AGAIN);

    auto results = collector.Results();
    auto result1 = FindResult(results, 1);
    auto result2 = FindResult(results, 2);
    auto result3 = FindResult(results, 3);
    ASSERT_NE(result1, nullptr);
    ASSERT_NE(result2, nullptr);
    ASSERT_NE(result3, nullptr);
    EXPECT_EQ(result1->state, FanoutPeerState::COMPLETED);
    EXPECT_EQ(result2->state, FanoutPeerState::PENDING);
    EXPECT_EQ(result3->state, FanoutPeerState::COMPLETED);
    EXPECT_EQ(result3->status.GetCode(), K_RUNTIME_ERROR);

    ASSERT_TRUE(collector.PollOnce(readFn).IsOk());
    EXPECT_TRUE(collector.Done());
    EXPECT_EQ(collector.PendingCount(), 0ul);
    EXPECT_EQ(collector.AggregateStatus().GetCode(), K_RUNTIME_ERROR);
}

TEST(FanoutCollectorTest, PollUntilDeadlineMarksAndCleansOutstandingTags)
{
    FanoutCollector collector(std::chrono::microseconds(100));
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    ASSERT_TRUE(collector.AddPeer(Peer(2), 2).IsOk());
    std::atomic<int> readCount{0};
    std::atomic<int> sleepCount{0};

    auto rc = collector.PollUntil(
        [&readCount](int64_t) {
            ++readCount;
            return TryAgain();
        },
        FanoutCollector::Clock::now() + std::chrono::milliseconds(2),
        [&sleepCount](std::chrono::microseconds duration) {
            if (duration > std::chrono::microseconds(0)) {
                ++sleepCount;
            }
            return Status::OK();
        });

    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_TRUE(collector.Done());
    EXPECT_EQ(collector.PendingCount(), 0ul);
    EXPECT_EQ(collector.OutstandingCount(), 2ul);
    EXPECT_GT(readCount.load(), 0);
    EXPECT_GT(sleepCount.load(), 0);

    auto results = collector.Results();
    auto result1 = FindResult(results, 1);
    auto result2 = FindResult(results, 2);
    ASSERT_NE(result1, nullptr);
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result1->state, FanoutPeerState::TIMED_OUT);
    EXPECT_EQ(result2->state, FanoutPeerState::TIMED_OUT);

    std::vector<int64_t> cleanedTags;
    ASSERT_TRUE(collector.CleanupOutstanding([&cleanedTags](int64_t tagId) {
        cleanedTags.push_back(tagId);
        return Status::OK();
    }).IsOk());
    EXPECT_EQ(cleanedTags.size(), 2ul);
    EXPECT_EQ(collector.OutstandingCount(), 0ul);

    results = collector.Results();
    result1 = FindResult(results, 1);
    result2 = FindResult(results, 2);
    ASSERT_NE(result1, nullptr);
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result1->state, FanoutPeerState::CLEANED);
    EXPECT_EQ(result2->state, FanoutPeerState::CLEANED);
    EXPECT_FALSE(result1->completed);
    EXPECT_TRUE(result1->cleaned);
    EXPECT_EQ(collector.AggregateStatus().GetCode(), K_RPC_DEADLINE_EXCEEDED);
}

TEST(FanoutCollectorTest, PollUntilRejectsLatePeersAfterCollectionStarts)
{
    FanoutCollector collector(std::chrono::microseconds(0));
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    bool lateAddChecked = false;

    auto rc = collector.PollUntil(
        [&collector, &lateAddChecked](int64_t) {
            lateAddChecked = true;
            EXPECT_EQ(collector.AddPeer(Peer(2), 2).GetCode(), K_INVALID);
            return TryAgain();
        },
        FanoutCollector::Clock::now(), [](std::chrono::microseconds) { return RuntimeError("sleep should not run"); });

    EXPECT_EQ(rc.GetCode(), K_RPC_DEADLINE_EXCEEDED);
    EXPECT_TRUE(lateAddChecked);
    EXPECT_EQ(collector.Size(), 1ul);
    EXPECT_EQ(collector.PendingCount(), 0ul);
}

TEST(FanoutCollectorTest, PollOnceRejectsLatePeersAfterCollectionStarts)
{
    FanoutCollector collector;
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    bool lateAddChecked = false;

    ASSERT_TRUE(collector.PollOnce([&collector, &lateAddChecked](int64_t) {
        lateAddChecked = true;
        EXPECT_EQ(collector.AddPeer(Peer(2), 2).GetCode(), K_INVALID);
        return TryAgain();
    }).IsOk());

    EXPECT_TRUE(lateAddChecked);
    EXPECT_EQ(collector.Size(), 1ul);
    EXPECT_EQ(collector.PendingCount(), 1ul);
}

TEST(FanoutCollectorTest, CleanupFailureCanBeRetried)
{
    FanoutCollector collector;
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    ASSERT_TRUE(collector.MarkDeadlineExceeded().IsOk());

    int attempts = 0;
    Status rc = collector.CleanupOutstanding([&attempts](int64_t) {
        ++attempts;
        return attempts == 1 ? RuntimeError("forget failed") : Status::OK();
    });
    EXPECT_EQ(rc.GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(collector.AggregateStatus().GetCode(), K_RUNTIME_ERROR);
    EXPECT_EQ(collector.OutstandingCount(), 1ul);
    auto results = collector.Results();
    auto result = FindResult(results, 1);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->state, FanoutPeerState::CLEANUP_FAILED);
    EXPECT_FALSE(result->cleaned);

    ASSERT_TRUE(collector.CleanupOutstanding([&attempts](int64_t) {
        ++attempts;
        return Status::OK();
    }).IsOk());
    EXPECT_EQ(collector.OutstandingCount(), 0ul);
    EXPECT_EQ(attempts, 2);
    results = collector.Results();
    result = FindResult(results, 1);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->state, FanoutPeerState::CLEANED);
    EXPECT_TRUE(result->cleaned);
}

TEST(FanoutCollectorTest, LateCompletionDoesNotOverwriteTimedOutPeer)
{
    FanoutCollector collector;
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    ASSERT_TRUE(collector.MarkDeadlineExceeded().IsOk());

    EXPECT_EQ(collector.CompletePeer(1, Status::OK()).GetCode(), K_TRY_AGAIN);
    auto results = collector.Results();
    auto result = FindResult(results, 1);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->state, FanoutPeerState::TIMED_OUT);
    EXPECT_EQ(result->status.GetCode(), K_RPC_DEADLINE_EXCEEDED);

    ASSERT_TRUE(collector.CleanupOutstanding([](int64_t) { return Status::OK(); }).IsOk());
    EXPECT_EQ(collector.CompletePeer(1, RuntimeError("late failure")).GetCode(), K_TRY_AGAIN);
    results = collector.Results();
    result = FindResult(results, 1);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->state, FanoutPeerState::CLEANED);
    EXPECT_EQ(result->status.GetCode(), K_RPC_DEADLINE_EXCEEDED);
}

TEST(FanoutCollectorTest, PollUntilAllCompleteDoesNotSleep)
{
    FanoutCollector collector;
    ASSERT_TRUE(collector.AddPeer(Peer(1), 1).IsOk());
    ASSERT_TRUE(collector.AddPeer(Peer(2), 2).IsOk());

    auto rc = collector.PollUntil(
        [](int64_t) { return Status::OK(); }, FanoutCollector::Clock::now() + std::chrono::seconds(1),
        [](std::chrono::microseconds) { return RuntimeError("sleep should not be called"); });

    EXPECT_TRUE(rc.IsOk());
    EXPECT_TRUE(collector.Done());
    EXPECT_EQ(collector.PendingCount(), 0ul);
    EXPECT_EQ(collector.OutstandingCount(), 0ul);
}

TEST(FanoutCollectorTest, ConcurrentCompletePeerIsSafe)
{
    static constexpr int kPeerCount = 64;
    static constexpr int kThreadCount = 8;
    FanoutCollector collector;
    for (int i = 1; i <= kPeerCount; ++i) {
        ASSERT_TRUE(collector.AddPeer(Peer(i), i).IsOk());
    }

    std::atomic<int> completed{0};
    std::vector<std::thread> threads;
    for (int threadId = 0; threadId < kThreadCount; ++threadId) {
        threads.emplace_back([threadId, &collector, &completed]() {
            for (int tagId = threadId + 1; tagId <= kPeerCount; tagId += kThreadCount) {
                if (collector.CompletePeer(tagId, Status::OK()).IsOk()) {
                    completed.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto &thread : threads) {
        thread.join();
    }

    EXPECT_EQ(completed.load(), kPeerCount);
    EXPECT_TRUE(collector.Done());
    EXPECT_TRUE(collector.AggregateStatus().IsOk());
    for (const auto &result : collector.Results()) {
        EXPECT_EQ(result.state, FanoutPeerState::COMPLETED);
        EXPECT_TRUE(result.completed);
    }
}

}  // namespace datasystem

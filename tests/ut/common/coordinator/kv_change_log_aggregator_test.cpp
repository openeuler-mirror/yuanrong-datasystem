/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Unit tests for the CLUSTER_KV_CHANGE log aggregator.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "datasystem/common/coordinator/kv_change_log_aggregator.h"

namespace datasystem {
namespace ut {
namespace {

class KvChangeLogAggregatorTest : public testing::Test {
protected:
    KvChangeLogAggregatorTest() : clock_(std::make_shared<SteadyClockMock>()), agg_(clock_) {}

    std::shared_ptr<SteadyClockMock> clock_;
    KvChangeLogAggregator agg_;
};

TEST_F(KvChangeLogAggregatorTest, SparseTrafficLogsEveryEventInDetail)
{
    constexpr uint64_t SPARSE_GAP_MS = 2000;  // far beyond the 1s window
    std::string summary;
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_TRUE(summary.empty());
    clock_->AdvanceMs(SPARSE_GAP_MS);
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    // The closed window held a single, fully reported event; a summary
    // would be 100% redundant, so none is emitted -- sparse traffic stays
    // at exactly one line per event.
    ASSERT_TRUE(summary.empty());
}

TEST_F(KvChangeLogAggregatorTest, StormSuppressesAllButFirstPerGroup)
{
    constexpr int SUPPRESSED_EVENTS = 99;
    std::string summary;
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    for (int i = 0; i < SUPPRESSED_EVENTS; i++) {
        ASSERT_FALSE(agg_.Record(0, "/cluster", summary));
    }
    ASSERT_TRUE(summary.empty());
    clock_->AdvanceMs(KvChangeLogAggregator::DEFAULT_WINDOW_MS);
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_EQ(summary, "CLUSTER_KV_CHANGE backend=coordinator action=summary window_ms=1000 total=100 "
                       "groups=0 /cluster=100");
}

TEST_F(KvChangeLogAggregatorTest, DistinctGroupsTrackIndependently)
{
    constexpr uint64_t MIXED_WINDOW_MS = 1500;
    std::string summary;
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_TRUE(agg_.Record(1, "/cluster", summary));  // different event type
    ASSERT_TRUE(agg_.Record(0, "/tasks/migrate", summary));
    ASSERT_FALSE(agg_.Record(0, "/cluster", summary));  // second in group
    ASSERT_TRUE(summary.empty());
    clock_->AdvanceMs(MIXED_WINDOW_MS);
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_EQ(summary, "CLUSTER_KV_CHANGE backend=coordinator action=summary window_ms=1500 total=4 "
                       "groups=0 /cluster=2,0 /tasks/migrate=1,1 /cluster=1");
}

TEST_F(KvChangeLogAggregatorTest, ConcurrentRecordsPreserveTotals)
{
    constexpr int THREADS = 4;
    constexpr int PER_THREAD = 500;
    constexpr uint64_t ROLLOVER_MS = 2000;
    static constexpr char TOTAL_TAG[] = "total=";
    constexpr size_t TOTAL_TAG_LEN = sizeof(TOTAL_TAG) - 1;
    std::atomic<uint64_t> summaryTotals{ 0 };
    std::atomic<int> detailCount{ 0 };
    auto addTotalFromSummary = [&](const std::string &summary) {
        if (!summary.empty()) {
            auto pos = summary.find(TOTAL_TAG);
            summaryTotals += std::stoull(summary.substr(pos + TOTAL_TAG_LEN));
        }
    };
    std::vector<std::thread> workers;
    for (int t = 0; t < THREADS; t++) {
        workers.emplace_back([&, t] {
            for (int i = 0; i < PER_THREAD; i++) {
                std::string summary;
                if (agg_.Record(t % 2, "/cluster", summary)) {
                    detailCount.fetch_add(1);
                }
                addTotalFromSummary(summary);
            }
        });
    }
    for (auto &w : workers) {
        w.join();
    }
    // Exactly one detail line per group in the single window, regardless of
    // which thread won each first-of-group race.
    ASSERT_EQ(detailCount.load(), 2);
    // The mock clock never advances during the storm, so the single window
    // holds all records; one rollover flushes exactly that total.
    std::string summary;
    clock_->AdvanceMs(ROLLOVER_MS);
    agg_.Record(0, "/cluster", summary);
    addTotalFromSummary(summary);
    ASSERT_EQ(summaryTotals.load(), static_cast<uint64_t>(THREADS * PER_THREAD));
}

TEST_F(KvChangeLogAggregatorTest, WindowSequenceAndGuardEdges)
{
    constexpr uint64_t SPARSE_GAP_MS = 2000;
    constexpr uint64_t JUST_UNDER_WINDOW_MS = 999;
    std::string summary;
    // windowMs=0 falls back to the 1s default: 999ms stays in-window
    // (second event suppressed), and crossing 1000ms flushes.
    KvChangeLogAggregator zero(clock_, 0);
    ASSERT_TRUE(zero.Record(0, "/cluster", summary));
    clock_->AdvanceMs(JUST_UNDER_WINDOW_MS);
    ASSERT_FALSE(zero.Record(0, "/cluster", summary));
    ASSERT_TRUE(summary.empty());
    clock_->AdvanceMs(1);
    ASSERT_TRUE(zero.Record(0, "/cluster", summary));
    ASSERT_FALSE(summary.empty());

    // Window sequence: suppressed window -> sparse window -> all-singleton
    // window; only the first one emits a summary.
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_FALSE(agg_.Record(0, "/cluster", summary));
    clock_->AdvanceMs(SPARSE_GAP_MS);
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_TRUE(summary.find("total=2") != std::string::npos);
    clock_->AdvanceMs(SPARSE_GAP_MS);
    ASSERT_TRUE(agg_.Record(1, "/notify", summary));
    ASSERT_TRUE(summary.empty());
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    clock_->AdvanceMs(SPARSE_GAP_MS);
    ASSERT_TRUE(agg_.Record(0, "/cluster", summary));
    ASSERT_TRUE(summary.empty());
}
}  // namespace
}  // namespace ut
}  // namespace datasystem

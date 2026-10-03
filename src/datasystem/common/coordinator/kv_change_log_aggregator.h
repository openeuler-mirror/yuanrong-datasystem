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
 * Description: Bounded aggregation for CLUSTER_KV_CHANGE diagnostics.
 */
#ifndef DATASYSTEM_COMMON_COORDINATOR_KV_CHANGE_LOG_AGGREGATOR_H
#define DATASYSTEM_COMMON_COORDINATOR_KV_CHANGE_LOG_AGGREGATOR_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "datasystem/common/coordinator/steady_clock.h"

#include <bthread/mutex.h>

namespace datasystem {

/**
 * @brief Aggregates CLUSTER_KV_CHANGE into one summary line per time window
 * instead of one INFO line per event (storm peak 16k lines/s, 99.9% of
 * coordinator INFO volume, a main driver of the 4-core quota exhaustion).
 * Sparse traffic is unchanged: the first event per (type, table) group keeps
 * its detail line, and a window with no suppressed detail emits no summary.
 */
class KvChangeLogAggregator {
public:
    static constexpr uint64_t DEFAULT_WINDOW_MS = 1000;

    explicit KvChangeLogAggregator(std::shared_ptr<SteadyClock> clock, uint64_t windowMs = DEFAULT_WINDOW_MS)
        : clock_(std::move(clock)), windowMs_(windowMs > 0 ? windowMs : DEFAULT_WINDOW_MS)
    {
    }

    ~KvChangeLogAggregator() = default;

    /**
     * @brief Record one committed KV change.
     * @param[out] summary The closed window's aggregated line, set only when
     * that window suppressed detail lines (some group had more than one
     * event).
     * @return True when the caller should emit the detail line for this
     * event (first of its group in the current window).
     */
    bool Record(int eventType, const std::string &table, std::string &summary)
    {
        summary.clear();
        std::map<std::string, uint64_t> flushedCounts;
        uint64_t flushedTotal = 0;
        uint64_t flushedElapsedMs = 0;
        bool detail = false;
        {
            // Callers are concurrent brpc worker bthreads plus the TTL
            // manager thread; bthread::Mutex parks only the calling bthread.
            std::lock_guard<bthread::Mutex> lock(mutex_);
            const uint64_t nowMs = clock_->NowMs();
            if (window_.total == 0) {
                window_.startMs = nowMs;
            } else if (nowMs - window_.startMs >= windowMs_) {
                flushedCounts = std::move(window_.groupCounts);
                flushedTotal = window_.total;
                flushedElapsedMs = nowMs - window_.startMs;
                window_ = WindowState{};
                window_.startMs = nowMs;
            }
            detail = ++window_.groupCounts[GroupKey(eventType, table)] == 1;
            window_.total++;
        }
        // Summary only when it replaces suppressed details (more events than
        // groups); flushedTotal comes from the window state, no re-count.
        if (flushedTotal > flushedCounts.size()) {
            summary = FormatSummary(flushedCounts, flushedElapsedMs, flushedTotal);
        }
        return detail;
    }

private:
    struct WindowState {
        uint64_t startMs = 0;
        uint64_t total = 0;
        std::map<std::string, uint64_t> groupCounts;
    };

    static std::string GroupKey(int eventType, const std::string &table)
    {
        return std::to_string(eventType) + " " + table;
    }

    static std::string FormatSummary(const std::map<std::string, uint64_t> &counts, uint64_t elapsedMs,
                                     uint64_t total)
    {
        std::string groups;
        for (const auto &entry : counts) {
            if (!groups.empty()) {
                groups += ",";
            }
            groups += entry.first + "=" + std::to_string(entry.second);
        }
        return "CLUSTER_KV_CHANGE backend=coordinator action=summary window_ms=" + std::to_string(elapsedMs)
            + " total=" + std::to_string(total) + " groups=" + groups;
    }

    std::shared_ptr<SteadyClock> clock_;
    const uint64_t windowMs_;
    bthread::Mutex mutex_;
    WindowState window_;
};

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_KV_CHANGE_LOG_AGGREGATOR_H

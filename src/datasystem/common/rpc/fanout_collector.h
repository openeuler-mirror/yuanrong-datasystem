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
 * Description: Deadline-bound fanout RPC result collector.
 */
#ifndef DATASYSTEM_COMMON_RPC_FANOUT_COLLECTOR_H
#define DATASYSTEM_COMMON_RPC_FANOUT_COLLECTOR_H

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include <bthread/mutex.h>

#include "datasystem/common/util/net_util.h"
#include "datasystem/utils/status.h"

namespace datasystem {

enum class FanoutPeerState {
    PENDING = 0,
    COMPLETED,
    TIMED_OUT,
    CLEANED,
    CLEANUP_FAILED,
};

struct FanoutPeerResult {
    HostPort peer;
    int64_t tagId = -1;
    Status status;
    Status cleanupStatus;
    FanoutPeerState state = FanoutPeerState::PENDING;
    bool completed = false;
    bool cleaned = false;
};

class FanoutCollector {
public:
    using Clock = std::chrono::steady_clock;
    using ReadFn = std::function<Status(int64_t tagId)>;
    using CleanupFn = std::function<Status(int64_t tagId)>;
    using SleepFn = std::function<Status(std::chrono::microseconds duration)>;

    explicit FanoutCollector(std::chrono::microseconds pollInterval = std::chrono::milliseconds(1));
    ~FanoutCollector() = default;

    FanoutCollector(const FanoutCollector &) = delete;
    FanoutCollector &operator=(const FanoutCollector &) = delete;

    static Status BthreadSleepFor(std::chrono::microseconds duration);
    static const char *StateName(FanoutPeerState state);

    Status AddPeer(const HostPort &peer, int64_t tagId);
    Status CompletePeer(int64_t tagId, const Status &status);
    Status PollOnce(const ReadFn &readFn);
    Status PollUntil(const ReadFn &readFn, Clock::time_point deadline, SleepFn sleepFn = BthreadSleepFor);
    Status MarkDeadlineExceeded();
    Status CleanupOutstanding(const CleanupFn &cleanupFn);

    bool Done() const;
    size_t PendingCount() const;
    size_t OutstandingCount() const;
    size_t Size() const;
    Status AggregateStatus() const;
    std::vector<FanoutPeerResult> Results() const;

private:
    std::vector<int64_t> PendingTagsSnapshot();
    std::vector<int64_t> OutstandingTagsSnapshot();
    Status CompletePeerLocked(int64_t tagId, const Status &status);
    Status CleanupPeerLocked(int64_t tagId, const Status &cleanupStatus);
    Status MarkDeadlineExceededLocked();
    Status MarkDeadlineExceededAndAggregate();
    Status AggregateStatusLocked() const;
    std::string SummaryLocked() const;
    FanoutPeerResult *FindByTagLocked(int64_t tagId);
    const FanoutPeerResult *FindByTagLocked(int64_t tagId) const;
    void SealLocked();
    static bool IsTerminal(FanoutPeerState state);
    static bool NeedsCleanup(FanoutPeerState state);

    const std::chrono::microseconds pollInterval_;
    mutable bthread::Mutex mutex_;
    std::vector<FanoutPeerResult> peers_;
    std::map<HostPort, size_t> peerIndex_;
    std::map<int64_t, size_t> tagIndex_;
    bool sealed_ = false;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_FANOUT_COLLECTOR_H

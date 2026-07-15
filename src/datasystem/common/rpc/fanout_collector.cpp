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

#include "datasystem/common/rpc/fanout_collector.h"

#include <algorithm>
#include <mutex>
#include <sstream>
#include <utility>

#include <bthread/bthread.h>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace {
Status MakeStatus(StatusCode code, int line, const std::string &msg)
{
    return Status(code, line, __FILE__, msg);
}
}  // namespace

FanoutCollector::FanoutCollector(std::chrono::microseconds pollInterval)
    : pollInterval_(std::max(pollInterval, std::chrono::microseconds(0)))
{
}

Status FanoutCollector::AddPeer(const HostPort &peer, int64_t tagId)
{
    if (peer.Empty()) {
        return MakeStatus(K_INVALID, __LINE__, "fanout peer is empty");
    }
    if (tagId <= 0) {
        return MakeStatus(K_INVALID, __LINE__, "fanout tag must be positive");
    }

    std::lock_guard<bthread::Mutex> lock(mutex_);
    if (sealed_) {
        return MakeStatus(K_INVALID, __LINE__, "fanout collector is sealed");
    }
    if (peerIndex_.find(peer) != peerIndex_.end()) {
        return MakeStatus(K_INVALID, __LINE__, "duplicate fanout peer " + peer.ToString());
    }
    if (tagIndex_.find(tagId) != tagIndex_.end()) {
        return MakeStatus(K_INVALID, __LINE__, "duplicate fanout tag " + std::to_string(tagId));
    }

    FanoutPeerResult result;
    result.peer = peer;
    result.tagId = tagId;
    const size_t index = peers_.size();
    peers_.emplace_back(std::move(result));
    peerIndex_.emplace(peer, index);
    tagIndex_.emplace(tagId, index);
    return Status::OK();
}

Status FanoutCollector::CompletePeer(int64_t tagId, const Status &status)
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return CompletePeerLocked(tagId, status);
}

Status FanoutCollector::PollOnce(const ReadFn &readFn)
{
    if (!readFn) {
        return MakeStatus(K_INVALID, __LINE__, "fanout read function is null");
    }

    auto tags = PendingTagsSnapshot();
    for (auto tagId : tags) {
        Status rc = readFn(tagId);
        if (rc.GetCode() == K_TRY_AGAIN) {
            continue;
        }
        Status completeRc = CompletePeer(tagId, rc);
        if (completeRc.IsError() && completeRc.GetCode() != K_TRY_AGAIN) {
            return completeRc;
        }
    }
    return Status::OK();
}

Status FanoutCollector::PollUntil(const ReadFn &readFn, Clock::time_point deadline, SleepFn sleepFn)
{
    if (!sleepFn) {
        return MakeStatus(K_INVALID, __LINE__, "fanout sleep function is null");
    }

    {
        std::lock_guard<bthread::Mutex> lock(mutex_);
        SealLocked();
    }

    while (!Done()) {
        RETURN_IF_NOT_OK(PollOnce(readFn));
        if (Done()) {
            break;
        }

        auto now = Clock::now();
        if (now >= deadline) {
            return MarkDeadlineExceededAndAggregate();
        }

        auto sleepFor = std::chrono::duration_cast<std::chrono::microseconds>(deadline - now);
        if (pollInterval_ > std::chrono::microseconds(0) && pollInterval_ < sleepFor) {
            sleepFor = pollInterval_;
        }
        RETURN_IF_NOT_OK(sleepFn(sleepFor));
    }
    return AggregateStatus();
}

Status FanoutCollector::MarkDeadlineExceededAndAggregate()
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    RETURN_IF_NOT_OK(MarkDeadlineExceededLocked());
    return AggregateStatusLocked();
}

Status FanoutCollector::MarkDeadlineExceeded()
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return MarkDeadlineExceededLocked();
}

Status FanoutCollector::CleanupOutstanding(const CleanupFn &cleanupFn)
{
    if (!cleanupFn) {
        return MakeStatus(K_INVALID, __LINE__, "fanout cleanup function is null");
    }

    Status firstError;
    auto tags = OutstandingTagsSnapshot();
    for (auto tagId : tags) {
        Status cleanupRc = cleanupFn(tagId);
        Status updateRc;
        {
            std::lock_guard<bthread::Mutex> lock(mutex_);
            updateRc = CleanupPeerLocked(tagId, cleanupRc);
        }
        if (firstError.IsOk() && cleanupRc.IsError()) {
            firstError = cleanupRc;
        }
        if (firstError.IsOk() && updateRc.IsError() && updateRc.GetCode() != K_TRY_AGAIN) {
            firstError = updateRc;
        }
    }
    return firstError;
}

bool FanoutCollector::Done() const
{
    return PendingCount() == 0;
}

size_t FanoutCollector::PendingCount() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    size_t count = 0;
    for (const auto &peer : peers_) {
        if (peer.state == FanoutPeerState::PENDING) {
            ++count;
        }
    }
    return count;
}

size_t FanoutCollector::OutstandingCount() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    size_t count = 0;
    for (const auto &peer : peers_) {
        if (NeedsCleanup(peer.state)) {
            ++count;
        }
    }
    return count;
}

size_t FanoutCollector::Size() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return peers_.size();
}

Status FanoutCollector::AggregateStatus() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return AggregateStatusLocked();
}

std::vector<FanoutPeerResult> FanoutCollector::Results() const
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    return peers_;
}

Status FanoutCollector::BthreadSleepFor(std::chrono::microseconds duration)
{
    if (duration <= std::chrono::microseconds(0)) {
        return Status::OK();
    }
    int ret = bthread_usleep(duration.count());
    if (ret != 0) {
        return MakeStatus(K_INTERRUPTED, __LINE__, "bthread_usleep interrupted");
    }
    return Status::OK();
}

const char *FanoutCollector::StateName(FanoutPeerState state)
{
    switch (state) {
        case FanoutPeerState::PENDING:
            return "PENDING";
        case FanoutPeerState::COMPLETED:
            return "COMPLETED";
        case FanoutPeerState::TIMED_OUT:
            return "TIMED_OUT";
        case FanoutPeerState::CLEANED:
            return "CLEANED";
        case FanoutPeerState::CLEANUP_FAILED:
            return "CLEANUP_FAILED";
        default:
            return "UNKNOWN";
    }
}

std::vector<int64_t> FanoutCollector::PendingTagsSnapshot()
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    SealLocked();
    std::vector<int64_t> tags;
    tags.reserve(peers_.size());
    for (const auto &peer : peers_) {
        if (peer.state == FanoutPeerState::PENDING) {
            tags.push_back(peer.tagId);
        }
    }
    return tags;
}

std::vector<int64_t> FanoutCollector::OutstandingTagsSnapshot()
{
    std::lock_guard<bthread::Mutex> lock(mutex_);
    SealLocked();
    std::vector<int64_t> tags;
    tags.reserve(peers_.size());
    for (const auto &peer : peers_) {
        if (NeedsCleanup(peer.state)) {
            tags.push_back(peer.tagId);
        }
    }
    return tags;
}

Status FanoutCollector::CompletePeerLocked(int64_t tagId, const Status &status)
{
    auto *peer = FindByTagLocked(tagId);
    if (peer == nullptr) {
        return MakeStatus(K_NOT_FOUND, __LINE__, "fanout tag not found " + std::to_string(tagId));
    }
    if (IsTerminal(peer->state)) {
        return MakeStatus(K_TRY_AGAIN, __LINE__, "fanout tag already terminal " + std::to_string(tagId));
    }

    peer->status = status;
    peer->state = FanoutPeerState::COMPLETED;
    peer->completed = true;
    return Status::OK();
}

Status FanoutCollector::CleanupPeerLocked(int64_t tagId, const Status &cleanupStatus)
{
    auto *peer = FindByTagLocked(tagId);
    if (peer == nullptr) {
        return MakeStatus(K_NOT_FOUND, __LINE__, "fanout tag not found " + std::to_string(tagId));
    }
    if (!NeedsCleanup(peer->state)) {
        return MakeStatus(K_TRY_AGAIN, __LINE__, "fanout tag does not need cleanup " + std::to_string(tagId));
    }

    peer->cleanupStatus = cleanupStatus;
    if (peer->state == FanoutPeerState::PENDING) {
        peer->status = MakeStatus(K_RPC_CANCELLED, __LINE__, "fanout peer cleaned before completion");
    }
    if (cleanupStatus.IsError()) {
        peer->state = FanoutPeerState::CLEANUP_FAILED;
        peer->cleaned = false;
        return cleanupStatus;
    }
    peer->state = FanoutPeerState::CLEANED;
    peer->cleaned = true;
    return Status::OK();
}

Status FanoutCollector::MarkDeadlineExceededLocked()
{
    SealLocked();
    for (auto &peer : peers_) {
        if (peer.state != FanoutPeerState::PENDING) {
            continue;
        }
        peer.status = MakeStatus(K_RPC_DEADLINE_EXCEEDED, __LINE__,
                                 "fanout peer timed out before completion");
        peer.state = FanoutPeerState::TIMED_OUT;
    }
    return Status::OK();
}

Status FanoutCollector::AggregateStatusLocked() const
{
    if (peers_.empty()) {
        return Status::OK();
    }

    for (const auto &peer : peers_) {
        if (peer.state == FanoutPeerState::PENDING) {
            return MakeStatus(K_TRY_AGAIN, __LINE__, SummaryLocked());
        }
    }
    for (const auto &peer : peers_) {
        if (peer.state == FanoutPeerState::CLEANUP_FAILED && peer.cleanupStatus.IsError()) {
            Status rc = peer.cleanupStatus;
            rc.AppendMsg(SummaryLocked());
            return rc;
        }
    }
    for (const auto &peer : peers_) {
        if (peer.status.IsError()) {
            Status rc = peer.status;
            rc.AppendMsg(SummaryLocked());
            return rc;
        }
    }
    return Status::OK();
}

std::string FanoutCollector::SummaryLocked() const
{
    size_t pending = 0;
    size_t completed = 0;
    size_t timedOut = 0;
    size_t cleaned = 0;
    size_t cleanupFailed = 0;
    for (const auto &peer : peers_) {
        switch (peer.state) {
            case FanoutPeerState::PENDING:
                ++pending;
                break;
            case FanoutPeerState::COMPLETED:
                ++completed;
                break;
            case FanoutPeerState::TIMED_OUT:
                ++timedOut;
                break;
            case FanoutPeerState::CLEANED:
                ++cleaned;
                break;
            case FanoutPeerState::CLEANUP_FAILED:
                ++cleanupFailed;
                break;
            default:
                break;
        }
    }

    std::ostringstream oss;
    oss << "fanout summary total=" << peers_.size() << ", pending=" << pending << ", completed=" << completed
        << ", timed_out=" << timedOut << ", cleaned=" << cleaned << ", cleanup_failed=" << cleanupFailed;
    return oss.str();
}

FanoutPeerResult *FanoutCollector::FindByTagLocked(int64_t tagId)
{
    auto iter = tagIndex_.find(tagId);
    if (iter == tagIndex_.end()) {
        return nullptr;
    }
    return &peers_[iter->second];
}

const FanoutPeerResult *FanoutCollector::FindByTagLocked(int64_t tagId) const
{
    auto iter = tagIndex_.find(tagId);
    if (iter == tagIndex_.end()) {
        return nullptr;
    }
    return &peers_[iter->second];
}

void FanoutCollector::SealLocked()
{
    sealed_ = true;
}

bool FanoutCollector::IsTerminal(FanoutPeerState state)
{
    return state == FanoutPeerState::COMPLETED || state == FanoutPeerState::TIMED_OUT
           || state == FanoutPeerState::CLEANED || state == FanoutPeerState::CLEANUP_FAILED;
}

bool FanoutCollector::NeedsCleanup(FanoutPeerState state)
{
    return state == FanoutPeerState::PENDING || state == FanoutPeerState::TIMED_OUT
           || state == FanoutPeerState::CLEANUP_FAILED;
}

}  // namespace datasystem

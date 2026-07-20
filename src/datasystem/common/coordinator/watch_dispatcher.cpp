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
 * Description: Watch dispatcher for coordinator store.
 */
#include "datasystem/common/coordinator/watch_dispatcher.h"

#include <algorithm>
#include <chrono>
#include <iterator>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/uuid_generator.h"

DS_DEFINE_int32(watch_event_dispatch_thread, 4, "Number of coordinator watch event dispatch threads");

namespace datasystem {
constexpr size_t DEFAULT_DISPATCH_THREAD_COUNT = 4;
constexpr size_t MAX_PENDING_EVENTS = 10000;
constexpr uint64_t DROP_LOG_INTERVAL = 1024;
constexpr int64_t INITIAL_RETRY_DELAY_MS = 100;
constexpr int64_t MAX_RETRY_DELAY_MS = 5000;
constexpr int64_t RETRY_DELAY_MULTIPLIER = 2;
constexpr int64_t RETRY_JITTER_PERCENT = 20;
constexpr int64_t PERCENT_SCALE = 100;

/**
 * @brief Build the payload-free lifecycle event used to rebuild an overflowed watch.
 * @param[in] overflowEvent Event whose revision identifies the overflow point.
 * @return RESET source event for the worker watch stream.
 */
std::shared_ptr<WatchEvent> MakeRewatchEvent(const std::shared_ptr<WatchEvent> &overflowEvent)
{
    auto rewatchEvent = std::make_shared<WatchEvent>();
    rewatchEvent->type = WatchEvent::Type::REWATCH;
    if (overflowEvent != nullptr) {
        rewatchEvent->revision = overflowEvent->revision;
    }
    return rewatchEvent;
}

WatchDispatcher::WatchDispatcher(WatchRegistry *watchRegistry) : watchRegistry_(watchRegistry)
{
}

WatchDispatcher::~WatchDispatcher()
{
    Stop();
}

void WatchDispatcher::Enqueue(std::shared_ptr<WatchEvent> event)
{
    bool wasEmpty = false;
    {
        std::lock_guard<std::mutex> lock(pendingMutex_);
        if (pendingQueue_.size() >= MAX_PENDING_EVENTS) {
            const uint64_t dropped = droppedPendingEvents_.fetch_add(1, std::memory_order_relaxed) + 1;
            pendingOverflow_.store(true, std::memory_order_release);
            if (dropped == 1 || dropped % DROP_LOG_INTERVAL == 0) {
                LOG(WARNING) << "CLUSTER_WATCH_PENDING_FULL droppedEvents=" << dropped;
            }
            return;
        }
        wasEmpty = pendingQueue_.empty();
        VLOG(1) << "Enqueue event, revision: " << event->revision;
        pendingQueue_.push_back(std::move(event));
    }
    if (wasEmpty) {
        pendingEmptyCv_.notify_one();
    }
}

size_t WatchDispatcher::GetPendingEventCount()
{
    std::lock_guard<std::mutex> lock(pendingMutex_);
    return pendingQueue_.size();
}

void WatchDispatcher::AddChannel(int64_t watchId, const std::string &watcherAddr)
{
    auto channel = std::make_shared<WatcherChannel>();
    channel->watchId = watchId;
    channel->watcherAddr = watcherAddr;

    {
        std::unique_lock<std::shared_mutex> lock(channelsMutex_);
        auto oldChannel = channels_.find(watchId);
        if (oldChannel != channels_.end()) {
            CancelChannel(oldChannel->second);
            RemoveReverseIndexLocked(oldChannel->second);
        }
        if (!dispatchThreadPool_.empty()) {
            channel->assignedThread = static_cast<size_t>(watchId) % dispatchThreadPool_.size();
        }
        channels_[watchId] = channel;
        watchIdsByWatcher_[watcherAddr].insert(watchId);
    }
}

void WatchDispatcher::RemoveChannel(int64_t watchId)
{
    std::unique_lock<std::shared_mutex> lock(channelsMutex_);
    auto it = channels_.find(watchId);
    if (it == channels_.end()) {
        return;
    }
    CancelChannel(it->second);
    RemoveReverseIndexLocked(it->second);
    channels_.erase(it);
}

void WatchDispatcher::RemoveChannelsByWatcher(const std::string &watcherAddr)
{
    std::vector<int64_t> watchIds;
    {
        std::shared_lock<std::shared_mutex> lock(channelsMutex_);
        auto reverseIt = watchIdsByWatcher_.find(watcherAddr);
        if (reverseIt == watchIdsByWatcher_.end()) {
            return;
        }
        watchIds.assign(reverseIt->second.begin(), reverseIt->second.end());
    }
    if (watchRegistry_ != nullptr) {
        for (int64_t watchId : watchIds) {
            Status status = watchRegistry_->Cancel(watchId, watcherAddr);
            if (status.IsError() && status.GetCode() != StatusCode::K_NOT_FOUND) {
                const uint64_t failureCount = cancelFailures_.fetch_add(1, std::memory_order_relaxed) + 1;
                if (failureCount == 1 || failureCount % DROP_LOG_INTERVAL == 0) {
                    LOG(WARNING) << "CLUSTER_WATCH_CANCEL_FAILED watchId=" << watchId
                                 << ", watcherAddr=" << watcherAddr << ", failureCount=" << failureCount
                                 << ", status=" << status.ToString();
                }
            }
        }
    }
    std::unique_lock<std::shared_mutex> lock(channelsMutex_);
    for (int64_t watchId : watchIds) {
        auto channel = channels_.find(watchId);
        if (channel == channels_.end()) {
            continue;
        }
        CancelChannel(channel->second);
        RemoveReverseIndexLocked(channel->second);
        channels_.erase(channel);
    }
}

void WatchDispatcher::CancelChannel(const std::shared_ptr<WatcherChannel> &channel)
{
    channel->cancelled.store(true, std::memory_order_release);
    std::lock_guard<std::mutex> lock(channel->mutex);
    channel->queue.clear();
    channel->dispatchQueued = false;
    channel->retryDelayMs = 0;
}

void WatchDispatcher::RemoveReverseIndexLocked(const std::shared_ptr<WatcherChannel> &channel)
{
    auto reverseIt = watchIdsByWatcher_.find(channel->watcherAddr);
    if (reverseIt == watchIdsByWatcher_.end()) {
        return;
    }
    reverseIt->second.erase(channel->watchId);
    if (reverseIt->second.empty()) {
        watchIdsByWatcher_.erase(reverseIt);
    }
}

void WatchDispatcher::SetSnapshotRevision(int64_t watchId, int64_t revision)
{
    std::shared_lock<std::shared_mutex> lock(channelsMutex_);
    auto it = channels_.find(watchId);
    if (it == channels_.end()) {
        return;
    }
    auto &channel = it->second;
    {
        std::lock_guard<std::mutex> chLock(channel->mutex);
        channel->snapshotRevision = revision;
        if (!channel->queue.empty()) {
            ScheduleChannelLocked(channel);
        }
    }
}

void WatchDispatcher::Start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;
    }

    size_t dispatchThreadCount = DEFAULT_DISPATCH_THREAD_COUNT;
    if (FLAGS_watch_event_dispatch_thread > 0) {
        dispatchThreadCount = static_cast<size_t>(FLAGS_watch_event_dispatch_thread);
    }
    for (size_t i = 0; i < dispatchThreadCount; ++i) {
        auto dt = std::make_unique<DispatchThread>();
        dispatchThreadPool_.push_back(std::move(dt));
    }
    for (size_t i = 0; i < dispatchThreadPool_.size(); ++i) {
        dispatchThreadPool_[i]->thread = Thread(&WatchDispatcher::DispatchLoop, this, i);
    }
    fanOutThread_ = Thread(&WatchDispatcher::FanOutLoop, this);
}

void WatchDispatcher::Stop()
{
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false)) {
        return;
    }

    pendingEmptyCv_.notify_one();
    for (auto &dt : dispatchThreadPool_) {
        dt->cv.notify_one();
    }

    if (fanOutThread_.joinable()) {
        fanOutThread_.join();
    }
    for (auto &dt : dispatchThreadPool_) {
        if (dt->thread.joinable()) {
            dt->thread.join();
        }
    }
    dispatchThreadPool_.clear();
}

void WatchDispatcher::FanOutLoop()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID("CoordWF;" + GetStringUuid());
    while (running_.load()) {
        std::deque<std::shared_ptr<WatchEvent>> batch;
        {
            std::unique_lock<std::mutex> lock(pendingMutex_);
            pendingEmptyCv_.wait(lock, [this] { return !pendingQueue_.empty() || !running_.load(); });
            if (!running_.load() && pendingQueue_.empty()) {
                break;
            }
            auto count = std::min(MAX_WATCH_EVENTS_PER_BATCH, pendingQueue_.size());
            for (size_t i = 0; i < count; ++i) {
                batch.push_back(std::move(pendingQueue_.front()));
                pendingQueue_.pop_front();
            }
        }

        if (pendingOverflow_.exchange(false, std::memory_order_acq_rel)) {
            ScheduleAllChannelsForRewatch();
        }

        for (auto &event : batch) {
            std::vector<std::shared_ptr<WatcherEntry>> matched;
            watchRegistry_->MatchWatchers(event->entry.key, matched);

            std::shared_lock<std::shared_mutex> chLock(channelsMutex_);
            for (auto &watcher : matched) {
                auto it = channels_.find(watcher->watchId);
                if (it == channels_.end()) {
                    continue;
                }
                auto &channel = it->second;
                {
                    std::lock_guard<std::mutex> qLock(channel->mutex);
                    if (channel->cancelled.load(std::memory_order_acquire) || channel->needReWatch) {
                        continue;
                    }
                    if (channel->queue.size() < channel->backpressureLimit) {
                        VLOG(1) << "Enqueue event to channel, watchId: " << channel->watchId
                                << ", watcherAddr:" << channel->watcherAddr << ", revision: " << event->revision;
                        channel->queue.push_back(event);
                    } else {
                        auto rewatchEvent = MakeRewatchEvent(event);
                        channel->queue.clear();
                        channel->queue.push_back(std::move(rewatchEvent));
                        channel->needReWatch = true;
                        LOG(WARNING) << "CLUSTER_WATCH_BACKPRESSURE_OVERFLOW watchId="
                                     << channel->watchId << ", watcherAddr=" << channel->watcherAddr
                                     << ", backpressureLimit=" << channel->backpressureLimit
                                     << ", rewatchRevision=" << event->revision;
                    }
                    ScheduleChannelLocked(channel);
                }
            }
        }
    }
}

void WatchDispatcher::ScheduleChannelLocked(const std::shared_ptr<WatcherChannel> &channel)
{
    if (channel->cancelled.load(std::memory_order_acquire) || channel->dispatchQueued
        || channel->assignedThread >= dispatchThreadPool_.size()) {
        return;
    }
    channel->dispatchQueued = true;
    auto &dt = dispatchThreadPool_[channel->assignedThread];
    {
        std::lock_guard<std::mutex> dtLock(dt->mutex);
        dt->readyChannels.push_back(channel);
    }
    dt->cv.notify_one();
}

void WatchDispatcher::DispatchLoop(size_t threadIndex)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceNewID("CoordWD;" + GetStringUuid());
    auto &dt = dispatchThreadPool_[threadIndex];
    while (running_.load()) {
        auto channel = WaitForReadyChannel(*dt);
        if (channel == nullptr) {
            return;
        }
        auto result = HandleChannelEvents(channel);
        if (!running_.load() || channel->cancelled.load(std::memory_order_acquire)) {
            continue;
        }
        if (result == HandleResult::READY_AGAIN) {
            std::lock_guard<std::mutex> lock(dt->mutex);
            if (!channel->cancelled.load(std::memory_order_acquire)) {
                dt->readyChannels.push_back(std::move(channel));
            }
            dt->cv.notify_one();
        } else if (result == HandleResult::RETRY_LATER) {
            std::chrono::milliseconds delay;
            if (!PrepareRetry(channel, delay)) {
                continue;
            }
            RetryChannel retry{ std::chrono::steady_clock::now() + delay, std::move(channel) };
            std::lock_guard<std::mutex> lock(dt->mutex);
            if (!retry.channel->cancelled.load(std::memory_order_acquire)) {
                InsertRetryLocked(*dt, std::move(retry));
            }
            dt->cv.notify_one();
        }
    }
}

std::shared_ptr<WatcherChannel> WatchDispatcher::WaitForReadyChannel(DispatchThread &dispatchThread)
{
    std::unique_lock<std::mutex> lock(dispatchThread.mutex);
    while (running_.load()) {
        const auto now = std::chrono::steady_clock::now();
        while (!dispatchThread.retryChannels.empty() && dispatchThread.retryChannels.front().readyTime <= now) {
            auto channel = std::move(dispatchThread.retryChannels.front().channel);
            dispatchThread.retryChannels.pop_front();
            if (!channel->cancelled.load(std::memory_order_acquire)) {
                dispatchThread.readyChannels.push_back(std::move(channel));
            }
        }
        while (!dispatchThread.readyChannels.empty()) {
            auto channel = std::move(dispatchThread.readyChannels.front());
            dispatchThread.readyChannels.pop_front();
            if (!channel->cancelled.load(std::memory_order_acquire)) {
                return channel;
            }
        }
        if (dispatchThread.retryChannels.empty()) {
            dispatchThread.cv.wait(lock);
        } else {
            dispatchThread.cv.wait_until(lock, dispatchThread.retryChannels.front().readyTime);
        }
    }
    return nullptr;
}

bool WatchDispatcher::PrepareRetry(const std::shared_ptr<WatcherChannel> &channel, std::chrono::milliseconds &delay)
{
    if (!running_.load() || channel->cancelled.load(std::memory_order_acquire)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(channel->mutex);
    if (!running_.load() || channel->cancelled.load(std::memory_order_acquire)) {
        return false;
    }
    if (channel->retryDelayMs == 0) {
        channel->retryDelayMs = INITIAL_RETRY_DELAY_MS;
    } else {
        channel->retryDelayMs = std::min(channel->retryDelayMs * RETRY_DELAY_MULTIPLIER, MAX_RETRY_DELAY_MS);
    }
    const int64_t jitterRange = channel->retryDelayMs * RETRY_JITTER_PERCENT / PERCENT_SCALE;
    const uint64_t jitterWidth = static_cast<uint64_t>(jitterRange * RETRY_DELAY_MULTIPLIER + 1);
    const uint64_t seed = static_cast<uint64_t>(channel->watchId) ^ static_cast<uint64_t>(channel->retryDelayMs);
    const int64_t offset = static_cast<int64_t>(seed % jitterWidth) - jitterRange;
    delay = std::chrono::milliseconds(
        std::clamp(channel->retryDelayMs + offset, INITIAL_RETRY_DELAY_MS, MAX_RETRY_DELAY_MS));
    return true;
}

void WatchDispatcher::InsertRetryLocked(DispatchThread &dispatchThread, RetryChannel retry)
{
    auto position = std::upper_bound(
        dispatchThread.retryChannels.begin(), dispatchThread.retryChannels.end(), retry.readyTime,
        [](const auto &readyTime, const RetryChannel &queued) { return readyTime < queued.readyTime; });
    dispatchThread.retryChannels.insert(position, std::move(retry));
}

bool WatchDispatcher::PrepareChannelEvents(const std::shared_ptr<WatcherChannel> &channel, bool &needReWatch,
                                           int64_t &maxEventRevision, std::vector<std::shared_ptr<WatchEvent>> &events)
{
    if (!running_.load() || channel->cancelled.load(std::memory_order_acquire)) {
        return true;
    }
    std::lock_guard<std::mutex> qLock(channel->mutex);
    if (!running_.load() || channel->cancelled.load(std::memory_order_acquire) || channel->queue.empty()) {
        channel->dispatchQueued = false;
        return true;
    }
    if (channel->needReWatch) {
        events.push_back(channel->queue.front());
        needReWatch = true;
        return false;
    }
    if (channel->snapshotRevision <= 0) {
        channel->dispatchQueued = false;
        return true;
    }
    while (!channel->queue.empty() && channel->queue.front()->revision <= channel->snapshotRevision) {
        channel->queue.pop_front();
    }
    if (channel->queue.empty()) {
        channel->dispatchQueued = false;
        return true;
    }
    auto count = std::min(MAX_WATCH_EVENTS_PER_BATCH, channel->queue.size());
    auto end = std::next(channel->queue.begin(), static_cast<std::ptrdiff_t>(count));
    size_t batchBytes = 0;
    for (auto it = channel->queue.begin(); it != end; ++it) {
        const size_t eventBytes = WATCH_EVENT_WIRE_OVERHEAD_BYTES
                                  + ((*it)->type == WatchEvent::Type::REWATCH
                                         ? 0
                                         : (*it)->entry.key.size() + (*it)->entry.value.size());
        if (eventBytes > MAX_WATCH_EVENT_BATCH_BYTES) {
            auto rewatchEvent = MakeRewatchEvent(*it);
            channel->queue.clear();
            channel->queue.push_back(rewatchEvent);
            channel->needReWatch = true;
            events.push_back(std::move(rewatchEvent));
            needReWatch = true;
            return false;
        }
        if (!events.empty() && eventBytes > MAX_WATCH_EVENT_BATCH_BYTES - batchBytes) {
            break;
        }
        VLOG(1) << "PrepareChannelEvents, event revision: " << (*it)->revision << " watchId: " << channel->watchId
                << ", watcherAddr: " << channel->watcherAddr;
        events.push_back(*it);
        batchBytes += eventBytes;
        maxEventRevision = std::max(maxEventRevision, (*it)->revision);
    }
    return false;
}

WatchDispatcher::HandleResult WatchDispatcher::HandleChannelEvents(const std::shared_ptr<WatcherChannel> &channel)
{
    bool needReWatch = false;
    int64_t maxEventRevision = 0;
    std::vector<std::shared_ptr<WatchEvent>> events;
    events.reserve(MAX_WATCH_EVENTS_PER_BATCH);
    if (PrepareChannelEvents(channel, needReWatch, maxEventRevision, events)) {
        return HandleResult::NONE;
    }
    if (!running_.load() || channel->cancelled.load(std::memory_order_acquire)) {
        return HandleResult::NONE;
    }

    VLOG(1) << "HandleChannelEvents, watchId: " << channel->watchId << ", watcherAddr: " << channel->watcherAddr
            << ", events size: " << events.size() << ", max revision: " << maxEventRevision;
    Status status = DoNotify(channel->watchId, channel->watcherAddr, events);
    if (!running_.load() || channel->cancelled.load(std::memory_order_acquire)) {
        return HandleResult::NONE;
    }
    if (status.GetCode() == K_NOT_FOUND) {
        RemoveRewatchRequiredWatcher(channel->watchId, channel->watcherAddr);
        return HandleResult::NONE;
    }
    if (status.IsError()) {
        const uint64_t failureCount = notifyFailures_.fetch_add(1, std::memory_order_relaxed) + 1;
        if (failureCount == 1 || failureCount % DROP_LOG_INTERVAL == 0) {
            LOG(WARNING) << "CLUSTER_WATCH_NOTIFY_FAILED watchId=" << channel->watchId
                         << ", watcherAddr=" << channel->watcherAddr << ", failureCount=" << failureCount
                         << ", status=" << status.ToString();
        }
        return HandleResult::RETRY_LATER;
    }
    if (needReWatch) {
        RemoveRewatchRequiredWatcher(channel->watchId, channel->watcherAddr);
        return HandleResult::NONE;
    }
    {
        std::lock_guard<std::mutex> qLock(channel->mutex);
        if (channel->cancelled.load(std::memory_order_acquire)) {
            return HandleResult::NONE;
        }
        channel->retryDelayMs = 0;
        while (!channel->queue.empty() && channel->queue.front()->revision <= maxEventRevision) {
            channel->queue.pop_front();
        }
        if (channel->queue.empty()) {
            channel->dispatchQueued = false;
            return HandleResult::NONE;
        }
    }
    return HandleResult::READY_AGAIN;
}

void WatchDispatcher::RemoveRewatchRequiredWatcher(int64_t watchId, const std::string &watcherAddr)
{
    if (watchRegistry_ != nullptr) {
        Status status = watchRegistry_->Cancel(watchId, watcherAddr);
        if (status.IsError() && status.GetCode() != StatusCode::K_NOT_FOUND) {
            LOG(WARNING) << "CLUSTER_WATCH_REBUILD_CANCEL_FAILED watchId=" << watchId
                         << ", watcherAddr=" << watcherAddr << ", status=" << status.ToString();
        }
    }
    RemoveChannel(watchId);
}

void WatchDispatcher::ScheduleAllChannelsForRewatch()
{
    std::shared_lock<std::shared_mutex> lock(channelsMutex_);
    for (const auto &[watchId, channel] : channels_) {
        static_cast<void>(watchId);
        std::lock_guard<std::mutex> queueLock(channel->mutex);
        if (channel->cancelled.load(std::memory_order_acquire) || channel->needReWatch) {
            continue;
        }
        channel->queue.clear();
        channel->queue.push_back(MakeRewatchEvent(nullptr));
        channel->needReWatch = true;
        ScheduleChannelLocked(channel);
    }
}

}  // namespace datasystem

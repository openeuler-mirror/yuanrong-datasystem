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

DS_DEFINE_int32(watch_event_dispatch_thread, 4, "Number of coordinator watch event dispatch threads");

namespace datasystem {
constexpr size_t DEFAULT_DISPATCH_THREAD_COUNT = 4;
constexpr int64_t DISPATCH_POLL_INTERVAL_MS = 10;
constexpr size_t MAX_EVENTS_PER_NOTIFY = 32;
constexpr size_t MAX_EVENTS_PER_FAN_OUT = 32;
constexpr size_t MAX_PENDING_EVENTS = 10000;

std::shared_ptr<WatchEvent> MakeRewatchEvent(const std::shared_ptr<WatchEvent> &overflowEvent)
{
    auto rewatchEvent = std::make_shared<WatchEvent>();
    rewatchEvent->type = WatchEvent::Type::REWATCH;
    if (overflowEvent != nullptr) {
        rewatchEvent->entry = overflowEvent->entry;
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
        std::unique_lock<std::mutex> lock(pendingMutex_);
        pendingFullCv_.wait(lock, [this] { return pendingQueue_.size() < MAX_PENDING_EVENTS; });
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

    std::unique_lock<std::shared_mutex> lock(channelsMutex_);
    channels_[watchId] = channel;

    if (!dispatchThreadPool_.empty()) {
        channel->assignedThread = static_cast<size_t>(watchId) % dispatchThreadPool_.size();
    }
}

void WatchDispatcher::RemoveChannel(int64_t watchId)
{
    std::unique_lock<std::shared_mutex> lock(channelsMutex_);
    auto it = channels_.find(watchId);
    if (it == channels_.end()) {
        return;
    }
    channels_.erase(it);
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
        dispatchThreadPool_[i]->thread = std::thread(&WatchDispatcher::DispatchLoop, this, i);
    }
    fanOutThread_ = std::thread(&WatchDispatcher::FanOutLoop, this);
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
    while (running_.load()) {
        std::deque<std::shared_ptr<WatchEvent>> batch;
        {
            std::unique_lock<std::mutex> lock(pendingMutex_);
            pendingEmptyCv_.wait(lock, [this] { return !pendingQueue_.empty() || !running_.load(); });
            if (!running_.load() && pendingQueue_.empty()) {
                break;
            }
            auto count = std::min(MAX_EVENTS_PER_FAN_OUT, pendingQueue_.size());
            for (size_t i = 0; i < count; ++i) {
                batch.push_back(std::move(pendingQueue_.front()));
                pendingQueue_.pop_front();
            }
            if (count > 0) {
                pendingFullCv_.notify_all();
            }
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
                    if (channel->needReWatch) {
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
                        LOG(WARNING) << "Watch channel backpressure overflow, enqueue rewatch event, watchId="
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
    if (channel->dispatchQueued || channel->assignedThread >= dispatchThreadPool_.size()) {
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
    auto &dt = dispatchThreadPool_[threadIndex];
    while (running_.load()) {
        std::shared_ptr<WatcherChannel> channel;
        {
            std::unique_lock<std::mutex> lock(dt->mutex);
            while (running_.load() && dt->readyChannels.empty()) {
                auto now = std::chrono::steady_clock::now();
                while (!dt->retryChannels.empty() && dt->retryChannels.front().readyTime <= now) {
                    dt->readyChannels.push_back(dt->retryChannels.front().channel);
                    dt->retryChannels.pop_front();
                }
                if (!dt->readyChannels.empty()) {
                    break;
                }
                if (dt->retryChannels.empty()) {
                    dt->cv.wait(lock, [this, &dt] { return !running_.load() || !dt->readyChannels.empty(); });
                } else {
                    dt->cv.wait_until(lock, dt->retryChannels.front().readyTime,
                                      [this, &dt] { return !running_.load() || !dt->readyChannels.empty(); });
                }
            }
            if (!running_.load()) {
                break;
            }
            channel = std::move(dt->readyChannels.front());
            dt->readyChannels.pop_front();
        }

        auto result = HandleChannelEvents(channel);
        if (result == HandleResult::READY_AGAIN) {
            std::lock_guard<std::mutex> lock(dt->mutex);
            dt->readyChannels.push_back(std::move(channel));
            dt->cv.notify_one();
        } else if (result == HandleResult::RETRY_LATER) {
            RetryChannel retry{ std::chrono::steady_clock::now() + std::chrono::milliseconds(DISPATCH_POLL_INTERVAL_MS),
                                std::move(channel) };
            std::lock_guard<std::mutex> lock(dt->mutex);
            dt->retryChannels.push_back(std::move(retry));
            dt->cv.notify_one();
        }
    }
}

bool WatchDispatcher::PrepareChannelEvents(const std::shared_ptr<WatcherChannel> &channel, bool &needReWatch,
                                           int64_t &maxEventRevision, std::vector<std::shared_ptr<WatchEvent>> &events)
{
    std::lock_guard<std::mutex> qLock(channel->mutex);
    if (channel->queue.empty()) {
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
    auto count = std::min(MAX_EVENTS_PER_NOTIFY, channel->queue.size());
    auto end = std::next(channel->queue.begin(), static_cast<long>(count));
    for (auto it = channel->queue.begin(); it != end; ++it) {
        VLOG(1) << "PrepareChannelEvents, event revision: " << (*it)->revision << " watchId: " << channel->watchId
                << ", watcherAddr: " << channel->watcherAddr;
        events.push_back(*it);
        maxEventRevision = std::max(maxEventRevision, (*it)->revision);
    }
    return false;
}

WatchDispatcher::HandleResult WatchDispatcher::HandleChannelEvents(const std::shared_ptr<WatcherChannel> &channel)
{
    bool needReWatch = false;
    int64_t maxEventRevision = 0;
    std::vector<std::shared_ptr<WatchEvent>> events;
    events.reserve(MAX_EVENTS_PER_NOTIFY);
    if (PrepareChannelEvents(channel, needReWatch, maxEventRevision, events)) {
        return HandleResult::NONE;
    }

    VLOG(1) << "HandleChannelEvents, watchId: " << channel->watchId << ", watcherAddr: " << channel->watcherAddr
            << ", events size: " << events.size() << ", max revision: " << maxEventRevision;
    Status status = DoNotify(channel->watchId, channel->watcherAddr, events);
    if (status.IsError()) {
        LOG(WARNING) << "Failed to notify watch events, watchId=" << channel->watchId
                     << ", watcherAddr=" << channel->watcherAddr << ", status=" << status.ToString();
        return HandleResult::RETRY_LATER;
    }
    if (needReWatch) {
        RemoveRewatchRequiredWatcher(channel->watchId, channel->watcherAddr);
        return HandleResult::NONE;
    }
    {
        std::lock_guard<std::mutex> qLock(channel->mutex);
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
        if (status.IsError()) {
            LOG(WARNING) << "Failed to cancel rewatch-required watcher from registry, watchId=" << watchId
                         << ", watcherAddr=" << watcherAddr << ", status=" << status.ToString();
        }
    }
    RemoveChannel(watchId);
}

}  // namespace datasystem

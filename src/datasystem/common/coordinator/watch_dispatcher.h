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
#ifndef DATASYSTEM_COMMON_COORDINATOR_WATCH_DISPATCHER_H
#define DATASYSTEM_COMMON_COORDINATOR_WATCH_DISPATCHER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/coordinator/watch_event.h"
#include "datasystem/common/coordinator/watch_registry.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/utils/status.h"

namespace datasystem {
struct WatcherChannel {
    int64_t watchId = 0;
    std::string watcherAddr;

    // Protects queue, backpressureLimit, snapshotRevision, needReWatch, dispatchQueued and retryDelayMs.
    std::mutex mutex;
    std::deque<std::shared_ptr<WatchEvent>> queue;
    size_t backpressureLimit = 1024;

    // Assigned before channel publication and immutable afterwards.
    size_t assignedThread = 0;
    int64_t snapshotRevision = 0;
    bool needReWatch = false;
    bool dispatchQueued = false;
    int64_t retryDelayMs = 0;
    std::atomic<bool> cancelled{ false };
};

class WatchDispatcher {
public:
    /**
     * @brief Construct a watch dispatcher using the specified registry.
     * @param[in] watchRegistry Registry that owns watch IDs.
     */
    explicit WatchDispatcher(WatchRegistry *watchRegistry);

    /**
     * @brief Stop dispatcher threads before destroying the dispatcher.
     */
    virtual ~WatchDispatcher();

    /**
     * @brief Enqueue a watch event into the pending queue.
     * Called by MemoryKvStore mutation callback. O(1).
     * @param[in] event The watch event.
     */
    void Enqueue(std::shared_ptr<WatchEvent> event);

    /**
     * @brief Get the current number of events waiting in the global pending queue.
     * @return Pending event count observed under the pending queue mutex.
     */
    size_t GetPendingEventCount();

    /**
     * @brief Create a channel for a watcher.
     * @param[in] watchId The watch ID.
     * @param[in] watcherAddr The watcher address.
     */
    void AddChannel(int64_t watchId, const std::string &watcherAddr);

    /**
     * @brief Remove a watcher's channel.
     * @param[in] watchId The watch ID.
     */
    void RemoveChannel(int64_t watchId);

    /**
     * @brief Cancel and remove all channels owned by one watcher address.
     * @param[in] watcherAddr Watcher address whose channels should be removed.
     */
    void RemoveChannelsByWatcher(const std::string &watcherAddr);

    /**
     * @brief Set the snapshot revision for dedup after WatchRange snapshot.
     * @param[in] watchId The watch ID.
     * @param[in] revision The snapshot revision.
     */
    void SetSnapshotRevision(int64_t watchId, int64_t revision);

    /**
     * @brief Start the fan-out thread and dispatch thread pool.
     */
    void Start();

    /**
     * @brief Stop all threads gracefully.
     */
    void Stop();

protected:
    /**
     * @brief Abstract notification method. Override for RPC or mock.
     * @param[in] watchId The watch ID.
     * @param[in] watcherAddr The watcher address.
     * @param[in] events Batch of events to deliver.
     * @return Status of the notification.
     */
    virtual Status DoNotify(int64_t watchId, const std::string &watcherAddr,
                            std::vector<std::shared_ptr<WatchEvent>> &events) = 0;

private:
    struct RetryChannel {
        std::chrono::steady_clock::time_point readyTime;
        std::shared_ptr<WatcherChannel> channel;
    };

    struct DispatchThread {
        Thread thread;

        // Protects readyChannels and retryChannels.
        std::mutex mutex;
        std::deque<std::shared_ptr<WatcherChannel>> readyChannels;
        std::deque<RetryChannel> retryChannels;
        std::condition_variable cv;
    };

    enum class HandleResult : uint8_t {
        NONE,
        READY_AGAIN,
        RETRY_LATER,
    };

    /**
     * @brief Fan-out thread main loop.
     */
    void FanOutLoop();

    /**
     * @brief Dispatch thread main loop.
     * @param[in] threadIndex Index of this thread in the pool.
     */
    void DispatchLoop(size_t threadIndex);

    /**
     * @brief Notify and commit queued events for one watcher channel.
     * @param[in] channel Watcher channel to process.
     * @return Scheduling decision after this notification attempt.
     */
    HandleResult HandleChannelEvents(const std::shared_ptr<WatcherChannel> &channel);

    /**
     * @brief Prepare a notification batch from one watcher channel.
     * @param[in] channel Watcher channel to process.
     * @param[out] needReWatch Whether the batch contains a rewatch event.
     * @param[out] maxEventRevision Maximum revision in the prepared batch.
     * @param[out] events Prepared events to notify.
     * @return True if no notification should be sent, otherwise false.
     */
    bool PrepareChannelEvents(const std::shared_ptr<WatcherChannel> &channel, bool &needReWatch,
                              int64_t &maxEventRevision, std::vector<std::shared_ptr<WatchEvent>> &events);

    /**
     * @brief Schedule a channel with pending work onto its assigned dispatch thread.
     * @param[in] channel Watcher channel to schedule.
     */
    void ScheduleChannelLocked(const std::shared_ptr<WatcherChannel> &channel);

    /**
     * @brief Prepare the next bounded retry delay for a failed channel.
     * @param[in] channel Failed watcher channel.
     * @param[out] delay Retry delay for this failure.
     * @return True when the retry may be queued.
     */
    bool PrepareRetry(const std::shared_ptr<WatcherChannel> &channel, std::chrono::milliseconds &delay);

    /**
     * @brief Insert a retry in ascending ready-time order.
     * @param[in] dispatchThread Dispatch thread that owns the channel.
     * @param[in] retry Retry record to insert.
     */
    void InsertRetryLocked(DispatchThread &dispatchThread, RetryChannel retry);

    /**
     * @brief Mark a channel cancelled and clear its pending events.
     * @param[in] channel Channel to cancel.
     */
    void CancelChannel(const std::shared_ptr<WatcherChannel> &channel);

    /**
     * @brief Remove a watch ID from the watcher-address reverse index.
     * @param[in] channel Channel whose reverse-index entry should be removed.
     */
    void RemoveReverseIndexLocked(const std::shared_ptr<WatcherChannel> &channel);

    /**
     * @brief Replace every live channel queue with one RESET after global pending overflow.
     */
    void ScheduleAllChannelsForRewatch();

    /**
     * @brief Wait for the next non-cancelled channel owned by a dispatch thread.
     * @param[in] dispatchThread Dispatch thread state to wait on.
     * @return Ready channel, or nullptr after shutdown.
     */
    std::shared_ptr<WatcherChannel> WaitForReadyChannel(DispatchThread &dispatchThread);

    /**
     * @brief Cancel a watcher after notifying that the stream must be rebuilt.
     * @param[in] watchId The watch ID.
     * @param[in] watcherAddr Address that owns the watch.
     */
    void RemoveRewatchRequiredWatcher(int64_t watchId, const std::string &watcherAddr);

    // The mutex protects pendingQueue_.
    std::mutex pendingMutex_;
    std::deque<std::shared_ptr<WatchEvent>> pendingQueue_;
    std::condition_variable pendingEmptyCv_;
    Thread fanOutThread_;

    std::atomic<uint64_t> droppedPendingEvents_{ 0 };
    std::atomic<uint64_t> cancelFailures_{ 0 };
    std::atomic<uint64_t> notifyFailures_{ 0 };
    std::atomic<bool> pendingOverflow_{ false };

    // The mutex protects channels_ and watchIdsByWatcher_.
    std::shared_mutex channelsMutex_;
    std::unordered_map<int64_t, std::shared_ptr<WatcherChannel>> channels_;
    std::unordered_map<std::string, std::unordered_set<int64_t>> watchIdsByWatcher_;

    std::vector<std::unique_ptr<DispatchThread>> dispatchThreadPool_;

    WatchRegistry *watchRegistry_ = nullptr;
    std::atomic<bool> running_{ false };
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_COORDINATOR_WATCH_DISPATCHER_H

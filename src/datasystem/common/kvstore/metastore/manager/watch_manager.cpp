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
 * Description: Watch manager implementation.
 */
#include "datasystem/common/kvstore/metastore/manager/watch_manager.h"

#include "datasystem/common/log/log.h"

namespace datasystem {

std::shared_ptr<std::mutex> WatchManager::GetOrCreateStreamMutex(
    grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream)
{
    std::lock_guard<std::mutex> lock(streamMutexMapMutex_);

    auto it = streamMutexMap_.find(stream);
    if (it == streamMutexMap_.end()) {
        it = streamMutexMap_.emplace(stream, std::make_shared<std::mutex>()).first;
    }

    return it->second;
}

void WatchManager::RemoveStreamMutex(
    grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream)
{
    std::lock_guard<std::mutex> lock(streamMutexMapMutex_);
    streamMutexMap_.erase(stream);
}

WatcherThreadPool::WatcherThreadPool(size_t threadCount)
{
    // Create ThreadData objects with mutexes and condition_variables initialized
    // Do NOT start threads yet to ensure threadData_ is fully ready before threads access it
    for (size_t i = 0; i < threadCount; ++i) {
        ThreadData data;
        data.thread = nullptr;
        data.watchersMutex = std::make_unique<std::mutex>();
        data.sleepCv = std::make_unique<std::condition_variable>();
        threadData_.push_back(std::move(data));
    }

    // Now that threadData_ is fully initialized, start the worker threads
    for (size_t i = 0; i < threadCount; ++i) {
        threadData_[i].thread = std::make_unique<std::thread>(&WatcherThreadPool::WorkerThreadFunc, this, i);
    }
}

WatcherThreadPool::~WatcherThreadPool()
{
    Shutdown();
    Join();
}

size_t WatcherThreadPool::AddWatcher(const std::shared_ptr<Watcher> &watcher)
{
    size_t threadIndex = nextThreadIndex_.fetch_add(1) % threadData_.size();
    watcher->assignedThreadIndex = threadIndex;

    {
        std::lock_guard<std::mutex> lock(*threadData_[threadIndex].watchersMutex);
        threadData_[threadIndex].watchers.push_back(watcher);
    }

    // Wake up the thread if it was sleeping
    threadData_[threadIndex].sleepCv->notify_one();

    return threadIndex;
}

void WatcherThreadPool::RemoveWatcher(const std::shared_ptr<Watcher> &watcher)
{
    size_t threadIndex = watcher->assignedThreadIndex;

    // Mark as inactive first to stop worker thread processing immediately
    watcher->active.store(false);

    std::lock_guard<std::mutex> lock(*threadData_[threadIndex].watchersMutex);
    auto &watchers = threadData_[threadIndex].watchers;

    watchers.erase(std::remove(watchers.begin(), watchers.end(), watcher), watchers.end());
}

void WatcherThreadPool::Shutdown()
{
    shutdown_.store(true);

    // Wake up all threads so they can exit
    for (auto &data : threadData_) {
        data.sleepCv->notify_all();
    }
}

void WatcherThreadPool::Join()
{
    for (auto &data : threadData_) {
        if (data.thread && data.thread->joinable()) {
            data.thread->join();
        }
    }
}

bool WatcherThreadPool::ProcessWatcherQueue(const std::shared_ptr<Watcher> &watcher)
{
    // Fast check: skip if watcher is already inactive
    if (!watcher->active.load()) {
        return false;
    }

    // Skip watchers that are still initializing (receiving historical events)
    if (watcher->initializing.load()) {
        return true;
    }

    // Try to get a response from this watcher's queue
    etcdserverpb::WatchResponse resp;
    bool hasResponse = false;
    {
        std::lock_guard<std::mutex> lock(watcher->writeQueueMutex_);
        if (!watcher->writeQueue_.empty()) {
            resp = std::move(watcher->writeQueue_.front());
            watcher->writeQueue_.pop();
            hasResponse = true;
        }
    }

    if (!hasResponse) {
        return true;
    }

    // Skip events that were already sent as historical events
    int64_t lastHistoricalRev = watcher->lastHistoricalRevision.load();
    if (resp.has_header() && resp.header().revision() <= lastHistoricalRev) {
        return true;
    }

    // Lock shared stream mutex and perform entire write operation under lock
    // This prevents concurrent writes to the same gRPC stream from different watchers
    {
        std::lock_guard<std::mutex> streamLock(*watcher->streamMutex_);

        // Double-check active under streamMutex_ to prevent use-after-free
        if (!watcher->active.load()) {
            return false;
        }

        // Check if stream pointer is still valid (not null from CancelWatcher)
        if (watcher->stream == nullptr) {
            LOG(WARNING) << "Watch stream is null for watch_id=" << watcher->watchId;
            watcher->active.store(false);
            return false;
        }

        // Write to stream - MUST stay under streamMutex_ lock!
        // This ensures the stream object is not released by gRPC while we're using it
        bool success = watcher->stream->Write(resp);
        if (!success) {
            LOG(WARNING) << "Failed to write to watch stream for watch_id=" << watcher->watchId;
            watcher->active.store(false);
            return false;
        }
    }

    return true;
}

void WatcherThreadPool::WorkerThreadFunc(size_t threadIndex)
{
    auto &data = threadData_[threadIndex];

    while (!shutdown_.load()) {
        // Snapshot watchers list to minimize lock hold time
        std::vector<std::shared_ptr<Watcher>> watchersCopy;
        {
            std::unique_lock<std::mutex> lock(*data.watchersMutex);

            // Wait if no watchers
            if (data.watchers.empty() && !shutdown_.load()) {
                data.sleepCv->wait(lock, [this, &data] { return !data.watchers.empty() || shutdown_.load(); });
            }

            if (shutdown_.load()) {
                break;
            }

            // Copy watcher pointers and release lock before processing
            // This allows other threads to add/remove watchers concurrently
            watchersCopy.reserve(data.watchers.size());
            for (const auto &watcher : data.watchers) {
                watchersCopy.push_back(watcher);
            }
        }

        // Process watchers without holding watchersMutex
        for (const auto &watcher : watchersCopy) {
            ProcessWatcherQueue(watcher);
        }

        // Small sleep to prevent busy-waiting
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

WatchManager::WatchManager(size_t watcherThreadPoolSize)
    : watcherThreadPool_(std::make_unique<WatcherThreadPool>(watcherThreadPoolSize))
{
}

WatchManager::~WatchManager()
{
    std::vector<std::shared_ptr<Watcher>> watchersCopy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = watchers_.begin(); it != watchers_.end(); ++it) {
            it->second->active.store(false);
            watchersCopy.push_back(it->second);
        }
    }

    // Remove watchers from thread pool
    for (const auto &watcher : watchersCopy) {
        watcherThreadPool_->RemoveWatcher(watcher);
    }
}

bool WatchManager::KeyMatchesWatcher(const std::string &key, const Watcher *watcher) const
{
    if (key < watcher->key) {
        return false;
    }
    if (watcher->rangeEnd.empty()) {
        return key == watcher->key;  // Single key
    }
    if (watcher->rangeEnd[0] == 0) {
        return key >= watcher->key;  // Range query [key, ...)
    }
    return key < watcher->rangeEnd;  // [key, rangeEnd)
}

Status WatchManager::RegisterWatcher(
    const etcdserverpb::WatchCreateRequest &req,
    grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream, int64_t *watchId,
    KVManager *kvManager)
{
    auto watcher = std::make_shared<Watcher>();
    *watchId = nextWatchId_++;

    watcher->watchId = *watchId;
    watcher->key = req.key();
    watcher->rangeEnd = req.range_end();
    watcher->startRevision = req.start_revision();
    watcher->prevKv = req.prev_kv();
    watcher->stream = stream;
    // Get or create shared mutex for this stream
    watcher->streamMutex_ = GetOrCreateStreamMutex(stream);

    // Set initializing flag BEFORE adding to watchers_ map
    // This prevents worker thread from processing queue while historical events are being sent
    watcher->initializing.store(true);

    // Add watcher to global map - now NotifyPut/NotifyDelete can find this watcher
    // and enqueue new events (avoid event loss)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        watchers_[*watchId] = watcher;
    }

    // Send created response directly via stream (not via queue)
    etcdserverpb::WatchResponse resp;
    auto *header = resp.mutable_header();
    header->set_revision(kvManager->CurrentRevision());
    resp.set_watch_id(*watchId);
    resp.set_created(true);

    {
        std::lock_guard<std::mutex> lock(*watcher->streamMutex_);
        if (!watcher->stream->Write(resp)) {
            return Status(StatusCode::K_RUNTIME_ERROR, "Failed to send watch created response for key: " + req.key());
        }
    }

    // Send historical events (if startRevision is specified)
    // Now that watcher is in the map, any concurrent events will be queued correctly
    // but worker thread won't process until initializing flag is cleared
    if (req.start_revision() > 0) {
        SendHistoricalEvents(watcher, kvManager);
    }

    // Add to thread pool - worker will see initializing flag and skip processing
    watcherThreadPool_->AddWatcher(watcher);

    // Clear initializing flag - now worker can start processing the queue
    // Historical events were sent directly via stream, new events in queue will be sent now
    watcher->initializing.store(false);

    return Status::OK();
}

Status WatchManager::CancelWatcher(int64_t watchId, bool sendResponse)
{
    std::shared_ptr<Watcher> watcher;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = watchers_.find(watchId);
        if (it == watchers_.end()) {
            return Status(StatusCode::K_NOT_FOUND, "Watcher not found: watchId=" + std::to_string(watchId));
        }
        watcher = it->second;
        watchers_.erase(it);
    }

    // Mark as inactive first
    watcher->active.store(false);

    // Send canceled response if requested (for normal cancel request, not for cleanup)
    if (sendResponse) {
        etcdserverpb::WatchResponse resp;
        resp.set_watch_id(watchId);
        resp.set_canceled(true);
        SendWatchResponse(watcher, &resp);
    }

    // Clear stream pointer to prevent use-after-free
    // This must be done while holding streamMutex_ to synchronize with WorkerThreadFunc
    {
        std::lock_guard<std::mutex> streamLock(*watcher->streamMutex_);
        watcher->stream = nullptr;
    }

    // Remove from thread pool
    watcherThreadPool_->RemoveWatcher(watcher);

    return Status::OK();
}

void WatchManager::CancelWatchersByStream(
    grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream)
{
    LOG(INFO) << "CancelWatchersByStream: cleaning up watchers for stream=" << static_cast<void *>(stream);

    std::vector<int64_t> watchIdsToCancel;

    // Collect all watch IDs that match the stream
    // We need to lock the shared stream mutex to safely access watcher->stream
    {
        std::lock_guard<std::mutex> lock(mutex_);
        for (const auto &[id, watcher] : watchers_) {
            if (watcher->streamMutex_) {
                std::lock_guard<std::mutex> streamLock(*watcher->streamMutex_);
                if (watcher->stream == stream) {
                    watchIdsToCancel.push_back(id);
                }
            }
        }
    }

    // Remove stream mutex from registry - no more watchers will need it
    RemoveStreamMutex(stream);

    // Cancel each watcher (this is safe because we have copies of watchIds)
    for (int64_t watchId : watchIdsToCancel) {
        LOG(INFO) << "Canceling watcher on stream disconnect: watch_id=" << watchId;
        // Don't send canceled response since client is already disconnected
        CancelWatcher(watchId, false);
    }

    if (!watchIdsToCancel.empty()) {
        LOG(INFO) << "Canceled " << watchIdsToCancel.size() << " watcher(s) for stream=" << static_cast<void *>(stream);
    }
}

void WatchManager::NotifyPut(const std::string &key, const mvccpb::KeyValue &kv, const mvccpb::KeyValue *prevKv,
                             int64_t revision)
{
    LOG(INFO) << "NotifyPut: key=" << key << ", revision=" << revision;
    // Collect responses to send, then release lock before sending
    struct PendingResponse {
        std::shared_ptr<Watcher> watcher;
        etcdserverpb::WatchResponse resp;
    };
    std::vector<PendingResponse> pendingResponses;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        for (auto it = watchers_.begin(); it != watchers_.end(); ++it) {
            auto &watcher = it->second;
            if (!watcher->active.load()) {
                continue;
            }
            if (!KeyMatchesWatcher(key, watcher.get())) {
                continue;
            }

            PendingResponse pending;
            pending.watcher = watcher;
            pending.resp.set_watch_id(watcher->watchId);
            auto *header = pending.resp.mutable_header();
            header->set_revision(revision);

            auto *event = pending.resp.add_events();
            event->set_type(mvccpb::Event::PUT);
            *event->mutable_kv() = kv;
            if (watcher->prevKv && prevKv) {
                *event->mutable_prev_kv() = *prevKv;
            }

            pendingResponses.push_back(std::move(pending));
        }
    }  // Lock released here

    // Send responses outside of lock to avoid blocking gRPC thread
    std::vector<int64_t> inactiveWatchers;
    for (auto &pending : pendingResponses) {
        if (!SendWatchResponse(pending.watcher, &pending.resp)) {
            inactiveWatchers.push_back(pending.watcher->watchId);
        }
    }

    // Clean up inactive watchers
    if (!inactiveWatchers.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (int64_t id : inactiveWatchers) {
            auto it = watchers_.find(id);
            if (it != watchers_.end()) {
                it->second->active.store(false);
                watchers_.erase(it);
            }
        }
    }
}

void WatchManager::NotifyDelete(const std::string &key, const mvccpb::KeyValue &kv, int64_t revision)
{
    LOG(INFO) << "NotifyDelete: key=" << key << ", revision=" << revision;
    // Collect responses to send, then release lock before sending
    struct PendingResponse {
        std::shared_ptr<Watcher> watcher;
        etcdserverpb::WatchResponse resp;
    };
    std::vector<PendingResponse> pendingResponses;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        for (auto it = watchers_.begin(); it != watchers_.end(); ++it) {
            auto &watcher = it->second;
            if (!watcher->active.load()) {
                continue;
            }
            if (!KeyMatchesWatcher(key, watcher.get())) {
                continue;
            }

            PendingResponse pending;
            pending.watcher = watcher;
            pending.resp.set_watch_id(watcher->watchId);
            auto *header = pending.resp.mutable_header();
            header->set_revision(revision);

            auto *event = pending.resp.add_events();
            event->set_type(mvccpb::Event::DELETE);
            *event->mutable_kv() = kv;

            pendingResponses.push_back(std::move(pending));
        }
    }  // Lock released here

    // Send responses outside of lock to avoid blocking gRPC thread
    std::vector<int64_t> inactiveWatchers;
    for (auto &pending : pendingResponses) {
        if (!SendWatchResponse(pending.watcher, &pending.resp)) {
            inactiveWatchers.push_back(pending.watcher->watchId);
        }
    }

    // Clean up inactive watchers
    if (!inactiveWatchers.empty()) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (int64_t id : inactiveWatchers) {
            auto it = watchers_.find(id);
            if (it != watchers_.end()) {
                it->second->active.store(false);
                watchers_.erase(it);
            }
        }
    }
}

bool WatchManager::SendWatchResponse(const std::shared_ptr<Watcher> &watcher, etcdserverpb::WatchResponse *response)
{
    if (!watcher->active.load()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(watcher->writeQueueMutex_);
    watcher->writeQueue_.push(*response);

    return true;
}

void WatchManager::SendHistoricalEvents(const std::shared_ptr<Watcher> &watcher, KVManager *kvManager)
{
    LOG(INFO) << "SendHistoricalEvents: watch_id=" << watcher->watchId << ", key=" << watcher->key
              << ", start_revision=" << watcher->startRevision;

    // Get historical events for the key range after specified revision
    std::vector<HistoricalEntry> events;
    Status status = kvManager->RangeHistory(watcher->key, watcher->rangeEnd, watcher->startRevision, &events);
    if (status.IsError()) {
        return;
    }

    // Track the maximum revision sent in historical events
    int64_t maxRevision = watcher->startRevision - 1;

    // Send historical events directly through stream (not via queue)
    // This ensures they are sent before any new events that go to the queue
    std::lock_guard<std::mutex> lock(*watcher->streamMutex_);

    // Check stream pointer validity under lock
    if (watcher->stream == nullptr) {
        LOG(WARNING) << "Watch stream is null during SendHistoricalEvents for watch_id=" << watcher->watchId;
        watcher->active.store(false);
        return;
    }

    for (const auto &entry : events) {
        // Double-check active under streamMutex_ lock
        if (!watcher->active.load()) {
            break;
        }

        etcdserverpb::WatchResponse resp;
        auto *header = resp.mutable_header();
        header->set_revision(entry.revision);
        resp.set_watch_id(watcher->watchId);

        auto *event = resp.add_events();
        event->set_type(entry.type);
        *event->mutable_kv() = entry.kv;

        if (!watcher->stream->Write(resp)) {
            watcher->active.store(false);
            break;
        }

        // Track the maximum revision sent
        if (entry.revision > maxRevision) {
            maxRevision = entry.revision;
        }
    }

    // Record the last historical revision to filter out duplicates from the queue
    watcher->lastHistoricalRevision.store(maxRevision);
}

}  // namespace datasystem

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
 * Description: Watch manager for metastore service.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_WATCH_WATCH_MANAGER_H
#define DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_WATCH_WATCH_MANAGER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include "etcd/api/mvccpb/kv.pb.h"
#include "datasystem/common/kvstore/metastore/manager/kv_manager.h"
#include "datasystem/utils/status.h"

namespace datasystem {

/**
 * @brief Watcher data structure representing a registered watch request.
 */
struct Watcher {
    int64_t watchId;
    std::string key;
    std::string rangeEnd;
    int64_t startRevision;
    bool prevKv;
    // Note: stream is a raw pointer because its lifetime is managed by gRPC framework
    grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream;
    std::atomic<bool> active{ true };
    // Shared mutex for stream access
    // Multiple watchers on same stream share the same mutex to ensure thread-safe gRPC writes
    std::shared_ptr<std::mutex> streamMutex_;

    // Asynchronous write queue to avoid blocking notification threads
    std::queue<etcdserverpb::WatchResponse> writeQueue_;
    std::mutex writeQueueMutex_;

    // Thread assignment in pool - which thread handles this watcher
    size_t assignedThreadIndex{ 0 };

    // Initializing flag - set to true while historical events are being sent
    // Worker thread skips processing until this flag is false
    std::atomic<bool> initializing{ false };

    // Maximum revision sent via historical events
    // Worker thread filters out events with revision <= lastHistoricalRevision
    std::atomic<int64_t> lastHistoricalRevision{ 0 };
};

/**
 * @brief Watcher thread pool for managing multiple watchers with a fixed number of threads.
 *
 * Each thread in the pool is responsible for sending messages to multiple watcher streams.
 * When a watcher is closed, only the watcher is deleted without stopping the write thread.
 * Each watcher is bound to a specific thread in the pool and maintains its own writeQueue.
 */
class WatcherThreadPool {
public:
    /**
     * @brief Constructor - creates worker threads
     * @param threadCount Number of threads to create (default: 4)
     */
    explicit WatcherThreadPool(size_t threadCount = 4);

    /**
     * @brief Destructor - shutdown and join all threads
     */
    ~WatcherThreadPool();

    /**
     * @brief Add a watcher to the pool using round-robin assignment
     * @param watcher The watcher to add
     * @return The assigned thread index
     */
    size_t AddWatcher(const std::shared_ptr<Watcher> &watcher);

    /**
     * @brief Remove a watcher from the pool (may be inactive)
     * @param watcher The watcher to remove
     */
    void RemoveWatcher(const std::shared_ptr<Watcher> &watcher);

    /**
     * @brief Shutdown all threads gracefully
     */
    void Shutdown();

    /**
     * @brief Wait for all threads to finish
     */
    void Join();

private:
    /**
     * @brief Thread data structure for worker threads
     */
    struct ThreadData {
        std::unique_ptr<std::thread> thread;
        std::vector<std::shared_ptr<Watcher>> watchers;
        std::unique_ptr<std::mutex> watchersMutex;
        std::unique_ptr<std::condition_variable> sleepCv;
    };

    /**
     * @brief Worker thread function that polls assigned watchers for messages
     * @param threadIndex Index of this thread in the pool
     */
    void WorkerThreadFunc(size_t threadIndex);

    /**
     * @brief Process a single watcher's queue and send responses
     * @param watcher The watcher to process
     * @return true if the watcher is still active, false otherwise
     */
    bool ProcessWatcherQueue(const std::shared_ptr<Watcher> &watcher);

    std::vector<ThreadData> threadData_;
    std::atomic<bool> shutdown_{ false };
    std::atomic<size_t> nextThreadIndex_{ 0 };
};

/**
 * @brief Watch manager for managing watchers and their notifications.
 */
class WatchManager {
public:
    /**
     * @brief Constructor
     * @param watcherThreadPoolSize Number of threads in watcher thread pool (default: 4)
     */
    explicit WatchManager(size_t watcherThreadPoolSize = 4);
    ~WatchManager();

    /**
     * @brief Register a watcher
     * @param req The watch create request
     * @param stream The gRPC stream (raw pointer, lifetime managed by gRPC)
     * @param watchId Output watch ID
     * @param kvManager The KV store for historical events
     * @return Status of operation
     */
    Status RegisterWatcher(const etcdserverpb::WatchCreateRequest &req,
                           grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream,
                           int64_t *watchId, KVManager *kvManager);

    /**
     * @brief Cancel a watcher
     * @param watchId The watch ID
     * @param sendResponse Whether to send canceled response (true for normal cancel, false for cleanup)
     * @return Status of operation
     */
    Status CancelWatcher(int64_t watchId, bool sendResponse = true);

    /**
     * @brief Cancel all watchers for a specific stream (called when client disconnects)
     * @param stream The gRPC stream pointer
     */
    void CancelWatchersByStream(
        grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream);

    /**
     * @brief Notify watchers of a put event
     * @param key The key
     * @param kv The key-value pair
     * @param prevKv Previous key-value (optional)
     * @param revision Current revision
     */
    void NotifyPut(const std::string &key, const mvccpb::KeyValue &kv, const mvccpb::KeyValue *prevKv = nullptr,
                   int64_t revision = 0);

    /**
     * @brief Notify watchers of a delete event
     * @param key The key
     * @param kv The key-value pair (the deleted one)
     * @param revision Current revision
     */
    void NotifyDelete(const std::string &key, const mvccpb::KeyValue &kv, int64_t revision = 0);

private:
    /**
     * @brief Check if a key matches a watcher's range
     * @param key The key to check
     * @param watcher The watcher to check against
     * @return true if key matches the watcher's range
     */
    bool KeyMatchesWatcher(const std::string &key, const Watcher *watcher) const;

    /**
     * @brief Send a watch response
     * @param watcher The watcher to send response to
     * @param response The watch response to send
     * @return true if response sent successfully
     */
    bool SendWatchResponse(const std::shared_ptr<Watcher> &watcher, etcdserverpb::WatchResponse *response);

    /**
     * @brief Send historical events for a watcher
     * @param watcher The watcher to send events to
     * @param kvManager The KV store for reading historical data
     */
    void SendHistoricalEvents(const std::shared_ptr<Watcher> &watcher, KVManager *kvManager);

    /**
     * @brief Get or create a shared mutex for a given stream
     * @param stream The gRPC stream pointer
     * @return Shared mutex for this stream
     */
    std::shared_ptr<std::mutex> GetOrCreateStreamMutex(
        grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream);

    /**
     * @brief Remove a stream and its mutex from the registry
     * @param stream The gRPC stream pointer
     */
    void RemoveStreamMutex(
        grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest> *stream);

    std::unordered_map<int64_t, std::shared_ptr<Watcher>> watchers_;
    std::atomic<int64_t> nextWatchId_{ 1 };
    std::mutex mutex_;

    // Map from stream to shared mutex for synchronization
    // Multiple watchers can share the same gRPC stream, so they need a shared mutex
    // to ensure thread-safe gRPC Write calls
    std::unordered_map<grpc::ServerReaderWriter<etcdserverpb::WatchResponse, etcdserverpb::WatchRequest>*,
                     std::shared_ptr<std::mutex>> streamMutexMap_;
    std::mutex streamMutexMapMutex_;

    // Watcher thread pool for managing write threads
    std::unique_ptr<WatcherThreadPool> watcherThreadPool_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_METASTORE_SERVICE_WATCH_WATCH_MANAGER_H

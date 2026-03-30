/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Interface to etcd watch.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_Watch_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_ETCD_Watch_H

#include "etcd/api/etcdserverpb/rpc.grpc.pb.h"
#include <condition_variable>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/kvstore/etcd/grpc_session.h"
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
struct EtcdWatchResponseHeader {
    // ID of the cluster which sent the response.
    uint64_t clusterId = 0;

    // ID of the member which sent the response.
    uint64_t memberId = 0;

    // Key-value store revision when the request was applied. For watch progress
    // responses, the header.revision indicates progress. All future events received
    // in this stream are guaranteed to have a higher revision number than the header.
    // revision number.
    int64_t revision = 0;

    // The raft term when the request was applied.
    uint64_t raftTerm = 0;
};

struct EtcdWatchResponse {
    EtcdWatchResponseHeader header;

    // The ID of the watch that corresponds to the response. All events sent to the created watcher will have the same
    // watchId.
    int64_t watchId = 0;

    // set to true if the response is for a create watch request.
    bool created = true;

    // Set to true if the response is for a cancel watch request.
    bool canceled = false;

    //  set to the minimum historical revision available to etcd if a watcher tries watching at a compacted revision.
    int64_t compactRevision = 0;

    // A list of new events in sequence corresponding to the given watch ID.
    std::vector<mvccpb::Event> events;
};

std::string GetEventMsg(const mvccpb::Event &event);

struct RangeSearchResult {
    std::string key;
    std::string value;
    int64_t modRevision = 0;
    int64_t version = 0;

    std::string ToString() const
    {
        std::stringstream s;
        s << "key: " << key << ", mod_revision: " << modRevision << ", version: " << version;
        return s.str();
    }

    void ParseKeyValue(const ::mvccpb::KeyValue &kv)
    {
        key = kv.key();
        value = kv.value();
        version = kv.version();
        modRevision = kv.mod_revision();
    }
};

struct VersionInfo {
    int64_t modRevision = 0;
    int64_t version = 0;
    bool isDelete = false;

    std::string ToString() const
    {
        std::stringstream s;
        s << "mod_revision: " << modRevision << ", version: " << version << ", isDelete: " << isDelete;
        return s.str();
    }
};

using EtcdRangeGetVector = std::vector<RangeSearchResult>;

class EtcdWatch : public std::enable_shared_from_this<EtcdWatch> {
public:
    EtcdWatch(std::string address, std::unique_ptr<std::unordered_map<std::string, int64_t>> &&prefixMap);

    /**
     * @brief Monitors key changes in etcd.
     * @param[in] address Etcd address.
     * @param[in] prefixMap Prefix to be monitored on etcd.
     * @param[in] clientKit Parameters required for authentication between route client and etcd.
     */
    EtcdWatch(std::string address, std::unique_ptr<std::unordered_map<std::string, int64_t>> &&prefixMap,
              RouterClientCurveKit clientKit);

    ~EtcdWatch();

    /**
     * @brief Shutdown watch stream and exit everything.
     * @return K_OK on success; the error code otherwise.
     */
    Status Shutdown();

    /**
     * @brief Close watch producer.
     */
    void CloseProducer();

    /**
     * @brief Init watch stub and stream client, connect to ETCD server.
     * @param[in] authToken Token for etcd server.
     * @return K_OK on success; the error code otherwise.
     */
    Status Init(const std::string &authToken);

    /**
     * @brief Start the background thread to send heartbeat to ETCD server periodically.
     * @return K_OK on success; the error code otherwise.
     */
    Status Run();

    /**
     * @brief Set watch event handle callback function. When an entry is inserted or removed, it will be called.
     * @param[in] eventHandler Watch event handler.
     */
    void SetWatchEventHandler(std::function<void(mvccpb::Event &&event)> eventHandler);

    /**
     * @brief Set check etcd state callback function.
     * @param[in] checkEtcdStateHandler check etcd state handler.
     */
    void SetCheckEtcdStateHandler(std::function<Status()> checkEtcdStateHandler);

    /**
     * @brief Watch events and help call the handler function.
     * @return K_OK on success; the error code otherwise.
     */
    Status WatchEvents();

    /**
     * @brief Call user defined eventHandler on each event in etcdEventQue_.
     * @return K_OK on success; the error code otherwise.
     */
    Status EventHandler();

    /**
     * @brief A synchronization between calling thread and the fresh launch of the running Run() thread.
     * @return Status of the call
     */
    Status WaitForRunStartup();

    /**
     * @brief Generate a fake event.
     * @param[in] key The key of this event.
     * @param[in] val The value of this event.
     * @param[in] modRevision The mod_revision of this key.
     * @param[in] eventType The event type.
     * @param[in] version The version of this key.
     * @return A fake event.
     */
    mvccpb::Event GenerateFakeEvent(const std::string &key, const std::string &val, int64_t modRevision,
                                    mvccpb::Event_EventType eventType, int64_t version);

    /**
     * @brief Shutsdown stream and completion queue
     */
    void ShutdownEtcd();

    /**
     * @brief Set prefixsearch handler.
     * @param[in] prefixSearchHandler Will be used when doing event compensation,
     */
    void SetPrefixSearchHandler(
        std::function<Status(const std::string &, EtcdRangeGetVector &, int64_t &)> prefixSearchHandler)
    {
        prefixSearchHandler_ = std::move(prefixSearchHandler);
    }

    /**
     * @brief Set update cluster info in rocksdb handler.
     * @param[in] updateClusterInfoInRocksDbHandler Functions that handle events that are watched or obtained through
     * other means.
     */
    void SetUpdateClusterInfoInRocksDbHandler(
        std::function<void(const mvccpb::Event &event)> updateClusterInfoInRocksDbHandler)
    {
        updateClusterInfoInRocksDbHandler_ = std::move(updateClusterInfoInRocksDbHandler);
    }

    /**
     * @brief Start event compensation.
     */
    void RetrieveEventPassively();

    /**
     * @brief Make a temporary event compensation.
     * @return Status of the call
     */
    Status RetrieveEventActively();

    /**
     * @brief Generate fake event if needed;
     * @param[in] watchedFailed If the watch fails, a temporary event compensation needs to be done, and the next watch
     * version is initialized based on the return value of this event compensation.
     * @return K_OK on success; the error code otherwise.
     */
    Status GenerateFakeEventIfNeeded(bool watchedFailed = false);

private:
    /**
     * @brief Store read/write events in an event queue.
     * @param[in] response The response received from the watch events
     */
    inline void StoreEvents(EtcdWatchResponse &response);

    /**
     * @brief Gets status of Async calls from GRPC completion queue
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessWatchResponse(const void *tag);

    /**
     * @brief Issues Read calls to watch stream
     * @return K_OK on success; the error code otherwise.
     */
    Status ReadWatchStream();

    /**
     * @brief Issues write commands to create watch for a key
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateWatch();

    /**
     * @brief Update key version.
     * @param[in] key The key of this event.
     * @param[in] modRevision The event mod revision.
     * @param[in] version The etcd key version.
     * @param[in] eventType The event type.
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateKeyVersion(const std::string &key, int64_t modRevision, int64_t version,
                            mvccpb::Event::EventType eventType);

    /**
     * @brief Check if event expired.
     * @param[in] key The key of this event.
     * @param[in] modRevision The event mod_revision.
     * @param[out] curRevision The event version in local cache.
     * @return K_OK on success; the error code otherwise.
     */
    inline bool CheckIfEventExpired(const std::string &key, int64_t modRevision, int64_t &curModRevision)
    {
        std::shared_lock<std::shared_timed_mutex> lck(keyVersionMutex_);
        auto it = keyVersion_->find(key);
        if (it != keyVersion_->end()) {
            curModRevision = it->second.modRevision;
        }
        bool alreadyExpired = it != keyVersion_->end() && modRevision <= it->second.modRevision;
        return alreadyExpired;
    }

    /**
     * @brief Generate fake put event if needed.
     * @param[in] watchedFailed If the watch fails, a temporary event compensation needs to be done, and the next watch
     * version is initialized based on the return value of this event compensation.
     * @param[out] copyKeyVersion A copy of EtcdWatch::keyVersion_.
     * @param[out] prefix2Revision The revision when processing range get.
     * @return K_OK on success; the error code otherwise.
     */
    Status GenerateFakePutEventIfNeeded(bool watchedFailed,
                                        std::unordered_map<std::string, VersionInfo> &copyKeyVersion,
                                        std::unordered_map<std::string, int64_t> &curMap);
    /**
     * @brief Delay generate fake put event.
     * @param[out] outKeyValue The key value get from etcd.
     */
    void DelayGenerateFakePutEvent(const RangeSearchResult &outKeyValue);

    /**
     * @brief Generate fake delete event if needed.
     * @param[in] copyKeyVersion A copy of EtcdWatch::keyVersion_.
     * @param[in] prefix2Revision The revision when processing range get.
     */
    void GenerateFakeDeleteEventIfNeeded(const std::unordered_map<std::string, VersionInfo> &copyKeyVersion,
                                         const std::unordered_map<std::string, int64_t> &curMap);

    /**
     * @brief The implement of passive retrieval of events.
     * @return K_OK on success; the error code otherwise.
     */
    Status RetrieveEventPassivelyImpl();

    mutable std::shared_timed_mutex keyVersionMutex_;  // protects keyVersion_
    // This map is used to prevent old events from being processed. When an event is successfully consumed, the
    // version of the corresponding key in the map will be updated. Details: <key, <version, isDelete>>
    std::unique_ptr<std::unordered_map<std::string, VersionInfo>> keyVersion_;

    // Watch callback function, when event occurs, it will be called.
    std::function<void(mvccpb::Event &&event)> eventHandler_;

    // Deadline of 100 milliseconds
    static constexpr int32_t WaitTimeInMilliSecForEvents = 100;

    // Pools the stream for any watch events
    ThreadPool watchPool_;

    // gets errors from Etcd
    std::future<Status> producerStatus_;

    // These 2 members provide synchronization on the producerStatus_ future so that a shutdown thread can wait for the
    // submitted producer result to be completed.
    std::condition_variable producerCond_;
    std::mutex producerMtx_;

    // gets errors from Etcd
    std::future<Status> consumerStatus_;

    // Check watch threads are running or not.
    std::atomic<bool> shuttingDown_;

    // Protect shuttingDown flag during shutdowns
    WriterPrefRWLock shutdownLock_;

    // ETCD server address.
    std::string address_;

    std::shared_timed_mutex prefixMapMutex_;  // protect prefixMap_;
    // Prefix to be watched. <prefix, revision>
    std::unique_ptr<std::unordered_map<std::string, int64_t>> prefixMap_;

    // GRPC client context for watch request.
    std::unique_ptr<grpc::ClientContext> context_;

    // GRPC completion queue for async read/write.
    std::unique_ptr<grpc::CompletionQueue> cq_;

    // Queue to hold events.
    BlockingQueue<mvccpb::Event> etcdEventQue_;

    // watch stub.
    std::unique_ptr<GrpcSession<etcdserverpb::Watch>> watchSession_;

    // Async client reader writer.
    using WatchStream = grpc::ClientAsyncReaderWriter<etcdserverpb::WatchRequest, etcdserverpb::WatchResponse>;
    std::unique_ptr<WatchStream> stream_;

    // These 3 members provide a flag and synchronization for startup logic
    std::condition_variable waitCond_;
    std::mutex waitMtx_;
    bool startupSyncPoint_;

    // Router client connect paras
    RouterClientCurveKit clientCurveKit_;
    std::atomic<bool> isRouterClientCurveConnect_{ false };

    std::function<Status()> checkEtcdStateHandler_;

    WaitPost retrieveEventWaitPost_;
    const uint32_t EVENT_COMPENSATION_INTERVAL_UNDER_NORMAL_CONDITIONS_MS = 30'000;   // 30s
    const uint32_t EVENT_COMPENSATION_INTERVAL_UNDER_ABNORMAL_CONDITIONS_MS = 1'000;  // 1s
    uint64_t delayGenerateFakePutEventTimeMs_ = 60'000;
    std::function<Status(const std::string &, EtcdRangeGetVector &, int64_t &)> prefixSearchHandler_;
    std::function<void(const mvccpb::Event &event)> updateClusterInfoInRocksDbHandler_ =
        [](const mvccpb::Event &event) { (void)event; };
};
}  // namespace datasystem
#endif

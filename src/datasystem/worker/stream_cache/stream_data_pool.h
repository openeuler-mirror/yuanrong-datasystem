/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Stream data page pool
 */

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_STREAM_DATA_POOL_H
#define DATASYSTEM_WORKER_STREAM_CACHE_STREAM_DATA_POOL_H

#include <memory>

#include <tbb/concurrent_hash_map.h>
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/common/util/bitmask_enum.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/worker/stream_cache/page_queue/shared_page_queue.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class ClientWorkerSCServiceImpl;
class StreamManager;

class StreamDataPool {
public:
    struct ScanInfo {
        uint64_t cursor_;
        std::vector<std::string> dest_;
        std::unique_ptr<std::future<Status>> future_;
        std::unique_ptr<std::shared_timed_mutex> mux_;
        std::chrono::high_resolution_clock::time_point start_;
        ScanInfo(uint64_t cursor, std::vector<std::string> dest, std::unique_ptr<std::future<Status>> future);
        virtual Status GetPageQueue(std::shared_ptr<PageQueueBase> &pageQueue) = 0;
    };
    struct StreamScanInfo : ScanInfo {
        std::shared_ptr<StreamManager> mgr_;
        StreamScanInfo(std::shared_ptr<StreamManager> mgr, uint64_t cursor, std::vector<std::string> dest,
                       std::unique_ptr<std::future<Status>> future);
        virtual Status GetPageQueue(std::shared_ptr<PageQueueBase> &pageQueue) override;
    };
    struct SharedPageScanInfo : ScanInfo {
        std::weak_ptr<SharedPageQueue> sharedPageQueue_;
        SharedPageScanInfo(std::shared_ptr<SharedPageQueue> sharedPageQueue, uint64_t cursor,
                           std::vector<std::string> dest, std::unique_ptr<std::future<Status>> future);
        virtual Status GetPageQueue(std::shared_ptr<PageQueueBase> &pageQueue) override;
    };
    StreamDataPool();
    ~StreamDataPool();

    /**
     * Initialization
     * @return
     */
    Status Init();

    /**
     * @brief Add a stream data object to scan list
     * @param[in] streamMgr The stream manager of the actual stream, for exclusive page queue purpose.
     * @param[in] streamName The stream name to scan.
     * @param[in] dest The remote worker destination.
     * @param[in] lastAckCursor The last ack cursor.
     * @return Status of the call.
     */
    Status AddStreamObject(std::shared_ptr<StreamManager> streamMgr, const std::string &streamName,
                           const std::vector<std::string> &dest, uint64_t lastAckCursor);

    /**
     * @brief Add a shared page queue to scan list.
     * @param[in] sharedPageQueue The shared page queue to scan.
     * @param[in] streamName The actual stream name to scan.
     * @param[in] dest The remote worker destination.
     * @param[in] lastAckCursor The last ack cursor.
     * @return Status of the call.
     */
    Status AddSharedPageObject(std::shared_ptr<SharedPageQueue> sharedPageQueue, const std::string &streamName,
                               const std::vector<std::string> &dest, uint64_t lastAckCursor);

    /**
     * @brief Remove a stream object from scan list
     * @param mgr
     * @return
     */
    Status RemoveStreamObject(const std::string &streamName, const std::vector<std::string> &dest);

    /**
     * @brief Reset the scan position
     * @param streamName
     * @return
     */
    Status ResetStreamScanPosition(const std::string &streamName);

private:
    std::atomic<bool> interrupt_;
    const int numPartitions_;
    std::unique_ptr<ThreadPool> threadPool_;
    Thread scanner_;
    mutable std::shared_timed_mutex queueIdMux_;
    std::unordered_map<std::string, std::unordered_set<std::string>> queueIdMap_;
    struct ObjectPartition {
        uint64_t myId_;
        std::atomic<bool> interrupt_;
        mutable std::shared_timed_mutex objMux_;
        // The key is stream name for the normal streams, destination for the merge-streams.
        std::unordered_map<std::string, std::shared_ptr<ScanInfo>> objMap_;
        explicit ObjectPartition(uint64_t i) : myId_(i), interrupt_(false)
        {
        }
        ~ObjectPartition() = default;

        template <typename T, typename S>
        Status AddScanObject(const std::shared_ptr<T> &streamObj, const std::string &keyName,
                             const std::vector<std::string> &dest, uint64_t lastAckCursor,
                             std::unique_ptr<ThreadPool> &pool);
        Status RemoveScanObject(const std::string &streamName, const std::vector<std::string> &dest);
        Status ScanChangesAndEval(std::unordered_map<std::string, std::shared_ptr<ScanInfo>>::iterator &iter);
        Status ResetStreamScanPosition(const std::string &streamName);
        Status SendElementsToRemote(const std::string &streamName);
        void ScanChanges(std::unique_ptr<ThreadPool> &pool);
    };
    std::vector<std::unique_ptr<ObjectPartition>> partitionList_;

    void ScanChanges();
    void Stop();
    uint64_t GetPartId(const std::string &streamName) const;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_STREAM_CACHE_STREAM_DATA_POOL_H

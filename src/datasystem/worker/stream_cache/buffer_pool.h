/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Buffer pool
 */

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_BUFFER_POOL_H
#define DATASYSTEM_WORKER_STREAM_CACHE_BUFFER_POOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <list>
#include <memory>
#include <queue>
#include <set>
#include <unordered_map>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
/**
 * @brief Simple key/hash for std::unordered_map consists of worker address and producer id.
 */
struct StreamProducerKey {
    std::string firstKey_;
    std::string producerId_;
    std::string producerInstanceId_;

    StreamProducerKey(std::string firstKey, std::string producerId, std::string producerInstanceId)
        : firstKey_(std::move(firstKey)),
          producerId_(std::move(producerId)),
          producerInstanceId_(std::move(producerInstanceId))
    {
    }
    ~StreamProducerKey() = default;

    bool operator==(const StreamProducerKey &rhs) const;

    [[nodiscard]] std::string ToString() const
    {
        return FormatString("K:%s P:%s I:%s", firstKey_, producerId_, producerInstanceId_);
    }

    friend std::ostream &operator<<(std::ostream &out, const StreamProducerKey &key)
    {
        out << key.ToString();
        return out;
    }
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

namespace std {
template <>
struct hash<datasystem::worker::stream_cache::StreamProducerKey> {
    size_t operator()(const datasystem::worker::stream_cache::StreamProducerKey &key) const;
};
template <>
struct less<datasystem::worker::stream_cache::StreamProducerKey> {
    bool operator()(const datasystem::worker::stream_cache::StreamProducerKey &lhs,
                    const datasystem::worker::stream_cache::StreamProducerKey &rhs) const;
};
}  // namespace std

namespace datasystem {
namespace worker {
namespace stream_cache {
class BaseBufferData {
public:
    BaseBufferData() = default;
    virtual ~BaseBufferData() = default;

    [[nodiscard]] virtual std::string StreamName() const = 0;
    [[nodiscard]] virtual std::string ProducerName() const = 0;
    [[nodiscard]] virtual std::string ProducerInstanceId() const = 0;
    [[nodiscard]] virtual uint64_t StreamHash() const = 0;
    virtual Status ReleasePage() = 0;

    bool IsEoS() const
    {
        return eos;
    }

    [[nodiscard]] virtual std::string KeyName() const
    {
        return StreamName();
    };

    virtual uint64_t RecordSeqNo(std::function<uint64_t(const std::string &)> fetchAddSeqNo)
    {
        return fetchAddSeqNo(StreamName());
    };

    std::string traceId_;

protected:
    bool eos = false;
};

using BaseData = std::pair<std::shared_ptr<BaseBufferData>, uint64_t>;

// A special instance of BaseBufferData that marks the end of stream
using EndOfStreamCallbackFn = std::function<Status(std::list<BaseData>, const std::string &, const std::string &)>;
class EndOfStreamBufferData : public BaseBufferData {
public:
    EndOfStreamBufferData(std::string streamName, EndOfStreamCallbackFn f);
    ~EndOfStreamBufferData() override = default;
    std::string StreamName() const override;
    std::string ProducerName() const override;
    std::string ProducerInstanceId() const override;
    uint64_t StreamHash() const override;
    Status ReleasePage() override;

    /**
     * @brief Call back functor
     * @param Buffer not yet flushed.
     * @return Status object
     */
    Status operator()(std::list<BaseData> p, const std::string &s1, const std::string &s2)
    {
        return callbackFn_(std::move(p), s1, s2);
    }

private:
    friend class BufferPool;
    EndOfStreamCallbackFn callbackFn_;
    const std::string streamName_;
    std::atomic<int64_t> numJobs_;
    std::mutex mux_;
    std::condition_variable cv_;
};

class StreamDestructData : public BaseBufferData {
public:
    StreamDestructData(std::string streamName, std::vector<StreamProducerKey> producerKeysToErase,
                       std::vector<StreamProducerKey> producerKeysToReset);
    ~StreamDestructData() override = default;
    std::string StreamName() const override;
    std::string ProducerName() const override;
    std::string ProducerInstanceId() const override;
    uint64_t StreamHash() const override;
    Status ReleasePage() override;
    const std::vector<StreamProducerKey> &GetProducerKeysToErase() const
    {
        return producerKeysToErase_;
    }
    const std::vector<StreamProducerKey> &GetProducerKeysToReset() const
    {
        return producerKeysToReset_;
    }

private:
    const std::string streamName_;
    const std::vector<StreamProducerKey> producerKeysToErase_;
    const std::vector<StreamProducerKey> producerKeysToReset_;
};

class ConcurrentList {
public:
    ConcurrentList() = default;
    virtual ~ConcurrentList() = default;

    /**
     * @brief Check if the list is empty
     * @return T/F
     */
    bool Empty() const
    {
        return list_.empty();
    }

    /**
     * @brief Return the size of the list
     * @return number of element in the list
     */
    auto Size() const
    {
        return list_.size();
    }

    bool WaitForNotEmpty(uint64_t timeoutMs);
    virtual void Append(const std::shared_ptr<BaseBufferData> &p);
    std::list<BaseData> GetAll();

protected:
    friend class BufferPool;
    mutable std::mutex mux_;
    std::condition_variable cv_;
    std::list<BaseData> list_;
};

using PendingFlushList = std::vector<std::pair<StreamProducerKey, std::list<BaseData> &>>;
using BatchFlushCallbackFn = std::function<Status(int id, PendingFlushList &flushList)>;

class BufferPool {
public:
    /**
     * @brief Constructor
     * @param[in] numPartitions Number of partitions
     * @param[in] name The name of the buffer pool.
     * @param[in] f Call back function
     */
    explicit BufferPool(int numPartitions, const std::string &name, BatchFlushCallbackFn f);

    ~BufferPool();

    /**
     * @brief Init function
     * @return
     */
    Status Init();

    /**
     * @brief Shutdown buffer pool
     */
    void Stop();

    /**
     * @brief Insert a buffer
     * @param ele
     */
    void Insert(const std::shared_ptr<BaseBufferData> &ele);

    Status UnsortedInsert(std::shared_ptr<BaseBufferData> ele, uint64_t seqNo, uint64_t firstSeqNo);

    void PurgeBuffer(const std::string &streamName, const EndOfStreamCallbackFn &fn);

    /**
     * @brief Remove the info of useless stream from BufferPool
     * @param keyName The stream name or page name.
     * @param sharedPageName The shared page name. Empty if the stream uses exclusive page or the keyName is page.
     */
    void RemoveStream(const std::string &keyName, const std::string &sharedPageName);

    /**
     * @brief Check if there are tasks to be processed
     * @return T/F
     */
    bool HaveTasksToProcess();

    // For heap sort
    struct Compare {
        bool operator()(const BaseData &a, const BaseData &b)
        {
            return a.second > b.second;
        }
    };

private:
    void AsyncFlushEntry(int partitionID);

    /**
     * @brief Get dirty producerKeys to be processed.
     * @param[in] numPartitions Number of partitions.
     * @param[in] discardKeys The discard producerKeys.
     * @return The producerKeys to be processed.
     */
    std::vector<StreamProducerKey> FetchDirtyList(int partitionID, std::vector<StreamProducerKey> &discardKeys);
    void ReleaseBuffers(int partitionID);
    Status BatchAsyncFlush(int partitionID, std::vector<StreamProducerKey> &streamList);
    void PurgeSortHeap(int partitionID, const std::string &streamName);
    void ProcessEoSEntries(const StreamProducerKey &key, std::list<BaseData> &producerDirtyList);

    /**
     * @brief Clear producerKeyMap_
     * @param[in] numPartitions Number of partitions
     * @param[in] streamName The stream name.
     * @param[in] keys The producerKeys.
     */
    void ClearProducerKeyMap(int partitionID, const std::string &streamName,
                             const std::vector<StreamProducerKey> &keys);

    std::string name_;
    std::atomic<bool> interrupt_;
    const int numPartitions_;

    struct ProducerKeyMap {
        std::shared_timed_mutex mapMutex_;  // Protect producerKeyMap_
        std::unordered_map<std::string, std::vector<StreamProducerKey>>
            producerKeyMap_;  // Key: streamName/sharedPageName
    };
    std::vector<std::unique_ptr<ProducerKeyMap>> producerKeyMaps_;  // The reverse lookup table of StreamProducerKey

    struct ProducerDirtyList {
        std::list<BaseData> list_;
        mutable std::shared_mutex mux_;
        std::unordered_map<std::string, uint64_t> seqNo_;  // <streamName, seqNo>, use in sharedPage
        ProducerDirtyList() = default;
        ~ProducerDirtyList() = default;
        uint64_t FetchAddSeqNo(const std::string &streamName);
    };
    struct HeapSort {
        mutable WriterPrefRWLock mux_;
        std::priority_queue<BaseData, std::vector<BaseData>, Compare> que_;
        std::atomic<uint64_t> expectedSeqNo_{ 0 };
    };
    // Each partition consists of a FIFO dirty list, and per producer dirty list, and a heap sort
    struct Partition {
        ConcurrentList dirtyList_;  // FIFO
        std::unordered_map<StreamProducerKey, std::unique_ptr<ProducerDirtyList>>
            producerDirtyMap_;  // Key: streamName/sharedPageName
        std::atomic<bool> eosInjected_{ false };
    };
    void InjectEoS(const std::shared_ptr<EndOfStreamBufferData> &eos,
                   std::unordered_map<StreamProducerKey, std::unique_ptr<ProducerDirtyList>> &map,
                   std::vector<StreamProducerKey> &fifo);

    std::vector<std::unique_ptr<Partition>> partitionList_;
    struct HeapSortPartition {
        mutable WriterPrefRWLock mux_;
        std::unordered_map<StreamProducerKey, std::unique_ptr<HeapSort>> heapSortMap_;
    };
    std::vector<std::unique_ptr<HeapSortPartition>> heapSortMapDict_;
    std::unique_ptr<ThreadPool> thrd_;  // One for each partition
    BatchFlushCallbackFn batchFlushFn_;
    std::atomic<bool> isAsynFlushing_{ false };
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_BUFFER_POOL_H

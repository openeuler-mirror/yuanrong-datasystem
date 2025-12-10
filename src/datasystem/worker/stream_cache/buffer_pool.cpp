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

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/worker/stream_cache/buffer_pool.h"
#include "datasystem/stream/stream_config.h"

namespace datasystem {
namespace worker {
namespace stream_cache {

void BufferPool::Insert(const std::shared_ptr<BaseBufferData> &ele)
{
    // Hash using the producer id and append to one of the dirty lists.
    // Order of the elements from the same producer is important.
    auto hashVal = ele->StreamHash();
    auto partitionID = hashVal % static_cast<unsigned int>(numPartitions_);
    auto &dirtyList = partitionList_[partitionID]->dirtyList_;
    dirtyList.Append(ele);
    // Wake up the i/o cleaner
    dirtyList.cv_.notify_all();
}

void BufferPool::PurgeSortHeap(int partitionID, const std::string &streamName)
{
    auto &heapSortMapPartition = heapSortMapDict_[partitionID];
    WriteLock xlock(&heapSortMapPartition->mux_);
    for (auto &m : heapSortMapPartition->heapSortMap_) {
        if (m.first.firstKey_ != streamName) {
            continue;
        }
        auto &heapSort = m.second;
        WriteLock writeLock(&heapSort->mux_);
        // Pop all elements and get rid of them
        while (!heapSort->que_.empty()) {
            auto v = heapSort->que_.top();
            heapSort->que_.pop();
            // We can discard it, but we will still pass it to the async thread
            // so UsageMonitor can keep track of it.
            partitionList_[partitionID]->dirtyList_.Append(v.first);
        }
        // Clear the sequence number
        heapSort->expectedSeqNo_ = 0;
    }
}

void BufferPool::PurgeBuffer(const std::string &streamName, const EndOfStreamCallbackFn &fn)
{
    // Go through each partition
    auto eos = std::make_shared<EndOfStreamBufferData>(streamName, fn);
    eos->numJobs_ = numPartitions_;
    for (auto partitionID = 0; partitionID < numPartitions_; ++partitionID) {
        PurgeSortHeap(partitionID, streamName);
        auto &part = partitionList_[partitionID];
        // Send the EOS
        part->dirtyList_.Append(eos);
        part->dirtyList_.cv_.notify_all();
    }
    // Now we wait
    const int waitMs = 100;
    while (eos->numJobs_ > 0 && !interrupt_) {
        std::unique_lock<std::mutex> lock(eos->mux_);
        eos->cv_.wait_for(lock, std::chrono::milliseconds(waitMs), [&eos]() { return eos->numJobs_ == 0; });
    }
}

void BufferPool::RemoveStream(const std::string &keyName, const std::string &sharedPageName)
{
    for (auto i = 0; i < numPartitions_; i++) {
        // 1. get the producer keys to clean
        auto &producerKeyMap = producerKeyMaps_[i];
        std::vector<StreamProducerKey> keysToErase;
        std::vector<StreamProducerKey> keysToResetSeqNo;
        {
            std::shared_lock<std::shared_timed_mutex> lk(producerKeyMap->mapMutex_);
            auto it = producerKeyMap->producerKeyMap_.find(keyName);
            if (it != producerKeyMap->producerKeyMap_.end()) {
                keysToErase = it->second;
            }
            it = producerKeyMap->producerKeyMap_.find(sharedPageName);
            if (it != producerKeyMap->producerKeyMap_.end()) {
                keysToResetSeqNo = it->second;
            }
        }
        // 2. clear heapSortMap_
        if (!keysToErase.empty()) {
            auto &heapSortMapPartition = heapSortMapDict_[i];
            WriteLock xlock(&heapSortMapPartition->mux_);
            for (auto &key : keysToErase) {
                heapSortMapPartition->heapSortMap_.erase(key);
            }
        }
        // 3. send notification to clear producerDirtyMap_ and producerKeyMap_
        if (!keysToErase.empty() || !keysToResetSeqNo.empty()) {
            auto streamDestructData =
                std::make_shared<StreamDestructData>(keyName, std::move(keysToErase), std::move(keysToResetSeqNo));
            auto &part = partitionList_[i];
            part->dirtyList_.Append(streamDestructData);
            part->dirtyList_.cv_.notify_all();
        }
    }
}

Status BufferPool::UnsortedInsert(std::shared_ptr<BaseBufferData> ele, uint64_t seqNo, uint64_t firstSeqNo)
{
    auto hashVal = ele->StreamHash();
    int partitionID = hashVal % numPartitions_;
    auto key = StreamProducerKey(ele->KeyName(), ele->ProducerName(), ele->ProducerInstanceId());
    auto &heapSortMapPartition = heapSortMapDict_[partitionID];
    WriteLock xlock(&heapSortMapPartition->mux_);
    auto it = heapSortMapPartition->heapSortMap_.find(key);
    if (it == heapSortMapPartition->heapSortMap_.end()) {
        bool success;
        std::tie(it, success) = heapSortMapPartition->heapSortMap_.emplace(key, std::make_unique<HeapSort>());
        auto &producerKeyMap = producerKeyMaps_[partitionID];
        std::lock_guard<std::shared_timed_mutex> lk(producerKeyMap->mapMutex_);
        producerKeyMap->producerKeyMap_[key.firstKey_].emplace_back(key);
    }
    auto &heapSort = it->second;
    xlock.UnlockIfLocked();
    WriteLock writeLock(&heapSort->mux_);
    BaseData p(std::move(ele), seqNo);
    const std::string &streamName = p.first->StreamName();
    const std::string &producerId = p.first->ProducerName();
    // If the sequence number is duplicated, ignore it. We already have a copy
    if (seqNo < heapSort->expectedSeqNo_) {
        RETURN_STATUS_LOG_ERROR(K_DUPLICATED, FormatString("[S:%s P:%s] duplicated seqNo %zu expecting %zu.",
                                                           streamName, producerId, seqNo, heapSort->expectedSeqNo_));
    }
    // Insert the element into the heap sort
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s P:%s] Push seqNo %zu to heapsort.", streamName, producerId,
                                                seqNo);
    heapSort->que_.push(std::move(p));
    // Check if the top is what we are looking for.
    // Remote worker will send us the range of sequence it is sending
    auto topSeq = heapSort->que_.top().second;
    VLOG(SC_DEBUG_LOG_LEVEL) << FormatString(
        "[S:%s P:%s] Top %zu. Expecting %zu or %zu", heapSort->que_.top().first->StreamName(),
        heapSort->que_.top().first->ProducerName(), topSeq, firstSeqNo, heapSort->expectedSeqNo_);
    while (topSeq == firstSeqNo || topSeq == heapSort->expectedSeqNo_) {
        // Pop the element and increment the next expected
        auto v = heapSort->que_.top();
        heapSort->que_.pop();
        heapSort->expectedSeqNo_ = v.second + 1;
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s P:%s] Pop seqNo %zu from heapsort.", v.first->StreamName(),
                                                    v.first->ProducerName(), v.second);
        partitionList_[partitionID]->dirtyList_.Append(v.first);
        if (heapSort->que_.empty()) {
            break;
        }
        topSeq = heapSort->que_.top().second;
    }
    partitionList_[partitionID]->dirtyList_.cv_.notify_all();
    return Status::OK();
}

void BufferPool::ProcessEoSEntries(const StreamProducerKey &key, std::list<BaseData> &producerDirtyList)
{
    // For eos, just pass the remaining buffer to the call back function.
    LOG(INFO) << FormatString("[%s] Processing EoS entries. Size of list %zu", key.ToString(),
                              producerDirtyList.size());
    do {
        auto iter = std::find_if(producerDirtyList.begin(), producerDirtyList.end(),
                                 [](const auto &kv) { return kv.first->IsEoS(); });
        if (iter == producerDirtyList.end()) {
            break;
        }
        std::list<BaseData> dataLst;
        dataLst.splice(dataLst.end(), producerDirtyList, producerDirtyList.begin(), iter);
        auto eos = std::move(*iter);
        producerDirtyList.erase(iter);
        auto func = std::static_pointer_cast<EndOfStreamBufferData>(eos.first);
        VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[%s] Purging %zu buffers", key.ToString(), dataLst.size());
        LOG_IF_ERROR((*func)(dataLst, key.firstKey_, key.producerId_), "");
        func->numJobs_--;
    } while (!producerDirtyList.empty());
    if (!producerDirtyList.empty()) {
        LOG(INFO) << FormatString("[%s] %zu buffers remained after EoS", key.ToString(), producerDirtyList.size());
    }
}

Status BufferPool::BatchAsyncFlush(int partitionID, std::vector<StreamProducerKey> &streamList)
{
    INJECT_POINT("BufferPool.BatchAsyncFlush");
    if (VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL)) {
        std::ostringstream oss;
        oss << FormatString("Flushing partition %d. Number of producers %d: ", partitionID, streamList.size());
        for (auto &ele : streamList) {
            oss << FormatString("[%s] ", ele.ToString());
        }
        LOG(INFO) << oss.str();
    }
    auto &myPartition = partitionList_[partitionID];
    PendingFlushList flushList;
    bool eosInjected = true;
    if (myPartition->eosInjected_.compare_exchange_strong(eosInjected, false)) {
        LOG(INFO) << FormatString("EoS detected for partition %zu", partitionID);
    }
    for (auto &key : streamList) {
        auto dirtyIt = myPartition->producerDirtyMap_.find(key);
        CHECK_FAIL_RETURN_STATUS(dirtyIt != myPartition->producerDirtyMap_.end(), K_RUNTIME_ERROR,
                                 key.ToString() + " not found in producerDirtyMap_");
        auto &producerDirtyList = dirtyIt->second->list_;
        if (producerDirtyList.empty()) {
            continue;
        }
        if (eosInjected) {
            // Buffers before the eos will be passed to the call back function.
            ProcessEoSEntries(key, producerDirtyList);
        }
        if (!producerDirtyList.empty()) {
            flushList.emplace_back(key, producerDirtyList);
        }
    }
    if (flushList.empty()) {
        // streamList is the list of streams failing to flush out on return.
        streamList.clear();
        return Status::OK();
    }
    Status rc = batchFlushFn_(partitionID, flushList);
    // Check which one still has pending elements to flush
    std::vector<StreamProducerKey> failList;
    for (auto &ele : flushList) {
        if (ele.second.empty()) {
            continue;
        }
        VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("[%s] fails to flush out %zu elements", ele.first.ToString(),
                                                 ele.second.size());
        failList.emplace_back(ele.first);
    }
    streamList.swap(failList);
    return rc;
}

void BufferPool::InjectEoS(const std::shared_ptr<EndOfStreamBufferData> &eos,
                           std::unordered_map<StreamProducerKey, std::unique_ptr<ProducerDirtyList>> &map,
                           std::vector<StreamProducerKey> &fifo)
{
    const std::string streamName = eos->streamName_;
    for (auto &ele : map) {
        if (ele.first.firstKey_ == streamName) {
            eos->numJobs_++;
            ele.second->list_.emplace_back(eos, std::numeric_limits<uint64_t>::max());
            auto key = ele.first;
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("EoS inserted for producer/worker %s", ele.first.producerId_);
            if (std::find_if(fifo.begin(), fifo.end(), [&key](StreamProducerKey &ele) { return ele == key; })
                == fifo.end()) {
                fifo.emplace_back(key);
            }
        }
    }
}

void BufferPool::ClearProducerKeyMap(int partitionID, const std::string &streamName,
                                     const std::vector<StreamProducerKey> &keys)
{
    auto &producerKeyMap = producerKeyMaps_[partitionID];
    std::lock_guard<std::shared_timed_mutex> lk(producerKeyMap->mapMutex_);
    (void)EraseIf(producerKeyMap->producerKeyMap_[streamName],
                  [&keys](const StreamProducerKey &key) { return ContainsKey(keys, key); });
    if (producerKeyMap->producerKeyMap_[streamName].empty()) {
        producerKeyMap->producerKeyMap_.erase(streamName);
    }
}

std::vector<StreamProducerKey> BufferPool::FetchDirtyList(int partitionID, std::vector<StreamProducerKey> &discardKeys)
{
    std::vector<StreamProducerKey> fifo;  // Track uniqueness and maintain fifo
    auto &myPartition = partitionList_[partitionID];
    auto &dirtyList = myPartition->dirtyList_;
    auto fifoList = dirtyList.GetAll();
    auto &producerDirtyMap = myPartition->producerDirtyMap_;
    auto it = fifoList.begin();
    while (it != fifoList.end()) {
        auto keyName = it->first->KeyName();
        auto producerId = it->first->ProducerName();
        auto instanceId = it->first->ProducerInstanceId();
        auto streamDestructData = std::dynamic_pointer_cast<StreamDestructData>(it->first);
        if (streamDestructData) {
            auto &eraseKeys = streamDestructData->GetProducerKeysToErase();
            auto &resetKeys = streamDestructData->GetProducerKeysToReset();
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString(
                "Process streamDestructData: keyName=%s, eraseKeys=[%s], resetKeys=[%s]", keyName,
                VectorToString(eraseKeys), VectorToString(resetKeys));
            discardKeys.insert(discardKeys.end(), eraseKeys.begin(), eraseKeys.end());
            // 1. clear producerDirtyMap
            for (auto &key : eraseKeys) {
                producerDirtyMap.erase(key);
            }
            for (auto &key : resetKeys) {
                auto it = producerDirtyMap.find(key);
                if (it != producerDirtyMap.end()) {
                    (void)it->second->seqNo_.erase(keyName);
                }
            }
            // 2. clear producer key map
            ClearProducerKeyMap(partitionID, keyName, eraseKeys);
            // 3. discard keys in fifo
            std::string log;
            EraseIf(fifo, [&keyName, &log](const StreamProducerKey &key) {
                if (keyName == key.firstKey_) {
                    log += key.ToString() + ", ";
                    return true;
                }
                return false;
            });
            LOG_IF(INFO, !log.empty()) << "Discard streamdata of " << log;
        } else if (!it->first->IsEoS()) {
            StreamProducerKey key(keyName, producerId, instanceId);
            auto iter = producerDirtyMap.find(key);
            if (iter == producerDirtyMap.end()) {
                iter = producerDirtyMap.emplace(key, std::make_unique<ProducerDirtyList>()).first;
                auto &producerKeyMap = producerKeyMaps_[partitionID];
                std::lock_guard<std::shared_timed_mutex> lk(producerKeyMap->mapMutex_);
                producerKeyMap->producerKeyMap_[key.firstKey_].emplace_back(key);
            }
            if (std::find_if(fifo.begin(), fifo.end(), [&key](StreamProducerKey &ele) { return ele == key; })
                == fifo.end()) {
                fifo.emplace_back(key);
            }
            auto &producerDirtyList = iter->second->list_;
            // Assign a sequence number relative to this stream/producer
            auto fetchAddSeqNoFunc = [iter](const std::string &streamName) {
                return iter->second->FetchAddSeqNo(streamName);
            };
            it->second = it->first->RecordSeqNo(fetchAddSeqNoFunc);
            producerDirtyList.emplace_back(*it);
        } else {
            // EoS
            VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("EoS detected for S:%s", keyName);
            myPartition->eosInjected_ = true;
            auto eos = std::static_pointer_cast<EndOfStreamBufferData>(it->first);
            InjectEoS(eos, producerDirtyMap, fifo);
            eos->numJobs_--;
        }
        it = fifoList.erase(it);
    }
    return fifo;
}

void BufferPool::ReleaseBuffers(int partitionID)
{
    auto &part = partitionList_[partitionID];
    auto &dirtyList = part->dirtyList_;
    std::lock_guard<std::mutex> lock(dirtyList.mux_);
    if (!dirtyList.Empty()) {
        LOG(WARNING) << FormatString("AsyncFlush thread %d exits but %d remaining fifo requests", partitionID,
                                     dirtyList.Size());
        for (auto &ele : dirtyList.list_) {
            (void)ele.first->ReleasePage();
        }
    }
    for (auto &dirtyStream : part->producerDirtyMap_) {
        auto &streamDirtyList = dirtyStream.second->list_;
        if (!streamDirtyList.empty()) {
            LOG(WARNING) << FormatString("AsyncFlush thread %d exits but stream %s producer %s %zu remaining requests",
                                         partitionID, dirtyStream.first.firstKey_, dirtyStream.first.producerId_,
                                         streamDirtyList.size());
            for (auto &ele : streamDirtyList) {
                LOG(WARNING) << FormatString("[%s] Element seqNo %zu not sent", dirtyStream.first.ToString(),
                                             ele.second);
                (void)ele.first->ReleasePage();
            }
        }
    }
}

bool BufferPool::HaveTasksToProcess()
{
    for (const auto &partition : partitionList_) {
        std::lock_guard<std::mutex> lck(partition->dirtyList_.mux_);
        if (!partition->dirtyList_.Empty()) {
            return true;
        }
    }
    return isAsynFlushing_;
}

void BufferPool::AsyncFlushEntry(int partitionID)
{
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s]AsyncFlush thread %d starts up", name_, partitionID);
    auto &part = partitionList_[partitionID];
    auto &dirtyList = part->dirtyList_;
    const uint64_t timeoutMs = 10;
    std::vector<StreamProducerKey> pendingFlushList;
    Status rc;
    while (true) {
        // Wait on the cv for 0.1s for work or interrupt
        auto hasWorkToDo = dirtyList.WaitForNotEmpty(timeoutMs);
        if (interrupt_) {
            VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("AsyncFlush thread %d exits", partitionID);
            break;
        }
        auto traceGuard = Trace::Instance().SetTraceUUID();
        std::vector<StreamProducerKey> flushList;
        std::vector<StreamProducerKey> discardKeys;
        if (hasWorkToDo) {
            flushList = FetchDirtyList(partitionID, discardKeys);
        }
        // Append whatever we need to resume from last time
        for (auto &ele : pendingFlushList) {
            if (!ContainsKey(flushList, ele) && !ContainsKey(discardKeys, ele)) {
                VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] resume", ele.ToString());
                flushList.push_back(ele);
            }
        }
        pendingFlushList.clear();

        if (flushList.empty()) {
            isAsynFlushing_ = false;
            continue;
        }
        isAsynFlushing_ = true;
        // Ensure that no data is sent to remote worker.
        RequestCounter::GetInstance().ResetLastArrivalTime("BufferPool::AsyncFlushEntry");
        rc = BatchAsyncFlush(partitionID, flushList);
        if (rc.IsError()) {
            const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
            LOG_EVERY_N(WARNING, logPerCount) << "BatchAsyncFlush failed " << rc.ToString();
        }
        // flushList on return is the list of streams we fail to flush out.
        // Save them and resume them in the future
        pendingFlushList = std::move(flushList);
        isAsynFlushing_ = !pendingFlushList.empty();
        for (auto &ele : pendingFlushList) {
            VLOG(SC_DEBUG_LOG_LEVEL) << FormatString("[%s] remains pending flush", ele.ToString());
        }
        if (rc.IsOk() && !pendingFlushList.empty()) {
            const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
            LOG_EVERY_N(INFO, logPerCount) << FormatString("[%d] eles is pending flush", pendingFlushList.size());
        }
    }
    isAsynFlushing_ = false;
    ReleaseBuffers(partitionID);
}

Status BufferPool::Init()
{
    try {
        for (auto i = 0; i < numPartitions_; ++i) {
            thrd_->Execute([this, i]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                AsyncFlushEntry(i);
            });
        }
        return Status::OK();
    } catch (const std::exception &e) {
        RETURN_STATUS(K_RUNTIME_ERROR, e.what());
    }
}

void BufferPool::Stop()
{
    interrupt_ = true;
    for (auto &part : partitionList_) {
        part->dirtyList_.cv_.notify_all();
    }
    if (thrd_) {
        thrd_.reset();
    }
}

BufferPool::BufferPool(int numPartitions, const std::string &name, BatchFlushCallbackFn f)
    : name_(name),
      interrupt_(false),
      numPartitions_(std::max<int>(1, numPartitions)),
      thrd_(std::make_unique<ThreadPool>(numPartitions_, 0, name)),
      batchFlushFn_(std::move(f))
{
    partitionList_.reserve(numPartitions_);
    heapSortMapDict_.reserve(numPartitions);
    producerKeyMaps_.reserve(numPartitions);
    for (auto i = 0; i < numPartitions_; ++i) {
        partitionList_.emplace_back(std::make_unique<Partition>());
        heapSortMapDict_.emplace_back(std::make_unique<HeapSortPartition>());
        producerKeyMaps_.emplace_back(std::make_unique<ProducerKeyMap>());
    }
}

BufferPool::~BufferPool()
{
    Stop();

    std::string logs;
    for (auto partitionID = 0; partitionID < numPartitions_; partitionID++) {
        std::string log;
        auto size = heapSortMapDict_[partitionID]->heapSortMap_.size();
        log += size == 0 ? "" : "heapSortMap_ size = " + std::to_string(size);
        size = partitionList_[partitionID]->dirtyList_.Size();
        log += size == 0 ? "" : ", dirtyList_ size = " + std::to_string(size);
        size = partitionList_[partitionID]->producerDirtyMap_.size();
        log += size == 0 ? "" : ", producerDirtyMap_ size = " + std::to_string(size);
        size = producerKeyMaps_[partitionID]->producerKeyMap_.size();
        log += size == 0 ? "" : ", producerKeyMap_ size = " + std::to_string(size);
        if (!log.empty()) {
            logs += "Partition " + std::to_string(partitionID) + ": " + log;
        }
    }
    LOG_IF(WARNING, !logs.empty()) << "~BufferPool " << name_ << " with: " << logs;
}

void ConcurrentList::Append(const std::shared_ptr<BaseBufferData> &p)
{
    std::unique_lock<std::mutex> lock(mux_);
    list_.emplace_back(p, 0);
}

std::list<BaseData> ConcurrentList::GetAll()
{
    std::list<BaseData> dataList;
    std::unique_lock<std::mutex> lock(mux_);
    dataList.swap(list_);
    return dataList;
}

bool ConcurrentList::WaitForNotEmpty(uint64_t timeoutMs)
{
    std::unique_lock<std::mutex> lock(mux_);
    return cv_.wait_for(lock, std::chrono::milliseconds(timeoutMs), [this]() { return !Empty(); });
}

bool StreamProducerKey::operator==(const StreamProducerKey &rhs) const
{
    if (rhs.firstKey_ == firstKey_ && rhs.producerId_ == producerId_
        && rhs.producerInstanceId_ == producerInstanceId_) {
        return true;
    }
    return false;
}

EndOfStreamBufferData::EndOfStreamBufferData(std::string streamName, EndOfStreamCallbackFn fn)
    : BaseBufferData(), callbackFn_(std::move(fn)), streamName_(std::move(streamName)), numJobs_(0)
{
    eos = true;
}

std::string EndOfStreamBufferData::StreamName() const
{
    return streamName_;
}

std::string EndOfStreamBufferData::ProducerName() const
{
    return "";
}

std::string EndOfStreamBufferData::ProducerInstanceId() const
{
    return "";
}

uint64_t EndOfStreamBufferData::StreamHash() const
{
    return 0;
}
Status EndOfStreamBufferData::ReleasePage()
{
    return Status::OK();
}

StreamDestructData::StreamDestructData(std::string streamName, std::vector<StreamProducerKey> producerKeysToErase,
                                       std::vector<StreamProducerKey> producerKeysToReset)
    : BaseBufferData(),
      streamName_(std::move(streamName)),
      producerKeysToErase_(std::move(producerKeysToErase)),
      producerKeysToReset_(std::move(producerKeysToReset))
{
}

std::string StreamDestructData::StreamName() const
{
    return streamName_;
}

std::string StreamDestructData::ProducerName() const
{
    return "";
}

std::string StreamDestructData::ProducerInstanceId() const
{
    return "";
}

uint64_t StreamDestructData::StreamHash() const
{
    return 0;
}

Status StreamDestructData::ReleasePage()
{
    return Status::OK();
}

uint64_t BufferPool::ProducerDirtyList::FetchAddSeqNo(const std::string &streamName)
{
    std::unique_lock<std::shared_mutex> lock(mux_);
    auto it = seqNo_.find(streamName);
    if (it == seqNo_.end()) {
        // Sequence number starts from 1.
        bool success;
        std::tie(it, success) = seqNo_.emplace(streamName, 1);
    }
    uint64_t ret = it->second;
    it->second++;
    return ret;
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

namespace std {
size_t hash<datasystem::worker::stream_cache::StreamProducerKey>::operator()(
    const datasystem::worker::stream_cache::StreamProducerKey &key) const
{
    // This is the golden ratio. (2^64) / (( 1 + sqrt(5)) / 2)
    constexpr static uint64_t MAGIC_HASH = 0x9E3779B97F4A7C15ul;
    constexpr static uint64_t LEFT_SHIFT = 6;
    constexpr static uint64_t RIGHT_SHIFT = 2;
    std::vector<std::string> v{ key.firstKey_, key.producerId_, key.producerInstanceId_ };
    size_t seed = 0;
    for (auto &str : v) {
        auto val = std::hash<std::string>{}(str);
        seed ^= val + MAGIC_HASH + (seed << LEFT_SHIFT) + (seed >> RIGHT_SHIFT);
    }
    return seed;
}

bool less<datasystem::worker::stream_cache::StreamProducerKey>::operator()(
    const datasystem::worker::stream_cache::StreamProducerKey &lhs,
    const datasystem::worker::stream_cache::StreamProducerKey &rhs) const
{
    if (lhs.firstKey_ == rhs.firstKey_ && lhs.producerId_ == rhs.producerId_) {
        return std::less<std::string>{}(lhs.producerInstanceId_, rhs.producerInstanceId_);
    }
    if (lhs.firstKey_ == rhs.firstKey_) {
        return std::less<std::string>{}(lhs.producerId_, rhs.producerId_);
    }
    return std::less<std::string>{}(lhs.firstKey_, rhs.firstKey_);
}
}  // namespace std

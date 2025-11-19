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
#include <utility>

#include "datasystem/common/constants.h"
#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/request_counter.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/stream_cache/client_worker_sc_service_impl.h"
#include "datasystem/worker/stream_cache/consumer.h"
#include "datasystem/worker/stream_cache/stream_data_pool.h"
#include "datasystem/worker/stream_cache/stream_manager.h"

DS_DEFINE_int32(sc_scan_num_buckets, 1024, "Number of partitions for scanning streams");
DS_DEFINE_int32(sc_scan_interval_ms, 10, "Scan interval for remote send. Default to 10ms");
DS_DEFINE_int32(sc_scan_thread_num, 16, "Number of threads for scanning shared memory changes");
DS_DEFINE_validator(sc_scan_thread_num, &Validator::ValidateThreadNum);

namespace datasystem {
namespace worker {
namespace stream_cache {

StreamDataPool::StreamDataPool() : interrupt_(false), numPartitions_(std::max<int>(1, FLAGS_sc_scan_num_buckets))
{
    const size_t MIN_THREADS = 1;
    const size_t MAX_THREADS = std::max<size_t>(1, FLAGS_sc_scan_thread_num);
    threadPool_ = std::make_unique<ThreadPool>(MIN_THREADS, MAX_THREADS, "RemoteWorkerManager", true);
    threadPool_->SetWarnLevel(ThreadPool::WarnLevel::LOW);
    partitionList_.reserve(numPartitions_);
    for (auto i = 0; i < numPartitions_; ++i) {
        partitionList_.emplace_back(std::make_unique<ObjectPartition>(i));
    }
}

StreamDataPool::~StreamDataPool()
{
    Stop();
    if (scanner_.joinable()) {
        scanner_.join();
    }
    if (threadPool_) {
        threadPool_.reset();
    }
}

Status StreamDataPool::Init()
{
    RETURN_IF_EXCEPTION_OCCURS(scanner_ = Thread([this] {
                                   TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
                                   ScanChanges();
                               }));
    return Status::OK();
}

void StreamDataPool::Stop()
{
    interrupt_ = true;
    for (auto &part : partitionList_) {
        part->interrupt_ = true;
    }
}

template <typename T, typename S>
Status StreamDataPool::ObjectPartition::AddScanObject(const std::shared_ptr<T> &streamObj, const std::string &keyName,
                                                      const std::vector<std::string> &dest, uint64_t lastAckCursor,
                                                      std::unique_ptr<ThreadPool> &pool)
{
    RETURN_RUNTIME_ERROR_IF_NULL(streamObj);
    LOG(INFO) << FormatString("[S:%s, P:%zu] Started adding Data object", keyName, myId_);
    WriteLockHelper wlock(objMux_, [this, &keyName, funName = __FUNCTION__] {
        return FormatString("S:%s P:%zu %s:%s", keyName, myId_, funName, __LINE__);
    });
    auto it = objMap_.find(keyName);
    if (it == objMap_.end()) {
        auto future = std::make_unique<std::future<Status>>(
            pool->Submit([this, keyName]() { return SendElementsToRemote(keyName); }));
        std::shared_ptr<S> scanInfo = std::make_shared<S>(streamObj, lastAckCursor, dest, std::move(future));
        objMap_.emplace(keyName, std::static_pointer_cast<ScanInfo>(scanInfo));
        for (auto &rw : dest) {
            LOG(INFO) << FormatString("[RW:%s, S:%s, P:%zu] Data object added to scan list", rw, keyName, myId_);
        }
        return Status::OK();
    }
    LOG(INFO) << FormatString("[S:%s, P:%zu] Found in scan list", keyName, myId_);
    // Update the new destination. Others remain unchanged.
    auto &scanInfo = *(it->second);
    scanInfo.dest_ = dest;
    return Status::OK();
}

void StreamDataPool::ObjectPartition::ScanChanges(std::unique_ptr<ThreadPool> &pool)
{
    std::shared_lock<std::shared_timed_mutex> rlock(objMux_);
    if (objMap_.empty()) {
        return;
    }
    Timer timer;
    std::for_each(objMap_.begin(), objMap_.end(), [this, &pool](auto &kv) {
        if (interrupt_) {
            return;
        }
        const std::string streamName = kv.first;
        auto &scanInfo = *(kv.second);
        auto &fut = scanInfo.future_;
        if (fut->valid()) {
            if (fut->wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
                Status rc = fut->get();
                if (rc.IsError() && rc.GetCode() != K_NOT_FOUND && rc.GetCode() != K_TRY_AGAIN) {
                    LOG(INFO) << FormatString("[S:%s] Scan changes failed. %s", kv.first, rc.ToString());
                }
            } else {
                // Scan result not ready.
                return;
            }
        }
        // Submit a new one after some specified interval
        auto start = scanInfo.start_;
        auto now = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count() >= FLAGS_sc_scan_interval_ms
            && !interrupt_) {
            scanInfo.start_ = now;
            scanInfo.future_ = std::make_unique<std::future<Status>>(
                pool->Submit([this, streamName]() { return SendElementsToRemote(streamName); }));
        }
    });
    const uint32_t intervalMs = 1000;
    if (timer.ElapsedMilliSecond() > intervalMs) {
        LOG(WARNING) << FormatString("[P:%zu] Data object map traversal takes %d ms for %d streams.", myId_,
                                     timer.ElapsedMilliSecond(), objMap_.size());
    }
}

Status StreamDataPool::ObjectPartition::RemoveScanObject(const std::string &streamName,
                                                         const std::vector<std::string> &dest)
{
    LOG(INFO) << FormatString("[S:%s, P:%zu] Started removing Data object", streamName, myId_);
    INJECT_POINT("StreamDataPool::ObjectPartition::RemoveStreamObject.sleep");
    WriteLockHelper wlock(objMux_, [this, &streamName, funName = __FUNCTION__] {
        return FormatString("S:%s P:%zu %s:%s", streamName, myId_, funName, __LINE__);
    });
    auto it = objMap_.find(streamName);
    CHECK_FAIL_RETURN_STATUS(it != objMap_.end(), K_SC_STREAM_NOT_FOUND,
                             FormatString("Stream %s already not on scan list", streamName));
    // If there is no more remote worker, remove it from the scan list.
    if (dest.empty()) {
        LOG(INFO) << FormatString("[S:%s, P:%zu] Data object removed from scan list", streamName, myId_);
        // We no longer scan this stream for newly added element
        (void)objMap_.erase(it);
    } else {
        // Otherwise, update the destination
        auto &scanInfo = *(it->second);
        scanInfo.dest_ = dest;
        for (auto &rw : dest) {
            LOG(INFO) << FormatString("[RW:%s, S:%s, P:%zu] Data object updated in scan list", rw, streamName, myId_);
        }
    }
    return Status::OK();
}

Status StreamDataPool::ObjectPartition::ResetStreamScanPosition(const std::string &streamName)
{
    // There is a thread which continuously scans the objects for changes
    // We need to pause this thread.
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s, P:%zu] Started Reseting Scan Position", streamName, myId_);
    WriteLockHelper wlock(objMux_, [this, &streamName, funName = __FUNCTION__] {
        return FormatString("S:%s P:%zu %s:%s", streamName, myId_, funName, __LINE__);
    });
    auto it = objMap_.find(streamName);
    CHECK_FAIL_RETURN_STATUS(it != objMap_.end(), K_SC_STREAM_NOT_FOUND,
                             FormatString("Stream %s already not on scan list", streamName));
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("[S:%s, P:%zu] Reset data object found in scan list", streamName, myId_);
    auto &scanInfo = *(it->second);
    scanInfo.cursor_ = 0;
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s, P:%zu] ResetStreamScanPosition Successful", streamName, myId_);
    return Status::OK();
}

uint64_t StreamDataPool::GetPartId(const std::string &streamName) const
{
    return std::hash<std::string>{}(streamName) % numPartitions_;
}

Status StreamDataPool::AddStreamObject(std::shared_ptr<StreamManager> streamMgr, const std::string &streamName,
                                       const std::vector<std::string> &dest, uint64_t lastAckCursor)
{
    auto partitionID = GetPartId(streamName);
    auto &part = partitionList_[partitionID];
    // We aren't passing the accessor to the RWM. That is, we have a copy of
    // stream manager but there is no lock protection. We will check again
    // later at the RW layer
    return part->AddScanObject<StreamManager, StreamScanInfo>(streamMgr, streamName, dest, lastAckCursor, threadPool_);
}

Status StreamDataPool::AddSharedPageObject(std::shared_ptr<SharedPageQueue> sharedPageQueue,
                                           const std::string &streamName, const std::vector<std::string> &dest,
                                           uint64_t lastAckCursor)
{
    auto queueId = sharedPageQueue->GetPageQueueId();
    auto partitionID = GetPartId(queueId);
    auto &part = partitionList_[partitionID];
    RETURN_IF_NOT_OK((part->AddScanObject<SharedPageQueue, SharedPageScanInfo>(sharedPageQueue, queueId, dest,
                                                                               lastAckCursor, threadPool_)));
    std::unique_lock<std::shared_timed_mutex> xlock(queueIdMux_);
    auto iter = queueIdMap_.find(queueId);
    if (iter == queueIdMap_.end()) {
        bool success;
        std::tie(iter, success) = queueIdMap_.emplace(queueId, std::unordered_set<std::string>());
    }
    iter->second.emplace(streamName);
    return Status::OK();
}

Status StreamDataPool::RemoveStreamObject(const std::string &streamName, const std::vector<std::string> &dest)
{
    std::string queueId;
    bool unregister = false;
    {
        std::unique_lock<std::shared_timed_mutex> xlock(queueIdMux_);
        for (auto pair : queueIdMap_) {
            auto &streamNames = pair.second;
            auto it = streamNames.find(streamName);
            if (streamNames.find(streamName) != streamNames.end()) {
                queueId = pair.first;
                // When destination is empty, we no longer scan a stream.
                // And then when all of the streams sharing the shared page queue are gone,
                // the scan for shared page queue can be stopped.
                if (dest.empty()) {
                    streamNames.erase(it);
                }
                if (streamNames.empty()) {
                    unregister = true;
                }
                break;
            }
        }
    }
    const std::string &keyName = unregister ? queueId : streamName;
    auto partitionID = GetPartId(keyName);
    return partitionList_[partitionID]->RemoveScanObject(keyName, dest);
}

Status StreamDataPool::ResetStreamScanPosition(const std::string &streamName)
{
    auto partitionID = GetPartId(streamName);
    return partitionList_[partitionID]->ResetStreamScanPosition(streamName);
}

Status StreamDataPool::ObjectPartition::ScanChangesAndEval(
    std::unordered_map<std::string, std::shared_ptr<ScanInfo>>::iterator &iter)
{
    auto &scanInfo = *(iter->second);
    auto &mux = *(scanInfo.mux_);
    std::unique_lock<std::shared_timed_mutex> xlock(mux);
    std::shared_ptr<PageQueueBase> pageQueue;
    RETURN_IF_NOT_OK(scanInfo.GetPageQueue(pageQueue));
    uint64_t lastAckCursor = scanInfo.cursor_;
    std::vector<std::string> remoteWorkers = scanInfo.dest_;
    const std::string keyName = iter->first;
    ScanFlags flag = ScanFlags::PAGE_BREAK | ScanFlags::EVAL_BREAK;
    const int timeoutMs = 10;
    Status rc = pageQueue->ScanAndEval(lastAckCursor, timeoutMs, remoteWorkers, flag);
    // All these errors are okay
    // K_TRY_AGAIN is ok when we reach the last cursor on the last page.
    // K_SC_END_OF_PAGE is okay when we do page break.
    if (rc.IsOk() || rc.GetCode() == K_TRY_AGAIN || rc.GetCode() == K_SC_END_OF_PAGE) {
        rc = Status::OK();
        INJECT_POINT("StreamDataPool.ScanChangesAndEval.delaywakeup", [&lastAckCursor](int lastAckReplace) {
            lastAckCursor = lastAckReplace;
            return Status::OK();
        });
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s, P:%zu] Scan position moves from %zu to %zu", keyName, myId_,
                                                    scanInfo.cursor_, lastAckCursor);
        scanInfo.cursor_ = lastAckCursor;
    }
    return rc;
}

Status StreamDataPool::ObjectPartition::SendElementsToRemote(const std::string &streamName)
{
    INJECT_POINT("StreamDataPool.SendElementsToRemote.wait");
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[S:%s, P:%zu] Scan for changes ...", streamName, myId_);
    ReadLockHelper rlock(objMux_, [this, &streamName, funName = __FUNCTION__] {
        return FormatString("S:%s P:%zu %s:%s", streamName, myId_, funName, __LINE__);
    });
    Timer timer;
    auto iter = objMap_.find(streamName);
    CHECK_FAIL_RETURN_STATUS(iter != objMap_.end(), K_SC_STREAM_NOT_FOUND,
                             FormatString("Stream %s not found", streamName));
    auto rc = ScanChangesAndEval(iter);
    const uint32_t intervalMs = 1000;
    if (timer.ElapsedMilliSecond() > intervalMs) {
        LOG(WARNING) << FormatString("[S:%s, P:%zu] Scan for changes takes %d ms.", streamName, myId_,
                                     timer.ElapsedMilliSecond());
    }
    return rc;
}

void StreamDataPool::ScanChanges()
{
    LOG(INFO) << "StreamDataPool scanner starts up";
    const int intervalMs = FLAGS_sc_scan_interval_ms;
    while (true) {
        for (auto &part : partitionList_) {
            part->ScanChanges(threadPool_);
        }
        if (interrupt_) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
}

StreamDataPool::ScanInfo::ScanInfo(uint64_t cursor, std::vector<std::string> dest,
                                   std::unique_ptr<std::future<Status>> future)
    : cursor_(cursor),
      dest_(std::move(dest)),
      future_(std::move(future)),
      mux_(std::make_unique<std::shared_timed_mutex>()),
      start_(std::chrono::high_resolution_clock::now())
{
}

StreamDataPool::StreamScanInfo::StreamScanInfo(std::shared_ptr<StreamManager> mgr, uint64_t cursor,
                                               std::vector<std::string> dest,
                                               std::unique_ptr<std::future<Status>> future)
    : ScanInfo(cursor, dest, std::move(future)), mgr_(mgr)
{
}

Status StreamDataPool::StreamScanInfo::GetPageQueue(std::shared_ptr<PageQueueBase> &pageQueue)
{
    RETURN_RUNTIME_ERROR_IF_NULL(mgr_);
    // If the stream is being deleted, move on
    RETURN_IF_NOT_OK(mgr_->CheckIfStreamActive());
    if (mgr_->IsRetainData()) {
        return { K_TRY_AGAIN,
                 FormatString("[S:%s] The expected num of consumers are not yet created.", mgr_->GetStreamName()) };
    }
    pageQueue = std::static_pointer_cast<PageQueueBase>(mgr_->GetExclusivePageQueue());
    return Status::OK();
}

StreamDataPool::SharedPageScanInfo::SharedPageScanInfo(std::shared_ptr<SharedPageQueue> sharedPageQueue,
                                                       uint64_t cursor, std::vector<std::string> dest,
                                                       std::unique_ptr<std::future<Status>> future)
    : ScanInfo(cursor, dest, std::move(future)), sharedPageQueue_(sharedPageQueue)
{
}

Status StreamDataPool::SharedPageScanInfo::GetPageQueue(std::shared_ptr<PageQueueBase> &pageQueue)
{
    std::shared_ptr<SharedPageQueue> sharedPageQue = sharedPageQueue_.lock();
    RETURN_RUNTIME_ERROR_IF_NULL(sharedPageQue);
    pageQueue = std::static_pointer_cast<PageQueueBase>(sharedPageQue);
    return Status::OK();
}

}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem

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
 * Description: Implementation of stream cache consumer.
 */
#include "datasystem/client/stream_cache/consumer_impl.h"
#include <numeric>
#include <utility>
#include "datasystem/client/listen_worker.h"
#include "datasystem/client/stream_cache/client_worker_api.h"
#include "datasystem/client/stream_cache/receive_element.h"
#include "datasystem/client/stream_cache/stream_client_impl.h"
#include "datasystem/common/constants.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/queue/circular_queue.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace client {
namespace stream_cache {
ConsumerImpl::ConsumerImpl(std::string streamName, std::string tenantId, SubscriptionConfig config,
                           std::string consumerId, const SubscribeRspPb &rsp,
                           std::shared_ptr<client::stream_cache::ProducerConsumerWorkerApi> workerApi,
                           std::shared_ptr<StreamClientImpl> client, client::MmapManager *mmapManager,
                           std::shared_ptr<client::ListenWorker> listenWorker, bool autoAck)
    : ClientBaseImpl(std::move(streamName), std::move(tenantId), std::move(workerApi), std::move(client), mmapManager,
                     std::move(listenWorker)),
      config_(std::move(config)),
      consumerId_(std::move(consumerId)),
      lastRecvCursor_(rsp.last_recv_cursor()),
      pageBoundaryCursor_(lastRecvCursor_.load()),
      consumedElements_(lastRecvCursor_.load()),
      ackedElementId_(lastRecvCursor_.load()),
      rsp_(rsp),
      autoAck_(autoAck)
{
    receiveWp_.Set();
    workArea_.fd = rsp_.worker_fd();
    workArea_.mmapSz = rsp_.mmap_size();
    workArea_.off = static_cast<ptrdiff_t>(rsp_.offset());
    workArea_.sz = rsp_.size();
}

ConsumerImpl::~ConsumerImpl()
{
    if (state_ == State::NORMAL) {
        LOG(INFO) << FormatString("[%s] Implicit close consumer.", LogPrefix());
        Status rc = Close();
        if (rc.IsError()) {
            LOG(ERROR) << "[" + LogPrefix() + "] Implicit close consumer failed " + rc.GetMsg();
        }
    }
    client_->ClearConsumer(consumerId_);
    LOG(INFO) << FormatString("[%s] Consumer destroy finish.", LogPrefix());
}

Status ConsumerImpl::Init()
{
    static const uint32_t SC_CACHE_MAX = 1048576;
    static const uint32_t SC_CACHE_MIN = 64;
    static const uint16_t PREFETCH_MAX = 100;  // 100 percent

    RETURN_IF_NOT_OK(ClientBaseImpl::Init());
    // Sanity check. The worker has already populated the lastAckCursor in the work area.
    // It should match the one we received.
    auto lastRecvCursor = GetWALastAckCursor();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        lastRecvCursor == lastRecvCursor_, K_OUT_OF_RANGE,
        FormatString("Work area eye catcher mismatch. Expect %zu but get %zu", lastRecvCursor_, lastRecvCursor));

    // Init prefetch area and fields if needed
    RETURN_RUNTIME_ERROR_IF_NULL(client_);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(config_.cacheCapacity <= SC_CACHE_MAX && config_.cacheCapacity >= SC_CACHE_MIN,
                                         K_INVALID, "Cache capacity must be between 64 and 1048576");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        config_.cachePrefetchLWM <= PREFETCH_MAX, K_INVALID,
        "Cache prefetch LWM must be between 0 and 100 (it is used as a percentage value).");

    elementCacheQueue_ = std::make_unique<HeapCircularQ<Element>>(config_.cacheCapacity);
    cachePrefetchLWM_ = (static_cast<float>(config_.cachePrefetchLWM) / PREFETCH_MAX) * config_.cacheCapacity;

    if (cachePrefetchLWM_ != 0) {
        RETURN_IF_NOT_OK(client_->CreatePrefetchPoolIfNotExist());
    }
    LOG(INFO) << "Consumer initialized. Prefetch LWM: " << cachePrefetchLWM_
              << " Cache capacity: " << elementCacheQueue_->Capacity() << " LastAckCursor: " << lastRecvCursor;
    return Status::OK();
}

void ConsumerImpl::SetInactive()
{
    // If we have a page, wake up any thread that is waiting on Receive
    std::shared_lock<std::shared_timed_mutex> lock(idxMutex_);
    for (auto &ele : idx_) {
        if (ele.second) {
            ele.second->WakeUpConsumers();
        } else {
            constexpr int timeSec = 10;
            LOG_EVERY_T(WARNING, timeSec) << "Page is nullptr!";
        }
    }
    ClientBaseImpl::SetInactive();
}

Status ConsumerImpl::CheckNormalState() const
{
    // If the worker's state is unknown, do not try to access the shared memory work area
    RETURN_IF_NOT_OK(ClientBaseImpl::CheckNormalState());
    if (cursor_ == nullptr || cursor_->ForceClose()) {
        RETURN_STATUS(K_SC_PRODUCER_NOT_FOUND, "Force close. No producer");
    }
    return Status::OK();
}

Status ConsumerImpl::ExtractBigElements(std::shared_ptr<StreamDataPage> &dataPage,
                                        std::vector<DataElement> &recvElements)
{
    for (size_t i = 0; i < recvElements.size(); ++i) {
        auto &ele = recvElements[i];
        auto cursor = ele.id;
        Status rc = ele.CheckAttribute();
        if (rc.IsError()) {
            RETURN_STATUS_LOG_ERROR(
                rc.GetCode(), FormatString("[%s] Page<%s> Cursor %zu:", LogPrefix(), dataPage->GetPageId(), cursor));
        }
        if (ele.IsBigElement()) {
            ShmView v;
            RETURN_IF_NOT_OK(StreamDataPage::ParseShmViewPb(recvElements[i].ptr, recvElements[i].size, v));
            std::shared_ptr<ShmUnitInfo> shmInfo;
            std::shared_ptr<client::MmapTableEntry> mmapEntry;
            RETURN_IF_NOT_OK(GetShmInfo(v, shmInfo, mmapEntry));
            auto bigElementPage = std::make_shared<StreamLobPage>(shmInfo, true, mmapEntry);
            RETURN_IF_NOT_OK(bigElementPage->Init());
            // Replace the original pointer with the big element pointer
            recvElements[i].ptr = reinterpret_cast<uint8_t *>(bigElementPage->GetPointer());
            recvElements[i].size = bigElementPage->PageSize();
            LOG(INFO) << FormatString("[%s] Page<%s> Cursor %zu BigElement<%s> Size %zu", LogPrefix(),
                                      dataPage->GetPageId(), cursor, bigElementPage->GetPageId(),
                                      bigElementPage->PageSize());
        }
    }
    return Status::OK();
}

Status ConsumerImpl::PrefetchElements(uint32_t timeoutMs, std::shared_ptr<StreamDataPage> &dataPage, uint32_t targetNum,
                                      uint32_t &totalFetched, bool nonBlockingFetch)
{
    while (totalFetched < targetNum) {
        uint32_t numFetched = 0;
        // If the cache has no room, do nothing.
        RETURN_OK_IF_TRUE(elementCacheQueue_->IsFull());
        auto remaining = elementCacheQueue_->Remaining();
        uint64_t lastRecvCursor = lastRecvCursor_.load(std::memory_order_relaxed);
        std::vector<DataElement> recvElements;
        // Grab whatever on the page.
        RETURN_IF_NOT_OK(dataPage->Receive(lastRecvCursor, timeoutMs, recvElements, LogPrefix()));
        RETURN_IF_NOT_OK(ExtractBigElements(dataPage, recvElements));
        RETURN_IF_NOT_OK(ProcessHeaders(recvElements));
        // Push them to the cache. Just copy up to remaining. We will resume next time
        // from we left off once we move up lastRecvCursor_;
        auto numElementsToAdd = std::min<size_t>(remaining, recvElements.size());
        std::vector<Element> elementListToAdd(numElementsToAdd);
        std::copy_n(recvElements.begin(), numElementsToAdd, elementListToAdd.begin());
        CHECK_FAIL_RETURN_STATUS(elementCacheQueue_->BatchPush(elementListToAdd), StatusCode::K_RUNTIME_ERROR,
                                 "Fail to batch push");
        VLOG(SC_NORMAL_LOG_LEVEL) << "Prefetch added " << elementListToAdd.size() << " elements to the local cache.";
        lastRecvCursor += elementListToAdd.size();
        lastRecvCursor_.store(lastRecvCursor);
        numFetched = static_cast<uint32_t>(elementListToAdd.size());
        totalFetched += numFetched;
        cursor_->IncrementElementCount(numFetched);
        RETURN_OK_IF_TRUE(totalFetched >= targetNum);
        // For non-blocking fetch, re-check the same page if there is more coming or a new
        // page has been created to continue.
        if (nonBlockingFetch) {
            continue;
        } else {
            // For blocking fetch, exit from here and the caller has its own clock to time it.
            break;
        }
    }
    return Status::OK();
}

Status ConsumerImpl::GetDataPage(const ShmView &shmView, std::shared_ptr<StreamDataPage> &out)
{
    std::shared_ptr<ShmUnitInfo> pageUnit;
    std::shared_ptr<client::MmapTableEntry> mmapEntry;
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(GetShmInfo(shmView, pageUnit, mmapEntry), "GetShmInfo");
    auto page = std::make_shared<StreamDataPage>(pageUnit, lockId_, true, false, mmapEntry);
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(page->Init(), "Page Init");
    // Worker Eyecatcher V1 works without consumer side client ref count.
    if (workerVersion_ < Cursor::K_WORKER_EYECATCHER_V1) {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(page->RefPage(LogPrefix()), "RefPage");
    }
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("[%s] Page<%s> acquired", LogPrefix(), page->GetPageId());
    lastPage_ = page;
    out = std::move(page);
    return Status::OK();
}

Status ConsumerImpl::GetPrefetchPage(int64_t timeoutMs, std::shared_ptr<StreamDataPage> &out)
{
    std::shared_ptr<StreamDataPage> page;
    GetDataPageReqPb req;
    req.set_stream_name(streamName_);
    req.set_subscription_name(config_.subscriptionName);
    req.set_consumer_id(consumerId_);
    req.set_last_recv_cursor(lastRecvCursor_);
    req.set_timeout_ms(timeoutMs);
    ShmView shmView;
    RETURN_IF_NOT_OK(workerApi_->GetDataPage(req, shmView));
    RETURN_IF_NOT_OK(GetDataPage(shmView, page));
    out = page;
    return Status::OK();
}

Status ConsumerImpl::LocatePrefetchPage(int64_t timeoutMs, std::shared_ptr<StreamDataPage> &out)
{
    std::unique_lock<std::shared_timed_mutex> xlock(idxMutex_);
    if (!idx_.empty()) {
        // Locate the page containing lastRecvCursor_;
        // But that should be the last page.
        out = idx_.rbegin()->second;
        return Status::OK();
    }
    // Without knowing where to start, send a rpc request to the worker.
    std::shared_ptr<StreamDataPage> startPage;
    RETURN_IF_NOT_OK(GetPrefetchPage(timeoutMs, startPage));
    LOG(INFO) << FormatString("%s Fetch page [%s]", LogPrefix(), startPage->GetPageId());
    // Use the current cursor as the key.
    idx_.emplace(startPage->GetBegCursor() - 1, startPage);
    out = startPage;
    return Status::OK();
}

Status ConsumerImpl::PrefetchEntry(uint32_t targetNum, uint32_t timeoutMs)
{
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("Fetch at least %zu elements within %zu ms", targetNum, timeoutMs);
    Timer t(timeoutMs);
    uint32_t totalFetched = 0;
    Status rc;
    std::shared_ptr<StreamDataPage> page;
    const uint32_t futexWaitMs = 10;
    // nonBlockingFetch is only true when user set timeoutMs to 0
    bool nonBlockingFetch = timeoutMs == 0;
    do {
        if (page == nullptr) {
            rc = LocatePrefetchPage(t.GetRemainingTimeMs(), page);
            if (rc.GetCode() == K_NOT_FOUND) {
                VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("Page for %zu not yet created", lastRecvCursor_);
                return Status::OK();
            }
            RETURN_IF_NOT_OK(rc);
        }
        RETURN_IF_NOT_OK(CheckNormalState());
        // There is a chance that remaining time is cast to int 0 if it is less than 1.
        // Add bool nonBlockingFetch to check if it is really a nonBlockingFetch or not.
        rc = PrefetchElements(futexWaitMs, page, targetNum, totalFetched, nonBlockingFetch);
        if (rc.GetCode() == K_SC_END_OF_PAGE) {
            // Without sending a rpc, following the pointer on the page to get the next page.
            ShmView nextPageView = page->GetNextPage();
            std::shared_ptr<StreamDataPage> nextPage;
            // Add the page to stream index (similar to worker logic). This marks the page boundary
            // where we will do ack or auto ack.
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(
                GetDataPage(nextPageView, nextPage),
                FormatString("[%s] Error acquiring the next page. Current page<%s>", LogPrefix(), page->GetPageId()));
            {
                std::unique_lock<std::shared_timed_mutex> xlock(idxMutex_);
                // Just like the worker's logic, we use the last slot of the previous page as the key
                uint64_t key = page->GetLastCursor();
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(key + 1 == nextPage->GetBegCursor(), K_OUT_OF_RANGE,
                                                     FormatString("[%s] Expect begCursor %zu but get %zu", LogPrefix(),
                                                                  key + 1, nextPage->GetBegCursor()));
                bool success = idx_.emplace(key, nextPage).second;
                CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(success, K_DUPLICATED,
                                                     FormatString("[%s] Duplicate key %zu", LogPrefix(), key));
                // It is a potential cursor to ack.
                pageBoundaryCursor_.store(key, std::memory_order_relaxed);
            }
            page = nextPage;
            rc = Status::OK();
        }
        RETURN_IF_NOT_OK_EXCEPT(rc, K_TRY_AGAIN);
    } while (totalFetched < targetNum
             && ((!nonBlockingFetch && t.ElapsedMilliSecond() < static_cast<double>(timeoutMs))
                 || (nonBlockingFetch && rc.GetCode() != K_TRY_AGAIN)));
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("Total element fetched %zu in %zu ms", totalFetched,
                                                static_cast<int64_t>(t.ElapsedMilliSecond()));
    return Status::OK();
}

Status ConsumerImpl::PrefetchReceive()
{
    PerfPoint point(PerfKey::CLIENT_PREFETCH_RECEIVE);
    // Set up a receive call to get as much as possible, limited by cache capacity (how many slots remaining).
    // This receive always uses zero timeout setting. Either the data is there or it
    // is not, do not wait for it.
    uint32_t totalCacheSlots = elementCacheQueue_->Remaining();
    VLOG(SC_NORMAL_LOG_LEVEL) << "Prefetch receive will be run. Total cache slots available: " << totalCacheSlots;
    // Pass a zero timeout setting. Either the data is there or it is not, do not wait for it.
    RETURN_IF_NOT_OK(PrefetchEntry(totalCacheSlots, 0));
    return Status::OK();
}

Status ConsumerImpl::CacheFetch(uint32_t cacheLength, uint32_t receiveNum, std::vector<Element> &outElements)
{
    PerfPoint point(PerfKey::CLIENT_CACHE_FETCH);
    VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("%s In cache, current remain %zu element, expectNum:%zu", LogPrefix(),
                                                cacheLength, receiveNum);
    RETURN_IF_NOT_OK(GetElementFromCache(receiveNum, outElements));
    RETURN_IF_NOT_OK(PostRecvAckHandler(outElements));
    return Status::OK();
}

Status ConsumerImpl::CacheHandler(uint32_t &cacheLength, uint32_t receiveNum, uint32_t timeoutMs,
                                  std::vector<Element> &outElements, bool &needsRecv)
{
    needsRecv = true;
    // If there is enough data in the cache, then fetch this data and return.
    if (cacheLength >= receiveNum) {
        RETURN_IF_NOT_OK(CacheFetch(cacheLength, receiveNum, outElements));
        needsRecv = false;

        // Before returning, check if we should replenish the cache by invoking a prefetch.
        // Conditions for submitting the prefetch task:
        // - The cache is currently below the prefetch low water mark.
        // - the prefetch thread pool is not at capacity (if too busy, just skip this prefetch)
        // - there was already an inflight (or recently completed prefetch that has not been handled yet).
        ThreadPool *pfPool = client_->GetPrefetchPool();
        uint64_t newCacheLength = elementCacheQueue_->Length();
        if (newCacheLength < cachePrefetchLWM_ && pfPool->GetWaitingTasksNum() == 0 && !prefetchStatus_.valid()) {
            prefetchStatus_ = client_->GetPrefetchPool()->Submit([this]() {
                RETURN_IF_NOT_OK(PrefetchReceive());
                return Status::OK();
            });
        }

        return Status::OK();
    }

    // At this point, there is not enough data in the cache. If a prefetch is running in the background (from
    // a previous receive), it does not make sense to invoke a new recv again. Wait for the prefetch to complete and
    // then perform cache fetch if possible.
    if (cachePrefetchLWM_ > 0 && prefetchStatus_.valid()) {
        prefetchStatus_.wait();
        Status rc = prefetchStatus_.get();
        LOG_IF_ERROR(rc, "A previous prefetch attempt got an error.");

        // refresh the cache length since we waited for a prefetch thread which may have put more in the cache.
        // If there is now enough data in cache queue the prefetch task came back, fetch it and we're done.
        cacheLength = elementCacheQueue_->Length();
        if (cacheLength >= receiveNum) {
            VLOG(SC_NORMAL_LOG_LEVEL) << "Waited for a prefetch task to complete. Fetch from cache now.";
            RETURN_IF_NOT_OK(CacheFetch(cacheLength, receiveNum, outElements));
            needsRecv = false;
        } else if (timeoutMs == 0) {
            // Not enough data in the cache, but a prefetch just completed (not enough data fetched).
            // Do not tell the caller to receive if the timeout is 0, because we just did a fetch and didn't get
            // enough data. No need to ask the worker again.
            VLOG(SC_NORMAL_LOG_LEVEL) << "Waited for a prefetch task to complete. Not enough data.";
            needsRecv = false;
        }
    }
    return Status::OK();
}

Status ConsumerImpl::ReceiveImpl(Optional<uint32_t> expectNum, uint32_t timeoutMs, std::vector<Element> &outElements)
{
    INJECT_POINT("consumerImpl.receive.fail");
    PerfPoint point(PerfKey::CLIENT_RECEIVE_ALL);
    receiveWp_.Clear();
    Raii receiveRaii([this]() { receiveWp_.Set(); });
    RETURN_IF_NOT_OK(CheckNormalState());
    if (expectNum) {
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(expectNum.value() > 0, StatusCode::K_INVALID,
                                             "The Receive expectNums must be greater than 0");
    }

    // if prefetching was enabled, collect the status of the previous prefetch if it was immediately ready.
    if (cachePrefetchLWM_ > 0 && prefetchStatus_.valid()
        && prefetchStatus_.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        Status rc = prefetchStatus_.get();
        LOG_IF_ERROR(rc, "A previous prefetch attempt got an error.");
    }
    outElements.clear();
    uint32_t cacheLength = elementCacheQueue_->Length();
    auto receiveNum = (expectNum) ? expectNum.value() : (cacheLength > 0) ? cacheLength : 1u;

    bool needsRecv;
    RETURN_IF_NOT_OK(CacheHandler(cacheLength, receiveNum, timeoutMs, outElements, needsRecv));
    RETURN_OK_IF_TRUE(!needsRecv);  // early exit if the cache satisfied it. No need to rpc receive.

    // x0 = expectNum - curCacheNum
    auto expectElementNum = receiveNum - cacheLength;
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("%s, cache did not satisfy receive requirement. Invoke worker receive.",
                                              LogPrefix());
    RETURN_IF_NOT_OK(PrefetchEntry(expectElementNum, timeoutMs));
    INJECT_POINT("consumer_after_get_datapage");
    // Log status
    LogConsumerCursors();

    // Refresh the cache length
    cacheLength = elementCacheQueue_->Length();
    receiveNum = (expectNum) ? expectNum.value() : (cacheLength > 0) ? cacheLength : 1u;
    auto reserveSize = std::min<uint32_t>(receiveNum, cacheLength);
    outElements.reserve(reserveSize);
    // get all local cache, then queue is empty
    if (cacheLength > 0) {
        VLOG(SC_INTERNAL_LOG_LEVEL) << FormatString("%s, %zu element remain in cache", LogPrefix(), cacheLength);
        RETURN_IF_NOT_OK(GetElementFromCache(reserveSize, outElements));
    }
    if (outElements.empty()) {
        const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
        LOG_EVERY_N(INFO, logPerCount) << FormatString("[%s] Consumer %s expect recv %d elements, got 0 elements",
                                                       LogPrefix(), consumerId_, receiveNum);
        return CheckNormalState();
    }

    RETURN_IF_NOT_OK(PostRecvAckHandler(outElements));

    const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
    LOG_EVERY_N(INFO, logPerCount) << FormatString(
        "[%s] Consumer %s expect recv %d elements, got %d elements [id:%zu-%zu]", LogPrefix(), consumerId_, receiveNum,
        outElements.size(), outElements.front().id, outElements.back().id);
    return Status::OK();
}

Status ConsumerImpl::Receive(Optional<uint32_t> expectNum, uint32_t timeoutMs, std::vector<Element> &outElements)
{
    cursor_->IncrementRequestCount();
    PreRecvAckHandler();
    Status rc = ReceiveImpl(expectNum, timeoutMs, outElements);
    if (recvTracer_.NeedWriteLog(!outElements.empty())) {
        LOG(INFO) << FormatString("[%s] Consumer first receive element count %zu with status %s", LogPrefix(),
                                  outElements.size(), rc.ToString());
    }
    // A special return code K_OUT_OF_RANGE may indicate the worker has already restarted.
    // Go check the state.
    consumedElements_ += outElements.size();
    if (rc.GetCode() == K_OUT_OF_RANGE) {
        RETURN_IF_NOT_OK(CheckNormalState());
    }
    return rc;
}

Status ConsumerImpl::GetElementFromCache(uint32_t expectNum, std::vector<Element> &outElements)
{
    int elementNums = static_cast<int>(std::min(static_cast<size_t>(expectNum), elementCacheQueue_->Length()));
    outElements.reserve(elementNums);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(elementCacheQueue_->BatchFetchAndPop(outElements, elementNums),
                                         StatusCode::K_RUNTIME_ERROR, "circular queue is empty");
    DCHECK(!outElements.empty()) << "The element size should be greater than 0";
    return Status::OK();
}

Status ConsumerImpl::Ack(uint64_t elementId)
{
    PerfPoint point(PerfKey::CLIENT_ACK_ALL);
    RETURN_IF_NOT_OK(CheckNormalState());
    // Make sure the data in cache can not be ack.
    // There is a racing condition that the check below is not valid
    // when the prefetching thread is running in the background.
    if (cachePrefetchLWM_ == 0) {
        uint64_t cacheLength = elementCacheQueue_->Length();
        uint64_t ackCheckNum = elementId + cacheLength;
        auto lastRecvCursor = lastRecvCursor_.load();
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
            ackCheckNum <= lastRecvCursor, StatusCode::K_INVALID,
            FormatString("The ack cursor %zu should less equal than lastRecvCursor %zu. cache size: %zu", elementId,
                         lastRecvCursor, cacheLength));
    }
    VLOG(SC_NORMAL_LOG_LEVEL) << FormatString("Consumer %s ack %zu", consumerId_, elementId);
    // Go through the stream index, and release the pages that we no longer need
    std::unique_lock<std::shared_timed_mutex> xlock(idxMutex_);
    auto it = idx_.begin();
    while (it != idx_.end()) {
        std::shared_ptr<StreamDataPage> page = it->second;
        // Get the last cursor on the page, which is also the key of the next page in idx_
        // if it exists.
        uint64_t lastCursorOnPage = page->GetLastCursor();
        // Worker Eyecatcher V1 works without consumer side client ref count.
        if (workerVersion_ >= Cursor::K_WORKER_EYECATCHER_V1 && lastCursorOnPage < elementId) {
            it = idx_.erase(it);
            bytesSinceLastAck_ = 0;
        } else if (workerVersion_ < Cursor::K_WORKER_EYECATCHER_V1 && lastCursorOnPage <= elementId) {
            // One more check. If this page is only partially full, we may still need it
            // in the future. A simpler way to check it if we have reached the end of the
            // index chain.
            if (it->first == pageBoundaryCursor_) {
                break;
            }
            RETURN_IF_NOT_OK(ReleasePage(page));
            // Notify the worker via shared memory that this page is no longer needed.
            UpdateWALastAckCursor(lastCursorOnPage);
            it = idx_.erase(it);
            bytesSinceLastAck_ = 0;
        } else {
            break;
        }
    }
    // Finally unconditionally set the value in the shared memory work area.
    // Future newly added consumer(s) will start from this elementId
    if (elementId > ackedElementId_.load()) {
        UpdateWALastAckCursor(elementId);
        ackedElementId_.store(elementId);
    }
    return Status::OK();
}

Status ConsumerImpl::Close()
{
    PerfPoint point(PerfKey::CLIENT_CLOSE_CONSUMER_ALL);
    // If consumer is already closed, return OK
    RETURN_OK_IF_TRUE(state_ == State::CLOSE);
    // If the current state is RESET, it is allowed to close
    RETURN_IF_NOT_OK(CheckState());
    {
        std::unique_lock<std::shared_timed_mutex> lock(idxMutex_);
        for (auto &ele : idx_) {
            ReleasePage(ele.second);
        }
        if (cursor_) {
            UpdateWALastAckCursor(lastRecvCursor_);
        }
        idx_.clear();
    }
    INJECT_POINT("ConsumerImpl.CloseConsumerRPC.Fail");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(workerApi_->CloseConsumer(streamName_, config_.subscriptionName, consumerId_),
                                     FormatString("[%s] CloseConsumer request error", LogPrefix()));
    RETURN_IF_NOT_OK(ChangeState(State::CLOSE));
    LOG(INFO) << FormatString("[%s] Close consumer success!", LogPrefix());
    return Status::OK();
}

Status ConsumerImpl::SetStateToReset()
{
    RETURN_IF_NOT_OK(ChangeState(State::RESET));
    // Wake up consumer and wait for Receive in progress to complete
    {
        std::shared_lock<std::shared_timed_mutex> lock(idxMutex_);
        for (auto &ele : idx_) {
            ele.second->WakeUpConsumers();
        }
    }
    receiveWp_.Wait();
    // Wait for prefetch in progress to complete
    if (cachePrefetchLWM_ > 0 && prefetchStatus_.valid()) {
        prefetchStatus_.wait();
        prefetchStatus_.get();  // call get() so that prefetchStatus_ is no longer valid
    }
    return Status::OK();
}

Status ConsumerImpl::Reset()
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        state_ == State::RESET, K_RUNTIME_ERROR,
        FormatString("[%s] The consumer should be in reset state already to cleanup data and metadata", LogPrefix()));
    elementCacheQueue_->Clear();
    for (auto &e : idx_) {
        RETURN_IF_NOT_OK(ReleasePage(e.second));
    }
    idx_.clear();
    lastRecvCursor_ = 0;
    UpdateWALastAckCursor(0);
    pageBoundaryCursor_ = 0;
    bytesSinceLastAck_ = 0;
    avgEleSize_ = 0;
    lastAvgCount_ = 0;
    ackedElementId_ = 0;
    consumedElements_ = 0;
    return Status::OK();
}

Status ConsumerImpl::Resume()
{
    return ChangeState(State::NORMAL);
}

std::string ConsumerImpl::LogPrefix() const
{
    return FormatString("S:%s, Sub:%s, C:%s", streamName_, config_.subscriptionName, consumerId_);
}

void ConsumerImpl::GetStatisticsMessage(uint64_t &totalElements, uint64_t &notProcessedElements)
{
    if (lastPage_ == nullptr || cursor_ == nullptr) {
        totalElements = 0;
        notProcessedElements = 0;
        return;
    }
    const uint64_t &proNum = cursor_->GetWALastAckCursor();
    totalElements = lastPage_->GetLastCursor();
    notProcessedElements = totalElements - proNum;
}

void ConsumerImpl::LogConsumerCursors()
{
    if (lastPage_) {
        auto lastAppendCursor = lastPage_->GetLastCursor();
        const int logPerCount = VLOG_IS_ON(SC_NORMAL_LOG_LEVEL) ? 1 : 1000;
        LOG_EVERY_N(INFO, logPerCount) << FormatString(
            "[%s] %d received elements, %d consumed elements, %d elements acked", LogPrefix(), lastAppendCursor,
            consumedElements_, ackedElementId_);
    }
}

void ConsumerImpl::PreRecvAckHandler()
{
    if (autoAck_) {
        // Trigger ack for the previous received data
        uint64_t cursorToAck = consumedElements_;
        Status rc = Ack(cursorToAck);
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Ack failed: %s", rc.GetMsg());
        }
    }
}

Status ConsumerImpl::PostRecvAckHandler(const std::vector<Element> &fetchedElements)
{
    // Track the amount of bytes from the receive that was just done, and move the next ack position further along.
    uint64_t elementByteSum = std::accumulate(fetchedElements.begin(), fetchedElements.end(), static_cast<uint64_t>(0),
                                              [](uint64_t sum, const Element &e) { return e.size + sum; });
    bytesSinceLastAck_ += elementByteSum;

    // Compute a running average to learn what the average element size is. Implicit conversion rules convert all these
    // types to match the float type of avgEleSize
    avgEleSize_ = ((avgEleSize_ * lastAvgCount_) + elementByteSum) / (lastAvgCount_ + fetchedElements.size());
    lastAvgCount_ += fetchedElements.size();

    VLOG(SC_NORMAL_LOG_LEVEL) << "Post Recv Ack stats:"
                              << "\nBytes since last ack    : " << bytesSinceLastAck_
                              << "\nNext cursor ack         : " << pageBoundaryCursor_
                              << "\nNew elements recv count : " << fetchedElements.size()
                              << "\nAverage element size    : " << avgEleSize_;

    return Status::OK();
}

Status ConsumerImpl::ReleasePage(std::shared_ptr<StreamDataPage> &page) const
{
    // Worker Eyecatcher V1 works without consumer side client ref count.
    RETURN_OK_IF_TRUE(workerVersion_ >= Cursor::K_WORKER_EYECATCHER_V1);
    Status rc = page->ReleasePage(LogPrefix());
    RETURN_OK_IF_TRUE(rc.IsOk());
    // Map all other errors to OUT_OF_RANGE. Let the high level code to
    // detect if the worker has restarted.
    RETURN_STATUS_LOG_ERROR(K_OUT_OF_RANGE, rc.ToString());
}

Status ConsumerImpl::ExtractVersion(DataElement &element, ElementHeader::Version &version)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        element.size > sizeof(ElementHeader::Version), K_OUT_OF_RANGE,
        FormatString("[%s] Element (version + header + data) size %llu is not greater than header version size %lu",
                     LogPrefix(), element.size, sizeof(ElementHeader::Version)));
    version = *(element.ptr);
    element.ptr++;
    element.size--;
    return Status::OK();
}

Status ConsumerImpl::ProcessHeaders(std::vector<DataElement> &recvElements)
{
    for (auto &element : recvElements) {
        if (!element.HasHeader()) {
            continue;
        }
        ElementHeader::Version version;
        RETURN_IF_NOT_OK(ExtractVersion(element, version));
        ElementHeader header;
        if (version == DATA_VERIFICATION_HEADER) {
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(DataVerificationHeader::ExtractHeader(element, header),
                                             FormatString("[%s]", LogPrefix()));
            RETURN_IF_NOT_OK(VerifyElement(element, header));
        } else {
            RETURN_STATUS_LOG_ERROR(
                K_INVALID, FormatString("[%s] Does not support element's header version %u", LogPrefix(), version));
        }
    }
    return Status::OK();
}

Status ConsumerImpl::VerifyElement(const DataElement &recvElement, const ElementHeader &recvHeader)
{
    struct DataVerificationHeader header(recvHeader);
    const std::string key =
        FormatString("%llu-%lu-%lu", header.GetSenderProducerNo(), header.GetAddress(), header.GetPort());
    auto iter = producerLastSeqNoReceive_.find(key);
    if (iter == producerLastSeqNoReceive_.end()) {
        producerLastSeqNoReceive_.emplace(key, header.GetSeqNo());
        LOG(INFO) << FormatString("[%s] Data Verification: First time receive from producer = %s, seqNo = %llu",
                                  LogPrefix(), key, header.GetSeqNo());
        return Status::OK();
    }
    const DataVerificationHeader::SeqNo previousSeqNo = iter->second;
    iter->second = header.GetSeqNo();
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(
        header.GetSeqNo() == previousSeqNo + 1, K_DATA_INCONSISTENCY,
        FormatString("[%s] Data Verification Failed: length = %zu, producer = %s, seqNo = %llu, "
                     "expect seqNo = %llu",
                     LogPrefix(), recvElement.size, key, header.GetSeqNo(), previousSeqNo + 1));
    // Success
    const int logPerCount = VLOG_IS_ON(SC_INTERNAL_LOG_LEVEL) ? 1 : 1000;
    LOG_EVERY_N(INFO, logPerCount) << FormatString(
        "[%s] Data Verification Success: length = %zu, producer = %s, "
        "seqNo = %llu",
        LogPrefix(), recvElement.size, key, header.GetSeqNo());
    INJECT_POINT("VerifyProducerNo", [&header, &recvElement]() {
        std::string expected(reinterpret_cast<const char *>(recvElement.ptr), recvElement.size);
        std::string producerNo = "producer" + std::to_string(header.GetSenderProducerNo());
        if (expected == producerNo) {
            return Status::OK();
        }
        RETURN_STATUS_LOG_ERROR(
            K_DATA_INCONSISTENCY,
            FormatString("DataVerification Failed: incorrect producerNo, get %s, expect %s", producerNo, expected));
    });
    return Status::OK();
}
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem

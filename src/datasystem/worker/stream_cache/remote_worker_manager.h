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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_REMOTE_WORKER_MANAGER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_REMOTE_WORKER_MANAGER_H

#include <tbb/concurrent_hash_map.h>

#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/metrics/metrics_vector/metrics_sc_remote_vector.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/wait_post.h"
#include "datasystem/protos/stream_posix.service.rpc.pb.h"
#include "datasystem/protos/worker_stream.stub.rpc.pb.h"
#include "datasystem/worker/stream_cache/buffer_pool.h"
#include "datasystem/worker/stream_cache/page_queue/exclusive_page_queue.h"
#include "datasystem/worker/stream_cache/page_queue/shared_page_queue_group.h"
#include "datasystem/worker/stream_cache/stream_data_pool.h"
#include "datasystem/worker/stream_cache/subscription.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
struct PushReq {
    std::variant<PushReqPb, SharedPagePushReqPb> req_;
    std::string keyName_;
    int64_t tag_;
    Status rc_;
    PushReq(std::variant<PushReqPb, SharedPagePushReqPb> &&req, const std::string &keyName)
        : req_(std::move(req)), keyName_(keyName){};
};
/**
 * A small class to keep track of which sequence of elements have been acknowledged
 * by the remote worker.
 */
class RemoteAckInfo {
public:
    explicit RemoteAckInfo(uint64_t cursor);
    ~RemoteAckInfo() = default;
    using AckRange = std::pair<uint64_t, uint64_t>;
    struct Compare {
        bool operator()(const AckRange &a, const AckRange &b)
        {
            return a.first > b.first;
        }
    };

    /**
     * @brief Get the last ack cursor
     * @param streamName
     * @param cursor
     * @return
     */
    uint64_t GetStreamLastAckCursor() const;

    /**
     * @brief Move up last ack cursor
     * @param nextAckCursor
     * @return
     */
    void SyncStreamLastAckCursor(Optional<AckRange> ackRange, const std::string &logPrefix);

    void Reset();

private:
    uint64_t lastAckCursor_;
    std::priority_queue<AckRange, std::vector<AckRange>, Compare> ackQue_;
};

// View of an element on a shared memory page.
// Note that Elements are packed in reverse order
struct SendElementView : public BaseBufferData {
    std::string streamName_;
    std::shared_ptr<PageQueueBase> dataObj_{ nullptr };
    bool remote_{ false };
    std::string remoteWorker_;
    std::string StreamName() const override;
    std::string ProducerName() const override;
    std::string ProducerInstanceId() const override;
    uint64_t StreamHash() const override;
    virtual bool IsSharedPage() = 0;
    virtual bool PackDataElement(const DataElement &element, bool skipChecks,
                                 RemoteWorkerManager *remoteworkerManager = nullptr) = 0;
    virtual Status ReleasePage() = 0;
    virtual Status IncRefCount() = 0;
    virtual RemoteAckInfo::AckRange GetAckRange() = 0;
    virtual uint64_t GetElementNum() = 0;
    virtual void DiscardBufferFromList(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter) = 0;

    /**
     * @brief Create StreamElementView for the remote send.
     * @param[in] page The stream data page of the element.
     * @param[in] remoteWorker The remote worker of the element.
     * @param[in] dataElement The element.
     * @param[in] obj The page queue base class object.
     * @param[in] remoteWorkerManager The remote worker manager ptr.
     * @param[out] out The send element view.
     * @return Status of the call.
     */
    static Status CreateSendElementView(const std::shared_ptr<StreamDataPage> &page, const std::string &remoteWorker,
                                        DataElement &dataElement, std::shared_ptr<PageQueueBase> obj,
                                        RemoteWorkerManager *remoteWorkerManager,
                                        std::shared_ptr<SendElementView> &out);
};

struct StreamElementView : public SendElementView {
    std::shared_ptr<StreamDataPage> page_;
    std::shared_ptr<StreamLobPage> bigElementPage_;
    uint64_t begCursor_;
    std::vector<size_t> sz_;
    std::atomic<uint8_t *> buf_;
    bool bigElement_{ false };
    size_t bigElementMetaSize_{ 0 };
    std::unique_ptr<ShmUnit> shmUnit_;
    uint8_t *secondaryAddr_{ nullptr };
    std::unique_ptr<uint8_t[]> localBuf_;  // when oom hit or to perform encryption
    size_t localBufSize_;
    std::atomic<bool> shmEnabled_{ true };
    std::atomic<bool> ref_{ false };
    std::shared_timed_mutex mux_;
    std::vector<bool> headerBits_;

    Status ReleasePage() override;
    virtual Status IncRefCount();
    Status MoveBufToAlternateMemory();
    uint8_t *GetBufferPointer();
    RemoteAckInfo::AckRange GetAckRange() override;
    bool IsSharedPage() override;
    Status MoveBufToShmUnit();
    bool PackDataElement(const DataElement &element, bool skipChecks,
                         RemoteWorkerManager *remoteworkerManager = nullptr) override;
    uint64_t GetElementNum() override;
    void DiscardBufferFromList(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter) override;
};

struct SharedPageElementView : public SendElementView {
    std::string sharedPageName_;
    // Element views can be of different stream names.
    std::list<std::shared_ptr<StreamElementView>> elementViews_;
    std::list<uint64_t> seqNums_;
    bool IsSharedPage() override;
    std::string KeyName() const override;
    bool PackDataElement(const DataElement &element, bool skipChecks,
                         RemoteWorkerManager *remoteworkerManager = nullptr) override;
    uint64_t RecordSeqNo(std::function<uint64_t(const std::string &)> fetchAddSeqNo) override;
    Status ReleasePage() override;
    Status IncRefCount() override;
    Status MoveBufToShmUnit();
    RemoteAckInfo::AckRange GetAckRange() override;
    uint64_t GetElementNum() override;
    void DiscardBufferFromList(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter) override;
};

using RemoteConsumers = std::tuple<bool, RemoteAckInfo, uint64_t, std::set<std::string>>;
using RemoteStreamInfoTbbMap = tbb::concurrent_hash_map<std::string, RemoteConsumers>;
using StreamManagerMap = tbb::concurrent_hash_map<std::string, std::shared_ptr<StreamManager>>;
using StreamRaii = std::unique_ptr<StreamManagerMap::const_accessor>;
constexpr static int K_BLOCKED = 0;
constexpr static int K_ACK = 1;
constexpr static int K_WINDOW_COUNT = 2;
constexpr static int K_CONSUMER_ID = 3;

class RemoteConsumerMap {
public:
    RemoteConsumerMap() = default;
    ~RemoteConsumerMap() = default;

    /**
     * @brief Add remote consumer to the map.
     * @param[in] streamName The stream name.
     * @param[in] consumerId consumer id
     * @param[in] windowCount tcp/ip window count
     * @param[in] lastAckCursor starting cursor
     * @return Status of the call.
     */
    Status AddConsumer(const std::string &streamName, const std::string &consumerId, uint64_t windowCount,
                       uint64_t lastAckCursor);

    /**
     * @brief Delete remote consumer from the map.
     * @param[in] streamName The stream name.
     * @param[in] consumerId The consumer id.
     * @return Status of the call.
     */
    Status DeleteConsumer(const std::string &streamName, const std::string &consumerId, Optional<bool> &lastConsumer);

    /**
     * @brief Delete a stream entry
     * @param streamName
     * @return
     */
    Status DeleteStream(const std::string &streamName, Optional<bool> &mapEmpty);

    /**
     * @brief Enable/Disable blocking of a stream so no remote push is done.
     * @param[in] streamName Target stream.
     * @param[in] enable T/F
     * @return Status of the call.
     */
    Status ToggleStreamBlocking(const std::string &streamName, bool enable);

    /**
     * @brief If the stream is blocked
     * @param streamName
     * @return T/F
     */
    bool IsStreamSendBlocked(const std::string &streamName);

    /**
     * @brief Check if there is any remote consumer for a given stream
     * @return
     */
    bool HasRemoteConsumers(const std::string &streamName);

    /**
     * @brief Get the last ack cursor
     * @param streamName
     * @param cursor
     * @return
     */
    Status GetStreamLastAckCursor(const std::string &streamName, uint64_t &cursor);

    /**
     * @brief Move up last ack cursor to K_NEXT value
     * @param streamName
     * @param nextAckCursor
     * @return
     */
    void SyncStreamLastAckCursor(RemoteStreamInfoTbbMap::accessor &accessor, Optional<RemoteAckInfo::AckRange> ackRange,
                                 const std::string &logPrefix);

    /**
     * @brief Identify whether have remote consumer.
     * @return True if have no remote consumer.
     */
    bool Empty() const;

    /**
     * @brief Get max window count
     */
    uint64_t GetMaxWindowCount(const std::string &streamName) const;

    Status GetAccessor(const std::string &streamName, RemoteStreamInfoTbbMap::accessor &accessor,
                       const std::string &logPrefix);

private:
    // key: streamName, value: dictionary of consumers on the remote node for corresponding stream.
    RemoteStreamInfoTbbMap streamConsumers_;
    mutable std::shared_timed_mutex consumerMutex_;  // protect streamConsumers_.
};

class RemoteWorker {
public:
    RemoteWorker(HostPort localAddress, HostPort remoteAddress, std::shared_ptr<AkSkManager> akSkManager,
                 ClientWorkerSCServiceImpl *scSvc, std::string &workerInstanceId,
                 std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager, RemoteWorkerManager *manager);
    ~RemoteWorker();

    /**
     * @brief Init thread pool used by remote worker.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief stream with streamName has remote consumer
     * @param streamName
     * @return T/F
     */
    bool HasRemoteConsumers(const std::string &streamName);

    /**
     * @brief Add one remote consumer for a stream on this remote worker node.
     * @param[in] streamName Target stream.
     * @param[in] subConfig Remote consumer's subscription config.
     * @param[in] consumerId Remote consumer's id.
     * @param[in] lastAckCursor Remote consumer's last ack cursor.
     * @return Status of the call.
     */
    Status AddRemoteConsumer(const std::string &streamName, const SubscriptionConfig &subConfig,
                             const std::string &consumerId, uint64_t windowCount, uint64_t lastAckCursor);

    /**
     * @brief Del one remote consumer for a stream on this remote worker node. Search dict by streamName at first, then
     * search the target remote consumer in dict by consumerId.
     * @param[in] streamName Target stream.
     * @param[in] consumerId Remote consumer's id.
     * @return Status of the call.
     */
    Status DelRemoteConsumer(const std::string &streamName, const std::string &consumerId,
                             Optional<bool> &lastConsumer);

    /**
     * @brief Delete a remote stream
     * @param streamName
     * @return
     */
    Status DeleteStream(const std::string &streamName, Optional<bool> &mapEmpty);

    /**
     * @brief Call back function from BufferPool class
     * @return Status object
     */
    Status BatchAsyncFlushEntry(PendingFlushList &pendingFlushList);

    /**
     * @brief Check whether exists remote consumer.
     * @return True if exists.
     */
    bool ExistsRemoteConsumer();

    /**
     * @brief Get log prefix
     * @return The log prefix
     */
    std::string LogPrefix() const;

    /**
     * @brief Get the last ack cursor from the remote worker
     * @param streamName
     * @param cursor
     * @return
     */
    Status GetStreamLastAckCursor(const std::string &streamName, uint64_t &cursor);

    /**
     * @brief Used by reset to stop sending to remote node.
     */
    Status ProcessEndOfStream(const std::shared_ptr<StreamManager> &streamMgr, std::list<BaseData> dataLst,
                              const std::string &streamName, const std::string &producerId);

    /**
     * @brief Obtain the success rate of sending data to the remote worker manager.
     */
    void RegisterRecordRemoteSendRateCallBack(std::function<void(int, int)> callBackFunction)
    {
        recordRemoteSendRate_ = callBackFunction;
    }

    bool IsStreamSendBlocked(const std::string &streamName);

    /**
     * @brief Get max window count
     */
    uint64_t GetMaxWindowCount(const std::string &streamName) const;

    /**
     * @brief Get the or create SharedPageQueue.
     * @param[in] namespaceUri The stream name.
     * @param[out] pageQueue The instance of SharedPageQueue.
     */
    void GetOrCreateSharedPageQueue(const std::string &namespaceUri, std::shared_ptr<SharedPageQueue> &pageQueue);

private:
    friend class RemoteWorkerManager;

    const HostPort localWorkerAddr_;
    const HostPort remoteWorkerAddr_;

    RemoteConsumerMap remoteConsumers_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    ClientWorkerSCServiceImpl *scSvc_;
    SharedPageQueueGroup sharedPageGroup_;

    /**
     * @brief Record remote send success rate if callback function is exist.
     * @param[in] success Success or not.
     */
    void RecordRemoteSendSuccess(bool success)
    {
        const int totalNum = 1;
        const int successNum = success ? 1 : 0;
        if (recordRemoteSendRate_ != nullptr) {
            recordRemoteSendRate_(successNum, totalNum);
        }
    }

    int BatchFlushAsyncWrite(const std::shared_ptr<WorkerWorkerSCService_Stub> &stub, std::vector<PushReq> &requests,
                             std::vector<std::vector<MemView>> &payloads);
    void BatchFlushAsyncRead(const std::shared_ptr<WorkerWorkerSCService_Stub> &stub,
                             PendingFlushList &pendingFlushList, std::vector<PushReq> &requests,
                             std::unordered_map<std::string, StreamRaii> &raii);
    void HandleBlockedElements(std::list<std::shared_ptr<SharedPageElementView>> &moveList,
                               std::unordered_set<std::shared_ptr<SharedPageQueue>> &needAckList);
    Status ParseProducerPendingFlushList(const std::string &streamName, const std::string &producerId,
                                         std::list<BaseData> &dataLst, std::vector<PushReq> &requests,
                                         std::vector<std::vector<MemView>> &payloads,
                                         std::unordered_map<std::string, StreamRaii> &raii,
                                         std::list<std::shared_ptr<SharedPageElementView>> &moveList,
                                         std::unordered_set<std::shared_ptr<SharedPageQueue>> &needAckList);
    Status ParsePendingFlushList(const PendingFlushList &pendingFlushList, std::vector<PushReq> &requests,
                                 std::vector<std::vector<MemView>> &payloads,
                                 std::unordered_map<std::string, StreamRaii> &raii,
                                 std::list<std::shared_ptr<SharedPageElementView>> &moveList,
                                 std::unordered_set<std::shared_ptr<SharedPageQueue>> &needAckList);
    Status FillExclusivePushReqHelper(const std::string &streamName, const std::string &producerId, uint64_t firstSeqNo,
                                      std::list<BaseData> &dataLst, std::list<BaseData>::iterator &it,
                                      uint64_t &firstCursor, uint64_t &lastCursor, PushReqPb &pushReqPb,
                                      std::vector<MemView> &elements,
                                      std::unordered_map<std::string, StreamRaii> &raii);
    Status FillSharedPushReqHelper(const std::string &producerId, std::list<BaseData> &dataLst,
                                   std::list<BaseData>::iterator &it, uint64_t &firstCursor, uint64_t &lastCursor,
                                   SharedPagePushReqPb &pushReqPb, std::vector<MemView> &elements,
                                   std::unordered_map<std::string, StreamRaii> &raii,
                                   std::list<std::shared_ptr<SharedPageElementView>> &moveList);
    Status LockStreamManagerHelper(const std::string &streamName, std::unordered_map<std::string, StreamRaii> &raii);
    void PostRecvCleanup(const std::string &streamName, const Status &status, PendingFlushList &pendingFlushList,
                         const PushReqPb &pushReq, const PushRspPb &pushRspPb,
                         std::unordered_map<std::string, StreamRaii> &raii);
    void PostRecvCleanup(const std::string &streamName, const PushReqPb &rq, const PushRspPb &pushRspPb,
                         RemoteStreamInfoTbbMap::accessor &accessor, std::list<BaseData> &dataLst,
                         std::unordered_map<std::string, StreamRaii> &raii);
    void PostRecvCleanup(const std::string &keyName, const Status &status, PendingFlushList &pendingFlushList,
                         const SharedPagePushReqPb &pushReq, const PushRspPb &pushRspPb,
                         std::unordered_map<std::string, StreamRaii> &raii);
    void PostRecvCleanup(const std::string &keyName, const SharedPagePushReqPb &rq, const PushRspPb &pushRspPb,
                         RemoteStreamInfoTbbMap::accessor &accessor, std::list<BaseData> &dataLst,
                         std::unordered_map<std::string, StreamRaii> &raii);

    /**
     * @brief Helper function to discard buffers when consumer does not exist.
     * @param[in] dataLst list of PVs.
     */
    static void DiscardBuffers(std::list<BaseData> &dataLst);

    /**
     * @brief Helper function to discard one buffer.
     * @param[in] dataLst list of PVs.
     * @param[in/out] iter The iterator of dataLst.
     */
    static void DiscardBufferHelper(std::list<BaseData> &dataLst, std::list<BaseData>::iterator &iter);

    void SyncStreamLastAckCursor(RemoteStreamInfoTbbMap::accessor &accessor,
                                 Optional<RemoteAckInfo::AckRange> ackRange);
    Status GetAccessor(const std::string &streamName, RemoteStreamInfoTbbMap::accessor &accessor);

    std::function<void(int, int)> recordRemoteSendRate_;
    std::string workerInstanceId_;  // unique id generated for each worker instance
    RemoteWorkerManager *remoteWorkerManager_;
};

// The RemoteWorkerManager structure introduction.
// remoteWorkerDict_:
// Key: remote worker address, Value: RemoteWorker
// ==================================
// address0    ->    RemoteWorker0
// address1    ->    RemoteWorker1
// address2    ->    RemoteWorker2
//  ...                  ...
//  ...                  ...
// addressI    ->    RemoteWorkerI
// ==================================
// The RemoteWorker structure
// streamConsumers_:
// Key: stream name, Value: stream's consumer dict map
// =================================
// stream0     ->   ConsumerDict0
// stream1     ->   ConsumerDict1
//   ...                 ...
// streamJ     ->   ConsumerDictJ
// =================================
// The ConsumerDict structure
// Key: consumer name, Value: pair(SubscriptionConfig, Consumer object)
// =====================================
// consumerId0   ->   (sub0, Consumer0)
// consumerId1   ->   (sub1, Consumer1)
//     ...                  ...
// consumerIdK   ->   (subK, ConsumerK)
// =====================================

class RemoteWorkerManager {
public:
    explicit RemoteWorkerManager(ClientWorkerSCServiceImpl *scSvc, std::shared_ptr<AkSkManager> akSkManager,
                                 std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager);
    ~RemoteWorkerManager();

    /**
     * @brief Init thread pool used by remote worker manager.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Check if there are tasks to be processed
     * @return T/F
     */
    bool HaveTasksToProcess()
    {
        return dataMap_->HaveTasksToProcess();
    }

    /**
     * @brief Add stream data in list of pending send data.
     * @param[in] eleView The data to send to remote worker.
     * @return Status of the call.
     */
    Status SendElementsView(const std::shared_ptr<SendElementView> &eleView);

    /**
     * @brief Get the remote ack from remote workers
     * @param streamName
     * @return remote ack
     */
    uint64_t GetLastAckCursor(const std::string &streamName);

    /**
     * @brief Purge buffer from the given stream manager
     * @param streamMgr
     */
    void PurgeBuffer(const std::shared_ptr<StreamManager> &streamMgr);

    /**
     * @brief Remove the info of useless stream from BufferPool
     * @param keyName The stream name or page name.
     * @param sharedPageName The shared page name. Empty if the stream use exclusive page or the keyName is page.
     */
    void RemoveStream(const std::string &keyName, const std::string &sharedPageName);

    /**
     * @brief stream with streamName has remote consumer
     * @param streamName
     * @return T/F
     */
    bool HasRemoteConsumers(const std::string &streamName);

    /**
     * @brief Delete a remote stream from all workers.
     * @param streamName
     * @return
     */
    Status DeleteStream(const std::string &streamName);

    /**
     * @brief Stop to push new buffer into RW.
     * @param streamName
     * @return Status of the call.
     */
    Status DoneScanning(const std::string &streamName);

    std::string GetSCRemoteSendSuccessRate();

    /**
     * @brief Del one remote consumer for a stream on this remote worker node. Search dict by streamName at first, then
     * search the target remote consumer in dict by consumerId.
     * @param[in] streamName Target stream.
     * @param[in] consumerId Remote consumer's id.
     * @return Status of the call.
     */
    Status DelRemoteConsumer(const std::string &workerAddr, const std::string &streamName,
                             const std::string &consumerId);

    /**
     * @brief Clear all remote consumer node for target stream on current worker node.
     * Invoked when last producer disappears within current node.
     * @param[in] forceClose Force close from master.
     * @return K_OK on success; the error code otherwise.
     */
    Status ClearAllRemoteConsumer(const std::string &streamName, bool forceClose);

    /**
     * @brief Enable/Disable blocking of a stream so no remote push is done or resume.
     * @param[in] workerAddr remote worker
     * @param[in] streamName Target stream.
     * @param[in] enable T/F
     * @return Status of the call.
     */
    Status ToggleStreamBlocking(const std::string &workerAddr, const std::string &streamName, bool enable);

    /**
     * @brief Add one remote consumer for a stream on this remote worker node.
     * @param[in] streamManager The stream manager ptr.
     * @param[in] localWorkerAddress Local worker's address.
     * @param[in] remoteWorkerAddress Target remote worker's address.
     * @param[in] streamName Target stream.
     * @param[in] subConfig Remote consumer's subscription config.
     * @param[in] consumerId Remote consumer's id.
     * @param[in] lastAckCursor Remote consumer's last ack cursor.
     * @return Status of the call.
     */
    Status AddRemoteConsumer(const std::shared_ptr<StreamManager> &streamMgr, const HostPort &localWorkerAddress,
                             const HostPort &remoteWorkerAddress, const std::string &streamName,
                             const SubscriptionConfig &subConfig, const std::string &consumerId,
                             uint64_t lastAckCursor);

    /**
     * @brief Reset scan position
     * @param[in] streamName
     * @return
     */
    Status ResetStreamScanList(const std::string &streamName);

    /**
     * @brief Convert from stream number to the corresponding stream name.
     * @param[in] streamNo The stream number.
     * @param[out] streamName The stream name.
     * @return Status of the call.
     */
    Status StreamNoToName(uint64_t streamNo, std::string &streamName);

private:
    /**
     * @brief Call back function from BufferPool class
     * @return Status object
     */
    Status BatchAsyncFlushEntry(int myId, const PendingFlushList &pendingFlushList);

    /**
     * @brief Used by reset to stop sending to remote node.
     */
    Status ProcessEndOfStream(const std::shared_ptr<StreamManager> &streamMgr, std::list<BaseData> dataLst,
                              const std::string &streamName, const std::string &producerId);

    /**
     * @brief Get remote worker object.
     * @param[in] address Remote worker's address.
     * @param[out] remoteWorker Pointer to the remote worker object.
     * @return Status of the call.
     */
    Status GetRemoteWorker(const std::string &address, std::shared_ptr<RemoteWorker> &remoteWorker);

    /**
     * @brief Get a list of remote worker address from a given streamName
     */
    std::vector<std::string> GetRemoteWorkers(const std::string &streamName);

    MetricsScRemoteVector remoteSendRateVec_;
    std::unordered_map<std::string, std::shared_ptr<RemoteWorker>> remoteWorkerDict_;
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
    ClientWorkerSCServiceImpl *scSvc_;
    std::unique_ptr<BufferPool> dataMap_;
    std::unique_ptr<StreamDataPool> dataPool_;  // holds all the stream pages
    std::string workerInstanceId_;              // unique id generated for each worker instance
    std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_REMOTE_WORKER_MANAGER_H

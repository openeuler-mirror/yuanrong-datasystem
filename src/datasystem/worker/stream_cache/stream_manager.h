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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_STREAM_MANAGER_H
#define DATASYSTEM_WORKER_STREAM_CACHE_STREAM_MANAGER_H
#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/stream_cache/consumer_meta.h"
#include "datasystem/common/stream_cache/stream_data_page.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/protos/stream_posix.pb.h"
#include "datasystem/protos/worker_stream.pb.h"
#include "datasystem/stream/stream_config.h"
#include "datasystem/utils/optional.h"
#include "datasystem/worker/stream_cache/consumer.h"
#include "datasystem/worker/stream_cache/page_queue/page_queue_handler.h"
#include "datasystem/worker/stream_cache/remote_worker_manager.h"
#include "datasystem/worker/stream_cache/producer.h"
#include "datasystem/worker/stream_cache/stream_data_pool.h"
#include "datasystem/worker/stream_cache/subscription.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "datasystem/worker/stream_cache/worker_worker_sc_service_impl.h"

DS_DECLARE_int32(log_monitor_interval_ms);

namespace datasystem {
namespace worker {
namespace stream_cache {
class ClientWorkerSCServiceImpl;
struct RecvElementView;
class WorkerWorkerSCServiceImpl;
enum class StreamState { ACTIVE = 0, RESET_IN_PROGRESS = 1, RESET_COMPLETE = 2, DELETE_IN_PROGRESS = 3 };
class SubWorkerDesc {
public:
    explicit SubWorkerDesc(HostPort subWorkerAddress) : hostPort_(std::move(subWorkerAddress))
    {
    }

    ~SubWorkerDesc() = default;
    SubWorkerDesc(const SubWorkerDesc &other) = delete;
    SubWorkerDesc &operator=(const SubWorkerDesc &other) = delete;
    SubWorkerDesc(SubWorkerDesc &&other) noexcept = default;
    SubWorkerDesc &operator=(SubWorkerDesc &&other) noexcept = default;

    /**
     * @brief Add consumer for this subWorkerDesc.
     * @param[in] subConfig Consumer sub config.
     * @param[in] consumerId Consumer id.
     * @return Status of the call.
     */
    Status AddConsumer(const SubscriptionConfig &subConfig, const std::string &consumerId)
    {
        CHECK_FAIL_RETURN_STATUS(
            subConfig.subscriptionType == SubscriptionType::STREAM, K_INVALID,
            FormatString("Only support STREAM mode. <%s> mode not supported yet.", subConfig.subscriptionName));
        auto ret = consumers_.emplace(consumerId);
        CHECK_FAIL_RETURN_STATUS(ret.second, K_DUPLICATED, "duplicate consumer");
        return Status::OK();
    }

    /**
     * @brief Delete consumer for this subWorkerDesc.
     * @param[in] consumerId Consumer id.
     * @return Status of the call.
     */
    Status DelConsumer(const std::string &consumerId)
    {
        CHECK_FAIL_RETURN_STATUS(
            consumers_.find(consumerId) != consumers_.end(), StatusCode::K_NOT_FOUND,
            FormatString("Consumer:<%s>, Worker:<%s>, State:<Not exist>", consumerId, hostPort_.ToString()));
        consumers_.erase(consumerId);
        return Status::OK();
    }

    /**
     * @brief Get the consumer number for a remote sub worker.
     * @return The consumer number.
     */
    size_t ConsumerNum() const
    {
        return consumers_.size();
    }

private:
    HostPort hostPort_;

    // Key: consumerName Value: Information structure for a consumer.
    std::set<std::string> consumers_;
};

class StreamManager : public std::enable_shared_from_this<StreamManager> {
public:
    explicit StreamManager(std::string streamName, RemoteWorkerManager *remoteWorkerManager,
                           std::string localWorkerAddr, std::shared_ptr<AkSkManager> akSkManager,
                           std::weak_ptr<ClientWorkerSCServiceImpl> scSvc,
                           std::shared_ptr<WorkerSCAllocateMemory> manager,
                           std::weak_ptr<WorkerWorkerSCServiceImpl> workerWorkerSCService, uint64_t localStreamNum);
    ~StreamManager();

    StreamManager(const StreamManager &streamManager) = delete;
    StreamManager &operator=(const StreamManager &streamManager) = delete;
    StreamManager(StreamManager &&streamManager) = delete;
    StreamManager &operator=(StreamManager &&streamManager) = delete;

    /**
     * @brief Create a producer, i.e., register a publisher to a stream.
     * @details Update producer session.
     * @param[in] producerId The generated producer id.
     * @param[out] senderProducerNo A locally unique number for the new producer within this stream.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddProducer(const std::string &producerId, DataVerificationHeader::SenderProducerNo &senderProducerNo);

    /**
     * @brief Set the cursor to producer.
     * @param[in] producerId The generated producer id.
     * @param[out] shmView The work area SHM
     * @return K_OK on success; the error code otherwise.
     */
    Status AddCursorForProducer(const std::string &producerId, ShmView &shmView);

    /**
     * @brief Close a producer, force flushing and page seal, unregister a publisher to a stream.
     * @param[in] producerId The generated producer id.
     * @param[in] forceClose  If the pub node had a crash or regular close.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducer(const std::string &producerId, bool forceClose);

    /**
     * @brief Subscribe to a stream, using a subscription name, i.e., register a consumer to a subscription.
     * @param[in] config Subscription config.
     * @param[in] consumerId Consumer id.
     * @param[out] lastAckCursor The last ack cursor of the new Consumer.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddConsumer(const SubscriptionConfig &config, const std::string &consumerId, uint64_t &lastAckCursor,
                       ShmView &waView);

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * @param[in] subName Subscription name.
     * @param[in] consumerId Consumer id.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumer(const std::string &subName, const std::string &consumerId);

    /**
     * @brief Check stream if no producer/consumer session remains.
     * @return Status of the call.
     */
    Status CheckDeleteStreamCondition();

    /**
     * @brief Create a stream page and get its related shared memory meta to perform zero-copy send.
     * @param[in] producerId Producer id.
     * @param[in] pageId The page id.
     * @param[out] shmView The 4-tuple to represent contiguous shared memory segment.
     * @param[in] retryOnOOM retry at out of memory if the para is set to true
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateOrGetLastDataPage(const std::string &producerId, uint64_t timeoutMs, const ShmView &lastView,
                                   std::shared_ptr<StreamDataPage> &lastPage, bool retryOnOOM);

    /**
     * @brief Call back function for BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>
     */
    Status AllocDataPage(BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb> *);

    Status AllocDataPageInternalReq(uint64_t timeoutMs, const ShmView &curView, ShmView &outView,
                                    const std::string &producerId);

    /**
     * @brief Create a BigElement page
     * @param serverApi
     * @param req
     * @return
     */
    Status AllocBigShmMemory(BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb> *);

    Status AllocBigShmMemoryInternalReq(uint64_t timeoutMs, size_t sz, ShmView &outView, const std::string &producerId);

    /**
     * @brief Release a BigElement page
     */
    Status ReleaseBigShmMemory(
        const std::shared_ptr<ServerUnaryWriterReader<ReleaseLobPageRspPb, ReleaseLobPageReqPb>> &serverApi,
        const ReleaseLobPageReqPb &req);

    /**
     * @brief Adds a blocked CreateShmPage request with timer
     * @param blockedReq The info about the blocked request to add
     * @return K_OK on success; the error code otherwise.
     */
    Status AddBlockedCreateRequest(
        ClientWorkerSCServiceImpl *scSvc,
        std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> blockedReq, bool lock);

    /**
     * @brief Adds a blocked CreateShmPage request with timer
     * @param blockedReq The info about the blocked request to add
     * @return K_OK on success; the error code otherwise.
     */
    Status AddBlockedCreateRequest(
        ClientWorkerSCServiceImpl *scSvc,
        std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> blockedReq, bool lock);

    /**
     * @brief Pop a blocked CreateShmPage request
     * @param blockedReq
     * @return K_OK on success
     */
    Status GetBlockedCreateRequest(
        std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> &blockedReq);

    /**
     * @brief Pop a blocked CreateLobPage request
     * @param blockedReq
     * @return K_OK on success
     */
    Status GetBlockedCreateRequest(
        std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> &blockedReq);

    /**
     * @brief If there were any BlockedCreateRequests waiting for the stream, it will fetch the blocked request
     * and then launch an async thread to execute it now (and cancel the timeout timer).
     * @return K_OK on success; the error code otherwise.
     */
    Status UnblockCreators();

    /**
     * @brief Return the oldest request's requested size and type
     * @return
     */
    std::pair<size_t, bool> GetNextBlockedRequestSize();

    /**
     * @brief Handle a blocked request
     * @param blockedReq
     * @return K_OK on success
     */
    template <typename W, typename R>
    Status HandleBlockedRequestImpl(std::shared_ptr<BlockedCreateRequest<W, R>> &&blockedReq, bool lock);

    /**
     * @brief As part of the Receive api to locate the starting page.
     * @param[in] req The request of ReceiveElements.
     * @param[in] req The subscription
     * @param[in] stream The stream rpc channel that used to get request and write response.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetDataPage(const GetDataPageReqPb &req, const std::shared_ptr<Subscription> &sub,
                       const std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> &serverApi);

    /**
     * @brief Check whether consumer exists.
     * @param[in] workerAddr Remote pub worker addr.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckConsumerExist(const std::string &workerAddr);

    /**
     * @brief Add a remote pub node for this worker in particular stream.
     * @param[in] pubWorkerAddr Remote pub node address
     * @return K_OK on success; the error code otherwise.
     */
    Status AddRemotePubNode(const std::string &pubWorkerAddr);

    /**
     * @brief Performs handling for a closed remote publisher for this worker in particular stream.
     * @param[in] forceClose If the pub node had a crash or regular close
     * @return K_OK on success; the error code otherwise.
     */
    Status HandleClosedRemotePubNode(bool forceClose);

    /**
     * @brief Add a remote sub consumer node for this worker in particular stream.
     * @param[in] subWorker Remote sub consumer node address protobuf.
     * @param[in] subConfig Remote sub consumer node subscription config protobuf.
     * @param[in] consumerId Remote sub consumer node consumer id.
     * @param[out] lastAckCursor Remote sub consumer last acknowledge Cursor.
     * @return K_OK on success; the error code otherwise.
     */
    Status AddRemoteSubNode(const HostPort &subWorker, const SubscriptionConfig &subConfig,
                            const std::string &consumerId, uint64_t &lastAckCursor);

    /**
     * @brief Delete a remote sub consumer node for this worker in particular stream.
     * @param[in] subWorker Remote sub consumer node address.
     * @param[in] consumerId Remote sub consumer node consumer id.
     * @return K_OK on success; the error code otherwise.
     */
    Status DelRemoteSubNode(const HostPort &subWorker, const std::string &consumerId);

    /**
     * @brief Synchronize all remote sub consumer nodes for this worker in particular stream.
     * @param[in] subTable A vector of all remote sub consumer nodes.
     * @param[in] isRecon Is this part of reconciliation process.
     * @param[out] lastAckCursor Remote sub consumer last acknowledge Cursor.
     * @return K_OK on success; the error code otherwise.
     */
    Status SyncSubTable(const std::vector<ConsumerMeta> &subTable, bool isRecon, uint64_t &lastAckCursor);

    /**
     * @brief Synchronize all remote pub worker nodes for this worker in particular stream.
     * @param[in] pubTable A vector of all remote pub worker nodes.
     * @param[in] isRecon Is this part of reconciliation process.
     * @return K_OK on success; the error code otherwise.
     */
    Status SyncPubTable(const std::vector<HostPort> &pubTable, bool isRecon);

    /**
     * @brief Find all local producers.
     * @param[out] localProducers Producer name list on local node.
     */
    void GetLocalProducers(std::vector<std::string> &localProducers);

    /**
     * @brief Find all local consumers.
     * @param[out] localConsumers Consumers name list on local node.
     */
    void GetLocalConsumers(std::vector<std::pair<std::string, SubscriptionConfig>> &localConsumers);

    /**
     * @brief Clear all remote pub.
     * @return Status of the call.
     */
    Status ClearAllRemotePub();

    /**
     * @brief Helper function to clear all remote pub when lock is already held.
     * @return Status of the call.
     */
    void ClearAllRemotePubUnlocked();

    /**
     * @brief Clear all remote consumer, without lock.
     * @param[in] forceClose If the pub client had a crash or regular close
     * @return Status of the call.
     */
    Status ClearAllRemoteConsumerUnlocked(bool forceClose);

    /**
     * @brief Get subscription type by its subName.
     * @param[in] subName The name of the subscription.
     * @param[out] type The output sub type.
     * @return Status of the call.
     */
    Status GetSubType(const std::string &subName, SubscriptionType &type);

    /**
     * @brief Verifies the input stream fields match the existing setting.
     * If the existing settings are uninitialized, updates the values.
     * @param[in] streamFields The stream fields with page size and max stream size to check
     * @return Status of the call.
     */
    Status UpdateStreamFields(const StreamFields &streamFields, bool reserveShm);

    /**
     * @brief Send back the stream fields for the stream of this object
     * @param[in] streamFields The stream fields with page size and max stream size
     */
    void GetStreamFields(StreamFields &streamFields);

    /**
     * @brief Copyies given pageview into stream shm pages
     * @param[in] recvPageView pageview object.
     * @return Status of the call.
     */
    Status CopyElementView(std::shared_ptr<RecvElementView> &recvElementView, UsageMonitor &usageMonitor,
                           uint64_t timeoutMs);

    /**
     * @brief Getter of lastAppendCursor_.
     * @return lastAppendCursor_.
     */
    uint64_t GetLastAppendCursor() const;

    /**
     * @return stream name
     */
    auto GetStreamName() const
    {
        return streamName_;
    }

    /**
     * @brief Block remote producer
     * @param[in] workerAddr Address of the remote producer
     * @return Status of the call.
     */
    Status BlockProducer(const std::string &workerAddr, bool addCallBack);

    /**
     * @brief UnBlock remote producer
     * @param[in] workerAddr Address of the remote producer
     * @return Status of the call.
     */
    Status UnBlockProducer(const std::string &workerAddr);

    /**
     * @brief Check whether remote producer blocked
     * @param[in] workerAddr Address of the remote producer
     * @return Ture if blocked.
     */
    bool IsProducerBlocked(const std::string &workerAddr);

    /**
     * @brief Wake up pending receive if the element is enough.
     */
    void TryWakeUpPendingReceive();

    /**
     * @brief Get subscription by subName.
     * @param[in] subName The name of the subscription.
     * @param[out] subscription The output subscription.
     * @return Status of the call.
     */
    Status GetSubscription(const std::string &subName, std::shared_ptr<Subscription> &subscription);

    /**
     * @brief Set stream state and start the process of cleaning up the buffer pool.
     * @param[in] prodConList The list of producers and consumers of the invoking client.
     * @return Status of the call.
     */
    Status ResetStreamStart(std::vector<std::string> &prodConList);

    /**
     * @brief Complete cleaning up stream data and metadata. Wakeup pending reset request on this stream.
     * @return Status of the call.
     */
    Status ResetStreamEnd();

    /**
     * @brief Force all producers/consumers
     */
    void ForceCloseClients();

    std::string GetStateString()
    {
        switch (streamState_) {
            case StreamState::ACTIVE:
                return "ACTIVE";
            case StreamState::RESET_IN_PROGRESS:
                return "RESET_IN_PROGRESS";
            case StreamState::RESET_COMPLETE:
                return "RESET_COMPLETE";
            case StreamState::DELETE_IN_PROGRESS:
                return "DELETE_IN_PROGRESS";
            default:
                return "INVALID";
        }
        return "INVALID";
    }

    /**
     * @brief Check if stream is in active state or not.
     * @return Status of the call.
     */
    inline Status CheckIfStreamActive()
    {
        std::shared_lock<std::shared_timed_mutex> lock(streamStateMutex_);
        if (streamState_ == StreamState::RESET_IN_PROGRESS || streamState_ == StreamState::RESET_COMPLETE) {
            RETURN_STATUS(K_SC_STREAM_IN_RESET_STATE,
                          FormatString("Reset is invoked on Stream [%s]. Current state: %s. Resume is needed.",
                                       streamName_, GetStateString()));
        } else if (streamState_ == StreamState::DELETE_IN_PROGRESS) {
            RETURN_STATUS(K_SC_STREAM_DELETE_IN_PROGRESS,
                          FormatString("Delete is in progress on Stream [%s].", streamName_));
        }
        return Status::OK();
    }

    inline std::string PrintStreamStatus()
    {
        Status rc = CheckIfStreamActive();
        if (rc.IsOk()) {
            return "Active";
        }
        return rc.GetMsg();
    }

    /**
     * @brief Check if stream is in Reset In Progress state or not.
     * @param[in] state input to check against.
     * @return true if state is same, false otherwise.
     */
    inline bool CheckIfStreamInState(StreamState state)
    {
        std::shared_lock<std::shared_timed_mutex> lock(streamStateMutex_);
        return streamState_ == state;
    }

    /**
     * @brief Set Stream into delete state
     */
    Status SetDeleteState(bool ignore = false)
    {
        // lock is used to protect streamState
        std::unique_lock<std::shared_timed_mutex> lock(streamStateMutex_);
        // If status is already delete then return error
        if (streamState_ == StreamState::DELETE_IN_PROGRESS && ignore) {
            deleteStateRefCount_ += 1;
            LOG(INFO) << FormatString("[S:%s] Ref Count is %d", streamName_, deleteStateRefCount_);
            RETURN_STATUS(K_SC_STREAM_DELETE_IN_PROGRESS,
                          FormatString("Delete is in progress on Stream [%s].", streamName_));
        } else if (streamState_ == StreamState::DELETE_IN_PROGRESS) {
            RETURN_STATUS(K_SC_STREAM_DELETE_IN_PROGRESS,
                          FormatString("Delete is in progress on Stream [%s].", streamName_));
        }
        deleteStateRefCount_ += 1;
        streamState_ = StreamState::DELETE_IN_PROGRESS;
        if (scStreamMetrics_) {
            scStreamMetrics_->LogMetric(StreamMetric::StreamState, (int)streamState_);
        }
        return Status::OK();
    }

    /**
     * @brief Sets stream into Active state
     */
    void SetActiveState()
    {
        // lock is used to protect streamState
        // only re-set Active if there are no other deletes running
        std::unique_lock<std::shared_timed_mutex> lock(streamStateMutex_);
        if (streamState_ == StreamState::DELETE_IN_PROGRESS) {
            deleteStateRefCount_ -= 1;
        }
        if (deleteStateRefCount_ == 0) {
            streamState_ = StreamState::ACTIVE;
            LOG(INFO) << FormatString("[S:%s] Active State Set", streamName_);
            if (scStreamMetrics_) {
                scStreamMetrics_->LogMetric(StreamMetric::StreamState, (int)streamState_);
            }
        } else {
            LOG(INFO) << FormatString("[S:%s] Active State Not Set, refCount %d", streamName_, deleteStateRefCount_);
        }
    }

    /**
     * @brief Set new stream state
     * @param[in] newState new state to set.
     * @return K_SC_STREAM_DELETE_IN_PROGRESS or OK
     */
    inline Status SetNewState(StreamState newState)
    {
        // lock is used to protect streamState
        std::unique_lock<std::shared_timed_mutex> lock(streamStateMutex_);
        if (streamState_ == StreamState::DELETE_IN_PROGRESS) {
            RETURN_STATUS(K_SC_STREAM_DELETE_IN_PROGRESS,
                          FormatString("Delete is in progress on Stream [%s].", streamName_));
        }
        streamState_ = newState;
        if (scStreamMetrics_) {
            scStreamMetrics_->LogMetric(StreamMetric::StreamState, (int)streamState_);
        }
        return Status::OK();
    }

    /**
     * @brief Allow stream to resume operation.
     * @return Status of the call.
     */
    inline Status ResumeStream()
    {
        if (CheckIfStreamInState(StreamState::ACTIVE) || CheckIfStreamInState(StreamState::RESET_COMPLETE)) {
            SetActiveState();
            return Status::OK();
        } else if (CheckIfStreamInState(StreamState::DELETE_IN_PROGRESS)) {
            RETURN_STATUS(K_SC_STREAM_DELETE_IN_PROGRESS,
                          FormatString("Delete is in progress on Stream [%s].", streamName_));
        }
        RETURN_STATUS(K_TRY_AGAIN, FormatString("Reset is still going on for string [%s]", streamName_));
    }
    /**
     * @brief Create the underlying page queue handler where all stream pages are stored
     * @return Status of the call.
     */
    Status CreatePageQueueHandler(Optional<StreamFields> cfg);

    /**
     * Pause the GC thread
     */
    void PauseAckThread();

    /**
     * Resume the GC thread
     */
    void ResumeAckThread();

    /**
     * Called by remote worker manager to move up the ack cursor
     * provided when there is no local consumers
     */
    Status RemoteAck();

    /**
     * @brief Crash recovery for lost client to unlock by cursor.
     * @param[in] cursorId The cursorId.
     * @param[in] isProducer Ture for producer.
     * @param[in] lockId The lock id.
     */
    void ForceUnlockByCursor(const std::string &cursorId, bool isProducer, uint32_t lockId);

    /**
     * @brief Crash recovery for lost client to unlock mem view on all pages.
     * @param[in] lockId The lock id.
     */
    void ForceUnlockMemViemForPages(uint32_t lockId);

    /**
     * @brief Garbage collection by scanning all consumers' last ack cursors
     * @return Status object
     */
    Status AckCursors();

    /**
     * @return T if auto cleanup is on
     */
    bool AutoCleanup() const;

    /**
     * @brief Get ratio of memory allocated to the StreamCache.
     * @return ratio of Mem Allocated to Stream / Total Mem Allocated to Stream Cache.
     */
    double GetStreamMemAllocRatio();

    /**
     * @brief Gets the page size of the stream
     * @return Page size of the stream
     */
    int64_t GetStreamPageSize();

    /**
     * @return the stream memory manager
     */
    auto GetAllocManager()
    {
        return scAllocateManager_;
    }

    /**
     * @return ClientService pointer
     */
    auto GetClientService()
    {
        return scSvc_.lock();
    }

    /**
     * @brief Get max window count
     */
    auto GetMaxWindowCount() const
    {
        return GetExclusivePageQueue()->GetMaxWindowCount();
    }

    /**
     * @brief Get remote worker manager
     */
    auto GetRemoteWorkerManager()
    {
        return remoteWorkerManager_;
    }

    /**
     * @brief Get log prefix.
     * @return The log prefix.
     */
    std::string LogPrefix() const;

    /**
     * @brief Get stream data object
     * @return
     */
    std::shared_ptr<ExclusivePageQueue> GetExclusivePageQueue() const
    {
        return pageQueueHandler_->GetExclusivePageQueue();
    }

    /**
     * @brief Get the stream number of the stream.
     * @return stream number.
     */
    uint64_t GetStreamNo() const
    {
        return localStreamNum_;
    }

    /**
     * @brief Get stream metrics
     * @return
     */
    auto GetSCStreamMetrics()
    {
        return scStreamMetrics_;
    }

    void InitRetainData(uint64_t retainForNumConsumers)
    {
        retainData_.Init(retainForNumConsumers);
        if (scStreamMetrics_) {
            scStreamMetrics_->LogMetric(StreamMetric::RetainDataState, retainData_.GetRetainDataState());
        }
        LOG(INFO) << "[RetainData] state changed for the stream: " << streamName_
                  << " current state: " << retainData_.PrintCurrentState();
    }

    void RollBackRetainDataStateToInit()
    {
        retainData_.RollBackToInit();
        if (scStreamMetrics_) {
            scStreamMetrics_->LogMetric(StreamMetric::RetainDataState, retainData_.GetRetainDataState());
        }
        LOG(INFO) << "[RetainData] state is rolled back to Init for the stream: " << streamName_;
    }

    bool IsRetainData()
    {
        return retainData_.GetRetainDataState() == RetainDataState::RETAIN;
    }

    void SetRetainData(uint32_t state)
    {
        retainData_.SetRetainDataState(static_cast<RetainDataState::State>(state));
        if (scStreamMetrics_) {
            scStreamMetrics_->LogMetric(StreamMetric::RetainDataState, retainData_.GetRetainDataState());
        }
        LOG(INFO) << "[RetainData] state changed for the stream: " << streamName_
                  << " current state: " << retainData_.PrintCurrentState();
    }

    std::vector<std::string> GetRemoteWorkers() const;

    /**
     * @brief Check if RemotePub is empty
     * @return T/F
     */
    bool IsRemotePubEmpty();

    /**
     * @brief Handle a timeout memory alloc rpc request
     * @param producerId
     * @param subTimeout
     * @param startTime The time when BlockedCreateRequest is created.
     */
    template <typename W, typename R>
    void HandleBlockedCreateTimeout(const std::string &producerId, int64_t subTimeout,
                                    const std::chrono::steady_clock::time_point &startTime)
    {
        (void)producerId;
        (void)subTimeout;
        (void)startTime;
    }

    /**
     * @brief Update stream metrics
     */
    void UpdateStreamMetrics();

    /**
     * @brief Initialize stream metrics for this stream
     * @return Status of the call.
     */
    Status InitStreamMetrics();

    /**
     * @brief A preliminary check if we can allocate a page or a big element.
     * @param sz
     * @return T/F
     * @note Not to be considered an absolute check
     */
    bool CheckHadEnoughMem(size_t sz) const;

    /**
     * @brief Get number of local producers
     * @return number of local producers
     */
    size_t GetLocalProducerCount() const
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        return pubs_.size();
    }

    /**
     * @brief Clear blocked request list
     */
    void ClearBlockedList();

    /**
     * (Un)block memory reclaim
     */
    void BlockMemoryReclaim();
    void UnblockMemoryReclaim();

    /**
     * @brief Check enable shared page or not.
     * @param[in] streamMode The stream mode.
     * @return number of local producers
     */
    static bool EnableSharedPage(StreamMode streamMode);

    void SetSharedPageQueue(std::shared_ptr<SharedPageQueue> sharedPageQueue)
    {
        pageQueueHandler_->SetSharedPageQueue(sharedPageQueue);
    }

    /**
     * @brief Get or create shm meta.
     * @param[in] tenantId The ID of tenant.
     * @param[out] view The view of shm meta.
     * @return Status of the call.
     */
    Status GetOrCreateShmMeta(const std::string &tenantId, ShmView &view)
    {
        return pageQueueHandler_->GetOrCreateShmMeta(tenantId, view);
    }

    /**
     * @brief Try to decrease the usage of shared memory in this node for this stream.
     * @param[in] size The size to be increased.
     * @return Status of the call.
     */
    Status TryDecUsage(uint64_t size)
    {
        return pageQueueHandler_->TryDecUsage(size);
    }

    /**
     * @brief Get stream meta shm.
     * @return The pointer to stream meta shm.
     */
    StreamMetaShm *GetStreamMetaShm()
    {
        return pageQueueHandler_->GetStreamMetaShm();
    }

    Status MarkMemAllocFinish(
        const std::string &streamName, BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb> *blockedReq,
        std::shared_ptr<BlockedCreateRequest<CreateShmPageRspPb, CreateShmPageReqPb>> &outblockedReq);
    Status MarkMemAllocFinish(
        const std::string &streamName, BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb> *blockedReq,
        std::shared_ptr<BlockedCreateRequest<CreateLobPageRspPb, CreateLobPageReqPb>> &outblockedReq);

protected:
    /**
     * @brief Create subscription if it not exist.
     * @param[in] config The config of the subscription.
     * @return Status of the call.
     */
    Status CreateSubscriptionIfMiss(const SubscriptionConfig &config, uint64_t &lastAckCursor);

    /**
     * @brief Get the min ack cursor of all subscriptions.
     * @return The min ack cursor of all subscriptions.
     */
    uint64_t UpdateLastAckCursorUnlocked(uint64_t lastAppendCursor);

private:
    /**
     * @brief Helper function to return the number of elements of this stream received by consumers, and reset
     * the count.
     * @return Returns the value of this variable before it was called.
     */
    uint64_t GetEleCount();

    /**
     * @brief Helper function to return the number of elements of this stream that were acked
     * @return Returns the amount.
     */
    uint64_t GetEleCountAcked();

    /**
     * @brief Helper function to return the number of elements of this stream sent and reset the count
     * @return Returns the amount.
     */
    uint64_t GetEleCountSentAndReset();

    /**
     * @brief Helper function to return the number of elements of this stream received
     * @return Returns the amount.
     */
    uint64_t GetEleCountReceived();

    /**
     * @brief Helper function to return the number of send requests this stream received and reset the counts
     * @return Returns the amount.
     */
    uint64_t GetSendRequestCountAndReset();

    /**
     * @brief Helper function to return the number of receive requests this stream received
     * @return Returns the amount.
     */
    uint64_t GetReceiveRequestCountAndReset();

    /**
     * @brief Inline function to add callback to unblock sending stream.
     * @param[in] addr The producer worker address.
     * @param[in] unblockCallback The callback functions to unblock the stream.
     */
    void AddUnblockCallback(const std::string &addr, std::function<void()> unblockCallback);

    /**
     * @brief Removes the producers and consumers received from a client from the reset pub/sub list.
     * @param[in] prodConList The list of producers and consumers which should be removed from reset pub/sub lsit.
     * @return K_OK on success; the error code otherwise.
     */
    Status RemovePubSubFromResetList(std::vector<std::string> &prodConList);

    /**
     * @brief Helper function to reclaim shared memory pages when producers and consumers are all gone.
     * @return K_OK on success; the error code otherwise.
     */
    Status EarlyReclaim(bool remoteAck = false, uint64_t lastAppendCursor = 0, uint64_t newAckCursor = 0);

    Status SendBlockProducerReq(const std::string &remoteWorkerAddr);
    Status SendUnBlockProducerReq(const std::string &remoteWorkerAddr);
    void ResetOOMState(const std::string &remoteWorkerAddr);

    std::string workerAddr_;
    const std::string streamName_;
    RemoteWorkerManager *remoteWorkerManager_;
    // protect pubs_/subs_/remoteSubWorkerDict_/remotePubWorkerDict_/blockOnOOM_
    mutable std::shared_timed_mutex mutex_;
    mutable std::shared_timed_mutex resetMutex_;
    // protect streamState_
    mutable std::shared_timed_mutex streamStateMutex_;
    // deleteStateRefCount_ to protect stream state from being reactivated
    int deleteStateRefCount_ = 0;

    std::unordered_map<std::string, std::shared_ptr<Producer>> pubs_;
    std::unordered_map<std::string, std::shared_ptr<Subscription>> subs_;
    MemAllocRequestList<CreateShmPageRspPb, CreateShmPageReqPb> dataBlockedList_;
    MemAllocRequestList<CreateLobPageRspPb, CreateLobPageReqPb> lobBlockedList_;
    std::shared_timed_mutex streamManagerBlockedListsMutex_;
    std::unordered_map<std::string, std::atomic<bool>> blockOnOOM_;  // block at a worker level

    // Remote sub workers, consumers. Key: remote sub worker address, Value: remote SubWorkerDesc
    std::unordered_map<std::string, std::shared_ptr<SubWorkerDesc>> remoteSubWorkerDict_;
    // Remote pub workers. Key: remote pub worker address, Value: remote HostPort
    std::unordered_set<std::string> remotePubWorkerDict_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::weak_ptr<ClientWorkerSCServiceImpl> scSvc_;
    std::unique_ptr<PageQueueHandler> pageQueueHandler_;
    mutable std::shared_timed_mutex ackMutex_;
    WaitPost ackWp_;
    std::atomic_uint64_t lastAckCursor_;
    bool wakeupPendingRecvOnProdFault_;
    StreamState streamState_{ StreamState::ACTIVE };
    std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager_{ nullptr };
    std::vector<std::string> prodConResetList_;
    std::weak_ptr<WorkerWorkerSCServiceImpl> workerWorkerSCService_;
    RetainDataState retainData_;
    std::atomic<bool> pendingLastProducerClose_{ false };
    std::atomic<bool> pendingLastProducerForceClose_{ false };
    std::shared_ptr<SCStreamMetrics> scStreamMetrics_{ nullptr };
    mutable std::shared_timed_mutex reclaimMutex_;
    WaitPost reclaimWp_;
    // +1 everytime a new producer is added to pubs_.
    // Use to identify each local producer within the stream with a unique number in data verification.
    // The count will not be decreased even if create producer failed, or producer is closed.
    DataVerificationHeader::SenderProducerNo lifetimeLocalProducerCount_{ 0 };
    uint64_t localStreamNum_{ 0 };
};

template <>
inline void StreamManager::HandleBlockedCreateTimeout<CreateShmPageRspPb, CreateShmPageReqPb>(
    const std::string &producerId, int64_t subTimeout, const std::chrono::steady_clock::time_point &startTime)
{
    dataBlockedList_.HandleBlockedCreateTimeout(streamName_, producerId, subTimeout, startTime);
}

template <>
inline void StreamManager::HandleBlockedCreateTimeout<CreateLobPageRspPb, CreateLobPageReqPb>(
    const std::string &producerId, int64_t subTimeout, const std::chrono::steady_clock::time_point &startTime)
{
    lobBlockedList_.HandleBlockedCreateTimeout(streamName_, producerId, subTimeout, startTime);
}
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_STREAM_CACHE_STREAM_MANAGER_H

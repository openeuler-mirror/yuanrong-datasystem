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

#ifndef DATASYSTEM_WORKER_STREAM_CACHE_CLIENT_WORKER_SC_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_STREAM_CACHE_CLIENT_WORKER_SC_SERVICE_IMPL_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/stream_cache/stream_fields.h"
#include "datasystem/common/util/lock_helper.h"
#include "datasystem/common/util/lock_map.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/master/stream_cache/master_sc_service_impl.h"
#include "datasystem/protos/stream_posix.service.rpc.pb.h"
#include "datasystem/protos/stream_posix.stub.rpc.pb.h"
#include "datasystem/utils/optional.h"
#include "datasystem/worker/stream_cache/remote_worker_manager.h"
#include "datasystem/worker/stream_cache/stream_producer.h"
#include "datasystem/worker/stream_cache/worker_master_sc_api.h"
#include "datasystem/worker/stream_cache/worker_sc_allocate_memory.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

namespace datasystem {
namespace worker {
namespace stream_cache {
class MasterWorkerSCServiceImpl;
class WorkerWorkerSCServiceImpl;
class UsageMonitor;
class RemoteWorkerManager;

/**
 * @brief A simple class to save a AllocMemory Request so that it can be executed at a later time.
 */
template <typename W, typename R>
class BlockedCreateRequest {
public:
    enum AckVal : uint32_t { NONE = 0, DONE = 1 };
    using BlockedCreateReqFn = std::function<Status(BlockedCreateRequest<W, R> *)>;

    /**
     * @brief Constructor for the blocked request. Initializes an elapsed timer start time.
     * @param[in] req The request details to save
     * @param[in] serverApi The UnaryWriterReader api to associate with the request.
     */
    BlockedCreateRequest(std::string streamName, const R &req, size_t reqSz,
                         std::shared_ptr<ServerUnaryWriterReader<W, R>> serverApi, BlockedCreateReqFn fn,
                         std::weak_ptr<StreamManager> streamMgr = {});

    /**
     * @brief default destructor
     */
    ~BlockedCreateRequest() = default;

    /**
     * @brief Sets the reference to the timer queue entry for this request
     */
    void SetTimer(std::unique_ptr<TimerQueue::TimerImpl> timer);

    /**
     * @brief Cancels the running timer queue entry for this request
     */
    void CancelTimer();

    /**
     * @brief Uses the internal timer to compute how much time has passed, and then subtract that from the input timeout
     * arg.
     * @return the new computed timeout value
     */
    int64_t GetRemainingTimeMs();

    /**
     * @brief A getter function for the request info (deep copy return)
     * @return A copy of the request info
     */
    R GetCreateRequest() const;

    Status SendStatus(const Status &rc);

    Status Write();

    Status Wait(uint64_t timeoutMs);

    Status SenderHandShake();

    Status ReceiverHandShake();

    /**
     * @brief Handle a timeout request
     * @return
     */
    Status HandleBlockedCreateTimeout();

    /**
     * @brief Functor to execute the call back
     */
    Status operator()();

    /**
     * @brief Check if the BlockedCreateRequest is created at startTime
     */
    bool HasStartTime(const std::chrono::steady_clock::time_point &startTime);

    /**
     * @brief Check if the internal request pb is older than the input request pb.
     * Require inputRequest is made by the same producer as this request.
     */
    bool HasRequestPbOlderThan(const BlockedCreateRequest<W, R> &inputRequest);

    W rsp_;
    R req_;
    size_t reqSize_;
    std::shared_ptr<ServerUnaryWriterReader<W, R>> serverApi_;
    const std::chrono::steady_clock::time_point startTime_;
    std::atomic<uint64_t> retryCount_;
    std::string streamName_;
    std::string traceId_;
    Status defaultRc_;
    BlockedCreateReqFn callBackFn_;
    WaitPost wp_;               // If serverApi is null
    WaitPost ackWp_;            // Handshake
    std::atomic_uint32_t ack_;  // Handshake
    std::weak_ptr<StreamManager> streamMgr_;

private:
    std::unique_ptr<TimerQueue::TimerImpl> timer_;
    Timer timeSpent_;  // Start time initialized at construction time
};

using CreatePubSubCtrl = LockMap<std::string, int>;
using ProduceGrpByStreamList = std::unordered_map<std::string, std::list<StreamProducer>>;

/**
 * A class to wrap StreamManager with an accessor which can be an exclusive or a read accessor
 * Used only in CreateProducer/Subscribe api
 */
class StreamManagerWithLock {
public:
    StreamManagerWithLock(std::shared_ptr<StreamManager> mgr, void *accessor, bool exclusive,
                          std::shared_ptr<ClientWorkerSCServiceImpl> service);
    ~StreamManagerWithLock();
    void Release();

    /**
     * @brief If necessary, clean up any effects that occurred during the lock period.
     * @param[in] callback The implementation of cleanup work.
     */
    void CleanUp(std::function<void(StreamManagerMap::accessor *accessor)> &&callback);

    bool needCleanUp = true;
    std::shared_ptr<StreamManager> mgr_;

private:
    using ReadLockHelperType = ReadLockHelper<std::shared_timed_mutex, std::function<std::string()>>;

    void *accessor_;
    bool exclusive_;
    std::shared_ptr<ClientWorkerSCServiceImpl> service_;
    std::unique_ptr<ReadLockHelperType> rlock_;
};
class ClientWorkerSCServiceImpl : public ClientWorkerSCService,
                                  public std::enable_shared_from_this<ClientWorkerSCServiceImpl> {
public:
    /**
     * @brief Construct the rpc service of ClientWorkerSCServiceImpl.
     * @param[in] serverAddr The address of worker.
     * @param[in] masterAddr The address of master.
     * @param[in] masterSCService The master service.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    ClientWorkerSCServiceImpl(HostPort serverAddr, HostPort masterAddr, master::MasterSCServiceImpl *masterSCService,
                              std::shared_ptr<AkSkManager> akSkManager,
                              std::shared_ptr<WorkerSCAllocateMemory> manager);

    /**
     * @brief Init the service.
     * @return Status of the call.
     */
    Status Init() override;

    ~ClientWorkerSCServiceImpl() override;

    /**
     * @brief Check if there are tasks to be processed
     * @return T/F
     */
    bool HaveTasksToProcess()
    {
        return remoteWorkerManager_->HaveTasksToProcess();
    }

    /**
     * @brief Create a producer, i.e., register a publisher to a stream.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateProducer(
        std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> serverApi) override;

    /**
     * @brief Close a producer, force flushing and page seal, unregister a publisher to a stream.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducer(
        std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> serverApi) override;

    /**
     * @brief Subscribe to a stream, using a subscription name, i.e., register a consumer to a subscription.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status Subscribe(std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> serverApi) override;

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumer(
        std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> serverApi) override;

    /**
     * @brief Create a stream page and get its related shared memory meta to perform zero-copy send.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateShmPage(
        std::shared_ptr<ServerUnaryWriterReader<CreateShmPageRspPb, CreateShmPageReqPb>> serverApi) override;

    Status GetDataPage(std::shared_ptr<ServerUnaryWriterReader<GetDataPageRspPb, GetDataPageReqPb>> serverApi) override;

    template <typename W, typename R>
    void AsyncSendMemReq(const std::string &namespaceUri);

    template <typename W, typename R>
    Status HandleBlockedRequestImpl(const std::string &streamName);

    /**
     * @brief Delete stream manager and related sessions when it is not used anymore.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status DeleteStream(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp) override;

    /**
     * @brief Query producer count in global scope for one stream
     * @param[in] req The rpc request protobuf
     * @param[out] rsp The rpc response protobuf
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalProducersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp) override;

    /**
     * @brief Query consumer count in global scope for one stream
     * @param[in] req The rpc request protobuf
     * @param[out] rsp The rpc response protobuf
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalConsumersNum(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp) override;

    /**
     * @brief Closes all of the producers and consumers for a given client when client crashes
     * - Any consumers of the producers from this client will cease to function and give error SC_PRODUCER_NOT_FOUND.
     * - Any in-progress data flowing from the closed producers will be dropped/freed and not sent to the remote.
     * @param[in] clientId The ID of client.
     * @return Status of the call.
     */
    Status ClosePubSubForClientLost(const std::string &clientId);

    /**
     * @brief Unlock mem view on all pages for given streams.
     * @param[in] streams The stream name list.
     * @param[in] lockId The lock id.
     */
    void ForceUnlockMemViemForPages(const std::set<std::string> &streams, uint32_t lockId);

    /**
     * @brief Get the Stream Metadata object
     * @param[in] streamName The stream name.
     * @param[out] meta The rpc protobuf for stream metadata
     * @return K_OK on success; the error code otherwise.
     */
    Status GetStreamMetadata(const std::string &streamName, GetStreamMetadataRspPb *meta);

    /**
     * @brief Collect the producer consumer metadata for the given list of producers and consumers.
     * @param[in] localProducers The list of producers from this worker for the given stream
     * @param[in] localConsumers The list of consumers from this worker for the given stream
     * @param[in] meta The rpc protobuf for stream metadata
     * @param[in] streamName The stream name of the producers and consumers
     * @param[in] hostPortPb The protobuf message for the address of this worker
     */
    void GetProducerConsumerMetadata(std::vector<std::string> &localProducers,
                                     std::vector<std::pair<std::string, SubscriptionConfig>> &localConsumers,
                                     GetStreamMetadataRspPb *meta, const std::string &streamName,
                                     HostPortPb &hostPortPb);

    /**
     * @brief Get metadata for all streams for the requesting master.
     * @param[in] masterAddr The GetMetadataAllStreamReqPb request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetAllStreamMetadata(const GetMetadataAllStreamReqPb &req, GetMetadataAllStreamRspPb &rsp);

    /**
     * @brief Send metadata for all streams for the requesting master using the stream rpc.
     * @param[in] req The GetMetadataAllStreamReqPb request protobuf.
     * @param[in/out] streamRpc Used to read request from master and write response to master.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendAllStreamMetadata(
        const GetMetadataAllStreamReqPb &req,
        std::shared_ptr<ServerWriterReader<GetStreamMetadataRspPb, GetMetadataAllStreamReqPb>> &streamRpc);

    /**
     * @brief Unblock producer sending stream
     * @param[in] serverApi The api used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status UnblockProducer(
        std::shared_ptr<ServerUnaryWriterReader<UnblockProducerRspPb, UnblockProducerReqPb>> serverApi) override;

    /**
     * @brief Blocks producer sending stream
     * @param[in] serverApi The api used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status BlockProducer(
        std::shared_ptr<ServerUnaryWriterReader<BlockProducerRspPb, BlockProducerReqPb>> serverApi) override;

    /**
     * @brief Get a pointer to the threadpool
     * @return raw pointer to the threadpool
     */
    ThreadPool *GetThreadPool()
    {
        return threadPool_.get();
    }

    /**
     * @brief Return the number of streams.
     * @return Usage: numStream.
     */
    std::string GetTotalStreamCount();

    Status GetStreamManager(const std::string &streamName, StreamManagerMap::const_accessor &accessor);

    /**
     * @brief Blocks a remote producer that belongs to a stream
     * @param[in] streamName Stream Name.
     * @param[in] remoteWorkerAddr Remote Worker address.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendBlockProducerReq(const std::string &streamName, const std::string &remoteWorkerAddr);

    /**
     * @brief UnBlocks a remote producer that belongs to a stream
     * @param[in] streamName Stream Name.
     * @param[in] remoteWorkerAddr Remote Worker address.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendUnBlockProducerReq(const std::string &streamName, const std::string &remoteWorkerAddr);

    /**
     * @brief Get last append cursor in worker consumer.
     * @param[in] req The LastAppendCursorReqPb Request.
     * @param[out] rsp The LastAppendCursorRspPb Response.
     * @return Status of the call.
     */
    Status GetLastAppendCursor(const LastAppendCursorReqPb &req, LastAppendCursorRspPb &rsp) override;

    /**
     * @brief The main delete stream driver is DeleteStream() call in the worker. This call DeleteStreamContext() is an
     * internal call when the master sends a delete to the worker and MasterWorkerSCServiceImpl wants to delete the
     * stream.
     * @param[in] streamName is the name/key into the streamManagerDict_ to erase
     * @param[in] forceDelete Force deletion
     * @param[in] timeout delete exits if exceeded the timeout
     * @return Status of the call
     */
    Status DeleteStreamContext(const std::string &streamName, bool forceDelete, int64_t timeout);

    /**
     * @brief Helper function to create a client stub for worker.
     * @param[in] workerHostPort worker Address.
     * @param[out] stub creates a client stub.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetWorkerStub(const HostPort &workerHostPort, std::shared_ptr<ClientWorkerSCService_Stub> &stub);

    /**
     * @brief Cleanup cached data and metadata for the requested streams.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status ResetStreams(std::shared_ptr<ServerUnaryWriterReader<ResetOrResumeStreamsRspPb, ResetOrResumeStreamsReqPb>>
                            serverApi) override;

    /**
     * @brief Resume streams to allow regular data flow.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status ResumeStreams(std::shared_ptr<ServerUnaryWriterReader<ResetOrResumeStreamsRspPb, ResetOrResumeStreamsReqPb>>
                             serverApi) override;

    /**
     * @brief Allocate shared memory for big element insert
     */
    Status AllocBigShmMemory(
        std::shared_ptr<ServerUnaryWriterReader<CreateLobPageRspPb, CreateLobPageReqPb>> serverApi) override;

    /**
     * @brief Release big element
     */
    Status ReleaseBigShmMemory(
        std::shared_ptr<ServerUnaryWriterReader<ReleaseLobPageRspPb, ReleaseLobPageReqPb>> serverApi) override;

    /**
     * @brief Setter method for assigning worker-worker service
     * @param[in] impl The pointer to worker-worker stream cache service
     */
    void SetWorkerWorkerSCServiceImpl(std::weak_ptr<WorkerWorkerSCServiceImpl> impl)
    {
        workerWorkerSCService_ = impl;
    }

    /**
     * @brief Setter method for assigning cluster manager
     * @param[in] cm The pointer to etcd cluster manager
     */
    void SetClusterManager(EtcdClusterManager *cm)
    {
        etcdCM_ = cm;
    }

    /**
     * @brief erase failed worker master api.
     * @param[in] masterAddr failed master addr.
     */
    void EraseFailedWorkerMasterApi(HostPort &masterAddr);

    /**
     * @brief Get remote worker manager
     */
    auto GetRemoteWorkerManager()
    {
        return remoteWorkerManager_.get();
    }

    /**
     * @brief Reserve memory from the usage monitor.
     * @return Status of the call.
     */
    Status ReserveMemoryFromUsageMonitor(const std::string &streamName, size_t reserveSize);

    /**
     * @brief Undo the memory reservation from the usage monitor.
     * @return Status of the call.
     */
    Status UndoReserveMemoryFromUsageMonitor(const std::string &streamName);

    /**
     * @brief Obtain the success rate of sending data to the remote worker.
     * @return The success rate string.
     */
    std::string GetSCRemoteSendSuccessRate() const;

    /**
     * @brief Called by TimerQueue for expired memory allocation request.
     * @return
     */
    template <typename W, typename R>
    Status HandleBlockedCreateTimeout(const std::string &streamName, const std::string &producerId,
                                      const std::string &traceId, int64_t subTimeout,
                                      const std::chrono::steady_clock::time_point &startTime)
    {
        (void)streamName;
        (void)producerId;
        (void)traceId;
        (void)subTimeout;
        (void)startTime;
        return Status::OK();
    }

    /**
     * @brief Convert from stream number to the corresponding stream name.
     * @param[in] streamNo The stream number.
     * @param[out] streamName The stream name.
     * @return Status of the call.
     */
    Status StreamNoToName(uint64_t streamNo, std::string &streamName);

    /**
     * @brief Record the stream number to stream name mapping.
     * @param[in] streamNo The stream number.
     * @param[out] streamName The stream name.
     * @return Status of the call.
     */
    Status AddStreamNo(uint64_t streamNo, const std::string &streamName);

    /**
     * @brief Take stream number out from the mapping.
     * @param[in] streamNo The stream number to remove.
     */
    void RemoveStreamNo(uint64_t streamNo);

private:
    /**
     * @brief Get the stream name list.
     * @return The stream name list.
     */
    std::vector<std::string> GetStreamNameList();

    /**
     * @brief Check workers health status
     * @return K_OK on success; the error code otherwise.
     */
    Status ValidateWorkerState();

    /**
     * @brief Create stream manager if not exist.
     * @param[in] streamName The name of the stream.
     * @param[in] streamFields Optional argument to pre-assign stream fields after stream construction.
     * @param[out] streamManager The output stream manager.
     * @param[out] streamExisted True if the stream already existed and a new one was not created.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateStreamManagerImpl(const std::string &streamName, const Optional<StreamFields> &streamFields,
                                   StreamManagerMap::accessor &accessor);
    Status CreateStreamManagerIfNotExist(const std::string &streamName, const Optional<StreamFields> &streamFields,
                                         std::shared_ptr<StreamManagerWithLock> &streamMgrWithLock,
                                         bool &streamExisted);

    /**
     * @brief Get current log with local worker address.
     * @param[in] withAddress This value is used to decide whether to add local address, default is false.
     * @return The head of log.
     */
    std::string LogPrefix(bool withAddress = false) const;

    /**
     * @brief Close a producer, force flushing and page seal, unregister a publisher to a stream.
     * @param[in] producerId The producer id.
     * @param[in] streamName The stream name.
     * @param[in] notifyMaster Notify master or not.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducerImpl(const std::string &producerId, const std::string &streamName, bool notifyMaster);

    /**
     * @brief Close a list of producers, force flushing and page seal, unregister a publisher to a stream.
     * @param[in] lockId The lock id.
     * @param[in/out] producerList A list of StreamProducers
     * On success, the producerList will be empty. If any of the producers failed to close, this list will contain
     * the producers that did not close successfully.
     * @return K_OK on success; the error code otherwise. In the case of multiple producers getting errors, the
     * returned error will be the first error that was encountered.
     */
    Status CloseProducerImplForceClose(uint32_t lockId, std::list<StreamProducer> &producerList);

    /**
     * @brief Helper function to send CloseProducer request through worker to master api.
     * @param[in] api The stream cache worker to master api.
     * @param[in/out] streamList A list of streams that failed the request to master.
     * @param[in] forceClose Force close in worker.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducerHandleSend(std::shared_ptr<WorkerMasterSCApi> api, std::list<std::string> &streamList,
                                   bool forceClose);

    /**
     * @brief Helper function for the list version of CloseProducerImpl.
     * @param[in/out] producerList A list of StreamProducers that failed the request to master.
     * On success, the producerList will be empty. If any of the producers failed to close, this list will contain
     * the producers that did not close successfully.
     * @param[in/out] successList A list of StreamProducers that were successful requests.
     * @param[in] forceClose Force close in worker.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducerHandleFailure(std::list<StreamProducer> &producerList, std::list<StreamProducer> &successList,
                                      bool forceClose);

    /**
     * @brief A helper function to lookup the stream and close each producer from the input list.
     * @param[in/out] producerList A list of producers to close in the stream manager. Successfully remove entries
     * will be removed from the producerList, leaving only the unsuccessful ones in the producerList.
     * @param[in] forceClose If the pub node had a crash or regular close.
     * @return Status of the call.
     */
    Status CloseStreamProducerList(std::list<StreamProducer> &producerList, bool forceClose);

    /**
     * @brief When Client Crashes, gets stream manager const accessor and unlocks the page lock
     * @param[in] producerList List of producers that have crashed
     * @param[in] lockId lock id for the producer
     * @param[out] producersGrpStreamName producerList grouped by stream names
     * @return K_OK on success; the error code otherwise.
     */
    Status UnlockAndProtect(std::list<StreamProducer> &producerList, uint32_t lockId,
                            ProduceGrpByStreamList &producersGrpStreamName);

    /**
     * @brief Closes all producers locally and gets list of streams that needs master notifications
     * @param[in] producerList List of producers that have crashed
     * @param[out] streamListForNotifications Set of streams that got there last producer closed
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducerLocallyOnForceClose(std::list<StreamProducer> &producerList,
                                            std::set<std::string> &streamListForNotifications);

    /**
     * @brief Sends master notifications to all streams in the list
     * @param[in] streamList Set of streams that have no producers
     * @param[out] failedList List of streams that failed to send master notifications
     * @return K_OK on success; the error code otherwise.
     */
    Status SendBatchedCloseProducerReq(std::set<std::string> &streamList, std::vector<std::string> &failedList);

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * @param[in] consumerId The consumer id.
     * @param[in] streamName The stream name.
     * @param[in] subName The subscription name.
     * @param[in] notifyMaster Notify master or not.
     * @param[in] lockId The lockId for client.
     * @param[in] forceClose Force close in worker.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumerImpl(const std::string &consumerId, const std::string &streamName, const std::string &subName,
                             bool notifyMaster, uint32_t lockId, bool forceClose = false);

    /**
     * @brief Check connection to master.
     * @param[in] streamName The stream name.
     * @return K_OK on success; the error code otherwise.
     */
    Status CheckConnection(const std::string &streamName);

    /**
     * @brief Helper function of the CreateProducer logic.
     * @param[in] api The stream cache worker to master api.
     * @param[in] streamName The stream to be constructed.
     * @param[out] streamFields Stream fields after stream construction.
     * @return Status of the call.
     */
    Status CreateProducerHandleSend(std::shared_ptr<WorkerMasterSCApi> api, const std::string &streamName,
                                    const Optional<StreamFields> &streamFields);

    /**
     * @brief Implementation of the CreateProducer logic.
     * @param[in] namespaceUri The stream name for the producer
     * @param[in] req The request info for the create producer
     * @return Status of the call
     */
    Status CreateProducerImpl(const std::string &namespaceUri, const CreateProducerReqPb &req,
                              CreateProducerRspPb &rsp);

    /**
     * @brief Implementation of the Subscribe logic.
     * @param[in] namespaceUri The stream name
     * @param[in] req The request info for the subscribe
     * @param[in] rsp The response info from the subscribe
     * @return Status of the call
     */
    Status SubscribeImpl(const std::string &namespaceUri, const SubscribeReqPb &req, SubscribeRspPb &rsp);

    /**
     * @brief Implementation of the Ack logic
     * @param[in] streamName The name of the stream
     * @param[in] streamManager The stream manager of the consumer to ack with.
     * @param[in] subscription The subscription to use for the Ack
     * @param[in] consumerId The id of the consumer to use for the ack
     * @param[in] elementId The cursor position for the Ack
     * @return K_OK on success; the error code otherwise.
     */
    Status AckImpl(const std::string &streamName, std::shared_ptr<StreamManager> streamManager,
                   std::shared_ptr<Subscription> subscription, const std::string &consumerId, uint64_t elementId);

    /**
     * Implementation function for auto ack
     */
    using AckTask = std::tuple<std::future<Status>, std::string, std::chrono::high_resolution_clock::time_point>;
    void AutoAckImpl(std::deque<AckTask> &ackList, const uint64_t waitTimeS);
    void WaitForAckTask(std::deque<AckTask> &ackList, const uint64_t waitTimeS);

    /**
     * @brief Send reply for streams to get reset.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @param[in] streamsSize The size of  list of streams to get reset.
     * @param[in] doneListSizse The sizse of streams already completed the reset operation.
     * @param[in] errListSize The size of streams encountered an error while doing the reset.
     * @return K_OK on success; the error code otherwise.
     */
    static Status ResetStreamsReply(
        std::shared_ptr<ServerUnaryWriterReader<ResetOrResumeStreamsRspPb, ResetOrResumeStreamsReqPb>> serverApi,
        size_t streamsSize, size_t doneListSize, size_t errListSize);

    /**
     * @brief Get the primary replica addr
     * @param[in] srcAddr The source address.
     * @param[out] destAddr The dest address.
     * @return Status of this call.
     */
    Status GetPrimaryReplicaAddr(const std::string &srcAddr, HostPort &destAddr);

    /**
     * @brief Retry and redirect
     * @tparam Req Request to master
     * @tparam Rsp Response of master
     * @param[in] req Request of redirect
     * @param[out] rsp Response of redirect
     * @param[in] workerMasterApi worker master api
     * @param[in] fun Create update or copy meta to master.
     * @return
     */
    template <typename Req, typename Rsp>
    Status RedirectRetryWhenMetaMoving(Req &req, Rsp &rsp, std::shared_ptr<WorkerMasterSCApi> &workerMasterApi,
                                       std::function<Status(Req &, Rsp &)> fun)
    {
        CHECK_FAIL_RETURN_STATUS(fun != nullptr, K_RUNTIME_ERROR, "function is nullptr");
        while (reqTimeoutDuration.CalcRealRemainingTime() > 0) {
            RETURN_IF_NOT_OK(fun(req, rsp));
            if (rsp.info().redirect_meta_address().empty()) {
                return Status::OK();
            } else if (!rsp.meta_is_moving()) {
                HostPort newMetaAddr;
                RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(rsp.info().redirect_meta_address(), newMetaAddr));
                LOG(INFO) << "meta has been migrated to the new master[%s]" << newMetaAddr.ToString();
                RETURN_IF_NOT_OK_APPEND_MSG(workerMasterApiManager_->GetWorkerMasterApi(newMetaAddr, workerMasterApi),
                                            "hash master get failed, RedirectRetryWhenMetaMoving failed");
                if (etcdCM_->MultiReplicaEnabled()) {
                    req.set_redirect(false);
                    RETURN_IF_NOT_OK(fun(req, rsp));
                    return Status::OK();
                }
            }
            static const int sleepTimeMs = 200;
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        }
        return Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
    }

    /**
     * @brief Retry when meta is moving
     * @param Rsp Response of redirect
     * @param Req Request of redirect
     * @param rsp Response of redirect
     * @param fun Query or delete request to master
     * @return
     */
    template <typename Req, typename Rsp>
    Status RedirectRetryWhenMetasMoving(Req &req, Rsp &rsp, std::shared_ptr<WorkerMasterSCApi> &workerMasterApi,
                                        std::function<Status(Req &, Rsp &)> fun)
    {
        CHECK_FAIL_RETURN_STATUS(fun != nullptr, K_RUNTIME_ERROR, "function is nullptr");
        while (reqTimeoutDuration.CalcRealRemainingTime() > 0) {
            RETURN_IF_NOT_OK(fun(req, rsp));
            if (rsp.info().empty()) {
                return Status::OK();
            } else if (!rsp.meta_is_moving()) {
                HostPort newMetaAddr;
                RETURN_IF_NOT_OK(GetPrimaryReplicaAddr(rsp.info(0).redirect_meta_address(), newMetaAddr));
                LOG(INFO) << "meta has been migrated to the new master[%s]" << newMetaAddr.ToString();
                workerMasterApi = workerMasterApiManager_->GetWorkerMasterApi(newMetaAddr);
                CHECK_FAIL_RETURN_STATUS(workerMasterApi != nullptr, K_RUNTIME_ERROR,
                                         "hash master get failed, RedirectRetryWhenMetaMoving failed");
                if (etcdCM_->MultiReplicaEnabled()) {
                    req.set_redirect(false);
                    RETURN_IF_NOT_OK(fun(req, rsp));
                    return Status::OK();
                }
            }
            static const int sleepTimeMs = 200;
            rsp.Clear();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleepTimeMs));
        }
        return Status(K_RPC_DEADLINE_EXCEEDED, "Rpc timeout");
    }

    /**
     * @brief Construct protobuf struct for producer
     * @param[in] streamName The stream to be constructed.
     * @param[in] streamFields Optional argument to pre-assign stream fields after stream construction..
     * @param[out] out The output CreateProducerReqPb.
     */
    void ConstructCreateProducerPb(const std::string &streamName, const Optional<StreamFields> &streamFields,
                                   master::CreateProducerReqPb &out) const noexcept;

    /**
     * @brief Construct protobuf struct for the CloseProducerReqPb
     * @param[in/out] streamList is the list of streams to add into the request. The input list will be
     * erased after this call.
     * @param[in] forceClose T/F if force close mode will be used
     * @param[out] req The close producer request structure that is now populated with data.
     */
    void ConstructCloseProducerReq(std::list<std::string> &streamList, bool forceClose,
                                   master::CloseProducerReqPb &req) const noexcept;

    /**
     * @brief Parse the CloseProducerRspPb to check it for error. If error, populate the failed list with the failed
     * producers and then return the rc of the failure. If no errors, failedList will remain unchanged.
     * @param[out] failedList The list of failed streams if there was an error.
     * @param[in] rsp The response pb to parse for errors.
     * @return The error code from the response, otherwise OK
     */
    Status HandleCloseProducerRsp(std::list<std::string> &failedList, const master::CloseProducerRspPb &rsp) const;

    /**
     * @brief Construct protobuf struct for consumer.
     * @param[in] streamName The stream to be constructed.
     * @param[in] consumerId The id of consumer.
     * @param[in] lastAckCursor The cursor of last ack.
     * @param[in] config The config of the Subscription.
     * @param[in] clientId The client id.
     * @param[out] out The output ConsumerMetaPb.
     */
    void ConstructConsumerMetaPb(const std::string &streamName, const std::string &consumerId, uint64_t lastAckCursor,
                                 const SubscriptionConfig &config, const std::string &clientId,
                                 ConsumerMetaPb &out) const noexcept;

    /**
     * @brief Helper function to send Subscribe request through worker to master api.
     * @param[in] streamMgr The stream manager for the stream.
     * @param[in] streamName The stream to be constructed.
     * @param[in] consumerId The id of consumer.
     * @param[in] lastAckCursor The cursor of last ack.
     * @param[in] config The config of the Subscription.
     * @param[in] clientId The client id.
     * @param[out] streamFields Stream fields after stream construction.
     * @param[out] masterAddress The master address.
     * @return K_OK on success; the error code otherwise.
     */
    Status SubscribeHandleSend(std::shared_ptr<StreamManager> streamMgr, const std::string &streamName,
                               const std::string &consumerId, uint64_t lastAckCursor, const SubscriptionConfig &config,
                               const std::string &clientId, Optional<StreamFields> &streamFields,
                               std::string &masterAddress);

    /**
     * @brief Check if the given address is the master for the testing stream.
     * @param[in] streamName The name of the testing stream
     * @param[in] masterAddr The given master address for the testing stream.
     * @param[in] hashRanges The given hash ranges for the testing stream.
     * @return True if the master address for the stream is found and matches with the given address, False otherwise.
     */
    bool CheckConditionsForStream(const std::string &streamName, const std::string &masterAddr,
                                  const worker::HashRange &hashRanges);

    /**
     * @brief Get all the producers and consumers for a client created on the given stream.
     * @param[in] clientId The requesting client Id.
     * @param[in] streamName The stream for which pubsub list should be returned.
     * @param[out] prodConList The list of producer and consumer Ids for the requesting client.
     * @return The status of the call.
     */
    Status GetPubSubForClientStream(const std::string &clientId, const std::string &streamName,
                                    std::vector<std::string> &prodConList);

    /**
     * @brief Cleanup all the data belong to the stream locally
     * @param[in] streamManager stream manager for the stream
     * @return The status of the call.
     */
    Status DeleteStreamLocally(StreamManagerMap::accessor &accessor);

    /**
     * @brief Create a producer, i.e., register a publisher to a stream.
     * @param[in] req The request instance.
     * @param[in] recorder The access recorder instance.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateProducerInternal(
        const CreateProducerReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
        std::shared_ptr<ServerUnaryWriterReader<CreateProducerRspPb, CreateProducerReqPb>> serverApi);

    /**
     * @brief Close a producer, force flushing and page seal, unregister a publisher to a stream.
     * @param[in] req The request instance.
     * @param[in] recorder The access recorder instance.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseProducerInternal(
        const CloseProducerReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
        std::shared_ptr<ServerUnaryWriterReader<CloseProducerRspPb, CloseProducerReqPb>> serverApi);

    /**
     * @brief Subscribe to a stream, using a subscription name, i.e., register a consumer to a subscription.
     * @param[in] req The request instance.
     * @param[in] recorder The access recorder instance.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status SubscribeInternal(const SubscribeReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
                             std::shared_ptr<ServerUnaryWriterReader<SubscribeRspPb, SubscribeReqPb>> serverApi);

    /**
     * @brief Close a consumer, trigger subscription cursor change and unregister a subscribed consumer to a stream.
     * @param[in] req The request instance.
     * @param[in] recorder The access recorder instance.
     * @param[in] serverApi Used to read request from client and write response to client.
     * @return K_OK on success; the error code otherwise.
     */
    Status CloseConsumerInternal(
        const CloseConsumerReqPb &req, std::shared_ptr<AccessRecorderStreamWrap> recorder,
        std::shared_ptr<ServerUnaryWriterReader<CloseConsumerRspPb, CloseConsumerReqPb>> serverApi);

    /**
     * @brief Delete stream manager and related sessions when it is not used anymore.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status DeleteStreamImpl(const DeleteStreamReqPb &req, DeleteStreamRspPb &rsp);

    /**
     * @brief Send delete stream request.
     * @param[in] streamName The stream name.
     * @return K_OK on success; the error code otherwise.
     */
    Status DeleteStreamHandleSend(const std::string &streamName);

    /**
     * @brief Query producer count in global scope for one stream
     * @param[in] req The rpc request protobuf
     * @param[out] rsp The rpc response protobuf
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalProducersNumImpl(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp);

    /**
     * @brief Query consumer count in global scope for one stream
     * @param[in] req The rpc request protobuf
     * @param[out] rsp The rpc response protobuf
     * @return K_OK on success; the error code otherwise.
     */
    Status QueryGlobalConsumersNumImpl(const QueryGlobalNumReqPb &req, QueryGlobalNumRsqPb &rsp);

    Status PostCreateStreamManager(const std::shared_ptr<StreamManager> &streamManager,
                                   const Optional<StreamFields> &streamFields, bool reserveShm);

    /**
     * @brief Erase from streamMgrDict_ without lock.
     * @param[in] namespaceUri The key to be erased.
     * @param[in] accessor The accessor of streamMgrDict_.
     */
    void EraseFromStreamMgrDictWithoutLck(const std::string &namespaceUri, StreamManagerMap::accessor *accessor);

    friend class MasterWorkerSCServiceImpl;  // They share the stream data on local worker node
    friend class StreamManagerWithLock;

    std::unique_ptr<RemoteWorkerManager> remoteWorkerManager_{ nullptr };
    std::shared_timed_mutex mutex_;  // protect streamMgrDict_.
    StreamManagerMap streamMgrDict_;
    std::atomic<uint64_t> lifetimeLocalStreamCount_{ 0 };
    std::shared_timed_mutex mappingMutex_;  // protect streamNum2StreamName_.
    std::unordered_map<uint64_t, std::string> streamNum2StreamName_;
    CreatePubSubCtrl createStreamLocks_;

    std::shared_ptr<WorkerMasterApiManagerBase<WorkerMasterSCApi>> workerMasterApiManager_{ nullptr };

    HostPort localWorkerAddress_;
    HostPort masterAddress_;

    std::shared_timed_mutex clearMutex_;  // Protect requests success when other client crash.
    struct SubInfo {
        SubInfo(std::string streamName, std::string subName, std::string consumerId)
            : streamName(std::move(streamName)), subName(std::move(subName)), consumerId(std::move(consumerId))
        {
        }
        std::string streamName;
        std::string subName;
        std::string consumerId;
    };
    std::map<std::string, std::list<StreamProducer>> clientProducers_;
    std::map<std::string, std::list<SubInfo>> clientConsumers_;
    std::shared_ptr<WorkerSCAllocateMemory> scAllocateManager_;
    std::weak_ptr<WorkerWorkerSCServiceImpl> workerWorkerSCService_;
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<ThreadPool> threadPool_{ nullptr };
    std::shared_ptr<ThreadPool> memAllocPool_{ nullptr };
    std::shared_ptr<ThreadPool> ackPool_{ nullptr };
    // For remote workers/producers
    std::mutex remotePubStubMutex_;
    std::unordered_map<std::string, std::shared_ptr<ClientWorkerSCService_Stub>> remotePubStubs_;
    std::atomic<bool> interrupt_;
    std::future<void> autoAck_;
    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager
};

template <>
Status ClientWorkerSCServiceImpl::HandleBlockedCreateTimeout<CreateShmPageRspPb, CreateShmPageReqPb>(
    const std::string &streamName, const std::string &producerId, const std::string &traceId, int64_t subTimeout,
    const std::chrono::steady_clock::time_point &startTime);

template <>
Status ClientWorkerSCServiceImpl::HandleBlockedCreateTimeout<CreateLobPageRspPb, CreateLobPageReqPb>(
    const std::string &streamName, const std::string &producerId, const std::string &traceId, int64_t subTimeout,
    const std::chrono::steady_clock::time_point &startTime);

template <typename W, typename R>
class MemAllocRequestList {
public:
    Status AddBlockedCreateRequest(ClientWorkerSCServiceImpl *scSvc,
                                   std::shared_ptr<BlockedCreateRequest<W, R>> blockedReq,
                                   std::shared_ptr<BlockedCreateRequest<W, R>> *out = nullptr);
    void RemoveBlockedCreateRequestFromQueueLocked(const BlockedCreateRequest<W, R> *blockedReqPtr);
    void HandleBlockedCreateTimeout(const std::string &streamName, const std::string &producerId, int64_t subTimeout,
                                    const std::chrono::steady_clock::time_point &startTime);
    Status GetBlockedCreateRequest(std::shared_ptr<BlockedCreateRequest<W, R>> &out);
    bool Empty();
    size_t Size();
    size_t GetNextBlockedRequestSize();
    auto GetNextStartTime()
    {
        if (queue_.empty()) {
            return std::chrono::steady_clock::now();
        }
        return queue_.top()->startTime_;
    }
    void ClearBlockedList();

    Status MarkMemAllocFinish(const std::string &streamName, const std::string &producerId,
                              std::shared_ptr<BlockedCreateRequest<W, R>> &outblockedReq);

    Status GetOrCreate(ClientWorkerSCServiceImpl *scSvc, std::shared_ptr<BlockedCreateRequest<W, R>> inblockedReq,
                     std::shared_ptr<BlockedCreateRequest<W, R>> &outblockedReq);

private:
    std::shared_timed_mutex blockedListMutex_;
    struct Compare {
        bool operator()(const BlockedCreateRequest<W, R> *a, const BlockedCreateRequest<W, R> *b)
        {
            return a->startTime_ > b->startTime_;
        }
    };
    std::unordered_map<std::string, std::shared_ptr<BlockedCreateRequest<W, R>>> processingBlockedList_;
    std::unordered_map<std::string, std::shared_ptr<BlockedCreateRequest<W, R>>> blockedList_;
    std::priority_queue<BlockedCreateRequest<W, R> *, std::vector<BlockedCreateRequest<W, R> *>, Compare> queue_;
};
}  // namespace stream_cache
}  // namespace worker
}  // namespace datasystem
#endif

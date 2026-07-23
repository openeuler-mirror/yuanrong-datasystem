/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Defines the worker worker service processing main class.
 */
#ifndef DATASYSTEM_WORKER_OC_WORKER_SERVICE_IMPL_H
#define DATASYSTEM_WORKER_OC_WORKER_SERVICE_IMPL_H

#include <functional>
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rdma/urma_send_lane.h"
#include "datasystem/cluster/membership/membership_endpoint_view.h"
#include "datasystem/cluster/runtime/control_backend_state.h"
#include "datasystem/protos/worker_object.irpc.pb.h"
#include "datasystem/protos/worker_object.service.rpc.pb.h"
#include "datasystem/protos/worker_object.brpc.pb.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/common/object_cache/shm_guard.h"
#include "datasystem/worker/runtime/worker_runtime_facade.h"

namespace datasystem {
namespace object_cache {

class WorkerOCServiceImpl;

class WorkerWorkerOCServiceImpl : public WorkerWorkerOCService, public IWorkerWorkerOCService {
public:
    using BackendObservationProvider = std::function<cluster::ControlBackendObservation()>;
    using CoordinationAvailabilityProvider = std::function<bool()>;

    /**
     * @brief Construct WorkerWorkerOCServiceImpl.
     * @param[in] clientSvc The implementation of worker service.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] membership Read-only topology membership view.
     * @param[in] coordinationAvailable Callback returning local coordination health.
     * @param[in] backendObservationProvider Callback returning current local backend evidence.
     */
    WorkerWorkerOCServiceImpl(std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> clientSvc,
                              std::shared_ptr<AkSkManager> akSkManager,
                              const cluster::MembershipEndpointView &membership,
                              CoordinationAvailabilityProvider coordinationAvailable,
                              BackendObservationProvider backendObservationProvider);

    ~WorkerWorkerOCServiceImpl() override;

    /**
     * @brief Initialize the WorkerOCMasterServiceApi Object(include rpc channel).
     * @return Status of the call.
     */
    Status Init() override;

    /**
     * @brief Borrow local runtime facade for migration target admission.
     * @param[in] runtime Runtime facade owned by WorkerOCServer.
     */
    void SetRuntimeFacade(const worker::WorkerRuntimeFacade *runtime);

    /**
     * @brief Get object data from remote worker and load data from disk if necessary.
     * @param [in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return Status of the call.
     */
    Status GetObjectRemote(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetObjectRemoteRspPb, GetObjectRemoteReqPb>> serverApi)
        override;

    /**
     * @brief Get object data from remote worker and load data from disk if necessary.
     * @param[in] req Remote get request.
     * @param[out] rsp Remote get response.
     * @param[out] payload Out payloads.
     * @param[in] isQueryAndGet Whether to use the metadata-query resident-data fast path.
     * @return Status of the call.
     */
    Status GetObjectRemote(GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payload,
                           bool isQueryAndGet = false);

    /**
     * @brief Check etcd state.
     * @param[in] req Check etcd state request.
     * @param[out] rsp Check etcd state response.
     * @return Status of the call.
     */
    Status CheckCoordinatorState(const CheckCoordinatorStateReqPb &req, CheckCoordinatorStateRspPb &rsp) override;

    /**
     * @brief Get cluster state.
     * @param[in] req Get cluster state request.
     * @param[out] rsp Get cluster state response.
     * @return Status of the call.
     */
    Status GetClusterState(const GetClusterStateReqPb &req, GetClusterStateRspPb &rsp) override;

    /**
     * @brief Migrate data when scale down happen.
     * @param[in] req Migrate data request.
     * @param[out] rsp Migrate data response.
     * @param[in] payloads Payloads.
     * @return Status of the call.
     */
    Status MigrateData(const MigrateDataReqPb &req, MigrateDataRspPb &rsp,
                       std::vector<::datasystem::RpcMessage> payloads) override;

    /**
     * @brief Migrate data directly.
     * @param[in] req Migrate data direct request.
     * @param[out] rsp Migrate data direct response.
     * @return Status of the call.
     */
    Status MigrateDataDirect(const MigrateDataDirectReqPb &req, MigrateDataDirectRspPb &rsp) override;

    /**
     * @brief Get batch of object data from remote worker and load data from disk if necessary.
     * @param [in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return Status of the call.
     */
    Status BatchGetObjectRemote(
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<BatchGetObjectRemoteRspPb, BatchGetObjectRemoteReqPb>>
            serverApi) override;

    /**
     * @brief Migrate data by triggering remote get during voluntary scale down.
     * @param[in] req rpc request.
     * @param[out] rsp rpc response.
     * @return Status of the call.
     */
    Status NotifyRemoteGet(const NotifyRemoteGetReqPb &req, NotifyRemoteGetRspPb &rsp) override;

private:
    struct AggregateInfo {
        bool canBatchHandler = false;
        std::vector<uint64_t> batchReqSize;
        std::vector<uint64_t> batchSizes;
        std::vector<uint64_t> batchStartIndex;
    };

    struct AggregateMemory {
        std::shared_ptr<ShmUnit> batchShmUnit = nullptr;
        uint64_t batchCursor = 0;
        std::vector<LocalSgeInfo> localSgeInfos;
        std::vector<RpcMessage> fallbackPayloads;  // store the rpc message for fallback in batch handler
    };

    struct ParallelRes {
        std::vector<GetObjectRemoteRspPb> respPbs;
        std::vector<std::pair<uint64_t, std::pair<std::vector<uint64_t>, std::vector<RpcMessage>>>> kps;
        std::vector<RpcMessage> fallbackPayloads;
        std::vector<Status> fallbackStatuses;
        std::vector<uint64_t> eventKeys;
        uint64_t subIndex = 0;
    };

    struct BatchRh2dContext {
        enum class UrmaTransportMode {
            DEFAULT,
            SHARED_LANE,
            TCP_FALLBACK,
        };

        bool prepared = false;
        int32_t devId = -1;
        // One worker-to-worker Batch Get RPC either owns one shared URMA lane or is pinned to TCP fallback before
        // any sub-request starts. TCP_FALLBACK prevents per-object and aggregate paths from attempting URMA again.
        UrmaTransportMode urmaTransportMode = UrmaTransportMode::DEFAULT;
        std::shared_ptr<UrmaSendLaneLease> sendLaneLease;
        Status urmaAcquireStatus;

        bool IsUrmaTcpFallback() const
        {
            return urmaTransportMode == UrmaTransportMode::TCP_FALLBACK;
        }
    };

    /**
     * @brief Load object data in remote get provider mode.
     * @param[in] req Pb Request for RemoteGet rpc.
     * @param[out] rsp Pb Response for RemoteGet rpc.
     * @param[out] outPayload Payload buffers.
     * @param[in] blocking Whether to blocking wait for the urma_write to finish.
     * @param[out] keys The new request id to wait for if not blocking.
     * @param[in] batchPtr Batch ptr, default is nullptr means not in aggregate path.
     * @param[in] batchRootInfo The common root info for batched requests.
     * @param[in] isQueryAndGet Whether this is a QueryAndGet fast-path attempt.
     * @return Status of the call.
     */
    Status GetObjectRemoteImpl(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                               std::vector<RpcMessage> &outPayload, bool blocking, std::vector<uint64_t> &eventKeys,
                               std::shared_ptr<AggregateMemory> batchPtr = nullptr,
                               RemoteH2DRootInfoPb *batchRootInfo = nullptr, Status *fallbackStatus = nullptr,
                               BatchRh2dContext *batchRh2dContext = nullptr, bool isQueryAndGet = false);

    Status LoadPayloadAndFillResponse(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, SafeObjType &entry,
                                      std::vector<RpcMessage> &outPayload, const std::string &objectKey,
                                      uint64_t offset, uint64_t size, bool blocking, std::vector<uint64_t> &eventKeys,
                                      const std::shared_ptr<AggregateMemory> &batchPtr,
                                      RemoteH2DRootInfoPb *batchRootInfo, BatchRh2dContext *batchRh2dContext,
                                      Status *fallbackStatus, bool isFastTransportEnabled, bool isUrmaFastTransport,
                                      bool isPipelineH2DRequest, PerfPoint &batchImplPoint, bool isQueryAndGet);

    /**
     * @brief Load a spilled object for the regular remote-get path.
     * @param[in] objectKey Object key.
     * @param[out] outPayload Payload buffers loaded from spill storage.
     * @param[in] objKv Object read range.
     * @param[in,out] point Remote-get performance point.
     * @param[in] isQueryAndGet Whether this is a QueryAndGet fast-path attempt.
     * @return K_OK on success; the error code otherwise.
     */
    Status LoadSpilledObjectData(const std::string &objectKey, std::vector<RpcMessage> &outPayload,
                                 const ReadObjectKV &objKv, PerfPoint &point, bool isQueryAndGet);

    /**
     * @brief Fill a successful remote-get response and finish its performance records.
     * @param[out] rsp Remote-get response.
     * @param[in] entry Object entry returned by the read path.
     * @param[in,out] loadDataPoint Object-load performance point.
     * @param[in,out] batchImplPoint Remote-get implementation performance point.
     */
    void FillGetObjectRemoteResponse(GetObjectRemoteRspPb &rsp, const SafeObjType &entry, PerfPoint &loadDataPoint,
                                     PerfPoint &batchImplPoint);

    Status LockEntryForRemoteGet(const std::string &objectKey, bool tryLock, uint64_t version,
                                 std::shared_ptr<SafeObjType> &safeEntry);

    Status CheckFastTransportSize(const SafeObjType &entry, uint64_t expectedDataSize, const std::string &objectKey,
                                  bool isFastTransportEnabled, GetObjectRemoteRspPb &rsp);

    Status WriteViaFastTransport(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, SafeObjType &entry,
                                 std::shared_ptr<ShmUnit> shmUnit, uint64_t localSegAddress, uint64_t localSegSize,
                                 uint64_t offset, uint64_t size, bool blocking, std::vector<uint64_t> &eventKeys,
                                 const std::shared_ptr<AggregateMemory> &batchPtr, bool isFastTransportEnabled,
                                 bool isPipelineH2DRequest, BatchRh2dContext *batchRh2dContext,
                                 Status &fastTransportStatus, std::string &fastTransportName);

    Status HandlePayloadFallback(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp, SafeObjType &entry,
                                 std::vector<RpcMessage> &outPayload, ShmGuard &shmGuard,
                                 std::shared_ptr<ShmUnit> shmUnit, Status &fastTransportStatus,
                                 const std::string &fastTransportName, const std::string &objectKey,
                                 bool isUrmaFastTransport, bool isPipelineH2DRequest, bool blocking,
                                 const std::shared_ptr<AggregateMemory> &batchPtr, Status *fallbackStatus,
                                 RemoteH2DRootInfoPb *batchRootInfo, BatchRh2dContext *batchRh2dContext,
                                 const ReadObjectKV &objKv, uint64_t localSegAddress, uint64_t localSegSize);

    Status ProcessFallbackTrackError(const Status &rc, const Status &fastTransportStatus, bool blocking,
                                     Status *fallbackStatus, bool &canPrepareFallbackPayload,
                                     const std::string &objectKey);

    /**
     * @brief Helper function to GetObjectRemote, but specialized for the batch get path.
     * @param[in] subIndex Sub slot index of the parallel list.
     * @param[in] req Remote get sub request.
     * @param[out] rsp Remote get response.
     * @param[out] payload Out payloads.
     * @param[out] keys The request id to wait for if not blocking.
     * @param[out] parallelRes Parallel result.
     * @param[in] batchPtr Batch ptr, default is nullptr means not in aggregate path.
     * @return Status of the call.
     */
    Status GetObjectRemoteBatchWrite(uint32_t subIndex, const GetObjectRemoteReqPb &req, BatchGetObjectRemoteRspPb &rsp,
                                     std::vector<ParallelRes> &parallelRes,
                                     std::shared_ptr<AggregateMemory> batchPtr = nullptr,
                                     BatchRh2dContext *batchRh2dContext = nullptr);

    /**
     * @brief Helper function to BatchGetObjectRemote to process batched requests and wait fast transport events.
     * @param[in] req Remote get batch request.
     * @param[out] rsp Remote get batch response.
     * @param[out] payload Out payloads.
     * @return Status of the call.
     */
    Status BatchGetObjectRemoteImpl(BatchGetObjectRemoteReqPb &req, BatchGetObjectRemoteRspPb &rsp,
                                    std::vector<RpcMessage> &payload);

    /**
     * @brief Prepare and validate a batch remote get request before execution.
     * @param[in, out] req Remote get batch request.
     * @return Status of the call.
     */
    Status PrepareBatchGetObjectRemoteReq(BatchGetObjectRemoteReqPb &req);

    /**
     * @brief Merge parallel batch get results to final response and payload.
     * @param[in] req Remote get batch request.
     * @param[in, out] parallelRes Parallel result list.
     * @param[out] rsp Remote get batch response.
     * @param[out] payload Out payloads.
     * @return Status of the call.
     */
    Status MergeParallelBatchGetResult(const BatchGetObjectRemoteReqPb &req, std::vector<ParallelRes> &parallelRes,
                                       BatchGetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payload);

    /**
     * @brief Wait fast transport events and fallback to payload when needed.
     * @param[in] req Remote get batch request.
     * @param[in, out] loc Local parallel result slot.
     * @param[in, out] kp Event key and fallback payload pair.
     * @param[out] rsp Remote get batch response.
     * @param[out] payload Out payloads.
     * @param[in, out] index Current response index.
     * @return Status of the call.
     */
    Status WaitFastTransportAndFallback(
        const BatchGetObjectRemoteReqPb &req, ParallelRes &loc,
        std::pair<uint64_t, std::pair<std::vector<uint64_t>, std::vector<RpcMessage>>> &kp,
        BatchGetObjectRemoteRspPb &rsp, std::vector<RpcMessage> &payload, uint64_t &index, uint64_t coveredRespNum,
        const Status &fallbackStatus);

    /**
     * @brief Helper function to BatchGetObjectRemote to process requests in parallel.
     * @param[in] req Remote get batch request.
     * @param[out] rsp Remote get batch response.
     * @param[out] payload Out payloads.
     * @param[out] keys The request id to wait for if not blocking.
     * @param[out] lastRc The last try-again status seen during parallel processing.
     * @return Status of the call.
     */
    Status ParallelBatchGetObject(BatchGetObjectRemoteReqPb &req, BatchGetObjectRemoteRspPb &rsp,
                                  std::vector<ParallelRes> &parallelRes, const BatchRh2dContext &batchTransportContext);

    /**
     * @brief Helper function to BatchGetObjectRemote to prepare the aggregate info.
     * @param[in] req Remote get request.
     * @param[out] info Aggregated info.
     * @return Status of the call.
     */
    Status PrepareAggregateMemory(BatchGetObjectRemoteReqPb &req, AggregateInfo &info);

    /**
     * @brief Helper function to BatchGetObjectRemote to send the aggregate memory without merge small data in remote
     * node.
     * @param[in] subIndex Sub slot index of the parallel list.
     * @param[in] info Aggregated info.
     * @param[in] batchPtr Batch ptr, default is nullptr means not in aggregate path.
     * @param[out] parallelRes Parallel result.
     * @param[in] req Remote get request.
     * @return Status of the call.
     */
    Status GatherWrite(uint64_t subIndex, AggregateInfo &info, std::shared_ptr<AggregateMemory> aggregatedMem,
                       std::vector<ParallelRes> &parallelRes, BatchGetObjectRemoteReqPb &req,
                       const std::shared_ptr<UrmaSendLaneLease> &sendLaneLease);

    /**
     * @brief Helper function pre-process and then trigger GetObjectRemoteImpl.
     * @param[in] req Remote get request.
     * @param[out] rsp Remote get response.
     * @param[out] payload Out payloads.
     * @param[in] blocking Whether to blocking wait for the urma_write to finish.
     * @param[out] keys The request id to wait for if not blocking.
     * @param[in] batchPtr Batch ptr, default is nullptr means not in aggregate path.
     * @param[in] batchRootInfo The common root info for batched requests.
     * @param[in] isQueryAndGet Whether this is a QueryAndGet fast-path attempt.
     * @return Status of the call.
     */
    Status GetObjectRemoteHandler(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp,
                                  std::vector<RpcMessage> &payload, bool blocking, std::vector<uint64_t> &eventKeys,
                                  std::shared_ptr<AggregateMemory> batchPtr = nullptr,
                                  RemoteH2DRootInfoPb *batchRootInfo = nullptr, Status *fallbackStatus = nullptr,
                                  BatchRh2dContext *batchRh2dContext = nullptr, bool isQueryAndGet = false);

    /**
     * @brief Complete a URMA warmup request when the target warmup object is not ready locally.
     * @param[in] req Remote get request.
     * @param[out] rsp Remote get response.
     * @return True if the request has been completed.
     */
    bool TryCompleteMissingUrmaWarmup(const GetObjectRemoteReqPb &req, GetObjectRemoteRspPb &rsp);

    /**
     * @brief Get the safe object entry.
     * @param[in] objectKey The object key.
     * @param[in] tryLock Try lock object or not.
     * @param[in] version Expected object version.
     * @param[out] safeEntry The safe object entry.
     */
    Status GetSafeObjectEntry(const std::string &objectKey, bool tryLock, uint64_t version,
                              std::shared_ptr<SafeObjType> &safeEntry);

    /**
     * @brief Establish P2P Communicator connection and also fill in the segment info.
     * @param[in] commId The client communicator uuid for unique connection.
     * @param[in] localSegAddress The local segment address.
     * @param[in] localSegSize The local segment size.
     * @param[in] shmUnit The object shared memory unit.
     * @param[in] metadataSize The metadata size of the object.
     * @param[out] rsp The remote get response.
     * @param[in] batchRootInfo The common root info for batched requests.
     */
    Status EstablishConnAndFillSeg(const std::string &commId, const uint64_t &localSegAddress,
                                   const uint64_t &localSegSize, std::shared_ptr<ShmUnit> shmUnit,
                                   uint64_t metadataSize, GetObjectRemoteRspPb &rsp,
                                   RemoteH2DRootInfoPb *batchRootInfo = nullptr,
                                   BatchRh2dContext *batchRh2dContext = nullptr);

    Status PrepareBatchRh2dContext(const GetObjectRemoteReqPb &req, BatchRh2dContext &batchRh2dContext);

    /**
     * @brief Check if the fast transport connection is stable.
     * @param[in] req Remote get request with one key.
     * @return Status of the call.
     */
    Status CheckConnectionStable(const GetObjectRemoteReqPb &req);

    std::shared_ptr<datasystem::object_cache::WorkerOCServiceImpl> ocClientWorkerSvc_;
    std::shared_ptr<AkSkManager> akSkManager_;
    const cluster::MembershipEndpointView &membership_;
    CoordinationAvailabilityProvider coordinationAvailable_;
    const worker::WorkerRuntimeFacade *runtime_{ nullptr };
    BackendObservationProvider backendObservationProvider_;
    std::shared_ptr<ThreadPool> communicatorThreadPool_{ nullptr };
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OC_WORKER_SERVICE_IMPL_H

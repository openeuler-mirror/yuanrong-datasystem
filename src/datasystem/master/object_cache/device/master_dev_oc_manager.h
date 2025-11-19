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
 * Description: Code to manage device object.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_OC_MANAGER_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_OC_MANAGER_H

#include <future>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/device/device_helper.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/master/object_cache/device/master_dev_dead_lock_manager.h"
#include "datasystem/master/object_cache/device/master_dev_hccl_rootinfo.h"
#include "datasystem/master/object_cache/device/master_dev_npu_events.h"
#include "datasystem/master/object_cache/device/master_dev_oc_directory.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/master/object_cache/device/master_dev_client_meta_manager.h"

namespace datasystem {

namespace master {
class MasterDevOcManager {
    using TbbLockTable = tbb::concurrent_hash_map<std::string, bool>;

public:
    MasterDevOcManager() = default;

    ~MasterDevOcManager();

    /**
     * @brief Initialize the MasterDevOcManager object.
     */
    void Init();

    /**
     * @brief The implementation of putting p2p metadata in master.
     * @param[in] req The rpc PutP2PMeta req protobuf.
     * @param[out] resp The rpc PutP2PMeta rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PutP2PMetaImpl(const PutP2PMetaReqPb &req, PutP2PMetaRspPb &resp);

    /**
     * @brief Process the GetP2PMeta request.
     * @param[in] req The rpc GetP2PMeta req protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessGetP2PMetaRequest(
        const GetP2PMetaReqPb &req,
        const std::shared_ptr<ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> &serverApi);

    /**
     * @brief Process the SubscribeReceiveEvent request.
     * @param[in] req The rpc SubscribeReceiveEvent req protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessSubscribeReceiveEventRequest(
        const SubscribeReceiveEventReqPb &req,
        const std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>>
            &serverApi);

    /**
     * @brief The implementation of sending RootInfo in master.
     * @param[in] req The rpc SendRootInfo req protobuf.
     * @param[out] resp The rpc SendRootInfo rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status SendRootInfoImpl(const SendRootInfoReqPb &req, SendRootInfoRspPb &resp);

    /**
     * @brief Process the RecvRootInfo request.
     * @param[in] req The rpc RecvRootInfo req protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessRecvRootInfoRequest(
        const RecvRootInfoReqPb &req,
        const std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> &serverApi);

    /**
     * @brief The implementation of acknowledging data receiving finished.
     * @param[in] req The rpc AckRecvFinish request protobuf.
     * @param[out] resp The rpc AckRecvFinish response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status AckRecvFinish(const AckRecvFinishReqPb &req, AckRecvFinishRspPb &resp);

    Status AckSrcLocation(const std::string &srcClientId, int srcDeviceId,
                          std::shared_ptr<ObjectDirectory> lockedObjDir);

    void NotifyClientClearMeta(const std::string &objectKey, const std::string &srcClientId, int srcDeviceId);

    /**
     * @brief Obtains all ObjKeys of ReceiveEvent for subscription.
     * @param[in] srcClientId The client Id.
     * @param[in] srcDeviceId The device Id.
     * @param[out] objectKeys The objectKeys to subscribe.
     * @return K_OK on success; the error code otherwise.
     */
    Status RemoveP2PLocation(const RemoveP2PLocationReqPb &req, RemoveP2PLocationRspPb &resp);

    /**
     * @brief Process the GetDataInfo request.
     * @param[in] req The rpc GetDataInfo req protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessGetDataInfoRequest(
        const GetDataInfoReqPb &req,
        const std::shared_ptr<ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi);

    /**
     * @brief Verify the clientId and dataInfos of the GetP2PMeta request.
     * @param[in] dstClientId The client id.
     * @param[in] dataInfosFromGet The vector of DataInfo from Get request.
     * @param[in] objDirectory The directory of object.
     * @return Status of the call
     */
    Status VerifyGetRequest(const std::string &dstClientId, const std::vector<DataInfo> &dataInfosFromGet,
                            const std::shared_ptr<ObjectDirectory> &objDirectory);

    /**
     * @brief Check and clear device metadata for a specific object key
     * @param[in] objectKey The unique identifier of the object to check and clear
     * @return Status::OK() if object is not found or successfully cleared;
     *         Error status if any unexpected error occurs during the operation
     */
    Status CheckAndClearDeviceMeta(const std::string &objectKey);

    /**
     * @brief Release client metadata for scaled-in worker nodes
     * @details Iterates through the list of removed worker nodes, retrieves all client IDs
     *          associated with each worker, and triggers metadata release for each client
     * @param[in] removeNodes List of worker node addresses that have been scaled in
     * @return Status of the call
     */
    Status ReleaseClientMetaForScaledInWorker(const std::vector<std::string> &removeNodes);

    /**
     * @brief Clears the metadata information on the directory.
     * @param[in] req The rpc DeviceReleaseMetaData req protobuf.
     * @return Status of the call
     */
    Status DeviceReleaseMetaDataImpl(const ReleaseMetaDataReqPb &req, ReleaseMetaDataRspPb &resp);

    /**
     * @brief Get the device match keys from match function.
     * @param[in] matchFunc The key match function.
     * @param[out] deviceMetaKeys The match keys list.
     */
    void GetDeviceMetasMatch(std::function<bool(const std::string &)> &&matchFunc,
                             std::vector<std::string> &deviceMetaKeys);

    /**
     * @brief Fill migrate data.
     * @param[out] req The migrate rpc request
     */
    void FillDeviceMetas(MigrateMetadataReqPb &req);

    /**
     * @brief Save migrate data.
     * @param[in] req The migrate rpc request
     */
    void SaveMigrationDeviceMeta(const MigrateMetadataReqPb &req);

    /**
     * @brief Handle when migrate success, clear device meta.
     * @param[in] req The migrate rpc request
     */
    void HandleMigrateSuccess();

    /**
     * @brief Check the device metas from req is empty.
     * @return True if device metas if empty.
     */
    bool CheckDeviceMetasMigrateInfoIsEmpty(const MigrateMetadataReqPb &req);

    /**
     * @brief Delete device objects.
     * @param[in] request The rpc DeleteAllCopyMetaReq protobuf.
     * @param[in] serverApi The unary writer reader.
     * @return Status of the call
     */
    Status DeleteDevObjects(
        const DeleteAllCopyMetaReqPb &request,
        const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi);

    /**
     * @brief Delete device objects.
     * @param[in] request The rpc DeleteAllCopyMetaReq protobuf.
     * @param[out] resp The rpc DeleteAllCopyMetaRsp protobuf.
     */
    void DeleteDevObjectsImpl(const DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &resp);

    /**
     * @brief Get meta info of keys.
     * @param[in] request The rpc GetMetaInfoReq protobuf.
     * @param[out] resp The rpc GetMetaInfoRsp protobuf.
     */
    Status GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp);

private:
    /**
     * @brief Check whether the request meets the conditions. If yes, return the result. If not, add a subscription
     * request.
     * @param[in] request The device request which to return.
     * @param[in] doneRequestCallBack Callback function for replying to a done request.
     * @param[in] subTimeoutMs Subscription time of the request. If the user has set the subscription time, the
     * configured value is used. Otherwise, the remaining time of the request is calculated as the subscription
     * time.
     * @return Status of the call.
     */
    template <typename RequestType>
    Status DirectRespOrAddTimer(const std::shared_ptr<RequestType> &request,
                                std::function<Status(std::shared_ptr<RequestType>)> doneRequestCallBack,
                                const int64_t subTimeoutMs = -1)
    {
        RETURN_OK_IF_TRUE(request->isReturn_);
        // Check whether the result is found. If yes, the result can be directly returned to client.
        int64_t subTimeout = (subTimeoutMs == -1) ? reqTimeoutDuration.CalcRemainingTime() : subTimeoutMs;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::IsInNonNegativeInt32(subTimeout), K_RUNTIME_ERROR,
                                             "SubTimeout is out of range.");
        int64_t remainingTimeMs = reqTimeoutDuration.CalcRealRemainingTime();
        if (request->numSatisfiedObjects_ == request->numWaitingObjects_ || subTimeout == 0 || remainingTimeMs <= 0) {
            VLOG(1) << "The satisfied objects num: " << request->numSatisfiedObjects_
                    << ", the waiting objects num: " << request->numWaitingObjects_
                    << ", the sub timeout: " << subTimeout;
            return doneRequestCallBack(request);
        }
        // If no result is found, add a subscription request.
        TimerQueue::TimerImpl timer;
        const auto &traceID = Trace::Instance().GetTraceID();
        RETURN_IF_NOT_OK(TimerQueue::GetInstance()->AddTimer(
            std::min<int64_t>(subTimeout, remainingTimeMs),
            [this, request, doneRequestCallBack, traceID, subTimeout]() {
                TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceID);
                LOG(ERROR) << "The device request times out, the sub timeoutMs: " << subTimeout
                           << ", clientId: " << request->clientId_ << ", deviceId: " << request->deviceId_
                           << ", satisfied num: " << request->numSatisfiedObjects_
                           << ", waiting num: " << request->numWaitingObjects_;
                (void)doneRequestCallBack(request);
            },
            timer));
        request->timer_ = std::make_unique<TimerQueue::TimerImpl>(timer);
        return Status::OK();
    }

    /**
     * @brief select directory location wehn get
     * @param[out] objDirectory directory of object
     * @param[in] objectKey The object key.
     * @param[in] subseq the sub proto request
     * @param[in] deviceLocation the location of device
     # @return true if successfull, false otherwise
     */
    bool SelectDirLocation(std::shared_ptr<ObjectDirectory> objDirectory, const std::string &objectKey,
                           const DeviceObjectMetaPb &subReq, DeviceLocationPb *&deviceLocation);

    /**
     * @brief Update source npu pending get requests.
     * @param[in] srcClientId The source client index.
     * @param[in] srcDeviceId The source device index.
     * @param[in] dstClientId The destination client id.
     * @param[in] dstDeviceId The destination device index.
     * @param[in] objectKey The object key.
     * @param[in] isSameNode Whether the transmit end and receive end are on the same node.
     * @param[in] srcOffset  The starting offset in the source data
     * @param[in] length The data length to get.
     */
    void UpdateNpuPendingGetReqsTable(const std::string &srcClientId, int32_t srcDeviceId,
                                      const std::string &dstClientId, int32_t dstDeviceId, const std::string &objectKey,
                                      bool isSameNode, int32_t srcOffset, int32_t length);

    /**
     * @brief Notify npu event subscribe.
     * @param[in] srcNpuId The source npu id.
     * @param[in] objectKey The object key.
     */
    void NotifyNpuEventSubscribe(const std::string &srcNPUId, const std::string &objectKey);

    /**
     * @brief Process single GetP2pMeta sub request.
     * @param[in] subReq The sub request.
     * @param[out] resp The GetP2PMetaRespPb.
     * @return Status of requests
     */
    Status ProcessSingleGetP2PMetaReq(const DeviceObjectMetaPb &subReq, GetP2PMetaRspPb &resp);

    std::shared_ptr<ObjectDirectoryTable> objectDirectoryTable_{ nullptr };
    std::shared_ptr<ObjectGetDataInfosReqSubscriptionTable> objectGetDataInfosReqSubscriptionTable_{ nullptr };
    std::shared_ptr<ObjectGetP2PMetaReqSubscriptionTable> objectGetP2PMetaReqSubscriptionTable_{ nullptr };

    std::shared_ptr<NpuEventsTable> npuEventsTable_{ nullptr };
    std::shared_ptr<NpuEventsSubscriptionTable> npuEventsSubscriptionTable_{ nullptr };

    std::shared_ptr<HcclRootInfoTable> rootInfoTable_{ nullptr };
    std::shared_ptr<HcclRootInfoSubscriptionTable> rootInfoSubscriptionTable_{ nullptr };

    std::shared_ptr<MasterDevClientMetaManager> deviceMetaOpRecordTable_{ nullptr };

    std::shared_ptr<HcclRelationshipTable> commRelationMap_{ nullptr };

    std::shared_ptr<TbbLockTable> objectKeyLockTable_ {nullptr};

    std::shared_ptr<MasterDevDeadLockManager> masterDevDeadLockManager_ {nullptr};
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_DEVICE_MASTER_DEV_OC_MANAGER_H
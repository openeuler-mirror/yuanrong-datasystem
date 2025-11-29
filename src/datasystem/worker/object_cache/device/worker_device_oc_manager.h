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
 * Description: Code to manage device object.
 */

#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_OC_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_OC_MANAGER_H

#include <memory>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_base.h"
#include "datasystem/common/object_cache/safe_table.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/worker/object_cache/device/worker_dev_req_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {
class WorkerOCServiceImpl;
class WorkerDeviceOcManager {
public:
    WorkerDeviceOcManager(WorkerOCServiceImpl *impl);

    /**
     * @brief Handle PublishDeviceObject request from the client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status PublishDeviceObject(const std::string &devObjectKey, const PublishDeviceObjectReqPb &req,
                               const std::vector<RpcMessage> &payload);

    /**
     * @brief Send create device meta request to master.
     * @param[in] objectKV The key-value of the device object.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateDeviceMetaToMaster(const ObjectKV &objectKV);

    /**
     * @brief Set a empty device object.
     * @param[out] entry The device object.
     */
    void SetEmptyDeviceObject(SafeObjType &entry);

    /**
     * @brief Process the get device object request.
     * @param[in] objectKeys The device object keys need to get.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @param[in] subTimeout The device get request timeout for subscribe.
     * @param[in] clientId The client id.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessGetDeviceObjectRequest(
        const std::vector<std::string> &objectKeys,
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDeviceObjectRspPb, GetDeviceObjectReqPb>> serverApi,
        int64_t subTimeout, const ClientKey &clientId);

    /**
     * @brief Get one device object from local memory.
     * @param[in] objectKey The device object key.
     * @param[out] request The GetDeviceObjectRequest.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     * @return Status of the call
     */
    Status GetDeviceObjectInLocalMem(const std::string &objectKey, std::shared_ptr<GetDeviceObjectRequest> &request,
                                     std::vector<std::string> &objectsNeedGetRemote);

    /**
     * @brief Get one device object from local.
     * @param[out] request The GetDeviceObjectRequest.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     * @return Status of the call
     */
    void TryGetDeviceObjectFromLocal(std::shared_ptr<GetDeviceObjectRequest> &request,
                                     std::vector<std::string> &objectsNeedGetRemote);

    /**
     * @brief Get one device object from remote.
     * @param[in] subTimeout The device get request timeout for subscribe.
     * @param[out] request The GetDeviceObjectRequest.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     * @return Status of the call
     */
    Status TryGetDeviceObjectFromRemote(const int64_t subTimeout, std::shared_ptr<GetDeviceObjectRequest> &request,
                                        std::vector<std::string> &objectsNeedGetRemote);

    /**
     * @brief Update request info after object sealed.
     * @param[in] objectKV The safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status UpdateRequestForSuccess(const ObjectKV &objectKV);

    /**
     * @brief Processes the request for getting p2p metadata from master.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The device GetP2PMeta request.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessGetP2PMetaRequest(
        GetP2PMetaReqPb &req,
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetP2PMetaRspPb, GetP2PMetaReqPb>> &serverApi);

    /**
     * @brief Processes the request for subscribing the p2p receiving event.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The device GetP2PMeta request.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessSubscribeReceiveEventRequest(
        SubscribeReceiveEventReqPb &req,
        std::shared_ptr<ServerUnaryWriterReader<SubscribeReceiveEventRspPb, SubscribeReceiveEventReqPb>> &serverApi);

    /**
     * @brief Processes the request for receiving root info from master.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The device RecvRootInfo request.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessRecvRootInfoRequest(
        RecvRootInfoReqPb &req,
        std::shared_ptr<ServerUnaryWriterReader<RecvRootInfoRspPb, RecvRootInfoReqPb>> &serverApi);

    /**
     * @brief Processes the request for GetDataInfo.
     * @param[in] req The rpc req protobuf.
     * @param[in] serverApi The unary writer reader.
     * @param[in] subTimeout The timeout for subscribe of the GetDataInfo request.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessGetDataInfoRequest(
        GetDataInfoReqPb &req,
        std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetDataInfoRspPb, GetDataInfoReqPb>> &serverApi,
        const int64_t subTimeout);

private:
    friend WorkerOCServiceImpl;
    WorkerOCServiceImpl *workerOcImpl_;
    WorkerDevReqManager deviceReqManager_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_OC_MANAGER_H
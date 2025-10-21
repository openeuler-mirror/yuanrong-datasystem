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
 * Description: Defines the class to manager device get request.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_WORKER_DEV_REQ_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_DEVICE_WORKER_DEV_REQ_MANAGER_H

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/object_posix.service.rpc.pb.h"
#include "datasystem/worker/object_cache/device/device_obj_cache.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/worker_request_manager.h"

namespace datasystem {
namespace object_cache {

struct DeviceObjEntryParams {
    static std::shared_ptr<DeviceObjEntryParams> ConstructDeviceObjEntryParams(SafeObjType &safeObj)
    {
        return std::make_shared<DeviceObjEntryParams>(DeviceObjEntryParams{ .dataSize = safeObj->GetDataSize(),
                                                                            .metaSize = safeObj->GetMetadataSize(),
                                                                            .objectMode = safeObj->modeInfo,
                                                                            .objectState = safeObj->stateInfo,
                                                                            .shmUnit = safeObj->GetShmUnit(),
                                                                            .isPublished = safeObj->IsPublished() });
    }

    const uint64_t dataSize;
    const uint64_t metaSize;
    const ModeInfo objectMode;
    const StateInfo objectState;
    std::shared_ptr<ShmUnit> shmUnit;
    const bool isPublished;
};

using GetDeviceObjectRequest = UnaryRequest<GetDeviceObjectReqPb, GetDeviceObjectRspPb, DeviceObjEntryParams>;

class WorkerDevReqManager {
public:
    WorkerDevReqManager() = default;

    ~WorkerDevReqManager() = default;

    /**
     * @brief Add request to WorkerRequestManager.
     * @param[in] objectKey The object key.
     * @param[in] request The request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddRequest(const std::string objectKey, std::shared_ptr<GetDeviceObjectRequest> &request);

    /**
     * @brief Update request info after object sealed.
     * @param[in] objectKV The safe object and its corresponding objectKey.
     * @param[in,out] memoryRefApi The memory refCnt table.
     * @return Status of the call.
     */
    Status UpdateRequestForSuccess(const ObjectKV &objectKV);

    /**
     * @brief Update request info after object process failed.
     * @param[in] objectKey The object key.
     * @param[in] lastRc The last error.
     * @return Status of the call.
     */
    Status UpdateRequestForFailed(const std::string objectKey, const Status &lastRc);

    /**
     * @brief Reply to client with the device get request.
     * @param[in] req The request which to return.
     * @param[in] lastRc The last error.
     * @return Status of the call.
     */
    Status ReturnFromGetDeviceObjectRequest(std::shared_ptr<GetDeviceObjectRequest> req, Status lastRc = Status::OK());

    /**
     * @brief Check if the device object is in getting object.
     * @param[in] objectKey Device object key.
     * @return True if device object is in getting.
     */
    bool IsInGettingObject(const std::string &objectKey)
    {
        return requestTable_.ObjectInRequest(objectKey);
    }

private:
    /**
     * @brief Add entry information to device get response and buffers.
     * @param[in] request The device get request which to return.
     * @param[in] objectKey The device object key.
     * @param[in] param The device object param.
     * @param[out] resp The response which to return
     * @param[out] outPayloads The payload to pass non shm data.
     * @return Status of the call.
     */
    Status AddEntryToGetDeviceObjectResponse(const std::shared_ptr<GetDeviceObjectRequest> &request,
                                             const std::string objectKey, std::shared_ptr<DeviceObjEntryParams> param,
                                             GetDeviceObjectRspPb &resp, std::vector<RpcMessage> &outPayloads);

    /**
     * @brief Update request.
     * @param[in] objectKey The object key.
     * @param[in] entry The device object entry parameter.
     * @param[in] lastRc The last error.
     * @return Status of the call.
     */
    Status UpdateRequestImpl(const std::string &objectKey, std::shared_ptr<DeviceObjEntryParams> entry, Status lastRc);

    /**
     * @brief Remove the request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveRequest(std::shared_ptr<GetDeviceObjectRequest> &request);

    RequestTable<GetDeviceObjectRequest> requestTable_;
};
}  // namespace object_cache
}  // namespace datasystem

#endif

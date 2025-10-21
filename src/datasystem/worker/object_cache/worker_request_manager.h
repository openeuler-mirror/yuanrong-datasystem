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

/**
 * Description: Defines the worker class to communicate with the worker service.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_REQUEST_MANAGER_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_REQUEST_MANAGER_H

#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/object_cache/safe_object.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/request_table.h"
#include "datasystem/protos/worker_object.pb.h"
#include "datasystem/worker/object_cache/obj_cache_shm_unit.h"
#include "datasystem/worker/object_cache/worker_oc_eviction_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"

namespace datasystem {
namespace object_cache {
struct GetObjEntryParams : public OffsetInfo {
    static std::shared_ptr<GetObjEntryParams> Create(SafeObjType &safeObj, uint64_t offset, uint64_t size)
    {
        auto objShmUnit = SafeObjType::GetDerived<ObjCacheShmUnit>(safeObj);
        GetObjEntryParams params;
        params.dataSize = safeObj->GetDataSize();
        params.metaSize = safeObj->GetMetadataSize();
        params.createTime = safeObj->GetCreateTime();
        params.objectMode = objShmUnit->modeInfo;
        params.objectState = objShmUnit->stateInfo;
        params.lifeState = objShmUnit->GetLifeState();
        params.shmUnit = safeObj->GetShmUnit();
        params.isSealed = safeObj->IsSealed();
        params.version = safeObj->GetCreateTime();
        params.readOffset = offset;
        params.readSize = size;
        params.AdjustReadSize(params.dataSize);
        VLOG(1) << "dataSize: " << params.dataSize << ", metaSize: " << params.metaSize
                << ", offset:" << params.readOffset << ", readSize:" << params.readSize;
        return std::make_shared<GetObjEntryParams>(std::move(params));
    }

    uint64_t dataSize;
    uint64_t metaSize;
    uint64_t createTime;
    ModeInfo objectMode;
    StateInfo objectState;
    ObjectLifeState lifeState;
    std::shared_ptr<ShmUnit> shmUnit;
    bool isSealed;
    uint64_t version;
};

using GetRequest = UnaryRequest<GetReqPb, GetRspPb, GetObjEntryParams>;

class WorkerRequestManager {
public:
    WorkerRequestManager() = default;

    ~WorkerRequestManager() = default;

    /**
     * @brief Add request to WorkerRequestManager.
     * @param[in] objectKey The object key.
     * @param[in] request The request that is waiting on the object key.
     * @return Status of the call.
     */
    Status AddRequest(const std::string &objectKey, std::shared_ptr<GetRequest> &request);

    /**
     * @brief Update request info after object sealed.
     * @param[in] objectKV The safe object and its corresponding objectKey.
     * @param[in,out] memoryRefApi The memory refCnt table.
     * @return Status of the call.
     */
    Status UpdateRequestForSuccess(ReadObjectKV &objectKV, std::shared_ptr<SharedMemoryRefTable> &memoryRefApi,
                                   bool isDelayToReturn, const std::shared_ptr<GetRequest> &request = nullptr);

    /**
     * @brief Update request info after object publish.
     * @param[in] objectKV The safe object and its corresponding objectKey.
     * @param[in,out] memoryRefApi The memory refCnt table.
     * @return Status of the call.
     */
    Status UpdateRequestForPublish(ObjectKV &objectKV, std::shared_ptr<SharedMemoryRefTable> &memoryRefApi);

    /**
     * @brief Update request info after object process failed.
     * @param[in] objectKey The object key.
     * @param[in] lastRc The last error.
     * @param[in,out] memRefApi The memory refCnt table.
     * @return Status of the call.
     */
    Status UpdateRequestForFailed(const std::string &objectKey, Status lastRc,
                                  std::shared_ptr<SharedMemoryRefTable> &memRefApi);

    /**
     * @brief Update request info after object process failed.
     * @param[in] objectKey The object key.
     * @param[in] lastRc The last error.
     * @param[in,out] memRefApi The memory refCnt table.
     * @return Status of the call.
     */
    Status UpdateSpecificRequestForFailed(const std::shared_ptr<GetRequest> &request, const std::string &objectKey,
                                          Status lastRc, std::shared_ptr<SharedMemoryRefTable> &memRefApi);

    /**
     * @brief Reply to client with the get request.
     * @param[in] req The request which to return.
     * @param[in,out] memoryRefApi The memory refCnt table.
     * @param[in] lastRc The last error.
     * @return Status of the call.
     */
    Status ReturnFromGetRequest(std::shared_ptr<GetRequest> req, std::shared_ptr<SharedMemoryRefTable> &memoryRefApi,
                                Status lastRc = Status::OK());

    /**
     * @brief Set DeleteObject function for deleting local cache when object is from other AZ.
     * @param[in] deleteFunc The delete object function.
     */
    static void SetDeleteObjectsFunc(std::function<Status(const std::string &, uint64_t)> deleteFunc);

    /**
     * @brief Check and return to client when request finished or object finish
     * @param[in] objectKey object key
     * @param[in] memoryRefApi The memory refCnt table.
     * @return Status of the call
     */
    void CheckAndReturnToClient(const std::string objectKey, std::shared_ptr<SharedMemoryRefTable> &memoryRefApi);

    /**
     * @brief Check if the object is in getting object.
     * @param[in] objectKey Object key.
     * @return True if object is in getting.
     */
    bool IsInGettingObject(const std::string &objectKey);

private:
    /**
     * @brief Add entry information to get response and buffers.
     * @param[in] request The request which to return.
     * @param[in,out] retIdEntry The id and shared memory unit of the object.
     * @param[out] resp The response which to return.
     * @param[out] outPayloads The buffers for non-shm passing.
     * @param[in,out] memoryRefApi The memory refCnt table.
     * @return Status of the call.
     */
    static Status AddEntryToGetResponse(const std::shared_ptr<GetRequest> &request,
                                        const std::pair<std::string *, std::shared_ptr<GetObjEntryParams>> &retIdEntry,
                                        GetRspPb &resp, std::vector<RpcMessage> &outPayloads,
                                        std::shared_ptr<SharedMemoryRefTable> &memoryRefApi,
                                        std::map<std::string, uint64_t> &needDeleteObjects);

    /**
     * @brief Remove the request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveGetRequest(std::shared_ptr<GetRequest> &request);

    /**
     * @brief Initialize an ObjCacheShmUnit, give it default value.
     * @param[in] objectKey The object key that responds to the request
     * @param[out] info The objectInfoPb need to init
     */
    static void SetDefaultObjectInfoPb(const std::string &objectKey, GetRspPb::ObjectInfoPb &info);

    /**
     * @brief Set objectInfoPb response
     * @param[in] objectKey The object key that responds to the request
     * @param[in] safeEntry The safe object entry
     * @param[out] info The objectInfoPb need to init
     */
    static void SetObjectInfoPb(const std::string &objectKey, GetObjEntryParams &safeEntry,
                                GetRspPb::ObjectInfoPb &info);

    /**
     * @brief Set PayloadInfoPb response
     * @param[in] objectKey The object key that responds to the request
     * @param[in] safeEntry The safe object entry
     * @param[out] info The PayloadInfoPb need to init
     */
    static void SetPayloadInfoPb(const std::string &objectKey, GetObjEntryParams &safeEntry,
                                 GetRspPb::PayloadInfoPb &info);

    /**
     * @brief Update request info after object sealed.
     * @param[in] objectKey The object key.
     * @param[in] entry The object entry parameter.
     * @param[in,out] memoryRefApi The memory refCnt table.
     * @param[in] lastRc The last error.
     * @param [in] isDelayToReturn if true, return After get request.
     * @return Status of the call.
     */
    Status UpdateRequestImpl(const std::string &objectKey, std::shared_ptr<GetObjEntryParams> entry,
                             std::shared_ptr<SharedMemoryRefTable> &memoryRefApi, Status lastRc = Status::OK(),
                             const std::shared_ptr<GetRequest> &request = nullptr, bool isDelayToReturn = false);

    /*
     * @brief Add entry information to get response and pageloads.
     * @param[in] retIdEntry The id and shared memory unit of the object.
     * @param[out] resp The response which to return.
     * @param[out] outPayloads The buffers for non-shm passing.
     * @return Status of the call.
     */
    static Status CopyShmUnitToPayloads(const std::pair<std::string *, std::shared_ptr<GetObjEntryParams>> &retIdEntry,
                                        GetRspPb &resp, std::vector<RpcMessage> &outPayloads);

    /*
     * @brief Construct GetRsp.
     * @param[in] req The request which to return.
     * @param[in] totalSize The size of objects.
     * @param[in] lastRc The last error.
     * @param[in] memoryRefApi The memory refCnt table.
     * @param[in] resp GetRsp.
     * @param[in] payloads The buffers for non-shm passing.
     */
    void ConstructGetRsp(std::shared_ptr<GetRequest> &req, uint64_t &totalSize, Status &lastRc,
                         std::shared_ptr<SharedMemoryRefTable> &memoryRefApi, GetRspPb &resp,
                         std::vector<RpcMessage> &payloads, std::map<std::string, uint64_t> &needDeleteObjects);

    /**
     * @brief Delete objects according to object key and version.
     * @param[in] objects The object info.
     */
    static void DeleteObjects(std::map<std::string, uint64_t> &objects);

    RequestTable<GetRequest> requestTable_;
    static std::function<Status(const std::string &, uint64_t)> deleteFunc_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_REQUEST_MANAGER_H

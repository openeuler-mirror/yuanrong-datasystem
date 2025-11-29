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
#include "datasystem/common/object_cache/object_base.h"
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
struct GetObjEntryParams {
    static std::unique_ptr<GetObjEntryParams> Create(const std::string &objectKey, SafeObjType &safeObj)
    {
        auto objShmUnit = SafeObjType::GetDerived<ObjCacheShmUnit>(safeObj);
        auto params = std::make_unique<GetObjEntryParams>();
        params->dataSize = safeObj->GetDataSize();
        params->metaSize = safeObj->GetMetadataSize();
        params->createTime = safeObj->GetCreateTime();
        params->objectMode = objShmUnit->modeInfo;
        params->objectState = objShmUnit->stateInfo;
        params->lifeState = objShmUnit->GetLifeState();
        params->shmUnit = safeObj->GetShmUnit();
        params->isSealed = safeObj->IsSealed();
        params->version = safeObj->GetCreateTime();
        VLOG(1) << "Create GetObjEntryParams for objectKey " << objectKey << ", dataSize: " << params->dataSize
                << ", metaSize: " << params->metaSize;
        return params;
    }

    std::unique_ptr<GetObjEntryParams> Clone() const
    {
        auto params = std::make_unique<GetObjEntryParams>();
        params->dataSize = dataSize;
        params->metaSize = metaSize;
        params->createTime = createTime;
        params->objectMode = objectMode;
        params->objectState = objectState;
        params->lifeState = lifeState;
        params->shmUnit = shmUnit;
        params->isSealed = isSealed;
        params->version = version;
        return params;
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

struct GetObjInfo {
    OffsetInfo offsetInfo;
    std::unique_ptr<GetObjEntryParams> params;
    Status rc;
    bool isRollBack = false;
    bool NotFound() const
    {
        return params == nullptr && rc.IsOk();
    }
};

using ObjectKey = std::string;
class WorkerRequestManager;
class GetRequest : public std::enable_shared_from_this<GetRequest> {
public:
    GetRequest(AccessRecorderKey key) noexcept : recorder_(key){};
    /**
     * @brief Init GetRequst
     * @param[in] tenantId The tenantId.
     * @param[in] req The GetReqPb instance.
     * @param[in] shmRefTable The instance of SharedMemoryRefTable.
     * @param[in] api The instance of server api.
     * @param[in] threadPool The thread pool for GET requests.
     * @param[in] clientId The client id.
     * @return Status of this call.
     */
    Status Init(const std::string &tenantId, const GetReqPb &req, std::shared_ptr<SharedMemoryRefTable> shmRefTable,
                std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> api,
                std::shared_ptr<ThreadPool> threadPool, const ClientKey &clientId);

    /**
     * @brief Update GetRequst according to local get result, return to client if all data has been obtained
     * @param[in] rc The status of local get.
     * @param[in] remoteObjectCount The object count need get from remote.
     * @return Status of this call.
     */
    Status UpdateAfterLocalGet(Status rc, size_t remoteObjectCount);

    /**
     * @brief Mark object get success, should be called in remote get logic.
     * @param[in] objectKey The object key.
     * @param[in] safeObj The instance reference of SafeObjType
     * @return Status of this call.
     */
    Status MarkSuccess(const ObjectKey &objectKey, SafeObjType &safeObj);

    /**
     * @brief Mark object get failed, should be called in remote get logic.
     * @param[in] objectKey The object key.
     * @param[in] rc The failed reason.
     * @return Status of this call.
     */
    Status MarkFailed(const ObjectKey &objectKey, const Status &rc);

    /**
     * @brief Mark object get success and try return to client if all data has been obtained, should be called when
     * object publish after the get reqeust happend.
     * @param[in] objectKey The object Key.
     * @param[in] params The instance of GetObjEntryParams.
     * @return Status of this call.
     */
    Status MarkSuccessForNotify(const ObjectKey &objectKey, std::unique_ptr<GetObjEntryParams> params);

    /**
     * @brief Response to client.
     * @param[in] rc The final status return to client.
     * @return Status of this call.
     */
    Status ReturnToClient(const Status &rc = Status::OK());

    /**
     * @brief Register Current GetRequest instance to WorkerRequestManager
     * @param[in] workerRequestManager The point of WorkerRequestManager.
     */
    void Register(WorkerRequestManager *workerRequestManager);

    /**
     * @brief Unregister Current GetRequest instance from WorkerRequestManager.
     */
    void UnRegister();

    /**
     * @brief Set the timer instance
     * @param[in] timer The instance of timer.
     */
    void SetTimer(std::unique_ptr<TimerQueue::TimerImpl> timer);

    const std::vector<ObjectKey> &GetRawObjectKeys() const;
    std::unordered_map<ObjectKey, GetObjInfo> &GetObjects();
    void SetStatus(const Status &rc);
    size_t GetReadyCount() const;
    size_t GetNotReadyCount() const;
    bool AlreadyReturn() const;
    const std::string &GetClientId() const;
    bool NoQueryL2Cache() const;

    std::vector<ObjectKey> GetUniqueObjectkeys() const;
    std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> GetServerApi() const;

private:
    Status MarkSuccessImpl(const ObjectKey &objectKey, std::unique_ptr<GetObjEntryParams> params);
    Status ConstructResponse(uint64_t &totalSize, GetRspPb &resp, std::vector<RpcMessage> &payloads,
                             std::map<std::string, uint64_t> &needDeleteObjects);

    Status AddObjectToResponse(const ObjectKey &objectKeyUri, GetObjInfo &objectInfo, size_t index, bool shmEnable,
                               GetRspPb &resp, std::vector<RpcMessage> &outPayloads);

    void SetShmObjectInfoPb(const ObjectKey &objectKeyUri, size_t objectIndex, GetObjEntryParams &safeEntry,
                                   GetRspPb::ObjectInfoPb &info);

    void SetNoShmObjectInfoPb(const ObjectKey &objectKeyUri, size_t objectIndex, const GetObjInfo &objectInfo,
                                     GetRspPb::PayloadInfoPb &info);
    void SetDefaultObjectInfoPb(const ObjectKey &objectKeyUri, size_t objectIndex, GetRspPb::ObjectInfoPb &info);
    bool Registered() const;

    // the mutex protect GetObjInfo and lastRc_
    // Only after the GetRequest instance is registered with the WorkerRequestManager can other threads become aware of
    // it; therefore, accessing data in objects_ before registration requires no lock protection
    std::mutex mutex_;
    std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> serverApi_;
    std::shared_ptr<SharedMemoryRefTable> shmRefTable_;
    ClientKey clientId_;
    std::vector<ObjectKey> rawObjectKeys_;
    std::unordered_map<ObjectKey, GetObjInfo> objects_;
    std::atomic<size_t> readyCount_{ 0 };
    AccessRecorder recorder_;
    Status lastRc_;
    std::atomic<bool> isReturn_{ false };

    int64_t subTimeout_{ 0 };
    WorkerRequestManager *workerRequestManager_{ nullptr };
    std::unique_ptr<TimerQueue::TimerImpl> timer_;
    bool noQueryL2Cache_ = false;
    bool enableReturnObjectIndex_ = false;
    std::shared_ptr<ThreadPool> threadPool_;
};

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
     * @brief Remove the request from the waiting requests table.
     * @param[in] request The request need to remove.
     */
    void RemoveGetRequest(const std::shared_ptr<GetRequest> &request);

    /**
     * @brief Update request info after object sealed.
     * @param[in] objectKV The safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status NotifyPendingGetRequest(ObjectKV &objectKV);

    /**
     * @brief Set DeleteObject function for deleting local cache when object is from other AZ.
     * @param[in] deleteFunc The delete object function.
     */
    static void SetDeleteObjectsFunc(std::function<Status(const std::string &, uint64_t)> deleteFunc);

    /**
     * @brief Delete objects according to object key and version.
     * @param[in] objects The object info.
     */
    static void DeleteObjects(const std::map<std::string, uint64_t> &objects);

private:
    static std::function<Status(const std::string &, uint64_t)> deleteFunc_;
    RequestTable<GetRequest> requestTable_;
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_WORKER_REQUEST_MANAGER_H

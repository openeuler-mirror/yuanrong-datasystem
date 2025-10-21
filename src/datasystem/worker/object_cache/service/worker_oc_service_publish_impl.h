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
 * Description: Defines the worker service processing publish process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_PUBLISH_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_PUBLISH_IMPL_H

#include <vector>
#include <future>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/object_cache/object_bitmap.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/async_send_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServicePublishImpl : public WorkerOcServiceCrudCommonApi {
public:
    /**
     * @brief Construct WorkerOcServicePublishImpl.
     * @param[in] initParam The parameter used to init WorkerOcServiceCrudCommonApi.
     * @param[in] etcdCM The cluster manager pointer to assign.
     * @param[in] memCpyThreadPool Used to copy data to memory.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] localAddress The local worker address.
     */
    WorkerOcServicePublishImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
        std::shared_ptr<ThreadPool> memCpyThreadPool, std::shared_ptr<AkSkManager> akSkManager, HostPort &localAddress);

    /**
     * @brief Handle Put/Publish/Seal request from the client.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status Publish(const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> &payloads);

private:
    struct PublishParams {
        const ObjectLifeState lifeState;
        const std::vector<std::string> &nestedObjectKeys;
        bool isRetry = false;
        uint32_t ttlSecond;
        int existence;
        CacheType cacheType;
    };

    /**
     * @brief The implementation of Publish.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] payloads The rpc request payload.
     * @return K_OK on success; the error code otherwise.
     */
    Status PublishImpl(const PublishReqPb &req, PublishRspPb &resp, std::vector<RpcMessage> &payloads);

    /**
     * @brief Reserve/get and lock a object from ObjectTable and publish/seal it.
     * @param[in] objectKey The object key.
     * @param[in] req Publish request meta.
     * @param[in] nestedObjectKeys Objects that depend on objectKey.
     * @param[in] payloads The object data to be saved.
     * @param[out] future Async send future if write back to L2 cache, can't get during entry locked.
     * @return Status of the call.
     */
    Status PublishObjectWithLock(const std::string &objectKey, const PublishReqPb &req,
                                 const std::vector<std::string> &nestedObjectKey, std::vector<RpcMessage> &payloads,
                                 std::future<Status> &future);

    /**
     * @brief Verify the validity of the object release.
     * @param[in] req Publish request meta.
     * @param[in] safeObj The object to be sealed.
     * @return Status of the call.
     */
    static Status VertifyObjectReleaseValidity(const PublishReqPb &req, const SafeObjType &safeObj);

    /**
     * @brief Prepare entry for immediate PublishObject call. Include set/update object entry,
     * allocate/get shared memory, and set params in entry.
     * @param[in] req Publish request meta.
     * @param[in/out] objectKV The object to be sealed and its corresponding objectKey.
     * @return Status of the call.
     */
    Status PrepareForPublish(const PublishReqPb &req, ObjectKV &objectKV);

    /**
     * @brief Create a new object metadata to master.
     * @param[in] objectKV The object to be sealed and its corresponding objectKey.
     * @param[in] params The params of Publish request.
     * @param[out] version The new version from master.
     * @return Status of the call.
     */
    Status CreateMetadataToMaster(const ObjectKV &objectKV, const PublishParams &params, uint64_t &version);

    /**
     * @brief Updates the metadata of a specified object in the master.
     * @param[in] objectKV The object to be published, and its corresponding objectKey.
     * @param[in] params The params of Publish request.
     * @param[out] version The new version from master.
     * @return Status of the call.
     */
    Status UpdateMetadataToMaster(const ObjectKV &objectKV, const PublishParams &params, uint64_t &version);

    /**
     * @brief Create or update metadata to master, object will be unlocked during requesting master.
     * @param[in] objectKV Safe object entry and its corresponding objectKey.
     * @param[in] params Publishing parameters.
     * @return Status of the call.
     */
    Status RequestingToMaster(ObjectKV &objectKV, const PublishParams &params);

    /**
     * @brief Rollback after publishing failure
     * @param[in] objectKV Safe object entry and its corresponding objectKey.
     * @param[in] oldLifeState The old life state of object.
     * @param[in] newLifeState The new life state of object.
     * @return Status of the call.
     */
    Status RollbackPublishFailure(ObjectKV &objectKV, ObjectLifeState oldLifeState, ObjectLifeState newLifeState);

    /**
     * @brief Publish a newly created or updated object. This function will publish entry and save data to cache.
     * @param[in/out] objectKV The object to be sealed and its corresponding id.
     * @param[in] params Publish parameters.
     * @param[out] payloads Payloads for non-shared-memory cases.
     * @return Status of the call.
     */
    Status PublishObject(ObjectKV &objectKV, const PublishParams &params, std::vector<RpcMessage> &payloads);

    /**
     * @brief Try to delete Obj from Eviction list and spill file.
     * @param[in] objectKV Safe object entry and its corresponding objectKey.
     * @param[in] isInsert Indicates whether this object were inserted.
     * @return Status of the call.
     */
    Status TryDeleteObjFromEvictionAndSpillFile(ObjectKV &objectKV, bool isInsert);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_ptr<ThreadPool> memCpyThreadPool_{ nullptr };

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    HostPort &localAddress_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_PUBLISH_IMPL_H

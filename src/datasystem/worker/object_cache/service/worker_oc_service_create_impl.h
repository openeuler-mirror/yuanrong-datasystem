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
 * Description: Defines the worker service processing create buffer process.
 */

#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_CREATE_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_CREATE_IMPL_H

#include "datasystem/object/object_enum.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServiceCreateImpl : public WorkerOcServiceCrudCommonApi {
public:
    WorkerOcServiceCreateImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                              std::shared_ptr<AkSkManager> akSkManager);
    /**
     * @brief Create a new object, allocate memory and return the pointer. for shm use only.
     * @param[in] req The rpc request protobuf
     * @param[out] resp The rpc response protobuf
     * @return K_OK on success; the error code otherwise.
     *         K_DUPLICATED: the object already exists, no need to create.
     */
    Status Create(const CreateReqPb &req, CreateRspPb &resp);

    /**
     * @brief Create multiple objects
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return Status of the call
     */
    Status MultiCreate(const MultiCreateReqPb &req, MultiCreateRspPb &resp);

private:
    /**
     * @brief The implementation of Create.
     * @param[in] req The rpc request protobuf
     * @param[out] resp The rpc response protobuf
     * @param[in] cacheType The type of cache.
     * @return K_OK on success; the error code otherwise.
     *         K_DUPLICATED: the object already exists, no need to create.
     */
    Status CreateImpl(const std::string &tenantId, const std::string &clientId, const std::string &rawObjectKey,
                      size_t dataSize, CreateRspPb &resp, CacheType cacheType = CacheType::MEMORY);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::atomic<uint64_t> shmIdCounter{0};
    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_CREATE_IMPL_H
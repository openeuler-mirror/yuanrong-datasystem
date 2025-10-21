/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Defines the worker service Expire process, for set expiration time.
 */
#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_EXPIRE_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_EXPIRE_IMPL_H

#include "datasystem/utils/status.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/object_posix.service.rpc.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServiceExpireImpl : public WorkerOcServiceCrudCommonApi {
public:
    WorkerOcServiceExpireImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM,
                              std::shared_ptr<AkSkManager> akSkManager);

    /**
     * @brief Set expiration time for metas by objectKeys.
     * @param[in] req The expire request protobuf.
     * @param[in] rsp The expire response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status Expire(const ExpireReqPb &req, ExpireRspPb &rsp);

private:
    /**
     * @brief Try to set expiration time for keys from other AZ.
     * @param[in] objectKeys The objKey which need to be tried to get metadata from other AZ.
     * @param[in] ttlSeconds TTL in seconds.
     * @param[in] absentObj The objects whose metadata fails to be queried.
     * @param[in] objExpireFailed The objects failed to expire.
     * @param[out] rsp The expire response protobuf.
     */
    Status TryExpireObjKeyFromOtherAZ(std::unordered_set<std::string> objectKeys, uint32_t ttlSeconds,
                                      std::vector<std::string> &absentObj,
                                      std::unordered_set<std::string> &objExpireFailed, ExpireRspPb &rsp);

    /**
     * @brief Set expiration time of the specified objects in the master.
     * @param[in] objectKeys The keys to set expiration for.
     * @param[in] masterAddr The dest master hostPort.
     * @param[in] ttlSeconds TTL in seconds.
     * @param[out] absentObj The objects whose metadata fails to be queried.
     * @param[out] objExpireFailed The objects failed to expire.
     * @param[out] rsp The expire response protobuf.
     */
    Status ExpireFromMaster(std::vector<std::string> objectKeys, HostPort masterAddr, uint32_t ttlSeconds,
                            std::vector<std::string> &absentObj, std::unordered_set<std::string> &objExpireFailed,
                            ExpireRspPb &rsp);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    std::shared_ptr<ThreadPool> batchExpireThreadPool_{ nullptr };

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    std::vector<std::string> otherAZNames_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_EXPIRE_IMPL_H

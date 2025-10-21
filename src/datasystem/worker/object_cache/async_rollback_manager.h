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
 * Description: Defines AsyncRollbackManager Interface.
 */
#ifndef DATASYSTEM_ASYNC_ROLLBACK_MANAGER_H
#define DATASYSTEM_ASYNC_ROLLBACK_MANAGER_H

#include <shared_mutex>
#include <unordered_set>
#include "datasystem/common/util/queue/blocking_queue.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/worker_master_oc_api.h"

namespace datasystem {
namespace object_cache {

class AsyncRollbackManager {
public:
    /**
     * @brief Construct AsyncRollbackManager.
     */
    AsyncRollbackManager()
    {
    }

    /**
     * @brief Deconstruct AsyncRollbackManager.
     */
    ~AsyncRollbackManager();

    /**
     * @brief Initialize the AsyncRollbackManager.
     * @param[int] localAddress The address of local worker.
     * @param[in] apiManager The manager of worker master api.
     * @param[in] etcdCM The cluster manager pointer to assign.
     * @return Status of the call.
     */
    void Init(HostPort &localAddress,
              std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager,
              EtcdClusterManager *etcdCM);

    /**
     * @brief Add multiple object to AsyncRollbackManager.
     * @param[in] objectKeys The ID of the objects need to rollback asynchronously.
     */
    void AddBatch(const std::vector<std::string> &objectKeys)
    {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        pendingObject_.insert(objectKeys.begin(), objectKeys.end());
        emptyCond_.notify_one();
    }

    /**
     * @brief Check if one of the objects is in the rollback queue.
     * @param[in] objectKeys The objectKeys to be checked.
     * @return true At least one object is in the rollback queue.
     */
    bool IsObjectsInRollBack(const std::vector<std::string> &objectKeys)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        for (const auto &obj : objectKeys) {
            if (pendingObject_.find(obj) != pendingObject_.end()
                || processingObject_.find(obj) != processingObject_.end()) {
                return true;
            }
        }
        return false;
    }

private:
    /**
     * @brief Rollback thread responsible for rollback metadata of pendingObject_.
     */
    void Rollback();

    /**
     * @brief Rollback to master.
     * @param[in] objs The objectKeys to be rolled back.
     * @param[out] failedObjs The objectKeys of failed rollback.
     */
    void RollbackToMaster(const std::vector<std::string> &objs, std::vector<std::string> &failedObjs);

    Thread thread_;
    std::atomic<bool> running_{ false };
    std::unordered_set<std::string> pendingObject_;
    std::unordered_set<std::string> processingObject_;
    std::shared_mutex mutex_;                // mutex for pendingObject_ and processingObject_.
    std::condition_variable_any emptyCond_;  // Signaled if pendingObject_ is not empty.
    std::shared_ptr<worker::WorkerMasterApiManagerBase<worker::WorkerMasterOCApi>> apiManager_{ nullptr };
    EtcdClusterManager *etcdCM_;
    HostPort localAddress_;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_ASYNC_ROLLBACK_MANAGER_H
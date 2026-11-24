/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
 * Description: Router client for selecting worker.
 */
 
#ifndef DATASYSTEM_ROUTER_CLIENT_H
#define DATASYSTEM_ROUTER_CLIENT_H
 
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
 
#include "datasystem/utils/status.h"
 
namespace datasystem {
class __attribute((visibility("default"))) RouterClient {
public:

    /**
     * @brief Construct a router for the specified worker AZ.
     * @note Applications upgrading from the etcd constructor must rebuild and relink with this SDK.
     * @param[in] azName Worker AZ name.
     * @param[in] coordinatorAddress Coordinator endpoint or comma-separated endpoints.
     */
    RouterClient(const std::string &azName, const std::string &coordinatorAddress);
 
    /**
     * @brief Stop the private Observer/backend chain and release it in reverse ownership order.
     */
    ~RouterClient();
 
    /**
     * @brief Read the first topology from Coordinator and start polling for changes.
     * @return Status of the call.
     */
    Status Init();
 
    /**
     * @brief Return ACTIVE addresses ordered by same-host preference and randomized within groups.
     * @param[in] targetHost Preferred host used only for ordering.
     * @param[out] addresses Ordered topology candidates.
     * @return K_OK or K_NOT_FOUND with an empty output.
     */
    Status GetWorkerCandidates(const std::string &targetHost, std::vector<std::string> &addresses) const;
 
    /**
     * @brief Get the worker address by worker id.
     * @param[in] workerIds The worker ids.
     * @param[out] workerAddrs The worker addresses. Each address is represented as ip:port format. If worker address is
     * not found, represented as empty instead.
     * @return K_OK on any object success. Otherwise K_NOT_FOUND.
     */
    Status GetWorkerAddrByWorkerId(const std::vector<std::string> &workerIds,
                                   std::vector<std::string> &workerAddrs) const;
 
private:
    struct Impl;
    std::string azName_;
    std::string coordinatorAddress_;
    std::mutex lifecycleMutex_;
    // Readers may run during Init. Once published, impl_ lives until destruction.
    std::atomic<bool> initialized_{ false };
    std::unique_ptr<Impl> impl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_ROUTER_CLIENT_H

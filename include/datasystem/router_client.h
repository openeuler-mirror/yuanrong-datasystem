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
 
#include <memory>
#include <mutex>
#include <string>
#include <vector>
 
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
 
namespace datasystem {
class __attribute((visibility("default"))) RouterClient {
public:

    /**
     * @brief Construct RouterClient. If certificate authentication is enabled for the etcd to be connected, must
     * specify etcdCa, etcdCert, etcdKey and etcdNameOverride.
     * @param[in] azName The AZ name of the worker address to be monitored.
     * @param[in] etcdAddress The Etcd address.
     * @param[in] etcdCa Root etcd certificate, optional parameters.
     * @param[in] etcdCert Etcd certificate chain, optional parameters.
     * @param[in] etcdKey Etcd private key, optional parameters.
     * @param[in] etcdDNSName Etcd DNS name, optional parameters.
     */
    RouterClient(const std::string &azName, const std::string &etcdAddress, const SensitiveValue &etcdCa = "",
                 const SensitiveValue &etcdCert = "", const SensitiveValue &etcdKey = "",
                 const std::string &etcdDNSName = "");
 
    /**
     * @brief Stop the private Observer/backend chain and release it in reverse ownership order.
     */
    ~RouterClient();
 
    /**
     * @brief Connects to etcd to obtain the worker address and listens to the change of the specified AZ worker
     * address.
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
    std::string etcdAddress_;
    SensitiveValue etcdCa_;
    SensitiveValue etcdCert_;
    SensitiveValue etcdKey_;
    std::string etcdDNSName_;
    // Protects construction, access, and destruction of impl_.
    mutable std::mutex lifecycleMutex_;
    std::unique_ptr<Impl> impl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_ROUTER_CLIENT_H

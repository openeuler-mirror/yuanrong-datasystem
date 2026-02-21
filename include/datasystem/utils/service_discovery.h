/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: When the SDK does not know which worker to connect to,
 * it can use this feature to obtain an available worker for connection.
 */

#ifndef DATASYSTEM_SERVICE_DISCOVERY_H
#define DATASYSTEM_SERVICE_DISCOVERY_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <map>

#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
class EtcdStore;
class RandomData;
}  // namespace datasystem

namespace datasystem {
class __attribute((visibility("default"))) ServiceDiscovery {
public:
    /**
     * @brief Construct ServiceDiscovery. If certificate authentication is enabled for the etcd to be connected, must
     *        specify etcdCa, etcdCert, etcdKey and etcdNameOverride.
     * @param[in] etcdAddress The Etcd address.
     * @param[in] clusterName The cluster name of the worker address to be monitored.
     * @param[in] etcdCa Root etcd certificate, optional parameters.
     * @param[in] etcdCert Etcd certificate chain, optional parameters.
     * @param[in] etcdKey Etcd private key, optional parameters.
     * @param[in] etcdDNSName Etcd DNS name, optional parameters.
     */
    ServiceDiscovery(const std::string &etcdAddress, const std::string &clusterName = "",
                     const SensitiveValue &etcdCa = "", const SensitiveValue &etcdCert = "",
                     const SensitiveValue &etcdKey = "", const std::string &etcdDNSName = "");

    ~ServiceDiscovery() = default;

    /**
     * @brief Initialize ServiceDiscovery.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Select a worker address.
     * @param[out] workerIp
     * @param[out] workerPort
     * @return Status of the call.
     */
    Status SelectWorker(std::string &workerIp, int &workerPort);

private:
    /**
     * @brief Populate active workers set at startup.
     */
    Status ObtainWorkers();

    std::string etcdAddress_;
    std::string clusterName_;
    SensitiveValue etcdCa_;
    SensitiveValue etcdCert_;
    SensitiveValue etcdKey_;
    std::string etcdDNSName_;
    std::set<std::string> activeWorkerAddrs_;
    std::shared_ptr<RandomData> randomData_;
    // workerHostPortMutext_ is used to protect the read and write of activeWorkerAddrs_.
    mutable std::shared_timed_mutex workerHostPortMutext_;
    // etcdStore_ uses workerHostPortMutext_ and activeWorkerAddrs_, so needs to be destructed first.
    std::shared_ptr<EtcdStore> etcdStore_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_SERVICE_DISCOVERY_H
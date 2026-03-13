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
#include <cstdint>
#include <functional>
#include <memory>
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
const uint32_t DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL = 30;  // seconds

enum class ServiceAffinityPolicy : uint8_t {
    PREFERRED_SAME_NODE = 0,
    REQUIRED_SAME_NODE = 1,
    RANDOM = 2,
};

struct ServiceDiscoveryOptions {
    std::string etcdAddress;
    std::string clusterName = "";

    // TLS - optional
    SensitiveValue etcdCa = "";
    SensitiveValue etcdCert = "";
    SensitiveValue etcdKey = "";
    std::string etcdDNSName = "";

    // Auth - optional
    std::string username = "";
    SensitiveValue password = "";
    uint32_t tokenRefreshIntervalSec = DEFAULT_ETCD_TOKEN_REFRESH_INTERVAL;
    std::string hostIdEnvName = "";
    ServiceAffinityPolicy affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
};

class __attribute((visibility("default"))) ServiceDiscovery {
public:
    /**
     * @brief Construct ServiceDiscovery. If certificate authentication is enabled for the etcd to be connected, must
     *        specify etcdCa, etcdCert, etcdKey and etcdNameOverride.
     * @param[in] opts ServiceDiscoveryOptions.
     */
    ServiceDiscovery(const ServiceDiscoveryOptions &opts);

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
    std::string username_;
    SensitiveValue password_;
    uint32_t tokenRefreshInterval_;
    std::string hostIdEnvName_;
    std::string hostId_;
    ServiceAffinityPolicy affinityPolicy_;
    // key is worker_address, value is worker_id.
    std::unordered_map<std::string, std::string> activeWorkerInfo_;
    std::shared_ptr<RandomData> randomData_;
    // workerHostPortMutext_ is used to protect the read and write of activeWorkerInfo_.
    mutable std::shared_timed_mutex workerHostPortMutext_;
    // etcdStore_ uses workerHostPortMutext_ and activeWorkerInfo_, so needs to be destructed first.
    std::shared_ptr<EtcdStore> etcdStore_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_SERVICE_DISCOVERY_H

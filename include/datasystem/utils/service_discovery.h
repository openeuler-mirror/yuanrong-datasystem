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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

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
     * @param[out] isSameNode If non-null, set to true when the selected worker is on the same node.
     * @return Status of the call.
     */
    Status SelectWorker(std::string &workerIp, int &workerPort, bool *isSameNode = nullptr);

    /**
     * @brief Select a same-node worker address only.
     * @param[out] workerIp
     * @param[out] workerPort
     * @return Status of the call.
     */
    Status SelectSameNodeWorker(std::string &workerIp, int &workerPort);

    /**
     * @brief Return every ready worker ("host:port") visible via etcd keepalive, split by host
     *        affinity. Under REQUIRED_SAME_NODE only same-node workers are returned; under RANDOM
     *        every worker is returned via otherAddrs with sameHostAddrs empty; otherwise same-node
     *        workers go in sameHostAddrs and the rest in otherAddrs.
     * @param[out] sameHostAddrs Addresses of workers whose hostId matches the local hostId.
     * @param[out] otherAddrs    Addresses of all remaining workers.
     * @return Status of the call.
     */
    Status GetAllWorkers(std::vector<std::string> &sameHostAddrs, std::vector<std::string> &otherAddrs);

    /**
     * @brief Get service affinity policy.
     * @return Service affinity policy.
     */
    ServiceAffinityPolicy GetAffinityPolicy() const
    {
        return affinityPolicy_;
    }

    /**
     * @brief Whether host locality is actually active: the configured policy exercises
     *        host affinity (PREFERRED_SAME_NODE or REQUIRED_SAME_NODE) AND hostId has
     *        been resolved. Under RANDOM, or when hostId is missing, this is false and
     *        same-node operations cannot be used.
     * @return True when the client can meaningfully select same-node workers.
     */
    bool HasHostAffinity() const
    {
        return affinityPolicy_ != ServiceAffinityPolicy::RANDOM && !hostId_.empty();
    }

private:
    /**
     * @brief Fetch ready worker addresses from etcd and partition by host affinity. When hostId_
     *        is empty, every worker goes into `other`.
     * @param[out] sameHost Addresses of workers whose hostId matches the local hostId.
     * @param[out] other    Remaining ready worker addresses.
     * @return Status of the call.
     */
    Status ObtainWorkers(std::vector<std::string> &sameHost, std::vector<std::string> &other);

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
    std::shared_ptr<RandomData> randomData_;
    std::shared_ptr<EtcdStore> etcdStore_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_SERVICE_DISCOVERY_H

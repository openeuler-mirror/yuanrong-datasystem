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
 * Description: R0 placement directory snapshot.
 */
#ifndef DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_DIRECTORY_H
#define DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_DIRECTORY_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/topology/routing/placement_types.h"

namespace datasystem {
namespace topology {

struct PlacementDirectorySnapshot {
    PlacementDirectorySnapshot() : localAddress()
    {
    }

    int64_t version = -1;
    std::string localWorkerId;
    HostPort localAddress;
    std::unordered_map<std::string, PlacementEndpoint> workers;
    std::unordered_map<std::string, std::string> workerIdsByAddress;
};

class IPlacementDirectory {
public:
    virtual ~IPlacementDirectory() = default;

    /**
     * @brief Resolve a worker endpoint from the local immutable directory snapshot.
     * @param[in] workerId Worker identity.
     * @param[out] endpoint Resolved endpoint.
     * @return K_OK if found, K_NOT_READY if directory is not published, K_NOT_FOUND if worker is absent.
     *
     * Request threads only read local snapshot state. This method must not perform RPC probing, repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status ResolveWorker(const std::string &workerId, PlacementEndpoint &endpoint) const = 0;

    /**
     * @brief Resolve a worker endpoint by worker address from the local immutable directory snapshot.
     * @param[in] workerAddress Worker address string.
     * @param[out] endpoint Resolved endpoint.
     * @return K_OK if found, K_NOT_READY if directory is not published, K_NOT_FOUND if worker is absent.
     */
    virtual Status ResolveWorkerByAddress(const std::string &workerAddress, PlacementEndpoint &endpoint) const = 0;

    /**
     * @brief Return local worker identity from the local immutable directory snapshot.
     * @param[out] endpoint Local worker endpoint.
     * @return K_OK if found, K_NOT_READY if directory is not published, K_NOT_FOUND if local worker is absent.
     */
    virtual Status GetLocalWorker(PlacementEndpoint &endpoint) const = 0;
};

class PlacementDirectory final : public IPlacementDirectory {
public:
    PlacementDirectory() = default;
    ~PlacementDirectory() override = default;

    Status ResolveWorker(const std::string &workerId, PlacementEndpoint &endpoint) const override;
    Status ResolveWorkerByAddress(const std::string &workerAddress, PlacementEndpoint &endpoint) const override;
    Status GetLocalWorker(PlacementEndpoint &endpoint) const override;

    /**
     * @brief Publish an immutable placement directory snapshot.
     * @param[in] snapshot Fully constructed directory snapshot.
     */
    void Publish(std::shared_ptr<const PlacementDirectorySnapshot> snapshot);

private:
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<const PlacementDirectorySnapshot> snapshot_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_ROUTING_PLACEMENT_DIRECTORY_H

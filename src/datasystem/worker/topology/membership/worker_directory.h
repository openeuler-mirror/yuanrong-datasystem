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
 * Description: R0 worker directory snapshot.
 */
#ifndef DATASYSTEM_WORKER_TOPOLOGY_MEMBERSHIP_WORKER_DIRECTORY_H
#define DATASYSTEM_WORKER_TOPOLOGY_MEMBERSHIP_WORKER_DIRECTORY_H

#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/topology/runtime/placement_types.h"

namespace datasystem {
namespace topology {

struct WorkerDirectorySnapshot {
    WorkerDirectorySnapshot() : localAddress()
    {
    }

    int64_t version = -1;
    std::string localWorkerId;
    HostPort localAddress;
    std::unordered_map<std::string, WorkerEndpoint> workers;
};

class IWorkerDirectory {
public:
    virtual ~IWorkerDirectory() = default;

    /**
     * @brief Resolve a worker endpoint from the local immutable directory snapshot.
     * @param[in] workerId Worker identity.
     * @param[out] endpoint Resolved endpoint.
     * @return K_OK if found, K_NOT_READY if directory is not published, K_NOT_FOUND if worker is absent.
     *
     * Request threads only read local snapshot state. This method must not perform RPC probing, repository/backend IO,
     * CAS/List/Watch, task scan, migration, recovery, cleanup, or success-path logging.
     */
    virtual Status ResolveWorker(const std::string &workerId, WorkerEndpoint &endpoint) const = 0;

    /**
     * @brief Return local worker identity from the local immutable directory snapshot.
     * @param[out] endpoint Local worker endpoint.
     * @return K_OK if found, K_NOT_READY if directory is not published, K_NOT_FOUND if local worker is absent.
     */
    virtual Status GetLocalWorker(WorkerEndpoint &endpoint) const = 0;
};

class WorkerDirectory final : public IWorkerDirectory {
public:
    WorkerDirectory() = default;
    ~WorkerDirectory() override = default;

    Status ResolveWorker(const std::string &workerId, WorkerEndpoint &endpoint) const override;
    Status GetLocalWorker(WorkerEndpoint &endpoint) const override;

    /**
     * @brief Publish an immutable worker directory snapshot.
     * @param[in] snapshot Fully constructed directory snapshot.
     */
    void Publish(std::shared_ptr<const WorkerDirectorySnapshot> snapshot);

private:
    mutable std::shared_timed_mutex mutex_;
    std::shared_ptr<const WorkerDirectorySnapshot> snapshot_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_WORKER_TOPOLOGY_MEMBERSHIP_WORKER_DIRECTORY_H

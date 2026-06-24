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
 * Description: Worker directory read view.
 */
#ifndef DATASYSTEM_COMMON_TOPOLOGY_MEMBERSHIP_WORKER_DIRECTORY_H
#define DATASYSTEM_COMMON_TOPOLOGY_MEMBERSHIP_WORKER_DIRECTORY_H

#include "datasystem/common/topology/membership/membership_types.h"

namespace datasystem {
namespace topology {

class WorkerDirectory final : public IWorkerDirectory {
public:
    explicit WorkerDirectory(IMembershipSnapshotProvider &snapshotProvider);
    ~WorkerDirectory() override = default;
    WorkerDirectory(const WorkerDirectory &) = delete;
    WorkerDirectory &operator=(const WorkerDirectory &) = delete;
    WorkerDirectory(WorkerDirectory &&) = delete;
    WorkerDirectory &operator=(WorkerDirectory &&) = delete;

    Status GetSnapshot(std::shared_ptr<const MembershipSnapshot> &snapshot) const override;
    Status GetWorkerRecord(const WorkerId &workerId, WorkerRecord &record) const override;
    Status GetReadyEndpoint(const WorkerId &workerId, WorkerEndpoint &endpoint) const override;
    Status ListReadyWorkers(std::vector<WorkerRecord> &workers) const override;

private:
    IMembershipSnapshotProvider &snapshotProvider_;
};

}  // namespace topology
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_TOPOLOGY_MEMBERSHIP_WORKER_DIRECTORY_H

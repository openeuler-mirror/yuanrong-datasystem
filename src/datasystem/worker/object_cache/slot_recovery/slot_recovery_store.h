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
 * Description: Store contract for slot recovery coordination data.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_STORE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_STORE_H

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/cluster/coordination_backend/coordination_backend.h"
#include "datasystem/protos/slot_recovery.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {

class SlotRecoveryStore {
public:
    using IncidentMutator = std::function<Status(SlotRecoveryInfoPb &, bool &, bool &)>;

    virtual ~SlotRecoveryStore() = default;

    /**
     * @brief Create the slot recovery coordination table if needed.
     * @return Status of the call.
     */
    virtual Status Init();

    /**
     * @brief Read one incident from the coordination store.
     * @param[in] failedWorker The failed worker key of the incident.
     * @param[out] info The parsed incident protobuf.
     * @return Status of the call.
     */
    virtual Status GetIncident(const std::string &failedWorker, SlotRecoveryInfoPb &info);

    /**
     * @brief List all incidents currently stored in the coordination store.
     * @param[out] incidents Parsed incidents keyed by failed worker.
     * @return Status of the call.
     */
    virtual Status ListIncidents(std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents);

    /**
     * @brief Delete one incident from the coordination store.
     * @param[in] failedWorker The failed worker key of the incident.
     * @return Status of the call.
     */
    virtual Status DeleteIncident(const std::string &failedWorker);

    /**
     * @brief Overwrite one incident in the coordination store.
     * @param[in] failedWorker The failed worker key of the incident.
     * @param[in] info The incident protobuf to write.
     * @return Status of the call.
     */
    virtual Status UpdateIncident(const std::string &failedWorker, const SlotRecoveryInfoPb &info);

    /**
     * @brief Perform a CAS update for one incident.
     * @param[in] failedWorker The failed worker key of the incident.
     * @param[in] mutator The incident mutator that transforms the current value.
     * @return Status of the call.
     */
    virtual Status CASIncident(const std::string &failedWorker, const IncidentMutator &mutator);
};

class CoordinationSlotRecoveryStore final : public SlotRecoveryStore {
public:
    explicit CoordinationSlotRecoveryStore(cluster::ICoordinationBackend *backend);
    ~CoordinationSlotRecoveryStore() override = default;

    Status Init() override;
    Status GetIncident(const std::string &failedWorker, SlotRecoveryInfoPb &info) override;
    Status ListIncidents(std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents) override;
    Status DeleteIncident(const std::string &failedWorker) override;
    Status UpdateIncident(const std::string &failedWorker, const SlotRecoveryInfoPb &info) override;
    Status CASIncident(const std::string &failedWorker, const IncidentMutator &mutator) override;

private:
    cluster::ICoordinationBackend *backend_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif

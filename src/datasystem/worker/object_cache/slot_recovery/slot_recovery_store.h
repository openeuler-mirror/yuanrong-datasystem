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
 * Description: Etcd wrapper for slot recovery coordination data.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_STORE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_SLOT_RECOVERY_STORE_H

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/protos/slot_recovery.pb.h"

namespace datasystem {
namespace object_cache {

class SlotRecoveryStore {
public:
    using IncidentMutator =
        std::function<Status(SlotRecoveryInfoPb &, bool &, bool &)>;

    explicit SlotRecoveryStore(EtcdStore *etcdStore) : etcdStore_(etcdStore) {}
    virtual ~SlotRecoveryStore() = default;

    /**
     * @brief Create the slot recovery ETCD table if needed.
     * @return Status of the call.
     */
    virtual Status Init();

    /**
     * @brief Read one incident from ETCD.
     * @param[in] failedWorker The failed worker key of the incident.
     * @param[out] info The parsed incident protobuf.
     * @return Status of the call.
     */
    virtual Status GetIncident(const std::string &failedWorker, SlotRecoveryInfoPb &info);

    /**
     * @brief List all incidents currently stored in ETCD.
     * @param[out] incidents Parsed incidents keyed by failed worker.
     * @return Status of the call.
     */
    virtual Status ListIncidents(std::vector<std::pair<std::string, SlotRecoveryInfoPb>> &incidents);

    /**
     * @brief Delete one incident from ETCD.
     * @param[in] failedWorker The failed worker key of the incident.
     * @return Status of the call.
     */
    virtual Status DeleteIncident(const std::string &failedWorker);

    /**
     * @brief Overwrite one incident in ETCD.
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

private:
    EtcdStore *etcdStore_;
};

}  // namespace object_cache
}  // namespace datasystem

#endif

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
 * Description: Local topology snapshot store.
 */
#ifndef DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_LOCAL_SNAPSHOT_STORE_H
#define DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_LOCAL_SNAPSHOT_STORE_H

#include <string>

#include "datasystem/topology/model/topology_types.h"

namespace datasystem {
namespace topology {

class ITopologyLocalSnapshotStore {
public:
    virtual ~ITopologyLocalSnapshotStore() = default;

    /**
     * @brief Persist one local topology snapshot for degraded startup.
     * @param[in] snapshot Snapshot with digest already populated.
     * @return K_OK on success.
     */
    virtual Status Save(const LocalTopologySnapshot &snapshot) = 0;

    /**
     * @brief Load the last local topology snapshot.
     * @param[out] snapshot Decoded snapshot.
     * @return K_OK on success; K_NOT_FOUND when absent; K_INVALID for malformed content.
     */
    virtual Status Load(LocalTopologySnapshot &snapshot) const = 0;
};

class FileTopologyLocalSnapshotStore final : public ITopologyLocalSnapshotStore {
public:
    explicit FileTopologyLocalSnapshotStore(std::string filePath);
    ~FileTopologyLocalSnapshotStore() override = default;
    FileTopologyLocalSnapshotStore(const FileTopologyLocalSnapshotStore &) = delete;
    FileTopologyLocalSnapshotStore &operator=(const FileTopologyLocalSnapshotStore &) = delete;

    Status Save(const LocalTopologySnapshot &snapshot) override;
    Status Load(LocalTopologySnapshot &snapshot) const override;

private:
    std::string filePath_;
};

}  // namespace topology
}  // namespace datasystem
#endif  // DATASYSTEM_TOPOLOGY_RUNTIME_TOPOLOGY_LOCAL_SNAPSHOT_STORE_H

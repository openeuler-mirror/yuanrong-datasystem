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
 * Description: Fast migrate transport.
 */
#ifndef DATASYSTEM_MIGRATE_DATA_FAST_TRANSPORT_H2
#define DATASYSTEM_MIGRATE_DATA_FAST_TRANSPORT_H2

#include "datasystem/worker/object_cache/data_migrator/transport/migrate_transport.h"

namespace datasystem {
namespace object_cache {
class FastMigrateTransport2 : public MigrateTransport {
public:
    FastMigrateTransport2() = default;

    ~FastMigrateTransport2() override = default;

    /**
     * @brief Migrate data to remote node.
     * @param[in] req The Requestparameters for migration.
     * @param[out] rsp The Response of migration.
     * @return Status of the call.
     */
    Status MigrateDataToRemote(const Request &req, Response &rsp) override;
};

}  // namespace object_cache
}  // namespace datasystem

#endif
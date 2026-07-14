/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DATASYSTEM_WORKER_WORKER_CLI_H
#define DATASYSTEM_WORKER_WORKER_CLI_H

#include <string>

#include "datasystem/utils/status.h"

namespace datasystem {
namespace cli {

/**
 * @brief Save cluster topology to the file.
 * @param[in] filename The filename to save the cluster topology information.
 * @return Status of this call
 */
Status SaveClusterTopologyToFile(const std::string &filename);

/**
 * @brief Updating the cluster topology from the file.
 * @param[in] filename Update the cluster topology based on the information in the file.
 * @return Status of this call
 */
Status UpdateClusterTopologyFromFile(const std::string &filename);

/**
 * @brief Handle a cluster-topology CLI operation when one was requested.
 * @return True when a CLI flag was handled.
 */
bool HandleCli();
}  // namespace cli
}  // namespace datasystem

#endif

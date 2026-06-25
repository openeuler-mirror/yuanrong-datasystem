/*
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
 * Description: Shared cluster metadata table names used by cluster manager stores.
 */
#ifndef DATASYSTEM_WORKER_CLUSTER_MANAGER_CLUSTER_CONSTANTS_H
#define DATASYSTEM_WORKER_CLUSTER_MANAGER_CLUSTER_CONSTANTS_H

namespace datasystem {
static constexpr char HASHRING_TABLE[] = "/datasystem/ring";
static constexpr char CLUSTER_TABLE[] = "datasystem/cluster";
static constexpr char MASTER_ADDRESS_TABLE[] = "/datasystem";
}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_CLUSTER_MANAGER_CLUSTER_CONSTANTS_H

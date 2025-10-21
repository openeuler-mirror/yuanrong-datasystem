/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Define the common constants.
 */
#ifndef DATASYSTEM_COMMON_CONSTANTS_H
#define DATASYSTEM_COMMON_CONSTANTS_H

#include <string>

namespace datasystem {
static const std::string DEFAULT_TENANT_ID = "";  // The default tanent id.
static const std::string CLIENT_ACCESS_LOG_NAME = "ds_client_access";
static const std::string ACCESS_LOG_NAME = "access";
static const std::string REQUEST_OUT_LOG_NAME = "request_out";
static const std::string RESOURCE_LOG_NAME = "resource";
static const std::string SC_METRICS_LOG_NAME = "sc_metrics";
// The maximum number of randomly selected nodes for reconciliation.
static const size_t MAX_QUERY_WORKER_NUM_FOR_RECONCILIATION = 5;

// master meta tables
// oc meta tables
static const std::string META_TABLE = "meta_table";                  // The object meta table name.
static const std::string LOCATION_TABLE = "object_location_table";   // The object location table name.
static const std::string NESTED_TABLE = "nested_table";              // The nested relationship table name.
static const std::string NESTED_COUNT_TABLE = "nested_count_table";  // The nested relationship count table name.
// This table stores operations that require asynchronous notification to workers.
static const std::string ASYNC_WORKER_OP_TABLE = "async_worker_op_table";
static const std::string GLOBAL_REF_TABLE = "global_reference_table";  // The global reference table name.
static const std::string GLOBAL_CACHE_TABLE = "global_cache_table";    // The l2cache delete table name.
static const std::string REMOTE_CLIENT_OBJ_REF_TABLE = "remote_client_obj_ref_table";  // The remoteClient obj ref table
static const std::string REMOTE_CLIENT_REF_TABLE = "remote_client_ref_table";  // The remoteClient ref table name
static const std::string HEALTH_TABLE = "health_table";                        // The l2_cache health table name.

static const int CHECK_FILE_EXIST_INTERVAL_S = 5;  // The time for back up replica check db path exsit.

static const int ASYNC_LOGGER_STOP_MAX_WAIT_SEC = 15;  // The max wait time when AsyncLogger Stop.

// cluster info table in rocksDb
static const std::string CLUSTER_TABLE = "cluster_table";
static const std::string HASHRING_TABLE = "hashring_table";
static const std::string REPLICA_GROUP_TABLE = "replica_group_table";

// bytes convert
static const uint64_t MB_TO_BYTES = 1024 * 1024;
static const uint64_t KB = 1024;

// worker lock id
static const uint32_t WORKER_LOCK_ID = 0;
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_CONSTANTS_H

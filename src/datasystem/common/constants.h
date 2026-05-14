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

#include <cstdint>
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

// sc meta tables
static const std::string STREAM_TABLE_NAME = "stream_table";
static const std::string PUB_TABLE_NAME = "pub_table";
static const std::string SUB_TABLE_NAME = "sub_table";
static const std::string NOTIFY_PUB_TABLE_NAME = "notify_pub_table";
static const std::string NOTIFY_SUB_TABLE_NAME = "notify_sub_table";
static const std::string STREAM_CON_CNT_TABLE_NAME = "stream_consumer_count";
static const std::string STREAM_PRODUCER_COUNT = "stream_producer_count";

static const int ASYNC_LOGGER_STOP_MAX_WAIT_SEC = 15;  // The max wait time when AsyncLogger Stop.

// stream data object
static const uint64_t DEFAULT_TIMEOUT_MS = 1000;

// cluster info table in rocksDb
static const std::string CLUSTER_TABLE = "cluster_table";
static const std::string HASHRING_TABLE = "hashring_table";
static const std::string REPLICA_GROUP_TABLE = "replica_group_table";

// bytes convert
static const uint64_t MB_TO_BYTES = 1024 * 1024;
static const uint64_t KB = 1024;

// worker lock id
static const uint32_t WORKER_LOCK_ID = 0;

enum class DataType : uint8_t {
    DATA_TYPE_INT8 = 0,   /**< int8 */
    DATA_TYPE_INT16 = 1,  /**< int16 */
    DATA_TYPE_INT32 = 2,  /**< int32 */
    DATA_TYPE_FP16 = 3,   /**< fp16 */
    DATA_TYPE_FP32 = 4,   /**< fp32 */
    DATA_TYPE_INT64 = 5,  /**< int64 */
    DATA_TYPE_UINT64 = 6, /**< uint64 */
    DATA_TYPE_UINT8 = 7,  /**< uint8 */
    DATA_TYPE_UINT16 = 8, /**< uint16 */
    DATA_TYPE_UINT32 = 9, /**< uint32 */
    DATA_TYPE_FP64 = 10,  /**< fp64 */
    DATA_TYPE_BFP16 = 11, /**< bfp16 */
    DATA_TYPE_RESERVED    /**< reserved */
};

enum class LifetimeType : uint8_t {
    REFERENCE = 0,
    MOVE = 1,
};

struct CreateDeviceParam {
    LifetimeType lifetime = LifetimeType::REFERENCE;
    bool cacheLocation = true;
};

// ub device
static const std::string ENV_UB_DEVICE_NAME = "DS_UB_DEV_NAME";
static const std::string ENV_UB_DEVICE_EID = "DS_UB_DEV_EID";
static const std::string DEFAULT_UB_DEVICE_NAME = "bonding_dev_0";

static const std::string CLIENT_LOG_FILENAME = "ds_client";

// log limit
static const int LOG_TIME_LIMIT_LEVEL1 = 1;   // 1 second
static const int LOG_TIME_LIMIT_LEVEL2 = 10;  // 10 seconds
static const int LOG_TIME_LIMIT_LEVEL3 = 60;  // 60 seconds

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_CONSTANTS_H

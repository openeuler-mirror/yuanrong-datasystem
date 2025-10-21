/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Client writer for file write.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_CONSTANTS_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_CONSTANTS_H

namespace datasystem {
static constexpr char ETCD_INODE_TABLE[] = "/datasystem/inode_table";
static constexpr char ETCD_EDGE_TABLE[] = "/datasystem/edge_table";
static constexpr char ETCD_WORKER_TABLE[] = "/datasystem/worker_table";
static constexpr char ETCD_LOGSTREAM_TABLE[] = "/datasystem/logstream_table";
static constexpr char ETCD_NAS_TABLE[] = "/datasystem/nas_table";
static constexpr char ETCD_DELETE_TABLE[] = "/datasystem/delete_table";
static constexpr char ETCD_ELECTION_KEY[] = "/datasystem/election";
static constexpr char KEEPALIVE_CREATE[] = "keepalive create";
static constexpr char KEEPALIVE_WRITE[] = "keepalive run";
static constexpr char KEEPALIVE_READ[] = "keepalive read";
static constexpr char KEEPALIVE_DONE[] = "keepalive done";
static constexpr char WATCH_CREATE[] = "watch create";
static constexpr char WATCH_READ[] = "watch read";
static constexpr char WATCH_WRITE[] = "watch run";
static constexpr char WRITES_DONE[] = "writes done";
static constexpr char ETCD_RING_PREFIX[] = "/datasystem/ring";
static constexpr char ETCD_CLUSTER_TABLE[] = "datasystem/cluster";  // Do not add leading '/'. prefix is customized
static constexpr char ETCD_MASTER_ADDRESS_TABLE[] = "/datasystem";
static constexpr char ETCD_REPLICA_GROUP_TABLE[] = "/datasystem/replica_group";
// Save key without worker id metadata
static constexpr char ETCD_META_TABLE_PREFIX[] = "/datasystem/metadata/meta_table/";
static constexpr char ETCD_LOCATION_TABLE_PREFIX[] = "/datasystem/metadata/object_location_table/";
static constexpr char ETCD_ASYNC_WORKER_OP_TABLE_PREFIX[] = "/datasystem/metadata/async_worker_op_table/";
static constexpr char ETCD_GLOBAL_CACHE_TABLE_PREFIX[] = "/datasystem/metadata/global_cache_table/";
static constexpr char ETCD_HASH_SUFFIX[] = "hash";
static constexpr char ETCD_WORKER_SUFFIX[] = "worker_id";

static constexpr char ETCD_NODE_EXITING[] = "exiting";
static constexpr char ETCD_NODE_READY[] = "ready";
static constexpr char ETCD_NODE_DOWNGRADE_RESTART[] = "d_rst";

static constexpr char ETCD_HEALTH_CHECK_TABLE[] = "/datasystem/health_check";
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_ETCD_CONSTANTS_H

/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: the type of hash ring events
 */
#ifndef DATASYSTEM_WORKER_HASH_RING_EVENT_H
#define DATASYSTEM_WORKER_HASH_RING_EVENT_H

#include <cstdint>
#include <functional>

#include "datasystem/common/util/event_subscribers.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/master/meta_addr_info.h"

namespace datasystem {
class HashRingEvent : public EventNotifier {
    enum HashRingEventType {
        BEFORE_VOLUNTARY_EXIT,
        MIGRATE_META_RANGES,
        RECOVER_META_RANGES,
        RECOVER_RANGES_WHEN_VOLUNTARY_SCALE_DOWN_FINISH,
        HASH_RING_MODIFY,
        CLEAR_DATA_WITHOUT_META,
        CLEAR_DEV_META,
        GET_FAILED_WORKERS,
        CLUSTER_INIT_FINISH,
        GET_DB_PRIMARY_LOCATION,
        GET_NEED_CHANGE_REPLICATION_DB_NAME,
        SCALE_DOWN_FINISH,
        VOLUNTARY_SCALE_DOWN_FINISH,
        SCALEUP_FINISH,
        OTHER_AZ_NODE_DEAD,
        DATA_MIGRATION_READY,
        NEED_REDIRECT,
    };

public:
    using BeforeVoluntaryExit =
        EventSubscribers<BEFORE_VOLUNTARY_EXIT, std::function<Status(const std::string &taskId)>>;
    using MigrateRanges = EventSubscribers<
        MIGRATE_META_RANGES,
        std::function<Status(const std::string &dbName, const std::string &dest, const std::string &destDbNAme,
                             const worker::HashRange &ranges, bool isNetworkRecovery)>>;
    using SyncClusterNodes = EventSubscribers<HASH_RING_MODIFY, std::function<void(const std::set<std::string> &)>>;
    using RecoverMetaRanges =
        EventSubscribers<RECOVER_META_RANGES, std::function<Status(const std::vector<std::string> &workerUuids,
                                                                   const worker::HashRange &extraRanges)>>;
    using RecoverAsyncTaskRanges = EventSubscribers<
        RECOVER_RANGES_WHEN_VOLUNTARY_SCALE_DOWN_FINISH,
        std::function<Status(const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges)>>;
    using ClearDataWithoutMeta = EventSubscribers<
        CLEAR_DATA_WITHOUT_META,
        std::function<Status(const worker::HashRange &ranges, const std::string &workerAddr,
                             const worker::HashRange &halfCompletedRanges, const std::vector<std::string> &uuids)>>;
    using ClearDevClientMetaForScaledInWorker =
        EventSubscribers<CLEAR_DEV_META, std::function<Status(const std::vector<std::string> &removeNodes)>>;
    using GetFailedWorkers =
        EventSubscribers<GET_FAILED_WORKERS, std::function<void(std::unordered_set<std::string> &)>>;

    using ClusterInitFinish =
        EventSubscribers<CLUSTER_INIT_FINISH, std::function<void(const std::string &, const std::string &)>>;

    using GetDbPrimaryLocation =
        EventSubscribers<GET_DB_PRIMARY_LOCATION,
                         std::function<Status(const std::string &, HostPort &, std::string &)>>;

    using GetNeedChangeReplicationDbName =
        EventSubscribers<GET_NEED_CHANGE_REPLICATION_DB_NAME,
                         std::function<void(const std::set<std::string> &, std::vector<std::string> &)>>;

    using ScaleDownFinish =
        EventSubscribers<GET_NEED_CHANGE_REPLICATION_DB_NAME, std::function<void(const std::vector<std::string> &)>>;

    using VoluntaryScaleDownFinsih =
        EventSubscribers<GET_NEED_CHANGE_REPLICATION_DB_NAME, std::function<void(const std::string &)>>;
    using ScaleupFinish = EventSubscribers<SCALEUP_FINISH, std::function<void(const std::string &)>>;
    using OtherAzNodeDeadEvent = EventSubscribers<OTHER_AZ_NODE_DEAD, std::function<Status(const std::string &)>>;
    using DataMigrationReady = EventSubscribers<DATA_MIGRATION_READY, std::function<Status(void)>>;
    using CheckNeedRedirect = EventSubscribers<NEED_REDIRECT, std::function<void(const std::string &, HostPort &, bool &)>>;
};

class EtcdClusterMagagerEvent : public EventNotifier {
    enum EtcdClusterMagagerEventType {
        QUERY_MASTER_ADDR_IN_OTHER_AZ,
        CHECK_IF_OTHER_AZ_NODE_CONNECTED,
    };

public:
    using CheckIfOtherAzNodeConnected =
        EventSubscribers<CHECK_IF_OTHER_AZ_NODE_CONNECTED, std::function<void(const HostPort &, bool &isConnect)>>;
    using QueryMasterAddrInOtherAz =
        EventSubscribers<QUERY_MASTER_ADDR_IN_OTHER_AZ,
                         std::function<Status(const std::string &, const std::string &, MetaAddrInfo &)>>;
};
}  // namespace datasystem
#endif
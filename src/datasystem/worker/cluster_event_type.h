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
 * Description: the type of events in cluster
 */
#ifndef DATASYSTEM_WORKER_CLUSTER_EVENT_TYPE_H
#define DATASYSTEM_WORKER_CLUSTER_EVENT_TYPE_H

#include <cstdint>
#include <functional>

#include <etcd/api/mvccpb/kv.pb.h>

#include "datasystem/common/util/event_subscribers.h"
#include "datasystem/common/util/net_util.h"
namespace datasystem {
enum ClusterEventType : uint32_t {
    ADD_LOCAL_FAILED_NODE,
    ERASE_FAILED_NODE_API,
    START_NODE_CHECK,
    NODE_TIMEOUT,
    NODE_NETWORK_RECOVERY,
    REMOVE_DEAD_WORKER,
    CHANGE_PRIMARY_COPY,
    REQUEST_META_FROM_WORKER,
    NODE_RESTART,
    CHECK_NEW_NODE_META,
    START_CLEAR_WORKER_META,
    CLEAR_WORKER_META,
    CHECK_AND_CLEAR_DEVICE_META,
    REPLICA,
    RECOVER_MASTER_APP_REF,
};

using AddLocalFailedNodeEvent = EventSubscribers<ADD_LOCAL_FAILED_NODE, std::function<Status(const HostPort &)>>;
using EraseFailedNodeApiEvent = EventSubscribers<ERASE_FAILED_NODE_API, std::function<void(HostPort &)>>;
using StartNodeCheckEvent = EventSubscribers<START_NODE_CHECK, std::function<Status()>>;
using NodeTimeoutEvent = EventSubscribers<NODE_TIMEOUT, std::function<Status(const std::string &, bool, bool, bool)>>;
using NodeNetworkRecoveryEvent =
    EventSubscribers<NODE_NETWORK_RECOVERY, std::function<Status(const std::string &, int64_t, bool)>>;
using RemoveDeadWorkerEvent = EventSubscribers<REMOVE_DEAD_WORKER, std::function<void(const std::string &)>>;
using ChangePrimaryCopy = EventSubscribers<CHANGE_PRIMARY_COPY, std::function<Status(const std::string &, bool)>>;
using RequestMetaFromWorkerEvent =
    EventSubscribers<REQUEST_META_FROM_WORKER, std::function<Status(const std::string &, const std::string &)>>;
using NodeRestartEvent = EventSubscribers<NODE_RESTART, std::function<Status(const std::string &, int64_t, bool)>>;
using CheckNewNodeMetaEvent = EventSubscribers<CHECK_NEW_NODE_META, std::function<Status(const HostPort &)>>;
using StartClearWorkerMeta = EventSubscribers<START_CLEAR_WORKER_META, std::function<Status(const HostPort &)>>;
using ClearWorkerMeta = EventSubscribers<CLEAR_WORKER_META, std::function<Status(const HostPort &)>>;
using ReplicaEvent = EventSubscribers<REPLICA, std::function<Status(mvccpb::Event &)>>;
using RecoverMasterAppRefEvent =
    EventSubscribers<RECOVER_MASTER_APP_REF,
                     std::function<Status(std::function<bool(const std::string &)>, const std::string &)>>;

class ReplicaMagagerEvent : public EventNotifier {
    enum ReplicaMagagerEventType {
        GET_PRIMARY_REPLICA_INFO_IN_WORKER,
        GET_PRIMARY_REPLICA_LOCATION,
        GET_PRIMARY_REPLICA_DBNAMES
    };

public:
    using GetPrimaryReplicaInfoInWorker =
        EventSubscribers<GET_PRIMARY_REPLICA_INFO_IN_WORKER,
                         std::function<void(const std::string &, std::map<std::string, std::string> &)>>;
    using GetPrimaryReplicaLocation =
        EventSubscribers<GET_PRIMARY_REPLICA_LOCATION, std::function<Status(const std::string &, std::string &)>>;
    using GetPrimaryReplicaDbNames =
        EventSubscribers<GET_PRIMARY_REPLICA_DBNAMES,
                         std::function<Status(const std::string &, std::vector<std::string> &)>>;
};
}  // namespace datasystem
#endif
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Role-minimal cluster topology watch plans.
 */
#include "datasystem/cluster/runtime/topology_role_watch_plan.h"

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

Status TopologyRoleWatchPlan::Build(TopologyRuntimeRole role, const std::string &localAddress,
                                    const TopologyKeyHelper &keys, int64_t startRevision,
                                    std::vector<WatchKey> &watchKeys)
{
    CHECK_FAIL_RETURN_STATUS(startRevision >= 0, K_INVALID, "negative cluster topology watch revision");
    std::vector<WatchKey> built{ { keys.TopologyTable(), TopologyKeyHelper::TopologyKey(), startRevision } };
    if (role == TopologyRuntimeRole::WORKER || role == TopologyRuntimeRole::UNIFIED_ETCD) {
        std::string notifyKey;
        RETURN_IF_NOT_OK(TopologyKeyHelper::NotifyKey(localAddress, notifyKey));
        built.emplace_back(WatchKey{ keys.NotifyTable(), std::move(notifyKey), startRevision });
    }
    if (role == TopologyRuntimeRole::CONTROLLER || role == TopologyRuntimeRole::UNIFIED_ETCD) {
        if (role == TopologyRuntimeRole::CONTROLLER) {
            CHECK_FAIL_RETURN_STATUS(localAddress.empty(), K_INVALID, "Controller watch plan has a local address");
        }
        built.emplace_back(WatchKey{ keys.MigrateTaskTable(), "", startRevision });
        built.emplace_back(WatchKey{ keys.DeleteTaskTable(), "", startRevision });
        built.emplace_back(WatchKey{ keys.MembershipTable(), "", startRevision });
    } else if (role != TopologyRuntimeRole::WORKER) {
        CHECK_FAIL_RETURN_STATUS(role == TopologyRuntimeRole::OBSERVER && localAddress.empty(), K_INVALID,
                                 "invalid cluster topology runtime role");
    }
    watchKeys = std::move(built);
    return Status::OK();
}

}  // namespace datasystem::cluster

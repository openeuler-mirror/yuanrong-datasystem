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
#include "datasystem/worker/hash_ring/read_hash_ring.h"

#include <algorithm>
#include <string>
#include <unordered_set>

#include <google/protobuf/util/message_differencer.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/iam/tenant_auth_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/meta_route_tool.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"

DS_DECLARE_string(etcd_address);
DS_DECLARE_string(master_address);
DS_DECLARE_bool(enable_distributed_master);

namespace datasystem {
namespace worker {
ReadHashRing::ReadHashRing(const std::string &azName, std::string workerAddr, EtcdStore *store)
    : HashRing(workerAddr, store),
      azName_(azName)
{
}

ReadHashRing::~ReadHashRing()
{
    LOG(INFO) << "ReadHashRing exit";
}

Status ReadHashRing::Init(const std::string &hashRingPb)
{
    return UpdateRing(hashRingPb, -1);
}

Status ReadHashRing::HandleRingEvent(const mvccpb::Event &event)
{
    if (event.type() == mvccpb::Event_EventType::Event_EventType_DELETE) {
        return Status::OK();
    }
    RETURN_IF_NOT_OK_APPEND_MSG(UpdateRing(event.kv().value(), event.kv().mod_revision()),
                                "UpdateRing failed when WatchKey");
    return Status::OK();
}

Status ReadHashRing::UpdateRing(const std::string &newSerializedRingInfo, int64_t version)
{
    if (newSerializedRingInfo.empty()) {
        return Status::OK();
    }
    HashRingPb newRing;
    if (!newRing.ParseFromString(newSerializedRingInfo)) {
        return Status(K_RUNTIME_ERROR, "Failed to parse RingInfoPb");
    }

    std::lock_guard<std::shared_timed_mutex> lock(mutex_);
    if (google::protobuf::util::MessageDifferencer::Equals(ringInfo_, newRing)) {
        LOG(INFO) << FormatString("The read ring is same for AZ %s, not update read ring.", azName_);
        return Status::OK();
    }
    LOG(INFO) << "Update " << azName_ << " readRing of version " << version << "." << SummarizeHashRing(newRing);
    auto lines = SplitRingJson(FormatString("Worker %s update az %s readRing to", workerAddr_, azName_), newRing);
    std::for_each(lines.begin(), lines.end(), [](const std::string &line) { LOG(INFO) << line; });

    HashRingPb oldRing = std::move(ringInfo_);
    ringInfo_.CopyFrom(newRing);
    if (ringInfo_.cluster_has_init()) {
        state_ = RUNNING;
        LOG(INFO) << FormatString("The AZ %s read ring started successfully", azName_);
    }

    if (::worker::IncrementalDelNodeInfo(oldRing, newRing)) {
        LOG(INFO) << FormatString("The AZ %s ClearDataWithoutMeta", azName_);

        std::unordered_set<std::string> delNodeAddrs;
        std::unordered_set<std::string> delNodeIds;
        for (const auto &delInfo : newRing.del_node_info()) {
            const auto& delNodeAddr = delInfo.first;
            if (delNodeAddrs.insert(delNodeAddr).second) {
                std::string workerId;
                auto rc = ::worker::GetWorkeridByWorkerAddr(newRing, delNodeAddr, workerId);
                if (rc.IsError()) {
                    LOG(WARNING) << FormatString("get workerId by workerAddr[%s] failed: %s", delNodeAddr,
                                                 rc.ToString());
                    continue;
                }
                (void)delNodeIds.insert(std::move(workerId));
            }
        }

        for (const auto &delNodeAddr : delNodeAddrs) {
            LOG_IF_ERROR(HashRingEvent::OtherAzNodeDeadEvent::GetInstance().NotifyAll(delNodeAddr),
                         FormatString("Clear AZ %s async notify op failed.", azName_));
        }

        if (!HashRing::isMultiReplicaEnable_) {
            std::vector<std::string> delNodeIdsVec(std::make_move_iterator(delNodeIds.begin()),
                                                   std::make_move_iterator(delNodeIds.end()));
            LOG_IF_ERROR(
                HashRingEvent::LocalClearDataWithoutMeta::GetInstance().NotifyAll(worker::HashRange{}, delNodeIdsVec),
                FormatString("Clear AZ %s data failed.", azName_));
        }
    }

    GenerateHashRingUuidMap(newRing, workerUuid2AddrMap_, workerAddr2UuidMap_, relatedWorkerMap_);
    HashRing::UpdateTokenMap();
    return Status::OK();
}

Status ReadHashRing::GetUuidInCurrCluster(const std::string &oldUuid, std::string &newUuid,
                                          std::optional<RouteInfo> &routeInfo)
{
    RETURN_IF_NOT_OK_APPEND_MSG(HashRing::GetUuidInCurrCluster(oldUuid, newUuid, routeInfo), " in az: " + azName_);
    return Status::OK();
}

Status ReadHashRing::GetWorkerAddrByUuidForAddressing(const std::string &workerUuid, HostPort &workerAddr)
{
    RETURN_IF_NOT_OK_APPEND_MSG(HashRing::GetWorkerAddrByUuidForAddressing(workerUuid, workerAddr),
                                " in az: " + azName_);
    return Status::OK();
}

Status ReadHashRing::GetWorkerAddrByUuidForMultiReplica(const std::string &workerUuid, HostPort &workerAddr)
{
    RETURN_IF_NOT_OK_APPEND_MSG(HashRing::GetWorkerAddrByUuidForMultiReplica(workerUuid, workerAddr),
                                " in az: " + azName_);
    return Status::OK();
}

Status ReadHashRing::GetPrimaryWorkerUuid(const std::string &key, std::string &outWorkerUuid,
                                          std::optional<RouteInfo> &routeInfo) const
{
    RETURN_IF_NOT_OK_APPEND_MSG(HashRing::GetPrimaryWorkerUuid(key, outWorkerUuid, routeInfo), " in az: " + azName_);
    return Status::OK();
}

bool ReadHashRing::IsPreLeaving(const std::string &workerAddr)
{
    return HashRing::IsPreLeaving(workerAddr);
}

Status ReadHashRing::GetHashRingWorkerNum(int &workerNum) const
{
    RETURN_IF_NOT_OK_APPEND_MSG(HashRing::GetHashRingWorkerNum(workerNum, true), " in az: " + azName_);
    return Status::OK();
}

std::set<std::string> ReadHashRing::GetValidWorkersInHashRing() const
{
    return HashRing::GetValidWorkersInHashRing();
}

Status ReadHashRing::GetUuidByWorkerAddr(const std::string &GetUuidByWorkerAddr, std::string &uuid)
{
    RETURN_IF_NOT_OK_APPEND_MSG(HashRing::GetUuidByWorkerAddr(GetUuidByWorkerAddr, uuid), " in az: " + azName_);
    return Status::OK();
}

bool ReadHashRing::IsWorkable() const
{
    return HashRing::IsWorkable();
}

Status ReadHashRing::GetMasterUuid(const std::string &objKey, std::string &masterUuid)
{
    // ReadHashRing will not retry when the ring is not workable.
    if (!IsWorkable()) {
        RETURN_STATUS(K_NOT_READY, FormatString("Hash ring of %s not ready, get workerId failed", azName_));
    }
    if (TrySplitWorkerIdFromObjecId(objKey, masterUuid).IsError()) {
        std::optional<RouteInfo> routeInfo;
        RETURN_IF_NOT_OK(GetPrimaryWorkerUuid(objKey, masterUuid, routeInfo));
    }
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem

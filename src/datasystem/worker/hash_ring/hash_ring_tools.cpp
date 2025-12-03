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
#include "datasystem/worker/hash_ring/hash_ring_tools.h"
#include <google/protobuf/util/json_util.h>

#include "datasystem/common/flags/flags.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/timer.h"

DS_DEFINE_uint32(rolling_update_timeout_s, 1800,
                 "Maximum duration of the rolling upgrade, default value is 1800 seconds.");

namespace datasystem {
namespace worker {
void GenerateHashRingUuidMap(const HashRingPb &ringInfo, std::map<std::string, HostPort> &workerUuid2AddrMap,
                             std::map<std::string, std::string> &workerAddr2UuidMap,
                             std::map<std::string, HostPort> &relatedWorkerMap)
{
    workerUuid2AddrMap.clear();
    workerAddr2UuidMap.clear();

    Status rc;
    for (const auto &kv : ringInfo.workers()) {
        const auto &workerAddr = kv.first;
        HostPort workerHostPort;
        rc = workerHostPort.ParseString(workerAddr);
        if (rc.IsError()) {
            LOG(ERROR) << "Failed to parse address " << workerAddr;
            continue;
        }
        if (kv.second.worker_uuid().empty()) {
            continue;
        }
        if (ringInfo.update_worker_map().find(kv.first) == ringInfo.update_worker_map().end()) {
            workerUuid2AddrMap[kv.second.worker_uuid()] = workerHostPort;
        }
        relatedWorkerMap[kv.second.worker_uuid()] = workerHostPort;
        workerAddr2UuidMap[workerAddr] = kv.second.worker_uuid();
    }
}

Status GetWorkerAddrByUuidForAddressing(const HashRingPb &ringInfo,
                                        const std::map<std::string, HostPort> &workerUuid2AddrMap,
                                        const std::string &workerUuid, HostPort &workerAddr)
{
    // from workable worker in current ring
    auto it = workerUuid2AddrMap.find(workerUuid);
    if (it != workerUuid2AddrMap.end()) {
        workerAddr = it->second;
        return Status::OK();
    }

    // maybe scale down, rehash with uuid
    auto substitute = ringInfo.key_with_worker_id_meta_map().find(workerUuid);
    if (substitute != ringInfo.key_with_worker_id_meta_map().end()) {
        VLOG(1) << "node has been remove, try to rehash to other worker";
        return workerAddr.ParseString(substitute->second);
    }

    return Status(K_NOT_FOUND, FormatString("Can not find the address of workerUuid %s", workerUuid));
}

Status GetWorkerAddrByUuidForMultiReplica(const HashRingPb &ringInfo,
                                          const std::map<std::string, HostPort> &workerUuid2AddrMap,
                                          const std::string &workerUuid, HostPort &workerAddr)
{
    // from workable worker in current ring
    auto it = workerUuid2AddrMap.find(workerUuid);
    if (it != workerUuid2AddrMap.end()) {
        workerAddr = it->second;
        return Status::OK();
    }

    // maybe scale down, find workerAddr from update_worker_map
    for (const auto &it : ringInfo.update_worker_map()) {
        if (it.second.worker_uuid() == workerUuid) {
            return workerAddr.ParseString(it.first);
        }
    }

    return Status(K_NOT_FOUND, FormatString("Can not find the address of workerUuid %s", workerUuid));
}

bool IncrementalAddNodeInfo(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    if (newRing.add_node_info().empty()) {
        return false;
    }

    // most common scaleup situation: the old is empty and the new is not empty
    if (oldRing.add_node_info().empty()) {
        return true;
    }

    // a joining node scaledown and its range is passed to another joining node
    int oldAddRangesSize{ 0 };
    for (auto &r : oldRing.add_node_info()) {
        oldAddRangesSize += r.second.changed_ranges_size();
    };
    int newAddRangesSize{ 0 };
    for (auto &r : newRing.add_node_info()) {
        newAddRangesSize += r.second.changed_ranges_size();
    };

    return (newAddRangesSize > oldAddRangesSize);
}

bool IncrementalDelNodeInfo(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    if (newRing.del_node_info().empty()) {
        return false;
    }
    // most common scaledown situation: the old is empty and the new is not empty
    if (oldRing.del_node_info().empty()) {
        return true;
    }

    auto delNodesInNewRing = GetKeysFromPairsContainer(newRing.del_node_info());
    auto delNodesInOldRing = GetKeysFromPairsContainer(oldRing.del_node_info());
    std::set<std::string> diff;
    std::set_difference(delNodesInNewRing.begin(), delNodesInNewRing.end(), delNodesInOldRing.begin(),
                        delNodesInOldRing.end(), std::inserter(diff, diff.begin()));
    return !diff.empty();
}

bool DecrementalDelNodeInfo(const HashRingPb &oldRing, const HashRingPb &newRing, std::set<std::string> &diff)
{
    if (oldRing.del_node_info().empty()) {
        return false;
    }

    auto delNodesInNewRing = GetKeysFromPairsContainer(newRing.del_node_info());
    auto delNodesInOldRing = GetKeysFromPairsContainer(oldRing.del_node_info());
    std::set_difference(delNodesInOldRing.begin(), delNodesInOldRing.end(), delNodesInNewRing.begin(),
                        delNodesInNewRing.end(), std::inserter(diff, diff.begin()));
    return !diff.empty();
}

Status GetWorkeridByWorkerAddr(const HashRingPb &currRing, const std::string &addr, std::string &workerId)
{
    auto worker = currRing.workers().find(addr);
    if (worker != currRing.workers().end() && !worker->second.worker_uuid().empty()) {
        workerId = worker->second.worker_uuid();
        return Status::OK();
    }
    auto updateIt = currRing.update_worker_map().find(addr);
    CHECK_FAIL_RETURN_STATUS(
        updateIt != currRing.update_worker_map().end(), K_NOT_FOUND,
        FormatString("Cannot find the workerid of %s in this ring %s.", addr, currRing.ShortDebugString()));
    workerId = updateIt->second.worker_uuid();
    return Status::OK();
}

std::string HashRingToJsonString(const HashRingPb &ring)
{
    std::string jsonStr;
    auto rc = google::protobuf::util::MessageToJsonString(ring, &jsonStr);
    if (!rc.ok()) {
        LOG(WARNING) << "Pb to Json string failed:" << rc.ToString();
    }
    return jsonStr;
}

std::vector<std::string> SplitRingJson(const std::string &prefix, const HashRingPb &ring)
{
    std::vector<std::string> lines;
    std::string log = HashRingToJsonString(ring);
    if (log.size() <= LOG_MAX_SIZE_LIMIT) {
        lines.emplace_back(FormatString("%s [%s]", prefix, log));
    } else {
        auto i = 0u;
        for (; i < log.size() / LOG_MAX_SIZE_LIMIT; i++) {
            lines.emplace_back(
                FormatString("%s part%d [%s]", prefix, i, log.substr(i * LOG_MAX_SIZE_LIMIT, LOG_MAX_SIZE_LIMIT)));
        }
        if (log.size() % LOG_MAX_SIZE_LIMIT != 0) {
            lines.emplace_back(FormatString("%s part%d [%s]", prefix, i, log.substr(i * LOG_MAX_SIZE_LIMIT)));
        }
    }
    return lines;
}

bool ClearWorkerMapIfNeed(HashRingPb &ring)
{
    bool ret = false;
    auto workers = ring.mutable_update_worker_map();
    for (auto iter = workers->begin(); iter != workers->end();) {
        if (WorkerUuidRemovable(iter->first, iter->second, ring)) {
            VLOG(1) << "Clear update worker map due to expiration: " << iter->first;
            iter = workers->erase(iter);
            ret = true;
        } else {
            iter++;
        }
    }
    return ret;
}

bool WorkerUuidRemovable(const std::string &addr, const UpdateNodePb &updateNodePb, const HashRingPb &ring)
{
    INJECT_POINT("HashRingTools.WorkerUuidRemovable", []() { return true; });
    auto now = static_cast<uint64_t>(GetSystemClockTimeStampUs());
    static const uint64_t US_TO_SECS = 1'000'000ul;
    auto workerIt = ring.workers().find(addr);
    bool waitRestart = workerIt != ring.workers().end() && workerIt->second.worker_uuid().empty();
    return now > updateNodePb.timestamp()
           && (now - updateNodePb.timestamp()) / US_TO_SECS > FLAGS_rolling_update_timeout_s && !waitRestart;
}

std::unordered_map<std::string, std::string> GetWorkersFromHashRingPb(const HashRingPb &hashRing)
{
    std::unordered_map<std::string, std::string> workerInfos;
    for (const auto &worker : hashRing.workers()) {
        std::string workerUuid = worker.second.worker_uuid();
        if (workerUuid.empty()) {
            auto iter = hashRing.update_worker_map().find(worker.first);
            if (iter != hashRing.update_worker_map().end()) {
                workerUuid = iter->second.worker_uuid();
            }
        }
        workerInfos.emplace(worker.first, std::move(workerUuid));
    }
    return workerInfos;
}

}  // namespace worker
}  // namespace datasystem

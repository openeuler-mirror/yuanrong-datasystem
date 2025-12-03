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
 * Description: Hash ring tools
 */
#ifndef DATASYSTEM_WORKER_HASH_RING_TOOLS_H
#define DATASYSTEM_WORKER_HASH_RING_TOOLS_H

#include <map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/hash_ring.pb.h"

namespace datasystem {
// print the ChangeNodePb
inline std::ostream &operator<<(std::ostream &os, const datasystem::ChangeNodePb &c)
{
    os << c.ShortDebugString();
    return os;
}
namespace worker {
#ifdef WITH_TESTS
#define HASH_RING_LOG_IF_ERROR(_statement, _msg)               \
    do {                                                       \
        auto _rc = (_statement);                               \
        if (_rc.IsError()) {                                   \
            LOG(ERROR) << (_msg) << ":" << _rc.ToString();     \
            INJECT_POINT_NO_RETURN("Hashring.Scaletask.Fail"); \
        }                                                      \
    } while (false)
#else
#define HASH_RING_LOG_IF_ERROR(_statement, _msg) LOG_IF_ERROR(_statement, _msg)
#endif

/**
 * @brief Generate the maps between workerUuid and workerAddr.
 * @param[in] ringInfo The ring
 * @param[out] workerUuid2AddrMap The {workerUuid, workerAddr} map of input ringInfo for route
 * @param[out] workerAddr2UuidMap The {workerAddr, workerUuid} map of input ringInfo
 * @param[out] workerAddr2UuidMap The {workerAddr, workerUuid} map of input ringInfo for relation
 */
void GenerateHashRingUuidMap(const HashRingPb &ringInfo, std::map<std::string, HostPort> &workerUuid2AddrMap,
                             std::map<std::string, std::string> &workerAddr2UuidMap,
                             std::map<std::string, HostPort> &relatedWorkerMap);

/**
 * @brief Get the worker address of specific workerUuid for addressing.
 * @param[in] ringInfo The ring
 * @param[in] workerUuid2AddrMap The {workerUuid, workerAddr} map of input ringInfo
 * @param[in] workerUuid The workerUuid of the wanted worker
 * @param[out] workerAddr The worker address of the wanted worker
 * @return Status of the call.
 */
Status GetWorkerAddrByUuidForAddressing(const HashRingPb &ringInfo,
                                        const std::map<std::string, HostPort> &workerUuid2AddrMap,
                                        const std::string &workerUuid, HostPort &workerAddr);

/**
 * @brief Get the worker address of specific workerUuid for multi replica.
 * @param[in] ringInfo The ring
 * @param[in] workerUuid2AddrMap The {workerUuid, workerAddr} map of input ringInfo
 * @param[in] workerUuid The workerUuid of the wanted worker
 * @param[out] workerAddr The worker address of the wanted worker
 * @return Status of the call.
 */
Status GetWorkerAddrByUuidForMultiReplica(const HashRingPb &ringInfo,
                                          const std::map<std::string, HostPort> &workerUuid2AddrMap,
                                          const std::string &workerUuid, HostPort &workerAddr);

/**
 * @brief Judge if the new ring has incremental add_node_info.
 * @param[in] oldRing The old ring that includes worker info.
 * @param[in] newRing The updated ring that includes worker info.
 * @return Return ture if the new ring has incremental add_node_info.
 */
bool IncrementalAddNodeInfo(const HashRingPb &oldRing, const HashRingPb &newRing);

/**
 * @brief Judge if the new ring has incremental del_node_info.
 * @param[in] oldRing The old ring that includes worker info.
 * @param[in] newRing The updated ring that includes worker info.
 * @return Return ture if the new ring has incremental del_node_info.
 */
bool IncrementalDelNodeInfo(const HashRingPb &oldRing, const HashRingPb &newRing);

/**
 * @brief Judge if the old ring has decremental del_node_info.
 * @param[in] oldRing The old ring that includes worker info.
 * @param[in] newRing The updated ring that includes worker info.
 * @return Return ture if the old ring has decremental del_node_info.
 */
bool DecrementalDelNodeInfo(const HashRingPb &oldRing, const HashRingPb &newRing, std::set<std::string> &diff);

/**
 * @brief Get the worker uuid of specific worker
 * @param[in] ringInfo The ring
 * @param[in] addr The worker address of the wanted worker
 * @param[out] workerId The workerUuid of the wanted worker
 * @return Status of the call.
 */
Status GetWorkeridByWorkerAddr(const HashRingPb &currRing, const std::string &addr, std::string &workerId);

/**
 * @brief Convert HashRing to json string
 * @param[in] ringInfo The ring
 * @return The json string.
 */
std::string HashRingToJsonString(const HashRingPb &ring);

/**
 * @brief spilt the hashring json to multiple lines.
 * @param[in] prefix The prefix content before printing ring
 * @param[in] ring The ring
 * @return The lines
 */
std::vector<std::string> SplitRingJson(const std::string &prefix, const HashRingPb &ring);

/**
 * @brief Clear worker map if uuid expired.
 * @param[in] ring The ring.
 * @return Return true if the ring clean up worker map.
 */
bool ClearWorkerMapIfNeed(HashRingPb &ring);

/**
 * @brief Check worker uuid is removable.
 * @param[in] addr Worker address.
 * @param[in] updateNodePb Update node protobuf.
 * @param[in] ring Hash ring protobuf.
 * @return True if worker uuid is removable.
 */
bool WorkerUuidRemovable(const std::string &addr, const UpdateNodePb &updateNodePb, const HashRingPb &ring);

/**
 * @brief Clear worker map if uuid expired.
 * @param[in] The HashRingPb instance.
 * @return Return the workers info, worker addr -> worker uuid.
 */
std::unordered_map<std::string, std::string> GetWorkersFromHashRingPb(const HashRingPb &currRing);
}  // namespace worker
}  // namespace datasystem
#endif

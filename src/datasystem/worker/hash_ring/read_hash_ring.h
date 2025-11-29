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
 * Description: The consistent hash ring.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_READ_HASH_RING_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_READ_HASH_RING_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>
#include <utility>

#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

namespace datasystem {
namespace worker {
class ReadHashRing : protected HashRing {
public:
    /**
     * @brief Create a new readHashRing object.
     * @param[in] AZName The az name that the hash ring belongs to
     * @param[in] workerId The worker uuid or ip address and port
     * @param[in] store The etcd store pointer
     * @param[in] cm The cluster manager pointer
     */
    explicit ReadHashRing(const std::string &AZName, std::string workerId, EtcdStore *store);

    ~ReadHashRing();

    /**
     * @brief Init consistent hash ring.
     * @param[in] revision Get the key of the specified version. If revision is less or equal to zero, the range is
     * over the newest key-value store.
     * @return Status of the call.
     */
    Status Init(const std::string &hashRingPb);

    /**
     * @brief Called when etcd event is about /datasystem/ring
     * @param[in] event etcd event
     * @return Status
     */
    Status HandleRingEvent(const mvccpb::Event &event);

    /**
     * @brief Update the hash ring according to newSerializedRingInfo.
     * @param[in] newSerializedRingInfo The serialized hash ring topology information received from etcd,
     * @param[in] version The version of key event.
     * @return Status of the call.
     */
    Status UpdateRing(const std::string &newSerializedRingInfo, int64_t version);

    /**
     * @brief Check whether the worker ID belongs to the current AZ. If yes, the worker ID after rehashing is returned.
     * @param[in] oldUuid Original worker ID.
     * @param[out] newUuid The rehashed worker ID.
     * @return Status of the call.
     */
    Status GetUuidInCurrCluster(const std::string &oldUuid, std::string &newUuid, std::optional<RouteInfo> &routeInfo);

    /**
     * @brief Get worker address by uuid for addressing.
     * @param[in] workerUuid The worker uuid.
     * @param[out] workerAddr The remote worker address.
     * @return Return the local worker uuid.
     */
    Status GetWorkerAddrByUuidForAddressing(const std::string &workerUuid, HostPort &workerAddr);

    /**
     * @brief Get worker address by uuid for multi replica.
     * @param[in] workerUuid The worker uuid.
     * @param[out] workerAddr The remote worker address.
     * @return Return the local worker uuid.
     */
    Status GetWorkerAddrByUuidForMultiReplica(const std::string &workerUuid, HostPort &workerAddr);

    /**
     * @brief Get the primary worker uuid by consistent hash algorithm.
     * @param[in] key Use the key to calculate the uint32 hash value, and then find the first node that is greater than
     * the hash value on the consistent hash ring.
     * @param[out] outWorkerUuid the outWorkerUuid is calc by consistent hash algorithm when enable consistent
     * hash(enable_distribute_master is true and etcd_address is valid);
     */
    Status GetPrimaryWorkerUuid(const std::string &key, std::string &outWorkerUuid,
                                std::optional<RouteInfo> &routeInfo) const;

    /**
     * @brief Return true if the hash ring is in RUNNING or PRE_RUNNING state.
     * @return Return true if the hash ring is in RUNNING or PRE_RUNNING state.
     * @brief If the node is voluntary scale down node.
     * @param workerAddr worker address.
     * @return If the node is voluntary scale down node.
     */
    bool IsPreLeaving(const std::string &workerAddr);

    /**
     * @brief Get the number of workers in hashring. For centralized master, return -1.
     * @param[out] workerNum The number of workers in hashring.
     * @return Status of the call.
     */
    Status GetHashRingWorkerNum(int &workerNum) const;

    /**
     * @brief Get the workers in hashring.
     * @return The addresses of workers.
     */
    std::set<std::string> GetValidWorkersInHashRing() const;

    /**
     * @brief Get worker address by uuid.
     * @param[in] workerAddr The worker address.
     * @param[out] uuid The worker uuid.
     * @return Status
     */
    Status GetUuidByWorkerAddr(const std::string &workerAddr, std::string &uuid);

    /**
     * @brief Return true if the hash ring is in RUNNING or PRE_RUNNING or PRE_LEAVING state.
     * @return Return true if the hash ring is in RUNNING or PRE_RUNNING or PRE_LEAVING state.
     */
    bool IsWorkable() const;

    /**
     * @brief Get master address.
     * @param[in] objKey The key of object.
     * @param[out] masterUuid The master uuid of the object.
     * @return Status of the call.
     */
    Status GetMasterUuid(const std::string &objKey, std::string &masterUuid);
private:
    std::string azName_;
};

}  // namespace worker
}  // namespace datasystem
#endif

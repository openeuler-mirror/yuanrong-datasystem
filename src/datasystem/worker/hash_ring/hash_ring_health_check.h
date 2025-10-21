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
 * Description: The hash ring health check.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_HASH_RING_HEALTH_CHECK_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_HASH_RING_HEALTH_CHECK_H

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace worker {
class HashRing;

class HashRingHealthCheck {
public:
    explicit HashRingHealthCheck(HashRing *hashRing);
    ~HashRingHealthCheck();

    /**
     * @brief Init HashRingHealthCheck instance.
     */
    void Init();

    /**
     * @brief Update hash ring info.
     * @param[in] hashRingStr The serialized hash ring topology information received from etcd.
     * @param[in] version The version of hash ring topology information.
     */
    void UpdateRing(const std::string &hashRingStr, int64_t version);

protected:
    /**
     * @brief Perform all hash ring health checks.
     * @param[in] autoFix Whether perform auto fix if hash ring in abnormal state.
     * @param[in/out] ring The hash ring object.
     * @return true: hash ring in abnormal state and already auto fix.
     */
    bool CheckHashRing(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check hash ring pending in scale up.
     * @param[in] autoFix Whether perform auto fix if hash ring pending in scale up.
     * @param[in/out] ring The hash ring object.
     * @return true: hash ring pending in scale up.
     */
    bool CheckScaleUpInfo(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check hash ring pending in voluntary scale down.
     * @param[in] autoFix Whether perform auto fix if hash ring pending in scale down.
     * @param[in/out] ring The hash ring object.
     * @return true: hash ring pending in scale down.
     */
    bool CheckVoluntaryScaleDownInfo(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check hash ring pending in passive scale down.
     * @param[in] autoFix Whether perform auto fix if hash ring pending in passive scale down.
     * @param[in/out] ring The hash ring object.
     * @return true: hash ring pending in passive scale down.
     */
    bool CheckPassiveScaleDownInfo(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check worker in joining state but not exists add_node_info.
     * @param[in] autoFix Always false, only print logs.
     * @param[in/out] ring The hash ring object.
     * @return true: worker in abnormal state.
     */
    bool CheckWorkerInJoiningState(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check worker in leaving state but not exists del_node_info.
     * @param[in] autoFix Always false, only print logs.
     * @param[in/out] ring The hash ring object.
     * @return true: worker in abnormal state.
     */
    bool CheckWorkerInLeavingState(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check worker in initial state for a long time.
     * @param[in] autoFix Always false, only print logs.
     * @param[in/out] ring The hash ring object.
     * @return true: worker in abnormal state.
     */
    bool CheckWorkerInInitialState(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check worker in failed state, not passive scale down not start.
     * @param[in] autoFix Always false, only print logs.
     * @param[in/out] ring The hash ring object.
     * @return true: worker in abnormal state.
     */
    bool CheckPassiveScaleDownNotStart(bool autoFix, HashRingPb &ring);

    /**
     * @brief Check worker in failed state, not passive scale down not start.
     * @param[in] autoFix Always false, only print logs.
     * @param[in/out] ring The hash ring object.
     * @return true: worker in abnormal state.
     */
    bool CheckScaleDownInfoStandbyWorker(bool autofix, HashRingPb &ring);

private:
    /**
     * @brief Start hash ring health check.
     */
    void Run();

    /**
     * @brief Reset hash ring last update time.
     */
    void ResetUpdateTime();

    /**
     * @brief Get the last update elapsed time.
     * @return uint64_t The elapsed time.
     */
    uint64_t GetLastUpdateElapsedMs();

    /**
     * @brief Do hash ring health check.
     * @param[in] tryUpdateRing Whether resend hash ring change event.
     * @param[in] checkRing Whether perform hash ring health check.
     * @return Status of this call.
     */
    Status DoHealthCheck(bool tryUpdateRing, bool checkRing);

    /**
     * @brief Get current hash ring from etcd.
     * @param[out] ring The hash ring object.
     * @param[out] value The serialized hash ring topology information.
     * @param[out] version The data version in etcd.
     * @return Status of this call.
     */
    Status TryGetAndParseHashRingPb(HashRingPb &ring, std::string &value, int64_t &version);

    /**
     * @brief Check whether hash ring changed.
     * @param[in] value The serialized hash ring topology information.
     * @param[in] version The data version in etcd.
     * @return true if changed.
     */
    bool IsChanged(const std::string &value, int64_t version);

    HashRing *hashRing_;
    std::unique_ptr<Thread> checkThread_;
    std::atomic_bool exitFlag_{ false };
    std::vector<std::function<bool(bool, HashRingPb &)>> policies_;
    std::mutex mutex_;
    Timer timer_;
    std::string preHashRingStr_;
    int64_t preVersion_{ -1 };
};
}  // namespace worker
}  // namespace datasystem
#endif

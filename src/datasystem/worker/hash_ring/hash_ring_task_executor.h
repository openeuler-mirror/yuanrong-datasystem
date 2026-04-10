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
 * Description: Process the change of hash ring in cluster
 */
#ifndef DATASYSTEM_WORKER_HASH_RING_TASK_EXECUTOR_H
#define DATASYSTEM_WORKER_HASH_RING_TASK_EXECUTOR_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "datasystem/common/kvstore/etcd/etcd_store.h"
#include "datasystem/common/kvstore/kv_store.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/protos/hash_ring.pb.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"

namespace datasystem {
namespace worker {
struct MigrateScaleUpInfo {
    std::string srcNode = "";
    HashRange ranges = {};
};

struct MigrateScaleDownInfo {
    std::string destWorker = "";
    std::string destPrimaryReplicaAddress = "";
    HashRange ranges = {};
};

// {destDbName: {destPrimaryReplicaAddress, {recoverHashRanges}}}
using ScaleDownMigrationTaskInfo = std::unordered_map<std::string, MigrateScaleDownInfo>;
// {srcDbName: MigrateScaleUpInfo}
using ScaleUpMigrationTaskInfo = std::unordered_map<std::string, MigrateScaleUpInfo>;
class HashRingTaskExecutor {
public:
    HashRingTaskExecutor(const std::string &workerAddr, const std::string &workerUuid, EtcdStore *etcdStore,
                         bool isMultiReplicaEnable);

    ~HashRingTaskExecutor();

    /**
     * @brief Process the scale up event triggered by nodes' join.
     * @param[in] currRing The current hash ring.
     * @return Status of the call.
     */
    Status SubmitScaleUpTask(const HashRingPb &currRing);

    /**
     * @brief Process the scale up event triggered by node's join.
     * @param[in] targetNode The target node of migrate task.
     * @param[in] isNetworkRecovery Determine whether it is a scene of network recovery.
     */
    void SubmitOneScaleUpTask(
        const google::protobuf::Map<std::string, datasystem::ChangeNodePb>::value_type &targetNode,
        bool isNetworkRecovery = false);

    /**
     * @brief Process the scale down event triggered by a node fault.
     * @param[in] currRing The current hash ring.
     * @return Status of the call.
     */
    Status SubmitScaleDownTask(const HashRingPb &currRing);

    /**
     * @brief SubmitScaleDownTaskRecoverFromEtcd
     * @param[in] currRing Current ring
     * @return Status of the call
     */
    Status SubmitScaleDownTaskRecoverFromEtcd(const HashRingPb &currRing);

    /**
     * @brief Get the Scale Down Migrate Uuid Task object
     * @param[in] removeWorkerAddr Remove worker addr
     * @param[in] currRing Current ring
     * @param[in] isVoluntaryDownNodeFault is voluntary down node fault
     * @param[out] taskInfos Migrate task info.
     * @return Status
     */
    Status ExcuteScaleDownMigrateUuidTask(const std::string &removeWorkerAddr, const HashRingPb &currRing,
                                          bool isVoluntaryDownNodeFault, ScaleDownMigrationTaskInfo &taskInfos);

    /**
     * @brief submit recovery async task for voluntary scale down worker.
     * @param[in] oldRing The ring before change.
     * @param[in] newRing The ring after change.
     */
    void SubmitVoluntaryRecoveryAsyncTask(const HashRingPb &oldRing, const HashRingPb &newRing);

    /**
     * @brief Restore the scale task.
     * @param[in] currRing The current hash ring.
     * @param[in] isRestartScenario True if triggered by worker restart.
     * @return Status of the call.
     */
    void RestoreScalingTask(const HashRingPb &currRing, bool isRestartScenario);

    void ClearVoluntaryTaskId()
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        voluntaryTaskIds_.clear();
    }

    bool CheckTaskExpired(const std::string &taskId)
    {
        INJECT_POINT("voluntaryscaledown.task.taskisrunning", []() { return false; });
        std::shared_lock<std::shared_timed_mutex> l(mutex_);
        return std::find(voluntaryTaskIds_.begin(), voluntaryTaskIds_.end(), taskId) == voluntaryTaskIds_.end();
    }

    /**
     * @brief Is migrate data task running.
     * @return true Migrate data task is running.
     */
    bool IsMigrateDataTaskRunning()
    {
        std::shared_lock<std::shared_timed_mutex> l(mutex_);
        return !voluntaryTaskIds_.empty();
    }

    /**
     * @brief Submit migrate data task.
     * @return Status of the call.
     */
    Status SubmitMigrateDataTask();

private:
    /**
     * @brief Submit scale up task if multireplica enable.
     * @param[in] targetNode target node
     * @param[in] isNetworkRecovery Is net worker recovery.
     */
    void SubmitOneScaleUpTaskMultiReplicaEnabled(
        const google::protobuf::Map<std::string, datasystem::ChangeNodePb>::value_type &targetNode,
        bool isNetworkRecovery = false);

    /**
     * @brief SubmitScaleDownTaskMultiReplica
     * SubmitScaleDownTaskMultiReplica function is used to submit a scale-down task
     * when multi-replica is enabled.
     * @param[in] currRing The current hash ring.
     * @return Status of the call.
     */
    Status SubmitScaleDownTaskMultiReplica(const HashRingPb &currRing);

    /**
     * @brief GetScaleUpMigrationTaskInfo
     * GetScaleUpMigrationTaskInfo function is used to get the information of scale-up
     * migration tasks.
     * @param[in] targetNode The target node.
     * @param[out] infos The scale-up migration task information.
     * @param[out] isVoluntaryScaleDown Whether it is a voluntary scale-down.
     * @param[out] scaleDownNode The node for scale-down.
     */
    void GetScaleUpMigrationTaskInfo(
        const google::protobuf::Map<std::string, datasystem::ChangeNodePb>::value_type &targetNode,
        ScaleUpMigrationTaskInfo &infos, bool &isVoluntaryScaleDown, std::string &scaleDownNode);

    /**
     * @brief Mark specific add_node_info finished in hash ring.
     * @param[in] destAddr Indicate which part of the add_node_info to be erased.
     * @param[out] ring The ring to be modified from and return the ring after modification.
     * @return Status of the call.
     */
    Status MarkAddNodeInfoFinished(const std::string &destAddr, const std::string &srcNode, HashRingPb &ring) const;

    /**
     * @brief Mark specific add_node_info finished in hash ring.
     * @param[in] newNode Indicate which part of the add_node_info to be marked.
     * @return Status of the call.
     */
    Status MarkAddNodeInfoFinished(const std::string &newNode, const std::string &srcNode);

    /**
     * @brief Erase specific del_node_info in hash ring.
     * @param[in] currRing The ring to be erased from.
     * @param[in] processedNodes Indicate which part of the del_node_info to be erased.
     * @param[in] allSubstitueUuidList Indicate the list of substitute uuids to overwrite.
     * @return Hash ring after erasure.
     */
    HashRingPb EraseFinishedDelNodeInfo(const HashRingPb &ring, const std::vector<std::string> &processedNodes,
                                        const std::vector<std::string> &allSubstitueUuidList) const;

    /**
     * @brief Recover the metadata and data of the faulty worker.
     * @param[in] currRing The current hash ring.
     * @param[in] removeNode The target node to remove.
     * @param[in] allSubstitueUuidList The list of substitute uuids.
     */
    void RecoverMetaAndDataOfFaultWorker(
        const HashRingPb &currRing,
        const google::protobuf::Map<std::basic_string<char>, datasystem::ChangeNodePb>::value_type &removeNode,
        std::vector<std::string> &allSubstitueUuidList);

    /**
     * @brief Recover meta and data of fault worker from replica.
     * @param[in] recorverDbName need recover db name.
     * @param[in] currRing current ring
     * @param[in] removeNode The target node to remove.
     */
    void RecoverMetaAndDataOfFaultWorkerByStandbyMaster(
        const std::string &scaleDownWorkerDbName, const HashRingPb &currRing,
        const google::protobuf::Map<std::basic_string<char>, datasystem::ChangeNodePb>::value_type &removeNode);

    /**
     * @brief Submit scale down migrate task.
     * @param[in] scaleDownWorkerDbName scale down worker db name.
     * @param[in] recoverDbPrimaryReplicaAddr recover db primary replica addr.
     * @param[in] recoverDbPrimaryDbName recover db primary db name
     * @param[in] recoverRanges recover ranges
     * @param[in] isVoluntary is voluntary node scale down or not.
     */
    void SubmitScaleDownMigrateTask(
        const MigrateScaleDownInfo &info, const std::string &recoverDbName, const std::string &scaleDownWorkerDbName,
        const google::protobuf::Map<std::basic_string<char>, datasystem::ChangeNodePb>::value_type &removeNode);

    /**
     * @brief Get workerId by hash
     * @param[in] ring The hash ring
     * @param[in] hash The hash value of worker
     * @param[out] workerId
     * @return Status of the call.
     */
    static Status GetWorkerByHash(const HashRingPb &ring, uint32_t hash, std::string &workerId);

    /**
     * @brief Get unfinished hash ranges of current worker from changeNodePb.
     * @param[in] ring The hash ring
     * @param[in] changeNode The changeNodePb.
     * @param[out] workerId
     * @return Hash ranges.
     */
    HashRange GetWorkHashRangeFromChangeNodePb(const HashRingPb &ring, const ChangeNodePb &changeNode,
                                               std::string &workerId);

    /**
     * @brief Get the Work Hash Range From Change Node Pb By Db Name object
     * @param[in] ring The hash ring
     * @param[in] changeNode The changeNodePb.
     * @param[out] workerId WorkerId.
     * @return ScaleDownMigrationTaskInfo
     */
    ScaleDownMigrationTaskInfo GetWorkHashRangeFromChangeNodePbByDbName(const HashRingPb &ring,
                                                                        const ChangeNodePb &changeNodePb,
                                                                        std::string &workerId);

    /**
     * @brief Erase specific del_node_info in hash ring.
     * @param[in] removeWorkerAddr The removing worker.
     * @param[in] currRing The ring to be erased from.
     * @param[out] allSubstituteUuidList Indicate the list of substitute uuids to overwrite.
     * @return Hash ring after erasure.
     */
    Status RecoverMetaWithWorkerIdOfFaultyWorker(const std::string &removeWorkerAddr, const HashRingPb &currRing,
                                                 std::vector<std::string> &allSubstituteUuidList,
                                                 bool isVoluntaryDownNodeFault = false);

    /**
     * @brief Clear data without meta.
     * @param[in] currRing Current hash ring info.
     * @param[out] ranges to clear data without meta.
     */
    void ClearDataWithoutMeta(const HashRingPb &currRing, HashRange &ranges);

    /**
     * @brief Clear data without meta.
     * @param[in] ranges Hash rranges.
     * @param[in] uuids Remove node uuids.
     */
    void ClearDataWithoutMeta(const HashRange &ranges, const std::vector<std::string> &uuids);

    /**
     * @brief Clear device client metadata for scaled-in worker nodes if current node is the metadata master
     * @details Retrieves the metadata master address from etcd and compares with current worker address.
     *          If this node is the metadata master, it triggers cleanup of client metadata associated
     *          with the removed worker nodes.
     * @param[in] removeNodes List of worker node addresses that have been scaled in and need cleanup
     * @return Status of the call.
     */
    void ClearDevClientMetaForScaledInWorker(const HashRingPb &currRing);

    /**
     * @brief Get remove node Key with uuid info.
     * @param[in] currRing current hash ring info.
     * @param[out] uuids Key with uuids info.
     */
    void GetRemoveNodeKeyWithUuidsInfo(const HashRingPb &currRing, const std::string &removeNodeAddr,
                                       std::vector<std::string> &uuids);

    /**
     * @brief recovery async task for voluntary scale down worker.
     * @param[in] oldRing The ring before change.
     * @param[in] voluntaryDonwWorker The addr of voluntary down worker.
     * @return Status of the call.
     */
    Status VoluntaryRecoveryAsyncTask(const HashRingPb &oldRing, const std::string &voluntaryDonwWorker);

    /**
     * @brief For inject testing, skip the real migration process.
     * @param[in] newWorkerAddr The scaleup worker.
     * @return Return true if in the injection test scenario
     */
    bool InjectTestMigration(const std::string &newWorkerAddr);

    /**
     * @brief Insert clear data task
     * @param[in] range clear data without meta ranges
     * @return true if a new task
     */
    bool InsertClearDataTask(const datasystem::ChangeNodePb_RangePb &range);

    bool InsertClearDataSubmitTask(const datasystem::ChangeNodePb_RangePb &range);

    /**
     * @brief Remove in-progress clear ranges after local clear flow finishes.
     * @param[in] ranges clear data without meta ranges
     */
    void RemoveClearDataTask(const HashRange &ranges);

    /**
     * @brief Remove submitted clear ranges after local clear flow finishes.
     * @param[in] ranges clear ranges of the finished clear task
     */
    void RemoveClearDataSubmitTask(const HashRange &ranges);

    /**
     * @brief Clear the finished hash_tokens of the scale-down node.
     * @param[in] ring The ring.
     * @param[in] srcNode The source worker.
     * @param[in] destAddr The destination worker.
     * @param[in] finishRanges The finish ranges.
     * @param[in] uuidRangeFinished Is the range of uuid finished or not.
     */
    void ClearTokenForScaleDown(HashRingPb &ring, const std::string &srcNode, const std::string &destAddr,
                                HashRange &finishRanges, bool uuidRangeFinished) const;

    /**
     * @brief Retry hash ring task.
     * @param[in] hashRingTask The hash ring task.
     * @param[in] breaker The condition under which the retry can exit.
     */
    void RetryHashRingTaskUntil(std::function<Status()> &&hashRingTask,
                                std::function<bool(const Status &)> &&breaker = nullptr);

    const std::string workerAddr_;
    const std::string workerUuid_;
    EtcdStore *etcdStore_;

    std::atomic<bool> exitFlag_{ false };
    bool multiReplicaEnabled_ = false;
    std::shared_timed_mutex mutex_;  // protect the following variables
    std::vector<std::string> voluntaryTaskIds_;
    std::shared_timed_mutex clearDataTaskmutex_;
    std::unordered_set<Range, SimplePairHash, RangeEqual> clearDataWithoutMetaTask_;
    std::unordered_set<Range, SimplePairHash, RangeEqual> clearDataWithoutMetaSubmitTasks_;
    std::unique_ptr<ThreadPool> scaleThreadPool_{ nullptr };
};
}  // namespace worker
}  // namespace datasystem
#endif

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
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/hash_algorithm.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/utils/status.h"
#include "datasystem/worker/hash_ring/hash_ring.h"

#include <algorithm>
#include <csignal>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <unordered_set>
#include <utility>

#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>

#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/log/log_helper.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/util/container_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uuid_generator.h"
#include "datasystem/worker/hash_ring/hash_ring_allocator.h"
#include "datasystem/worker/hash_ring/hash_ring_event.h"
#include "datasystem/worker/hash_ring/hash_ring_task_executor.h"
#include "datasystem/worker/hash_ring/hash_ring_tools.h"
#include "datasystem/worker/cluster_manager/worker_health_check.h"

namespace datasystem {
namespace worker {
static constexpr uint32_t MAX_SCALE_TASK_THREAD = 4;
static const std::string HASH_RING_TASK_EXECUTOR = "HashRingTaskExecutor";

HashRingTaskExecutor::HashRingTaskExecutor(const std::string &workerAddr, const std::string &workerUuid,
                                           EtcdStore *etcdStore, bool isMultiReplicaEnable)
    : workerAddr_(workerAddr),
      workerUuid_(workerUuid),
      etcdStore_(etcdStore),
      multiReplicaEnabled_(isMultiReplicaEnable)
{
    scaleThreadPool_ = std::make_unique<ThreadPool>(0, MAX_SCALE_TASK_THREAD, "HashRingScaleTask");
    HashRingEvent::LocalClearDataWithoutMetaFinish::GetInstance().AddSubscriber(
        HASH_RING_TASK_EXECUTOR, [this](const worker::HashRange &clearRanges) {
            RemoveClearDataSubmitTask(clearRanges);
            return Status::OK();
        });
}

HashRingTaskExecutor::~HashRingTaskExecutor()
{
    exitFlag_ = true;
    HashRingEvent::LocalClearDataWithoutMetaFinish::GetInstance().RemoveSubscriber(HASH_RING_TASK_EXECUTOR);
}

Status HashRingTaskExecutor::SubmitScaleUpTask(const HashRingPb &currRing)
{
    // submit async task to migrate meta.
    INJECT_POINT("skip.SubmitScaleUpTask");
    LOG(INFO) << "Submit async task to migrate meta.";
    for (auto &newWorker : currRing.add_node_info()) {
        if (!multiReplicaEnabled_) {
            SubmitOneScaleUpTask(newWorker);
        } else {
            SubmitOneScaleUpTaskMultiReplicaEnabled(newWorker);
        }
    }
    return Status::OK();
}

bool HashRingTaskExecutor::InjectTestMigration(const std::string &newWorkerAddr)
{
    (void)newWorkerAddr;  // for release version, avoid the compile warning of "unused parameter"
    INJECT_POINT("HashRing.SubmitScaleUpTask.skip", [&newWorkerAddr, this](uint32_t time_s) {
        scaleThreadPool_->Execute([newWorkerAddr, time_s, this] {
            Timer timer;
            while (timer.ElapsedSecond() < time_s) {
            };
            MarkAddNodeInfoFinished(newWorkerAddr, workerAddr_);
        });
        return true;
    });
    return false;
}

void HashRingTaskExecutor::SubmitOneScaleUpTask(
    const google::protobuf::Map<std::string, datasystem::ChangeNodePb>::value_type &targetNode, bool isNetworkRecovery)
{
    // 1. check if any ranges to migrate
    HashRange ranges;
    for (auto &range : targetNode.second.changed_ranges()) {
        if (range.workerid() == workerAddr_ && !range.finished()) {
            ranges.emplace_back(range.from(), range.end());
        }
    }
    if (ranges.empty()) {
        LOG(INFO) << "No migration tasks to process";
        return;
    }

    // only for test
    if (InjectTestMigration(targetNode.first)) {
        return;
    }

    // 2. notify to process
    auto dest = targetNode.first;
    auto traceId = GetStringUuid();
    LOG(INFO) << FormatString("Start migrate task from %s to %s with traceId %s", workerAddr_, dest, traceId);
    scaleThreadPool_->Execute([this, ranges, dest, isNetworkRecovery, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Timer timer;
        Raii logHelper([&] {
            LOG(INFO) << FormatString("Finish migrate task from %s to %s cost %f ms.", workerAddr_, dest,
                                      timer.ElapsedMilliSecond());
        });
        auto status =
            HashRingEvent::MigrateRanges::GetInstance().NotifyAll(workerUuid_, dest, "", ranges, isNetworkRecovery);
        INJECT_POINT_NO_RETURN("HashRingTaskExecutor.SubmitOneScaleUpTask.PreMarkAddNodeInfoFinished");
        if (status.IsOk()) {
            RetryHashRingTaskUntil([this, &dest]() {
                auto status = MarkAddNodeInfoFinished(dest, workerAddr_);
                HASH_RING_LOG_IF_ERROR(status, "Mark failed.");
                return status;
            });
        } else {
            LOG(ERROR) << "Migrate to " << dest
                       << " failed after retry. Wait for connection restoration or hash ring health check. Status: "
                       << status.ToString();
        }
    });
}

void HashRingTaskExecutor::GetScaleUpMigrationTaskInfo(
    const google::protobuf::Map<std::string, datasystem::ChangeNodePb>::value_type &targetNode,
    ScaleUpMigrationTaskInfo &infos, bool &isVoluntaryScaleDown, std::string &scaleDownNode)
{
    for (auto &range : targetNode.second.changed_ranges()) {
        HostPort migrateDbAddr;
        std::string migrateDbName;

        if (HashRingEvent::GetDbPrimaryLocation::GetInstance()
                .NotifyAll(range.workerid(), migrateDbAddr, migrateDbName)
                .IsError()) {
            continue;
        } else if (migrateDbAddr.ToString() == workerAddr_ && !range.finished()) {
            infos[migrateDbName].ranges.emplace_back(range.from(), range.end());
            infos[migrateDbName].srcNode = range.workerid();
        }
        if (range.from() == range.end() && !range.is_upgrade()) {
            isVoluntaryScaleDown = true;
            scaleDownNode = range.workerid();
        }
    }
}

void HashRingTaskExecutor::SubmitOneScaleUpTaskMultiReplicaEnabled(
    const google::protobuf::Map<std::string, datasystem::ChangeNodePb>::value_type &targetNode, bool isNetworkRecovery)
{
    bool isVoluntaryScaleDown = false;
    std::string scaleDownNode = "";
    ScaleUpMigrationTaskInfo infos;
    GetScaleUpMigrationTaskInfo(targetNode, infos, isVoluntaryScaleDown, scaleDownNode);
    if (infos.empty() || (isVoluntaryScaleDown && scaleDownNode != workerAddr_)) {
        return;
    }
    HostPort destDbAddr;
    std::string destDbName;

    std::string originDestAddr = targetNode.first;
    if (HashRingEvent::GetDbPrimaryLocation::GetInstance()
            .NotifyAll(originDestAddr, destDbAddr, destDbName)
            .IsError()) {
        return;
    }

    for (const auto &info : infos) {
        auto traceId = GetStringUuid();
        LOG(INFO) << FormatString("Start migrate task from %s srcdbName %s to %s dest dbname %s with traceId %s",
                                  workerAddr_, info.first, destDbAddr.ToString(), destDbName, traceId);
        scaleThreadPool_->Execute([this, info, originDestAddr, destDbAddr, destDbName, isNetworkRecovery, traceId] {
            TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
            Timer timer;
            Raii logHelper([&] {
                LOG(INFO) << FormatString("Finish migrate task from %s srcdbName %s to %s dest dbname %s cost %f ms.",
                                          workerAddr_, info.first, destDbAddr.ToString(), destDbName,
                                          timer.ElapsedMilliSecond());
            });
            auto status = HashRingEvent::MigrateRanges::GetInstance().NotifyAll(
                info.first, destDbAddr.ToString(), destDbName, info.second.ranges, isNetworkRecovery);
            if (status.IsOk()) {
                RetryHashRingTaskUntil([this, &originDestAddr, &info]() {
                    auto status = MarkAddNodeInfoFinished(originDestAddr, info.second.srcNode);
                    HASH_RING_LOG_IF_ERROR(status, "Mark failed.");
                    return status;
                });
            } else {
                LOG(ERROR) << "Migrate to " << destDbAddr.ToString()
                           << " failed after retry. Wait for connection restoration or hash ring health check. Status: "
                           << status.ToString();
            }
        });
    }
}

Status HashRingTaskExecutor::SubmitMigrateDataTask()
{
    if (!voluntaryTaskIds_.empty()) {
        return Status(K_TRY_AGAIN, "Waiting for voluntary scale down task to finish");
    }
    auto taskId = GetStringUuid();
    {
        std::lock_guard<std::shared_timed_mutex> l(mutex_);
        if (!voluntaryTaskIds_.empty()) {
            return Status(K_TRY_AGAIN, "Waiting for voluntary scale down task to finish");
        }
        voluntaryTaskIds_.emplace_back(taskId);
    }
    auto traceId = GetStringUuid();
    LOG(INFO) << FormatString("Start migrate data on %s with taskId(%s) and traceId(%s)", workerAddr_, taskId, traceId);
    INJECT_POINT("hashring.after_finish_add_node_info", []() {
        raise(SIGKILL);
        return Status::OK();
    });
    scaleThreadPool_->Execute([this, taskId, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        Status rc;
        Raii clearTaskId([this, &rc, &taskId]() {
            if (rc.IsOk()) {
                LOG(INFO) << "process voluntary scale down finish, ready to shutdown";
                return;
            }
            {
                std::lock_guard<std::shared_timed_mutex> l(mutex_);
                voluntaryTaskIds_.erase(std::remove(voluntaryTaskIds_.begin(), voluntaryTaskIds_.end(), taskId),
                                        voluntaryTaskIds_.end());
            }
        });
        rc = HashRingEvent::BeforeVoluntaryExit::GetInstance().NotifyAll(taskId);
        if (rc.IsError()) {
            LOG_IF_ERROR(rc, "worker voluntary scale down failed");
            return;
        }
        rc = etcdStore_->CAS(
            ETCD_RING_PREFIX, "",
            [&](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                HashRingPb ring;
                if (!ring.ParseFromString(oldValue)) {
                    return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
                }
                ring.mutable_workers()->erase(workerAddr_);
                VLOG(1) << "Erase worker on the ring: " << workerAddr_;
                newValue = std::make_unique<std::string>(ring.SerializeAsString());
                return Status::OK();
            });
        LOG_IF_ERROR(rc, "Voluntary scale down failed due to CAS conflict");
    });
    return Status::OK();
}

void HashRingTaskExecutor::ClearTokenForScaleDown(HashRingPb &ring, const std::string &srcNode,
                                                  const std::string &destAddr, HashRange &finishRanges,
                                                  bool uuidRangeFinished) const
{
    auto worker = ring.mutable_workers()->find(srcNode);
    if (worker == ring.mutable_workers()->end()) {
        return;
    }
    auto tokens = worker->second.mutable_hash_tokens();
    tokens->erase(std::remove_if(tokens->begin(), tokens->end(),
                                 [&finishRanges](uint32_t token) {
                                     for (const auto &range : finishRanges) {
                                         if (range.first > range.second) {
                                             if (token > range.first || token <= range.second) {
                                                 return true;
                                             }
                                         } else {
                                             if (token > range.first && token <= range.second) {
                                                 return true;
                                             }
                                         }
                                     }
                                     return false;
                                 }),
                  tokens->end());
    if (!uuidRangeFinished) {
        return;
    }
    // If worker uuid range finished, record key_with_worker_id_meta_map and update_worker_map, then clear uuid.
    if (!worker->second.worker_uuid().empty()) {
        (*ring.mutable_key_with_worker_id_meta_map())[worker->second.worker_uuid()] = destAddr;
        UpdateNodePb update;
        update.set_worker_uuid(worker->second.worker_uuid());
        update.set_timestamp(GetSystemClockTimeStampUs());
        ring.mutable_update_worker_map()->insert({ worker->first, update });
        worker->second.clear_worker_uuid();
    }
    for (auto &iter : (*ring.mutable_key_with_worker_id_meta_map())) {
        if (iter.second == srcNode) {
            iter.second = destAddr;
        }
    }
}

Status HashRingTaskExecutor::MarkAddNodeInfoFinished(const std::string &destAddr, const std::string &srcNode,
                                                     HashRingPb &ring) const
{
    auto nodePb = ring.mutable_add_node_info()->find(destAddr);
    if (nodePb == ring.mutable_add_node_info()->end()) {
        return Status(K_NOT_FOUND,
                      FormatString("Can not find the add_node_info of %s in %s.", destAddr, ring.ShortDebugString()));
    }
    auto worker = ring.mutable_workers()->find(srcNode);
    if (worker == ring.mutable_workers()->end()) {
        return Status(K_NOT_FOUND,
                      FormatString("Can not find the worker of %s in %s.", srcNode, ring.ShortDebugString()));
    }

    // mark the finished ranges.
    bool isThisNodeFinished{ true };
    HashRange finishRanges;
    bool uuidRangeFinished{ false };
    for (auto &range : (*nodePb->second.mutable_changed_ranges())) {
        if (range.workerid() != srcNode) {
            if (!range.finished()) {
                isThisNodeFinished = false;
            }
            continue;
        }
        range.set_finished(true);
        finishRanges.emplace_back(range.from(), range.end());
        if (range.is_upgrade()) {
            auto iter = ring.update_worker_map().find(destAddr);
            if (iter != ring.update_worker_map().end()) {
                ring.mutable_key_with_worker_id_meta_map()->erase(iter->second.worker_uuid());
                ring.mutable_update_worker_map()->erase(destAddr);
            }
            auto destWorker = ring.mutable_workers()->find(destAddr);
            if (destWorker == ring.mutable_workers()->end() || destWorker->second.worker_uuid().empty()) {
                LOG(WARNING) << FormatString("destNode[%s] can not be found in hash ring", destAddr);
            } else if (ring.mutable_key_with_worker_id_meta_map()->erase(destWorker->second.worker_uuid())) {
                LOG(INFO) << FormatString(
                    "Erase destNode[%s]'s workerId in keyWithWorkerIdMetaMap without check updateWorkerMap", destAddr);
            }
        } else if (range.from() == range.end()) {
            uuidRangeFinished = true;
        }
    }
    if (worker->second.need_scale_down()) {
        ClearTokenForScaleDown(ring, srcNode, destAddr, finishRanges, uuidRangeFinished);
    }

    if (!isThisNodeFinished) {
        return Status::OK();
    }

    HashRingAllocator::FinishAddNodeInfoIfNeed(ring);
    return Status::OK();
}

Status HashRingTaskExecutor::MarkAddNodeInfoFinished(const std::string &newNode, const std::string &srcNode)
{
    VLOG(1) << "mark add_node_info finished to etcd: ";
    INJECT_POINT("hashring.finishaddnodeinfo");
    return etcdStore_->CAS(
        ETCD_RING_PREFIX, "", [&](const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool &retry) {
            HashRingPb ring;
            if (!ring.ParseFromString(oldValue)) {
                return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
            }
            retry = false;
            auto status = MarkAddNodeInfoFinished(newNode, srcNode, ring);
            if (status.IsError()) {
                LOG(WARNING) << "Migrate task may be submitted repeatedly and has been processed." << status.ToString();
                return Status::OK();
            }
            VLOG(1) << "After mark add_node_info finished: " << MapToString(ring.add_node_info());
            newValue = std::make_unique<std::string>(ring.SerializeAsString());
            return Status::OK();
        });
}

Status HashRingTaskExecutor::SubmitScaleDownTask(const HashRingPb &currRing)
{
    if (!multiReplicaEnabled_) {
        return SubmitScaleDownTaskRecoverFromEtcd(currRing);
    } else {
        return SubmitScaleDownTaskMultiReplica(currRing);
    }
}

Status HashRingTaskExecutor::SubmitScaleDownTaskRecoverFromEtcd(const HashRingPb &currRing)
{
    auto traceId = GetStringUuid();
    LOG(INFO) << "Submit scale down task of " << VectorToString(GetKeysFromPairsContainer(currRing.del_node_info()))
              << " with traceid " << traceId;
    scaleThreadPool_->Execute([this, currRing, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        INJECT_POINT("SubmitScaleDownTask.skip", [] { return; });

        // clean up dev client metadata
        ClearDevClientMetaForScaledInWorker(currRing);

        Status status;
        HashRange ranges;
        std::vector<std::string> processedNodes, allSubstitueUuidList;
        ClearDataWithoutMeta(currRing, ranges);
        if (ranges.empty()) {
            LOG(INFO) << "all scale down task is processing, no need to excute, skip";
            return;
        }
        for (auto &removeNode : currRing.del_node_info()) {
            LOG(INFO) << "Process del_node_info: " << removeNode.first;
            processedNodes.emplace_back(removeNode.first);
            RecoverMetaAndDataOfFaultWorker(currRing, removeNode, allSubstitueUuidList);
        }
        // 3. remove del_node_info together
        LOG(INFO) << "remove del_node_info range from etcd: ";
        RetryHashRingTaskUntil([this, &processedNodes, &allSubstitueUuidList]() {
            auto status = etcdStore_->CAS(
                ETCD_RING_PREFIX, "",
                [this, &processedNodes, &allSubstitueUuidList](
                    const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
                    HashRingPb oldRing;
                    if (!oldRing.ParseFromString(oldValue)) {
                        return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
                    }
                    HashRingPb ringAfterClear = EraseFinishedDelNodeInfo(oldRing, processedNodes, allSubstitueUuidList);
                    google::protobuf::util::MessageDifferencer differencer;
                    differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
                    if (differencer.Compare(oldRing, ringAfterClear)) {
                        VLOG(1) << "Not responsible for the scale down of " << VectorToString(processedNodes)
                                << ", no need to modify";
                        return Status::OK();
                    }
                    newValue = std::make_unique<std::string>(ringAfterClear.SerializeAsString());
                    VLOG(1) << "After mark del_node_info finished: " << MapToString(ringAfterClear.del_node_info());
                    INJECT_POINT("HashRingTaskExecutor.SubmitScaleDownTask.ProcessSlowly");
                    return Status::OK();
                });
            HASH_RING_LOG_IF_ERROR(status, "Mark failed");
            return status;
        });
        RemoveClearDataTask(ranges);
    });
    LOG(INFO) << scaleThreadPool_->GetStatistics();
    return Status::OK();
}

Status HashRingTaskExecutor::SubmitScaleDownTaskMultiReplica(const HashRingPb &currRing)
{
    auto traceId = GetStringUuid();
    LOG(INFO) << "Submit scale down task of " << VectorToString(GetKeysFromPairsContainer(currRing.del_node_info()))
              << " with traceid " << traceId;

    std::unordered_map<std::string, std::string> migrateWorkerAddrForRemoveNodes;
    for (auto &removeNode : currRing.del_node_info()) {
        LOG(INFO) << "Process del_node_info: " << removeNode.first;
        HostPort removeNodePrimaryDbAddr;
        std::string removeNodeDbName;
        Status status = HashRingEvent::GetDbPrimaryLocation::GetInstance().NotifyAll(
            removeNode.first, removeNodePrimaryDbAddr, removeNodeDbName);
        if (status.IsError()) {
            LOG(ERROR) << "get primary db addr failed:" << status.ToString();
            continue;
        }
        if (currRing.del_node_info().find(removeNodePrimaryDbAddr.ToString()) != currRing.del_node_info().end()) {
            LOG(WARNING)
                << "no worker to handle scale down task, no need excute scale down task, erase del_node_info for :"
                << removeNode.first;
            auto func = [removeNode](const std::string &oldValue, std::unique_ptr<std::string> &newValue,
                                     bool & /* retry */) {
                HashRingPb oldRing;
                if (!oldRing.ParseFromString(oldValue)) {
                    return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
                }
                HashRingPb ringAfterClear = oldRing;
                ringAfterClear.mutable_del_node_info()->erase(removeNode.first);
                ringAfterClear.mutable_workers()->erase(removeNode.first);
                newValue = std::make_unique<std::string>(ringAfterClear.SerializeAsString());
                VLOG(1) << "After mark del_node_info finished: " << MapToString(ringAfterClear.del_node_info());
                return Status::OK();
            };
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(etcdStore_->CAS(ETCD_RING_PREFIX, "", func),

                                             "[dfx]Scale down failed.");
        }
        if (removeNodePrimaryDbAddr.ToString() != workerAddr_) {
            LOG(INFO) << "removeNode: " << removeNode.first
                      << " primary db location is : " << removeNodePrimaryDbAddr.ToString() << ", skip";
            continue;
        }
        migrateWorkerAddrForRemoveNodes[removeNode.first] = removeNodePrimaryDbAddr.ToString();
        LOG(INFO) << "removeNode: " << removeNode.first
                  << " primary db location is : " << removeNodePrimaryDbAddr.ToString()
                  << ", need excute scale down migration task";
        RecoverMetaAndDataOfFaultWorkerByStandbyMaster(removeNodeDbName, currRing, removeNode);
    };
    return Status::OK();
}

void HashRingTaskExecutor::GetRemoveNodeKeyWithUuidsInfo(const HashRingPb &currRing, const std::string &removeNodeAddr,
                                                         std::vector<std::string> &uuids)
{
    for (const auto &keyWithWorkerIdReplaceInfo : currRing.key_with_worker_id_meta_map()) {
        if (keyWithWorkerIdReplaceInfo.second != removeNodeAddr) {
            continue;
        }
        uuids.emplace_back(keyWithWorkerIdReplaceInfo.first);
    }
}

void HashRingTaskExecutor::ClearDataWithoutMeta(const HashRingPb &currRing, HashRange &recoverRanges)
{
    LOG(INFO) << "clear meta data without meta";
    HashRange clearRanges;
    std::vector<std::string> uuids;
    for (const auto &delInfo : currRing.del_node_info()) {
        for (const auto &range : delInfo.second.changed_ranges()) {
            if (InsertClearDataSubmitTask(range)) {
                clearRanges.emplace_back(range.from(), range.end());
            }
            if (range.workerid() == workerAddr_ && InsertClearDataTask(range)) {
                recoverRanges.emplace_back(range.from(), range.end());
            }
        }
        std::string uuid;
        HASH_RING_LOG_IF_ERROR(GetWorkeridByWorkerAddr(currRing, delInfo.first, uuid),
                               "Failed to clear data because uuid not found");
        uuids.emplace_back(uuid);
        auto iter = currRing.key_with_worker_id_meta_map().find(uuid);
        if (iter != currRing.key_with_worker_id_meta_map().end() && iter->second == workerAddr_) {
            datasystem::ChangeNodePb_RangePb tmpRange;
            tmpRange.set_from(MurmurHash3_32(uuid));
            tmpRange.set_end(MurmurHash3_32(uuid));
            if (InsertClearDataTask(tmpRange)) {
                recoverRanges.emplace_back(tmpRange.from(), tmpRange.end());
            }
        }
        GetRemoveNodeKeyWithUuidsInfo(currRing, delInfo.first, uuids);
    }
    if (clearRanges.empty()) {
        LOG(INFO) << "Skip clear data task submission because all matching clear tasks are already in progress";
        return;
    }
    ClearDataWithoutMeta(clearRanges, uuids);
}

void HashRingTaskExecutor::ClearDevClientMetaForScaledInWorker(const HashRingPb &currRing)
{
    std::vector<std::string> removeNodes;
    for (const auto &delInfo : currRing.del_node_info()) {
        std::string workerAddres = delInfo.first;
        removeNodes.emplace_back(workerAddres);
    }
    HASH_RING_LOG_IF_ERROR(HashRingEvent::ClearDevClientMetaForScaledInWorker::GetInstance().NotifyAll(removeNodes),
                           "Clear client meta failed.");
}

void HashRingTaskExecutor::RemoveClearDataTask(const HashRange &ranges)
{
    std::lock_guard<std::shared_timed_mutex> l(clearDataTaskmutex_);
    for (const auto &range : ranges) {
        auto iter = clearDataWithoutMetaTask_.find(range);
        if (iter != clearDataWithoutMetaTask_.end()) {
            clearDataWithoutMetaTask_.erase(iter);
        }
    }
}

void HashRingTaskExecutor::RemoveClearDataSubmitTask(const HashRange &ranges)
{
    std::lock_guard<std::shared_timed_mutex> l(clearDataTaskmutex_);
    for (const auto &range : ranges) {
        auto iter = clearDataWithoutMetaSubmitTasks_.find(range);
        if (iter != clearDataWithoutMetaSubmitTasks_.end()) {
            clearDataWithoutMetaSubmitTasks_.erase(iter);
        }
    }
}

bool HashRingTaskExecutor::InsertClearDataTask(const datasystem::ChangeNodePb_RangePb &range)
{
    std::lock_guard<std::shared_timed_mutex> l(clearDataTaskmutex_);
    Range rangeForCheck = std::make_pair(range.from(), range.end());
    auto iter = clearDataWithoutMetaTask_.find(rangeForCheck);
    if (iter == clearDataWithoutMetaTask_.end()) {
        clearDataWithoutMetaTask_.insert(rangeForCheck);
        return true;
    }
    return false;
}

bool HashRingTaskExecutor::InsertClearDataSubmitTask(const datasystem::ChangeNodePb_RangePb &range)
{
    std::lock_guard<std::shared_timed_mutex> l(clearDataTaskmutex_);
    Range rangeForCheck = std::make_pair(range.from(), range.end());
    auto iter = clearDataWithoutMetaSubmitTasks_.find(rangeForCheck);
    if (iter == clearDataWithoutMetaSubmitTasks_.end()) {
        clearDataWithoutMetaSubmitTasks_.insert(rangeForCheck);
        return true;
    }
    return false;
}

void HashRingTaskExecutor::ClearDataWithoutMeta(const HashRange &ranges, const std::vector<std::string> &uuids)
{
    HASH_RING_LOG_IF_ERROR(HashRingEvent::LocalClearDataWithoutMeta::GetInstance().NotifyAll(ranges, uuids),
                           "Local ClearData failed.");
    INJECT_POINT("ClearDataDelay", [] { return; });
    INJECT_POINT("notExcuteClearData", [] {
        auto injectSkip = []() {
            INJECT_POINT("notExcuteClearData.skip", [] { return true; });
            return false;
        };
        if (!injectSkip()) {
            abort();
        }
    });
    LOG(INFO) << "clear meta data without meta finish";
}

HashRingPb HashRingTaskExecutor::EraseFinishedDelNodeInfo(const HashRingPb &ring,
                                                          const std::vector<std::string> &processedNodes,
                                                          const std::vector<std::string> &allSubstitueUuidList) const
{
    // erase the finished range.
    HashRingPb ringAfterClear = ring;
    ringAfterClear.clear_del_node_info();

    for (auto &node : ring.del_node_info()) {
        if (!ContainsKey(processedNodes, node.first)) {
            (*ringAfterClear.mutable_del_node_info())[node.first] = node.second;
            continue;
        }
        for (auto range : node.second.changed_ranges()) {
            if (range.workerid() != workerAddr_) {
                (*ringAfterClear.mutable_del_node_info())[node.first].mutable_changed_ranges()->Add(std::move(range));
            }
        }
        // If the scale down task for a certain node has been completed,
        // clear the information of this node in etcd directly.
        if (ringAfterClear.del_node_info().find(node.first) == ringAfterClear.del_node_info().end()) {
            LOG(INFO) << "Scale down"
                      << " worker[" << node.first << "] finished";
            ringAfterClear.mutable_workers()->erase(node.first);
        }
    }

    // replace the uuid
    for (const auto &uuid : allSubstitueUuidList) {
        (*ringAfterClear.mutable_key_with_worker_id_meta_map())[uuid] = workerAddr_;
    }

    return ringAfterClear;
}

Status HashRingTaskExecutor::RecoverMetaWithWorkerIdOfFaultyWorker(const std::string &removeWorkerAddr,
                                                                   const HashRingPb &currRing,
                                                                   std::vector<std::string> &allSubstituteUuidList,
                                                                   bool isVoluntaryDownNodeFault)
{
    std::string removeNodeUuid;
    RETURN_IF_NOT_OK(GetWorkeridByWorkerAddr(currRing, removeWorkerAddr, removeNodeUuid));

    if (!isVoluntaryDownNodeFault) {
        auto substituteNode = currRing.key_with_worker_id_meta_map().find(removeNodeUuid);
        if (substituteNode == currRing.key_with_worker_id_meta_map().end()) {
            return Status(K_RUNTIME_ERROR, "Cannot find the substitute node of remove worker.");
        }
        if (substituteNode->second != workerAddr_) {
            LOG(INFO) << "Skip, substitute node is " << substituteNode->second;
            return Status::OK();
        }
    } else if (removeWorkerAddr != workerAddr_) {
        LOG(INFO) << "Skip, recover node is " << removeWorkerAddr;
        return Status::OK();
    }

    LOG(INFO) << workerAddr_ << " Recover meta with workerid of " << removeWorkerAddr;

    std::vector<std::string> recoverUuids{ removeNodeUuid };
    // if the removeWorker is the substitute node of any other, recover
    for (auto &keyWithWorkerIdReplaceInfo : currRing.key_with_worker_id_meta_map()) {
        if (keyWithWorkerIdReplaceInfo.second != removeWorkerAddr) {
            continue;
        }
        recoverUuids.emplace_back(keyWithWorkerIdReplaceInfo.first);
        if (!isVoluntaryDownNodeFault) {
            allSubstituteUuidList.emplace_back(keyWithWorkerIdReplaceInfo.first);
        }
    }
    RETURN_IF_NOT_OK(HashRingEvent::RecoverMetaRanges::GetInstance().NotifyAll(recoverUuids, worker::HashRange{}));
    return Status::OK();
}

Status HashRingTaskExecutor::ExcuteScaleDownMigrateUuidTask(const std::string &removeWorkerAddr,
                                                            const HashRingPb &currRing, bool isVoluntaryDownNodeFault,
                                                            ScaleDownMigrationTaskInfo &taskInfos)
{
    MigrateScaleDownInfo uuidRecoverTask;
    HostPort removeWorkerPrimaryReplicaAddr;
    std::string removeWorkerDbName;
    RETURN_IF_NOT_OK(HashRingEvent::GetDbPrimaryLocation::GetInstance().NotifyAll(
        removeWorkerAddr, removeWorkerPrimaryReplicaAddr, removeWorkerDbName));
    HostPort destWorkerReceiveMetaAddr;
    std::string destWorkerReceiveMetaDbName;
    if (!isVoluntaryDownNodeFault) {
        auto substituteNode = currRing.key_with_worker_id_meta_map().find(removeWorkerDbName);
        if (substituteNode == currRing.key_with_worker_id_meta_map().end()) {
            return Status(K_RUNTIME_ERROR, "Cannot find the substitute node of remove worker.");
        }
        if (removeWorkerPrimaryReplicaAddr.ToString() != workerAddr_) {
            return Status::OK();
        }
        RETURN_IF_NOT_OK(HashRingEvent::GetDbPrimaryLocation::GetInstance().NotifyAll(
            substituteNode->second, destWorkerReceiveMetaAddr, destWorkerReceiveMetaDbName));
        uuidRecoverTask.destPrimaryReplicaAddress = destWorkerReceiveMetaAddr.ToString();
        uuidRecoverTask.ranges.emplace_back(MurmurHash3_32(removeWorkerDbName), MurmurHash3_32(removeWorkerDbName));
    } else {
        taskInfos[removeWorkerDbName].destWorker = removeWorkerAddr;
        taskInfos[removeWorkerDbName].destPrimaryReplicaAddress = removeWorkerPrimaryReplicaAddr.ToString();
    }

    LOG(INFO) << workerAddr_ << " Recover meta with workerid of " << removeWorkerAddr;

    LOG_IF_ERROR(HashRingEvent::MigrateRanges::GetInstance().NotifyAll(
                     removeWorkerDbName, uuidRecoverTask.destPrimaryReplicaAddress, destWorkerReceiveMetaDbName,
                     uuidRecoverTask.ranges, false),
                 "scale down migrate task failed");
    return Status::OK();
}

Status HashRingTaskExecutor::GetWorkerByHash(const HashRingPb &ring, uint32_t hash, std::string &workerId)
{
    for (auto &worker : ring.workers()) {
        if (HashRing::hashFunction_(worker.second.worker_uuid()) == hash) {
            workerId = worker.first;
            return Status::OK();
        }
    }
    return Status(K_RUNTIME_ERROR, "cant find worker by hash, the worker not exist");
}

HashRange HashRingTaskExecutor::GetWorkHashRangeFromChangeNodePb(const HashRingPb &ring, const ChangeNodePb &changeNode,
                                                                 std::string &workerId)
{
    HashRange targetRanges;
    for (auto &range : changeNode.changed_ranges()) {
        if (range.workerid() != workerAddr_ || range.finished()) {
            continue;
        }
        // if range from == end, the worker receive the key with uuid of voluntary scale node
        if (range.from() == range.end()) {
            LOG_IF_ERROR(GetWorkerByHash(ring, range.from(), workerId), "failed to get worker by hash");
            continue;
        }
        if (range.from() < range.end()) {
            targetRanges.emplace_back(range.from(), range.end());
        } else {
            targetRanges.emplace_back(range.from(), UINT32_MAX);
            targetRanges.emplace_back(0, range.end());
        }
    }
    return targetRanges;
}

ScaleDownMigrationTaskInfo HashRingTaskExecutor::GetWorkHashRangeFromChangeNodePbByDbName(
    const HashRingPb &ring, const ChangeNodePb &changeNode, std::string &workerId)
{
    ScaleDownMigrationTaskInfo infos;
    for (auto &range : changeNode.changed_ranges()) {
        // if range from == end, the worker receive the key with uuid of voluntary scale node
        HostPort hashRangeRecoverDbAddr;
        std::string hashRangeRecoverDbName;
        // wait retry?
        if (HashRingEvent::GetDbPrimaryLocation::GetInstance()
                .NotifyAll(range.workerid(), hashRangeRecoverDbAddr, hashRangeRecoverDbName)
                .IsError()) {
            LOG(ERROR) << "get primary db addr failed";
            continue;
        }
        if (range.finished()) {
            continue;
        }
        if (range.from() == range.end()) {
            std::string UuidRecoverWorkerAddr;
            LOG_IF_ERROR(GetWorkerByHash(ring, range.from(), UuidRecoverWorkerAddr), "failed to get worker by hash");
            workerId = UuidRecoverWorkerAddr;
        }
        if (range.from() < range.end()) {
            infos[hashRangeRecoverDbName].destPrimaryReplicaAddress = hashRangeRecoverDbAddr.ToString();
            infos[hashRangeRecoverDbName].ranges.emplace_back(range.from(), range.end());
        } else {
            infos[hashRangeRecoverDbName].destPrimaryReplicaAddress = hashRangeRecoverDbAddr.ToString();
            infos[hashRangeRecoverDbName].ranges.emplace_back(range.from(), UINT32_MAX);
            infos[hashRangeRecoverDbName].ranges.emplace_back(0, range.end());
        }
        infos[hashRangeRecoverDbName].destWorker = range.workerid();
    }
    return infos;
}

void HashRingTaskExecutor::RecoverMetaAndDataOfFaultWorker(
    const HashRingPb &currRing,
    const google::protobuf::Map<std::basic_string<char>, datasystem::ChangeNodePb>::value_type &removeNode,
    std::vector<std::string> &allSubstitueUuidList)
{
    // 1. delete if needed.
    // 2.1 recover according to hash range
    std::string workerId;
    auto hashRanges = GetWorkHashRangeFromChangeNodePb(currRing, removeNode.second, workerId);
    if (!workerId.empty()) {
        HASH_RING_LOG_IF_ERROR(RecoverMetaWithWorkerIdOfFaultyWorker(workerId, currRing, allSubstitueUuidList, true),
                               "[dfx]voluntary worker recover not success: ");
    }
    auto status = HashRingEvent::RecoverMetaRanges::GetInstance().NotifyAll(std::vector<std::string>{}, hashRanges);
    HASH_RING_LOG_IF_ERROR(status, "[dfx]recover not success");
    INJECT_POINT("SubmitScaleDownTask.delay", [](uint32_t delay_s) { sleep(delay_s); });
    // 2.2 recover meta key with id (only substitute node needs)
    status = RecoverMetaWithWorkerIdOfFaultyWorker(removeNode.first, currRing, allSubstitueUuidList);
    HASH_RING_LOG_IF_ERROR(status, "[dfx]recover not success");
}

void HashRingTaskExecutor::RecoverMetaAndDataOfFaultWorkerByStandbyMaster(
    const std::string &scaleDownWorkerDbName, const HashRingPb &currRing,
    const google::protobuf::Map<std::basic_string<char>, datasystem::ChangeNodePb>::value_type &removeNode)
{
    std::vector<std::future<void>> tasks;
    std::string workerId;
    auto recoverMigrateTaskInfos = GetWorkHashRangeFromChangeNodePbByDbName(currRing, removeNode.second, workerId);
    if (!workerId.empty()) {
        LOG_IF_ERROR(ExcuteScaleDownMigrateUuidTask(workerId, currRing, true, recoverMigrateTaskInfos),
                     "[dfx]voluntary worker recover not success: ");
    } else {
        LOG_IF_ERROR(ExcuteScaleDownMigrateUuidTask(removeNode.first, currRing, false, recoverMigrateTaskInfos),
                     "[dfx] worker recover not success: ");
    }
    for (const auto &recoverMigrateTaskInfo : recoverMigrateTaskInfos) {
        SubmitScaleDownMigrateTask(recoverMigrateTaskInfo.second, recoverMigrateTaskInfo.first, scaleDownWorkerDbName,
                                   removeNode);
    }
    return;
}

void HashRingTaskExecutor::SubmitScaleDownMigrateTask(
    const MigrateScaleDownInfo &info, const std::string &recoverDbName, const std::string &scaleDownWorkerDbName,
    const google::protobuf::Map<std::basic_string<char>, datasystem::ChangeNodePb>::value_type &removeNode)
{
    auto func = [scaleDownWorkerDbName, info, recoverDbName, removeNode](
                    const std::string &oldValue, std::unique_ptr<std::string> &newValue, bool & /* retry */) {
        HashRingPb oldRing;
        if (!oldRing.ParseFromString(oldValue)) {
            return Status(K_RUNTIME_ERROR, "Failed to parse HashRingPb from string");
        }
        HashRingPb ringAfterClear = oldRing;
        auto nodePb = ringAfterClear.mutable_del_node_info()->find(removeNode.first);
        if (nodePb == ringAfterClear.mutable_del_node_info()->end()) {
            LOG(ERROR) << "cant find del node info for :" << removeNode.first;
            return Status(K_RUNTIME_ERROR, "Failed to get del node info");
        }
        bool allFinished = true;
        for (auto &changePb : *nodePb->second.mutable_changed_ranges()) {
            if (changePb.workerid() == info.destWorker) {
                changePb.set_finished(true);
                continue;
            } else if (!changePb.finished()) {
                allFinished = false;
            }
        }
        if (allFinished) {
            ringAfterClear.mutable_del_node_info()->erase(removeNode.first);
            ringAfterClear.mutable_workers()->erase(removeNode.first);
        }
        google::protobuf::util::MessageDifferencer differencer;
        differencer.set_repeated_field_comparison(google::protobuf::util::MessageDifferencer::AS_SET);
        if (differencer.Compare(oldRing, ringAfterClear)) {
            VLOG(1) << "Not responsible for the scale down of " << removeNode.first << ", no need to modify";
            return Status::OK();
        }
        newValue = std::make_unique<std::string>(ringAfterClear.SerializeAsString());
        VLOG(1) << "After mark del_node_info finished: " << MapToString(ringAfterClear.del_node_info());
        return Status::OK();
    };
    auto traceId = GetStringUuid();
    LOG(INFO) << "Start scale down migrate task with traceId " + traceId;
    scaleThreadPool_->Execute([this, info, recoverDbName, removeNode, scaleDownWorkerDbName, func, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        HASH_RING_LOG_IF_ERROR(
            HashRingEvent::MigrateRanges::GetInstance().NotifyAll(scaleDownWorkerDbName, info.destPrimaryReplicaAddress,
                                                                  recoverDbName, info.ranges, false),
            "scale down migrate task failed");
        RetryHashRingTaskUntil([this, &func]() {
            auto status = etcdStore_->CAS(ETCD_RING_PREFIX, "", func);
            HASH_RING_LOG_IF_ERROR(status, "Mark failed.");
            return status;
        });
    });
}

Status HashRingTaskExecutor::VoluntaryRecoveryAsyncTask(const HashRingPb &oldRing,
                                                        const std::string &voluntaryDonwWorker)
{
    std::string workerUuid;
    HashRange targetRanges;
    for (const auto &info : oldRing.add_node_info()) {
        if (info.first != workerAddr_) {
            continue;
        }
        for (const auto &range : info.second.changed_ranges()) {
            if (range.workerid() != voluntaryDonwWorker) {
                continue;
            }
            if (range.from() < range.end()) {
                targetRanges.emplace_back(range.from(), range.end());
            } else if (range.from() == range.end()) {
                workerUuid = oldRing.workers().at(voluntaryDonwWorker).worker_uuid();
            } else {
                targetRanges.emplace_back(range.from(), UINT32_MAX);
                targetRanges.emplace_back(0, range.end());
            }
        }
    }
    std::vector<std::string> recoverUuids;
    if (!workerUuid.empty()) {
        recoverUuids.emplace_back(workerUuid);
        // if the removeWorker is the substitute node of any other, recover
        for (auto &keyWithWorkerIdReplaceInfo : oldRing.key_with_worker_id_meta_map()) {
            if (keyWithWorkerIdReplaceInfo.second != workerUuid) {
                continue;
            }
            recoverUuids.emplace_back(keyWithWorkerIdReplaceInfo.first);
        }
    }
    RETURN_OK_IF_TRUE(recoverUuids.empty() && targetRanges.empty());
    LOG(INFO) << "local worker: " << workerAddr_
              << " recover async task for voluntary scale down worker: " << voluntaryDonwWorker;
    RETURN_IF_NOT_OK_APPEND_MSG(
        HashRingEvent::RecoverAsyncTaskRanges::GetInstance().NotifyAll(recoverUuids, targetRanges),
        "voluntary scale down recover async failed");

    return Status::OK();
}

void HashRingTaskExecutor::SubmitVoluntaryRecoveryAsyncTask(const HashRingPb &oldRing, const HashRingPb &newRing)
{
    std::vector<std::string> voluntaryDownWorkerAddrs;
    for (const auto &worker : oldRing.workers()) {
        if (worker.second.state() == WorkerPb::LEAVING
            && (newRing.workers().find(worker.first) == newRing.workers().end()
                || newRing.workers().at(worker.first).worker_uuid().empty())) {
            voluntaryDownWorkerAddrs.emplace_back(worker.first);
        }
    }
    if (voluntaryDownWorkerAddrs.empty()) {
        return;
    }
    auto traceId = Trace::Instance().GetTraceID();
    scaleThreadPool_->Execute([this, oldRing, voluntaryDownWorkerAddrs, traceId]() {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        for (const auto &worker : voluntaryDownWorkerAddrs) {
            HASH_RING_LOG_IF_ERROR(VoluntaryRecoveryAsyncTask(oldRing, worker), "recover async task failed");
        }
        LOG(INFO) << "recover voluntary scale down worker async task finished";
    });
    LOG(INFO) << scaleThreadPool_->GetStatistics();
}

void HashRingTaskExecutor::RestoreScalingTask(const HashRingPb &currRing, bool isRestartScenario)
{
    if (scaleThreadPool_->GetRunningTasksNum() != 0 || scaleThreadPool_->GetWaitingTasksNum() != 0) {
        LOG(INFO) << "skip restore, there is task executing, " << scaleThreadPool_->GetStatistics();
        return;
    }

    // We need to wait for the worker to be healthy to make sure the migration task happened after rocksdb's
    // recovery. No need to check the worker healthy in the non-restart restoration scenario because the node will
    // set itself unhealthy in voluntary scale down scenario.
    auto waitUntilHealth = [this, isRestartScenario] {
        INJECT_POINT("waitUntilHealth", [] { return true; });
        while (isRestartScenario && !exitFlag_ && !IsHealthy()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return !exitFlag_;
    };

    auto traceId = Trace::Instance().GetTraceID();
    scaleThreadPool_->Execute([this, waitUntilHealth, currRing, traceId] {
        TraceGuard traceGuard = Trace::Instance().SetTraceNewID(traceId);
        if (!waitUntilHealth()) {
            return;
        }

        LOG(INFO) << "Restore the scaling task " << HashRing::SummarizeHashRing(currRing);
        // restore scale up task.
        if (!currRing.add_node_info().empty()) {
            HASH_RING_LOG_IF_ERROR(SubmitScaleUpTask(currRing), "restore scale up task failed.");
        }
        // restore scale down task.
        if (!currRing.del_node_info().empty()) {
            HASH_RING_LOG_IF_ERROR(SubmitScaleDownTask(currRing), "restore scale down task failed.");
        }
    });
    LOG(INFO) << scaleThreadPool_->GetStatistics();
}

void HashRingTaskExecutor::RetryHashRingTaskUntil(std::function<Status()> &&hashRingTask,
                                                  std::function<bool(const Status &)> &&breaker)
{
    if (breaker != nullptr) {
        LOG_IF_ERROR(RetryUntil(std::move(hashRingTask), std::move(breaker)), "RetryHashRingTaskUntil failed");
    } else {
        static const std::unordered_set<StatusCode> retryStatusSet = { K_TRY_AGAIN, K_RPC_UNAVAILABLE,
                                                                       K_RETRY_IF_LEAVING };
        static const auto defaultBreaker = [this](const Status &status) {
            return status.IsOk() || exitFlag_ || retryStatusSet.find(status.GetCode()) == retryStatusSet.end();
        };
        LOG_IF_ERROR(RetryUntil(std::move(hashRingTask), defaultBreaker), "RetryHashRingTaskUntil failed");
    }
}
}  // namespace worker
}  // namespace datasystem
